#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::unwrap_used
    )
)]

use agent_os::config::{
    AppConfig, ConfigProfile, discover_config, load_config,
    validate_seed_config as validate_seed_config_impl, write_profile_config,
};
use agent_os::executor::CommandExecutor;
use agent_os::models::{
    Policy, is_valid_env_var_name, memory_relevance_score, normalize_list, tail_text_by_bytes,
    text_tail_was_truncated,
};
use agent_os::policy::{check_shell_command, check_shell_writes, check_workspace};
use agent_os::shell_capture::run_shell_capture;
use agent_os::tools::is_valid_tool_arg_key;
use agent_os::{
    Agent, AgentId, AgentKind, AgentProfile, AgentStatus, AgentUpdate, ApiAuth, ApiCors, ApiServer,
    ApprovalRequest, ApprovalStatus, DaemonState, DaemonStatus, EvalRecord, EvalRunDetails, Event,
    EventKind, GitCommandOutput, LaunchdService, LaunchdServiceOptions, McpServer, MemoryRecallHit,
    MemoryRecord, MemoryVisibility, OperatingSystem, Priority, RunArtifact, RunArtifactKind, RunId,
    RunRecord, RunStatus, Runtime, RuntimeReport, Scheduler, SecretsBackend, SecretsBackendKind,
    SqliteStore, Store, Task, TaskId, TaskStatus, TaskUpdate, ToolDefinition, ToolId,
    ToolInvocation, ToolKind, ToolUpdate, WindowsScheduledTask, WindowsScheduledTaskOptions,
    WorkerNode, Workflow, WorkflowId, WorkflowProgress, WorkflowTemplate, WorkflowTemplateEdge,
    build_launchd_service as build_launchd_service_definition,
    build_systemd_service as build_systemd_service_definition,
    build_windows_scheduled_task as build_windows_scheduled_task_definition,
    default_launchd_plist_path, install_launchd_service as install_launchd_service_definition,
    install_systemd_service as install_systemd_service_definition, is_valid_secret_reference,
    memory_recall_hit, metrics_json, metrics_prometheus, metrics_unavailable_json,
    normalize_cors_origin, openapi_schema, render_workflow_template_text, repair_state,
    resolve_git_cwd, resolve_launchd_domain, run_external_command, run_git_capture,
    run_git_command, run_launchctl, run_systemctl, secret_check_report,
    terminate_child_process_tree,
    uninstall_launchd_service as uninstall_launchd_service_definition,
    uninstall_systemd_service as uninstall_systemd_service_definition, validate_json_schema,
    validate_service_control_inputs, validate_state, validate_systemd_control_inputs,
    validate_tool_invocation, validate_tool_template, workflow_template_edges,
    workflow_template_task,
};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use clap::{Args, CommandFactory, Parser, Subcommand};
use clap_complete::Shell;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, IsTerminal, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tabled::{Table, Tabled, settings::Style};

const DAEMON_JSON_TICK_HISTORY_LIMIT: usize = 1000;
const MIN_API_TOKEN_BYTES: usize = 8;
const MCP_CHILD_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Parser)]
#[command(name = "agent-os")]
#[command(about = "A local operating system for coordinating AI agents.")]
#[command(
    after_help = "Examples:\n  agent-os config init --profile safe\n  agent-os --state ./sandbox init --profile dev\n  agent-os --state ./sandbox task create \"Review README\" --need rust\n  agent-os --state ./sandbox run --dry-run\n  agent-os --state ./sandbox daemon status"
)]
#[command(version)]
struct Cli {
    #[arg(
        long,
        global = true,
        help = "Path to state.json or a directory that will contain it"
    )]
    state: Option<std::path::PathBuf>,
    #[arg(
        long,
        global = true,
        help = "Emit machine-readable JSON where supported"
    )]
    json: bool,
    #[arg(long, global = true, help = "Path to agent-os.toml")]
    config: Option<std::path::PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Initialize durable Agent OS state")]
    Init(InitArgs),
    #[command(about = "Print a concise state summary")]
    Status,
    #[command(about = "Print monitor-friendly counters")]
    Metrics(MetricsArgs),
    #[command(about = "Run state and config preflight diagnostics")]
    Doctor,
    #[command(about = "Create, inspect, and validate configuration")]
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    #[command(about = "Export, import, migrate, repair, and validate state")]
    State {
        #[command(subcommand)]
        command: StateCommand,
    },
    #[command(about = "Register agents, heartbeats, claims, and capacity")]
    Agent {
        #[command(subcommand)]
        command: AgentCommand,
    },
    #[command(about = "Create, inspect, schedule, and update tasks")]
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
    #[command(about = "Register reusable tool definitions")]
    Tool {
        #[command(subcommand)]
        command: ToolCommand,
    },
    #[command(about = "Manage shared agent memory")]
    Memory {
        #[command(subcommand)]
        command: MemoryCommand,
    },
    #[command(about = "Inspect and use agent profiles, workflow templates, and MCP servers")]
    Registry {
        #[command(subcommand)]
        command: RegistryCommand,
    },
    #[command(about = "Register and monitor distributed worker nodes")]
    Worker {
        #[command(subcommand)]
        command: WorkerCommand,
    },
    #[command(about = "Record and inspect benchmark evaluation results")]
    Eval {
        #[command(subcommand)]
        command: EvalCommand,
    },
    #[command(about = "Register secret manager backends")]
    Secrets {
        #[command(subcommand)]
        command: SecretsCommand,
    },
    #[command(about = "Review and resolve human approval gates")]
    Approval {
        #[command(subcommand)]
        command: ApprovalCommand,
    },
    #[command(about = "Create branches, commits, PRs, and code review tasks")]
    Git {
        #[command(subcommand)]
        command: GitCommand,
    },
    #[command(about = "Inspect the durable event stream")]
    Events(EventsArgs),
    #[command(about = "Inspect, tail, replay, and cancel runs")]
    Runs {
        #[command(subcommand)]
        command: RunsCommand,
    },
    #[command(about = "Run or control the scheduler daemon")]
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    #[command(about = "Render, install, and control launchd services")]
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
    },
    #[command(about = "Serve Agent OS tools over MCP stdio")]
    Mcp {
        #[command(subcommand)]
        command: McpCommand,
    },
    #[command(about = "Generate shell completions")]
    Completions(CompletionsArgs),
    #[command(about = "Serve or inspect the local HTTP API")]
    Api {
        #[command(subcommand)]
        command: ApiCommand,
    },
    #[command(about = "Create and advance dependency-aware workflows")]
    Workflow {
        #[command(subcommand)]
        command: WorkflowCommand,
    },
    #[command(about = "Run one scheduler tick and optionally execute work")]
    Run(RunArgs),
}

#[derive(Args)]
struct InitArgs {
    #[arg(long, help = "Override the config/default OS name")]
    name: Option<String>,
    #[arg(long, help = "Overwrite an existing state file")]
    force: bool,
    #[arg(
        long,
        value_parser = ["safe", "dev", "autonomous", "ci"],
        help = "Apply a config profile before initialization; missing config defaults to safe"
    )]
    profile: Option<String>,
}

#[derive(Args)]
struct MetricsArgs {
    #[arg(long, help = "Print Prometheus text exposition format")]
    prometheus: bool,
}

#[derive(Subcommand)]
enum ConfigCommand {
    #[command(about = "Write a default agent-os.toml")]
    Init(ConfigInitArgs),
    #[command(about = "Show the loaded config or default fallback")]
    Show,
    #[command(about = "Validate config load and semantic rules")]
    Validate,
}

#[derive(Subcommand)]
enum StateCommand {
    #[command(about = "Print or write a state JSON snapshot")]
    Export(StateExportArgs),
    #[command(about = "Import a validated state JSON file")]
    Import(StateImportArgs),
    #[command(about = "Write a point-in-time state backup")]
    Backup(StateBackupArgs),
    #[command(about = "Migrate state JSON to the current schema")]
    Migrate(StateMigrateArgs),
    #[command(about = "Prune old finished runs, logs, and events")]
    Prune(StatePruneArgs),
    #[command(about = "Repair safe state consistency issues")]
    Repair(StateRepairArgs),
    #[command(about = "Initialize or sync a SQLite backend snapshot")]
    Sqlite(StateSqliteArgs),
    #[command(about = "Validate durable state consistency")]
    Validate,
}

#[derive(Args)]
struct StateExportArgs {
    #[arg(long, help = "Write exported JSON to this path instead of stdout")]
    output: Option<std::path::PathBuf>,
    #[arg(long, help = "Report export path without writing a file")]
    dry_run: bool,
}

#[derive(Args)]
struct StateImportArgs {
    path: std::path::PathBuf,
    #[arg(long, help = "Overwrite existing state")]
    force: bool,
    #[arg(long, help = "Validate import without writing state")]
    dry_run: bool,
}

#[derive(Args)]
struct StateBackupArgs {
    #[arg(long, help = "Backup path; defaults next to state.json")]
    output: Option<std::path::PathBuf>,
    #[arg(long, help = "Report backup path without writing a file")]
    dry_run: bool,
}

#[derive(Args)]
struct StateMigrateArgs {
    #[arg(long, help = "Read state from this path; defaults to current state")]
    input: Option<std::path::PathBuf>,
    #[arg(
        long,
        help = "Write migrated state to this path; defaults to current state"
    )]
    output: Option<std::path::PathBuf>,
    #[arg(long, help = "Report migration outcome without writing state")]
    dry_run: bool,
}

#[derive(Args)]
struct StatePruneArgs {
    #[arg(
        long,
        default_value_t = 100,
        help = "Keep this many newest finished runs"
    )]
    keep_runs: usize,
    #[arg(long, default_value_t = 500, help = "Keep this many newest events")]
    keep_events: usize,
    #[arg(long, help = "Report what would be removed without changing state")]
    dry_run: bool,
}

#[derive(Args)]
struct StateRepairArgs {
    #[arg(long, help = "Report repairs without writing state")]
    dry_run: bool,
}

#[derive(Args)]
struct StateSqliteArgs {
    #[arg(long, help = "SQLite database path; defaults next to state.json")]
    output: Option<std::path::PathBuf>,
    #[arg(
        long,
        conflicts_with = "restore",
        help = "Create tables without importing JSON state"
    )]
    init_only: bool,
    #[arg(
        long,
        conflicts_with = "init_only",
        help = "Restore JSON state from the SQLite snapshot"
    )]
    restore: bool,
    #[arg(
        long,
        requires = "restore",
        help = "Overwrite existing JSON state when restoring"
    )]
    force: bool,
    #[arg(
        long,
        requires = "restore",
        help = "Validate restore without writing JSON state"
    )]
    dry_run: bool,
}

#[derive(Args)]
struct ConfigInitArgs {
    #[arg(long, help = "Overwrite an existing config file")]
    force: bool,
    #[arg(
        long,
        default_value = "safe",
        value_parser = ["safe", "dev", "autonomous", "ci"],
        help = "Config profile to write: safe, dev, autonomous, or ci"
    )]
    profile: String,
}

#[derive(Subcommand)]
enum AgentCommand {
    #[command(about = "Register a new agent")]
    Add(AgentAddArgs),
    #[command(about = "Show one agent")]
    Show(AgentShowArgs),
    #[command(about = "Update agent metadata, model, capabilities, or capacity")]
    Update(AgentUpdateArgs),
    #[command(about = "Record an agent heartbeat and optional lease")]
    Heartbeat(AgentHeartbeatArgs),
    #[command(about = "Claim the next ready task for an agent")]
    Claim(AgentClaimArgs),
    #[command(about = "Remove an unreferenced agent")]
    Remove(AgentShowArgs),
    #[command(about = "List and filter agents")]
    List(AgentListArgs),
}

#[derive(Args)]
struct AgentAddArgs {
    name: String,
    #[arg(long, default_value = "builder")]
    kind: String,
    #[arg(long)]
    model: Option<String>,
    #[arg(long = "cap", value_delimiter = ',', required = true)]
    capabilities: Vec<String>,
    #[arg(long, default_value_t = 1)]
    parallel: usize,
}

#[derive(Args)]
struct AgentShowArgs {
    id: String,
}

#[derive(Args)]
struct AgentUpdateArgs {
    id: String,
    #[arg(long, help = "Replace the agent display name without changing its ID")]
    name: Option<String>,
    #[arg(long, help = "Replace the agent kind")]
    kind: Option<String>,
    #[arg(long, help = "Replace the model override")]
    model: Option<String>,
    #[arg(long, help = "Clear the model override")]
    clear_model: bool,
    #[arg(
        long = "cap",
        value_delimiter = ',',
        help = "Replace advertised capabilities"
    )]
    capabilities: Vec<String>,
    #[arg(long, help = "Replace maximum parallel task capacity")]
    parallel: Option<usize>,
}

#[derive(Args)]
struct AgentHeartbeatArgs {
    id: String,
    #[arg(
        long,
        default_value = "online",
        help = "Agent status: online/up, busy, paused/pause, or offline/down"
    )]
    status: String,
    #[arg(long)]
    lease_seconds: Option<i64>,
}

#[derive(Args)]
struct AgentClaimArgs {
    id: String,
    #[arg(long)]
    lease_seconds: Option<i64>,
}

#[derive(Args)]
struct AgentListArgs {
    #[arg(
        long,
        help = "Only show agents with this status: online/up, busy, paused/pause, or offline/down"
    )]
    status: Option<String>,
    #[arg(long, help = "Only show agents with this kind")]
    kind: Option<String>,
    #[arg(
        long = "cap",
        value_delimiter = ',',
        help = "Only show agents with this capability"
    )]
    capabilities: Vec<String>,
    #[arg(
        long,
        help = "Only show agents updated at or after this RFC3339 timestamp"
    )]
    since: Option<String>,
    #[arg(
        long,
        help = "Only show agents updated at or before this RFC3339 timestamp"
    )]
    until: Option<String>,
    #[arg(long, help = "Only show agents whose text contains this query")]
    query: Option<String>,
    #[arg(long, help = "Maximum number of recently updated agents to show")]
    limit: Option<usize>,
}

#[derive(Subcommand)]
enum TaskCommand {
    #[command(about = "Create a pending task")]
    Create(TaskCreateArgs),
    #[command(about = "List and filter tasks")]
    List(TaskListArgs),
    #[command(about = "Show one task")]
    Show(TaskShowArgs),
    #[command(about = "Update a pending or blocked task")]
    Update(TaskUpdateArgs),
    #[command(about = "Assign a ready task to an agent")]
    Assign(TaskAssignArgs),
    #[command(about = "Change task priority")]
    Priority(TaskPriorityArgs),
    #[command(about = "Replace or clear task dependencies")]
    Dependencies(TaskDependenciesArgs),
    #[command(about = "Replace task plan steps")]
    Plan(TaskPlanArgs),
    #[command(about = "Mark a task complete")]
    Complete(TaskFinishArgs),
    #[command(about = "Mark a task failed")]
    Fail(TaskFinishArgs),
    #[command(about = "Mark a task blocked")]
    Block(TaskFinishArgs),
    #[command(about = "Cancel a task")]
    Cancel(TaskFinishArgs),
    #[command(about = "Retry a failed, cancelled, or blocked task")]
    Retry(TaskFinishArgs),
    #[command(about = "Return a blocked task to pending")]
    Unblock(TaskFinishArgs),
    #[command(about = "Delete an unreferenced task")]
    Delete(TaskShowArgs),
    #[command(about = "Recover stale running tasks")]
    Recover(TaskRecoverArgs),
}

#[derive(Args)]
struct TaskCreateArgs {
    title: String,
    #[arg(long)]
    objective: Option<String>,
    #[arg(
        long,
        help = "Shell command to execute when `run --execute` schedules the task"
    )]
    command: Option<String>,
    #[arg(long, help = "Working directory for the task command")]
    cwd: Option<String>,
    #[arg(long, help = "Registered tool ID to invoke when the task executes")]
    tool: Option<String>,
    #[arg(
        long = "arg",
        value_name = "KEY=VALUE",
        help = "Tool argument; repeat for multiple arguments"
    )]
    tool_args: Vec<String>,
    #[arg(
        long = "secret-arg",
        value_name = "KEY=ENV_VAR",
        help = "Tool argument resolved from an environment variable at execution time"
    )]
    secret_tool_args: Vec<String>,
    #[arg(
        long,
        default_value = "normal",
        help = "Task priority: low, normal, high, or critical/urgent"
    )]
    priority: String,
    #[arg(long = "need", value_delimiter = ',')]
    required_capabilities: Vec<String>,
    #[arg(long, help = "Maximum execution attempts before the task fails")]
    max_attempts: Option<u32>,
    #[arg(
        long = "after",
        value_delimiter = ',',
        help = "Task IDs that must complete first"
    )]
    dependencies: Vec<String>,
}

#[derive(Args)]
struct TaskListArgs {
    #[arg(long)]
    all: bool,
    #[arg(
        long,
        help = "Only show tasks with this status: pending, running, blocked, complete/completed, failed, or cancelled/canceled"
    )]
    status: Option<String>,
    #[arg(
        long,
        help = "Only show tasks with this priority: low, normal, high, or critical/urgent"
    )]
    priority: Option<String>,
    #[arg(long, help = "Only show tasks assigned to this agent")]
    agent: Option<String>,
    #[arg(long, help = "Only show tasks invoking this tool")]
    tool: Option<String>,
    #[arg(long = "after", help = "Only show tasks depending on this task")]
    dependency: Option<String>,
    #[arg(
        long = "cap",
        value_delimiter = ',',
        help = "Only show tasks requiring this capability"
    )]
    capabilities: Vec<String>,
    #[arg(
        long,
        help = "Only show tasks updated at or after this RFC3339 timestamp"
    )]
    since: Option<String>,
    #[arg(
        long,
        help = "Only show tasks updated at or before this RFC3339 timestamp"
    )]
    until: Option<String>,
    #[arg(long, help = "Only show tasks whose text contains this query")]
    query: Option<String>,
    #[arg(long, help = "Maximum number of recently updated tasks to show")]
    limit: Option<usize>,
}

#[derive(Args)]
struct TaskShowArgs {
    id: String,
}

#[derive(Args)]
struct TaskUpdateArgs {
    id: String,
    #[arg(long, help = "Replace the task title")]
    title: Option<String>,
    #[arg(long, help = "Replace the task objective")]
    objective: Option<String>,
    #[arg(long, help = "Replace the shell command")]
    command: Option<String>,
    #[arg(long, help = "Clear the shell command")]
    clear_command: bool,
    #[arg(long, help = "Replace the registered tool invocation")]
    tool: Option<String>,
    #[arg(long, help = "Clear the registered tool invocation")]
    clear_tool: bool,
    #[arg(long = "arg", help = "Replace or add plain tool argument KEY=VALUE")]
    tool_args: Vec<String>,
    #[arg(
        long = "secret-arg",
        help = "Replace or add secret tool argument KEY=ENV_VAR"
    )]
    secret_tool_args: Vec<String>,
    #[arg(long, help = "Clear plain tool arguments")]
    clear_args: bool,
    #[arg(long, help = "Clear secret tool arguments")]
    clear_secret_args: bool,
    #[arg(long, help = "Replace the working directory")]
    cwd: Option<String>,
    #[arg(long, help = "Clear the working directory")]
    clear_cwd: bool,
    #[arg(
        long = "need",
        value_delimiter = ',',
        help = "Replace required capabilities"
    )]
    required_capabilities: Vec<String>,
    #[arg(long, help = "Clear required capabilities")]
    clear_needs: bool,
    #[arg(long, help = "Replace maximum execution attempts")]
    max_attempts: Option<u32>,
}

#[derive(Args)]
struct TaskAssignArgs {
    id: String,
    agent: String,
}

#[derive(Args)]
struct TaskPriorityArgs {
    id: String,
    #[arg(
        long,
        help = "New task priority: low, normal, high, or critical/urgent"
    )]
    priority: String,
}

#[derive(Args)]
struct TaskDependenciesArgs {
    id: String,
    #[arg(
        long = "after",
        value_delimiter = ',',
        help = "Replace dependencies with these task IDs"
    )]
    dependencies: Vec<String>,
    #[arg(long, help = "Remove all dependencies")]
    clear: bool,
}

#[derive(Args)]
struct TaskPlanArgs {
    id: String,
    #[arg(long = "step", required = true)]
    steps: Vec<String>,
}

#[derive(Args)]
struct TaskFinishArgs {
    id: String,
    #[arg(long)]
    note: Option<String>,
}

#[derive(Args)]
struct TaskRecoverArgs {
    #[arg(long, default_value_t = 1800)]
    older_than_seconds: i64,
}

#[derive(Subcommand)]
enum ToolCommand {
    #[command(about = "Register a reusable tool")]
    Add(ToolAddArgs),
    #[command(about = "List and filter tools")]
    List(ToolListArgs),
    #[command(about = "Show one tool")]
    Show(ToolShowArgs),
    #[command(about = "Update a tool definition")]
    Update(ToolUpdateArgs),
    #[command(about = "Remove an unreferenced tool")]
    Remove(ToolShowArgs),
}

#[derive(Args)]
struct ToolAddArgs {
    name: String,
    #[arg(
        long,
        default_value = "shell",
        help = "Tool kind: shell, file-read/read-file, or file-write/write-file"
    )]
    kind: String,
    #[arg(long, default_value = "")]
    description: String,
    #[arg(long = "need", value_delimiter = ',')]
    required_capabilities: Vec<String>,
    #[arg(long, help = "Command template, using placeholders like {message}")]
    command_template: String,
    #[arg(long)]
    cwd: Option<String>,
}

#[derive(Args)]
struct ToolShowArgs {
    id: String,
}

#[derive(Args)]
struct ToolUpdateArgs {
    id: String,
    #[arg(
        long,
        help = "Replace the tool kind: shell, file-read/read-file, or file-write/write-file"
    )]
    kind: Option<String>,
    #[arg(long, help = "Replace the tool description")]
    description: Option<String>,
    #[arg(long, help = "Clear the tool description")]
    clear_description: bool,
    #[arg(
        long = "need",
        value_delimiter = ',',
        help = "Replace required capabilities"
    )]
    required_capabilities: Vec<String>,
    #[arg(long, help = "Clear required capabilities")]
    clear_needs: bool,
    #[arg(long, help = "Replace the command or path template")]
    command_template: Option<String>,
    #[arg(long, help = "Replace the default working directory")]
    cwd: Option<String>,
    #[arg(long, help = "Clear the default working directory")]
    clear_cwd: bool,
}

#[derive(Args)]
struct ToolListArgs {
    #[arg(
        long,
        help = "Only show tools with this kind: shell, file-read/read-file, or file-write/write-file"
    )]
    kind: Option<String>,
    #[arg(
        long = "cap",
        value_delimiter = ',',
        help = "Only show tools requiring this capability"
    )]
    capabilities: Vec<String>,
    #[arg(
        long,
        help = "Only show tools updated at or after this RFC3339 timestamp"
    )]
    since: Option<String>,
    #[arg(
        long,
        help = "Only show tools updated at or before this RFC3339 timestamp"
    )]
    until: Option<String>,
    #[arg(long, help = "Only show tools whose text contains this query")]
    query: Option<String>,
    #[arg(long, help = "Maximum number of recently updated tools to show")]
    limit: Option<usize>,
}

#[derive(Subcommand)]
enum MemoryCommand {
    #[command(about = "Add a memory record")]
    Add(MemoryAddArgs),
    #[command(about = "Search memory records")]
    Search(MemorySearchArgs),
    #[command(about = "Return scored memory snippets for RAG recall")]
    Recall(MemorySearchArgs),
    #[command(about = "List recent memory records")]
    List(MemoryListArgs),
    #[command(about = "Show one memory record")]
    Show(MemoryShowArgs),
    #[command(about = "Update a memory record")]
    Update(MemoryUpdateArgs),
    #[command(about = "Remove a memory record")]
    Remove(MemoryShowArgs),
    #[command(about = "Prune expired memory records")]
    Prune(MemoryPruneArgs),
}

#[derive(Subcommand)]
enum ApprovalCommand {
    #[command(about = "List approval requests")]
    List,
    #[command(about = "Approve a pending request")]
    Approve(ApprovalResolveArgs),
    #[command(about = "Deny a pending request")]
    Deny(ApprovalResolveArgs),
}

#[derive(Subcommand)]
enum RegistryCommand {
    #[command(about = "Show the full reusable registry")]
    List,
    #[command(about = "List reusable agent profiles")]
    Profiles,
    #[command(about = "Show one agent profile")]
    Profile(RegistryIdArgs),
    #[command(about = "Install an agent from a reusable profile")]
    InstallAgent(RegistryInstallAgentArgs),
    #[command(about = "List reusable workflow templates")]
    Templates,
    #[command(about = "Show one workflow template")]
    Template(RegistryIdArgs),
    #[command(about = "Create a workflow from a reusable template")]
    CreateWorkflow(RegistryCreateWorkflowArgs),
    #[command(about = "List MCP server definitions")]
    McpList,
    #[command(about = "Register an MCP server definition")]
    McpAdd(RegistryMcpAddArgs),
    #[command(about = "Enable an MCP server definition")]
    McpEnable(RegistryIdArgs),
    #[command(about = "Disable an MCP server definition")]
    McpDisable(RegistryIdArgs),
    #[command(about = "Remove an MCP server definition")]
    McpRemove(RegistryIdArgs),
    #[command(about = "Import marketplace registry entries from a JSON manifest")]
    MarketplaceImport(RegistryMarketplaceImportArgs),
}

#[derive(Args)]
struct RegistryIdArgs {
    id: String,
}

#[derive(Args)]
struct RegistryInstallAgentArgs {
    profile: String,
    #[arg(long, help = "Override the profile name for this agent")]
    name: Option<String>,
    #[arg(long, help = "Override the profile model for this agent")]
    model: Option<String>,
    #[arg(long, default_value_t = 1)]
    parallel: usize,
}

#[derive(Args)]
struct RegistryCreateWorkflowArgs {
    template: String,
    objective: String,
    #[arg(
        long,
        default_value = "normal",
        help = "Workflow priority: low, normal, high, or critical/urgent"
    )]
    priority: String,
}

#[derive(Args)]
struct RegistryMcpAddArgs {
    id: String,
    #[arg(long)]
    command: String,
    #[arg(long = "arg")]
    args: Vec<String>,
    #[arg(long = "env", help = "Environment entry as KEY=VALUE")]
    env: Vec<String>,
    #[arg(long, help = "Register the MCP server disabled")]
    disabled: bool,
}

#[derive(Args)]
struct RegistryMarketplaceImportArgs {
    path: std::path::PathBuf,
    #[arg(
        long,
        help = "Require the manifest content checksum to match this value"
    )]
    expect_checksum: Option<String>,
    #[arg(
        long,
        help = "Overwrite existing marketplace entries with matching IDs"
    )]
    force: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketplaceManifest {
    #[serde(default)]
    metadata: Option<MarketplaceManifestMetadata>,
    #[serde(default)]
    agent_profiles: Vec<AgentProfile>,
    #[serde(default)]
    workflow_templates: Vec<WorkflowTemplate>,
    #[serde(default)]
    mcp_servers: Vec<McpServer>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketplaceManifestMetadata {
    id: String,
    version: String,
    #[serde(default)]
    publisher: Option<String>,
    #[serde(default)]
    homepage: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct MarketplaceImportReport {
    source: String,
    checksum: String,
    verified_checksum: bool,
    manifest_id: Option<String>,
    manifest_version: Option<String>,
    imported_agent_profiles: usize,
    imported_workflow_templates: usize,
    imported_mcp_servers: usize,
    overwritten: usize,
}

#[derive(Subcommand)]
enum WorkerCommand {
    #[command(about = "List distributed worker nodes")]
    List(WorkerListArgs),
    #[command(about = "Register a distributed worker node")]
    Register(WorkerRegisterArgs),
    #[command(about = "Show one distributed worker node")]
    Show(WorkerIdArgs),
    #[command(about = "Record a worker heartbeat and optional metadata update")]
    Heartbeat(WorkerHeartbeatArgs),
    #[command(about = "Claim the next ready task for a matching worker agent")]
    Claim(WorkerClaimArgs),
    #[command(about = "Report completion or failure for a claimed worker task")]
    Report(WorkerReportArgs),
    #[command(about = "Remove a distributed worker node")]
    Remove(WorkerIdArgs),
}

#[derive(Args)]
struct WorkerListArgs {
    #[arg(long, help = "Only show workers with this status")]
    status: Option<String>,
    #[arg(
        long,
        help = "Only show workers seen at or after this RFC3339 timestamp"
    )]
    since: Option<String>,
    #[arg(
        long,
        help = "Only show workers seen at or before this RFC3339 timestamp"
    )]
    until: Option<String>,
    #[arg(
        long,
        help = "Only show workers whose id or endpoint contains this text"
    )]
    query: Option<String>,
    #[arg(long, help = "Maximum number of recent workers to show")]
    limit: Option<usize>,
}

#[derive(Args)]
struct WorkerRegisterArgs {
    id: String,
    #[arg(long)]
    endpoint: String,
    #[arg(long, default_value = "online")]
    status: String,
}

#[derive(Args)]
struct WorkerHeartbeatArgs {
    id: String,
    #[arg(long)]
    endpoint: Option<String>,
    #[arg(long)]
    status: Option<String>,
    #[arg(long, help = "Refresh the matching agent lease for this many seconds")]
    lease_seconds: Option<i64>,
}

#[derive(Args)]
struct WorkerClaimArgs {
    id: String,
    #[arg(long)]
    lease_seconds: Option<i64>,
}

#[derive(Args)]
struct WorkerReportArgs {
    id: String,
    task_id: String,
    #[arg(long, default_value = "complete")]
    status: String,
    #[arg(long)]
    note: Option<String>,
    #[arg(long)]
    command: Option<String>,
    #[arg(long)]
    cwd: Option<String>,
    #[arg(long)]
    exit_code: Option<i32>,
    #[arg(long = "artifact", value_name = "KIND=PATH")]
    artifacts: Vec<String>,
}

#[derive(Args)]
struct WorkerIdArgs {
    id: String,
}

#[derive(Subcommand)]
enum EvalCommand {
    #[command(about = "List benchmark evaluation records")]
    List(EvalListArgs),
    #[command(about = "Show one benchmark evaluation record")]
    Show(EvalIdArgs),
    #[command(about = "Record a benchmark evaluation result")]
    Record(EvalRecordArgs),
    #[command(about = "Run a local shell evaluation and record the result")]
    Run(EvalRunArgs),
}

#[derive(Args)]
struct EvalListArgs {
    #[arg(long, help = "Only show evals for this target")]
    target: Option<String>,
    #[arg(long, help = "Only show evals with this success value")]
    success: Option<bool>,
    #[arg(
        long,
        help = "Only show evals recorded at or after this RFC3339 timestamp"
    )]
    since: Option<String>,
    #[arg(
        long,
        help = "Only show evals recorded at or before this RFC3339 timestamp"
    )]
    until: Option<String>,
    #[arg(long, help = "Only show evals whose id or target contains this text")]
    query: Option<String>,
    #[arg(long, help = "Maximum number of recent eval records to show")]
    limit: Option<usize>,
}

#[derive(Args)]
struct EvalRecordArgs {
    target: String,
    #[arg(long, conflicts_with = "failure")]
    success: bool,
    #[arg(long, conflicts_with = "success")]
    failure: bool,
    #[arg(long)]
    cost_micros: Option<u64>,
    #[arg(long)]
    latency_ms: Option<u64>,
}

#[derive(Args)]
struct EvalRunArgs {
    target: String,
    #[arg(long)]
    command: String,
    #[arg(long)]
    cwd: Option<std::path::PathBuf>,
    #[arg(long, help = "Require stdout or stderr to contain this text")]
    success_pattern: Option<String>,
    #[arg(
        long,
        value_name = "PATH",
        help = "Validate stdout JSON against this JSON schema file"
    )]
    output_schema: Option<std::path::PathBuf>,
}

#[derive(Args)]
struct EvalIdArgs {
    id: String,
}

#[derive(Subcommand)]
enum SecretsCommand {
    #[command(about = "List secret manager backends")]
    List(SecretsListArgs),
    #[command(about = "Check task secret environment references without exposing values")]
    Check,
    #[command(about = "Register a secret manager backend")]
    Register(SecretsRegisterArgs),
    #[command(about = "Show one secret manager backend")]
    Show(SecretsIdArgs),
    #[command(about = "Remove a secret manager backend")]
    Remove(SecretsIdArgs),
}

#[derive(Args)]
struct SecretsListArgs {
    #[arg(long, help = "Only show backends of this kind")]
    kind: Option<String>,
    #[arg(
        long,
        help = "Only show backends whose id, kind, or reference contains this text"
    )]
    query: Option<String>,
    #[arg(long, help = "Maximum number of backends to show")]
    limit: Option<usize>,
}

#[derive(Args)]
struct SecretsRegisterArgs {
    id: String,
    #[arg(long)]
    kind: String,
    #[arg(
        long,
        help = "Backend reference such as a vault, keychain service, or item path"
    )]
    reference: Option<String>,
}

#[derive(Args)]
struct SecretsIdArgs {
    id: String,
}

#[derive(Args)]
struct ApprovalResolveArgs {
    id: String,
    #[arg(long, help = "Name or identifier of the human resolver")]
    by: Option<String>,
}

#[derive(Subcommand)]
enum GitCommand {
    #[command(about = "Show git status for a workspace")]
    Status(GitCwdArgs),
    #[command(about = "Create or switch to a branch")]
    Branch(GitBranchArgs),
    #[command(about = "Create a git commit")]
    Commit(GitCommitArgs),
    #[command(about = "Create a pull request with gh")]
    Pr(GitPrArgs),
    #[command(about = "Create an Agent OS code-review task for a git diff")]
    ReviewTask(GitReviewTaskArgs),
}

#[derive(Args)]
struct GitCwdArgs {
    #[arg(long, help = "Git workspace directory; defaults to current directory")]
    cwd: Option<std::path::PathBuf>,
}

#[derive(Args)]
struct GitBranchArgs {
    name: String,
    #[arg(long, help = "Git workspace directory; defaults to current directory")]
    cwd: Option<std::path::PathBuf>,
    #[arg(long, help = "Create the branch before switching to it")]
    create: bool,
}

#[derive(Args)]
struct GitCommitArgs {
    #[arg(short, long)]
    message: String,
    #[arg(long, help = "Git workspace directory; defaults to current directory")]
    cwd: Option<std::path::PathBuf>,
    #[arg(
        long,
        help = "Stage all tracked and untracked changes before committing"
    )]
    all: bool,
    #[arg(long, help = "Print the git commands without running them")]
    dry_run: bool,
}

#[derive(Args)]
struct GitPrArgs {
    #[arg(long)]
    title: String,
    #[arg(long)]
    body: String,
    #[arg(long)]
    base: Option<String>,
    #[arg(long)]
    head: Option<String>,
    #[arg(long)]
    draft: bool,
    #[arg(long, help = "Git workspace directory; defaults to current directory")]
    cwd: Option<std::path::PathBuf>,
    #[arg(long, help = "Print the gh command without running it")]
    dry_run: bool,
}

#[derive(Args)]
struct GitReviewTaskArgs {
    #[arg(long, default_value = "main")]
    base: String,
    #[arg(long, help = "Git workspace directory; defaults to current directory")]
    cwd: Option<std::path::PathBuf>,
    #[arg(long, default_value = "Code review")]
    title: String,
    #[arg(long, default_value = "normal")]
    priority: String,
}

#[derive(Args)]
struct MemoryAddArgs {
    topic: String,
    body: String,
    #[arg(long = "tag", value_delimiter = ',')]
    tags: Vec<String>,
    #[arg(
        long,
        default_value = "shared",
        help = "Memory visibility: shared or private"
    )]
    visibility: String,
    #[arg(long, help = "Optional memory scope for scoped provider recall")]
    scope: Option<String>,
}

#[derive(Args)]
struct MemorySearchArgs {
    query: String,
    #[arg(
        long = "tag",
        value_delimiter = ',',
        help = "Only show memory with this tag"
    )]
    tags: Vec<String>,
    #[arg(
        long,
        help = "Only show memory updated at or after this RFC3339 timestamp"
    )]
    since: Option<String>,
    #[arg(
        long,
        help = "Only show memory updated at or before this RFC3339 timestamp"
    )]
    until: Option<String>,
    #[arg(long, help = "Maximum number of recent matching records to show")]
    limit: Option<usize>,
    #[arg(long, help = "Only show shared or private memory")]
    visibility: Option<String>,
    #[arg(long, help = "Only show memory in this scope")]
    scope: Option<String>,
}

#[derive(Args)]
struct MemoryListArgs {
    #[arg(
        long = "tag",
        value_delimiter = ',',
        help = "Only show memory with this tag"
    )]
    tags: Vec<String>,
    #[arg(
        long,
        help = "Only show memory updated at or after this RFC3339 timestamp"
    )]
    since: Option<String>,
    #[arg(
        long,
        help = "Only show memory updated at or before this RFC3339 timestamp"
    )]
    until: Option<String>,
    #[arg(long, help = "Maximum number of recent records to show")]
    limit: Option<usize>,
    #[arg(long, help = "Only show shared or private memory")]
    visibility: Option<String>,
    #[arg(long, help = "Only show memory in this scope")]
    scope: Option<String>,
}

#[derive(Args)]
struct MemoryShowArgs {
    id: String,
}

#[derive(Args)]
struct MemoryPruneArgs {
    #[arg(long, help = "Prune memory not updated within this many days")]
    max_age_days: Option<u64>,
    #[arg(long, help = "Report expired memory without removing it")]
    dry_run: bool,
}

#[derive(Args)]
struct MemoryUpdateArgs {
    id: String,
    #[arg(long)]
    topic: Option<String>,
    #[arg(long)]
    body: Option<String>,
    #[arg(long = "tag", value_delimiter = ',')]
    tags: Vec<String>,
    #[arg(long)]
    clear_tags: bool,
    #[arg(long, help = "Set memory visibility: shared or private")]
    visibility: Option<String>,
    #[arg(long, help = "Set memory scope")]
    scope: Option<String>,
    #[arg(long, help = "Clear memory scope")]
    clear_scope: bool,
}

#[derive(Args)]
struct EventsArgs {
    #[arg(long, default_value_t = 20)]
    limit: usize,
    #[arg(
        long,
        help = "Only show events with this kind, such as task-created, run-finished, policy-rejected, or state-repaired"
    )]
    kind: Option<String>,
    #[arg(long, help = "Only show events at or after this RFC3339 timestamp")]
    since: Option<String>,
    #[arg(long, help = "Only show events at or before this RFC3339 timestamp")]
    until: Option<String>,
    #[arg(long, help = "Only show events whose message contains this text")]
    query: Option<String>,
}

#[derive(Subcommand)]
enum RunsCommand {
    #[command(about = "List and filter run history")]
    List(RunsListArgs),
    #[command(about = "Show one run")]
    Show(RunShowArgs),
    #[command(about = "Print run log output")]
    Logs(RunLogArgs),
    #[command(about = "Tail run log output")]
    Tail(RunTailArgs),
    #[command(about = "Reconstruct a run from state, events, and logs")]
    Replay(RunReplayArgs),
    #[command(about = "Collect a run debug bundle with state, events, logs, and artifacts")]
    Debug(RunReplayArgs),
    #[command(about = "List or read artifacts captured for a run")]
    Artifacts(RunArtifactsArgs),
    #[command(about = "Request cancellation for a running run")]
    Cancel(RunShowArgs),
}

#[derive(Subcommand)]
enum McpCommand {
    #[command(about = "Serve core Agent OS operations as MCP tools over stdio")]
    Serve(McpServeArgs),
}

#[derive(Args)]
struct McpServeArgs {
    #[arg(long, help = "Exit after this many JSON-RPC responses")]
    max_requests: Option<usize>,
}

#[derive(Args)]
struct RunsListArgs {
    #[arg(
        long,
        help = "Only show runs with this status: running, cancel-requested/cancel_requested, cancelled/canceled, success/succeeded, failed, or rejected"
    )]
    status: Option<String>,
    #[arg(long, help = "Only show runs for this task")]
    task: Option<String>,
    #[arg(long, help = "Only show runs assigned to this agent")]
    agent: Option<String>,
    #[arg(
        long,
        help = "Only show runs started at or after this RFC3339 timestamp"
    )]
    since: Option<String>,
    #[arg(
        long,
        help = "Only show runs started at or before this RFC3339 timestamp"
    )]
    until: Option<String>,
    #[arg(long, help = "Only show runs whose command contains this text")]
    query: Option<String>,
    #[arg(long, help = "Maximum number of recent runs to show")]
    limit: Option<usize>,
}

#[derive(Args)]
struct RunShowArgs {
    id: String,
}

#[derive(Args)]
struct RunLogArgs {
    id: String,
    #[arg(long, help = "Only print the final N bytes of the run log")]
    tail_bytes: Option<usize>,
}

#[derive(Args)]
struct RunReplayArgs {
    id: String,
    #[arg(long, help = "Only include the final N bytes of the run log")]
    tail_bytes: Option<usize>,
}

#[derive(Args)]
struct RunArtifactsArgs {
    id: String,
    #[arg(help = "Artifact ID, kind, or zero-based artifact index to read")]
    artifact: Option<String>,
    #[arg(long, help = "Only include the final N bytes when reading an artifact")]
    tail_bytes: Option<usize>,
}

#[derive(Args)]
struct RunTailArgs {
    id: String,
    #[arg(long, help = "Keep reading until the run exits")]
    follow: bool,
    #[arg(long, help = "Only print the final N bytes before following")]
    tail_bytes: Option<usize>,
    #[arg(long, default_value_t = 200)]
    interval_ms: u64,
}

#[derive(Args)]
struct RunArgs {
    #[arg(long, default_value_t = 1)]
    limit: usize,
    #[arg(long, help = "Execute task commands after scheduling them")]
    execute: bool,
    #[arg(long, help = "Preview scheduler assignments without mutating state")]
    dry_run: bool,
    #[arg(
        long,
        help = "Before scheduling, return running tasks older than this many seconds to pending"
    )]
    recover_stale_seconds: Option<i64>,
}

#[derive(Subcommand)]
enum DaemonCommand {
    #[command(about = "Run the scheduler service loop")]
    Run(DaemonRunArgs),
    #[command(about = "Show durable daemon status")]
    Status,
    #[command(about = "Request the running daemon to stop")]
    Stop,
}

#[derive(Subcommand)]
enum ServiceCommand {
    #[command(about = "Render a launchd service plist")]
    Launchd(ServiceLaunchdArgs),
    #[command(about = "Render a systemd user service unit")]
    Systemd(ServiceSystemdArgs),
    #[command(about = "Render a Windows Scheduled Task PowerShell script")]
    WindowsTask(ServiceWindowsTaskArgs),
    #[command(about = "Install the launchd service plist")]
    Install(ServiceLaunchdArgs),
    #[command(about = "Install a systemd user service unit")]
    InstallSystemd(ServiceSystemdArgs),
    #[command(about = "Uninstall the launchd service plist")]
    Uninstall(ServiceUninstallArgs),
    #[command(about = "Uninstall a systemd user service unit")]
    UninstallSystemd(ServiceSystemdUninstallArgs),
    #[command(about = "Start the launchd service")]
    Start(ServiceControlArgs),
    #[command(about = "Start a systemd user service")]
    StartSystemd(ServiceSystemdControlArgs),
    #[command(about = "Stop the launchd service")]
    Stop(ServiceControlArgs),
    #[command(about = "Stop a systemd user service")]
    StopSystemd(ServiceSystemdControlArgs),
    #[command(about = "Show launchd service status")]
    Status(ServiceStatusArgs),
    #[command(about = "Show systemd user service status")]
    StatusSystemd(ServiceSystemdControlArgs),
}

#[derive(Subcommand)]
enum ApiCommand {
    #[command(about = "Serve the local HTTP API")]
    Serve(ApiServeArgs),
    #[command(about = "Print the OpenAPI schema")]
    Schema,
}

#[derive(Args)]
struct DaemonRunArgs {
    #[arg(long, default_value_t = 1000)]
    interval_ms: u64,
    #[arg(long, default_value_t = 1)]
    limit: usize,
    #[arg(long)]
    execute: bool,
    #[arg(
        long,
        help = "Stop after this many ticks; omit to run until interrupted"
    )]
    max_ticks: Option<usize>,
    #[arg(
        long,
        help = "Before each tick, return running tasks older than this many seconds to pending"
    )]
    recover_stale_seconds: Option<i64>,
}

#[derive(Args)]
struct ServiceLaunchdArgs {
    #[arg(long, default_value = "com.infinite-apps.agent-os")]
    label: String,
    #[arg(
        long,
        help = "Path to the agent-os binary; defaults to the current executable"
    )]
    bin_path: Option<std::path::PathBuf>,
    #[arg(long, default_value_t = 1000)]
    interval_ms: u64,
    #[arg(long, default_value_t = 1)]
    limit: usize,
    #[arg(long)]
    execute: bool,
    #[arg(long)]
    recover_stale_seconds: Option<i64>,
    #[arg(long, help = "Disable generated StandardOutPath and StandardErrorPath")]
    no_logs: bool,
    #[arg(long, help = "LaunchAgent plist path; defaults from --label")]
    plist_path: Option<std::path::PathBuf>,
}

#[derive(Args)]
struct ServiceSystemdArgs {
    #[arg(long, default_value = "agent-os.service")]
    unit_name: String,
    #[arg(
        long,
        help = "Path to the agent-os binary; defaults to the current executable"
    )]
    bin_path: Option<std::path::PathBuf>,
    #[arg(long, default_value_t = 1000)]
    interval_ms: u64,
    #[arg(long, default_value_t = 1)]
    limit: usize,
    #[arg(long)]
    execute: bool,
    #[arg(long)]
    recover_stale_seconds: Option<i64>,
    #[arg(
        long,
        help = "Systemd unit path; defaults under ~/.config/systemd/user"
    )]
    unit_path: Option<std::path::PathBuf>,
}

#[derive(Args)]
struct ServiceWindowsTaskArgs {
    #[arg(long, default_value = "Agent OS Daemon")]
    task_name: String,
    #[arg(
        long,
        help = "Path to the agent-os binary; defaults to the current executable"
    )]
    bin_path: Option<std::path::PathBuf>,
    #[arg(long, default_value_t = 1000)]
    interval_ms: u64,
    #[arg(long, default_value_t = 1)]
    limit: usize,
    #[arg(long)]
    execute: bool,
    #[arg(long)]
    recover_stale_seconds: Option<i64>,
}

#[derive(Args)]
struct ServiceUninstallArgs {
    #[arg(long, default_value = "com.infinite-apps.agent-os")]
    label: String,
    #[arg(long, help = "LaunchAgent plist path; defaults from --label")]
    plist_path: Option<std::path::PathBuf>,
}

#[derive(Args)]
struct ServiceSystemdUninstallArgs {
    #[arg(long, default_value = "agent-os.service")]
    unit_name: String,
    #[arg(
        long,
        help = "Systemd unit path; defaults under ~/.config/systemd/user"
    )]
    unit_path: Option<std::path::PathBuf>,
}

#[derive(Args)]
struct ServiceControlArgs {
    #[arg(long, default_value = "com.infinite-apps.agent-os")]
    label: String,
    #[arg(long, help = "LaunchAgent plist path; defaults from --label")]
    plist_path: Option<std::path::PathBuf>,
    #[arg(long, help = "launchd domain; defaults to gui/$(id -u)")]
    domain: Option<String>,
    #[arg(long, default_value = "launchctl", help = "Path to launchctl")]
    launchctl_path: std::path::PathBuf,
}

#[derive(Args)]
struct ServiceSystemdControlArgs {
    #[arg(long, default_value = "agent-os.service")]
    unit_name: String,
    #[arg(long, default_value = "systemctl", help = "Path to systemctl")]
    systemctl_path: std::path::PathBuf,
}

#[derive(Args)]
struct ServiceStatusArgs {
    #[arg(long, default_value = "com.infinite-apps.agent-os")]
    label: String,
    #[arg(long, help = "launchd domain; defaults to gui/$(id -u)")]
    domain: Option<String>,
    #[arg(long, default_value = "launchctl", help = "Path to launchctl")]
    launchctl_path: std::path::PathBuf,
}

#[derive(Args)]
struct CompletionsArgs {
    shell: Shell,
}

#[derive(Args)]
struct ApiServeArgs {
    #[arg(
        long,
        default_value = "127.0.0.1:7373",
        help = "Bind address for the local API listener"
    )]
    addr: String,
    #[arg(
        long,
        help = "Read a bearer token from this environment variable and require it for requests"
    )]
    token_env: Option<String>,
    #[arg(
        long,
        help = "Read a full-access bearer token from this file and require it for requests"
    )]
    token_file: Option<std::path::PathBuf>,
    #[arg(
        long,
        help = "Read a read-only bearer token from this environment variable for GET requests"
    )]
    read_token_env: Option<String>,
    #[arg(
        long,
        help = "Read a read-only bearer token from this file for GET requests"
    )]
    read_token_file: Option<std::path::PathBuf>,
    #[arg(
        long,
        help = "Read a mutation bearer token from this environment variable for POST and DELETE requests"
    )]
    write_token_env: Option<String>,
    #[arg(
        long,
        help = "Read a mutation bearer token from this file for POST and DELETE requests"
    )]
    write_token_file: Option<std::path::PathBuf>,
    #[arg(
        long,
        help = "Allow serving an unauthenticated API on a non-loopback address"
    )]
    unsafe_no_token: bool,
    #[arg(
        long,
        help = "Allow this exact browser Origin in addition to loopback origins; may be repeated"
    )]
    allow_origin: Vec<String>,
    #[arg(
        long,
        help = "Stop after serving this many requests; useful for tests and supervisors"
    )]
    max_requests: Option<usize>,
}

#[derive(Subcommand)]
enum WorkflowCommand {
    #[command(about = "Create a planner-builder-reviewer workflow")]
    Create(WorkflowCreateArgs),
    #[command(about = "List and filter workflows")]
    List(WorkflowListArgs),
    #[command(about = "Show one workflow")]
    Show(WorkflowShowArgs),
    #[command(about = "Show workflow progress")]
    Status(WorkflowShowArgs),
    #[command(about = "Add a task stage to a workflow DAG")]
    AddTask(WorkflowAddTaskArgs),
    #[command(about = "Add a dependency edge between workflow stages")]
    Link(WorkflowEdgeArgs),
    #[command(about = "Remove a dependency edge between workflow stages")]
    Unlink(WorkflowEdgeArgs),
    #[command(about = "Pause pending workflow stages")]
    Pause(WorkflowNoteArgs),
    #[command(about = "Resume blocked workflow stages")]
    Resume(WorkflowNoteArgs),
    #[command(about = "Retry failed, cancelled, or blocked workflow stages")]
    Retry(WorkflowNoteArgs),
    #[command(about = "Advance workflow tasks")]
    Run(WorkflowRunArgs),
    #[command(about = "Cancel active workflow tasks")]
    Cancel(WorkflowCancelArgs),
    #[command(about = "Remove workflow metadata")]
    Remove(WorkflowShowArgs),
}

#[derive(Args)]
struct WorkflowCreateArgs {
    objective: String,
    #[arg(
        long,
        default_value = "normal",
        help = "Workflow priority: low, normal, high, or critical/urgent"
    )]
    priority: String,
    #[arg(long, help = "Immediately execute the first runnable workflow task")]
    execute: bool,
}

#[derive(Args)]
struct WorkflowListArgs {
    #[arg(
        long,
        help = "Only show workflows with this priority: low, normal, high, or critical/urgent"
    )]
    priority: Option<String>,
    #[arg(long, help = "Only show workflows referencing this task")]
    task: Option<String>,
    #[arg(
        long,
        help = "Only show workflows updated at or after this RFC3339 timestamp"
    )]
    since: Option<String>,
    #[arg(
        long,
        help = "Only show workflows updated at or before this RFC3339 timestamp"
    )]
    until: Option<String>,
    #[arg(long, help = "Only show workflows whose text contains this query")]
    query: Option<String>,
    #[arg(long, help = "Maximum number of recently updated workflows to show")]
    limit: Option<usize>,
}

#[derive(Args)]
struct WorkflowShowArgs {
    id: String,
}

#[derive(Args)]
struct WorkflowAddTaskArgs {
    id: String,
    stage: String,
    title: String,
    #[arg(long, help = "Task objective; defaults to title")]
    objective: Option<String>,
    #[arg(long, help = "Optional shell command for the stage task")]
    command: Option<String>,
    #[arg(long = "need", value_delimiter = ',')]
    required_capabilities: Vec<String>,
    #[arg(long = "after", help = "Existing workflow stage this stage depends on")]
    dependencies: Vec<String>,
    #[arg(long, help = "Override workflow priority for this task")]
    priority: Option<String>,
}

#[derive(Args)]
struct WorkflowEdgeArgs {
    id: String,
    #[arg(long, help = "Dependency stage name")]
    from: String,
    #[arg(long, help = "Dependent stage name")]
    to: String,
}

#[derive(Args)]
struct WorkflowNoteArgs {
    id: String,
    #[arg(long, help = "Optional note to store on affected stages")]
    note: Option<String>,
}

#[derive(Args)]
struct WorkflowRunArgs {
    id: String,
    #[arg(
        long,
        help = "Run ready workflow stages until the workflow blocks or completes"
    )]
    all: bool,
}

#[derive(Args)]
struct WorkflowCancelArgs {
    id: String,
    #[arg(long, help = "Optional cancellation note to store on cancelled stages")]
    note: Option<String>,
}

#[derive(Tabled)]
struct AgentRow {
    id: String,
    kind: String,
    status: String,
    model: String,
    caps: String,
    load: String,
}

#[derive(Tabled)]
struct TaskRow {
    id: String,
    status: String,
    priority: String,
    attempts: String,
    assigned: String,
    needs: String,
    title: String,
}

#[derive(Tabled)]
struct ToolRow {
    id: String,
    kind: String,
    needs: String,
    command: String,
    description: String,
}

#[derive(Tabled)]
struct MemoryRow {
    id: String,
    topic: String,
    visibility: String,
    scope: String,
    tags: String,
    body: String,
}

#[derive(Tabled)]
struct MemoryRecallRow {
    id: String,
    score: usize,
    topic: String,
    visibility: String,
    scope: String,
    tags: String,
    snippet: String,
}

#[derive(Tabled)]
struct EventRow {
    at: String,
    kind: String,
    message: String,
}

#[derive(Tabled)]
struct RunRow {
    id: String,
    task: String,
    agent: String,
    status: String,
    exit: String,
    command: String,
}

#[derive(Tabled)]
struct WorkflowRow {
    id: String,
    priority: String,
    objective: String,
    tasks: String,
    updated: String,
}

#[derive(Tabled)]
struct RegistryItemRow {
    id: String,
    kind: String,
    name: String,
    detail: String,
}

#[derive(Tabled)]
struct WorkerRow {
    id: String,
    status: String,
    endpoint: String,
    last_seen: String,
}

#[derive(Tabled)]
struct EvalRow {
    id: String,
    target: String,
    success: String,
    cost_micros: String,
    latency_ms: String,
    recorded: String,
}

#[derive(Tabled)]
struct SecretCheckRow {
    task: String,
    tool: String,
    arg: String,
    env: String,
    present: String,
}

#[derive(Tabled)]
struct SecretsBackendRow {
    id: String,
    kind: String,
    reference: String,
}

#[derive(Serialize)]
struct StatusSummary<'a> {
    name: &'a str,
    agent_os_version: &'static str,
    state_path: String,
    agents: usize,
    tasks_pending: usize,
    tasks_running: usize,
    tasks_blocked: usize,
    tasks_complete: usize,
    tasks_failed: usize,
    tasks_cancelled: usize,
    workflows: usize,
    runs: usize,
    tools: usize,
    daemon_status: Option<String>,
    daemon_ticks: Option<usize>,
    memories: usize,
    events: usize,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    validate_optional_path("state", cli.state.as_deref())?;
    let store = resolve_store(cli.state)?;
    let config_path = discover_config(cli.config);
    validate_path("config", &config_path)?;
    let json = cli.json;

    match cli.command {
        Command::Init(args) => init(store, &config_path, args, json),
        Command::Status => {
            let os = store.load().with_context(|| state_init_hint(&store))?;
            print_status(&os, store.path().display().to_string(), json)
        }
        Command::Metrics(args) => {
            let metrics = match store.load() {
                Ok(os) => metrics_json(&os),
                Err(error) => metrics_unavailable_json(error.to_string()),
            };
            print_metrics(&metrics, json, args.prometheus)
        }
        Command::Doctor => doctor(&store, &config_path, json),
        Command::Config { command } => handle_config(config_path, command, json),
        Command::State { command } => handle_state(store, command, json),
        Command::Agent { command } => handle_agent(store, command, json),
        Command::Task { command } => handle_task(store, command, json),
        Command::Tool { command } => handle_tool(store, command, json),
        Command::Memory { command } => handle_memory(store, command, json),
        Command::Registry { command } => handle_registry(store, command, json),
        Command::Worker { command } => handle_worker(store, command, json),
        Command::Eval { command } => handle_eval(store, command, json),
        Command::Secrets { command } => handle_secrets(store, command, json),
        Command::Approval { command } => handle_approval(store, command, json),
        Command::Git { command } => handle_git(store, command, json),
        Command::Events(args) => {
            validate_events_limit(args.limit)?;
            let kind = args.kind.as_deref().map(parse_event_kind).transpose()?;
            let since = args
                .since
                .as_deref()
                .map(|value| parse_filter_timestamp(value, "event", "since"))
                .transpose()?;
            let until = args
                .until
                .as_deref()
                .map(|value| parse_filter_timestamp(value, "event", "until"))
                .transpose()?;
            if let Some(query) = &args.query {
                validate_event_query(query)?;
            }
            let query = args.query.as_ref().map(|query| query.to_ascii_lowercase());
            let os = store.load().with_context(|| state_init_hint(&store))?;
            let events = os
                .events
                .iter()
                .filter(|event| {
                    kind.as_ref()
                        .map(|kind| &event.kind == kind)
                        .unwrap_or(true)
                        && since
                            .as_ref()
                            .map(|since| event.at >= *since)
                            .unwrap_or(true)
                        && until
                            .as_ref()
                            .map(|until| event.at <= *until)
                            .unwrap_or(true)
                        && query
                            .as_ref()
                            .map(|query| event.message.to_ascii_lowercase().contains(query))
                            .unwrap_or(true)
                })
                .cloned()
                .collect::<Vec<_>>();
            if json {
                print_json(&events.iter().rev().take(args.limit).collect::<Vec<_>>())?;
            } else {
                print_events(&events, args.limit);
            }
            Ok(())
        }
        Command::Runs { command } => handle_runs(store, command, json),
        Command::Daemon { command } => handle_daemon(store, command, json),
        Command::Service { command } => handle_service(store, command, json),
        Command::Mcp { command } => handle_mcp(store, command),
        Command::Completions(args) => {
            let mut command = Cli::command();
            clap_complete::generate(args.shell, &mut command, "agent-os", &mut std::io::stdout());
            Ok(())
        }
        Command::Api { command } => handle_api(store, config_path, command),
        Command::Workflow { command } => handle_workflow(store, command, json),
        Command::Run(args) => {
            let mut os = store.load().with_context(|| state_init_hint(&store))?;
            let (report, executed, errors) = run_once(
                &mut os,
                &store,
                args.limit,
                args.execute,
                args.dry_run,
                args.recover_stale_seconds,
            )?;
            if json {
                print_json(&serde_json::json!({
                    "dry_run": args.dry_run,
                    "scheduler": report,
                    "runs": executed,
                    "errors": errors,
                }))?;
            } else if args.dry_run && report.assignments.is_empty() {
                println!("No runnable tasks would be assigned.");
                print_scheduler_notes(&report);
            } else if args.dry_run {
                for assignment in &report.assignments {
                    println!(
                        "Would assign task {} to {} ({})",
                        assignment.task_id, assignment.agent_id, assignment.reason
                    );
                }
                print_scheduler_notes(&report);
            } else if report.assignments.is_empty() {
                println!("No runnable tasks found.");
                print_scheduler_notes(&report);
            } else {
                for assignment in &report.assignments {
                    println!(
                        "Assigned task {} to {} ({})",
                        assignment.task_id, assignment.agent_id, assignment.reason
                    );
                }
                print_scheduler_notes(&report);
                for run in executed {
                    println!(
                        "Executed run {} for task {}: {}",
                        run.id, run.task_id, run.status
                    );
                }
                for error in errors {
                    eprintln!("could not execute task: {error}");
                }
            }
            Ok(())
        }
    }
}

fn handle_mcp(store: Store, command: McpCommand) -> Result<()> {
    match command {
        McpCommand::Serve(args) => serve_mcp_stdio(&store, args.max_requests),
    }
}

fn serve_mcp_stdio(store: &Store, max_requests: Option<usize>) -> Result<()> {
    if matches!(max_requests, Some(0)) {
        bail!("max-requests must be greater than 0");
    }
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut responses = 0usize;
    for line in stdin.lock().lines() {
        let line = line.context("read MCP request")?;
        if line.trim().is_empty() {
            continue;
        }
        let Some(response) = mcp_response_for_line(store, &line) else {
            continue;
        };
        serde_json::to_writer(&mut stdout, &response).context("write MCP response")?;
        writeln!(&mut stdout).context("write MCP response newline")?;
        stdout.flush().context("flush MCP response")?;
        responses += 1;
        if max_requests.is_some_and(|limit| responses >= limit) {
            break;
        }
    }
    Ok(())
}

fn mcp_response_for_line(store: &Store, line: &str) -> Option<Value> {
    let request = match serde_json::from_str::<Value>(line) {
        Ok(request) => request,
        Err(error) => {
            return Some(mcp_protocol_error(
                Value::Null,
                -32700,
                format!("Parse error: {error}"),
            ));
        }
    };
    let Some(id) = request.get("id").cloned() else {
        return None;
    };
    let Some(method) = request.get("method").and_then(Value::as_str) else {
        return Some(mcp_protocol_error(
            id,
            -32600,
            "Invalid Request: missing method",
        ));
    };
    match method {
        "initialize" => Some(mcp_success(
            id,
            serde_json::json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {},
                    "resources": {},
                    "prompts": {}
                },
                "serverInfo": {
                    "name": "agent-os",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }),
        )),
        "ping" => Some(mcp_success(id, serde_json::json!({}))),
        "tools/list" => Some(mcp_tools_list_response(store, id)),
        "tools/call" => Some(mcp_tools_call_response(store, id, &request)),
        "resources/list" => Some(mcp_resources_list_response(store, id)),
        "resources/read" => Some(mcp_resources_read_response(store, id, &request)),
        "prompts/list" => Some(mcp_prompts_list_response(store, id)),
        "prompts/get" => Some(mcp_prompts_get_response(store, id, &request)),
        _ => Some(mcp_protocol_error(
            id,
            -32601,
            format!("Method not found: {method}"),
        )),
    }
}

fn mcp_tools_call_response(store: &Store, id: Value, request: &Value) -> Value {
    let params = request
        .get("params")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return mcp_tool_result(id, true, "missing tool name");
    };
    let arguments = params
        .get("arguments")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    match call_mcp_tool(store, name, &arguments) {
        Ok(value) => {
            return match serde_json::to_string_pretty(&value) {
                Ok(text) => mcp_tool_result(id, false, text),
                Err(error) => {
                    mcp_tool_result(id, true, format!("could not encode tool result: {error}"))
                }
            };
        }
        Err(error) if error.starts_with("unknown MCP tool `") => {}
        Err(error) => return mcp_tool_result(id, true, error),
    }
    if let Some(response) = mcp_registered_tool_call_response(store, id.clone(), name, &arguments) {
        return response;
    }
    mcp_tool_result(id, true, format!("unknown MCP tool `{name}`"))
}

fn mcp_tools_list_response(store: &Store, id: Value) -> Value {
    let mut tools = mcp_tools();
    let mut proxy_errors = Vec::new();
    match mcp_enabled_servers(store) {
        Ok(servers) => {
            for server in servers {
                match mcp_registered_server_tools(&server) {
                    Ok(mut registered_tools) => tools.append(&mut registered_tools),
                    Err(error) => proxy_errors.push(serde_json::json!({
                        "server": server.id,
                        "error": error
                    })),
                }
            }
        }
        Err(error) => proxy_errors.push(serde_json::json!({
            "server": null,
            "error": error
        })),
    }
    mcp_success(
        id,
        serde_json::json!({
            "tools": tools,
            "proxy_errors": proxy_errors
        }),
    )
}

fn mcp_registered_tool_call_response(
    store: &Store,
    id: Value,
    name: &str,
    arguments: &serde_json::Map<String, Value>,
) -> Option<Value> {
    let servers = match mcp_enabled_servers(store) {
        Ok(servers) => servers,
        Err(error) => return Some(mcp_tool_result(id, true, error)),
    };
    for server in servers {
        let prefix = mcp_proxy_tool_prefix(&server.id);
        let Some(remote_name) = name.strip_prefix(&prefix) else {
            continue;
        };
        if remote_name.trim().is_empty() {
            return Some(mcp_tool_result(id, true, "missing remote MCP tool name"));
        }
        return Some(
            match mcp_registered_server_tool_call(&server, remote_name, arguments) {
                Ok(result) => mcp_success(id, result),
                Err(error) => mcp_tool_result(id, true, error),
            },
        );
    }
    None
}

fn call_mcp_tool(
    store: &Store,
    name: &str,
    arguments: &serde_json::Map<String, Value>,
) -> Result<Value, String> {
    match name {
        "agent_os_status" => {
            let os = store.load().map_err(|error| error.to_string())?;
            Ok(mcp_status_payload(&os, store.path().display().to_string()))
        }
        "agent_os_create_task" => mcp_create_task(store, arguments),
        "agent_os_memory_search" => mcp_memory_search(store, arguments),
        "agent_os_approval_list" => mcp_approval_list(store, arguments),
        "agent_os_resolve_approval" => mcp_resolve_approval(store, arguments),
        "agent_os_git_status" => mcp_git_status(arguments),
        "agent_os_secrets_check" => mcp_secrets_check(store, arguments),
        _ => Err(format!("unknown MCP tool `{name}`")),
    }
}

fn mcp_resources_list_response(store: &Store, id: Value) -> Value {
    let mut resources = mcp_resources();
    let mut proxy_errors = Vec::new();
    match mcp_enabled_servers(store) {
        Ok(servers) => {
            for server in servers {
                match mcp_registered_server_resources(&server) {
                    Ok(mut registered_resources) => resources.append(&mut registered_resources),
                    Err(error) => proxy_errors.push(serde_json::json!({
                        "server": server.id,
                        "error": error
                    })),
                }
            }
        }
        Err(error) => proxy_errors.push(serde_json::json!({
            "server": null,
            "error": error
        })),
    }
    mcp_success(
        id,
        serde_json::json!({
            "resources": resources,
            "proxy_errors": proxy_errors
        }),
    )
}

fn mcp_resources_read_response(store: &Store, id: Value, request: &Value) -> Value {
    let params = request
        .get("params")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let Some(uri) = params.get("uri").and_then(Value::as_str) else {
        return mcp_invalid_params(id, "missing resource uri");
    };
    if let Some(response) = mcp_registered_resource_read_response(store, id.clone(), uri) {
        return response;
    }
    match mcp_builtin_resource_contents(store, uri) {
        Ok(contents) => mcp_success(id, serde_json::json!({ "contents": [contents] })),
        Err(error) => mcp_invalid_params(id, error),
    }
}

fn mcp_prompts_list_response(store: &Store, id: Value) -> Value {
    let mut prompts = mcp_prompts();
    let mut proxy_errors = Vec::new();
    match mcp_enabled_servers(store) {
        Ok(servers) => {
            for server in servers {
                match mcp_registered_server_prompts(&server) {
                    Ok(mut registered_prompts) => prompts.append(&mut registered_prompts),
                    Err(error) => proxy_errors.push(serde_json::json!({
                        "server": server.id,
                        "error": error
                    })),
                }
            }
        }
        Err(error) => proxy_errors.push(serde_json::json!({
            "server": null,
            "error": error
        })),
    }
    mcp_success(
        id,
        serde_json::json!({
            "prompts": prompts,
            "proxy_errors": proxy_errors
        }),
    )
}

fn mcp_prompts_get_response(store: &Store, id: Value, request: &Value) -> Value {
    let params = request
        .get("params")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return mcp_invalid_params(id, "missing prompt name");
    };
    let arguments = params
        .get("arguments")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    if let Some(response) = mcp_registered_prompt_get_response(store, id.clone(), name, &arguments)
    {
        return response;
    }
    match mcp_builtin_prompt(name, &arguments) {
        Ok(prompt) => mcp_success(id, prompt),
        Err(error) => mcp_invalid_params(id, error),
    }
}

fn mcp_enabled_servers(store: &Store) -> Result<Vec<McpServer>, String> {
    let os = store.load().map_err(|error| error.to_string())?;
    Ok(os
        .mcp_servers
        .values()
        .filter(|server| server.enabled)
        .cloned()
        .collect())
}

fn mcp_registered_server_tools(server: &McpServer) -> Result<Vec<Value>, String> {
    let result = mcp_child_request(server, "tools/list", serde_json::json!({}))?;
    let tools = result
        .get("tools")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("mcp server {} returned no tools array", server.id))?;
    let mut proxied = Vec::new();
    for tool in tools {
        let Some(remote_name) = tool.get("name").and_then(Value::as_str) else {
            continue;
        };
        if remote_name.trim().is_empty() {
            continue;
        }
        let mut tool = tool.clone();
        if let Some(object) = tool.as_object_mut() {
            object.insert(
                "name".into(),
                Value::String(mcp_proxy_tool_name(&server.id, remote_name)),
            );
            let description = object
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("Registered MCP tool");
            object.insert(
                "description".into(),
                Value::String(format!("[{}] {}", server.id, description)),
            );
            object.insert(
                "x-agent-os-mcp-server".into(),
                Value::String(server.id.clone()),
            );
            object.insert(
                "x-agent-os-mcp-tool".into(),
                Value::String(remote_name.into()),
            );
            proxied.push(tool);
        }
    }
    Ok(proxied)
}

fn mcp_registered_server_resources(server: &McpServer) -> Result<Vec<Value>, String> {
    let result = mcp_child_request(server, "resources/list", serde_json::json!({}))?;
    let resources = result
        .get("resources")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("mcp server {} returned no resources array", server.id))?;
    let mut proxied = Vec::new();
    for resource in resources {
        let Some(remote_uri) = resource.get("uri").and_then(Value::as_str) else {
            continue;
        };
        if remote_uri.trim().is_empty() {
            continue;
        }
        let mut resource = resource.clone();
        if let Some(object) = resource.as_object_mut() {
            object.insert(
                "uri".into(),
                Value::String(mcp_proxy_resource_uri(&server.id, remote_uri)),
            );
            let name = object
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or(remote_uri);
            object.insert(
                "name".into(),
                Value::String(format!("[{}] {}", server.id, name)),
            );
            object.insert(
                "x-agent-os-mcp-server".into(),
                Value::String(server.id.clone()),
            );
            object.insert(
                "x-agent-os-mcp-resource-uri".into(),
                Value::String(remote_uri.into()),
            );
            proxied.push(resource);
        }
    }
    Ok(proxied)
}

fn mcp_registered_resource_read_response(store: &Store, id: Value, uri: &str) -> Option<Value> {
    let servers = match mcp_enabled_servers(store) {
        Ok(servers) => servers,
        Err(error) => return Some(mcp_invalid_params(id, error)),
    };
    for server in servers {
        let prefix = mcp_proxy_resource_prefix(&server.id);
        let Some(remote_uri) = uri.strip_prefix(&prefix) else {
            continue;
        };
        if remote_uri.trim().is_empty() {
            return Some(mcp_invalid_params(id, "missing remote MCP resource uri"));
        }
        return Some(
            match mcp_child_request(
                &server,
                "resources/read",
                serde_json::json!({ "uri": remote_uri }),
            ) {
                Ok(result) => mcp_success(id, result),
                Err(error) => mcp_invalid_params(id, error),
            },
        );
    }
    None
}

fn mcp_registered_server_prompts(server: &McpServer) -> Result<Vec<Value>, String> {
    let result = mcp_child_request(server, "prompts/list", serde_json::json!({}))?;
    let prompts = result
        .get("prompts")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("mcp server {} returned no prompts array", server.id))?;
    let mut proxied = Vec::new();
    for prompt in prompts {
        let Some(remote_name) = prompt.get("name").and_then(Value::as_str) else {
            continue;
        };
        if remote_name.trim().is_empty() {
            continue;
        }
        let mut prompt = prompt.clone();
        if let Some(object) = prompt.as_object_mut() {
            object.insert(
                "name".into(),
                Value::String(mcp_proxy_tool_name(&server.id, remote_name)),
            );
            let description = object
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("Registered MCP prompt");
            object.insert(
                "description".into(),
                Value::String(format!("[{}] {}", server.id, description)),
            );
            object.insert(
                "x-agent-os-mcp-server".into(),
                Value::String(server.id.clone()),
            );
            object.insert(
                "x-agent-os-mcp-prompt".into(),
                Value::String(remote_name.into()),
            );
            proxied.push(prompt);
        }
    }
    Ok(proxied)
}

fn mcp_registered_prompt_get_response(
    store: &Store,
    id: Value,
    name: &str,
    arguments: &serde_json::Map<String, Value>,
) -> Option<Value> {
    let servers = match mcp_enabled_servers(store) {
        Ok(servers) => servers,
        Err(error) => return Some(mcp_invalid_params(id, error)),
    };
    for server in servers {
        let prefix = mcp_proxy_tool_prefix(&server.id);
        let Some(remote_name) = name.strip_prefix(&prefix) else {
            continue;
        };
        if remote_name.trim().is_empty() {
            return Some(mcp_invalid_params(id, "missing remote MCP prompt name"));
        }
        return Some(
            match mcp_child_request(
                &server,
                "prompts/get",
                serde_json::json!({
                    "name": remote_name,
                    "arguments": arguments
                }),
            ) {
                Ok(result) => mcp_success(id, result),
                Err(error) => mcp_invalid_params(id, error),
            },
        );
    }
    None
}

fn mcp_registered_server_tool_call(
    server: &McpServer,
    remote_name: &str,
    arguments: &serde_json::Map<String, Value>,
) -> Result<Value, String> {
    mcp_child_request(
        server,
        "tools/call",
        serde_json::json!({
            "name": remote_name,
            "arguments": arguments
        }),
    )
}

fn mcp_child_request(server: &McpServer, method: &str, params: Value) -> Result<Value, String> {
    mcp_child_request_with_timeout(server, method, params, MCP_CHILD_TIMEOUT)
}

fn mcp_child_request_with_timeout(
    server: &McpServer,
    method: &str,
    params: Value,
    timeout: Duration,
) -> Result<Value, String> {
    let mut child = std::process::Command::new(&server.command)
        .args(&server.args)
        .envs(&server.env)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|error| format!("failed to start mcp server {}: {error}", server.id))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| format!("mcp server {} stdin unavailable", server.id))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| format!("mcp server {} stdout unavailable", server.id))?;
    let (tx, rx) = std::sync::mpsc::channel::<Result<String, std::io::Error>>();
    std::thread::spawn(move || {
        let reader = std::io::BufReader::new(stdout);
        for line in reader.lines() {
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    let init = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "agent-os",
                "version": env!("CARGO_PKG_VERSION")
            }
        }
    });
    let initialized = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized",
        "params": {}
    });
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": method,
        "params": params
    });
    for message in [&init, &initialized, &request] {
        serde_json::to_writer(&mut stdin, message)
            .map_err(|error| format!("failed to write to mcp server {}: {error}", server.id))?;
        writeln!(&mut stdin)
            .map_err(|error| format!("failed to write to mcp server {}: {error}", server.id))?;
    }
    stdin
        .flush()
        .map_err(|error| format!("failed to flush mcp server {}: {error}", server.id))?;

    let response = mcp_read_child_response(&server.id, &rx, 2, timeout);
    let _ = terminate_child_process_tree(&mut child, false);
    let _ = child.wait();
    response
}

fn mcp_read_child_response(
    server_id: &str,
    rx: &std::sync::mpsc::Receiver<Result<String, std::io::Error>>,
    expected_id: i64,
    timeout: Duration,
) -> Result<Value, String> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err(format!("mcp server {server_id} timed out"));
        }
        let line = rx
            .recv_timeout(remaining.min(Duration::from_millis(100)))
            .map_err(|error| match error {
                std::sync::mpsc::RecvTimeoutError::Timeout => {
                    format!("mcp server {server_id} timed out")
                }
                std::sync::mpsc::RecvTimeoutError::Disconnected => {
                    format!("mcp server {server_id} closed without response")
                }
            })?
            .map_err(|error| format!("failed reading mcp server {server_id}: {error}"))?;
        if line.trim().is_empty() {
            continue;
        }
        let response = serde_json::from_str::<Value>(&line)
            .map_err(|error| format!("invalid response from mcp server {server_id}: {error}"))?;
        if response.get("id").and_then(Value::as_i64) != Some(expected_id) {
            continue;
        }
        if let Some(error) = response.get("error") {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("MCP server returned an error");
            return Err(format!("mcp server {server_id}: {message}"));
        }
        return response
            .get("result")
            .cloned()
            .ok_or_else(|| format!("mcp server {server_id} response missing result"));
    }
}

fn mcp_proxy_tool_prefix(server_id: &str) -> String {
    format!("mcp_{server_id}__")
}

fn mcp_proxy_tool_name(server_id: &str, remote_name: &str) -> String {
    format!("{}{remote_name}", mcp_proxy_tool_prefix(server_id))
}

fn mcp_proxy_resource_prefix(server_id: &str) -> String {
    format!("mcp+agent-os://{server_id}/")
}

fn mcp_proxy_resource_uri(server_id: &str, remote_uri: &str) -> String {
    format!("{}{remote_uri}", mcp_proxy_resource_prefix(server_id))
}

fn mcp_create_task(
    store: &Store,
    arguments: &serde_json::Map<String, Value>,
) -> Result<Value, String> {
    let title = required_mcp_string(arguments, "title")?;
    validate_task_title(&title).map_err(|error| error.to_string())?;
    let objective = optional_mcp_string(arguments, "objective")?.unwrap_or_else(|| title.clone());
    validate_task_objective(&objective).map_err(|error| error.to_string())?;
    let command = optional_mcp_string(arguments, "command")?;
    if let Some(command) = &command {
        validate_task_command(command).map_err(|error| error.to_string())?;
    }
    let cwd = optional_mcp_string(arguments, "cwd")?;
    validate_optional_text("task cwd", cwd.as_deref()).map_err(|error| error.to_string())?;
    let capabilities = optional_mcp_string_list(arguments, "need")?;
    validate_capability_values("task required capabilities", &capabilities, false)
        .map_err(|error| error.to_string())?;
    let priority = optional_mcp_string(arguments, "priority")?
        .as_deref()
        .map(parse_priority)
        .transpose()
        .map_err(|error| error.to_string())?
        .unwrap_or(Priority::Normal);

    let task = store
        .update(|os| {
            let mut task = Task::new(title, objective, priority, capabilities);
            os.ensure_unique_task_id(&mut task);
            task.command = command;
            task.cwd = cwd;
            os.create_task(task.clone());
            Ok::<_, anyhow::Error>(task)
        })
        .map_err(|error| error.to_string())?;
    Ok(serde_json::json!({
        "id": task.id,
        "task": task
    }))
}

fn mcp_memory_search(
    store: &Store,
    arguments: &serde_json::Map<String, Value>,
) -> Result<Value, String> {
    let query = required_mcp_string(arguments, "query")?;
    if query.trim().is_empty() {
        return Err("memory search query must not be empty".into());
    }
    let tag = optional_mcp_string(arguments, "tag")?;
    let tags = tag
        .as_deref()
        .map(|value| normalize_list(vec![value.to_owned()]))
        .unwrap_or_default();
    let limit = optional_mcp_usize(arguments, "limit")?.unwrap_or(20);
    if limit == 0 {
        return Err("memory limit must be greater than 0".into());
    }
    let os = store.load().map_err(|error| error.to_string())?;
    let mut records = os
        .memory
        .into_iter()
        .filter_map(|record| {
            let score = memory_relevance_score(&record, &query);
            (score > 0).then_some((score, record))
        })
        .filter(|record| {
            tags.iter()
                .all(|tag| record.1.tags.iter().any(|value| value == tag))
        })
        .collect::<Vec<_>>();
    records.sort_by(|(left_score, left), (right_score, right)| {
        right_score
            .cmp(left_score)
            .then_with(|| right.updated_at.cmp(&left.updated_at))
    });
    records.truncate(limit);
    let records = records
        .into_iter()
        .map(|(_, record)| record)
        .collect::<Vec<_>>();
    Ok(serde_json::json!({
        "query": query,
        "records": records
    }))
}

fn mcp_approval_list(
    store: &Store,
    arguments: &serde_json::Map<String, Value>,
) -> Result<Value, String> {
    let status = optional_mcp_string(arguments, "status")?
        .as_deref()
        .map(parse_mcp_approval_status)
        .transpose()?;
    let os = store.load().map_err(|error| error.to_string())?;
    let approvals = os
        .approvals
        .values()
        .filter(|approval| {
            status
                .as_ref()
                .map(|status| &approval.status == status)
                .unwrap_or(true)
        })
        .cloned()
        .collect::<Vec<_>>();
    Ok(serde_json::json!({
        "approvals": approvals,
        "count": approvals.len()
    }))
}

fn mcp_resolve_approval(
    store: &Store,
    arguments: &serde_json::Map<String, Value>,
) -> Result<Value, String> {
    let id = required_mcp_string(arguments, "id")?;
    validate_approval_id(&id).map_err(|error| error.to_string())?;
    let approved = required_mcp_bool(arguments, "approved")?;
    let by = optional_mcp_string(arguments, "by")?;
    validate_optional_text("approval resolver", by.as_deref())
        .map_err(|error| error.to_string())?;
    let approval = store
        .update(|os| {
            os.resolve_approval(&id, approved, by)
                .with_context(|| format!("approval not found: {id}"))
        })
        .map_err(|error: anyhow::Error| error.to_string())?;
    Ok(serde_json::json!({
        "id": id,
        "approval": approval
    }))
}

fn parse_mcp_approval_status(input: &str) -> Result<ApprovalStatus, String> {
    match input {
        "pending" => Ok(ApprovalStatus::Pending),
        "approved" => Ok(ApprovalStatus::Approved),
        "denied" => Ok(ApprovalStatus::Denied),
        _ => Err("approval status must be pending, approved, or denied".to_owned()),
    }
}

fn mcp_git_status(arguments: &serde_json::Map<String, Value>) -> Result<Value, String> {
    let cwd = optional_mcp_string(arguments, "cwd")?.map(PathBuf::from);
    let cwd = resolve_git_cwd(cwd).map_err(|error| error.to_string())?;
    let output = run_git_command(&cwd, &["status", "--short", "--branch"], false)
        .map_err(|error| error.to_string())?;
    Ok(serde_json::json!(output))
}

fn mcp_secrets_check(
    store: &Store,
    arguments: &serde_json::Map<String, Value>,
) -> Result<Value, String> {
    if !arguments.is_empty() {
        return Err("agent_os_secrets_check does not accept arguments".into());
    }
    let os = store.load().map_err(|error| error.to_string())?;
    Ok(serde_json::json!(secret_check_report(&os)))
}

fn mcp_status_payload(os: &OperatingSystem, state_path: String) -> Value {
    serde_json::json!({
        "name": os.name,
        "agent_os_version": env!("CARGO_PKG_VERSION"),
        "state_path": state_path,
        "agents": os.agents.len(),
        "tasks_pending": os.tasks.values().filter(|task| task.status == TaskStatus::Pending).count(),
        "tasks_running": os.tasks.values().filter(|task| task.status == TaskStatus::Running).count(),
        "tasks_blocked": os.tasks.values().filter(|task| task.status == TaskStatus::Blocked).count(),
        "tasks_complete": os.tasks.values().filter(|task| task.status == TaskStatus::Complete).count(),
        "tasks_failed": os.tasks.values().filter(|task| task.status == TaskStatus::Failed).count(),
        "tasks_cancelled": os.tasks.values().filter(|task| task.status == TaskStatus::Cancelled).count(),
        "workflows": os.workflows.len(),
        "runs": os.runs.len(),
        "tools": os.tools.len(),
        "memories": os.memory.len(),
        "events": os.events.len()
    })
}

fn mcp_resources() -> Vec<Value> {
    vec![
        serde_json::json!({
            "uri": "agent-os://status",
            "name": "Agent OS status",
            "description": "Current Agent OS counters and runtime status.",
            "mimeType": "application/json"
        }),
        serde_json::json!({
            "uri": "agent-os://tasks",
            "name": "Agent OS tasks",
            "description": "Current task records keyed by task id.",
            "mimeType": "application/json"
        }),
        serde_json::json!({
            "uri": "agent-os://workflows",
            "name": "Agent OS workflows",
            "description": "Current workflow records keyed by workflow id.",
            "mimeType": "application/json"
        }),
        serde_json::json!({
            "uri": "agent-os://memory",
            "name": "Agent OS memory",
            "description": "Shared memory records.",
            "mimeType": "application/json"
        }),
        serde_json::json!({
            "uri": "agent-os://runs",
            "name": "Agent OS runs",
            "description": "Task run records keyed by run id.",
            "mimeType": "application/json"
        }),
        serde_json::json!({
            "uri": "agent-os://approvals",
            "name": "Agent OS approvals",
            "description": "Human approval gate records keyed by approval id.",
            "mimeType": "application/json"
        }),
        serde_json::json!({
            "uri": "agent-os://workers",
            "name": "Agent OS workers",
            "description": "Distributed worker node records keyed by worker id.",
            "mimeType": "application/json"
        }),
        serde_json::json!({
            "uri": "agent-os://evals",
            "name": "Agent OS evals",
            "description": "Benchmark evaluation records.",
            "mimeType": "application/json"
        }),
        serde_json::json!({
            "uri": "agent-os://registry",
            "name": "Agent OS registry",
            "description": "Reusable agent profiles, workflow templates, and MCP server registrations.",
            "mimeType": "application/json"
        }),
        serde_json::json!({
            "uri": "agent-os://secrets",
            "name": "Agent OS secrets",
            "description": "Secret backend metadata without resolved secret values.",
            "mimeType": "application/json"
        }),
        serde_json::json!({
            "uri": "agent-os://policy",
            "name": "Agent OS policy",
            "description": "Current execution policy, autonomy, sandbox, network, approval, and memory policy settings.",
            "mimeType": "application/json"
        }),
    ]
}

fn mcp_builtin_resource_contents(store: &Store, uri: &str) -> Result<Value, String> {
    let os = store.load().map_err(|error| error.to_string())?;
    let payload = match uri {
        "agent-os://status" => mcp_status_payload(&os, store.path().display().to_string()),
        "agent-os://tasks" => {
            let count = os.tasks.len();
            serde_json::json!({
                "tasks": os.tasks,
                "count": count
            })
        }
        "agent-os://workflows" => {
            let count = os.workflows.len();
            serde_json::json!({
                "workflows": os.workflows,
                "count": count
            })
        }
        "agent-os://memory" => {
            let count = os.memory.len();
            serde_json::json!({
                "memory": os.memory,
                "count": count
            })
        }
        "agent-os://runs" => {
            let count = os.runs.len();
            serde_json::json!({
                "runs": os.runs,
                "count": count
            })
        }
        "agent-os://approvals" => {
            let count = os.approvals.len();
            serde_json::json!({
                "approvals": os.approvals,
                "count": count
            })
        }
        "agent-os://workers" => {
            let count = os.workers.len();
            serde_json::json!({
                "workers": os.workers,
                "count": count
            })
        }
        "agent-os://evals" => {
            let count = os.evals.len();
            serde_json::json!({
                "evals": os.evals,
                "count": count
            })
        }
        "agent-os://registry" => {
            serde_json::json!({
                "agent_profiles": os.agent_profiles,
                "workflow_templates": os.workflow_templates,
                "mcp_servers": os.mcp_servers,
                "counts": {
                    "agent_profiles": os.agent_profiles.len(),
                    "workflow_templates": os.workflow_templates.len(),
                    "mcp_servers": os.mcp_servers.len()
                }
            })
        }
        "agent-os://secrets" => {
            let count = os.secrets_backends.len();
            serde_json::json!({
                "secrets_backends": os.secrets_backends,
                "count": count
            })
        }
        "agent-os://policy" => {
            serde_json::json!({
                "policy": os.policy,
                "memory_policy": os.memory_policy,
                "provider": os.provider,
            })
        }
        _ => return Err(format!("unknown MCP resource `{uri}`")),
    };
    let text = serde_json::to_string_pretty(&payload)
        .map_err(|error| format!("could not encode resource: {error}"))?;
    Ok(serde_json::json!({
        "uri": uri,
        "mimeType": "application/json",
        "text": text
    }))
}

fn mcp_tools() -> Vec<Value> {
    vec![
        serde_json::json!({
            "name": "agent_os_status",
            "description": "Return Agent OS state counters and runtime status.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }
        }),
        serde_json::json!({
            "name": "agent_os_create_task",
            "description": "Create a pending Agent OS task for the scheduler.",
            "inputSchema": {
                "type": "object",
                "required": ["title"],
                "properties": {
                    "title": { "type": "string", "minLength": 1 },
                    "objective": { "type": "string", "minLength": 1 },
                    "command": { "type": "string", "minLength": 1 },
                    "cwd": { "type": "string", "minLength": 1 },
                    "priority": { "type": "string", "enum": Priority::VALUES },
                    "need": {
                        "oneOf": [
                            { "type": "string", "minLength": 1 },
                            { "type": "array", "items": { "type": "string", "minLength": 1 } }
                        ]
                    }
                },
                "additionalProperties": false
            }
        }),
        serde_json::json!({
            "name": "agent_os_memory_search",
            "description": "Search shared Agent OS memory by text and optional tag.",
            "inputSchema": {
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": { "type": "string", "minLength": 1 },
                    "tag": { "type": "string", "minLength": 1 },
                    "limit": { "type": "integer", "minimum": 1 }
                },
                "additionalProperties": false
            }
        }),
        serde_json::json!({
            "name": "agent_os_approval_list",
            "description": "List Agent OS human approval gate requests, optionally filtered by status.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "status": { "type": "string", "enum": ["pending", "approved", "denied"] }
                },
                "additionalProperties": false
            }
        }),
        serde_json::json!({
            "name": "agent_os_resolve_approval",
            "description": "Approve or deny a pending Agent OS approval gate request.",
            "inputSchema": {
                "type": "object",
                "required": ["id", "approved"],
                "properties": {
                    "id": { "type": "string", "minLength": 1 },
                    "approved": { "type": "boolean" },
                    "by": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }
        }),
        serde_json::json!({
            "name": "agent_os_git_status",
            "description": "Return git status --short --branch for a local workspace.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cwd": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }
        }),
        serde_json::json!({
            "name": "agent_os_secrets_check",
            "description": "Report Agent OS secret references and presence metadata without exposing secret values.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }
        }),
    ]
}

fn mcp_prompts() -> Vec<Value> {
    vec![
        serde_json::json!({
            "name": "agent_os_plan_task",
            "description": "Draft an Agent OS task plan from an objective, optional constraints, and capabilities.",
            "arguments": [
                {
                    "name": "objective",
                    "description": "The task objective to plan.",
                    "required": true
                },
                {
                    "name": "constraints",
                    "description": "Operational or policy constraints to honor.",
                    "required": false
                },
                {
                    "name": "capabilities",
                    "description": "Comma-separated capabilities the task should require.",
                    "required": false
                }
            ]
        }),
        serde_json::json!({
            "name": "agent_os_memory_brief",
            "description": "Prepare a response brief that first searches Agent OS memory for relevant context.",
            "arguments": [
                {
                    "name": "query",
                    "description": "The memory search query.",
                    "required": true
                },
                {
                    "name": "tag",
                    "description": "Optional memory tag filter.",
                    "required": false
                }
            ]
        }),
    ]
}

fn mcp_builtin_prompt(
    name: &str,
    arguments: &serde_json::Map<String, Value>,
) -> Result<Value, String> {
    match name {
        "agent_os_plan_task" => {
            let objective = required_mcp_string(arguments, "objective")?;
            let constraints = optional_mcp_string(arguments, "constraints")?;
            let capabilities = optional_mcp_string(arguments, "capabilities")?;
            let mut text = format!(
                "Create an Agent OS task plan for this objective:\n\n{objective}\n\nReturn a concise plan with title, objective, command if shell execution is needed, required capabilities, risks, and verification steps."
            );
            if let Some(constraints) = constraints {
                text.push_str("\n\nConstraints:\n");
                text.push_str(&constraints);
            }
            if let Some(capabilities) = capabilities {
                text.push_str("\n\nCandidate capabilities:\n");
                text.push_str(&capabilities);
            }
            Ok(mcp_prompt_response("Agent OS task planning prompt", text))
        }
        "agent_os_memory_brief" => {
            let query = required_mcp_string(arguments, "query")?;
            let tag = optional_mcp_string(arguments, "tag")?;
            let mut text = format!(
                "Use the `agent_os_memory_search` tool before answering. Search query: {query}\n\nSynthesize the relevant memory records, call out stale or conflicting context, and keep the final answer actionable."
            );
            if let Some(tag) = tag {
                text.push_str("\n\nMemory tag filter: ");
                text.push_str(&tag);
            }
            Ok(mcp_prompt_response("Agent OS memory brief prompt", text))
        }
        _ => Err(format!("unknown MCP prompt `{name}`")),
    }
}

fn mcp_prompt_response(description: &str, text: String) -> Value {
    serde_json::json!({
        "description": description,
        "messages": [
            {
                "role": "user",
                "content": {
                    "type": "text",
                    "text": text
                }
            }
        ]
    })
}

fn required_mcp_string(
    arguments: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<String, String> {
    optional_mcp_string(arguments, key)?.ok_or_else(|| format!("missing `{key}`"))
}

fn optional_mcp_string(
    arguments: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<String>, String> {
    let Some(value) = arguments.get(key) else {
        return Ok(None);
    };
    value
        .as_str()
        .map(|value| Some(value.to_owned()))
        .ok_or_else(|| format!("`{key}` must be a string"))
}

fn optional_mcp_string_list(
    arguments: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Vec<String>, String> {
    let Some(value) = arguments.get(key) else {
        return Ok(Vec::new());
    };
    if let Some(value) = value.as_str() {
        return Ok(normalize_list(vec![value.to_owned()]));
    }
    let Some(values) = value.as_array() else {
        return Err(format!("`{key}` must be a string or array of strings"));
    };
    let mut parsed = Vec::new();
    for value in values {
        let Some(value) = value.as_str() else {
            return Err(format!("`{key}` entries must be strings"));
        };
        parsed.push(value.to_owned());
    }
    Ok(normalize_list(parsed))
}

fn optional_mcp_usize(
    arguments: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<usize>, String> {
    let Some(value) = arguments.get(key) else {
        return Ok(None);
    };
    let Some(value) = value.as_u64() else {
        return Err(format!("`{key}` must be a positive integer"));
    };
    usize::try_from(value)
        .map(Some)
        .map_err(|_| format!("`{key}` is too large"))
}

fn required_mcp_bool(
    arguments: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<bool, String> {
    arguments
        .get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("`{key}` must be a boolean"))
}

fn mcp_success(id: Value, result: Value) -> Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
}

fn mcp_tool_result(id: Value, is_error: bool, text: impl Into<String>) -> Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "content": [
                {
                    "type": "text",
                    "text": text.into()
                }
            ],
            "isError": is_error
        }
    })
}

fn mcp_protocol_error(id: Value, code: i64, message: impl Into<String>) -> Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message.into()
        }
    })
}

fn mcp_invalid_params(id: Value, message: impl Into<String>) -> Value {
    mcp_protocol_error(id, -32602, message)
}

fn run_once(
    os: &mut OperatingSystem,
    store: &Store,
    limit: usize,
    execute: bool,
    dry_run: bool,
    recover_stale_seconds: Option<i64>,
) -> Result<(RuntimeReport, Vec<RunRecord>, Vec<String>)> {
    validate_scheduler_inputs(limit, recover_stale_seconds)?;
    if dry_run && execute {
        bail!("--dry-run cannot be combined with --execute");
    }
    if dry_run {
        let mut preview = os.clone();
        let mut report = RuntimeReport::default();
        if let Some(seconds) = recover_stale_seconds {
            report.absorb_recovery(Runtime::recover_stale_state(
                &mut preview,
                ChronoDuration::seconds(seconds),
            ));
        }
        let tick_report = Runtime::tick(&mut preview, limit);
        report.absorb_tick(tick_report);
        return Ok((report, Vec::new(), Vec::new()));
    }
    let (report, scheduled_os) = store.update(|os| {
        let mut report = RuntimeReport::default();
        if let Some(seconds) = recover_stale_seconds {
            report.absorb_recovery(Runtime::recover_stale_state(
                os,
                ChronoDuration::seconds(seconds),
            ));
        }
        let tick_report = Runtime::tick(os, limit);
        report.absorb_tick(tick_report);
        Ok::<_, anyhow::Error>((report, os.clone()))
    })?;
    *os = scheduled_os;
    let assignments = report.assignments.clone();
    let mut executed = Vec::new();
    let mut errors = Vec::new();
    if execute {
        let task_ids = assignments
            .iter()
            .map(|assignment| assignment.task_id.clone())
            .collect::<Vec<_>>();
        for result in CommandExecutor::execute_tasks_parallel(os, store, &task_ids) {
            match result {
                Ok(run) => executed.push(run),
                Err(error) => errors.push(error.to_string()),
            }
        }
    }
    Ok((report, executed, errors))
}

fn print_scheduler_notes(report: &RuntimeReport) {
    for note in &report.notes {
        println!("note: {note}");
    }
}

fn execute_next_workflow_stage(
    store: &Store,
    workflow_id: &WorkflowId,
) -> Result<(Vec<RunRecord>, Vec<String>)> {
    let (assignment, mut os) = store.update(|os| {
        let task_id = os.workflow_progress(workflow_id).and_then(|progress| {
            progress
                .stages
                .into_iter()
                .find(|stage| stage.status.as_ref() != Some(&TaskStatus::Complete))
                .map(|stage| stage.task_id)
        });
        let assignment = task_id.and_then(|task_id| Scheduler::assign_ready_task(os, &task_id));
        Ok::<_, anyhow::Error>((assignment, os.clone()))
    })?;

    let Some(assignment) = assignment else {
        return Ok((Vec::new(), Vec::new()));
    };

    let mut executed = Vec::new();
    let mut errors = Vec::new();
    for result in CommandExecutor::execute_tasks_parallel(
        &mut os,
        store,
        std::slice::from_ref(&assignment.task_id),
    ) {
        match result {
            Ok(run) => executed.push(run),
            Err(error) => errors.push(error.to_string()),
        }
    }
    Ok((executed, errors))
}

fn execute_workflow_stages(
    store: &Store,
    workflow_id: &WorkflowId,
    run_all: bool,
) -> Result<(Vec<RunRecord>, Vec<String>)> {
    let max_runs = if run_all {
        store
            .load()
            .with_context(|| state_init_hint(&store))?
            .workflow_progress(workflow_id)
            .map(|progress| progress.total_tasks)
            .unwrap_or(0)
    } else {
        1
    };
    let mut runs = Vec::new();
    let mut errors = Vec::new();
    for _ in 0..max_runs {
        let (stage_runs, stage_errors) = execute_next_workflow_stage(store, workflow_id)?;
        if stage_runs.is_empty() && stage_errors.is_empty() {
            break;
        }
        runs.extend(stage_runs);
        errors.extend(stage_errors);
        if !run_all {
            break;
        }
    }
    Ok((runs, errors))
}

fn validate_scheduler_inputs(limit: usize, recover_stale_seconds: Option<i64>) -> Result<()> {
    if limit == 0 {
        bail!("limit must be greater than 0");
    }
    if let Some(seconds) = recover_stale_seconds
        && seconds < 0
    {
        bail!("recover_stale_seconds must be greater than or equal to 0");
    }
    Ok(())
}

fn validate_daemon_timing(interval_ms: u64, max_ticks: Option<usize>) -> Result<()> {
    if interval_ms == 0 {
        bail!("interval_ms must be greater than 0");
    }
    if let Some(max_ticks) = max_ticks
        && max_ticks == 0
    {
        bail!("max_ticks must be greater than 0");
    }
    Ok(())
}

fn validate_tail_interval(interval_ms: u64) -> Result<()> {
    if interval_ms == 0 {
        bail!("interval_ms must be greater than 0");
    }
    Ok(())
}

fn validate_events_limit(limit: usize) -> Result<()> {
    if limit == 0 {
        bail!("events limit must be greater than 0");
    }
    Ok(())
}

fn validate_lease_seconds(lease_seconds: Option<i64>) -> Result<()> {
    if let Some(lease_seconds) = lease_seconds
        && lease_seconds <= 0
    {
        bail!("lease_seconds must be greater than 0");
    }
    Ok(())
}

fn validate_runs_limit(limit: Option<usize>) -> Result<()> {
    if limit == Some(0) {
        bail!("runs limit must be greater than 0");
    }
    Ok(())
}

fn validate_log_tail_bytes(tail_bytes: Option<usize>) -> Result<()> {
    if tail_bytes == Some(0) {
        bail!("tail_bytes must be greater than 0");
    }
    Ok(())
}

fn validate_memory_limit(limit: Option<usize>) -> Result<()> {
    if limit == Some(0) {
        bail!("memory limit must be greater than 0");
    }
    Ok(())
}

fn parse_memory_visibility(value: &str) -> Result<MemoryVisibility> {
    MemoryVisibility::try_parse(value)
        .with_context(|| "memory visibility must be one of: shared, private")
}

fn validate_task_list_limit(limit: Option<usize>) -> Result<()> {
    if limit == Some(0) {
        bail!("task list limit must be greater than 0");
    }
    Ok(())
}

fn validate_agent_list_limit(limit: Option<usize>) -> Result<()> {
    if limit == Some(0) {
        bail!("agent list limit must be greater than 0");
    }
    Ok(())
}

fn validate_tool_list_limit(limit: Option<usize>) -> Result<()> {
    if limit == Some(0) {
        bail!("tool list limit must be greater than 0");
    }
    Ok(())
}

fn validate_workflow_list_limit(limit: Option<usize>) -> Result<()> {
    if limit == Some(0) {
        bail!("workflow list limit must be greater than 0");
    }
    Ok(())
}

fn validate_worker_list_limit(limit: Option<usize>) -> Result<()> {
    if limit == Some(0) {
        bail!("worker list limit must be greater than 0");
    }
    Ok(())
}

fn validate_eval_list_limit(limit: Option<usize>) -> Result<()> {
    if limit == Some(0) {
        bail!("eval list limit must be greater than 0");
    }
    Ok(())
}

fn validate_secrets_list_limit(limit: Option<usize>) -> Result<()> {
    if limit == Some(0) {
        bail!("secrets list limit must be greater than 0");
    }
    Ok(())
}

fn validate_api_serve_inputs(args: &ApiServeArgs) -> Result<()> {
    if let Some(max_requests) = args.max_requests
        && max_requests == 0
    {
        bail!("max_requests must be greater than 0");
    }
    if let Some(token_env) = &args.token_env {
        validate_env_var_name("token_env", token_env)?;
    }
    if let Some(token_file) = &args.token_file {
        validate_path("token_file", token_file)?;
    }
    if args.token_env.is_some() && args.token_file.is_some() {
        bail!("use either --token-env or --token-file, not both");
    }
    if let Some(token_env) = &args.read_token_env {
        validate_env_var_name("read_token_env", token_env)?;
    }
    if let Some(token_file) = &args.read_token_file {
        validate_path("read_token_file", token_file)?;
    }
    if args.read_token_env.is_some() && args.read_token_file.is_some() {
        bail!("use either --read-token-env or --read-token-file, not both");
    }
    if let Some(token_env) = &args.write_token_env {
        validate_env_var_name("write_token_env", token_env)?;
    }
    if let Some(token_file) = &args.write_token_file {
        validate_path("write_token_file", token_file)?;
    }
    if args.write_token_env.is_some() && args.write_token_file.is_some() {
        bail!("use either --write-token-env or --write-token-file, not both");
    }
    for origin in &args.allow_origin {
        if normalize_cors_origin(origin).is_none() {
            bail!(
                "allow_origin must be an http(s) origin without credentials, path, query, or fragment: {origin}"
            );
        }
    }
    if args.token_env.is_none()
        && args.token_file.is_none()
        && args.read_token_env.is_none()
        && args.read_token_file.is_none()
        && args.write_token_env.is_none()
        && args.write_token_file.is_none()
        && !args.unsafe_no_token
        && api_bind_requires_token(&args.addr)
    {
        bail!(
            "api serve on non-loopback addresses requires --token-env, --token-file, --read-token-env, --read-token-file, --write-token-env, --write-token-file, or --unsafe-no-token"
        );
    }
    Ok(())
}

fn api_cors_from_args(args: &ApiServeArgs) -> ApiCors {
    ApiCors::allow_origins(
        args.allow_origin
            .iter()
            .filter_map(|origin| normalize_cors_origin(origin))
            .collect(),
    )
}

fn api_bind_requires_token(addr: &str) -> bool {
    if let Ok(socket_addr) = addr.parse::<SocketAddr>() {
        return !socket_addr.ip().is_loopback();
    }
    let lowercase = addr.to_ascii_lowercase();
    !(lowercase.starts_with("localhost:") || lowercase.starts_with("[::1]:"))
}

fn validate_os_name(name: &str) -> Result<()> {
    if name.trim().is_empty() {
        bail!("OS name must not be empty");
    }
    Ok(())
}

fn parse_agent_status(input: &str) -> Result<AgentStatus> {
    AgentStatus::try_parse(input).with_context(|| {
        format!(
            "invalid agent status `{input}`; expected online/up, busy, paused/pause, or offline/down"
        )
    })
}

fn parse_agent_kind_filter(input: &str) -> Result<AgentKind> {
    validate_agent_kind(input)?;
    Ok(AgentKind::parse(input))
}

fn parse_priority(input: &str) -> Result<Priority> {
    Priority::try_parse(input).with_context(|| {
        format!("invalid priority `{input}`; expected low, normal, high, or critical/urgent")
    })
}

fn parse_tool_kind(input: &str) -> Result<ToolKind> {
    ToolKind::try_parse(input).with_context(|| {
        format!(
            "invalid tool kind `{input}`; expected shell, file-read/read-file, or file-write/write-file"
        )
    })
}

fn parse_task_status(input: &str) -> Result<TaskStatus> {
    TaskStatus::try_parse(input).with_context(|| {
        format!(
            "invalid task status `{input}`; expected pending, running, blocked, complete/completed, failed, or cancelled/canceled"
        )
    })
}

fn parse_run_status(input: &str) -> Result<RunStatus> {
    RunStatus::try_parse(input).with_context(|| {
        format!(
            "invalid run status `{input}`; expected running, cancel-requested/cancel_requested, cancelled/canceled, success/succeeded, failed, or rejected"
        )
    })
}

fn parse_run_artifact_kind(input: &str) -> Result<RunArtifactKind> {
    match input.trim().to_ascii_lowercase().as_str() {
        "stdout" => Ok(RunArtifactKind::Stdout),
        "stderr" => Ok(RunArtifactKind::Stderr),
        "summary" => Ok(RunArtifactKind::Summary),
        "diff" => Ok(RunArtifactKind::Diff),
        "file" => Ok(RunArtifactKind::File),
        _ => bail!(
            "invalid run artifact kind `{input}`; expected stdout, stderr, summary, diff, or file"
        ),
    }
}

fn parse_worker_report_status(input: &str) -> Result<TaskStatus> {
    let status = parse_task_status(input)?;
    if matches!(status, TaskStatus::Complete | TaskStatus::Failed) {
        Ok(status)
    } else {
        bail!("worker report status must be complete or failed")
    }
}

fn parse_secrets_backend_kind(input: &str) -> Result<SecretsBackendKind> {
    match input.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "environment" | "env" => Ok(SecretsBackendKind::Environment),
        "one-password" | "1password" | "1-password" | "op" => Ok(SecretsBackendKind::OnePassword),
        "os-keychain" | "keychain" | "macos-keychain" => Ok(SecretsBackendKind::OsKeychain),
        "env-vault" | "envvault" => Ok(SecretsBackendKind::EnvVault),
        _ => bail!(
            "invalid secrets backend kind `{input}`; expected environment, 1password, os-keychain, or env-vault"
        ),
    }
}

fn secrets_backend_kind_name(kind: &SecretsBackendKind) -> &'static str {
    match kind {
        SecretsBackendKind::Environment => "environment",
        SecretsBackendKind::OnePassword => "one-password",
        SecretsBackendKind::OsKeychain => "os-keychain",
        SecretsBackendKind::EnvVault => "env-vault",
    }
}

fn parse_event_kind(input: &str) -> Result<EventKind> {
    EventKind::try_parse(input).with_context(|| {
        format!(
            "invalid event kind `{input}`; expected one of: {}",
            EventKind::VALUES.join(", ")
        )
    })
}

fn parse_filter_timestamp(input: &str, resource: &str, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(input)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .with_context(|| {
            format!("invalid {resource} {field} timestamp `{input}`; expected RFC3339")
        })
}

fn capability_filter(field: &str, values: &[String]) -> Result<Vec<String>> {
    validate_capability_values(field, values, false)?;
    Ok(normalize_list(values.to_vec()))
}

fn has_all_capabilities(available: &[String], required: &[String]) -> bool {
    required
        .iter()
        .all(|required| available.iter().any(|available| available == required))
}

fn tag_filter(field: &str, values: &[String]) -> Result<Vec<String>> {
    validate_tag_values(field, values)?;
    Ok(normalize_list(values.to_vec()))
}

fn has_all_tags(available: &[String], required: &[String]) -> bool {
    required
        .iter()
        .all(|required| available.iter().any(|available| available == required))
}

fn validate_agent_name(name: &str) -> Result<()> {
    if AgentId::new(name).as_str().is_empty() {
        bail!("agent name must contain at least one ASCII letter, digit, or hyphen");
    }
    Ok(())
}

fn parse_agent_id_arg(id: &str) -> Result<AgentId> {
    let id = AgentId::new(id);
    if id.as_str().is_empty() {
        bail!("agent id must contain at least one ASCII letter, digit, or hyphen");
    }
    Ok(id)
}

fn validate_agent_kind(kind: &str) -> Result<()> {
    if kind.trim().is_empty() {
        bail!("agent kind must not be empty");
    }
    Ok(())
}

fn validate_tool_name(name: &str) -> Result<()> {
    if ToolId::new(name).as_str().is_empty() {
        bail!("tool name must contain at least one ASCII letter, digit, or hyphen");
    }
    Ok(())
}

fn validate_tool_id(id: &str) -> Result<()> {
    if ToolId::new(id).as_str().is_empty() {
        bail!("tool id must contain at least one ASCII letter, digit, or hyphen");
    }
    Ok(())
}

fn parse_tool_id_arg(id: &str) -> Result<ToolId> {
    validate_tool_id(id)?;
    Ok(ToolId::new(id))
}

fn validate_task_id(id: &str, field: &str) -> Result<()> {
    if TaskId::from_slug(id).as_str().is_empty() {
        bail!("{field} must contain at least one ASCII letter, digit, or hyphen");
    }
    Ok(())
}

fn parse_task_id_arg(id: &str) -> Result<TaskId> {
    validate_task_id(id, "task id")?;
    Ok(TaskId::from_slug(id))
}

fn parse_run_id_arg(id: &str) -> Result<RunId> {
    let id = RunId::from_slug(id);
    if id.to_string().is_empty() {
        bail!("run id must contain at least one ASCII letter, digit, or hyphen");
    }
    Ok(id)
}

fn parse_workflow_id_arg(id: &str) -> Result<WorkflowId> {
    let id = WorkflowId::from_slug(id);
    if id.as_str().is_empty() {
        bail!("workflow id must contain at least one ASCII letter, digit, or hyphen");
    }
    Ok(id)
}

fn validate_task_title(title: &str) -> Result<()> {
    if title.trim().is_empty() {
        bail!("task title must not be empty");
    }
    Ok(())
}

fn validate_task_objective(objective: &str) -> Result<()> {
    if objective.trim().is_empty() {
        bail!("task objective must not be empty");
    }
    Ok(())
}

fn validate_task_command(command: &str) -> Result<()> {
    if command.trim().is_empty() {
        bail!("task command must not be empty");
    }
    Ok(())
}

fn validate_plan_steps(steps: &[String]) -> Result<()> {
    if steps.is_empty() {
        bail!("plan steps must not be empty");
    }
    if steps.iter().any(|step| step.trim().is_empty()) {
        bail!("plan steps must not contain empty steps");
    }
    Ok(())
}

fn validate_tool_command_template(command_template: &str) -> Result<()> {
    if command_template.trim().is_empty() {
        bail!("tool command template must not be empty");
    }
    Ok(())
}

fn validate_memory_topic(topic: &str) -> Result<()> {
    if topic.trim().is_empty() {
        bail!("memory topic must not be empty");
    }
    Ok(())
}

fn validate_memory_body(body: &str) -> Result<()> {
    if body.trim().is_empty() {
        bail!("memory body must not be empty");
    }
    Ok(())
}

fn validate_memory_scope(scope: &str) -> Result<()> {
    if scope.trim().is_empty() {
        bail!("memory scope must not be empty");
    }
    Ok(())
}

fn validate_memory_query(query: &str) -> Result<()> {
    if query.trim().is_empty() {
        bail!("memory search query must not be empty");
    }
    Ok(())
}

fn validate_memory_id(id: &str) -> Result<()> {
    if !contains_slug_character(id) {
        bail!("memory id must contain at least one ASCII letter, digit, or hyphen");
    }
    Ok(())
}

fn contains_slug_character(value: &str) -> bool {
    value
        .chars()
        .any(|ch| ch.is_ascii_alphanumeric() || ch == '-')
}

fn validate_event_query(query: &str) -> Result<()> {
    if query.trim().is_empty() {
        bail!("event query must not be empty");
    }
    Ok(())
}

fn validate_run_query(query: &str) -> Result<()> {
    if query.trim().is_empty() {
        bail!("run query must not be empty");
    }
    Ok(())
}

fn validate_tool_query(query: &str) -> Result<()> {
    if query.trim().is_empty() {
        bail!("tool query must not be empty");
    }
    Ok(())
}

fn validate_agent_query(query: &str) -> Result<()> {
    if query.trim().is_empty() {
        bail!("agent query must not be empty");
    }
    Ok(())
}

fn agent_matches_query(agent: &Agent, query: &str) -> bool {
    agent.id.to_string().to_ascii_lowercase().contains(query)
        || agent.name.to_ascii_lowercase().contains(query)
        || agent.kind.to_string().to_ascii_lowercase().contains(query)
        || agent
            .model
            .as_deref()
            .map(|model| model.to_ascii_lowercase().contains(query))
            .unwrap_or(false)
        || agent
            .capabilities
            .iter()
            .any(|capability| capability.to_ascii_lowercase().contains(query))
}

fn tool_matches_query(tool: &ToolDefinition, query: &str) -> bool {
    tool.id.to_string().to_ascii_lowercase().contains(query)
        || tool.name.to_ascii_lowercase().contains(query)
        || tool.description.to_ascii_lowercase().contains(query)
        || tool.command_template.to_ascii_lowercase().contains(query)
}

fn validate_task_query(query: &str) -> Result<()> {
    if query.trim().is_empty() {
        bail!("task query must not be empty");
    }
    Ok(())
}

fn validate_workflow_query(query: &str) -> Result<()> {
    if query.trim().is_empty() {
        bail!("workflow query must not be empty");
    }
    Ok(())
}

fn validate_worker_query(query: &str) -> Result<()> {
    if query.trim().is_empty() {
        bail!("worker query must not be empty");
    }
    Ok(())
}

fn validate_eval_query(query: &str) -> Result<()> {
    if query.trim().is_empty() {
        bail!("eval query must not be empty");
    }
    Ok(())
}

fn validate_secrets_query(query: &str) -> Result<()> {
    if query.trim().is_empty() {
        bail!("secrets query must not be empty");
    }
    Ok(())
}

fn workflow_matches_query(workflow: &Workflow, query: &str) -> bool {
    workflow.id.to_string().to_ascii_lowercase().contains(query)
        || workflow.objective.to_ascii_lowercase().contains(query)
        || workflow
            .tasks
            .keys()
            .any(|stage| stage.to_ascii_lowercase().contains(query))
        || workflow
            .tasks
            .values()
            .any(|task_id| task_id.to_string().contains(query))
}

fn worker_matches_query(worker: &WorkerNode, query: &str) -> bool {
    worker.id.to_ascii_lowercase().contains(query)
        || worker.endpoint.to_ascii_lowercase().contains(query)
        || worker
            .status
            .to_string()
            .to_ascii_lowercase()
            .contains(query)
}

fn parse_worker_report_artifacts(values: Vec<String>) -> Result<Vec<RunArtifact>> {
    values
        .into_iter()
        .map(|value| {
            let (kind, path) = value
                .split_once('=')
                .with_context(|| format!("worker artifact `{value}` must use KIND=PATH"))?;
            let kind = parse_run_artifact_kind(kind)?;
            validate_optional_text("worker artifact path", Some(path))?;
            Ok(RunArtifact::new(kind, path))
        })
        .collect()
}

fn eval_matches_query(record: &EvalRecord, query: &str) -> bool {
    record.id.to_ascii_lowercase().contains(query)
        || record.target.to_ascii_lowercase().contains(query)
        || yes_no(record.success).contains(query)
        || record
            .run
            .as_ref()
            .map(|run| {
                run.command.to_ascii_lowercase().contains(query)
                    || run.cwd.to_ascii_lowercase().contains(query)
                    || run.stdout.to_ascii_lowercase().contains(query)
                    || run.stderr.to_ascii_lowercase().contains(query)
                    || run
                        .success_pattern
                        .as_ref()
                        .map(|pattern| pattern.to_ascii_lowercase().contains(query))
                        .unwrap_or(false)
            })
            .unwrap_or(false)
}

fn secrets_backend_matches_query(backend: &SecretsBackend, query: &str) -> bool {
    backend.id.to_ascii_lowercase().contains(query)
        || secrets_backend_kind_name(&backend.kind).contains(query)
        || backend
            .reference
            .as_deref()
            .map(|reference| reference.to_ascii_lowercase().contains(query))
            .unwrap_or(false)
}

fn task_matches_query(task: &Task, query: &str) -> bool {
    task.title.to_ascii_lowercase().contains(query)
        || task.objective.to_ascii_lowercase().contains(query)
        || task
            .command
            .as_deref()
            .map(|command| command.to_ascii_lowercase().contains(query))
            .unwrap_or(false)
        || task
            .output
            .as_deref()
            .map(|output| output.to_ascii_lowercase().contains(query))
            .unwrap_or(false)
        || task
            .plan
            .iter()
            .any(|step| step.to_ascii_lowercase().contains(query))
}

fn validate_workflow_objective(objective: &str) -> Result<()> {
    if objective.trim().is_empty() {
        bail!("workflow objective must not be empty");
    }
    Ok(())
}

fn validate_workflow_stage(stage: &str) -> Result<()> {
    if stage.trim().is_empty() {
        bail!("workflow stage must not be empty");
    }
    if stage
        .chars()
        .any(|ch| ch.is_ascii_control() || matches!(ch, '/' | '\\'))
    {
        bail!("workflow stage must not contain path separators or control characters");
    }
    Ok(())
}

fn validate_capability_values(
    field: &str,
    values: &[String],
    require_non_empty: bool,
) -> Result<()> {
    for value in values {
        if value.trim().is_empty() || value.split(',').any(|part| part.trim().is_empty()) {
            bail!("{field} must not contain empty capabilities");
        }
    }
    if require_non_empty && normalize_list(values.to_vec()).is_empty() {
        bail!("{field} must include at least one capability");
    }
    Ok(())
}

fn validate_env_var_name(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    if !is_valid_env_var_name(value) {
        bail!("{field} must be a valid environment variable name");
    }
    Ok(())
}

fn validate_optional_text(field: &str, value: Option<&str>) -> Result<()> {
    if let Some(value) = value
        && value.trim().is_empty()
    {
        bail!("{field} must not be empty");
    }
    Ok(())
}

fn validate_path(field: &str, path: &std::path::Path) -> Result<()> {
    if path.as_os_str().is_empty() || path.to_string_lossy().trim().is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(())
}

fn validate_optional_path(field: &str, path: Option<&std::path::Path>) -> Result<()> {
    if let Some(path) = path {
        validate_path(field, path)?;
    }
    Ok(())
}

fn validate_tag_values(field: &str, values: &[String]) -> Result<()> {
    if values
        .iter()
        .any(|value| value.trim().is_empty() || value.split(',').any(|part| part.trim().is_empty()))
    {
        bail!("{field} must not contain empty tags");
    }
    Ok(())
}

fn shell_arg(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':'))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn state_init_hint(store: &Store) -> String {
    let state_path = store.path();
    let state_arg_path = if !is_sqlite_state_path(state_path)
        && state_path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name == "state.json")
            .unwrap_or(false)
    {
        state_path.parent().unwrap_or(state_path)
    } else {
        state_path
    };
    format!(
        "state unavailable at {}; run `agent-os --state {} init --profile safe` first",
        state_path.display(),
        shell_arg(&state_arg_path.display().to_string())
    )
}

#[allow(clippy::too_many_arguments)]
fn doctor_next_steps(
    state_directory: &str,
    state_path: &str,
    state_exists: bool,
    state_loads: bool,
    state_valid: Option<bool>,
    config_path: &str,
    config_exists: bool,
    config_loads: bool,
    config_valid: Option<bool>,
) -> Vec<String> {
    let mut steps = Vec::new();
    let state_arg = shell_arg(state_directory);
    let config_arg = shell_arg(config_path);

    if !state_exists {
        steps.push(format!("agent-os --state {state_arg} init"));
    } else if !state_loads {
        steps.push(format!(
            "Restore or repair {}, then rerun agent-os --state {state_arg} doctor",
            shell_arg(state_path)
        ));
    } else if state_valid == Some(false) {
        steps.push(format!(
            "agent-os --state {state_arg} state repair --dry-run"
        ));
    }

    if !config_exists {
        steps.push(format!(
            "agent-os --config {config_arg} config init --profile safe"
        ));
    } else if !config_loads || config_valid == Some(false) {
        steps.push(format!(
            "Fix config issues, then run agent-os --config {config_arg} config validate"
        ));
    }

    steps
}

fn doctor(store: &Store, config_path: &std::path::Path, json: bool) -> Result<()> {
    let state_exists = store.exists();
    let (loaded_state, state_error) = if state_exists {
        match store.load() {
            Ok(os) => (Some(os), None),
            Err(error) => (None, Some(error.to_string())),
        }
    } else {
        (None, None)
    };
    let state_loads = loaded_state.is_some();
    let validation = loaded_state.as_ref().map(validate_state);
    let state_valid = validation.as_ref().map(|report| report.valid);
    let state_issues = validation
        .as_ref()
        .map(|report| report.issues.clone())
        .unwrap_or_default();
    let parent = store
        .path()
        .parent()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| ".".into());

    let config_exists = config_path.exists();
    let (config_loads, config_valid, config_issues, config_error) = if config_exists {
        match load_config(config_path) {
            Ok(Some(config)) => match validate_seed_config(&config) {
                Ok(()) => (true, Some(true), Vec::new(), None),
                Err(error) => (true, Some(false), vec![error.to_string()], None),
            },
            Ok(None) => (false, None, Vec::new(), None),
            Err(error) => {
                let error = error.to_string();
                (false, Some(false), vec![error.clone()], Some(error))
            }
        }
    } else {
        (false, None, Vec::new(), None)
    };
    let state_path = store.path().display().to_string();
    let config_path_text = config_path.display().to_string();
    let next_steps = doctor_next_steps(
        &parent,
        &state_path,
        state_exists,
        state_loads,
        state_valid,
        &config_path_text,
        config_exists,
        config_loads,
        config_valid,
    );

    let report = serde_json::json!({
        "agent_os_version": env!("CARGO_PKG_VERSION"),
        "platform": doctor_platform(),
        "service_manager": doctor_service_manager(),
        "service_recommendation": doctor_service_recommendation(),
        "shell_execution_supported": doctor_shell_execution_supported(),
        "shell_execution_note": doctor_shell_execution_note(),
        "state_path": state_path,
        "state_directory": parent,
        "state_exists": state_exists,
        "state_loads": state_loads,
        "state_error": state_error,
        "state_valid": state_valid,
        "state_issues": state_issues,
        "config_path": config_path_text,
        "config_exists": config_exists,
        "config_loads": config_loads,
        "config_valid": config_valid,
        "config_issues": config_issues,
        "config_error": config_error,
        "next_steps": next_steps,
    });

    if json {
        print_json(&report)?;
    } else {
        println!("agent-os version: {}", env!("CARGO_PKG_VERSION"));
        println!("platform: {}", doctor_platform());
        println!("service manager: {}", doctor_service_manager());
        println!(
            "service recommendation: {}",
            doctor_service_recommendation()
        );
        println!(
            "shell execution supported: {}",
            yes_no(doctor_shell_execution_supported())
        );
        println!("shell execution note: {}", doctor_shell_execution_note());
        println!("state path: {}", store.path().display());
        println!("state exists: {}", yes_no(state_exists));
        println!("state loads: {}", yes_no(state_loads));
        if let Some(error) = &state_error {
            println!("state error: {error}");
        }
        let state_valid_text = state_valid.map(yes_no).unwrap_or("n/a");
        println!("state valid: {state_valid_text}");
        if !state_issues.is_empty() {
            println!("state issues:");
            for issue in &state_issues {
                println!("- {issue}");
            }
        }
        println!("config path: {}", config_path.display());
        println!("config exists: {}", yes_no(config_exists));
        println!("config loads: {}", yes_no(config_loads));
        let config_valid_text = config_valid.map(yes_no).unwrap_or("n/a");
        println!("config valid: {config_valid_text}");
        if !config_issues.is_empty() {
            println!("config issues:");
            for issue in &config_issues {
                println!("- {issue}");
            }
        }
        if !next_steps.is_empty() {
            println!("next steps:");
            for step in &next_steps {
                println!("- {step}");
            }
        }
    }
    Ok(())
}

fn doctor_platform() -> &'static str {
    std::env::consts::OS
}

fn doctor_service_manager() -> &'static str {
    if cfg!(target_os = "macos") {
        "launchd"
    } else if cfg!(target_os = "linux") {
        "systemd"
    } else {
        "manual"
    }
}

fn doctor_service_recommendation() -> &'static str {
    if cfg!(target_os = "macos") {
        "agent-os service install && agent-os service start"
    } else if cfg!(target_os = "linux") {
        "agent-os service install-systemd && agent-os service start-systemd"
    } else if cfg!(target_os = "windows") {
        "run agent-os daemon run from a supervised terminal or external Windows service wrapper"
    } else {
        "run agent-os daemon run under the local platform supervisor"
    }
}

fn doctor_shell_execution_supported() -> bool {
    cfg!(unix)
}

fn doctor_shell_execution_note() -> &'static str {
    if cfg!(unix) {
        "shell tasks execute with sh -c and can use Unix process-group cancellation when enabled"
    } else {
        "shell tasks require a Unix-like sh; use provider planning, built-in file tools, or an external supervisor on this platform"
    }
}

fn resolve_store(path: Option<std::path::PathBuf>) -> Result<Store> {
    let Some(path) = path else {
        return Ok(Store::from_environment()?);
    };

    if is_sqlite_state_path(&path) {
        return Ok(Store::new_sqlite(path));
    }
    if path.is_dir() {
        Ok(Store::new(path.join("state.json")))
    } else if path.extension().is_some() {
        Ok(Store::new(path))
    } else {
        Ok(Store::new(path.join("state.json")))
    }
}

fn is_sqlite_state_path(path: &std::path::Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "sqlite" | "sqlite3" | "db"
            )
        })
        .unwrap_or(false)
}

fn init(store: Store, config_path: &std::path::Path, args: InitArgs, json: bool) -> Result<()> {
    let profile = args
        .profile
        .as_deref()
        .map(parse_config_profile)
        .transpose()?;
    let config = match (load_config(config_path)?, profile) {
        (Some(mut config), Some(profile)) => {
            config.apply_profile(profile);
            config
        }
        (Some(config), None) => config,
        (None, Some(profile)) => AppConfig::for_profile(profile),
        (None, None) => AppConfig::effective_default(),
    };
    validate_seed_config(&config)?;
    let name = args.name.unwrap_or_else(|| config.name.clone());
    validate_os_name(&name)?;
    let mut os = OperatingSystem::new(name);
    os.policy = config.policy.clone();
    os.provider = config.provider.clone();
    os.memory_policy = config.memory_policy.clone();
    for agent in config.clone().into_agents() {
        if os.agents.contains_key(&agent.id) {
            bail!("duplicate agent id in config: {}", agent.id);
        }
        os.register_agent(agent);
    }
    for tool in config.into_tools() {
        if os.tools.contains_key(&tool.id) {
            bail!("duplicate tool id in config: {}", tool.id);
        }
        os.register_tool(tool);
    }
    os.write_memory(MemoryRecord::new(
        "operating-principles",
        "Prefer explicit plans, durable state, small reviewable tasks, and evented execution.",
        vec!["system".into(), "policy".into()],
    ));
    store.save_validated_checked(&os, args.force)?;
    if json {
        print_json(&serde_json::json!({
            "state_path": store.path(),
            "os": os,
        }))?;
    } else {
        println!("Initialized {} at {}", os.name, store.path().display());
        let state_arg = shell_arg(&store.path().display().to_string());
        println!("Next: agent-os --state {state_arg} status");
        println!(
            "Create a task: agent-os --state {state_arg} task create 'Review README' --need rust"
        );
        println!("Preview scheduling: agent-os --state {state_arg} run --dry-run");
    }
    Ok(())
}

fn validate_seed_config(config: &AppConfig) -> Result<()> {
    validate_seed_config_impl(config).map_err(anyhow::Error::msg)
}

fn handle_config(
    config_path: std::path::PathBuf,
    command: ConfigCommand,
    json: bool,
) -> Result<()> {
    match command {
        ConfigCommand::Init(args) => {
            let profile = parse_config_profile(&args.profile)?;
            write_profile_config(&config_path, args.force, profile)?;
            let config = AppConfig::for_profile(profile);
            if json {
                print_json(&serde_json::json!({
                    "path": config_path.display().to_string(),
                    "written": true,
                    "profile": profile.as_str(),
                    "config": config,
                }))?;
            } else {
                println!(
                    "Wrote {} config {}",
                    profile.as_str(),
                    config_path.display()
                );
                let config_arg = shell_arg(&config_path.display().to_string());
                println!("Next: agent-os --config {config_arg} config validate");
            }
        }
        ConfigCommand::Show => {
            let exists = config_path.exists();
            let config = load_config(&config_path)?.unwrap_or_else(AppConfig::effective_default);
            if json {
                print_json(&serde_json::json!({
                    "path": config_path.display().to_string(),
                    "exists": exists,
                    "config": config,
                }))?;
            } else {
                println!("{}", toml::to_string_pretty(&config)?);
            }
        }
        ConfigCommand::Validate => validate_config_file(&config_path, json)?,
    }
    Ok(())
}

fn parse_config_profile(profile: &str) -> Result<ConfigProfile> {
    ConfigProfile::try_parse(profile).with_context(|| {
        format!(
            "profile must be one of: {}",
            ConfigProfile::VALUES.join(", ")
        )
    })
}

fn validate_config_file(config_path: &std::path::Path, json: bool) -> Result<()> {
    let exists = config_path.exists();
    let (loads, valid, issues, config_error) = if exists {
        match load_config(config_path) {
            Ok(Some(config)) => match validate_seed_config(&config) {
                Ok(()) => (true, Some(true), Vec::new(), None),
                Err(error) => (true, Some(false), vec![error.to_string()], None),
            },
            Ok(None) => (false, None, Vec::new(), None),
            Err(error) => {
                let error = error.to_string();
                (false, Some(false), vec![error.clone()], Some(error))
            }
        }
    } else {
        (false, None, Vec::new(), None)
    };
    let report = serde_json::json!({
        "config_path": config_path.display().to_string(),
        "config_exists": exists,
        "config_loads": loads,
        "config_valid": valid,
        "config_issues": issues.clone(),
        "config_error": config_error,
    });

    if json {
        print_json(&report)?;
    } else {
        println!("config path: {}", config_path.display());
        println!("config exists: {}", yes_no(exists));
        println!("config loads: {}", yes_no(loads));
        println!("config valid: {}", valid.map(yes_no).unwrap_or("n/a"));
        if !issues.is_empty() {
            println!("config issues:");
            for issue in &issues {
                println!("- {issue}");
            }
        }
    }

    if valid == Some(false) {
        bail!("config invalid");
    }
    Ok(())
}

fn handle_state(store: Store, command: StateCommand, json: bool) -> Result<()> {
    match command {
        StateCommand::Export(args) => {
            if let Some(path) = args.output {
                validate_path("output", &path)?;
                let path = if args.dry_run {
                    store.preview_export_to_path(&path)?
                } else {
                    store.export_to_path(&path)?
                };
                if json {
                    print_json(&serde_json::json!({
                        "dry_run": args.dry_run,
                        "exported": path,
                    }))?;
                } else if args.dry_run {
                    println!("Would export state to {}", path.display());
                } else {
                    println!("Exported state to {}", path.display());
                }
            } else if args.dry_run {
                bail!("--dry-run requires --output");
            } else {
                let body = store
                    .export_json()
                    .with_context(|| state_init_hint(&store))?;
                print!("{body}");
            }
        }
        StateCommand::Import(args) => {
            validate_path("import path", &args.path)?;
            let (_os, report) = if args.dry_run {
                store.preview_import_from_path_checked(&args.path, args.force)?
            } else {
                store.import_from_path_checked(&args.path, args.force)?
            };
            if json {
                print_json(&serde_json::json!({
                    "dry_run": args.dry_run,
                    "imported": args.path,
                    "state_path": store.path(),
                    "validation": report,
                }))?;
            } else if args.dry_run {
                println!(
                    "Would import {} into {}",
                    args.path.display(),
                    store.path().display()
                );
            } else {
                println!(
                    "Imported {} into {}",
                    args.path.display(),
                    store.path().display()
                );
            }
        }
        StateCommand::Backup(args) => {
            let output = args.output.unwrap_or_else(|| store.default_backup_path());
            validate_path("output", &output)?;
            let path = if args.dry_run {
                store.preview_backup_to_path(&output)?
            } else {
                store.backup_to_path(&output)?
            };
            if json {
                print_json(&serde_json::json!({
                    "dry_run": args.dry_run,
                    "backup": path,
                }))?;
            } else if args.dry_run {
                println!("Would back up state to {}", path.display());
            } else {
                println!("Backed up state to {}", path.display());
            }
        }
        StateCommand::Migrate(args) => {
            let input = args.input.unwrap_or_else(|| store.path().to_path_buf());
            let output = args.output.unwrap_or_else(|| store.path().to_path_buf());
            validate_path("input", &input)?;
            validate_path("output", &output)?;
            let output_preexisting = output.exists();
            let (report, validation) = if args.dry_run {
                store.preview_migrate_path(&input)?
            } else {
                store.migrate_path_to(&input, &output)?
            };
            if json {
                print_json(&serde_json::json!({
                    "dry_run": args.dry_run,
                    "input": input,
                    "output": output,
                    "output_preexisting": output_preexisting,
                    "migration": report,
                    "validation": validation,
                }))?;
            } else if args.dry_run && report.changed {
                println!(
                    "Would migrate state from version {} to {} at {}",
                    report.from_version,
                    report.to_version,
                    output.display()
                );
                if output_preexisting {
                    println!(
                        "Output exists and would be overwritten: {}",
                        output.display()
                    );
                }
                print_migration_steps(&report);
                print_migration_validation(&validation);
                print_migration_downgrade_notes(&report);
            } else if args.dry_run {
                println!(
                    "State already at version {}; no migration needed.",
                    report.to_version
                );
                print_migration_validation(&validation);
            } else if report.changed {
                println!(
                    "Migrated state from version {} to {} at {}",
                    report.from_version,
                    report.to_version,
                    output.display()
                );
                print_migration_steps(&report);
                print_migration_validation(&validation);
                print_migration_downgrade_notes(&report);
            } else {
                println!(
                    "State already at version {}: {}",
                    report.to_version,
                    output.display()
                );
                print_migration_validation(&validation);
            }
        }
        StateCommand::Prune(args) => {
            let report = store
                .prune(args.keep_runs, args.keep_events, args.dry_run)
                .with_context(|| state_init_hint(&store))?;
            if json {
                print_json(&report)?;
            } else if args.dry_run {
                println!(
                    "Would remove {} run(s), {} log file(s), {} artifact file(s), and {} event(s).",
                    report.removed_runs.len(),
                    report.removed_log_paths.len(),
                    report.removed_artifact_paths.len(),
                    report.removed_events
                );
            } else {
                println!(
                    "Removed {} run(s), {} log file(s), {} artifact file(s), and {} event(s).",
                    report.removed_runs.len(),
                    report.removed_log_paths.len(),
                    report.removed_artifact_paths.len(),
                    report.removed_events
                );
            }
        }
        StateCommand::Repair(args) => {
            let report = if args.dry_run {
                let mut os = store.load().with_context(|| state_init_hint(&store))?;
                repair_state(&mut os)
            } else {
                store
                    .repair_state()
                    .with_context(|| state_init_hint(&store))?
            };
            if json {
                print_json(&serde_json::json!({
                    "dry_run": args.dry_run,
                    "repair": report,
                }))?;
            } else if report.repairs.is_empty() {
                println!("No repairable state issues found.");
            } else if args.dry_run {
                println!("Would repair {} state issue(s):", report.repairs.len());
                for repair in &report.repairs {
                    println!("- {repair}");
                }
            } else if !report.persisted {
                println!(
                    "Found {} repairable state issue(s), but did not persist because state remains invalid:",
                    report.repairs.len()
                );
                for repair in &report.repairs {
                    println!("- {repair}");
                }
            } else {
                println!("Repaired {} state issue(s):", report.repairs.len());
                for repair in &report.repairs {
                    println!("- {repair}");
                }
            }
            if !report.validation.valid {
                bail!(
                    "state still has unrepairable validation issue(s): {}",
                    report.validation.issues.join("; ")
                );
            }
        }
        StateCommand::Sqlite(args) => {
            let output = args
                .output
                .unwrap_or_else(|| store.path().with_extension("sqlite"));
            validate_path("sqlite output", &output)?;
            let sqlite = SqliteStore::new(&output);
            if args.restore {
                let restore = sqlite.restore_json_store(&store, args.force, args.dry_run)?;
                if json {
                    print_json(&serde_json::json!({
                        "path": sqlite.path(),
                        "state_path": store.path(),
                        "dry_run": args.dry_run,
                        "restored": !args.dry_run,
                        "restore": restore,
                    }))?;
                } else if args.dry_run {
                    println!(
                        "Would restore JSON state at {} from SQLite backend {} (runs: {})",
                        store.path().display(),
                        sqlite.path().display(),
                        restore.restored_runs
                    );
                } else {
                    println!(
                        "Restored JSON state at {} from SQLite backend {} (runs: {})",
                        store.path().display(),
                        sqlite.path().display(),
                        restore.restored_runs
                    );
                }
                return Ok(());
            }
            let import = if args.init_only {
                sqlite.init()?;
                None
            } else {
                Some(
                    sqlite
                        .import_json_store(&store)
                        .with_context(|| state_init_hint(&store))?,
                )
            };
            if json {
                print_json(&serde_json::json!({
                    "path": sqlite.path(),
                    "initialized": true,
                    "imported": !args.init_only,
                    "import": import,
                }))?;
            } else if args.init_only {
                println!("Initialized SQLite backend at {}", sqlite.path().display());
            } else if let Some(import) = import {
                println!(
                    "Synced JSON state into SQLite backend at {} (runs: {}, logs: {}, skipped logs: {})",
                    sqlite.path().display(),
                    import.imported_runs,
                    import.imported_run_logs,
                    import.skipped_run_logs
                );
            }
        }
        StateCommand::Validate => {
            let os = store.load().with_context(|| state_init_hint(&store))?;
            let report = validate_state(&os);
            if json {
                print_json(&report)?;
            } else if report.valid {
                println!("State is valid.");
            } else {
                println!("State is invalid:");
                for issue in &report.issues {
                    println!("- {issue}");
                }
            }
            if !report.valid {
                bail!("state validation failed");
            }
        }
    }
    Ok(())
}

fn handle_agent(store: Store, command: AgentCommand, json: bool) -> Result<()> {
    match command {
        AgentCommand::Add(args) => {
            validate_agent_name(&args.name)?;
            validate_agent_kind(&args.kind)?;
            validate_optional_text("agent model", args.model.as_deref())?;
            validate_capability_values("agent capabilities", &args.capabilities, true)?;
            if args.parallel == 0 {
                bail!("parallel must be greater than 0");
            }
            let (id, agent) = store.update(|os| {
                let agent = Agent::new(
                    args.name,
                    AgentKind::parse(&args.kind),
                    args.model,
                    args.capabilities,
                    args.parallel,
                );
                let id = agent.id.clone();
                if os.agents.contains_key(&id) {
                    bail!("agent already exists: {id}");
                }
                os.register_agent(agent);
                let agent = os
                    .agents
                    .get(&id)
                    .cloned()
                    .with_context(|| format!("agent not found after registration: {id}"))?;
                Ok::<_, anyhow::Error>((id, agent))
            })?;
            if json {
                print_json(&serde_json::json!({
                    "id": id,
                    "agent": agent,
                }))?;
            } else {
                println!("Registered agent {}", id);
            }
        }
        AgentCommand::Show(args) => {
            let os = store.load().with_context(|| state_init_hint(&store))?;
            let id = parse_agent_id_arg(&args.id)?;
            let agent = os
                .agents
                .get(&id)
                .with_context(|| format!("agent not found: {id}"))?;
            if json {
                print_json(agent)?;
            } else {
                print_agent_detail(agent);
            }
        }
        AgentCommand::Update(args) => {
            let id = parse_agent_id_arg(&args.id)?;
            if let Some(name) = &args.name {
                validate_agent_name(name)?;
            }
            if let Some(kind) = &args.kind {
                validate_agent_kind(kind)?;
            }
            if args.clear_model && args.model.is_some() {
                bail!("--clear-model cannot be combined with --model");
            }
            validate_optional_text("agent model", args.model.as_deref())?;
            if !args.capabilities.is_empty() {
                validate_capability_values("agent capabilities", &args.capabilities, true)?;
            }
            if let Some(parallel) = args.parallel
                && parallel == 0
            {
                bail!("parallel must be greater than 0");
            }
            if args.name.is_none()
                && args.kind.is_none()
                && args.model.is_none()
                && !args.clear_model
                && args.capabilities.is_empty()
                && args.parallel.is_none()
            {
                bail!("agent update must include at least one field");
            }
            let update = AgentUpdate {
                name: args.name,
                kind: args.kind.as_deref().map(AgentKind::parse),
                model: if args.clear_model {
                    Some(None)
                } else {
                    args.model.map(Some)
                },
                capabilities: if args.capabilities.is_empty() {
                    None
                } else {
                    Some(args.capabilities)
                },
                max_parallel_tasks: args.parallel,
            };
            let agent = store
                .update(|os| Ok::<_, anyhow::Error>(Runtime::update_agent(os, &id, update)?))?;
            if json {
                print_json(&serde_json::json!({
                    "id": id,
                    "agent": agent,
                }))?;
            } else {
                println!("Updated agent {}", id);
            }
        }
        AgentCommand::Heartbeat(args) => {
            let id = parse_agent_id_arg(&args.id)?;
            let status = parse_agent_status(&args.status)?;
            validate_lease_seconds(args.lease_seconds)?;
            let agent = store.update(|os| {
                Runtime::heartbeat_agent(os, &id, status, args.lease_seconds)?;
                os.agents
                    .get(&id)
                    .cloned()
                    .with_context(|| format!("agent not found: {id}"))
            })?;
            if json {
                print_json(&serde_json::json!({
                    "id": id,
                    "agent": agent,
                }))?;
            } else {
                println!("Updated agent {}: {}", agent.id, agent.status);
            }
        }
        AgentCommand::Claim(args) => {
            let id = parse_agent_id_arg(&args.id)?;
            validate_lease_seconds(args.lease_seconds)?;
            let (assignment, task) = store.update(|os| {
                if !os.agents.contains_key(&id) {
                    bail!("agent not found: {id}");
                }
                let lease_seconds = args.lease_seconds.or_else(|| {
                    os.agents
                        .get(&id)
                        .and_then(|agent| agent.lease_expires_at)
                        .and_then(|expires_at| {
                            let remaining = (expires_at - Utc::now()).num_seconds();
                            (remaining > 0).then_some(remaining)
                        })
                });
                Runtime::heartbeat_agent(os, &id, AgentStatus::Online, lease_seconds)?;
                let assignment = Scheduler::assign_next_for_agent(os, &id);
                let task = assignment
                    .as_ref()
                    .and_then(|assignment| os.tasks.get(&assignment.task_id))
                    .cloned();
                Ok::<_, anyhow::Error>((assignment, task))
            })?;
            if json {
                print_json(&serde_json::json!({
                    "claimed": assignment.is_some(),
                    "assignment": assignment,
                    "task": task,
                }))?;
            } else if let (Some(assignment), Some(task)) = (assignment, task) {
                println!(
                    "Claimed task {} for {}: {}",
                    assignment.task_id, assignment.agent_id, task.title
                );
            } else {
                println!("No runnable tasks found for agent {id}.");
            }
        }
        AgentCommand::List(args) => {
            validate_agent_list_limit(args.limit)?;
            let status = args.status.as_deref().map(parse_agent_status).transpose()?;
            let kind = args
                .kind
                .as_deref()
                .map(parse_agent_kind_filter)
                .transpose()?;
            let capabilities = capability_filter("agent capability filter", &args.capabilities)?;
            let since = args
                .since
                .as_deref()
                .map(|value| parse_filter_timestamp(value, "agent", "since"))
                .transpose()?;
            let until = args
                .until
                .as_deref()
                .map(|value| parse_filter_timestamp(value, "agent", "until"))
                .transpose()?;
            if let Some(query) = &args.query {
                validate_agent_query(query)?;
            }
            let query = args.query.as_ref().map(|query| query.to_ascii_lowercase());
            let os = store.load().with_context(|| state_init_hint(&store))?;
            let mut agents = os
                .agents
                .values()
                .filter(|agent| {
                    status
                        .as_ref()
                        .map(|status| &agent.status == status)
                        .unwrap_or(true)
                        && kind
                            .as_ref()
                            .map(|kind| &agent.kind == kind)
                            .unwrap_or(true)
                        && has_all_capabilities(&agent.capabilities, &capabilities)
                        && since
                            .as_ref()
                            .map(|since| agent.updated_at >= *since)
                            .unwrap_or(true)
                        && until
                            .as_ref()
                            .map(|until| agent.updated_at <= *until)
                            .unwrap_or(true)
                        && query
                            .as_ref()
                            .map(|query| agent_matches_query(agent, query))
                            .unwrap_or(true)
                })
                .collect::<Vec<_>>();
            agents.sort_by_key(|agent| std::cmp::Reverse(agent.updated_at));
            if let Some(limit) = args.limit {
                agents.truncate(limit);
            }
            if json {
                print_json(&agents)?;
            } else {
                print_agents(agents);
            }
        }
        AgentCommand::Remove(args) => {
            let id = parse_agent_id_arg(&args.id)?;
            let removed = store.update(|os| {
                if !os.agents.contains_key(&id) {
                    bail!("agent not found: {id}");
                }
                let blockers = os.agent_removal_blockers(&id);
                if !blockers.is_empty() {
                    bail!(
                        "agent {id} is still referenced; mark it offline instead or clear references first: {}",
                        blockers.join("; ")
                    );
                }
                os.remove_agent(&id)
                    .with_context(|| format!("agent not found: {id}"))
            })?;
            if json {
                print_json(&serde_json::json!({
                    "id": id,
                    "removed": true,
                    "agent": removed,
                }))?;
            } else {
                println!("Removed agent {}", id);
            }
        }
    }
    Ok(())
}

fn handle_task(store: Store, command: TaskCommand, json: bool) -> Result<()> {
    match command {
        TaskCommand::Create(args) => {
            validate_task_title(&args.title)?;
            if let Some(objective) = &args.objective {
                validate_task_objective(objective)?;
            }
            if let Some(command) = &args.command {
                validate_task_command(command)?;
            }
            validate_optional_text("task cwd", args.cwd.as_deref())?;
            validate_capability_values(
                "task required capabilities",
                &args.required_capabilities,
                false,
            )?;
            if matches!(args.max_attempts, Some(0)) {
                bail!("task max_attempts must be greater than 0");
            }
            if let Some(tool_id) = &args.tool {
                validate_tool_id(tool_id)?;
            }
            if args.tool.is_none() && !args.tool_args.is_empty() {
                bail!("--arg requires --tool");
            }
            if args.tool.is_none() && !args.secret_tool_args.is_empty() {
                bail!("--secret-arg requires --tool");
            }
            let priority = parse_priority(&args.priority)?;
            let (id, task) = store.update(|os| {
                if args.command.is_some() && args.tool.is_some() {
                    bail!("task cannot define both --command and --tool");
                }
                let title = args.title;
                let objective = args.objective.clone().unwrap_or_else(|| title.clone());
                let mut task = Task::new(title, objective, priority, args.required_capabilities);
                os.ensure_unique_task_id(&mut task);
                if let Some(max_attempts) = args.max_attempts {
                    task.max_attempts = max_attempts;
                }
                task.command = args.command;
                task.cwd = args.cwd;
                if let Some(tool_id) = args.tool {
                    let tool_id = ToolId::new(tool_id);
                    let tool = os
                        .tools
                        .get(&tool_id)
                        .with_context(|| format!("tool not found: {}", tool_id))?;
                    if task.required_capabilities.is_empty() {
                        task.required_capabilities = tool.required_capabilities.clone();
                    }
                    let tool_args = parse_key_values(args.tool_args)?;
                    reject_secret_like_plain_args(&tool_args, &os.policy.redacted_env_patterns)?;
                    let secret_tool_args = parse_key_values(args.secret_tool_args)?;
                    validate_secret_env_args(&secret_tool_args)?;
                    for key in secret_tool_args.keys() {
                        if tool_args.contains_key(key) {
                            bail!("tool argument `{key}` cannot be both --arg and --secret-arg");
                        }
                    }
                    let invocation =
                        ToolInvocation::with_secret_env_args(tool_id, tool_args, secret_tool_args);
                    validate_tool_invocation(tool, &invocation)?;
                    task.tool = Some(invocation);
                }
                task.dependencies = validate_task_dependencies(os, args.dependencies)?;
                let id = task.id.clone();
                os.create_task(task);
                let task = os
                    .tasks
                    .get(&id)
                    .cloned()
                    .with_context(|| format!("task not found after creation: {id}"))?;
                Ok::<_, anyhow::Error>((id, task))
            })?;
            if json {
                print_json(&serde_json::json!({
                    "id": id,
                    "task": task,
                }))?;
            } else {
                println!("Created task {}", id);
            }
        }
        TaskCommand::List(args) => {
            validate_task_list_limit(args.limit)?;
            let status = args.status.as_deref().map(parse_task_status).transpose()?;
            let priority = args.priority.as_deref().map(parse_priority).transpose()?;
            let agent = args.agent.as_deref().map(parse_agent_id_arg).transpose()?;
            let tool = args.tool.as_deref().map(parse_tool_id_arg).transpose()?;
            let dependency = args
                .dependency
                .as_deref()
                .map(parse_task_id_arg)
                .transpose()?;
            let capabilities = capability_filter("task capability filter", &args.capabilities)?;
            let since = args
                .since
                .as_deref()
                .map(|value| parse_filter_timestamp(value, "task", "since"))
                .transpose()?;
            let until = args
                .until
                .as_deref()
                .map(|value| parse_filter_timestamp(value, "task", "until"))
                .transpose()?;
            if let Some(query) = &args.query {
                validate_task_query(query)?;
            }
            let query = args.query.as_ref().map(|query| query.to_ascii_lowercase());
            let os = store.load().with_context(|| state_init_hint(&store))?;
            let mut tasks = os
                .tasks
                .values()
                .filter(|task| {
                    let status_matches = if let Some(status) = &status {
                        &task.status == status
                    } else {
                        args.all
                            || !matches!(
                                task.status,
                                TaskStatus::Complete | TaskStatus::Failed | TaskStatus::Cancelled
                            )
                    };
                    let priority_matches = priority
                        .as_ref()
                        .map(|priority| &task.priority == priority)
                        .unwrap_or(true);
                    let agent_matches = agent
                        .as_ref()
                        .map(|agent| task.assigned_to.as_ref() == Some(agent))
                        .unwrap_or(true);
                    let tool_matches = tool
                        .as_ref()
                        .map(|tool| {
                            task.tool
                                .as_ref()
                                .map(|invocation| &invocation.tool_id == tool)
                                .unwrap_or(false)
                        })
                        .unwrap_or(true);
                    let dependency_matches = dependency
                        .as_ref()
                        .map(|dependency| {
                            task.dependencies
                                .iter()
                                .any(|task_id| task_id == dependency)
                        })
                        .unwrap_or(true);
                    status_matches
                        && priority_matches
                        && agent_matches
                        && tool_matches
                        && dependency_matches
                        && since
                            .as_ref()
                            .map(|since| task.updated_at >= *since)
                            .unwrap_or(true)
                        && until
                            .as_ref()
                            .map(|until| task.updated_at <= *until)
                            .unwrap_or(true)
                        && query
                            .as_ref()
                            .map(|query| task_matches_query(task, query))
                            .unwrap_or(true)
                        && has_all_capabilities(&task.required_capabilities, &capabilities)
                })
                .collect::<Vec<_>>();
            tasks.sort_by_key(|task| std::cmp::Reverse(task.updated_at));
            if let Some(limit) = args.limit {
                tasks.truncate(limit);
            }
            if json {
                print_json(&tasks)?;
            } else {
                print_tasks(tasks);
            }
        }
        TaskCommand::Show(args) => {
            let os = store.load().with_context(|| state_init_hint(&store))?;
            let id = parse_task_id_arg(&args.id)?;
            let task = os
                .tasks
                .get(&id)
                .with_context(|| format!("task not found: {}", id))?;
            if json {
                print_json(task)?;
            } else {
                print_task_detail(task);
            }
        }
        TaskCommand::Update(args) => {
            let id = parse_task_id_arg(&args.id)?;
            if let Some(title) = &args.title {
                validate_task_title(title)?;
            }
            if let Some(objective) = &args.objective {
                validate_task_objective(objective)?;
            }
            if args.clear_command && args.command.is_some() {
                bail!("--clear-command cannot be combined with --command");
            }
            if let Some(command) = &args.command {
                validate_task_command(command)?;
            }
            if args.clear_tool && args.tool.is_some() {
                bail!("--clear-tool cannot be combined with --tool");
            }
            if args.clear_tool
                && (!args.tool_args.is_empty()
                    || !args.secret_tool_args.is_empty()
                    || args.clear_args
                    || args.clear_secret_args)
            {
                bail!("--clear-tool cannot be combined with tool argument updates");
            }
            if args.clear_args && !args.tool_args.is_empty() {
                bail!("--clear-args cannot be combined with --arg");
            }
            if args.clear_secret_args && !args.secret_tool_args.is_empty() {
                bail!("--clear-secret-args cannot be combined with --secret-arg");
            }
            if let Some(tool_id) = &args.tool {
                validate_tool_id(tool_id)?;
            }
            if args.clear_cwd && args.cwd.is_some() {
                bail!("--clear-cwd cannot be combined with --cwd");
            }
            validate_optional_text("task cwd", args.cwd.as_deref())?;
            if args.clear_needs && !args.required_capabilities.is_empty() {
                bail!("--clear-needs cannot be combined with --need");
            }
            if !args.required_capabilities.is_empty() {
                validate_capability_values(
                    "task required capabilities",
                    &args.required_capabilities,
                    false,
                )?;
            }
            if matches!(args.max_attempts, Some(0)) {
                bail!("task max_attempts must be greater than 0");
            }
            if args.title.is_none()
                && args.objective.is_none()
                && args.command.is_none()
                && !args.clear_command
                && args.tool.is_none()
                && !args.clear_tool
                && args.tool_args.is_empty()
                && args.secret_tool_args.is_empty()
                && !args.clear_args
                && !args.clear_secret_args
                && args.cwd.is_none()
                && !args.clear_cwd
                && args.required_capabilities.is_empty()
                && !args.clear_needs
                && args.max_attempts.is_none()
            {
                bail!("task update must include at least one field");
            }
            let plain_arg_update = if args.clear_args {
                Some(BTreeMap::new())
            } else if args.tool_args.is_empty() {
                None
            } else {
                Some(parse_key_values(args.tool_args)?)
            };
            let secret_arg_update = if args.clear_secret_args {
                Some(BTreeMap::new())
            } else if args.secret_tool_args.is_empty() {
                None
            } else {
                let values = parse_key_values(args.secret_tool_args)?;
                validate_secret_env_args(&values)?;
                Some(values)
            };
            let tool_id_update = args.tool.map(ToolId::new);
            let task = store.update(|os| {
                let tool_update = if args.clear_tool {
                    Some(None)
                } else if let Some(tool_id) = tool_id_update {
                    let tool = os
                        .tools
                        .get(&tool_id)
                        .with_context(|| format!("tool not found: {}", tool_id))?;
                    let tool_args = plain_arg_update.unwrap_or_default();
                    reject_secret_like_plain_args(&tool_args, &os.policy.redacted_env_patterns)?;
                    let secret_tool_args = secret_arg_update.unwrap_or_default();
                    for key in secret_tool_args.keys() {
                        if tool_args.contains_key(key) {
                            bail!("tool argument `{key}` cannot be both --arg and --secret-arg");
                        }
                    }
                    let invocation =
                        ToolInvocation::with_secret_env_args(tool_id, tool_args, secret_tool_args);
                    validate_tool_invocation(tool, &invocation)?;
                    Some(Some(invocation))
                } else if plain_arg_update.is_some() || secret_arg_update.is_some() {
                    let existing = os
                        .tasks
                        .get(&id)
                        .with_context(|| format!("task not found: {id}"))?
                        .tool
                        .clone()
                        .with_context(|| format!("task has no tool invocation: {id}"))?;
                    let tool = os
                        .tools
                        .get(&existing.tool_id)
                        .with_context(|| format!("tool not found: {}", existing.tool_id))?;
                    let mut invocation = existing;
                    if let Some(args) = plain_arg_update {
                        reject_secret_like_plain_args(&args, &os.policy.redacted_env_patterns)?;
                        invocation.args = args;
                    }
                    if let Some(secret_args) = secret_arg_update {
                        invocation.secret_env_args = secret_args;
                    }
                    for key in invocation.secret_env_args.keys() {
                        if invocation.args.contains_key(key) {
                            bail!("tool argument `{key}` cannot be both --arg and --secret-arg");
                        }
                    }
                    validate_tool_invocation(tool, &invocation)?;
                    Some(Some(invocation))
                } else {
                    None
                };
                let update = TaskUpdate {
                    title: args.title,
                    objective: args.objective,
                    command: if args.clear_command {
                        Some(None)
                    } else {
                        args.command.map(Some)
                    },
                    tool: tool_update,
                    cwd: if args.clear_cwd {
                        Some(None)
                    } else {
                        args.cwd.map(Some)
                    },
                    required_capabilities: if args.clear_needs {
                        Some(Vec::new())
                    } else if args.required_capabilities.is_empty() {
                        None
                    } else {
                        Some(args.required_capabilities)
                    },
                    max_attempts: args.max_attempts,
                };
                Ok::<_, anyhow::Error>(Runtime::update_task(os, &id, update)?)
            })?;
            if json {
                print_json(&serde_json::json!({
                    "id": id,
                    "task": task,
                }))?;
            } else {
                println!("Updated task {}", id);
            }
        }
        TaskCommand::Assign(args) => {
            let id = parse_task_id_arg(&args.id)?;
            let agent_id = parse_agent_id_arg(&args.agent)?;
            let (assignment, task, agent) = store.update(|os| {
                let assignment = Runtime::assign_task(os, &id, &agent_id)?;
                let task = os
                    .tasks
                    .get(&id)
                    .cloned()
                    .with_context(|| format!("task not found: {id}"))?;
                let agent = os
                    .agents
                    .get(&agent_id)
                    .cloned()
                    .with_context(|| format!("agent not found: {agent_id}"))?;
                Ok::<_, anyhow::Error>((assignment, task, agent))
            })?;
            if json {
                print_json(&serde_json::json!({
                    "assignment": assignment,
                    "task": task,
                    "agent": agent,
                }))?;
            } else {
                println!("Assigned task {} to {}", id, agent_id);
            }
        }
        TaskCommand::Priority(args) => {
            let id = parse_task_id_arg(&args.id)?;
            let priority = parse_priority(&args.priority)?;
            let task = store.update(|os| {
                Runtime::reprioritize_task(os, &id, priority)?;
                os.tasks
                    .get(&id)
                    .cloned()
                    .with_context(|| format!("task not found: {id}"))
            })?;
            if json {
                print_task_mutation_json(&id, &task)?;
            } else {
                println!("Set task {} priority to {}", id, task.priority);
            }
        }
        TaskCommand::Dependencies(args) => {
            if args.clear && !args.dependencies.is_empty() {
                bail!("use either --clear or --after, not both");
            }
            if !args.clear && args.dependencies.is_empty() {
                bail!("provide at least one --after dependency or use --clear");
            }
            let id = parse_task_id_arg(&args.id)?;
            let dependencies = if args.clear {
                Vec::new()
            } else {
                validate_task_dependencies_for_task(Some(&id), args.dependencies)?
            };
            let task = store.update(|os| {
                for dependency in &dependencies {
                    if !os.tasks.contains_key(dependency) {
                        bail!("dependency task not found: {}", dependency);
                    }
                }
                Runtime::set_task_dependencies(os, &id, dependencies)?;
                os.tasks
                    .get(&id)
                    .cloned()
                    .with_context(|| format!("task not found: {id}"))
            })?;
            if json {
                print_task_mutation_json(&id, &task)?;
            } else if task.dependencies.is_empty() {
                println!("Cleared dependencies for task {}", id);
            } else {
                println!("Updated dependencies for task {}", id);
            }
        }
        TaskCommand::Plan(args) => {
            validate_plan_steps(&args.steps)?;
            let id = parse_task_id_arg(&args.id)?;
            let task = store.update(|os| {
                Ok::<_, anyhow::Error>(Runtime::set_task_plan(os, &id, args.steps)?)
            })?;
            if json {
                print_task_mutation_json(&id, &task)?;
            } else {
                println!("Updated plan for task {}", id);
            }
        }
        TaskCommand::Complete(args) => {
            validate_optional_text("note", args.note.as_deref())?;
            let id = parse_task_id_arg(&args.id)?;
            let task = store.update(|os| {
                Runtime::complete_task(os, &id, args.note)?;
                os.tasks
                    .get(&id)
                    .cloned()
                    .with_context(|| format!("task not found: {id}"))
            })?;
            if json {
                print_task_mutation_json(&id, &task)?;
            } else {
                println!("Completed task {}", id);
            }
        }
        TaskCommand::Fail(args) => {
            validate_optional_text("note", args.note.as_deref())?;
            let id = parse_task_id_arg(&args.id)?;
            let task = store.update(|os| {
                Runtime::fail_task(os, &id, args.note)?;
                os.tasks
                    .get(&id)
                    .cloned()
                    .with_context(|| format!("task not found: {id}"))
            })?;
            if json {
                print_task_mutation_json(&id, &task)?;
            } else {
                println!("Failed task {}", id);
            }
        }
        TaskCommand::Block(args) => {
            validate_optional_text("note", args.note.as_deref())?;
            let id = parse_task_id_arg(&args.id)?;
            let task = store.update(|os| {
                Runtime::block_task(os, &id, args.note)?;
                os.tasks
                    .get(&id)
                    .cloned()
                    .with_context(|| format!("task not found: {id}"))
            })?;
            if json {
                print_task_mutation_json(&id, &task)?;
            } else {
                println!("Blocked task {}", id);
            }
        }
        TaskCommand::Cancel(args) => {
            validate_optional_text("note", args.note.as_deref())?;
            let id = parse_task_id_arg(&args.id)?;
            let task = store.update(|os| {
                Runtime::cancel_task(os, &id, args.note)?;
                os.tasks
                    .get(&id)
                    .cloned()
                    .with_context(|| format!("task not found: {id}"))
            })?;
            if json {
                print_task_mutation_json(&id, &task)?;
            } else {
                println!("Cancelled task {}", id);
            }
        }
        TaskCommand::Retry(args) => {
            validate_optional_text("note", args.note.as_deref())?;
            let id = parse_task_id_arg(&args.id)?;
            let task = store.update(|os| {
                Runtime::retry_task(os, &id, args.note)?;
                os.tasks
                    .get(&id)
                    .cloned()
                    .with_context(|| format!("task not found: {id}"))
            })?;
            if json {
                print_task_mutation_json(&id, &task)?;
            } else {
                println!("Retried task {}", id);
            }
        }
        TaskCommand::Unblock(args) => {
            validate_optional_text("note", args.note.as_deref())?;
            let id = parse_task_id_arg(&args.id)?;
            let task = store.update(|os| {
                Runtime::unblock_task(os, &id, args.note)?;
                os.tasks
                    .get(&id)
                    .cloned()
                    .with_context(|| format!("task not found: {id}"))
            })?;
            if json {
                print_task_mutation_json(&id, &task)?;
            } else {
                println!("Unblocked task {}", id);
            }
        }
        TaskCommand::Delete(args) => {
            let id = parse_task_id_arg(&args.id)?;
            store.update(|os| {
                Runtime::delete_task(os, &id)?;
                Ok::<_, anyhow::Error>(())
            })?;
            if json {
                print_json(&serde_json::json!({
                    "id": id,
                    "deleted": true,
                }))?;
            } else {
                println!("Deleted task {}", id);
            }
        }
        TaskCommand::Recover(args) => {
            if args.older_than_seconds < 0 {
                bail!("older_than_seconds must be greater than or equal to 0");
            }
            let recovery = store.update(|os| {
                Ok::<_, anyhow::Error>(Runtime::recover_stale_state(
                    os,
                    ChronoDuration::seconds(args.older_than_seconds),
                ))
            })?;
            if json {
                print_json(&serde_json::json!({
                    "older_than_seconds": args.older_than_seconds,
                    "recovered": recovery.recovered_tasks,
                    "recovered_runs": recovery.recovered_runs,
                    "recovered_daemon": recovery.recovered_daemon,
                    "notes": recovery.notes,
                }))?;
            } else if recovery.recovered_tasks.is_empty()
                && recovery.recovered_runs.is_empty()
                && !recovery.recovered_daemon
            {
                println!("No stale running tasks found.");
            } else {
                println!(
                    "Recovered {} stale running task(s), {} active run record(s), daemon recovered: {}.",
                    recovery.recovered_tasks.len(),
                    recovery.recovered_runs.len(),
                    yes_no(recovery.recovered_daemon)
                );
                for task_id in recovery.recovered_tasks {
                    println!("{}", task_id);
                }
            }
        }
    }
    Ok(())
}

fn handle_tool(store: Store, command: ToolCommand, json: bool) -> Result<()> {
    match command {
        ToolCommand::Add(args) => {
            validate_tool_name(&args.name)?;
            validate_tool_command_template(&args.command_template)?;
            validate_capability_values(
                "tool required capabilities",
                &args.required_capabilities,
                false,
            )?;
            validate_optional_text("tool cwd", args.cwd.as_deref())?;
            let kind = parse_tool_kind(&args.kind)?;
            let (id, tool) = store.update(|os| {
                let tool = ToolDefinition::new(
                    args.name,
                    kind,
                    args.description,
                    args.required_capabilities,
                    args.command_template,
                    args.cwd,
                );
                validate_tool_template(&tool)?;
                let id = tool.id.clone();
                if os.tools.contains_key(&id) {
                    bail!("tool already exists: {id}");
                }
                os.register_tool(tool);
                let tool = os
                    .tools
                    .get(&id)
                    .cloned()
                    .with_context(|| format!("tool not found after registration: {id}"))?;
                Ok::<_, anyhow::Error>((id, tool))
            })?;
            if json {
                print_json(&serde_json::json!({
                    "id": id,
                    "tool": tool,
                }))?;
            } else {
                println!("Registered tool {}", id);
            }
        }
        ToolCommand::List(args) => {
            validate_tool_list_limit(args.limit)?;
            let kind = args.kind.as_deref().map(parse_tool_kind).transpose()?;
            let capabilities = capability_filter("tool capability filter", &args.capabilities)?;
            let since = args
                .since
                .as_deref()
                .map(|value| parse_filter_timestamp(value, "tool", "since"))
                .transpose()?;
            let until = args
                .until
                .as_deref()
                .map(|value| parse_filter_timestamp(value, "tool", "until"))
                .transpose()?;
            if let Some(query) = &args.query {
                validate_tool_query(query)?;
            }
            let query = args.query.as_ref().map(|query| query.to_ascii_lowercase());
            let os = store.load().with_context(|| state_init_hint(&store))?;
            let mut tools = os
                .tools
                .values()
                .filter(|tool| {
                    kind.as_ref().map(|kind| &tool.kind == kind).unwrap_or(true)
                        && has_all_capabilities(&tool.required_capabilities, &capabilities)
                        && since
                            .as_ref()
                            .map(|since| tool.updated_at >= *since)
                            .unwrap_or(true)
                        && until
                            .as_ref()
                            .map(|until| tool.updated_at <= *until)
                            .unwrap_or(true)
                        && query
                            .as_ref()
                            .map(|query| tool_matches_query(tool, query))
                            .unwrap_or(true)
                })
                .collect::<Vec<_>>();
            tools.sort_by_key(|tool| std::cmp::Reverse(tool.updated_at));
            if let Some(limit) = args.limit {
                tools.truncate(limit);
            }
            if json {
                print_json(&tools)?;
            } else {
                print_tools(tools);
            }
        }
        ToolCommand::Show(args) => {
            let os = store.load().with_context(|| state_init_hint(&store))?;
            let id = parse_tool_id_arg(&args.id)?;
            let tool = os
                .tools
                .get(&id)
                .with_context(|| format!("tool not found: {}", id))?;
            if json {
                print_json(tool)?;
            } else {
                print_tool_detail(tool);
            }
        }
        ToolCommand::Update(args) => {
            let id = parse_tool_id_arg(&args.id)?;
            if args.clear_description && args.description.is_some() {
                bail!("--clear-description cannot be combined with --description");
            }
            if args.clear_needs && !args.required_capabilities.is_empty() {
                bail!("--clear-needs cannot be combined with --need");
            }
            if args.clear_cwd && args.cwd.is_some() {
                bail!("--clear-cwd cannot be combined with --cwd");
            }
            if args.kind.is_none()
                && args.description.is_none()
                && !args.clear_description
                && args.required_capabilities.is_empty()
                && !args.clear_needs
                && args.command_template.is_none()
                && args.cwd.is_none()
                && !args.clear_cwd
            {
                bail!("tool update must include at least one field");
            }
            let kind = args.kind.as_deref().map(parse_tool_kind).transpose()?;
            if let Some(command_template) = &args.command_template {
                validate_tool_command_template(command_template)?;
            }
            if !args.required_capabilities.is_empty() {
                validate_capability_values(
                    "tool required capabilities",
                    &args.required_capabilities,
                    false,
                )?;
            }
            validate_optional_text("tool cwd", args.cwd.as_deref())?;
            let update = ToolUpdate {
                kind,
                description: if args.clear_description {
                    Some(String::new())
                } else {
                    args.description
                },
                required_capabilities: if args.clear_needs {
                    Some(Vec::new())
                } else if args.required_capabilities.is_empty() {
                    None
                } else {
                    Some(args.required_capabilities)
                },
                command_template: args.command_template,
                default_cwd: if args.clear_cwd {
                    Some(None)
                } else {
                    args.cwd.map(Some)
                },
            };
            let tool = store
                .update(|os| Ok::<_, anyhow::Error>(Runtime::update_tool(os, &id, update)?))?;
            if json {
                print_json(&serde_json::json!({
                    "id": id,
                    "tool": tool,
                }))?;
            } else {
                println!("Updated tool {}", id);
            }
        }
        ToolCommand::Remove(args) => {
            let id = parse_tool_id_arg(&args.id)?;
            let removed = store.update(|os| {
                if !os.tools.contains_key(&id) {
                    bail!("tool not found: {id}");
                }
                let blockers = os.tool_removal_blockers(&id);
                if !blockers.is_empty() {
                    bail!(
                        "tool {id} is still referenced; delete or rewrite referencing tasks first: {}",
                        blockers.join("; ")
                    );
                }
                os.remove_tool(&id)
                    .with_context(|| format!("tool not found: {}", id))
            })?;
            if json {
                print_json(&serde_json::json!({
                    "id": id,
                    "removed": true,
                    "tool": removed,
                }))?;
            } else {
                println!("Removed tool {}", id);
            }
        }
    }
    Ok(())
}

fn handle_memory(store: Store, command: MemoryCommand, json: bool) -> Result<()> {
    match command {
        MemoryCommand::Add(args) => {
            validate_memory_topic(&args.topic)?;
            validate_memory_body(&args.body)?;
            validate_tag_values("memory tags", &args.tags)?;
            let visibility = parse_memory_visibility(&args.visibility)?;
            if let Some(scope) = &args.scope {
                validate_memory_scope(scope)?;
            }
            let (id, record) = store.update(|os| {
                let record = MemoryRecord::with_access(
                    args.topic, args.body, args.tags, visibility, args.scope,
                );
                let id = record.id.clone();
                os.write_memory(record);
                let record = os
                    .memory
                    .iter()
                    .find(|record| record.id == id)
                    .cloned()
                    .with_context(|| format!("memory record not found after write: {id}"))?;
                Ok::<_, anyhow::Error>((id, record))
            })?;
            if json {
                print_json(&serde_json::json!({
                    "id": id,
                    "memory": record,
                }))?;
            } else {
                println!("Stored memory {}", id);
            }
        }
        MemoryCommand::Search(args) => {
            validate_memory_query(&args.query)?;
            validate_memory_limit(args.limit)?;
            let tags = tag_filter("memory tag filter", &args.tags)?;
            let visibility = args
                .visibility
                .as_deref()
                .map(parse_memory_visibility)
                .transpose()?;
            if let Some(scope) = &args.scope {
                validate_memory_scope(scope)?;
            }
            let since = args
                .since
                .as_deref()
                .map(|value| parse_filter_timestamp(value, "memory", "since"))
                .transpose()?;
            let until = args
                .until
                .as_deref()
                .map(|value| parse_filter_timestamp(value, "memory", "until"))
                .transpose()?;
            let os = store.load().with_context(|| state_init_hint(&store))?;
            let mut matches = os
                .memory
                .iter()
                .filter_map(|record| {
                    let score = memory_relevance_score(record, &args.query);
                    (score > 0
                        && has_all_tags(&record.tags, &tags)
                        && visibility
                            .as_ref()
                            .map(|visibility| record.visibility == *visibility)
                            .unwrap_or(true)
                        && args
                            .scope
                            .as_ref()
                            .map(|scope| record.scope.as_deref() == Some(scope.as_str()))
                            .unwrap_or(true)
                        && since
                            .as_ref()
                            .map(|since| record.updated_at >= *since)
                            .unwrap_or(true)
                        && until
                            .as_ref()
                            .map(|until| record.updated_at <= *until)
                            .unwrap_or(true))
                    .then_some((score, record.clone()))
                })
                .collect::<Vec<_>>();
            matches.sort_by(|(left_score, left), (right_score, right)| {
                right_score
                    .cmp(left_score)
                    .then_with(|| right.updated_at.cmp(&left.updated_at))
            });
            if let Some(limit) = args.limit {
                matches.truncate(limit);
            }
            let matches = matches
                .into_iter()
                .map(|(_, record)| record)
                .collect::<Vec<_>>();
            if json {
                print_json(&matches)?;
            } else {
                print_memory(&matches);
            }
        }
        MemoryCommand::Recall(args) => {
            validate_memory_query(&args.query)?;
            validate_memory_limit(args.limit)?;
            let tags = tag_filter("memory tag filter", &args.tags)?;
            let visibility = args
                .visibility
                .as_deref()
                .map(parse_memory_visibility)
                .transpose()?;
            if let Some(scope) = &args.scope {
                validate_memory_scope(scope)?;
            }
            let since = args
                .since
                .as_deref()
                .map(|value| parse_filter_timestamp(value, "memory", "since"))
                .transpose()?;
            let until = args
                .until
                .as_deref()
                .map(|value| parse_filter_timestamp(value, "memory", "until"))
                .transpose()?;
            let os = store.load().with_context(|| state_init_hint(&store))?;
            let mut hits = os
                .memory
                .iter()
                .filter_map(|record| {
                    let score = memory_relevance_score(record, &args.query);
                    (score > 0
                        && has_all_tags(&record.tags, &tags)
                        && visibility
                            .as_ref()
                            .map(|visibility| record.visibility == *visibility)
                            .unwrap_or(true)
                        && args
                            .scope
                            .as_ref()
                            .map(|scope| record.scope.as_deref() == Some(scope.as_str()))
                            .unwrap_or(true)
                        && since
                            .as_ref()
                            .map(|since| record.updated_at >= *since)
                            .unwrap_or(true)
                        && until
                            .as_ref()
                            .map(|until| record.updated_at <= *until)
                            .unwrap_or(true))
                    .then(|| memory_recall_hit(record, &args.query, score))
                })
                .collect::<Vec<_>>();
            hits.sort_by(|left, right| {
                right
                    .score
                    .cmp(&left.score)
                    .then_with(|| right.record.updated_at.cmp(&left.record.updated_at))
            });
            if let Some(limit) = args.limit {
                hits.truncate(limit);
            }
            if json {
                print_json(&hits)?;
            } else {
                print_memory_recall(&hits);
            }
        }
        MemoryCommand::List(args) => {
            validate_memory_limit(args.limit)?;
            let tags = tag_filter("memory tag filter", &args.tags)?;
            let visibility = args
                .visibility
                .as_deref()
                .map(parse_memory_visibility)
                .transpose()?;
            if let Some(scope) = &args.scope {
                validate_memory_scope(scope)?;
            }
            let since = args
                .since
                .as_deref()
                .map(|value| parse_filter_timestamp(value, "memory", "since"))
                .transpose()?;
            let until = args
                .until
                .as_deref()
                .map(|value| parse_filter_timestamp(value, "memory", "until"))
                .transpose()?;
            let os = store.load().with_context(|| state_init_hint(&store))?;
            let mut records = os
                .memory
                .iter()
                .filter(|record| {
                    has_all_tags(&record.tags, &tags)
                        && visibility
                            .as_ref()
                            .map(|visibility| record.visibility == *visibility)
                            .unwrap_or(true)
                        && args
                            .scope
                            .as_ref()
                            .map(|scope| record.scope.as_deref() == Some(scope.as_str()))
                            .unwrap_or(true)
                        && since
                            .as_ref()
                            .map(|since| record.updated_at >= *since)
                            .unwrap_or(true)
                        && until
                            .as_ref()
                            .map(|until| record.updated_at <= *until)
                            .unwrap_or(true)
                })
                .cloned()
                .collect::<Vec<_>>();
            records.sort_by_key(|record| std::cmp::Reverse(record.updated_at));
            if let Some(limit) = args.limit {
                records.truncate(limit);
            }
            if json {
                print_json(&records)?;
            } else {
                print_memory(&records);
            }
        }
        MemoryCommand::Show(args) => {
            validate_memory_id(&args.id)?;
            let os = store.load().with_context(|| state_init_hint(&store))?;
            let record = os
                .memory
                .iter()
                .find(|record| record.id == args.id)
                .with_context(|| format!("memory not found: {}", args.id))?;
            if json {
                print_json(record)?;
            } else {
                print_memory_detail(record);
            }
        }
        MemoryCommand::Update(args) => {
            validate_memory_id(&args.id)?;
            if let Some(topic) = &args.topic {
                validate_memory_topic(topic)?;
            }
            if let Some(body) = &args.body {
                validate_memory_body(body)?;
            }
            if args.clear_tags && !args.tags.is_empty() {
                bail!("use either --clear-tags or --tag, not both");
            }
            if args.clear_scope && args.scope.is_some() {
                bail!("use either --clear-scope or --scope, not both");
            }
            validate_tag_values("memory tags", &args.tags)?;
            let visibility = args
                .visibility
                .as_deref()
                .map(parse_memory_visibility)
                .transpose()?;
            if let Some(scope) = &args.scope {
                validate_memory_scope(scope)?;
            }
            if args.topic.is_none()
                && args.body.is_none()
                && args.tags.is_empty()
                && !args.clear_tags
                && visibility.is_none()
                && args.scope.is_none()
                && !args.clear_scope
            {
                bail!(
                    "provide --topic, --body, --tag, --clear-tags, --visibility, --scope, or --clear-scope"
                );
            }
            let tags = if args.clear_tags {
                Some(Vec::new())
            } else if args.tags.is_empty() {
                None
            } else {
                Some(args.tags)
            };
            let scope = if args.clear_scope {
                Some(None)
            } else {
                args.scope.map(Some)
            };
            let record = store.update(|os| {
                os.update_memory(&args.id, args.topic, args.body, tags, visibility, scope)
                    .with_context(|| format!("memory not found: {}", args.id))
            })?;
            if json {
                print_json(&serde_json::json!({
                    "id": args.id,
                    "memory": record,
                }))?;
            } else {
                println!("Updated memory {}", args.id);
            }
        }
        MemoryCommand::Remove(args) => {
            validate_memory_id(&args.id)?;
            let removed = store.update(|os| {
                os.remove_memory(&args.id)
                    .with_context(|| format!("memory not found: {}", args.id))
            })?;
            if json {
                print_json(&serde_json::json!({
                    "id": args.id,
                    "removed": true,
                    "memory": removed,
                }))?;
            } else {
                println!("Removed memory {}", args.id);
            }
        }
        MemoryCommand::Prune(args) => {
            let max_age_days = resolve_memory_max_age_days(&store, args.max_age_days)?;
            if args.dry_run {
                let os = store.load().with_context(|| state_init_hint(&store))?;
                let expired = os.expired_memory(Utc::now(), max_age_days);
                if json {
                    print_json(&serde_json::json!({
                        "dry_run": true,
                        "max_age_days": max_age_days,
                        "removed": [],
                        "expired": expired,
                    }))?;
                } else {
                    println!("Would prune {} expired memory record(s).", expired.len());
                }
            } else {
                let removed = store.update(|os| {
                    Ok::<_, anyhow::Error>(os.prune_expired_memory(Utc::now(), max_age_days))
                })?;
                if json {
                    print_json(&serde_json::json!({
                        "dry_run": false,
                        "max_age_days": max_age_days,
                        "removed": removed,
                        "expired": [],
                    }))?;
                } else {
                    println!("Pruned {} expired memory record(s).", removed.len());
                }
            }
        }
    }
    Ok(())
}

fn resolve_memory_max_age_days(store: &Store, explicit: Option<u64>) -> Result<u64> {
    if matches!(explicit, Some(0)) {
        bail!("memory max_age_days must be greater than 0");
    }
    if let Some(max_age_days) = explicit {
        return Ok(max_age_days);
    }
    let os = store.load().with_context(|| state_init_hint(&store))?;
    os.memory_policy
        .max_age_days
        .filter(|days| *days > 0)
        .with_context(|| "memory max_age_days is not configured; pass --max-age-days")
}

fn handle_registry(store: Store, command: RegistryCommand, json: bool) -> Result<()> {
    match command {
        RegistryCommand::List => {
            let os = store.load().with_context(|| state_init_hint(&store))?;
            if json {
                print_json(&serde_json::json!({
                    "agent_profiles": os.agent_profiles,
                    "workflow_templates": os.workflow_templates,
                    "mcp_servers": os.mcp_servers,
                    "tools": os.tools,
                    "secrets_backends": os.secrets_backends,
                }))?;
            } else {
                print_registry(&os);
            }
        }
        RegistryCommand::Profiles => {
            let os = store.load().with_context(|| state_init_hint(&store))?;
            if json {
                print_json(&os.agent_profiles)?;
            } else {
                print_registry_rows(
                    os.agent_profiles
                        .values()
                        .map(|profile| RegistryItemRow {
                            id: profile.id.clone(),
                            kind: "agent-profile".into(),
                            name: profile.name.clone(),
                            detail: profile.capabilities.join(", "),
                        })
                        .collect(),
                );
            }
        }
        RegistryCommand::Profile(args) => {
            validate_registry_id("profile id", &args.id)?;
            let os = store.load().with_context(|| state_init_hint(&store))?;
            let profile = os
                .agent_profiles
                .get(&args.id)
                .with_context(|| format!("agent profile not found: {}", args.id))?;
            if json {
                print_json(profile)?;
            } else {
                println!("{} {}", profile.id, profile.name);
                println!("kind: {}", profile.kind);
                println!("model: {}", profile.model.as_deref().unwrap_or("-"));
                println!("capabilities: {}", display_list(&profile.capabilities));
                if let Some(prompt) = &profile.system_prompt {
                    println!("prompt: {}", prompt);
                }
            }
        }
        RegistryCommand::InstallAgent(args) => install_registry_agent(store, args, json)?,
        RegistryCommand::Templates => {
            let os = store.load().with_context(|| state_init_hint(&store))?;
            if json {
                print_json(&os.workflow_templates)?;
            } else {
                print_registry_rows(
                    os.workflow_templates
                        .values()
                        .map(|template| RegistryItemRow {
                            id: template.id.clone(),
                            kind: "workflow-template".into(),
                            name: template.name.clone(),
                            detail: template.stages.join(" -> "),
                        })
                        .collect(),
                );
            }
        }
        RegistryCommand::Template(args) => {
            validate_registry_id("template id", &args.id)?;
            let os = store.load().with_context(|| state_init_hint(&store))?;
            let template = os
                .workflow_templates
                .get(&args.id)
                .with_context(|| format!("workflow template not found: {}", args.id))?;
            if json {
                print_json(template)?;
            } else {
                println!("{} {}", template.id, template.name);
                println!("description: {}", template.description);
                println!("stages: {}", template.stages.join(" -> "));
            }
        }
        RegistryCommand::CreateWorkflow(args) => create_registry_workflow(store, args, json)?,
        RegistryCommand::McpList => {
            let os = store.load().with_context(|| state_init_hint(&store))?;
            if json {
                print_json(&os.mcp_servers)?;
            } else {
                print_registry_rows(
                    os.mcp_servers
                        .values()
                        .map(|server| RegistryItemRow {
                            id: server.id.clone(),
                            kind: "mcp-server".into(),
                            name: if server.enabled {
                                "enabled".into()
                            } else {
                                "disabled".into()
                            },
                            detail: format!("{} {}", server.command, server.args.join(" ")),
                        })
                        .collect(),
                );
            }
        }
        RegistryCommand::McpAdd(args) => add_mcp_server(store, args, json)?,
        RegistryCommand::McpEnable(args) => set_mcp_enabled(store, args, true, json)?,
        RegistryCommand::McpDisable(args) => set_mcp_enabled(store, args, false, json)?,
        RegistryCommand::McpRemove(args) => remove_mcp_server(store, args, json)?,
        RegistryCommand::MarketplaceImport(args) => import_marketplace_manifest(store, args, json)?,
    }
    Ok(())
}

fn import_marketplace_manifest(
    store: Store,
    args: RegistryMarketplaceImportArgs,
    json: bool,
) -> Result<()> {
    validate_path("marketplace manifest path", &args.path)?;
    let body = std::fs::read_to_string(&args.path)
        .with_context(|| format!("read marketplace manifest {}", args.path.display()))?;
    let checksum = fnv1a64_checksum(body.as_bytes());
    if let Some(expected) = &args.expect_checksum {
        validate_optional_text("marketplace checksum", Some(expected))?;
        if expected != &checksum {
            bail!("marketplace checksum mismatch: expected {expected}, got {checksum}");
        }
    }
    let mut manifest: MarketplaceManifest = serde_json::from_str(&body)
        .with_context(|| format!("parse marketplace manifest {}", args.path.display()))?;
    validate_marketplace_manifest(&mut manifest)?;
    let source = args.path.display().to_string();
    let manifest_id = manifest
        .metadata
        .as_ref()
        .map(|metadata| metadata.id.clone());
    let manifest_version = manifest
        .metadata
        .as_ref()
        .map(|metadata| metadata.version.clone());
    let imported_agent_profiles = manifest.agent_profiles.len();
    let imported_workflow_templates = manifest.workflow_templates.len();
    let imported_mcp_servers = manifest.mcp_servers.len();
    let report = store.update(|os| {
        let mut overwritten = 0usize;
        for profile in manifest.agent_profiles {
            if os.agent_profiles.contains_key(&profile.id) && !args.force {
                bail!("agent profile already exists: {}", profile.id);
            }
            if os
                .agent_profiles
                .insert(profile.id.clone(), profile)
                .is_some()
            {
                overwritten += 1;
            }
        }
        for template in manifest.workflow_templates {
            if os.workflow_templates.contains_key(&template.id) && !args.force {
                bail!("workflow template already exists: {}", template.id);
            }
            if os
                .workflow_templates
                .insert(template.id.clone(), template)
                .is_some()
            {
                overwritten += 1;
            }
        }
        for server in manifest.mcp_servers {
            if os.mcp_servers.contains_key(&server.id) && !args.force {
                bail!("mcp server already exists: {}", server.id);
            }
            if os.mcp_servers.insert(server.id.clone(), server).is_some() {
                overwritten += 1;
            }
        }
        let report = MarketplaceImportReport {
            source: source.clone(),
            checksum: checksum.clone(),
            verified_checksum: args.expect_checksum.is_some(),
            manifest_id: manifest_id.clone(),
            manifest_version: manifest_version.clone(),
            imported_agent_profiles,
            imported_workflow_templates,
            imported_mcp_servers,
            overwritten,
        };
        os.record(
            EventKind::MarketplaceImported,
            format!(
                "imported marketplace {} profiles, {} templates, {} mcp servers from {} ({})",
                report.imported_agent_profiles,
                report.imported_workflow_templates,
                report.imported_mcp_servers,
                report.source,
                report.checksum
            ),
        );
        Ok::<_, anyhow::Error>(report)
    })?;
    if json {
        print_json(&report)?;
    } else {
        println!(
            "Imported marketplace manifest {} checksum={} (profiles: {}, templates: {}, mcp servers: {}, overwritten: {})",
            report.source,
            report.checksum,
            report.imported_agent_profiles,
            report.imported_workflow_templates,
            report.imported_mcp_servers,
            report.overwritten
        );
    }
    Ok(())
}

fn validate_marketplace_manifest(manifest: &mut MarketplaceManifest) -> Result<()> {
    if let Some(metadata) = &manifest.metadata {
        validate_registry_id("marketplace manifest id", &metadata.id)?;
        validate_optional_text("marketplace manifest version", Some(&metadata.version))?;
        validate_optional_text(
            "marketplace manifest publisher",
            metadata.publisher.as_deref(),
        )?;
        validate_optional_text(
            "marketplace manifest homepage",
            metadata.homepage.as_deref(),
        )?;
    }

    let mut profile_ids = BTreeSet::new();
    for profile in &mut manifest.agent_profiles {
        validate_registry_id("marketplace agent profile id", &profile.id)?;
        if !profile_ids.insert(profile.id.clone()) {
            bail!("duplicate marketplace agent profile id: {}", profile.id);
        }
        validate_agent_name(&profile.name)?;
        validate_optional_text("marketplace agent profile model", profile.model.as_deref())?;
        validate_optional_text(
            "marketplace agent profile system_prompt",
            profile.system_prompt.as_deref(),
        )?;
        validate_capability_values(
            "marketplace agent profile capabilities",
            &profile.capabilities,
            true,
        )?;
        profile.capabilities = normalize_list(profile.capabilities.clone());
    }

    let mut template_ids = BTreeSet::new();
    for template in &mut manifest.workflow_templates {
        validate_registry_id("marketplace workflow template id", &template.id)?;
        if !template_ids.insert(template.id.clone()) {
            bail!(
                "duplicate marketplace workflow template id: {}",
                template.id
            );
        }
        validate_optional_text("marketplace workflow template name", Some(&template.name))?;
        validate_optional_text(
            "marketplace workflow template description",
            Some(&template.description),
        )?;
        validate_workflow_template_definition(template)?;
    }

    let mut server_ids = BTreeSet::new();
    for server in &manifest.mcp_servers {
        validate_registry_id("marketplace MCP server id", &server.id)?;
        if !server_ids.insert(server.id.clone()) {
            bail!("duplicate marketplace MCP server id: {}", server.id);
        }
        validate_task_command(&server.command)?;
        for arg in &server.args {
            validate_optional_text("marketplace MCP argument", Some(arg))?;
        }
        for key in server.env.keys() {
            validate_env_var_name("marketplace MCP environment key", key)?;
        }
    }

    if manifest.agent_profiles.is_empty()
        && manifest.workflow_templates.is_empty()
        && manifest.mcp_servers.is_empty()
    {
        bail!("marketplace manifest must include at least one registry entry");
    }
    Ok(())
}

fn validate_workflow_template_definition(template: &mut WorkflowTemplate) -> Result<()> {
    if template.stages.is_empty() {
        bail!(
            "marketplace workflow template {} has no stages",
            template.id
        );
    }
    let mut stages = BTreeSet::new();
    for stage in &template.stages {
        validate_workflow_stage(stage)?;
        if !stages.insert(stage.clone()) {
            bail!(
                "marketplace workflow template {} has duplicate stage {}",
                template.id,
                stage
            );
        }
    }

    let mut task_stages = BTreeSet::new();
    for task in &mut template.tasks {
        validate_workflow_stage(&task.stage)?;
        if !stages.contains(&task.stage) {
            bail!(
                "marketplace workflow template {} task references unknown stage {}",
                template.id,
                task.stage
            );
        }
        if !task_stages.insert(task.stage.clone()) {
            bail!(
                "marketplace workflow template {} has duplicate task metadata for stage {}",
                template.id,
                task.stage
            );
        }
        validate_optional_text(
            "marketplace workflow template task title",
            task.title.as_deref(),
        )?;
        validate_optional_text(
            "marketplace workflow template task objective",
            task.objective.as_deref(),
        )?;
        if let Some(command) = &task.command {
            validate_task_command(command)?;
        }
        validate_capability_values(
            "marketplace workflow template task capabilities",
            &task.capabilities,
            true,
        )?;
        task.capabilities = normalize_list(task.capabilities.clone());
    }

    let mut edges = BTreeSet::new();
    for edge in &template.edges {
        validate_workflow_template_edge(template, edge, &stages, &mut edges)?;
    }
    if workflow_template_has_cycle(&template.stages, &workflow_template_edges(template)) {
        bail!(
            "marketplace workflow template {} has a dependency cycle",
            template.id
        );
    }
    Ok(())
}

fn validate_workflow_template_edge(
    template: &WorkflowTemplate,
    edge: &WorkflowTemplateEdge,
    stages: &BTreeSet<String>,
    edges: &mut BTreeSet<(String, String)>,
) -> Result<()> {
    validate_workflow_stage(&edge.from)?;
    validate_workflow_stage(&edge.to)?;
    if edge.from == edge.to {
        bail!(
            "marketplace workflow template {} edge cannot point to itself: {}",
            template.id,
            edge.from
        );
    }
    if !stages.contains(&edge.from) {
        bail!(
            "marketplace workflow template {} edge references unknown stage {}",
            template.id,
            edge.from
        );
    }
    if !stages.contains(&edge.to) {
        bail!(
            "marketplace workflow template {} edge references unknown stage {}",
            template.id,
            edge.to
        );
    }
    if !edges.insert((edge.from.clone(), edge.to.clone())) {
        bail!(
            "marketplace workflow template {} has duplicate edge {} -> {}",
            template.id,
            edge.from,
            edge.to
        );
    }
    Ok(())
}

fn workflow_template_has_cycle(stages: &[String], edges: &[WorkflowTemplateEdge]) -> bool {
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    stages
        .iter()
        .any(|stage| workflow_template_visit_has_cycle(stage, edges, &mut visiting, &mut visited))
}

fn workflow_template_visit_has_cycle(
    stage: &str,
    edges: &[WorkflowTemplateEdge],
    visiting: &mut BTreeSet<String>,
    visited: &mut BTreeSet<String>,
) -> bool {
    if visited.contains(stage) {
        return false;
    }
    if !visiting.insert(stage.to_owned()) {
        return true;
    }
    for edge in edges.iter().filter(|edge| edge.from == stage) {
        if workflow_template_visit_has_cycle(&edge.to, edges, visiting, visited) {
            return true;
        }
    }
    visiting.remove(stage);
    visited.insert(stage.to_owned());
    false
}

fn install_registry_agent(store: Store, args: RegistryInstallAgentArgs, json: bool) -> Result<()> {
    validate_registry_id("profile id", &args.profile)?;
    validate_optional_text("agent model", args.model.as_deref())?;
    if let Some(name) = &args.name {
        validate_agent_name(name)?;
    }
    if args.parallel == 0 {
        bail!("agent parallel must be greater than 0");
    }
    let (id, agent) = store.update(|os| {
        let profile = os
            .agent_profiles
            .get(&args.profile)
            .cloned()
            .with_context(|| format!("agent profile not found: {}", args.profile))?;
        let name = args.name.unwrap_or(profile.name);
        let mut agent = Agent::new(
            name,
            profile.kind,
            args.model.or(profile.model),
            profile.capabilities,
            args.parallel,
        );
        if os.agents.contains_key(&agent.id) {
            bail!("agent already exists: {}", agent.id);
        }
        let id = agent.id.clone();
        os.register_agent(agent);
        agent = os
            .agents
            .get(&id)
            .cloned()
            .with_context(|| format!("agent not found after registration: {id}"))?;
        Ok::<_, anyhow::Error>((id, agent))
    })?;
    if json {
        print_json(&serde_json::json!({
            "id": id,
            "agent": agent,
        }))?;
    } else {
        println!("Installed agent {} from profile {}", id, args.profile);
    }
    Ok(())
}

fn create_registry_workflow(
    store: Store,
    args: RegistryCreateWorkflowArgs,
    json: bool,
) -> Result<()> {
    validate_registry_id("template id", &args.template)?;
    validate_workflow_objective(&args.objective)?;
    let priority = parse_priority(&args.priority)?;
    let (workflow, tasks) = store.update(|os| {
        let template = os
            .workflow_templates
            .get(&args.template)
            .cloned()
            .with_context(|| format!("workflow template not found: {}", args.template))?;
        if template.stages.is_empty() {
            bail!("workflow template {} has no stages", template.id);
        }
        let mut task_map = BTreeMap::new();
        let mut pending_tasks = BTreeMap::new();
        for stage in &template.stages {
            validate_workflow_stage(stage)?;
            let task_spec = workflow_template_task(&template, stage);
            let title = task_spec
                .and_then(|task| task.title.as_ref())
                .map(|title| render_workflow_template_text(title, &args.objective))
                .unwrap_or_else(|| format!("{}: {}", stage, args.objective));
            let objective = task_spec
                .and_then(|task| task.objective.as_ref())
                .map(|objective| render_workflow_template_text(objective, &args.objective))
                .unwrap_or_else(|| format!("Run template stage `{stage}` for: {}", args.objective));
            let capabilities = task_spec
                .map(|task| task.capabilities.clone())
                .unwrap_or_default();
            let mut task = Task::new(title, objective, priority, capabilities);
            task.command = task_spec
                .and_then(|task| task.command.as_ref())
                .map(|command| render_workflow_template_text(command, &args.objective));
            os.ensure_unique_task_id(&mut task);
            let task_id = task.id.clone();
            task_map.insert(stage.clone(), task_id);
            pending_tasks.insert(stage.clone(), task);
        }
        for edge in workflow_template_edges(&template) {
            let Some(from_id) = task_map.get(&edge.from).cloned() else {
                bail!(
                    "workflow template {} edge references unknown stage {}",
                    template.id,
                    edge.from
                );
            };
            let Some(task) = pending_tasks.get_mut(&edge.to) else {
                bail!(
                    "workflow template {} edge references unknown stage {}",
                    template.id,
                    edge.to
                );
            };
            task.dependencies.push(from_id);
        }
        for task in pending_tasks.into_values() {
            os.create_task(task);
        }
        let mut workflow = Workflow::new(args.objective, priority, task_map.clone());
        os.ensure_unique_workflow_id(&mut workflow);
        let workflow_id = workflow.id.clone();
        os.create_workflow(workflow);
        let workflow = os
            .workflows
            .get(&workflow_id)
            .cloned()
            .with_context(|| format!("workflow not found after creation: {workflow_id}"))?;
        Ok::<_, anyhow::Error>((workflow, task_map))
    })?;
    if json {
        print_json(&serde_json::json!({
            "id": workflow.id,
            "workflow": workflow,
            "tasks": tasks,
            "template": args.template,
        }))?;
    } else {
        println!(
            "Created workflow {} from template {}",
            workflow.id, args.template
        );
    }
    Ok(())
}

fn add_mcp_server(store: Store, args: RegistryMcpAddArgs, json: bool) -> Result<()> {
    validate_registry_id("mcp server id", &args.id)?;
    validate_task_command(&args.command)?;
    for arg in &args.args {
        validate_optional_text("mcp argument", Some(arg))?;
    }
    let env = parse_env_values(args.env)?;
    let server = store.update(|os| {
        if os.mcp_servers.contains_key(&args.id) {
            bail!("mcp server already exists: {}", args.id);
        }
        let server = McpServer {
            id: args.id.clone(),
            command: args.command,
            args: args.args,
            env,
            enabled: !args.disabled,
        };
        os.register_mcp_server(server.clone());
        Ok::<_, anyhow::Error>(server)
    })?;
    if json {
        print_json(&server)?;
    } else {
        println!("Registered MCP server {}", server.id);
    }
    Ok(())
}

fn set_mcp_enabled(store: Store, args: RegistryIdArgs, enabled: bool, json: bool) -> Result<()> {
    validate_registry_id("mcp server id", &args.id)?;
    let server = store.update(|os| {
        let server = os
            .mcp_servers
            .get_mut(&args.id)
            .with_context(|| format!("mcp server not found: {}", args.id))?;
        server.enabled = enabled;
        let server = server.clone();
        os.record(
            EventKind::McpServerUpdated,
            format!(
                "{} mcp server {}",
                if enabled { "enabled" } else { "disabled" },
                args.id
            ),
        );
        Ok::<_, anyhow::Error>(server)
    })?;
    if json {
        print_json(&server)?;
    } else {
        println!(
            "{} MCP server {}",
            if enabled { "Enabled" } else { "Disabled" },
            server.id
        );
    }
    Ok(())
}

fn remove_mcp_server(store: Store, args: RegistryIdArgs, json: bool) -> Result<()> {
    validate_registry_id("mcp server id", &args.id)?;
    let server = store.update(|os| {
        let server = os
            .mcp_servers
            .remove(&args.id)
            .with_context(|| format!("mcp server not found: {}", args.id))?;
        os.record(
            EventKind::McpServerRemoved,
            format!("removed mcp server {}", args.id),
        );
        Ok::<_, anyhow::Error>(server)
    })?;
    if json {
        print_json(&serde_json::json!({
            "id": args.id,
            "removed": true,
            "mcp_server": server,
        }))?;
    } else {
        println!("Removed MCP server {}", server.id);
    }
    Ok(())
}

fn validate_registry_id(field: &str, value: &str) -> Result<()> {
    if !contains_slug_character(value) {
        bail!("{field} must contain at least one ASCII letter, digit, or hyphen");
    }
    Ok(())
}

fn parse_env_values(values: Vec<String>) -> Result<BTreeMap<String, String>> {
    let mut parsed = BTreeMap::new();
    for value in values {
        let Some((key, value)) = value.split_once('=') else {
            bail!("environment entry must use KEY=VALUE syntax: {}", value);
        };
        validate_env_var_name("environment key", key)?;
        if parsed.insert(key.to_owned(), value.to_owned()).is_some() {
            bail!("duplicate environment key: {key}");
        }
    }
    Ok(parsed)
}

fn print_registry(os: &OperatingSystem) {
    let mut rows = Vec::new();
    rows.extend(os.agent_profiles.values().map(|profile| RegistryItemRow {
        id: profile.id.clone(),
        kind: "agent-profile".into(),
        name: profile.name.clone(),
        detail: profile.capabilities.join(", "),
    }));
    rows.extend(
        os.workflow_templates
            .values()
            .map(|template| RegistryItemRow {
                id: template.id.clone(),
                kind: "workflow-template".into(),
                name: template.name.clone(),
                detail: template.stages.join(" -> "),
            }),
    );
    rows.extend(os.mcp_servers.values().map(|server| RegistryItemRow {
        id: server.id.clone(),
        kind: "mcp-server".into(),
        name: if server.enabled {
            "enabled".into()
        } else {
            "disabled".into()
        },
        detail: format!("{} {}", server.command, server.args.join(" ")),
    }));
    print_registry_rows(rows);
}

fn print_registry_rows(rows: Vec<RegistryItemRow>) {
    if rows.is_empty() {
        println!("No registry entries");
    } else {
        println!("{}", Table::new(rows).with(Style::rounded()));
    }
}

fn handle_worker(store: Store, command: WorkerCommand, json: bool) -> Result<()> {
    match command {
        WorkerCommand::List(args) => list_workers(store, args, json),
        WorkerCommand::Register(args) => register_worker(store, args, json),
        WorkerCommand::Show(args) => show_worker(store, args, json),
        WorkerCommand::Heartbeat(args) => heartbeat_worker(store, args, json),
        WorkerCommand::Claim(args) => claim_worker_task(store, args, json),
        WorkerCommand::Report(args) => report_worker_task(store, args, json),
        WorkerCommand::Remove(args) => remove_worker(store, args, json),
    }
}

fn list_workers(store: Store, args: WorkerListArgs, json: bool) -> Result<()> {
    validate_worker_list_limit(args.limit)?;
    let status = args.status.as_deref().map(parse_agent_status).transpose()?;
    let since = args
        .since
        .as_deref()
        .map(|value| parse_filter_timestamp(value, "worker", "since"))
        .transpose()?;
    let until = args
        .until
        .as_deref()
        .map(|value| parse_filter_timestamp(value, "worker", "until"))
        .transpose()?;
    if let Some(query) = &args.query {
        validate_worker_query(query)?;
    }
    let query = args.query.as_ref().map(|query| query.to_ascii_lowercase());
    let os = store.load().with_context(|| state_init_hint(&store))?;
    let mut workers = os
        .workers
        .values()
        .filter(|worker| {
            status
                .as_ref()
                .map(|status| &worker.status == status)
                .unwrap_or(true)
                && since
                    .as_ref()
                    .map(|since| worker.last_seen_at >= *since)
                    .unwrap_or(true)
                && until
                    .as_ref()
                    .map(|until| worker.last_seen_at <= *until)
                    .unwrap_or(true)
                && query
                    .as_ref()
                    .map(|query| worker_matches_query(worker, query))
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    workers.sort_by(|left, right| {
        right
            .last_seen_at
            .cmp(&left.last_seen_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    if let Some(limit) = args.limit {
        workers.truncate(limit);
    }
    if json {
        print_json(&workers)?;
    } else {
        print_table(
            workers
                .into_iter()
                .map(|worker| WorkerRow {
                    id: worker.id.clone(),
                    status: worker.status.to_string(),
                    endpoint: truncate(&worker.endpoint, 80),
                    last_seen: worker.last_seen_at.to_rfc3339(),
                })
                .collect(),
        );
    }
    Ok(())
}

fn register_worker(store: Store, args: WorkerRegisterArgs, json: bool) -> Result<()> {
    validate_registry_id("worker id", &args.id)?;
    validate_optional_text("worker endpoint", Some(&args.endpoint))?;
    let status = parse_agent_status(&args.status)?;
    let worker = store.update(|os| {
        if os.workers.contains_key(&args.id) {
            bail!("worker already exists: {}", args.id);
        }
        let worker = WorkerNode {
            id: args.id.clone(),
            endpoint: args.endpoint,
            status,
            last_seen_at: Utc::now(),
        };
        os.register_worker(worker.clone());
        Ok::<_, anyhow::Error>(worker)
    })?;
    if json {
        print_json(&worker)?;
    } else {
        println!("Registered worker {}", worker.id);
    }
    Ok(())
}

fn show_worker(store: Store, args: WorkerIdArgs, json: bool) -> Result<()> {
    validate_registry_id("worker id", &args.id)?;
    let os = store.load().with_context(|| state_init_hint(&store))?;
    let worker = os
        .workers
        .get(&args.id)
        .with_context(|| format!("worker not found: {}", args.id))?;
    if json {
        print_json(worker)?;
    } else {
        println!("{} {}", worker.id, worker.status);
        println!("endpoint: {}", worker.endpoint);
        println!("last seen: {}", worker.last_seen_at.to_rfc3339());
    }
    Ok(())
}

fn heartbeat_worker(store: Store, args: WorkerHeartbeatArgs, json: bool) -> Result<()> {
    validate_registry_id("worker id", &args.id)?;
    if let Some(endpoint) = &args.endpoint {
        validate_optional_text("worker endpoint", Some(endpoint))?;
    }
    validate_lease_seconds(args.lease_seconds)?;
    let status = args.status.as_deref().map(parse_agent_status).transpose()?;
    let worker = store.update(|os| {
        Runtime::heartbeat_worker(os, &args.id, args.endpoint, status, args.lease_seconds)
            .map_err(anyhow::Error::from)
    })?;
    if json {
        print_json(&worker)?;
    } else {
        println!("Updated worker {}", worker.id);
    }
    Ok(())
}

fn claim_worker_task(store: Store, args: WorkerClaimArgs, json: bool) -> Result<()> {
    validate_registry_id("worker id", &args.id)?;
    validate_lease_seconds(args.lease_seconds)?;
    let worker_id = args.id.clone();
    let agent_id = AgentId::new(&worker_id);
    let (worker, assignment, task) = store.update(|os| {
        {
            let worker = os
                .workers
                .get_mut(&worker_id)
                .with_context(|| format!("worker not found: {worker_id}"))?;
            worker.status = AgentStatus::Online;
            worker.last_seen_at = Utc::now();
        }
        if !os.agents.contains_key(&agent_id) {
            bail!("matching agent not found for worker: {agent_id}");
        }
        let lease_seconds = args.lease_seconds.or_else(|| {
            os.agents
                .get(&agent_id)
                .and_then(|agent| agent.lease_expires_at)
                .and_then(|expires_at| {
                    let remaining = (expires_at - Utc::now()).num_seconds();
                    (remaining > 0).then_some(remaining)
                })
        });
        Runtime::heartbeat_agent(os, &agent_id, AgentStatus::Online, lease_seconds)?;
        let assignment = Scheduler::assign_next_for_agent(os, &agent_id);
        let task = assignment
            .as_ref()
            .and_then(|assignment| os.tasks.get(&assignment.task_id))
            .cloned();
        let worker = os
            .workers
            .get(&worker_id)
            .cloned()
            .with_context(|| format!("worker not found: {worker_id}"))?;
        os.record(
            EventKind::WorkerUpdated,
            format!("worker {} claimed task via agent {}", worker.id, agent_id),
        );
        Ok::<_, anyhow::Error>((worker, assignment, task))
    })?;
    if json {
        print_json(&serde_json::json!({
            "id": worker.id,
            "worker": worker,
            "claimed": assignment.is_some(),
            "assignment": assignment,
            "task": task,
        }))?;
    } else if let (Some(assignment), Some(task)) = (assignment, task) {
        println!(
            "Claimed task {} for worker {} via agent {}: {}",
            assignment.task_id, worker.id, assignment.agent_id, task.title
        );
    } else {
        println!("No runnable tasks found for worker {}.", worker.id);
    }
    Ok(())
}

fn report_worker_task(store: Store, args: WorkerReportArgs, json: bool) -> Result<()> {
    validate_registry_id("worker id", &args.id)?;
    validate_optional_text("note", args.note.as_deref())?;
    validate_optional_text("worker command", args.command.as_deref())?;
    validate_optional_text("worker cwd", args.cwd.as_deref())?;
    let task_id = parse_task_id_arg(&args.task_id)?;
    let status = parse_worker_report_status(&args.status)?;
    let artifacts = parse_worker_report_artifacts(args.artifacts)?;
    let (worker, task, run) = store.update(|os| {
        Runtime::report_worker_task(
            os,
            &args.id,
            &task_id,
            status,
            args.note,
            args.command,
            args.cwd,
            args.exit_code,
            artifacts,
        )
        .map_err(anyhow::Error::from)
    })?;
    if json {
        print_json(&serde_json::json!({
            "id": worker.id,
            "worker": worker,
            "task": task,
            "run": run,
            "reported": true,
        }))?;
    } else {
        println!(
            "Reported task {} as {} for worker {} (run {})",
            task.id, task.status, worker.id, run.id
        );
    }
    Ok(())
}

fn remove_worker(store: Store, args: WorkerIdArgs, json: bool) -> Result<()> {
    validate_registry_id("worker id", &args.id)?;
    let worker = store.update(|os| {
        let worker = os
            .workers
            .remove(&args.id)
            .with_context(|| format!("worker not found: {}", args.id))?;
        os.record(
            EventKind::WorkerRemoved,
            format!("removed worker {}", args.id),
        );
        Ok::<_, anyhow::Error>(worker)
    })?;
    if json {
        print_json(&serde_json::json!({
            "id": args.id,
            "removed": true,
            "worker": worker,
        }))?;
    } else {
        println!("Removed worker {}", worker.id);
    }
    Ok(())
}

fn handle_eval(store: Store, command: EvalCommand, json: bool) -> Result<()> {
    match command {
        EvalCommand::List(args) => list_evals(store, args, json),
        EvalCommand::Show(args) => show_eval(store, args, json),
        EvalCommand::Record(args) => record_eval(store, args, json),
        EvalCommand::Run(args) => run_eval(store, args, json),
    }
}

fn list_evals(store: Store, args: EvalListArgs, json: bool) -> Result<()> {
    validate_eval_list_limit(args.limit)?;
    if let Some(target) = &args.target {
        validate_optional_text("eval target", Some(target))?;
    }
    let since = args
        .since
        .as_deref()
        .map(|value| parse_filter_timestamp(value, "eval", "since"))
        .transpose()?;
    let until = args
        .until
        .as_deref()
        .map(|value| parse_filter_timestamp(value, "eval", "until"))
        .transpose()?;
    if let Some(query) = &args.query {
        validate_eval_query(query)?;
    }
    let query = args.query.as_ref().map(|query| query.to_ascii_lowercase());
    let os = store.load().with_context(|| state_init_hint(&store))?;
    let mut evals = os
        .evals
        .iter()
        .filter(|record| {
            args.target
                .as_ref()
                .map(|target| &record.target == target)
                .unwrap_or(true)
                && args
                    .success
                    .map(|success| record.success == success)
                    .unwrap_or(true)
                && since
                    .as_ref()
                    .map(|since| record.recorded_at >= *since)
                    .unwrap_or(true)
                && until
                    .as_ref()
                    .map(|until| record.recorded_at <= *until)
                    .unwrap_or(true)
                && query
                    .as_ref()
                    .map(|query| eval_matches_query(record, query))
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    evals.sort_by(|left, right| {
        right
            .recorded_at
            .cmp(&left.recorded_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    if let Some(limit) = args.limit {
        evals.truncate(limit);
    }
    if json {
        print_json(&evals)?;
    } else {
        print_table(
            evals
                .into_iter()
                .map(|record| EvalRow {
                    id: record.id.clone(),
                    target: record.target.clone(),
                    success: yes_no(record.success).into(),
                    cost_micros: record
                        .cost_micros
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".into()),
                    latency_ms: record
                        .latency_ms
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".into()),
                    recorded: record.recorded_at.to_rfc3339(),
                })
                .collect(),
        );
    }
    Ok(())
}

fn show_eval(store: Store, args: EvalIdArgs, json: bool) -> Result<()> {
    validate_registry_id("eval id", &args.id)?;
    let os = store.load().with_context(|| state_init_hint(&store))?;
    let record = os
        .evals
        .iter()
        .find(|record| record.id == args.id)
        .with_context(|| format!("eval not found: {}", args.id))?;
    if json {
        print_json(record)?;
    } else {
        println!("{} {}", record.id, record.target);
        println!("success: {}", yes_no(record.success));
        println!(
            "cost micros: {}",
            record
                .cost_micros
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into())
        );
        println!(
            "latency ms: {}",
            record
                .latency_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into())
        );
        println!("recorded: {}", record.recorded_at.to_rfc3339());
        if let Some(run) = &record.run {
            println!("command: {}", run.command);
            println!("cwd: {}", run.cwd);
            println!(
                "status: {}",
                run.status
                    .map(|status| status.to_string())
                    .unwrap_or_else(|| "-".into())
            );
            println!("timed out: {}", yes_no(run.timed_out));
            println!(
                "success pattern matched: {}",
                yes_no(run.success_pattern_matched)
            );
        }
    }
    Ok(())
}

fn record_eval(store: Store, args: EvalRecordArgs, json: bool) -> Result<()> {
    validate_optional_text("eval target", Some(&args.target))?;
    if args.success == args.failure {
        bail!("eval record requires exactly one of --success or --failure");
    }
    let record = store.update(|os| {
        let record = os.record_eval(
            EvalRecord {
                id: os.next_eval_id(),
                target: args.target,
                success: args.success,
                cost_micros: args.cost_micros,
                latency_ms: args.latency_ms,
                run: None,
                recorded_at: Utc::now(),
            },
            "recorded",
        );
        Ok::<_, anyhow::Error>(record)
    })?;
    if json {
        print_json(&record)?;
    } else {
        println!("Recorded eval {}", record.id);
    }
    Ok(())
}

fn run_eval(store: Store, args: EvalRunArgs, json: bool) -> Result<()> {
    validate_optional_text("eval target", Some(&args.target))?;
    validate_optional_text("eval command", Some(&args.command))?;
    validate_optional_text("eval success pattern", args.success_pattern.as_deref())?;
    validate_optional_path("eval cwd", args.cwd.as_deref())?;
    validate_optional_path("eval output schema", args.output_schema.as_deref())?;
    let output_schema = match args.output_schema.as_ref() {
        Some(path) => {
            let body = std::fs::read_to_string(path)
                .with_context(|| format!("could not read eval output schema {}", path.display()))?;
            let schema = serde_json::from_str::<Value>(&body).with_context(|| {
                format!(
                    "could not parse eval output schema {} as JSON",
                    path.display()
                )
            })?;
            Some(schema)
        }
        None => None,
    };
    let os = store.load().with_context(|| state_init_hint(&store))?;
    check_shell_command(&os.policy, &args.command)?;
    let cwd = args
        .cwd
        .clone()
        .unwrap_or(std::env::current_dir().context("could not read current directory")?);
    check_workspace(&os.policy, &cwd)?;
    check_shell_writes(&os.policy, &args.command, &cwd)?;
    let env = eval_environment(&os.policy);
    let started = std::time::Instant::now();
    let output = run_shell_capture(
        &args.command,
        &cwd,
        &env,
        std::time::Duration::from_secs(os.policy.command_timeout_seconds),
        os.policy.sandbox.process_isolation,
        os.policy.max_output_bytes,
    )
    .with_context(|| format!("could not run eval command in {}", cwd.display()))?;
    let latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let combined = format!("{stdout}{stderr}");
    let pattern_matched = args
        .success_pattern
        .as_ref()
        .map(|pattern| combined.contains(pattern))
        .unwrap_or(true);
    let (output_schema_valid, output_schema_error) =
        eval_output_schema_report(&stdout, output_schema.as_ref());
    let success = !output.timed_out
        && output.status_code == Some(0)
        && pattern_matched
        && output_schema_valid;
    let status_code = output.status_code;
    let run_details = EvalRunDetails {
        command: args.command.clone(),
        cwd: cwd.display().to_string(),
        status: status_code,
        stdout: tail_text_by_bytes(&stdout, Some(os.policy.max_output_bytes)),
        stderr: tail_text_by_bytes(&stderr, Some(os.policy.max_output_bytes)),
        success_pattern: args.success_pattern.clone(),
        success_pattern_matched: pattern_matched,
        output_schema,
        output_schema_valid,
        output_schema_error,
        timed_out: output.timed_out,
    };
    let record = store.update(|os| {
        let record = os.record_eval(
            EvalRecord {
                id: os.next_eval_id(),
                target: args.target,
                success,
                cost_micros: None,
                latency_ms: Some(latency_ms),
                run: Some(run_details.clone()),
                recorded_at: Utc::now(),
            },
            "ran",
        );
        Ok::<_, anyhow::Error>(record)
    })?;
    if json {
        print_json(&serde_json::json!({
            "id": record.id,
            "eval": record,
            "command": run_details.command,
            "cwd": run_details.cwd,
            "status": run_details.status,
            "stdout": run_details.stdout,
            "stderr": run_details.stderr,
            "success_pattern_matched": run_details.success_pattern_matched,
            "output_schema_valid": run_details.output_schema_valid,
            "output_schema_error": run_details.output_schema_error,
            "timed_out": run_details.timed_out,
        }))?;
    } else {
        println!(
            "Ran eval {}: {}",
            record.id,
            if record.success { "success" } else { "failure" }
        );
    }
    Ok(())
}

fn eval_output_schema_report(stdout: &str, schema: Option<&Value>) -> (bool, Option<String>) {
    let Some(schema) = schema else {
        return (true, None);
    };
    let output = match serde_json::from_str::<Value>(stdout) {
        Ok(output) => output,
        Err(error) => {
            return (
                false,
                Some(format!(
                    "output_schema requires stdout to be a JSON value: {error}"
                )),
            );
        }
    };
    match validate_json_schema(&output, schema, "output", "output_schema") {
        Ok(()) => (true, None),
        Err(error) => (false, Some(error.to_string())),
    }
}

fn eval_environment(policy: &Policy) -> Vec<(String, String)> {
    let mut env = std::env::vars()
        .filter(|(key, _)| {
            policy.inherit_environment
                || policy.allowed_env_vars.iter().any(|allowed| allowed == key)
        })
        .collect::<Vec<_>>();
    env.sort_by(|left, right| left.0.cmp(&right.0));
    env
}

fn handle_secrets(store: Store, command: SecretsCommand, json: bool) -> Result<()> {
    match command {
        SecretsCommand::List(args) => list_secrets_backends(store, args, json),
        SecretsCommand::Check => check_secrets(store, json),
        SecretsCommand::Register(args) => register_secrets_backend(store, args, json),
        SecretsCommand::Show(args) => show_secrets_backend(store, args, json),
        SecretsCommand::Remove(args) => remove_secrets_backend(store, args, json),
    }
}

fn check_secrets(store: Store, json: bool) -> Result<()> {
    let os = store.load().with_context(|| state_init_hint(&store))?;
    let report = secret_check_report(&os);
    if json {
        print_json(&report)?;
    } else if report.total == 0 {
        println!("No secret environment references found.");
    } else {
        print_table(
            report
                .references
                .iter()
                .map(|reference| SecretCheckRow {
                    task: reference.task_id.to_string(),
                    tool: reference.tool_id.to_string(),
                    arg: reference.arg.clone(),
                    env: reference.env.clone(),
                    present: if reference.present { "yes" } else { "no" }.into(),
                })
                .collect(),
        );
        println!(
            "Secret references: {} total, {} present, {} missing, {} invalid",
            report.total, report.present, report.missing, report.invalid
        );
    }
    Ok(())
}

fn list_secrets_backends(store: Store, args: SecretsListArgs, json: bool) -> Result<()> {
    validate_secrets_list_limit(args.limit)?;
    let kind = args
        .kind
        .as_deref()
        .map(parse_secrets_backend_kind)
        .transpose()?;
    if let Some(query) = &args.query {
        validate_secrets_query(query)?;
    }
    let query = args.query.as_ref().map(|query| query.to_ascii_lowercase());
    let os = store.load().with_context(|| state_init_hint(&store))?;
    let mut backends = os
        .secrets_backends
        .values()
        .filter(|backend| {
            kind.as_ref()
                .map(|kind| &backend.kind == kind)
                .unwrap_or(true)
                && query
                    .as_ref()
                    .map(|query| secrets_backend_matches_query(backend, query))
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    backends.sort_by(|left, right| left.id.cmp(&right.id));
    if let Some(limit) = args.limit {
        backends.truncate(limit);
    }
    if json {
        print_json(&backends)?;
    } else {
        print_table(
            backends
                .into_iter()
                .map(|backend| SecretsBackendRow {
                    id: backend.id.clone(),
                    kind: secrets_backend_kind_name(&backend.kind).into(),
                    reference: backend.reference.clone().unwrap_or_else(|| "-".into()),
                })
                .collect(),
        );
    }
    Ok(())
}

fn register_secrets_backend(store: Store, args: SecretsRegisterArgs, json: bool) -> Result<()> {
    validate_registry_id("secrets backend id", &args.id)?;
    let kind = parse_secrets_backend_kind(&args.kind)?;
    validate_optional_text("secrets backend reference", args.reference.as_deref())?;
    let backend = store.update(|os| {
        if os.secrets_backends.contains_key(&args.id) {
            bail!("secrets backend already exists: {}", args.id);
        }
        let backend = SecretsBackend {
            id: args.id.clone(),
            kind,
            reference: args.reference,
        };
        os.register_secrets_backend(backend.clone());
        Ok::<_, anyhow::Error>(backend)
    })?;
    if json {
        print_json(&backend)?;
    } else {
        println!("Registered secrets backend {}", backend.id);
    }
    Ok(())
}

fn show_secrets_backend(store: Store, args: SecretsIdArgs, json: bool) -> Result<()> {
    validate_registry_id("secrets backend id", &args.id)?;
    let os = store.load().with_context(|| state_init_hint(&store))?;
    let backend = os
        .secrets_backends
        .get(&args.id)
        .with_context(|| format!("secrets backend not found: {}", args.id))?;
    if json {
        print_json(backend)?;
    } else {
        println!(
            "{} {}",
            backend.id,
            secrets_backend_kind_name(&backend.kind)
        );
        println!("reference: {}", backend.reference.as_deref().unwrap_or("-"));
    }
    Ok(())
}

fn remove_secrets_backend(store: Store, args: SecretsIdArgs, json: bool) -> Result<()> {
    validate_registry_id("secrets backend id", &args.id)?;
    let backend = store.update(|os| {
        let backend = os
            .secrets_backends
            .remove(&args.id)
            .with_context(|| format!("secrets backend not found: {}", args.id))?;
        os.record(
            EventKind::SecretsBackendRemoved,
            format!("removed secrets backend {}", args.id),
        );
        Ok::<_, anyhow::Error>(backend)
    })?;
    if json {
        print_json(&serde_json::json!({
            "id": args.id,
            "removed": true,
            "secrets_backend": backend,
        }))?;
    } else {
        println!("Removed secrets backend {}", backend.id);
    }
    Ok(())
}

fn handle_approval(store: Store, command: ApprovalCommand, json: bool) -> Result<()> {
    match command {
        ApprovalCommand::List => {
            let os = store.load().with_context(|| state_init_hint(&store))?;
            let approvals = os.approvals.values().collect::<Vec<_>>();
            if json {
                print_json(&approvals)?;
            } else {
                for approval in approvals {
                    println!(
                        "{}\t{:?}\t{}\t{}",
                        approval.id, approval.status, approval.task_id, approval.action
                    );
                }
            }
        }
        ApprovalCommand::Approve(args) => resolve_approval(store, args, true, json)?,
        ApprovalCommand::Deny(args) => resolve_approval(store, args, false, json)?,
    }
    Ok(())
}

fn resolve_approval(
    store: Store,
    args: ApprovalResolveArgs,
    approved: bool,
    json: bool,
) -> Result<()> {
    validate_approval_id(&args.id)?;
    let approval = store.update(|os| {
        os.resolve_approval(&args.id, approved, args.by)
            .with_context(|| format!("approval not found: {}", args.id))
    })?;
    if json {
        print_json(&serde_json::json!({
            "id": args.id,
            "approval": approval,
        }))?;
    } else {
        println!(
            "{} approval {}",
            if approved { "Approved" } else { "Denied" },
            args.id
        );
    }
    Ok(())
}

fn validate_approval_id(id: &str) -> Result<()> {
    if !contains_slug_character(id) {
        bail!("approval id must contain at least one ASCII letter, digit, or hyphen");
    }
    Ok(())
}

fn handle_git(store: Store, command: GitCommand, json: bool) -> Result<()> {
    match command {
        GitCommand::Status(args) => {
            let cwd = resolve_git_cwd(args.cwd)?;
            let output = run_git_command(&cwd, &["status", "--short", "--branch"], false)?;
            print_git_output(&output, json)?;
        }
        GitCommand::Branch(args) => {
            validate_git_ref("branch name", &args.name)?;
            let cwd = resolve_git_cwd(args.cwd)?;
            let git_args = if args.create {
                vec!["switch", "-c", args.name.as_str()]
            } else {
                vec!["switch", args.name.as_str()]
            };
            let output = run_git_command(&cwd, &git_args, false)?;
            print_git_output(&output, json)?;
        }
        GitCommand::Commit(args) => {
            validate_git_message(&args.message)?;
            let cwd = resolve_git_cwd(args.cwd)?;
            if args.all {
                let add = run_git_command(&cwd, &["add", "-A"], args.dry_run)?;
                if json {
                    let commit =
                        run_git_command(&cwd, &["commit", "-m", args.message.as_str()], true)?;
                    print_json(&serde_json::json!({
                        "steps": [add, commit],
                    }))?;
                    return Ok(());
                }
                print_git_output(&add, false)?;
                if args.dry_run {
                    let commit =
                        run_git_command(&cwd, &["commit", "-m", args.message.as_str()], true)?;
                    print_git_output(&commit, false)?;
                    return Ok(());
                }
            }
            let output =
                run_git_command(&cwd, &["commit", "-m", args.message.as_str()], args.dry_run)?;
            print_git_output(&output, json)?;
        }
        GitCommand::Pr(args) => {
            validate_git_message(&args.title)?;
            validate_git_message(&args.body)?;
            if let Some(base) = &args.base {
                validate_git_ref("base branch", base)?;
            }
            if let Some(head) = &args.head {
                validate_git_ref("head branch", head)?;
            }
            let cwd = resolve_git_cwd(args.cwd)?;
            let mut owned_args = vec![
                "pr".to_owned(),
                "create".to_owned(),
                "--title".to_owned(),
                args.title,
                "--body".to_owned(),
                args.body,
            ];
            if let Some(base) = args.base {
                owned_args.push("--base".into());
                owned_args.push(base);
            }
            if let Some(head) = args.head {
                owned_args.push("--head".into());
                owned_args.push(head);
            }
            if args.draft {
                owned_args.push("--draft".into());
            }
            let borrowed = owned_args.iter().map(String::as_str).collect::<Vec<_>>();
            let output = run_external_command(&cwd, "gh", &borrowed, args.dry_run)?;
            print_git_output(&output, json)?;
        }
        GitCommand::ReviewTask(args) => {
            validate_git_ref("base branch", &args.base)?;
            validate_task_title(&args.title)?;
            let priority = parse_priority(&args.priority)?;
            let cwd = resolve_git_cwd(args.cwd)?;
            let top_level = run_git_capture(&cwd, &["rev-parse", "--show-toplevel"])?;
            let top_level = top_level.trim();
            if top_level.is_empty() {
                bail!("git workspace root is empty");
            }
            let base = args.base;
            let command = format!("git diff --stat {base}...HEAD && git diff {base}...HEAD");
            let objective = format!("Review the git diff for {} against {base}", top_level);
            let (id, task) = store.update(|os| {
                let mut task = Task::new(args.title, objective, priority, vec!["review".into()]);
                os.ensure_unique_task_id(&mut task);
                task.command = Some(command);
                task.cwd = Some(top_level.to_owned());
                let id = task.id.clone();
                os.create_task(task);
                let task = os
                    .tasks
                    .get(&id)
                    .cloned()
                    .with_context(|| format!("task not found after creation: {id}"))?;
                Ok::<_, anyhow::Error>((id, task))
            })?;
            if json {
                print_json(&serde_json::json!({
                    "id": id,
                    "task": task,
                    "base": base,
                    "cwd": top_level,
                }))?;
            } else {
                println!("Created git review task {}", id);
            }
        }
    }
    Ok(())
}

fn validate_git_ref(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    if value.chars().any(|ch| {
        ch.is_ascii_control() || matches!(ch, ' ' | '~' | '^' | ':' | '?' | '*' | '[' | '\\')
    }) {
        bail!("{field} contains characters git refs cannot safely use");
    }
    Ok(())
}

fn validate_git_message(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("git message text must not be empty");
    }
    Ok(())
}

fn print_git_output(output: &GitCommandOutput, json: bool) -> Result<()> {
    if json {
        print_json(output)?;
        return Ok(());
    }
    if output.dry_run {
        println!("{}", output.command.join(" "));
        return Ok(());
    }
    print!("{}", output.stdout);
    if !output.stderr.is_empty() {
        eprint!("{}", output.stderr);
    }
    Ok(())
}

fn handle_runs(store: Store, command: RunsCommand, json: bool) -> Result<()> {
    let os = store.load().with_context(|| state_init_hint(&store))?;
    match command {
        RunsCommand::List(args) => {
            validate_runs_limit(args.limit)?;
            let status = args.status.as_deref().map(parse_run_status).transpose()?;
            let task = args.task.as_deref().map(parse_task_id_arg).transpose()?;
            let agent = args.agent.as_deref().map(parse_agent_id_arg).transpose()?;
            let since = args
                .since
                .as_deref()
                .map(|value| parse_filter_timestamp(value, "run", "since"))
                .transpose()?;
            let until = args
                .until
                .as_deref()
                .map(|value| parse_filter_timestamp(value, "run", "until"))
                .transpose()?;
            if let Some(query) = &args.query {
                validate_run_query(query)?;
            }
            let query = args.query.as_ref().map(|query| query.to_ascii_lowercase());
            let mut runs = os
                .runs
                .values()
                .filter(|run| {
                    status
                        .as_ref()
                        .map(|status| &run.status == status)
                        .unwrap_or(true)
                        && task
                            .as_ref()
                            .map(|task| &run.task_id == task)
                            .unwrap_or(true)
                        && agent
                            .as_ref()
                            .map(|agent| run.agent_id.as_ref() == Some(agent))
                            .unwrap_or(true)
                        && since
                            .as_ref()
                            .map(|since| run.started_at >= *since)
                            .unwrap_or(true)
                        && until
                            .as_ref()
                            .map(|until| run.started_at <= *until)
                            .unwrap_or(true)
                        && query
                            .as_ref()
                            .map(|query| run.command.to_ascii_lowercase().contains(query))
                            .unwrap_or(true)
                })
                .collect::<Vec<_>>();
            runs.sort_by_key(|run| std::cmp::Reverse(run.started_at));
            if let Some(limit) = args.limit {
                runs.truncate(limit);
            }
            if json {
                print_json(&runs)?;
            } else {
                print_runs(runs);
            }
        }
        RunsCommand::Show(args) => {
            let id = parse_run_id_arg(&args.id)?;
            let run = os
                .runs
                .get(&id)
                .with_context(|| format!("run not found: {}", id))?;
            if json {
                print_json(run)?;
            } else {
                print_run_detail(run);
            }
        }
        RunsCommand::Logs(args) => {
            validate_log_tail_bytes(args.tail_bytes)?;
            let id = parse_run_id_arg(&args.id)?;
            let run = os
                .runs
                .get(&id)
                .with_context(|| format!("run not found: {}", id))?;
            let path = run_log_path(&store, run);
            let mut body = std::fs::read_to_string(&path)
                .with_context(|| format!("could not read run log: {}", path.display()))?;
            let truncated = text_tail_was_truncated(&body, args.tail_bytes);
            body = tail_text_by_bytes(&body, args.tail_bytes);
            if json {
                print_json(&serde_json::json!({
                    "run_id": id,
                    "log_path": path,
                    "tail_bytes": args.tail_bytes,
                    "truncated": truncated,
                    "body": body,
                }))?;
            } else {
                print!("{body}");
            }
        }
        RunsCommand::Tail(args) => {
            validate_log_tail_bytes(args.tail_bytes)?;
            validate_tail_interval(args.interval_ms)?;
            let id = parse_run_id_arg(&args.id)?;
            tail_run(
                &store,
                &os,
                &id,
                args.follow,
                args.tail_bytes,
                args.interval_ms,
                json,
            )?;
        }
        RunsCommand::Replay(args) => {
            validate_log_tail_bytes(args.tail_bytes)?;
            let id = parse_run_id_arg(&args.id)?;
            let run = os
                .runs
                .get(&id)
                .with_context(|| format!("run not found: {}", id))?;
            let task = os.tasks.get(&run.task_id);
            let related_events = os
                .events
                .iter()
                .filter(|event| {
                    event.message.contains(&run.id.to_string())
                        || event.message.contains(&run.task_id.to_string())
                })
                .collect::<Vec<_>>();
            let log_path = run_log_path(&store, run);
            let mut log_truncated = false;
            let (log, log_error) = match std::fs::read_to_string(&log_path) {
                Ok(body) => {
                    log_truncated = text_tail_was_truncated(&body, args.tail_bytes);
                    (Some(tail_text_by_bytes(&body, args.tail_bytes)), None)
                }
                Err(error) => (
                    None,
                    Some(format!(
                        "could not read run log: {}: {error}",
                        log_path.display()
                    )),
                ),
            };

            if json {
                print_json(&serde_json::json!({
                    "run": run,
                    "task": task,
                    "events": related_events,
                    "log": log,
                    "log_tail_bytes": args.tail_bytes,
                    "log_truncated": log_truncated,
                    "log_error": log_error,
                }))?;
            } else {
                print_replay(
                    run,
                    task,
                    &related_events,
                    log.as_deref(),
                    log_error.as_deref(),
                );
            }
        }
        RunsCommand::Debug(args) => {
            validate_log_tail_bytes(args.tail_bytes)?;
            let id = parse_run_id_arg(&args.id)?;
            let run = os
                .runs
                .get(&id)
                .with_context(|| format!("run not found: {}", id))?;
            let task = os.tasks.get(&run.task_id);
            let agent = run
                .agent_id
                .as_ref()
                .and_then(|agent_id| os.agents.get(agent_id));
            let workflows = os
                .workflows
                .values()
                .filter(|workflow| {
                    workflow
                        .tasks
                        .values()
                        .any(|task_id| task_id == &run.task_id)
                })
                .collect::<Vec<_>>();
            let approvals = os
                .approvals
                .values()
                .filter(|approval| {
                    approval.task_id == run.task_id || approval.run_id.as_ref() == Some(&run.id)
                })
                .collect::<Vec<_>>();
            let related_events = os
                .events
                .iter()
                .filter(|event| {
                    event.message.contains(&run.id.to_string())
                        || event.message.contains(&run.task_id.to_string())
                })
                .collect::<Vec<_>>();
            let log_path = run_log_path(&store, run);
            let mut log_truncated = false;
            let (log, log_error) = match std::fs::read_to_string(&log_path) {
                Ok(body) => {
                    log_truncated = text_tail_was_truncated(&body, args.tail_bytes);
                    (Some(tail_text_by_bytes(&body, args.tail_bytes)), None)
                }
                Err(error) => (
                    None,
                    Some(format!(
                        "could not read run log: {}: {error}",
                        log_path.display()
                    )),
                ),
            };
            let artifact_status = run
                .artifacts
                .iter()
                .map(|artifact| {
                    serde_json::json!({
                        "artifact": artifact,
                        "exists": std::path::Path::new(&artifact.path).exists(),
                    })
                })
                .collect::<Vec<_>>();

            if json {
                print_json(&serde_json::json!({
                    "run": run,
                    "task": task,
                    "agent": agent,
                    "workflows": workflows,
                    "approvals": approvals,
                    "events": related_events,
                    "log": log,
                    "log_tail_bytes": args.tail_bytes,
                    "log_truncated": log_truncated,
                    "log_error": log_error,
                    "artifact_status": artifact_status,
                    "diagnostics": {
                        "log_path": log_path.display().to_string(),
                        "related_events": related_events.len(),
                        "related_workflows": workflows.len(),
                        "related_approvals": approvals.len(),
                        "artifacts": run.artifacts.len(),
                    }
                }))?;
            } else {
                let agent_id = agent.map(|agent| agent.id.to_string());
                print_debug(
                    run,
                    task,
                    agent_id.as_deref(),
                    &workflows,
                    &approvals,
                    &related_events,
                    log.as_deref(),
                    log_error.as_deref(),
                    &artifact_status,
                    &log_path,
                );
            }
        }
        RunsCommand::Artifacts(args) => {
            validate_log_tail_bytes(args.tail_bytes)?;
            let id = parse_run_id_arg(&args.id)?;
            let run = os
                .runs
                .get(&id)
                .with_context(|| format!("run not found: {}", id))?;
            match args.artifact.as_deref() {
                Some(artifact_id) => {
                    let (index, artifact) = find_run_artifact(run, artifact_id)
                        .with_context(|| format!("run artifact not found: {artifact_id}"))?;
                    let resolved_id = run_artifact_id(run, index, artifact);
                    let body =
                        run_artifact_read_json(run, index, &resolved_id, artifact, args.tail_bytes);
                    if json {
                        print_json(&body)?;
                    } else {
                        print_artifact_body(&body);
                    }
                }
                None => {
                    let artifacts = run_artifact_entries(run);
                    if json {
                        print_json(&serde_json::json!({
                            "run_id": id,
                            "artifacts": artifacts,
                        }))?;
                    } else {
                        print_artifacts(&artifacts);
                    }
                }
            }
        }
        RunsCommand::Cancel(args) => {
            let id = parse_run_id_arg(&args.id)?;
            let (run, requested) = store.update(|os| {
                let mut requested = false;
                let mut changed = false;
                let run = {
                    let run = os
                        .runs
                        .get_mut(&id)
                        .with_context(|| format!("run not found: {}", id))?;
                    if run.status == RunStatus::Running {
                        run.status = RunStatus::CancelRequested;
                        run.finished_at = None;
                        run.exit_code = None;
                        requested = true;
                        changed = true;
                    } else if run.status == RunStatus::CancelRequested {
                        requested = true;
                    }
                    run.clone()
                };
                if changed {
                    os.record(
                        EventKind::RunFinished,
                        format!("cancel requested for run {}", id),
                    );
                    if let Some(task) = os.tasks.get_mut(&run.task_id) {
                        task.output = Some(format!("cancel requested for run {}", id));
                        task.updated_at = chrono::Utc::now();
                    }
                }
                Ok::<_, anyhow::Error>((run, requested))
            })?;
            if json {
                print_json(&serde_json::json!({
                    "id": id,
                    "cancel_requested": requested,
                    "run": run,
                }))?;
            } else if requested {
                println!("Cancel requested for run {}", id);
            } else {
                println!("Run {} is not running; status is {}", id, run.status);
            }
        }
    }
    Ok(())
}

fn tail_run(
    store: &Store,
    initial_os: &OperatingSystem,
    id: &RunId,
    follow: bool,
    tail_bytes: Option<usize>,
    interval_ms: u64,
    json: bool,
) -> Result<()> {
    let mut offset = 0usize;
    let mut first_read = true;
    let mut os = initial_os.clone();
    loop {
        let run = os
            .runs
            .get(id)
            .with_context(|| format!("run not found: {}", id))?;
        let path = run_log_path(store, run);
        let is_live = matches!(run.status, RunStatus::Running | RunStatus::CancelRequested);
        let body = match std::fs::read_to_string(&path) {
            Ok(body) => body,
            Err(error) if follow && is_live => String::new(),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("could not read run log: {}", path.display()));
            }
        };
        if json {
            let truncated = text_tail_was_truncated(&body, tail_bytes);
            let body = tail_text_by_bytes(&body, tail_bytes);
            print_json(&serde_json::json!({
                "run_id": id,
                "log_path": path,
                "tail_bytes": tail_bytes,
                "truncated": truncated,
                "body": body,
                "follow": follow,
            }))?;
            return Ok(());
        }
        if body.len() > offset {
            if first_read {
                print!("{}", tail_text_by_bytes(&body, tail_bytes));
            } else {
                print!("{}", &body[offset..]);
            }
            std::io::stdout().flush()?;
            offset = body.len();
        }
        first_read = false;
        if !follow || !is_live {
            break;
        }
        std::thread::sleep(Duration::from_millis(interval_ms.max(20)));
        os = store
            .load()
            .context("run state disappeared while tailing")?;
    }
    Ok(())
}

fn run_log_path(store: &Store, run: &RunRecord) -> std::path::PathBuf {
    store.run_log_path(&run.id)
}

fn handle_daemon(store: Store, command: DaemonCommand, json: bool) -> Result<()> {
    match command {
        DaemonCommand::Status => {
            let os = store.load().with_context(|| state_init_hint(&store))?;
            if json {
                print_json(&serde_json::json!({
                    "daemon": os.daemon,
                }))?;
            } else {
                print_daemon(os.daemon.as_ref());
            }
            Ok(())
        }
        DaemonCommand::Run(args) => run_daemon(store, args, json),
        DaemonCommand::Stop => stop_daemon(store, json),
    }
}

fn handle_service(store: Store, command: ServiceCommand, json: bool) -> Result<()> {
    match command {
        ServiceCommand::Launchd(args) => {
            let (service, plist_path) = build_launchd_service(&store, args)?;

            if json {
                print_json(&serde_json::json!({
                    "platform": "launchd",
                    "service": service,
                    "plist": service.render_plist(),
                    "plist_path": plist_path,
                }))?;
            } else {
                print!("{}", service.render_plist());
            }
        }
        ServiceCommand::Systemd(args) => {
            let (service, unit_path) = build_systemd_service(&store, args)?;
            if json {
                print_json(&serde_json::json!({
                    "platform": "systemd",
                    "service": service,
                    "unit": service.render_unit(),
                    "unit_path": unit_path,
                }))?;
            } else {
                print!("{}", service.render_unit());
            }
        }
        ServiceCommand::WindowsTask(args) => {
            let task = build_windows_scheduled_task(&store, args)?;
            if json {
                print_json(&serde_json::json!({
                    "platform": "windows-scheduled-task",
                    "task": task,
                    "powershell": task.render_powershell(),
                }))?;
            } else {
                print!("{}", task.render_powershell());
            }
        }
        ServiceCommand::Install(args) => {
            let installation = install_launchd_service(&store, args)?;
            if json {
                print_json(&serde_json::json!({
                    "platform": "launchd",
                    "installed": installation.installed,
                    "plist_path": installation.plist_path,
                    "service": installation.service,
                }))?;
            } else {
                println!(
                    "Installed launchd service plist at {}",
                    installation.plist_path.display()
                );
            }
        }
        ServiceCommand::InstallSystemd(args) => {
            let installation = install_systemd_service(&store, args)?;
            if json {
                print_json(&serde_json::json!({
                    "platform": "systemd",
                    "installed": installation.installed,
                    "unit_path": installation.unit_path,
                    "service": installation.service,
                }))?;
            } else {
                println!(
                    "Installed systemd user service unit at {}",
                    installation.unit_path.display()
                );
            }
        }
        ServiceCommand::Uninstall(args) => {
            let removal = uninstall_launchd_service_definition(&args.label, args.plist_path)
                .map_err(anyhow::Error::from)?;
            if json {
                print_json(&serde_json::json!({
                    "platform": "launchd",
                    "removed": removal.removed,
                    "plist_path": removal.plist_path,
                }))?;
            } else if removal.removed {
                println!(
                    "Removed launchd service plist at {}",
                    removal.plist_path.display()
                );
            } else {
                println!(
                    "No launchd service plist found at {}",
                    removal.plist_path.display()
                );
            }
        }
        ServiceCommand::UninstallSystemd(args) => {
            let removal = uninstall_systemd_service_definition(&args.unit_name, args.unit_path)
                .map_err(anyhow::Error::from)?;
            if json {
                print_json(&serde_json::json!({
                    "platform": "systemd",
                    "removed": removal.removed,
                    "unit_path": removal.unit_path,
                }))?;
            } else if removal.removed {
                println!(
                    "Removed systemd user service unit at {}",
                    removal.unit_path.display()
                );
            } else {
                println!(
                    "No systemd user service unit found at {}",
                    removal.unit_path.display()
                );
            }
        }
        ServiceCommand::Start(args) => {
            validate_service_control_inputs(
                &args.label,
                args.domain.as_deref(),
                args.plist_path.as_deref(),
                &args.launchctl_path,
            )?;
            let label = args.label.clone();
            let plist_path = args
                .plist_path
                .clone()
                .unwrap_or_else(|| default_launchd_plist_path(&args.label));
            let domain = resolve_launchd_domain(args.domain)?;
            if !plist_path.exists() {
                bail!(
                    "launchd plist does not exist at {}; run `agent-os service install` first",
                    plist_path.display()
                );
            }
            let output = run_launchctl(
                &args.launchctl_path,
                &["bootstrap", &domain, &plist_path.display().to_string()],
            )?;
            if !output.success {
                bail!(
                    "launchctl bootstrap failed for {} in {}: {}",
                    label,
                    domain,
                    output.stderr.trim()
                );
            }
            if json {
                print_json(&serde_json::json!({
                    "platform": "launchd",
                    "started": true,
                    "label": label,
                    "domain": domain,
                    "plist_path": plist_path,
                    "launchctl": output,
                }))?;
            } else {
                println!("Started launchd service {} in {}", label, domain);
            }
        }
        ServiceCommand::StartSystemd(args) => {
            validate_systemd_control_inputs(&args.unit_name, &args.systemctl_path)?;
            let unit_name = args.unit_name.clone();
            let output = run_systemctl(&args.systemctl_path, &["--user", "start", &unit_name])?;
            if !output.success {
                bail!(
                    "systemctl --user start failed for {}: {}",
                    unit_name,
                    output.stderr.trim()
                );
            }
            if json {
                print_json(&serde_json::json!({
                    "platform": "systemd",
                    "started": true,
                    "unit_name": unit_name,
                    "systemctl": output,
                }))?;
            } else {
                println!("Started systemd user service {unit_name}");
            }
        }
        ServiceCommand::Stop(args) => {
            validate_service_control_inputs(
                &args.label,
                args.domain.as_deref(),
                args.plist_path.as_deref(),
                &args.launchctl_path,
            )?;
            let label = args.label.clone();
            let plist_path = args
                .plist_path
                .clone()
                .unwrap_or_else(|| default_launchd_plist_path(&args.label));
            let domain = resolve_launchd_domain(args.domain)?;
            let output = run_launchctl(
                &args.launchctl_path,
                &["bootout", &domain, &plist_path.display().to_string()],
            )?;
            if !output.success {
                bail!(
                    "launchctl bootout failed for {} in {}: {}",
                    label,
                    domain,
                    output.stderr.trim()
                );
            }
            if json {
                print_json(&serde_json::json!({
                    "platform": "launchd",
                    "stopped": true,
                    "label": label,
                    "domain": domain,
                    "plist_path": plist_path,
                    "launchctl": output,
                }))?;
            } else {
                println!("Stopped launchd service {} in {}", label, domain);
            }
        }
        ServiceCommand::StopSystemd(args) => {
            validate_systemd_control_inputs(&args.unit_name, &args.systemctl_path)?;
            let unit_name = args.unit_name.clone();
            let output = run_systemctl(&args.systemctl_path, &["--user", "stop", &unit_name])?;
            if !output.success {
                bail!(
                    "systemctl --user stop failed for {}: {}",
                    unit_name,
                    output.stderr.trim()
                );
            }
            if json {
                print_json(&serde_json::json!({
                    "platform": "systemd",
                    "stopped": true,
                    "unit_name": unit_name,
                    "systemctl": output,
                }))?;
            } else {
                println!("Stopped systemd user service {unit_name}");
            }
        }
        ServiceCommand::Status(args) => {
            validate_service_control_inputs(
                &args.label,
                args.domain.as_deref(),
                None,
                &args.launchctl_path,
            )?;
            let label = args.label.clone();
            let domain = resolve_launchd_domain(args.domain)?;
            let target = format!("{domain}/{label}");
            let output = run_launchctl(&args.launchctl_path, &["print", &target])?;
            if json {
                print_json(&serde_json::json!({
                    "platform": "launchd",
                    "loaded": output.success,
                    "label": label,
                    "domain": domain,
                    "launchctl": output,
                }))?;
            } else if output.success {
                println!("launchd service {} is loaded in {}", label, domain);
                if !output.stdout.trim().is_empty() {
                    print!("{}", output.stdout);
                }
            } else {
                println!("launchd service {} is not loaded in {}", label, domain);
                if !output.stderr.trim().is_empty() {
                    eprintln!("{}", output.stderr.trim());
                }
            }
        }
        ServiceCommand::StatusSystemd(args) => {
            validate_systemd_control_inputs(&args.unit_name, &args.systemctl_path)?;
            let unit_name = args.unit_name.clone();
            let output = run_systemctl(&args.systemctl_path, &["--user", "is-active", &unit_name])?;
            if json {
                print_json(&serde_json::json!({
                    "platform": "systemd",
                    "active": output.success,
                    "unit_name": unit_name,
                    "systemctl": output,
                }))?;
            } else if output.success {
                println!("systemd user service {unit_name} is active");
                if !output.stdout.trim().is_empty() {
                    print!("{}", output.stdout);
                }
            } else {
                println!("systemd user service {unit_name} is not active");
                if !output.stderr.trim().is_empty() {
                    eprintln!("{}", output.stderr.trim());
                }
            }
        }
    }
    Ok(())
}

fn print_migration_downgrade_notes(report: &agent_os::MigrationReport) {
    for note in &report.downgrade_notes {
        println!("Downgrade note: {note}");
    }
}

fn print_migration_steps(report: &agent_os::MigrationReport) {
    if report.steps.is_empty() {
        return;
    }
    println!("Migration steps:");
    for step in &report.steps {
        println!("- {step}");
    }
}

fn print_migration_validation(validation: &agent_os::ValidationReport) {
    if validation.valid {
        println!("Migrated state validation passed.");
    }
}

fn build_launchd_service(
    store: &Store,
    args: ServiceLaunchdArgs,
) -> Result<(LaunchdService, std::path::PathBuf)> {
    build_launchd_service_definition(store.path(), launchd_service_options(args))
        .map_err(anyhow::Error::from)
}

fn install_launchd_service(
    store: &Store,
    args: ServiceLaunchdArgs,
) -> Result<agent_os::LaunchdServiceInstall> {
    install_launchd_service_definition(store.path(), launchd_service_options(args))
        .map_err(anyhow::Error::from)
}

fn build_systemd_service(
    store: &Store,
    args: ServiceSystemdArgs,
) -> Result<(agent_os::SystemdService, std::path::PathBuf)> {
    build_systemd_service_definition(store.path(), systemd_service_options(args))
        .map_err(anyhow::Error::from)
}

fn build_windows_scheduled_task(
    store: &Store,
    args: ServiceWindowsTaskArgs,
) -> Result<WindowsScheduledTask> {
    build_windows_scheduled_task_definition(store.path(), windows_scheduled_task_options(args))
        .map_err(anyhow::Error::from)
}

fn install_systemd_service(
    store: &Store,
    args: ServiceSystemdArgs,
) -> Result<agent_os::SystemdServiceInstall> {
    install_systemd_service_definition(store.path(), systemd_service_options(args))
        .map_err(anyhow::Error::from)
}

fn launchd_service_options(args: ServiceLaunchdArgs) -> LaunchdServiceOptions {
    LaunchdServiceOptions {
        label: args.label,
        program: args.bin_path,
        interval_ms: args.interval_ms,
        limit: args.limit,
        execute: args.execute,
        recover_stale_seconds: args.recover_stale_seconds,
        no_logs: args.no_logs,
        plist_path: args.plist_path,
    }
}

fn systemd_service_options(args: ServiceSystemdArgs) -> agent_os::SystemdServiceOptions {
    agent_os::SystemdServiceOptions {
        unit_name: args.unit_name,
        program: args.bin_path,
        interval_ms: args.interval_ms,
        limit: args.limit,
        execute: args.execute,
        recover_stale_seconds: args.recover_stale_seconds,
        unit_path: args.unit_path,
    }
}

fn windows_scheduled_task_options(args: ServiceWindowsTaskArgs) -> WindowsScheduledTaskOptions {
    WindowsScheduledTaskOptions {
        task_name: args.task_name,
        program: args.bin_path,
        interval_ms: args.interval_ms,
        limit: args.limit,
        execute: args.execute,
        recover_stale_seconds: args.recover_stale_seconds,
    }
}

fn handle_api(store: Store, config_path: std::path::PathBuf, command: ApiCommand) -> Result<()> {
    match command {
        ApiCommand::Serve(args) => {
            validate_api_serve_inputs(&args)?;
            let auth = ApiAuth::scoped(
                read_full_access_api_token(&args)?,
                read_scoped_api_token(
                    "read_token_env",
                    args.read_token_env.as_deref(),
                    "read_token_file",
                    args.read_token_file.as_deref(),
                )?,
                read_scoped_api_token(
                    "write_token_env",
                    args.write_token_env.as_deref(),
                    "write_token_file",
                    args.write_token_file.as_deref(),
                )?,
            );
            validate_api_auth_tokens(&auth)?;
            let cors = api_cors_from_args(&args);
            let server = ApiServer::bind_with_auth_config_path_and_cors(
                store,
                &args.addr,
                args.max_requests,
                auth,
                config_path,
                cors,
            )?;
            println!("API listening on http://{}", server.local_addr()?);
            server.serve()?;
        }
        ApiCommand::Schema => {
            print_json(&openapi_schema())?;
        }
    }
    Ok(())
}

fn read_api_token_env(label: &str, env_name: Option<&str>) -> Result<Option<String>> {
    let Some(env_name) = env_name else {
        return Ok(None);
    };
    let token = std::env::var(env_name)
        .with_context(|| format!("could not read API token env {env_name}"))?;
    if token.trim().is_empty() {
        bail!("API token from {label} must not be empty");
    }
    validate_api_token_value(label, &token)?;
    Ok(Some(token))
}

fn read_api_token_file(label: &str, path: Option<&std::path::Path>) -> Result<Option<String>> {
    let Some(path) = path else {
        return Ok(None);
    };
    validate_api_token_file_security(label, path)?;
    let token = std::fs::read_to_string(path)
        .with_context(|| format!("could not read API token file {}", path.display()))?;
    let token = token.trim().to_owned();
    if token.is_empty() {
        bail!("API token from {label} must not be empty");
    }
    validate_api_token_value(label, &token)?;
    Ok(Some(token))
}

#[cfg(unix)]
fn validate_api_token_file_security(label: &str, path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let symlink_metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("could not inspect API token file {}", path.display()))?;
    if symlink_metadata.file_type().is_symlink() {
        bail!("API token file {label} must be a regular file, not a symlink");
    }
    if !symlink_metadata.file_type().is_file() {
        bail!("API token file {label} must be a regular file");
    }
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("could not inspect API token file {}", path.display()))?;
    if !metadata.file_type().is_file() {
        bail!("API token file {label} must be a regular file");
    }
    let mode = metadata.permissions().mode();
    if mode & 0o077 != 0 {
        bail!(
            "API token file {label} must not be accessible by group or others; run `chmod 600 {}`",
            path.display()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_api_token_file_security(label: &str, path: &std::path::Path) -> Result<()> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("could not inspect API token file {}", path.display()))?;
    if !metadata.file_type().is_file() {
        bail!("API token file {label} must be a regular file");
    }
    Ok(())
}

fn validate_api_token_value(label: &str, token: &str) -> Result<()> {
    if token.len() < MIN_API_TOKEN_BYTES {
        bail!("API token from {label} must be at least {MIN_API_TOKEN_BYTES} bytes");
    }
    if token
        .bytes()
        .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        bail!("API token from {label} must not contain whitespace or control characters");
    }
    Ok(())
}

fn validate_api_auth_tokens(auth: &ApiAuth) -> Result<()> {
    if let (Some(read_token), Some(write_token)) =
        (auth.read_token.as_deref(), auth.write_token.as_deref())
        && read_token == write_token
    {
        bail!("read and write API tokens must be distinct");
    }
    Ok(())
}

fn read_full_access_api_token(args: &ApiServeArgs) -> Result<Option<String>> {
    if let Some(token) = read_api_token_env("token_env", args.token_env.as_deref())? {
        return Ok(Some(token));
    }
    read_api_token_file("token_file", args.token_file.as_deref())
}

fn read_scoped_api_token(
    env_label: &str,
    env_name: Option<&str>,
    file_label: &str,
    file_path: Option<&std::path::Path>,
) -> Result<Option<String>> {
    if let Some(token) = read_api_token_env(env_label, env_name)? {
        return Ok(Some(token));
    }
    read_api_token_file(file_label, file_path)
}

fn handle_workflow(store: Store, command: WorkflowCommand, json: bool) -> Result<()> {
    match command {
        WorkflowCommand::Create(args) => create_workflow(store, args, json),
        WorkflowCommand::List(args) => list_workflows(store, args, json),
        WorkflowCommand::Show(args) => show_workflow(store, args, json),
        WorkflowCommand::Status(args) => show_workflow_status(store, args, json),
        WorkflowCommand::AddTask(args) => add_workflow_task(store, args, json),
        WorkflowCommand::Link(args) => link_workflow_stages(store, args, json),
        WorkflowCommand::Unlink(args) => unlink_workflow_stages(store, args, json),
        WorkflowCommand::Pause(args) => pause_workflow(store, args, json),
        WorkflowCommand::Resume(args) => resume_workflow(store, args, json),
        WorkflowCommand::Retry(args) => retry_workflow(store, args, json),
        WorkflowCommand::Run(args) => run_workflow(store, args, json),
        WorkflowCommand::Cancel(args) => cancel_workflow(store, args, json),
        WorkflowCommand::Remove(args) => remove_workflow(store, args, json),
    }
}

fn create_workflow(store: Store, args: WorkflowCreateArgs, json: bool) -> Result<()> {
    validate_workflow_objective(&args.objective)?;
    let priority = parse_priority(&args.priority)?;
    let objective = args.objective;

    let (workflow, plan_id, build_id, review_id) = store
        .update(|os| {
            let mut plan = Task::new(
                format!("Plan: {}", objective),
                format!("Design an implementation plan for: {}", objective),
                priority,
                vec!["plan".into()],
            );
            os.ensure_unique_task_id(&mut plan);
            let plan_id = plan.id.clone();

            let mut build = Task::new(
                format!("Build: {}", objective),
                format!("Implement the approved plan for: {}", objective),
                priority,
                vec!["rust".into(), "code".into()],
            );
            os.ensure_unique_task_id(&mut build);
            build.dependencies.push(plan_id.clone());
            let build_id = build.id.clone();

            let mut review = Task::new(
                format!("Review: {}", objective),
                format!(
                    "Review and verify the completed implementation for: {}",
                    objective
                ),
                priority,
                vec!["review".into()],
            );
            os.ensure_unique_task_id(&mut review);
            review.dependencies.push(build_id.clone());
            let review_id = review.id.clone();

            os.create_task(plan);
            os.create_task(build);
            os.create_task(review);
            let mut workflow = Workflow::new(
                objective.clone(),
                priority,
                BTreeMap::from([
                    ("plan".into(), plan_id.clone()),
                    ("build".into(), build_id.clone()),
                    ("review".into(), review_id.clone()),
                ]),
            );
            os.ensure_unique_workflow_id(&mut workflow);
            os.create_workflow(workflow.clone());
            Ok::<_, anyhow::Error>((workflow, plan_id, build_id, review_id))
        })
        .with_context(|| state_init_hint(&store))?;

    let mut executed = Vec::new();
    let mut errors = Vec::new();
    if args.execute {
        (executed, errors) = execute_workflow_stages(&store, &workflow.id, false)?;
    }

    if json {
        print_json(&serde_json::json!({
            "id": workflow.id,
            "workflow": workflow,
            "tasks": {
                "plan": plan_id,
                "build": build_id,
                "review": review_id,
            },
            "runs": executed,
            "errors": errors,
        }))?;
    } else {
        println!("Created workflow {} for {}", workflow.id, objective);
        println!("plan: {}", plan_id);
        println!("build: {} after {}", build_id, plan_id);
        println!("review: {} after {}", review_id, build_id);
        for error in errors {
            eprintln!("could not execute workflow task: {error}");
        }
        for run in executed {
            println!(
                "Executed run {} for task {}: {}",
                run.id, run.task_id, run.status
            );
        }
    }

    Ok(())
}

fn run_daemon(store: Store, args: DaemonRunArgs, json: bool) -> Result<()> {
    validate_scheduler_inputs(args.limit, args.recover_stale_seconds)?;
    validate_daemon_timing(args.interval_ms, args.max_ticks)?;
    let mut ticks = 0usize;
    let max_ticks = args.max_ticks.unwrap_or(usize::MAX);
    let mut tick_reports = Vec::new();
    let mut total_assigned = 0usize;
    let mut total_executed = 0usize;
    let mut total_recovered = 0usize;
    let mut total_errors = 0usize;

    store
        .update(|os| {
            os.daemon = Some(DaemonState::running(
                std::process::id(),
                args.limit,
                args.execute,
            ));
            os.record(
                EventKind::DaemonStarted,
                format!(
                    "daemon started with limit {} execute {}",
                    args.limit, args.execute
                ),
            );
            Ok::<_, anyhow::Error>(())
        })
        .with_context(|| state_init_hint(&store))?;

    while ticks < max_ticks {
        let mut os = store.load().with_context(|| state_init_hint(&store))?;
        if os
            .daemon
            .as_ref()
            .map(|daemon| daemon.stop_requested)
            .unwrap_or(false)
        {
            break;
        }
        let (report, executed, errors) = run_once(
            &mut os,
            &store,
            args.limit,
            args.execute,
            false,
            args.recover_stale_seconds,
        )?;
        ticks += 1;
        let assigned_count = report.assignments.len();
        let executed_count = executed.len();
        let recovered_count = report.recovered_tasks.len();
        let error_count = errors.len();
        total_assigned += assigned_count;
        total_executed += executed_count;
        total_recovered += recovered_count;
        total_errors += error_count;
        let message = format!(
            "tick {} assigned {} executed {}",
            ticks, assigned_count, executed_count
        );
        store.update(|os| {
            let daemon = os.daemon.get_or_insert_with(|| {
                DaemonState::running(std::process::id(), args.limit, args.execute)
            });
            daemon.status = DaemonStatus::Running;
            daemon.pid = Some(std::process::id());
            daemon.ticks = ticks;
            daemon.last_tick_at = Some(Utc::now());
            daemon.last_message = Some(message.clone());
            os.record(EventKind::DaemonTick, &message);
            Ok::<_, anyhow::Error>(())
        })?;

        if json {
            if tick_reports.len() == DAEMON_JSON_TICK_HISTORY_LIMIT {
                tick_reports.remove(0);
            }
            tick_reports.push(serde_json::json!({
                "tick": ticks,
                "assigned": assigned_count,
                "executed": executed_count,
                "recovered": recovered_count,
                "errors": errors,
            }));
        } else {
            for error in errors {
                eprintln!("could not execute task: {error}");
            }
            if report.recovered_tasks.is_empty() {
                println!("{message}");
            } else {
                println!("{message} recovered {}", report.recovered_tasks.len());
            }
        }

        if ticks < max_ticks {
            std::thread::sleep(Duration::from_millis(args.interval_ms));
        }
    }

    let daemon = store.update(|os| {
        if let Some(daemon) = &mut os.daemon {
            daemon.status = DaemonStatus::Stopped;
            daemon.pid = None;
            daemon.stop_requested = false;
            daemon.last_message = Some(format!("daemon stopped after {ticks} ticks"));
        }
        os.record(
            EventKind::DaemonStopped,
            format!("daemon stopped after {ticks} ticks"),
        );
        Ok::<_, anyhow::Error>(os.daemon.clone())
    })?;
    if json {
        print_json(&serde_json::json!({
            "tick_count": ticks,
            "totals": {
                "assigned": total_assigned,
                "executed": total_executed,
                "recovered": total_recovered,
                "errors": total_errors,
            },
            "ticks": tick_reports,
            "ticks_truncated": ticks > DAEMON_JSON_TICK_HISTORY_LIMIT,
            "tick_history_limit": DAEMON_JSON_TICK_HISTORY_LIMIT,
            "daemon": daemon,
        }))?;
    }
    Ok(())
}

fn stop_daemon(store: Store, json: bool) -> Result<()> {
    let (stopped, daemon) = store.update(|os| {
        let stopped = Runtime::request_daemon_stop(os);
        Ok::<_, anyhow::Error>((stopped, os.daemon.clone()))
    })?;

    if json {
        print_json(&serde_json::json!({
            "stop_requested": stopped,
            "daemon": daemon,
        }))?;
    } else if stopped {
        println!("Stop requested.");
    } else {
        println!("Daemon is not started.");
    }
    Ok(())
}

fn print_status(os: &OperatingSystem, path: String, json: bool) -> Result<()> {
    let pending = os
        .tasks
        .values()
        .filter(|task| task.status == TaskStatus::Pending)
        .count();
    let running = os
        .tasks
        .values()
        .filter(|task| task.status == TaskStatus::Running)
        .count();
    let complete = os
        .tasks
        .values()
        .filter(|task| task.status == TaskStatus::Complete)
        .count();
    let blocked = os
        .tasks
        .values()
        .filter(|task| task.status == TaskStatus::Blocked)
        .count();
    let failed = os
        .tasks
        .values()
        .filter(|task| task.status == TaskStatus::Failed)
        .count();
    let cancelled = os
        .tasks
        .values()
        .filter(|task| task.status == TaskStatus::Cancelled)
        .count();

    let summary = StatusSummary {
        name: &os.name,
        agent_os_version: env!("CARGO_PKG_VERSION"),
        state_path: path,
        agents: os.agents.len(),
        tasks_pending: pending,
        tasks_running: running,
        tasks_blocked: blocked,
        tasks_complete: complete,
        tasks_failed: failed,
        tasks_cancelled: cancelled,
        workflows: os.workflows.len(),
        runs: os.runs.len(),
        tools: os.tools.len(),
        daemon_status: os.daemon.as_ref().map(|daemon| daemon.status.to_string()),
        daemon_ticks: os.daemon.as_ref().map(|daemon| daemon.ticks),
        memories: os.memory.len(),
        events: os.events.len(),
    };

    if json {
        print_json(&summary)?;
        return Ok(());
    }

    println!("{}", os.name);
    println!("agent-os version: {}", summary.agent_os_version);
    println!("state: {}", summary.state_path);
    let color = terminal_colors_enabled();
    let pending_display = color_count(pending, "33", color);
    let running_display = color_count(running, "36", color);
    let blocked_display = color_count(blocked, "35", color);
    let complete_display = color_count(complete, "32", color);
    let failed_display = color_count(failed, "31", color);
    let cancelled_display = color_count(cancelled, "90", color);
    let daemon_display = color_daemon_status(summary.daemon_status.as_deref(), color);
    println!(
        "agents: {} | tools: {} | tasks: {} pending, {} running, {} blocked, {} complete, {} failed, {} cancelled | workflows: {} | runs: {} | daemon: {} | memories: {} | events: {}",
        summary.agents,
        summary.tools,
        pending_display,
        running_display,
        blocked_display,
        complete_display,
        failed_display,
        cancelled_display,
        summary.workflows,
        summary.runs,
        daemon_display,
        summary.memories,
        summary.events
    );
    Ok(())
}

fn terminal_colors_enabled() -> bool {
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    if std::env::var("CLICOLOR_FORCE").is_ok_and(|value| value != "0") {
        return true;
    }
    if std::env::var("CLICOLOR").is_ok_and(|value| value == "0") {
        return false;
    }
    std::io::stdout().is_terminal()
}

fn color_count(value: usize, code: &str, enabled: bool) -> String {
    let text = value.to_string();
    if enabled && value > 0 {
        paint(&text, code)
    } else {
        text
    }
}

fn color_daemon_status(status: Option<&str>, enabled: bool) -> String {
    let status = status.unwrap_or("not-started");
    if !enabled {
        return status.to_owned();
    }
    let code = match status {
        "running" => "32",
        "stopped" | "not-started" => "90",
        _ => "33",
    };
    paint(status, code)
}

fn paint(text: &str, code: &str) -> String {
    format!("\x1b[{code}m{text}\x1b[0m")
}

fn print_metrics(metrics: &Value, json: bool, prometheus: bool) -> Result<()> {
    if json && prometheus {
        bail!("metrics --prometheus cannot be combined with --json");
    }
    if prometheus {
        print!("{}", metrics_prometheus(metrics));
        return Ok(());
    }
    if json {
        print_json(metrics)?;
        return Ok(());
    }

    println!("agent-os metrics");
    println!(
        "state: {} | valid: {} | issues: {}",
        if metric_bool(metrics, "ok") {
            "ok"
        } else {
            "unhealthy"
        },
        metrics
            .get("state_valid")
            .and_then(Value::as_bool)
            .map(yes_no)
            .unwrap_or("n/a"),
        metric_u64(metrics, "state_issue_count")
    );
    println!(
        "agents: {} total, {} online, {} busy, {} paused, {} offline",
        metric_u64(metrics, "agents_total"),
        metric_u64(metrics, "agents_online"),
        metric_u64(metrics, "agents_busy"),
        metric_u64(metrics, "agents_paused"),
        metric_u64(metrics, "agents_offline")
    );
    println!(
        "tasks: {} total, {} pending, {} running, {} blocked, {} complete, {} failed, {} cancelled, oldest queued {} ms",
        metric_u64(metrics, "tasks_total"),
        metric_u64(metrics, "tasks_pending"),
        metric_u64(metrics, "tasks_running"),
        metric_u64(metrics, "tasks_blocked"),
        metric_u64(metrics, "tasks_complete"),
        metric_u64(metrics, "tasks_failed"),
        metric_u64(metrics, "tasks_cancelled"),
        metric_u64(metrics, "oldest_queued_task_age_ms")
    );
    println!(
        "runs: {} total, {} running, {} cancel-requested, {} cancelled, {} success, {} failed, {} rejected, oldest active {} ms",
        metric_u64(metrics, "runs_total"),
        metric_u64(metrics, "runs_running"),
        metric_u64(metrics, "runs_cancel_requested"),
        metric_u64(metrics, "runs_cancelled"),
        metric_u64(metrics, "runs_success"),
        metric_u64(metrics, "runs_failed"),
        metric_u64(metrics, "runs_rejected"),
        metric_u64(metrics, "oldest_active_run_age_ms")
    );
    println!(
        "task queue ages: {} queued, {} ms total",
        metric_u64(metrics, "task_queue_age_ms_count"),
        metric_u64(metrics, "task_queue_age_ms_sum")
    );
    println!(
        "run durations: {} finished, {} ms total",
        metric_u64(metrics, "run_duration_ms_count"),
        metric_u64(metrics, "run_duration_ms_sum")
    );
    println!(
        "tools: {} | workflows: {} | events: {} | memories: {} | daemon: {} | ticks: {}",
        metric_u64(metrics, "tools_total"),
        metric_u64(metrics, "workflows_total"),
        metric_u64(metrics, "events_total"),
        metric_u64(metrics, "memories_total"),
        metric_string(metrics, "daemon_status").unwrap_or("not-started"),
        metrics
            .get("daemon_ticks")
            .and_then(Value::as_u64)
            .map(|ticks| ticks.to_string())
            .unwrap_or_else(|| "-".into())
    );
    Ok(())
}

fn metric_bool(metrics: &Value, key: &str) -> bool {
    metrics.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn metric_u64(metrics: &Value, key: &str) -> u64 {
    metrics.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn metric_string<'a>(metrics: &'a Value, key: &str) -> Option<&'a str> {
    metrics.get(key).and_then(Value::as_str)
}

fn print_agent_detail(agent: &Agent) {
    println!("{} {}", agent.id, agent.status);
    println!("name: {}", agent.name);
    println!("kind: {}", agent.kind);
    println!("model: {}", agent.model.as_deref().unwrap_or("-"));
    println!("capabilities: {}", display_list(&agent.capabilities));
    println!(
        "load: {}/{}",
        agent.current_tasks.len(),
        agent.max_parallel_tasks
    );
    println!(
        "last heartbeat: {}",
        agent
            .last_heartbeat_at
            .map(|timestamp| timestamp.to_rfc3339())
            .unwrap_or_else(|| "-".into())
    );
    println!(
        "lease expires: {}",
        agent
            .lease_expires_at
            .map(|timestamp| timestamp.to_rfc3339())
            .unwrap_or_else(|| "-".into())
    );
}

fn print_workflow_detail(workflow: &Workflow, os: &OperatingSystem) {
    println!("{} {}", workflow.id, workflow.priority);
    println!("objective: {}", workflow.objective);
    println!("created: {}", workflow.created_at.to_rfc3339());
    println!("updated: {}", workflow.updated_at.to_rfc3339());
    println!("tasks:");
    for (stage, task_id) in &workflow.tasks {
        let status = os
            .tasks
            .get(task_id)
            .map(|task| task.status.to_string())
            .unwrap_or_else(|| "missing".into());
        println!("  {}: {} ({})", stage, task_id, status);
    }
}

fn print_workflow_progress(progress: &WorkflowProgress) {
    println!("{} {}", progress.id, progress.priority);
    println!("objective: {}", progress.objective);
    println!(
        "tasks: {}/{} complete | {} pending | {} running | {} blocked | {} failed | {} cancelled | {} missing",
        progress.tasks_complete,
        progress.total_tasks,
        progress.tasks_pending,
        progress.tasks_running,
        progress.tasks_blocked,
        progress.tasks_failed,
        progress.tasks_cancelled,
        progress.tasks_missing
    );
    println!(
        "current stage: {}",
        progress.current_stage.as_deref().unwrap_or("complete")
    );
    println!("stages:");
    for stage in &progress.stages {
        println!(
            "  {}: {} ({})",
            stage.stage,
            stage.task_id,
            stage
                .status
                .as_ref()
                .map(|status| status.to_string())
                .unwrap_or_else(|| "missing".into())
        );
    }
}

fn workflow_task_summary(workflow: &Workflow) -> String {
    workflow
        .tasks
        .iter()
        .map(|(stage, task_id)| format!("{stage}:{task_id}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn print_agents(agents: Vec<&Agent>) {
    let rows = agents
        .into_iter()
        .map(|agent| AgentRow {
            id: agent.id.to_string(),
            kind: agent.kind.to_string(),
            status: agent.status.to_string(),
            model: agent.model.clone().unwrap_or_else(|| "-".into()),
            caps: agent.capabilities.join(", "),
            load: format!("{}/{}", agent.current_tasks.len(), agent.max_parallel_tasks),
        })
        .collect::<Vec<_>>();
    print_table(rows);
}

fn print_tasks(tasks: Vec<&Task>) {
    let rows = tasks
        .into_iter()
        .map(|task| TaskRow {
            id: task.id.to_string(),
            status: task.status.to_string(),
            priority: task.priority.to_string(),
            attempts: format!("{}/{}", task.attempts, task.max_attempts),
            assigned: task
                .assigned_to
                .as_ref()
                .map(AgentId::to_string)
                .unwrap_or_else(|| "-".into()),
            needs: task.required_capabilities.join(", "),
            title: if task.command.is_some() {
                format!("{} [cmd]", task.title)
            } else if task.tool.is_some() {
                format!("{} [tool]", task.title)
            } else if !task.dependencies.is_empty() {
                format!("{} [deps]", task.title)
            } else {
                task.title.clone()
            },
        })
        .collect::<Vec<_>>();
    print_table(rows);
}

fn print_task_detail(task: &Task) {
    println!("{} {}", task.id, task.title);
    println!(
        "status: {} | priority: {} | attempts: {}/{}",
        task.status, task.priority, task.attempts, task.max_attempts
    );
    println!(
        "assigned: {}",
        task.assigned_to
            .as_ref()
            .map(AgentId::to_string)
            .unwrap_or_else(|| "-".into())
    );
    println!("needs: {}", display_list(&task.required_capabilities));
    let dependencies = task
        .dependencies
        .iter()
        .map(TaskId::to_string)
        .collect::<Vec<_>>();
    println!("after: {}", display_list(&dependencies));
    println!("objective: {}", task.objective);
    if let Some(command) = &task.command {
        println!("command: {}", command);
    }
    if let Some(tool) = &task.tool {
        println!("tool: {}", tool.tool_id);
        if !tool.args.is_empty() {
            println!("tool args:");
            for (key, value) in &tool.args {
                println!("  {}={}", key, value);
            }
        }
    }
    if let Some(cwd) = &task.cwd {
        println!("cwd: {}", cwd);
    }
    if !task.plan.is_empty() {
        println!("plan:");
        for (index, step) in task.plan.iter().enumerate() {
            println!("  {}. {}", index + 1, step);
        }
    }
    if let Some(output) = &task.output {
        println!("output: {}", output);
    }
}

fn print_tools(tools: Vec<&ToolDefinition>) {
    let rows = tools
        .into_iter()
        .map(|tool| ToolRow {
            id: tool.id.to_string(),
            kind: tool.kind.to_string(),
            needs: tool.required_capabilities.join(", "),
            command: truncate(&tool.command_template, 64),
            description: truncate(&tool.description, 64),
        })
        .collect::<Vec<_>>();
    print_table(rows);
}

fn print_tool_detail(tool: &ToolDefinition) {
    println!("{} {}", tool.id, tool.name);
    println!("kind: {}", tool.kind);
    println!("needs: {}", display_list(&tool.required_capabilities));
    println!("command template: {}", tool.command_template);
    println!("cwd: {}", tool.default_cwd.as_deref().unwrap_or("-"));
    if !tool.description.is_empty() {
        println!("description: {}", tool.description);
    }
}

fn print_runs(runs: Vec<&RunRecord>) {
    let rows = runs
        .into_iter()
        .map(|run| RunRow {
            id: run.id.to_string(),
            task: run.task_id.to_string(),
            agent: run
                .agent_id
                .as_ref()
                .map(AgentId::to_string)
                .unwrap_or_else(|| "-".into()),
            status: run.status.to_string(),
            exit: run
                .exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "-".into()),
            command: truncate(&run.command, 64),
        })
        .collect::<Vec<_>>();
    print_table(rows);
}

fn print_run_detail(run: &RunRecord) {
    println!("{} {}", run.id, run.status);
    println!("task: {}", run.task_id);
    println!(
        "agent: {}",
        run.agent_id
            .as_ref()
            .map(AgentId::to_string)
            .unwrap_or_else(|| "-".into())
    );
    println!("cwd: {}", run.cwd);
    println!("command: {}", run.command);
    println!(
        "exit: {}",
        run.exit_code
            .map(|code| code.to_string())
            .unwrap_or_else(|| "-".into())
    );
    if let Some(path) = &run.log_path {
        println!("log: {}", path);
    }
}

fn print_artifacts(artifacts: &[Value]) {
    if artifacts.is_empty() {
        println!("No artifacts");
        return;
    }
    for artifact in artifacts {
        println!(
            "{} {} bytes={} exists={} readable={} checksum={}",
            artifact["id"].as_str().unwrap_or("-"),
            artifact["name"].as_str().unwrap_or("-"),
            artifact["bytes"]
                .as_u64()
                .map(|bytes| bytes.to_string())
                .unwrap_or_else(|| "-".into()),
            artifact["exists"].as_bool().unwrap_or(false),
            artifact["readable"].as_bool().unwrap_or(false),
            artifact["checksum"].as_str().unwrap_or("-")
        );
        if let Some(error) = artifact["error"].as_str() {
            println!("  error: {error}");
        }
    }
}

fn print_artifact_body(body: &Value) {
    if let Some(error) = body["error"].as_str() {
        println!(
            "artifact {} unavailable",
            body["artifact_id"].as_str().unwrap_or("-")
        );
        println!("error: {error}");
        return;
    }
    println!(
        "artifact {} bytes={} checksum={} truncated={}",
        body["artifact_id"].as_str().unwrap_or("-"),
        body["bytes"]
            .as_u64()
            .map(|bytes| bytes.to_string())
            .unwrap_or_else(|| "-".into()),
        body["checksum"].as_str().unwrap_or("-"),
        body["truncated"].as_bool().unwrap_or(false)
    );
    if let Some(text) = body["body"].as_str() {
        print!("{text}");
        if !text.ends_with('\n') {
            println!();
        }
    }
}

fn run_artifact_entries(run: &RunRecord) -> Vec<Value> {
    run.artifacts
        .iter()
        .enumerate()
        .map(|(index, artifact)| {
            let id = run_artifact_id(run, index, artifact);
            let status = run_artifact_file_status(artifact);
            serde_json::json!({
                "id": id,
                "index": index,
                "name": run_artifact_kind_name(&artifact.kind),
                "artifact": artifact,
                "exists": status.exists,
                "readable": status.readable,
                "bytes": status.bytes.or(artifact.bytes),
                "checksum": status.checksum,
                "error": status.error,
            })
        })
        .collect()
}

fn run_artifact_read_json(
    run: &RunRecord,
    index: usize,
    artifact_id: &str,
    artifact: &RunArtifact,
    tail_bytes: Option<usize>,
) -> Value {
    match run_artifact_body(run, artifact, tail_bytes) {
        Ok(body) => serde_json::json!({
            "run_id": run.id,
            "artifact_id": artifact_id,
            "index": index,
            "artifact": artifact,
            "exists": true,
            "bytes": body.bytes,
            "checksum": body.checksum,
            "content_type": body.content_type,
            "tail_bytes": tail_bytes,
            "truncated": body.truncated,
            "body": body.body,
            "error": Value::Null,
        }),
        Err(error) => serde_json::json!({
            "run_id": run.id,
            "artifact_id": artifact_id,
            "index": index,
            "artifact": artifact,
            "exists": false,
            "bytes": artifact.bytes,
            "checksum": Value::Null,
            "content_type": artifact.content_type,
            "tail_bytes": tail_bytes,
            "truncated": false,
            "body": Value::Null,
            "error": error,
        }),
    }
}

struct ArtifactBody {
    body: String,
    bytes: u64,
    checksum: String,
    content_type: String,
    truncated: bool,
}

fn run_artifact_body(
    run: &RunRecord,
    artifact: &RunArtifact,
    tail_bytes: Option<usize>,
) -> Result<ArtifactBody, String> {
    if matches!(artifact.kind, RunArtifactKind::Summary) && artifact.path.starts_with("run:") {
        let body = format!(
            "run: {}\nstatus: {}\ntask: {}\nagent: {}\ncommand: {}\ncwd: {}\nexit: {}\nstarted: {}\nfinished: {}\n",
            run.id,
            run.status,
            run.task_id,
            run.agent_id
                .as_ref()
                .map(AgentId::to_string)
                .unwrap_or_else(|| "-".into()),
            run.command,
            run.cwd,
            run.exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "-".into()),
            run.started_at.to_rfc3339(),
            run.finished_at
                .map(|at| at.to_rfc3339())
                .unwrap_or_else(|| "-".into()),
        );
        return Ok(artifact_body_from_string(
            body,
            tail_bytes,
            artifact
                .content_type
                .clone()
                .unwrap_or_else(|| "text/plain".into()),
        ));
    }

    let bytes = std::fs::read(&artifact.path)
        .map_err(|error| format!("could not read artifact {}: {error}", artifact.path))?;
    let body = String::from_utf8_lossy(&bytes).into_owned();
    Ok(artifact_body_from_string(
        body,
        tail_bytes,
        artifact
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".into()),
    ))
}

fn artifact_body_from_string(
    body: String,
    tail_bytes: Option<usize>,
    content_type: String,
) -> ArtifactBody {
    let bytes = body.len() as u64;
    let checksum = fnv1a64_checksum(body.as_bytes());
    let truncated = text_tail_was_truncated(&body, tail_bytes);
    let body = tail_text_by_bytes(&body, tail_bytes);
    ArtifactBody {
        body,
        bytes,
        checksum,
        content_type,
        truncated,
    }
}

struct ArtifactFileStatus {
    exists: bool,
    readable: bool,
    bytes: Option<u64>,
    checksum: Option<String>,
    error: Option<String>,
}

fn run_artifact_file_status(artifact: &RunArtifact) -> ArtifactFileStatus {
    if matches!(artifact.kind, RunArtifactKind::Summary) && artifact.path.starts_with("run:") {
        return ArtifactFileStatus {
            exists: true,
            readable: true,
            bytes: None,
            checksum: None,
            error: None,
        };
    }
    match std::fs::read(&artifact.path) {
        Ok(bytes) => ArtifactFileStatus {
            exists: true,
            readable: true,
            bytes: Some(bytes.len() as u64),
            checksum: Some(fnv1a64_checksum(&bytes)),
            error: None,
        },
        Err(error) => ArtifactFileStatus {
            exists: std::path::Path::new(&artifact.path).exists(),
            readable: false,
            bytes: artifact.bytes,
            checksum: None,
            error: Some(error.to_string()),
        },
    }
}

fn fnv1a64_checksum(bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("fnv1a64:{hash:016x}")
}

fn find_run_artifact<'a>(
    run: &'a RunRecord,
    artifact_id: &str,
) -> Option<(usize, &'a RunArtifact)> {
    let artifact_id = artifact_id.trim();
    if artifact_id.is_empty() {
        return None;
    }
    if let Ok(index) = artifact_id.parse::<usize>() {
        return run.artifacts.get(index).map(|artifact| (index, artifact));
    }
    run.artifacts.iter().enumerate().find(|(index, artifact)| {
        run_artifact_id(run, *index, artifact) == artifact_id
            || run_artifact_kind_name(&artifact.kind) == artifact_id
    })
}

fn run_artifact_id(run: &RunRecord, index: usize, artifact: &RunArtifact) -> String {
    let kind = run_artifact_kind_name(&artifact.kind);
    let count = run
        .artifacts
        .iter()
        .filter(|candidate| candidate.kind == artifact.kind)
        .count();
    if count == 1 {
        kind.to_owned()
    } else {
        format!("{kind}-{index}")
    }
}

fn run_artifact_kind_name(kind: &RunArtifactKind) -> &'static str {
    match kind {
        RunArtifactKind::Stdout => "stdout",
        RunArtifactKind::Stderr => "stderr",
        RunArtifactKind::Summary => "summary",
        RunArtifactKind::Diff => "diff",
        RunArtifactKind::File => "file",
    }
}

fn print_replay(
    run: &RunRecord,
    task: Option<&Task>,
    events: &[&Event],
    log: Option<&str>,
    log_error: Option<&str>,
) {
    println!("replay {}", run.id);
    println!("status: {}", run.status);
    println!("task: {}", run.task_id);
    if let Some(task) = task {
        println!("title: {}", task.title);
        println!("task status: {}", task.status);
        println!("objective: {}", task.objective);
    }
    println!(
        "agent: {}",
        run.agent_id
            .as_ref()
            .map(AgentId::to_string)
            .unwrap_or_else(|| "-".into())
    );
    println!("command: {}", run.command);
    println!("cwd: {}", run.cwd);
    println!(
        "exit: {}",
        run.exit_code
            .map(|code| code.to_string())
            .unwrap_or_else(|| "-".into())
    );
    println!("started: {}", run.started_at.to_rfc3339());
    println!(
        "finished: {}",
        run.finished_at
            .map(|at| at.to_rfc3339())
            .unwrap_or_else(|| "-".into())
    );

    if !events.is_empty() {
        println!("events:");
        for event in events {
            println!(
                "  {} {} {}",
                event.at.format("%Y-%m-%d %H:%M:%S"),
                event.kind,
                event.message
            );
        }
    }

    if let Some(log) = log {
        println!("log:");
        print!("{log}");
        if !log.ends_with('\n') {
            println!();
        }
    } else if let Some(log_error) = log_error {
        println!("log unavailable: {log_error}");
    }
}

fn print_debug(
    run: &RunRecord,
    task: Option<&Task>,
    agent_id: Option<&str>,
    workflows: &[&Workflow],
    approvals: &[&ApprovalRequest],
    events: &[&Event],
    log: Option<&str>,
    log_error: Option<&str>,
    artifact_status: &[Value],
    log_path: &std::path::Path,
) {
    println!("debug {}", run.id);
    println!("log path: {}", log_path.display());
    println!("agent: {}", agent_id.unwrap_or("-"));
    println!("workflows: {}", workflows.len());
    for workflow in workflows {
        println!("  {} {}", workflow.id, workflow.objective);
    }
    println!("approvals: {}", approvals.len());
    for approval in approvals {
        println!(
            "  {} {:?} {}",
            approval.id, approval.status, approval.action
        );
    }
    println!("artifacts: {}", artifact_status.len());
    for entry in artifact_status {
        let path = entry["artifact"]["path"].as_str().unwrap_or("-");
        let kind = entry["artifact"]["kind"].as_str().unwrap_or("-");
        let exists = entry["exists"].as_bool().unwrap_or(false);
        println!("  {kind} {path} exists={exists}");
    }
    print_replay(run, task, events, log, log_error);
}

fn print_daemon(daemon: Option<&DaemonState>) {
    let Some(daemon) = daemon else {
        println!("daemon: not-started");
        return;
    };

    println!("daemon: {}", daemon.status);
    println!(
        "pid: {}",
        daemon
            .pid
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "-".into())
    );
    println!("ticks: {}", daemon.ticks);
    println!("limit: {}", daemon.limit);
    println!("execute: {}", yes_no(daemon.execute));
    println!("stop requested: {}", yes_no(daemon.stop_requested));
    println!(
        "last tick: {}",
        daemon
            .last_tick_at
            .map(|at| at.to_rfc3339())
            .unwrap_or_else(|| "-".into())
    );
    println!("message: {}", daemon.last_message.as_deref().unwrap_or("-"));
}

fn print_memory(records: &[MemoryRecord]) {
    let rows = records
        .iter()
        .map(|record| MemoryRow {
            id: record.id.clone(),
            topic: record.topic.clone(),
            visibility: record.visibility.to_string(),
            scope: record.scope.clone().unwrap_or_else(|| "-".into()),
            tags: record.tags.join(", "),
            body: truncate(&record.body, 84),
        })
        .collect::<Vec<_>>();
    print_table(rows);
}

fn print_memory_recall(hits: &[MemoryRecallHit]) {
    let rows = hits
        .iter()
        .map(|hit| MemoryRecallRow {
            id: hit.record.id.clone(),
            score: hit.score,
            topic: hit.record.topic.clone(),
            visibility: hit.record.visibility.to_string(),
            scope: hit.record.scope.clone().unwrap_or_else(|| "-".into()),
            tags: hit.record.tags.join(", "),
            snippet: truncate(&hit.snippet, 120),
        })
        .collect::<Vec<_>>();
    print_table(rows);
}

fn print_memory_detail(record: &MemoryRecord) {
    println!("{}", record.id);
    println!("topic: {}", record.topic);
    println!("visibility: {}", record.visibility);
    println!("scope: {}", record.scope.as_deref().unwrap_or("-"));
    println!("tags: {}", display_list(&record.tags));
    println!("created: {}", record.created_at.to_rfc3339());
    println!("updated: {}", record.updated_at.to_rfc3339());
    println!("body: {}", record.body);
}

fn print_events(events: &[Event], limit: usize) {
    let rows = events
        .iter()
        .rev()
        .take(limit)
        .map(|event| EventRow {
            at: event.at.format("%Y-%m-%d %H:%M:%S").to_string(),
            kind: event.kind.to_string(),
            message: event.message.clone(),
        })
        .collect::<Vec<_>>();
    print_table(rows);
}

fn print_table<T: Tabled>(rows: Vec<T>) {
    if rows.is_empty() {
        println!("No records.");
        return;
    }
    let mut table = Table::new(rows);
    table.with(Style::rounded());
    println!("{table}");
}

fn truncate(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_owned();
    }
    let prefix = value
        .chars()
        .take(max.saturating_sub(3))
        .collect::<String>();
    format!("{prefix}...")
}

fn display_list(values: &[String]) -> String {
    if values.is_empty() {
        "-".into()
    } else {
        values.join(", ")
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn parse_key_values(values: Vec<String>) -> Result<BTreeMap<String, String>> {
    let mut parsed = BTreeMap::new();
    for value in values {
        let Some((key, value)) = value.split_once('=') else {
            bail!("tool argument must use KEY=VALUE syntax: {}", value);
        };
        if key.trim().is_empty() {
            bail!("tool argument key cannot be empty");
        }
        if !is_valid_tool_arg_key(key) {
            bail!("invalid tool argument key: {key}");
        }
        if parsed.insert(key.to_owned(), value.to_owned()).is_some() {
            bail!("duplicate tool argument key: {key}");
        }
    }
    Ok(parsed)
}

fn validate_secret_env_args(values: &BTreeMap<String, String>) -> Result<()> {
    for (key, reference) in values {
        if reference.trim().is_empty() {
            bail!(
                "secret tool argument `{key}` must name an environment variable or backend:id reference"
            );
        }
        if !is_valid_secret_reference(reference) {
            bail!(
                "secret tool argument `{key}` must name a valid environment variable or backend:id reference"
            );
        }
    }
    Ok(())
}

fn list_workflows(store: Store, args: WorkflowListArgs, json: bool) -> Result<()> {
    validate_workflow_list_limit(args.limit)?;
    let priority = args.priority.as_deref().map(parse_priority).transpose()?;
    let task = args.task.as_deref().map(parse_task_id_arg).transpose()?;
    let since = args
        .since
        .as_deref()
        .map(|value| parse_filter_timestamp(value, "workflow", "since"))
        .transpose()?;
    let until = args
        .until
        .as_deref()
        .map(|value| parse_filter_timestamp(value, "workflow", "until"))
        .transpose()?;
    if let Some(query) = &args.query {
        validate_workflow_query(query)?;
    }
    let query = args.query.as_ref().map(|query| query.to_ascii_lowercase());
    let os = store.load().with_context(|| state_init_hint(&store))?;
    let mut workflows = os
        .workflows
        .values()
        .filter(|workflow| {
            priority
                .as_ref()
                .map(|priority| &workflow.priority == priority)
                .unwrap_or(true)
                && task
                    .as_ref()
                    .map(|task| {
                        workflow
                            .tasks
                            .values()
                            .any(|workflow_task| workflow_task == task)
                    })
                    .unwrap_or(true)
                && since
                    .as_ref()
                    .map(|since| workflow.updated_at >= *since)
                    .unwrap_or(true)
                && until
                    .as_ref()
                    .map(|until| workflow.updated_at <= *until)
                    .unwrap_or(true)
                && query
                    .as_ref()
                    .map(|query| workflow_matches_query(workflow, query))
                    .unwrap_or(true)
        })
        .cloned()
        .collect::<Vec<_>>();
    workflows.sort_by_key(|workflow| std::cmp::Reverse(workflow.updated_at));
    if let Some(limit) = args.limit {
        workflows.truncate(limit);
    }
    if json {
        print_json(&workflows)?;
        return Ok(());
    }
    let rows = workflows
        .into_iter()
        .map(|workflow| WorkflowRow {
            id: workflow.id.to_string(),
            priority: workflow.priority.to_string(),
            objective: truncate(&workflow.objective, 48),
            tasks: workflow_task_summary(&workflow),
            updated: workflow.updated_at.to_rfc3339(),
        })
        .collect::<Vec<_>>();
    if rows.is_empty() {
        println!("No workflows");
    } else {
        println!("{}", Table::new(rows).with(Style::rounded()));
    }
    Ok(())
}

fn show_workflow(store: Store, args: WorkflowShowArgs, json: bool) -> Result<()> {
    let id = parse_workflow_id_arg(&args.id)?;
    let os = store.load().with_context(|| state_init_hint(&store))?;
    let workflow = os
        .workflows
        .get(&id)
        .with_context(|| format!("workflow not found: {id}"))?;
    if json {
        print_json(workflow)?;
    } else {
        print_workflow_detail(workflow, &os);
    }
    Ok(())
}

fn show_workflow_status(store: Store, args: WorkflowShowArgs, json: bool) -> Result<()> {
    let id = parse_workflow_id_arg(&args.id)?;
    let os = store.load().with_context(|| state_init_hint(&store))?;
    let progress = os
        .workflow_progress(&id)
        .with_context(|| format!("workflow not found: {id}"))?;
    if json {
        print_json(&progress)?;
    } else {
        print_workflow_progress(&progress);
    }
    Ok(())
}

fn add_workflow_task(store: Store, args: WorkflowAddTaskArgs, json: bool) -> Result<()> {
    let id = parse_workflow_id_arg(&args.id)?;
    validate_workflow_stage(&args.stage)?;
    validate_task_title(&args.title)?;
    if let Some(objective) = &args.objective {
        validate_task_objective(objective)?;
    }
    if let Some(command) = &args.command {
        validate_task_command(command)?;
    }
    validate_capability_values(
        "task required capabilities",
        &args.required_capabilities,
        false,
    )?;
    for stage in &args.dependencies {
        validate_workflow_stage(stage)?;
    }
    let priority = args.priority.as_deref().map(parse_priority).transpose()?;
    let stage = args.stage;
    let (task_id, task, progress) = store.update(|os| {
        let workflow = os
            .workflows
            .get(&id)
            .cloned()
            .with_context(|| format!("workflow not found: {id}"))?;
        if workflow.tasks.contains_key(&stage) {
            bail!("workflow {id} already has stage {stage}");
        }
        let dependencies = args
            .dependencies
            .iter()
            .map(|stage| {
                workflow
                    .tasks
                    .get(stage)
                    .cloned()
                    .with_context(|| format!("workflow {id} stage not found: {stage}"))
            })
            .collect::<Result<Vec<_>>>()?;
        let title = args.title;
        let objective = args.objective.unwrap_or_else(|| title.clone());
        let mut task = Task::new(
            title,
            objective,
            priority.unwrap_or(workflow.priority),
            args.required_capabilities,
        );
        os.ensure_unique_task_id(&mut task);
        task.command = args.command;
        task.dependencies = dependencies;
        let task_id = task.id.clone();
        os.create_task(task);
        let workflow = os
            .workflows
            .get_mut(&id)
            .with_context(|| format!("workflow not found: {id}"))?;
        workflow.tasks.insert(stage.clone(), task_id.clone());
        workflow.updated_at = Utc::now();
        os.record(
            EventKind::WorkflowUpdated,
            format!("added workflow {id} stage {stage} as task {task_id}"),
        );
        let task = os
            .tasks
            .get(&task_id)
            .cloned()
            .with_context(|| format!("task not found after creation: {task_id}"))?;
        let progress = os
            .workflow_progress(&id)
            .with_context(|| format!("workflow not found: {id}"))?;
        Ok::<_, anyhow::Error>((task_id, task, progress))
    })?;
    if json {
        print_json(&serde_json::json!({
            "id": id,
            "stage": stage,
            "task_id": task_id,
            "task": task,
            "progress": progress,
        }))?;
    } else {
        println!("Added workflow stage {} as task {}", stage, task_id);
    }
    Ok(())
}

fn link_workflow_stages(store: Store, args: WorkflowEdgeArgs, json: bool) -> Result<()> {
    edit_workflow_edge(store, args, true, json)
}

fn unlink_workflow_stages(store: Store, args: WorkflowEdgeArgs, json: bool) -> Result<()> {
    edit_workflow_edge(store, args, false, json)
}

fn edit_workflow_edge(store: Store, args: WorkflowEdgeArgs, add: bool, json: bool) -> Result<()> {
    let id = parse_workflow_id_arg(&args.id)?;
    validate_workflow_stage(&args.from)?;
    validate_workflow_stage(&args.to)?;
    if args.from == args.to {
        bail!("workflow stage cannot depend on itself");
    }
    let from_stage = args.from;
    let to_stage = args.to;
    let progress = store.update(|os| {
        let (from_task, to_task) = workflow_stage_task_pair(os, &id, &from_stage, &to_stage)?;
        let current = os
            .tasks
            .get(&to_task)
            .with_context(|| format!("task not found: {to_task}"))?
            .dependencies
            .clone();
        let mut dependencies = current;
        if add {
            if !dependencies
                .iter()
                .any(|dependency| dependency == &from_task)
            {
                dependencies.push(from_task.clone());
            }
        } else {
            dependencies.retain(|dependency| dependency != &from_task);
        }
        Runtime::set_task_dependencies(os, &to_task, dependencies).map_err(anyhow::Error::from)?;
        let workflow = os
            .workflows
            .get_mut(&id)
            .with_context(|| format!("workflow not found: {id}"))?;
        workflow.updated_at = Utc::now();
        os.record(
            EventKind::WorkflowUpdated,
            format!(
                "{} dependency {} -> {} in workflow {}",
                if add { "linked" } else { "unlinked" },
                from_stage,
                to_stage,
                id
            ),
        );
        os.workflow_progress(&id)
            .with_context(|| format!("workflow not found: {id}"))
    })?;
    if json {
        print_json(&serde_json::json!({
            "id": id,
            "from": from_stage,
            "to": to_stage,
            "linked": add,
            "progress": progress,
        }))?;
    } else {
        println!(
            "{} workflow dependency {} -> {}",
            if add { "Linked" } else { "Unlinked" },
            from_stage,
            to_stage
        );
    }
    Ok(())
}

fn pause_workflow(store: Store, args: WorkflowNoteArgs, json: bool) -> Result<()> {
    validate_optional_text("note", args.note.as_deref())?;
    transition_workflow_tasks(
        store,
        args,
        json,
        "paused",
        |status| matches!(status, TaskStatus::Pending),
        Runtime::block_task,
    )
}

fn resume_workflow(store: Store, args: WorkflowNoteArgs, json: bool) -> Result<()> {
    validate_optional_text("note", args.note.as_deref())?;
    transition_workflow_tasks(
        store,
        args,
        json,
        "resumed",
        |status| matches!(status, TaskStatus::Blocked),
        Runtime::unblock_task,
    )
}

fn retry_workflow(store: Store, args: WorkflowNoteArgs, json: bool) -> Result<()> {
    validate_optional_text("note", args.note.as_deref())?;
    transition_workflow_tasks(
        store,
        args,
        json,
        "retried",
        |status| {
            matches!(
                status,
                TaskStatus::Blocked | TaskStatus::Failed | TaskStatus::Cancelled
            )
        },
        Runtime::retry_task,
    )
}

fn transition_workflow_tasks<F, T>(
    store: Store,
    args: WorkflowNoteArgs,
    json: bool,
    action: &str,
    should_transition: F,
    transition: T,
) -> Result<()>
where
    F: Fn(&TaskStatus) -> bool,
    T: Fn(&mut OperatingSystem, &TaskId, Option<String>) -> Result<(), agent_os::RuntimeError>,
{
    let id = parse_workflow_id_arg(&args.id)?;
    let note = args.note;
    let affected = store.update(|os| {
        let workflow = os
            .workflows
            .get(&id)
            .cloned()
            .with_context(|| format!("workflow not found: {id}"))?;
        let mut affected = Vec::new();
        for task_id in workflow.tasks.values() {
            let status = os
                .tasks
                .get(task_id)
                .with_context(|| format!("task not found: {task_id}"))?
                .status
                .clone();
            if should_transition(&status) {
                transition(os, task_id, note.clone()).map_err(anyhow::Error::from)?;
                affected.push(task_id.clone());
            }
        }
        if !affected.is_empty() {
            let workflow = os
                .workflows
                .get_mut(&id)
                .with_context(|| format!("workflow not found: {id}"))?;
            workflow.updated_at = Utc::now();
            os.record(
                EventKind::WorkflowUpdated,
                format!("{action} {} workflow task(s) for {}", affected.len(), id),
            );
        }
        Ok::<_, anyhow::Error>(affected)
    })?;
    let os = store.load().with_context(|| state_init_hint(&store))?;
    let progress = os
        .workflow_progress(&id)
        .with_context(|| format!("workflow not found: {id}"))?;
    if json {
        print_json(&serde_json::json!({
            "id": id,
            "action": action,
            "affected_tasks": affected,
            "progress": progress,
        }))?;
    } else {
        println!("{} {} workflow task(s) for {}", action, affected.len(), id);
    }
    Ok(())
}

fn workflow_stage_task_pair(
    os: &OperatingSystem,
    workflow_id: &WorkflowId,
    from_stage: &str,
    to_stage: &str,
) -> Result<(TaskId, TaskId)> {
    let workflow = os
        .workflows
        .get(workflow_id)
        .with_context(|| format!("workflow not found: {workflow_id}"))?;
    let from_task = workflow
        .tasks
        .get(from_stage)
        .cloned()
        .with_context(|| format!("workflow {workflow_id} stage not found: {from_stage}"))?;
    let to_task = workflow
        .tasks
        .get(to_stage)
        .cloned()
        .with_context(|| format!("workflow {workflow_id} stage not found: {to_stage}"))?;
    Ok((from_task, to_task))
}

fn run_workflow(store: Store, args: WorkflowRunArgs, json: bool) -> Result<()> {
    let id = parse_workflow_id_arg(&args.id)?;
    let os = store.load().with_context(|| state_init_hint(&store))?;
    if !os.workflows.contains_key(&id) {
        bail!("workflow not found: {id}");
    }
    drop(os);

    let (runs, errors) = execute_workflow_stages(&store, &id, args.all)?;
    let os = store.load().with_context(|| state_init_hint(&store))?;
    let progress = os
        .workflow_progress(&id)
        .with_context(|| format!("workflow not found: {id}"))?;

    if json {
        print_json(&serde_json::json!({
            "id": id,
            "progress": progress,
            "runs": runs,
            "errors": errors,
        }))?;
    } else if runs.is_empty() && errors.is_empty() {
        println!("No runnable workflow stages found.");
    } else {
        for error in errors {
            eprintln!("could not execute workflow task: {error}");
        }
        for run in runs {
            println!(
                "Executed run {} for workflow {} task {}: {}",
                run.id, id, run.task_id, run.status
            );
        }
    }
    Ok(())
}

fn cancel_workflow(store: Store, args: WorkflowCancelArgs, json: bool) -> Result<()> {
    validate_optional_text("note", args.note.as_deref())?;
    let id = parse_workflow_id_arg(&args.id)?;
    let note = args.note;
    let cancelled_tasks = store
        .update(|os| Runtime::cancel_workflow(os, &id, note.clone()).map_err(anyhow::Error::from))
        .with_context(|| state_init_hint(&store))?;
    let os = store.load().with_context(|| state_init_hint(&store))?;
    let progress = os
        .workflow_progress(&id)
        .with_context(|| format!("workflow not found: {id}"))?;

    if json {
        print_json(&serde_json::json!({
            "id": id,
            "cancelled_tasks": cancelled_tasks,
            "progress": progress,
        }))?;
    } else {
        println!(
            "Cancelled {} workflow task(s) for {}.",
            cancelled_tasks.len(),
            id
        );
    }
    Ok(())
}

fn remove_workflow(store: Store, args: WorkflowShowArgs, json: bool) -> Result<()> {
    let id = parse_workflow_id_arg(&args.id)?;
    let removed = store.update(|os| {
        os.remove_workflow(&id)
            .with_context(|| format!("workflow not found: {id}"))
    })?;
    if json {
        print_json(&serde_json::json!({
            "id": id,
            "removed": true,
            "workflow": removed,
        }))?;
    } else {
        println!("Removed workflow {}", id);
    }
    Ok(())
}

fn reject_secret_like_plain_args(
    values: &BTreeMap<String, String>,
    redacted_patterns: &[String],
) -> Result<()> {
    for key in values.keys() {
        if key_matches_patterns(key, redacted_patterns) {
            bail!(
                "tool argument `{}` looks secret-like; use --secret-arg {}=ENV_VAR instead",
                key,
                key
            );
        }
    }
    Ok(())
}

fn validate_task_dependencies(
    os: &OperatingSystem,
    dependencies: Vec<String>,
) -> Result<Vec<TaskId>> {
    let dependencies = validate_task_dependencies_for_task(None, dependencies)?;
    for dependency in &dependencies {
        if !os.tasks.contains_key(dependency) {
            bail!("dependency task not found: {}", dependency);
        }
    }
    Ok(dependencies)
}

fn validate_task_dependencies_for_task(
    task_id: Option<&TaskId>,
    dependencies: Vec<String>,
) -> Result<Vec<TaskId>> {
    let mut seen = BTreeSet::new();
    dependencies
        .into_iter()
        .map(|dependency| {
            validate_task_id(&dependency, "dependency task id")?;
            let id = TaskId::from_slug(dependency);
            if task_id.map(|task_id| &id == task_id).unwrap_or(false) {
                bail!("task cannot depend on itself: {}", id);
            }
            if !seen.insert(id.clone()) {
                bail!("duplicate dependency task: {}", id);
            }
            Ok(id)
        })
        .collect()
}

fn key_matches_patterns(key: &str, patterns: &[String]) -> bool {
    let key = key.to_ascii_uppercase();
    patterns.iter().any(|pattern| {
        let pattern = pattern.trim();
        !pattern.is_empty() && key.contains(&pattern.to_ascii_uppercase())
    })
}

fn print_json<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn print_task_mutation_json(id: &TaskId, task: &Task) -> Result<()> {
    print_json(&serde_json::json!({
        "id": id,
        "task": task,
    }))
}

#[allow(dead_code)]
fn _normalize_for_cli(values: Vec<String>) -> Vec<String> {
    normalize_list(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[cfg(unix)]
    #[test]
    fn mcp_child_timeout_terminates_descendant_processes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pid_path = dir.path().join("mcp-grandchild.pid");
        let script = format!(
            "sleep 60 & printf '%s\\n' \"$!\" > {}; while :; do sleep 1; done",
            pid_path.display()
        );
        let server = McpServer {
            id: "timeout-test".into(),
            command: "sh".into(),
            args: vec!["-c".into(), script],
            env: BTreeMap::new(),
            enabled: true,
        };

        let error = mcp_child_request_with_timeout(
            &server,
            "tools/list",
            serde_json::json!({}),
            Duration::from_millis(250),
        )
        .expect_err("mcp child should time out");

        assert!(error.contains("timed out"), "{error}");
        let grandchild_pid = std::fs::read_to_string(&pid_path)
            .expect("grandchild pid")
            .trim()
            .parse::<u32>()
            .expect("pid");
        let exited = wait_for_pid_exit(grandchild_pid, Duration::from_secs(2));
        if !exited {
            let _ = std::process::Command::new("kill")
                .arg("-KILL")
                .arg(grandchild_pid.to_string())
                .status();
        }
        assert!(
            exited,
            "mcp grandchild process {grandchild_pid} survived timeout cleanup"
        );
    }

    #[cfg(unix)]
    fn wait_for_pid_exit(pid: u32, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if !pid_exists(pid) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        !pid_exists(pid)
    }

    #[cfg(unix)]
    fn pid_exists(pid: u32) -> bool {
        std::process::Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }
}
