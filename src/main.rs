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
    AppConfig, discover_config, load_config, validate_seed_config as validate_seed_config_impl,
    write_default_config,
};
use agent_os::executor::CommandExecutor;
use agent_os::models::{
    is_valid_env_var_name, memory_matches_query, normalize_list, tail_text_by_bytes,
    text_tail_was_truncated,
};
use agent_os::tools::is_valid_tool_arg_key;
use agent_os::{
    Agent, AgentId, AgentKind, AgentStatus, AgentUpdate, ApiServer, DaemonState, DaemonStatus,
    Event, EventKind, LaunchdService, LaunchdServiceOptions, MemoryRecord, OperatingSystem,
    Priority, RunId, RunRecord, RunStatus, Runtime, RuntimeReport, Scheduler, Store, Task, TaskId,
    TaskStatus, TaskUpdate, ToolDefinition, ToolId, ToolInvocation, ToolKind, ToolUpdate, Workflow,
    WorkflowId, WorkflowProgress, build_launchd_service as build_launchd_service_definition,
    default_launchd_plist_path, install_launchd_service as install_launchd_service_definition,
    metrics_json, metrics_unavailable_json, openapi_schema, repair_state, resolve_launchd_domain,
    run_launchctl, uninstall_launchd_service as uninstall_launchd_service_definition,
    validate_service_control_inputs, validate_state, validate_tool_invocation,
    validate_tool_template,
};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use clap::{Args, CommandFactory, Parser, Subcommand};
use clap_complete::Shell;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::net::SocketAddr;
use std::time::Duration;
use tabled::{Table, Tabled, settings::Style};

const DAEMON_JSON_TICK_HISTORY_LIMIT: usize = 1000;

#[derive(Parser)]
#[command(name = "agent-os")]
#[command(about = "A local operating system for coordinating AI agents.")]
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
    Metrics,
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
struct ConfigInitArgs {
    #[arg(long, help = "Overwrite an existing config file")]
    force: bool,
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
    #[command(about = "List recent memory records")]
    List(MemoryListArgs),
    #[command(about = "Show one memory record")]
    Show(MemoryShowArgs),
    #[command(about = "Update a memory record")]
    Update(MemoryUpdateArgs),
    #[command(about = "Remove a memory record")]
    Remove(MemoryShowArgs),
}

#[derive(Args)]
struct MemoryAddArgs {
    topic: String,
    body: String,
    #[arg(long = "tag", value_delimiter = ',')]
    tags: Vec<String>,
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
}

#[derive(Args)]
struct MemoryShowArgs {
    id: String,
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
    #[command(about = "Request cancellation for a running run")]
    Cancel(RunShowArgs),
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
    #[command(about = "Install the launchd service plist")]
    Install(ServiceLaunchdArgs),
    #[command(about = "Uninstall the launchd service plist")]
    Uninstall(ServiceUninstallArgs),
    #[command(about = "Start the launchd service")]
    Start(ServiceControlArgs),
    #[command(about = "Stop the launchd service")]
    Stop(ServiceControlArgs),
    #[command(about = "Show launchd service status")]
    Status(ServiceStatusArgs),
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
struct ServiceUninstallArgs {
    #[arg(long, default_value = "com.infinite-apps.agent-os")]
    label: String,
    #[arg(long, help = "LaunchAgent plist path; defaults from --label")]
    plist_path: Option<std::path::PathBuf>,
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
        help = "Allow serving an unauthenticated API on a non-loopback address"
    )]
    unsafe_no_token: bool,
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
    tags: String,
    body: String,
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
            let os = store.load().context("run `agent-os init` first")?;
            print_status(&os, store.path().display().to_string(), json)
        }
        Command::Metrics => {
            let metrics = match store.load() {
                Ok(os) => metrics_json(&os),
                Err(error) => metrics_unavailable_json(error.to_string()),
            };
            print_metrics(&metrics, json)
        }
        Command::Doctor => doctor(&store, &config_path, json),
        Command::Config { command } => handle_config(config_path, command, json),
        Command::State { command } => handle_state(store, command, json),
        Command::Agent { command } => handle_agent(store, command, json),
        Command::Task { command } => handle_task(store, command, json),
        Command::Tool { command } => handle_tool(store, command, json),
        Command::Memory { command } => handle_memory(store, command, json),
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
            let os = store.load().context("run `agent-os init` first")?;
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
        Command::Completions(args) => {
            let mut command = Cli::command();
            clap_complete::generate(args.shell, &mut command, "agent-os", &mut std::io::stdout());
            Ok(())
        }
        Command::Api { command } => handle_api(store, config_path, command),
        Command::Workflow { command } => handle_workflow(store, command, json),
        Command::Run(args) => {
            let mut os = store.load().context("run `agent-os init` first")?;
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
            } else if args.dry_run {
                for assignment in report.assignments {
                    println!(
                        "Would assign task {} to {} ({})",
                        assignment.task_id, assignment.agent_id, assignment.reason
                    );
                }
            } else if report.assignments.is_empty() {
                println!("No runnable tasks found.");
            } else {
                for assignment in report.assignments {
                    println!(
                        "Assigned task {} to {} ({})",
                        assignment.task_id, assignment.agent_id, assignment.reason
                    );
                }
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
            report.recovered_tasks =
                Runtime::recover_stale_tasks(&mut preview, ChronoDuration::seconds(seconds));
        }
        let tick_report = Runtime::tick(&mut preview, limit);
        report.assignments = tick_report.assignments;
        report.completed_tasks = tick_report.completed_tasks;
        report.expired_agents = tick_report.expired_agents;
        report.notes.extend(tick_report.notes);
        return Ok((report, Vec::new(), Vec::new()));
    }
    let (report, scheduled_os) = store.update(|os| {
        let mut report = RuntimeReport::default();
        if let Some(seconds) = recover_stale_seconds {
            report.recovered_tasks =
                Runtime::recover_stale_tasks(os, ChronoDuration::seconds(seconds));
        }
        let tick_report = Runtime::tick(os, limit);
        report.assignments = tick_report.assignments;
        report.completed_tasks = tick_report.completed_tasks;
        report.expired_agents = tick_report.expired_agents;
        report.notes.extend(tick_report.notes);
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
            .context("run `agent-os init` first")?
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

fn validate_api_serve_inputs(args: &ApiServeArgs) -> Result<()> {
    if let Some(max_requests) = args.max_requests
        && max_requests == 0
    {
        bail!("max_requests must be greater than 0");
    }
    if let Some(token_env) = &args.token_env {
        validate_env_var_name("token_env", token_env)?;
    }
    if args.token_env.is_none() && !args.unsafe_no_token && api_bind_requires_token(&args.addr) {
        bail!("api serve on non-loopback addresses requires --token-env or --unsafe-no-token");
    }
    Ok(())
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

    let report = serde_json::json!({
        "agent_os_version": env!("CARGO_PKG_VERSION"),
        "state_path": store.path().display().to_string(),
        "state_directory": parent,
        "state_exists": state_exists,
        "state_loads": state_loads,
        "state_error": state_error,
        "state_valid": state_valid,
        "state_issues": state_issues,
        "config_path": config_path.display().to_string(),
        "config_exists": config_exists,
        "config_loads": config_loads,
        "config_valid": config_valid,
        "config_issues": config_issues,
        "config_error": config_error,
    });

    if json {
        print_json(&report)?;
    } else {
        println!("agent-os version: {}", env!("CARGO_PKG_VERSION"));
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
    }
    Ok(())
}

fn resolve_store(path: Option<std::path::PathBuf>) -> Result<Store> {
    let Some(path) = path else {
        return Ok(Store::from_environment()?);
    };

    if path.is_dir() {
        Ok(Store::new(path.join("state.json")))
    } else if path.extension().is_some() {
        Ok(Store::new(path))
    } else {
        Ok(Store::new(path.join("state.json")))
    }
}

fn init(store: Store, config_path: &std::path::Path, args: InitArgs, json: bool) -> Result<()> {
    let config = load_config(config_path)?.unwrap_or_default();
    validate_seed_config(&config)?;
    let name = args.name.unwrap_or_else(|| config.name.clone());
    validate_os_name(&name)?;
    let mut os = OperatingSystem::new(name);
    os.policy = config.policy.clone();
    os.provider = config.provider.clone();
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
            write_default_config(&config_path, args.force)?;
            if json {
                print_json(&serde_json::json!({
                    "path": config_path.display().to_string(),
                    "written": true,
                    "config": AppConfig::default(),
                }))?;
            } else {
                println!("Wrote config {}", config_path.display());
            }
        }
        ConfigCommand::Show => {
            let exists = config_path.exists();
            let config = load_config(&config_path)?.unwrap_or_default();
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
                let body = store.export_json().context("run `agent-os init` first")?;
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
            } else if args.dry_run {
                println!(
                    "State already at version {}; no migration needed.",
                    report.to_version
                );
            } else if report.changed {
                println!(
                    "Migrated state from version {} to {} at {}",
                    report.from_version,
                    report.to_version,
                    output.display()
                );
            } else {
                println!(
                    "State already at version {}: {}",
                    report.to_version,
                    output.display()
                );
            }
        }
        StateCommand::Prune(args) => {
            let report = store
                .prune(args.keep_runs, args.keep_events, args.dry_run)
                .context("run `agent-os init` first")?;
            if json {
                print_json(&report)?;
            } else if args.dry_run {
                println!(
                    "Would remove {} run(s), {} log file(s), and {} event(s).",
                    report.removed_runs.len(),
                    report.removed_log_paths.len(),
                    report.removed_events
                );
            } else {
                println!(
                    "Removed {} run(s), {} log file(s), and {} event(s).",
                    report.removed_runs.len(),
                    report.removed_log_paths.len(),
                    report.removed_events
                );
            }
        }
        StateCommand::Repair(args) => {
            let report = if args.dry_run {
                let mut os = store.load().context("run `agent-os init` first")?;
                repair_state(&mut os)
            } else {
                store.repair_state().context("run `agent-os init` first")?
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
        StateCommand::Validate => {
            let os = store.load().context("run `agent-os init` first")?;
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
            let os = store.load().context("run `agent-os init` first")?;
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
            let os = store.load().context("run `agent-os init` first")?;
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
            let os = store.load().context("run `agent-os init` first")?;
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
            let os = store.load().context("run `agent-os init` first")?;
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
            let recovered = store.update(|os| {
                Ok::<_, anyhow::Error>(Runtime::recover_stale_tasks(
                    os,
                    ChronoDuration::seconds(args.older_than_seconds),
                ))
            })?;
            if json {
                print_json(&serde_json::json!({
                    "older_than_seconds": args.older_than_seconds,
                    "recovered": recovered,
                }))?;
            } else if recovered.is_empty() {
                println!("No stale running tasks found.");
            } else {
                println!("Recovered {} stale running task(s).", recovered.len());
                for task_id in recovered {
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
            let os = store.load().context("run `agent-os init` first")?;
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
            let os = store.load().context("run `agent-os init` first")?;
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
            let (id, record) = store.update(|os| {
                let record = MemoryRecord::new(args.topic, args.body, args.tags);
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
            let os = store.load().context("run `agent-os init` first")?;
            let mut matches = os
                .memory
                .iter()
                .filter(|record| {
                    memory_matches_query(record, &args.query)
                        && has_all_tags(&record.tags, &tags)
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
            matches.sort_by_key(|record| std::cmp::Reverse(record.updated_at));
            if let Some(limit) = args.limit {
                matches.truncate(limit);
            }
            if json {
                print_json(&matches)?;
            } else {
                print_memory(&matches);
            }
        }
        MemoryCommand::List(args) => {
            validate_memory_limit(args.limit)?;
            let tags = tag_filter("memory tag filter", &args.tags)?;
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
            let os = store.load().context("run `agent-os init` first")?;
            let mut records = os
                .memory
                .iter()
                .filter(|record| {
                    has_all_tags(&record.tags, &tags)
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
            let os = store.load().context("run `agent-os init` first")?;
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
            validate_tag_values("memory tags", &args.tags)?;
            if args.topic.is_none()
                && args.body.is_none()
                && args.tags.is_empty()
                && !args.clear_tags
            {
                bail!("provide --topic, --body, --tag, or --clear-tags");
            }
            let tags = if args.clear_tags {
                Some(Vec::new())
            } else if args.tags.is_empty() {
                None
            } else {
                Some(args.tags)
            };
            let record = store.update(|os| {
                os.update_memory(&args.id, args.topic, args.body, tags)
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
    }
    Ok(())
}

fn handle_runs(store: Store, command: RunsCommand, json: bool) -> Result<()> {
    let os = store.load().context("run `agent-os init` first")?;
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
            let os = store.load().context("run `agent-os init` first")?;
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
    }
    Ok(())
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

fn handle_api(store: Store, config_path: std::path::PathBuf, command: ApiCommand) -> Result<()> {
    match command {
        ApiCommand::Serve(args) => {
            validate_api_serve_inputs(&args)?;
            let bearer_token = args
                .token_env
                .as_ref()
                .map(std::env::var)
                .transpose()
                .with_context(|| {
                    format!(
                        "could not read API token env {}",
                        args.token_env.as_deref().unwrap_or_default()
                    )
                })?;
            if let Some(token) = &bearer_token
                && token.trim().is_empty()
            {
                bail!("API token from token_env must not be empty");
            }
            let server = ApiServer::bind_with_config_path(
                store,
                &args.addr,
                args.max_requests,
                bearer_token,
                config_path,
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

fn handle_workflow(store: Store, command: WorkflowCommand, json: bool) -> Result<()> {
    match command {
        WorkflowCommand::Create(args) => create_workflow(store, args, json),
        WorkflowCommand::List(args) => list_workflows(store, args, json),
        WorkflowCommand::Show(args) => show_workflow(store, args, json),
        WorkflowCommand::Status(args) => show_workflow_status(store, args, json),
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
            let plan = Task::new(
                format!("Plan: {}", objective),
                format!("Design an implementation plan for: {}", objective),
                priority,
                vec!["plan".into()],
            );
            let plan_id = plan.id.clone();

            let mut build = Task::new(
                format!("Build: {}", objective),
                format!("Implement the approved plan for: {}", objective),
                priority,
                vec!["rust".into(), "code".into()],
            );
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
            review.dependencies.push(build_id.clone());
            let review_id = review.id.clone();

            os.create_task(plan);
            os.create_task(build);
            os.create_task(review);
            let workflow = Workflow::new(
                objective.clone(),
                priority,
                BTreeMap::from([
                    ("plan".into(), plan_id.clone()),
                    ("build".into(), build_id.clone()),
                    ("review".into(), review_id.clone()),
                ]),
            );
            os.create_workflow(workflow.clone());
            Ok::<_, anyhow::Error>((workflow, plan_id, build_id, review_id))
        })
        .context("run `agent-os init` first")?;

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
        .context("run `agent-os init` first")?;

    while ticks < max_ticks {
        let mut os = store.load().context("run `agent-os init` first")?;
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
    println!(
        "agents: {} | tools: {} | tasks: {} pending, {} running, {} blocked, {} complete, {} failed, {} cancelled | workflows: {} | runs: {} | daemon: {} | memories: {} | events: {}",
        summary.agents,
        summary.tools,
        pending,
        running,
        blocked,
        complete,
        failed,
        cancelled,
        summary.workflows,
        summary.runs,
        summary.daemon_status.as_deref().unwrap_or("not-started"),
        summary.memories,
        summary.events
    );
    Ok(())
}

fn print_metrics(metrics: &Value, json: bool) -> Result<()> {
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
        "tasks: {} total, {} pending, {} running, {} blocked, {} complete, {} failed, {} cancelled",
        metric_u64(metrics, "tasks_total"),
        metric_u64(metrics, "tasks_pending"),
        metric_u64(metrics, "tasks_running"),
        metric_u64(metrics, "tasks_blocked"),
        metric_u64(metrics, "tasks_complete"),
        metric_u64(metrics, "tasks_failed"),
        metric_u64(metrics, "tasks_cancelled")
    );
    println!(
        "runs: {} total, {} running, {} cancel-requested, {} cancelled, {} success, {} failed, {} rejected",
        metric_u64(metrics, "runs_total"),
        metric_u64(metrics, "runs_running"),
        metric_u64(metrics, "runs_cancel_requested"),
        metric_u64(metrics, "runs_cancelled"),
        metric_u64(metrics, "runs_success"),
        metric_u64(metrics, "runs_failed"),
        metric_u64(metrics, "runs_rejected")
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
    println!("status: {} | priority: {}", task.status, task.priority);
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
            tags: record.tags.join(", "),
            body: truncate(&record.body, 84),
        })
        .collect::<Vec<_>>();
    print_table(rows);
}

fn print_memory_detail(record: &MemoryRecord) {
    println!("{}", record.id);
    println!("topic: {}", record.topic);
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
    for (key, env) in values {
        if env.trim().is_empty() {
            bail!("secret tool argument `{key}` must name an environment variable");
        }
        if !is_valid_env_var_name(env) {
            bail!("secret tool argument `{key}` must name a valid environment variable");
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
    let os = store.load().context("run `agent-os init` first")?;
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
    let os = store.load().context("run `agent-os init` first")?;
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
    let os = store.load().context("run `agent-os init` first")?;
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

fn run_workflow(store: Store, args: WorkflowRunArgs, json: bool) -> Result<()> {
    let id = parse_workflow_id_arg(&args.id)?;
    let os = store.load().context("run `agent-os init` first")?;
    if !os.workflows.contains_key(&id) {
        bail!("workflow not found: {id}");
    }
    drop(os);

    let (runs, errors) = execute_workflow_stages(&store, &id, args.all)?;
    let os = store.load().context("run `agent-os init` first")?;
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
        .context("run `agent-os init` first")?;
    let os = store.load().context("run `agent-os init` first")?;
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
