#![recursion_limit = "256"]
//! Local-first runtime for coordinating AI agents.
//!
//! `agent_os` provides the core building blocks behind the `agent-os` CLI and
//! local HTTP API: durable state, agent registration, task scheduling, command
//! execution, provider-backed work, shared memory, state validation/repair, and
//! launchd service rendering.
//!
//! The crate is intentionally state-file based. [`Store`] owns atomic,
//! lock-protected reads and writes around an [`OperatingSystem`] value, while
//! [`Runtime`] and [`Scheduler`] update that state through explicit, testable
//! transitions.
//!
//! # Example
//!
//! ```
//! use agent_os::{Agent, AgentKind, OperatingSystem, Priority, Runtime, Task};
//!
//! let mut os = OperatingSystem::new("example");
//! os.register_agent(Agent::new(
//!     "builder",
//!     AgentKind::Builder,
//!     None,
//!     vec!["rust".into()],
//!     1,
//! ));
//! os.create_task(Task::new(
//!     "Implement feature",
//!     "Add a focused runtime improvement.",
//!     Priority::Normal,
//!     vec!["rust".into()],
//! ));
//!
//! let report = Runtime::tick(&mut os, 1);
//! assert_eq!(report.assignments.len(), 1);
//! ```
//!
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

pub mod api;
pub mod config;
mod durable_io;
pub mod executor;
pub mod git_integration;
pub mod json_schema;
pub mod migrations;
pub mod models;
pub mod policy;
mod process_tree;
pub mod providers;
pub mod runtime;
pub mod scheduler;
pub mod secrets;
pub mod service;
pub mod shell_capture;
pub mod sqlite_store;
pub mod store;
pub mod tools;
pub mod validation;

pub use api::{
    ApiAuth, ApiCors, ApiError, ApiServer, metrics_json, metrics_prometheus,
    metrics_unavailable_json, normalize_cors_origin, openapi_schema,
};
pub use config::{AgentConfig, AppConfig, ConfigError, ConfigProfile, ConfigSource};
pub use git_integration::{
    GitCommandOutput, GitIntegrationError, resolve_git_cwd, run_external_command, run_git_capture,
    run_git_command, validate_git_cwd,
};
pub use json_schema::{JsonSchemaError, validate_json_schema};
pub use migrations::{CURRENT_STATE_VERSION, MigrationError, MigrationReport, migrate_state_value};
pub use models::{
    Agent, AgentId, AgentKind, AgentProfile, AgentStatus, ApprovalPolicy, ApprovalRequest,
    ApprovalStatus, AutonomyLevel, DaemonState, DaemonStatus, EvalRecord, EvalRunDetails, Event,
    EventKind, MAX_PROVIDER_RETRIES, McpServer, MemoryPolicy, MemoryRecallHit, MemoryRecord,
    MemoryVisibility, NetworkMode, NetworkPolicy, OperatingSystem, Priority, ProviderKind,
    ProviderSettings, RunArtifact, RunArtifactKind, RunId, RunRecord, RunStatus, SandboxPolicy,
    SecretCheckReference, SecretCheckReport, SecretsBackend, SecretsBackendKind, Task, TaskId,
    TaskStatus, ToolDefinition, ToolId, ToolInvocation, ToolKind, WorkerNode, Workflow, WorkflowId,
    WorkflowProgress, WorkflowStageProgress, WorkflowTemplate, WorkflowTemplateEdge,
    WorkflowTemplateTask, memory_recall_hit, render_workflow_template_text, secret_check_report,
    workflow_template_edges, workflow_template_task,
};
pub use policy::{PolicyDecision, PolicyError};
pub use process_tree::terminate_child_process_tree;
pub use providers::{
    AgentResponse, MockProvider, PluginProvider, ProviderError, ProviderMemory, ProviderRequest,
    ProviderTool, ProviderToolCall,
};
pub use runtime::{
    AgentUpdate, Runtime, RuntimeError, RuntimeReport, TaskUpdate, ToolUpdate, UnscheduledTask,
};
pub use scheduler::{Assignment, Scheduler};
pub use secrets::{
    EnvironmentSecretResolver, OperatingSystemSecretResolver, ParsedSecretReference,
    SecretResolveError, SecretResolver, is_valid_secret_reference, parse_secret_reference,
};
pub use service::{
    DEFAULT_LAUNCHD_LABEL, DEFAULT_SYSTEMD_UNIT, DEFAULT_WINDOWS_TASK_NAME, LaunchctlCommandOutput,
    LaunchdService, LaunchdServiceInstall, LaunchdServiceOptions, LaunchdServiceUninstall,
    ServiceError, SystemctlCommandOutput, SystemdService, SystemdServiceInstall,
    SystemdServiceOptions, SystemdServiceUninstall, WindowsScheduledTask,
    WindowsScheduledTaskOptions, build_launchd_service, build_systemd_service,
    build_windows_scheduled_task, default_launchd_log_paths, default_launchd_plist_path,
    default_systemd_unit_path, install_launchd_service, install_systemd_service,
    resolve_launchd_domain, run_launchctl, run_systemctl, uninstall_launchd_service,
    uninstall_systemd_service, validate_service_control_inputs, validate_service_label,
    validate_systemd_control_inputs,
};
pub use sqlite_store::{SqliteImportReport, SqliteRestoreReport, SqliteStore, SqliteStoreError};
pub use store::{PruneReport, Store, StoreError};
pub use tools::{
    RenderedToolCommand, RenderedToolText, ToolError, render_tool_command,
    render_tool_command_with_redaction_patterns, resolve_tool_arg, validate_tool_invocation,
    validate_tool_template,
};
pub use validation::{RepairReport, ValidationReport, repair_state, validate_state};
