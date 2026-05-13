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
pub mod migrations;
pub mod models;
pub mod policy;
pub mod providers;
pub mod runtime;
pub mod scheduler;
pub mod service;
pub mod store;
pub mod tools;
pub mod validation;

pub use api::{ApiError, ApiServer, metrics_json, metrics_unavailable_json, openapi_schema};
pub use config::{AgentConfig, AppConfig, ConfigError, ConfigSource};
pub use migrations::{CURRENT_STATE_VERSION, MigrationError, MigrationReport, migrate_state_value};
pub use models::{
    Agent, AgentId, AgentKind, AgentStatus, DaemonState, DaemonStatus, Event, EventKind,
    MemoryRecord, OperatingSystem, Priority, ProviderKind, ProviderSettings, RunId, RunRecord,
    RunStatus, Task, TaskId, TaskStatus, ToolDefinition, ToolId, ToolInvocation, ToolKind,
    Workflow, WorkflowId, WorkflowProgress, WorkflowStageProgress,
};
pub use policy::{PolicyDecision, PolicyError};
pub use providers::{
    AgentResponse, MockProvider, ProviderError, ProviderMemory, ProviderRequest, ProviderTool,
    ProviderToolCall,
};
pub use runtime::{AgentUpdate, Runtime, RuntimeError, RuntimeReport, TaskUpdate, ToolUpdate};
pub use scheduler::{Assignment, Scheduler};
pub use service::{
    DEFAULT_LAUNCHD_LABEL, LaunchctlCommandOutput, LaunchdService, LaunchdServiceInstall,
    LaunchdServiceOptions, LaunchdServiceUninstall, ServiceError, build_launchd_service,
    default_launchd_log_paths, default_launchd_plist_path, install_launchd_service,
    resolve_launchd_domain, run_launchctl, uninstall_launchd_service,
    validate_service_control_inputs, validate_service_label,
};
pub use store::{PruneReport, Store, StoreError};
pub use tools::{
    RenderedToolCommand, RenderedToolText, ToolError, render_tool_command,
    render_tool_command_with_redaction_patterns, resolve_tool_arg, validate_tool_invocation,
    validate_tool_template,
};
pub use validation::{RepairReport, ValidationReport, repair_state, validate_state};
