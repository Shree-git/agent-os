use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentId(String);

impl AgentId {
    pub fn new(slug: impl Into<String>) -> Self {
        Self(normalize_slug(slug.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(String);

impl TaskId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().simple().to_string()[..8].to_owned())
    }

    pub fn from_slug(slug: impl Into<String>) -> Self {
        Self(normalize_slug(slug.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolId(String);

impl ToolId {
    pub fn new(slug: impl Into<String>) -> Self {
        Self(normalize_slug(slug.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ToolId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(String);

impl RunId {
    pub fn new() -> Self {
        Self(format!(
            "run-{}",
            &Uuid::new_v4().simple().to_string()[..10]
        ))
    }

    pub fn from_slug(slug: impl Into<String>) -> Self {
        Self(normalize_slug(slug.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for RunId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkflowId(String);

impl WorkflowId {
    pub fn new() -> Self {
        Self(format!("wf-{}", &Uuid::new_v4().simple().to_string()[..8]))
    }

    pub fn from_slug(slug: impl Into<String>) -> Self {
        Self(normalize_slug(slug.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for WorkflowId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for WorkflowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentKind {
    Planner,
    Builder,
    Reviewer,
    Researcher,
    Operator,
    Custom(String),
}

impl AgentKind {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "planner" => Self::Planner,
            "builder" => Self::Builder,
            "reviewer" => Self::Reviewer,
            "researcher" => Self::Researcher,
            "operator" => Self::Operator,
            other => Self::Custom(other.to_owned()),
        }
    }
}

impl fmt::Display for AgentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Planner => f.write_str("planner"),
            Self::Builder => f.write_str("builder"),
            Self::Reviewer => f.write_str("reviewer"),
            Self::Researcher => f.write_str("researcher"),
            Self::Operator => f.write_str("operator"),
            Self::Custom(kind) => f.write_str(kind),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentStatus {
    Online,
    Busy,
    Paused,
    Offline,
}

impl fmt::Display for AgentStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Online => f.write_str("online"),
            Self::Busy => f.write_str("busy"),
            Self::Paused => f.write_str("paused"),
            Self::Offline => f.write_str("offline"),
        }
    }
}

impl AgentStatus {
    pub const VALUES: &'static [&'static str] = &["online", "busy", "paused", "offline"];
    pub const INPUT_VALUES: &'static [&'static str] =
        &["online", "up", "busy", "paused", "pause", "offline", "down"];

    pub fn try_parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "online" | "up" => Some(Self::Online),
            "busy" => Some(Self::Busy),
            "paused" | "pause" => Some(Self::Paused),
            "offline" | "down" => Some(Self::Offline),
            _ => None,
        }
    }

    pub fn parse(input: &str) -> Self {
        Self::try_parse(input).unwrap_or(Self::Online)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Priority {
    Low,
    Normal,
    High,
    Critical,
}

impl Priority {
    pub const VALUES: &'static [&'static str] = &["low", "normal", "high", "critical"];
    pub const INPUT_VALUES: &'static [&'static str] =
        &["low", "normal", "high", "critical", "urgent"];

    pub fn try_parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "low" => Some(Self::Low),
            "normal" => Some(Self::Normal),
            "high" => Some(Self::High),
            "critical" | "urgent" => Some(Self::Critical),
            _ => None,
        }
    }

    pub fn parse(input: &str) -> Self {
        Self::try_parse(input).unwrap_or(Self::Normal)
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Low => f.write_str("low"),
            Self::Normal => f.write_str("normal"),
            Self::High => f.write_str("high"),
            Self::Critical => f.write_str("critical"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskStatus {
    Pending,
    Running,
    Blocked,
    Complete,
    Failed,
    Cancelled,
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::Running => f.write_str("running"),
            Self::Blocked => f.write_str("blocked"),
            Self::Complete => f.write_str("complete"),
            Self::Failed => f.write_str("failed"),
            Self::Cancelled => f.write_str("cancelled"),
        }
    }
}

impl TaskStatus {
    pub const VALUES: &'static [&'static str] = &[
        "pending",
        "running",
        "blocked",
        "complete",
        "failed",
        "cancelled",
    ];
    pub const INPUT_VALUES: &'static [&'static str] = &[
        "pending",
        "running",
        "blocked",
        "complete",
        "completed",
        "failed",
        "cancelled",
        "canceled",
    ];

    pub fn try_parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "blocked" => Some(Self::Blocked),
            "complete" | "completed" => Some(Self::Complete),
            "failed" => Some(Self::Failed),
            "cancelled" | "canceled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Agent {
    pub id: AgentId,
    pub name: String,
    pub kind: AgentKind,
    pub model: Option<String>,
    pub capabilities: Vec<String>,
    pub max_parallel_tasks: usize,
    pub status: AgentStatus,
    #[serde(default)]
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub current_tasks: Vec<TaskId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Agent {
    pub fn new(
        name: impl Into<String>,
        kind: AgentKind,
        model: Option<String>,
        capabilities: Vec<String>,
        max_parallel_tasks: usize,
    ) -> Self {
        let name = name.into();
        let now = Utc::now();
        Self {
            id: AgentId::new(&name),
            name,
            kind,
            model,
            capabilities: normalize_list(capabilities),
            max_parallel_tasks: max_parallel_tasks.max(1),
            status: AgentStatus::Online,
            last_heartbeat_at: None,
            lease_expires_at: None,
            current_tasks: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }

    pub fn can_accept(&self, required: &[String]) -> bool {
        self.can_accept_at(required, Utc::now())
    }

    pub fn can_accept_at(&self, required: &[String], now: DateTime<Utc>) -> bool {
        self.status == AgentStatus::Online
            && self
                .lease_expires_at
                .map(|expires_at| expires_at > now)
                .unwrap_or(true)
            && self.current_tasks.len() < self.max_parallel_tasks
            && required
                .iter()
                .all(|need| self.capabilities.iter().any(|cap| cap == need))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub title: String,
    pub objective: String,
    pub priority: Priority,
    pub required_capabilities: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<TaskId>,
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub tool: Option<ToolInvocation>,
    pub status: TaskStatus,
    pub assigned_to: Option<AgentId>,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default = "default_task_max_attempts")]
    pub max_attempts: u32,
    pub plan: Vec<String>,
    pub output: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Task {
    pub fn new(
        title: impl Into<String>,
        objective: impl Into<String>,
        priority: Priority,
        required_capabilities: Vec<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: TaskId::new(),
            title: title.into(),
            objective: objective.into(),
            priority,
            required_capabilities: normalize_list(required_capabilities),
            dependencies: Vec::new(),
            command: None,
            cwd: None,
            tool: None,
            status: TaskStatus::Pending,
            assigned_to: None,
            attempts: 0,
            max_attempts: default_task_max_attempts(),
            plan: Vec::new(),
            output: None,
            created_at: now,
            updated_at: now,
        }
    }
}

fn default_task_max_attempts() -> u32 {
    1
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Workflow {
    pub id: WorkflowId,
    pub objective: String,
    pub priority: Priority,
    pub tasks: BTreeMap<String, TaskId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Workflow {
    pub fn new(
        objective: impl Into<String>,
        priority: Priority,
        tasks: BTreeMap<String, TaskId>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: WorkflowId::new(),
            objective: objective.into(),
            priority,
            tasks,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowStageProgress {
    pub stage: String,
    pub task_id: TaskId,
    pub title: Option<String>,
    pub status: Option<TaskStatus>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowProgress {
    pub id: WorkflowId,
    pub objective: String,
    pub priority: Priority,
    pub total_tasks: usize,
    pub tasks_pending: usize,
    pub tasks_running: usize,
    pub tasks_blocked: usize,
    pub tasks_complete: usize,
    pub tasks_failed: usize,
    pub tasks_cancelled: usize,
    pub tasks_missing: usize,
    pub current_stage: Option<String>,
    pub stages: Vec<WorkflowStageProgress>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ToolKind {
    Shell,
    FileRead,
    FileWrite,
}

impl fmt::Display for ToolKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shell => f.write_str("shell"),
            Self::FileRead => f.write_str("file-read"),
            Self::FileWrite => f.write_str("file-write"),
        }
    }
}

impl ToolKind {
    pub const VALUES: &'static [&'static str] = &["shell", "file-read", "file-write"];
    pub const INPUT_VALUES: &'static [&'static str] = &[
        "shell",
        "file-read",
        "read-file",
        "file-write",
        "write-file",
    ];

    pub fn try_parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "shell" => Some(Self::Shell),
            "file-read" | "read-file" => Some(Self::FileRead),
            "file-write" | "write-file" => Some(Self::FileWrite),
            _ => None,
        }
    }

    pub fn parse(input: &str) -> Self {
        Self::try_parse(input).unwrap_or(Self::Shell)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub id: ToolId,
    pub name: String,
    pub kind: ToolKind,
    pub description: String,
    pub required_capabilities: Vec<String>,
    pub command_template: String,
    #[serde(default)]
    pub default_cwd: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToolDefinition {
    pub fn new(
        name: impl Into<String>,
        kind: ToolKind,
        description: impl Into<String>,
        required_capabilities: Vec<String>,
        command_template: impl Into<String>,
        default_cwd: Option<String>,
    ) -> Self {
        let name = name.into();
        let now = Utc::now();
        Self {
            id: ToolId::new(&name),
            name,
            kind,
            description: description.into(),
            required_capabilities: normalize_list(required_capabilities),
            command_template: command_template.into(),
            default_cwd,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ToolInvocation {
    pub tool_id: ToolId,
    #[serde(default)]
    pub args: BTreeMap<String, String>,
    #[serde(default)]
    pub secret_env_args: BTreeMap<String, String>,
}

impl ToolInvocation {
    pub fn new(tool_id: ToolId, args: BTreeMap<String, String>) -> Self {
        Self {
            tool_id,
            args,
            secret_env_args: BTreeMap::new(),
        }
    }

    pub fn with_secret_env_args(
        tool_id: ToolId,
        args: BTreeMap<String, String>,
        secret_env_args: BTreeMap<String, String>,
    ) -> Self {
        Self {
            tool_id,
            args,
            secret_env_args,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RunStatus {
    Running,
    CancelRequested,
    Cancelled,
    Success,
    Failed,
    Rejected,
}

impl fmt::Display for RunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Running => f.write_str("running"),
            Self::CancelRequested => f.write_str("cancel-requested"),
            Self::Cancelled => f.write_str("cancelled"),
            Self::Success => f.write_str("success"),
            Self::Failed => f.write_str("failed"),
            Self::Rejected => f.write_str("rejected"),
        }
    }
}

impl RunStatus {
    pub const VALUES: &'static [&'static str] = &[
        "running",
        "cancel-requested",
        "cancelled",
        "success",
        "failed",
        "rejected",
    ];
    pub const INPUT_VALUES: &'static [&'static str] = &[
        "running",
        "cancel-requested",
        "cancel_requested",
        "cancelled",
        "canceled",
        "success",
        "succeeded",
        "failed",
        "rejected",
    ];

    pub fn try_parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "running" => Some(Self::Running),
            "cancel-requested" | "cancel_requested" => Some(Self::CancelRequested),
            "cancelled" | "canceled" => Some(Self::Cancelled),
            "success" | "succeeded" => Some(Self::Success),
            "failed" => Some(Self::Failed),
            "rejected" => Some(Self::Rejected),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DaemonStatus {
    Running,
    Stopped,
}

impl fmt::Display for DaemonStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Running => f.write_str("running"),
            Self::Stopped => f.write_str("stopped"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DaemonState {
    pub status: DaemonStatus,
    pub pid: Option<u32>,
    pub ticks: usize,
    pub limit: usize,
    pub execute: bool,
    #[serde(default)]
    pub stop_requested: bool,
    pub last_tick_at: Option<DateTime<Utc>>,
    pub last_message: Option<String>,
}

impl DaemonState {
    pub fn running(pid: u32, limit: usize, execute: bool) -> Self {
        Self {
            status: DaemonStatus::Running,
            pid: Some(pid),
            ticks: 0,
            limit,
            execute,
            stop_requested: false,
            last_tick_at: None,
            last_message: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderKind {
    Mock,
    #[serde(rename = "openai", alias = "open-ai")]
    OpenAi,
    Anthropic,
    Gemini,
    Ollama,
    Local,
    Plugin,
    #[serde(rename = "openai-compatible", alias = "open-ai-compatible")]
    OpenAiCompatible,
    Custom,
}

impl fmt::Display for ProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mock => f.write_str("mock"),
            Self::OpenAi => f.write_str("openai"),
            Self::Anthropic => f.write_str("anthropic"),
            Self::Gemini => f.write_str("gemini"),
            Self::Ollama => f.write_str("ollama"),
            Self::Local => f.write_str("local"),
            Self::Plugin => f.write_str("plugin"),
            Self::OpenAiCompatible => f.write_str("openai-compatible"),
            Self::Custom => f.write_str("custom"),
        }
    }
}

impl ProviderKind {
    pub const VALUES: &'static [&'static str] = &[
        "mock",
        "openai",
        "anthropic",
        "gemini",
        "ollama",
        "local",
        "plugin",
        "openai-compatible",
        "custom",
    ];
    pub const INPUT_VALUES: &'static [&'static str] = &[
        "mock",
        "openai",
        "anthropic",
        "gemini",
        "ollama",
        "local",
        "plugin",
        "openai-compatible",
        "open-ai-compatible",
        "custom",
    ];
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderSettings {
    #[serde(default = "default_provider_kind")]
    pub kind: ProviderKind,
    #[serde(default = "default_provider_model")]
    pub model: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default = "default_api_key_env")]
    pub api_key_env: String,
    #[serde(default = "default_provider_request_timeout_seconds")]
    pub request_timeout_seconds: u64,
    #[serde(default = "default_provider_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_provider_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    #[serde(default)]
    pub adapter: Option<String>,
    #[serde(default)]
    pub request_options: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub response_schema: Option<serde_json::Value>,
    #[serde(default)]
    pub plugin_command: Option<String>,
    #[serde(default)]
    pub plugin_args: Vec<String>,
    #[serde(default)]
    pub plugin_env: BTreeMap<String, String>,
}

pub const MAX_PROVIDER_RETRIES: u32 = 8;

impl Default for ProviderSettings {
    fn default() -> Self {
        Self {
            kind: default_provider_kind(),
            model: default_provider_model(),
            endpoint: None,
            api_key_env: default_api_key_env(),
            request_timeout_seconds: default_provider_request_timeout_seconds(),
            max_retries: default_provider_max_retries(),
            retry_backoff_ms: default_provider_retry_backoff_ms(),
            adapter: None,
            request_options: BTreeMap::new(),
            response_schema: None,
            plugin_command: None,
            plugin_args: Vec::new(),
            plugin_env: BTreeMap::new(),
        }
    }
}

fn default_provider_kind() -> ProviderKind {
    ProviderKind::Mock
}

fn default_provider_model() -> String {
    "mock-agent".into()
}

fn default_api_key_env() -> String {
    "OPENAI_API_KEY".into()
}

fn default_provider_request_timeout_seconds() -> u64 {
    30
}

fn default_provider_max_retries() -> u32 {
    2
}

fn default_provider_retry_backoff_ms() -> u64 {
    250
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: RunId,
    #[serde(default = "new_trace_id")]
    pub trace_id: String,
    pub task_id: TaskId,
    pub agent_id: Option<AgentId>,
    pub command: String,
    pub cwd: String,
    pub status: RunStatus,
    pub exit_code: Option<i32>,
    pub log_path: Option<String>,
    #[serde(default)]
    pub artifacts: Vec<RunArtifact>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

impl RunRecord {
    pub fn new(
        task_id: TaskId,
        agent_id: Option<AgentId>,
        command: impl Into<String>,
        cwd: impl Into<String>,
    ) -> Self {
        Self {
            id: RunId::new(),
            trace_id: new_trace_id(),
            task_id,
            agent_id,
            command: command.into(),
            cwd: cwd.into(),
            status: RunStatus::Running,
            exit_code: None,
            log_path: None,
            artifacts: Vec::new(),
            started_at: Utc::now(),
            finished_at: None,
        }
    }
}

fn new_trace_id() -> String {
    format!("trace-{}", &Uuid::new_v4().simple().to_string()[..12])
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RunArtifactKind {
    Stdout,
    Stderr,
    Summary,
    Diff,
    File,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunArtifact {
    pub kind: RunArtifactKind,
    pub path: String,
    #[serde(default)]
    pub bytes: Option<u64>,
    #[serde(default)]
    pub content_type: Option<String>,
}

impl RunArtifact {
    pub fn new(kind: RunArtifactKind, path: impl Into<String>) -> Self {
        Self {
            kind,
            path: path.into(),
            bytes: None,
            content_type: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    #[serde(default = "default_allow_shell")]
    pub allow_shell: bool,
    #[serde(default)]
    pub allowed_commands: Vec<String>,
    #[serde(default = "default_allowed_workspaces")]
    pub allowed_workspaces: Vec<String>,
    #[serde(default = "default_denied_patterns")]
    pub denied_patterns: Vec<String>,
    #[serde(default = "default_max_output_bytes")]
    pub max_output_bytes: usize,
    #[serde(default = "default_command_timeout_seconds")]
    pub command_timeout_seconds: u64,
    #[serde(default = "default_inherit_environment")]
    pub inherit_environment: bool,
    #[serde(default = "default_allowed_env_vars")]
    pub allowed_env_vars: Vec<String>,
    #[serde(default = "default_redacted_env_patterns")]
    pub redacted_env_patterns: Vec<String>,
    #[serde(default)]
    pub sandbox: SandboxPolicy,
    #[serde(default)]
    pub network: NetworkPolicy,
    #[serde(default)]
    pub approval: ApprovalPolicy,
    #[serde(default)]
    pub autonomy: AutonomyLevel,
    #[serde(default)]
    pub rules: Vec<String>,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            allow_shell: default_allow_shell(),
            allowed_commands: Vec::new(),
            allowed_workspaces: default_allowed_workspaces(),
            denied_patterns: default_denied_patterns(),
            max_output_bytes: default_max_output_bytes(),
            command_timeout_seconds: default_command_timeout_seconds(),
            inherit_environment: default_inherit_environment(),
            allowed_env_vars: default_allowed_env_vars(),
            redacted_env_patterns: default_redacted_env_patterns(),
            sandbox: SandboxPolicy::default(),
            network: NetworkPolicy::default(),
            approval: ApprovalPolicy::default(),
            autonomy: AutonomyLevel::default(),
            rules: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxPolicy {
    #[serde(default = "default_process_isolation")]
    pub process_isolation: bool,
    #[serde(default = "default_jailed_workspaces")]
    pub jailed_workspaces: bool,
    #[serde(default)]
    pub writable_paths: Vec<String>,
}

impl Default for SandboxPolicy {
    fn default() -> Self {
        Self {
            process_isolation: default_process_isolation(),
            jailed_workspaces: default_jailed_workspaces(),
            writable_paths: Vec::new(),
        }
    }
}

fn default_process_isolation() -> bool {
    true
}

fn default_jailed_workspaces() -> bool {
    true
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkMode {
    Disabled,
    ProvidersOnly,
    Allowed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkPolicy {
    #[serde(default = "default_network_mode")]
    pub mode: NetworkMode,
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
}

impl Default for NetworkPolicy {
    fn default() -> Self {
        Self {
            mode: default_network_mode(),
            allowed_hosts: Vec::new(),
        }
    }
}

fn default_network_mode() -> NetworkMode {
    NetworkMode::ProvidersOnly
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalPolicy {
    #[serde(default)]
    pub require_for_risky_actions: bool,
    #[serde(default = "default_approval_risky_patterns")]
    pub risky_patterns: Vec<String>,
}

impl Default for ApprovalPolicy {
    fn default() -> Self {
        Self {
            require_for_risky_actions: true,
            risky_patterns: default_approval_risky_patterns(),
        }
    }
}

fn default_approval_risky_patterns() -> Vec<String> {
    [
        "git push",
        "git commit",
        "rm ",
        "mv ",
        "chmod",
        "curl ",
        "wget ",
        "ssh ",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AutonomyLevel {
    ObserveOnly,
    Suggest,
    #[default]
    ExecuteWithApproval,
    ExecuteFreely,
}

fn default_allow_shell() -> bool {
    false
}

fn default_allowed_workspaces() -> Vec<String> {
    vec![".".into()]
}

fn default_denied_patterns() -> Vec<String> {
    vec![
        "rm -rf".into(),
        "rm -fr".into(),
        "sudo".into(),
        "shutdown".into(),
        "reboot".into(),
        "mkfs".into(),
        "dd if=".into(),
        "dd of=".into(),
    ]
}

fn default_max_output_bytes() -> usize {
    128 * 1024
}

fn default_command_timeout_seconds() -> u64 {
    300
}

fn default_inherit_environment() -> bool {
    false
}

fn default_allowed_env_vars() -> Vec<String> {
    [
        "PATH",
        "HOME",
        "USER",
        "TMPDIR",
        "TMP",
        "TEMP",
        "RUSTUP_HOME",
        "CARGO_HOME",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn default_redacted_env_patterns() -> Vec<String> {
    ["KEY", "TOKEN", "SECRET", "PASSWORD", "AUTH", "CREDENTIAL"]
        .into_iter()
        .map(str::to_owned)
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryVisibility {
    Shared,
    Private,
}

impl Default for MemoryVisibility {
    fn default() -> Self {
        Self::Shared
    }
}

impl MemoryVisibility {
    pub fn try_parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "shared" => Some(Self::Shared),
            "private" => Some(Self::Private),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::Private => "private",
        }
    }
}

impl fmt::Display for MemoryVisibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemoryRecord {
    pub id: String,
    pub topic: String,
    pub body: String,
    pub tags: Vec<String>,
    #[serde(default)]
    pub visibility: MemoryVisibility,
    #[serde(default)]
    pub scope: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemoryRecallHit {
    pub record: MemoryRecord,
    pub score: usize,
    pub snippet: String,
}

impl MemoryRecord {
    pub fn new(topic: impl Into<String>, body: impl Into<String>, tags: Vec<String>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().simple().to_string()[..10].to_owned(),
            topic: topic.into(),
            body: body.into(),
            tags: normalize_list(tags),
            visibility: MemoryVisibility::Shared,
            scope: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn with_access(
        topic: impl Into<String>,
        body: impl Into<String>,
        tags: Vec<String>,
        visibility: MemoryVisibility,
        scope: Option<String>,
    ) -> Self {
        let mut record = Self::new(topic, body, tags);
        record.visibility = visibility;
        record.scope = scope;
        record
    }
}

pub fn memory_matches_query(record: &MemoryRecord, query: &str) -> bool {
    memory_relevance_score(record, query) > 0
}

pub fn memory_recall_hit(record: &MemoryRecord, query: &str, score: usize) -> MemoryRecallHit {
    MemoryRecallHit {
        record: record.clone(),
        score,
        snippet: memory_recall_snippet(record, query, 240),
    }
}

pub fn memory_relevance_score(record: &MemoryRecord, query: &str) -> usize {
    let query = query.trim().to_ascii_lowercase();
    if query.is_empty() {
        return 0;
    }

    let topic = record.topic.to_ascii_lowercase();
    let body = record.body.to_ascii_lowercase();
    let tags = record
        .tags
        .iter()
        .map(|tag| tag.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let mut score = 0usize;
    if topic.contains(&query) {
        score += 120;
    }
    if body.contains(&query) {
        score += 80;
    }
    if tags.iter().any(|tag| tag.contains(&query)) {
        score += 100;
    }

    let query_tokens = memory_query_tokens(&query);
    if query_tokens.is_empty() {
        return score;
    }
    let topic_tokens = memory_query_tokens(&topic);
    let body_tokens = memory_query_tokens(&body);
    let tag_tokens = memory_query_tokens(&tags.join(" "));
    for token in query_tokens {
        if topic_tokens.iter().any(|candidate| candidate == &token) {
            score += 18;
        } else if token_matches_any(&token, &topic_tokens) {
            score += 9;
        }
        if tag_tokens.iter().any(|candidate| candidate == &token) {
            score += 15;
        } else if token_matches_any(&token, &tag_tokens) {
            score += 7;
        }
        if body_tokens.iter().any(|candidate| candidate == &token) {
            score += 10;
        } else if token_matches_any(&token, &body_tokens) {
            score += 4;
        }
    }
    score
}

fn memory_recall_snippet(record: &MemoryRecord, query: &str, max_chars: usize) -> String {
    let body = record.body.trim();
    if body.chars().count() <= max_chars {
        return body.to_owned();
    }
    let query = query.trim().to_ascii_lowercase();
    let lower = body.to_ascii_lowercase();
    let start_at = lower.find(&query).or_else(|| {
        memory_query_tokens(&query)
            .into_iter()
            .find_map(|token| lower.find(&token))
    });
    let Some(start_at) = start_at else {
        return truncate_chars(body, max_chars);
    };
    snippet_around(body, start_at, max_chars)
}

fn snippet_around(body: &str, byte_index: usize, max_chars: usize) -> String {
    let half_window = max_chars / 2;
    let mut chars = body.char_indices().collect::<Vec<_>>();
    chars.push((body.len(), '\0'));
    let center = chars
        .iter()
        .position(|(index, _)| *index >= byte_index)
        .unwrap_or(chars.len().saturating_sub(1));
    let start_char = center.saturating_sub(half_window);
    let end_char = (start_char + max_chars).min(chars.len().saturating_sub(1));
    let start_byte = chars[start_char].0;
    let end_byte = chars[end_char].0;
    let mut snippet = String::new();
    if start_byte > 0 {
        snippet.push_str("...");
    }
    snippet.push_str(body[start_byte..end_byte].trim());
    if end_byte < body.len() {
        snippet.push_str("...");
    }
    snippet
}

fn truncate_chars(body: &str, max_chars: usize) -> String {
    let mut snippet = body.chars().take(max_chars).collect::<String>();
    if body.chars().count() > max_chars {
        snippet.push_str("...");
    }
    snippet
}

fn token_matches_any(token: &str, candidates: &[String]) -> bool {
    token.len() >= 4
        && candidates
            .iter()
            .any(|candidate| candidate.contains(token) || token.contains(candidate))
}

fn memory_query_tokens(value: &str) -> Vec<String> {
    value
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .map(normalize_memory_token)
        .filter(|token| token.len() >= 3)
        .fold(Vec::new(), |mut tokens, token| {
            if !tokens.contains(&token) {
                tokens.push(token);
            }
            tokens
        })
}

fn normalize_memory_token(token: &str) -> String {
    let mut token = token.trim().to_ascii_lowercase();
    for suffix in ["ing", "ed", "es", "s"] {
        if token.len() > suffix.len() + 3 && token.ends_with(suffix) {
            token.truncate(token.len() - suffix.len());
            break;
        }
    }
    token
}

pub fn tail_text_by_bytes(body: &str, tail_bytes: Option<usize>) -> String {
    let Some(tail_bytes) = tail_bytes else {
        return body.to_owned();
    };
    if body.len() <= tail_bytes {
        return body.to_owned();
    }
    let mut start = body.len() - tail_bytes;
    while !body.is_char_boundary(start) {
        start += 1;
    }
    body[start..].to_owned()
}

pub fn text_tail_was_truncated(body: &str, tail_bytes: Option<usize>) -> bool {
    tail_bytes
        .map(|tail_bytes| body.len() > tail_bytes)
        .unwrap_or(false)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EventKind {
    SystemBooted,
    AgentRegistered,
    AgentUpdated,
    AgentHeartbeat,
    AgentRemoved,
    TaskCreated,
    TaskAssigned,
    TaskUpdated,
    WorkflowCreated,
    WorkflowUpdated,
    WorkflowRemoved,
    RunStarted,
    RunFinished,
    DaemonStarted,
    DaemonTick,
    DaemonStopped,
    PolicyRejected,
    MemoryWritten,
    MemoryUpdated,
    MemoryRemoved,
    ToolRegistered,
    ToolUpdated,
    ToolRemoved,
    StateRepaired,
    ApprovalRequested,
    ApprovalResolved,
    McpServerRegistered,
    McpServerUpdated,
    McpServerRemoved,
    WorkerRegistered,
    WorkerUpdated,
    WorkerRemoved,
    EvalRecorded,
    SecretsBackendRegistered,
    SecretsBackendRemoved,
    MarketplaceImported,
}

impl fmt::Display for EventKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SystemBooted => f.write_str("system-booted"),
            Self::AgentRegistered => f.write_str("agent-registered"),
            Self::AgentUpdated => f.write_str("agent-updated"),
            Self::AgentHeartbeat => f.write_str("agent-heartbeat"),
            Self::AgentRemoved => f.write_str("agent-removed"),
            Self::TaskCreated => f.write_str("task-created"),
            Self::TaskAssigned => f.write_str("task-assigned"),
            Self::TaskUpdated => f.write_str("task-updated"),
            Self::WorkflowCreated => f.write_str("workflow-created"),
            Self::WorkflowUpdated => f.write_str("workflow-updated"),
            Self::WorkflowRemoved => f.write_str("workflow-removed"),
            Self::RunStarted => f.write_str("run-started"),
            Self::RunFinished => f.write_str("run-finished"),
            Self::DaemonStarted => f.write_str("daemon-started"),
            Self::DaemonTick => f.write_str("daemon-tick"),
            Self::DaemonStopped => f.write_str("daemon-stopped"),
            Self::PolicyRejected => f.write_str("policy-rejected"),
            Self::MemoryWritten => f.write_str("memory-written"),
            Self::MemoryUpdated => f.write_str("memory-updated"),
            Self::MemoryRemoved => f.write_str("memory-removed"),
            Self::ToolRegistered => f.write_str("tool-registered"),
            Self::ToolUpdated => f.write_str("tool-updated"),
            Self::ToolRemoved => f.write_str("tool-removed"),
            Self::StateRepaired => f.write_str("state-repaired"),
            Self::ApprovalRequested => f.write_str("approval-requested"),
            Self::ApprovalResolved => f.write_str("approval-resolved"),
            Self::McpServerRegistered => f.write_str("mcp-server-registered"),
            Self::McpServerUpdated => f.write_str("mcp-server-updated"),
            Self::McpServerRemoved => f.write_str("mcp-server-removed"),
            Self::WorkerRegistered => f.write_str("worker-registered"),
            Self::WorkerUpdated => f.write_str("worker-updated"),
            Self::WorkerRemoved => f.write_str("worker-removed"),
            Self::EvalRecorded => f.write_str("eval-recorded"),
            Self::SecretsBackendRegistered => f.write_str("secrets-backend-registered"),
            Self::SecretsBackendRemoved => f.write_str("secrets-backend-removed"),
            Self::MarketplaceImported => f.write_str("marketplace-imported"),
        }
    }
}

impl EventKind {
    pub const VALUES: &'static [&'static str] = &[
        "system-booted",
        "agent-registered",
        "agent-updated",
        "agent-heartbeat",
        "agent-removed",
        "task-created",
        "task-assigned",
        "task-updated",
        "workflow-created",
        "workflow-updated",
        "workflow-removed",
        "run-started",
        "run-finished",
        "daemon-started",
        "daemon-tick",
        "daemon-stopped",
        "policy-rejected",
        "memory-written",
        "memory-updated",
        "memory-removed",
        "tool-registered",
        "tool-updated",
        "tool-removed",
        "state-repaired",
        "approval-requested",
        "approval-resolved",
        "mcp-server-registered",
        "mcp-server-updated",
        "mcp-server-removed",
        "worker-registered",
        "worker-updated",
        "worker-removed",
        "eval-recorded",
        "secrets-backend-registered",
        "secrets-backend-removed",
        "marketplace-imported",
    ];

    pub fn try_parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "system-booted" => Some(Self::SystemBooted),
            "agent-registered" => Some(Self::AgentRegistered),
            "agent-updated" => Some(Self::AgentUpdated),
            "agent-heartbeat" => Some(Self::AgentHeartbeat),
            "agent-removed" => Some(Self::AgentRemoved),
            "task-created" => Some(Self::TaskCreated),
            "task-assigned" => Some(Self::TaskAssigned),
            "task-updated" => Some(Self::TaskUpdated),
            "workflow-created" => Some(Self::WorkflowCreated),
            "workflow-updated" => Some(Self::WorkflowUpdated),
            "workflow-removed" => Some(Self::WorkflowRemoved),
            "run-started" => Some(Self::RunStarted),
            "run-finished" => Some(Self::RunFinished),
            "daemon-started" => Some(Self::DaemonStarted),
            "daemon-tick" => Some(Self::DaemonTick),
            "daemon-stopped" => Some(Self::DaemonStopped),
            "policy-rejected" => Some(Self::PolicyRejected),
            "memory-written" => Some(Self::MemoryWritten),
            "memory-updated" => Some(Self::MemoryUpdated),
            "memory-removed" => Some(Self::MemoryRemoved),
            "tool-registered" => Some(Self::ToolRegistered),
            "tool-updated" => Some(Self::ToolUpdated),
            "tool-removed" => Some(Self::ToolRemoved),
            "state-repaired" => Some(Self::StateRepaired),
            "approval-requested" => Some(Self::ApprovalRequested),
            "approval-resolved" => Some(Self::ApprovalResolved),
            "mcp-server-registered" => Some(Self::McpServerRegistered),
            "mcp-server-updated" => Some(Self::McpServerUpdated),
            "mcp-server-removed" => Some(Self::McpServerRemoved),
            "worker-registered" => Some(Self::WorkerRegistered),
            "worker-updated" => Some(Self::WorkerUpdated),
            "worker-removed" => Some(Self::WorkerRemoved),
            "eval-recorded" => Some(Self::EvalRecorded),
            "secrets-backend-registered" => Some(Self::SecretsBackendRegistered),
            "secrets-backend-removed" => Some(Self::SecretsBackendRemoved),
            "marketplace-imported" => Some(Self::MarketplaceImported),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    pub kind: EventKind,
    pub message: String,
    pub at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ApprovalStatus {
    Pending,
    Approved,
    Denied,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub id: String,
    pub task_id: TaskId,
    pub run_id: Option<RunId>,
    pub action: String,
    pub reason: String,
    pub status: ApprovalStatus,
    pub requested_at: DateTime<Utc>,
    #[serde(default)]
    pub resolved_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub resolved_by: Option<String>,
}

impl ApprovalRequest {
    pub fn new(
        task_id: TaskId,
        run_id: Option<RunId>,
        action: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().simple().to_string()[..10].to_owned(),
            task_id,
            run_id,
            action: action.into(),
            reason: reason.into(),
            status: ApprovalStatus::Pending,
            requested_at: Utc::now(),
            resolved_at: None,
            resolved_by: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct McpServer {
    pub id: String,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AgentProfile {
    pub id: String,
    pub name: String,
    pub kind: AgentKind,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub system_prompt: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowTemplate {
    pub id: String,
    pub name: String,
    pub description: String,
    pub stages: Vec<String>,
    #[serde(default)]
    pub tasks: Vec<WorkflowTemplateTask>,
    #[serde(default)]
    pub edges: Vec<WorkflowTemplateEdge>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowTemplateTask {
    pub stage: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub objective: Option<String>,
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowTemplateEdge {
    pub from: String,
    pub to: String,
}

pub fn workflow_template_task<'a>(
    template: &'a WorkflowTemplate,
    stage: &str,
) -> Option<&'a WorkflowTemplateTask> {
    template.tasks.iter().find(|task| task.stage == stage)
}

pub fn workflow_template_edges(template: &WorkflowTemplate) -> Vec<WorkflowTemplateEdge> {
    if !template.edges.is_empty() {
        return template.edges.clone();
    }
    template
        .stages
        .windows(2)
        .map(|stages| WorkflowTemplateEdge {
            from: stages[0].clone(),
            to: stages[1].clone(),
        })
        .collect()
}

pub fn render_workflow_template_text(text: &str, objective: &str) -> String {
    text.replace("{objective}", objective)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemoryPolicy {
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub semantic_recall: bool,
    #[serde(default)]
    pub max_provider_memories: usize,
    #[serde(default)]
    pub max_age_days: Option<u64>,
}

impl Default for MemoryPolicy {
    fn default() -> Self {
        Self {
            scope: None,
            semantic_recall: false,
            max_provider_memories: 5,
            max_age_days: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EvalRecord {
    pub id: String,
    pub target: String,
    pub success: bool,
    #[serde(default)]
    pub cost_micros: Option<u64>,
    #[serde(default)]
    pub latency_ms: Option<u64>,
    #[serde(default)]
    pub run: Option<EvalRunDetails>,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EvalRunDetails {
    pub command: String,
    pub cwd: String,
    pub status: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    #[serde(default)]
    pub success_pattern: Option<String>,
    pub success_pattern_matched: bool,
    #[serde(default)]
    pub output_schema: Option<serde_json::Value>,
    #[serde(default = "default_true")]
    pub output_schema_valid: bool,
    #[serde(default)]
    pub output_schema_error: Option<String>,
    pub timed_out: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerNode {
    pub id: String,
    pub endpoint: String,
    pub status: AgentStatus,
    pub last_seen_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SecretsBackendKind {
    Environment,
    OnePassword,
    OsKeychain,
    EnvVault,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SecretsBackend {
    pub id: String,
    pub kind: SecretsBackendKind,
    #[serde(default)]
    pub reference: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SecretCheckReference {
    pub task_id: TaskId,
    pub task_title: String,
    pub tool_id: ToolId,
    pub arg: String,
    pub env: String,
    pub valid_env: bool,
    pub present: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SecretCheckReport {
    pub total: usize,
    pub present: usize,
    pub missing: usize,
    pub invalid: usize,
    pub references: Vec<SecretCheckReference>,
}

impl Event {
    pub fn new(kind: EventKind, message: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().simple().to_string()[..10].to_owned(),
            kind,
            message: message.into(),
            at: Utc::now(),
        }
    }
}

pub fn secret_check_report(os: &OperatingSystem) -> SecretCheckReport {
    let mut references = os
        .tasks
        .values()
        .filter_map(|task| task.tool.as_ref().map(|invocation| (task, invocation)))
        .flat_map(|(task, invocation)| {
            invocation.secret_env_args.iter().map(move |(arg, env)| {
                let (valid_env, present) = secret_reference_status(os, env);
                SecretCheckReference {
                    task_id: task.id.clone(),
                    task_title: task.title.clone(),
                    tool_id: invocation.tool_id.clone(),
                    arg: arg.clone(),
                    env: env.clone(),
                    valid_env,
                    present,
                }
            })
        })
        .collect::<Vec<_>>();
    references.sort_by(|left, right| {
        left.task_id
            .cmp(&right.task_id)
            .then_with(|| left.tool_id.cmp(&right.tool_id))
            .then_with(|| left.arg.cmp(&right.arg))
    });
    let total = references.len();
    let present = references
        .iter()
        .filter(|reference| reference.present)
        .count();
    let invalid = references
        .iter()
        .filter(|reference| !reference.valid_env)
        .count();
    SecretCheckReport {
        total,
        present,
        missing: total.saturating_sub(present + invalid),
        invalid,
        references,
    }
}

fn secret_reference_status(os: &OperatingSystem, reference: &str) -> (bool, bool) {
    let reference = reference.trim();
    if reference.is_empty() || reference.contains('\0') {
        return (false, false);
    }
    let Some((backend_id, name)) = reference.split_once(':') else {
        let valid = is_valid_env_var_name(reference);
        return (valid, valid && std::env::var_os(reference).is_some());
    };
    let backend_id = backend_id.trim();
    let name = name.trim();
    if backend_id.is_empty() || name.is_empty() || backend_id.contains('/') {
        return (false, false);
    }
    if matches!(backend_id, "env" | "environment") {
        let valid = is_valid_env_var_name(name);
        return (valid, valid && std::env::var_os(name).is_some());
    }
    let Some(backend) = os.secrets_backends.get(backend_id) else {
        return (false, false);
    };
    match backend.kind {
        SecretsBackendKind::Environment => {
            let valid = is_valid_env_var_name(name);
            (valid, valid && std::env::var_os(name).is_some())
        }
        SecretsBackendKind::EnvVault => {
            let reference = backend
                .reference
                .as_deref()
                .unwrap_or("AGENT_OS_ENV_VAULT")
                .trim();
            let valid = is_valid_env_var_name(reference);
            (valid, valid && std::env::var_os(reference).is_some())
        }
        SecretsBackendKind::OnePassword | SecretsBackendKind::OsKeychain => (true, true),
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperatingSystem {
    pub version: u32,
    pub name: String,
    pub agents: BTreeMap<AgentId, Agent>,
    pub tasks: BTreeMap<TaskId, Task>,
    #[serde(default)]
    pub workflows: BTreeMap<WorkflowId, Workflow>,
    #[serde(default)]
    pub runs: BTreeMap<RunId, RunRecord>,
    #[serde(default)]
    pub policy: Policy,
    #[serde(default)]
    pub provider: ProviderSettings,
    #[serde(default)]
    pub daemon: Option<DaemonState>,
    #[serde(default)]
    pub tools: BTreeMap<ToolId, ToolDefinition>,
    #[serde(default)]
    pub approvals: BTreeMap<String, ApprovalRequest>,
    #[serde(default)]
    pub mcp_servers: BTreeMap<String, McpServer>,
    #[serde(default)]
    pub agent_profiles: BTreeMap<String, AgentProfile>,
    #[serde(default)]
    pub workflow_templates: BTreeMap<String, WorkflowTemplate>,
    #[serde(default)]
    pub memory_policy: MemoryPolicy,
    #[serde(default)]
    pub evals: Vec<EvalRecord>,
    #[serde(default)]
    pub workers: BTreeMap<String, WorkerNode>,
    #[serde(default)]
    pub secrets_backends: BTreeMap<String, SecretsBackend>,
    pub memory: Vec<MemoryRecord>,
    pub events: Vec<Event>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl OperatingSystem {
    pub fn new(name: impl Into<String>) -> Self {
        let now = Utc::now();
        let mut os = Self {
            version: 4,
            name: name.into(),
            agents: BTreeMap::new(),
            tasks: BTreeMap::new(),
            workflows: BTreeMap::new(),
            runs: BTreeMap::new(),
            policy: Policy::default(),
            provider: ProviderSettings::default(),
            daemon: None,
            tools: BTreeMap::new(),
            approvals: BTreeMap::new(),
            mcp_servers: BTreeMap::new(),
            agent_profiles: default_agent_profiles(),
            workflow_templates: default_workflow_templates(),
            memory_policy: MemoryPolicy::default(),
            evals: Vec::new(),
            workers: BTreeMap::new(),
            secrets_backends: default_secrets_backends(),
            memory: Vec::new(),
            events: Vec::new(),
            created_at: now,
            updated_at: now,
        };
        os.record(
            EventKind::SystemBooted,
            "agent operating system initialized",
        );
        os
    }

    pub fn try_register_agent(&mut self, agent: Agent) -> bool {
        if self.agents.contains_key(&agent.id) {
            return false;
        }
        self.record(
            EventKind::AgentRegistered,
            format!("registered agent {} ({})", agent.id, agent.kind),
        );
        self.agents.insert(agent.id.clone(), agent);
        self.touch();
        true
    }

    pub fn register_agent(&mut self, agent: Agent) {
        let _ = self.try_register_agent(agent);
    }

    pub fn next_task_id(&self) -> TaskId {
        loop {
            let id = TaskId::new();
            if !self.tasks.contains_key(&id) {
                return id;
            }
        }
    }

    pub fn next_run_id(&self) -> RunId {
        loop {
            let id = RunId::new();
            if !self.runs.contains_key(&id) {
                return id;
            }
        }
    }

    pub fn next_workflow_id(&self) -> WorkflowId {
        loop {
            let id = WorkflowId::new();
            if !self.workflows.contains_key(&id) {
                return id;
            }
        }
    }

    pub fn next_memory_id(&self) -> String {
        loop {
            let id = Uuid::new_v4().simple().to_string()[..10].to_owned();
            if !self.memory.iter().any(|record| record.id == id) {
                return id;
            }
        }
    }

    pub fn next_approval_id(&self) -> String {
        loop {
            let id = Uuid::new_v4().simple().to_string()[..10].to_owned();
            if !self.approvals.contains_key(&id) {
                return id;
            }
        }
    }

    pub fn next_eval_id(&self) -> String {
        loop {
            let id = format!("eval-{}", &Uuid::new_v4().simple().to_string()[..10]);
            if !self.evals.iter().any(|record| record.id == id) {
                return id;
            }
        }
    }

    pub fn next_event_id(&self) -> String {
        loop {
            let id = Uuid::new_v4().simple().to_string()[..10].to_owned();
            if !self.events.iter().any(|event| event.id == id) {
                return id;
            }
        }
    }

    pub fn ensure_unique_task_id(&self, task: &mut Task) {
        if self.tasks.contains_key(&task.id) {
            task.id = self.next_task_id();
        }
    }

    pub fn ensure_unique_run_id(&self, run: &mut RunRecord) {
        if self.runs.contains_key(&run.id) {
            run.id = self.next_run_id();
        }
    }

    pub fn ensure_unique_workflow_id(&self, workflow: &mut Workflow) {
        if self.workflows.contains_key(&workflow.id) {
            workflow.id = self.next_workflow_id();
        }
    }

    pub fn ensure_unique_memory_id(&self, record: &mut MemoryRecord) {
        if self
            .memory
            .iter()
            .any(|existing| existing.id == record.id && !memory_records_duplicate(existing, record))
        {
            record.id = self.next_memory_id();
        }
    }

    pub fn ensure_unique_approval_id(&self, request: &mut ApprovalRequest) {
        if self.approvals.contains_key(&request.id) {
            request.id = self.next_approval_id();
        }
    }

    pub fn ensure_unique_eval_id(&self, record: &mut EvalRecord) {
        if self.evals.iter().any(|existing| existing.id == record.id) {
            record.id = self.next_eval_id();
        }
    }

    pub fn ensure_unique_event_id(&self, event: &mut Event) {
        if self.events.iter().any(|existing| existing.id == event.id) {
            event.id = self.next_event_id();
        }
    }

    pub fn agent_removal_blockers(&self, agent_id: &AgentId) -> Vec<String> {
        let mut blockers = Vec::new();
        if let Some(agent) = self.agents.get(agent_id) {
            for task_id in &agent.current_tasks {
                blockers.push(format!("agent {} has current task {}", agent_id, task_id));
            }
        }
        for task in self.tasks.values() {
            if task.assigned_to.as_ref() == Some(agent_id) {
                blockers.push(format!(
                    "task {} is assigned to agent {}",
                    task.id, agent_id
                ));
            }
        }
        for run in self.runs.values() {
            if run.agent_id.as_ref() == Some(agent_id) {
                blockers.push(format!("run {} references agent {}", run.id, agent_id));
            }
        }
        blockers
    }

    pub fn remove_agent(&mut self, agent_id: &AgentId) -> Option<Agent> {
        let removed = self.agents.remove(agent_id);
        if removed.is_some() {
            self.record(
                EventKind::AgentRemoved,
                format!("removed agent {}", agent_id),
            );
        }
        removed
    }

    pub fn create_task(&mut self, mut task: Task) {
        self.ensure_unique_task_id(&mut task);
        self.record(
            EventKind::TaskCreated,
            format!("created task {}: {}", task.id, task.title),
        );
        self.tasks.insert(task.id.clone(), task);
        self.touch();
    }

    pub fn create_workflow(&mut self, mut workflow: Workflow) {
        self.ensure_unique_workflow_id(&mut workflow);
        self.record(
            EventKind::WorkflowCreated,
            format!("created workflow {}: {}", workflow.id, workflow.objective),
        );
        self.workflows.insert(workflow.id.clone(), workflow);
        self.touch();
    }

    pub fn remove_workflow(&mut self, workflow_id: &WorkflowId) -> Option<Workflow> {
        let removed = self.workflows.remove(workflow_id);
        if removed.is_some() {
            self.record(
                EventKind::WorkflowRemoved,
                format!("removed workflow {}", workflow_id),
            );
        }
        removed
    }

    pub fn workflow_progress(&self, workflow_id: &WorkflowId) -> Option<WorkflowProgress> {
        let workflow = self.workflows.get(workflow_id)?;
        let mut progress = WorkflowProgress {
            id: workflow.id.clone(),
            objective: workflow.objective.clone(),
            priority: workflow.priority,
            total_tasks: workflow.tasks.len(),
            tasks_pending: 0,
            tasks_running: 0,
            tasks_blocked: 0,
            tasks_complete: 0,
            tasks_failed: 0,
            tasks_cancelled: 0,
            tasks_missing: 0,
            current_stage: None,
            stages: Vec::new(),
        };

        let mut stages = workflow.tasks.iter().collect::<Vec<_>>();
        stages.sort_by_key(|(stage, task_id)| {
            (
                workflow_stage_depth(self, workflow, task_id),
                stage.as_str().to_owned(),
            )
        });

        for (stage, task_id) in stages {
            let task = self.tasks.get(task_id);
            let status = task.map(|task| task.status.clone());
            match status.as_ref() {
                Some(TaskStatus::Pending) => progress.tasks_pending += 1,
                Some(TaskStatus::Running) => progress.tasks_running += 1,
                Some(TaskStatus::Blocked) => progress.tasks_blocked += 1,
                Some(TaskStatus::Complete) => progress.tasks_complete += 1,
                Some(TaskStatus::Failed) => progress.tasks_failed += 1,
                Some(TaskStatus::Cancelled) => progress.tasks_cancelled += 1,
                None => progress.tasks_missing += 1,
            }
            if progress.current_stage.is_none() && status.as_ref() != Some(&TaskStatus::Complete) {
                progress.current_stage = Some(stage.clone());
            }
            progress.stages.push(WorkflowStageProgress {
                stage: stage.clone(),
                task_id: task_id.clone(),
                title: task.map(|task| task.title.clone()),
                status,
            });
        }

        Some(progress)
    }

    pub fn task_removal_blockers(&self, task_id: &TaskId) -> Vec<String> {
        let mut blockers = self
            .tasks
            .values()
            .filter(|task| {
                task.dependencies
                    .iter()
                    .any(|dependency| dependency == task_id)
            })
            .map(|task| format!("task {} depends on task {}", task.id, task_id))
            .collect::<Vec<_>>();
        blockers.extend(
            self.runs
                .values()
                .filter(|run| &run.task_id == task_id)
                .map(|run| format!("run {} references task {}", run.id, task_id)),
        );
        blockers.extend(self.workflows.values().filter_map(|workflow| {
            workflow.tasks.iter().find_map(|(stage, workflow_task_id)| {
                (workflow_task_id == task_id).then(|| {
                    format!(
                        "workflow {} stage {} references task {}",
                        workflow.id, stage, task_id
                    )
                })
            })
        }));
        blockers
    }

    pub fn write_memory(&mut self, mut record: MemoryRecord) {
        self.ensure_unique_memory_id(&mut record);
        let duplicate_ids = self
            .memory
            .iter()
            .filter(|existing| memory_records_duplicate(existing, &record))
            .map(|existing| existing.id.clone())
            .collect::<Vec<_>>();
        if !duplicate_ids.is_empty() {
            self.memory
                .retain(|existing| !duplicate_ids.iter().any(|id| id == &existing.id));
            for duplicate_id in duplicate_ids {
                self.record(
                    EventKind::MemoryRemoved,
                    format!(
                        "deduplicated memory {} before storing {}",
                        duplicate_id, record.id
                    ),
                );
            }
        }
        self.record(
            EventKind::MemoryWritten,
            format!("stored memory {} on {}", record.id, record.topic),
        );
        self.memory.push(record);
        self.touch();
    }

    pub fn provider_memory(&self, now: DateTime<Utc>) -> Vec<MemoryRecord> {
        let mut records = self
            .memory
            .iter()
            .filter(|record| memory_record_available_to_provider(record, &self.memory_policy, now))
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by_key(|record| std::cmp::Reverse(record.updated_at));
        records.truncate(self.memory_policy.max_provider_memories);
        records
    }

    pub fn provider_memory_for_task(&self, task: &Task, now: DateTime<Utc>) -> Vec<MemoryRecord> {
        if !self.memory_policy.semantic_recall {
            return self.provider_memory(now);
        }
        let query = memory_task_query(task);
        if query.trim().is_empty() {
            return self.provider_memory(now);
        }
        let mut scored = self
            .memory
            .iter()
            .filter(|record| memory_record_available_to_provider(record, &self.memory_policy, now))
            .filter_map(|record| {
                let score = memory_relevance_score(record, &query);
                (score > 0).then(|| (score, record.clone()))
            })
            .collect::<Vec<_>>();
        scored.sort_by(|(left_score, left), (right_score, right)| {
            right_score
                .cmp(left_score)
                .then_with(|| right.updated_at.cmp(&left.updated_at))
        });
        scored.truncate(self.memory_policy.max_provider_memories);
        scored.into_iter().map(|(_, record)| record).collect()
    }

    pub fn expired_memory(&self, now: DateTime<Utc>, max_age_days: u64) -> Vec<MemoryRecord> {
        let mut expired = self
            .memory
            .iter()
            .filter(|record| memory_record_older_than(record, now, max_age_days))
            .cloned()
            .collect::<Vec<_>>();
        expired.sort_by(|left, right| {
            left.updated_at
                .cmp(&right.updated_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        expired
    }

    pub fn prune_expired_memory(
        &mut self,
        now: DateTime<Utc>,
        max_age_days: u64,
    ) -> Vec<MemoryRecord> {
        let removed = self.expired_memory(now, max_age_days);
        let expired_ids = removed
            .iter()
            .map(|record| record.id.clone())
            .collect::<BTreeSet<_>>();
        self.memory
            .retain(|record| !expired_ids.contains(&record.id));
        for record in &removed {
            self.record(
                EventKind::MemoryRemoved,
                format!("expired memory {} after {} day(s)", record.id, max_age_days),
            );
        }
        if !removed.is_empty() {
            self.touch();
        }
        removed
    }

    pub fn remove_memory(&mut self, memory_id: &str) -> Option<MemoryRecord> {
        let position = self
            .memory
            .iter()
            .position(|record| record.id == memory_id)?;
        let removed = self.memory.remove(position);
        self.record(
            EventKind::MemoryRemoved,
            format!("removed memory {}", removed.id),
        );
        Some(removed)
    }

    pub fn update_memory(
        &mut self,
        memory_id: &str,
        topic: Option<String>,
        body: Option<String>,
        tags: Option<Vec<String>>,
        visibility: Option<MemoryVisibility>,
        scope: Option<Option<String>>,
    ) -> Option<MemoryRecord> {
        let updated = {
            let record = self
                .memory
                .iter_mut()
                .find(|record| record.id == memory_id)?;
            if let Some(topic) = topic {
                record.topic = topic;
            }
            if let Some(body) = body {
                record.body = body;
            }
            if let Some(tags) = tags {
                record.tags = normalize_list(tags);
            }
            if let Some(visibility) = visibility {
                record.visibility = visibility;
            }
            if let Some(scope) = scope {
                record.scope = scope;
            }
            record.updated_at = Utc::now();
            record.clone()
        };
        let duplicate_ids = self
            .memory
            .iter()
            .filter(|existing| {
                existing.id != updated.id && memory_records_duplicate(existing, &updated)
            })
            .map(|existing| existing.id.clone())
            .collect::<Vec<_>>();
        if !duplicate_ids.is_empty() {
            self.memory
                .retain(|existing| !duplicate_ids.iter().any(|id| id == &existing.id));
            for duplicate_id in duplicate_ids {
                self.record(
                    EventKind::MemoryRemoved,
                    format!(
                        "deduplicated memory {} after updating {}",
                        duplicate_id, updated.id
                    ),
                );
            }
        }
        self.record(
            EventKind::MemoryUpdated,
            format!("updated memory {}", memory_id),
        );
        Some(updated)
    }

    pub fn try_register_tool(&mut self, tool: ToolDefinition) -> bool {
        if self.tools.contains_key(&tool.id) {
            return false;
        }
        self.record(
            EventKind::ToolRegistered,
            format!("registered tool {} ({})", tool.id, tool.kind),
        );
        self.tools.insert(tool.id.clone(), tool);
        self.touch();
        true
    }

    pub fn register_tool(&mut self, tool: ToolDefinition) {
        let _ = self.try_register_tool(tool);
    }

    pub fn try_register_worker(&mut self, worker: WorkerNode) -> bool {
        if self.workers.contains_key(&worker.id) {
            return false;
        }
        self.record(
            EventKind::WorkerRegistered,
            format!("registered worker {}", worker.id),
        );
        self.workers.insert(worker.id.clone(), worker);
        self.touch();
        true
    }

    pub fn register_worker(&mut self, worker: WorkerNode) {
        let _ = self.try_register_worker(worker);
    }

    pub fn try_register_mcp_server(&mut self, server: McpServer) -> bool {
        if self.mcp_servers.contains_key(&server.id) {
            return false;
        }
        self.record(
            EventKind::McpServerRegistered,
            format!("registered mcp server {}", server.id),
        );
        self.mcp_servers.insert(server.id.clone(), server);
        self.touch();
        true
    }

    pub fn register_mcp_server(&mut self, server: McpServer) {
        let _ = self.try_register_mcp_server(server);
    }

    pub fn try_register_secrets_backend(&mut self, backend: SecretsBackend) -> bool {
        if self.secrets_backends.contains_key(&backend.id) {
            return false;
        }
        self.record(
            EventKind::SecretsBackendRegistered,
            format!("registered secrets backend {}", backend.id),
        );
        self.secrets_backends.insert(backend.id.clone(), backend);
        self.touch();
        true
    }

    pub fn register_secrets_backend(&mut self, backend: SecretsBackend) {
        let _ = self.try_register_secrets_backend(backend);
    }

    pub fn record_eval(&mut self, mut record: EvalRecord, action: impl AsRef<str>) -> EvalRecord {
        self.ensure_unique_eval_id(&mut record);
        let record = record;
        self.evals.push(record.clone());
        self.record(
            EventKind::EvalRecorded,
            format!(
                "{} eval {} for {}",
                action.as_ref(),
                record.id,
                record.target
            ),
        );
        record
    }

    pub fn request_approval(&mut self, mut request: ApprovalRequest) {
        self.ensure_unique_approval_id(&mut request);
        self.record(
            EventKind::ApprovalRequested,
            format!(
                "approval {} requested for task {}",
                request.id, request.task_id
            ),
        );
        self.approvals.insert(request.id.clone(), request);
        self.touch();
    }

    pub fn resolve_approval(
        &mut self,
        approval_id: &str,
        approved: bool,
        resolved_by: Option<String>,
    ) -> Option<ApprovalRequest> {
        let request = self.approvals.get_mut(approval_id)?;
        if request.status != ApprovalStatus::Pending {
            return Some(request.clone());
        }
        request.status = if approved {
            ApprovalStatus::Approved
        } else {
            ApprovalStatus::Denied
        };
        request.resolved_at = Some(Utc::now());
        request.resolved_by = resolved_by;
        let updated = request.clone();
        let pending_approval_for_task = self.approvals.values().any(|approval| {
            approval.id != updated.id
                && approval.task_id == updated.task_id
                && approval.status == ApprovalStatus::Pending
        });
        let task_is_blocked = self
            .tasks
            .get(&updated.task_id)
            .map(|task| task.status == TaskStatus::Blocked)
            .unwrap_or(false);
        if task_is_blocked {
            for agent in self.agents.values_mut() {
                agent
                    .current_tasks
                    .retain(|task_id| task_id != &updated.task_id);
                agent.updated_at = Utc::now();
            }
        }
        if let Some(task) = self.tasks.get_mut(&updated.task_id)
            && task.status == TaskStatus::Blocked
        {
            task.assigned_to = None;
            if updated.status == ApprovalStatus::Denied {
                task.status = TaskStatus::Failed;
                task.output = Some(format!("approval {} denied", updated.id));
                task.updated_at = Utc::now();
            } else if !pending_approval_for_task {
                task.status = TaskStatus::Pending;
                task.assigned_to = None;
                task.output = Some(format!("approval {} approved", updated.id));
                task.updated_at = Utc::now();
            }
        }
        self.record(
            EventKind::ApprovalResolved,
            format!("approval {} resolved as {:?}", approval_id, updated.status),
        );
        Some(updated)
    }

    pub fn tool_removal_blockers(&self, tool_id: &ToolId) -> Vec<String> {
        self.tasks
            .values()
            .filter_map(|task| {
                task.tool
                    .as_ref()
                    .filter(|tool| &tool.tool_id == tool_id)
                    .map(|_| format!("task {} invokes tool {}", task.id, tool_id))
            })
            .collect()
    }

    pub fn remove_tool(&mut self, tool_id: &ToolId) -> Option<ToolDefinition> {
        let removed = self.tools.remove(tool_id);
        if removed.is_some() {
            self.record(EventKind::ToolRemoved, format!("removed tool {}", tool_id));
        }
        removed
    }

    pub fn record(&mut self, kind: EventKind, message: impl Into<String>) {
        let mut event = Event::new(kind, message);
        self.ensure_unique_event_id(&mut event);
        self.events.push(event);
        self.events.sort_by_key(|event| event.at);
        if self.events.len() > 500 {
            self.events.drain(0..self.events.len() - 500);
        }
        self.touch();
    }

    pub fn touch(&mut self) {
        self.updated_at = Utc::now();
    }
}

fn workflow_stage_depth(os: &OperatingSystem, workflow: &Workflow, task_id: &TaskId) -> usize {
    fn depth(
        os: &OperatingSystem,
        workflow: &Workflow,
        task_id: &TaskId,
        seen: &mut Vec<TaskId>,
    ) -> usize {
        if seen.iter().any(|seen_id| seen_id == task_id) {
            return 0;
        }
        seen.push(task_id.clone());
        os.tasks
            .get(task_id)
            .map(|task| {
                task.dependencies
                    .iter()
                    .filter(|dependency| {
                        workflow
                            .tasks
                            .values()
                            .any(|workflow_task_id| workflow_task_id == *dependency)
                    })
                    .map(|dependency| {
                        let mut branch = seen.clone();
                        1 + depth(os, workflow, dependency, &mut branch)
                    })
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0)
    }

    depth(os, workflow, task_id, &mut Vec::new())
}

fn default_agent_profiles() -> BTreeMap<String, AgentProfile> {
    [
        AgentProfile {
            id: "rust-project-maintainer".into(),
            name: "Rust project maintainer".into(),
            kind: AgentKind::Builder,
            model: None,
            capabilities: vec!["rust".into(), "code".into(), "review".into()],
            system_prompt: Some(
                "Maintain Rust code with tests, formatting, and focused diffs.".into(),
            ),
        },
        AgentProfile {
            id: "ci-fixer".into(),
            name: "CI fixer".into(),
            kind: AgentKind::Operator,
            model: None,
            capabilities: vec!["ci".into(), "debug".into(), "code".into()],
            system_prompt: Some(
                "Reproduce CI failures locally, isolate root cause, and patch narrowly.".into(),
            ),
        },
        AgentProfile {
            id: "research-assistant".into(),
            name: "Research assistant".into(),
            kind: AgentKind::Researcher,
            model: None,
            capabilities: vec!["research".into(), "summarize".into()],
            system_prompt: Some(
                "Collect sources, synthesize findings, and cite assumptions.".into(),
            ),
        },
        AgentProfile {
            id: "ops-bot".into(),
            name: "Ops bot".into(),
            kind: AgentKind::Operator,
            model: None,
            capabilities: vec!["ops".into(), "monitoring".into()],
            system_prompt: Some(
                "Watch operational signals and request approval before risky changes.".into(),
            ),
        },
    ]
    .into_iter()
    .map(|profile| (profile.id.clone(), profile))
    .collect()
}

fn default_workflow_templates() -> BTreeMap<String, WorkflowTemplate> {
    [
        WorkflowTemplate {
            id: "plan-build-review".into(),
            name: "Plan, build, review".into(),
            description: "Planner, builder, and reviewer dependency chain.".into(),
            stages: vec!["plan".into(), "build".into(), "review".into()],
            tasks: Vec::new(),
            edges: Vec::new(),
        },
        WorkflowTemplate {
            id: "ci-fix".into(),
            name: "CI fix".into(),
            description: "Reproduce, patch, verify, and summarize a CI failure.".into(),
            stages: vec![
                "reproduce".into(),
                "patch".into(),
                "verify".into(),
                "summarize".into(),
            ],
            tasks: Vec::new(),
            edges: Vec::new(),
        },
    ]
    .into_iter()
    .map(|template| (template.id.clone(), template))
    .collect()
}

fn default_secrets_backends() -> BTreeMap<String, SecretsBackend> {
    [SecretsBackend {
        id: "environment".into(),
        kind: SecretsBackendKind::Environment,
        reference: None,
    }]
    .into_iter()
    .map(|backend| (backend.id.clone(), backend))
    .collect()
}

fn memory_records_duplicate(left: &MemoryRecord, right: &MemoryRecord) -> bool {
    memory_dedupe_key(left) == memory_dedupe_key(right)
}

fn memory_record_available_to_provider(
    record: &MemoryRecord,
    policy: &MemoryPolicy,
    now: DateTime<Utc>,
) -> bool {
    !memory_record_expired(record, policy, now)
        && record.visibility == MemoryVisibility::Shared
        && match (&record.scope, &policy.scope) {
            (Some(record_scope), Some(policy_scope)) => record_scope == policy_scope,
            (Some(_), None) => false,
            (None, _) => true,
        }
}

fn memory_record_expired(record: &MemoryRecord, policy: &MemoryPolicy, now: DateTime<Utc>) -> bool {
    policy
        .max_age_days
        .map(|max_age_days| memory_record_older_than(record, now, max_age_days))
        .unwrap_or(false)
}

fn memory_record_older_than(record: &MemoryRecord, now: DateTime<Utc>, max_age_days: u64) -> bool {
    let days = i64::try_from(max_age_days).unwrap_or(i64::MAX);
    let cutoff = now - chrono::Duration::days(days);
    record.updated_at <= cutoff
}

fn memory_task_query(task: &Task) -> String {
    let mut parts = vec![task.title.clone(), task.objective.clone()];
    parts.extend(task.required_capabilities.iter().cloned());
    parts.join(" ")
}

fn memory_dedupe_key(
    record: &MemoryRecord,
) -> (String, String, Vec<String>, String, Option<String>) {
    (
        record.topic.trim().to_ascii_lowercase(),
        record.body.trim().to_owned(),
        normalize_list(record.tags.clone()),
        record.visibility.as_str().to_owned(),
        record.scope.as_ref().map(|scope| scope.trim().to_owned()),
    )
}

pub fn normalize_list(values: Vec<String>) -> Vec<String> {
    let mut values = values
        .into_iter()
        .flat_map(|value| {
            value
                .split(',')
                .map(|part| part.trim().to_ascii_lowercase())
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

pub fn is_valid_env_var_name(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

pub fn is_valid_provider_endpoint(value: &str) -> bool {
    let Ok(url) = url::Url::parse(value) else {
        return false;
    };
    matches!(url.scheme(), "http" | "https") && url.host_str().is_some()
}

pub fn is_valid_slug(value: &str) -> bool {
    !value.is_empty() && normalize_slug(value.to_owned()) == value
}

fn normalize_slug(value: String) -> String {
    let slug = value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    slug.split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

#[cfg(test)]
mod tests {
    use super::{
        Agent, AgentKind, AgentStatus, ApprovalRequest, ApprovalStatus, EvalRecord, Event,
        EventKind, McpServer, MemoryPolicy, MemoryRecord, MemoryVisibility, OperatingSystem,
        Priority, RunRecord, SecretsBackend, SecretsBackendKind, Task, TaskStatus, ToolDefinition,
        ToolId, ToolInvocation, ToolKind, WorkerNode, Workflow, is_valid_env_var_name,
        memory_recall_hit, memory_relevance_score, secret_check_report, tail_text_by_bytes,
        text_tail_was_truncated,
    };
    use chrono::{Duration, TimeZone, Utc};
    use std::collections::BTreeMap;

    #[test]
    fn validates_shell_style_environment_variable_names() {
        assert!(is_valid_env_var_name("PATH"));
        assert!(is_valid_env_var_name("_AGENT_OS_TOKEN"));
        assert!(is_valid_env_var_name("AGENT_OS_TOKEN_2"));
        assert!(!is_valid_env_var_name(""));
        assert!(!is_valid_env_var_name("1BAD"));
        assert!(!is_valid_env_var_name(" BAD"));
        assert!(!is_valid_env_var_name("BAD "));
        assert!(!is_valid_env_var_name("BAD=VALUE"));
        assert!(!is_valid_env_var_name("BAD-NAME"));
        assert!(!is_valid_env_var_name("BAD.NAME"));
    }

    #[test]
    fn secret_check_report_reads_task_tool_secret_env_args() {
        let mut os = OperatingSystem::new("test");
        let mut task = Task::new("Use secret", "call tool", Priority::Normal, vec![]);
        task.tool = Some(ToolInvocation::with_secret_env_args(
            ToolId::new("deploy"),
            BTreeMap::new(),
            BTreeMap::from([
                ("token".into(), "AGENT_OS_TEST_TOKEN".into()),
                ("bad".into(), "1BAD".into()),
            ]),
        ));
        let task_id = task.id.clone();
        os.create_task(task);

        let report = secret_check_report(&os);

        assert_eq!(report.total, 2);
        assert_eq!(report.invalid, 1);
        assert!(
            report
                .references
                .iter()
                .any(|reference| reference.task_id == task_id
                    && reference.arg == "token"
                    && reference.env == "AGENT_OS_TEST_TOKEN"
                    && reference.valid_env)
        );
    }

    #[test]
    fn tail_text_by_bytes_returns_full_body_without_limit_or_short_body() {
        assert_eq!(tail_text_by_bytes("agent-os", None), "agent-os");
        assert_eq!(tail_text_by_bytes("agent-os", Some(99)), "agent-os");
    }

    #[test]
    fn tail_text_by_bytes_returns_ascii_suffix() {
        assert_eq!(tail_text_by_bytes("agent-os", Some(2)), "os");
    }

    #[test]
    fn tail_text_by_bytes_preserves_utf8_boundaries() {
        assert_eq!(tail_text_by_bytes("alpha λ", Some(1)), "");
        assert_eq!(tail_text_by_bytes("alpha λ", Some(2)), "λ");
        assert_eq!(tail_text_by_bytes("alpha λ", Some(3)), " λ");
    }

    #[test]
    fn text_tail_was_truncated_reports_byte_limit_effect() {
        assert!(!text_tail_was_truncated("agent-os", None));
        assert!(!text_tail_was_truncated("agent-os", Some(99)));
        assert!(text_tail_was_truncated("agent-os", Some(2)));
        assert!(text_tail_was_truncated("alpha λ", Some(1)));
    }

    #[test]
    fn create_task_retries_generated_id_collisions() {
        let mut os = OperatingSystem::new("test");
        let first = Task::new("First", "first", Priority::Normal, vec![]);
        let colliding_id = first.id.clone();
        os.create_task(first);

        let mut second = Task::new("Second", "second", Priority::Normal, vec![]);
        second.id = colliding_id.clone();
        os.create_task(second);

        assert_eq!(
            os.tasks
                .get(&colliding_id)
                .expect("first task")
                .title
                .as_str(),
            "First"
        );
        assert_eq!(os.tasks.len(), 2);
        assert!(os.tasks.values().any(|task| task.title == "Second"));
    }

    #[test]
    fn register_agent_refuses_duplicate_ids_without_overwrite() {
        let mut os = OperatingSystem::new("test");
        let first = Agent::new(
            "Builder",
            AgentKind::Builder,
            Some("model-a".into()),
            vec!["rust".into()],
            1,
        );
        let id = first.id.clone();
        assert!(os.try_register_agent(first));
        let duplicate = Agent::new(
            "Builder",
            AgentKind::Reviewer,
            Some("model-b".into()),
            vec!["review".into()],
            2,
        );

        assert!(!os.try_register_agent(duplicate));

        let stored = os.agents.get(&id).expect("agent");
        assert_eq!(stored.kind, AgentKind::Builder);
        assert_eq!(stored.model.as_deref(), Some("model-a"));
        assert_eq!(stored.max_parallel_tasks, 1);
    }

    #[test]
    fn register_tool_refuses_duplicate_ids_without_overwrite() {
        let mut os = OperatingSystem::new("test");
        let first = ToolDefinition::new(
            "cargo test",
            ToolKind::Shell,
            "run tests",
            vec!["rust".into()],
            "cargo test",
            None,
        );
        let id = first.id.clone();
        assert!(os.try_register_tool(first));
        let duplicate = ToolDefinition::new(
            "cargo test",
            ToolKind::FileRead,
            "read file",
            vec!["docs".into()],
            "README.md",
            None,
        );

        assert!(!os.try_register_tool(duplicate));

        let stored = os.tools.get(&id).expect("tool");
        assert_eq!(stored.kind, ToolKind::Shell);
        assert_eq!(stored.command_template, "cargo test");
        assert_eq!(stored.required_capabilities, vec!["rust"]);
    }

    #[test]
    fn register_worker_refuses_duplicate_ids_without_overwrite() {
        let mut os = OperatingSystem::new("test");
        let first = WorkerNode {
            id: "worker-a".into(),
            endpoint: "http://127.0.0.1:9000".into(),
            status: AgentStatus::Online,
            last_seen_at: Utc::now(),
        };
        assert!(os.try_register_worker(first));
        let duplicate = WorkerNode {
            id: "worker-a".into(),
            endpoint: "http://127.0.0.1:9001".into(),
            status: AgentStatus::Busy,
            last_seen_at: Utc::now(),
        };

        assert!(!os.try_register_worker(duplicate));

        let stored = os.workers.get("worker-a").expect("worker");
        assert_eq!(stored.endpoint, "http://127.0.0.1:9000");
        assert_eq!(stored.status, AgentStatus::Online);
    }

    #[test]
    fn register_mcp_server_refuses_duplicate_ids_without_overwrite() {
        let mut os = OperatingSystem::new("test");
        let first = McpServer {
            id: "docs".into(),
            command: "docs-server".into(),
            args: vec!["--stdio".into()],
            env: BTreeMap::new(),
            enabled: true,
        };
        assert!(os.try_register_mcp_server(first));
        let duplicate = McpServer {
            id: "docs".into(),
            command: "other-server".into(),
            args: vec![],
            env: BTreeMap::new(),
            enabled: false,
        };

        assert!(!os.try_register_mcp_server(duplicate));

        let stored = os.mcp_servers.get("docs").expect("server");
        assert_eq!(stored.command, "docs-server");
        assert_eq!(stored.args, vec!["--stdio"]);
        assert!(stored.enabled);
    }

    #[test]
    fn register_secrets_backend_refuses_duplicate_ids_without_overwrite() {
        let mut os = OperatingSystem::new("test");
        let first = SecretsBackend {
            id: "prod".into(),
            kind: SecretsBackendKind::Environment,
            reference: Some("PROD_".into()),
        };
        assert!(os.try_register_secrets_backend(first));
        let duplicate = SecretsBackend {
            id: "prod".into(),
            kind: SecretsBackendKind::EnvVault,
            reference: Some("vault://prod".into()),
        };

        assert!(!os.try_register_secrets_backend(duplicate));

        let stored = os.secrets_backends.get("prod").expect("backend");
        assert_eq!(stored.kind, SecretsBackendKind::Environment);
        assert_eq!(stored.reference.as_deref(), Some("PROD_"));
    }

    #[test]
    fn create_workflow_retries_generated_id_collisions() {
        let mut os = OperatingSystem::new("test");
        let first = Workflow::new("First", Priority::Normal, BTreeMap::new());
        let colliding_id = first.id.clone();
        os.create_workflow(first);

        let mut second = Workflow::new("Second", Priority::Normal, BTreeMap::new());
        second.id = colliding_id.clone();
        os.create_workflow(second);

        assert_eq!(
            os.workflows
                .get(&colliding_id)
                .expect("first workflow")
                .objective
                .as_str(),
            "First"
        );
        assert_eq!(os.workflows.len(), 2);
        assert!(
            os.workflows
                .values()
                .any(|workflow| workflow.objective == "Second")
        );
    }

    #[test]
    fn resolving_approval_resumes_blocked_task() {
        let mut os = OperatingSystem::new("test");
        let mut task = Task::new("Deploy", "deploy after review", Priority::Normal, vec![]);
        task.status = TaskStatus::Blocked;
        task.output = Some("waiting on approval".into());
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec![], 1);
        let agent_id = agent.id.clone();
        task.assigned_to = Some(agent_id.clone());
        let task_id = task.id.clone();
        agent.current_tasks.push(task_id.clone());
        os.register_agent(agent);
        os.create_task(task);
        let approval = ApprovalRequest::new(
            task_id.clone(),
            None,
            "git push origin main",
            "matched risky pattern",
        );
        let approval_id = approval.id.clone();
        os.request_approval(approval);

        let resolved = os
            .resolve_approval(&approval_id, true, Some("operator".into()))
            .expect("approval");

        assert_eq!(resolved.status, ApprovalStatus::Approved);
        let task = os.tasks.get(&task_id).expect("task");
        assert_eq!(task.status, TaskStatus::Pending);
        assert!(task.assigned_to.is_none());
        assert!(
            os.agents
                .get(&agent_id)
                .expect("agent")
                .current_tasks
                .is_empty()
        );
        assert_eq!(
            task.output.as_deref(),
            Some(format!("approval {approval_id} approved").as_str())
        );
    }

    #[test]
    fn denying_approval_fails_blocked_task_and_resolution_is_final() {
        let mut os = OperatingSystem::new("test");
        let mut task = Task::new("Deploy", "deploy after review", Priority::Normal, vec![]);
        task.status = TaskStatus::Blocked;
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec![], 1);
        let agent_id = agent.id.clone();
        task.assigned_to = Some(agent_id.clone());
        let task_id = task.id.clone();
        agent.current_tasks.push(task_id.clone());
        os.register_agent(agent);
        os.create_task(task);
        let approval = ApprovalRequest::new(
            task_id.clone(),
            None,
            "git push origin main",
            "matched risky pattern",
        );
        let approval_id = approval.id.clone();
        os.request_approval(approval);

        let denied = os
            .resolve_approval(&approval_id, false, Some("operator".into()))
            .expect("approval");
        let second_resolution = os
            .resolve_approval(&approval_id, true, Some("operator".into()))
            .expect("approval");

        assert_eq!(denied.status, ApprovalStatus::Denied);
        assert_eq!(second_resolution.status, ApprovalStatus::Denied);
        let task = os.tasks.get(&task_id).expect("task");
        assert_eq!(task.status, TaskStatus::Failed);
        assert!(task.assigned_to.is_none());
        assert!(
            os.agents
                .get(&agent_id)
                .expect("agent")
                .current_tasks
                .is_empty()
        );
        assert_eq!(
            task.output.as_deref(),
            Some(format!("approval {approval_id} denied").as_str())
        );
    }

    #[test]
    fn request_approval_retries_generated_id_collisions() {
        let mut os = OperatingSystem::new("test");
        let task = Task::new("Deploy", "deploy", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);
        let first = ApprovalRequest::new(task_id.clone(), None, "git push", "risky");
        let colliding_id = first.id.clone();
        os.request_approval(first);

        let mut second = ApprovalRequest::new(task_id, None, "rm -rf target", "risky");
        second.id = colliding_id.clone();
        os.request_approval(second);

        assert_eq!(
            os.approvals
                .get(&colliding_id)
                .expect("first approval")
                .action
                .as_str(),
            "git push"
        );
        assert_eq!(os.approvals.len(), 2);
        assert!(
            os.approvals.values().any(|approval| {
                approval.id != colliding_id && approval.action == "rm -rf target"
            })
        );
    }

    #[test]
    fn ensure_unique_run_id_retries_generated_id_collisions() {
        let mut os = OperatingSystem::new("test");
        let task = Task::new("Task", "task", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);
        let first = RunRecord::new(task_id.clone(), None, "first", ".");
        let colliding_id = first.id.clone();
        os.runs.insert(first.id.clone(), first);

        let mut second = RunRecord::new(task_id, None, "second", ".");
        second.id = colliding_id.clone();
        os.ensure_unique_run_id(&mut second);

        assert_ne!(second.id, colliding_id);
        assert!(!os.runs.contains_key(&second.id));
    }

    #[test]
    fn write_memory_retries_generated_id_collisions_for_distinct_records() {
        let mut os = OperatingSystem::new("test");
        let first = MemoryRecord::new("Ops", "Remember this", vec!["rust".into()]);
        let colliding_id = first.id.clone();
        os.write_memory(first);

        let mut second = MemoryRecord::new("Security", "Use safe profile", vec!["ops".into()]);
        second.id = colliding_id.clone();
        os.write_memory(second);

        assert_eq!(os.memory.len(), 2);
        assert!(
            os.memory
                .iter()
                .any(|record| record.id == colliding_id && record.topic == "Ops")
        );
        assert!(
            os.memory
                .iter()
                .any(|record| { record.id != colliding_id && record.topic == "Security" })
        );
    }

    #[test]
    fn ensure_unique_eval_id_retries_generated_id_collisions() {
        let mut os = OperatingSystem::new("test");
        let first = EvalRecord {
            id: os.next_eval_id(),
            target: "quality".into(),
            success: true,
            cost_micros: None,
            latency_ms: None,
            run: None,
            recorded_at: Utc::now(),
        };
        let colliding_id = first.id.clone();
        os.evals.push(first);
        let mut second = EvalRecord {
            id: colliding_id.clone(),
            target: "quality".into(),
            success: false,
            cost_micros: None,
            latency_ms: None,
            run: None,
            recorded_at: Utc::now(),
        };

        os.ensure_unique_eval_id(&mut second);

        assert_ne!(second.id, colliding_id);
        assert!(second.id.starts_with("eval-"));
        assert!(!os.evals.iter().any(|record| record.id == second.id));
    }

    #[test]
    fn record_eval_retries_generated_id_collisions_and_records_event() {
        let mut os = OperatingSystem::new("test");
        let first = EvalRecord {
            id: os.next_eval_id(),
            target: "quality".into(),
            success: true,
            cost_micros: None,
            latency_ms: None,
            run: None,
            recorded_at: Utc::now(),
        };
        let colliding_id = first.id.clone();
        os.record_eval(first, "recorded");
        let second = EvalRecord {
            id: colliding_id.clone(),
            target: "quality".into(),
            success: false,
            cost_micros: None,
            latency_ms: None,
            run: None,
            recorded_at: Utc::now(),
        };

        let recorded = os.record_eval(second, "ran");

        assert_ne!(recorded.id, colliding_id);
        assert_eq!(os.evals.len(), 2);
        assert!(os.evals.iter().any(|record| record.id == colliding_id));
        assert!(os.evals.iter().any(|record| record.id == recorded.id));
        assert!(os.events.iter().any(|event| {
            event.kind == EventKind::EvalRecorded
                && event.message == format!("ran eval {} for quality", recorded.id)
        }));
    }

    #[test]
    fn ensure_unique_event_id_retries_generated_id_collisions() {
        let mut os = OperatingSystem::new("test");
        let first = Event::new(EventKind::TaskCreated, "first");
        let colliding_id = first.id.clone();
        os.events.push(first);
        let mut second = Event::new(EventKind::TaskUpdated, "second");
        second.id = colliding_id.clone();

        os.ensure_unique_event_id(&mut second);

        assert_ne!(second.id, colliding_id);
        assert!(!os.events.iter().any(|event| event.id == second.id));
    }

    #[test]
    fn write_memory_deduplicates_normalized_content() {
        let mut os = OperatingSystem::new("test");
        let first = MemoryRecord::new("Ops", "Remember this", vec!["rust".into(), "ci".into()]);
        let first_id = first.id.clone();
        os.write_memory(first);
        let second = MemoryRecord::new(" ops ", "Remember this ", vec!["CI,RUST".into()]);
        let second_id = second.id.clone();
        os.write_memory(second);

        assert_eq!(os.memory.len(), 1);
        assert_eq!(os.memory[0].id, second_id);
        assert!(!os.memory.iter().any(|record| record.id == first_id));
        assert!(os.events.iter().any(|event| {
            event.kind == EventKind::MemoryRemoved
                && event.message.contains("deduplicated memory")
                && event.message.contains(&first_id)
        }));
    }

    #[test]
    fn update_memory_deduplicates_normalized_content() {
        let mut os = OperatingSystem::new("test");
        let first = MemoryRecord::with_access(
            "Ops",
            "Remember this",
            vec!["rust".into(), "ci".into()],
            MemoryVisibility::Private,
            Some("workspace-a".into()),
        );
        let first_id = first.id.clone();
        os.write_memory(first);
        let second = MemoryRecord::with_access(
            "Temporary",
            "Different note",
            vec!["draft".into()],
            MemoryVisibility::Shared,
            None,
        );
        let second_id = second.id.clone();
        os.write_memory(second);

        let updated = os
            .update_memory(
                &second_id,
                Some(" ops ".into()),
                Some("Remember this ".into()),
                Some(vec!["CI,RUST".into()]),
                Some(MemoryVisibility::Private),
                Some(Some("workspace-a".into())),
            )
            .expect("updated memory");

        assert_eq!(updated.id, second_id);
        assert_eq!(os.memory.len(), 1);
        assert_eq!(os.memory[0].id, second_id);
        assert!(!os.memory.iter().any(|record| record.id == first_id));
        assert!(os.events.iter().any(|event| {
            event.kind == EventKind::MemoryRemoved
                && event.message.contains("deduplicated memory")
                && event.message.contains(&first_id)
                && event.message.contains(&second_id)
        }));
    }

    #[test]
    fn provider_memory_filters_expired_records_and_applies_cap() {
        let now = Utc.with_ymd_and_hms(2026, 5, 20, 12, 0, 0).unwrap();
        let mut os = OperatingSystem::new("test");
        os.memory_policy = MemoryPolicy {
            max_provider_memories: 2,
            max_age_days: Some(30),
            ..MemoryPolicy::default()
        };

        for (topic, age_days) in [("old", 45), ("third", 3), ("first", 1), ("second", 2)] {
            let mut record = MemoryRecord::new(topic, format!("{topic} body"), vec![]);
            record.created_at = now - Duration::days(age_days);
            record.updated_at = now - Duration::days(age_days);
            os.write_memory(record);
        }

        let memory = os.provider_memory(now);

        assert_eq!(memory.len(), 2);
        assert_eq!(memory[0].topic, "first");
        assert_eq!(memory[1].topic, "second");
        assert!(!memory.iter().any(|record| record.topic == "old"));
    }

    #[test]
    fn provider_memory_only_uses_shared_records_in_active_scope() {
        let now = Utc.with_ymd_and_hms(2026, 5, 20, 12, 0, 0).unwrap();
        let mut os = OperatingSystem::new("test");
        os.memory_policy = MemoryPolicy {
            scope: Some("workspace-a".into()),
            max_provider_memories: 10,
            ..MemoryPolicy::default()
        };

        os.write_memory(MemoryRecord::with_access(
            "global shared",
            "available to every provider scope",
            vec![],
            MemoryVisibility::Shared,
            None,
        ));
        os.write_memory(MemoryRecord::with_access(
            "scoped shared",
            "available to workspace a",
            vec![],
            MemoryVisibility::Shared,
            Some("workspace-a".into()),
        ));
        os.write_memory(MemoryRecord::with_access(
            "other scoped shared",
            "not available to workspace a",
            vec![],
            MemoryVisibility::Shared,
            Some("workspace-b".into()),
        ));
        os.write_memory(MemoryRecord::with_access(
            "private note",
            "never sent to providers",
            vec![],
            MemoryVisibility::Private,
            None,
        ));

        let topics = os
            .provider_memory(now)
            .into_iter()
            .map(|record| record.topic)
            .collect::<Vec<_>>();

        assert!(topics.contains(&"global shared".to_owned()));
        assert!(topics.contains(&"scoped shared".to_owned()));
        assert!(!topics.contains(&"other scoped shared".to_owned()));
        assert!(!topics.contains(&"private note".to_owned()));
    }

    #[test]
    fn memory_relevance_scores_token_overlap_without_exact_substring() {
        let record = MemoryRecord::new(
            "database tuning",
            "Pool idle postgres connections before API requests",
            vec!["runtime".into()],
        );

        assert!(memory_relevance_score(&record, "connection pooling postgres") > 0);
        assert_eq!(
            memory_relevance_score(&record, "unrelated mobile design"),
            0
        );
    }

    #[test]
    fn memory_recall_hit_includes_score_and_focused_snippet() {
        let body = format!(
            "{} PostgreSQL connection pooling should be tuned before running scheduler workers. {}",
            "intro ".repeat(80),
            "tail ".repeat(80)
        );
        let record = MemoryRecord::new("database operations", body, vec!["backend".into()]);
        let score = memory_relevance_score(&record, "connection pooling scheduler");
        let hit = memory_recall_hit(&record, "connection pooling scheduler", score);

        assert!(hit.score > 0);
        assert_eq!(hit.record.topic, "database operations");
        assert!(hit.snippet.contains("connection pooling"));
        assert!(hit.snippet.len() < hit.record.body.len());
    }

    #[test]
    fn semantic_provider_memory_ranks_task_relevant_records() {
        let now = Utc.with_ymd_and_hms(2026, 5, 20, 12, 0, 0).unwrap();
        let mut os = OperatingSystem::new("test");
        os.memory_policy = MemoryPolicy {
            semantic_recall: true,
            max_provider_memories: 2,
            ..MemoryPolicy::default()
        };
        let task = Task::new(
            "Fix postgres pooling",
            "Tune database connection reuse",
            Priority::Normal,
            vec!["database".into()],
        );
        let mut unrelated = MemoryRecord::new("latest ui", "Polish mobile navigation", vec![]);
        unrelated.updated_at = now;
        os.write_memory(unrelated);
        let mut relevant = MemoryRecord::new(
            "older database",
            "Postgres pool settings cap idle connections",
            vec!["backend".into()],
        );
        relevant.updated_at = now - Duration::days(7);
        os.write_memory(relevant);

        let memory = os.provider_memory_for_task(&task, now);

        assert_eq!(memory.len(), 1);
        assert_eq!(memory[0].topic, "older database");
    }

    #[test]
    fn prune_expired_memory_removes_old_records_and_records_events() {
        let now = Utc.with_ymd_and_hms(2026, 5, 20, 12, 0, 0).unwrap();
        let mut os = OperatingSystem::new("test");
        let mut less_expired = MemoryRecord::new("less expired", "less expired body", vec![]);
        let less_expired_id = less_expired.id.clone();
        less_expired.created_at = now - Duration::days(31);
        less_expired.updated_at = now - Duration::days(31);
        os.write_memory(less_expired);
        let mut expired = MemoryRecord::new("expired", "expired body", vec![]);
        let expired_id = expired.id.clone();
        expired.created_at = now - Duration::days(40);
        expired.updated_at = now - Duration::days(40);
        os.write_memory(expired);
        let mut retained = MemoryRecord::new("retained", "retained body", vec![]);
        retained.created_at = now - Duration::days(1);
        retained.updated_at = now - Duration::days(1);
        os.write_memory(retained);

        let removed = os.prune_expired_memory(now, 30);

        assert_eq!(removed.len(), 2);
        assert_eq!(removed[0].id, expired_id);
        assert_eq!(removed[1].id, less_expired_id);
        assert!(!os.memory.iter().any(|record| record.id == expired_id));
        assert!(!os.memory.iter().any(|record| record.id == less_expired_id));
        assert!(os.memory.iter().any(|record| record.topic == "retained"));
        assert!(os.events.iter().any(|event| {
            event.kind == EventKind::MemoryRemoved
                && event.message.contains("expired memory")
                && event.message.contains(&expired_id)
        }));
    }
}
