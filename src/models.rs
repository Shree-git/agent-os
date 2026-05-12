use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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
            plan: Vec::new(),
            output: None,
            created_at: now,
            updated_at: now,
        }
    }
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
    #[serde(rename = "openai-compatible", alias = "open-ai-compatible")]
    OpenAiCompatible,
}

impl fmt::Display for ProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mock => f.write_str("mock"),
            Self::OpenAiCompatible => f.write_str("openai-compatible"),
        }
    }
}

impl ProviderKind {
    pub const VALUES: &'static [&'static str] = &["mock", "openai-compatible"];
    pub const INPUT_VALUES: &'static [&'static str] =
        &["mock", "openai-compatible", "open-ai-compatible"];
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
}

impl Default for ProviderSettings {
    fn default() -> Self {
        Self {
            kind: default_provider_kind(),
            model: default_provider_model(),
            endpoint: None,
            api_key_env: default_api_key_env(),
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: RunId,
    pub task_id: TaskId,
    pub agent_id: Option<AgentId>,
    pub command: String,
    pub cwd: String,
    pub status: RunStatus,
    pub exit_code: Option<i32>,
    pub log_path: Option<String>,
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
            task_id,
            agent_id,
            command: command.into(),
            cwd: cwd.into(),
            status: RunStatus::Running,
            exit_code: None,
            log_path: None,
            started_at: Utc::now(),
            finished_at: None,
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
        }
    }
}

fn default_allow_shell() -> bool {
    true
}

fn default_allowed_workspaces() -> Vec<String> {
    vec![".".into()]
}

fn default_denied_patterns() -> Vec<String> {
    vec![
        "rm -rf".into(),
        "sudo".into(),
        "shutdown".into(),
        "reboot".into(),
        "mkfs".into(),
        "dd if=".into(),
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
pub struct MemoryRecord {
    pub id: String,
    pub topic: String,
    pub body: String,
    pub tags: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MemoryRecord {
    pub fn new(topic: impl Into<String>, body: impl Into<String>, tags: Vec<String>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().simple().to_string()[..10].to_owned(),
            topic: topic.into(),
            body: body.into(),
            tags: normalize_list(tags),
            created_at: now,
            updated_at: now,
        }
    }
}

pub fn memory_matches_query(record: &MemoryRecord, query: &str) -> bool {
    let query = query.to_ascii_lowercase();
    record.topic.to_ascii_lowercase().contains(&query)
        || record.body.to_ascii_lowercase().contains(&query)
        || record
            .tags
            .iter()
            .any(|tag| tag.to_ascii_lowercase().contains(&query))
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
    pub memory: Vec<MemoryRecord>,
    pub events: Vec<Event>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl OperatingSystem {
    pub fn new(name: impl Into<String>) -> Self {
        let now = Utc::now();
        let mut os = Self {
            version: 3,
            name: name.into(),
            agents: BTreeMap::new(),
            tasks: BTreeMap::new(),
            workflows: BTreeMap::new(),
            runs: BTreeMap::new(),
            policy: Policy::default(),
            provider: ProviderSettings::default(),
            daemon: None,
            tools: BTreeMap::new(),
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

    pub fn register_agent(&mut self, agent: Agent) {
        self.record(
            EventKind::AgentRegistered,
            format!("registered agent {} ({})", agent.id, agent.kind),
        );
        self.agents.insert(agent.id.clone(), agent);
        self.touch();
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

    pub fn create_task(&mut self, task: Task) {
        self.record(
            EventKind::TaskCreated,
            format!("created task {}: {}", task.id, task.title),
        );
        self.tasks.insert(task.id.clone(), task);
        self.touch();
    }

    pub fn create_workflow(&mut self, workflow: Workflow) {
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

    pub fn write_memory(&mut self, record: MemoryRecord) {
        self.record(
            EventKind::MemoryWritten,
            format!("stored memory {} on {}", record.id, record.topic),
        );
        self.memory.push(record);
        self.touch();
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
    ) -> Option<MemoryRecord> {
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
        record.updated_at = Utc::now();
        let updated = record.clone();
        self.record(
            EventKind::MemoryUpdated,
            format!("updated memory {}", memory_id),
        );
        Some(updated)
    }

    pub fn register_tool(&mut self, tool: ToolDefinition) {
        self.record(
            EventKind::ToolRegistered,
            format!("registered tool {} ({})", tool.id, tool.kind),
        );
        self.tools.insert(tool.id.clone(), tool);
        self.touch();
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
        self.events.push(Event::new(kind, message));
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
    use super::{is_valid_env_var_name, tail_text_by_bytes, text_tail_was_truncated};

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
}
