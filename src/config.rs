use crate::durable_io;
use crate::models::{
    Agent, AgentId, AgentKind, AutonomyLevel, MAX_PROVIDER_RETRIES, MemoryPolicy, NetworkMode,
    Policy, ProviderKind, ProviderSettings, ToolDefinition, ToolId, ToolKind,
    is_valid_env_var_name, is_valid_provider_endpoint, normalize_list,
};
use crate::tools::validate_tool_template;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    #[serde(default = "default_name")]
    pub name: String,
    #[serde(default)]
    pub policy: Policy,
    #[serde(default)]
    pub provider: ProviderSettings,
    #[serde(default)]
    pub memory_policy: MemoryPolicy,
    #[serde(default = "default_agents")]
    pub agents: Vec<AgentConfig>,
    #[serde(default = "default_tools")]
    pub tools: Vec<ToolConfig>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            name: default_name(),
            policy: Policy::default(),
            provider: ProviderSettings::default(),
            memory_policy: MemoryPolicy::default(),
            agents: default_agents(),
            tools: default_tools(),
        }
    }
}

impl AppConfig {
    pub fn effective_default() -> Self {
        Self::for_profile(ConfigProfile::Safe)
    }

    pub fn for_profile(profile: ConfigProfile) -> Self {
        let mut config = Self::default();
        config.apply_profile(profile);
        config
    }

    pub fn apply_profile(&mut self, profile: ConfigProfile) {
        match profile {
            ConfigProfile::Dev => {
                self.policy.allow_shell = true;
                self.policy.allowed_commands.clear();
                self.policy.allowed_workspaces = vec![".".into()];
                self.policy.max_output_bytes = 65_536;
                self.policy.command_timeout_seconds = 60;
                self.policy.inherit_environment = false;
                self.policy.network.mode = NetworkMode::Allowed;
                self.policy.approval.require_for_risky_actions = false;
                self.policy.autonomy = AutonomyLevel::ExecuteFreely;
            }
            ConfigProfile::Safe => {
                self.policy.allow_shell = false;
                self.policy.allowed_commands.clear();
                self.policy.allowed_workspaces = vec![".".into()];
                self.policy.max_output_bytes = 65_536;
                self.policy.command_timeout_seconds = 60;
                self.policy.inherit_environment = false;
                self.policy.network.mode = NetworkMode::ProvidersOnly;
                self.policy.approval.require_for_risky_actions = true;
                self.policy.autonomy = AutonomyLevel::ExecuteWithApproval;
            }
            ConfigProfile::Autonomous => {
                self.policy.allow_shell = true;
                self.policy.allowed_commands.clear();
                self.policy.allowed_workspaces = vec![".".into()];
                self.policy.network.mode = NetworkMode::Allowed;
                self.policy.approval.require_for_risky_actions = false;
                self.policy.autonomy = AutonomyLevel::ExecuteFreely;
            }
            ConfigProfile::Ci => {
                self.policy.allow_shell = true;
                self.policy.allowed_commands = vec!["cargo".into(), "rustc".into(), "git".into()];
                self.policy.allowed_workspaces = vec![".".into()];
                self.policy.max_output_bytes = 262_144;
                self.policy.command_timeout_seconds = 600;
                self.policy.inherit_environment = false;
                self.policy.network.mode = NetworkMode::Disabled;
                self.policy.approval.require_for_risky_actions = false;
                self.policy.autonomy = AutonomyLevel::ExecuteFreely;
            }
        }
    }

    pub fn into_agents(self) -> Vec<Agent> {
        self.agents
            .into_iter()
            .map(AgentConfig::into_agent)
            .collect()
    }

    pub fn into_tools(self) -> Vec<ToolDefinition> {
        self.tools.into_iter().map(ToolConfig::into_tool).collect()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConfigProfile {
    #[default]
    Safe,
    Dev,
    Autonomous,
    Ci,
}

impl ConfigProfile {
    pub const VALUES: &'static [&'static str] = &["safe", "dev", "autonomous", "ci"];

    pub fn try_parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "safe" => Some(Self::Safe),
            "dev" => Some(Self::Dev),
            "autonomous" => Some(Self::Autonomous),
            "ci" => Some(Self::Ci),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Safe => "safe",
            Self::Dev => "dev",
            Self::Autonomous => "autonomous",
            Self::Ci => "ci",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub name: String,
    #[serde(default = "default_agent_kind")]
    pub kind: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default = "default_parallel")]
    pub parallel: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolConfig {
    pub name: String,
    #[serde(default = "default_tool_kind")]
    pub kind: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    pub command_template: String,
    #[serde(default)]
    pub default_cwd: Option<String>,
}

impl ToolConfig {
    pub fn into_tool(self) -> ToolDefinition {
        ToolDefinition::new(
            self.name,
            ToolKind::parse(&self.kind),
            self.description,
            self.required_capabilities,
            self.command_template,
            self.default_cwd,
        )
    }
}

impl AgentConfig {
    pub fn into_agent(self) -> Agent {
        Agent::new(
            self.name,
            AgentKind::parse(&self.kind),
            self.model,
            self.capabilities,
            self.parallel,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ConfigSource {
    pub path: PathBuf,
    pub exists: bool,
    pub loaded: bool,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("io error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("invalid config at {path}: {source}")]
    Toml {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("could not serialize default config: {0}")]
    Serialize(#[from] toml::ser::Error),
}

pub fn discover_config(explicit: Option<PathBuf>) -> PathBuf {
    if let Some(path) = explicit {
        return path;
    }
    if let Ok(path) = std::env::var("AGENT_OS_CONFIG") {
        return PathBuf::from(path);
    }
    PathBuf::from("agent-os.toml")
}

pub fn load_config(path: &Path) -> Result<Option<AppConfig>, ConfigError> {
    if !path.exists() {
        return Ok(None);
    }
    let body = fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let config = toml::from_str(&body).map_err(|source| ConfigError::Toml {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(Some(config))
}

pub fn write_default_config(path: &Path, force: bool) -> Result<(), ConfigError> {
    write_profile_config(path, force, ConfigProfile::Safe)
}

pub fn write_profile_config(
    path: &Path,
    force: bool,
    profile: ConfigProfile,
) -> Result<(), ConfigError> {
    if path.exists() && !force {
        return Err(ConfigError::Io {
            path: path.to_path_buf(),
            source: io::Error::new(io::ErrorKind::AlreadyExists, "config already exists"),
        });
    }
    let body = toml::to_string_pretty(&AppConfig::for_profile(profile))?;
    durable_io::write_file_atomic_creating_parent(path, body.as_bytes()).map_err(|error| {
        ConfigError::Io {
            path: error.path,
            source: error.source,
        }
    })
}

pub fn validate_seed_config(config: &AppConfig) -> Result<(), String> {
    validate_os_name(&config.name)?;
    if config.provider.model.trim().is_empty() {
        return Err("provider model must not be empty".into());
    }
    validate_env_var_name("provider api_key_env", &config.provider.api_key_env)?;
    if let Some(endpoint) = &config.provider.endpoint
        && endpoint.trim().is_empty()
    {
        return Err("provider endpoint must not be empty".into());
    }
    if let Some(endpoint) = &config.provider.endpoint
        && !is_valid_provider_endpoint(endpoint)
    {
        return Err("provider endpoint must be an absolute http(s) URL".into());
    }
    if matches!(config.provider.kind, ProviderKind::Plugin) {
        validate_plugin_provider_settings(&config.provider)?;
    } else if !matches!(config.provider.kind, ProviderKind::Mock)
        && config.provider.endpoint.is_none()
    {
        return Err(format!(
            "provider endpoint is required for {} provider",
            config.provider.kind
        ));
    }
    if config.provider.request_timeout_seconds == 0 {
        return Err("provider request_timeout_seconds must be greater than 0".into());
    }
    if config.provider.max_retries > MAX_PROVIDER_RETRIES {
        return Err(format!(
            "provider max_retries must be less than or equal to {MAX_PROVIDER_RETRIES}"
        ));
    }
    if config.provider.retry_backoff_ms == 0 {
        return Err("provider retry_backoff_ms must be greater than 0".into());
    }
    validate_optional_provider_adapter(config.provider.adapter.as_deref())?;
    validate_provider_request_options(&config.provider)?;
    if config.memory_policy.max_provider_memories == 0 {
        return Err("memory_policy max_provider_memories must be greater than 0".into());
    }
    if matches!(config.memory_policy.max_age_days, Some(0)) {
        return Err("memory_policy max_age_days must be greater than 0 when set".into());
    }
    validate_optional_text("memory_policy scope", config.memory_policy.scope.as_deref())?;
    if config.policy.max_output_bytes == 0 {
        return Err("policy max_output_bytes must be greater than 0".into());
    }
    if config.policy.command_timeout_seconds == 0 {
        return Err("policy command_timeout_seconds must be greater than 0".into());
    }
    validate_no_empty_values("policy allowed_commands", &config.policy.allowed_commands)?;
    validate_no_empty_values(
        "policy allowed_workspaces",
        &config.policy.allowed_workspaces,
    )?;
    validate_no_empty_values("policy denied_patterns", &config.policy.denied_patterns)?;
    validate_env_var_names("policy allowed_env_vars", &config.policy.allowed_env_vars)?;
    validate_no_empty_values(
        "policy redacted_env_patterns",
        &config.policy.redacted_env_patterns,
    )?;
    validate_no_empty_values(
        "policy sandbox writable_paths",
        &config.policy.sandbox.writable_paths,
    )?;
    validate_no_empty_values(
        "policy network allowed_hosts",
        &config.policy.network.allowed_hosts,
    )?;
    validate_no_empty_values("policy rules", &config.policy.rules)?;

    let mut agent_ids = std::collections::BTreeSet::new();
    for agent in &config.agents {
        validate_agent_name(&agent.name)?;
        if agent.kind.trim().is_empty() {
            return Err(format!("agent {} kind must not be empty", agent.name));
        }
        validate_optional_text(
            &format!("agent {} model", agent.name),
            agent.model.as_deref(),
        )?;
        validate_capability_values(
            &format!("agent {} capabilities", agent.name),
            &agent.capabilities,
            false,
        )?;
        if agent.parallel == 0 {
            return Err(format!(
                "agent {} parallel must be greater than 0",
                agent.name
            ));
        }
        let id = AgentId::new(&agent.name);
        if !agent_ids.insert(id.clone()) {
            return Err(format!("duplicate agent id in config: {id}"));
        }
    }

    let mut tool_ids = std::collections::BTreeSet::new();
    for tool in &config.tools {
        validate_tool_name(&tool.name)?;
        validate_tool_command_template(&tool.command_template)?;
        let Some(kind) = ToolKind::try_parse(&tool.kind) else {
            return Err(format!(
                "invalid tool kind `{}` in config for {}; expected shell, file-read/read-file, or file-write/write-file",
                tool.kind, tool.name
            ));
        };
        validate_tool_template(&ToolDefinition::new(
            tool.name.clone(),
            kind,
            String::new(),
            Vec::new(),
            tool.command_template.clone(),
            None,
        ))
        .map_err(|error| error.to_string())?;
        validate_optional_text(
            &format!("tool {} default_cwd", tool.name),
            tool.default_cwd.as_deref(),
        )?;
        validate_capability_values(
            &format!("tool {} required capabilities", tool.name),
            &tool.required_capabilities,
            false,
        )?;
        let id = ToolId::new(&tool.name);
        if !tool_ids.insert(id.clone()) {
            return Err(format!("duplicate tool id in config: {id}"));
        }
    }
    Ok(())
}

fn validate_plugin_provider_settings(provider: &ProviderSettings) -> Result<(), String> {
    let Some(command) = provider.plugin_command.as_deref() else {
        return Err("provider plugin_command is required for plugin provider".into());
    };
    validate_optional_text("provider plugin_command", Some(command))?;
    validate_no_empty_values("provider plugin_args", &provider.plugin_args)?;
    for key in provider.plugin_env.keys() {
        validate_env_var_name(&format!("provider plugin_env key {key}"), key)?;
    }
    Ok(())
}

fn validate_os_name(name: &str) -> Result<(), String> {
    if name.trim().is_empty() {
        return Err("OS name must not be empty".into());
    }
    Ok(())
}

fn validate_agent_name(name: &str) -> Result<(), String> {
    if AgentId::new(name).as_str().is_empty() {
        return Err("agent name must contain at least one ASCII letter, digit, or hyphen".into());
    }
    Ok(())
}

fn validate_tool_name(name: &str) -> Result<(), String> {
    if ToolId::new(name).as_str().is_empty() {
        return Err("tool name must contain at least one ASCII letter, digit, or hyphen".into());
    }
    Ok(())
}

fn validate_tool_command_template(command_template: &str) -> Result<(), String> {
    if command_template.trim().is_empty() {
        return Err("tool command template must not be empty".into());
    }
    Ok(())
}

fn validate_capability_values(
    field: &str,
    values: &[String],
    require_non_empty: bool,
) -> Result<(), String> {
    for value in values {
        if value.trim().is_empty() || value.split(',').any(|part| part.trim().is_empty()) {
            return Err(format!("{field} must not contain empty capabilities"));
        }
    }
    if require_non_empty && normalize_list(values.to_vec()).is_empty() {
        return Err(format!("{field} must include at least one capability"));
    }
    Ok(())
}

fn validate_no_empty_values(field: &str, values: &[String]) -> Result<(), String> {
    if values.iter().any(|value| value.trim().is_empty()) {
        return Err(format!("{field} must not contain empty values"));
    }
    Ok(())
}

fn validate_provider_request_options(provider: &ProviderSettings) -> Result<(), String> {
    for key in provider.request_options.keys() {
        if key.trim().is_empty() {
            return Err("provider request_options contains an empty key".into());
        }
        if key != key.trim() {
            return Err(format!(
                "provider request_options key `{key}` must not have surrounding whitespace"
            ));
        }
        if provider_request_option_is_reserved(provider, key) {
            return Err(format!(
                "provider request_options cannot override `{key}`; use provider model or agent model instead"
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProviderAdapterFamily {
    OpenAi,
    Anthropic,
    Gemini,
    Ollama,
}

fn provider_adapter_family(provider: &ProviderSettings) -> Option<ProviderAdapterFamily> {
    if let Some(adapter) = provider.adapter.as_deref() {
        return match adapter.trim().to_ascii_lowercase().as_str() {
            "openai" | "openai-chat" | "openai-compatible" | "chat-completions" => {
                Some(ProviderAdapterFamily::OpenAi)
            }
            "anthropic" | "anthropic-messages" | "claude" => Some(ProviderAdapterFamily::Anthropic),
            "gemini" | "gemini-generate-content" | "google-gemini" => {
                Some(ProviderAdapterFamily::Gemini)
            }
            "ollama" | "ollama-chat" => Some(ProviderAdapterFamily::Ollama),
            _ => None,
        };
    }
    Some(match provider.kind {
        ProviderKind::Anthropic => ProviderAdapterFamily::Anthropic,
        ProviderKind::Gemini => ProviderAdapterFamily::Gemini,
        ProviderKind::Ollama => ProviderAdapterFamily::Ollama,
        ProviderKind::Mock
        | ProviderKind::OpenAi
        | ProviderKind::OpenAiCompatible
        | ProviderKind::Custom
        | ProviderKind::Plugin
        | ProviderKind::Local => ProviderAdapterFamily::OpenAi,
    })
}

fn provider_request_option_is_reserved(provider: &ProviderSettings, key: &str) -> bool {
    match provider_adapter_family(provider) {
        Some(ProviderAdapterFamily::OpenAi) | Some(ProviderAdapterFamily::Ollama) => {
            matches!(key, "model" | "messages")
        }
        Some(ProviderAdapterFamily::Anthropic) => matches!(key, "model" | "messages" | "system"),
        Some(ProviderAdapterFamily::Gemini) => matches!(key, "contents"),
        None => false,
    }
}

fn validate_optional_provider_adapter(adapter: Option<&str>) -> Result<(), String> {
    let Some(adapter) = adapter else {
        return Ok(());
    };
    if adapter.trim().is_empty() {
        return Err("provider adapter must not be empty".into());
    }
    if !matches!(
        adapter.trim().to_ascii_lowercase().as_str(),
        "openai"
            | "openai-chat"
            | "openai-compatible"
            | "chat-completions"
            | "anthropic"
            | "anthropic-messages"
            | "claude"
            | "gemini"
            | "gemini-generate-content"
            | "google-gemini"
            | "ollama"
            | "ollama-chat"
    ) {
        return Err(format!("unsupported provider adapter `{adapter}`"));
    }
    Ok(())
}

fn validate_env_var_name(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    if !is_valid_env_var_name(value) {
        return Err(format!("{field} must be a valid environment variable name"));
    }
    Ok(())
}

fn validate_env_var_names(field: &str, values: &[String]) -> Result<(), String> {
    validate_no_empty_values(field, values)?;
    if values.iter().any(|value| !is_valid_env_var_name(value)) {
        return Err(format!(
            "{field} must contain valid environment variable names"
        ));
    }
    Ok(())
}

fn validate_optional_text(field: &str, value: Option<&str>) -> Result<(), String> {
    if let Some(value) = value
        && value.trim().is_empty()
    {
        return Err(format!("{field} must not be empty"));
    }
    Ok(())
}

#[cfg(test)]
fn temp_config_path(path: &Path) -> PathBuf {
    let mut extension = path
        .extension()
        .map(|extension| extension.to_os_string())
        .unwrap_or_default();
    if extension.is_empty() {
        extension.push("tmp");
    } else {
        extension.push(".tmp");
    }
    path.with_extension(extension)
}

pub fn source_status(path: PathBuf) -> ConfigSource {
    let exists = path.exists();
    let loaded = exists && load_config(&path).is_ok();
    ConfigSource {
        path,
        exists,
        loaded,
    }
}

fn default_name() -> String {
    "Agent OS".into()
}

fn default_agent_kind() -> String {
    "builder".into()
}

fn default_parallel() -> usize {
    1
}

fn default_tool_kind() -> String {
    "shell".into()
}

fn default_agents() -> Vec<AgentConfig> {
    vec![
        AgentConfig {
            name: "architect".into(),
            kind: "planner".into(),
            model: Some("local-default".into()),
            capabilities: vec!["plan".into(), "review".into(), "system".into()],
            parallel: 2,
        },
        AgentConfig {
            name: "builder".into(),
            kind: "builder".into(),
            model: Some("local-default".into()),
            capabilities: vec!["code".into(), "rust".into(), "test".into()],
            parallel: 2,
        },
    ]
}

fn default_tools() -> Vec<ToolConfig> {
    vec![ToolConfig {
        name: "cargo-test".into(),
        kind: "shell".into(),
        description: "Run the Rust test suite in a workspace".into(),
        required_capabilities: vec!["rust".into(), "test".into()],
        command_template: "cargo test".into(),
        default_cwd: None,
    }]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ProviderKind, ToolKind};

    #[test]
    fn parses_policy_and_agents() {
        let config: AppConfig = toml::from_str(
            r#"
name = "Test OS"

[policy]
allow_shell = false
allowed_commands = ["printf"]
denied_patterns = ["rm -rf"]
max_output_bytes = 1024
inherit_environment = false
allowed_env_vars = ["PATH"]
redacted_env_patterns = ["SECRET"]

[provider]
kind = "mock"
model = "mock-agent"

[[agents]]
name = "critic"
kind = "reviewer"
capabilities = ["review", "risk"]
parallel = 3

[[tools]]
name = "say"
kind = "shell"
description = "Print a message"
required_capabilities = ["rust"]
command_template = "printf {message}"
"#,
        )
        .expect("config");

        assert_eq!(config.name, "Test OS");
        assert!(!config.policy.allow_shell);
        assert_eq!(config.agents[0].name, "critic");
        assert_eq!(config.tools[0].name, "say");
    }

    #[test]
    fn parses_documented_openai_provider_kind() {
        let config: AppConfig = toml::from_str(
            r#"
[provider]
kind = "openai-compatible"
endpoint = "https://api.openai.com/v1/chat/completions"
"#,
        )
        .expect("config");

        assert_eq!(config.provider.kind, ProviderKind::OpenAiCompatible);
        assert_eq!(
            config.provider.endpoint.as_deref(),
            Some("https://api.openai.com/v1/chat/completions")
        );
    }

    #[test]
    fn parses_documented_openai_provider_kind_alias() {
        let config: AppConfig = toml::from_str(
            r#"
[provider]
kind = "open-ai-compatible"
endpoint = "https://api.openai.com/v1/chat/completions"
max_retries = 3
retry_backoff_ms = 100
request_options = { temperature = 0.1, top_p = 0.9 }
"#,
        )
        .expect("config");

        validate_seed_config(&config).expect("valid config");
        assert_eq!(config.provider.kind, ProviderKind::OpenAiCompatible);
        assert_eq!(config.provider.max_retries, 3);
        assert_eq!(config.provider.retry_backoff_ms, 100);
        assert_eq!(
            config.provider.request_options.get("temperature"),
            Some(&serde_json::json!(0.1))
        );
    }

    #[test]
    fn parses_provider_plugin_kinds_and_custom_adapter() {
        for (kind, endpoint) in [
            ("openai", "https://api.openai.com/v1/chat/completions"),
            ("anthropic", "https://api.anthropic.com/v1/messages"),
            (
                "gemini",
                "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent",
            ),
            ("ollama", "http://localhost:11434/api/chat"),
            ("local", "http://localhost:8080/v1/chat/completions"),
        ] {
            let body = format!(
                r#"
[provider]
kind = "{kind}"
endpoint = "{endpoint}"
"#
            );
            let config: AppConfig = toml::from_str(&body).expect("config");

            validate_seed_config(&config).expect("valid provider plugin config");
        }

        let custom: AppConfig = toml::from_str(
            r#"
[provider]
kind = "custom"
adapter = "anthropic"
endpoint = "https://provider.example/v1/messages"
"#,
        )
        .expect("custom config");

        validate_seed_config(&custom).expect("valid custom adapter config");
        assert_eq!(custom.provider.adapter.as_deref(), Some("anthropic"));
    }

    #[test]
    fn rejects_unsupported_provider_adapter() {
        let config: AppConfig = toml::from_str(
            r#"
[provider]
kind = "custom"
adapter = "not-real"
endpoint = "https://provider.example/v1"
"#,
        )
        .expect("config");

        let error = validate_seed_config(&config).expect_err("invalid adapter");

        assert!(error.contains("unsupported provider adapter"));
    }

    #[test]
    fn provider_request_options_cannot_override_contract_fields() {
        let config: AppConfig = toml::from_str(
            r#"
[provider]
kind = "openai-compatible"
endpoint = "https://api.openai.com/v1/chat/completions"
request_options = { model = "other-model" }
"#,
        )
        .expect("config");

        let error = validate_seed_config(&config).expect_err("invalid request option");

        assert!(error.contains("provider request_options cannot override `model`"));
    }

    #[test]
    fn provider_request_options_reject_adapter_reserved_fields() {
        let config: AppConfig = toml::from_str(
            r#"
[provider]
kind = "custom"
adapter = "anthropic"
endpoint = "https://provider.example/v1/messages"
request_options = { system = "ignored system prompt", max_tokens = 2048 }
"#,
        )
        .expect("config");

        let error = validate_seed_config(&config).expect_err("reserved request option");

        assert!(error.contains("provider request_options cannot override `system`"));
    }

    #[test]
    fn provider_retries_are_capped() {
        let config: AppConfig = toml::from_str(
            r#"
[provider]
kind = "openai-compatible"
endpoint = "https://api.openai.com/v1/chat/completions"
max_retries = 9
"#,
        )
        .expect("config");

        let error = validate_seed_config(&config).expect_err("retry cap");

        assert!(error.contains("provider max_retries must be less than or equal to 8"));
    }

    #[test]
    fn config_profiles_apply_expected_policy_defaults() {
        assert_eq!(ConfigProfile::default(), ConfigProfile::Safe);
        let model_default = AppConfig::default();
        assert!(!model_default.policy.allow_shell);
        assert_eq!(
            model_default.policy.network.mode,
            NetworkMode::ProvidersOnly
        );
        assert!(model_default.policy.approval.require_for_risky_actions);
        assert_eq!(
            model_default.policy.autonomy,
            AutonomyLevel::ExecuteWithApproval
        );

        let generated_default = AppConfig::for_profile(ConfigProfile::default());
        assert!(!generated_default.policy.allow_shell);
        assert_eq!(
            generated_default.policy.network.mode,
            NetworkMode::ProvidersOnly
        );

        let safe = AppConfig::for_profile(ConfigProfile::Safe);
        assert!(!safe.policy.allow_shell);
        assert_eq!(safe.policy.network.mode, NetworkMode::ProvidersOnly);
        assert!(safe.policy.approval.require_for_risky_actions);
        assert_eq!(safe.policy.autonomy, AutonomyLevel::ExecuteWithApproval);

        let ci = AppConfig::for_profile(ConfigProfile::Ci);
        assert!(ci.policy.allow_shell);
        assert_eq!(ci.policy.allowed_commands, vec!["cargo", "rustc", "git"]);
        assert_eq!(ci.policy.network.mode, NetworkMode::Disabled);
        assert_eq!(ci.policy.command_timeout_seconds, 600);

        let autonomous = AppConfig::for_profile(ConfigProfile::Autonomous);
        assert!(autonomous.policy.allow_shell);
        assert!(autonomous.policy.allowed_commands.is_empty());
        assert_eq!(autonomous.policy.network.mode, NetworkMode::Allowed);
    }

    #[test]
    fn partial_policy_config_keeps_safe_missing_defaults() {
        let config: AppConfig = toml::from_str(
            r#"
name = "partial"

[policy]
allowed_workspaces = ["."]
"#,
        )
        .expect("partial config");

        assert!(!config.policy.allow_shell);
        assert_eq!(config.policy.network.mode, NetworkMode::ProvidersOnly);
        assert!(config.policy.approval.require_for_risky_actions);
        assert_eq!(config.policy.autonomy, AutonomyLevel::ExecuteWithApproval);
    }

    #[test]
    fn writes_selected_config_profile() {
        let dir = tempfile::tempdir().expect("tempdir");
        let destination = dir.path().join("agent-os.toml");

        write_profile_config(&destination, false, ConfigProfile::Safe).expect("write safe config");
        let config = load_config(&destination)
            .expect("load")
            .expect("config exists");

        assert!(!config.policy.allow_shell);
        assert_eq!(config.policy.network.mode, NetworkMode::ProvidersOnly);
    }

    #[test]
    fn tool_kind_aliases_validate_and_normalize() {
        let config: AppConfig = toml::from_str(
            r#"
[[tools]]
name = "read-note"
kind = "read-file"
command_template = "notes/{name}.txt"

[[tools]]
name = "write-note"
kind = "write-file"
command_template = "notes/{name}.txt"
"#,
        )
        .expect("config");

        validate_seed_config(&config).expect("valid config");
        let tools = config.into_tools();

        assert_eq!(tools[0].kind, ToolKind::FileRead);
        assert_eq!(tools[1].kind, ToolKind::FileWrite);
    }

    #[test]
    fn default_config_write_removes_temp_file_when_rename_fails() {
        let dir = tempfile::tempdir().expect("tempdir");
        let destination = dir.path().join("agent-os.toml");
        fs::create_dir(&destination).expect("destination directory");

        let error = write_default_config(&destination, true)
            .expect_err("rename over directory should fail");

        assert!(matches!(error, ConfigError::Io { .. }));
        assert!(destination.is_dir());
        assert_eq!(fs::read_dir(dir.path()).expect("read dir").count(), 1);
    }

    #[test]
    fn default_config_write_preserves_existing_legacy_temp_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let destination = dir.path().join("agent-os.toml");
        let legacy_temp_path = temp_config_path(&destination);
        fs::write(&legacy_temp_path, "sentinel").expect("legacy temp");

        write_default_config(&destination, true).expect("write config");

        assert!(load_config(&destination).expect("load").is_some());
        assert_eq!(
            fs::read_to_string(&legacy_temp_path).expect("legacy temp"),
            "sentinel"
        );
    }
}
