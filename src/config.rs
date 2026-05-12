use crate::models::{
    Agent, AgentId, AgentKind, Policy, ProviderKind, ProviderSettings, ToolDefinition, ToolId,
    ToolKind, is_valid_env_var_name, is_valid_provider_endpoint, normalize_list,
};
use crate::tools::validate_tool_template;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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
            agents: default_agents(),
            tools: default_tools(),
        }
    }
}

impl AppConfig {
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
    if path.exists() && !force {
        return Err(ConfigError::Io {
            path: path.to_path_buf(),
            source: io::Error::new(io::ErrorKind::AlreadyExists, "config already exists"),
        });
    }
    let body = toml::to_string_pretty(&AppConfig::default())?;
    write_file_atomic_creating_parent(path, body.as_bytes()).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
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
    if matches!(config.provider.kind, ProviderKind::OpenAiCompatible)
        && config.provider.endpoint.is_none()
    {
        return Err("provider endpoint is required for openai-compatible provider".into());
    }
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

fn write_file_atomic_creating_parent(path: &Path, body: &[u8]) -> io::Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let temp_path = unique_temp_config_path(path);
    let mut temp_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)?;
    if let Err(error) = temp_file.write_all(body) {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    drop(temp_file);
    if let Err(error) = fs::rename(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
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

fn unique_temp_config_path(path: &Path) -> PathBuf {
    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "config".into());
    let process_id = std::process::id();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);

    parent.join(format!(".{file_name}.{process_id}.{nanos}.{counter}.tmp"))
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
"#,
        )
        .expect("config");

        validate_seed_config(&config).expect("valid config");
        assert_eq!(config.provider.kind, ProviderKind::OpenAiCompatible);
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
