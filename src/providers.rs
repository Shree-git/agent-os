use crate::models::{
    Agent, MAX_PROVIDER_RETRIES, MemoryRecord, ProviderKind, ProviderSettings, Task,
    ToolDefinition, is_valid_env_var_name, is_valid_provider_endpoint,
};
use crate::process_tree::terminate_child_process_tree;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use thiserror::Error;

const MAX_PROVIDER_RETRY_BACKOFF_FACTOR: u32 = 16;
const MAX_PROVIDER_RETRY_AFTER_SECONDS: u64 = 60;
const MAX_PROVIDER_HTTP_BODY_BYTES: usize = 2 * 1024 * 1024;
const MAX_PLUGIN_OUTPUT_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProviderRequest {
    pub agent_name: String,
    pub agent_kind: String,
    pub model: Option<String>,
    pub task_title: String,
    pub objective: String,
    pub capabilities: Vec<String>,
    pub existing_plan: Vec<String>,
    pub available_tools: Vec<ProviderTool>,
    pub memory: Vec<ProviderMemory>,
}

impl ProviderRequest {
    pub fn new(agent: Option<&Agent>, task: &Task) -> Self {
        Self::with_context(agent, task, &[], &[])
    }

    pub fn with_context(
        agent: Option<&Agent>,
        task: &Task,
        tools: &[ToolDefinition],
        memory: &[MemoryRecord],
    ) -> Self {
        Self::with_context_limited(agent, task, tools, memory, 5)
    }

    pub fn with_context_limited(
        agent: Option<&Agent>,
        task: &Task,
        tools: &[ToolDefinition],
        memory: &[MemoryRecord],
        max_memory: usize,
    ) -> Self {
        let mut memory = memory.iter().collect::<Vec<_>>();
        memory.sort_by_key(|record| std::cmp::Reverse(record.updated_at));
        memory.truncate(max_memory);
        Self::from_ordered_memory(agent, task, tools, memory)
    }

    pub fn with_ordered_context_limited(
        agent: Option<&Agent>,
        task: &Task,
        tools: &[ToolDefinition],
        memory: &[MemoryRecord],
        max_memory: usize,
    ) -> Self {
        let mut memory = memory.iter().collect::<Vec<_>>();
        memory.truncate(max_memory);
        Self::from_ordered_memory(agent, task, tools, memory)
    }

    fn from_ordered_memory(
        agent: Option<&Agent>,
        task: &Task,
        tools: &[ToolDefinition],
        memory: Vec<&MemoryRecord>,
    ) -> Self {
        Self {
            agent_name: agent
                .map(|agent| agent.name.clone())
                .unwrap_or_else(|| "unassigned-agent".into()),
            agent_kind: agent
                .map(|agent| agent.kind.to_string())
                .unwrap_or_else(|| "operator".into()),
            model: agent.and_then(|agent| agent.model.clone()),
            task_title: task.title.clone(),
            objective: task.objective.clone(),
            capabilities: task.required_capabilities.clone(),
            existing_plan: task.plan.clone(),
            available_tools: tools
                .iter()
                .filter(|tool| {
                    tool.required_capabilities.is_empty()
                        || tool
                            .required_capabilities
                            .iter()
                            .all(|capability| task.required_capabilities.contains(capability))
                })
                .map(ProviderTool::from)
                .collect(),
            memory: memory.into_iter().map(ProviderMemory::from).collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProviderTool {
    pub name: String,
    pub kind: String,
    pub description: String,
    pub required_capabilities: Vec<String>,
}

impl From<&ToolDefinition> for ProviderTool {
    fn from(tool: &ToolDefinition) -> Self {
        Self {
            name: tool.name.clone(),
            kind: tool.kind.to_string(),
            description: tool.description.clone(),
            required_capabilities: tool.required_capabilities.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProviderMemory {
    pub topic: String,
    pub body: String,
    pub tags: Vec<String>,
}

impl From<&MemoryRecord> for ProviderMemory {
    fn from(memory: &MemoryRecord) -> Self {
        Self {
            topic: memory.topic.clone(),
            body: memory.body.clone(),
            tags: memory.tags.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AgentResponse {
    pub summary: String,
    pub plan: Vec<String>,
    pub confidence: u8,
    #[serde(default)]
    pub tool_calls: Vec<ProviderToolCall>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProviderToolCall {
    pub tool: String,
    #[serde(default)]
    pub args: BTreeMap<String, String>,
}

#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("provider request is invalid: {0}")]
    InvalidRequest(String),
    #[error("provider is not configured: {0}")]
    NotConfigured(String),
    #[error("provider request failed: {0}")]
    Request(String),
    #[error("provider response was invalid: {0}")]
    InvalidResponse(String),
}

pub trait AgentProvider {
    fn complete(&self, request: &ProviderRequest) -> Result<AgentResponse, ProviderError>;
}

#[derive(Clone, Debug, Default)]
pub struct MockProvider;

impl AgentProvider for MockProvider {
    fn complete(&self, request: &ProviderRequest) -> Result<AgentResponse, ProviderError> {
        if request.objective.trim().is_empty() {
            return Err(ProviderError::InvalidRequest("objective is empty".into()));
        }

        let plan = if request.existing_plan.is_empty() {
            vec![
                format!("Clarify success criteria for `{}`.", request.task_title),
                "Inspect the current workspace and constraints.".into(),
                "Produce a focused implementation or decision record.".into(),
            ]
        } else {
            request.existing_plan.clone()
        };

        Ok(AgentResponse {
            summary: format!(
                "{} ({}) completed `{}` with capabilities [{}]. Objective: {}",
                request.agent_name,
                request.agent_kind,
                request.task_title,
                request.capabilities.join(", "),
                request.objective
            ),
            plan,
            confidence: 87,
            tool_calls: Vec::new(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct OpenAiCompatibleProvider {
    pub endpoint: String,
    pub model: String,
    pub api_key: Option<String>,
    pub adapter: ProviderAdapter,
    pub request_timeout: Duration,
    pub max_retries: u32,
    pub retry_backoff: Duration,
    pub request_options: BTreeMap<String, serde_json::Value>,
    pub response_schema: Option<serde_json::Value>,
}

#[derive(Clone, Debug)]
pub struct PluginProvider {
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub request_timeout: Duration,
    pub max_retries: u32,
    pub retry_backoff: Duration,
    pub response_schema: Option<serde_json::Value>,
}

impl PluginProvider {
    pub fn from_settings(settings: &ProviderSettings) -> Result<Self, ProviderError> {
        let command = settings
            .plugin_command
            .as_deref()
            .ok_or_else(|| {
                ProviderError::NotConfigured("provider.plugin_command is required".into())
            })?
            .trim();
        if command.is_empty() {
            return Err(ProviderError::NotConfigured(
                "provider.plugin_command must not be empty".into(),
            ));
        }
        for arg in &settings.plugin_args {
            if arg.trim().is_empty() {
                return Err(ProviderError::NotConfigured(
                    "provider.plugin_args must not contain empty values".into(),
                ));
            }
        }
        for key in settings.plugin_env.keys() {
            if !is_valid_env_var_name(key) {
                return Err(ProviderError::NotConfigured(format!(
                    "provider.plugin_env key `{key}` must be a valid environment variable name"
                )));
            }
        }
        if settings.request_timeout_seconds == 0 {
            return Err(ProviderError::NotConfigured(
                "provider request_timeout_seconds must be greater than 0".into(),
            ));
        }
        validate_provider_runtime_settings(settings)?;
        Ok(Self {
            command: command.to_owned(),
            args: settings.plugin_args.clone(),
            env: settings.plugin_env.clone(),
            request_timeout: Duration::from_secs(settings.request_timeout_seconds),
            max_retries: settings.max_retries,
            retry_backoff: Duration::from_millis(settings.retry_backoff_ms),
            response_schema: settings.response_schema.clone(),
        })
    }
}

impl AgentProvider for PluginProvider {
    fn complete(&self, request: &ProviderRequest) -> Result<AgentResponse, ProviderError> {
        let stdout = self.run_with_retries(request)?;
        parse_agent_response_with_schema(&stdout, self.response_schema.as_ref())
    }
}

impl PluginProvider {
    fn run_with_retries(&self, request: &ProviderRequest) -> Result<String, ProviderError> {
        let mut last_error = None;
        for attempt in 0..=self.max_retries {
            match self.run(request) {
                Ok(stdout) => return Ok(stdout),
                Err(error) if attempt < self.max_retries && plugin_error_is_retryable(&error) => {
                    last_error = Some(error.to_string());
                    thread::sleep(provider_backoff_delay(self.retry_backoff, attempt));
                }
                Err(error) => return Err(error),
            }
        }
        Err(ProviderError::Request(
            last_error.unwrap_or_else(|| "provider plugin request failed".into()),
        ))
    }

    fn run(&self, request: &ProviderRequest) -> Result<String, ProviderError> {
        let input = serde_json::to_vec(request).map_err(|error| {
            ProviderError::InvalidRequest(format!("could not encode provider request: {error}"))
        })?;
        let mut command = Command::new(&self.command);
        command
            .args(&self.args)
            .envs(&self.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn().map_err(|error| {
            ProviderError::Request(format!("failed to start provider plugin: {error}"))
        })?;

        let stdout_reader = child
            .stdout
            .take()
            .map(|stdout| thread::spawn(move || read_capped(stdout, MAX_PLUGIN_OUTPUT_BYTES)));
        let stderr_reader = child
            .stderr
            .take()
            .map(|stderr| thread::spawn(move || read_capped(stderr, MAX_PLUGIN_OUTPUT_BYTES)));

        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| ProviderError::Request("provider plugin stdin unavailable".into()))?;
        stdin.write_all(&input).map_err(|error| {
            ProviderError::Request(format!("failed to write provider plugin request: {error}"))
        })?;
        stdin.write_all(b"\n").map_err(|error| {
            ProviderError::Request(format!("failed to write provider plugin newline: {error}"))
        })?;
        drop(stdin);

        let started = Instant::now();
        let mut timed_out = false;
        loop {
            if child
                .try_wait()
                .map_err(|error| {
                    ProviderError::Request(format!("failed to wait for provider plugin: {error}"))
                })?
                .is_some()
            {
                break;
            }
            if started.elapsed() >= self.request_timeout {
                timed_out = true;
                let _ = terminate_child_process_tree(&mut child, cfg!(unix));
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }

        let status = child.wait().map_err(|error| {
            ProviderError::Request(format!("failed to collect provider plugin status: {error}"))
        })?;
        let stdout = join_plugin_reader(stdout_reader)?;
        let stderr = join_plugin_reader(stderr_reader)?;
        let stderr_text = String::from_utf8_lossy(&stderr).trim().to_owned();
        if timed_out {
            return Err(ProviderError::Request(format!(
                "provider plugin timed out after {} seconds",
                self.request_timeout.as_secs()
            )));
        }
        if !status.success() {
            return Err(ProviderError::Request(format!(
                "provider plugin exited with status {}{}",
                status,
                if stderr_text.is_empty() {
                    String::new()
                } else {
                    format!(": {stderr_text}")
                }
            )));
        }
        String::from_utf8(stdout).map_err(|error| {
            ProviderError::InvalidResponse(format!("provider plugin stdout was not UTF-8: {error}"))
        })
    }
}

fn plugin_error_is_retryable(error: &ProviderError) -> bool {
    matches!(error, ProviderError::Request(_))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderAdapter {
    OpenAiChat,
    AnthropicMessages,
    GeminiGenerateContent,
    OllamaChat,
}

impl ProviderAdapter {
    fn from_settings(settings: &ProviderSettings) -> Result<Self, ProviderError> {
        if let Some(adapter) = settings.adapter.as_deref() {
            return Self::parse(adapter).ok_or_else(|| {
                ProviderError::NotConfigured(format!("unsupported provider.adapter `{adapter}`"))
            });
        }
        Ok(match settings.kind {
            ProviderKind::Anthropic => Self::AnthropicMessages,
            ProviderKind::Gemini => Self::GeminiGenerateContent,
            ProviderKind::Ollama => Self::OllamaChat,
            ProviderKind::Mock
            | ProviderKind::OpenAi
            | ProviderKind::OpenAiCompatible
            | ProviderKind::Custom
            | ProviderKind::Plugin
            | ProviderKind::Local => Self::OpenAiChat,
        })
    }

    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "openai" | "openai-chat" | "openai-compatible" | "chat-completions" => {
                Some(Self::OpenAiChat)
            }
            "anthropic" | "anthropic-messages" | "claude" => Some(Self::AnthropicMessages),
            "gemini" | "gemini-generate-content" | "google-gemini" => {
                Some(Self::GeminiGenerateContent)
            }
            "ollama" | "ollama-chat" => Some(Self::OllamaChat),
            _ => None,
        }
    }

    fn request_body(&self, model: &str, prompt: &str) -> serde_json::Value {
        match self {
            Self::OpenAiChat => serde_json::json!({
                "model": model,
                "messages": [
                    {
                        "role": "system",
                        "content": "You are an AI agent inside Agent OS. Be concise, operational, deterministic, and return valid JSON when asked."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "temperature": 0.2
            }),
            Self::AnthropicMessages => serde_json::json!({
                "model": model,
                "max_tokens": 1024,
                "system": "You are an AI agent inside Agent OS. Be concise, operational, deterministic, and return valid JSON when asked.",
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }),
            Self::GeminiGenerateContent => serde_json::json!({
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            {
                                "text": format!(
                                    "You are an AI agent inside Agent OS. Be concise, operational, deterministic, and return valid JSON when asked.\n\n{prompt}"
                                )
                            }
                        ]
                    }
                ],
                "generationConfig": {
                    "responseMimeType": "application/json"
                }
            }),
            Self::OllamaChat => serde_json::json!({
                "model": model,
                "stream": false,
                "format": "json",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are an AI agent inside Agent OS. Be concise, operational, deterministic, and return valid JSON when asked."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }),
        }
    }

    fn response_content<'a>(&self, value: &'a serde_json::Value) -> Result<&'a str, ProviderError> {
        match self {
            Self::OpenAiChat => value
                .pointer("/choices/0/message/content")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    ProviderError::InvalidResponse("missing choices[0].message.content".into())
                }),
            Self::AnthropicMessages => value
                .pointer("/content/0/text")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| ProviderError::InvalidResponse("missing content[0].text".into())),
            Self::GeminiGenerateContent => value
                .pointer("/candidates/0/content/parts/0/text")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    ProviderError::InvalidResponse(
                        "missing candidates[0].content.parts[0].text".into(),
                    )
                }),
            Self::OllamaChat => value
                .pointer("/message/content")
                .or_else(|| value.get("response"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    ProviderError::InvalidResponse("missing message.content or response".into())
                }),
        }
    }

    fn auth_headers(&self, api_key: Option<&str>) -> Vec<(String, String)> {
        let Some(api_key) = api_key else {
            return Vec::new();
        };
        match self {
            Self::OpenAiChat | Self::OllamaChat => Vec::new(),
            Self::AnthropicMessages => vec![
                ("x-api-key".into(), api_key.into()),
                ("anthropic-version".into(), "2023-06-01".into()),
            ],
            Self::GeminiGenerateContent => vec![("x-goog-api-key".into(), api_key.into())],
        }
    }

    fn uses_bearer_authorization(&self) -> bool {
        matches!(self, Self::OpenAiChat)
    }

    fn reserved_request_field(&self, key: &str) -> bool {
        match self {
            Self::OpenAiChat => matches!(key, "model" | "messages"),
            Self::AnthropicMessages => matches!(key, "model" | "messages" | "system"),
            Self::GeminiGenerateContent => matches!(key, "contents"),
            Self::OllamaChat => matches!(key, "model" | "messages"),
        }
    }
}

impl OpenAiCompatibleProvider {
    pub fn from_settings(settings: &ProviderSettings) -> Result<Self, ProviderError> {
        let adapter = ProviderAdapter::from_settings(settings)?;
        let endpoint = settings
            .endpoint
            .clone()
            .ok_or_else(|| ProviderError::NotConfigured("provider.endpoint is required".into()))?;
        if !is_valid_provider_endpoint(&endpoint) {
            return Err(ProviderError::NotConfigured(
                "provider.endpoint must be an absolute http(s) URL".into(),
            ));
        }
        if !is_valid_env_var_name(&settings.api_key_env) {
            return Err(ProviderError::NotConfigured(
                "provider.api_key_env must be a valid environment variable name".into(),
            ));
        }
        validate_provider_runtime_settings(settings)?;
        let api_key = std::env::var(&settings.api_key_env).ok();
        Ok(Self {
            endpoint,
            model: settings.model.clone(),
            api_key,
            adapter,
            request_timeout: Duration::from_secs(settings.request_timeout_seconds),
            max_retries: settings.max_retries,
            retry_backoff: Duration::from_millis(settings.retry_backoff_ms),
            request_options: settings.request_options.clone(),
            response_schema: settings.response_schema.clone(),
        })
    }
}

impl AgentProvider for OpenAiCompatibleProvider {
    fn complete(&self, request: &ProviderRequest) -> Result<AgentResponse, ProviderError> {
        let body = self.request_body(request);
        let content = self.send_content_with_retries(body)?;

        parse_agent_response_with_schema(&content, self.response_schema.as_ref())
    }
}

impl OpenAiCompatibleProvider {
    fn request_body(&self, request: &ProviderRequest) -> serde_json::Value {
        let prompt = self.prompt(request);
        let mut body = self
            .adapter
            .request_body(request.model.as_deref().unwrap_or(&self.model), &prompt);
        if let Some(body) = body.as_object_mut() {
            for (key, value) in &self.request_options {
                if self.adapter.reserved_request_field(key) {
                    continue;
                }
                body.insert(key.clone(), value.clone());
            }
        }
        body
    }

    fn prompt(&self, request: &ProviderRequest) -> String {
        format!(
            "You are agent `{}` ({}) working on `{}`.\nObjective: {}\nCapabilities: {}\nExisting plan:\n{}\nAvailable tools:\n{}\nRecent memory:\n{}\nReturn strict JSON with keys: summary string, plan array of strings, confidence integer 0-100, and optional tool_calls array. Each tool call is an object with tool string and args object.",
            request.agent_name,
            request.agent_kind,
            request.task_title,
            request.objective,
            request.capabilities.join(", "),
            request.existing_plan.join("\n"),
            format_tools(&request.available_tools),
            format_memory(&request.memory)
        )
    }

    fn send_content_with_retries(&self, body: serde_json::Value) -> Result<String, ProviderError> {
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(self.request_timeout)
            .timeout_read(self.request_timeout)
            .timeout_write(self.request_timeout)
            .build();
        let mut last_error = None;
        for attempt in 0..=self.max_retries {
            let mut request_builder = agent
                .post(&self.endpoint)
                .set("content-type", "application/json");
            for (key, value) in self.adapter.auth_headers(self.api_key.as_deref()) {
                request_builder = request_builder.set(&key, &value);
            }
            if let Some(api_key) = &self.api_key {
                if self.adapter.uses_bearer_authorization() {
                    request_builder =
                        request_builder.set("authorization", &format!("Bearer {api_key}"));
                }
            }
            match request_builder.send_json(body.clone()) {
                Ok(response) => {
                    return self.response_content_from_response(response, &body);
                }
                Err(error) if should_retry_provider_error(&error) && attempt < self.max_retries => {
                    last_error = Some(error.to_string());
                    thread::sleep(provider_retry_delay(&error, self.retry_backoff, attempt));
                }
                Err(error) => return Err(ProviderError::Request(error.to_string())),
            }
        }
        Err(ProviderError::Request(
            last_error.unwrap_or_else(|| "provider request failed".into()),
        ))
    }

    fn response_content_from_response(
        &self,
        response: ureq::Response,
        body: &serde_json::Value,
    ) -> Result<String, ProviderError> {
        if self.adapter == ProviderAdapter::OpenAiChat && request_streams(body) {
            let stream_body = read_provider_http_body(response)?;
            return openai_streaming_content(&stream_body);
        }

        let response_body = read_provider_http_body(response)?;
        let value: serde_json::Value = serde_json::from_str(&response_body)
            .map_err(|error| ProviderError::InvalidResponse(error.to_string()))?;
        self.adapter.response_content(&value).map(str::to_owned)
    }
}

fn read_provider_http_body(response: ureq::Response) -> Result<String, ProviderError> {
    if let Some(length) = response
        .header("content-length")
        .and_then(|value| value.trim().parse::<usize>().ok())
    {
        if length > MAX_PROVIDER_HTTP_BODY_BYTES {
            return Err(provider_body_too_large_error());
        }
    }

    let mut reader = response
        .into_reader()
        .take((MAX_PROVIDER_HTTP_BODY_BYTES + 1) as u64);
    let mut body = Vec::new();
    reader
        .read_to_end(&mut body)
        .map_err(|error| ProviderError::InvalidResponse(error.to_string()))?;
    if body.len() > MAX_PROVIDER_HTTP_BODY_BYTES {
        return Err(provider_body_too_large_error());
    }
    String::from_utf8(body).map_err(|error| {
        ProviderError::InvalidResponse(format!("provider response body was not utf-8: {error}"))
    })
}

fn provider_body_too_large_error() -> ProviderError {
    ProviderError::InvalidResponse(format!(
        "provider response body exceeded {MAX_PROVIDER_HTTP_BODY_BYTES} bytes"
    ))
}

fn request_streams(body: &serde_json::Value) -> bool {
    body.get("stream")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn openai_streaming_content(body: &str) -> Result<String, ProviderError> {
    let mut content = String::new();
    for line in body.lines().map(str::trim) {
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data == "[DONE]" {
            break;
        }
        let value: serde_json::Value = serde_json::from_str(data).map_err(|error| {
            ProviderError::InvalidResponse(format!("invalid streaming response chunk: {error}"))
        })?;
        if let Some(delta) = value
            .pointer("/choices/0/delta/content")
            .and_then(serde_json::Value::as_str)
        {
            content.push_str(delta);
        } else if let Some(message) = value
            .pointer("/choices/0/message/content")
            .and_then(serde_json::Value::as_str)
        {
            content.push_str(message);
        }
    }

    if content.trim().is_empty() {
        return Err(ProviderError::InvalidResponse(
            "streaming response contained no content".into(),
        ));
    }
    Ok(content)
}

fn should_retry_provider_error(error: &ureq::Error) -> bool {
    match error {
        ureq::Error::Status(status, _) => matches!(*status, 408 | 409 | 425 | 429 | 500..=599),
        ureq::Error::Transport(_) => true,
    }
}

fn validate_provider_runtime_settings(settings: &ProviderSettings) -> Result<(), ProviderError> {
    if settings.request_timeout_seconds == 0 {
        return Err(ProviderError::NotConfigured(
            "provider.request_timeout_seconds must be greater than 0".into(),
        ));
    }
    if settings.max_retries > MAX_PROVIDER_RETRIES {
        return Err(ProviderError::NotConfigured(format!(
            "provider.max_retries must be less than or equal to {MAX_PROVIDER_RETRIES}"
        )));
    }
    if settings.retry_backoff_ms == 0 {
        return Err(ProviderError::NotConfigured(
            "provider.retry_backoff_ms must be greater than 0".into(),
        ));
    }
    Ok(())
}

fn provider_retry_delay(error: &ureq::Error, base: Duration, attempt: u32) -> Duration {
    if let ureq::Error::Status(_, response) = error {
        if let Some(retry_after) = response.header("retry-after") {
            if let Some(delay) = parse_retry_after_delay(retry_after) {
                return delay;
            }
        }
    }
    provider_backoff_delay(base, attempt)
}

fn provider_backoff_delay(base: Duration, attempt: u32) -> Duration {
    let base = if base.is_zero() {
        Duration::from_millis(1)
    } else {
        base
    };
    let factor = match attempt {
        0 => 1,
        1 => 2,
        2 => 4,
        3 => 8,
        _ => MAX_PROVIDER_RETRY_BACKOFF_FACTOR,
    };
    base * factor
}

fn parse_retry_after_delay(value: &str) -> Option<Duration> {
    parse_retry_after_delay_at(value, chrono::Utc::now())
}

fn parse_retry_after_delay_at(value: &str, now: chrono::DateTime<chrono::Utc>) -> Option<Duration> {
    let value = value.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(
            seconds.min(MAX_PROVIDER_RETRY_AFTER_SECONDS),
        ));
    }
    let retry_at = chrono::DateTime::parse_from_rfc2822(value)
        .ok()?
        .with_timezone(&chrono::Utc);
    let seconds = retry_at.signed_duration_since(now).num_seconds().max(0) as u64;
    Some(Duration::from_secs(
        seconds.min(MAX_PROVIDER_RETRY_AFTER_SECONDS),
    ))
}

fn read_capped<R>(mut reader: R, max_output_bytes: usize) -> std::io::Result<Vec<u8>>
where
    R: Read,
{
    let mut output = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        if output.len() < max_output_bytes {
            let remaining = max_output_bytes.saturating_sub(output.len());
            output.extend_from_slice(&buffer[..read.min(remaining)]);
        }
    }
    Ok(output)
}

fn join_plugin_reader(
    handle: Option<thread::JoinHandle<std::io::Result<Vec<u8>>>>,
) -> Result<Vec<u8>, ProviderError> {
    match handle {
        Some(handle) => match handle.join() {
            Ok(result) => result
                .map_err(|error| ProviderError::Request(format!("provider plugin io: {error}"))),
            Err(_) => Err(ProviderError::Request(
                "provider plugin output reader panicked".into(),
            )),
        },
        None => Ok(Vec::new()),
    }
}

fn format_tools(tools: &[ProviderTool]) -> String {
    if tools.is_empty() {
        return "- none".into();
    }
    tools
        .iter()
        .map(|tool| {
            format!(
                "- {} [{}]: {}",
                tool.name,
                tool.kind,
                if tool.description.is_empty() {
                    "no description"
                } else {
                    &tool.description
                }
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_memory(memory: &[ProviderMemory]) -> String {
    if memory.is_empty() {
        return "- none".into();
    }
    memory
        .iter()
        .map(|memory| format!("- {}: {}", memory.topic, memory.body))
        .collect::<Vec<_>>()
        .join("\n")
}

fn parse_agent_response_with_schema(
    content: &str,
    schema: Option<&serde_json::Value>,
) -> Result<AgentResponse, ProviderError> {
    let trimmed = content.trim();
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
        if let Some(schema) = schema {
            validate_response_schema(&value, schema)?;
        }
        let summary = value
            .get("summary")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ProviderError::InvalidResponse("missing summary".into()))?
            .trim()
            .to_owned();
        if summary.is_empty() {
            return Err(ProviderError::InvalidResponse("summary is empty".into()));
        }
        let plan = parse_plan(&value)?;
        let confidence = parse_confidence(&value)?;
        return Ok(AgentResponse {
            summary,
            plan,
            confidence,
            tool_calls: parse_tool_calls(&value)?,
        });
    }

    if trimmed.is_empty() {
        return Err(ProviderError::InvalidResponse(
            "response body is empty".into(),
        ));
    }

    if schema.is_some() {
        return Err(ProviderError::InvalidResponse(
            "response_schema requires a JSON response body".into(),
        ));
    }

    Ok(AgentResponse {
        summary: trimmed.to_owned(),
        plan: trimmed
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .take(5)
            .map(ToOwned::to_owned)
            .collect(),
        confidence: 80,
        tool_calls: Vec::new(),
    })
}

#[cfg(test)]
fn parse_agent_response(content: &str) -> Result<AgentResponse, ProviderError> {
    parse_agent_response_with_schema(content, None)
}

fn validate_response_schema(
    value: &serde_json::Value,
    schema: &serde_json::Value,
) -> Result<(), ProviderError> {
    validate_json_schema_value(value, schema, "response")
}

fn validate_json_schema_value(
    value: &serde_json::Value,
    schema: &serde_json::Value,
    path: &str,
) -> Result<(), ProviderError> {
    let Some(schema) = schema.as_object() else {
        return Err(ProviderError::InvalidResponse(
            "response_schema must be an object".into(),
        ));
    };
    if let Some(types) = schema.get("type") {
        validate_json_schema_type(value, types, path)?;
    }
    if let Some(enum_values) = schema.get("enum") {
        let Some(enum_values) = enum_values.as_array() else {
            return Err(ProviderError::InvalidResponse(
                "response_schema.enum must be an array".into(),
            ));
        };
        if !enum_values.iter().any(|expected| expected == value) {
            return Err(ProviderError::InvalidResponse(format!(
                "{path} does not match any response_schema.enum value"
            )));
        }
    }
    if let Some(required) = schema.get("required") {
        let Some(required) = required.as_array() else {
            return Err(ProviderError::InvalidResponse(
                "response_schema.required must be an array".into(),
            ));
        };
        for key in required {
            let Some(key) = key.as_str() else {
                return Err(ProviderError::InvalidResponse(
                    "response_schema.required entries must be strings".into(),
                ));
            };
            if value.get(key).is_none() {
                return Err(ProviderError::InvalidResponse(format!(
                    "{path} missing schema-required key `{key}`"
                )));
            }
        }
    }
    if let Some(properties) = schema.get("properties") {
        let Some(properties) = properties.as_object() else {
            return Err(ProviderError::InvalidResponse(
                "response_schema.properties must be an object".into(),
            ));
        };
        let Some(object) = value.as_object() else {
            return Err(ProviderError::InvalidResponse(format!(
                "{path} must be an object"
            )));
        };
        for (key, property_schema) in properties {
            if let Some(property_value) = object.get(key) {
                validate_json_schema_value(
                    property_value,
                    property_schema,
                    &format!("{path}.{key}"),
                )?;
            }
        }
        if schema
            .get("additionalProperties")
            .and_then(serde_json::Value::as_bool)
            == Some(false)
        {
            for key in object.keys() {
                if !properties.contains_key(key) {
                    return Err(ProviderError::InvalidResponse(format!(
                        "{path}.{key} is not allowed by response_schema.additionalProperties=false"
                    )));
                }
            }
        }
    }
    if let Some(items) = schema.get("items") {
        let Some(array) = value.as_array() else {
            return Err(ProviderError::InvalidResponse(format!(
                "{path} must be an array"
            )));
        };
        for (index, item) in array.iter().enumerate() {
            validate_json_schema_value(item, items, &format!("{path}[{index}]"))?;
        }
    }
    Ok(())
}

fn validate_json_schema_type(
    value: &serde_json::Value,
    types: &serde_json::Value,
    path: &str,
) -> Result<(), ProviderError> {
    let allowed = if let Some(kind) = types.as_str() {
        vec![kind]
    } else if let Some(kinds) = types.as_array() {
        let mut parsed = Vec::new();
        for kind in kinds {
            let Some(kind) = kind.as_str() else {
                return Err(ProviderError::InvalidResponse(
                    "response_schema.type array entries must be strings".into(),
                ));
            };
            parsed.push(kind);
        }
        parsed
    } else {
        return Err(ProviderError::InvalidResponse(
            "response_schema.type must be a string or array".into(),
        ));
    };
    if allowed
        .iter()
        .any(|kind| json_value_matches_type(value, kind))
    {
        return Ok(());
    }
    Err(ProviderError::InvalidResponse(format!(
        "{path} must match response_schema.type {}",
        allowed.join("|")
    )))
}

fn json_value_matches_type(value: &serde_json::Value, kind: &str) -> bool {
    match kind {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "number" => value.is_number(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => false,
    }
}

fn parse_plan(value: &serde_json::Value) -> Result<Vec<String>, ProviderError> {
    let Some(plan) = value.get("plan") else {
        return Ok(Vec::new());
    };
    let steps = plan
        .as_array()
        .ok_or_else(|| ProviderError::InvalidResponse("plan must be an array".into()))?;
    steps
        .iter()
        .enumerate()
        .map(|(index, step)| {
            let step = step.as_str().ok_or_else(|| {
                ProviderError::InvalidResponse(format!("plan step {index} must be a string"))
            })?;
            let step = step.trim();
            if step.is_empty() {
                return Err(ProviderError::InvalidResponse(format!(
                    "plan step {index} is empty"
                )));
            }
            Ok(step.to_owned())
        })
        .collect()
}

fn parse_confidence(value: &serde_json::Value) -> Result<u8, ProviderError> {
    let Some(confidence) = value.get("confidence") else {
        return Ok(80);
    };
    let confidence = confidence
        .as_u64()
        .ok_or_else(|| ProviderError::InvalidResponse("confidence must be an integer".into()))?;
    if confidence > 100 {
        return Err(ProviderError::InvalidResponse(
            "confidence must be between 0 and 100".into(),
        ));
    }
    Ok(confidence as u8)
}

fn parse_tool_calls(value: &serde_json::Value) -> Result<Vec<ProviderToolCall>, ProviderError> {
    let Some(tool_calls) = value.get("tool_calls") else {
        return Ok(Vec::new());
    };
    let calls = tool_calls
        .as_array()
        .ok_or_else(|| ProviderError::InvalidResponse("tool_calls must be an array".into()))?;
    calls
        .iter()
        .enumerate()
        .map(|(index, call)| {
            let call = call.as_object().ok_or_else(|| {
                ProviderError::InvalidResponse(format!("tool call {index} must be an object"))
            })?;
            let tool = call
                .get("tool")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    ProviderError::InvalidResponse(format!(
                        "tool call {index} must include a tool string"
                    ))
                })?
                .trim();
            if tool.is_empty() {
                return Err(ProviderError::InvalidResponse(format!(
                    "tool call {index} tool is empty"
                )));
            }
            let args = match call.get("args") {
                Some(args) => {
                    let args = args.as_object().ok_or_else(|| {
                        ProviderError::InvalidResponse(format!(
                            "tool call {index} args must be an object"
                        ))
                    })?;
                    args.iter()
                        .map(|(key, value)| {
                            let value = value.as_str().ok_or_else(|| {
                                ProviderError::InvalidResponse(format!(
                                    "tool call {index} arg {key} must be a string"
                                ))
                            })?;
                            Ok((key.clone(), value.to_owned()))
                        })
                        .collect::<Result<BTreeMap<_, _>, ProviderError>>()?
                }
                None => BTreeMap::new(),
            };
            Ok(ProviderToolCall {
                tool: tool.to_owned(),
                args,
            })
        })
        .collect()
}

#[derive(Clone, Debug)]
pub enum ProviderRuntime {
    Mock(MockProvider),
    OpenAiCompatible(OpenAiCompatibleProvider),
    Plugin(PluginProvider),
}

impl ProviderRuntime {
    pub fn from_settings(settings: &ProviderSettings) -> Result<Self, ProviderError> {
        match settings.kind {
            ProviderKind::Mock => Ok(Self::Mock(MockProvider)),
            ProviderKind::Plugin => Ok(Self::Plugin(PluginProvider::from_settings(settings)?)),
            ProviderKind::OpenAi
            | ProviderKind::Anthropic
            | ProviderKind::Gemini
            | ProviderKind::Ollama
            | ProviderKind::Local
            | ProviderKind::OpenAiCompatible
            | ProviderKind::Custom => Ok(Self::OpenAiCompatible(
                OpenAiCompatibleProvider::from_settings(settings)?,
            )),
        }
    }
}

impl AgentProvider for ProviderRuntime {
    fn complete(&self, request: &ProviderRequest) -> Result<AgentResponse, ProviderError> {
        match self {
            Self::Mock(provider) => provider.complete(request),
            Self::OpenAiCompatible(provider) => provider.complete(request),
            Self::Plugin(provider) => provider.complete(request),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Agent, AgentKind, MemoryRecord, Priority, Task, ToolDefinition, ToolKind};
    use chrono::Duration as ChronoDuration;
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpListener};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn mock_provider_returns_deterministic_response() {
        let agent = Agent::new("planner", AgentKind::Planner, None, vec!["plan".into()], 1);
        let task = Task::new(
            "Design runtime",
            "Create the runtime plan",
            Priority::High,
            vec!["plan".into()],
        );
        let response = MockProvider
            .complete(&ProviderRequest::new(Some(&agent), &task))
            .expect("response");

        assert!(response.summary.contains("Design runtime"));
        assert_eq!(response.confidence, 87);
        assert!(!response.plan.is_empty());
    }

    #[test]
    fn provider_request_includes_matching_tools_and_recent_memory() {
        let agent = Agent::new("planner", AgentKind::Planner, None, vec!["plan".into()], 1);
        let task = Task::new(
            "Design runtime",
            "Create the runtime plan",
            Priority::High,
            vec!["plan".into()],
        );
        let matching_tool = ToolDefinition::new(
            "plan-tool",
            ToolKind::Shell,
            "plan things",
            vec!["plan".into()],
            "printf plan",
            None,
        );
        let hidden_tool = ToolDefinition::new(
            "build-tool",
            ToolKind::Shell,
            "build things",
            vec!["build".into()],
            "printf build",
            None,
        );
        let mut old_memory = MemoryRecord::new("old", "old memory", vec![]);
        old_memory.updated_at -= ChronoDuration::seconds(1);
        let new_memory = MemoryRecord::new("new", "new memory", vec!["context".into()]);

        let request = ProviderRequest::with_context(
            Some(&agent),
            &task,
            &[matching_tool, hidden_tool],
            &[old_memory, new_memory],
        );

        assert_eq!(request.available_tools.len(), 1);
        assert_eq!(request.available_tools[0].name, "plan-tool");
        assert_eq!(request.memory[0].topic, "new");
        assert_eq!(request.memory[1].topic, "old");
    }

    #[test]
    fn provider_request_respects_memory_limit() {
        let task = Task::new(
            "Design runtime",
            "Create the runtime plan",
            Priority::High,
            vec!["plan".into()],
        );
        let mut old_memory = MemoryRecord::new("old", "old memory", vec![]);
        old_memory.updated_at -= ChronoDuration::seconds(1);
        let new_memory = MemoryRecord::new("new", "new memory", vec![]);

        let request =
            ProviderRequest::with_context_limited(None, &task, &[], &[old_memory, new_memory], 1);

        assert_eq!(request.memory.len(), 1);
        assert_eq!(request.memory[0].topic, "new");
    }

    #[test]
    fn provider_request_can_preserve_ranked_memory_order() {
        let task = Task::new(
            "Design runtime",
            "Create the runtime plan",
            Priority::High,
            vec!["plan".into()],
        );
        let mut recent = MemoryRecord::new("recent", "recent memory", vec![]);
        let mut ranked = MemoryRecord::new("ranked", "ranked memory", vec![]);
        ranked.updated_at -= ChronoDuration::seconds(60);
        recent.updated_at = ranked.updated_at + ChronoDuration::seconds(30);

        let request =
            ProviderRequest::with_ordered_context_limited(None, &task, &[], &[ranked, recent], 2);

        assert_eq!(request.memory[0].topic, "ranked");
        assert_eq!(request.memory[1].topic, "recent");
    }

    #[test]
    fn openai_provider_requires_endpoint() {
        let settings = ProviderSettings {
            kind: ProviderKind::OpenAiCompatible,
            ..ProviderSettings::default()
        };

        assert!(OpenAiCompatibleProvider::from_settings(&settings).is_err());
    }

    #[test]
    fn openai_provider_rejects_invalid_endpoint() {
        let settings = ProviderSettings {
            kind: ProviderKind::OpenAiCompatible,
            endpoint: Some("/v1/chat/completions".into()),
            ..ProviderSettings::default()
        };

        let error = OpenAiCompatibleProvider::from_settings(&settings).expect_err("invalid URL");

        assert!(
            error
                .to_string()
                .contains("provider.endpoint must be an absolute http(s) URL")
        );
    }

    #[test]
    fn openai_provider_rejects_invalid_api_key_env() {
        let settings = ProviderSettings {
            kind: ProviderKind::OpenAiCompatible,
            endpoint: Some("https://api.openai.com/v1/chat/completions".into()),
            api_key_env: " BAD_KEY ".into(),
            ..ProviderSettings::default()
        };

        let error =
            OpenAiCompatibleProvider::from_settings(&settings).expect_err("invalid api_key_env");

        assert!(
            error
                .to_string()
                .contains("provider.api_key_env must be a valid environment variable name")
        );
    }

    #[test]
    fn openai_provider_rejects_unbounded_retries() {
        let settings = ProviderSettings {
            kind: ProviderKind::OpenAiCompatible,
            endpoint: Some("https://api.openai.com/v1/chat/completions".into()),
            max_retries: MAX_PROVIDER_RETRIES + 1,
            ..ProviderSettings::default()
        };

        let error = OpenAiCompatibleProvider::from_settings(&settings).expect_err("retry cap");

        assert!(error.to_string().contains("provider.max_retries"));
    }

    #[test]
    fn openai_provider_rejects_zero_retry_backoff() {
        let settings = ProviderSettings {
            kind: ProviderKind::OpenAiCompatible,
            endpoint: Some("https://api.openai.com/v1/chat/completions".into()),
            retry_backoff_ms: 0,
            ..ProviderSettings::default()
        };

        let error = OpenAiCompatibleProvider::from_settings(&settings).expect_err("retry backoff");

        assert!(error.to_string().contains("provider.retry_backoff_ms"));
    }

    #[test]
    fn provider_runtime_selects_kind_specific_adapters() {
        for (kind, expected) in [
            (ProviderKind::OpenAi, ProviderAdapter::OpenAiChat),
            (ProviderKind::Anthropic, ProviderAdapter::AnthropicMessages),
            (ProviderKind::Gemini, ProviderAdapter::GeminiGenerateContent),
            (ProviderKind::Ollama, ProviderAdapter::OllamaChat),
        ] {
            let settings = ProviderSettings {
                kind,
                endpoint: Some("https://provider.example/v1".into()),
                ..ProviderSettings::default()
            };

            let ProviderRuntime::OpenAiCompatible(provider) =
                ProviderRuntime::from_settings(&settings).expect("provider")
            else {
                panic!("expected http provider");
            };

            assert_eq!(provider.adapter, expected);
        }
    }

    #[test]
    fn custom_provider_adapter_overrides_request_shape() {
        let settings = ProviderSettings {
            kind: ProviderKind::Custom,
            adapter: Some("anthropic".into()),
            endpoint: Some("https://provider.example/v1/messages".into()),
            ..ProviderSettings::default()
        };
        let ProviderRuntime::OpenAiCompatible(provider) =
            ProviderRuntime::from_settings(&settings).expect("provider")
        else {
            panic!("expected http provider");
        };

        assert_eq!(provider.adapter, ProviderAdapter::AnthropicMessages);
    }

    #[test]
    fn plugin_provider_runs_external_command_contract() {
        let settings = ProviderSettings {
            kind: ProviderKind::Plugin,
            plugin_command: Some("sh".into()),
            plugin_args: vec![
                "-c".into(),
                r#"read request
case "$request" in
  *"Plugin task"*) printf '{"summary":"plugin completed task","plan":["from plugin"],"confidence":91}' ;;
  *) printf 'unexpected request' >&2; exit 4 ;;
esac
"#
                .into(),
            ],
            ..ProviderSettings::default()
        };
        let ProviderRuntime::Plugin(provider) =
            ProviderRuntime::from_settings(&settings).expect("plugin provider")
        else {
            panic!("expected plugin provider");
        };
        let task = Task::new(
            "Plugin task",
            "Run through external provider plugin",
            Priority::Normal,
            vec!["provider-plugin".into()],
        );
        let response = provider
            .complete(&ProviderRequest::new(None, &task))
            .expect("plugin response");
        assert_eq!(response.summary, "plugin completed task");
        assert_eq!(response.plan, vec!["from plugin"]);
        assert_eq!(response.confidence, 91);
    }

    #[test]
    fn plugin_provider_retries_transient_failures() {
        let dir = tempfile::tempdir().expect("tempdir");
        let attempts_path = dir.path().join("plugin-attempts");
        let settings = ProviderSettings {
            kind: ProviderKind::Plugin,
            plugin_command: Some("sh".into()),
            plugin_args: vec![
                "-c".into(),
                r#"read request
attempts="$1"
if [ ! -f "$attempts" ]; then
  printf first > "$attempts"
  printf 'transient plugin failure' >&2
  exit 42
fi
printf '{"summary":"plugin retry completed","plan":["retried plugin"],"confidence":89}'"#
                    .into(),
                "agent-os-plugin-retry".into(),
                attempts_path.display().to_string(),
            ],
            max_retries: 1,
            retry_backoff_ms: 1,
            ..ProviderSettings::default()
        };
        let ProviderRuntime::Plugin(provider) =
            ProviderRuntime::from_settings(&settings).expect("plugin provider")
        else {
            panic!("expected plugin provider");
        };
        let task = Task::new(
            "Retry plugin task",
            "Retry through external provider plugin",
            Priority::Normal,
            vec!["provider-plugin".into()],
        );

        let response = provider
            .complete(&ProviderRequest::new(None, &task))
            .expect("plugin retry response");

        assert_eq!(response.summary, "plugin retry completed");
        assert_eq!(response.plan, vec!["retried plugin"]);
        assert_eq!(response.confidence, 89);
        assert_eq!(
            std::fs::read_to_string(&attempts_path).expect("attempt marker"),
            "first"
        );
    }

    #[test]
    fn plugin_provider_rejects_invalid_retry_settings() {
        let settings = ProviderSettings {
            kind: ProviderKind::Plugin,
            plugin_command: Some("agent-os-provider-plugin".into()),
            max_retries: MAX_PROVIDER_RETRIES + 1,
            ..ProviderSettings::default()
        };
        let error = ProviderRuntime::from_settings(&settings).expect_err("retry cap");
        assert!(error.to_string().contains("provider.max_retries"));

        let settings = ProviderSettings {
            kind: ProviderKind::Plugin,
            plugin_command: Some("agent-os-provider-plugin".into()),
            retry_backoff_ms: 0,
            ..ProviderSettings::default()
        };
        let error = ProviderRuntime::from_settings(&settings).expect_err("retry backoff");
        assert!(error.to_string().contains("provider.retry_backoff_ms"));
    }

    #[cfg(unix)]
    #[test]
    fn plugin_provider_timeout_terminates_spawned_process_group() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pid_path = dir.path().join("plugin-child.pid");
        let settings = ProviderSettings {
            kind: ProviderKind::Plugin,
            plugin_command: Some("sh".into()),
            plugin_args: vec![
                "-c".into(),
                "sleep 30 & echo $! > \"$1\"; sleep 30".into(),
                "agent-os-plugin-timeout".into(),
                pid_path.display().to_string(),
            ],
            request_timeout_seconds: 1,
            ..ProviderSettings::default()
        };
        let ProviderRuntime::Plugin(provider) =
            ProviderRuntime::from_settings(&settings).expect("plugin provider")
        else {
            panic!("expected plugin provider");
        };
        let task = Task::new(
            "Timeout plugin task",
            "Exercise provider plugin timeout cleanup",
            Priority::Normal,
            vec![],
        );

        let error = provider
            .complete(&ProviderRequest::new(None, &task))
            .expect_err("plugin should time out");

        assert!(error.to_string().contains("provider plugin timed out"));
        let child_pid = std::fs::read_to_string(&pid_path)
            .expect("child pid file")
            .trim()
            .parse::<u32>()
            .expect("child pid");
        if !wait_for_process_exit(child_pid, Duration::from_secs(2)) {
            let _ = std::process::Command::new("kill")
                .arg("-KILL")
                .arg(child_pid.to_string())
                .status();
            panic!("provider plugin grandchild {child_pid} was not terminated");
        }
    }

    #[test]
    fn plugin_provider_requires_command() {
        let settings = ProviderSettings {
            kind: ProviderKind::Plugin,
            ..ProviderSettings::default()
        };
        let error = ProviderRuntime::from_settings(&settings).expect_err("missing plugin command");
        assert!(
            error
                .to_string()
                .contains("provider.plugin_command is required"),
            "{error}"
        );
    }

    #[cfg(unix)]
    fn wait_for_process_exit(pid: u32, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            let running = std::process::Command::new("kill")
                .arg("-0")
                .arg(pid.to_string())
                .stderr(Stdio::null())
                .status()
                .map(|status| status.success())
                .unwrap_or(false);
            if !running {
                return true;
            }
            thread::sleep(Duration::from_millis(20));
        }
        false
    }

    #[test]
    fn anthropic_adapter_uses_messages_shape_headers_and_response_path() {
        let provider = OpenAiCompatibleProvider {
            endpoint: "https://api.anthropic.com/v1/messages".into(),
            model: "claude-test".into(),
            api_key: Some("anthropic-key".into()),
            adapter: ProviderAdapter::AnthropicMessages,
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
            retry_backoff: Duration::from_millis(1),
            request_options: BTreeMap::new(),
            response_schema: None,
        };
        let task = Task::new(
            "External provider",
            "Call provider",
            Priority::Normal,
            vec!["plan".into()],
        );

        let body = provider.request_body(&ProviderRequest::new(None, &task));
        assert_eq!(body["model"], "claude-test");
        assert_eq!(body["messages"][0]["role"], "user");
        assert!(
            body["system"]
                .as_str()
                .is_some_and(|system| system.contains("Agent OS"))
        );
        let headers = provider.adapter.auth_headers(provider.api_key.as_deref());
        assert!(headers.contains(&("x-api-key".into(), "anthropic-key".into())));
        assert!(headers.iter().any(|(key, _)| key == "anthropic-version"));
        let response = serde_json::json!({
            "content": [
                {
                    "type": "text",
                    "text": r#"{"summary":"ok","plan":["step"],"confidence":88}"#
                }
            ]
        });

        assert_eq!(
            provider
                .adapter
                .response_content(&response)
                .expect("content"),
            r#"{"summary":"ok","plan":["step"],"confidence":88}"#
        );
    }

    #[test]
    fn gemini_and_ollama_adapters_use_native_request_and_response_shapes() {
        let task = Task::new(
            "External provider",
            "Call provider",
            Priority::Normal,
            vec!["plan".into()],
        );
        let request = ProviderRequest::new(None, &task);
        let gemini = ProviderAdapter::GeminiGenerateContent;
        let gemini_body = gemini.request_body("gemini-test", "prompt");
        assert!(
            gemini_body["contents"][0]["parts"][0]["text"]
                .as_str()
                .is_some_and(|text| text.contains("prompt"))
        );
        assert_eq!(
            gemini
                .response_content(&serde_json::json!({
                    "candidates": [
                        {
                            "content": {
                                "parts": [
                                    {
                                        "text": r#"{"summary":"gemini","plan":["step"],"confidence":77}"#
                                    }
                                ]
                            }
                        }
                    ]
                }))
                .expect("gemini content"),
            r#"{"summary":"gemini","plan":["step"],"confidence":77}"#
        );

        let ollama = OpenAiCompatibleProvider {
            endpoint: "http://localhost:11434/api/chat".into(),
            model: "llama-test".into(),
            api_key: None,
            adapter: ProviderAdapter::OllamaChat,
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
            retry_backoff: Duration::from_millis(1),
            request_options: BTreeMap::new(),
            response_schema: None,
        };
        let ollama_body = ollama.request_body(&request);
        assert_eq!(ollama_body["stream"], false);
        assert_eq!(ollama_body["format"], "json");
        assert_eq!(
            ollama
                .adapter
                .response_content(&serde_json::json!({
                    "message": {
                        "content": r#"{"summary":"ollama","plan":["step"],"confidence":81}"#
                    }
                }))
                .expect("ollama content"),
            r#"{"summary":"ollama","plan":["step"],"confidence":81}"#
        );
    }

    #[test]
    fn provider_response_rejects_empty_plain_text() {
        let error = parse_agent_response("   ").expect_err("empty response");

        assert!(error.to_string().contains("response body is empty"));
    }

    #[test]
    fn provider_response_rejects_empty_json_summary() {
        let error = parse_agent_response(r#"{"summary":"   ","plan":["step"],"confidence":90}"#)
            .expect_err("empty summary");

        assert!(error.to_string().contains("summary is empty"));
    }

    #[test]
    fn provider_response_rejects_malformed_plan() {
        let error = parse_agent_response(r#"{"summary":"ok","plan":["step",12],"confidence":90}"#)
            .expect_err("malformed plan");

        assert!(error.to_string().contains("plan step 1 must be a string"));
    }

    #[test]
    fn provider_response_rejects_malformed_confidence() {
        let error = parse_agent_response(r#"{"summary":"ok","plan":["step"],"confidence":101}"#)
            .expect_err("malformed confidence");

        assert!(
            error
                .to_string()
                .contains("confidence must be between 0 and 100")
        );
    }

    #[test]
    fn provider_response_validates_configured_json_schema() {
        let schema = serde_json::json!({
            "type": "object",
            "required": ["summary", "confidence", "metadata"],
            "properties": {
                "summary": { "type": "string" },
                "plan": {
                    "type": "array",
                    "items": { "type": "string" }
                },
                "confidence": { "type": "integer" },
                "metadata": {
                    "type": "object",
                    "required": ["risk"],
                    "properties": {
                        "risk": { "enum": ["low", "medium", "high"] }
                    },
                    "additionalProperties": false
                }
            }
        });

        let response = parse_agent_response_with_schema(
            r#"{"summary":"ok","plan":["step"],"confidence":90,"metadata":{"risk":"low"}}"#,
            Some(&schema),
        )
        .expect("schema-valid response");

        assert_eq!(response.summary, "ok");

        let type_error = parse_agent_response_with_schema(
            r#"{"summary":"ok","plan":["step"],"confidence":"90","metadata":{"risk":"low"}}"#,
            Some(&schema),
        )
        .expect_err("schema type error");
        assert!(
            type_error
                .to_string()
                .contains("response.confidence must match response_schema.type integer")
        );

        let additional_property_error = parse_agent_response_with_schema(
            r#"{"summary":"ok","plan":["step"],"confidence":90,"metadata":{"risk":"low","extra":true}}"#,
            Some(&schema),
        )
        .expect_err("schema additional property error");
        assert!(
            additional_property_error
                .to_string()
                .contains("response.metadata.extra is not allowed")
        );

        let plain_text_error =
            parse_agent_response_with_schema("plain text fallback", Some(&schema))
                .expect_err("schema should require JSON response");
        assert!(
            plain_text_error
                .to_string()
                .contains("response_schema requires a JSON response body")
        );
    }

    #[test]
    fn provider_response_rejects_malformed_tool_calls() {
        let error = parse_agent_response(
            r#"{"summary":"ok","plan":["step"],"confidence":90,"tool_calls":[{"tool":"say","args":{"message":12}}]}"#,
        )
        .expect_err("malformed tool call");

        assert!(
            error
                .to_string()
                .contains("tool call 0 arg message must be a string")
        );
    }

    #[test]
    fn openai_provider_parses_chat_completion_response() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let addr = listener.local_addr().expect("addr");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("timeout");
            read_http_request(&mut stream);
            let body = serde_json::json!({
                "choices": [
                    {
                        "message": {
                            "content": "Summary from provider\n- first\n- second"
                        }
                    }
                ]
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).expect("write");
            stream.flush().expect("flush");
            let _ = stream.shutdown(Shutdown::Write);
        });

        let provider = OpenAiCompatibleProvider {
            endpoint: format!("http://{addr}/v1/chat/completions"),
            model: "test-model".into(),
            api_key: Some("test-key".into()),
            adapter: ProviderAdapter::OpenAiChat,
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
            retry_backoff: Duration::from_millis(1),
            request_options: BTreeMap::new(),
            response_schema: None,
        };
        let task = Task::new(
            "External provider",
            "Call provider",
            Priority::Normal,
            vec!["plan".into()],
        );
        let response = provider
            .complete(&ProviderRequest::new(None, &task))
            .expect("response");

        handle.join().expect("server join");
        assert_eq!(response.confidence, 80);
        assert!(response.summary.contains("Summary from provider"));
        assert!(response.plan.iter().any(|line| line.contains("first")));
    }

    #[test]
    fn openai_provider_parses_structured_json_and_uses_agent_model() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let addr = listener.local_addr().expect("addr");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("timeout");
            let request_body = read_http_request_body(&mut stream);
            let value: serde_json::Value =
                serde_json::from_str(&request_body).expect("request json");
            assert_eq!(value["model"], "agent-specific-model");
            assert!(value["messages"].is_array());
            assert_eq!(value["temperature"], 0.7);
            assert_eq!(value["top_p"], 0.9);
            let content = serde_json::json!({
                "summary": "Structured summary",
                "plan": ["first", "second"],
                "confidence": 93,
                "tool_calls": [
                    {
                        "tool": "plan-tool",
                        "args": {
                            "message": "hello"
                        }
                    }
                ]
            })
            .to_string();
            let body = serde_json::json!({
                "choices": [
                    {
                        "message": {
                            "content": content
                        }
                    }
                ]
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).expect("write");
            stream.flush().expect("flush");
            let _ = stream.shutdown(Shutdown::Write);
        });

        let provider = OpenAiCompatibleProvider {
            endpoint: format!("http://{addr}/v1/chat/completions"),
            model: "global-model".into(),
            api_key: Some("test-key".into()),
            adapter: ProviderAdapter::OpenAiChat,
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
            retry_backoff: Duration::from_millis(1),
            request_options: BTreeMap::from([
                ("temperature".into(), serde_json::json!(0.7)),
                ("top_p".into(), serde_json::json!(0.9)),
                ("model".into(), serde_json::json!("ignored-model")),
                ("messages".into(), serde_json::json!("ignored-messages")),
            ]),
            response_schema: None,
        };
        let agent = Agent::new(
            "planner",
            AgentKind::Planner,
            Some("agent-specific-model".into()),
            vec!["plan".into()],
            1,
        );
        let task = Task::new(
            "External provider",
            "Call provider",
            Priority::Normal,
            vec!["plan".into()],
        );
        let response = provider
            .complete(&ProviderRequest::new(Some(&agent), &task))
            .expect("response");

        handle.join().expect("server join");
        assert_eq!(response.summary, "Structured summary");
        assert_eq!(response.plan, vec!["first", "second"]);
        assert_eq!(response.confidence, 93);
        assert_eq!(response.tool_calls.len(), 1);
        assert_eq!(response.tool_calls[0].tool, "plan-tool");
        assert_eq!(
            response.tool_calls[0].args.get("message"),
            Some(&"hello".to_owned())
        );
    }

    #[test]
    fn openai_provider_parses_streaming_chat_completion_response() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let addr = listener.local_addr().expect("addr");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("timeout");
            let request_body = read_http_request_body(&mut stream);
            let value: serde_json::Value =
                serde_json::from_str(&request_body).expect("request json");
            assert_eq!(value["stream"], true);

            let first_chunk = serde_json::json!({
                "choices": [
                    {
                        "delta": {
                            "content": "{\"summary\":\"Streamed summary\",\"plan\":[\"first\"]"
                        }
                    }
                ]
            })
            .to_string();
            let second_chunk = serde_json::json!({
                "choices": [
                    {
                        "delta": {
                            "content": ",\"confidence\":88,\"tool_calls\":[]}"
                        }
                    }
                ]
            })
            .to_string();
            let body = format!("data: {first_chunk}\n\ndata: {second_chunk}\n\ndata: [DONE]\n\n");
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).expect("write");
            stream.flush().expect("flush");
            let _ = stream.shutdown(Shutdown::Write);
        });

        let provider = OpenAiCompatibleProvider {
            endpoint: format!("http://{addr}/v1/chat/completions"),
            model: "test-model".into(),
            api_key: Some("test-key".into()),
            adapter: ProviderAdapter::OpenAiChat,
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
            retry_backoff: Duration::from_millis(1),
            request_options: BTreeMap::from([("stream".into(), serde_json::json!(true))]),
            response_schema: None,
        };
        let task = Task::new(
            "External provider",
            "Call provider",
            Priority::Normal,
            vec!["plan".into()],
        );
        let response = provider
            .complete(&ProviderRequest::new(None, &task))
            .expect("streamed response");

        handle.join().expect("server join");
        assert_eq!(response.summary, "Streamed summary");
        assert_eq!(response.plan, vec!["first"]);
        assert_eq!(response.confidence, 88);
        assert!(response.tool_calls.is_empty());
    }

    #[test]
    fn openai_provider_rejects_oversized_response_body() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let addr = listener.local_addr().expect("addr");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("timeout");
            read_http_request(&mut stream);
            let response =
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n";
            stream
                .write_all(response.as_bytes())
                .expect("write headers");
            let oversized_body = vec![b'a'; MAX_PROVIDER_HTTP_BODY_BYTES + 1];
            stream
                .write_all(&oversized_body)
                .expect("write oversized body");
            stream.flush().expect("flush");
            let _ = stream.shutdown(Shutdown::Write);
        });

        let provider = OpenAiCompatibleProvider {
            endpoint: format!("http://{addr}/v1/chat/completions"),
            model: "test-model".into(),
            api_key: None,
            adapter: ProviderAdapter::OpenAiChat,
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
            retry_backoff: Duration::from_millis(1),
            request_options: BTreeMap::new(),
            response_schema: None,
        };
        let task = Task::new(
            "External provider",
            "Call provider",
            Priority::Normal,
            vec!["plan".into()],
        );
        let error = provider
            .complete(&ProviderRequest::new(None, &task))
            .expect_err("oversized response should fail");

        handle.join().expect("server join");
        assert!(
            error
                .to_string()
                .contains("provider response body exceeded"),
            "{error}"
        );
    }

    #[test]
    fn openai_provider_retries_transient_server_failures() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let addr = listener.local_addr().expect("addr");
        let handle = thread::spawn(move || {
            let (mut first, _) = listener.accept().expect("first accept");
            read_http_request(&mut first);
            let first_response = "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
            first
                .write_all(first_response.as_bytes())
                .expect("write first");
            first.flush().expect("flush first");

            let (mut second, _) = listener.accept().expect("second accept");
            read_http_request(&mut second);
            let content = serde_json::json!({
                "summary": "Retried summary",
                "plan": ["retry worked"],
                "confidence": 91
            })
            .to_string();
            let body = serde_json::json!({
                "choices": [
                    {
                        "message": {
                            "content": content
                        }
                    }
                ]
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            second.write_all(response.as_bytes()).expect("write second");
            second.flush().expect("flush second");
        });

        let provider = OpenAiCompatibleProvider {
            endpoint: format!("http://{addr}/v1/chat/completions"),
            model: "test-model".into(),
            api_key: None,
            adapter: ProviderAdapter::OpenAiChat,
            request_timeout: Duration::from_secs(5),
            max_retries: 1,
            retry_backoff: Duration::from_millis(1),
            request_options: BTreeMap::new(),
            response_schema: None,
        };
        let task = Task::new(
            "External provider",
            "Call provider",
            Priority::Normal,
            vec!["plan".into()],
        );
        let response = provider
            .complete(&ProviderRequest::new(None, &task))
            .expect("retried response");

        handle.join().expect("server join");
        assert_eq!(response.summary, "Retried summary");
        assert_eq!(response.confidence, 91);
    }

    #[test]
    fn provider_retry_delay_uses_retry_after_and_caps_backoff() {
        let response: ureq::Response =
            "HTTP/1.1 429 Too Many Requests\r\nretry-after: 3\r\ncontent-length: 0\r\n\r\n"
                .parse()
                .expect("response");
        let error = ureq::Error::Status(429, response);

        assert_eq!(
            provider_retry_delay(&error, Duration::from_secs(30), 0),
            Duration::from_secs(3)
        );
        assert_eq!(
            provider_backoff_delay(Duration::from_millis(10), 0),
            Duration::from_millis(10)
        );
        assert_eq!(
            provider_backoff_delay(Duration::from_millis(10), 1),
            Duration::from_millis(20)
        );
        assert_eq!(
            provider_backoff_delay(Duration::from_millis(10), 3),
            Duration::from_millis(80)
        );
        assert_eq!(
            provider_backoff_delay(Duration::from_millis(10), 99),
            Duration::from_millis(160)
        );
        assert_eq!(
            parse_retry_after_delay("999"),
            Some(Duration::from_secs(MAX_PROVIDER_RETRY_AFTER_SECONDS))
        );
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-20T12:00:00Z")
            .expect("now")
            .with_timezone(&chrono::Utc);
        assert_eq!(
            parse_retry_after_delay_at("Wed, 20 May 2026 12:00:03 GMT", now),
            Some(Duration::from_secs(3))
        );
        assert_eq!(
            parse_retry_after_delay_at("Wed, 20 May 2026 12:02:00 GMT", now),
            Some(Duration::from_secs(MAX_PROVIDER_RETRY_AFTER_SECONDS))
        );
        assert_eq!(
            parse_retry_after_delay_at("Wed, 20 May 2026 11:59:00 GMT", now),
            Some(Duration::from_secs(0))
        );
        assert_eq!(parse_retry_after_delay("not-a-number"), None);
    }

    fn read_http_request(stream: &mut std::net::TcpStream) {
        let _ = read_http_request_body(stream);
    }

    fn read_http_request_body(stream: &mut std::net::TcpStream) -> String {
        let mut buffer = Vec::new();
        let mut chunk = [0; 1024];
        loop {
            let read = stream.read(&mut chunk).expect("read");
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);
            if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }

        let request = String::from_utf8_lossy(&buffer);
        let Some(length) = request
            .lines()
            .find_map(|line| line.strip_prefix("content-length: "))
            .or_else(|| {
                request
                    .lines()
                    .find_map(|line| line.strip_prefix("Content-Length: "))
            })
            .and_then(|value| value.trim().parse::<usize>().ok())
        else {
            return String::new();
        };

        let header_end = buffer
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|index| index + 4)
            .unwrap_or(buffer.len());
        let mut body_read = buffer.len().saturating_sub(header_end);
        while body_read < length {
            let read = stream.read(&mut chunk).expect("read body");
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);
            body_read += read;
        }
        String::from_utf8_lossy(&buffer[header_end..header_end + body_read]).to_string()
    }
}
