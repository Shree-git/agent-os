use crate::models::{
    Agent, MemoryRecord, ProviderKind, ProviderSettings, Task, ToolDefinition,
    is_valid_env_var_name, is_valid_provider_endpoint,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;
use thiserror::Error;

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
            memory: memory
                .iter()
                .rev()
                .take(5)
                .map(ProviderMemory::from)
                .collect(),
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
    pub request_timeout: Duration,
}

impl OpenAiCompatibleProvider {
    pub fn from_settings(settings: &ProviderSettings) -> Result<Self, ProviderError> {
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
        let api_key = std::env::var(&settings.api_key_env).ok();
        Ok(Self {
            endpoint,
            model: settings.model.clone(),
            api_key,
            request_timeout: Duration::from_secs(settings.request_timeout_seconds),
        })
    }
}

impl AgentProvider for OpenAiCompatibleProvider {
    fn complete(&self, request: &ProviderRequest) -> Result<AgentResponse, ProviderError> {
        let prompt = format!(
            "You are agent `{}` ({}) working on `{}`.\nObjective: {}\nCapabilities: {}\nExisting plan:\n{}\nAvailable tools:\n{}\nRecent memory:\n{}\nReturn strict JSON with keys: summary string, plan array of strings, confidence integer 0-100, and optional tool_calls array. Each tool call is an object with tool string and args object.",
            request.agent_name,
            request.agent_kind,
            request.task_title,
            request.objective,
            request.capabilities.join(", "),
            request.existing_plan.join("\n"),
            format_tools(&request.available_tools),
            format_memory(&request.memory)
        );
        let body = serde_json::json!({
            "model": request.model.as_deref().unwrap_or(&self.model),
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
        });

        let agent = ureq::AgentBuilder::new()
            .timeout_connect(self.request_timeout)
            .timeout_read(self.request_timeout)
            .timeout_write(self.request_timeout)
            .build();
        let mut request_builder = agent
            .post(&self.endpoint)
            .set("content-type", "application/json");
        if let Some(api_key) = &self.api_key {
            request_builder = request_builder.set("authorization", &format!("Bearer {api_key}"));
        }
        let response = request_builder
            .send_json(body)
            .map_err(|error| ProviderError::Request(error.to_string()))?;
        let value: serde_json::Value = response
            .into_json()
            .map_err(|error| ProviderError::InvalidResponse(error.to_string()))?;
        let content = value
            .pointer("/choices/0/message/content")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ProviderError::InvalidResponse("missing choices[0].message.content".into())
            })?;

        parse_agent_response(content)
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

fn parse_agent_response(content: &str) -> Result<AgentResponse, ProviderError> {
    let trimmed = content.trim();
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
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
}

impl ProviderRuntime {
    pub fn from_settings(settings: &ProviderSettings) -> Result<Self, ProviderError> {
        match settings.kind {
            ProviderKind::Mock => Ok(Self::Mock(MockProvider)),
            ProviderKind::OpenAiCompatible => Ok(Self::OpenAiCompatible(
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Agent, AgentKind, MemoryRecord, Priority, Task, ToolDefinition, ToolKind};
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
        let old_memory = MemoryRecord::new("old", "old memory", vec![]);
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
            request_timeout: Duration::from_secs(5),
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
            request_timeout: Duration::from_secs(5),
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
