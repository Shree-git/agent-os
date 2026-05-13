use crate::config::{
    AppConfig, ConfigError, load_config, validate_seed_config, write_default_config,
};
use crate::executor::CommandExecutor;
use crate::migrations::CURRENT_STATE_VERSION;
use crate::models::{
    Agent, AgentId, AgentKind, AgentStatus, EventKind, MemoryRecord, OperatingSystem, Priority,
    ProviderKind, RunId, RunRecord, RunStatus, Task, TaskId, TaskStatus, ToolDefinition, ToolId,
    ToolInvocation, ToolKind, Workflow, WorkflowId, is_valid_env_var_name, memory_matches_query,
    normalize_list, tail_text_by_bytes, text_tail_was_truncated,
};
use crate::runtime::{AgentUpdate, Runtime, RuntimeError, TaskUpdate, ToolUpdate};
use crate::scheduler::Scheduler;
use crate::service::{
    LaunchdServiceOptions, ServiceError, build_launchd_service as build_launchd_service_definition,
    default_launchd_plist_path, install_launchd_service as install_launchd_service_definition,
    resolve_launchd_domain, run_launchctl, uninstall_launchd_service,
    validate_service_control_inputs,
};
use crate::store::{Store, StoreError};
use crate::tools::{validate_tool_invocation, validate_tool_template};
use crate::validation::{repair_state, validate_state};
use serde::Deserialize;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::thread;
use thiserror::Error;

const MAX_HTTP_HEADER_BYTES: usize = 64 * 1024;
const MAX_HTTP_BODY_BYTES: usize = 1024 * 1024;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("api io error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("invalid bind address: {0}")]
    InvalidAddress(String),
    #[error("api worker panicked")]
    WorkerPanicked,
}

#[derive(Debug, Error)]
enum HttpRequestError {
    #[error("api io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("incomplete http headers")]
    IncompleteHeaders,
    #[error("missing http request line")]
    MissingRequestLine,
    #[error("malformed http request line: {line}")]
    MalformedRequestLine { line: String },
    #[error("unsupported http version: {version}")]
    UnsupportedHttpVersion { version: String },
    #[error("http headers exceeded {limit} bytes")]
    HeaderTooLarge { limit: usize },
    #[error("invalid content-length header: {value}")]
    InvalidContentLength { value: String },
    #[error("conflicting content-length headers: {first} and {second}")]
    ConflictingContentLength { first: usize, second: usize },
    #[error("request body exceeded {limit} bytes")]
    PayloadTooLarge { limit: usize },
    #[error("incomplete request body: expected {expected} bytes, received {received}")]
    IncompleteBody { expected: usize, received: usize },
}

pub struct ApiServer {
    store: Store,
    listener: TcpListener,
    max_requests: Option<usize>,
    bearer_token: Option<String>,
    config_path: Option<PathBuf>,
}

#[derive(Clone)]
struct ApiHandler {
    store: Store,
    bearer_token: Option<String>,
    config_path: Option<PathBuf>,
}

impl ApiServer {
    pub fn bind(
        store: Store,
        addr: &str,
        max_requests: Option<usize>,
        bearer_token: Option<String>,
    ) -> Result<Self, ApiError> {
        let listener = TcpListener::bind(addr).map_err(|error| {
            if addr.parse::<SocketAddr>().is_err() {
                ApiError::InvalidAddress(addr.to_owned())
            } else {
                ApiError::Io(error)
            }
        })?;
        Ok(Self {
            store,
            listener,
            max_requests,
            bearer_token,
            config_path: None,
        })
    }

    pub fn bind_with_config_path(
        store: Store,
        addr: &str,
        max_requests: Option<usize>,
        bearer_token: Option<String>,
        config_path: PathBuf,
    ) -> Result<Self, ApiError> {
        let mut server = Self::bind(store, addr, max_requests, bearer_token)?;
        server.config_path = Some(config_path);
        Ok(server)
    }

    pub fn local_addr(&self) -> Result<SocketAddr, ApiError> {
        Ok(self.listener.local_addr()?)
    }

    pub fn serve(&self) -> Result<(), ApiError> {
        let handler = ApiHandler {
            store: self.store.clone(),
            bearer_token: self.bearer_token.clone(),
            config_path: self.config_path.clone(),
        };
        let mut handles = Vec::new();
        for (index, stream) in self.listener.incoming().enumerate() {
            let stream = stream?;
            let handler = handler.clone();
            let handle = thread::spawn(move || handler.handle_stream(stream));
            if self.max_requests.is_some() {
                handles.push(handle);
            }
            if self
                .max_requests
                .map(|max_requests| index + 1 >= max_requests)
                .unwrap_or(false)
            {
                break;
            }
        }
        for handle in handles {
            handle.join().map_err(|_| ApiError::WorkerPanicked)??;
        }
        Ok(())
    }
}

impl ApiHandler {
    fn handle_stream(&self, mut stream: TcpStream) -> Result<(), ApiError> {
        let request = match read_http_request(&mut stream) {
            Ok(request) => request,
            Err(HttpRequestError::Io(error)) => return Err(ApiError::Io(error)),
            Err(error) => {
                let (status, body) = http_request_error_response(&error);
                write_http_response(&mut stream, status, &body)?;
                return Ok(());
            }
        };
        let (method, path) = match parse_http_request_line(request.request_line.as_deref()) {
            Ok(parts) => parts,
            Err(error) => {
                let (status, body) = http_request_error_response(&error);
                write_http_response(&mut stream, status, &body)?;
                return Ok(());
            }
        };

        let (status, body) = if method == "OPTIONS" {
            ("204 No Content", String::new())
        } else if !self.authorized(&request.headers) {
            (
                "401 Unauthorized",
                json!({
                    "error": "unauthorized",
                })
                .to_string(),
            )
        } else if method == "GET" && is_openapi_path(path) {
            ("200 OK", openapi_schema().to_string())
        } else if method == "GET" && is_doctor_path(path) {
            (
                "200 OK",
                doctor_json(&self.store, self.config_path.as_deref()).to_string(),
            )
        } else if method == "GET" && is_config_path(path) {
            config_response_for_path(self.config_path.as_deref(), path)
        } else if method == "GET" {
            match self.store.load() {
                Ok(os) => response_for_path_with_context(
                    &self.store,
                    self.config_path.as_deref(),
                    &os,
                    path,
                ),
                Err(error) if is_health_path(path) => (
                    "503 Service Unavailable",
                    health_unavailable_json(error.to_string(), self.config_path.as_deref())
                        .to_string(),
                ),
                Err(error) if is_metrics_path(path) => (
                    "503 Service Unavailable",
                    metrics_unavailable_json(error.to_string()).to_string(),
                ),
                Err(error) => (
                    "500 Internal Server Error",
                    json!({
                        "error": "state unavailable",
                        "detail": error.to_string(),
                    })
                    .to_string(),
                ),
            }
        } else if matches!(method, "POST" | "DELETE") {
            match validate_mutation_content_type(method, &request.headers, &request.body) {
                Ok(()) => response_for_mutation_with_context(
                    &self.store,
                    self.config_path.as_deref(),
                    method,
                    path,
                    &request.body,
                ),
                Err(response) => response,
            }
        } else {
            (
                "405 Method Not Allowed",
                json!({
                    "error": "method not allowed",
                    "method": method,
                })
                .to_string(),
            )
        };
        write_http_response(&mut stream, status, &body)?;
        Ok(())
    }

    fn authorized(&self, headers: &str) -> bool {
        let Some(token) = &self.bearer_token else {
            return true;
        };
        headers.lines().skip(1).any(|line| {
            let Some((key, value)) = line.split_once(':') else {
                return false;
            };
            key.eq_ignore_ascii_case("authorization") && value.trim() == format!("Bearer {token}")
        })
    }
}

struct HttpRequest {
    request_line: Option<String>,
    headers: String,
    body: Vec<u8>,
}

fn read_http_request(stream: &mut impl Read) -> Result<HttpRequest, HttpRequestError> {
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            break find_header_end(&bytes).ok_or(HttpRequestError::IncompleteHeaders)?;
        }
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(header_end) = find_header_end(&bytes) {
            if header_end + 4 > MAX_HTTP_HEADER_BYTES {
                return Err(HttpRequestError::HeaderTooLarge {
                    limit: MAX_HTTP_HEADER_BYTES,
                });
            }
            break header_end;
        }
        if bytes.len() > MAX_HTTP_HEADER_BYTES {
            return Err(HttpRequestError::HeaderTooLarge {
                limit: MAX_HTTP_HEADER_BYTES,
            });
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]).to_string();
    let content_length = parse_content_length(&headers)?;
    if content_length > MAX_HTTP_BODY_BYTES {
        return Err(HttpRequestError::PayloadTooLarge {
            limit: MAX_HTTP_BODY_BYTES,
        });
    }
    let body_start = (header_end + 4).min(bytes.len());
    while bytes.len().saturating_sub(body_start) < content_length {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            return Err(HttpRequestError::IncompleteBody {
                expected: content_length,
                received: bytes.len().saturating_sub(body_start),
            });
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
    let body_end = (body_start + content_length).min(bytes.len());
    let request_line = headers.lines().next().map(str::to_owned);
    Ok(HttpRequest {
        request_line,
        headers,
        body: bytes[body_start..body_end].to_vec(),
    })
}

fn parse_content_length(headers: &str) -> Result<usize, HttpRequestError> {
    let mut parsed = None;
    for line in headers.lines().skip(1) {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if key.eq_ignore_ascii_case("content-length") {
            let value = value.trim();
            let content_length =
                value
                    .parse::<usize>()
                    .map_err(|_| HttpRequestError::InvalidContentLength {
                        value: value.to_owned(),
                    })?;
            if let Some(first) = parsed {
                if first != content_length {
                    return Err(HttpRequestError::ConflictingContentLength {
                        first,
                        second: content_length,
                    });
                }
            } else {
                parsed = Some(content_length);
            }
        }
    }
    Ok(parsed.unwrap_or(0))
}

fn validate_mutation_content_type(
    method: &str,
    headers: &str,
    body: &[u8],
) -> Result<(), (&'static str, String)> {
    if body.is_empty() {
        return Ok(());
    }
    let Some(content_type) = header_value(headers, "content-type") else {
        return Err(unsupported_media_type_response(
            method,
            "missing content-type",
        ));
    };
    let media_type = content_type
        .split_once(';')
        .map(|(media_type, _)| media_type)
        .unwrap_or(content_type)
        .trim();
    if media_type.eq_ignore_ascii_case("application/json") {
        return Ok(());
    }
    Err(unsupported_media_type_response(method, content_type.trim()))
}

fn header_value<'a>(headers: &'a str, name: &str) -> Option<&'a str> {
    headers.lines().skip(1).find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name).then_some(value.trim())
    })
}

fn unsupported_media_type_response(method: &str, content_type: &str) -> (&'static str, String) {
    (
        "415 Unsupported Media Type",
        json!({
            "error": "unsupported media type",
            "method": method,
            "detail": format!("mutation request bodies must use application/json; got {content_type}"),
        })
        .to_string(),
    )
}

fn parse_http_request_line(line: Option<&str>) -> Result<(&str, &str), HttpRequestError> {
    let line = line.ok_or(HttpRequestError::MissingRequestLine)?;
    let parts = line.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(HttpRequestError::MalformedRequestLine {
            line: line.to_owned(),
        });
    }
    let method = parts[0];
    let path = parts[1];
    let version = parts[2];
    if !path.starts_with('/') {
        return Err(HttpRequestError::MalformedRequestLine {
            line: line.to_owned(),
        });
    }
    if !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        return Err(HttpRequestError::UnsupportedHttpVersion {
            version: version.to_owned(),
        });
    }
    Ok((method, path))
}

fn is_health_path(path: &str) -> bool {
    let (path, _) = path.split_once('?').unwrap_or((path, ""));
    matches!(path.trim_end_matches('/'), "" | "/health")
}

fn is_doctor_path(path: &str) -> bool {
    let (path, _) = path.split_once('?').unwrap_or((path, ""));
    path.trim_end_matches('/') == "/doctor"
}

fn is_openapi_path(path: &str) -> bool {
    let (path, _) = path.split_once('?').unwrap_or((path, ""));
    path.trim_end_matches('/') == "/openapi.json"
}

fn is_config_path(path: &str) -> bool {
    let (path, _) = path.split_once('?').unwrap_or((path, ""));
    matches!(path.trim_end_matches('/'), "/config" | "/config/validate")
}

fn is_metrics_path(path: &str) -> bool {
    let (path, _) = path.split_once('?').unwrap_or((path, ""));
    path.trim_end_matches('/') == "/metrics"
}

fn write_http_response(stream: &mut TcpStream, status: &str, body: &str) -> Result<(), ApiError> {
    let auth_challenge = if status.starts_with("401 ") {
        "www-authenticate: Bearer\r\n"
    } else {
        ""
    };
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncache-control: no-store\r\nx-content-type-options: nosniff\r\nallow: GET, POST, DELETE, OPTIONS\r\naccess-control-allow-origin: *\r\naccess-control-allow-methods: GET, POST, DELETE, OPTIONS\r\naccess-control-allow-headers: authorization, content-type\r\n{auth_challenge}content-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes())?;
    Ok(())
}

fn http_request_error_response(error: &HttpRequestError) -> (&'static str, String) {
    let status = match error {
        HttpRequestError::HeaderTooLarge { .. } => "431 Request Header Fields Too Large",
        HttpRequestError::PayloadTooLarge { .. } => "413 Payload Too Large",
        HttpRequestError::Io(_) => "500 Internal Server Error",
        HttpRequestError::IncompleteHeaders
        | HttpRequestError::MissingRequestLine
        | HttpRequestError::MalformedRequestLine { .. }
        | HttpRequestError::UnsupportedHttpVersion { .. }
        | HttpRequestError::InvalidContentLength { .. }
        | HttpRequestError::ConflictingContentLength { .. }
        | HttpRequestError::IncompleteBody { .. } => "400 Bad Request",
    };
    (
        status,
        json!({
            "error": "invalid http request",
            "detail": error.to_string(),
        })
        .to_string(),
    )
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

pub fn response_for_path(os: &OperatingSystem, path: &str) -> (&'static str, String) {
    response_for_path_inner(None, None, os, path)
}

fn response_for_path_with_context(
    store: &Store,
    config_path: Option<&Path>,
    os: &OperatingSystem,
    path: &str,
) -> (&'static str, String) {
    response_for_path_inner(Some(store), config_path, os, path)
}

fn response_for_path_inner(
    store: Option<&Store>,
    config_path: Option<&Path>,
    os: &OperatingSystem,
    path: &str,
) -> (&'static str, String) {
    let (path, query) = path.split_once('?').unwrap_or((path, ""));
    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    if let Some(response) = response_for_detail_path(store, os, &segments, query) {
        return response;
    }

    let body = match path.trim_end_matches('/') {
        "" | "/" | "/health" => health_json(os, config_path),
        "/metrics" => metrics_json(os),
        "/openapi.json" => openapi_schema(),
        "/status" => status_json(os),
        "/daemon" => daemon_json(os),
        "/state/export" => json!(os),
        "/state/validate" => json!(validate_state(os)),
        "/agents" => match agents_json(os, query) {
            Ok(agents) => agents,
            Err(response) => return response,
        },
        "/tasks" => match tasks_json(os, query) {
            Ok(tasks) => tasks,
            Err(response) => return response,
        },
        "/workflows" => match workflows_json(os, query) {
            Ok(workflows) => workflows,
            Err(response) => return response,
        },
        "/tools" => match tools_json(os, query) {
            Ok(tools) => tools,
            Err(response) => return response,
        },
        "/runs" => match runs_json(os, query) {
            Ok(runs) => runs,
            Err(response) => return response,
        },
        "/events" => match limited_events_json(os, query) {
            Ok(events) => events,
            Err(response) => return response,
        },
        "/memory" => match memory_json(os, query) {
            Ok(memory) => memory,
            Err(response) => return response,
        },
        _ => {
            return (
                "404 Not Found",
                json!({
                    "error": "not found",
                    "path": path,
                })
                .to_string(),
            );
        }
    };
    ("200 OK", body.to_string())
}

fn limited_events_json(
    os: &OperatingSystem,
    query: &str,
) -> Result<serde_json::Value, (&'static str, String)> {
    reject_unknown_query_keys(query, &["limit", "kind", "since", "until", "query"])?;
    let kind = event_kind_query(query)?;
    let since = timestamp_query(query, "since")?;
    let until = timestamp_query(query, "until")?;
    let search = non_empty_query_string(query, "query")?.map(|query| query.to_ascii_lowercase());
    let mut events = os
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
                && search
                    .as_ref()
                    .map(|query| event.message.to_ascii_lowercase().contains(query))
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    events.reverse();
    if let Some(limit) = positive_query_usize(query, "limit")? {
        events.truncate(limit);
    }
    Ok(json!(events))
}

fn daemon_json(os: &OperatingSystem) -> serde_json::Value {
    json!({
        "daemon": os.daemon,
    })
}

fn agents_json(
    os: &OperatingSystem,
    query: &str,
) -> Result<serde_json::Value, (&'static str, String)> {
    reject_unknown_query_keys(
        query,
        &[
            "status",
            "kind",
            "capability",
            "since",
            "until",
            "query",
            "limit",
        ],
    )?;
    let status = agent_status_query(query)?;
    let kind = agent_kind_query(query)?;
    let capabilities = capability_query(query)?;
    let since = timestamp_query(query, "since")?;
    let until = timestamp_query(query, "until")?;
    let search = non_empty_query_string(query, "query")?.map(|query| query.to_ascii_lowercase());
    let limit = positive_query_usize(query, "limit")?;
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
                && search
                    .as_ref()
                    .map(|query| agent_matches_query(agent, query))
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    agents.sort_by_key(|agent| std::cmp::Reverse(agent.updated_at));
    if let Some(limit) = limit {
        agents.truncate(limit);
    }
    Ok(json!(agents))
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

fn tasks_json(
    os: &OperatingSystem,
    query: &str,
) -> Result<serde_json::Value, (&'static str, String)> {
    reject_unknown_query_keys(
        query,
        &[
            "status",
            "priority",
            "agent",
            "tool",
            "after",
            "capability",
            "since",
            "until",
            "query",
            "limit",
        ],
    )?;
    let status = task_status_query(query)?;
    let priority = priority_query(query)?;
    let agent = agent_id_query(query)?;
    let tool = tool_id_query(query)?;
    let dependency = dependency_query(query)?;
    let capabilities = capability_query(query)?;
    let since = timestamp_query(query, "since")?;
    let until = timestamp_query(query, "until")?;
    let search = non_empty_query_string(query, "query")?.map(|query| query.to_ascii_lowercase());
    let limit = positive_query_usize(query, "limit")?;
    let mut tasks = os
        .tasks
        .values()
        .filter(|task| {
            status
                .as_ref()
                .map(|status| &task.status == status)
                .unwrap_or(true)
                && priority
                    .as_ref()
                    .map(|priority| &task.priority == priority)
                    .unwrap_or(true)
                && agent
                    .as_ref()
                    .map(|agent| task.assigned_to.as_ref() == Some(agent))
                    .unwrap_or(true)
                && tool
                    .as_ref()
                    .map(|tool| {
                        task.tool
                            .as_ref()
                            .map(|invocation| &invocation.tool_id == tool)
                            .unwrap_or(false)
                    })
                    .unwrap_or(true)
                && dependency
                    .as_ref()
                    .map(|dependency| {
                        task.dependencies
                            .iter()
                            .any(|task_id| task_id == dependency)
                    })
                    .unwrap_or(true)
                && has_all_capabilities(&task.required_capabilities, &capabilities)
                && since
                    .as_ref()
                    .map(|since| task.updated_at >= *since)
                    .unwrap_or(true)
                && until
                    .as_ref()
                    .map(|until| task.updated_at <= *until)
                    .unwrap_or(true)
                && search
                    .as_ref()
                    .map(|query| task_matches_query(task, query))
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    tasks.sort_by_key(|task| std::cmp::Reverse(task.updated_at));
    if let Some(limit) = limit {
        tasks.truncate(limit);
    }
    Ok(json!(tasks))
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

fn workflows_json(
    os: &OperatingSystem,
    query: &str,
) -> Result<serde_json::Value, (&'static str, String)> {
    reject_unknown_query_keys(
        query,
        &["priority", "task", "since", "until", "query", "limit"],
    )?;
    let priority = priority_query(query)?;
    let task = task_id_query(query)?;
    let since = timestamp_query(query, "since")?;
    let until = timestamp_query(query, "until")?;
    let search = non_empty_query_string(query, "query")?.map(|query| query.to_ascii_lowercase());
    let limit = positive_query_usize(query, "limit")?;
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
                && search
                    .as_ref()
                    .map(|query| workflow_matches_query(workflow, query))
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    workflows.sort_by_key(|workflow| std::cmp::Reverse(workflow.updated_at));
    if let Some(limit) = limit {
        workflows.truncate(limit);
    }
    Ok(json!(workflows))
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

fn memory_json(
    os: &OperatingSystem,
    query: &str,
) -> Result<serde_json::Value, (&'static str, String)> {
    reject_unknown_query_keys(query, &["query", "tag", "since", "until", "limit"])?;
    let search = non_empty_query_string(query, "query")?;
    let tags = tag_query(query)?;
    let since = timestamp_query(query, "since")?;
    let until = timestamp_query(query, "until")?;
    let limit = positive_query_usize(query, "limit")?;
    let mut records = os
        .memory
        .iter()
        .filter(|record| {
            search
                .as_ref()
                .map(|query| memory_matches_query(record, query))
                .unwrap_or(true)
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
        .collect::<Vec<_>>();
    records.sort_by_key(|record| std::cmp::Reverse(record.updated_at));
    if let Some(limit) = limit {
        records.truncate(limit);
    }
    Ok(json!(records))
}

fn tools_json(
    os: &OperatingSystem,
    query: &str,
) -> Result<serde_json::Value, (&'static str, String)> {
    reject_unknown_query_keys(
        query,
        &["kind", "capability", "since", "until", "query", "limit"],
    )?;
    let kind = tool_kind_query(query)?;
    let capabilities = capability_query(query)?;
    let since = timestamp_query(query, "since")?;
    let until = timestamp_query(query, "until")?;
    let search = non_empty_query_string(query, "query")?.map(|query| query.to_ascii_lowercase());
    let limit = positive_query_usize(query, "limit")?;
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
                && search
                    .as_ref()
                    .map(|query| tool_matches_query(tool, query))
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    tools.sort_by_key(|tool| std::cmp::Reverse(tool.updated_at));
    if let Some(limit) = limit {
        tools.truncate(limit);
    }
    Ok(json!(tools))
}

fn tool_matches_query(tool: &ToolDefinition, query: &str) -> bool {
    tool.id.to_string().to_ascii_lowercase().contains(query)
        || tool.name.to_ascii_lowercase().contains(query)
        || tool.description.to_ascii_lowercase().contains(query)
        || tool.command_template.to_ascii_lowercase().contains(query)
}

fn runs_json(
    os: &OperatingSystem,
    query: &str,
) -> Result<serde_json::Value, (&'static str, String)> {
    reject_unknown_query_keys(
        query,
        &[
            "status", "task", "agent", "since", "until", "query", "limit",
        ],
    )?;
    let status = run_status_query(query)?;
    let task = task_id_query(query)?;
    let agent = agent_id_query(query)?;
    let since = timestamp_query(query, "since")?;
    let until = timestamp_query(query, "until")?;
    let search = non_empty_query_string(query, "query")?.map(|query| query.to_ascii_lowercase());
    let limit = positive_query_usize(query, "limit")?;
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
                && search
                    .as_ref()
                    .map(|query| run.command.to_ascii_lowercase().contains(query))
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    runs.sort_by_key(|run| std::cmp::Reverse(run.started_at));
    if let Some(limit) = limit {
        runs.truncate(limit);
    }
    Ok(json!(runs))
}

fn positive_query_usize(query: &str, key: &str) -> Result<Option<usize>, (&'static str, String)> {
    let mut value = None;
    for (candidate_key, candidate_value) in url::form_urlencoded::parse(query.as_bytes()) {
        if candidate_key != key {
            continue;
        }
        if value.is_some() {
            return Err(invalid_query_response(key, "must not be repeated"));
        }
        let parsed = candidate_value
            .trim()
            .parse::<usize>()
            .ok()
            .filter(|parsed| *parsed > 0)
            .ok_or_else(|| invalid_query_response(key, "must be greater than 0"))?;
        value = Some(parsed);
    }
    Ok(value)
}

fn non_empty_query_string(
    query: &str,
    key: &str,
) -> Result<Option<String>, (&'static str, String)> {
    let mut value = None;
    for (candidate_key, candidate_value) in url::form_urlencoded::parse(query.as_bytes()) {
        if candidate_key != key {
            continue;
        }
        if value.is_some() {
            return Err(invalid_query_response(key, "must not be repeated"));
        }
        if candidate_value.trim().is_empty() {
            return Err(invalid_query_response(key, "must not be empty"));
        }
        value = Some(candidate_value.into_owned());
    }
    Ok(value)
}

fn capability_query(query: &str) -> Result<Vec<String>, (&'static str, String)> {
    let Some(value) = non_empty_query_string(query, "capability")? else {
        return Ok(Vec::new());
    };
    let capabilities = normalize_list(vec![value]);
    if capabilities.is_empty() {
        return Err(invalid_query_response(
            "capability",
            "must include at least one capability",
        ));
    }
    Ok(capabilities)
}

fn tag_query(query: &str) -> Result<Vec<String>, (&'static str, String)> {
    let Some(value) = non_empty_query_string(query, "tag")? else {
        return Ok(Vec::new());
    };
    let tags = normalize_list(vec![value]);
    if tags.is_empty() {
        return Err(invalid_query_response(
            "tag",
            "must include at least one tag",
        ));
    }
    Ok(tags)
}

fn has_all_capabilities(available: &[String], required: &[String]) -> bool {
    required
        .iter()
        .all(|required| available.iter().any(|available| available == required))
}

fn has_all_tags(available: &[String], required: &[String]) -> bool {
    required
        .iter()
        .all(|required| available.iter().any(|available| available == required))
}

fn reject_unknown_query_keys(query: &str, allowed: &[&str]) -> Result<(), (&'static str, String)> {
    for (key, _) in url::form_urlencoded::parse(query.as_bytes()) {
        if !allowed.iter().any(|allowed_key| key == *allowed_key) {
            return Err(unsupported_query_response(&key));
        }
    }
    Ok(())
}

fn agent_status_query(query: &str) -> Result<Option<AgentStatus>, (&'static str, String)> {
    let Some(status) = non_empty_query_string(query, "status")? else {
        return Ok(None);
    };
    AgentStatus::try_parse(&status)
        .ok_or_else(|| invalid_query_response("status", "must be a valid agent status"))
        .map(Some)
}

fn agent_kind_query(query: &str) -> Result<Option<AgentKind>, (&'static str, String)> {
    let Some(kind) = non_empty_query_string(query, "kind")? else {
        return Ok(None);
    };
    Ok(Some(AgentKind::parse(&kind)))
}

fn task_status_query(query: &str) -> Result<Option<TaskStatus>, (&'static str, String)> {
    let Some(status) = non_empty_query_string(query, "status")? else {
        return Ok(None);
    };
    TaskStatus::try_parse(&status)
        .ok_or_else(|| invalid_query_response("status", "must be a valid task status"))
        .map(Some)
}

fn priority_query(query: &str) -> Result<Option<Priority>, (&'static str, String)> {
    let Some(priority) = non_empty_query_string(query, "priority")? else {
        return Ok(None);
    };
    Priority::try_parse(&priority)
        .ok_or_else(|| invalid_query_response("priority", "must be a valid priority"))
        .map(Some)
}

fn run_status_query(query: &str) -> Result<Option<RunStatus>, (&'static str, String)> {
    let Some(status) = non_empty_query_string(query, "status")? else {
        return Ok(None);
    };
    RunStatus::try_parse(&status)
        .ok_or_else(|| invalid_query_response("status", "must be a valid run status"))
        .map(Some)
}

fn event_kind_query(query: &str) -> Result<Option<EventKind>, (&'static str, String)> {
    let Some(kind) = non_empty_query_string(query, "kind")? else {
        return Ok(None);
    };
    EventKind::try_parse(&kind)
        .ok_or_else(|| invalid_query_response("kind", "must be a valid event kind"))
        .map(Some)
}

fn timestamp_query(
    query: &str,
    field: &'static str,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, (&'static str, String)> {
    let Some(value) = non_empty_query_string(query, field)? else {
        return Ok(None);
    };
    chrono::DateTime::parse_from_rfc3339(&value)
        .map(|timestamp| timestamp.with_timezone(&chrono::Utc))
        .map(Some)
        .map_err(|_| invalid_query_response(field, "must be a valid RFC3339 timestamp"))
}

fn task_id_query(query: &str) -> Result<Option<TaskId>, (&'static str, String)> {
    let Some(task) = non_empty_query_string(query, "task")? else {
        return Ok(None);
    };
    let task = TaskId::from_slug(task);
    if task.as_str().is_empty() {
        return Err(invalid_query_response(
            "task",
            "must contain at least one ASCII letter, digit, or hyphen",
        ));
    }
    Ok(Some(task))
}

fn dependency_query(query: &str) -> Result<Option<TaskId>, (&'static str, String)> {
    let Some(task) = non_empty_query_string(query, "after")? else {
        return Ok(None);
    };
    let task = TaskId::from_slug(task);
    if task.as_str().is_empty() {
        return Err(invalid_query_response(
            "after",
            "must contain at least one ASCII letter, digit, or hyphen",
        ));
    }
    Ok(Some(task))
}

fn agent_id_query(query: &str) -> Result<Option<AgentId>, (&'static str, String)> {
    let Some(agent) = non_empty_query_string(query, "agent")? else {
        return Ok(None);
    };
    let agent = AgentId::new(agent);
    if agent.as_str().is_empty() {
        return Err(invalid_query_response(
            "agent",
            "must contain at least one ASCII letter, digit, or hyphen",
        ));
    }
    Ok(Some(agent))
}

fn tool_id_query(query: &str) -> Result<Option<ToolId>, (&'static str, String)> {
    let Some(tool) = non_empty_query_string(query, "tool")? else {
        return Ok(None);
    };
    let tool = ToolId::new(tool);
    if tool.as_str().is_empty() {
        return Err(invalid_query_response(
            "tool",
            "must contain at least one ASCII letter, digit, or hyphen",
        ));
    }
    Ok(Some(tool))
}

fn tool_kind_query(query: &str) -> Result<Option<ToolKind>, (&'static str, String)> {
    let Some(kind) = non_empty_query_string(query, "kind")? else {
        return Ok(None);
    };
    ToolKind::try_parse(&kind)
        .ok_or_else(|| invalid_query_response("kind", "must be a valid tool kind"))
        .map(Some)
}

fn invalid_query_response(field: &str, error: &str) -> (&'static str, String) {
    (
        "400 Bad Request",
        json!({
            "error": format!("{field} {error}"),
        })
        .to_string(),
    )
}

fn unsupported_query_response(field: &str) -> (&'static str, String) {
    let error = if field.trim().is_empty() {
        "query parameter must not be empty".to_owned()
    } else {
        format!("unsupported query parameter `{field}`")
    };
    (
        "400 Bad Request",
        json!({
            "error": error,
        })
        .to_string(),
    )
}

#[cfg(test)]
fn response_for_mutation(
    store: &Store,
    method: &str,
    path: &str,
    body: &[u8],
) -> (&'static str, String) {
    response_for_mutation_with_context(store, None, method, path, body)
}

fn response_for_mutation_with_context(
    store: &Store,
    config_path: Option<&Path>,
    method: &str,
    path: &str,
    body: &[u8],
) -> (&'static str, String) {
    let path = path.split('?').next().unwrap_or(path);
    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    let result = match (method, segments.as_slice()) {
        ("POST", ["init"]) => init_response(store, config_path, body),
        ("POST", ["config"]) => Ok(write_config_response(config_path, body)),
        ("POST", ["agents"]) => create_agent_response(store, body),
        ("POST", ["agents", id]) => update_agent_response(store, id, body),
        ("DELETE", ["agents", id]) => delete_agent_response(store, id),
        ("POST", ["agents", id, "heartbeat"]) => heartbeat_agent_response(store, id, body),
        ("POST", ["agents", id, "claim"]) => claim_task_response(store, id, body),
        ("POST", ["tasks"]) => create_task_response(store, body),
        ("POST", ["tasks", "recover"]) => recover_tasks_response(store, body),
        ("POST", ["tasks", id]) => update_task_response(store, id, body),
        ("POST", ["tasks", id, "complete"]) => {
            finish_task_response(store, id, body, TaskMutation::Complete)
        }
        ("POST", ["tasks", id, "fail"]) => {
            finish_task_response(store, id, body, TaskMutation::Fail)
        }
        ("POST", ["tasks", id, "block"]) => {
            finish_task_response(store, id, body, TaskMutation::Block)
        }
        ("POST", ["tasks", id, "cancel"]) => {
            finish_task_response(store, id, body, TaskMutation::Cancel)
        }
        ("POST", ["tasks", id, "retry"]) => {
            finish_task_response(store, id, body, TaskMutation::Retry)
        }
        ("POST", ["tasks", id, "unblock"]) => {
            finish_task_response(store, id, body, TaskMutation::Unblock)
        }
        ("POST", ["tasks", id, "assign"]) => assign_task_response(store, id, body),
        ("POST", ["tasks", id, "priority"]) => task_priority_response(store, id, body),
        ("POST", ["tasks", id, "dependencies"]) => task_dependencies_response(store, id, body),
        ("POST", ["tasks", id, "plan"]) => plan_task_response(store, id, body),
        ("DELETE", ["tasks", id]) => delete_task_response(store, id),
        ("POST", ["workflows"]) => create_workflow_response(store, body),
        ("POST", ["workflows", id, "run"]) => run_workflow_response(store, id, body),
        ("POST", ["workflows", id, "cancel"]) => cancel_workflow_response(store, id, body),
        ("DELETE", ["workflows", id]) => delete_workflow_response(store, id),
        ("POST", ["tools"]) => create_tool_response(store, body),
        ("POST", ["tools", id]) => update_tool_response(store, id, body),
        ("DELETE", ["tools", id]) => delete_tool_response(store, id),
        ("POST", ["state", "export"]) => export_state_response(store, body),
        ("POST", ["state", "import"]) => import_state_response(store, body),
        ("POST", ["state", "migrate"]) => migrate_state_response(store, body),
        ("POST", ["state", "repair"]) => repair_state_response(store, body),
        ("POST", ["state", "prune"]) => prune_state_response(store, body),
        ("POST", ["state", "backup"]) => backup_state_response(store, body),
        ("POST", ["run"]) => run_once_response(store, body),
        ("POST", ["daemon", "stop"]) => stop_daemon_response(store),
        ("POST", ["service", "launchd"]) => render_launchd_service_response(store, body),
        ("POST", ["service", "launchd", "install"]) => {
            install_launchd_service_response(store, body)
        }
        ("POST", ["service", "launchd", "uninstall"]) => uninstall_launchd_service_response(body),
        ("POST", ["service", "launchd", "start"]) => start_launchd_service_response(body),
        ("POST", ["service", "launchd", "stop"]) => stop_launchd_service_response(body),
        ("POST", ["service", "launchd", "status"]) => status_launchd_service_response(body),
        ("POST", ["runs", id, "cancel"]) => cancel_run_response(store, id),
        ("POST", ["memory"]) => create_memory_response(store, body),
        ("POST", ["memory", id]) => update_memory_response(store, id, body),
        ("DELETE", ["memory", id]) => delete_memory_response(store, id),
        _ => Ok((
            "404 Not Found",
            json!({
                "error": "not found",
                "path": path,
            })
            .to_string(),
        )),
    };

    result.unwrap_or_else(store_error_response)
}

fn store_error_response(error: StoreError) -> (&'static str, String) {
    let status = match error {
        StoreError::AlreadyExists { .. } | StoreError::InvalidState { .. } => "409 Conflict",
        StoreError::EmptyHome
        | StoreError::Io { .. }
        | StoreError::Json { .. }
        | StoreError::Migration { .. }
        | StoreError::MissingHome => "500 Internal Server Error",
    };
    (
        status,
        json!({
            "error": if status == "409 Conflict" {
                "state conflict"
            } else {
                "state unavailable"
            },
            "detail": error.to_string(),
        })
        .to_string(),
    )
}

fn config_response_for_path(config_path: Option<&Path>, path: &str) -> (&'static str, String) {
    let (path, _) = path.split_once('?').unwrap_or((path, ""));
    match path.trim_end_matches('/') {
        "/config" => config_json_response(config_path),
        "/config/validate" => ("200 OK", config_health_json(config_path).to_string()),
        _ => (
            "404 Not Found",
            json!({
                "error": "not found",
                "path": path,
            })
            .to_string(),
        ),
    }
}

fn config_json_response(config_path: Option<&Path>) -> (&'static str, String) {
    let Some(path) = config_path else {
        return (
            "500 Internal Server Error",
            json!({
                "error": "config unavailable",
                "detail": "config path unavailable",
            })
            .to_string(),
        );
    };
    let exists = path.exists();
    match load_config(path) {
        Ok(config) => (
            "200 OK",
            json!({
                "path": path.display().to_string(),
                "exists": exists,
                "config": config.unwrap_or_default(),
            })
            .to_string(),
        ),
        Err(error) => (
            "500 Internal Server Error",
            json!({
                "error": "config unavailable",
                "detail": error.to_string(),
            })
            .to_string(),
        ),
    }
}

fn write_config_response(config_path: Option<&Path>, body: &[u8]) -> (&'static str, String) {
    let request = match parse_json_or_default::<WriteConfigRequest>(body) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let Some(path) = config_path else {
        return (
            "500 Internal Server Error",
            json!({
                "error": "config unavailable",
                "detail": "config path unavailable",
            })
            .to_string(),
        );
    };
    match write_default_config(path, request.force) {
        Ok(()) => (
            "201 Created",
            json!({
                "path": path.display().to_string(),
                "written": true,
                "config": AppConfig::default(),
            })
            .to_string(),
        ),
        Err(error) => config_error_response(error),
    }
}

fn config_error_response(error: ConfigError) -> (&'static str, String) {
    let status = match &error {
        ConfigError::Io { source, .. } if source.kind() == std::io::ErrorKind::AlreadyExists => {
            "409 Conflict"
        }
        ConfigError::Io { .. } | ConfigError::Toml { .. } | ConfigError::Serialize(_) => {
            "500 Internal Server Error"
        }
    };
    (
        status,
        json!({
            "error": if status == "409 Conflict" {
                "config conflict"
            } else {
                "config unavailable"
            },
            "detail": error.to_string(),
        })
        .to_string(),
    )
}

fn init_response(
    store: &Store,
    config_path: Option<&Path>,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json_or_default::<InitRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(name) = &request.name
        && name.trim().is_empty()
    {
        return Ok(invalid_text_response("name", "OS name must not be empty"));
    }
    let config = match config_path {
        Some(path) => match load_config(path) {
            Ok(Some(config)) => config,
            Ok(None) => AppConfig::default(),
            Err(error) => {
                return Ok((
                    "400 Bad Request",
                    json!({
                        "error": "invalid config",
                        "detail": error.to_string(),
                    })
                    .to_string(),
                ));
            }
        },
        None => AppConfig::default(),
    };
    if let Err(error) = validate_seed_config(&config) {
        return Ok((
            "400 Bad Request",
            json!({
                "error": "invalid config",
                "detail": error,
            })
            .to_string(),
        ));
    }
    let os = initialized_os_from_config(config, request.name);
    store.save_validated_checked(&os, request.force)?;
    Ok((
        "201 Created",
        json!({
            "state_path": store.path(),
            "os": os,
        })
        .to_string(),
    ))
}

fn initialized_os_from_config(config: AppConfig, name: Option<String>) -> OperatingSystem {
    let name = name.unwrap_or_else(|| config.name.clone());
    let mut os = OperatingSystem::new(name);
    os.policy = config.policy.clone();
    os.provider = config.provider.clone();
    for agent in config.clone().into_agents() {
        os.register_agent(agent);
    }
    for tool in config.into_tools() {
        os.register_tool(tool);
    }
    os.write_memory(MemoryRecord::new(
        "operating-principles",
        "Prefer explicit plans, durable state, small reviewable tasks, and evented execution.",
        vec!["system".into(), "policy".into()],
    ));
    os
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteConfigRequest {
    #[serde(default)]
    force: bool,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct InitRequest {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    force: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateAgentRequest {
    name: String,
    #[serde(default = "default_builder_kind")]
    kind: String,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    capabilities: Vec<String>,
    #[serde(default = "default_parallel")]
    parallel: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateAgentRequest {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    clear_model: bool,
    #[serde(default)]
    capabilities: Option<Vec<String>>,
    #[serde(default)]
    parallel: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTaskRequest {
    title: String,
    #[serde(default)]
    objective: Option<String>,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    tool: Option<String>,
    #[serde(default)]
    args: BTreeMap<String, String>,
    #[serde(default)]
    secret_args: BTreeMap<String, String>,
    #[serde(default = "default_normal_priority")]
    priority: String,
    #[serde(default)]
    required_capabilities: Vec<String>,
    #[serde(default)]
    dependencies: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateWorkflowRequest {
    objective: String,
    #[serde(default = "default_normal_priority")]
    priority: String,
    #[serde(default)]
    execute: bool,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunWorkflowRequest {
    #[serde(default)]
    all: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateTaskRequest {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    objective: Option<String>,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    clear_command: bool,
    #[serde(default)]
    tool: Option<String>,
    #[serde(default)]
    clear_tool: bool,
    #[serde(default)]
    args: Option<BTreeMap<String, String>>,
    #[serde(default)]
    secret_args: Option<BTreeMap<String, String>>,
    #[serde(default)]
    clear_args: bool,
    #[serde(default)]
    clear_secret_args: bool,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    clear_cwd: bool,
    #[serde(default)]
    required_capabilities: Option<Vec<String>>,
    #[serde(default)]
    clear_required_capabilities: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateToolRequest {
    name: String,
    #[serde(default = "default_shell_kind")]
    kind: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    required_capabilities: Vec<String>,
    command_template: String,
    #[serde(default)]
    cwd: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateToolRequest {
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    clear_description: bool,
    #[serde(default)]
    required_capabilities: Option<Vec<String>>,
    #[serde(default)]
    clear_required_capabilities: bool,
    #[serde(default)]
    command_template: Option<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    clear_cwd: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateMemoryRequest {
    topic: String,
    body: String,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateMemoryRequest {
    #[serde(default)]
    topic: Option<String>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    tags: Option<Vec<String>>,
    #[serde(default)]
    clear_tags: bool,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FinishTaskRequest {
    #[serde(default)]
    note: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssignTaskRequest {
    agent: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskPriorityRequest {
    priority: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskDependenciesRequest {
    dependencies: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanTaskRequest {
    steps: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HeartbeatRequest {
    #[serde(default = "default_online_status")]
    status: String,
    #[serde(default)]
    lease_seconds: Option<i64>,
}

impl Default for HeartbeatRequest {
    fn default() -> Self {
        Self {
            status: default_online_status(),
            lease_seconds: None,
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimTaskRequest {
    #[serde(default)]
    lease_seconds: Option<i64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunRequest {
    #[serde(default = "default_parallel")]
    limit: usize,
    #[serde(default)]
    execute: bool,
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    recover_stale_seconds: Option<i64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LaunchdServiceRequest {
    #[serde(default = "default_launchd_label")]
    label: String,
    #[serde(default)]
    bin_path: Option<PathBuf>,
    #[serde(default = "default_interval_ms")]
    interval_ms: u64,
    #[serde(default = "default_parallel")]
    limit: usize,
    #[serde(default)]
    execute: bool,
    #[serde(default)]
    recover_stale_seconds: Option<i64>,
    #[serde(default)]
    no_logs: bool,
    #[serde(default)]
    plist_path: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UninstallLaunchdServiceRequest {
    #[serde(default = "default_launchd_label")]
    label: String,
    #[serde(default)]
    plist_path: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LaunchdServiceControlRequest {
    #[serde(default = "default_launchd_label")]
    label: String,
    #[serde(default)]
    plist_path: Option<PathBuf>,
    #[serde(default)]
    domain: Option<String>,
    #[serde(default = "default_launchctl_path")]
    launchctl_path: PathBuf,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RepairStateRequest {
    #[serde(default)]
    dry_run: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PruneStateRequest {
    #[serde(default = "default_keep_runs")]
    keep_runs: usize,
    #[serde(default = "default_keep_events")]
    keep_events: usize,
    #[serde(default)]
    dry_run: bool,
}

impl Default for PruneStateRequest {
    fn default() -> Self {
        Self {
            keep_runs: default_keep_runs(),
            keep_events: default_keep_events(),
            dry_run: false,
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackupStateRequest {
    #[serde(default)]
    output: Option<String>,
    #[serde(default)]
    dry_run: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportStateRequest {
    output: String,
    #[serde(default)]
    dry_run: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportStateRequest {
    path: String,
    #[serde(default)]
    force: bool,
    #[serde(default)]
    dry_run: bool,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct MigrateStateRequest {
    #[serde(default)]
    input: Option<String>,
    #[serde(default)]
    output: Option<String>,
    #[serde(default)]
    dry_run: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoverTasksRequest {
    #[serde(default = "default_recover_stale_seconds")]
    older_than_seconds: i64,
}

impl Default for RecoverTasksRequest {
    fn default() -> Self {
        Self {
            older_than_seconds: default_recover_stale_seconds(),
        }
    }
}

impl Default for RunRequest {
    fn default() -> Self {
        Self {
            limit: default_parallel(),
            execute: false,
            dry_run: false,
            recover_stale_seconds: None,
        }
    }
}

impl Default for LaunchdServiceRequest {
    fn default() -> Self {
        Self {
            label: default_launchd_label(),
            bin_path: None,
            interval_ms: default_interval_ms(),
            limit: default_parallel(),
            execute: false,
            recover_stale_seconds: None,
            no_logs: false,
            plist_path: None,
        }
    }
}

impl Default for UninstallLaunchdServiceRequest {
    fn default() -> Self {
        Self {
            label: default_launchd_label(),
            plist_path: None,
        }
    }
}

impl Default for LaunchdServiceControlRequest {
    fn default() -> Self {
        Self {
            label: default_launchd_label(),
            plist_path: None,
            domain: None,
            launchctl_path: default_launchctl_path(),
        }
    }
}

enum TaskMutation {
    Complete,
    Fail,
    Block,
    Cancel,
    Retry,
    Unblock,
}

fn create_agent_response(store: &Store, body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json::<CreateAgentRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_invalid_agent_name(&request.name) {
        return Ok(response);
    }
    if let Some(response) = reject_empty_text_field("kind", &request.kind) {
        return Ok(response);
    }
    if let Some(response) = reject_empty_optional_text_field("model", request.model.as_deref()) {
        return Ok(response);
    }
    if let Some(response) =
        reject_invalid_capability_values("capabilities", &request.capabilities, true)
    {
        return Ok(response);
    }
    if request.parallel == 0 {
        return Ok((
            "400 Bad Request",
            json!({
                "error": "parallel must be greater than 0",
                "parallel": request.parallel,
            })
            .to_string(),
        ));
    }
    store.update(|os| {
        let agent = Agent::new(
            request.name,
            AgentKind::parse(&request.kind),
            request.model,
            request.capabilities,
            request.parallel,
        );
        let id = agent.id.clone();
        if os.agents.contains_key(&id) {
            return Ok((
                "409 Conflict",
                json!({
                    "error": "agent already exists",
                    "id": id,
                })
                .to_string(),
            ));
        }
        os.register_agent(agent);
        Ok((
            "201 Created",
            json!({
                "id": id,
                "agent": os.agents.get(&id),
            })
            .to_string(),
        ))
    })
}

fn update_agent_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_agent_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json::<UpdateAgentRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if request.name.is_none()
        && request.kind.is_none()
        && request.model.is_none()
        && !request.clear_model
        && request.capabilities.is_none()
        && request.parallel.is_none()
    {
        return Ok((
            "400 Bad Request",
            json!({ "error": "agent update must include at least one field" }).to_string(),
        ));
    }
    if request.clear_model && request.model.is_some() {
        return Ok((
            "400 Bad Request",
            json!({ "error": "clear_model cannot be combined with model" }).to_string(),
        ));
    }
    if let Some(name) = &request.name
        && let Some(response) = reject_invalid_agent_name(name)
    {
        return Ok(response);
    }
    if let Some(kind) = &request.kind
        && let Some(response) = reject_empty_text_field("kind", kind)
    {
        return Ok(response);
    }
    if let Some(response) = reject_empty_optional_text_field("model", request.model.as_deref()) {
        return Ok(response);
    }
    if let Some(capabilities) = &request.capabilities
        && let Some(response) = reject_invalid_capability_values("capabilities", capabilities, true)
    {
        return Ok(response);
    }
    if let Some(parallel) = request.parallel
        && parallel == 0
    {
        return Ok((
            "400 Bad Request",
            json!({
                "error": "parallel must be greater than 0",
                "parallel": parallel,
            })
            .to_string(),
        ));
    }
    let update = AgentUpdate {
        name: request.name,
        kind: request.kind.as_deref().map(AgentKind::parse),
        model: if request.clear_model {
            Some(None)
        } else {
            request.model.map(Some)
        },
        capabilities: request.capabilities,
        max_parallel_tasks: request.parallel,
    };
    store.update(|os| {
        Ok(match Runtime::update_agent(os, &id, update) {
            Ok(agent) => (
                "200 OK",
                json!({
                    "id": id,
                    "agent": agent,
                })
                .to_string(),
            ),
            Err(error) => runtime_error_response(error),
        })
    })
}

fn heartbeat_agent_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_agent_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json_or_default::<HeartbeatRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_non_positive_lease(request.lease_seconds) {
        return Ok(response);
    }
    let status = match parse_agent_status_request(&request.status) {
        Ok(status) => status,
        Err(response) => return Ok(response),
    };
    store.update(|os| {
        Ok(
            match Runtime::heartbeat_agent(os, &id, status, request.lease_seconds) {
                Ok(()) => (
                    "200 OK",
                    json!({
                        "id": id,
                        "agent": os.agents.get(&id),
                    })
                    .to_string(),
                ),
                Err(error) => (
                    "404 Not Found",
                    json!({ "error": error.to_string() }).to_string(),
                ),
            },
        )
    })
}

fn delete_agent_response(store: &Store, id: &str) -> Result<(&'static str, String), StoreError> {
    let id = match parse_agent_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    store.update(|os| {
        Ok(if !os.agents.contains_key(&id) {
            (
                "404 Not Found",
                json!({ "error": "agent not found" }).to_string(),
            )
        } else {
            let blockers = os.agent_removal_blockers(&id);
            if blockers.is_empty() {
                let removed = os.remove_agent(&id);
                (
                    "200 OK",
                    json!({
                        "id": id,
                        "removed": true,
                        "agent": removed,
                    })
                    .to_string(),
                )
            } else {
                (
                    "409 Conflict",
                    json!({
                        "error": "agent is still referenced",
                        "references": blockers,
                    })
                    .to_string(),
                )
            }
        })
    })
}

fn claim_task_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_agent_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json_or_default::<ClaimTaskRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_non_positive_lease(request.lease_seconds) {
        return Ok(response);
    }
    store.update(|os| {
        let lease_seconds = request.lease_seconds.or_else(|| {
            os.agents
                .get(&id)
                .and_then(|agent| agent.lease_expires_at)
                .and_then(|expires_at| {
                    let remaining = (expires_at - chrono::Utc::now()).num_seconds();
                    (remaining > 0).then_some(remaining)
                })
        });
        let response = match Runtime::heartbeat_agent(os, &id, AgentStatus::Online, lease_seconds) {
            Ok(()) => match Scheduler::assign_next_for_agent(os, &id) {
                Some(assignment) => {
                    let task_id = assignment.task_id.clone();
                    (
                        "200 OK",
                        json!({
                            "claimed": true,
                            "assignment": assignment,
                            "task": os.tasks.get(&task_id),
                        })
                        .to_string(),
                    )
                }
                None => (
                    "200 OK",
                    json!({
                        "claimed": false,
                        "assignment": null,
                        "task": null,
                    })
                    .to_string(),
                ),
            },
            Err(error) => runtime_error_response(error),
        };
        Ok(response)
    })
}

fn create_task_response(store: &Store, body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json::<CreateTaskRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_empty_text_field("title", &request.title) {
        return Ok(response);
    }
    if let Some(objective) = &request.objective
        && let Some(response) = reject_empty_text_field("objective", objective)
    {
        return Ok(response);
    }
    if let Some(command) = &request.command
        && let Some(response) = reject_empty_text_field("command", command)
    {
        return Ok(response);
    }
    if let Some(response) = reject_empty_optional_text_field("cwd", request.cwd.as_deref()) {
        return Ok(response);
    }
    if let Some(response) = reject_invalid_capability_values(
        "required_capabilities",
        &request.required_capabilities,
        false,
    ) {
        return Ok(response);
    }
    if let Some(tool_id) = &request.tool
        && let Some(response) = reject_invalid_tool_id(tool_id)
    {
        return Ok(response);
    }
    if request.tool.is_none() && !request.args.is_empty() {
        return Ok(invalid_text_response("args", "args require a tool"));
    }
    if request.tool.is_none() && !request.secret_args.is_empty() {
        return Ok(invalid_text_response(
            "secret_args",
            "secret_args require a tool",
        ));
    }
    let priority = match parse_priority_request(&request.priority) {
        Ok(priority) => priority,
        Err(response) => return Ok(response),
    };
    store.update(|os| {
        let response = if request.command.is_some() && request.tool.is_some() {
            (
                "400 Bad Request",
                json!({ "error": "task cannot define both command and tool" }).to_string(),
            )
        } else {
            let title = request.title;
            let objective = request.objective.clone().unwrap_or_else(|| title.clone());
            let mut task = Task::new(title, objective, priority, request.required_capabilities);
            task.command = request.command;
            task.cwd = request.cwd;
            task.dependencies = match validate_task_dependencies(os, request.dependencies) {
                Ok(dependencies) => dependencies,
                Err(response) => return Ok(response),
            };
            if let Some(tool_id) = request.tool {
                let tool_id = ToolId::new(tool_id);
                if let Some(tool) = os.tools.get(&tool_id) {
                    if task.required_capabilities.is_empty() {
                        task.required_capabilities = tool.required_capabilities.clone();
                    }
                    if let Some(response) = reject_invalid_tool_invocation_args(
                        &request.args,
                        &request.secret_args,
                        &os.policy.redacted_env_patterns,
                    ) {
                        return Ok(response);
                    }
                    let invocation = ToolInvocation::with_secret_env_args(
                        tool_id,
                        request.args,
                        request.secret_args,
                    );
                    if let Err(error) = validate_tool_invocation(tool, &invocation) {
                        return Ok((
                            "422 Unprocessable Entity",
                            json!({ "error": error.to_string() }).to_string(),
                        ));
                    }
                    task.tool = Some(invocation);
                } else {
                    return Ok((
                        "404 Not Found",
                        json!({ "error": "tool not found" }).to_string(),
                    ));
                }
            }
            let id = task.id.clone();
            os.create_task(task);
            (
                "201 Created",
                json!({
                    "id": id,
                    "task": os.tasks.get(&id),
                })
                .to_string(),
            )
        };
        Ok(response)
    })
}

fn create_workflow_response(
    store: &Store,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json::<CreateWorkflowRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_empty_text_field("objective", &request.objective) {
        return Ok(response);
    }
    let priority = match parse_priority_request(&request.priority) {
        Ok(priority) => priority,
        Err(response) => return Ok(response),
    };
    let execute = request.execute;
    let (id, workflow, plan_id, build_id, review_id) = store.update(|os| {
        let objective = request.objective;
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
            objective,
            priority,
            BTreeMap::from([
                ("plan".into(), plan_id.clone()),
                ("build".into(), build_id.clone()),
                ("review".into(), review_id.clone()),
            ]),
        );
        let id = workflow.id.clone();
        os.create_workflow(workflow.clone());
        Ok((id, workflow, plan_id, build_id, review_id))
    })?;

    let mut runs = Vec::new();
    let mut errors = Vec::new();
    if execute {
        (runs, errors) = execute_workflow_stages(store, &id, false)?;
    }

    Ok((
        "201 Created",
        json!({
            "id": id,
            "workflow": workflow,
            "tasks": {
                "plan": plan_id,
                "build": build_id,
                "review": review_id,
            },
            "runs": runs,
            "errors": errors,
        })
        .to_string(),
    ))
}

fn execute_next_workflow_stage(
    store: &Store,
    workflow_id: &WorkflowId,
) -> Result<(Vec<RunRecord>, Vec<String>), StoreError> {
    let (assignment, mut os) = store.update(|os| {
        let task_id = os.workflow_progress(workflow_id).and_then(|progress| {
            progress
                .stages
                .into_iter()
                .find(|stage| stage.status.as_ref() != Some(&TaskStatus::Complete))
                .map(|stage| stage.task_id)
        });
        let assignment = task_id.and_then(|task_id| Scheduler::assign_ready_task(os, &task_id));
        Ok::<_, StoreError>((assignment, os.clone()))
    })?;

    let Some(assignment) = assignment else {
        return Ok((Vec::new(), Vec::new()));
    };

    let mut runs = Vec::new();
    let mut errors = Vec::new();
    for result in CommandExecutor::execute_tasks_parallel(
        &mut os,
        store,
        std::slice::from_ref(&assignment.task_id),
    ) {
        match result {
            Ok(run) => runs.push(run),
            Err(error) => errors.push(error.to_string()),
        }
    }
    Ok((runs, errors))
}

fn execute_workflow_stages(
    store: &Store,
    workflow_id: &WorkflowId,
    run_all: bool,
) -> Result<(Vec<RunRecord>, Vec<String>), StoreError> {
    let max_runs = if run_all {
        store
            .load()?
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

fn delete_workflow_response(store: &Store, id: &str) -> Result<(&'static str, String), StoreError> {
    let id = match parse_workflow_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    store.update(|os| {
        Ok(match os.remove_workflow(&id) {
            Some(workflow) => (
                "200 OK",
                json!({
                    "id": id,
                    "removed": true,
                    "workflow": workflow,
                })
                .to_string(),
            ),
            None => (
                "404 Not Found",
                json!({ "error": "workflow not found" }).to_string(),
            ),
        })
    })
}

fn update_task_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_task_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json::<UpdateTaskRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if request.title.is_none()
        && request.objective.is_none()
        && request.command.is_none()
        && !request.clear_command
        && request.tool.is_none()
        && !request.clear_tool
        && request.args.is_none()
        && request.secret_args.is_none()
        && !request.clear_args
        && !request.clear_secret_args
        && request.cwd.is_none()
        && !request.clear_cwd
        && request.required_capabilities.is_none()
        && !request.clear_required_capabilities
    {
        return Ok((
            "400 Bad Request",
            json!({ "error": "task update must include at least one field" }).to_string(),
        ));
    }
    if request.clear_command && request.command.is_some() {
        return Ok((
            "400 Bad Request",
            json!({ "error": "clear_command cannot be combined with command" }).to_string(),
        ));
    }
    if request.clear_tool && request.tool.is_some() {
        return Ok((
            "400 Bad Request",
            json!({ "error": "clear_tool cannot be combined with tool" }).to_string(),
        ));
    }
    if request.clear_tool
        && (request.args.is_some()
            || request.secret_args.is_some()
            || request.clear_args
            || request.clear_secret_args)
    {
        return Ok((
            "400 Bad Request",
            json!({ "error": "clear_tool cannot be combined with tool argument updates" })
                .to_string(),
        ));
    }
    if request.clear_args && request.args.is_some() {
        return Ok((
            "400 Bad Request",
            json!({ "error": "clear_args cannot be combined with args" }).to_string(),
        ));
    }
    if request.clear_secret_args && request.secret_args.is_some() {
        return Ok((
            "400 Bad Request",
            json!({ "error": "clear_secret_args cannot be combined with secret_args" }).to_string(),
        ));
    }
    if request.clear_cwd && request.cwd.is_some() {
        return Ok((
            "400 Bad Request",
            json!({ "error": "clear_cwd cannot be combined with cwd" }).to_string(),
        ));
    }
    if request.clear_required_capabilities && request.required_capabilities.is_some() {
        return Ok((
            "400 Bad Request",
            json!({
                "error": "clear_required_capabilities cannot be combined with required_capabilities"
            })
            .to_string(),
        ));
    }
    if let Some(title) = &request.title
        && let Some(response) = reject_empty_text_field("title", title)
    {
        return Ok(response);
    }
    if let Some(objective) = &request.objective
        && let Some(response) = reject_empty_text_field("objective", objective)
    {
        return Ok(response);
    }
    if let Some(command) = &request.command
        && let Some(response) = reject_empty_text_field("command", command)
    {
        return Ok(response);
    }
    if let Some(tool_id) = &request.tool
        && let Some(response) = reject_invalid_tool_id(tool_id)
    {
        return Ok(response);
    }
    if let Some(args) = &request.args
        && let Some(response) = reject_invalid_tool_invocation_args(
            args,
            &BTreeMap::new(),
            &store.load()?.policy.redacted_env_patterns,
        )
    {
        return Ok(response);
    }
    if let Some(secret_args) = &request.secret_args
        && let Some(response) =
            reject_invalid_tool_invocation_args(&BTreeMap::new(), secret_args, &[])
    {
        return Ok(response);
    }
    if let (Some(args), Some(secret_args)) = (&request.args, &request.secret_args)
        && let Some(response) = reject_invalid_tool_invocation_args(
            args,
            secret_args,
            &store.load()?.policy.redacted_env_patterns,
        )
    {
        return Ok(response);
    }
    if let Some(response) = reject_empty_optional_text_field("cwd", request.cwd.as_deref()) {
        return Ok(response);
    }
    if let Some(required_capabilities) = &request.required_capabilities
        && let Some(response) =
            reject_invalid_capability_values("required_capabilities", required_capabilities, false)
    {
        return Ok(response);
    }
    store.update(|os| {
        let tool_update = if request.clear_tool {
            Some(None)
        } else if let Some(tool_id) = request.tool {
            let tool_id = ToolId::new(tool_id);
            let Some(tool) = os.tools.get(&tool_id) else {
                return Ok((
                    "404 Not Found",
                    json!({ "error": "tool not found" }).to_string(),
                ));
            };
            let args = request.args.unwrap_or_default();
            let secret_args = request.secret_args.unwrap_or_default();
            if let Some(response) = reject_invalid_tool_invocation_args(
                &args,
                &secret_args,
                &os.policy.redacted_env_patterns,
            ) {
                return Ok(response);
            }
            let invocation = ToolInvocation::with_secret_env_args(tool_id, args, secret_args);
            if let Err(error) = validate_tool_invocation(tool, &invocation) {
                return Ok((
                    "422 Unprocessable Entity",
                    json!({ "error": error.to_string() }).to_string(),
                ));
            }
            Some(Some(invocation))
        } else if request.args.is_some()
            || request.secret_args.is_some()
            || request.clear_args
            || request.clear_secret_args
        {
            let Some(existing) = os.tasks.get(&id).and_then(|task| task.tool.clone()) else {
                return Ok((
                    "400 Bad Request",
                    json!({ "error": "task has no tool invocation" }).to_string(),
                ));
            };
            let Some(tool) = os.tools.get(&existing.tool_id) else {
                return Ok((
                    "404 Not Found",
                    json!({ "error": "tool not found" }).to_string(),
                ));
            };
            let mut invocation = existing;
            if request.clear_args {
                invocation.args.clear();
            }
            if request.clear_secret_args {
                invocation.secret_env_args.clear();
            }
            if let Some(args) = request.args {
                invocation.args = args;
            }
            if let Some(secret_args) = request.secret_args {
                invocation.secret_env_args = secret_args;
            }
            if let Some(response) = reject_invalid_tool_invocation_args(
                &invocation.args,
                &invocation.secret_env_args,
                &os.policy.redacted_env_patterns,
            ) {
                return Ok(response);
            }
            if let Err(error) = validate_tool_invocation(tool, &invocation) {
                return Ok((
                    "422 Unprocessable Entity",
                    json!({ "error": error.to_string() }).to_string(),
                ));
            }
            Some(Some(invocation))
        } else {
            None
        };
        let update = TaskUpdate {
            title: request.title,
            objective: request.objective,
            command: if request.clear_command {
                Some(None)
            } else {
                request.command.map(Some)
            },
            tool: tool_update,
            cwd: if request.clear_cwd {
                Some(None)
            } else {
                request.cwd.map(Some)
            },
            required_capabilities: if request.clear_required_capabilities {
                Some(Vec::new())
            } else {
                request.required_capabilities
            },
        };
        Ok(match Runtime::update_task(os, &id, update) {
            Ok(task) => (
                "200 OK",
                json!({
                    "id": id,
                    "task": task,
                })
                .to_string(),
            ),
            Err(error) => runtime_error_response(error),
        })
    })
}

fn finish_task_response(
    store: &Store,
    id: &str,
    body: &[u8],
    mutation: TaskMutation,
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_task_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json_or_default::<FinishTaskRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_empty_optional_text_field("note", request.note.as_deref()) {
        return Ok(response);
    }
    store.update(|os| {
        let result = match mutation {
            TaskMutation::Complete => Runtime::complete_task(os, &id, request.note),
            TaskMutation::Fail => Runtime::fail_task(os, &id, request.note),
            TaskMutation::Block => Runtime::block_task(os, &id, request.note),
            TaskMutation::Cancel => Runtime::cancel_task(os, &id, request.note),
            TaskMutation::Retry => Runtime::retry_task(os, &id, request.note),
            TaskMutation::Unblock => Runtime::unblock_task(os, &id, request.note),
        };
        Ok(match result {
            Ok(()) => (
                "200 OK",
                json!({
                    "id": id,
                    "task": os.tasks.get(&id),
                })
                .to_string(),
            ),
            Err(error) => runtime_error_response(error),
        })
    })
}

fn runtime_error_response(error: RuntimeError) -> (&'static str, String) {
    let status = match error {
        RuntimeError::TaskNotFound(_)
        | RuntimeError::AgentNotFound(_)
        | RuntimeError::ToolNotFound(_)
        | RuntimeError::WorkflowNotFound(_) => "404 Not Found",
        RuntimeError::DuplicateTaskDependency(_)
        | RuntimeError::SelfDependency(_)
        | RuntimeError::TaskDependencyNotFound(_)
        | RuntimeError::TaskInvalid { .. }
        | RuntimeError::AgentInvalid { .. } => "400 Bad Request",
        RuntimeError::ToolInvalid { .. } => "422 Unprocessable Entity",
        RuntimeError::AgentMissing(_)
        | RuntimeError::AgentIncompatible { .. }
        | RuntimeError::AgentCannotAccept { .. }
        | RuntimeError::TaskDependencyIncomplete { .. }
        | RuntimeError::TaskNotEditable { .. }
        | RuntimeError::TaskReferenced { .. }
        | RuntimeError::ToolIncompatible { .. }
        | RuntimeError::InvalidTransition { .. } => "409 Conflict",
    };
    (status, json!({ "error": error.to_string() }).to_string())
}

fn assign_task_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_task_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json::<AssignTaskRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let agent_id = match parse_agent_path_id(&request.agent) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    store.update(|os| {
        Ok(match Runtime::assign_task(os, &id, &agent_id) {
            Ok(assignment) => (
                "200 OK",
                json!({
                    "assignment": assignment,
                    "task": os.tasks.get(&id),
                    "agent": os.agents.get(&agent_id),
                })
                .to_string(),
            ),
            Err(error) => runtime_error_response(error),
        })
    })
}

fn task_priority_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_task_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json::<TaskPriorityRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let priority = match parse_priority_request(&request.priority) {
        Ok(priority) => priority,
        Err(response) => return Ok(response),
    };
    store.update(|os| {
        Ok(match Runtime::reprioritize_task(os, &id, priority) {
            Ok(()) => (
                "200 OK",
                json!({
                    "id": id,
                    "task": os.tasks.get(&id),
                })
                .to_string(),
            ),
            Err(error) => runtime_error_response(error),
        })
    })
}

fn task_dependencies_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_task_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json::<TaskDependenciesRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let dependencies = match validate_task_dependencies_for_task(Some(&id), request.dependencies) {
        Ok(dependencies) => dependencies,
        Err(response) => return Ok(response),
    };
    store.update(|os| {
        Ok(
            match Runtime::set_task_dependencies(os, &id, dependencies) {
                Ok(()) => (
                    "200 OK",
                    json!({
                        "id": id,
                        "task": os.tasks.get(&id),
                    })
                    .to_string(),
                ),
                Err(error) => runtime_error_response(error),
            },
        )
    })
}

fn plan_task_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_task_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json::<PlanTaskRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_invalid_plan_steps(&request.steps) {
        return Ok(response);
    }
    store.update(|os| {
        Ok(match Runtime::set_task_plan(os, &id, request.steps) {
            Ok(task) => (
                "200 OK",
                json!({
                    "id": id,
                    "task": task,
                })
                .to_string(),
            ),
            Err(error) => runtime_error_response(error),
        })
    })
}

fn delete_task_response(store: &Store, id: &str) -> Result<(&'static str, String), StoreError> {
    let id = match parse_task_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    store.update(|os| {
        Ok(match Runtime::delete_task(os, &id) {
            Ok(()) => (
                "200 OK",
                json!({
                    "id": id,
                    "deleted": true,
                })
                .to_string(),
            ),
            Err(error) => runtime_error_response(error),
        })
    })
}

fn recover_tasks_response(
    store: &Store,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json_or_default::<RecoverTasksRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if request.older_than_seconds < 0 {
        return Ok((
            "400 Bad Request",
            json!({
                "error": "older_than_seconds must be greater than or equal to 0",
                "older_than_seconds": request.older_than_seconds,
            })
            .to_string(),
        ));
    }

    let recovered = store.update(|os| {
        Ok::<_, StoreError>(Runtime::recover_stale_tasks(
            os,
            chrono::Duration::seconds(request.older_than_seconds),
        ))
    })?;
    Ok((
        "200 OK",
        json!({
            "older_than_seconds": request.older_than_seconds,
            "recovered": recovered,
        })
        .to_string(),
    ))
}

fn create_tool_response(store: &Store, body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json::<CreateToolRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_invalid_tool_name(&request.name) {
        return Ok(response);
    }
    if let Some(response) = reject_empty_text_field("command_template", &request.command_template) {
        return Ok(response);
    }
    if let Some(response) = reject_empty_optional_text_field("cwd", request.cwd.as_deref()) {
        return Ok(response);
    }
    if let Some(response) = reject_invalid_capability_values(
        "required_capabilities",
        &request.required_capabilities,
        false,
    ) {
        return Ok(response);
    }
    let kind = match parse_tool_kind_request(&request.kind) {
        Ok(kind) => kind,
        Err(response) => return Ok(response),
    };
    store.update(|os| {
        let tool = ToolDefinition::new(
            request.name,
            kind,
            request.description,
            request.required_capabilities,
            request.command_template,
            request.cwd,
        );
        if let Err(error) = validate_tool_template(&tool) {
            return Ok((
                "422 Unprocessable Entity",
                json!({ "error": error.to_string() }).to_string(),
            ));
        }
        let id = tool.id.clone();
        if os.tools.contains_key(&id) {
            return Ok((
                "409 Conflict",
                json!({
                    "error": "tool already exists",
                    "id": id,
                })
                .to_string(),
            ));
        }
        os.register_tool(tool);
        Ok((
            "201 Created",
            json!({
                "id": id,
                "tool": os.tools.get(&id),
            })
            .to_string(),
        ))
    })
}

fn update_tool_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_tool_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json::<UpdateToolRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if request.kind.is_none()
        && request.description.is_none()
        && !request.clear_description
        && request.required_capabilities.is_none()
        && !request.clear_required_capabilities
        && request.command_template.is_none()
        && request.cwd.is_none()
        && !request.clear_cwd
    {
        return Ok((
            "400 Bad Request",
            json!({ "error": "tool update must include at least one field" }).to_string(),
        ));
    }
    if request.clear_description && request.description.is_some() {
        return Ok((
            "400 Bad Request",
            json!({ "error": "clear_description cannot be combined with description" }).to_string(),
        ));
    }
    if request.clear_cwd && request.cwd.is_some() {
        return Ok((
            "400 Bad Request",
            json!({ "error": "clear_cwd cannot be combined with cwd" }).to_string(),
        ));
    }
    if request.clear_required_capabilities && request.required_capabilities.is_some() {
        return Ok((
            "400 Bad Request",
            json!({
                "error": "clear_required_capabilities cannot be combined with required_capabilities"
            })
            .to_string(),
        ));
    }
    let kind = match request.kind.as_deref().map(parse_tool_kind_request) {
        Some(Ok(kind)) => Some(kind),
        Some(Err(response)) => return Ok(response),
        None => None,
    };
    if let Some(command_template) = &request.command_template
        && let Some(response) = reject_empty_text_field("command_template", command_template)
    {
        return Ok(response);
    }
    if let Some(response) = reject_empty_optional_text_field("cwd", request.cwd.as_deref()) {
        return Ok(response);
    }
    if let Some(required_capabilities) = &request.required_capabilities
        && let Some(response) =
            reject_invalid_capability_values("required_capabilities", required_capabilities, false)
    {
        return Ok(response);
    }
    let update = ToolUpdate {
        kind,
        description: if request.clear_description {
            Some(String::new())
        } else {
            request.description
        },
        required_capabilities: if request.clear_required_capabilities {
            Some(Vec::new())
        } else {
            request.required_capabilities
        },
        command_template: request.command_template,
        default_cwd: if request.clear_cwd {
            Some(None)
        } else {
            request.cwd.map(Some)
        },
    };
    store.update(|os| {
        Ok(match Runtime::update_tool(os, &id, update) {
            Ok(tool) => (
                "200 OK",
                json!({
                    "id": id,
                    "tool": tool,
                })
                .to_string(),
            ),
            Err(error) => runtime_error_response(error),
        })
    })
}

fn run_workflow_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_workflow_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json_or_default::<RunWorkflowRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if !store.load()?.workflows.contains_key(&id) {
        return Ok((
            "404 Not Found",
            json!({ "error": "workflow not found" }).to_string(),
        ));
    }

    let (runs, errors) = execute_workflow_stages(store, &id, request.all)?;
    let os = store.load()?;
    let Some(progress) = os.workflow_progress(&id) else {
        return Ok((
            "404 Not Found",
            json!({ "error": "workflow not found" }).to_string(),
        ));
    };

    Ok((
        "200 OK",
        json!({
            "id": id,
            "progress": progress,
            "runs": runs,
            "errors": errors,
        })
        .to_string(),
    ))
}

fn stop_daemon_response(store: &Store) -> Result<(&'static str, String), StoreError> {
    store.update(|os| {
        let stop_requested = Runtime::request_daemon_stop(os);
        Ok((
            "200 OK",
            json!({
                "stop_requested": stop_requested,
                "daemon": os.daemon,
            })
            .to_string(),
        ))
    })
}

fn cancel_workflow_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_workflow_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json_or_default::<FinishTaskRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_empty_optional_text_field("note", request.note.as_deref()) {
        return Ok(response);
    }

    store.update(|os| {
        Ok(match Runtime::cancel_workflow(os, &id, request.note) {
            Ok(cancelled_tasks) => {
                let Some(progress) = os.workflow_progress(&id) else {
                    return Ok((
                        "404 Not Found",
                        json!({ "error": "workflow not found" }).to_string(),
                    ));
                };
                (
                    "200 OK",
                    json!({
                        "id": id,
                        "cancelled_tasks": cancelled_tasks,
                        "progress": progress,
                    })
                    .to_string(),
                )
            }
            Err(error) => runtime_error_response(error),
        })
    })
}

fn delete_tool_response(store: &Store, id: &str) -> Result<(&'static str, String), StoreError> {
    let id = match parse_tool_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    store.update(|os| {
        Ok(if !os.tools.contains_key(&id) {
            (
                "404 Not Found",
                json!({ "error": "tool not found" }).to_string(),
            )
        } else {
            let blockers = os.tool_removal_blockers(&id);
            if blockers.is_empty() {
                let removed = os.remove_tool(&id);
                (
                    "200 OK",
                    json!({
                        "id": id,
                        "removed": true,
                        "tool": removed,
                    })
                    .to_string(),
                )
            } else {
                (
                    "409 Conflict",
                    json!({
                        "error": "tool is still referenced",
                        "references": blockers,
                    })
                    .to_string(),
                )
            }
        })
    })
}

fn repair_state_response(store: &Store, body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json_or_default::<RepairStateRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };

    if request.dry_run {
        let mut os = store.load()?;
        let report = repair_state(&mut os);
        let status = if report.validation.valid {
            "200 OK"
        } else {
            "409 Conflict"
        };
        return Ok((
            status,
            json!({
                "dry_run": true,
                "repair": report,
            })
            .to_string(),
        ));
    }

    let report = store.repair_state()?;
    let status = if report.validation.valid {
        "200 OK"
    } else {
        "409 Conflict"
    };
    Ok((
        status,
        json!({
            "dry_run": false,
            "repair": report,
        })
        .to_string(),
    ))
}

fn prune_state_response(store: &Store, body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json_or_default::<PruneStateRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let report = store.prune(request.keep_runs, request.keep_events, request.dry_run)?;
    Ok(("200 OK", json!(report).to_string()))
}

fn backup_state_response(store: &Store, body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json_or_default::<BackupStateRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let output = match request.output {
        Some(output) if output.trim().is_empty() => {
            return Ok(invalid_text_response("output", "output must not be empty"));
        }
        Some(output) => std::path::PathBuf::from(output),
        None => store.default_backup_path(),
    };
    let backup = if request.dry_run {
        store.preview_backup_to_path(&output)?
    } else {
        store.backup_to_path(&output)?
    };
    Ok((
        "200 OK",
        json!({
            "dry_run": request.dry_run,
            "backup": backup,
        })
        .to_string(),
    ))
}

fn export_state_response(store: &Store, body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json::<ExportStateRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if request.output.trim().is_empty() {
        return Ok(invalid_text_response("output", "output must not be empty"));
    }
    let output = std::path::PathBuf::from(request.output);
    let exported = if request.dry_run {
        store.preview_export_to_path(&output)?
    } else {
        store.export_to_path(&output)?
    };
    Ok((
        "200 OK",
        json!({
            "dry_run": request.dry_run,
            "exported": exported,
        })
        .to_string(),
    ))
}

fn import_state_response(store: &Store, body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json::<ImportStateRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if request.path.trim().is_empty() {
        return Ok(invalid_text_response(
            "path",
            "import path must not be empty",
        ));
    }
    let path = std::path::PathBuf::from(request.path);
    let (_os, validation) = if request.dry_run {
        store.preview_import_from_path_checked(&path, request.force)?
    } else {
        store.import_from_path_checked(&path, request.force)?
    };
    Ok((
        "200 OK",
        json!({
            "dry_run": request.dry_run,
            "imported": path,
            "state_path": store.path(),
            "validation": validation,
        })
        .to_string(),
    ))
}

fn migrate_state_response(
    store: &Store,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json_or_default::<MigrateStateRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let input = match request.input {
        Some(input) if input.trim().is_empty() => {
            return Ok(invalid_text_response("input", "input must not be empty"));
        }
        Some(input) => std::path::PathBuf::from(input),
        None => store.path().to_path_buf(),
    };
    let output = match request.output {
        Some(output) if output.trim().is_empty() => {
            return Ok(invalid_text_response("output", "output must not be empty"));
        }
        Some(output) => std::path::PathBuf::from(output),
        None => store.path().to_path_buf(),
    };
    let (migration, validation) = if request.dry_run {
        store.preview_migrate_path(&input)?
    } else {
        store.migrate_path_to(&input, &output)?
    };
    Ok((
        "200 OK",
        json!({
            "dry_run": request.dry_run,
            "input": input,
            "output": output,
            "migration": migration,
            "validation": validation,
        })
        .to_string(),
    ))
}

fn run_once_response(store: &Store, body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json_or_default::<RunRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_invalid_run_request(&request) {
        return Ok(response);
    }
    if request.dry_run {
        let mut os = store.load()?;
        let mut report = crate::RuntimeReport::default();
        if let Some(seconds) = request.recover_stale_seconds {
            report.recovered_tasks =
                Runtime::recover_stale_tasks(&mut os, chrono::Duration::seconds(seconds));
        }
        let tick_report = Runtime::tick(&mut os, request.limit);
        report.assignments = tick_report.assignments;
        report.completed_tasks = tick_report.completed_tasks;
        report.expired_agents = tick_report.expired_agents;
        report.notes.extend(tick_report.notes);
        return Ok((
            "200 OK",
            json!({
                "dry_run": true,
                "scheduler": report,
                "runs": [],
                "errors": [],
            })
            .to_string(),
        ));
    }
    let (mut os, report) = store.update(|os| {
        let mut report = crate::RuntimeReport::default();
        if let Some(seconds) = request.recover_stale_seconds {
            report.recovered_tasks =
                Runtime::recover_stale_tasks(os, chrono::Duration::seconds(seconds));
        }
        let tick_report = Runtime::tick(os, request.limit);
        report.assignments = tick_report.assignments;
        report.completed_tasks = tick_report.completed_tasks;
        report.expired_agents = tick_report.expired_agents;
        report.notes.extend(tick_report.notes);
        Ok::<_, StoreError>((os.clone(), report))
    })?;
    let assignments = report.assignments.clone();

    let mut runs = Vec::new();
    let mut errors = Vec::new();
    if request.execute {
        let task_ids = assignments
            .iter()
            .map(|assignment| assignment.task_id.clone())
            .collect::<Vec<_>>();
        for result in CommandExecutor::execute_tasks_parallel(&mut os, store, &task_ids) {
            match result {
                Ok(run) => runs.push(run),
                Err(error) => errors.push(error.to_string()),
            }
        }
    }

    Ok((
        "200 OK",
        json!({
            "dry_run": false,
            "scheduler": report,
            "runs": runs,
            "errors": errors,
        })
        .to_string(),
    ))
}

fn render_launchd_service_response(
    store: &Store,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json_or_default::<LaunchdServiceRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let options = LaunchdServiceOptions {
        label: request.label,
        program: request.bin_path,
        interval_ms: request.interval_ms,
        limit: request.limit,
        execute: request.execute,
        recover_stale_seconds: request.recover_stale_seconds,
        no_logs: request.no_logs,
        plist_path: request.plist_path,
    };
    match build_launchd_service_definition(store.path(), options) {
        Ok((service, plist_path)) => Ok((
            "200 OK",
            json!({
                "platform": "launchd",
                "service": service,
                "plist": service.render_plist(),
                "plist_path": plist_path,
            })
            .to_string(),
        )),
        Err(error) => Ok(service_error_response(error)),
    }
}

fn install_launchd_service_response(
    store: &Store,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json_or_default::<LaunchdServiceRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let options = LaunchdServiceOptions {
        label: request.label,
        program: request.bin_path,
        interval_ms: request.interval_ms,
        limit: request.limit,
        execute: request.execute,
        recover_stale_seconds: request.recover_stale_seconds,
        no_logs: request.no_logs,
        plist_path: request.plist_path,
    };
    let installation = match install_launchd_service_definition(store.path(), options) {
        Ok(installation) => installation,
        Err(error) => return Ok(service_error_response(error)),
    };
    Ok((
        "200 OK",
        json!({
            "platform": "launchd",
            "installed": installation.installed,
            "plist_path": installation.plist_path,
            "service": installation.service,
        })
        .to_string(),
    ))
}

fn uninstall_launchd_service_response(body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json_or_default::<UninstallLaunchdServiceRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let removal = match uninstall_launchd_service(&request.label, request.plist_path) {
        Ok(removal) => removal,
        Err(error) => return Ok(service_error_response(error)),
    };
    Ok((
        "200 OK",
        json!({
            "platform": "launchd",
            "removed": removal.removed,
            "plist_path": removal.plist_path,
        })
        .to_string(),
    ))
}

fn start_launchd_service_response(body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_launchd_control_request(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let label = request.label.clone();
    let plist_path = request
        .plist_path
        .clone()
        .unwrap_or_else(|| default_launchd_plist_path(&request.label));
    let domain = match resolve_launchd_domain(request.domain) {
        Ok(domain) => domain,
        Err(error) => return Ok(service_error_response(error)),
    };
    if !plist_path.exists() {
        return Ok((
            "409 Conflict",
            json!({
                "error": "service conflict",
                "detail": format!("launchd plist does not exist at {}; install service first", plist_path.display()),
            })
            .to_string(),
        ));
    }
    let output = match run_launchctl(
        &request.launchctl_path,
        &["bootstrap", &domain, &plist_path.display().to_string()],
    ) {
        Ok(output) => output,
        Err(error) => return Ok(service_error_response(error)),
    };
    if !output.success {
        return Ok(launchctl_failure_response(
            "bootstrap",
            &label,
            &domain,
            &output,
        ));
    }
    Ok((
        "200 OK",
        json!({
            "platform": "launchd",
            "started": true,
            "label": label,
            "domain": domain,
            "plist_path": plist_path,
            "launchctl": output,
        })
        .to_string(),
    ))
}

fn stop_launchd_service_response(body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_launchd_control_request(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let label = request.label.clone();
    let plist_path = request
        .plist_path
        .clone()
        .unwrap_or_else(|| default_launchd_plist_path(&request.label));
    let domain = match resolve_launchd_domain(request.domain) {
        Ok(domain) => domain,
        Err(error) => return Ok(service_error_response(error)),
    };
    let output = match run_launchctl(
        &request.launchctl_path,
        &["bootout", &domain, &plist_path.display().to_string()],
    ) {
        Ok(output) => output,
        Err(error) => return Ok(service_error_response(error)),
    };
    if !output.success {
        return Ok(launchctl_failure_response(
            "bootout", &label, &domain, &output,
        ));
    }
    Ok((
        "200 OK",
        json!({
            "platform": "launchd",
            "stopped": true,
            "label": label,
            "domain": domain,
            "plist_path": plist_path,
            "launchctl": output,
        })
        .to_string(),
    ))
}

fn status_launchd_service_response(body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_launchd_control_request(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let label = request.label.clone();
    let domain = match resolve_launchd_domain(request.domain) {
        Ok(domain) => domain,
        Err(error) => return Ok(service_error_response(error)),
    };
    let target = format!("{domain}/{label}");
    let output = match run_launchctl(&request.launchctl_path, &["print", &target]) {
        Ok(output) => output,
        Err(error) => return Ok(service_error_response(error)),
    };
    Ok((
        "200 OK",
        json!({
            "platform": "launchd",
            "loaded": output.success,
            "label": label,
            "domain": domain,
            "launchctl": output,
        })
        .to_string(),
    ))
}

fn parse_launchd_control_request(
    body: &[u8],
) -> Result<LaunchdServiceControlRequest, (&'static str, String)> {
    let request = parse_json_or_default::<LaunchdServiceControlRequest>(body)?;
    validate_service_control_inputs(
        &request.label,
        request.domain.as_deref(),
        request.plist_path.as_deref(),
        &request.launchctl_path,
    )
    .map_err(service_error_response)?;
    Ok(request)
}

fn launchctl_failure_response(
    action: &str,
    label: &str,
    domain: &str,
    output: &crate::service::LaunchctlCommandOutput,
) -> (&'static str, String) {
    (
        "409 Conflict",
        json!({
            "error": "service command failed",
            "detail": format!("launchctl {action} failed for {label} in {domain}: {}", output.stderr.trim()),
            "launchctl": output,
        })
        .to_string(),
    )
}

fn service_error_response(error: ServiceError) -> (&'static str, String) {
    let status = match &error {
        ServiceError::CurrentExe { .. }
        | ServiceError::Io { .. }
        | ServiceError::CommandIo { .. }
        | ServiceError::DomainIo { .. }
        | ServiceError::EmptyDomainUid
        | ServiceError::DomainCommandFailed { .. } => "500 Internal Server Error",
        ServiceError::EmptyLabel
        | ServiceError::EmptyText { .. }
        | ServiceError::EmptyPath { .. }
        | ServiceError::InvalidInterval
        | ServiceError::InvalidLimit
        | ServiceError::InvalidRecoverStaleSeconds => "400 Bad Request",
    };
    (
        status,
        json!({
            "error": if status == "400 Bad Request" {
                "invalid service request"
            } else {
                "service unavailable"
            },
            "detail": error.to_string(),
        })
        .to_string(),
    )
}

fn cancel_run_response(store: &Store, id: &str) -> Result<(&'static str, String), StoreError> {
    let id = match parse_run_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    store.update(|os| {
        let mut requested = false;
        let mut changed = false;
        let run = {
            let Some(run) = os.runs.get_mut(&id) else {
                return Ok((
                    "404 Not Found",
                    json!({ "error": "run not found" }).to_string(),
                ));
            };
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
        let status = if requested { "200 OK" } else { "409 Conflict" };
        Ok((
            status,
            json!({
                "id": id,
                "cancel_requested": requested,
                "run": run,
            })
            .to_string(),
        ))
    })
}

fn create_memory_response(
    store: &Store,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json::<CreateMemoryRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_empty_text_field("topic", &request.topic) {
        return Ok(response);
    }
    if let Some(response) = reject_empty_text_field("body", &request.body) {
        return Ok(response);
    }
    if let Some(response) = reject_invalid_tag_values("tags", &request.tags) {
        return Ok(response);
    }
    store.update(|os| {
        let record = MemoryRecord::new(request.topic, request.body, request.tags);
        let id = record.id.clone();
        os.write_memory(record);
        Ok((
            "201 Created",
            json!({
                "id": id,
                "memory": os.memory.iter().find(|record| record.id == id),
            })
            .to_string(),
        ))
    })
}

fn delete_memory_response(store: &Store, id: &str) -> Result<(&'static str, String), StoreError> {
    let id = match parse_memory_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    store.update(|os| {
        Ok(match os.remove_memory(&id) {
            Some(memory) => (
                "200 OK",
                json!({
                    "id": id,
                    "removed": true,
                    "memory": memory,
                })
                .to_string(),
            ),
            None => (
                "404 Not Found",
                json!({ "error": "memory not found" }).to_string(),
            ),
        })
    })
}

fn update_memory_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_memory_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json::<UpdateMemoryRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(topic) = &request.topic
        && let Some(response) = reject_empty_text_field("topic", topic)
    {
        return Ok(response);
    }
    if let Some(body) = &request.body
        && let Some(response) = reject_empty_text_field("body", body)
    {
        return Ok(response);
    }
    if let Some(tags) = &request.tags
        && let Some(response) = reject_invalid_tag_values("tags", tags)
    {
        return Ok(response);
    }
    if request.clear_tags && request.tags.is_some() {
        return Ok(invalid_text_response(
            "tags",
            "use either tags or clear_tags, not both",
        ));
    }
    if request.topic.is_none()
        && request.body.is_none()
        && request.tags.is_none()
        && !request.clear_tags
    {
        return Ok((
            "400 Bad Request",
            json!({ "error": "memory update must include topic, body, tags, or clear_tags" })
                .to_string(),
        ));
    }
    let tags = if request.clear_tags {
        Some(Vec::new())
    } else {
        request.tags
    };
    store.update(|os| {
        Ok(
            match os.update_memory(&id, request.topic, request.body, tags) {
                Some(memory) => (
                    "200 OK",
                    json!({
                        "id": id,
                        "memory": memory,
                    })
                    .to_string(),
                ),
                None => (
                    "404 Not Found",
                    json!({ "error": "memory not found" }).to_string(),
                ),
            },
        )
    })
}

fn parse_json<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, (&'static str, String)> {
    serde_json::from_slice(body).map_err(|error| {
        (
            "400 Bad Request",
            json!({
                "error": "invalid json",
                "detail": error.to_string(),
            })
            .to_string(),
        )
    })
}

fn parse_json_or_default<T>(body: &[u8]) -> Result<T, (&'static str, String)>
where
    T: Default + serde::de::DeserializeOwned,
{
    if body.is_empty() {
        Ok(T::default())
    } else {
        parse_json(body)
    }
}

fn parse_agent_status_request(input: &str) -> Result<AgentStatus, (&'static str, String)> {
    AgentStatus::try_parse(input)
        .ok_or_else(|| invalid_choice_response("status", input, AgentStatus::INPUT_VALUES))
}

fn parse_priority_request(input: &str) -> Result<Priority, (&'static str, String)> {
    Priority::try_parse(input)
        .ok_or_else(|| invalid_choice_response("priority", input, Priority::INPUT_VALUES))
}

fn parse_tool_kind_request(input: &str) -> Result<ToolKind, (&'static str, String)> {
    ToolKind::try_parse(input)
        .ok_or_else(|| invalid_choice_response("kind", input, ToolKind::INPUT_VALUES))
}

fn invalid_choice_response(field: &str, value: &str, expected: &[&str]) -> (&'static str, String) {
    (
        "400 Bad Request",
        json!({
            "error": format!("invalid {field}"),
            "field": field,
            "value": value,
            "expected": expected,
        })
        .to_string(),
    )
}

fn reject_invalid_agent_name(name: &str) -> Option<(&'static str, String)> {
    AgentId::new(name).as_str().is_empty().then(|| {
        invalid_text_response(
            "name",
            "agent name must contain at least one ASCII letter, digit, or hyphen",
        )
    })
}

fn reject_invalid_tool_name(name: &str) -> Option<(&'static str, String)> {
    ToolId::new(name).as_str().is_empty().then(|| {
        invalid_text_response(
            "name",
            "tool name must contain at least one ASCII letter, digit, or hyphen",
        )
    })
}

fn reject_invalid_tool_id(id: &str) -> Option<(&'static str, String)> {
    ToolId::new(id).as_str().is_empty().then(|| {
        invalid_text_response(
            "tool",
            "tool id must contain at least one ASCII letter, digit, or hyphen",
        )
    })
}

fn parse_agent_path_id(id: &str) -> Result<AgentId, (&'static str, String)> {
    let id = AgentId::new(id);
    if id.as_str().is_empty() {
        Err(invalid_text_response(
            "agent id",
            "agent id must contain at least one ASCII letter, digit, or hyphen",
        ))
    } else {
        Ok(id)
    }
}

fn parse_task_path_id(id: &str) -> Result<TaskId, (&'static str, String)> {
    let id = TaskId::from_slug(id);
    if id.as_str().is_empty() {
        Err(invalid_text_response(
            "task id",
            "task id must contain at least one ASCII letter, digit, or hyphen",
        ))
    } else {
        Ok(id)
    }
}

fn parse_memory_path_id(id: &str) -> Result<String, (&'static str, String)> {
    if !contains_slug_character(id) {
        Err(invalid_text_response(
            "memory id",
            "memory id must contain at least one ASCII letter, digit, or hyphen",
        ))
    } else {
        Ok(id.to_owned())
    }
}

fn contains_slug_character(value: &str) -> bool {
    value
        .chars()
        .any(|ch| ch.is_ascii_alphanumeric() || ch == '-')
}

fn parse_tool_path_id(id: &str) -> Result<ToolId, (&'static str, String)> {
    let id = ToolId::new(id);
    if id.as_str().is_empty() {
        Err(invalid_text_response(
            "tool id",
            "tool id must contain at least one ASCII letter, digit, or hyphen",
        ))
    } else {
        Ok(id)
    }
}

fn parse_run_path_id(id: &str) -> Result<RunId, (&'static str, String)> {
    let id = RunId::from_slug(id);
    if id.to_string().is_empty() {
        Err(invalid_text_response(
            "run id",
            "run id must contain at least one ASCII letter, digit, or hyphen",
        ))
    } else {
        Ok(id)
    }
}

fn parse_workflow_path_id(id: &str) -> Result<WorkflowId, (&'static str, String)> {
    let id = WorkflowId::from_slug(id);
    if id.as_str().is_empty() {
        Err(invalid_text_response(
            "workflow id",
            "workflow id must contain at least one ASCII letter, digit, or hyphen",
        ))
    } else {
        Ok(id)
    }
}

fn reject_invalid_task_id(field: &'static str, id: &str) -> Option<(&'static str, String)> {
    TaskId::from_slug(id).as_str().is_empty().then(|| {
        invalid_text_response(
            field,
            &format!("{field} must contain at least one ASCII letter, digit, or hyphen"),
        )
    })
}

fn reject_empty_text_field(field: &str, value: &str) -> Option<(&'static str, String)> {
    value
        .trim()
        .is_empty()
        .then(|| invalid_text_response(field, &format!("{field} must not be empty")))
}

fn reject_empty_optional_text_field(
    field: &str,
    value: Option<&str>,
) -> Option<(&'static str, String)> {
    value.and_then(|value| reject_empty_text_field(field, value))
}

fn reject_invalid_plan_steps(steps: &[String]) -> Option<(&'static str, String)> {
    if steps.is_empty() {
        return Some(invalid_text_response(
            "steps",
            "plan steps must not be empty",
        ));
    }
    steps
        .iter()
        .any(|step| step.trim().is_empty())
        .then(|| invalid_text_response("steps", "plan steps must not contain empty steps"))
}

fn reject_invalid_capability_values(
    field: &'static str,
    values: &[String],
    require_non_empty: bool,
) -> Option<(&'static str, String)> {
    if values
        .iter()
        .any(|value| value.trim().is_empty() || value.split(',').any(|part| part.trim().is_empty()))
    {
        return Some(invalid_text_response(
            field,
            &format!("{field} must not contain empty capabilities"),
        ));
    }
    if require_non_empty && crate::models::normalize_list(values.to_vec()).is_empty() {
        return Some(invalid_text_response(
            field,
            &format!("{field} must include at least one capability"),
        ));
    }
    None
}

fn reject_invalid_tag_values(
    field: &'static str,
    values: &[String],
) -> Option<(&'static str, String)> {
    values
        .iter()
        .any(|value| value.trim().is_empty() || value.split(',').any(|part| part.trim().is_empty()))
        .then(|| invalid_text_response(field, &format!("{field} must not contain empty tags")))
}

fn reject_invalid_tool_invocation_args(
    args: &BTreeMap<String, String>,
    secret_args: &BTreeMap<String, String>,
    redacted_patterns: &[String],
) -> Option<(&'static str, String)> {
    if args.keys().any(|key| key.trim().is_empty())
        || secret_args.keys().any(|key| key.trim().is_empty())
    {
        return Some(invalid_text_response(
            "args",
            "tool argument key cannot be empty",
        ));
    }
    if let Some(key) = args.keys().find(|key| secret_args.contains_key(*key)) {
        return Some(invalid_text_response(
            "args",
            &format!("tool argument `{key}` cannot be both args and secret_args"),
        ));
    }
    if let Some((key, _)) = secret_args.iter().find(|(_, env)| env.trim().is_empty()) {
        return Some(invalid_text_response(
            "secret_args",
            &format!("secret tool argument `{key}` must name an environment variable"),
        ));
    }
    if let Some((key, _)) = secret_args
        .iter()
        .find(|(_, env)| !is_valid_env_var_name(env))
    {
        return Some(invalid_text_response(
            "secret_args",
            &format!("secret tool argument `{key}` must name a valid environment variable"),
        ));
    }
    if key_matches_patterns(args, redacted_patterns) {
        return Some(invalid_text_response(
            "args",
            "secret-like tool args must use secret_args",
        ));
    }
    None
}

fn invalid_text_response(field: &str, error: &str) -> (&'static str, String) {
    (
        "400 Bad Request",
        json!({
            "error": error,
            "field": field,
        })
        .to_string(),
    )
}

fn key_matches_patterns(values: &BTreeMap<String, String>, patterns: &[String]) -> bool {
    values.keys().any(|key| {
        let key = key.to_ascii_uppercase();
        patterns.iter().any(|pattern| {
            let pattern = pattern.trim();
            !pattern.is_empty() && key.contains(&pattern.to_ascii_uppercase())
        })
    })
}

fn validate_task_dependencies(
    os: &OperatingSystem,
    dependencies: Vec<String>,
) -> Result<Vec<TaskId>, (&'static str, String)> {
    let dependencies = validate_task_dependencies_for_task(None, dependencies)?;
    for dependency in &dependencies {
        if !os.tasks.contains_key(dependency) {
            return Err((
                "400 Bad Request",
                json!({
                    "error": "dependency task not found",
                    "dependency": dependency,
                })
                .to_string(),
            ));
        }
    }
    Ok(dependencies)
}

fn validate_task_dependencies_for_task(
    task_id: Option<&TaskId>,
    dependencies: Vec<String>,
) -> Result<Vec<TaskId>, (&'static str, String)> {
    let mut seen = BTreeSet::new();
    dependencies
        .into_iter()
        .map(|dependency| {
            if let Some(response) = reject_invalid_task_id("dependency task id", &dependency) {
                return Err(response);
            }
            let id = TaskId::from_slug(dependency);
            if task_id.map(|task_id| &id == task_id).unwrap_or(false) {
                return Err((
                    "400 Bad Request",
                    json!({
                        "error": "task cannot depend on itself",
                        "dependency": id,
                    })
                    .to_string(),
                ));
            }
            if !seen.insert(id.clone()) {
                return Err((
                    "400 Bad Request",
                    json!({
                        "error": "duplicate dependency task",
                        "dependency": id,
                    })
                    .to_string(),
                ));
            }
            Ok(id)
        })
        .collect()
}

fn reject_non_positive_lease(lease_seconds: Option<i64>) -> Option<(&'static str, String)> {
    lease_seconds
        .filter(|seconds| *seconds <= 0)
        .map(|seconds| {
            (
                "400 Bad Request",
                json!({
                    "error": "lease_seconds must be greater than 0",
                    "lease_seconds": seconds,
                })
                .to_string(),
            )
        })
}

fn reject_invalid_run_request(request: &RunRequest) -> Option<(&'static str, String)> {
    if request.limit == 0 {
        return Some((
            "400 Bad Request",
            json!({
                "error": "limit must be greater than 0",
                "limit": request.limit,
            })
            .to_string(),
        ));
    }
    if request.dry_run && request.execute {
        return Some((
            "400 Bad Request",
            json!({
                "error": "dry_run cannot be combined with execute",
            })
            .to_string(),
        ));
    }
    request
        .recover_stale_seconds
        .filter(|seconds| *seconds < 0)
        .map(|seconds| {
            (
                "400 Bad Request",
                json!({
                    "error": "recover_stale_seconds must be greater than or equal to 0",
                    "recover_stale_seconds": seconds,
                })
                .to_string(),
            )
        })
}

fn default_builder_kind() -> String {
    "builder".into()
}

fn default_online_status() -> String {
    "online".into()
}

fn default_shell_kind() -> String {
    "shell".into()
}

fn default_normal_priority() -> String {
    "normal".into()
}

fn default_parallel() -> usize {
    1
}

fn default_interval_ms() -> u64 {
    1000
}

fn default_launchd_label() -> String {
    crate::service::DEFAULT_LAUNCHD_LABEL.into()
}

fn default_launchctl_path() -> PathBuf {
    PathBuf::from("launchctl")
}

fn default_keep_runs() -> usize {
    100
}

fn default_keep_events() -> usize {
    500
}

fn default_recover_stale_seconds() -> i64 {
    1800
}

pub fn openapi_schema() -> serde_json::Value {
    let mut schema = json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Agent OS Local API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Local API for Agent OS state, task orchestration, runs, logs, replay, and authenticated mutation."
        },
        "servers": [
            { "url": "http://127.0.0.1:7373" }
        ],
        "tags": [
            { "name": "system", "description": "Health, status, and local service metadata." },
            { "name": "config", "description": "Config inspection and validation." },
            { "name": "service", "description": "Supervisor service rendering." },
            { "name": "agents", "description": "Agent registration, heartbeats, and work claims." },
            { "name": "tasks", "description": "Task queue inspection and lifecycle transitions." },
            { "name": "workflows", "description": "Workflow creation and inspection." },
            { "name": "tools", "description": "Tool catalog inspection and lookup." },
            { "name": "state", "description": "State validation and repair operations." },
            { "name": "runs", "description": "Run execution, history, logs, replay, and cancellation." },
            { "name": "events", "description": "Event stream inspection." },
            { "name": "memory", "description": "Agent memory inspection." },
            { "name": "schema", "description": "OpenAPI schema discovery." }
        ],
        "security": [
            {},
            { "bearerAuth": [] }
        ],
        "paths": {
            "/health": health_endpoint(),
            "/metrics": metrics_endpoint(),
            "/status": status_endpoint(),
            "/agents": agents_endpoint(),
            "/agents/{id}": with_id_parameter(
                agent_detail_endpoint(),
                "Agent ID. Values are normalized before lookup.",
            ),
            "/agents/{id}/heartbeat": with_id_parameter(
                agent_heartbeat_endpoint(),
                "Agent ID. Values are normalized before lookup.",
            ),
            "/agents/{id}/claim": with_id_parameter(
                agent_claim_endpoint(),
                "Agent ID. Values are normalized before lookup.",
            ),
            "/tasks": tasks_endpoint(),
            "/tasks/{id}": with_id_parameter(
                task_detail_endpoint(),
                "Task ID. Values are normalized before lookup.",
            ),
            "/tasks/{id}/complete": with_id_parameter(task_lifecycle_endpoint("Complete a task"), "Task ID. Values are normalized before lookup."),
            "/tasks/{id}/fail": with_id_parameter(task_lifecycle_endpoint("Fail a task"), "Task ID. Values are normalized before lookup."),
            "/tasks/{id}/block": with_id_parameter(task_lifecycle_endpoint("Block a task"), "Task ID. Values are normalized before lookup."),
            "/tasks/{id}/cancel": with_id_parameter(task_lifecycle_endpoint("Cancel a task"), "Task ID. Values are normalized before lookup."),
            "/tasks/{id}/retry": with_id_parameter(task_lifecycle_endpoint("Retry a task by resetting it to pending"), "Task ID. Values are normalized before lookup."),
            "/tasks/{id}/unblock": with_id_parameter(task_lifecycle_endpoint("Unblock a blocked task"), "Task ID. Values are normalized before lookup."),
            "/tasks/{id}/assign": with_id_parameter(task_assign_endpoint(), "Task ID. Values are normalized before lookup."),
            "/tasks/{id}/priority": with_id_parameter(task_priority_endpoint(), "Task ID. Values are normalized before lookup."),
            "/tasks/{id}/dependencies": with_id_parameter(task_dependencies_endpoint(), "Task ID. Values are normalized before lookup."),
            "/tasks/{id}/plan": with_id_parameter(task_plan_endpoint(), "Task ID. Values are normalized before lookup."),
            "/workflows": workflows_endpoint(),
            "/workflows/{id}": with_id_parameter(
                workflow_detail_endpoint(),
                "Workflow ID. Values are normalized before lookup.",
            ),
            "/tools": tools_endpoint(),
            "/tools/{id}": with_id_parameter(
                tool_detail_endpoint(),
                "Tool ID. Values are normalized before lookup.",
            ),
            "/state/validate": state_validate_endpoint(),
            "/state/repair": repair_state_endpoint(),
            "/run": run_once_endpoint(),
            "/runs": runs_endpoint(),
            "/runs/{id}": with_id_parameter(
                run_detail_endpoint(),
                "Run ID. Values are normalized before lookup.",
            ),
            "/runs/{id}/logs": with_id_parameter(
                run_logs_endpoint(),
                "Run ID. Values are normalized before lookup.",
            ),
            "/runs/{id}/replay": with_id_parameter(
                run_replay_endpoint(),
                "Run ID. Values are normalized before lookup.",
            ),
            "/runs/{id}/cancel": with_id_parameter(
                run_cancel_endpoint(),
                "Run ID. Values are normalized before lookup.",
            ),
            "/events": events_endpoint(),
            "/memory": memory_endpoint(),
            "/memory/{id}": with_id_parameter(memory_detail_endpoint(), "Memory ID."),
            "/openapi.json": openapi_json_endpoint()
        },
        "components": {
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer"
                }
            },
            "schemas": request_schemas()
        }
    });
    if let Some(paths) = schema
        .get_mut("paths")
        .and_then(|paths| paths.as_object_mut())
    {
        paths.insert("/config".into(), config_endpoint());
        paths.insert("/config/validate".into(), config_validate_endpoint());
        paths.insert("/doctor".into(), doctor_endpoint());
        paths.insert("/init".into(), init_endpoint());
        paths.insert("/daemon".into(), daemon_endpoint());
        paths.insert("/daemon/stop".into(), daemon_stop_endpoint());
        paths.insert("/service/launchd".into(), service_launchd_endpoint());
        paths.insert(
            "/service/launchd/install".into(),
            service_launchd_install_endpoint(),
        );
        paths.insert(
            "/service/launchd/uninstall".into(),
            service_launchd_uninstall_endpoint(),
        );
        paths.insert(
            "/service/launchd/start".into(),
            service_launchd_start_endpoint(),
        );
        paths.insert(
            "/service/launchd/stop".into(),
            service_launchd_stop_endpoint(),
        );
        paths.insert(
            "/service/launchd/status".into(),
            service_launchd_status_endpoint(),
        );
        paths.insert("/state/backup".into(), state_backup_endpoint());
        paths.insert("/state/export".into(), state_export_endpoint());
        paths.insert("/state/import".into(), state_import_endpoint());
        paths.insert("/state/migrate".into(), state_migrate_endpoint());
        paths.insert("/state/prune".into(), state_prune_endpoint());
        paths.insert("/tasks/recover".into(), task_recover_endpoint());
        paths.insert(
            "/workflows/{id}/status".into(),
            with_id_parameter(
                workflow_status_endpoint(),
                "Workflow ID. Values are normalized before lookup.",
            ),
        );
        paths.insert(
            "/workflows/{id}/run".into(),
            with_id_parameter(
                workflow_run_endpoint(),
                "Workflow ID. Values are normalized before lookup.",
            ),
        );
        paths.insert(
            "/workflows/{id}/cancel".into(),
            with_id_parameter(
                workflow_cancel_endpoint(),
                "Workflow ID. Values are normalized before lookup.",
            ),
        );
    }
    add_options_operations(&mut schema);
    add_operation_ids(&mut schema);
    add_operation_tags(&mut schema);
    add_common_error_responses(&mut schema);
    add_standard_response_headers(&mut schema);
    schema
}

fn add_options_operations(schema: &mut serde_json::Value) {
    let Some(paths) = schema
        .get_mut("paths")
        .and_then(|paths| paths.as_object_mut())
    else {
        return;
    };
    for endpoint in paths.values_mut() {
        let Some(endpoint) = endpoint.as_object_mut() else {
            continue;
        };
        let path_parameters = path_parameters_for_options(endpoint);
        endpoint.entry("options").or_insert_with(|| {
            let mut operation = json!({
                "summary": "CORS preflight",
                "security": [],
                "responses": {
                    "204": {
                        "description": "No content"
                    }
                }
            });
            if !path_parameters.is_empty() {
                operation["parameters"] = json!(path_parameters);
            }
            operation
        });
    }
}

fn path_parameters_for_options(
    endpoint: &serde_json::Map<String, serde_json::Value>,
) -> Vec<serde_json::Value> {
    for method in ["get", "post", "delete"] {
        let Some(parameters) = endpoint
            .get(method)
            .and_then(|operation| operation.get("parameters"))
            .and_then(|parameters| parameters.as_array())
        else {
            continue;
        };
        let path_parameters = parameters
            .iter()
            .filter(|parameter| {
                parameter.get("in").and_then(|value| value.as_str()) == Some("path")
            })
            .cloned()
            .collect::<Vec<_>>();
        if !path_parameters.is_empty() {
            return path_parameters;
        }
    }
    Vec::new()
}

fn add_operation_ids(schema: &mut serde_json::Value) {
    let Some(paths) = schema
        .get_mut("paths")
        .and_then(|paths| paths.as_object_mut())
    else {
        return;
    };
    for (path, endpoint) in paths {
        let Some(endpoint) = endpoint.as_object_mut() else {
            continue;
        };
        for method in ["get", "post", "delete", "options"] {
            let Some(operation) = endpoint.get_mut(method) else {
                continue;
            };
            operation["operationId"] = json!(operation_id(method, path));
        }
    }
}

fn operation_id(method: &str, path: &str) -> String {
    let mut id = method.to_owned();
    for segment in path.trim_matches('/').split('/') {
        let segment = segment.trim_matches(|character| character == '{' || character == '}');
        push_pascal_words(&mut id, segment);
    }
    id
}

fn push_pascal_words(id: &mut String, segment: &str) {
    let mut word = String::new();
    for character in segment.chars() {
        if character.is_ascii_alphanumeric() {
            word.push(character);
        } else if !word.is_empty() {
            push_pascal_word(id, &word);
            word.clear();
        }
    }
    if !word.is_empty() {
        push_pascal_word(id, &word);
    }
}

fn push_pascal_word(id: &mut String, word: &str) {
    let mut characters = word.chars();
    let Some(first) = characters.next() else {
        return;
    };
    id.push(first.to_ascii_uppercase());
    for character in characters {
        id.push(character.to_ascii_lowercase());
    }
}

fn add_operation_tags(schema: &mut serde_json::Value) {
    let Some(paths) = schema
        .get_mut("paths")
        .and_then(|paths| paths.as_object_mut())
    else {
        return;
    };
    for (path, endpoint) in paths {
        let tag = operation_tag(path);
        let Some(endpoint) = endpoint.as_object_mut() else {
            continue;
        };
        for method in ["get", "post", "delete", "options"] {
            let Some(operation) = endpoint.get_mut(method) else {
                continue;
            };
            operation["tags"] = json!([tag]);
        }
    }
}

fn operation_tag(path: &str) -> &'static str {
    let segment = path.trim_start_matches('/').split('/').next().unwrap_or("");
    match segment {
        "health" | "doctor" | "metrics" | "status" => "system",
        "config" => "config",
        "service" => "service",
        "agents" => "agents",
        "tasks" => "tasks",
        "workflows" => "workflows",
        "tools" => "tools",
        "state" => "state",
        "run" | "runs" => "runs",
        "events" => "events",
        "memory" => "memory",
        "openapi.json" => "schema",
        _ => "system",
    }
}

fn add_common_error_responses(schema: &mut serde_json::Value) {
    let Some(paths) = schema
        .get_mut("paths")
        .and_then(|paths| paths.as_object_mut())
    else {
        return;
    };
    for endpoint in paths.values_mut() {
        let Some(endpoint) = endpoint.as_object_mut() else {
            continue;
        };
        for method in ["get", "post", "delete"] {
            let Some(operation) = endpoint.get_mut(method) else {
                continue;
            };
            add_response_if_missing(operation, "401", "Authentication required");
            add_response_if_missing(operation, "405", "Method not allowed");
            add_response_if_missing(operation, "431", "Request headers too large");
            add_response_if_missing(operation, "500", "Server error");
            if matches!(method, "post" | "delete") {
                add_response_if_missing(operation, "415", "Unsupported media type");
            }
            if method == "post" {
                add_response_if_missing(operation, "413", "Request body too large");
            }
        }
    }
}

fn add_standard_response_headers(schema: &mut serde_json::Value) {
    let Some(paths) = schema
        .get_mut("paths")
        .and_then(|paths| paths.as_object_mut())
    else {
        return;
    };
    for endpoint in paths.values_mut() {
        let Some(endpoint) = endpoint.as_object_mut() else {
            continue;
        };
        for method in ["get", "post", "delete", "options"] {
            let Some(responses) = endpoint
                .get_mut(method)
                .and_then(|operation| operation.get_mut("responses"))
                .and_then(|responses| responses.as_object_mut())
            else {
                continue;
            };
            for response in responses.values_mut() {
                if let Some(response) = response.as_object_mut() {
                    let standard_headers = standard_response_headers();
                    let headers = response
                        .entry("headers".to_owned())
                        .or_insert_with(|| json!({}));
                    if let (Some(headers), Some(standard_headers)) =
                        (headers.as_object_mut(), standard_headers.as_object())
                    {
                        for (name, schema) in standard_headers {
                            headers
                                .entry(name.clone())
                                .or_insert_with(|| schema.clone());
                        }
                    }
                }
            }
        }
    }
}

fn standard_response_headers() -> serde_json::Value {
    json!({
        "Allow": {
            "description": "HTTP methods supported by the local API listener.",
            "schema": {
                "type": "string",
                "example": "GET, POST, DELETE, OPTIONS"
            }
        },
        "Cache-Control": {
            "description": "Cache policy for local API responses that may include operational state or logs.",
            "schema": {
                "type": "string",
                "example": "no-store"
            }
        },
        "X-Content-Type-Options": {
            "description": "Browser content sniffing protection for JSON API responses.",
            "schema": {
                "type": "string",
                "example": "nosniff"
            }
        },
        "Access-Control-Allow-Origin": {
            "description": "CORS origin policy for browser clients.",
            "schema": {
                "type": "string",
                "example": "*"
            }
        },
        "Access-Control-Allow-Methods": {
            "description": "CORS methods allowed by the local API listener.",
            "schema": {
                "type": "string",
                "example": "GET, POST, DELETE, OPTIONS"
            }
        },
        "Access-Control-Allow-Headers": {
            "description": "CORS request headers allowed by the local API listener.",
            "schema": {
                "type": "string",
                "example": "authorization, content-type"
            }
        }
    })
}

fn request_schemas() -> serde_json::Value {
    let mut schemas = match json!({
        "CreateAgentRequest": {
            "type": "object",
            "required": ["name", "capabilities"],
            "properties": {
                "name": id_source_string("Agent display name. Normalizes to a unique ID."),
                "kind": non_empty_string_with_default("builder"),
                "model": nullable_non_empty_string(),
                "capabilities": required_capability_array(),
                "parallel": positive_integer_with_default(1)
            },
            "additionalProperties": false
        },
        "CreateTaskRequest": {
            "type": "object",
            "required": ["title"],
            "dependentRequired": {
                "args": ["tool"],
                "secret_args": ["tool"]
            },
            "not": {
                "required": ["command", "tool"]
            },
            "properties": {
                "title": non_empty_string("Task title."),
                "objective": non_empty_string("Task objective."),
                "command": non_empty_string("Shell command to execute."),
                "cwd": non_empty_string("Working directory override."),
                "tool": id_source_string("Registered tool ID to invoke."),
                "args": string_map("Plain tool arguments. Secret-like keys are rejected here."),
                "secret_args": env_var_map("Tool argument keys mapped to environment variable names."),
                "priority": priority_input_schema(),
                "required_capabilities": capability_array(),
                "dependencies": task_id_array("Task IDs that must complete before this task is ready. Empty or malformed IDs are rejected.")
            },
            "additionalProperties": false
        },
        "CreateToolRequest": {
            "type": "object",
            "required": ["name", "command_template"],
            "properties": {
                "name": id_source_string("Tool display name. Normalizes to a unique ID."),
                "kind": tool_kind_input_schema(),
                "description": { "type": "string" },
                "required_capabilities": capability_array(),
                "command_template": non_empty_string("Command or path template."),
                "cwd": non_empty_string("Default working directory for this tool.")
            },
            "additionalProperties": false
        },
        "CreateMemoryRequest": {
            "type": "object",
            "required": ["topic", "body"],
            "properties": {
                "topic": non_empty_string("Memory topic."),
                "body": non_empty_string("Memory body."),
                "tags": string_array("Search tags. Empty entries are rejected.")
            },
            "additionalProperties": false
        },
        "FinishTaskRequest": {
            "type": "object",
            "properties": {
                "note": non_empty_string("Optional lifecycle note.")
            },
            "additionalProperties": false
        },
        "PlanTaskRequest": {
            "type": "object",
            "required": ["steps"],
            "properties": {
                "steps": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "string",
                        "minLength": 1,
                        "pattern": ".*\\S.*"
                    }
                }
            },
            "additionalProperties": false
        },
        "HeartbeatRequest": {
            "type": "object",
            "properties": {
                "status": agent_status_input_schema(),
                "lease_seconds": positive_integer()
            },
            "additionalProperties": false
        },
        "ClaimTaskRequest": {
            "type": "object",
            "properties": {
                "lease_seconds": positive_integer()
            },
            "additionalProperties": false
        },
        "RunRequest": {
            "type": "object",
            "allOf": [
                true_flags_conflict("dry_run", "execute")
            ],
            "properties": {
                "limit": positive_integer_with_default(1),
                "execute": { "type": "boolean", "default": false },
                "dry_run": { "type": "boolean", "default": false },
                "recover_stale_seconds": { "type": "integer", "minimum": 0 }
            },
            "additionalProperties": false
        },
        "RepairStateRequest": {
            "type": "object",
            "properties": {
                "dry_run": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        },
        "ValidationReport": {
            "type": "object",
            "required": ["valid", "issues"],
            "properties": {
                "valid": { "type": "boolean" },
                "issues": {
                    "type": "array",
                    "items": { "type": "string" }
                }
            },
            "additionalProperties": false
        },
        "HealthResponse": health_response_schema(),
        "MetricsResponse": metrics_response_schema(),
        "StatusResponse": status_response_schema(),
        "OpenApiDocument": openapi_document_schema(),
        "ErrorResponse": error_response_schema(),
        "RepairReport": {
            "type": "object",
            "required": ["changed", "persisted", "repairs", "validation"],
            "properties": {
                "changed": { "type": "boolean" },
                "persisted": { "type": "boolean" },
                "repairs": {
                    "type": "array",
                    "items": { "type": "string" }
                },
                "validation": schema_ref("ValidationReport")
            },
            "additionalProperties": false
        },
        "RepairStateResponse": {
            "type": "object",
            "required": ["dry_run", "repair"],
            "properties": {
                "dry_run": { "type": "boolean" },
                "repair": schema_ref("RepairReport")
            },
            "additionalProperties": false
        },
        "AgentMutationResponse": {
            "type": "object",
            "required": ["id", "agent"],
            "properties": {
                "id": slug_string_schema(),
                "agent": schema_ref("Agent")
            },
            "additionalProperties": false
        },
        "AgentDeleteResponse": {
            "type": "object",
            "required": ["id", "removed", "agent"],
            "properties": {
                "id": slug_string_schema(),
                "removed": { "type": "boolean" },
                "agent": schema_ref("Agent")
            },
            "additionalProperties": false
        },
        "TaskMutationResponse": {
            "type": "object",
            "required": ["id", "task"],
            "properties": {
                "id": slug_string_schema(),
                "task": schema_ref("Task")
            },
            "additionalProperties": false
        },
        "TaskDeleteResponse": {
            "type": "object",
            "required": ["id", "deleted"],
            "properties": {
                "id": slug_string_schema(),
                "deleted": { "type": "boolean" }
            },
            "additionalProperties": false
        },
        "ToolMutationResponse": {
            "type": "object",
            "required": ["id", "tool"],
            "properties": {
                "id": slug_string_schema(),
                "tool": schema_ref("ToolDefinition")
            },
            "additionalProperties": false
        },
        "ToolDeleteResponse": {
            "type": "object",
            "required": ["id", "removed", "tool"],
            "properties": {
                "id": slug_string_schema(),
                "removed": { "type": "boolean" },
                "tool": schema_ref("ToolDefinition")
            },
            "additionalProperties": false
        },
        "MemoryMutationResponse": {
            "type": "object",
            "required": ["id", "memory"],
            "properties": {
                "id": slug_string_schema(),
                "memory": schema_ref("MemoryRecord")
            },
            "additionalProperties": false
        },
        "MemoryDeleteResponse": {
            "type": "object",
            "required": ["id", "removed", "memory"],
            "properties": {
                "id": slug_string_schema(),
                "removed": { "type": "boolean" },
                "memory": schema_ref("MemoryRecord")
            },
            "additionalProperties": false
        },
        "AgentClaimResponse": {
            "type": "object",
            "required": ["claimed", "assignment", "task"],
            "properties": {
                "claimed": { "type": "boolean" },
                "assignment": nullable_schema(schema_ref("Assignment")),
                "task": nullable_schema(schema_ref("Task"))
            },
            "additionalProperties": false
        },
        "Assignment": {
            "type": "object",
            "required": ["task_id", "agent_id", "reason"],
            "properties": {
                "task_id": slug_string_schema(),
                "agent_id": slug_string_schema(),
                "reason": persisted_non_empty_string_schema()
            },
            "additionalProperties": false
        },
        "RuntimeReport": runtime_report_schema(),
        "RunRecord": run_record_schema(),
        "RunOnceResponse": run_once_response_schema(),
        "RunLogsResponse": run_logs_response_schema(),
        "RunReplayResponse": run_replay_response_schema(),
        "RunCancelResponse": {
            "type": "object",
            "required": ["id", "cancel_requested", "run"],
            "properties": {
                "id": slug_string_schema(),
                "cancel_requested": { "type": "boolean" },
                "run": schema_ref("RunRecord")
            },
            "additionalProperties": false
        }
    }) {
        serde_json::Value::Object(schemas) => schemas,
        _ => serde_json::Map::new(),
    };
    add_agent_update_schemas(&mut schemas);
    add_task_update_schemas(&mut schemas);
    add_workflow_schemas(&mut schemas);
    add_task_assignment_schemas(&mut schemas);
    add_tool_update_schemas(&mut schemas);
    add_memory_update_schemas(&mut schemas);
    add_daemon_schemas(&mut schemas);
    add_service_schemas(&mut schemas);
    add_doctor_schemas(&mut schemas);
    add_init_schemas(&mut schemas);
    add_prune_schemas(&mut schemas);
    add_backup_schemas(&mut schemas);
    add_export_schemas(&mut schemas);
    add_import_schemas(&mut schemas);
    add_migration_schemas(&mut schemas);
    add_config_schemas(&mut schemas);
    add_recovery_schemas(&mut schemas);
    add_entity_schemas(&mut schemas);
    serde_json::Value::Object(schemas)
}

fn add_backup_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "BackupStateRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "output": non_empty_string("Optional backup destination path."),
                "dry_run": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "BackupStateResponse".into(),
        json!({
            "type": "object",
            "required": ["dry_run", "backup"],
            "properties": {
                "dry_run": { "type": "boolean" },
                "backup": { "type": "string" }
            },
            "additionalProperties": false
        }),
    );
}

fn add_export_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "ExportStateRequest".into(),
        json!({
            "type": "object",
            "required": ["output"],
            "properties": {
                "output": non_empty_string("Export destination path."),
                "dry_run": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "ExportStateResponse".into(),
        json!({
            "type": "object",
            "required": ["dry_run", "exported"],
            "properties": {
                "dry_run": { "type": "boolean" },
                "exported": { "type": "string" }
            },
            "additionalProperties": false
        }),
    );
}

fn add_import_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "ImportStateRequest".into(),
        json!({
            "type": "object",
            "required": ["path"],
            "properties": {
                "path": non_empty_string("State JSON file to import."),
                "force": { "type": "boolean", "default": false },
                "dry_run": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "ImportStateResponse".into(),
        json!({
            "type": "object",
            "required": ["dry_run", "imported", "state_path", "validation"],
            "properties": {
                "dry_run": { "type": "boolean" },
                "imported": { "type": "string" },
                "state_path": { "type": "string" },
                "validation": schema_ref("ValidationReport")
            },
            "additionalProperties": false
        }),
    );
}

fn add_migration_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "MigrateStateRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "input": non_empty_string("Optional source state JSON path. Defaults to the active state path."),
                "output": non_empty_string("Optional migrated state JSON path. Defaults to the active state path."),
                "dry_run": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "MigrationReport".into(),
        json!({
            "type": "object",
            "required": ["from_version", "to_version", "changed", "steps"],
            "properties": {
                "from_version": non_negative_integer(),
                "to_version": non_negative_integer(),
                "changed": { "type": "boolean" },
                "steps": string_list_schema()
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "MigrateStateResponse".into(),
        json!({
            "type": "object",
            "required": ["dry_run", "input", "output", "migration", "validation"],
            "properties": {
                "dry_run": { "type": "boolean" },
                "input": { "type": "string" },
                "output": { "type": "string" },
                "migration": schema_ref("MigrationReport"),
                "validation": schema_ref("ValidationReport")
            },
            "additionalProperties": false
        }),
    );
}

fn add_recovery_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "RecoverTasksRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "older_than_seconds": { "type": "integer", "minimum": 0, "default": 1800 }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "RecoverTasksResponse".into(),
        json!({
            "type": "object",
            "required": ["older_than_seconds", "recovered"],
            "properties": {
                "older_than_seconds": { "type": "integer", "minimum": 0 },
                "recovered": slug_list_schema()
            },
            "additionalProperties": false
        }),
    );
}

fn add_prune_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "PruneStateRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "keep_runs": { "type": "integer", "minimum": 0, "default": 100 },
                "keep_events": { "type": "integer", "minimum": 0, "default": 500 },
                "dry_run": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "PruneReport".into(),
        json!({
            "type": "object",
            "required": ["dry_run", "removed_runs", "removed_log_paths", "removed_events"],
            "properties": {
                "dry_run": { "type": "boolean" },
                "removed_runs": slug_list_schema(),
                "removed_log_paths": string_list_schema(),
                "removed_events": non_negative_integer()
            },
            "additionalProperties": false
        }),
    );
}

fn add_daemon_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "DaemonResponse".into(),
        json!({
            "type": "object",
            "required": ["daemon"],
            "properties": {
                "daemon": nullable_schema(schema_ref("DaemonState"))
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "DaemonStopResponse".into(),
        json!({
            "type": "object",
            "required": ["stop_requested", "daemon"],
            "properties": {
                "stop_requested": { "type": "boolean" },
                "daemon": nullable_schema(schema_ref("DaemonState"))
            },
            "additionalProperties": false
        }),
    );
}

fn add_service_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "LaunchdServiceRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "label": non_empty_string_with_default(crate::service::DEFAULT_LAUNCHD_LABEL),
                "bin_path": non_empty_string("Optional agent-os binary path. Defaults to the current executable."),
                "interval_ms": { "type": "integer", "minimum": 1, "default": 1000 },
                "limit": positive_integer_with_default(1),
                "execute": { "type": "boolean", "default": false },
                "recover_stale_seconds": { "type": "integer", "minimum": 0 },
                "no_logs": { "type": "boolean", "default": false },
                "plist_path": non_empty_string("Optional LaunchAgent plist path. Defaults from label.")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "LaunchdService".into(),
        json!({
            "type": "object",
            "required": [
                "label",
                "program",
                "state_path",
                "interval_ms",
                "limit",
                "execute",
                "recover_stale_seconds",
                "stdout_path",
                "stderr_path"
            ],
            "properties": {
                "label": { "type": "string" },
                "program": { "type": "string" },
                "state_path": { "type": "string" },
                "interval_ms": { "type": "integer", "minimum": 1 },
                "limit": positive_integer(),
                "execute": { "type": "boolean" },
                "recover_stale_seconds": { "type": ["integer", "null"], "minimum": 0 },
                "stdout_path": { "type": ["string", "null"] },
                "stderr_path": { "type": ["string", "null"] }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "LaunchdServiceResponse".into(),
        json!({
            "type": "object",
            "required": ["platform", "service", "plist", "plist_path"],
            "properties": {
                "platform": { "type": "string", "enum": ["launchd"] },
                "service": schema_ref("LaunchdService"),
                "plist": { "type": "string" },
                "plist_path": { "type": "string" }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "InstallLaunchdServiceResponse".into(),
        json!({
            "type": "object",
            "required": ["platform", "installed", "plist_path", "service"],
            "properties": {
                "platform": { "type": "string", "enum": ["launchd"] },
                "installed": { "type": "boolean" },
                "plist_path": { "type": "string" },
                "service": schema_ref("LaunchdService")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "UninstallLaunchdServiceRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "label": non_empty_string_with_default(crate::service::DEFAULT_LAUNCHD_LABEL),
                "plist_path": non_empty_string("Optional LaunchAgent plist path. Defaults from label.")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "UninstallLaunchdServiceResponse".into(),
        json!({
            "type": "object",
            "required": ["platform", "removed", "plist_path"],
            "properties": {
                "platform": { "type": "string", "enum": ["launchd"] },
                "removed": { "type": "boolean" },
                "plist_path": { "type": "string" }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "LaunchdServiceControlRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "label": non_empty_string_with_default(crate::service::DEFAULT_LAUNCHD_LABEL),
                "plist_path": non_empty_string("Optional LaunchAgent plist path. Defaults from label."),
                "domain": non_empty_string("Optional launchd domain. Defaults to gui/$(id -u)."),
                "launchctl_path": non_empty_string_with_default("launchctl")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "LaunchctlCommandOutput".into(),
        json!({
            "type": "object",
            "required": ["success", "status", "stdout", "stderr"],
            "properties": {
                "success": { "type": "boolean" },
                "status": { "type": ["integer", "null"] },
                "stdout": { "type": "string" },
                "stderr": { "type": "string" }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "LaunchctlCommandErrorResponse".into(),
        json!({
            "type": "object",
            "required": ["error", "detail", "launchctl"],
            "properties": {
                "error": { "type": "string" },
                "detail": { "type": "string" },
                "launchctl": schema_ref("LaunchctlCommandOutput")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "LaunchdServiceStartResponse".into(),
        json!({
            "type": "object",
            "required": ["platform", "started", "label", "domain", "plist_path", "launchctl"],
            "properties": {
                "platform": { "type": "string", "enum": ["launchd"] },
                "started": { "type": "boolean" },
                "label": { "type": "string" },
                "domain": { "type": "string" },
                "plist_path": { "type": "string" },
                "launchctl": schema_ref("LaunchctlCommandOutput")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "LaunchdServiceStopResponse".into(),
        json!({
            "type": "object",
            "required": ["platform", "stopped", "label", "domain", "plist_path", "launchctl"],
            "properties": {
                "platform": { "type": "string", "enum": ["launchd"] },
                "stopped": { "type": "boolean" },
                "label": { "type": "string" },
                "domain": { "type": "string" },
                "plist_path": { "type": "string" },
                "launchctl": schema_ref("LaunchctlCommandOutput")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "LaunchdServiceStatusResponse".into(),
        json!({
            "type": "object",
            "required": ["platform", "loaded", "label", "domain", "launchctl"],
            "properties": {
                "platform": { "type": "string", "enum": ["launchd"] },
                "loaded": { "type": "boolean" },
                "label": { "type": "string" },
                "domain": { "type": "string" },
                "launchctl": schema_ref("LaunchctlCommandOutput")
            },
            "additionalProperties": false
        }),
    );
}

fn add_doctor_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert("DoctorResponse".into(), doctor_response_schema());
}

fn add_init_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "InitRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "name": non_empty_string("Optional OS name override."),
                "force": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "InitResponse".into(),
        json!({
            "type": "object",
            "required": ["state_path", "os"],
            "properties": {
                "state_path": { "type": "string" },
                "os": schema_ref("OperatingSystem")
            },
            "additionalProperties": false
        }),
    );
}

fn add_agent_update_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "UpdateAgentRequest".into(),
        json!({
            "type": "object",
            "anyOf": update_requires_any_of(
                &["name", "kind", "model", "capabilities", "parallel"],
                &["clear_model"],
            ),
            "allOf": [
                true_flag_conflicts_with_field("clear_model", "model")
            ],
            "properties": {
                "name": id_source_string("Updated display name. Agent ID is preserved."),
                "kind": non_empty_string("Updated agent kind."),
                "model": non_empty_string("Updated model override."),
                "clear_model": { "type": "boolean", "default": false },
                "capabilities": required_capability_array(),
                "parallel": positive_integer()
            },
            "additionalProperties": false
        }),
    );
}

fn add_task_update_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "UpdateTaskRequest".into(),
        json!({
            "type": "object",
            "anyOf": update_requires_any_of(
                &[
                    "title",
                    "objective",
                    "command",
                    "tool",
                    "args",
                    "secret_args",
                    "cwd",
                    "required_capabilities",
                ],
                &[
                    "clear_command",
                    "clear_tool",
                    "clear_args",
                    "clear_secret_args",
                    "clear_cwd",
                    "clear_required_capabilities",
                ],
            ),
            "allOf": [
                true_flag_conflicts_with_field("clear_command", "command"),
                true_flag_conflicts_with_field("clear_tool", "tool"),
                true_flag_conflicts_with_field("clear_tool", "args"),
                true_flag_conflicts_with_field("clear_tool", "secret_args"),
                true_flags_conflict("clear_tool", "clear_args"),
                true_flags_conflict("clear_tool", "clear_secret_args"),
                true_flag_conflicts_with_field("clear_args", "args"),
                true_flag_conflicts_with_field("clear_secret_args", "secret_args"),
                true_flag_conflicts_with_field("clear_cwd", "cwd"),
                true_flag_conflicts_with_field(
                    "clear_required_capabilities",
                    "required_capabilities",
                )
            ],
            "properties": {
                "title": non_empty_string("Updated task title."),
                "objective": non_empty_string("Updated task objective."),
                "command": non_empty_string("Updated shell command to execute."),
                "clear_command": { "type": "boolean", "default": false },
                "tool": id_source_string("Registered tool ID to invoke."),
                "clear_tool": { "type": "boolean", "default": false },
                "args": string_map("Replacement plain tool arguments. Secret-like keys are rejected here."),
                "secret_args": env_var_map("Replacement tool argument keys mapped to environment variable names."),
                "clear_args": { "type": "boolean", "default": false },
                "clear_secret_args": { "type": "boolean", "default": false },
                "cwd": non_empty_string("Updated working directory override."),
                "clear_cwd": { "type": "boolean", "default": false },
                "required_capabilities": capability_array(),
                "clear_required_capabilities": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
    );
}

fn add_workflow_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "CreateWorkflowRequest".into(),
        json!({
            "type": "object",
            "required": ["objective"],
            "properties": {
                "objective": non_empty_string("Workflow objective."),
                "priority": priority_input_schema(),
                "execute": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkflowMutationResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "workflow", "tasks", "runs", "errors"],
            "properties": {
                "id": slug_string_schema(),
                "workflow": schema_ref("Workflow"),
                "tasks": stage_task_map_schema(),
                "runs": {
                    "type": "array",
                    "items": schema_ref("RunRecord")
                },
                "errors": {
                    "type": "array",
                    "items": { "type": "string" }
                }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkflowDeleteResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "removed", "workflow"],
            "properties": {
                "id": slug_string_schema(),
                "removed": { "type": "boolean" },
                "workflow": schema_ref("Workflow")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkflowRunResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "progress", "runs", "errors"],
            "properties": {
                "id": slug_string_schema(),
                "progress": schema_ref("WorkflowProgress"),
                "runs": {
                    "type": "array",
                    "items": schema_ref("RunRecord")
                },
                "errors": {
                    "type": "array",
                    "items": { "type": "string" }
                }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkflowCancelResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "cancelled_tasks", "progress"],
            "properties": {
                "id": slug_string_schema(),
                "cancelled_tasks": {
                    "type": "array",
                    "items": slug_string_schema()
                },
                "progress": schema_ref("WorkflowProgress")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "RunWorkflowRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "all": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
    );
}

fn add_tool_update_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "UpdateToolRequest".into(),
        json!({
            "type": "object",
            "anyOf": update_requires_any_of(
                &["kind", "description", "required_capabilities", "command_template", "cwd"],
                &["clear_description", "clear_required_capabilities", "clear_cwd"],
            ),
            "allOf": [
                true_flag_conflicts_with_field("clear_description", "description"),
                true_flag_conflicts_with_field(
                    "clear_required_capabilities",
                    "required_capabilities",
                ),
                true_flag_conflicts_with_field("clear_cwd", "cwd")
            ],
            "properties": {
                "kind": tool_kind_input_schema(),
                "description": { "type": "string" },
                "clear_description": { "type": "boolean", "default": false },
                "required_capabilities": capability_array(),
                "clear_required_capabilities": { "type": "boolean", "default": false },
                "command_template": non_empty_string("Updated command or path template."),
                "cwd": non_empty_string("Updated default working directory for this tool."),
                "clear_cwd": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
    );
}

fn add_memory_update_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "UpdateMemoryRequest".into(),
        json!({
            "type": "object",
            "anyOf": update_requires_any_of(&["topic", "body", "tags"], &["clear_tags"]),
            "allOf": [
                true_flag_conflicts_with_field("clear_tags", "tags")
            ],
            "properties": {
                "topic": non_empty_string("Updated memory topic."),
                "body": non_empty_string("Updated memory body."),
                "tags": string_array("Replacement search tags."),
                "clear_tags": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
    );
}

fn add_task_assignment_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "AssignTaskRequest".into(),
        json!({
            "type": "object",
            "required": ["agent"],
            "properties": {
                "agent": non_empty_string("Agent ID to assign the task to.")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "TaskAssignmentResponse".into(),
        json!({
            "type": "object",
            "required": ["assignment", "task", "agent"],
            "properties": {
                "assignment": schema_ref("Assignment"),
                "task": schema_ref("Task"),
                "agent": schema_ref("Agent")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "TaskPriorityRequest".into(),
        json!({
            "type": "object",
            "required": ["priority"],
            "properties": {
                "priority": priority_input_schema()
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "TaskDependenciesRequest".into(),
        json!({
            "type": "object",
            "required": ["dependencies"],
            "properties": {
                "dependencies": task_id_array("Replacement dependency task IDs. Empty array clears dependencies.")
            },
            "additionalProperties": false
        }),
    );
}

fn add_entity_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert("OperatingSystem".into(), operating_system_schema());
    schemas.insert("Agent".into(), agent_schema());
    schemas.insert("Task".into(), task_schema());
    schemas.insert("Workflow".into(), workflow_schema());
    schemas.insert("WorkflowProgress".into(), workflow_progress_schema());
    schemas.insert(
        "WorkflowStageProgress".into(),
        workflow_stage_progress_schema(),
    );
    schemas.insert("ToolDefinition".into(), tool_definition_schema());
    schemas.insert("ToolInvocation".into(), tool_invocation_schema());
    schemas.insert("MemoryRecord".into(), memory_record_schema());
    schemas.insert("Event".into(), event_schema());
    schemas.insert("DaemonState".into(), daemon_state_schema());
}

fn schema_ref(name: &str) -> serde_json::Value {
    json!({ "$ref": format!("#/components/schemas/{name}") })
}

fn nullable_schema(schema: serde_json::Value) -> serde_json::Value {
    json!({ "anyOf": [schema, { "type": "null" }] })
}

fn date_time_schema() -> serde_json::Value {
    json!({ "type": "string", "format": "date-time" })
}

fn nullable_date_time_schema() -> serde_json::Value {
    nullable_schema(date_time_schema())
}

const SLUG_PATTERN: &str = r"^[a-z0-9]+(?:-[a-z0-9]+)*$";
const NORMALIZED_LIST_ENTRY_PATTERN: &str = r"^(?!\s)(?!.*\s$)(?!.*,)[^A-Z]+$";

fn slug_string_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "minLength": 1,
        "pattern": SLUG_PATTERN
    })
}

fn nullable_slug_string_schema() -> serde_json::Value {
    nullable_schema(slug_string_schema())
}

fn slug_list_schema() -> serde_json::Value {
    json!({
        "type": "array",
        "items": slug_string_schema()
    })
}

fn slug_keyed_map_schema(value_schema: serde_json::Value) -> serde_json::Value {
    json!({
        "type": "object",
        "propertyNames": slug_string_schema(),
        "additionalProperties": value_schema
    })
}

fn stage_task_map_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "propertyNames": {
            "type": "string",
            "minLength": 1,
            "pattern": ".*\\S.*"
        },
        "additionalProperties": slug_string_schema()
    })
}

fn normalized_entry_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "minLength": 1,
        "pattern": NORMALIZED_LIST_ENTRY_PATTERN
    })
}

fn normalized_list_schema() -> serde_json::Value {
    json!({
        "type": "array",
        "items": normalized_entry_schema()
    })
}

fn non_empty_normalized_list_schema() -> serde_json::Value {
    let mut schema = normalized_list_schema();
    schema["minItems"] = json!(1);
    schema
}

fn persisted_non_empty_string_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "minLength": 1,
        "pattern": ".*\\S.*"
    })
}

fn nullable_persisted_non_empty_string_schema() -> serde_json::Value {
    nullable_schema(persisted_non_empty_string_schema())
}

fn persisted_non_empty_string_list_schema() -> serde_json::Value {
    json!({
        "type": "array",
        "items": persisted_non_empty_string_schema()
    })
}

fn string_list_schema() -> serde_json::Value {
    json!({
        "type": "array",
        "items": { "type": "string" }
    })
}

fn non_empty_string_list_schema() -> serde_json::Value {
    json!({
        "type": "array",
        "items": {
            "type": "string",
            "minLength": 1,
            "pattern": ".*\\S.*"
        }
    })
}

fn env_var_list_schema() -> serde_json::Value {
    json!({
        "type": "array",
        "items": {
            "type": "string",
            "pattern": r"^[A-Za-z_][A-Za-z0-9_]*$"
        }
    })
}

fn operating_system_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "version",
            "name",
            "agents",
            "tasks",
            "workflows",
            "runs",
            "policy",
            "provider",
            "daemon",
            "tools",
            "memory",
            "events",
            "created_at",
            "updated_at"
        ],
        "properties": {
            "version": {
                "type": "integer",
                "minimum": CURRENT_STATE_VERSION,
                "maximum": CURRENT_STATE_VERSION
            },
            "name": persisted_non_empty_string_schema(),
            "agents": slug_keyed_map_schema(schema_ref("Agent")),
            "tasks": slug_keyed_map_schema(schema_ref("Task")),
            "workflows": slug_keyed_map_schema(schema_ref("Workflow")),
            "runs": slug_keyed_map_schema(schema_ref("RunRecord")),
            "policy": schema_ref("Policy"),
            "provider": schema_ref("ProviderSettings"),
            "daemon": nullable_schema(schema_ref("DaemonState")),
            "tools": slug_keyed_map_schema(schema_ref("ToolDefinition")),
            "memory": {
                "type": "array",
                "items": schema_ref("MemoryRecord")
            },
            "events": {
                "type": "array",
                "maxItems": 500,
                "items": schema_ref("Event")
            },
            "created_at": date_time_schema(),
            "updated_at": date_time_schema()
        },
        "additionalProperties": false
    })
}

fn agent_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "id",
            "name",
            "kind",
            "model",
            "capabilities",
            "max_parallel_tasks",
            "status",
            "last_heartbeat_at",
            "lease_expires_at",
            "current_tasks",
            "created_at",
            "updated_at"
        ],
        "properties": {
            "id": slug_string_schema(),
            "name": persisted_non_empty_string_schema(),
            "kind": persisted_non_empty_string_schema(),
            "model": nullable_persisted_non_empty_string_schema(),
            "capabilities": non_empty_normalized_list_schema(),
            "max_parallel_tasks": { "type": "integer", "minimum": 1 },
            "status": agent_status_schema(),
            "last_heartbeat_at": nullable_date_time_schema(),
            "lease_expires_at": nullable_date_time_schema(),
            "current_tasks": slug_list_schema(),
            "created_at": date_time_schema(),
            "updated_at": date_time_schema()
        },
        "additionalProperties": false
    })
}

fn task_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "id",
            "title",
            "objective",
            "priority",
            "required_capabilities",
            "dependencies",
            "command",
            "cwd",
            "tool",
            "status",
            "assigned_to",
            "plan",
            "output",
            "created_at",
            "updated_at"
        ],
        "properties": {
            "id": slug_string_schema(),
            "title": persisted_non_empty_string_schema(),
            "objective": persisted_non_empty_string_schema(),
            "priority": priority_schema(),
            "required_capabilities": normalized_list_schema(),
            "dependencies": slug_list_schema(),
            "command": nullable_persisted_non_empty_string_schema(),
            "cwd": nullable_persisted_non_empty_string_schema(),
            "tool": nullable_schema(schema_ref("ToolInvocation")),
            "status": task_status_schema(),
            "assigned_to": nullable_slug_string_schema(),
            "plan": persisted_non_empty_string_list_schema(),
            "output": nullable_persisted_non_empty_string_schema(),
            "created_at": date_time_schema(),
            "updated_at": date_time_schema()
        },
        "additionalProperties": false
    })
}

fn workflow_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["id", "objective", "priority", "tasks", "created_at", "updated_at"],
        "properties": {
            "id": slug_string_schema(),
            "objective": persisted_non_empty_string_schema(),
            "priority": priority_schema(),
            "tasks": stage_task_map_schema(),
            "created_at": date_time_schema(),
            "updated_at": date_time_schema()
        },
        "additionalProperties": false
    })
}

fn workflow_progress_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "id",
            "objective",
            "priority",
            "total_tasks",
            "tasks_pending",
            "tasks_running",
            "tasks_blocked",
            "tasks_complete",
            "tasks_failed",
            "tasks_cancelled",
            "tasks_missing",
            "current_stage",
            "stages"
        ],
        "properties": {
            "id": slug_string_schema(),
            "objective": persisted_non_empty_string_schema(),
            "priority": priority_schema(),
            "total_tasks": non_negative_integer(),
            "tasks_pending": non_negative_integer(),
            "tasks_running": non_negative_integer(),
            "tasks_blocked": non_negative_integer(),
            "tasks_complete": non_negative_integer(),
            "tasks_failed": non_negative_integer(),
            "tasks_cancelled": non_negative_integer(),
            "tasks_missing": non_negative_integer(),
            "current_stage": nullable_persisted_non_empty_string_schema(),
            "stages": {
                "type": "array",
                "items": schema_ref("WorkflowStageProgress")
            }
        },
        "additionalProperties": false
    })
}

fn workflow_stage_progress_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["stage", "task_id", "title", "status"],
        "properties": {
            "stage": persisted_non_empty_string_schema(),
            "task_id": slug_string_schema(),
            "title": nullable_persisted_non_empty_string_schema(),
            "status": nullable_schema(task_status_schema())
        },
        "additionalProperties": false
    })
}

fn daemon_state_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "status",
            "pid",
            "ticks",
            "limit",
            "execute",
            "stop_requested",
            "last_tick_at",
            "last_message"
        ],
        "properties": {
            "status": daemon_status_schema(),
            "pid": { "type": ["integer", "null"], "minimum": 0 },
            "ticks": non_negative_integer(),
            "limit": { "type": "integer", "minimum": 1 },
            "execute": { "type": "boolean" },
            "stop_requested": { "type": "boolean" },
            "last_tick_at": nullable_date_time_schema(),
            "last_message": nullable_persisted_non_empty_string_schema()
        },
        "additionalProperties": false
    })
}

fn tool_definition_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "id",
            "name",
            "kind",
            "description",
            "required_capabilities",
            "command_template",
            "default_cwd",
            "created_at",
            "updated_at"
        ],
        "properties": {
            "id": slug_string_schema(),
            "name": persisted_non_empty_string_schema(),
            "kind": tool_kind_schema(),
            "description": { "type": "string" },
            "required_capabilities": normalized_list_schema(),
            "command_template": persisted_non_empty_string_schema(),
            "default_cwd": nullable_persisted_non_empty_string_schema(),
            "created_at": date_time_schema(),
            "updated_at": date_time_schema()
        },
        "additionalProperties": false
    })
}

fn tool_invocation_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["tool_id", "args", "secret_env_args"],
        "properties": {
            "tool_id": slug_string_schema(),
            "args": string_map("Plain tool arguments."),
            "secret_env_args": env_var_map("Tool argument keys mapped to environment variable names.")
        },
        "additionalProperties": false
    })
}

fn memory_record_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["id", "topic", "body", "tags", "created_at", "updated_at"],
        "properties": {
            "id": slug_string_schema(),
            "topic": persisted_non_empty_string_schema(),
            "body": persisted_non_empty_string_schema(),
            "tags": normalized_list_schema(),
            "created_at": date_time_schema(),
            "updated_at": date_time_schema()
        },
        "additionalProperties": false
    })
}

fn event_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["id", "kind", "message", "at"],
        "properties": {
            "id": slug_string_schema(),
            "kind": event_kind_schema(),
            "message": persisted_non_empty_string_schema(),
            "at": date_time_schema()
        },
        "additionalProperties": false
    })
}

fn error_response_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["error"],
        "properties": {
            "error": { "type": "string" },
            "detail": { "type": "string" },
            "field": { "type": "string" },
            "method": { "type": "string" },
            "path": { "type": "string" },
            "value": { "type": "string" },
            "id": { "type": "string" },
            "dependency": { "type": "string" },
            "parallel": { "type": "integer", "minimum": 0 },
            "lease_seconds": { "type": "integer", "maximum": 0 },
            "limit": { "type": "integer", "maximum": 0 },
            "recover_stale_seconds": { "type": "integer", "maximum": -1 },
            "tail_bytes": { "type": "integer", "maximum": 0 },
            "expected": {
                "type": "array",
                "items": { "type": "string" }
            },
            "references": {
                "type": "array",
                "items": { "type": "string" }
            }
        },
        "additionalProperties": false
    })
}

fn health_response_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "ok",
            "service",
            "agent_os_version",
            "name",
            "version",
            "state_loads",
            "state_valid",
            "state_issue_count",
            "state_issues",
            "state_error",
            "config_path",
            "config_exists",
            "config_loads",
            "config_valid",
            "config_issues",
            "config_error"
        ],
        "properties": {
            "ok": { "type": "boolean" },
            "service": { "type": "string" },
            "agent_os_version": { "type": "string" },
            "name": { "type": ["string", "null"] },
            "version": { "type": ["integer", "null"], "minimum": 0 },
            "state_loads": { "type": "boolean" },
            "state_valid": { "type": ["boolean", "null"] },
            "state_issue_count": { "type": "integer", "minimum": 0 },
            "state_issues": {
                "type": "array",
                "items": { "type": "string" }
            },
            "state_error": { "type": ["string", "null"] },
            "config_path": { "type": ["string", "null"] },
            "config_exists": { "type": "boolean" },
            "config_loads": { "type": "boolean" },
            "config_valid": { "type": ["boolean", "null"] },
            "config_issues": {
                "type": "array",
                "items": { "type": "string" }
            },
            "config_error": { "type": ["string", "null"] }
        },
        "additionalProperties": false
    })
}

fn doctor_response_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "state_path",
            "agent_os_version",
            "state_directory",
            "state_exists",
            "state_loads",
            "state_error",
            "state_valid",
            "state_issues",
            "config_path",
            "config_exists",
            "config_loads",
            "config_valid",
            "config_issues",
            "config_error"
        ],
        "properties": {
            "state_path": { "type": "string" },
            "agent_os_version": { "type": "string" },
            "state_directory": { "type": "string" },
            "state_exists": { "type": "boolean" },
            "state_loads": { "type": "boolean" },
            "state_error": { "type": ["string", "null"] },
            "state_valid": { "type": ["boolean", "null"] },
            "state_issues": {
                "type": "array",
                "items": { "type": "string" }
            },
            "config_path": { "type": ["string", "null"] },
            "config_exists": { "type": "boolean" },
            "config_loads": { "type": "boolean" },
            "config_valid": { "type": ["boolean", "null"] },
            "config_issues": {
                "type": "array",
                "items": { "type": "string" }
            },
            "config_error": { "type": ["string", "null"] }
        },
        "additionalProperties": false
    })
}

fn config_response_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["path", "exists", "config"],
        "properties": {
            "path": { "type": "string" },
            "exists": { "type": "boolean" },
            "config": schema_ref("AppConfig")
        },
        "additionalProperties": false
    })
}

fn config_validation_response_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "config_path",
            "config_exists",
            "config_loads",
            "config_valid",
            "config_issues",
            "config_error"
        ],
        "properties": {
            "config_path": { "type": ["string", "null"] },
            "config_exists": { "type": "boolean" },
            "config_loads": { "type": "boolean" },
            "config_valid": { "type": ["boolean", "null"] },
            "config_issues": {
                "type": "array",
                "items": { "type": "string" }
            },
            "config_error": { "type": ["string", "null"] }
        },
        "additionalProperties": false
    })
}

fn app_config_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["name", "policy", "provider", "agents", "tools"],
        "properties": {
            "name": non_empty_string("OS name."),
            "policy": schema_ref("Policy"),
            "provider": schema_ref("ProviderSettings"),
            "agents": {
                "type": "array",
                "items": schema_ref("AgentConfig")
            },
            "tools": {
                "type": "array",
                "items": schema_ref("ToolConfig")
            }
        },
        "additionalProperties": false
    })
}

fn policy_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "allow_shell",
            "allowed_commands",
            "allowed_workspaces",
            "denied_patterns",
            "max_output_bytes",
            "command_timeout_seconds",
            "inherit_environment",
            "allowed_env_vars",
            "redacted_env_patterns"
        ],
        "properties": {
            "allow_shell": { "type": "boolean", "default": true },
            "allowed_commands": non_empty_string_list_schema(),
            "allowed_workspaces": non_empty_string_list_schema(),
            "denied_patterns": non_empty_string_list_schema(),
            "max_output_bytes": {
                "type": "integer",
                "minimum": 1,
                "default": 131072
            },
            "command_timeout_seconds": {
                "type": "integer",
                "minimum": 1,
                "default": 300
            },
            "inherit_environment": { "type": "boolean", "default": false },
            "allowed_env_vars": env_var_list_schema(),
            "redacted_env_patterns": non_empty_string_list_schema()
        },
        "additionalProperties": false
    })
}

fn provider_settings_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["kind", "model", "endpoint", "api_key_env", "request_timeout_seconds"],
        "properties": {
            "kind": {
                "type": "string",
                "default": "mock",
                "enum": ProviderKind::INPUT_VALUES,
            },
            "model": non_empty_string("Default provider model."),
            "endpoint": {
                "type": ["string", "null"],
                "format": "uri",
                "pattern": r"^https?://.*\S.*"
            },
            "api_key_env": {
                "type": "string",
                "default": "OPENAI_API_KEY",
                "pattern": r"^[A-Za-z_][A-Za-z0-9_]*$"
            },
            "request_timeout_seconds": {
                "type": "integer",
                "minimum": 1,
                "default": 30
            }
        },
        "additionalProperties": false
    })
}

fn agent_config_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["name", "kind", "capabilities", "parallel"],
        "properties": {
            "name": id_source_string("Agent display name. Normalizes to a unique ID."),
            "kind": non_empty_string_with_default("builder"),
            "model": nullable_non_empty_string(),
            "capabilities": capability_array(),
            "parallel": positive_integer()
        },
        "additionalProperties": false
    })
}

fn tool_config_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "name",
            "kind",
            "description",
            "required_capabilities",
            "command_template",
            "default_cwd"
        ],
        "properties": {
            "name": id_source_string("Tool display name. Normalizes to a unique ID."),
            "kind": tool_kind_input_schema(),
            "description": { "type": "string" },
            "required_capabilities": capability_array(),
            "command_template": non_empty_string("Command or path template."),
            "default_cwd": nullable_non_empty_string()
        },
        "additionalProperties": false
    })
}

fn add_config_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "WriteConfigRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "force": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WriteConfigResponse".into(),
        json!({
            "type": "object",
            "required": ["path", "written", "config"],
            "properties": {
                "path": { "type": "string" },
                "written": { "type": "boolean" },
                "config": schema_ref("AppConfig")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert("ConfigResponse".into(), config_response_schema());
    schemas.insert(
        "ConfigValidationResponse".into(),
        config_validation_response_schema(),
    );
    schemas.insert("AppConfig".into(), app_config_schema());
    schemas.insert("Policy".into(), policy_schema());
    schemas.insert("ProviderSettings".into(), provider_settings_schema());
    schemas.insert("AgentConfig".into(), agent_config_schema());
    schemas.insert("ToolConfig".into(), tool_config_schema());
}

fn metrics_response_schema() -> serde_json::Value {
    let non_negative_integer = || json!({ "type": "integer", "minimum": 0 });
    json!({
        "type": "object",
        "required": [
            "ok",
            "service",
            "agent_os_version",
            "name",
            "version",
            "state_loads",
            "state_valid",
            "state_issue_count",
            "state_error",
            "agents_total",
            "agents_online",
            "agents_busy",
            "agents_paused",
            "agents_offline",
            "tasks_total",
            "tasks_pending",
            "tasks_running",
            "tasks_blocked",
            "tasks_complete",
            "tasks_failed",
            "tasks_cancelled",
            "workflows_total",
            "runs_total",
            "runs_running",
            "runs_cancel_requested",
            "runs_cancelled",
            "runs_success",
            "runs_failed",
            "runs_rejected",
            "tools_total",
            "events_total",
            "memories_total",
            "daemon_status",
            "daemon_ticks"
        ],
        "properties": {
            "ok": { "type": "boolean" },
            "service": { "type": "string" },
            "agent_os_version": { "type": "string" },
            "name": { "type": ["string", "null"] },
            "version": { "type": ["integer", "null"], "minimum": 0 },
            "state_loads": { "type": "boolean" },
            "state_valid": { "type": ["boolean", "null"] },
            "state_issue_count": non_negative_integer(),
            "state_error": { "type": ["string", "null"] },
            "agents_total": non_negative_integer(),
            "agents_online": non_negative_integer(),
            "agents_busy": non_negative_integer(),
            "agents_paused": non_negative_integer(),
            "agents_offline": non_negative_integer(),
            "tasks_total": non_negative_integer(),
            "tasks_pending": non_negative_integer(),
            "tasks_running": non_negative_integer(),
            "tasks_blocked": non_negative_integer(),
            "tasks_complete": non_negative_integer(),
            "tasks_failed": non_negative_integer(),
            "tasks_cancelled": non_negative_integer(),
            "workflows_total": non_negative_integer(),
            "runs_total": non_negative_integer(),
            "runs_running": non_negative_integer(),
            "runs_cancel_requested": non_negative_integer(),
            "runs_cancelled": non_negative_integer(),
            "runs_success": non_negative_integer(),
            "runs_failed": non_negative_integer(),
            "runs_rejected": non_negative_integer(),
            "tools_total": non_negative_integer(),
            "events_total": non_negative_integer(),
            "memories_total": non_negative_integer(),
            "daemon_status": { "type": ["string", "null"] },
            "daemon_ticks": { "type": ["integer", "null"], "minimum": 0 }
        },
        "additionalProperties": false
    })
}

fn status_response_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "name",
            "agent_os_version",
            "version",
            "agents",
            "tasks_pending",
            "tasks_running",
            "tasks_blocked",
            "tasks_complete",
            "tasks_failed",
            "tasks_cancelled",
            "workflows",
            "runs",
            "tools",
            "events",
            "memories",
            "daemon_status",
            "daemon_ticks"
        ],
        "properties": {
            "name": { "type": "string" },
            "agent_os_version": { "type": "string" },
            "version": { "type": "integer", "minimum": 0 },
            "agents": { "type": "integer", "minimum": 0 },
            "tasks_pending": { "type": "integer", "minimum": 0 },
            "tasks_running": { "type": "integer", "minimum": 0 },
            "tasks_blocked": { "type": "integer", "minimum": 0 },
            "tasks_complete": { "type": "integer", "minimum": 0 },
            "tasks_failed": { "type": "integer", "minimum": 0 },
            "tasks_cancelled": { "type": "integer", "minimum": 0 },
            "workflows": { "type": "integer", "minimum": 0 },
            "runs": { "type": "integer", "minimum": 0 },
            "tools": { "type": "integer", "minimum": 0 },
            "events": { "type": "integer", "minimum": 0 },
            "memories": { "type": "integer", "minimum": 0 },
            "daemon_status": { "type": ["string", "null"] },
            "daemon_ticks": { "type": ["integer", "null"], "minimum": 0 }
        },
        "additionalProperties": false
    })
}

fn openapi_document_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["openapi", "info", "paths", "components"],
        "properties": {
            "openapi": { "type": "string" },
            "info": {
                "type": "object",
                "required": ["title", "version"],
                "properties": {
                    "title": { "type": "string" },
                    "version": { "type": "string" },
                    "description": { "type": "string" }
                },
                "additionalProperties": true
            },
            "servers": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": true
                }
            },
            "security": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": true
                }
            },
            "paths": {
                "type": "object",
                "additionalProperties": true
            },
            "components": {
                "type": "object",
                "additionalProperties": true
            }
        },
        "additionalProperties": true
    })
}

fn runtime_report_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["assignments", "completed_tasks", "recovered_tasks", "expired_agents", "notes"],
        "properties": {
            "assignments": {
                "type": "array",
                "items": schema_ref("Assignment")
            },
            "completed_tasks": slug_list_schema(),
            "recovered_tasks": slug_list_schema(),
            "expired_agents": slug_list_schema(),
            "notes": {
                "type": "array",
                "items": { "type": "string" }
            }
        },
        "additionalProperties": false
    })
}

fn run_record_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "id",
            "task_id",
            "agent_id",
            "command",
            "cwd",
            "status",
            "exit_code",
            "log_path",
            "started_at",
            "finished_at"
        ],
        "properties": {
            "id": slug_string_schema(),
            "task_id": slug_string_schema(),
            "agent_id": nullable_slug_string_schema(),
            "command": persisted_non_empty_string_schema(),
            "cwd": persisted_non_empty_string_schema(),
            "status": run_status_schema(),
            "exit_code": { "type": ["integer", "null"] },
            "log_path": nullable_persisted_non_empty_string_schema(),
            "started_at": {
                "type": "string",
                "format": "date-time"
            },
            "finished_at": {
                "type": ["string", "null"],
                "format": "date-time"
            }
        },
        "additionalProperties": false
    })
}

fn run_once_response_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["dry_run", "scheduler", "runs", "errors"],
        "properties": {
            "dry_run": { "type": "boolean" },
            "scheduler": schema_ref("RuntimeReport"),
            "runs": {
                "type": "array",
                "items": schema_ref("RunRecord")
            },
            "errors": {
                "type": "array",
                "items": { "type": "string" }
            }
        },
        "additionalProperties": false
    })
}

fn run_logs_response_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["run_id", "log_path", "tail_bytes", "truncated", "body"],
        "properties": {
            "run_id": slug_string_schema(),
            "log_path": persisted_non_empty_string_schema(),
            "tail_bytes": { "type": ["integer", "null"], "minimum": 1 },
            "truncated": { "type": "boolean" },
            "body": { "type": "string" }
        },
        "additionalProperties": false
    })
}

fn run_replay_response_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["run", "task", "events", "log", "log_tail_bytes", "log_truncated", "log_error"],
        "properties": {
            "run": schema_ref("RunRecord"),
            "task": nullable_schema(schema_ref("Task")),
            "events": {
                "type": "array",
                "items": schema_ref("Event")
            },
            "log": { "type": ["string", "null"] },
            "log_tail_bytes": { "type": ["integer", "null"], "minimum": 1 },
            "log_truncated": { "type": "boolean" },
            "log_error": { "type": ["string", "null"] }
        },
        "additionalProperties": false
    })
}

fn non_empty_string(description: &str) -> serde_json::Value {
    json!({
        "type": "string",
        "minLength": 1,
        "pattern": ".*\\S.*",
        "description": description,
    })
}

fn id_source_string(description: &str) -> serde_json::Value {
    json!({
        "type": "string",
        "minLength": 1,
        "pattern": ".*[A-Za-z0-9-].*",
        "description": description,
    })
}

fn non_empty_string_with_default(default: &str) -> serde_json::Value {
    json!({
        "type": "string",
        "minLength": 1,
        "pattern": ".*\\S.*",
        "default": default,
    })
}

fn nullable_non_empty_string() -> serde_json::Value {
    json!({ "type": ["string", "null"], "minLength": 1, "pattern": ".*\\S.*" })
}

fn string_array(description: &str) -> serde_json::Value {
    json!({
        "type": "array",
        "description": description,
        "items": normalized_list_string_schema(),
    })
}

fn task_id_array(description: &str) -> serde_json::Value {
    json!({
        "type": "array",
        "description": description,
        "items": {
            "type": "string",
            "minLength": 1,
            "pattern": ".*[A-Za-z0-9-].*"
        },
    })
}

fn capability_array() -> serde_json::Value {
    json!({
        "type": "array",
        "description": "Capability names. Empty entries are rejected.",
        "items": normalized_list_string_schema(),
    })
}

fn normalized_list_string_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "minLength": 1,
        "pattern": r"^\s*[^,\s][^,]*(?:,\s*[^,\s][^,]*)*\s*$"
    })
}

fn required_capability_array() -> serde_json::Value {
    let mut schema = capability_array();
    schema["minItems"] = json!(1);
    schema
}

fn string_map(description: &str) -> serde_json::Value {
    json!({
        "type": "object",
        "description": description,
        "additionalProperties": { "type": "string" },
        "propertyNames": { "minLength": 1, "pattern": "^(?!\\s)(?!.*\\s$)[^=\\u0000]+$" },
    })
}

fn env_var_map(description: &str) -> serde_json::Value {
    json!({
        "type": "object",
        "description": description,
        "additionalProperties": {
            "type": "string",
            "minLength": 1,
            "pattern": r"^[A-Za-z_][A-Za-z0-9_]*$"
        },
        "propertyNames": { "minLength": 1, "pattern": "^(?!\\s)(?!.*\\s$)[^=\\u0000]+$" },
    })
}

fn update_requires_any_of(fields: &[&str], true_flags: &[&str]) -> serde_json::Value {
    let mut options = Vec::new();
    for field in fields {
        options.push(json!({ "required": [*field] }));
    }
    for flag in true_flags {
        options.push(json!({
            "required": [*flag],
            "properties": {
                (*flag): { "const": true }
            }
        }));
    }
    json!(options)
}

fn true_flag_conflicts_with_field(flag: &str, field: &str) -> serde_json::Value {
    json!({
        "not": {
            "required": [flag, field],
            "properties": {
                flag: { "const": true }
            }
        }
    })
}

fn true_flags_conflict(first: &str, second: &str) -> serde_json::Value {
    json!({
        "not": {
            "required": [first, second],
            "properties": {
                first: { "const": true },
                second: { "const": true }
            }
        }
    })
}

fn positive_integer() -> serde_json::Value {
    json!({ "type": "integer", "minimum": 1 })
}

fn non_negative_integer() -> serde_json::Value {
    json!({ "type": "integer", "minimum": 0 })
}

fn positive_integer_with_default(default: usize) -> serde_json::Value {
    json!({ "type": "integer", "minimum": 1, "default": default })
}

fn priority_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "default": "normal",
        "enum": Priority::VALUES,
    })
}

fn priority_input_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "default": "normal",
        "enum": Priority::INPUT_VALUES,
    })
}

fn agent_status_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "default": "online",
        "enum": AgentStatus::VALUES,
    })
}

fn agent_status_input_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "default": "online",
        "enum": AgentStatus::INPUT_VALUES,
    })
}

fn daemon_status_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "enum": ["running", "stopped"],
    })
}

fn event_kind_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "enum": EventKind::VALUES,
    })
}

fn capability_query_parameter(description: &str) -> serde_json::Value {
    json!({
        "name": "capability",
        "in": "query",
        "required": false,
        "description": description,
        "schema": normalized_list_string_schema()
    })
}

fn task_status_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "enum": TaskStatus::VALUES,
    })
}

fn task_status_input_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "enum": TaskStatus::INPUT_VALUES,
    })
}

fn run_status_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "enum": RunStatus::VALUES,
    })
}

fn run_status_input_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "enum": RunStatus::INPUT_VALUES,
    })
}

fn tool_kind_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "default": "shell",
        "enum": ToolKind::VALUES,
    })
}

fn tool_kind_input_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "default": "shell",
        "enum": ToolKind::INPUT_VALUES,
    })
}

fn endpoint(summary: &str) -> serde_json::Value {
    json!({
        "get": {
            "summary": summary,
            "responses": {
                "200": {
                    "description": "JSON response"
                }
            }
        }
    })
}

fn openapi_json_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Get this API schema");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("OpenApiDocument"));
    endpoint
}

fn health_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Health and OS identity");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("HealthResponse"));
    endpoint["get"]["responses"]["503"] = json!({
        "description": "State unavailable",
        "content": json_response(schema_ref("HealthResponse"))
    });
    endpoint
}

fn doctor_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Run preflight diagnostics");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("DoctorResponse"));
    endpoint
}

fn init_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Initialize state from config",
        schema_ref("InitRequest"),
    );
    if let Some(responses) = endpoint["post"]["responses"].as_object_mut() {
        responses.remove("200");
    }
    endpoint["post"]["responses"]["201"] = json!({
        "description": "Initialized state",
        "content": json_response(schema_ref("InitResponse"))
    });
    endpoint
}

fn config_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Read effective config");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("ConfigResponse"));
    let post = mutation_endpoint_with_optional_body(
        "Write default config",
        schema_ref("WriteConfigRequest"),
    );
    endpoint["post"] = post["post"].clone();
    if let Some(responses) = endpoint["post"]["responses"].as_object_mut() {
        responses.remove("200");
    }
    endpoint["post"]["responses"]["201"] = json!({
        "description": "Wrote default config",
        "content": json_response(schema_ref("WriteConfigResponse"))
    });
    endpoint
}

fn config_validate_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Validate config");
    endpoint["get"]["responses"]["200"]["content"] =
        json_response(schema_ref("ConfigValidationResponse"));
    endpoint
}

fn metrics_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Aggregate monitor metrics");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("MetricsResponse"));
    endpoint["get"]["responses"]["503"] = json!({
        "description": "State unavailable",
        "content": json_response(schema_ref("MetricsResponse"))
    });
    endpoint
}

fn status_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Aggregate runtime status");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("StatusResponse"));
    endpoint
}

fn agents_endpoint() -> serde_json::Value {
    let mut endpoint = collection_endpoint_with_body(
        "List agents",
        "Create an agent",
        schema_ref("CreateAgentRequest"),
    );
    endpoint["post"]["responses"]["201"]["content"] =
        json_response(schema_ref("AgentMutationResponse"));
    endpoint["get"]["responses"]["200"]["content"] = array_response(schema_ref("Agent"));
    endpoint["get"]["parameters"] = json!([
        {
            "name": "status",
            "in": "query",
            "required": false,
            "description": "Filter agents by status. Aliases `up`, `pause`, and `down` map to `online`, `paused`, and `offline`.",
            "schema": agent_status_input_schema()
        },
        {
            "name": "kind",
            "in": "query",
            "required": false,
            "description": "Filter agents by kind. Built-in kinds and custom kinds are supported.",
            "schema": non_empty_string("Agent kind.")
        },
        capability_query_parameter("Filter agents that advertise this capability. Comma-separated values require all listed capabilities."),
        {
            "name": "since",
            "in": "query",
            "required": false,
            "description": "Filter agents updated at or after this RFC3339 timestamp.",
            "schema": {
                "type": "string",
                "format": "date-time"
            }
        },
        {
            "name": "until",
            "in": "query",
            "required": false,
            "description": "Filter agents updated at or before this RFC3339 timestamp.",
            "schema": {
                "type": "string",
                "format": "date-time"
            }
        },
        {
            "name": "query",
            "in": "query",
            "required": false,
            "description": "Case-insensitive agent text search query over ID, name, kind, model, and capabilities. Empty values are rejected.",
            "schema": {
                "type": "string",
                "minLength": 1,
                "pattern": r".*\S.*"
            }
        },
        {
            "name": "limit",
            "in": "query",
            "required": false,
            "description": "Maximum number of recently updated agents to return. Must be greater than 0.",
            "schema": positive_integer()
        }
    ]);
    endpoint["get"]["responses"]["400"] =
        error_response_with_description("Invalid query parameter");
    endpoint
}

fn agent_detail_endpoint() -> serde_json::Value {
    let mut endpoint = detail_delete_endpoint("Get one agent", "Delete one unreferenced agent");
    endpoint["post"] =
        mutation_endpoint_with_body("Update agent", schema_ref("UpdateAgentRequest"))["post"]
            .clone();
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("Agent"));
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("AgentMutationResponse"));
    endpoint["delete"]["responses"]["200"]["content"] =
        json_response(schema_ref("AgentDeleteResponse"));
    endpoint
}

fn agent_heartbeat_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Record an agent heartbeat and status",
        schema_ref("HeartbeatRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("AgentMutationResponse"));
    endpoint
}

fn tasks_endpoint() -> serde_json::Value {
    let mut endpoint = collection_endpoint_with_body_and_responses(
        "List tasks",
        "Create a task",
        schema_ref("CreateTaskRequest"),
        &[
            ("404", "Referenced record not found"),
            ("422", "Unprocessable request"),
        ],
    );
    endpoint["post"]["responses"]["201"]["content"] =
        json_response(schema_ref("TaskMutationResponse"));
    endpoint["get"]["responses"]["200"]["content"] = array_response(schema_ref("Task"));
    endpoint["get"]["parameters"] = json!([
        {
            "name": "status",
            "in": "query",
            "required": false,
            "description": "Filter tasks by status. Aliases `completed` and `canceled` map to `complete` and `cancelled`.",
            "schema": task_status_input_schema()
        },
        {
            "name": "priority",
            "in": "query",
            "required": false,
            "description": "Filter tasks by priority. Alias `urgent` maps to `critical`.",
            "schema": priority_input_schema()
        },
        {
            "name": "agent",
            "in": "query",
            "required": false,
            "description": "Filter tasks assigned to this agent ID.",
            "schema": id_source_string("Agent ID.")
        },
        {
            "name": "tool",
            "in": "query",
            "required": false,
            "description": "Filter tasks invoking this tool ID.",
            "schema": id_source_string("Tool ID.")
        },
        {
            "name": "after",
            "in": "query",
            "required": false,
            "description": "Filter tasks that depend on this task ID.",
            "schema": id_source_string("Dependency task ID.")
        },
        capability_query_parameter("Filter tasks requiring this capability. Comma-separated values require all listed capabilities."),
        {
            "name": "since",
            "in": "query",
            "required": false,
            "description": "Filter tasks updated at or after this RFC3339 timestamp.",
            "schema": {
                "type": "string",
                "format": "date-time"
            }
        },
        {
            "name": "until",
            "in": "query",
            "required": false,
            "description": "Filter tasks updated at or before this RFC3339 timestamp.",
            "schema": {
                "type": "string",
                "format": "date-time"
            }
        },
        {
            "name": "query",
            "in": "query",
            "required": false,
            "description": "Case-insensitive task text search query over title, objective, plan steps, output, and command. Empty values are rejected.",
            "schema": {
                "type": "string",
                "minLength": 1,
                "pattern": r".*\S.*"
            }
        },
        {
            "name": "limit",
            "in": "query",
            "required": false,
            "description": "Maximum number of recently updated tasks to return. Must be greater than 0.",
            "schema": positive_integer()
        }
    ]);
    endpoint["get"]["responses"]["400"] =
        error_response_with_description("Invalid query parameter");
    endpoint
}

fn task_detail_endpoint() -> serde_json::Value {
    let mut endpoint = detail_delete_endpoint("Get one task", "Delete one task");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("Task"));
    endpoint["post"] =
        mutation_endpoint_with_body("Update task", schema_ref("UpdateTaskRequest"))["post"].clone();
    add_response_if_missing(&mut endpoint["post"], "422", "Unprocessable request");
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("TaskMutationResponse"));
    endpoint["delete"]["responses"]["200"]["content"] =
        json_response(schema_ref("TaskDeleteResponse"));
    endpoint
}

fn workflows_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("List or create workflows");
    endpoint["get"]["responses"]["200"]["content"] = array_response(schema_ref("Workflow"));
    endpoint["post"] =
        mutation_endpoint_with_body("Create workflow", schema_ref("CreateWorkflowRequest"))["post"]
            .clone();
    if let Some(responses) = endpoint["post"]["responses"].as_object_mut() {
        responses.remove("200");
    }
    endpoint["post"]["responses"]["201"] = json!({
        "description": "Created",
        "content": json_response(schema_ref("WorkflowMutationResponse"))
    });
    endpoint["get"]["parameters"] = json!([
        {
            "name": "priority",
            "in": "query",
            "required": false,
            "description": "Filter workflows by priority. Alias `urgent` maps to `critical`.",
            "schema": priority_input_schema()
        },
        {
            "name": "task",
            "in": "query",
            "required": false,
            "description": "Filter workflows that reference a task ID.",
            "schema": id_source_string("Task ID.")
        },
        {
            "name": "since",
            "in": "query",
            "required": false,
            "description": "Filter workflows updated at or after this RFC3339 timestamp.",
            "schema": { "type": "string", "format": "date-time" }
        },
        {
            "name": "until",
            "in": "query",
            "required": false,
            "description": "Filter workflows updated at or before this RFC3339 timestamp.",
            "schema": { "type": "string", "format": "date-time" }
        },
        {
            "name": "query",
            "in": "query",
            "required": false,
            "description": "Case-insensitive search over workflow ID, objective, stage names, and task IDs.",
            "schema": non_empty_string("Workflow search query.")
        },
        {
            "name": "limit",
            "in": "query",
            "required": false,
            "description": "Maximum number of recently updated workflows to return.",
            "schema": positive_integer()
        }
    ]);
    endpoint["get"]["responses"]["400"] =
        error_response_with_description("Invalid query parameter");
    endpoint
}

fn workflow_detail_endpoint() -> serde_json::Value {
    let mut endpoint = detail_delete_endpoint("Get one workflow", "Remove one workflow");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("Workflow"));
    endpoint["delete"]["responses"]["200"]["content"] =
        json_response(schema_ref("WorkflowDeleteResponse"));
    endpoint
}

fn workflow_status_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Get workflow progress status");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("WorkflowProgress"));
    endpoint
}

fn workflow_run_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint("Execute the next runnable workflow stage");
    endpoint["post"]["requestBody"] = optional_request_body(schema_ref("RunWorkflowRequest"));
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("WorkflowRunResponse"));
    endpoint
}

fn workflow_cancel_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint("Cancel active workflow tasks");
    endpoint["post"]["requestBody"] = optional_request_body(schema_ref("FinishTaskRequest"));
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("WorkflowCancelResponse"));
    endpoint
}

fn run_detail_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Get one run");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("RunRecord"));
    endpoint
}

fn runs_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("List runs");
    endpoint["get"]["responses"]["200"]["content"] = array_response(schema_ref("RunRecord"));
    endpoint["get"]["parameters"] = json!([
        {
            "name": "status",
            "in": "query",
            "required": false,
            "description": "Filter runs by status. Aliases `cancel_requested`, `canceled`, and `succeeded` map to canonical statuses.",
            "schema": run_status_input_schema()
        },
        {
            "name": "task",
            "in": "query",
            "required": false,
            "description": "Filter runs by task ID.",
            "schema": id_source_string("Task ID.")
        },
        {
            "name": "agent",
            "in": "query",
            "required": false,
            "description": "Filter runs by assigned agent ID.",
            "schema": id_source_string("Agent ID.")
        },
        {
            "name": "since",
            "in": "query",
            "required": false,
            "description": "Filter runs started at or after this RFC3339 timestamp.",
            "schema": {
                "type": "string",
                "format": "date-time"
            }
        },
        {
            "name": "until",
            "in": "query",
            "required": false,
            "description": "Filter runs started at or before this RFC3339 timestamp.",
            "schema": {
                "type": "string",
                "format": "date-time"
            }
        },
        {
            "name": "query",
            "in": "query",
            "required": false,
            "description": "Case-insensitive run command search query. Empty values are rejected.",
            "schema": {
                "type": "string",
                "minLength": 1,
                "pattern": r".*\S.*"
            }
        },
        {
            "name": "limit",
            "in": "query",
            "required": false,
            "description": "Maximum number of recent runs to return. Must be greater than 0.",
            "schema": positive_integer()
        }
    ]);
    endpoint["get"]["responses"]["400"] =
        error_response_with_description("Invalid query parameter");
    endpoint
}

fn tools_endpoint() -> serde_json::Value {
    let mut endpoint = collection_endpoint_with_body_and_responses(
        "List tools",
        "Create a tool",
        schema_ref("CreateToolRequest"),
        &[("422", "Unprocessable request")],
    );
    endpoint["post"]["responses"]["201"]["content"] =
        json_response(schema_ref("ToolMutationResponse"));
    endpoint["get"]["responses"]["200"]["content"] = array_response(schema_ref("ToolDefinition"));
    endpoint["get"]["parameters"] = json!([
        {
            "name": "kind",
            "in": "query",
            "required": false,
            "description": "Filter tools by kind. Aliases `read-file` and `write-file` map to `file-read` and `file-write`.",
            "schema": tool_kind_input_schema()
        },
        capability_query_parameter("Filter tools requiring this capability. Comma-separated values require all listed capabilities."),
        {
            "name": "since",
            "in": "query",
            "required": false,
            "description": "Filter tools updated at or after this RFC3339 timestamp.",
            "schema": {
                "type": "string",
                "format": "date-time"
            }
        },
        {
            "name": "until",
            "in": "query",
            "required": false,
            "description": "Filter tools updated at or before this RFC3339 timestamp.",
            "schema": {
                "type": "string",
                "format": "date-time"
            }
        },
        {
            "name": "query",
            "in": "query",
            "required": false,
            "description": "Case-insensitive tool text search query over ID, name, description, and command template. Empty values are rejected.",
            "schema": {
                "type": "string",
                "minLength": 1,
                "pattern": r".*\S.*"
            }
        },
        {
            "name": "limit",
            "in": "query",
            "required": false,
            "description": "Maximum number of recently updated tools to return. Must be greater than 0.",
            "schema": positive_integer()
        }
    ]);
    endpoint["get"]["responses"]["400"] =
        error_response_with_description("Invalid query parameter");
    endpoint
}

fn tool_detail_endpoint() -> serde_json::Value {
    let mut endpoint = detail_delete_endpoint("Get one tool", "Delete one tool");
    endpoint["post"] =
        mutation_endpoint_with_body("Update tool", schema_ref("UpdateToolRequest"))["post"].clone();
    add_response_if_missing(&mut endpoint["post"], "422", "Unprocessable request");
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("ToolMutationResponse"));
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("ToolDefinition"));
    endpoint["delete"]["responses"]["200"]["content"] =
        json_response(schema_ref("ToolDeleteResponse"));
    endpoint
}

fn events_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("List audit events");
    endpoint["get"]["responses"]["200"]["content"] = array_response(schema_ref("Event"));
    endpoint["get"]["parameters"] = json!([
        {
            "name": "limit",
            "in": "query",
            "required": false,
            "description": "Maximum number of recent events to return. Must be greater than 0.",
            "schema": {
                "type": "integer",
                "minimum": 1
            }
        },
        {
            "name": "kind",
            "in": "query",
            "required": false,
            "description": "Filter events by kind.",
            "schema": event_kind_schema()
        },
        {
            "name": "since",
            "in": "query",
            "required": false,
            "description": "Filter events at or after this RFC3339 timestamp.",
            "schema": {
                "type": "string",
                "format": "date-time"
            }
        },
        {
            "name": "until",
            "in": "query",
            "required": false,
            "description": "Filter events at or before this RFC3339 timestamp.",
            "schema": {
                "type": "string",
                "format": "date-time"
            }
        },
        {
            "name": "query",
            "in": "query",
            "required": false,
            "description": "Case-insensitive event message search query. Empty values are rejected.",
            "schema": {
                "type": "string",
                "minLength": 1,
                "pattern": r".*\S.*"
            }
        }
    ]);
    endpoint["get"]["responses"]["400"] =
        error_response_with_description("Invalid query parameter");
    endpoint
}

fn memory_endpoint() -> serde_json::Value {
    let mut endpoint = collection_endpoint_with_body(
        "List memory records",
        "Create a memory record",
        schema_ref("CreateMemoryRequest"),
    );
    endpoint["post"]["responses"]["201"]["content"] =
        json_response(schema_ref("MemoryMutationResponse"));
    endpoint["get"]["responses"]["200"]["content"] = array_response(schema_ref("MemoryRecord"));
    endpoint["get"]["parameters"] = json!([
        {
            "name": "query",
            "in": "query",
            "required": false,
            "description": "Case-insensitive memory search query. Empty values are rejected.",
            "schema": {
                "type": "string",
                "minLength": 1,
                "pattern": r".*\S.*"
            }
        },
        {
            "name": "tag",
            "in": "query",
            "required": false,
            "description": "Filter memory records that have this tag. Comma-separated values require all listed tags.",
            "schema": normalized_list_string_schema()
        },
        {
            "name": "since",
            "in": "query",
            "required": false,
            "description": "Filter memory records updated at or after this RFC3339 timestamp.",
            "schema": {
                "type": "string",
                "format": "date-time"
            }
        },
        {
            "name": "until",
            "in": "query",
            "required": false,
            "description": "Filter memory records updated at or before this RFC3339 timestamp.",
            "schema": {
                "type": "string",
                "format": "date-time"
            }
        },
        {
            "name": "limit",
            "in": "query",
            "required": false,
            "description": "Maximum number of recent memory records to return. Must be greater than 0.",
            "schema": positive_integer()
        }
    ]);
    endpoint["get"]["responses"]["400"] =
        error_response_with_description("Invalid query parameter");
    endpoint
}

fn memory_detail_endpoint() -> serde_json::Value {
    let mut endpoint = detail_delete_endpoint("Get memory record", "Remove memory record");
    endpoint["post"] = mutation_endpoint_with_body(
        "Update memory record",
        schema_ref("UpdateMemoryRequest"),
    )["post"]
        .clone();
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("MemoryRecord"));
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("MemoryMutationResponse"));
    endpoint["delete"]["responses"]["200"]["content"] =
        json_response(schema_ref("MemoryDeleteResponse"));
    endpoint
}

fn run_logs_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Get run log body");
    endpoint["get"]["description"] = json!(
        "Non-following API equivalent for `runs logs` and bounded `runs tail`; use the CLI `runs tail --follow` for streaming."
    );
    endpoint["get"]["parameters"] = log_tail_parameters();
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("RunLogsResponse"));
    endpoint["get"]["responses"]["400"] =
        error_response_with_description("Invalid query parameter");
    endpoint
}

fn run_replay_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Replay run with task, events, and log");
    endpoint["get"]["parameters"] = log_tail_parameters();
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("RunReplayResponse"));
    endpoint["get"]["responses"]["400"] =
        error_response_with_description("Invalid query parameter");
    endpoint
}

fn log_tail_parameters() -> serde_json::Value {
    json!([
        {
            "name": "tail_bytes",
            "in": "query",
            "required": false,
            "description": "Only return the final N bytes of the run log. Must be greater than 0.",
            "schema": positive_integer()
        }
    ])
}

fn with_id_parameter(mut endpoint: serde_json::Value, description: &str) -> serde_json::Value {
    for method in ["get", "post", "delete"] {
        if let Some(operation) = endpoint.get_mut(method) {
            let mut parameters = operation
                .get("parameters")
                .and_then(|parameters| parameters.as_array())
                .cloned()
                .unwrap_or_default();
            parameters.insert(0, path_id_parameter(description));
            operation["parameters"] = json!(parameters);
            add_response_if_missing(operation, "400", "Invalid path parameter");
            add_response_if_missing(operation, "404", "Record not found");
        }
    }
    endpoint
}

fn path_id_parameter(description: &str) -> serde_json::Value {
    json!({
        "name": "id",
        "in": "path",
        "required": true,
        "description": description,
        "schema": {
            "type": "string",
            "minLength": 1,
            "pattern": ".*[A-Za-z0-9].*"
        }
    })
}

fn collection_endpoint_with_body(
    get_summary: &str,
    post_summary: &str,
    schema: serde_json::Value,
) -> serde_json::Value {
    collection_endpoint_with_body_and_responses(get_summary, post_summary, schema, &[])
}

fn collection_endpoint_with_body_and_responses(
    get_summary: &str,
    post_summary: &str,
    schema: serde_json::Value,
    extra_post_responses: &[(&str, &str)],
) -> serde_json::Value {
    let mut endpoint = collection_endpoint(get_summary, post_summary);
    endpoint["post"]["requestBody"] = request_body(schema);
    add_responses(&mut endpoint["post"]["responses"], extra_post_responses);
    endpoint
}

fn add_responses(responses: &mut serde_json::Value, extra: &[(&str, &str)]) {
    let Some(responses) = responses.as_object_mut() else {
        return;
    };
    for (code, description) in extra {
        responses.insert(
            (*code).into(),
            json!({
                "description": description,
                "content": error_response(),
            }),
        );
    }
}

fn add_response_if_missing(operation: &mut serde_json::Value, code: &str, description: &str) {
    let Some(responses) = operation
        .get_mut("responses")
        .and_then(|responses| responses.as_object_mut())
    else {
        return;
    };
    responses.entry(code.to_owned()).or_insert_with(|| {
        if code == "401" {
            authentication_required_response(description)
        } else {
            error_response_with_description(description)
        }
    });
}

fn collection_endpoint(get_summary: &str, post_summary: &str) -> serde_json::Value {
    json!({
        "get": {
            "summary": get_summary,
            "responses": {
                "200": {
                    "description": "JSON response"
                }
            }
        },
        "post": {
            "summary": post_summary,
            "responses": {
                "201": {
                    "description": "Created JSON response"
                },
                "400": {
                    "description": "Invalid request",
                    "content": error_response()
                },
                "409": {
                    "description": "Conflict",
                    "content": error_response()
                }
            }
        }
    })
}

fn mutation_endpoint_with_body(summary: &str, schema: serde_json::Value) -> serde_json::Value {
    let mut endpoint = mutation_endpoint(summary);
    endpoint["post"]["requestBody"] = request_body(schema);
    endpoint
}

fn run_once_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Run one scheduler tick and optionally execute assigned tasks",
        schema_ref("RunRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] = json_response(schema_ref("RunOnceResponse"));
    endpoint
}

fn daemon_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Get daemon status");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("DaemonResponse"));
    endpoint
}

fn daemon_stop_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint("Request daemon stop");
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("DaemonStopResponse"));
    endpoint
}

fn service_launchd_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Render launchd service plist",
        schema_ref("LaunchdServiceRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("LaunchdServiceResponse"));
    endpoint
}

fn service_launchd_install_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Install launchd service plist",
        schema_ref("LaunchdServiceRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("InstallLaunchdServiceResponse"));
    endpoint
}

fn service_launchd_uninstall_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Uninstall launchd service plist",
        schema_ref("UninstallLaunchdServiceRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("UninstallLaunchdServiceResponse"));
    endpoint
}

fn service_launchd_start_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Start launchd service",
        schema_ref("LaunchdServiceControlRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("LaunchdServiceStartResponse"));
    endpoint["post"]["responses"]["409"]["content"] = json_response(json!({
        "oneOf": [
            schema_ref("ErrorResponse"),
            schema_ref("LaunchctlCommandErrorResponse")
        ]
    }));
    endpoint
}

fn service_launchd_stop_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Stop launchd service",
        schema_ref("LaunchdServiceControlRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("LaunchdServiceStopResponse"));
    endpoint["post"]["responses"]["409"]["content"] =
        json_response(schema_ref("LaunchctlCommandErrorResponse"));
    endpoint
}

fn service_launchd_status_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Read launchd service status",
        schema_ref("LaunchdServiceControlRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("LaunchdServiceStatusResponse"));
    endpoint
}

fn task_plan_endpoint() -> serde_json::Value {
    let mut endpoint =
        mutation_endpoint_with_body("Replace a task plan", schema_ref("PlanTaskRequest"));
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("TaskMutationResponse"));
    endpoint
}

fn task_recover_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Recover stale running tasks",
        schema_ref("RecoverTasksRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("RecoverTasksResponse"));
    endpoint
}

fn task_assign_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_body(
        "Assign a ready pending task",
        schema_ref("AssignTaskRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("TaskAssignmentResponse"));
    endpoint
}

fn task_priority_endpoint() -> serde_json::Value {
    let mut endpoint =
        mutation_endpoint_with_body("Update a task priority", schema_ref("TaskPriorityRequest"));
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("TaskMutationResponse"));
    endpoint
}

fn task_dependencies_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_body(
        "Replace task dependencies",
        schema_ref("TaskDependenciesRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("TaskMutationResponse"));
    endpoint
}

fn mutation_endpoint_with_optional_body(
    summary: &str,
    schema: serde_json::Value,
) -> serde_json::Value {
    let mut endpoint = mutation_endpoint(summary);
    endpoint["post"]["requestBody"] = optional_request_body(schema);
    endpoint
}

fn agent_claim_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Claim the next ready task for an agent",
        schema_ref("ClaimTaskRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("AgentClaimResponse"));
    endpoint
}

fn task_lifecycle_endpoint(summary: &str) -> serde_json::Value {
    let mut endpoint =
        mutation_endpoint_with_optional_body(summary, schema_ref("FinishTaskRequest"));
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("TaskMutationResponse"));
    endpoint
}

fn repair_state_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Repair recoverable state consistency issues",
        schema_ref("RepairStateRequest"),
    );
    let response_schema = json_response(schema_ref("RepairStateResponse"));
    endpoint["post"]["responses"]["200"]["content"] = response_schema.clone();
    endpoint["post"]["responses"]["409"]["content"] = response_schema;
    endpoint
}

fn state_prune_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Prune old finished runs, run logs, and events",
        schema_ref("PruneStateRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] = json_response(schema_ref("PruneReport"));
    endpoint
}

fn state_backup_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Back up durable state",
        schema_ref("BackupStateRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("BackupStateResponse"));
    endpoint
}

fn state_export_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Export durable state snapshot");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("OperatingSystem"));
    let post = mutation_endpoint_with_body(
        "Export durable state snapshot to a path",
        schema_ref("ExportStateRequest"),
    );
    endpoint["post"] = post["post"].clone();
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("ExportStateResponse"));
    endpoint
}

fn state_import_endpoint() -> serde_json::Value {
    let mut endpoint =
        mutation_endpoint_with_body("Import durable state", schema_ref("ImportStateRequest"));
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("ImportStateResponse"));
    endpoint
}

fn state_migrate_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Migrate durable state",
        schema_ref("MigrateStateRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("MigrateStateResponse"));
    endpoint
}

fn state_validate_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Validate durable state consistency");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("ValidationReport"));
    endpoint
}

fn mutation_endpoint(summary: &str) -> serde_json::Value {
    json!({
        "post": {
            "summary": summary,
            "responses": {
                "200": {
                    "description": "Updated JSON response"
                },
                "400": {
                    "description": "Invalid request",
                    "content": error_response()
                },
                "404": {
                    "description": "Record not found",
                    "content": error_response()
                },
                "409": {
                    "description": "Conflict",
                    "content": error_response()
                }
            }
        }
    })
}

fn run_cancel_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint("Request cancellation for a running run");
    let response_schema = json_response(schema_ref("RunCancelResponse"));
    endpoint["post"]["responses"]["200"]["content"] = response_schema.clone();
    endpoint["post"]["responses"]["409"]["content"] = response_schema;
    endpoint
}

fn request_body(schema: serde_json::Value) -> serde_json::Value {
    request_body_with_required(schema, true)
}

fn optional_request_body(schema: serde_json::Value) -> serde_json::Value {
    request_body_with_required(schema, false)
}

fn json_response(schema: serde_json::Value) -> serde_json::Value {
    json!({
        "application/json": {
            "schema": schema,
        }
    })
}

fn error_response() -> serde_json::Value {
    json_response(schema_ref("ErrorResponse"))
}

fn error_response_with_description(description: &str) -> serde_json::Value {
    json!({
        "description": description,
        "content": error_response(),
    })
}

fn authentication_required_response(description: &str) -> serde_json::Value {
    json!({
        "description": description,
        "headers": {
            "WWW-Authenticate": {
                "description": "Bearer authentication challenge.",
                "schema": {
                    "type": "string",
                    "example": "Bearer"
                }
            }
        },
        "content": error_response(),
    })
}

fn array_response(item_schema: serde_json::Value) -> serde_json::Value {
    json_response(json!({
        "type": "array",
        "items": item_schema,
    }))
}

fn request_body_with_required(schema: serde_json::Value, required: bool) -> serde_json::Value {
    json!({
        "required": required,
        "content": {
            "application/json": {
                "schema": schema,
            }
        }
    })
}

fn detail_delete_endpoint(get_summary: &str, delete_summary: &str) -> serde_json::Value {
    json!({
        "get": {
            "summary": get_summary,
            "responses": {
                "200": {
                    "description": "JSON response"
                },
                "404": {
                    "description": "Record not found",
                    "content": error_response()
                }
            }
        },
        "delete": {
            "summary": delete_summary,
            "responses": {
                "200": {
                    "description": "Deleted JSON response"
                },
                "404": {
                    "description": "Record not found",
                    "content": error_response()
                },
                "409": {
                    "description": "Conflict",
                    "content": error_response()
                }
            }
        }
    })
}

fn response_for_detail_path(
    store: Option<&Store>,
    os: &OperatingSystem,
    segments: &[&str],
    query: &str,
) -> Option<(&'static str, String)> {
    match segments {
        ["agents", id] => Some(match parse_agent_path_id(id) {
            Ok(id) => json_detail(os.agents.get(&id), "agent not found"),
            Err(response) => response,
        }),
        ["tasks", id] => Some(match parse_task_path_id(id) {
            Ok(id) => json_detail(os.tasks.get(&id), "task not found"),
            Err(response) => response,
        }),
        ["workflows", id] => Some(match parse_workflow_path_id(id) {
            Ok(id) => json_detail(os.workflows.get(&id), "workflow not found"),
            Err(response) => response,
        }),
        ["workflows", id, "status"] => Some(match parse_workflow_path_id(id) {
            Ok(id) => match os.workflow_progress(&id) {
                Some(progress) => ("200 OK", json!(progress).to_string()),
                None => (
                    "404 Not Found",
                    json!({ "error": "workflow not found" }).to_string(),
                ),
            },
            Err(response) => response,
        }),
        ["tools", id] => Some(match parse_tool_path_id(id) {
            Ok(id) => json_detail(os.tools.get(&id), "tool not found"),
            Err(response) => response,
        }),
        ["memory", id] => Some(match parse_memory_path_id(id) {
            Ok(id) => json_detail(
                os.memory.iter().find(|record| record.id == id),
                "memory not found",
            ),
            Err(response) => response,
        }),
        ["runs", id] => Some(match parse_run_path_id(id) {
            Ok(id) => json_detail(os.runs.get(&id), "run not found"),
            Err(response) => response,
        }),
        ["runs", id, "logs"] => Some(match parse_run_path_id(id) {
            Ok(id) => run_logs_response(store, os, &id, query),
            Err(response) => response,
        }),
        ["runs", id, "replay"] => Some(match parse_run_path_id(id) {
            Ok(id) => run_replay_response(store, os, &id, query),
            Err(response) => response,
        }),
        _ => None,
    }
}

fn json_detail<T: serde::Serialize>(value: Option<&T>, missing: &str) -> (&'static str, String) {
    match value {
        Some(value) => ("200 OK", json!(value).to_string()),
        None => ("404 Not Found", json!({ "error": missing }).to_string()),
    }
}

fn run_logs_response(
    store: Option<&Store>,
    os: &OperatingSystem,
    id: &RunId,
    query: &str,
) -> (&'static str, String) {
    if !os.runs.contains_key(id) {
        return (
            "404 Not Found",
            json!({ "error": "run not found" }).to_string(),
        );
    }
    if let Err(response) = reject_unknown_query_keys(query, &["tail_bytes"]) {
        return response;
    }
    let tail_bytes = match positive_query_usize(query, "tail_bytes") {
        Ok(tail_bytes) => tail_bytes,
        Err(response) => return response,
    };
    let Some(store) = store else {
        return (
            "500 Internal Server Error",
            json!({ "error": "store unavailable" }).to_string(),
        );
    };
    let log_path = store.run_log_path(id);
    match std::fs::read_to_string(&log_path) {
        Ok(body) => {
            let truncated = text_tail_was_truncated(&body, tail_bytes);
            let body = tail_text_by_bytes(&body, tail_bytes);
            (
                "200 OK",
                json!({
                    "run_id": id,
                    "log_path": log_path.display().to_string(),
                    "tail_bytes": tail_bytes,
                    "truncated": truncated,
                    "body": body,
                })
                .to_string(),
            )
        }
        Err(error) => (
            "404 Not Found",
            json!({
                "error": "run log not found",
                "detail": error.to_string(),
            })
            .to_string(),
        ),
    }
}

fn run_replay_response(
    store: Option<&Store>,
    os: &OperatingSystem,
    id: &RunId,
    query: &str,
) -> (&'static str, String) {
    let Some(run) = os.runs.get(id) else {
        return (
            "404 Not Found",
            json!({ "error": "run not found" }).to_string(),
        );
    };
    if let Err(response) = reject_unknown_query_keys(query, &["tail_bytes"]) {
        return response;
    }
    let tail_bytes = match positive_query_usize(query, "tail_bytes") {
        Ok(tail_bytes) => tail_bytes,
        Err(response) => return response,
    };
    let mut log_truncated = false;
    let (log, log_error) = match store {
        Some(store) => {
            let log_path = store.run_log_path(id);
            match std::fs::read_to_string(&log_path) {
                Ok(body) => {
                    log_truncated = text_tail_was_truncated(&body, tail_bytes);
                    (Some(tail_text_by_bytes(&body, tail_bytes)), None)
                }
                Err(error) => (
                    None,
                    Some(format!(
                        "could not read run log: {}: {error}",
                        log_path.display()
                    )),
                ),
            }
        }
        None => (None, Some("store unavailable".to_owned())),
    };
    let related_events = os
        .events
        .iter()
        .filter(|event| {
            event.message.contains(&run.id.to_string())
                || event.message.contains(&run.task_id.to_string())
        })
        .collect::<Vec<_>>();

    (
        "200 OK",
        json!({
            "run": run,
            "task": os.tasks.get(&run.task_id),
            "events": related_events,
            "log": log,
            "log_tail_bytes": tail_bytes,
            "log_truncated": log_truncated,
            "log_error": log_error,
        })
        .to_string(),
    )
}

fn status_json(os: &OperatingSystem) -> serde_json::Value {
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
    let blocked = os
        .tasks
        .values()
        .filter(|task| task.status == TaskStatus::Blocked)
        .count();
    let complete = os
        .tasks
        .values()
        .filter(|task| task.status == TaskStatus::Complete)
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

    json!({
        "name": os.name,
        "agent_os_version": env!("CARGO_PKG_VERSION"),
        "version": os.version,
        "agents": os.agents.len(),
        "tasks_pending": pending,
        "tasks_running": running,
        "tasks_blocked": blocked,
        "tasks_complete": complete,
        "tasks_failed": failed,
        "tasks_cancelled": cancelled,
        "workflows": os.workflows.len(),
        "runs": os.runs.len(),
        "tools": os.tools.len(),
        "events": os.events.len(),
        "memories": os.memory.len(),
        "daemon_status": os.daemon.as_ref().map(|daemon| daemon.status.to_string()),
        "daemon_ticks": os.daemon.as_ref().map(|daemon| daemon.ticks),
    })
}

fn doctor_json(store: &Store, config_path: Option<&Path>) -> serde_json::Value {
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
    let state_directory = store
        .path()
        .parent()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| ".".into());
    let config = config_health_json(config_path);

    json!({
        "agent_os_version": env!("CARGO_PKG_VERSION"),
        "state_path": store.path().display().to_string(),
        "state_directory": state_directory,
        "state_exists": state_exists,
        "state_loads": state_loads,
        "state_error": state_error,
        "state_valid": state_valid,
        "state_issues": state_issues,
        "config_path": config["config_path"].clone(),
        "config_exists": config["config_exists"].clone(),
        "config_loads": config["config_loads"].clone(),
        "config_valid": config["config_valid"].clone(),
        "config_issues": config["config_issues"].clone(),
        "config_error": config["config_error"].clone(),
    })
}

pub fn metrics_json(os: &OperatingSystem) -> serde_json::Value {
    let validation = validate_state(os);
    let agent_status_count = |status: AgentStatus| {
        os.agents
            .values()
            .filter(|agent| agent.status == status)
            .count()
    };
    let task_status_count = |status: TaskStatus| {
        os.tasks
            .values()
            .filter(|task| task.status == status)
            .count()
    };
    let run_status_count =
        |status: RunStatus| os.runs.values().filter(|run| run.status == status).count();

    json!({
        "ok": validation.valid,
        "service": "agent-os",
        "agent_os_version": env!("CARGO_PKG_VERSION"),
        "name": os.name,
        "version": os.version,
        "state_loads": true,
        "state_valid": validation.valid,
        "state_issue_count": validation.issues.len(),
        "state_error": null,
        "agents_total": os.agents.len(),
        "agents_online": agent_status_count(AgentStatus::Online),
        "agents_busy": agent_status_count(AgentStatus::Busy),
        "agents_paused": agent_status_count(AgentStatus::Paused),
        "agents_offline": agent_status_count(AgentStatus::Offline),
        "tasks_total": os.tasks.len(),
        "tasks_pending": task_status_count(TaskStatus::Pending),
        "tasks_running": task_status_count(TaskStatus::Running),
        "tasks_blocked": task_status_count(TaskStatus::Blocked),
        "tasks_complete": task_status_count(TaskStatus::Complete),
        "tasks_failed": task_status_count(TaskStatus::Failed),
        "tasks_cancelled": task_status_count(TaskStatus::Cancelled),
        "workflows_total": os.workflows.len(),
        "runs_total": os.runs.len(),
        "runs_running": run_status_count(RunStatus::Running),
        "runs_cancel_requested": run_status_count(RunStatus::CancelRequested),
        "runs_cancelled": run_status_count(RunStatus::Cancelled),
        "runs_success": run_status_count(RunStatus::Success),
        "runs_failed": run_status_count(RunStatus::Failed),
        "runs_rejected": run_status_count(RunStatus::Rejected),
        "tools_total": os.tools.len(),
        "events_total": os.events.len(),
        "memories_total": os.memory.len(),
        "daemon_status": os.daemon.as_ref().map(|daemon| daemon.status.to_string()),
        "daemon_ticks": os.daemon.as_ref().map(|daemon| daemon.ticks),
    })
}

fn health_json(os: &OperatingSystem, config_path: Option<&Path>) -> serde_json::Value {
    let validation = validate_state(os);
    let config = config_health_json(config_path);
    json!({
        "ok": validation.valid && config["config_valid"].as_bool().unwrap_or(true),
        "service": "agent-os",
        "agent_os_version": env!("CARGO_PKG_VERSION"),
        "name": os.name,
        "version": os.version,
        "state_loads": true,
        "state_valid": validation.valid,
        "state_issue_count": validation.issues.len(),
        "state_issues": validation.issues,
        "state_error": null,
        "config_path": config["config_path"].clone(),
        "config_exists": config["config_exists"].clone(),
        "config_loads": config["config_loads"].clone(),
        "config_valid": config["config_valid"].clone(),
        "config_issues": config["config_issues"].clone(),
        "config_error": config["config_error"].clone(),
    })
}

fn config_health_json(config_path: Option<&Path>) -> serde_json::Value {
    let Some(path) = config_path else {
        return json!({
            "config_path": null,
            "config_exists": false,
            "config_loads": false,
            "config_valid": null,
            "config_issues": [],
            "config_error": null,
        });
    };
    let exists = path.exists();
    if !exists {
        return json!({
            "config_path": path.display().to_string(),
            "config_exists": false,
            "config_loads": false,
            "config_valid": null,
            "config_issues": [],
            "config_error": null,
        });
    }
    match load_config(path) {
        Ok(Some(config)) => match validate_seed_config(&config) {
            Ok(()) => json!({
                "config_path": path.display().to_string(),
                "config_exists": true,
                "config_loads": true,
                "config_valid": true,
                "config_issues": [],
                "config_error": null,
            }),
            Err(error) => json!({
                "config_path": path.display().to_string(),
                "config_exists": true,
                "config_loads": true,
                "config_valid": false,
                "config_issues": [error],
                "config_error": null,
            }),
        },
        Ok(None) => json!({
            "config_path": path.display().to_string(),
            "config_exists": false,
            "config_loads": false,
            "config_valid": null,
            "config_issues": [],
            "config_error": null,
        }),
        Err(error) => json!({
            "config_path": path.display().to_string(),
            "config_exists": true,
            "config_loads": false,
            "config_valid": false,
            "config_issues": [error.to_string()],
            "config_error": error.to_string(),
        }),
    }
}

pub fn metrics_unavailable_json(error: String) -> serde_json::Value {
    json!({
        "ok": false,
        "service": "agent-os",
        "agent_os_version": env!("CARGO_PKG_VERSION"),
        "name": null,
        "version": null,
        "state_loads": false,
        "state_valid": null,
        "state_issue_count": 0,
        "state_error": error,
        "agents_total": 0,
        "agents_online": 0,
        "agents_busy": 0,
        "agents_paused": 0,
        "agents_offline": 0,
        "tasks_total": 0,
        "tasks_pending": 0,
        "tasks_running": 0,
        "tasks_blocked": 0,
        "tasks_complete": 0,
        "tasks_failed": 0,
        "tasks_cancelled": 0,
        "workflows_total": 0,
        "runs_total": 0,
        "runs_running": 0,
        "runs_cancel_requested": 0,
        "runs_cancelled": 0,
        "runs_success": 0,
        "runs_failed": 0,
        "runs_rejected": 0,
        "tools_total": 0,
        "events_total": 0,
        "memories_total": 0,
        "daemon_status": null,
        "daemon_ticks": null,
    })
}

fn health_unavailable_json(error: String, config_path: Option<&Path>) -> serde_json::Value {
    let config = config_health_json(config_path);
    json!({
        "ok": false,
        "service": "agent-os",
        "agent_os_version": env!("CARGO_PKG_VERSION"),
        "name": null,
        "version": null,
        "state_loads": false,
        "state_valid": null,
        "state_issue_count": 0,
        "state_issues": [],
        "state_error": error,
        "config_path": config["config_path"].clone(),
        "config_exists": config["config_exists"].clone(),
        "config_loads": config["config_loads"].clone(),
        "config_valid": config["config_valid"].clone(),
        "config_issues": config["config_issues"].clone(),
        "config_error": config["config_error"].clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        Agent, AgentKind, EventKind, OperatingSystem, Priority, RunRecord, Task, TaskStatus,
        ToolId, ToolInvocation,
    };
    use std::fs;
    use std::io::Cursor;

    #[test]
    fn status_endpoint_reports_counts() {
        let mut os = OperatingSystem::new("api-test");
        os.create_task(Task::new(
            "Task",
            "Objective",
            Priority::Normal,
            vec!["plan".into()],
        ));

        let (status, body) = response_for_path(&os, "/status");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(value["name"], "api-test");
        assert_eq!(value["tasks_pending"], 1);
    }

    #[test]
    fn agents_endpoint_supports_status_filter_query() {
        let mut os = OperatingSystem::new("api-test");
        let base = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&chrono::Utc);
        os.register_agent(Agent::new(
            "online",
            AgentKind::Builder,
            None,
            vec!["rust".into()],
            1,
        ));
        let mut paused = Agent::new(
            "paused",
            AgentKind::Reviewer,
            None,
            vec!["review".into()],
            1,
        );
        paused.status = AgentStatus::Paused;
        paused.updated_at = base;
        os.register_agent(paused);
        let mut latest_paused = Agent::new(
            "latest-paused",
            AgentKind::Reviewer,
            None,
            vec!["review".into()],
            1,
        );
        latest_paused.status = AgentStatus::Paused;
        latest_paused.updated_at = base + chrono::Duration::seconds(1);
        os.register_agent(latest_paused);

        let (status, body) = response_for_path(
            &os,
            "/agents?status=paused&kind=reviewer&capability=review&since=2026-01-01T00:00:01Z&until=2026-01-01T00:00:01Z&query=latest&limit=1",
        );
        let agents: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(agents.as_array().expect("agents").len(), 1);
        assert_eq!(agents[0]["name"], "latest-paused");

        let (status, body) =
            response_for_path(&os, "/agents?status=paused&kind=reviewer&capability=review");
        let agents: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(agents.as_array().expect("agents").len(), 2);
        assert_eq!(agents[0]["name"], "latest-paused");
        assert_eq!(agents[1]["name"], "paused");

        let (status, body) = response_for_path(&os, "/agents?status=up&query=online");
        let agents: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(agents.as_array().expect("agents").len(), 1);
        assert_eq!(agents[0]["name"], "online");
        assert_eq!(agents[0]["status"], "online");

        let (status, body) = response_for_path(&os, "/agents?status=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "status must not be empty");

        let (status, body) = response_for_path(&os, "/agents?status=sleeping");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "status must be a valid agent status");

        let (status, body) = response_for_path(&os, "/agents?status=online&status=paused");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "status must not be repeated");

        let (status, body) = response_for_path(&os, "/agents?kind=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "kind must not be empty");

        let (status, body) = response_for_path(&os, "/agents?kind=builder&kind=reviewer");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "kind must not be repeated");

        let (status, body) = response_for_path(&os, "/agents?capability=,");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            value["error"],
            "capability must include at least one capability"
        );

        let (status, body) = response_for_path(&os, "/agents?limit=0");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "limit must be greater than 0");

        let (status, body) = response_for_path(&os, "/agents?limit=1&limit=2");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "limit must not be repeated");

        let (status, body) = response_for_path(&os, "/agents?since=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must not be empty");

        let (status, body) = response_for_path(&os, "/agents?since=not-a-time");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must be a valid RFC3339 timestamp");

        let (status, body) = response_for_path(&os, "/agents?until=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must not be empty");

        let (status, body) = response_for_path(&os, "/agents?until=not-a-time");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must be a valid RFC3339 timestamp");

        let (status, body) = response_for_path(
            &os,
            "/agents?until=2026-01-01T00:00:00Z&until=2026-01-01T00:00:01Z",
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must not be repeated");

        let (status, body) = response_for_path(&os, "/agents?query=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "query must not be empty");

        let (status, body) = response_for_path(&os, "/agents?query=latest&query=paused");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "query must not be repeated");

        let (status, body) = response_for_path(
            &os,
            "/agents?since=2026-01-01T00:00:00Z&since=2026-01-01T00:00:01Z",
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must not be repeated");

        let (status, body) = response_for_path(&os, "/agents?availability=online");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "unsupported query parameter `availability`");
    }

    #[test]
    fn tasks_endpoint_supports_status_filter_query() {
        let mut os = OperatingSystem::new("api-test");
        let base = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&chrono::Utc);
        os.create_task(Task::new(
            "Pending task",
            "Objective",
            Priority::Normal,
            vec![],
        ));
        let mut running = Task::new(
            "Running task",
            "Objective",
            Priority::High,
            vec!["rust".into()],
        );
        running.status = TaskStatus::Running;
        running.assigned_to = Some(AgentId::new("runner"));
        running.tool = Some(ToolInvocation::new(
            ToolId::new("cargo-test"),
            Default::default(),
        ));
        let running_id = running.id.clone();
        os.create_task(running);
        let mut dependent = Task::new("Dependent task", "Objective", Priority::Normal, vec![]);
        dependent.dependencies = vec![running_id.clone()];
        os.create_task(dependent);
        let mut complete = Task::new("Complete task", "Objective", Priority::High, vec![]);
        complete.status = TaskStatus::Complete;
        complete.required_capabilities = vec!["review".into()];
        complete.updated_at = base;
        os.create_task(complete);
        let mut latest_complete = Task::new(
            "Latest complete task",
            "Objective",
            Priority::High,
            vec!["review".into()],
        );
        latest_complete.status = TaskStatus::Complete;
        latest_complete.updated_at = base + chrono::Duration::seconds(1);
        os.create_task(latest_complete);
        let mut canceled_task = Task::new(
            "Canceled task",
            "Objective",
            Priority::Normal,
            vec!["ops".into()],
        );
        canceled_task.status = TaskStatus::Cancelled;
        os.create_task(canceled_task);
        let mut latest_critical = Task::new(
            "Latest critical task",
            "Objective",
            Priority::Critical,
            vec!["review".into()],
        );
        latest_critical.status = TaskStatus::Complete;
        latest_critical.updated_at = base + chrono::Duration::seconds(2);
        os.create_task(latest_critical);

        let (status, body) = response_for_path(
            &os,
            "/tasks?status=complete&priority=high&capability=review&since=2026-01-01T00:00:01Z&until=2026-01-01T00:00:01Z&query=latest&limit=1",
        );
        let tasks: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(tasks.as_array().expect("tasks").len(), 1);
        assert_eq!(tasks[0]["title"], "Latest complete task");

        let (status, body) = response_for_path(
            &os,
            "/tasks?status=complete&priority=high&capability=review",
        );
        let tasks: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(tasks.as_array().expect("tasks").len(), 2);
        assert_eq!(tasks[0]["title"], "Latest complete task");
        assert_eq!(tasks[1]["title"], "Complete task");

        let (status, body) = response_for_path(
            &os,
            "/tasks?status=completed&priority=high&capability=review",
        );
        let tasks: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(tasks.as_array().expect("tasks").len(), 2);
        assert_eq!(tasks[0]["status"], "complete");

        let (status, body) = response_for_path(&os, "/tasks?status=canceled&query=canceled");
        let tasks: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(tasks.as_array().expect("tasks").len(), 1);
        assert_eq!(tasks[0]["status"], "cancelled");

        let (status, body) = response_for_path(&os, "/tasks?priority=urgent&query=critical");
        let tasks: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(tasks.as_array().expect("tasks").len(), 1);
        assert_eq!(tasks[0]["title"], "Latest critical task");
        assert_eq!(tasks[0]["priority"], "critical");

        let (status, body) = response_for_path(
            &os,
            "/tasks?status=running&priority=high&agent=runner&tool=cargo-test&capability=rust",
        );
        let tasks: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(tasks.as_array().expect("tasks").len(), 1);
        assert_eq!(tasks[0]["title"], "Running task");

        let (status, body) = response_for_path(&os, &format!("/tasks?after={running_id}"));
        let tasks: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(tasks.as_array().expect("tasks").len(), 1);
        assert_eq!(tasks[0]["title"], "Dependent task");

        let (status, body) = response_for_path(&os, "/tasks?status=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "status must not be empty");

        let (status, body) = response_for_path(&os, "/tasks?status=waiting");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "status must be a valid task status");

        let (status, body) = response_for_path(&os, "/tasks?priority=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "priority must not be empty");

        let (status, body) = response_for_path(&os, "/tasks?priority=eventually");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "priority must be a valid priority");

        let (status, body) = response_for_path(&os, "/tasks?priority=high&priority=low");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "priority must not be repeated");

        let (status, body) = response_for_path(&os, "/tasks?agent=!!!");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            value["error"],
            "agent must contain at least one ASCII letter, digit, or hyphen"
        );

        let (status, body) = response_for_path(&os, "/tasks?tool=!!!");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            value["error"],
            "tool must contain at least one ASCII letter, digit, or hyphen"
        );

        let (status, body) = response_for_path(&os, "/tasks?after=!!!");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            value["error"],
            "after must contain at least one ASCII letter, digit, or hyphen"
        );

        let (status, body) = response_for_path(&os, "/tasks?status=pending&status=complete");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "status must not be repeated");

        let (status, body) = response_for_path(&os, "/tasks?capability=,");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            value["error"],
            "capability must include at least one capability"
        );

        let (status, body) = response_for_path(&os, "/tasks?limit=0");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "limit must be greater than 0");

        let (status, body) = response_for_path(&os, "/tasks?limit=1&limit=2");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "limit must not be repeated");

        let (status, body) = response_for_path(&os, "/tasks?since=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must not be empty");

        let (status, body) = response_for_path(&os, "/tasks?since=not-a-time");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must be a valid RFC3339 timestamp");

        let (status, body) = response_for_path(&os, "/tasks?until=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must not be empty");

        let (status, body) = response_for_path(&os, "/tasks?until=not-a-time");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must be a valid RFC3339 timestamp");

        let (status, body) = response_for_path(
            &os,
            "/tasks?until=2026-01-01T00:00:00Z&until=2026-01-01T00:00:01Z",
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must not be repeated");

        let (status, body) = response_for_path(&os, "/tasks?query=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "query must not be empty");

        let (status, body) = response_for_path(&os, "/tasks?query=latest&query=complete");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "query must not be repeated");

        let (status, body) = response_for_path(
            &os,
            "/tasks?since=2026-01-01T00:00:00Z&since=2026-01-01T00:00:01Z",
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must not be repeated");

        let (status, body) = response_for_path(&os, "/tasks?state=pending");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "unsupported query parameter `state`");
    }

    #[test]
    fn workflows_endpoint_supports_filter_queries() {
        let mut os = OperatingSystem::new("api-test");
        let base = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&chrono::Utc);
        let plan = Task::new("Plan release", "Objective", Priority::Normal, vec![]);
        let plan_id = plan.id.clone();
        os.create_task(plan);
        let build = Task::new("Build release", "Objective", Priority::Normal, vec![]);
        let build_id = build.id.clone();
        os.create_task(build);

        let mut old_workflow = Workflow::new(
            "Old release",
            Priority::Normal,
            BTreeMap::from([("plan".into(), plan_id.clone())]),
        );
        old_workflow.updated_at = base;
        os.create_workflow(old_workflow);

        let mut latest_workflow = Workflow::new(
            "Latest release",
            Priority::Critical,
            BTreeMap::from([("build".into(), build_id.clone())]),
        );
        latest_workflow.updated_at = base + chrono::Duration::seconds(1);
        os.create_workflow(latest_workflow);

        let (status, body) = response_for_path(
            &os,
            &format!(
                "/workflows?priority=critical&task={build_id}&since=2026-01-01T00:00:01Z&until=2026-01-01T00:00:01Z&query=latest&limit=1"
            ),
        );
        let workflows: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(workflows.as_array().expect("workflows").len(), 1);
        assert_eq!(workflows[0]["objective"], "Latest release");

        let (status, body) = response_for_path(&os, "/workflows?priority=urgent&query=latest");
        let workflows: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(workflows.as_array().expect("workflows").len(), 1);
        assert_eq!(workflows[0]["objective"], "Latest release");
        assert_eq!(workflows[0]["priority"], "critical");

        let (status, body) = response_for_path(&os, "/workflows?priority=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "priority must not be empty");

        let (status, body) = response_for_path(&os, "/workflows?priority=eventually");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "priority must be a valid priority");

        let (status, body) = response_for_path(&os, "/workflows?priority=high&priority=low");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "priority must not be repeated");

        let (status, body) = response_for_path(&os, "/workflows?task=!!!");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            value["error"],
            "task must contain at least one ASCII letter, digit, or hyphen"
        );

        let (status, body) = response_for_path(&os, "/workflows?limit=0");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "limit must be greater than 0");

        let (status, body) = response_for_path(&os, "/workflows?limit=1&limit=2");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "limit must not be repeated");

        let (status, body) = response_for_path(&os, "/workflows?since=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must not be empty");

        let (status, body) = response_for_path(&os, "/workflows?since=not-a-time");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must be a valid RFC3339 timestamp");

        let (status, body) = response_for_path(
            &os,
            "/workflows?until=2026-01-01T00:00:00Z&until=2026-01-01T00:00:01Z",
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must not be repeated");

        let (status, body) = response_for_path(&os, "/workflows?query=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "query must not be empty");

        let (status, body) = response_for_path(&os, "/workflows?state=pending");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "unsupported query parameter `state`");
    }

    #[test]
    fn tools_endpoint_supports_kind_filter_query() {
        let mut os = OperatingSystem::new("api-test");
        let base = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&chrono::Utc);
        os.register_tool(ToolDefinition::new(
            "Shell tool",
            ToolKind::Shell,
            "",
            vec![],
            "printf hi",
            None,
        ));
        let mut reader = ToolDefinition::new(
            "Reader",
            ToolKind::FileRead,
            "",
            vec!["docs".into()],
            "notes/{name}.txt",
            None,
        );
        reader.updated_at = base;
        os.register_tool(reader);
        let mut latest_reader = ToolDefinition::new(
            "Latest reader",
            ToolKind::FileRead,
            "",
            vec!["docs".into()],
            "notes/latest-{name}.txt",
            None,
        );
        latest_reader.updated_at = base + chrono::Duration::seconds(1);
        os.register_tool(latest_reader);
        os.register_tool(ToolDefinition::new(
            "Writer",
            ToolKind::FileWrite,
            "",
            vec!["docs".into()],
            "notes/{name}.txt",
            None,
        ));

        let (status, body) = response_for_path(
            &os,
            "/tools?kind=file-read&capability=docs&since=2026-01-01T00:00:01Z&until=2026-01-01T00:00:01Z&query=latest&limit=1",
        );
        let tools: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(tools.as_array().expect("tools").len(), 1);
        assert_eq!(tools[0]["name"], "Latest reader");

        let (status, body) = response_for_path(&os, "/tools?kind=file-read&capability=docs");
        let tools: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(tools.as_array().expect("tools").len(), 2);
        assert_eq!(tools[0]["name"], "Latest reader");
        assert_eq!(tools[1]["name"], "Reader");

        let (status, body) = response_for_path(&os, "/tools?kind=read-file&capability=docs");
        let tools: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(tools.as_array().expect("tools").len(), 2);
        assert_eq!(tools[0]["kind"], "file-read");

        let (status, body) = response_for_path(&os, "/tools?kind=write-file&capability=docs");
        let tools: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(tools.as_array().expect("tools").len(), 1);
        assert_eq!(tools[0]["kind"], "file-write");

        let (status, body) = response_for_path(&os, "/tools?kind=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "kind must not be empty");

        let (status, body) = response_for_path(&os, "/tools?kind=magic");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "kind must be a valid tool kind");

        let (status, body) = response_for_path(&os, "/tools?kind=shell&kind=file-read");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "kind must not be repeated");

        let (status, body) = response_for_path(&os, "/tools?capability=,");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            value["error"],
            "capability must include at least one capability"
        );

        let (status, body) = response_for_path(&os, "/tools?limit=0");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "limit must be greater than 0");

        let (status, body) = response_for_path(&os, "/tools?limit=1&limit=2");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "limit must not be repeated");

        let (status, body) = response_for_path(&os, "/tools?since=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must not be empty");

        let (status, body) = response_for_path(&os, "/tools?since=not-a-time");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must be a valid RFC3339 timestamp");

        let (status, body) = response_for_path(&os, "/tools?until=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must not be empty");

        let (status, body) = response_for_path(&os, "/tools?until=not-a-time");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must be a valid RFC3339 timestamp");

        let (status, body) = response_for_path(
            &os,
            "/tools?until=2026-01-01T00:00:00Z&until=2026-01-01T00:00:01Z",
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must not be repeated");

        let (status, body) = response_for_path(&os, "/tools?query=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "query must not be empty");

        let (status, body) = response_for_path(&os, "/tools?query=reader&query=latest");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "query must not be repeated");

        let (status, body) = response_for_path(
            &os,
            "/tools?since=2026-01-01T00:00:00Z&since=2026-01-01T00:00:01Z",
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must not be repeated");

        let (status, body) = response_for_path(&os, "/tools?type=shell");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "unsupported query parameter `type`");
    }

    #[test]
    fn runs_endpoint_supports_status_filter_query() {
        let mut os = OperatingSystem::new("api-test");
        let base = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&chrono::Utc);
        let task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);
        os.runs.insert(
            RunId::new(),
            RunRecord::new(task_id.clone(), None, "running-command", "."),
        );
        let mut success = RunRecord::new(
            task_id.clone(),
            Some(AgentId::new("runner")),
            "success-command",
            ".",
        );
        success.status = RunStatus::Success;
        success.exit_code = Some(0);
        success.started_at = base;
        success.finished_at = Some(success.started_at);
        os.runs.insert(success.id.clone(), success);
        let mut latest_success = RunRecord::new(
            task_id.clone(),
            Some(AgentId::new("runner")),
            "latest-success-command",
            ".",
        );
        latest_success.status = RunStatus::Success;
        latest_success.exit_code = Some(0);
        latest_success.started_at = base + chrono::Duration::seconds(1);
        latest_success.finished_at = Some(latest_success.started_at);
        os.runs.insert(latest_success.id.clone(), latest_success);
        let mut cancel_requested = RunRecord::new(
            task_id.clone(),
            Some(AgentId::new("runner")),
            "cancel-command",
            ".",
        );
        cancel_requested.status = RunStatus::CancelRequested;
        cancel_requested.started_at = base + chrono::Duration::seconds(2);
        os.runs
            .insert(cancel_requested.id.clone(), cancel_requested);

        let (status, body) = response_for_path(
            &os,
            &format!(
                "/runs?status=success&task={task_id}&agent=runner&since=2026-01-01T00:00:01Z&until=2026-01-01T00:00:01Z&query=latest&limit=1"
            ),
        );
        let runs: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(runs.as_array().expect("runs").len(), 1);
        assert_eq!(runs[0]["command"], "latest-success-command");

        let (status, body) = response_for_path(
            &os,
            &format!("/runs?status=success&task={task_id}&agent=runner"),
        );
        let runs: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(runs.as_array().expect("runs").len(), 2);
        assert_eq!(runs[0]["command"], "latest-success-command");
        assert_eq!(runs[1]["command"], "success-command");

        let (status, body) = response_for_path(
            &os,
            &format!("/runs?status=succeeded&task={task_id}&agent=runner"),
        );
        let runs: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(runs.as_array().expect("runs").len(), 2);
        assert_eq!(runs[0]["status"], "success");

        let (status, body) = response_for_path(
            &os,
            &format!("/runs?status=cancel_requested&task={task_id}&agent=runner"),
        );
        let runs: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(runs.as_array().expect("runs").len(), 1);
        assert_eq!(runs[0]["status"], "cancel-requested");

        let (status, body) = response_for_path(&os, "/runs?status=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "status must not be empty");

        let (status, body) = response_for_path(&os, "/runs?status=waiting");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "status must be a valid run status");

        let (status, body) = response_for_path(&os, "/runs?task=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "task must not be empty");

        let (status, body) = response_for_path(&os, "/runs?agent=!!!");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            value["error"],
            "agent must contain at least one ASCII letter, digit, or hyphen"
        );

        let (status, body) = response_for_path(&os, "/runs?limit=0");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "limit must be greater than 0");

        let (status, body) = response_for_path(&os, "/runs?limit=1&limit=2");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "limit must not be repeated");

        let (status, body) = response_for_path(&os, "/runs?since=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must not be empty");

        let (status, body) = response_for_path(&os, "/runs?since=not-a-time");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must be a valid RFC3339 timestamp");

        let (status, body) = response_for_path(
            &os,
            "/runs?since=2026-01-01T00:00:00Z&since=2026-01-01T00:00:01Z",
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must not be repeated");

        let (status, body) = response_for_path(&os, "/runs?until=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must not be empty");

        let (status, body) = response_for_path(&os, "/runs?until=not-a-time");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must be a valid RFC3339 timestamp");

        let (status, body) = response_for_path(
            &os,
            "/runs?until=2026-01-01T00:00:00Z&until=2026-01-01T00:00:01Z",
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must not be repeated");

        let (status, body) = response_for_path(&os, "/runs?query=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "query must not be empty");

        let (status, body) = response_for_path(&os, "/runs?query=latest&query=success");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "query must not be repeated");

        let (status, body) = response_for_path(&os, "/runs?task=one&task=two");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "task must not be repeated");

        let (status, body) = response_for_path(&os, "/runs?state=success");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "unsupported query parameter `state`");
    }

    #[test]
    fn health_endpoint_reports_validation_state() {
        let mut os = OperatingSystem::new("api-test");
        let agent = Agent::new("builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        os.register_agent(agent);

        let mut task = Task::new("Task", "Objective", Priority::Normal, vec!["rust".into()]);
        task.status = TaskStatus::Running;
        task.assigned_to = Some(agent_id);
        os.create_task(task);

        let (status, body) = response_for_path(&os, "/health");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(value["ok"], false);
        assert_eq!(value["state_valid"], false);
        assert_eq!(value["state_issue_count"], 1);
        assert!(
            value["state_issues"][0]
                .as_str()
                .expect("issue")
                .contains("missing from agent current tasks")
        );
    }

    #[test]
    fn events_endpoint_supports_recent_limit_query() {
        let mut os = OperatingSystem::new("api-test");
        let base = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&chrono::Utc);
        os.record(EventKind::TaskCreated, "old");
        os.events.last_mut().expect("old event").at = base;
        os.record(EventKind::TaskCreated, "second");
        os.events.last_mut().expect("second event").at = base + chrono::Duration::seconds(1);
        os.record(EventKind::TaskUpdated, "third");
        os.events.last_mut().expect("third event").at = base + chrono::Duration::seconds(2);

        let (status, body) = response_for_path(
            &os,
            "/events?limit=1&kind=task-created&since=2026-01-01T00:00:01Z&until=2026-01-01T00:00:01Z&query=sec",
        );
        let events: serde_json::Value = serde_json::from_str(&body).expect("json");
        let messages = events
            .as_array()
            .expect("event array")
            .iter()
            .map(|event| event["message"].as_str().expect("message"))
            .collect::<Vec<_>>();

        assert_eq!(status, "200 OK");
        assert_eq!(messages, vec!["second"]);

        let (status, body) = response_for_path(&os, "/events?kind=task-created");
        let events: serde_json::Value = serde_json::from_str(&body).expect("json");
        let messages = events
            .as_array()
            .expect("event array")
            .iter()
            .map(|event| event["message"].as_str().expect("message"))
            .collect::<Vec<_>>();

        assert_eq!(status, "200 OK");
        assert_eq!(messages, vec!["second", "old"]);

        let (status, body) = response_for_path(&os, "/events?limit=0");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "limit must be greater than 0");

        let (status, body) = response_for_path(&os, "/events?limit=1&limit=2");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "limit must not be repeated");

        let (status, body) = response_for_path(&os, "/events?kind=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "kind must not be empty");

        let (status, body) = response_for_path(&os, "/events?kind=magic");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "kind must be a valid event kind");

        let (status, body) = response_for_path(&os, "/events?kind=task-created&kind=task-updated");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "kind must not be repeated");

        let (status, body) = response_for_path(&os, "/events?since=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must not be empty");

        let (status, body) = response_for_path(&os, "/events?since=not-a-time");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must be a valid RFC3339 timestamp");

        let (status, body) = response_for_path(
            &os,
            "/events?since=2026-01-01T00:00:00Z&since=2026-01-01T00:00:01Z",
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must not be repeated");

        let (status, body) = response_for_path(&os, "/events?until=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must not be empty");

        let (status, body) = response_for_path(&os, "/events?until=not-a-time");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must be a valid RFC3339 timestamp");

        let (status, body) = response_for_path(
            &os,
            "/events?until=2026-01-01T00:00:00Z&until=2026-01-01T00:00:01Z",
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must not be repeated");

        let (status, body) = response_for_path(&os, "/events?query=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "query must not be empty");

        let (status, body) = response_for_path(&os, "/events?query=second&query=third");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "query must not be repeated");

        let (status, body) = response_for_path(&os, "/events?offset=1");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "unsupported query parameter `offset`");
    }

    #[test]
    fn memory_endpoint_supports_query_search() {
        let mut os = OperatingSystem::new("api-test");
        let base = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&chrono::Utc);
        let mut rust = MemoryRecord::new("Rust", "Scheduler notes", vec!["OpsTag".into()]);
        rust.created_at = base;
        rust.updated_at = base;
        os.write_memory(rust);
        os.write_memory(MemoryRecord::new(
            "Design",
            "UI notes",
            vec!["frontend".into()],
        ));
        let mut latest_rust =
            MemoryRecord::new("Latest Rust", "Scheduler notes", vec!["OpsTag".into()]);
        latest_rust.created_at = base + chrono::Duration::seconds(1);
        latest_rust.updated_at = base + chrono::Duration::seconds(1);
        os.write_memory(latest_rust);

        let (status, body) = response_for_path(
            &os,
            "/memory?query=opstag&tag=opsTag&since=2026-01-01T00:00:01Z&until=2026-01-01T00:00:01Z&limit=1",
        );
        let records: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(records.as_array().expect("records").len(), 1);
        assert_eq!(records[0]["topic"], "Latest Rust");

        let (status, body) = response_for_path(&os, "/memory?query=scheduler&tag=opsTag");
        let records: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(records.as_array().expect("records").len(), 2);
        assert_eq!(records[0]["topic"], "Latest Rust");
        assert_eq!(records[1]["topic"], "Rust");

        let (status, body) = response_for_path(&os, "/memory?tag=frontend");
        let records: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(records.as_array().expect("records").len(), 1);
        assert_eq!(records[0]["topic"], "Design");

        let (status, body) = response_for_path(&os, "/memory?query=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "query must not be empty");

        let (status, body) = response_for_path(&os, "/memory?query=rust&query=design");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "query must not be repeated");

        let (status, body) = response_for_path(&os, "/memory?tag=,");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "tag must include at least one tag");

        let (status, body) = response_for_path(&os, "/memory?tag=ops&tag=frontend");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "tag must not be repeated");

        let (status, body) = response_for_path(&os, "/memory?limit=0");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "limit must be greater than 0");

        let (status, body) = response_for_path(&os, "/memory?limit=1&limit=2");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "limit must not be repeated");

        let (status, body) = response_for_path(&os, "/memory?since=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must not be empty");

        let (status, body) = response_for_path(&os, "/memory?since=not-a-time");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must be a valid RFC3339 timestamp");

        let (status, body) = response_for_path(&os, "/memory?until=%20%20");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must not be empty");

        let (status, body) = response_for_path(&os, "/memory?until=not-a-time");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must be a valid RFC3339 timestamp");

        let (status, body) = response_for_path(
            &os,
            "/memory?until=2026-01-01T00:00:00Z&until=2026-01-01T00:00:01Z",
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "until must not be repeated");

        let (status, body) = response_for_path(
            &os,
            "/memory?since=2026-01-01T00:00:00Z&since=2026-01-01T00:00:01Z",
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "since must not be repeated");
    }

    #[test]
    fn memory_update_can_clear_tags_and_reject_conflicting_tags() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("api-test");
        let memory = MemoryRecord::new("Ops", "Tag maintenance", vec!["ops".into()]);
        let memory_id = memory.id.clone();
        os.write_memory(memory);
        store.save_unchecked(&os).expect("save state");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/memory/{memory_id}"),
            br#"{"clear_tags":true}"#,
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(value["memory"]["tags"], serde_json::json!([]));

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/memory/{memory_id}"),
            br#"{"tags":["ops"],"clear_tags":true}"#,
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "use either tags or clear_tags, not both");
        assert_eq!(
            store
                .load()
                .expect("load")
                .memory
                .iter()
                .find(|record| record.id == memory_id)
                .expect("memory")
                .tags,
            Vec::<String>::new()
        );
    }

    #[test]
    fn tool_update_can_clear_description_and_reject_conflicting_description() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("api-test");
        let tool = ToolDefinition::new(
            "Say",
            ToolKind::Shell,
            "Old description",
            vec![],
            "printf hi",
            None,
        );
        let tool_id = tool.id.clone();
        os.register_tool(tool);
        store.save_unchecked(&os).expect("save state");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/tools/{tool_id}"),
            br#"{"clear_description":true}"#,
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(value["tool"]["description"], "");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/tools/{tool_id}"),
            br#"{"description":"New description","clear_description":true}"#,
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            value["error"],
            "clear_description cannot be combined with description"
        );
        assert_eq!(
            store
                .load()
                .expect("load")
                .tools
                .get(&tool_id)
                .expect("tool")
                .description,
            ""
        );
    }

    #[test]
    fn task_update_can_clear_required_capabilities_and_reject_conflicting_capabilities() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("api-test");
        let task = Task::new(
            "Build",
            "Compile the project",
            Priority::Normal,
            vec!["rust".into()],
        );
        let task_id = task.id.clone();
        os.create_task(task);
        store.save(&os).expect("save state");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/tasks/{task_id}"),
            br#"{"clear_required_capabilities":true}"#,
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(
            value["task"]["required_capabilities"],
            serde_json::json!([])
        );

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/tasks/{task_id}"),
            br#"{"required_capabilities":["rust"],"clear_required_capabilities":true}"#,
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            value["error"],
            "clear_required_capabilities cannot be combined with required_capabilities"
        );
        assert_eq!(
            store
                .load()
                .expect("load")
                .tasks
                .get(&task_id)
                .expect("task")
                .required_capabilities,
            Vec::<String>::new()
        );
    }

    #[test]
    fn tool_update_can_clear_required_capabilities_and_reject_conflicting_capabilities() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("api-test");
        let tool = ToolDefinition::new(
            "Say",
            ToolKind::Shell,
            "Needs rust",
            vec!["rust".into()],
            "printf hi",
            None,
        );
        let tool_id = tool.id.clone();
        os.register_tool(tool);
        store.save(&os).expect("save state");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/tools/{tool_id}"),
            br#"{"clear_required_capabilities":true}"#,
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(
            value["tool"]["required_capabilities"],
            serde_json::json!([])
        );

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/tools/{tool_id}"),
            br#"{"required_capabilities":["rust"],"clear_required_capabilities":true}"#,
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            value["error"],
            "clear_required_capabilities cannot be combined with required_capabilities"
        );
        assert_eq!(
            store
                .load()
                .expect("load")
                .tools
                .get(&tool_id)
                .expect("tool")
                .required_capabilities,
            Vec::<String>::new()
        );
    }

    #[test]
    fn unknown_endpoint_returns_404() {
        let os = OperatingSystem::new("api-test");
        let (status, body) = response_for_path(&os, "/missing");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "404 Not Found");
        assert_eq!(value["error"], "not found");
    }

    #[test]
    fn oversized_headers_are_rejected_when_delimiter_arrives_in_same_read() {
        let request = format!(
            "GET /status HTTP/1.1\r\nx-large: {}\r\n\r\n",
            "a".repeat(MAX_HTTP_HEADER_BYTES)
        );
        let mut reader = Cursor::new(request.into_bytes());

        let error = match read_http_request(&mut reader) {
            Ok(_) => panic!("oversized headers should fail"),
            Err(error) => error,
        };

        assert!(matches!(error, HttpRequestError::HeaderTooLarge { .. }));
    }

    #[test]
    fn conflicting_content_length_headers_are_rejected() {
        let request = b"POST /memory HTTP/1.1\r\ncontent-length: 0\r\ncontent-length: 2\r\n\r\n{}";
        let mut reader = Cursor::new(request.as_slice());

        let error = match read_http_request(&mut reader) {
            Ok(_) => panic!("conflicting content-length headers should fail"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            HttpRequestError::ConflictingContentLength {
                first: 0,
                second: 2
            }
        ));
    }

    #[test]
    fn mutation_validation_failures_return_conflict() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("api-test");
        os.name = " ".into();
        store.save_unchecked(&os).expect("save invalid state");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/tasks",
            br#"{"title":"Task","required_capabilities":[]}"#,
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "409 Conflict");
        assert_eq!(value["error"], "state conflict");
        assert!(
            value["detail"]
                .as_str()
                .expect("detail")
                .contains("state validation failed")
        );
        assert!(store.load().expect("load").tasks.is_empty());
    }

    #[test]
    fn create_agent_requires_capabilities_before_mutating_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        store
            .save(&OperatingSystem::new("api-test"))
            .expect("save state");

        let (status, body) =
            response_for_mutation(&store, "POST", "/agents", br#"{"name":"builder"}"#);
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            value["error"],
            "capabilities must include at least one capability"
        );
        assert!(store.load().expect("load").agents.is_empty());
    }

    #[test]
    fn no_op_mutation_error_does_not_rewrite_state_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let os = OperatingSystem::new("api-test");
        let minified = serde_json::to_string(&os).expect("json");
        fs::write(store.path(), &minified).expect("write minified state");

        let (status, body) = response_for_mutation(&store, "DELETE", "/tasks/missing", b"");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "404 Not Found");
        assert!(
            value["error"]
                .as_str()
                .expect("error")
                .contains("task not found")
        );
        assert_eq!(
            fs::read_to_string(store.path()).expect("state body"),
            minified
        );
    }

    #[test]
    fn optional_mutation_bodies_default_when_omitted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("api-test");
        let agent = Agent::new("builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        os.register_agent(agent);
        let task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);
        let failed_task = Task::new("Fail task", "Objective", Priority::Normal, vec![]);
        let failed_task_id = failed_task.id.clone();
        os.create_task(failed_task);
        let blocked_task = Task::new("Block task", "Objective", Priority::Normal, vec![]);
        let blocked_task_id = blocked_task.id.clone();
        os.create_task(blocked_task);
        let cancelled_task = Task::new("Cancel task", "Objective", Priority::Normal, vec![]);
        let cancelled_task_id = cancelled_task.id.clone();
        os.create_task(cancelled_task);
        store.save_unchecked(&os).expect("save");

        let (heartbeat_status, heartbeat_body) = response_for_mutation(
            &store,
            "POST",
            &format!("/agents/{agent_id}/heartbeat"),
            b"",
        );
        let heartbeat: serde_json::Value = serde_json::from_str(&heartbeat_body).expect("json");
        assert_eq!(heartbeat_status, "200 OK");
        assert_eq!(heartbeat["agent"]["status"], "online");

        let (complete_status, complete_body) =
            response_for_mutation(&store, "POST", &format!("/tasks/{task_id}/complete"), b"");
        let complete: serde_json::Value = serde_json::from_str(&complete_body).expect("json");
        assert_eq!(complete_status, "200 OK");
        assert_eq!(complete["task"]["status"], "complete");
        assert_eq!(complete["task"]["output"], serde_json::Value::Null);

        for (task_id, endpoint, status) in [
            (&failed_task_id, "fail", TaskStatus::Failed),
            (&blocked_task_id, "block", TaskStatus::Blocked),
            (&cancelled_task_id, "cancel", TaskStatus::Cancelled),
        ] {
            let (mutation_status, mutation_body) =
                response_for_mutation(&store, "POST", &format!("/tasks/{task_id}/{endpoint}"), b"");
            let mutation: serde_json::Value = serde_json::from_str(&mutation_body).expect("json");
            assert_eq!(mutation_status, "200 OK");
            assert_eq!(mutation["task"]["status"], status.to_string());
            assert_eq!(mutation["task"]["output"], serde_json::Value::Null);
        }

        let (retry_status, retry_body) = response_for_mutation(
            &store,
            "POST",
            &format!("/tasks/{failed_task_id}/retry"),
            b"",
        );
        let retry: serde_json::Value = serde_json::from_str(&retry_body).expect("json");
        assert_eq!(retry_status, "200 OK");
        assert_eq!(retry["task"]["status"], "pending");
        assert_eq!(retry["task"]["output"], serde_json::Value::Null);

        let (unblock_status, unblock_body) = response_for_mutation(
            &store,
            "POST",
            &format!("/tasks/{blocked_task_id}/unblock"),
            b"",
        );
        let unblock: serde_json::Value = serde_json::from_str(&unblock_body).expect("json");
        assert_eq!(unblock_status, "200 OK");
        assert_eq!(unblock["task"]["status"], "pending");
        assert_eq!(unblock["task"]["output"], serde_json::Value::Null);
    }

    #[test]
    fn claim_unknown_agent_returns_not_found_without_mutating_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let os = OperatingSystem::new("api-test");
        let minified = serde_json::to_string(&os).expect("json");
        fs::write(store.path(), &minified).expect("write minified state");

        let (status, body) = response_for_mutation(&store, "POST", "/agents/missing/claim", b"");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "404 Not Found");
        assert!(
            value["error"]
                .as_str()
                .expect("error")
                .contains("agent not found")
        );
        assert_eq!(
            fs::read_to_string(store.path()).expect("state body"),
            minified
        );
    }

    #[test]
    fn claim_drops_expired_lease_when_request_omits_lease() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("api-test");
        let mut agent = Agent::new("builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        agent.last_heartbeat_at = Some(chrono::Utc::now() - chrono::Duration::seconds(120));
        agent.lease_expires_at = Some(chrono::Utc::now() - chrono::Duration::seconds(60));
        os.register_agent(agent);
        os.create_task(Task::new(
            "Claim after expired lease",
            "Agent claim should not preserve an expired lease.",
            Priority::Normal,
            vec!["rust".into()],
        ));
        store.save_unchecked(&os).expect("save state");

        let (status, body) =
            response_for_mutation(&store, "POST", &format!("/agents/{agent_id}/claim"), b"");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        let loaded = store.load().expect("load");
        let agent = loaded.agents.get(&agent_id).expect("agent");

        assert_eq!(status, "200 OK");
        assert_eq!(value["claimed"], true);
        assert_eq!(value["assignment"]["agent_id"], agent_id.to_string());
        assert!(agent.lease_expires_at.is_none());
        assert!(validate_state(&loaded).valid);
    }

    #[test]
    fn cancel_run_clears_active_run_terminal_fields() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("api-test");
        let task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);
        let mut run = RunRecord::new(task_id, None, "command", ".");
        let run_id = run.id.clone();
        run.finished_at = Some(chrono::Utc::now());
        run.exit_code = Some(0);
        os.runs.insert(run_id.clone(), run);
        store.save_unchecked(&os).expect("save");

        let (status, body) =
            cancel_run_response(&store, &run_id.to_string()).expect("cancel response");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        let loaded = store.load().expect("load");
        let run = loaded.runs.get(&run_id).expect("run");

        assert_eq!(status, "200 OK");
        assert_eq!(value["cancel_requested"], true);
        assert_eq!(run.status, RunStatus::CancelRequested);
        assert!(run.finished_at.is_none());
        assert!(run.exit_code.is_none());
        assert!(validate_state(&loaded).valid);
    }
}
