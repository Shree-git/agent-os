use crate::config::{
    AppConfig, ConfigError, ConfigProfile, load_config, validate_seed_config, write_profile_config,
};
use crate::executor::CommandExecutor;
use crate::git_integration::{
    GitIntegrationError, resolve_git_cwd, run_git_capture, run_git_command,
};
use crate::json_schema::validate_json_schema;
use crate::migrations::CURRENT_STATE_VERSION;
use crate::models::{
    Agent, AgentId, AgentKind, AgentProfile, AgentStatus, ApprovalStatus, AutonomyLevel,
    EvalRecord, EvalRunDetails, EventKind, MAX_PROVIDER_RETRIES, McpServer, MemoryRecord,
    MemoryVisibility, NetworkMode, OperatingSystem, Priority, ProviderKind, RunArtifact,
    RunArtifactKind, RunId, RunRecord, RunStatus, SecretsBackend, SecretsBackendKind, Task, TaskId,
    TaskStatus, ToolDefinition, ToolId, ToolInvocation, ToolKind, WorkerNode, Workflow, WorkflowId,
    WorkflowTemplate, WorkflowTemplateEdge, memory_recall_hit, memory_relevance_score,
    normalize_list, render_workflow_template_text, secret_check_report, tail_text_by_bytes,
    text_tail_was_truncated, workflow_template_edges, workflow_template_task,
};
use crate::policy::{check_shell_command, check_shell_writes, check_workspace};
use crate::runtime::{AgentUpdate, Runtime, RuntimeError, TaskUpdate, ToolUpdate};
use crate::scheduler::Scheduler;
use crate::secrets::is_valid_secret_reference;
use crate::service::{
    LaunchdServiceOptions, ServiceError, SystemdServiceOptions,
    build_launchd_service as build_launchd_service_definition,
    build_systemd_service as build_systemd_service_definition, default_launchd_plist_path,
    install_launchd_service as install_launchd_service_definition,
    install_systemd_service as install_systemd_service_definition, resolve_launchd_domain,
    run_launchctl, run_systemctl, uninstall_launchd_service, uninstall_systemd_service,
    validate_service_control_inputs, validate_systemd_control_inputs,
};
use crate::shell_capture::run_shell_capture;
use crate::sqlite_store::{SqliteStore, SqliteStoreError};
use crate::store::{Store, StoreError};
use crate::tools::{validate_tool_invocation, validate_tool_template};
use crate::validation::{repair_state, validate_state};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as FmtWrite;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

const MAX_HTTP_HEADER_BYTES: usize = 64 * 1024;
const MAX_HTTP_BODY_BYTES: usize = 1024 * 1024;
const HTTP_READ_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

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
    #[error("http headers must be valid utf-8")]
    InvalidHeaderUtf8,
    #[error("http headers exceeded {limit} bytes")]
    HeaderTooLarge { limit: usize },
    #[error("malformed http header line: {line}")]
    MalformedHeaderLine { line: String },
    #[error("invalid http header name: {name}")]
    InvalidHeaderName { name: String },
    #[error("duplicate host header")]
    DuplicateHostHeader,
    #[error("unsupported transfer-encoding header: {value}")]
    UnsupportedTransferEncoding { value: String },
    #[error("invalid content-length header: {value}")]
    InvalidContentLength { value: String },
    #[error("duplicate content-length header")]
    DuplicateContentLength,
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
    auth: ApiAuth,
    cors: ApiCors,
    config_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Default)]
pub struct ApiAuth {
    pub full_token: Option<String>,
    pub read_token: Option<String>,
    pub write_token: Option<String>,
}

impl ApiAuth {
    pub fn bearer(token: Option<String>) -> Self {
        Self {
            full_token: token,
            ..Self::default()
        }
    }

    pub fn scoped(
        full_token: Option<String>,
        read_token: Option<String>,
        write_token: Option<String>,
    ) -> Self {
        Self {
            full_token,
            read_token,
            write_token,
        }
    }

    fn is_disabled(&self) -> bool {
        self.full_token.is_none() && self.read_token.is_none() && self.write_token.is_none()
    }
}

#[derive(Clone, Debug, Default)]
pub struct ApiCors {
    pub allowed_origins: Vec<String>,
}

impl ApiCors {
    pub fn allow_origins(origins: Vec<String>) -> Self {
        Self {
            allowed_origins: origins
                .into_iter()
                .filter_map(|origin| normalize_cors_origin(&origin))
                .collect(),
        }
    }
}

#[derive(Clone)]
struct ApiHandler {
    store: Store,
    auth: ApiAuth,
    cors: ApiCors,
    config_path: Option<PathBuf>,
}

impl ApiServer {
    pub fn bind(
        store: Store,
        addr: &str,
        max_requests: Option<usize>,
        bearer_token: Option<String>,
    ) -> Result<Self, ApiError> {
        Self::bind_with_auth(store, addr, max_requests, ApiAuth::bearer(bearer_token))
    }

    pub fn bind_with_auth(
        store: Store,
        addr: &str,
        max_requests: Option<usize>,
        auth: ApiAuth,
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
            auth,
            cors: ApiCors::default(),
            config_path: None,
        })
    }

    pub fn bind_with_auth_and_cors(
        store: Store,
        addr: &str,
        max_requests: Option<usize>,
        auth: ApiAuth,
        cors: ApiCors,
    ) -> Result<Self, ApiError> {
        let mut server = Self::bind_with_auth(store, addr, max_requests, auth)?;
        server.cors = cors;
        Ok(server)
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

    pub fn bind_with_auth_config_path(
        store: Store,
        addr: &str,
        max_requests: Option<usize>,
        auth: ApiAuth,
        config_path: PathBuf,
    ) -> Result<Self, ApiError> {
        let mut server = Self::bind_with_auth(store, addr, max_requests, auth)?;
        server.config_path = Some(config_path);
        Ok(server)
    }

    pub fn bind_with_auth_config_path_and_cors(
        store: Store,
        addr: &str,
        max_requests: Option<usize>,
        auth: ApiAuth,
        config_path: PathBuf,
        cors: ApiCors,
    ) -> Result<Self, ApiError> {
        let mut server = Self::bind_with_auth_and_cors(store, addr, max_requests, auth, cors)?;
        server.config_path = Some(config_path);
        Ok(server)
    }

    pub fn local_addr(&self) -> Result<SocketAddr, ApiError> {
        Ok(self.listener.local_addr()?)
    }

    pub fn serve(&self) -> Result<(), ApiError> {
        let handler = ApiHandler {
            store: self.store.clone(),
            auth: self.auth.clone(),
            cors: self.cors.clone(),
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
        let trace_id = Uuid::new_v4().to_string();
        stream.set_read_timeout(Some(HTTP_READ_TIMEOUT))?;
        stream.set_write_timeout(Some(HTTP_WRITE_TIMEOUT))?;
        let request = match read_http_request(&mut stream) {
            Ok(request) => request,
            Err(HttpRequestError::Io(error)) => return Err(ApiError::Io(error)),
            Err(error) => {
                let (status, body) = http_request_error_response(&error);
                write_http_response(&mut stream, status, &body, None, &trace_id)?;
                return Ok(());
            }
        };
        let (method, path) = match parse_http_request_line(request.request_line.as_deref()) {
            Ok(parts) => parts,
            Err(error) => {
                let (status, body) = http_request_error_response(&error);
                write_http_response(&mut stream, status, &body, None, &trace_id)?;
                return Ok(());
            }
        };

        let origin = match allowed_cors_origin(&request.headers, &self.cors) {
            Ok(origin) => origin,
            Err((status, body)) => {
                write_http_response(&mut stream, status, &body, None, &trace_id)?;
                return Ok(());
            }
        };

        let auth = if method == "OPTIONS" {
            AuthDecision::Allowed
        } else {
            self.authorize(method, &request.headers)
        };

        let (status, body) = if method == "OPTIONS" {
            ("204 No Content", String::new())
        } else if auth == AuthDecision::MissingOrInvalid {
            (
                "401 Unauthorized",
                json!({
                    "error": "unauthorized",
                })
                .to_string(),
            )
        } else if auth == AuthDecision::InsufficientScope {
            (
                "403 Forbidden",
                json!({
                    "error": "forbidden",
                    "detail": "bearer token does not grant the required API scope",
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
                    metrics_response_body(metrics_unavailable_json(error.to_string()), path),
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
        let content_type = response_content_type(method, path, status);
        write_http_response_with_content_type(
            &mut stream,
            status,
            &body,
            origin.as_deref(),
            content_type,
            &trace_id,
        )?;
        Ok(())
    }

    fn authorize(&self, method: &str, headers: &str) -> AuthDecision {
        if self.auth.is_disabled() {
            return AuthDecision::Allowed;
        }
        let Some(candidate) = bearer_candidate(headers) else {
            return AuthDecision::MissingOrInvalid;
        };
        if token_matches(self.auth.full_token.as_deref(), candidate)
            || (method == "GET" && token_matches(self.auth.read_token.as_deref(), candidate))
            || (matches!(method, "POST" | "DELETE")
                && token_matches(self.auth.write_token.as_deref(), candidate))
        {
            return AuthDecision::Allowed;
        }
        if token_matches(self.auth.read_token.as_deref(), candidate)
            || token_matches(self.auth.write_token.as_deref(), candidate)
        {
            AuthDecision::InsufficientScope
        } else {
            AuthDecision::MissingOrInvalid
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuthDecision {
    Allowed,
    MissingOrInvalid,
    InsufficientScope,
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
    let headers = std::str::from_utf8(&bytes[..header_end])
        .map_err(|_| HttpRequestError::InvalidHeaderUtf8)?
        .to_owned();
    let request_line = headers.lines().next().map(str::to_owned);
    parse_http_request_line(request_line.as_deref())?;
    validate_http_headers(&headers)?;
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
                return Err(HttpRequestError::DuplicateContentLength);
            } else {
                parsed = Some(content_length);
            }
        }
    }
    Ok(parsed.unwrap_or(0))
}

fn validate_http_headers(headers: &str) -> Result<(), HttpRequestError> {
    let mut host_seen = false;
    for line in headers.lines().skip(1) {
        if line.is_empty() {
            continue;
        }
        let Some((name, _value)) = line.split_once(':') else {
            return Err(HttpRequestError::MalformedHeaderLine {
                line: line.to_owned(),
            });
        };
        if !valid_http_header_name(name) {
            return Err(HttpRequestError::InvalidHeaderName {
                name: name.to_owned(),
            });
        }
        if name.eq_ignore_ascii_case("host") {
            if host_seen {
                return Err(HttpRequestError::DuplicateHostHeader);
            }
            host_seen = true;
        }
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err(HttpRequestError::UnsupportedTransferEncoding {
                value: _value.trim().to_owned(),
            });
        }
    }
    Ok(())
}

fn valid_http_header_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~'))
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
    header_values(headers, name).into_iter().next()
}

fn header_values<'a>(headers: &'a str, name: &str) -> Vec<&'a str> {
    headers
        .lines()
        .skip(1)
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name).then_some(value.trim())
        })
        .collect()
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
    if !valid_http_header_name(method) {
        return Err(HttpRequestError::MalformedRequestLine {
            line: line.to_owned(),
        });
    }
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
    matches!(
        path.trim_end_matches('/'),
        "/metrics" | "/metrics/prometheus"
    )
}

fn is_prometheus_metrics_path(path: &str) -> bool {
    let (path, _) = path.split_once('?').unwrap_or((path, ""));
    path.trim_end_matches('/') == "/metrics/prometheus"
}

fn is_dashboard_html_path(path: &str) -> bool {
    let (path, _) = path.split_once('?').unwrap_or((path, ""));
    path.trim_end_matches('/') == "/dashboard.html"
}

fn response_content_type(method: &str, path: &str, status: &str) -> &'static str {
    if method == "GET"
        && is_prometheus_metrics_path(path)
        && (status.starts_with("200 ") || status.starts_with("503 "))
    {
        "text/plain; version=0.0.4; charset=utf-8"
    } else if method == "GET" && is_dashboard_html_path(path) && status.starts_with("200 ") {
        "text/html; charset=utf-8"
    } else {
        "application/json"
    }
}

fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    body: &str,
    allowed_origin: Option<&str>,
    trace_id: &str,
) -> Result<(), ApiError> {
    write_http_response_with_content_type(
        stream,
        status,
        body,
        allowed_origin,
        "application/json",
        trace_id,
    )
}

fn write_http_response_with_content_type(
    stream: &mut TcpStream,
    status: &str,
    body: &str,
    allowed_origin: Option<&str>,
    content_type: &str,
    trace_id: &str,
) -> Result<(), ApiError> {
    let auth_challenge = if status.starts_with("401 ") {
        "www-authenticate: Bearer\r\n"
    } else {
        ""
    };
    let cors_headers = allowed_origin
        .map(|origin| {
            format!(
                "vary: Origin\r\naccess-control-allow-origin: {origin}\r\naccess-control-allow-methods: GET, POST, DELETE, OPTIONS\r\naccess-control-allow-headers: authorization, content-type\r\n"
            )
        })
        .unwrap_or_else(|| "vary: Origin\r\n".to_owned());
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncache-control: no-store\r\nx-content-type-options: nosniff\r\nx-trace-id: {trace_id}\r\nallow: GET, POST, DELETE, OPTIONS\r\n{cors_headers}{auth_challenge}content-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes())?;
    Ok(())
}

fn allowed_cors_origin(
    headers: &str,
    cors: &ApiCors,
) -> Result<Option<String>, (&'static str, String)> {
    let origins = header_values(headers, "origin");
    if origins.len() > 1 {
        return Err((
            "400 Bad Request",
            json!({
                "error": "origin must not be repeated",
            })
            .to_string(),
        ));
    }
    let Some(origin) = origins.first().copied() else {
        return Ok(None);
    };
    let origin = origin.trim();
    if is_allowed_local_origin(origin)
        || cors.allowed_origins.iter().any(|allowed| allowed == origin)
    {
        return Ok(Some(origin.to_owned()));
    }
    Err((
        "403 Forbidden",
        json!({
            "error": "origin not allowed",
        })
        .to_string(),
    ))
}

pub fn normalize_cors_origin(origin: &str) -> Option<String> {
    let origin = origin.trim();
    let url = url::Url::parse(origin).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return None;
    }
    let host = url.host_str()?;
    let mut normalized = format!("{}://{}", url.scheme(), host);
    if host.contains(':') && !host.starts_with('[') {
        normalized = format!("{}://[{}]", url.scheme(), host);
    }
    if let Some(port) = url.port() {
        normalized.push(':');
        normalized.push_str(&port.to_string());
    }
    Some(normalized)
}

fn is_allowed_local_origin(origin: &str) -> bool {
    let Some(origin) = normalize_cors_origin(origin) else {
        return false;
    };
    let Ok(url) = url::Url::parse(&origin) else {
        return false;
    };
    let host = url
        .host_str()
        .map(|host| host.trim_start_matches('[').trim_end_matches(']'))
        .unwrap_or_default();
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

fn bearer_candidate(headers: &str) -> Option<&str> {
    let authorization = header_values(headers, "authorization");
    if authorization.len() != 1 {
        return None;
    }
    authorization.first().and_then(|value| {
        let (scheme, candidate) = value.trim().split_once(' ')?;
        scheme
            .eq_ignore_ascii_case("bearer")
            .then(|| candidate.trim())
    })
}

fn token_matches(token: Option<&str>, candidate: &str) -> bool {
    token
        .map(|token| constant_time_eq(candidate.as_bytes(), token.as_bytes()))
        .unwrap_or(false)
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |acc, (left, right)| acc | (left ^ right))
        == 0
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
        | HttpRequestError::InvalidHeaderUtf8
        | HttpRequestError::MalformedHeaderLine { .. }
        | HttpRequestError::InvalidHeaderName { .. }
        | HttpRequestError::DuplicateHostHeader
        | HttpRequestError::UnsupportedTransferEncoding { .. }
        | HttpRequestError::InvalidContentLength { .. }
        | HttpRequestError::DuplicateContentLength
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
        "/metrics/prometheus" => {
            return ("200 OK", metrics_prometheus(&metrics_json(os)));
        }
        "/dashboard" => dashboard_json(os),
        "/dashboard.html" => {
            return ("200 OK", dashboard_html(os));
        }
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
        "/workers" => match workers_json(os, query) {
            Ok(workers) => workers,
            Err(response) => return response,
        },
        "/evals" => match evals_json(os, query) {
            Ok(evals) => evals,
            Err(response) => return response,
        },
        "/git/status" => match git_status_json(query) {
            Ok(status) => status,
            Err(response) => return response,
        },
        "/secrets" => match secrets_json(os, query) {
            Ok(backends) => backends,
            Err(response) => return response,
        },
        "/secrets/check" => json!(secret_check_report(os)),
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
        "/memory/recall" => match memory_recall_json(os, query) {
            Ok(memory) => memory,
            Err(response) => return response,
        },
        "/approvals" => json!(os.approvals.values().collect::<Vec<_>>()),
        "/registry" => registry_json(os),
        "/registry/profiles" => json!(&os.agent_profiles),
        "/registry/templates" => json!(&os.workflow_templates),
        "/registry/mcp-servers" => json!(&os.mcp_servers),
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

fn dashboard_json(os: &OperatingSystem) -> serde_json::Value {
    json!({
        "status": status_json(os),
        "metrics": metrics_json(os),
        "daemon": &os.daemon,
        "agents": os.agents.values().collect::<Vec<_>>(),
        "tasks": os.tasks.values().collect::<Vec<_>>(),
        "workflows": os.workflows.values().map(|workflow| {
            os.workflow_progress(&workflow.id)
        }).collect::<Vec<_>>(),
        "workflow_dags": os.workflows.values().map(|workflow| {
            workflow_dag_value(os, workflow)
        }).collect::<Vec<_>>(),
        "runs": os.runs.values().collect::<Vec<_>>(),
        "approvals": os.approvals.values().collect::<Vec<_>>(),
        "workers": os.workers.values().collect::<Vec<_>>(),
        "evals": &os.evals,
        "memory": &os.memory,
        "recent_events": os.events.iter().rev().take(50).collect::<Vec<_>>(),
    })
}

fn dashboard_html(os: &OperatingSystem) -> String {
    let metrics = metrics_json(os);
    let mut body = String::new();
    let _ = write!(
        body,
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>Agent OS Dashboard</title><style>{}</style></head><body>",
        dashboard_css()
    );
    let _ = write!(
        body,
        "<header><div><p>Agent OS</p><h1>{}</h1></div><dl><div><dt>Agents</dt><dd>{}</dd></div><div><dt>Tasks</dt><dd>{}</dd></div><div><dt>Runs</dt><dd>{}</dd></div><div><dt>Memory</dt><dd>{}</dd></div></dl></header>",
        escape_html(&os.name),
        os.agents.len(),
        os.tasks.len(),
        os.runs.len(),
        os.memory.len()
    );
    body.push_str("<main>");
    body.push_str("<section><h2>Metrics</h2><div class=\"metric-grid\">");
    for key in [
        "tasks_pending",
        "tasks_running",
        "tasks_complete",
        "runs_running",
        "runs_success",
        "runs_failed",
        "approvals_pending",
        "events_total",
    ] {
        let value = metrics.get(key).cloned().unwrap_or(Value::Null);
        let _ = write!(
            body,
            "<article><span>{}</span><strong>{}</strong></article>",
            escape_html(&key.replace('_', " ")),
            escape_html(&dashboard_metric_value(&value))
        );
    }
    body.push_str("</div></section>");

    body.push_str("<section class=\"wide\"><h2>Agents</h2><div class=\"worker-actions\"><form data-dashboard-form data-endpoint=\"/agents\"><h4>Create Agent</h4><label>Name<input name=\"name\" required></label><label>Kind<input name=\"kind\" value=\"builder\" required></label><label>Model<input name=\"model\" placeholder=\"gpt-5.2\"></label><label>Capabilities<input name=\"capabilities\" data-list=\"true\" placeholder=\"rust,review\"></label><label>Parallel<input name=\"parallel\" type=\"number\" min=\"1\" value=\"1\" data-number=\"true\"></label><button type=\"submit\">Create Agent</button><output></output></form><form data-dashboard-query data-endpoint-template=\"/agents/{agent_id}\"><h4>Inspect Agent</h4><label>Agent ID<input name=\"agent_id\" data-path=\"true\" required></label><button type=\"submit\">Inspect Agent</button><output></output></form><form data-dashboard-form data-endpoint-template=\"/agents/{agent_id}\"><h4>Update Agent</h4><label>Agent ID<input name=\"agent_id\" data-path=\"true\" required></label><label>Name<input name=\"name\"></label><label>Kind<input name=\"kind\" placeholder=\"builder\"></label><label>Model<input name=\"model\"></label><label>Clear model<select name=\"clear_model\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Capabilities<input name=\"capabilities\" data-list=\"true\" placeholder=\"rust,review\"></label><label>Parallel<input name=\"parallel\" type=\"number\" min=\"1\" data-number=\"true\"></label><button type=\"submit\">Update Agent</button><output></output></form><form data-dashboard-form data-endpoint-template=\"/agents/{agent_id}/heartbeat\"><h4>Heartbeat Agent</h4><label>Agent ID<input name=\"agent_id\" data-path=\"true\" required></label><label>Status<select name=\"status\"><option value=\"online\">Online</option><option value=\"busy\">Busy</option><option value=\"paused\">Paused</option><option value=\"offline\">Offline</option></select></label><label>Lease seconds<input name=\"lease_seconds\" type=\"number\" min=\"1\" data-number=\"true\"></label><button type=\"submit\">Heartbeat Agent</button><output></output></form><form data-dashboard-form data-dashboard-result=\"json\" data-endpoint-template=\"/agents/{agent_id}/claim\"><h4>Claim Agent Task</h4><label>Agent ID<input name=\"agent_id\" data-path=\"true\" required></label><label>Lease seconds<input name=\"lease_seconds\" type=\"number\" min=\"1\" data-number=\"true\"></label><button type=\"submit\">Claim Agent Task</button><output></output></form><form data-dashboard-form data-method=\"DELETE\" data-endpoint-template=\"/agents/{agent_id}\"><h4>Remove Agent</h4><label>Agent ID<input name=\"agent_id\" data-path=\"true\" required></label><button class=\"deny\" type=\"submit\">Remove Agent</button><output></output></form></div><table><thead><tr><th>Name</th><th>Kind</th><th>Status</th><th>Load</th></tr></thead><tbody>");
    for agent in os.agents.values() {
        let _ = write!(
            body,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}/{}</td></tr>",
            escape_html(&agent.name),
            escape_html(&agent.kind.to_string()),
            escape_html(&agent.status.to_string()),
            agent.current_tasks.len(),
            agent.max_parallel_tasks
        );
    }
    body.push_str("</tbody></table></section>");

    body.push_str("<section class=\"wide\"><h2>Tasks</h2><div class=\"worker-actions\"><form data-dashboard-form data-endpoint=\"/tasks\"><h4>Create Task</h4><label>Title<input name=\"title\" required></label><label>Objective<input name=\"objective\"></label><label>Command<input name=\"command\"></label><label>CWD<input name=\"cwd\"></label><label>Tool<input name=\"tool\"></label><label>Args JSON<textarea name=\"args\" data-json=\"true\" placeholder='{\"name\":\"value\"}'></textarea></label><label>Secret args JSON<textarea name=\"secret_args\" data-json=\"true\" placeholder='{\"token\":\"AGENT_OS_TOKEN\"}'></textarea></label><label>Priority<select name=\"priority\"><option value=\"normal\">Normal</option><option value=\"low\">Low</option><option value=\"high\">High</option><option value=\"critical\">Critical</option><option value=\"urgent\">Urgent</option></select></label><label>Capabilities<input name=\"required_capabilities\" data-list=\"true\" placeholder=\"rust,review\"></label><label>Dependencies<input name=\"dependencies\" data-list=\"true\" placeholder=\"task-a,task-b\"></label><label>Max attempts<input name=\"max_attempts\" type=\"number\" min=\"1\" data-number=\"true\"></label><button type=\"submit\">Create Task</button><output></output></form><form data-dashboard-query data-endpoint-template=\"/tasks/{task_id}\"><h4>Inspect Task</h4><label>Task ID<input name=\"task_id\" data-path=\"true\" required></label><button type=\"submit\">Inspect Task</button><output></output></form><form data-dashboard-form data-endpoint-template=\"/tasks/{task_id}\"><h4>Update Task</h4><label>Task ID<input name=\"task_id\" data-path=\"true\" required></label><label>Title<input name=\"title\"></label><label>Objective<input name=\"objective\"></label><label>Command<input name=\"command\"></label><label>Clear command<select name=\"clear_command\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Tool<input name=\"tool\"></label><label>Clear tool<select name=\"clear_tool\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Args JSON<textarea name=\"args\" data-json=\"true\" placeholder='{\"name\":\"value\"}'></textarea></label><label>Secret args JSON<textarea name=\"secret_args\" data-json=\"true\" placeholder='{\"token\":\"AGENT_OS_TOKEN\"}'></textarea></label><label>Clear args<select name=\"clear_args\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Clear secret args<select name=\"clear_secret_args\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>CWD<input name=\"cwd\"></label><label>Clear CWD<select name=\"clear_cwd\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Capabilities<input name=\"required_capabilities\" data-list=\"true\" placeholder=\"rust,review\"></label><label>Clear capabilities<select name=\"clear_required_capabilities\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Max attempts<input name=\"max_attempts\" type=\"number\" min=\"1\" data-number=\"true\"></label><button type=\"submit\">Update Task</button><output></output></form><form data-dashboard-form data-endpoint-template=\"/tasks/{task_id}/assign\"><h4>Assign Task</h4><label>Task ID<input name=\"task_id\" data-path=\"true\" required></label><label>Agent ID<input name=\"agent\" required></label><button type=\"submit\">Assign Task</button><output></output></form><form data-dashboard-form data-endpoint-template=\"/tasks/{task_id}/priority\"><h4>Set Priority</h4><label>Task ID<input name=\"task_id\" data-path=\"true\" required></label><label>Priority<select name=\"priority\"><option value=\"normal\">Normal</option><option value=\"low\">Low</option><option value=\"high\">High</option><option value=\"critical\">Critical</option><option value=\"urgent\">Urgent</option></select></label><button type=\"submit\">Set Priority</button><output></output></form><form data-dashboard-form data-endpoint-template=\"/tasks/{task_id}/dependencies\"><h4>Set Dependencies</h4><label>Task ID<input name=\"task_id\" data-path=\"true\" required></label><label>Dependencies<input name=\"dependencies\" data-list=\"true\" placeholder=\"task-a,task-b\" required></label><button type=\"submit\">Set Dependencies</button><output></output></form><form data-dashboard-form data-endpoint-template=\"/tasks/{task_id}/plan\"><h4>Set Plan</h4><label>Task ID<input name=\"task_id\" data-path=\"true\" required></label><label>Steps<input name=\"steps\" data-list=\"true\" placeholder=\"inspect,edit,test\" required></label><button type=\"submit\">Set Plan</button><output></output></form><form data-dashboard-form data-endpoint-template=\"/tasks/{task_id}/complete\"><h4>Complete Task</h4><label>Task ID<input name=\"task_id\" data-path=\"true\" required></label><label>Note<input name=\"note\"></label><button type=\"submit\">Complete Task</button><output></output></form><form data-dashboard-form data-endpoint-template=\"/tasks/{task_id}/fail\"><h4>Fail Task</h4><label>Task ID<input name=\"task_id\" data-path=\"true\" required></label><label>Note<input name=\"note\"></label><button class=\"deny\" type=\"submit\">Fail Task</button><output></output></form><form data-dashboard-form data-endpoint-template=\"/tasks/{task_id}/block\"><h4>Block Task</h4><label>Task ID<input name=\"task_id\" data-path=\"true\" required></label><label>Note<input name=\"note\"></label><button type=\"submit\">Block Task</button><output></output></form><form data-dashboard-form data-endpoint-template=\"/tasks/{task_id}/cancel\"><h4>Cancel Task</h4><label>Task ID<input name=\"task_id\" data-path=\"true\" required></label><label>Note<input name=\"note\"></label><button class=\"deny\" type=\"submit\">Cancel Task</button><output></output></form><form data-dashboard-form data-endpoint-template=\"/tasks/{task_id}/retry\"><h4>Retry Task</h4><label>Task ID<input name=\"task_id\" data-path=\"true\" required></label><label>Note<input name=\"note\"></label><button type=\"submit\">Retry Task</button><output></output></form><form data-dashboard-form data-endpoint-template=\"/tasks/{task_id}/unblock\"><h4>Unblock Task</h4><label>Task ID<input name=\"task_id\" data-path=\"true\" required></label><label>Note<input name=\"note\"></label><button type=\"submit\">Unblock Task</button><output></output></form><form data-dashboard-form data-method=\"DELETE\" data-endpoint-template=\"/tasks/{task_id}\"><h4>Remove Task</h4><label>Task ID<input name=\"task_id\" data-path=\"true\" required></label><button class=\"deny\" type=\"submit\">Remove Task</button><output></output></form><form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/tasks/recover\"><h4>Recover Stale Tasks</h4><label>Older than seconds<input name=\"older_than_seconds\" type=\"number\" min=\"1\" data-number=\"true\"></label><button type=\"submit\">Recover Tasks</button><output></output></form></div><table><thead><tr><th>Title</th><th>Status</th><th>Priority</th><th>Agent</th></tr></thead><tbody>");
    for task in os.tasks.values().take(20) {
        let _ = write!(
            body,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape_html(&task.title),
            escape_html(&task.status.to_string()),
            escape_html(&task.priority.to_string()),
            task.assigned_to
                .as_ref()
                .map(|agent| escape_html(&agent.to_string()))
                .unwrap_or_else(|| "-".into())
        );
    }
    body.push_str("</tbody></table></section>");

    body.push_str("<section class=\"wide\"><h2>Tools</h2><div class=\"worker-actions\"><form data-dashboard-form data-endpoint=\"/tools\"><h4>Create Tool</h4><label>Name<input name=\"name\" required></label><label>Kind<select name=\"kind\"><option value=\"shell\">Shell</option><option value=\"file-read\">File Read</option><option value=\"file-write\">File Write</option></select></label><label>Description<input name=\"description\"></label><label>Capabilities<input name=\"required_capabilities\" data-list=\"true\" placeholder=\"rust,docs\"></label><label>Command template<input name=\"command_template\" required></label><label>CWD<input name=\"cwd\"></label><button type=\"submit\">Create Tool</button><output></output></form><form data-dashboard-query data-endpoint-template=\"/tools/{tool_id}\"><h4>Inspect Tool</h4><label>Tool ID<input name=\"tool_id\" data-path=\"true\" required></label><button type=\"submit\">Inspect Tool</button><output></output></form><form data-dashboard-form data-endpoint-template=\"/tools/{tool_id}\"><h4>Update Tool</h4><label>Tool ID<input name=\"tool_id\" data-path=\"true\" required></label><label>Kind<select name=\"kind\"><option value=\"\">Unchanged</option><option value=\"shell\">Shell</option><option value=\"file-read\">File Read</option><option value=\"file-write\">File Write</option></select></label><label>Description<input name=\"description\"></label><label>Clear description<select name=\"clear_description\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Capabilities<input name=\"required_capabilities\" data-list=\"true\" placeholder=\"rust,docs\"></label><label>Clear capabilities<select name=\"clear_required_capabilities\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Command template<input name=\"command_template\"></label><label>CWD<input name=\"cwd\"></label><label>Clear CWD<select name=\"clear_cwd\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><button type=\"submit\">Update Tool</button><output></output></form><form data-dashboard-form data-method=\"DELETE\" data-endpoint-template=\"/tools/{tool_id}\"><h4>Remove Tool</h4><label>Tool ID<input name=\"tool_id\" data-path=\"true\" required></label><button class=\"deny\" type=\"submit\">Remove Tool</button><output></output></form></div><table><thead><tr><th>Tool</th><th>Kind</th><th>Capabilities</th><th>Template</th><th>CWD</th></tr></thead><tbody>");
    for tool in os.tools.values().take(20) {
        let _ = write!(
            body,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape_html(&tool.name),
            escape_html(&tool.kind.to_string()),
            escape_html(&dashboard_list_value(&tool.required_capabilities)),
            escape_html(&tool.command_template),
            tool.default_cwd
                .as_ref()
                .map(|cwd| escape_html(cwd))
                .unwrap_or_else(|| "-".into())
        );
    }
    body.push_str("</tbody></table></section>");

    body.push_str("<section><h2>Runs</h2><table><thead><tr><th>Run</th><th>Status</th><th>Command</th><th>Artifacts</th><th>Trace</th></tr></thead><tbody>");
    for run in os.runs.values().take(20) {
        let _ = write!(
            body,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape_html(&run.id.to_string()),
            escape_html(&run.status.to_string()),
            escape_html(&run.command),
            run.artifacts.len(),
            escape_html(&run.trace_id)
        );
    }
    body.push_str("</tbody></table></section>");

    let (daemon_status, daemon_ticks, daemon_message) = os
        .daemon
        .as_ref()
        .map(|daemon| {
            (
                daemon.status.to_string(),
                daemon.ticks.to_string(),
                daemon
                    .last_message
                    .as_deref()
                    .unwrap_or("no daemon message")
                    .to_owned(),
            )
        })
        .unwrap_or_else(|| {
            (
                "not-started".into(),
                "-".into(),
                "daemon has not been started".into(),
            )
        });
    let _ = write!(
        body,
        "<section class=\"wide\"><h2>Scheduler &amp; Daemon</h2><div class=\"run-actions\"><form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/run\"><h4>Run Scheduler</h4><label>Limit<input name=\"limit\" type=\"number\" min=\"1\" value=\"1\" data-number=\"true\"></label><label>Execute<select name=\"execute\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Dry run<select name=\"dry_run\" data-bool=\"true\"><option value=\"true\">Yes</option><option value=\"false\">No</option></select></label><label>Recover stale seconds<input name=\"recover_stale_seconds\" type=\"number\" min=\"1\" data-number=\"true\"></label><button type=\"submit\">Run Scheduler</button><output></output></form><form data-dashboard-query data-endpoint=\"/daemon\"><h4>Daemon Status</h4><button type=\"submit\">Inspect Daemon</button><output></output></form><form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/daemon/stop\"><h4>Stop Daemon</h4><button class=\"deny\" type=\"submit\">Request Stop</button><output></output></form></div><table><thead><tr><th>Status</th><th>Ticks</th><th>Message</th></tr></thead><tbody><tr><td>{}</td><td>{}</td><td>{}</td></tr></tbody></table></section>",
        escape_html(&daemon_status),
        escape_html(&daemon_ticks),
        escape_html(&daemon_message)
    );

    body.push_str("<section><h2>Workflows</h2><table><thead><tr><th>Objective</th><th>Priority</th><th>Progress</th><th>DAG</th></tr></thead><tbody>");
    for workflow in os.workflows.values().take(20) {
        let progress = os.workflow_progress(&workflow.id);
        let _ = write!(
            body,
            "<tr><td>{}</td><td>{}</td><td>{}/{} complete</td><td>{}</td></tr>",
            escape_html(&workflow.objective),
            escape_html(&workflow.priority.to_string()),
            progress
                .as_ref()
                .map(|progress| progress.tasks_complete)
                .unwrap_or(0),
            progress
                .as_ref()
                .map(|progress| progress.total_tasks)
                .unwrap_or(0)
                .to_string(),
            escape_html(&workflow_dag_summary(os, workflow))
        );
    }
    body.push_str("</tbody></table></section>");

    body.push_str("<section class=\"wide\"><h2>Workflow DAG Editor</h2>");
    if os.workflows.is_empty() {
        body.push_str("<p class=\"muted\">No workflows</p>");
    }
    for workflow in os.workflows.values().take(8) {
        let workflow_path = format!("/workflows/{}", workflow.id);
        let _ = write!(
            body,
            "<article class=\"dag-editor\"><h3>{}</h3><p class=\"muted\">{} · {}</p><pre>{}</pre>",
            escape_html(&workflow.objective),
            escape_html(&workflow.id.to_string()),
            escape_html(&workflow.priority.to_string()),
            escape_html(&workflow_dag_summary(os, workflow))
        );
        let _ = write!(
            body,
            "<form data-dashboard-form data-endpoint=\"{}/tasks\"><h4>Add Stage</h4><label>Stage<input name=\"stage\" required></label><label>Title<input name=\"title\" required></label><label>Objective<input name=\"objective\"></label><label>Command<input name=\"command\"></label><label>Needs<input name=\"required_capabilities\" data-list=\"true\"></label><label>After<input name=\"dependencies\" data-list=\"true\"></label><label>Priority<input name=\"priority\"></label><button type=\"submit\">Add</button><output></output></form>",
            escape_html(&workflow_path)
        );
        let _ = write!(
            body,
            "<div class=\"dag-actions\"><form data-dashboard-form data-endpoint=\"{}/link\"><h4>Link</h4><label>From<input name=\"from\" required></label><label>To<input name=\"to\" required></label><button type=\"submit\">Link</button><output></output></form><form data-dashboard-form data-endpoint=\"{}/unlink\"><h4>Unlink</h4><label>From<input name=\"from\" required></label><label>To<input name=\"to\" required></label><button type=\"submit\">Unlink</button><output></output></form></div></article>",
            escape_html(&workflow_path),
            escape_html(&workflow_path)
        );
    }
    body.push_str("</section>");

    body.push_str("<section class=\"wide\"><h2>Registry Templates</h2><form class=\"marketplace-import\" data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/registry/marketplace-import\"><h4>Import Marketplace</h4><label>Manifest<textarea name=\"manifest\" data-json=\"true\" required placeholder=\"{&quot;workflow_templates&quot;:[]}\"></textarea></label><label>Source<input name=\"source\" placeholder=\"marketplace.json\"></label><label>Expect checksum<input name=\"expect_checksum\" placeholder=\"fnv1a64:...\"></label><label>Force<select name=\"force\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><button type=\"submit\">Import Marketplace</button><output></output></form>");
    body.push_str("<h3>Agent Profiles</h3>");
    if os.agent_profiles.is_empty() {
        body.push_str("<p class=\"muted\">No agent profiles</p>");
    }
    for profile in os.agent_profiles.values().take(8) {
        let endpoint = escape_html(&format!("/registry/profiles/{}/agents", profile.id));
        let _ = write!(
            body,
            "<article class=\"template-editor\"><h3>{}</h3><p class=\"muted\">{} · {} · {}</p><form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"{}\"><h4>Install Agent</h4><label>Name override<input name=\"name\" placeholder=\"{}\"></label><label>Model override<input name=\"model\" placeholder=\"{}\"></label><label>Parallel<input name=\"parallel\" type=\"number\" min=\"1\" value=\"1\" data-number=\"true\"></label><button type=\"submit\">Install Agent</button><output></output></form></article>",
            escape_html(&profile.name),
            escape_html(&profile.id),
            escape_html(&profile.kind.to_string()),
            escape_html(&dashboard_list_value(&profile.capabilities)),
            endpoint,
            escape_html(&profile.name),
            escape_html(profile.model.as_deref().unwrap_or("profile default"))
        );
    }
    body.push_str("<h3>Workflow Templates</h3>");
    if os.workflow_templates.is_empty() {
        body.push_str("<p class=\"muted\">No workflow templates</p>");
    }
    for template in os.workflow_templates.values().take(8) {
        let endpoint = escape_html(&format!("/registry/templates/{}/workflows", template.id));
        let _ = write!(
            body,
            "<article class=\"template-editor\"><h3>{}</h3><p class=\"muted\">{} · {}</p><p>{}</p><form data-dashboard-form data-endpoint=\"{}\"><h4>Create Workflow</h4><label>Objective<input name=\"objective\" required></label><label>Priority<select name=\"priority\"><option value=\"normal\">Normal</option><option value=\"high\">High</option><option value=\"critical\">Critical</option><option value=\"low\">Low</option></select></label><label>Execute<select name=\"execute\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><button type=\"submit\">Create Workflow</button><output></output></form></article>",
            escape_html(&template.name),
            escape_html(&template.id),
            escape_html(&template.stages.join(" -> ")),
            escape_html(&template.description),
            endpoint
        );
    }
    body.push_str("</section>");

    body.push_str("<section class=\"wide\"><h2>MCP Servers</h2><form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/registry/mcp-servers\"><h4>Register MCP Server</h4><label>ID<input name=\"id\" required></label><label>Command<input name=\"command\" required></label><label>Args<input name=\"args\" data-list=\"true\" placeholder=\"--stdio,--verbose\"></label><label>Env JSON<textarea name=\"env\" data-json=\"true\" placeholder=\"{&quot;TOKEN&quot;:&quot;value&quot;}\"></textarea></label><label>Enabled<select name=\"enabled\" data-bool=\"true\"><option value=\"true\">Yes</option><option value=\"false\">No</option></select></label><button type=\"submit\">Register MCP Server</button><output></output></form>");
    if os.mcp_servers.is_empty() {
        body.push_str("<p class=\"muted\">No MCP servers</p>");
    }
    for server in os.mcp_servers.values().take(12) {
        let endpoint = escape_html(&format!("/registry/mcp-servers/{}", server.id));
        let env_json = serde_json::to_string_pretty(&server.env).unwrap_or_else(|_| "{}".into());
        let _ = write!(
            body,
            "<article class=\"mcp-editor\"><h3>{}</h3><p class=\"muted\">{} · {}</p><form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"{}\"><h4>Update MCP Server</h4><label>Command<input name=\"command\" value=\"{}\"></label><label>Args<input name=\"args\" data-list=\"true\" value=\"{}\"></label><label>Env JSON<textarea name=\"env\" data-json=\"true\">{}</textarea></label><label>Enabled<select name=\"enabled\" data-bool=\"true\"><option value=\"{}\">Keep {}</option><option value=\"true\">Yes</option><option value=\"false\">No</option></select></label><button type=\"submit\">Update MCP</button><output></output></form><form data-dashboard-form data-method=\"DELETE\" data-dashboard-result=\"json\" data-endpoint=\"{}\"><button class=\"deny\" type=\"submit\">Remove MCP</button><output></output></form></article>",
            escape_html(&server.id),
            escape_html(&server.command),
            escape_html(if server.enabled {
                "enabled"
            } else {
                "disabled"
            }),
            endpoint,
            escape_html(&server.command),
            escape_html(&server.args.join(",")),
            escape_html(&env_json),
            if server.enabled { "true" } else { "false" },
            escape_html(if server.enabled {
                "enabled"
            } else {
                "disabled"
            }),
            endpoint
        );
    }
    body.push_str("</section>");

    body.push_str("<section class=\"wide\"><h2>Git Workspace</h2><div class=\"git-actions\"><form data-dashboard-query data-endpoint=\"/git/status\"><h4>Status</h4><label>Workspace<input name=\"cwd\" placeholder=\"/path/to/repo\"></label><button type=\"submit\">Inspect Status</button><output></output></form><form data-dashboard-form data-endpoint=\"/git/review-task\"><h4>Review Task</h4><label>Workspace<input name=\"cwd\" placeholder=\"/path/to/repo\"></label><label>Base<input name=\"base\" value=\"main\"></label><label>Title<input name=\"title\" value=\"Code review\"></label><label>Priority<input name=\"priority\" value=\"normal\"></label><button type=\"submit\">Create Review Task</button><output></output></form></div></section>");

    body.push_str(
        "<section class=\"wide\"><h2>Service Definitions</h2><div class=\"service-actions\">",
    );
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/service/launchd\"><h4>Render Launchd</h4><label>Label<input name=\"label\" placeholder=\"com.agent-os.daemon\"></label><label>Binary<input name=\"bin_path\" placeholder=\"/usr/local/bin/agent-os\"></label><label>Interval ms<input name=\"interval_ms\" type=\"number\" min=\"1\" value=\"1000\" data-number=\"true\"></label><label>Limit<input name=\"limit\" type=\"number\" min=\"1\" value=\"1\" data-number=\"true\"></label><label>Execute<select name=\"execute\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Recover stale seconds<input name=\"recover_stale_seconds\" type=\"number\" min=\"1\" data-number=\"true\"></label><label>No logs<select name=\"no_logs\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Plist path<input name=\"plist_path\" placeholder=\"~/Library/LaunchAgents/com.agent-os.daemon.plist\"></label><button type=\"submit\">Render Launchd</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/service/launchd/install\"><h4>Install Launchd</h4><label>Label<input name=\"label\" placeholder=\"com.agent-os.daemon\"></label><label>Binary<input name=\"bin_path\" placeholder=\"/usr/local/bin/agent-os\"></label><label>Interval ms<input name=\"interval_ms\" type=\"number\" min=\"1\" value=\"1000\" data-number=\"true\"></label><label>Limit<input name=\"limit\" type=\"number\" min=\"1\" value=\"1\" data-number=\"true\"></label><label>Execute<select name=\"execute\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Recover stale seconds<input name=\"recover_stale_seconds\" type=\"number\" min=\"1\" data-number=\"true\"></label><label>No logs<select name=\"no_logs\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Plist path<input name=\"plist_path\" placeholder=\"~/Library/LaunchAgents/com.agent-os.daemon.plist\"></label><button type=\"submit\">Install Launchd</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/service/launchd/uninstall\"><h4>Uninstall Launchd</h4><label>Label<input name=\"label\" placeholder=\"com.agent-os.daemon\"></label><label>Plist path<input name=\"plist_path\" placeholder=\"~/Library/LaunchAgents/com.agent-os.daemon.plist\"></label><button class=\"deny\" type=\"submit\">Uninstall Launchd</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/service/launchd/start\"><h4>Start Launchd</h4><label>Label<input name=\"label\" placeholder=\"com.agent-os.daemon\"></label><label>Plist path<input name=\"plist_path\" placeholder=\"~/Library/LaunchAgents/com.agent-os.daemon.plist\"></label><label>Domain<input name=\"domain\" placeholder=\"gui/501\"></label><label>Launchctl path<input name=\"launchctl_path\" placeholder=\"launchctl\"></label><button type=\"submit\">Start Launchd</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/service/launchd/stop\"><h4>Stop Launchd</h4><label>Label<input name=\"label\" placeholder=\"com.agent-os.daemon\"></label><label>Plist path<input name=\"plist_path\" placeholder=\"~/Library/LaunchAgents/com.agent-os.daemon.plist\"></label><label>Domain<input name=\"domain\" placeholder=\"gui/501\"></label><label>Launchctl path<input name=\"launchctl_path\" placeholder=\"launchctl\"></label><button class=\"deny\" type=\"submit\">Stop Launchd</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/service/launchd/status\"><h4>Launchd Status</h4><label>Label<input name=\"label\" placeholder=\"com.agent-os.daemon\"></label><label>Domain<input name=\"domain\" placeholder=\"gui/501\"></label><label>Launchctl path<input name=\"launchctl_path\" placeholder=\"launchctl\"></label><button type=\"submit\">Launchd Status</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/service/systemd\"><h4>Render Systemd</h4><label>Unit name<input name=\"unit_name\" placeholder=\"agent-os.service\"></label><label>Binary<input name=\"bin_path\" placeholder=\"/usr/local/bin/agent-os\"></label><label>Interval ms<input name=\"interval_ms\" type=\"number\" min=\"1\" value=\"1000\" data-number=\"true\"></label><label>Limit<input name=\"limit\" type=\"number\" min=\"1\" value=\"1\" data-number=\"true\"></label><label>Execute<select name=\"execute\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Recover stale seconds<input name=\"recover_stale_seconds\" type=\"number\" min=\"1\" data-number=\"true\"></label><label>Unit path<input name=\"unit_path\" placeholder=\"~/.config/systemd/user/agent-os.service\"></label><button type=\"submit\">Render Systemd</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/service/systemd/install\"><h4>Install Systemd</h4><label>Unit name<input name=\"unit_name\" placeholder=\"agent-os.service\"></label><label>Binary<input name=\"bin_path\" placeholder=\"/usr/local/bin/agent-os\"></label><label>Interval ms<input name=\"interval_ms\" type=\"number\" min=\"1\" value=\"1000\" data-number=\"true\"></label><label>Limit<input name=\"limit\" type=\"number\" min=\"1\" value=\"1\" data-number=\"true\"></label><label>Execute<select name=\"execute\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Recover stale seconds<input name=\"recover_stale_seconds\" type=\"number\" min=\"1\" data-number=\"true\"></label><label>Unit path<input name=\"unit_path\" placeholder=\"~/.config/systemd/user/agent-os.service\"></label><button type=\"submit\">Install Systemd</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/service/systemd/uninstall\"><h4>Uninstall Systemd</h4><label>Unit name<input name=\"unit_name\" placeholder=\"agent-os.service\"></label><label>Unit path<input name=\"unit_path\" placeholder=\"~/.config/systemd/user/agent-os.service\"></label><button class=\"deny\" type=\"submit\">Uninstall Systemd</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/service/systemd/start\"><h4>Start Systemd</h4><label>Unit name<input name=\"unit_name\" placeholder=\"agent-os.service\"></label><label>Systemctl path<input name=\"systemctl_path\" placeholder=\"systemctl\"></label><button type=\"submit\">Start Systemd</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/service/systemd/stop\"><h4>Stop Systemd</h4><label>Unit name<input name=\"unit_name\" placeholder=\"agent-os.service\"></label><label>Systemctl path<input name=\"systemctl_path\" placeholder=\"systemctl\"></label><button class=\"deny\" type=\"submit\">Stop Systemd</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/service/systemd/status\"><h4>Systemd Status</h4><label>Unit name<input name=\"unit_name\" placeholder=\"agent-os.service\"></label><label>Systemctl path<input name=\"systemctl_path\" placeholder=\"systemctl\"></label><button type=\"submit\">Systemd Status</button><output></output></form>");
    body.push_str("</div></section>");

    body.push_str(
        "<section class=\"wide\"><h2>State Maintenance</h2><div class=\"state-actions\">",
    );
    body.push_str("<form data-dashboard-query data-endpoint=\"/state/validate\"><h4>Validate State</h4><button type=\"submit\">Validate State</button><output></output></form>");
    body.push_str("<form data-dashboard-query data-endpoint=\"/state/export\"><h4>Read Snapshot</h4><button type=\"submit\">Read Snapshot</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/state/export\"><h4>Export State</h4><label>Output<input name=\"output\" placeholder=\"state.json\" required></label><label>Dry run<select name=\"dry_run\" data-bool=\"true\"><option value=\"true\">Yes</option><option value=\"false\">No</option></select></label><button type=\"submit\">Export State</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/state/import\"><h4>Import State</h4><label>Path<input name=\"path\" placeholder=\"state.json\" required></label><label>Force<select name=\"force\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Dry run<select name=\"dry_run\" data-bool=\"true\"><option value=\"true\">Yes</option><option value=\"false\">No</option></select></label><button class=\"deny\" type=\"submit\">Import State</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/state/migrate\"><h4>Migrate State</h4><label>Input<input name=\"input\" placeholder=\"legacy.json\"></label><label>Output<input name=\"output\" placeholder=\"state.json\"></label><label>Dry run<select name=\"dry_run\" data-bool=\"true\"><option value=\"true\">Yes</option><option value=\"false\">No</option></select></label><button type=\"submit\">Migrate State</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/state/sqlite\"><h4>SQLite Mirror</h4><label>Output<input name=\"output\" placeholder=\"state.sqlite\"></label><label>Init only<select name=\"init_only\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Restore<select name=\"restore\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Force<select name=\"force\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Dry run<select name=\"dry_run\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><button type=\"submit\">Sync SQLite</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/state/backup\"><h4>Backup</h4><label>Output<input name=\"output\" placeholder=\"backup.json\"></label><label>Dry run<select name=\"dry_run\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><button type=\"submit\">Backup State</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/state/repair\"><h4>Repair</h4><label>Dry run<select name=\"dry_run\" data-bool=\"true\"><option value=\"true\">Yes</option><option value=\"false\">No</option></select></label><button type=\"submit\">Repair State</button><output></output></form>");
    body.push_str("<form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/state/prune\"><h4>Prune</h4><label>Keep runs<input name=\"keep_runs\" type=\"number\" min=\"0\" value=\"100\" data-number=\"true\"></label><label>Keep events<input name=\"keep_events\" type=\"number\" min=\"0\" value=\"500\" data-number=\"true\"></label><label>Dry run<select name=\"dry_run\" data-bool=\"true\"><option value=\"true\">Yes</option><option value=\"false\">No</option></select></label><button type=\"submit\">Prune State</button><output></output></form>");
    body.push_str("</div></section>");

    let _ = write!(
        body,
        "<section class=\"wide\"><h2>Policy Posture</h2><div class=\"policy-grid\"><article><span>Autonomy</span><strong>{}</strong><small>Shell {}</small></article><article><span>Sandbox</span><strong>{}</strong><small>Workspace jail {}</small><small>Writable paths: {}</small></article><article><span>Network</span><strong>{}</strong><small>Allowed hosts: {}</small></article><article><span>Approval Gates</span><strong>{}</strong><small>Risky patterns: {}</small></article><article><span>Runtime Limits</span><strong>{}s timeout</strong><small>Max output: {} bytes</small></article><article><span>Memory Policy</span><strong>{}</strong><small>Provider memories: {}</small><small>Scope: {}</small></article><article><span>Allowed Workspaces</span><strong>{}</strong></article><article><span>Policy Rules</span><strong>{}</strong></article></div><div class=\"policy-actions\"><form data-dashboard-query data-endpoint=\"/config\"><h4>Config</h4><button type=\"submit\">Inspect Config</button><output></output></form><form data-dashboard-query data-endpoint=\"/config/validate\"><h4>Config Validation</h4><button type=\"submit\">Validate Config</button><output></output></form><form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/config\"><h4>Write Config Profile</h4><label>Profile<select name=\"profile\"><option value=\"safe\">Safe</option><option value=\"dev\">Dev</option><option value=\"autonomous\">Autonomous</option><option value=\"ci\">CI</option></select></label><label>Force<select name=\"force\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><button type=\"submit\">Write Config</button><output></output></form></div></section>",
        escape_html(autonomy_level_label(&os.policy.autonomy)),
        escape_html(enabled_label(os.policy.allow_shell)),
        escape_html(enabled_label(os.policy.sandbox.process_isolation)),
        escape_html(enabled_label(os.policy.sandbox.jailed_workspaces)),
        escape_html(&dashboard_list_value(&os.policy.sandbox.writable_paths)),
        escape_html(network_mode_label(&os.policy.network.mode)),
        escape_html(&dashboard_list_value(&os.policy.network.allowed_hosts)),
        escape_html(enabled_label(os.policy.approval.require_for_risky_actions)),
        escape_html(&dashboard_list_value(&os.policy.approval.risky_patterns)),
        os.policy.command_timeout_seconds,
        os.policy.max_output_bytes,
        escape_html(if os.memory_policy.semantic_recall {
            "semantic recall"
        } else {
            "recency recall"
        }),
        os.memory_policy.max_provider_memories,
        escape_html(os.memory_policy.scope.as_deref().unwrap_or("-")),
        escape_html(&dashboard_list_value(&os.policy.allowed_workspaces)),
        escape_html(&dashboard_list_value(&os.policy.rules))
    );

    let _ = write!(
        body,
        "<section class=\"wide\"><h2>Provider Boundary</h2><div class=\"provider-grid\"><article><span>Provider</span><strong>{}</strong><small>Model: {}</small><small>Adapter: {}</small></article><article><span>Endpoint</span><strong>{}</strong><small>API key env: {}</small></article><article><span>Plugin</span><strong>{}</strong><small>Args: {}</small><small>Env keys: {}</small></article><article><span>Structured Output</span><strong>{}</strong><small>Request options: {}</small></article><article><span>Retries</span><strong>{}</strong><small>Backoff: {} ms</small><small>Timeout: {}s</small></article></div></section>",
        escape_html(&os.provider.kind.to_string()),
        escape_html(&os.provider.model),
        escape_html(os.provider.adapter.as_deref().unwrap_or("-")),
        escape_html(os.provider.endpoint.as_deref().unwrap_or("-")),
        escape_html(&os.provider.api_key_env),
        escape_html(os.provider.plugin_command.as_deref().unwrap_or("-")),
        escape_html(&dashboard_list_value(&os.provider.plugin_args)),
        escape_html(&dashboard_map_keys_value(&os.provider.plugin_env)),
        escape_html(if os.provider.response_schema.is_some() {
            "schema configured"
        } else {
            "plain text allowed"
        }),
        escape_html(&dashboard_map_keys_value(&os.provider.request_options)),
        os.provider.max_retries,
        os.provider.retry_backoff_ms,
        os.provider.request_timeout_seconds
    );

    body.push_str("<section><h2>Approvals</h2><table><thead><tr><th>Action</th><th>Status</th><th>Reason</th><th>Task</th><th>Resolve</th></tr></thead><tbody>");
    for approval in os.approvals.values().take(20) {
        let resolution = if approval.status == ApprovalStatus::Pending {
            let endpoint = escape_html(&format!("/approvals/{}", approval.id));
            format!(
                "<div class=\"approval-actions\"><form data-dashboard-form data-endpoint=\"{endpoint}/approve\"><label>By<input name=\"by\" placeholder=\"operator\"></label><button type=\"submit\">Approve</button><output></output></form><form data-dashboard-form data-endpoint=\"{endpoint}/deny\"><label>By<input name=\"by\" placeholder=\"operator\"></label><button class=\"deny\" type=\"submit\">Deny</button><output></output></form></div>"
            )
        } else {
            approval
                .resolved_by
                .as_ref()
                .map(|resolved_by| escape_html(resolved_by))
                .unwrap_or_else(|| "-".into())
        };
        let _ = write!(
            body,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape_html(&approval.action),
            escape_html(&approval_status_label(&approval.status)),
            escape_html(&approval.reason),
            escape_html(&approval.task_id.to_string()),
            resolution
        );
    }
    body.push_str("</tbody></table></section>");

    body.push_str("<section class=\"wide\"><h2>Workers</h2><div class=\"worker-actions\"><form data-dashboard-form data-endpoint=\"/workers\"><h4>Register Worker</h4><label>Worker ID<input name=\"id\" required></label><label>Endpoint<input name=\"endpoint\" placeholder=\"http://127.0.0.1:9200\" required></label><label>Status<select name=\"status\"><option value=\"online\">Online</option><option value=\"busy\">Busy</option><option value=\"paused\">Paused</option><option value=\"offline\">Offline</option></select></label><button type=\"submit\">Register Worker</button><output></output></form><form data-dashboard-form data-endpoint-template=\"/workers/{worker_id}/heartbeat\"><h4>Heartbeat Worker</h4><label>Worker ID<input name=\"worker_id\" data-path=\"true\" required></label><label>Endpoint<input name=\"endpoint\" placeholder=\"http://127.0.0.1:9200\"></label><label>Status<select name=\"status\"><option value=\"\">Unchanged</option><option value=\"online\">Online</option><option value=\"busy\">Busy</option><option value=\"paused\">Paused</option><option value=\"offline\">Offline</option></select></label><label>Lease seconds<input name=\"lease_seconds\" type=\"number\" min=\"1\" data-number=\"true\"></label><button type=\"submit\">Heartbeat Worker</button><output></output></form><form data-dashboard-form data-dashboard-result=\"json\" data-endpoint-template=\"/workers/{worker_id}/claim\"><h4>Claim Task</h4><label>Worker ID<input name=\"worker_id\" data-path=\"true\" required></label><label>Lease seconds<input name=\"lease_seconds\" type=\"number\" min=\"1\" data-number=\"true\"></label><button type=\"submit\">Claim Task</button><output></output></form><form data-dashboard-form data-dashboard-result=\"json\" data-endpoint-template=\"/workers/{worker_id}/report\"><h4>Report Task</h4><label>Worker ID<input name=\"worker_id\" data-path=\"true\" required></label><label>Task ID<input name=\"task_id\" required></label><label>Status<select name=\"status\"><option value=\"complete\">Complete</option><option value=\"failed\">Failed</option></select></label><label>Note<input name=\"note\"></label><label>Command<input name=\"command\"></label><label>CWD<input name=\"cwd\"></label><label>Exit code<input name=\"exit_code\" type=\"number\" data-number=\"true\"></label><label>Artifacts JSON<textarea name=\"artifacts\" data-json=\"true\" placeholder='[{\"kind\":\"stdout\",\"path\":\"artifacts/stdout.log\"}]'></textarea></label><button type=\"submit\">Report Task</button><output></output></form><form data-dashboard-form data-method=\"DELETE\" data-endpoint-template=\"/workers/{worker_id}\"><h4>Remove Worker</h4><label>Worker ID<input name=\"worker_id\" data-path=\"true\" required></label><button class=\"deny\" type=\"submit\">Remove Worker</button><output></output></form></div><table><thead><tr><th>Worker</th><th>Status</th><th>Endpoint</th><th>Last seen</th></tr></thead><tbody>");
    for worker in os.workers.values().take(20) {
        let _ = write!(
            body,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape_html(&worker.id),
            escape_html(&worker.status.to_string()),
            escape_html(&worker.endpoint),
            escape_html(&worker.last_seen_at.to_rfc3339())
        );
    }
    body.push_str("</tbody></table></section>");

    body.push_str("<section class=\"wide\"><h2>Evals</h2><div class=\"worker-actions\"><form data-dashboard-form data-endpoint=\"/evals\"><h4>Record Eval</h4><label>Target<input name=\"target\" required></label><label>Result<select name=\"success\" data-bool=\"true\"><option value=\"true\">Pass</option><option value=\"false\">Fail</option></select></label><label>Cost micros<input name=\"cost_micros\" type=\"number\" min=\"0\" data-number=\"true\"></label><label>Latency ms<input name=\"latency_ms\" type=\"number\" min=\"0\" data-number=\"true\"></label><button type=\"submit\">Record Eval</button><output></output></form><form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/evals/run\"><h4>Run Eval</h4><label>Target<input name=\"target\" required></label><label>Command<input name=\"command\" required></label><label>CWD<input name=\"cwd\"></label><label>Success pattern<input name=\"success_pattern\"></label><label>Output schema JSON<textarea name=\"output_schema\" data-json=\"true\" placeholder='{\"type\":\"object\",\"required\":[\"ok\"]}'></textarea></label><button type=\"submit\">Run Eval</button><output></output></form></div><table><thead><tr><th>Target</th><th>Success</th><th>Latency</th><th>Run</th></tr></thead><tbody>");
    for eval in os.evals.iter().rev().take(20) {
        let run_summary = eval
            .run
            .as_ref()
            .map(|run| {
                if run.timed_out {
                    "timed out".to_owned()
                } else {
                    run.status
                        .map(|status| format!("exit {status}"))
                        .unwrap_or_else(|| "manual".into())
                }
            })
            .unwrap_or_else(|| "manual".into());
        let _ = write!(
            body,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape_html(&eval.target),
            escape_html(if eval.success { "yes" } else { "no" }),
            escape_html(
                &eval
                    .latency_ms
                    .map(|latency| format!("{latency} ms"))
                    .unwrap_or_else(|| "-".into())
            ),
            escape_html(&run_summary)
        );
    }
    body.push_str("</tbody></table></section>");

    body.push_str("<section class=\"wide\"><h2>Run Inspector</h2><div class=\"run-actions\"><form data-dashboard-query data-endpoint-template=\"/runs/{run_id}/debug\"><h4>Debug</h4><label>Run ID<input name=\"run_id\" data-path=\"true\" required></label><label>Tail bytes<input name=\"tail_bytes\" type=\"number\" min=\"1\"></label><button type=\"submit\">Debug Run</button><output></output></form><form data-dashboard-query data-endpoint-template=\"/runs/{run_id}/replay\"><h4>Replay</h4><label>Run ID<input name=\"run_id\" data-path=\"true\" required></label><label>Tail bytes<input name=\"tail_bytes\" type=\"number\" min=\"1\"></label><button type=\"submit\">Replay Run</button><output></output></form><form data-dashboard-query data-endpoint-template=\"/runs/{run_id}/artifacts\"><h4>Artifacts</h4><label>Run ID<input name=\"run_id\" data-path=\"true\" required></label><button type=\"submit\">List Artifacts</button><output></output></form><form data-dashboard-query data-endpoint-template=\"/runs/{run_id}/artifacts/{artifact_id}\"><h4>Read Artifact</h4><label>Run ID<input name=\"run_id\" data-path=\"true\" required></label><label>Artifact ID<input name=\"artifact_id\" data-path=\"true\" placeholder=\"stdout\" required></label><label>Tail bytes<input name=\"tail_bytes\" type=\"number\" min=\"1\"></label><button type=\"submit\">Read Artifact</button><output></output></form><form data-dashboard-form data-dashboard-result=\"json\" data-endpoint-template=\"/runs/{run_id}/cancel\"><h4>Cancel Run</h4><label>Run ID<input name=\"run_id\" data-path=\"true\" required></label><button class=\"deny\" type=\"submit\">Cancel Run</button><output></output></form></div></section>");

    body.push_str("<section><h2>Run Artifacts</h2><table><thead><tr><th>Run</th><th>Kind</th><th>Bytes</th><th>Path</th></tr></thead><tbody>");
    for run in os.runs.values().take(20) {
        for artifact in run.artifacts.iter().take(8) {
            let _ = write!(
                body,
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                escape_html(&run.id.to_string()),
                escape_html(run_artifact_kind_label(&artifact.kind)),
                escape_html(
                    &artifact
                        .bytes
                        .map(|bytes| bytes.to_string())
                        .unwrap_or_else(|| "-".into())
                ),
                escape_html(&artifact.path)
            );
        }
    }
    body.push_str("</tbody></table></section>");

    body.push_str("<section class=\"wide\"><h2>Secrets</h2><div class=\"worker-actions\"><form data-dashboard-form data-endpoint=\"/secrets\"><h4>Register Backend</h4><label>Backend ID<input name=\"id\" required></label><label>Kind<select name=\"kind\"><option value=\"environment\">Environment</option><option value=\"1password\">1Password</option><option value=\"os-keychain\">OS Keychain</option><option value=\"env-vault\">Env Vault</option></select></label><label>Reference<input name=\"reference\"></label><button type=\"submit\">Register Secret Backend</button><output></output></form><form data-dashboard-query data-endpoint-template=\"/secrets/{backend_id}\"><h4>Inspect Backend</h4><label>Backend ID<input name=\"backend_id\" data-path=\"true\" required></label><button type=\"submit\">Inspect Backend</button><output></output></form><form data-dashboard-query data-endpoint=\"/secrets/check\"><h4>Check References</h4><button type=\"submit\">Check Secrets</button><output></output></form><form data-dashboard-form data-method=\"DELETE\" data-endpoint-template=\"/secrets/{backend_id}\"><h4>Remove Backend</h4><label>Backend ID<input name=\"backend_id\" data-path=\"true\" required></label><button class=\"deny\" type=\"submit\">Remove Backend</button><output></output></form></div><table><thead><tr><th>Backend</th><th>Kind</th><th>Reference</th></tr></thead><tbody>");
    for backend in os.secrets_backends.values().take(20) {
        let _ = write!(
            body,
            "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape_html(&backend.id),
            escape_html(secrets_backend_kind_name(&backend.kind)),
            backend
                .reference
                .as_ref()
                .map(|reference| escape_html(reference))
                .unwrap_or_else(|| "-".into())
        );
    }
    body.push_str("</tbody></table></section>");

    body.push_str("<section class=\"wide\"><h2>Memory</h2><div class=\"worker-actions\"><form data-dashboard-form data-endpoint=\"/memory\"><h4>Add Memory</h4><label>Topic<input name=\"topic\" required></label><label>Body<textarea name=\"body\" required></textarea></label><label>Tags<input name=\"tags\" data-list=\"true\" placeholder=\"ops,release\"></label><label>Visibility<select name=\"visibility\"><option value=\"shared\">Shared</option><option value=\"private\">Private</option></select></label><label>Scope<input name=\"scope\"></label><button type=\"submit\">Add Memory</button><output></output></form><form data-dashboard-query data-endpoint=\"/memory/recall\"><h4>Recall</h4><label>Query<input name=\"query\" required></label><label>Tag<input name=\"tag\"></label><label>Visibility<select name=\"visibility\"><option value=\"\">Any</option><option value=\"shared\">Shared</option><option value=\"private\">Private</option></select></label><label>Scope<input name=\"scope\"></label><label>Limit<input name=\"limit\" type=\"number\" min=\"1\" value=\"5\"></label><button type=\"submit\">Recall Memory</button><output></output></form><form data-dashboard-query data-endpoint-template=\"/memory/{memory_id}\"><h4>Inspect Memory</h4><label>Memory ID<input name=\"memory_id\" data-path=\"true\" required></label><button type=\"submit\">Inspect Memory</button><output></output></form><form data-dashboard-form data-endpoint-template=\"/memory/{memory_id}\"><h4>Update Memory</h4><label>Memory ID<input name=\"memory_id\" data-path=\"true\" required></label><label>Topic<input name=\"topic\"></label><label>Body<textarea name=\"body\"></textarea></label><label>Tags<input name=\"tags\" data-list=\"true\" placeholder=\"ops,release\"></label><label>Clear tags<select name=\"clear_tags\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><label>Visibility<select name=\"visibility\"><option value=\"\">Unchanged</option><option value=\"shared\">Shared</option><option value=\"private\">Private</option></select></label><label>Scope<input name=\"scope\"></label><label>Clear scope<select name=\"clear_scope\" data-bool=\"true\"><option value=\"false\">No</option><option value=\"true\">Yes</option></select></label><button type=\"submit\">Update Memory</button><output></output></form><form data-dashboard-form data-dashboard-result=\"json\" data-endpoint=\"/memory/prune\"><h4>Prune Memory</h4><label>Max age days<input name=\"max_age_days\" type=\"number\" min=\"1\" data-number=\"true\"></label><label>Dry run<select name=\"dry_run\" data-bool=\"true\"><option value=\"true\">Yes</option><option value=\"false\">No</option></select></label><button type=\"submit\">Prune Memory</button><output></output></form><form data-dashboard-form data-method=\"DELETE\" data-endpoint-template=\"/memory/{memory_id}\"><h4>Remove Memory</h4><label>Memory ID<input name=\"memory_id\" data-path=\"true\" required></label><button class=\"deny\" type=\"submit\">Remove Memory</button><output></output></form></div><ul>");
    for memory in os.memory.iter().take(20) {
        let _ = write!(
            body,
            "<li><strong>{}</strong><span>{}</span></li>",
            escape_html(&memory.topic),
            escape_html(&memory.tags.join(", "))
        );
    }
    body.push_str("</ul></section>");

    body.push_str("<section><h2>Timeline</h2><ol>");
    for event in os.events.iter().rev().take(30) {
        let _ = write!(
            body,
            "<li><time>{}</time><strong>{}</strong><span>{}</span></li>",
            escape_html(&event.at.to_rfc3339()),
            escape_html(&event.kind.to_string()),
            escape_html(&event.message)
        );
    }
    body.push_str("</ol></section>");
    body.push_str("</main>");
    body.push_str(dashboard_script());
    body.push_str("</body></html>");
    body
}

fn dashboard_css() -> &'static str {
    "body{margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f6f7f8;color:#182026}header{background:#182026;color:white;padding:24px 32px;display:flex;gap:24px;justify-content:space-between;align-items:end}header p{margin:0 0 4px;color:#b8c2cc}h1{margin:0;font-size:28px}h2{font-size:16px;margin:0 0 12px}h3{font-size:15px;margin:0 0 4px}h4{font-size:13px;margin:0 0 8px}dl{display:grid;grid-template-columns:repeat(4,minmax(90px,1fr));gap:12px;margin:0}dt{color:#b8c2cc;font-size:12px}dd{margin:0;font-size:24px;font-weight:700}main{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:16px;padding:16px}section{background:white;border:1px solid #dde3ea;border-radius:8px;padding:16px;overflow:auto}.wide{grid-column:1/-1}table{width:100%;border-collapse:collapse;font-size:13px;margin-top:12px}th{text-align:left;color:#5f6b76;font-weight:600;border-bottom:1px solid #e6ebf0;padding:8px}td{border-bottom:1px solid #eef2f5;padding:8px;vertical-align:top}.metric-grid,.policy-grid,.provider-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.metric-grid article,.policy-grid article,.provider-grid article{border:1px solid #e6ebf0;border-radius:6px;padding:10px}.metric-grid span,.policy-grid span,.provider-grid span{display:block;color:#5f6b76;font-size:12px}.metric-grid strong,.policy-grid strong,.provider-grid strong{display:block;font-size:22px}.policy-grid small,.provider-grid small{display:block;color:#5f6b76;font-size:12px;margin-top:4px}ul,ol{margin:0;padding-left:20px}li{margin:0 0 10px}li span{display:block;color:#5f6b76}time,.muted{display:block;color:#5f6b76;font-size:12px}.dag-editor,.template-editor,.mcp-editor{border-top:1px solid #eef2f5;padding:12px 0}.dag-editor:first-of-type,.template-editor:first-of-type,.mcp-editor:first-of-type{border-top:0}.dag-editor pre{white-space:pre-wrap;background:#f6f7f8;border:1px solid #e6ebf0;border-radius:6px;padding:8px;font-size:12px}.template-editor p{margin:0 0 10px}.dag-actions,.approval-actions,.git-actions,.service-actions,.state-actions,.run-actions,.worker-actions,.policy-actions{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.state-actions{grid-template-columns:repeat(4,minmax(0,1fr))}.run-actions,.worker-actions,.policy-actions{grid-template-columns:repeat(3,minmax(0,1fr))}.approval-actions{min-width:360px;gap:8px}form{border:1px solid #e6ebf0;border-radius:6px;padding:10px;display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:8px;align-items:end}label{display:grid;gap:4px;color:#5f6b76;font-size:12px}input,select,textarea{box-sizing:border-box;width:100%;border:1px solid #c9d3df;border-radius:6px;padding:7px;font:inherit;background:white}textarea{min-height:140px;resize:vertical}.marketplace-import label:first-of-type{grid-column:1/-1}button{border:0;border-radius:6px;background:#1f6feb;color:white;padding:8px 10px;font-weight:600}output{font-size:12px;color:#5f6b76;white-space:pre-wrap}.service-actions output,.state-actions output,.worker-actions output,.policy-actions output{grid-column:1/-1;max-height:260px;overflow:auto;background:#f6f7f8;border:1px solid #e6ebf0;border-radius:6px;padding:8px}.approval-actions form{grid-template-columns:minmax(120px,1fr) auto;padding:8px}.approval-actions output{grid-column:1/-1}.approval-actions button{white-space:nowrap}button.deny{background:#b42318}@media(max-width:1100px){.state-actions,.worker-actions,.policy-actions{grid-template-columns:repeat(2,minmax(0,1fr))}}@media(max-width:900px){.run-actions,.policy-actions{grid-template-columns:1fr}}@media(max-width:720px){header{display:block}dl{grid-template-columns:repeat(2,minmax(90px,1fr));margin-top:16px}main{grid-template-columns:1fr;padding:12px}.dag-actions,.approval-actions,.git-actions,.service-actions,.state-actions,.worker-actions,.policy-actions{grid-template-columns:1fr}.approval-actions{min-width:0}}"
}

fn dashboard_script() -> &'static str {
    r#"<script>
function dashboardHeaders(includeContentType = false) {
  const headers = {};
  if (includeContentType) headers['content-type'] = 'application/json';
  const token = localStorage.getItem('agent_os_api_token') || '';
  if (token) headers.authorization = `Bearer ${token}`;
  return headers;
}
document.querySelectorAll('[data-dashboard-form]').forEach((form) => {
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const output = form.querySelector('output');
    const method = form.dataset.method || 'POST';
    let endpoint = form.dataset.endpointTemplate || form.dataset.endpoint;
    const body = {};
    for (const field of new FormData(form).entries()) {
      const [key, value] = field;
      const input = form.querySelector(`[name="${key}"]`);
      const text = String(value).trim();
      if (!text) continue;
      if (input && input.dataset.path === 'true') {
        endpoint = endpoint.replace(`{${key}}`, encodeURIComponent(text));
      } else if (input && input.dataset.list === 'true') {
        body[key] = text.split(',').map((item) => item.trim()).filter(Boolean);
      } else if (input && input.dataset.bool === 'true') {
        body[key] = text === 'true';
      } else if (input && input.dataset.number === 'true') {
        body[key] = Number(text);
      } else if (input && input.dataset.json === 'true') {
        try {
          body[key] = JSON.parse(text);
        } catch (_error) {
          output.textContent = `${key} must be valid JSON`;
          return;
        }
      } else {
        body[key] = text;
      }
    }
    try {
      const response = await fetch(endpoint, {
        method,
        headers: dashboardHeaders(true),
        body: JSON.stringify(body)
      });
      const result = await response.json().catch(() => null);
      if (!response.ok) {
        output.textContent = result && result.error ? result.error : `HTTP ${response.status}`;
      } else if (form.dataset.dashboardResult === 'json') {
        output.textContent = result ? JSON.stringify(result, null, 2) : 'OK';
      } else {
        output.textContent = 'Saved';
        setTimeout(() => location.reload(), 500);
      }
    } catch (error) {
      output.textContent = error.message || 'Request failed';
    }
  });
});
document.querySelectorAll('[data-dashboard-query]').forEach((form) => {
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const output = form.querySelector('output');
    const params = new URLSearchParams();
    let endpoint = form.dataset.endpointTemplate || form.dataset.endpoint;
    for (const field of new FormData(form).entries()) {
      const [key, value] = field;
      const input = form.querySelector(`[name="${key}"]`);
      const text = String(value).trim();
      if (!text) continue;
      if (input && input.dataset.path === 'true') {
        endpoint = endpoint.replace(`{${key}}`, encodeURIComponent(text));
      } else {
        params.set(key, text);
      }
    }
    const query = params.toString();
    endpoint = query ? `${endpoint}?${query}` : endpoint;
    try {
      const response = await fetch(endpoint, {
        method: 'GET',
        headers: dashboardHeaders(false)
      });
      const result = await response.json();
      if (!response.ok) {
        output.textContent = result.error || `HTTP ${response.status}`;
      } else if (Object.prototype.hasOwnProperty.call(result, 'stdout')) {
        output.textContent = result.stdout || 'clean';
      } else {
        output.textContent = JSON.stringify(result, null, 2);
      }
    } catch (error) {
      output.textContent = error.message || 'Request failed';
    }
  });
});
</script>"#
}

fn dashboard_metric_value(value: &Value) -> String {
    match value {
        Value::Number(number) => number.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Null => "-".into(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn dashboard_list_value(values: &[String]) -> String {
    if values.is_empty() {
        "-".into()
    } else {
        values.join(", ")
    }
}

fn dashboard_map_keys_value<T>(values: &BTreeMap<String, T>) -> String {
    if values.is_empty() {
        "-".into()
    } else {
        values.keys().cloned().collect::<Vec<_>>().join(", ")
    }
}

fn enabled_label(value: bool) -> &'static str {
    if value { "enabled" } else { "disabled" }
}

fn autonomy_level_label(level: &AutonomyLevel) -> &'static str {
    match level {
        AutonomyLevel::ObserveOnly => "observe-only",
        AutonomyLevel::Suggest => "suggest",
        AutonomyLevel::ExecuteWithApproval => "execute-with-approval",
        AutonomyLevel::ExecuteFreely => "execute-freely",
    }
}

fn network_mode_label(mode: &NetworkMode) -> &'static str {
    match mode {
        NetworkMode::Disabled => "disabled",
        NetworkMode::ProvidersOnly => "providers-only",
        NetworkMode::Allowed => "allowed",
    }
}

fn approval_status_label(status: &ApprovalStatus) -> String {
    match status {
        ApprovalStatus::Pending => "pending",
        ApprovalStatus::Approved => "approved",
        ApprovalStatus::Denied => "denied",
    }
    .into()
}

fn run_artifact_kind_label(kind: &RunArtifactKind) -> &'static str {
    match kind {
        RunArtifactKind::Stdout => "stdout",
        RunArtifactKind::Stderr => "stderr",
        RunArtifactKind::Summary => "summary",
        RunArtifactKind::Diff => "diff",
        RunArtifactKind::File => "file",
    }
}

fn escape_html(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn registry_json(os: &OperatingSystem) -> serde_json::Value {
    json!({
        "agent_profiles": &os.agent_profiles,
        "workflow_templates": &os.workflow_templates,
        "tools": &os.tools,
        "mcp_servers": &os.mcp_servers,
        "secrets_backends": &os.secrets_backends,
    })
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

fn workflow_dag_json(os: &OperatingSystem, id: &WorkflowId, query: &str) -> (&'static str, String) {
    if let Err(response) = reject_unknown_query_keys(query, &[]) {
        return response;
    }
    match os.workflows.get(id) {
        Some(workflow) => ("200 OK", workflow_dag_value(os, workflow).to_string()),
        None => (
            "404 Not Found",
            json!({ "error": "workflow not found" }).to_string(),
        ),
    }
}

fn workflow_dag_value(os: &OperatingSystem, workflow: &Workflow) -> serde_json::Value {
    let task_to_stage = workflow
        .tasks
        .iter()
        .map(|(stage, task_id)| (task_id.clone(), stage.clone()))
        .collect::<BTreeMap<_, _>>();

    let nodes = workflow
        .tasks
        .iter()
        .map(|(stage, task_id)| {
            let task = os.tasks.get(task_id);
            json!({
                "stage": stage,
                "task_id": task_id,
                "title": task.map(|task| task.title.as_str()),
                "status": task.map(|task| task.status.to_string()),
                "priority": task.map(|task| task.priority.to_string()),
                "assigned_to": task.and_then(|task| task.assigned_to.as_ref()).map(AgentId::to_string),
                "missing": task.is_none(),
            })
        })
        .collect::<Vec<_>>();

    let mut edges = Vec::new();
    let mut external_dependencies = Vec::new();
    for (to_stage, to_task_id) in &workflow.tasks {
        let Some(task) = os.tasks.get(to_task_id) else {
            continue;
        };
        for dependency in &task.dependencies {
            if let Some(from_stage) = task_to_stage.get(dependency) {
                edges.push(json!({
                    "from": from_stage,
                    "to": to_stage,
                    "from_task_id": dependency,
                    "to_task_id": to_task_id,
                }));
            } else {
                external_dependencies.push(json!({
                    "stage": to_stage,
                    "task_id": to_task_id,
                    "dependency_task_id": dependency,
                }));
            }
        }
    }

    json!({
        "id": workflow.id,
        "objective": workflow.objective,
        "priority": workflow.priority,
        "nodes": nodes,
        "edges": edges,
        "external_dependencies": external_dependencies,
        "progress": os.workflow_progress(&workflow.id),
    })
}

fn workflow_dag_summary(os: &OperatingSystem, workflow: &Workflow) -> String {
    let dag = workflow_dag_value(os, workflow);
    let Some(edges) = dag.get("edges").and_then(Value::as_array) else {
        return "-".into();
    };
    let summary = edges
        .iter()
        .filter_map(|edge| {
            let from = edge.get("from").and_then(Value::as_str)?;
            let to = edge.get("to").and_then(Value::as_str)?;
            Some(format!("{from} -> {to}"))
        })
        .collect::<Vec<_>>();
    if summary.is_empty() {
        "-".into()
    } else {
        summary.join("; ")
    }
}

fn workers_json(
    os: &OperatingSystem,
    query: &str,
) -> Result<serde_json::Value, (&'static str, String)> {
    reject_unknown_query_keys(query, &["status", "since", "until", "query", "limit"])?;
    let status = agent_status_query(query)?;
    let since = timestamp_query(query, "since")?;
    let until = timestamp_query(query, "until")?;
    let search = non_empty_query_string(query, "query")?.map(|query| query.to_ascii_lowercase());
    let limit = positive_query_usize(query, "limit")?;
    let mut workers = os
        .workers
        .values()
        .filter(|worker| {
            status
                .as_ref()
                .map(|status| &worker.status == status)
                .unwrap_or(true)
                && since
                    .as_ref()
                    .map(|since| worker.last_seen_at >= *since)
                    .unwrap_or(true)
                && until
                    .as_ref()
                    .map(|until| worker.last_seen_at <= *until)
                    .unwrap_or(true)
                && search
                    .as_ref()
                    .map(|query| worker_matches_query(worker, query))
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    workers.sort_by(|left, right| {
        right
            .last_seen_at
            .cmp(&left.last_seen_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    if let Some(limit) = limit {
        workers.truncate(limit);
    }
    Ok(json!(workers))
}

fn worker_matches_query(worker: &WorkerNode, query: &str) -> bool {
    worker.id.to_ascii_lowercase().contains(query)
        || worker.endpoint.to_ascii_lowercase().contains(query)
        || worker
            .status
            .to_string()
            .to_ascii_lowercase()
            .contains(query)
}

fn evals_json(
    os: &OperatingSystem,
    query: &str,
) -> Result<serde_json::Value, (&'static str, String)> {
    reject_unknown_query_keys(
        query,
        &["target", "success", "since", "until", "query", "limit"],
    )?;
    let target = non_empty_query_string(query, "target")?;
    let success = bool_query(query, "success")?;
    let since = timestamp_query(query, "since")?;
    let until = timestamp_query(query, "until")?;
    let search = non_empty_query_string(query, "query")?.map(|query| query.to_ascii_lowercase());
    let limit = positive_query_usize(query, "limit")?;
    let mut evals = os
        .evals
        .iter()
        .filter(|record| {
            target
                .as_ref()
                .map(|target| &record.target == target)
                .unwrap_or(true)
                && success
                    .map(|success| record.success == success)
                    .unwrap_or(true)
                && since
                    .as_ref()
                    .map(|since| record.recorded_at >= *since)
                    .unwrap_or(true)
                && until
                    .as_ref()
                    .map(|until| record.recorded_at <= *until)
                    .unwrap_or(true)
                && search
                    .as_ref()
                    .map(|query| eval_matches_query(record, query))
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    evals.sort_by(|left, right| {
        right
            .recorded_at
            .cmp(&left.recorded_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    if let Some(limit) = limit {
        evals.truncate(limit);
    }
    Ok(json!(evals))
}

fn eval_matches_query(record: &EvalRecord, query: &str) -> bool {
    record.id.to_ascii_lowercase().contains(query)
        || record.target.to_ascii_lowercase().contains(query)
        || if record.success { "yes" } else { "no" }.contains(query)
        || record
            .run
            .as_ref()
            .map(|run| {
                run.command.to_ascii_lowercase().contains(query)
                    || run.cwd.to_ascii_lowercase().contains(query)
                    || run.stdout.to_ascii_lowercase().contains(query)
                    || run.stderr.to_ascii_lowercase().contains(query)
                    || run
                        .success_pattern
                        .as_ref()
                        .map(|pattern| pattern.to_ascii_lowercase().contains(query))
                        .unwrap_or(false)
            })
            .unwrap_or(false)
}

fn git_status_json(query: &str) -> Result<serde_json::Value, (&'static str, String)> {
    reject_unknown_query_keys(query, &["cwd"])?;
    let cwd = non_empty_query_string(query, "cwd")?.map(PathBuf::from);
    let cwd = resolve_git_cwd(cwd).map_err(git_integration_error_response)?;
    let output = run_git_command(&cwd, &["status", "--short", "--branch"], false)
        .map_err(git_integration_error_response)?;
    Ok(json!(output))
}

fn create_git_review_task_response(
    store: &Store,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json_or_default::<GitReviewTaskRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_empty_optional_text_field("cwd", request.cwd.as_deref()) {
        return Ok(response);
    }
    if let Some(response) = reject_empty_text_field("base", &request.base) {
        return Ok(response);
    }
    if let Some(response) = reject_empty_text_field("title", &request.title) {
        return Ok(response);
    }
    if let Some(response) = reject_invalid_git_ref("base", &request.base) {
        return Ok(response);
    }
    let priority = match parse_priority_request(&request.priority) {
        Ok(priority) => priority,
        Err(response) => return Ok(response),
    };
    let cwd = request.cwd.map(PathBuf::from);
    let cwd = match resolve_git_cwd(cwd) {
        Ok(cwd) => cwd,
        Err(error) => return Ok(git_integration_error_response(error)),
    };
    let top_level = match run_git_capture(&cwd, &["rev-parse", "--show-toplevel"]) {
        Ok(top_level) => top_level,
        Err(error) => return Ok(git_integration_error_response(error)),
    };
    let top_level = top_level.trim().to_owned();
    if let Some(response) = reject_empty_text_field("git root", &top_level) {
        return Ok(response);
    }
    let base = request.base;
    let command = format!("git diff --stat {base}...HEAD && git diff {base}...HEAD");
    let objective = format!("Review the git diff for {} against {base}", top_level);

    store.update(|os| {
        let mut task = Task::new(
            request.title,
            objective,
            priority,
            vec!["review".to_owned()],
        );
        os.ensure_unique_task_id(&mut task);
        task.command = Some(command);
        task.cwd = Some(top_level);
        let id = task.id.clone();
        os.create_task(task);
        Ok((
            "201 Created",
            json!({
                "id": id,
                "task": os.tasks.get(&id),
            })
            .to_string(),
        ))
    })
}

fn git_integration_error_response(error: GitIntegrationError) -> (&'static str, String) {
    (
        "400 Bad Request",
        json!({
            "error": error.to_string(),
            "stderr": error.stderr(),
        })
        .to_string(),
    )
}

fn secrets_json(
    os: &OperatingSystem,
    query: &str,
) -> Result<serde_json::Value, (&'static str, String)> {
    reject_unknown_query_keys(query, &["kind", "query", "limit"])?;
    let kind = match non_empty_query_string(query, "kind")? {
        Some(kind) => Some(parse_secrets_backend_kind_request(&kind)?),
        None => None,
    };
    let search = non_empty_query_string(query, "query")?.map(|query| query.to_ascii_lowercase());
    let limit = positive_query_usize(query, "limit")?;
    let mut backends = os
        .secrets_backends
        .values()
        .filter(|backend| {
            kind.as_ref()
                .map(|kind| &backend.kind == kind)
                .unwrap_or(true)
                && search
                    .as_ref()
                    .map(|query| secrets_backend_matches_query(backend, query))
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    backends.sort_by(|left, right| left.id.cmp(&right.id));
    if let Some(limit) = limit {
        backends.truncate(limit);
    }
    Ok(json!(backends))
}

fn secrets_backend_matches_query(backend: &SecretsBackend, query: &str) -> bool {
    backend.id.to_ascii_lowercase().contains(query)
        || secrets_backend_kind_name(&backend.kind).contains(query)
        || backend
            .reference
            .as_ref()
            .map(|reference| reference.to_ascii_lowercase().contains(query))
            .unwrap_or(false)
}

fn memory_json(
    os: &OperatingSystem,
    query: &str,
) -> Result<serde_json::Value, (&'static str, String)> {
    reject_unknown_query_keys(
        query,
        &[
            "query",
            "tag",
            "since",
            "until",
            "limit",
            "visibility",
            "scope",
        ],
    )?;
    let search = non_empty_query_string(query, "query")?;
    let tags = tag_query(query)?;
    let visibility = memory_visibility_query(query)?;
    let scope = non_empty_query_string(query, "scope")?;
    let since = timestamp_query(query, "since")?;
    let until = timestamp_query(query, "until")?;
    let limit = positive_query_usize(query, "limit")?;
    let mut records = os
        .memory
        .iter()
        .filter_map(|record| {
            let score = search
                .as_ref()
                .map(|query| memory_relevance_score(record, query))
                .unwrap_or(0);
            (search.is_none() || score > 0).then_some((score, record))
        })
        .filter(|(_, record)| {
            has_all_tags(&record.tags, &tags)
                && visibility
                    .as_ref()
                    .map(|visibility| record.visibility == *visibility)
                    .unwrap_or(true)
                && scope
                    .as_ref()
                    .map(|scope| record.scope.as_deref() == Some(scope.as_str()))
                    .unwrap_or(true)
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
    records.sort_by(|(left_score, left), (right_score, right)| {
        right_score
            .cmp(left_score)
            .then_with(|| right.updated_at.cmp(&left.updated_at))
    });
    if let Some(limit) = limit {
        records.truncate(limit);
    }
    let records = records
        .into_iter()
        .map(|(_, record)| record)
        .collect::<Vec<_>>();
    Ok(json!(records))
}

fn memory_recall_json(
    os: &OperatingSystem,
    query: &str,
) -> Result<serde_json::Value, (&'static str, String)> {
    reject_unknown_query_keys(
        query,
        &[
            "query",
            "tag",
            "since",
            "until",
            "limit",
            "visibility",
            "scope",
        ],
    )?;
    let Some(search) = non_empty_query_string(query, "query")? else {
        return Err((
            "400 Bad Request",
            json!({ "error": "query is required for memory recall" }).to_string(),
        ));
    };
    let tags = tag_query(query)?;
    let visibility = memory_visibility_query(query)?;
    let scope = non_empty_query_string(query, "scope")?;
    let since = timestamp_query(query, "since")?;
    let until = timestamp_query(query, "until")?;
    let limit = positive_query_usize(query, "limit")?;
    let mut hits = os
        .memory
        .iter()
        .filter_map(|record| {
            let score = memory_relevance_score(record, &search);
            (score > 0
                && has_all_tags(&record.tags, &tags)
                && visibility
                    .as_ref()
                    .map(|visibility| record.visibility == *visibility)
                    .unwrap_or(true)
                && scope
                    .as_ref()
                    .map(|scope| record.scope.as_deref() == Some(scope.as_str()))
                    .unwrap_or(true)
                && since
                    .as_ref()
                    .map(|since| record.updated_at >= *since)
                    .unwrap_or(true)
                && until
                    .as_ref()
                    .map(|until| record.updated_at <= *until)
                    .unwrap_or(true))
            .then(|| memory_recall_hit(record, &search, score))
        })
        .collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| right.record.updated_at.cmp(&left.record.updated_at))
    });
    if let Some(limit) = limit {
        hits.truncate(limit);
    }
    Ok(json!(hits))
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

fn bool_query(query: &str, key: &str) -> Result<Option<bool>, (&'static str, String)> {
    let Some(value) = non_empty_query_string(query, key)? else {
        return Ok(None);
    };
    match value.as_str() {
        "true" => Ok(Some(true)),
        "false" => Ok(Some(false)),
        _ => Err(invalid_query_response(key, "must be true or false")),
    }
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

fn parse_worker_report_status_request(status: &str) -> Result<TaskStatus, (&'static str, String)> {
    match TaskStatus::try_parse(status) {
        Some(TaskStatus::Complete) => Ok(TaskStatus::Complete),
        Some(TaskStatus::Failed) => Ok(TaskStatus::Failed),
        Some(_) => Err((
            "400 Bad Request",
            json!({ "error": "worker report status must be complete or failed" }).to_string(),
        )),
        None => Err((
            "400 Bad Request",
            json!({ "error": "invalid worker report status" }).to_string(),
        )),
    }
}

fn parse_run_artifact_kind_request(kind: &str) -> Result<RunArtifactKind, (&'static str, String)> {
    match kind.trim().to_ascii_lowercase().as_str() {
        "stdout" => Ok(RunArtifactKind::Stdout),
        "stderr" => Ok(RunArtifactKind::Stderr),
        "summary" => Ok(RunArtifactKind::Summary),
        "diff" => Ok(RunArtifactKind::Diff),
        "file" => Ok(RunArtifactKind::File),
        _ => Err((
            "400 Bad Request",
            json!({ "error": "invalid run artifact kind" }).to_string(),
        )),
    }
}

fn event_kind_query(query: &str) -> Result<Option<EventKind>, (&'static str, String)> {
    let Some(kind) = non_empty_query_string(query, "kind")? else {
        return Ok(None);
    };
    EventKind::try_parse(&kind)
        .ok_or_else(|| invalid_query_response("kind", "must be a valid event kind"))
        .map(Some)
}

fn memory_visibility_query(
    query: &str,
) -> Result<Option<MemoryVisibility>, (&'static str, String)> {
    let Some(visibility) = non_empty_query_string(query, "visibility")? else {
        return Ok(None);
    };
    MemoryVisibility::try_parse(&visibility)
        .ok_or_else(|| invalid_query_response("visibility", "must be shared or private"))
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
        ("POST", ["workflows", id, "tasks"]) => add_workflow_task_response(store, id, body),
        ("POST", ["workflows", id, "link"]) => edit_workflow_edge_response(store, id, body, true),
        ("POST", ["workflows", id, "unlink"]) => {
            edit_workflow_edge_response(store, id, body, false)
        }
        ("POST", ["workflows", id, "pause"]) => transition_workflow_response(
            store,
            id,
            body,
            "paused",
            |status| matches!(status, TaskStatus::Pending),
            Runtime::block_task,
        ),
        ("POST", ["workflows", id, "resume"]) => transition_workflow_response(
            store,
            id,
            body,
            "resumed",
            |status| matches!(status, TaskStatus::Blocked),
            Runtime::unblock_task,
        ),
        ("POST", ["workflows", id, "retry"]) => transition_workflow_response(
            store,
            id,
            body,
            "retried",
            |status| {
                matches!(
                    status,
                    TaskStatus::Blocked | TaskStatus::Failed | TaskStatus::Cancelled
                )
            },
            Runtime::retry_task,
        ),
        ("POST", ["workflows", id, "run"]) => run_workflow_response(store, id, body),
        ("POST", ["workflows", id, "cancel"]) => cancel_workflow_response(store, id, body),
        ("DELETE", ["workflows", id]) => delete_workflow_response(store, id),
        ("POST", ["approvals", id, "approve"]) => resolve_approval_response(store, id, body, true),
        ("POST", ["approvals", id, "deny"]) => resolve_approval_response(store, id, body, false),
        ("POST", ["workers"]) => register_worker_response(store, body),
        ("POST", ["workers", id, "heartbeat"]) => heartbeat_worker_response(store, id, body),
        ("POST", ["workers", id, "claim"]) => claim_worker_response(store, id, body),
        ("POST", ["workers", id, "report"]) => report_worker_response(store, id, body),
        ("DELETE", ["workers", id]) => delete_worker_response(store, id),
        ("POST", ["evals"]) => record_eval_response(store, body),
        ("POST", ["evals", "run"]) => run_eval_response(store, body),
        ("POST", ["git", "review-task"]) => create_git_review_task_response(store, body),
        ("POST", ["registry", "marketplace-import"]) => {
            import_marketplace_manifest_response(store, body)
        }
        ("POST", ["registry", "profiles", id, "agents"]) => {
            install_agent_profile_response(store, id, body)
        }
        ("POST", ["registry", "mcp-servers"]) => register_mcp_server_response(store, body),
        ("POST", ["registry", "mcp-servers", id]) => update_mcp_server_response(store, id, body),
        ("DELETE", ["registry", "mcp-servers", id]) => delete_mcp_server_response(store, id),
        ("POST", ["registry", "templates", id, "workflows"]) => {
            create_template_workflow_response(store, id, body)
        }
        ("POST", ["secrets"]) => register_secrets_backend_response(store, body),
        ("DELETE", ["secrets", id]) => delete_secrets_backend_response(store, id),
        ("POST", ["tools"]) => create_tool_response(store, body),
        ("POST", ["tools", id]) => update_tool_response(store, id, body),
        ("DELETE", ["tools", id]) => delete_tool_response(store, id),
        ("POST", ["state", "export"]) => export_state_response(store, body),
        ("POST", ["state", "import"]) => import_state_response(store, body),
        ("POST", ["state", "migrate"]) => migrate_state_response(store, body),
        ("POST", ["state", "repair"]) => repair_state_response(store, body),
        ("POST", ["state", "prune"]) => prune_state_response(store, body),
        ("POST", ["state", "backup"]) => backup_state_response(store, body),
        ("POST", ["state", "sqlite"]) => sqlite_state_response(store, body),
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
        ("POST", ["service", "systemd"]) => render_systemd_service_response(store, body),
        ("POST", ["service", "systemd", "install"]) => {
            install_systemd_service_response(store, body)
        }
        ("POST", ["service", "systemd", "uninstall"]) => uninstall_systemd_service_response(body),
        ("POST", ["service", "systemd", "start"]) => start_systemd_service_response(body),
        ("POST", ["service", "systemd", "stop"]) => stop_systemd_service_response(body),
        ("POST", ["service", "systemd", "status"]) => status_systemd_service_response(body),
        ("POST", ["runs", id, "cancel"]) => cancel_run_response(store, id),
        ("POST", ["memory"]) => create_memory_response(store, body),
        ("POST", ["memory", "prune"]) => prune_memory_response(store, body),
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
        | StoreError::Backend { .. }
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
                "config": config.unwrap_or_else(AppConfig::effective_default),
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
    let profile = request.profile.unwrap_or(ConfigProfile::Safe);
    match write_profile_config(path, request.force, profile) {
        Ok(()) => (
            "201 Created",
            json!({
                "path": path.display().to_string(),
                "written": true,
                "profile": profile.as_str(),
                "config": AppConfig::for_profile(profile),
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
    let loaded_config = match config_path {
        Some(path) => match load_config(path) {
            Ok(config) => config,
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
        None => None,
    };
    let config = match (loaded_config, request.profile) {
        (Some(mut config), Some(profile)) => {
            config.apply_profile(profile);
            config
        }
        (Some(config), None) => config,
        (None, Some(profile)) => AppConfig::for_profile(profile),
        (None, None) => AppConfig::effective_default(),
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
    os.memory_policy = config.memory_policy.clone();
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
    #[serde(default)]
    profile: Option<ConfigProfile>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct InitRequest {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    force: bool,
    #[serde(default)]
    profile: Option<ConfigProfile>,
}

#[derive(Default, Deserialize)]
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

#[derive(Default, Deserialize)]
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

#[derive(Default, Deserialize)]
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
    #[serde(default)]
    max_attempts: Option<u32>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct GitReviewTaskRequest {
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default = "default_main_branch")]
    base: String,
    #[serde(default = "default_code_review_title")]
    title: String,
    #[serde(default = "default_normal_priority")]
    priority: String,
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
struct WorkflowAddTaskRequest {
    stage: String,
    title: String,
    #[serde(default)]
    objective: Option<String>,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    required_capabilities: Vec<String>,
    #[serde(default)]
    dependencies: Vec<String>,
    #[serde(default)]
    priority: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowEdgeRequest {
    from: String,
    to: String,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowNoteRequest {
    #[serde(default)]
    note: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApprovalResolveRequest {
    #[serde(default)]
    by: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerRegisterRequest {
    id: String,
    endpoint: String,
    #[serde(default = "default_online_status")]
    status: String,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerHeartbeatRequest {
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    lease_seconds: Option<i64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerReportRequest {
    task_id: String,
    #[serde(default = "default_complete_status")]
    status: String,
    #[serde(default)]
    note: Option<String>,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    exit_code: Option<i32>,
    #[serde(default)]
    artifacts: Vec<WorkerReportArtifactRequest>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerReportArtifactRequest {
    kind: String,
    path: String,
    #[serde(default)]
    bytes: Option<u64>,
    #[serde(default)]
    content_type: Option<String>,
}

fn default_complete_status() -> String {
    "complete".into()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EvalRecordRequest {
    target: String,
    success: bool,
    #[serde(default)]
    cost_micros: Option<u64>,
    #[serde(default)]
    latency_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EvalRunRequest {
    target: String,
    command: String,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    success_pattern: Option<String>,
    #[serde(default)]
    output_schema: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretsRegisterRequest {
    id: String,
    kind: String,
    #[serde(default)]
    reference: Option<String>,
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
    #[serde(default)]
    max_attempts: Option<u32>,
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

#[derive(Clone, Debug, Default, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct MarketplaceManifestMetadata {
    id: String,
    version: String,
    #[serde(default)]
    publisher: Option<String>,
    #[serde(default)]
    homepage: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct MarketplaceManifest {
    #[serde(default)]
    metadata: Option<MarketplaceManifestMetadata>,
    #[serde(default)]
    agent_profiles: Vec<AgentProfile>,
    #[serde(default)]
    workflow_templates: Vec<WorkflowTemplate>,
    #[serde(default)]
    mcp_servers: Vec<McpServer>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketplaceImportRequest {
    manifest: MarketplaceManifest,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    expect_checksum: Option<String>,
    #[serde(default)]
    force: bool,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstallAgentProfileRequest {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default = "default_parallel")]
    parallel: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterMcpServerRequest {
    id: String,
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default = "default_mcp_server_enabled")]
    enabled: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateMcpServerRequest {
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    args: Option<Vec<String>>,
    #[serde(default)]
    env: Option<BTreeMap<String, String>>,
    #[serde(default)]
    enabled: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateMemoryRequest {
    topic: String,
    body: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    visibility: MemoryVisibility,
    #[serde(default)]
    scope: Option<String>,
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
    #[serde(default)]
    visibility: Option<MemoryVisibility>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    clear_scope: bool,
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
struct SystemdServiceRequest {
    #[serde(default = "default_systemd_unit_name")]
    unit_name: String,
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
    unit_path: Option<PathBuf>,
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
struct UninstallSystemdServiceRequest {
    #[serde(default = "default_systemd_unit_name")]
    unit_name: String,
    #[serde(default)]
    unit_path: Option<PathBuf>,
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SystemdServiceControlRequest {
    #[serde(default = "default_systemd_unit_name")]
    unit_name: String,
    #[serde(default = "default_systemctl_path")]
    systemctl_path: PathBuf,
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

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateSqliteRequest {
    #[serde(default)]
    output: Option<String>,
    #[serde(default)]
    init_only: bool,
    #[serde(default)]
    restore: bool,
    #[serde(default)]
    force: bool,
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

impl Default for SystemdServiceRequest {
    fn default() -> Self {
        Self {
            unit_name: default_systemd_unit_name(),
            bin_path: None,
            interval_ms: default_interval_ms(),
            limit: default_parallel(),
            execute: false,
            recover_stale_seconds: None,
            unit_path: None,
        }
    }
}

impl Default for UninstallSystemdServiceRequest {
    fn default() -> Self {
        Self {
            unit_name: default_systemd_unit_name(),
            unit_path: None,
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

impl Default for SystemdServiceControlRequest {
    fn default() -> Self {
        Self {
            unit_name: default_systemd_unit_name(),
            systemctl_path: default_systemctl_path(),
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
    if request.max_attempts == Some(0) {
        return Ok(invalid_text_response(
            "max_attempts",
            "max_attempts must be greater than 0",
        ));
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
            os.ensure_unique_task_id(&mut task);
            if let Some(max_attempts) = request.max_attempts {
                task.max_attempts = max_attempts;
            }
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
        let mut plan = Task::new(
            format!("Plan: {}", objective),
            format!("Design an implementation plan for: {}", objective),
            priority,
            vec!["plan".into()],
        );
        os.ensure_unique_task_id(&mut plan);
        let plan_id = plan.id.clone();

        let mut build = Task::new(
            format!("Build: {}", objective),
            format!("Implement the approved plan for: {}", objective),
            priority,
            vec!["rust".into(), "code".into()],
        );
        os.ensure_unique_task_id(&mut build);
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
        os.ensure_unique_task_id(&mut review);
        review.dependencies.push(build_id.clone());
        let review_id = review.id.clone();

        os.create_task(plan);
        os.create_task(build);
        os.create_task(review);
        let mut workflow = Workflow::new(
            objective,
            priority,
            BTreeMap::from([
                ("plan".into(), plan_id.clone()),
                ("build".into(), build_id.clone()),
                ("review".into(), review_id.clone()),
            ]),
        );
        os.ensure_unique_workflow_id(&mut workflow);
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

fn create_template_workflow_response(
    store: &Store,
    template_id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let template_id = match parse_registry_path_id("template id", template_id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
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
    let template_workflow = store.update(|os| {
        let Some(template) = os.workflow_templates.get(&template_id).cloned() else {
            return Ok(Err((
                "404 Not Found",
                json!({ "error": "workflow template not found" }).to_string(),
            )));
        };
        if template.stages.is_empty() {
            return Ok(Err((
                "409 Conflict",
                json!({
                    "error": "workflow template has no stages",
                    "template": template.id,
                })
                .to_string(),
            )));
        }
        let mut seen_stages = BTreeSet::new();
        for stage in &template.stages {
            if let Some(response) = reject_invalid_workflow_stage("stage", stage) {
                return Ok(Err(response));
            }
            if !seen_stages.insert(stage.clone()) {
                return Ok(Err((
                    "409 Conflict",
                    json!({
                        "error": "workflow template has duplicate stage",
                        "template": template.id,
                        "stage": stage,
                    })
                    .to_string(),
                )));
            }
        }

        let mut task_map = BTreeMap::new();
        let mut pending_tasks = BTreeMap::new();
        for stage in &template.stages {
            let task_spec = workflow_template_task(&template, stage);
            let title = task_spec
                .and_then(|task| task.title.as_ref())
                .map(|title| render_workflow_template_text(title, &request.objective))
                .unwrap_or_else(|| format!("{}: {}", stage, request.objective));
            let objective = task_spec
                .and_then(|task| task.objective.as_ref())
                .map(|objective| render_workflow_template_text(objective, &request.objective))
                .unwrap_or_else(|| {
                    format!("Run template stage `{stage}` for: {}", request.objective)
                });
            let capabilities = task_spec
                .map(|task| task.capabilities.clone())
                .unwrap_or_default();
            let mut task = Task::new(title, objective, priority, capabilities);
            task.command = task_spec
                .and_then(|task| task.command.as_ref())
                .map(|command| render_workflow_template_text(command, &request.objective));
            os.ensure_unique_task_id(&mut task);
            let task_id = task.id.clone();
            task_map.insert(stage.clone(), task_id);
            pending_tasks.insert(stage.clone(), task);
        }
        for edge in workflow_template_edges(&template) {
            let Some(from_id) = task_map.get(&edge.from).cloned() else {
                return Ok(Err((
                    "409 Conflict",
                    json!({
                        "error": "workflow template edge references unknown stage",
                        "template": template.id,
                        "stage": edge.from,
                    })
                    .to_string(),
                )));
            };
            let Some(task) = pending_tasks.get_mut(&edge.to) else {
                return Ok(Err((
                    "409 Conflict",
                    json!({
                        "error": "workflow template edge references unknown stage",
                        "template": template.id,
                        "stage": edge.to,
                    })
                    .to_string(),
                )));
            };
            task.dependencies.push(from_id);
        }
        for task in pending_tasks.into_values() {
            os.create_task(task);
        }
        let mut workflow = Workflow::new(request.objective, priority, task_map.clone());
        os.ensure_unique_workflow_id(&mut workflow);
        let workflow_id = workflow.id.clone();
        os.create_workflow(workflow);
        let Some(workflow) = os.workflows.get(&workflow_id).cloned() else {
            return Ok(Err((
                "404 Not Found",
                json!({ "error": "workflow not found after creation" }).to_string(),
            )));
        };
        Ok(Ok((workflow, task_map, template)))
    })?;
    let (workflow, tasks, template) = match template_workflow {
        Ok(workflow) => workflow,
        Err(response) => return Ok(response),
    };

    let mut runs = Vec::new();
    let mut errors = Vec::new();
    if execute {
        (runs, errors) = execute_workflow_stages(store, &workflow.id, true)?;
    }
    let os = store.load()?;
    let dag = os
        .workflows
        .get(&workflow.id)
        .map(|workflow| workflow_dag_value(&os, workflow))
        .unwrap_or(serde_json::Value::Null);

    Ok((
        "201 Created",
        json!({
            "id": workflow.id,
            "template": template.id,
            "workflow": workflow,
            "tasks": tasks,
            "runs": runs,
            "errors": errors,
            "dag": dag,
        })
        .to_string(),
    ))
}

fn add_workflow_task_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_workflow_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json::<WorkflowAddTaskRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_invalid_workflow_stage("stage", &request.stage) {
        return Ok(response);
    }
    if let Some(response) = reject_empty_text_field("title", &request.title) {
        return Ok(response);
    }
    if let Some(response) =
        reject_empty_optional_text_field("objective", request.objective.as_deref())
    {
        return Ok(response);
    }
    if let Some(response) = reject_empty_optional_text_field("command", request.command.as_deref())
    {
        return Ok(response);
    }
    if let Some(response) = reject_invalid_capability_values(
        "required_capabilities",
        &request.required_capabilities,
        false,
    ) {
        return Ok(response);
    }
    for dependency in &request.dependencies {
        if let Some(response) = reject_invalid_workflow_stage("dependencies", dependency) {
            return Ok(response);
        }
    }
    let priority = match request.priority.as_deref().map(parse_priority_request) {
        Some(Ok(priority)) => Some(priority),
        Some(Err(response)) => return Ok(response),
        None => None,
    };

    store.update(|os| {
        let Some(workflow) = os.workflows.get(&id).cloned() else {
            return Ok((
                "404 Not Found",
                json!({ "error": "workflow not found" }).to_string(),
            ));
        };
        if workflow.tasks.contains_key(&request.stage) {
            return Ok((
                "409 Conflict",
                json!({
                    "error": "workflow stage already exists",
                    "stage": request.stage,
                })
                .to_string(),
            ));
        }
        let mut dependencies = Vec::new();
        for stage in &request.dependencies {
            let Some(task_id) = workflow.tasks.get(stage).cloned() else {
                return Ok((
                    "404 Not Found",
                    json!({
                        "error": "workflow stage not found",
                        "stage": stage,
                    })
                    .to_string(),
                ));
            };
            dependencies.push(task_id);
        }

        let title = request.title;
        let objective = request.objective.unwrap_or_else(|| title.clone());
        let mut task = Task::new(
            title,
            objective,
            priority.unwrap_or(workflow.priority),
            request.required_capabilities,
        );
        os.ensure_unique_task_id(&mut task);
        task.command = request.command;
        task.dependencies = dependencies;
        let task_id = task.id.clone();
        os.create_task(task);
        let Some(workflow) = os.workflows.get_mut(&id) else {
            return Ok((
                "404 Not Found",
                json!({ "error": "workflow not found" }).to_string(),
            ));
        };
        workflow
            .tasks
            .insert(request.stage.clone(), task_id.clone());
        workflow.updated_at = chrono::Utc::now();
        os.record(
            EventKind::WorkflowUpdated,
            format!(
                "added workflow {} stage {} as task {}",
                id, request.stage, task_id
            ),
        );
        let Some(task) = os.tasks.get(&task_id).cloned() else {
            return Ok((
                "404 Not Found",
                json!({ "error": "task not found after creation" }).to_string(),
            ));
        };
        let Some(progress) = os.workflow_progress(&id) else {
            return Ok((
                "404 Not Found",
                json!({ "error": "workflow not found" }).to_string(),
            ));
        };
        Ok((
            "201 Created",
            json!({
                "id": id,
                "stage": request.stage,
                "task_id": task_id,
                "task": task,
                "progress": progress,
            })
            .to_string(),
        ))
    })
}

fn edit_workflow_edge_response(
    store: &Store,
    id: &str,
    body: &[u8],
    add: bool,
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_workflow_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json::<WorkflowEdgeRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_invalid_workflow_stage("from", &request.from) {
        return Ok(response);
    }
    if let Some(response) = reject_invalid_workflow_stage("to", &request.to) {
        return Ok(response);
    }
    if request.from == request.to {
        return Ok((
            "400 Bad Request",
            json!({ "error": "workflow stage cannot depend on itself" }).to_string(),
        ));
    }

    store.update(|os| {
        let (from_task, to_task) =
            match workflow_stage_task_pair_response(os, &id, &request.from, &request.to) {
                Ok(Some(pair)) => pair,
                Ok(None) => {
                    return Ok((
                        "404 Not Found",
                        json!({ "error": "workflow not found" }).to_string(),
                    ));
                }
                Err(response) => return Ok(response),
            };
        let Some(to) = os.tasks.get(&to_task) else {
            return Ok((
                "404 Not Found",
                json!({ "error": "workflow task not found", "task": to_task }).to_string(),
            ));
        };
        let mut dependencies = to.dependencies.clone();
        if add {
            if !dependencies
                .iter()
                .any(|dependency| dependency == &from_task)
            {
                dependencies.push(from_task);
            }
        } else {
            dependencies.retain(|dependency| dependency != &from_task);
        }
        if let Err(error) = Runtime::set_task_dependencies(os, &to_task, dependencies) {
            return Ok(runtime_error_response(error));
        }
        let Some(workflow) = os.workflows.get_mut(&id) else {
            return Ok((
                "404 Not Found",
                json!({ "error": "workflow not found" }).to_string(),
            ));
        };
        workflow.updated_at = chrono::Utc::now();
        os.record(
            EventKind::WorkflowUpdated,
            format!(
                "{} dependency {} -> {} in workflow {}",
                if add { "linked" } else { "unlinked" },
                request.from,
                request.to,
                id
            ),
        );
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
                "from": request.from,
                "to": request.to,
                "linked": add,
                "progress": progress,
            })
            .to_string(),
        ))
    })
}

fn transition_workflow_response<F, T>(
    store: &Store,
    id: &str,
    body: &[u8],
    action: &'static str,
    should_transition: F,
    transition: T,
) -> Result<(&'static str, String), StoreError>
where
    F: Fn(&TaskStatus) -> bool,
    T: Fn(&mut OperatingSystem, &TaskId, Option<String>) -> Result<(), RuntimeError>,
{
    let id = match parse_workflow_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json_or_default::<WorkflowNoteRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_empty_optional_text_field("note", request.note.as_deref()) {
        return Ok(response);
    }

    store.update(|os| {
        let Some(workflow) = os.workflows.get(&id).cloned() else {
            return Ok((
                "404 Not Found",
                json!({ "error": "workflow not found" }).to_string(),
            ));
        };
        let mut affected = Vec::new();
        for task_id in workflow.tasks.values() {
            let Some(task) = os.tasks.get(task_id) else {
                return Ok((
                    "404 Not Found",
                    json!({ "error": "workflow task not found", "task": task_id }).to_string(),
                ));
            };
            if should_transition(&task.status) {
                if let Err(error) = transition(os, task_id, request.note.clone()) {
                    return Ok(runtime_error_response(error));
                }
                affected.push(task_id.clone());
            }
        }
        if !affected.is_empty() {
            let Some(workflow) = os.workflows.get_mut(&id) else {
                return Ok((
                    "404 Not Found",
                    json!({ "error": "workflow not found" }).to_string(),
                ));
            };
            workflow.updated_at = chrono::Utc::now();
            os.record(
                EventKind::WorkflowUpdated,
                format!("{action} {} workflow task(s) for {}", affected.len(), id),
            );
        }
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
                "action": action,
                "affected_tasks": affected,
                "progress": progress,
            })
            .to_string(),
        ))
    })
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
        && request.max_attempts.is_none()
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
    if request.max_attempts == Some(0) {
        return Ok(invalid_text_response(
            "max_attempts",
            "max_attempts must be greater than 0",
        ));
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
            max_attempts: request.max_attempts,
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
        | RuntimeError::WorkflowNotFound(_)
        | RuntimeError::WorkerNotFound(_) => "404 Not Found",
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
        | RuntimeError::WorkerReportRejected { .. }
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

    let recovery = store.update(|os| {
        Ok::<_, StoreError>(Runtime::recover_stale_state(
            os,
            chrono::Duration::seconds(request.older_than_seconds),
        ))
    })?;
    Ok((
        "200 OK",
        json!({
            "older_than_seconds": request.older_than_seconds,
            "recovered": recovery.recovered_tasks,
            "recovered_runs": recovery.recovered_runs,
            "recovered_daemon": recovery.recovered_daemon,
            "notes": recovery.notes,
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

fn resolve_approval_response(
    store: &Store,
    id: &str,
    body: &[u8],
    approved: bool,
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_approval_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json_or_default::<ApprovalResolveRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_empty_optional_text_field("by", request.by.as_deref()) {
        return Ok(response);
    }

    store.update(|os| {
        Ok(match os.resolve_approval(&id, approved, request.by) {
            Some(approval) => (
                "200 OK",
                json!({
                    "id": id,
                    "approval": approval,
                })
                .to_string(),
            ),
            None => (
                "404 Not Found",
                json!({ "error": "approval not found" }).to_string(),
            ),
        })
    })
}

fn register_worker_response(
    store: &Store,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json::<WorkerRegisterRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_invalid_worker_id(&request.id) {
        return Ok(response);
    }
    if let Some(response) = reject_empty_text_field("endpoint", &request.endpoint) {
        return Ok(response);
    }
    let status = match parse_agent_status_request(&request.status) {
        Ok(status) => status,
        Err(response) => return Ok(response),
    };

    store.update(|os| {
        if os.workers.contains_key(&request.id) {
            return Ok((
                "409 Conflict",
                json!({
                    "error": "worker already exists",
                    "id": request.id,
                })
                .to_string(),
            ));
        }
        let worker = WorkerNode {
            id: request.id,
            endpoint: request.endpoint,
            status,
            last_seen_at: chrono::Utc::now(),
        };
        let id = worker.id.clone();
        os.register_worker(worker.clone());
        Ok((
            "201 Created",
            json!({
                "id": id,
                "worker": worker,
            })
            .to_string(),
        ))
    })
}

fn heartbeat_worker_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_worker_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json_or_default::<WorkerHeartbeatRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) =
        reject_empty_optional_text_field("endpoint", request.endpoint.as_deref())
    {
        return Ok(response);
    }
    if let Some(response) = reject_non_positive_lease(request.lease_seconds) {
        return Ok(response);
    }
    let status = match request.status.as_deref().map(parse_agent_status_request) {
        Some(Ok(status)) => Some(status),
        Some(Err(response)) => return Ok(response),
        None => None,
    };

    store.update(|os| {
        Ok(
            match Runtime::heartbeat_worker(
                os,
                &id,
                request.endpoint,
                status,
                request.lease_seconds,
            ) {
                Ok(worker) => (
                    "200 OK",
                    json!({
                        "id": id,
                        "worker": worker,
                    })
                    .to_string(),
                ),
                Err(error) => runtime_error_response(error),
            },
        )
    })
}

fn claim_worker_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_worker_path_id(id) {
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
        let agent_id = AgentId::new(&id);
        if !os.workers.contains_key(&id) {
            return Ok((
                "404 Not Found",
                json!({ "error": "worker not found" }).to_string(),
            ));
        }
        if !os.agents.contains_key(&agent_id) {
            return Ok((
                "409 Conflict",
                json!({
                    "error": "matching agent not found for worker",
                    "agent_id": agent_id,
                    "worker_id": id,
                })
                .to_string(),
            ));
        }
        {
            let Some(worker) = os.workers.get_mut(&id) else {
                return Ok((
                    "404 Not Found",
                    json!({ "error": "worker not found" }).to_string(),
                ));
            };
            worker.status = AgentStatus::Online;
            worker.last_seen_at = chrono::Utc::now();
        }
        let lease_seconds = request.lease_seconds.or_else(|| {
            os.agents
                .get(&agent_id)
                .and_then(|agent| agent.lease_expires_at)
                .and_then(|expires_at| {
                    let remaining = (expires_at - chrono::Utc::now()).num_seconds();
                    (remaining > 0).then_some(remaining)
                })
        });
        let response =
            match Runtime::heartbeat_agent(os, &agent_id, AgentStatus::Online, lease_seconds) {
                Ok(()) => {
                    let assignment = Scheduler::assign_next_for_agent(os, &agent_id);
                    let task = assignment
                        .as_ref()
                        .and_then(|assignment| os.tasks.get(&assignment.task_id))
                        .cloned();
                    let worker = os.workers.get(&id).cloned();
                    os.record(
                        EventKind::WorkerUpdated,
                        format!("worker {id} claimed task via agent {agent_id}"),
                    );
                    (
                        "200 OK",
                        json!({
                            "id": id,
                            "worker": worker,
                            "claimed": assignment.is_some(),
                            "assignment": assignment,
                            "task": task,
                        })
                        .to_string(),
                    )
                }
                Err(error) => runtime_error_response(error),
            };
        Ok(response)
    })
}

fn report_worker_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_worker_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json::<WorkerReportRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let task_id = match parse_task_path_id(&request.task_id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let status = match parse_worker_report_status_request(&request.status) {
        Ok(status) => status,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_empty_optional_text_field("note", request.note.as_deref()) {
        return Ok(response);
    }
    if let Some(response) = reject_empty_optional_text_field("command", request.command.as_deref())
    {
        return Ok(response);
    }
    if let Some(response) = reject_empty_optional_text_field("cwd", request.cwd.as_deref()) {
        return Ok(response);
    }
    let mut artifacts = Vec::new();
    for artifact in request.artifacts {
        let kind = match parse_run_artifact_kind_request(&artifact.kind) {
            Ok(kind) => kind,
            Err(response) => return Ok(response),
        };
        if let Some(response) = reject_empty_text_field("artifact path", &artifact.path) {
            return Ok(response);
        }
        if let Some(response) = reject_empty_optional_text_field(
            "artifact content_type",
            artifact.content_type.as_deref(),
        ) {
            return Ok(response);
        }
        let mut artifact_record = RunArtifact::new(kind, artifact.path);
        artifact_record.bytes = artifact.bytes;
        artifact_record.content_type = artifact.content_type;
        artifacts.push(artifact_record);
    }

    store.update(|os| {
        Ok(
            match Runtime::report_worker_task(
                os,
                &id,
                &task_id,
                status,
                request.note,
                request.command,
                request.cwd,
                request.exit_code,
                artifacts,
            ) {
                Ok((worker, task, run)) => (
                    "200 OK",
                    json!({
                        "id": id,
                        "worker": worker,
                        "task": task,
                        "run": run,
                        "reported": true,
                    })
                    .to_string(),
                ),
                Err(error) => runtime_error_response(error),
            },
        )
    })
}

fn delete_worker_response(store: &Store, id: &str) -> Result<(&'static str, String), StoreError> {
    let id = match parse_worker_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    store.update(|os| {
        Ok(match os.workers.remove(&id) {
            Some(worker) => {
                os.record(EventKind::WorkerRemoved, format!("removed worker {id}"));
                (
                    "200 OK",
                    json!({
                        "id": id,
                        "removed": true,
                        "worker": worker,
                    })
                    .to_string(),
                )
            }
            None => (
                "404 Not Found",
                json!({ "error": "worker not found" }).to_string(),
            ),
        })
    })
}

fn record_eval_response(store: &Store, body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json::<EvalRecordRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_empty_text_field("target", &request.target) {
        return Ok(response);
    }
    store.update(|os| {
        let record = os.record_eval(
            EvalRecord {
                id: os.next_eval_id(),
                target: request.target,
                success: request.success,
                cost_micros: request.cost_micros,
                latency_ms: request.latency_ms,
                run: None,
                recorded_at: chrono::Utc::now(),
            },
            "recorded",
        );
        Ok((
            "201 Created",
            json!({
                "id": record.id,
                "eval": record,
            })
            .to_string(),
        ))
    })
}

fn import_marketplace_manifest_response(
    store: &Store,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json::<MarketplaceImportRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let mut manifest = request.manifest;
    if let Some(response) = reject_empty_optional_text_field("source", request.source.as_deref()) {
        return Ok(response);
    }
    if let Some(response) =
        reject_empty_optional_text_field("expect_checksum", request.expect_checksum.as_deref())
    {
        return Ok(response);
    }
    if let Some(response) = reject_invalid_marketplace_manifest(&mut manifest) {
        return Ok(response);
    }
    let manifest_bytes = match serde_json::to_vec(&manifest) {
        Ok(bytes) => bytes,
        Err(error) => {
            return Ok((
                "500 Internal Server Error",
                json!({
                    "error": "marketplace manifest serialization failed",
                    "detail": error.to_string(),
                })
                .to_string(),
            ));
        }
    };
    let checksum = fnv1a64_checksum(&manifest_bytes);
    if let Some(expected) = &request.expect_checksum
        && expected != &checksum
    {
        return Ok((
            "409 Conflict",
            json!({
                "error": "marketplace checksum mismatch",
                "expected": expected,
                "checksum": checksum,
            })
            .to_string(),
        ));
    }
    let source = request.source.unwrap_or_else(|| "api".into());
    let manifest_id = manifest
        .metadata
        .as_ref()
        .map(|metadata| metadata.id.clone());
    let manifest_version = manifest
        .metadata
        .as_ref()
        .map(|metadata| metadata.version.clone());
    let imported_agent_profiles = manifest.agent_profiles.len();
    let imported_workflow_templates = manifest.workflow_templates.len();
    let imported_mcp_servers = manifest.mcp_servers.len();
    let force = request.force;
    let verified_checksum = request.expect_checksum.is_some();
    store.update(|os| {
        if !force {
            for profile in &manifest.agent_profiles {
                if os.agent_profiles.contains_key(&profile.id) {
                    return Ok((
                        "409 Conflict",
                        json!({
                            "error": "agent profile already exists",
                            "id": profile.id,
                        })
                        .to_string(),
                    ));
                }
            }
            for template in &manifest.workflow_templates {
                if os.workflow_templates.contains_key(&template.id) {
                    return Ok((
                        "409 Conflict",
                        json!({
                            "error": "workflow template already exists",
                            "id": template.id,
                        })
                        .to_string(),
                    ));
                }
            }
            for server in &manifest.mcp_servers {
                if os.mcp_servers.contains_key(&server.id) {
                    return Ok((
                        "409 Conflict",
                        json!({
                            "error": "mcp server already exists",
                            "id": server.id,
                        })
                        .to_string(),
                    ));
                }
            }
        }

        let mut overwritten = 0usize;
        for profile in manifest.agent_profiles {
            if os
                .agent_profiles
                .insert(profile.id.clone(), profile)
                .is_some()
            {
                overwritten += 1;
            }
        }
        for template in manifest.workflow_templates {
            if os
                .workflow_templates
                .insert(template.id.clone(), template)
                .is_some()
            {
                overwritten += 1;
            }
        }
        for server in manifest.mcp_servers {
            if os.mcp_servers.insert(server.id.clone(), server).is_some() {
                overwritten += 1;
            }
        }
        os.record(
            EventKind::MarketplaceImported,
            format!(
                "imported marketplace {} profiles, {} templates, {} mcp servers from {} ({})",
                imported_agent_profiles,
                imported_workflow_templates,
                imported_mcp_servers,
                source,
                checksum
            ),
        );
        Ok((
            "201 Created",
            json!({
                "source": source,
                "checksum": checksum,
                "verified_checksum": verified_checksum,
                "manifest_id": manifest_id,
                "manifest_version": manifest_version,
                "imported_agent_profiles": imported_agent_profiles,
                "imported_workflow_templates": imported_workflow_templates,
                "imported_mcp_servers": imported_mcp_servers,
                "overwritten": overwritten,
            })
            .to_string(),
        ))
    })
}

fn install_agent_profile_response(
    store: &Store,
    profile_id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let profile_id = match parse_registry_path_id("profile id", profile_id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json_or_default::<InstallAgentProfileRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(name) = &request.name
        && let Some(response) = reject_invalid_agent_name(name)
    {
        return Ok(response);
    }
    if let Some(response) = reject_empty_optional_text_field("model", request.model.as_deref()) {
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
        let Some(profile) = os.agent_profiles.get(&profile_id).cloned() else {
            return Ok((
                "404 Not Found",
                json!({ "error": "agent profile not found" }).to_string(),
            ));
        };
        let agent = Agent::new(
            request.name.unwrap_or(profile.name),
            profile.kind,
            request.model.or(profile.model),
            profile.capabilities,
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
                "profile": profile_id,
                "agent": os.agents.get(&id),
            })
            .to_string(),
        ))
    })
}

fn register_mcp_server_response(
    store: &Store,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json::<RegisterMcpServerRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let id = match parse_registry_path_id("mcp server id", &request.id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_empty_text_field("command", &request.command) {
        return Ok(response);
    }
    if let Some(response) = reject_invalid_mcp_args(&request.args) {
        return Ok(response);
    }
    if let Some(response) = reject_invalid_mcp_env(&request.env) {
        return Ok(response);
    }

    store.update(|os| {
        if os.mcp_servers.contains_key(&id) {
            return Ok((
                "409 Conflict",
                json!({
                    "error": "mcp server already exists",
                    "id": id,
                })
                .to_string(),
            ));
        }
        let server = McpServer {
            id: id.clone(),
            command: request.command,
            args: request.args,
            env: request.env,
            enabled: request.enabled,
        };
        os.register_mcp_server(server.clone());
        Ok((
            "201 Created",
            json!({
                "id": id,
                "mcp_server": server,
            })
            .to_string(),
        ))
    })
}

fn update_mcp_server_response(
    store: &Store,
    id: &str,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_registry_path_id("mcp server id", id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let request = match parse_json::<UpdateMcpServerRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(command) = &request.command
        && let Some(response) = reject_empty_text_field("command", command)
    {
        return Ok(response);
    }
    if let Some(args) = &request.args
        && let Some(response) = reject_invalid_mcp_args(args)
    {
        return Ok(response);
    }
    if let Some(env) = &request.env
        && let Some(response) = reject_invalid_mcp_env(env)
    {
        return Ok(response);
    }

    store.update(|os| {
        let Some(server) = os.mcp_servers.get_mut(&id) else {
            return Ok((
                "404 Not Found",
                json!({ "error": "mcp server not found" }).to_string(),
            ));
        };
        if let Some(command) = request.command {
            server.command = command;
        }
        if let Some(args) = request.args {
            server.args = args;
        }
        if let Some(env) = request.env {
            server.env = env;
        }
        if let Some(enabled) = request.enabled {
            server.enabled = enabled;
        }
        let server = server.clone();
        os.record(
            EventKind::McpServerUpdated,
            format!("updated mcp server {id}"),
        );
        Ok((
            "200 OK",
            json!({
                "id": id,
                "mcp_server": server,
            })
            .to_string(),
        ))
    })
}

fn delete_mcp_server_response(
    store: &Store,
    id: &str,
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_registry_path_id("mcp server id", id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    store.update(|os| {
        Ok(match os.mcp_servers.remove(&id) {
            Some(server) => {
                os.record(
                    EventKind::McpServerRemoved,
                    format!("removed mcp server {id}"),
                );
                (
                    "200 OK",
                    json!({
                        "id": id,
                        "removed": true,
                        "mcp_server": server,
                    })
                    .to_string(),
                )
            }
            None => (
                "404 Not Found",
                json!({ "error": "mcp server not found" }).to_string(),
            ),
        })
    })
}

fn register_secrets_backend_response(
    store: &Store,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json::<SecretsRegisterRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_invalid_secrets_backend_id(&request.id) {
        return Ok(response);
    }
    let kind = match parse_secrets_backend_kind_request(&request.kind) {
        Ok(kind) => kind,
        Err(response) => return Ok(response),
    };
    if let Some(response) =
        reject_empty_optional_text_field("reference", request.reference.as_deref())
    {
        return Ok(response);
    }

    store.update(|os| {
        if os.secrets_backends.contains_key(&request.id) {
            return Ok((
                "409 Conflict",
                json!({
                    "error": "secrets backend already exists",
                    "id": request.id,
                })
                .to_string(),
            ));
        }
        let backend = SecretsBackend {
            id: request.id,
            kind,
            reference: request.reference,
        };
        let id = backend.id.clone();
        os.register_secrets_backend(backend.clone());
        Ok((
            "201 Created",
            json!({
                "id": id,
                "secrets_backend": backend,
            })
            .to_string(),
        ))
    })
}

fn delete_secrets_backend_response(
    store: &Store,
    id: &str,
) -> Result<(&'static str, String), StoreError> {
    let id = match parse_secrets_backend_path_id(id) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    store.update(|os| {
        Ok(match os.secrets_backends.remove(&id) {
            Some(backend) => {
                os.record(
                    EventKind::SecretsBackendRemoved,
                    format!("removed secrets backend {id}"),
                );
                (
                    "200 OK",
                    json!({
                        "id": id,
                        "removed": true,
                        "secrets_backend": backend,
                    })
                    .to_string(),
                )
            }
            None => (
                "404 Not Found",
                json!({ "error": "secrets backend not found" }).to_string(),
            ),
        })
    })
}

fn run_eval_response(store: &Store, body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json::<EvalRunRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if let Some(response) = reject_empty_text_field("target", &request.target) {
        return Ok(response);
    }
    if let Some(response) = reject_empty_text_field("command", &request.command) {
        return Ok(response);
    }
    if let Some(response) =
        reject_empty_optional_text_field("success_pattern", request.success_pattern.as_deref())
    {
        return Ok(response);
    }
    if let Some(response) = reject_empty_optional_text_field("cwd", request.cwd.as_deref()) {
        return Ok(response);
    }

    let os = store.load()?;
    if let Err(error) = check_shell_command(&os.policy, &request.command) {
        return Ok((
            "409 Conflict",
            json!({ "error": error.to_string() }).to_string(),
        ));
    }
    let cwd = request
        .cwd
        .as_ref()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."))
        });
    if let Err(error) = check_workspace(&os.policy, &cwd) {
        return Ok((
            "409 Conflict",
            json!({ "error": error.to_string() }).to_string(),
        ));
    }
    if let Err(error) = check_shell_writes(&os.policy, &request.command, &cwd) {
        return Ok((
            "409 Conflict",
            json!({ "error": error.to_string() }).to_string(),
        ));
    }
    let env = eval_environment(&os.policy);
    let started = std::time::Instant::now();
    let output = match run_shell_capture(
        &request.command,
        &cwd,
        &env,
        std::time::Duration::from_secs(os.policy.command_timeout_seconds),
        os.policy.sandbox.process_isolation,
        os.policy.max_output_bytes,
    ) {
        Ok(output) => output,
        Err(error) => {
            return Ok((
                "400 Bad Request",
                json!({
                    "error": format!("could not run eval command: {error}"),
                })
                .to_string(),
            ));
        }
    };
    let latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let combined = format!("{stdout}{stderr}");
    let success_pattern_matched = request
        .success_pattern
        .as_ref()
        .map(|pattern| combined.contains(pattern))
        .unwrap_or(true);
    let (output_schema_valid, output_schema_error) =
        eval_output_schema_report(&stdout, request.output_schema.as_ref());
    let success = !output.timed_out
        && output.status_code == Some(0)
        && success_pattern_matched
        && output_schema_valid;
    let status_code = output.status_code;
    let run_details = EvalRunDetails {
        command: request.command.clone(),
        cwd: cwd.display().to_string(),
        status: status_code,
        stdout: tail_text_by_bytes(&stdout, Some(os.policy.max_output_bytes)),
        stderr: tail_text_by_bytes(&stderr, Some(os.policy.max_output_bytes)),
        success_pattern: request.success_pattern.clone(),
        success_pattern_matched,
        output_schema: request.output_schema.clone(),
        output_schema_valid,
        output_schema_error,
        timed_out: output.timed_out,
    };
    store.update(|os| {
        let record = os.record_eval(
            EvalRecord {
                id: os.next_eval_id(),
                target: request.target,
                success,
                cost_micros: None,
                latency_ms: Some(latency_ms),
                run: Some(run_details.clone()),
                recorded_at: chrono::Utc::now(),
            },
            "ran",
        );
        Ok((
            "201 Created",
            json!({
                "id": record.id,
                "eval": record,
                "command": run_details.command,
                "cwd": run_details.cwd,
                "status": run_details.status,
                "stdout": run_details.stdout,
                "stderr": run_details.stderr,
                "success_pattern_matched": run_details.success_pattern_matched,
                "output_schema_valid": run_details.output_schema_valid,
                "output_schema_error": run_details.output_schema_error,
                "timed_out": run_details.timed_out,
            })
            .to_string(),
        ))
    })
}

fn eval_output_schema_report(stdout: &str, schema: Option<&Value>) -> (bool, Option<String>) {
    let Some(schema) = schema else {
        return (true, None);
    };
    let output = match serde_json::from_str::<Value>(stdout) {
        Ok(output) => output,
        Err(error) => {
            return (
                false,
                Some(format!(
                    "output_schema requires stdout to be a JSON value: {error}"
                )),
            );
        }
    };
    match validate_json_schema(&output, schema, "output", "output_schema") {
        Ok(()) => (true, None),
        Err(error) => (false, Some(error.to_string())),
    }
}

fn eval_environment(policy: &crate::models::Policy) -> Vec<(String, String)> {
    let mut env = std::env::vars()
        .filter(|(key, _)| {
            policy.inherit_environment
                || policy.allowed_env_vars.iter().any(|allowed| allowed == key)
        })
        .collect::<Vec<_>>();
    env.sort_by(|left, right| left.0.cmp(&right.0));
    env
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

fn sqlite_state_response(store: &Store, body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json_or_default::<StateSqliteRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if request.init_only && request.restore {
        return Ok((
            "400 Bad Request",
            json!({ "error": "init_only cannot be combined with restore" }).to_string(),
        ));
    }
    let output = match request.output {
        Some(output) if output.trim().is_empty() => {
            return Ok(invalid_text_response("output", "output must not be empty"));
        }
        Some(output) => std::path::PathBuf::from(output),
        None => store.path().with_extension("sqlite"),
    };
    let sqlite = SqliteStore::new(&output);
    if request.restore {
        let restore = sqlite
            .restore_json_store(store, request.force, request.dry_run)
            .map_err(sqlite_state_error)?;
        return Ok((
            "200 OK",
            json!({
                "path": sqlite.path(),
                "state_path": store.path(),
                "initialized": true,
                "imported": false,
                "dry_run": request.dry_run,
                "force": request.force,
                "import": null,
                "restore": restore,
            })
            .to_string(),
        ));
    }

    let import = if request.init_only {
        sqlite.init().map_err(sqlite_state_error)?;
        None
    } else {
        Some(
            sqlite
                .import_json_store(store)
                .map_err(sqlite_state_error)?,
        )
    };
    Ok((
        "200 OK",
        json!({
            "path": sqlite.path(),
            "state_path": store.path(),
            "initialized": true,
            "imported": !request.init_only,
            "dry_run": false,
            "force": false,
            "import": import,
            "restore": null,
        })
        .to_string(),
    ))
}

fn sqlite_state_error(error: SqliteStoreError) -> StoreError {
    match error {
        SqliteStoreError::Store(error) => error,
        SqliteStoreError::Sqlite { path, source } => StoreError::Backend {
            path,
            message: source.to_string(),
        },
        SqliteStoreError::Json { path, source } => StoreError::Backend {
            path,
            message: format!("invalid sqlite state payload: {source}"),
        },
    }
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
    let output_preexisting = output.exists();
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
            "output_preexisting": output_preexisting,
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
            report.absorb_recovery(Runtime::recover_stale_state(
                &mut os,
                chrono::Duration::seconds(seconds),
            ));
        }
        let tick_report = Runtime::tick(&mut os, request.limit);
        report.absorb_tick(tick_report);
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
            report.absorb_recovery(Runtime::recover_stale_state(
                os,
                chrono::Duration::seconds(seconds),
            ));
        }
        let tick_report = Runtime::tick(os, request.limit);
        report.absorb_tick(tick_report);
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

fn render_systemd_service_response(
    store: &Store,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json_or_default::<SystemdServiceRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let options = systemd_options_from_request(request);
    match build_systemd_service_definition(store.path(), options) {
        Ok((service, unit_path)) => Ok((
            "200 OK",
            json!({
                "platform": "systemd",
                "service": service,
                "unit": service.render_unit(),
                "unit_path": unit_path,
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

fn install_systemd_service_response(
    store: &Store,
    body: &[u8],
) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json_or_default::<SystemdServiceRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let installation = match install_systemd_service_definition(
        store.path(),
        systemd_options_from_request(request),
    ) {
        Ok(installation) => installation,
        Err(error) => return Ok(service_error_response(error)),
    };
    Ok((
        "200 OK",
        json!({
            "platform": "systemd",
            "installed": installation.installed,
            "unit_path": installation.unit_path,
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

fn uninstall_systemd_service_response(body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json_or_default::<UninstallSystemdServiceRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let removal = match uninstall_systemd_service(&request.unit_name, request.unit_path) {
        Ok(removal) => removal,
        Err(error) => return Ok(service_error_response(error)),
    };
    Ok((
        "200 OK",
        json!({
            "platform": "systemd",
            "removed": removal.removed,
            "unit_path": removal.unit_path,
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

fn start_systemd_service_response(body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_systemd_control_request(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let unit_name = request.unit_name.clone();
    let output = match run_systemctl(&request.systemctl_path, &["--user", "start", &unit_name]) {
        Ok(output) => output,
        Err(error) => return Ok(service_error_response(error)),
    };
    if !output.success {
        return Ok(systemctl_failure_response("start", &unit_name, &output));
    }
    Ok((
        "200 OK",
        json!({
            "platform": "systemd",
            "started": true,
            "unit_name": unit_name,
            "systemctl": output,
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

fn stop_systemd_service_response(body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_systemd_control_request(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let unit_name = request.unit_name.clone();
    let output = match run_systemctl(&request.systemctl_path, &["--user", "stop", &unit_name]) {
        Ok(output) => output,
        Err(error) => return Ok(service_error_response(error)),
    };
    if !output.success {
        return Ok(systemctl_failure_response("stop", &unit_name, &output));
    }
    Ok((
        "200 OK",
        json!({
            "platform": "systemd",
            "stopped": true,
            "unit_name": unit_name,
            "systemctl": output,
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

fn status_systemd_service_response(body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_systemd_control_request(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let unit_name = request.unit_name.clone();
    let output = match run_systemctl(
        &request.systemctl_path,
        &["--user", "is-active", &unit_name],
    ) {
        Ok(output) => output,
        Err(error) => return Ok(service_error_response(error)),
    };
    Ok((
        "200 OK",
        json!({
            "platform": "systemd",
            "active": output.success,
            "unit_name": unit_name,
            "systemctl": output,
        })
        .to_string(),
    ))
}

fn systemd_options_from_request(request: SystemdServiceRequest) -> SystemdServiceOptions {
    SystemdServiceOptions {
        unit_name: request.unit_name,
        program: request.bin_path,
        interval_ms: request.interval_ms,
        limit: request.limit,
        execute: request.execute,
        recover_stale_seconds: request.recover_stale_seconds,
        unit_path: request.unit_path,
    }
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

fn parse_systemd_control_request(
    body: &[u8],
) -> Result<SystemdServiceControlRequest, (&'static str, String)> {
    let request = parse_json_or_default::<SystemdServiceControlRequest>(body)?;
    validate_systemd_control_inputs(&request.unit_name, &request.systemctl_path)
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

fn systemctl_failure_response(
    action: &str,
    unit_name: &str,
    output: &crate::service::SystemctlCommandOutput,
) -> (&'static str, String) {
    (
        "409 Conflict",
        json!({
            "error": "service command failed",
            "detail": format!("systemctl {action} failed for {unit_name}: {}", output.stderr.trim()),
            "systemctl": output,
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
    if let Some(scope) = &request.scope
        && let Some(response) = reject_empty_text_field("scope", scope)
    {
        return Ok(response);
    }
    store.update(|os| {
        let record = MemoryRecord::with_access(
            request.topic,
            request.body,
            request.tags,
            request.visibility,
            request.scope,
        );
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

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PruneMemoryRequest {
    max_age_days: Option<u64>,
    #[serde(default)]
    dry_run: bool,
}

fn prune_memory_response(store: &Store, body: &[u8]) -> Result<(&'static str, String), StoreError> {
    let request = match parse_json_or_default::<PruneMemoryRequest>(body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    if matches!(request.max_age_days, Some(0)) {
        return Ok((
            "400 Bad Request",
            json!({ "error": "max_age_days must be greater than 0" }).to_string(),
        ));
    }
    let max_age_days = if let Some(max_age_days) = request.max_age_days {
        max_age_days
    } else {
        match store.load()?.memory_policy.max_age_days {
            Some(max_age_days) if max_age_days > 0 => max_age_days,
            _ => {
                return Ok((
                    "400 Bad Request",
                    json!({ "error": "memory max_age_days is not configured" }).to_string(),
                ));
            }
        }
    };
    if request.dry_run {
        let os = store.load()?;
        let expired = os.expired_memory(chrono::Utc::now(), max_age_days);
        return Ok((
            "200 OK",
            json!({
                "dry_run": true,
                "max_age_days": max_age_days,
                "removed": [],
                "expired": expired,
            })
            .to_string(),
        ));
    }
    store.update(|os| {
        let removed = os.prune_expired_memory(chrono::Utc::now(), max_age_days);
        Ok((
            "200 OK",
            json!({
                "dry_run": false,
                "max_age_days": max_age_days,
                "removed": removed,
                "expired": [],
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
    if let Some(scope) = &request.scope
        && let Some(response) = reject_empty_text_field("scope", scope)
    {
        return Ok(response);
    }
    if request.clear_scope && request.scope.is_some() {
        return Ok(invalid_text_response(
            "scope",
            "use either scope or clear_scope, not both",
        ));
    }
    if request.topic.is_none()
        && request.body.is_none()
        && request.tags.is_none()
        && !request.clear_tags
        && request.visibility.is_none()
        && request.scope.is_none()
        && !request.clear_scope
    {
        return Ok((
            "400 Bad Request",
            json!({ "error": "memory update must include topic, body, tags, clear_tags, visibility, scope, or clear_scope" })
                .to_string(),
        ));
    }
    let tags = if request.clear_tags {
        Some(Vec::new())
    } else {
        request.tags
    };
    let scope = if request.clear_scope {
        Some(None)
    } else {
        request.scope.map(Some)
    };
    store.update(|os| {
        Ok(
            match os.update_memory(
                &id,
                request.topic,
                request.body,
                tags,
                request.visibility,
                scope,
            ) {
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

fn reject_invalid_git_ref(field: &str, value: &str) -> Option<(&'static str, String)> {
    if value.chars().any(|ch| {
        ch.is_ascii_control() || matches!(ch, ' ' | '~' | '^' | ':' | '?' | '*' | '[' | '\\')
    }) {
        return Some(invalid_text_response(
            field,
            &format!("{field} contains characters git refs cannot safely use"),
        ));
    }
    None
}

fn parse_tool_kind_request(input: &str) -> Result<ToolKind, (&'static str, String)> {
    ToolKind::try_parse(input)
        .ok_or_else(|| invalid_choice_response("kind", input, ToolKind::INPUT_VALUES))
}

const SECRETS_BACKEND_KIND_INPUT_VALUES: &[&str] = &[
    "environment",
    "env",
    "one-password",
    "1password",
    "1-password",
    "op",
    "os-keychain",
    "keychain",
    "macos-keychain",
    "env-vault",
    "envvault",
];

const SECRETS_BACKEND_KIND_VALUES: &[&str] =
    &["environment", "one-password", "os-keychain", "env-vault"];

fn parse_secrets_backend_kind_request(
    input: &str,
) -> Result<SecretsBackendKind, (&'static str, String)> {
    match input.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "environment" | "env" => Ok(SecretsBackendKind::Environment),
        "one-password" | "1password" | "1-password" | "op" => Ok(SecretsBackendKind::OnePassword),
        "os-keychain" | "keychain" | "macos-keychain" => Ok(SecretsBackendKind::OsKeychain),
        "env-vault" | "envvault" => Ok(SecretsBackendKind::EnvVault),
        _ => Err(invalid_choice_response(
            "kind",
            input,
            SECRETS_BACKEND_KIND_INPUT_VALUES,
        )),
    }
}

fn secrets_backend_kind_name(kind: &SecretsBackendKind) -> &'static str {
    match kind {
        SecretsBackendKind::Environment => "environment",
        SecretsBackendKind::OnePassword => "one-password",
        SecretsBackendKind::OsKeychain => "os-keychain",
        SecretsBackendKind::EnvVault => "env-vault",
    }
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

fn parse_approval_path_id(id: &str) -> Result<String, (&'static str, String)> {
    if !contains_slug_character(id) {
        Err(invalid_text_response(
            "approval id",
            "approval id must contain at least one ASCII letter, digit, or hyphen",
        ))
    } else {
        Ok(id.to_owned())
    }
}

fn parse_worker_path_id(id: &str) -> Result<String, (&'static str, String)> {
    if !contains_slug_character(id) {
        Err(invalid_text_response(
            "worker id",
            "worker id must contain at least one ASCII letter, digit, or hyphen",
        ))
    } else {
        Ok(id.to_owned())
    }
}

fn parse_eval_path_id(id: &str) -> Result<String, (&'static str, String)> {
    if !contains_slug_character(id) {
        Err(invalid_text_response(
            "eval id",
            "eval id must contain at least one ASCII letter, digit, or hyphen",
        ))
    } else {
        Ok(id.to_owned())
    }
}

fn parse_secrets_backend_path_id(id: &str) -> Result<String, (&'static str, String)> {
    if !contains_slug_character(id) {
        Err(invalid_text_response(
            "secrets backend id",
            "secrets backend id must contain at least one ASCII letter, digit, or hyphen",
        ))
    } else {
        Ok(id.to_owned())
    }
}

fn parse_registry_path_id(field: &'static str, id: &str) -> Result<String, (&'static str, String)> {
    if !contains_slug_character(id) {
        Err(invalid_text_response(
            field,
            &format!("{field} must contain at least one ASCII letter, digit, or hyphen"),
        ))
    } else {
        Ok(id.to_owned())
    }
}

fn reject_invalid_worker_id(id: &str) -> Option<(&'static str, String)> {
    (!contains_slug_character(id)).then(|| {
        invalid_text_response(
            "id",
            "worker id must contain at least one ASCII letter, digit, or hyphen",
        )
    })
}

fn reject_invalid_secrets_backend_id(id: &str) -> Option<(&'static str, String)> {
    (!contains_slug_character(id)).then(|| {
        invalid_text_response(
            "id",
            "secrets backend id must contain at least one ASCII letter, digit, or hyphen",
        )
    })
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

fn reject_invalid_mcp_args(args: &[String]) -> Option<(&'static str, String)> {
    for arg in args {
        if let Some(response) = reject_empty_text_field("mcp argument", arg) {
            return Some(response);
        }
    }
    None
}

fn reject_invalid_mcp_env(env: &BTreeMap<String, String>) -> Option<(&'static str, String)> {
    for key in env.keys() {
        if !crate::models::is_valid_env_var_name(key) {
            return Some(invalid_text_response(
                "mcp environment key",
                "mcp environment key must be a valid environment variable name",
            ));
        }
    }
    None
}

fn reject_invalid_workflow_stage(
    field: &'static str,
    stage: &str,
) -> Option<(&'static str, String)> {
    if stage.trim().is_empty() {
        return Some(invalid_text_response(
            field,
            "workflow stage must not be empty",
        ));
    }
    if stage
        .chars()
        .any(|ch| ch.is_ascii_control() || matches!(ch, '/' | '\\'))
    {
        return Some(invalid_text_response(
            field,
            "workflow stage must not contain path separators or control characters",
        ));
    }
    None
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

fn reject_invalid_marketplace_manifest(
    manifest: &mut MarketplaceManifest,
) -> Option<(&'static str, String)> {
    if let Some(metadata) = &manifest.metadata {
        if let Err(response) = parse_registry_path_id("marketplace manifest id", &metadata.id) {
            return Some(response);
        }
        if let Some(response) =
            reject_empty_text_field("marketplace manifest version", &metadata.version)
        {
            return Some(response);
        }
        if let Some(response) = reject_empty_optional_text_field(
            "marketplace manifest publisher",
            metadata.publisher.as_deref(),
        ) {
            return Some(response);
        }
        if let Some(response) = reject_empty_optional_text_field(
            "marketplace manifest homepage",
            metadata.homepage.as_deref(),
        ) {
            return Some(response);
        }
    }

    let mut profile_ids = BTreeSet::new();
    for profile in &mut manifest.agent_profiles {
        if let Err(response) = parse_registry_path_id("marketplace agent profile id", &profile.id) {
            return Some(response);
        }
        if !profile_ids.insert(profile.id.clone()) {
            return Some(invalid_text_response(
                "marketplace agent profile id",
                &format!("duplicate marketplace agent profile id: {}", profile.id),
            ));
        }
        if let Some(response) = reject_invalid_agent_name(&profile.name) {
            return Some(response);
        }
        if let Some(response) = reject_empty_optional_text_field(
            "marketplace agent profile model",
            profile.model.as_deref(),
        ) {
            return Some(response);
        }
        if let Some(response) = reject_empty_optional_text_field(
            "marketplace agent profile system_prompt",
            profile.system_prompt.as_deref(),
        ) {
            return Some(response);
        }
        if let Some(response) = reject_invalid_capability_values(
            "marketplace agent profile capabilities",
            &profile.capabilities,
            true,
        ) {
            return Some(response);
        }
        profile.capabilities = normalize_list(profile.capabilities.clone());
    }

    let mut template_ids = BTreeSet::new();
    for template in &mut manifest.workflow_templates {
        if let Some(response) = reject_invalid_marketplace_template(template, &mut template_ids) {
            return Some(response);
        }
    }

    let mut server_ids = BTreeSet::new();
    for server in &manifest.mcp_servers {
        if let Err(response) = parse_registry_path_id("marketplace MCP server id", &server.id) {
            return Some(response);
        }
        if !server_ids.insert(server.id.clone()) {
            return Some(invalid_text_response(
                "marketplace MCP server id",
                &format!("duplicate marketplace MCP server id: {}", server.id),
            ));
        }
        if let Some(response) = reject_empty_text_field("marketplace MCP command", &server.command)
        {
            return Some(response);
        }
        for arg in &server.args {
            if let Some(response) = reject_empty_text_field("marketplace MCP argument", arg) {
                return Some(response);
            }
        }
        for key in server.env.keys() {
            if !crate::models::is_valid_env_var_name(key) {
                return Some(invalid_text_response(
                    "marketplace MCP environment key",
                    "marketplace MCP environment key must be a valid environment variable name",
                ));
            }
        }
    }

    if manifest.agent_profiles.is_empty()
        && manifest.workflow_templates.is_empty()
        && manifest.mcp_servers.is_empty()
    {
        return Some(invalid_text_response(
            "manifest",
            "marketplace manifest must include at least one registry entry",
        ));
    }
    None
}

fn reject_invalid_marketplace_template(
    template: &mut WorkflowTemplate,
    template_ids: &mut BTreeSet<String>,
) -> Option<(&'static str, String)> {
    if let Err(response) = parse_registry_path_id("marketplace workflow template id", &template.id)
    {
        return Some(response);
    }
    if !template_ids.insert(template.id.clone()) {
        return Some(invalid_text_response(
            "marketplace workflow template id",
            &format!(
                "duplicate marketplace workflow template id: {}",
                template.id
            ),
        ));
    }
    if let Some(response) =
        reject_empty_text_field("marketplace workflow template name", &template.name)
    {
        return Some(response);
    }
    if let Some(response) = reject_empty_text_field(
        "marketplace workflow template description",
        &template.description,
    ) {
        return Some(response);
    }
    if template.stages.is_empty() {
        return Some(invalid_text_response(
            "marketplace workflow template stages",
            &format!(
                "marketplace workflow template {} has no stages",
                template.id
            ),
        ));
    }
    let mut stages = BTreeSet::new();
    for stage in &template.stages {
        if let Some(response) =
            reject_invalid_workflow_stage("marketplace workflow template stage", stage)
        {
            return Some(response);
        }
        if !stages.insert(stage.clone()) {
            return Some(invalid_text_response(
                "marketplace workflow template stage",
                &format!(
                    "marketplace workflow template {} has duplicate stage {}",
                    template.id, stage
                ),
            ));
        }
    }

    let mut task_stages = BTreeSet::new();
    for task in &mut template.tasks {
        if let Some(response) =
            reject_invalid_workflow_stage("marketplace workflow template task stage", &task.stage)
        {
            return Some(response);
        }
        if !stages.contains(&task.stage) {
            return Some(invalid_text_response(
                "marketplace workflow template task stage",
                &format!(
                    "marketplace workflow template {} task references unknown stage {}",
                    template.id, task.stage
                ),
            ));
        }
        if !task_stages.insert(task.stage.clone()) {
            return Some(invalid_text_response(
                "marketplace workflow template task stage",
                &format!(
                    "marketplace workflow template {} has duplicate task metadata for stage {}",
                    template.id, task.stage
                ),
            ));
        }
        if let Some(response) = reject_empty_optional_text_field(
            "marketplace workflow template task title",
            task.title.as_deref(),
        ) {
            return Some(response);
        }
        if let Some(response) = reject_empty_optional_text_field(
            "marketplace workflow template task objective",
            task.objective.as_deref(),
        ) {
            return Some(response);
        }
        if let Some(response) = reject_empty_optional_text_field(
            "marketplace workflow template task command",
            task.command.as_deref(),
        ) {
            return Some(response);
        }
        if let Some(response) = reject_invalid_capability_values(
            "marketplace workflow template task capabilities",
            &task.capabilities,
            true,
        ) {
            return Some(response);
        }
        task.capabilities = normalize_list(task.capabilities.clone());
    }

    let mut edges = BTreeSet::new();
    for edge in &template.edges {
        if let Some(response) =
            reject_invalid_marketplace_template_edge(template, edge, &stages, &mut edges)
        {
            return Some(response);
        }
    }
    if marketplace_template_has_cycle(&template.stages, &workflow_template_edges(template)) {
        return Some(invalid_text_response(
            "marketplace workflow template edges",
            &format!(
                "marketplace workflow template {} has a dependency cycle",
                template.id
            ),
        ));
    }
    None
}

fn reject_invalid_marketplace_template_edge(
    template: &WorkflowTemplate,
    edge: &WorkflowTemplateEdge,
    stages: &BTreeSet<String>,
    edges: &mut BTreeSet<(String, String)>,
) -> Option<(&'static str, String)> {
    if let Some(response) =
        reject_invalid_workflow_stage("marketplace workflow template edge from", &edge.from)
    {
        return Some(response);
    }
    if let Some(response) =
        reject_invalid_workflow_stage("marketplace workflow template edge to", &edge.to)
    {
        return Some(response);
    }
    if edge.from == edge.to {
        return Some(invalid_text_response(
            "marketplace workflow template edge",
            &format!(
                "marketplace workflow template {} edge cannot point to itself: {}",
                template.id, edge.from
            ),
        ));
    }
    if !stages.contains(&edge.from) {
        return Some(invalid_text_response(
            "marketplace workflow template edge from",
            &format!(
                "marketplace workflow template {} edge references unknown stage {}",
                template.id, edge.from
            ),
        ));
    }
    if !stages.contains(&edge.to) {
        return Some(invalid_text_response(
            "marketplace workflow template edge to",
            &format!(
                "marketplace workflow template {} edge references unknown stage {}",
                template.id, edge.to
            ),
        ));
    }
    if !edges.insert((edge.from.clone(), edge.to.clone())) {
        return Some(invalid_text_response(
            "marketplace workflow template edge",
            &format!(
                "marketplace workflow template {} has duplicate edge {} -> {}",
                template.id, edge.from, edge.to
            ),
        ));
    }
    None
}

fn marketplace_template_has_cycle(stages: &[String], edges: &[WorkflowTemplateEdge]) -> bool {
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    stages.iter().any(|stage| {
        marketplace_template_visit_has_cycle(stage, edges, &mut visiting, &mut visited)
    })
}

fn marketplace_template_visit_has_cycle(
    stage: &str,
    edges: &[WorkflowTemplateEdge],
    visiting: &mut BTreeSet<String>,
    visited: &mut BTreeSet<String>,
) -> bool {
    if visited.contains(stage) {
        return false;
    }
    if !visiting.insert(stage.to_owned()) {
        return true;
    }
    for edge in edges.iter().filter(|edge| edge.from == stage) {
        if marketplace_template_visit_has_cycle(&edge.to, edges, visiting, visited) {
            return true;
        }
    }
    visiting.remove(stage);
    visited.insert(stage.to_owned());
    false
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
    if let Some((key, _)) = secret_args
        .iter()
        .find(|(_, reference)| reference.trim().is_empty())
    {
        return Some(invalid_text_response(
            "secret_args",
            &format!(
                "secret tool argument `{key}` must name an environment variable or backend:id reference"
            ),
        ));
    }
    if let Some((key, _)) = secret_args
        .iter()
        .find(|(_, reference)| !is_valid_secret_reference(reference))
    {
        return Some(invalid_text_response(
            "secret_args",
            &format!(
                "secret tool argument `{key}` must name a valid environment variable or backend:id reference"
            ),
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

fn workflow_stage_task_pair_response(
    os: &OperatingSystem,
    workflow_id: &WorkflowId,
    from_stage: &str,
    to_stage: &str,
) -> Result<Option<(TaskId, TaskId)>, (&'static str, String)> {
    let Some(workflow) = os.workflows.get(workflow_id) else {
        return Ok(None);
    };
    let Some(from_task) = workflow.tasks.get(from_stage).cloned() else {
        return Err((
            "404 Not Found",
            json!({
                "error": "workflow stage not found",
                "stage": from_stage,
            })
            .to_string(),
        ));
    };
    let Some(to_task) = workflow.tasks.get(to_stage).cloned() else {
        return Err((
            "404 Not Found",
            json!({
                "error": "workflow stage not found",
                "stage": to_stage,
            })
            .to_string(),
        ));
    };
    Ok(Some((from_task, to_task)))
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

fn default_main_branch() -> String {
    "main".into()
}

fn default_code_review_title() -> String {
    "Code review".into()
}

fn default_mcp_server_enabled() -> bool {
    true
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

fn default_systemd_unit_name() -> String {
    crate::service::DEFAULT_SYSTEMD_UNIT.into()
}

fn default_launchctl_path() -> PathBuf {
    PathBuf::from("launchctl")
}

fn default_systemctl_path() -> PathBuf {
    PathBuf::from("systemctl")
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
            { "name": "approvals", "description": "Human approval gate inspection and resolution." },
            { "name": "workers", "description": "Distributed worker node registration and heartbeat records." },
            { "name": "evals", "description": "Benchmark evaluation records." },
            { "name": "git", "description": "Git workspace inspection." },
            { "name": "secrets", "description": "Secret manager backend metadata." },
            { "name": "tools", "description": "Tool catalog inspection and lookup." },
            { "name": "state", "description": "State validation and repair operations." },
            { "name": "runs", "description": "Run execution, history, logs, replay, and cancellation." },
            { "name": "events", "description": "Event stream inspection." },
            { "name": "memory", "description": "Agent memory inspection." },
            { "name": "registry", "description": "Reusable agent profiles, workflow templates, and MCP server registry." },
            { "name": "schema", "description": "OpenAPI schema discovery." }
        ],
        "security": [
            {},
            { "bearerAuth": [] }
        ],
        "paths": {
            "/health": health_endpoint(),
            "/dashboard.html": dashboard_html_endpoint(),
            "/metrics": metrics_endpoint(),
            "/metrics/prometheus": prometheus_metrics_endpoint(),
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
            "/approvals": approvals_endpoint(),
            "/approvals/{id}/approve": with_id_parameter(
                approval_resolve_endpoint("Approve an approval gate"),
                "Approval ID.",
            ),
            "/approvals/{id}/deny": with_id_parameter(
                approval_resolve_endpoint("Deny an approval gate"),
                "Approval ID.",
            ),
            "/workers": workers_endpoint(),
            "/workers/{id}": with_id_parameter(
                worker_detail_endpoint(),
                "Worker ID.",
            ),
            "/workers/{id}/heartbeat": with_id_parameter(
                worker_heartbeat_endpoint(),
                "Worker ID.",
            ),
            "/workers/{id}/claim": with_id_parameter(
                worker_claim_endpoint(),
                "Worker ID. A matching agent with the same normalized ID must exist.",
            ),
            "/workers/{id}/report": with_id_parameter(
                worker_report_endpoint(),
                "Worker ID. A matching agent with the same normalized ID must own the running task.",
            ),
            "/evals": evals_endpoint(),
            "/evals/run": eval_run_endpoint(),
            "/evals/{id}": with_id_parameter(
                eval_detail_endpoint(),
                "Eval ID.",
            ),
            "/git/status": git_status_endpoint(),
            "/git/review-task": git_review_task_endpoint(),
            "/secrets": secrets_endpoint(),
            "/secrets/check": secrets_check_endpoint(),
            "/secrets/{id}": with_id_parameter(
                secrets_detail_endpoint(),
                "Secrets backend ID.",
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
            "/runs/{id}/debug": with_id_parameter(
                run_debug_endpoint(),
                "Run ID. Values are normalized before lookup.",
            ),
            "/runs/{id}/artifacts": with_id_parameter(
                run_artifacts_endpoint(),
                "Run ID. Values are normalized before lookup.",
            ),
            "/runs/{id}/artifacts/{artifact_id}": with_id_parameter(
                with_artifact_id_parameter(run_artifact_endpoint()),
                "Run ID. Values are normalized before lookup.",
            ),
            "/runs/{id}/cancel": with_id_parameter(
                run_cancel_endpoint(),
                "Run ID. Values are normalized before lookup.",
            ),
            "/events": events_endpoint(),
            "/memory": memory_endpoint(),
            "/memory/recall": memory_recall_endpoint(),
            "/memory/prune": memory_prune_endpoint(),
            "/memory/{id}": with_id_parameter(memory_detail_endpoint(), "Memory ID."),
            "/registry": registry_endpoint(),
            "/registry/profiles": registry_profiles_endpoint(),
            "/registry/profiles/{id}": with_id_parameter(
                registry_profile_detail_endpoint(),
                "Agent profile ID.",
            ),
            "/registry/profiles/{id}/agents": with_id_parameter(
                registry_profile_agents_endpoint(),
                "Agent profile ID.",
            ),
            "/registry/templates": registry_templates_endpoint(),
            "/registry/templates/{id}": with_id_parameter(
                registry_template_detail_endpoint(),
                "Workflow template ID.",
            ),
            "/registry/templates/{id}/workflows": with_id_parameter(
                registry_template_workflows_endpoint(),
                "Workflow template ID.",
            ),
            "/registry/marketplace-import": registry_marketplace_import_endpoint(),
            "/registry/mcp-servers": registry_mcp_servers_endpoint(),
            "/registry/mcp-servers/{id}": with_id_parameter(
                registry_mcp_server_detail_endpoint(),
                "MCP server ID.",
            ),
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
        paths.insert("/service/systemd".into(), service_systemd_endpoint());
        paths.insert(
            "/service/systemd/install".into(),
            service_systemd_install_endpoint(),
        );
        paths.insert(
            "/service/systemd/uninstall".into(),
            service_systemd_uninstall_endpoint(),
        );
        paths.insert(
            "/service/systemd/start".into(),
            service_systemd_start_endpoint(),
        );
        paths.insert(
            "/service/systemd/stop".into(),
            service_systemd_stop_endpoint(),
        );
        paths.insert(
            "/service/systemd/status".into(),
            service_systemd_status_endpoint(),
        );
        paths.insert(
            "/registry/profiles/{id}/agents".into(),
            with_id_parameter(registry_profile_agents_endpoint(), "Agent profile ID."),
        );
        paths.insert("/state/backup".into(), state_backup_endpoint());
        paths.insert("/state/export".into(), state_export_endpoint());
        paths.insert("/state/import".into(), state_import_endpoint());
        paths.insert("/state/migrate".into(), state_migrate_endpoint());
        paths.insert("/state/prune".into(), state_prune_endpoint());
        paths.insert("/state/sqlite".into(), state_sqlite_endpoint());
        paths.insert("/tasks/recover".into(), task_recover_endpoint());
        paths.insert(
            "/workflows/{id}/status".into(),
            with_id_parameter(
                workflow_status_endpoint(),
                "Workflow ID. Values are normalized before lookup.",
            ),
        );
        paths.insert(
            "/workflows/{id}/dag".into(),
            with_id_parameter(
                workflow_dag_endpoint(),
                "Workflow ID. Values are normalized before lookup.",
            ),
        );
        paths.insert(
            "/workflows/{id}/tasks".into(),
            with_id_parameter(
                workflow_add_task_endpoint(),
                "Workflow ID. Values are normalized before lookup.",
            ),
        );
        paths.insert(
            "/workflows/{id}/link".into(),
            with_id_parameter(
                workflow_edge_endpoint("Add a dependency edge between workflow stages"),
                "Workflow ID. Values are normalized before lookup.",
            ),
        );
        paths.insert(
            "/workflows/{id}/unlink".into(),
            with_id_parameter(
                workflow_edge_endpoint("Remove a dependency edge between workflow stages"),
                "Workflow ID. Values are normalized before lookup.",
            ),
        );
        paths.insert(
            "/workflows/{id}/pause".into(),
            with_id_parameter(
                workflow_transition_endpoint("Pause pending workflow stages"),
                "Workflow ID. Values are normalized before lookup.",
            ),
        );
        paths.insert(
            "/workflows/{id}/resume".into(),
            with_id_parameter(
                workflow_transition_endpoint("Resume blocked workflow stages"),
                "Workflow ID. Values are normalized before lookup.",
            ),
        );
        paths.insert(
            "/workflows/{id}/retry".into(),
            with_id_parameter(
                workflow_transition_endpoint("Retry failed, cancelled, or blocked workflow stages"),
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
        "registry" => "registry",
        "approvals" => "approvals",
        "workers" => "workers",
        "evals" => "evals",
        "git" => "git",
        "secrets" => "secrets",
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
            add_response_if_missing(operation, "403", "Forbidden");
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
        "X-Trace-Id": {
            "description": "Per-request trace ID generated by the local API listener for log and client correlation.",
            "schema": {
                "type": "string",
                "pattern": r"^[0-9a-fA-F-]{36}$",
                "example": "123e4567-e89b-12d3-a456-426614174000"
            }
        },
        "Access-Control-Allow-Origin": {
            "description": "CORS origin policy for browser clients. Missing origins and loopback browser origins are allowed; other origins receive 403.",
            "schema": {
                "type": "string",
                "example": "http://localhost:3000"
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
                "secret_args": secret_reference_map("Tool argument keys mapped to environment variable names or backend:id secret references."),
                "priority": priority_input_schema(),
                "required_capabilities": capability_array(),
                "dependencies": task_id_array("Task IDs that must complete before this task is ready. Empty or malformed IDs are rejected."),
                "max_attempts": positive_integer_with_default(1)
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
                "tags": string_array("Search tags. Empty entries are rejected."),
                "visibility": memory_visibility_schema(),
                "scope": nullable_persisted_non_empty_string_schema()
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
        "UnscheduledTask": {
            "type": "object",
            "required": ["task_id", "reason"],
            "properties": {
                "task_id": slug_string_schema(),
                "reason": persisted_non_empty_string_schema()
            },
            "additionalProperties": false
        },
        "RuntimeReport": runtime_report_schema(),
        "RunRecord": run_record_schema(),
        "RunOnceResponse": run_once_response_schema(),
        "RunLogsResponse": run_logs_response_schema(),
        "RunReplayResponse": run_replay_response_schema(),
        "RunDebugResponse": run_debug_response_schema(),
        "RunArtifactsResponse": run_artifacts_response_schema(),
        "RunArtifactReadResponse": run_artifact_read_response_schema(),
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
    add_registry_schemas(&mut schemas);
    add_approval_schemas(&mut schemas);
    add_worker_eval_schemas(&mut schemas);
    add_git_schemas(&mut schemas);
    add_secrets_schemas(&mut schemas);
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
    add_sqlite_state_schemas(&mut schemas);
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
            "required": ["from_version", "to_version", "changed", "steps", "downgrade_notes"],
            "properties": {
                "from_version": non_negative_integer(),
                "to_version": non_negative_integer(),
                "changed": { "type": "boolean" },
                "steps": string_list_schema(),
                "downgrade_notes": string_list_schema()
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "MigrateStateResponse".into(),
        json!({
            "type": "object",
            "required": ["dry_run", "input", "output", "output_preexisting", "migration", "validation"],
            "properties": {
                "dry_run": { "type": "boolean" },
                "input": { "type": "string" },
                "output": { "type": "string" },
                "output_preexisting": { "type": "boolean" },
                "migration": schema_ref("MigrationReport"),
                "validation": schema_ref("ValidationReport")
            },
            "additionalProperties": false
        }),
    );
}

fn add_sqlite_state_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "StateSqliteRequest".into(),
        json!({
            "type": "object",
            "allOf": [
                true_flags_conflict("init_only", "restore")
            ],
            "properties": {
                "output": non_empty_string("Optional SQLite mirror path. Defaults to the active state path with a .sqlite extension."),
                "init_only": { "type": "boolean", "default": false },
                "restore": { "type": "boolean", "default": false },
                "force": { "type": "boolean", "default": false },
                "dry_run": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "SqliteImportReport".into(),
        json!({
            "type": "object",
            "required": ["imported_snapshot", "imported_runs", "imported_run_logs", "skipped_run_logs"],
            "properties": {
                "imported_snapshot": { "type": "boolean" },
                "imported_runs": non_negative_integer(),
                "imported_run_logs": non_negative_integer(),
                "skipped_run_logs": non_negative_integer()
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "SqliteRestoreReport".into(),
        json!({
            "type": "object",
            "required": ["restored_snapshot", "restored_runs", "validation"],
            "properties": {
                "restored_snapshot": { "type": "boolean" },
                "restored_runs": non_negative_integer(),
                "validation": schema_ref("ValidationReport")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "StateSqliteResponse".into(),
        json!({
            "type": "object",
            "required": [
                "path",
                "state_path",
                "initialized",
                "imported",
                "dry_run",
                "force",
                "import",
                "restore"
            ],
            "properties": {
                "path": { "type": "string" },
                "state_path": { "type": "string" },
                "initialized": { "type": "boolean" },
                "imported": { "type": "boolean" },
                "dry_run": { "type": "boolean" },
                "force": { "type": "boolean" },
                "import": nullable_schema(schema_ref("SqliteImportReport")),
                "restore": nullable_schema(schema_ref("SqliteRestoreReport"))
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
            "required": [
                "older_than_seconds",
                "recovered",
                "recovered_runs",
                "recovered_daemon",
                "notes"
            ],
            "properties": {
                "older_than_seconds": { "type": "integer", "minimum": 0 },
                "recovered": slug_list_schema(),
                "recovered_runs": slug_list_schema(),
                "recovered_daemon": { "type": "boolean" },
                "notes": {
                    "type": "array",
                    "items": { "type": "string" }
                }
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
            "required": ["dry_run", "removed_runs", "removed_log_paths", "removed_artifact_paths", "removed_events"],
            "properties": {
                "dry_run": { "type": "boolean" },
                "removed_runs": slug_list_schema(),
                "removed_log_paths": string_list_schema(),
                "removed_artifact_paths": string_list_schema(),
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
    schemas.insert(
        "SystemdServiceRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "unit_name": non_empty_string_with_default(crate::service::DEFAULT_SYSTEMD_UNIT),
                "bin_path": non_empty_string("Optional agent-os binary path. Defaults to the current executable."),
                "interval_ms": { "type": "integer", "minimum": 1, "default": 1000 },
                "limit": positive_integer_with_default(1),
                "execute": { "type": "boolean", "default": false },
                "recover_stale_seconds": { "type": "integer", "minimum": 0 },
                "unit_path": non_empty_string("Optional systemd user unit path. Defaults from unit_name.")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "SystemdService".into(),
        json!({
            "type": "object",
            "required": [
                "unit_name",
                "program",
                "state_path",
                "interval_ms",
                "limit",
                "execute",
                "recover_stale_seconds"
            ],
            "properties": {
                "unit_name": { "type": "string" },
                "program": { "type": "string" },
                "state_path": { "type": "string" },
                "interval_ms": { "type": "integer", "minimum": 1 },
                "limit": positive_integer(),
                "execute": { "type": "boolean" },
                "recover_stale_seconds": { "type": ["integer", "null"], "minimum": 0 }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "SystemdServiceResponse".into(),
        json!({
            "type": "object",
            "required": ["platform", "service", "unit", "unit_path"],
            "properties": {
                "platform": { "type": "string", "enum": ["systemd"] },
                "service": schema_ref("SystemdService"),
                "unit": { "type": "string" },
                "unit_path": { "type": "string" }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "InstallSystemdServiceResponse".into(),
        json!({
            "type": "object",
            "required": ["platform", "installed", "unit_path", "service"],
            "properties": {
                "platform": { "type": "string", "enum": ["systemd"] },
                "installed": { "type": "boolean" },
                "unit_path": { "type": "string" },
                "service": schema_ref("SystemdService")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "UninstallSystemdServiceRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "unit_name": non_empty_string_with_default(crate::service::DEFAULT_SYSTEMD_UNIT),
                "unit_path": non_empty_string("Optional systemd user unit path. Defaults from unit_name.")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "UninstallSystemdServiceResponse".into(),
        json!({
            "type": "object",
            "required": ["platform", "removed", "unit_path"],
            "properties": {
                "platform": { "type": "string", "enum": ["systemd"] },
                "removed": { "type": "boolean" },
                "unit_path": { "type": "string" }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "SystemdServiceControlRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "unit_name": non_empty_string_with_default(crate::service::DEFAULT_SYSTEMD_UNIT),
                "systemctl_path": non_empty_string_with_default("systemctl")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "SystemctlCommandOutput".into(),
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
        "SystemctlCommandErrorResponse".into(),
        json!({
            "type": "object",
            "required": ["error", "detail", "systemctl"],
            "properties": {
                "error": { "type": "string" },
                "detail": { "type": "string" },
                "systemctl": schema_ref("SystemctlCommandOutput")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "SystemdServiceStartResponse".into(),
        json!({
            "type": "object",
            "required": ["platform", "started", "unit_name", "systemctl"],
            "properties": {
                "platform": { "type": "string", "enum": ["systemd"] },
                "started": { "type": "boolean" },
                "unit_name": { "type": "string" },
                "systemctl": schema_ref("SystemctlCommandOutput")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "SystemdServiceStopResponse".into(),
        json!({
            "type": "object",
            "required": ["platform", "stopped", "unit_name", "systemctl"],
            "properties": {
                "platform": { "type": "string", "enum": ["systemd"] },
                "stopped": { "type": "boolean" },
                "unit_name": { "type": "string" },
                "systemctl": schema_ref("SystemctlCommandOutput")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "SystemdServiceStatusResponse".into(),
        json!({
            "type": "object",
            "required": ["platform", "active", "unit_name", "systemctl"],
            "properties": {
                "platform": { "type": "string", "enum": ["systemd"] },
                "active": { "type": "boolean" },
                "unit_name": { "type": "string" },
                "systemctl": schema_ref("SystemctlCommandOutput")
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
                "force": { "type": "boolean", "default": false },
                "profile": config_profile_schema()
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
                    "max_attempts",
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
                "secret_args": secret_reference_map("Replacement tool argument keys mapped to environment variable names or backend:id secret references."),
                "clear_args": { "type": "boolean", "default": false },
                "clear_secret_args": { "type": "boolean", "default": false },
                "cwd": non_empty_string("Updated working directory override."),
                "clear_cwd": { "type": "boolean", "default": false },
                "required_capabilities": capability_array(),
                "clear_required_capabilities": { "type": "boolean", "default": false },
                "max_attempts": positive_integer()
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
        "WorkflowAddTaskRequest".into(),
        json!({
            "type": "object",
            "required": ["stage", "title"],
            "properties": {
                "stage": workflow_stage_schema("Workflow stage name."),
                "title": non_empty_string("Task title for the new stage."),
                "objective": non_empty_string("Task objective. Defaults to title."),
                "command": non_empty_string("Optional shell command for the stage task."),
                "required_capabilities": capability_array(),
                "dependencies": {
                    "type": "array",
                    "description": "Existing workflow stages this new stage depends on.",
                    "items": workflow_stage_schema("Workflow dependency stage.")
                },
                "priority": priority_input_schema()
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkflowEdgeRequest".into(),
        json!({
            "type": "object",
            "required": ["from", "to"],
            "properties": {
                "from": workflow_stage_schema("Dependency stage name."),
                "to": workflow_stage_schema("Dependent stage name.")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkflowNoteRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "note": non_empty_string("Optional transition note.")
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
        "WorkflowAddTaskResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "stage", "task_id", "task", "progress"],
            "properties": {
                "id": slug_string_schema(),
                "stage": workflow_stage_schema("Workflow stage name."),
                "task_id": slug_string_schema(),
                "task": schema_ref("Task"),
                "progress": schema_ref("WorkflowProgress")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkflowEdgeResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "from", "to", "linked", "progress"],
            "properties": {
                "id": slug_string_schema(),
                "from": workflow_stage_schema("Dependency stage name."),
                "to": workflow_stage_schema("Dependent stage name."),
                "linked": { "type": "boolean" },
                "progress": schema_ref("WorkflowProgress")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkflowDagNode".into(),
        json!({
            "type": "object",
            "required": ["stage", "task_id", "title", "status", "priority", "assigned_to", "missing"],
            "properties": {
                "stage": workflow_stage_schema("Workflow stage name."),
                "task_id": slug_string_schema(),
                "title": nullable_persisted_non_empty_string_schema(),
                "status": nullable_schema(task_status_schema()),
                "priority": nullable_schema(priority_schema()),
                "assigned_to": nullable_slug_string_schema(),
                "missing": { "type": "boolean" }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkflowDagEdge".into(),
        json!({
            "type": "object",
            "required": ["from", "to", "from_task_id", "to_task_id"],
            "properties": {
                "from": workflow_stage_schema("Dependency stage name."),
                "to": workflow_stage_schema("Dependent stage name."),
                "from_task_id": slug_string_schema(),
                "to_task_id": slug_string_schema()
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkflowExternalDependency".into(),
        json!({
            "type": "object",
            "required": ["stage", "task_id", "dependency_task_id"],
            "properties": {
                "stage": workflow_stage_schema("Workflow stage name."),
                "task_id": slug_string_schema(),
                "dependency_task_id": slug_string_schema()
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkflowDag".into(),
        json!({
            "type": "object",
            "required": ["id", "objective", "priority", "nodes", "edges", "external_dependencies", "progress"],
            "properties": {
                "id": slug_string_schema(),
                "objective": persisted_non_empty_string_schema(),
                "priority": priority_schema(),
                "nodes": {
                    "type": "array",
                    "items": schema_ref("WorkflowDagNode")
                },
                "edges": {
                    "type": "array",
                    "items": schema_ref("WorkflowDagEdge")
                },
                "external_dependencies": {
                    "type": "array",
                    "items": schema_ref("WorkflowExternalDependency")
                },
                "progress": nullable_schema(schema_ref("WorkflowProgress"))
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkflowTransitionResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "action", "affected_tasks", "progress"],
            "properties": {
                "id": slug_string_schema(),
                "action": {
                    "type": "string",
                    "enum": ["paused", "resumed", "retried"]
                },
                "affected_tasks": slug_list_schema(),
                "progress": schema_ref("WorkflowProgress")
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

fn add_registry_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "AgentProfile".into(),
        json!({
            "type": "object",
            "required": ["id", "name", "kind", "model", "capabilities", "system_prompt"],
            "properties": {
                "id": persisted_non_empty_string_schema(),
                "name": persisted_non_empty_string_schema(),
                "kind": non_empty_string("Agent kind."),
                "model": nullable_persisted_non_empty_string_schema(),
                "capabilities": normalized_list_schema(),
                "system_prompt": nullable_persisted_non_empty_string_schema()
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkflowTemplate".into(),
        json!({
            "type": "object",
            "required": ["id", "name", "description", "stages", "tasks", "edges"],
            "properties": {
                "id": persisted_non_empty_string_schema(),
                "name": persisted_non_empty_string_schema(),
                "description": persisted_non_empty_string_schema(),
                "stages": {
                    "type": "array",
                    "minItems": 1,
                    "items": workflow_stage_schema("Workflow template stage.")
                },
                "tasks": {
                    "type": "array",
                    "items": schema_ref("WorkflowTemplateTask")
                },
                "edges": {
                    "type": "array",
                    "items": schema_ref("WorkflowTemplateEdge")
                }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "InstallAgentProfileRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "name": id_source_string("Optional agent display name override."),
                "model": nullable_non_empty_string(),
                "parallel": positive_integer_with_default(1)
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkflowTemplateTask".into(),
        json!({
            "type": "object",
            "required": ["stage", "title", "objective", "command", "capabilities"],
            "properties": {
                "stage": workflow_stage_schema("Workflow template stage."),
                "title": nullable_persisted_non_empty_string_schema(),
                "objective": nullable_persisted_non_empty_string_schema(),
                "command": nullable_persisted_non_empty_string_schema(),
                "capabilities": normalized_list_schema()
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkflowTemplateEdge".into(),
        json!({
            "type": "object",
            "required": ["from", "to"],
            "properties": {
                "from": workflow_stage_schema("Source workflow template stage."),
                "to": workflow_stage_schema("Destination workflow template stage.")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "McpServer".into(),
        json!({
            "type": "object",
            "required": ["id", "command", "args", "env", "enabled"],
            "properties": {
                "id": persisted_non_empty_string_schema(),
                "command": persisted_non_empty_string_schema(),
                "args": {
                    "type": "array",
                    "items": persisted_non_empty_string_schema()
                },
                "env": {
                    "type": "object",
                    "description": "Environment variables passed to the MCP server process.",
                    "propertyNames": { "minLength": 1, "pattern": r"^[A-Za-z_][A-Za-z0-9_]*$" },
                    "additionalProperties": { "type": "string" }
                },
                "enabled": { "type": "boolean" }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "RegisterMcpServerRequest".into(),
        json!({
            "type": "object",
            "required": ["id", "command"],
            "properties": {
                "id": id_source_string("MCP server ID."),
                "command": non_empty_string("MCP server stdio command."),
                "args": {
                    "type": "array",
                    "items": persisted_non_empty_string_schema()
                },
                "env": {
                    "type": "object",
                    "description": "Environment variables passed to the MCP server process.",
                    "propertyNames": { "minLength": 1, "pattern": r"^[A-Za-z_][A-Za-z0-9_]*$" },
                    "additionalProperties": { "type": "string" }
                },
                "enabled": { "type": "boolean", "default": true }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "UpdateMcpServerRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "command": non_empty_string("MCP server stdio command."),
                "args": {
                    "type": "array",
                    "items": persisted_non_empty_string_schema()
                },
                "env": {
                    "type": "object",
                    "description": "Replacement MCP server environment map.",
                    "propertyNames": { "minLength": 1, "pattern": r"^[A-Za-z_][A-Za-z0-9_]*$" },
                    "additionalProperties": { "type": "string" }
                },
                "enabled": { "type": "boolean" }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "McpServerMutationResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "mcp_server"],
            "properties": {
                "id": persisted_non_empty_string_schema(),
                "mcp_server": schema_ref("McpServer")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "McpServerDeleteResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "removed", "mcp_server"],
            "properties": {
                "id": persisted_non_empty_string_schema(),
                "removed": { "type": "boolean" },
                "mcp_server": schema_ref("McpServer")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "RegistryResponse".into(),
        json!({
            "type": "object",
            "required": ["agent_profiles", "workflow_templates", "tools", "mcp_servers", "secrets_backends"],
            "properties": {
                "agent_profiles": string_keyed_map_schema(schema_ref("AgentProfile")),
                "workflow_templates": string_keyed_map_schema(schema_ref("WorkflowTemplate")),
                "tools": string_keyed_map_schema(schema_ref("ToolDefinition")),
                "mcp_servers": string_keyed_map_schema(schema_ref("McpServer")),
                "secrets_backends": string_keyed_map_schema(schema_ref("SecretsBackend"))
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "TemplateWorkflowRequest".into(),
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
        "TemplateWorkflowResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "template", "workflow", "tasks", "runs", "errors", "dag"],
            "properties": {
                "id": slug_string_schema(),
                "template": persisted_non_empty_string_schema(),
                "workflow": schema_ref("Workflow"),
                "tasks": stage_task_map_schema(),
                "runs": {
                    "type": "array",
                    "items": schema_ref("RunRecord")
                },
                "errors": {
                    "type": "array",
                    "items": { "type": "string" }
                },
                "dag": schema_ref("WorkflowDag")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "MarketplaceManifestMetadata".into(),
        json!({
            "type": "object",
            "required": ["id", "version", "publisher", "homepage"],
            "properties": {
                "id": persisted_non_empty_string_schema(),
                "version": persisted_non_empty_string_schema(),
                "publisher": nullable_persisted_non_empty_string_schema(),
                "homepage": nullable_persisted_non_empty_string_schema()
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "MarketplaceManifest".into(),
        json!({
            "type": "object",
            "required": ["metadata", "agent_profiles", "workflow_templates", "mcp_servers"],
            "properties": {
                "metadata": {
                    "oneOf": [
                        { "type": "null" },
                        schema_ref("MarketplaceManifestMetadata")
                    ]
                },
                "agent_profiles": {
                    "type": "array",
                    "items": schema_ref("AgentProfile")
                },
                "workflow_templates": {
                    "type": "array",
                    "items": schema_ref("WorkflowTemplate")
                },
                "mcp_servers": {
                    "type": "array",
                    "items": schema_ref("McpServer")
                }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "MarketplaceImportRequest".into(),
        json!({
            "type": "object",
            "required": ["manifest"],
            "properties": {
                "manifest": schema_ref("MarketplaceManifest"),
                "source": persisted_non_empty_string_schema(),
                "expect_checksum": persisted_non_empty_string_schema(),
                "force": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "MarketplaceImportResponse".into(),
        json!({
            "type": "object",
            "required": [
                "source",
                "checksum",
                "verified_checksum",
                "manifest_id",
                "manifest_version",
                "imported_agent_profiles",
                "imported_workflow_templates",
                "imported_mcp_servers",
                "overwritten"
            ],
            "properties": {
                "source": persisted_non_empty_string_schema(),
                "checksum": persisted_non_empty_string_schema(),
                "verified_checksum": { "type": "boolean" },
                "manifest_id": nullable_persisted_non_empty_string_schema(),
                "manifest_version": nullable_persisted_non_empty_string_schema(),
                "imported_agent_profiles": { "type": "integer", "minimum": 0 },
                "imported_workflow_templates": { "type": "integer", "minimum": 0 },
                "imported_mcp_servers": { "type": "integer", "minimum": 0 },
                "overwritten": { "type": "integer", "minimum": 0 }
            },
            "additionalProperties": false
        }),
    );
}

fn add_approval_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert("ApprovalRequest".into(), approval_request_schema());
    schemas.insert(
        "ApprovalResolveRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "by": non_empty_string("Operator resolving the approval.")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "ApprovalResolveResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "approval"],
            "properties": {
                "id": slug_string_schema(),
                "approval": schema_ref("ApprovalRequest")
            },
            "additionalProperties": false
        }),
    );
}

fn add_worker_eval_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "WorkerRegisterRequest".into(),
        json!({
            "type": "object",
            "required": ["id", "endpoint"],
            "properties": {
                "id": id_source_string("Worker ID."),
                "endpoint": non_empty_string("Worker endpoint."),
                "status": agent_status_input_schema()
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkerHeartbeatRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "endpoint": non_empty_string("Updated worker endpoint."),
                "status": agent_status_input_schema(),
                "lease_seconds": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Optional matching agent lease refresh in seconds."
                }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkerReportArtifactRequest".into(),
        json!({
            "type": "object",
            "required": ["kind", "path"],
            "properties": {
                "kind": {
                    "type": "string",
                    "enum": ["stdout", "stderr", "summary", "diff", "file"]
                },
                "path": non_empty_string("Artifact path or URI."),
                "bytes": { "type": "integer", "minimum": 0 },
                "content_type": non_empty_string("Artifact content type.")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkerReportRequest".into(),
        json!({
            "type": "object",
            "required": ["task_id"],
            "properties": {
                "task_id": slug_string_schema(),
                "status": {
                    "type": "string",
                    "enum": ["complete", "completed", "failed"],
                    "default": "complete"
                },
                "note": non_empty_string("Task output or failure note."),
                "command": non_empty_string("Command or provider invocation executed by the worker."),
                "cwd": non_empty_string("Worker execution directory."),
                "exit_code": { "type": "integer" },
                "artifacts": {
                    "type": "array",
                    "items": schema_ref("WorkerReportArtifactRequest")
                }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkerMutationResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "worker"],
            "properties": {
                "id": persisted_non_empty_string_schema(),
                "worker": schema_ref("WorkerNode")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkerClaimResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "worker", "claimed", "assignment", "task"],
            "properties": {
                "id": persisted_non_empty_string_schema(),
                "worker": schema_ref("WorkerNode"),
                "claimed": { "type": "boolean" },
                "assignment": nullable_schema(schema_ref("Assignment")),
                "task": nullable_schema(schema_ref("Task"))
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkerReportResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "worker", "task", "run", "reported"],
            "properties": {
                "id": persisted_non_empty_string_schema(),
                "worker": schema_ref("WorkerNode"),
                "task": schema_ref("Task"),
                "run": schema_ref("RunRecord"),
                "reported": { "type": "boolean" }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkerDeleteResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "removed", "worker"],
            "properties": {
                "id": persisted_non_empty_string_schema(),
                "removed": { "type": "boolean" },
                "worker": schema_ref("WorkerNode")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WorkerNode".into(),
        json!({
            "type": "object",
            "required": ["id", "endpoint", "status", "last_seen_at"],
            "properties": {
                "id": persisted_non_empty_string_schema(),
                "endpoint": persisted_non_empty_string_schema(),
                "status": agent_status_input_schema(),
                "last_seen_at": date_time_schema()
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "EvalRecordRequest".into(),
        json!({
            "type": "object",
            "required": ["target", "success"],
            "properties": {
                "target": non_empty_string("Eval target."),
                "success": { "type": "boolean" },
                "cost_micros": { "type": "integer", "minimum": 0 },
                "latency_ms": { "type": "integer", "minimum": 0 }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "EvalRunRequest".into(),
        json!({
            "type": "object",
            "required": ["target", "command"],
            "properties": {
                "target": non_empty_string("Eval target."),
                "command": non_empty_string("Shell command to evaluate."),
                "cwd": non_empty_string("Working directory for the eval command."),
                "success_pattern": non_empty_string("Text that stdout or stderr must contain for success."),
                "output_schema": json_schema_object_schema("JSON schema used to validate stdout as JSON.")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "EvalMutationResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "eval"],
            "properties": {
                "id": slug_string_schema(),
                "eval": schema_ref("EvalRecord")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "EvalRunResponse".into(),
        json!({
            "type": "object",
            "required": [
                "id",
                "eval",
                "command",
                "cwd",
                "status",
                "stdout",
                "stderr",
                "success_pattern_matched",
                "output_schema_valid",
                "output_schema_error",
                "timed_out"
            ],
            "properties": {
                "id": slug_string_schema(),
                "eval": schema_ref("EvalRecord"),
                "command": persisted_non_empty_string_schema(),
                "cwd": persisted_non_empty_string_schema(),
                "status": { "type": ["integer", "null"] },
                "stdout": { "type": "string" },
                "stderr": { "type": "string" },
                "success_pattern_matched": { "type": "boolean" },
                "output_schema_valid": { "type": "boolean" },
                "output_schema_error": nullable_persisted_non_empty_string_schema(),
                "timed_out": { "type": "boolean" }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "EvalRunDetails".into(),
        json!({
            "type": "object",
            "required": [
                "command",
                "cwd",
                "status",
                "stdout",
                "stderr",
                "success_pattern",
                "success_pattern_matched",
                "output_schema",
                "output_schema_valid",
                "output_schema_error",
                "timed_out"
            ],
            "properties": {
                "command": persisted_non_empty_string_schema(),
                "cwd": persisted_non_empty_string_schema(),
                "status": { "type": ["integer", "null"] },
                "stdout": { "type": "string" },
                "stderr": { "type": "string" },
                "success_pattern": nullable_persisted_non_empty_string_schema(),
                "success_pattern_matched": { "type": "boolean" },
                "output_schema": nullable_schema(json_schema_object_schema("JSON schema used to validate stdout as JSON.")),
                "output_schema_valid": { "type": "boolean" },
                "output_schema_error": nullable_persisted_non_empty_string_schema(),
                "timed_out": { "type": "boolean" }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "EvalRecord".into(),
        json!({
            "type": "object",
            "required": ["id", "target", "success", "cost_micros", "latency_ms", "run", "recorded_at"],
            "properties": {
                "id": slug_string_schema(),
                "target": persisted_non_empty_string_schema(),
                "success": { "type": "boolean" },
                "cost_micros": { "type": ["integer", "null"], "minimum": 0 },
                "latency_ms": { "type": ["integer", "null"], "minimum": 0 },
                "run": nullable_schema(schema_ref("EvalRunDetails")),
                "recorded_at": date_time_schema()
            },
            "additionalProperties": false
        }),
    );
}

fn add_git_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "GitReviewTaskRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "cwd": non_empty_string("Git workspace directory. Defaults to the API process current directory."),
                "base": non_empty_string_with_default("main"),
                "title": non_empty_string_with_default("Code review"),
                "priority": priority_input_schema()
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "GitCommandResponse".into(),
        json!({
            "type": "object",
            "required": ["command", "cwd", "status", "stdout", "stderr", "dry_run"],
            "properties": {
                "command": {
                    "type": "array",
                    "items": persisted_non_empty_string_schema()
                },
                "cwd": persisted_non_empty_string_schema(),
                "status": { "type": ["integer", "null"] },
                "stdout": { "type": "string" },
                "stderr": { "type": "string" },
                "dry_run": { "type": "boolean" }
            },
            "additionalProperties": false
        }),
    );
}

fn add_secrets_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    schemas.insert(
        "SecretsRegisterRequest".into(),
        json!({
            "type": "object",
            "required": ["id", "kind"],
            "properties": {
                "id": id_source_string("Secrets backend ID."),
                "kind": secrets_backend_kind_input_schema(),
                "reference": non_empty_string("Backend-specific reference metadata.")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "SecretsMutationResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "secrets_backend"],
            "properties": {
                "id": persisted_non_empty_string_schema(),
                "secrets_backend": schema_ref("SecretsBackend")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "SecretsDeleteResponse".into(),
        json!({
            "type": "object",
            "required": ["id", "removed", "secrets_backend"],
            "properties": {
                "id": persisted_non_empty_string_schema(),
                "removed": { "type": "boolean" },
                "secrets_backend": schema_ref("SecretsBackend")
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "SecretCheckReference".into(),
        json!({
            "type": "object",
            "required": ["task_id", "task_title", "tool_id", "arg", "env", "valid_env", "present"],
            "properties": {
                "task_id": slug_string_schema(),
                "task_title": persisted_non_empty_string_schema(),
                "tool_id": slug_string_schema(),
                "arg": persisted_non_empty_string_schema(),
                "env": { "type": "string" },
                "valid_env": { "type": "boolean" },
                "present": { "type": "boolean" }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "SecretCheckReport".into(),
        json!({
            "type": "object",
            "required": ["total", "present", "missing", "invalid", "references"],
            "properties": {
                "total": { "type": "integer", "minimum": 0 },
                "present": { "type": "integer", "minimum": 0 },
                "missing": { "type": "integer", "minimum": 0 },
                "invalid": { "type": "integer", "minimum": 0 },
                "references": {
                    "type": "array",
                    "items": schema_ref("SecretCheckReference")
                }
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
        "PruneMemoryRequest".into(),
        json!({
            "type": "object",
            "properties": {
                "max_age_days": { "type": "integer", "minimum": 1 },
                "dry_run": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "PruneMemoryResponse".into(),
        json!({
            "type": "object",
            "required": ["dry_run", "max_age_days", "removed", "expired"],
            "properties": {
                "dry_run": { "type": "boolean" },
                "max_age_days": { "type": "integer", "minimum": 1 },
                "removed": {
                    "type": "array",
                    "items": schema_ref("MemoryRecord")
                },
                "expired": {
                    "type": "array",
                    "items": schema_ref("MemoryRecord")
                }
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "UpdateMemoryRequest".into(),
        json!({
            "type": "object",
            "anyOf": update_requires_any_of(
                &["topic", "body", "tags", "visibility", "scope"],
                &["clear_tags", "clear_scope"],
            ),
            "allOf": [
                true_flag_conflicts_with_field("clear_tags", "tags"),
                true_flag_conflicts_with_field("clear_scope", "scope")
            ],
            "properties": {
                "topic": non_empty_string("Updated memory topic."),
                "body": non_empty_string("Updated memory body."),
                "tags": string_array("Replacement search tags."),
                "clear_tags": { "type": "boolean", "default": false },
                "visibility": memory_visibility_schema(),
                "scope": non_empty_string("Updated memory scope."),
                "clear_scope": { "type": "boolean", "default": false }
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
    schemas.insert("MemoryPolicy".into(), memory_policy_schema());
    schemas.insert("MemoryRecord".into(), memory_record_schema());
    schemas.insert("MemoryRecallHit".into(), memory_recall_hit_schema());
    schemas.insert("SecretsBackend".into(), secrets_backend_schema());
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

fn string_keyed_map_schema(value_schema: serde_json::Value) -> serde_json::Value {
    json!({
        "type": "object",
        "propertyNames": persisted_non_empty_string_schema(),
        "additionalProperties": value_schema
    })
}

fn json_schema_object_schema(description: &str) -> serde_json::Value {
    json!({
        "type": "object",
        "description": description,
        "additionalProperties": true
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

fn workflow_stage_schema(description: &str) -> serde_json::Value {
    json!({
        "type": "string",
        "description": description,
        "minLength": 1,
        "pattern": r"^[^\u0000-\u001F/\\]*\S[^\u0000-\u001F/\\]*$"
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
            "memory_policy",
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
            "memory_policy": schema_ref("MemoryPolicy"),
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
            "attempts",
            "max_attempts",
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
            "attempts": { "type": "integer", "minimum": 0 },
            "max_attempts": { "type": "integer", "minimum": 1 },
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
            "secret_env_args": secret_reference_map("Tool argument keys mapped to environment variable names or backend:id secret references.")
        },
        "additionalProperties": false
    })
}

fn memory_record_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["id", "topic", "body", "tags", "visibility", "scope", "created_at", "updated_at"],
        "properties": {
            "id": slug_string_schema(),
            "topic": persisted_non_empty_string_schema(),
            "body": persisted_non_empty_string_schema(),
            "tags": normalized_list_schema(),
            "visibility": memory_visibility_schema(),
            "scope": nullable_persisted_non_empty_string_schema(),
            "created_at": date_time_schema(),
            "updated_at": date_time_schema()
        },
        "additionalProperties": false
    })
}

fn memory_recall_hit_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["record", "score", "snippet"],
        "properties": {
            "record": schema_ref("MemoryRecord"),
            "score": non_negative_integer(),
            "snippet": persisted_non_empty_string_schema()
        },
        "additionalProperties": false
    })
}

fn memory_visibility_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "enum": ["shared", "private"],
        "default": "shared"
    })
}

fn memory_policy_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["scope", "semantic_recall", "max_provider_memories", "max_age_days"],
        "properties": {
            "scope": nullable_persisted_non_empty_string_schema(),
            "semantic_recall": { "type": "boolean", "default": false },
            "max_provider_memories": { "type": "integer", "minimum": 1, "default": 5 },
            "max_age_days": { "type": ["integer", "null"], "minimum": 1 }
        },
        "additionalProperties": false
    })
}

fn secrets_backend_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["id", "kind", "reference"],
        "properties": {
            "id": persisted_non_empty_string_schema(),
            "kind": secrets_backend_kind_schema(),
            "reference": nullable_persisted_non_empty_string_schema()
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
            "platform",
            "service_manager",
            "service_recommendation",
            "shell_execution_supported",
            "shell_execution_note",
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
            "config_error",
            "next_steps"
        ],
        "properties": {
            "state_path": { "type": "string" },
            "agent_os_version": { "type": "string" },
            "platform": { "type": "string" },
            "service_manager": { "type": "string", "enum": ["launchd", "systemd", "manual"] },
            "service_recommendation": persisted_non_empty_string_schema(),
            "shell_execution_supported": { "type": "boolean" },
            "shell_execution_note": persisted_non_empty_string_schema(),
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
            "config_error": { "type": ["string", "null"] },
            "next_steps": {
                "type": "array",
                "items": { "type": "string" }
            }
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
        "required": ["name", "policy", "provider", "memory_policy", "agents", "tools"],
        "properties": {
            "name": non_empty_string("OS name."),
            "policy": schema_ref("Policy"),
            "provider": schema_ref("ProviderSettings"),
            "memory_policy": schema_ref("MemoryPolicy"),
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
            "redacted_env_patterns",
            "sandbox",
            "network",
            "approval",
            "autonomy",
            "rules"
        ],
        "properties": {
            "allow_shell": { "type": "boolean", "default": false },
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
            "redacted_env_patterns": non_empty_string_list_schema(),
            "sandbox": {
                "type": "object",
                "required": ["process_isolation", "jailed_workspaces", "writable_paths"],
                "properties": {
                    "process_isolation": { "type": "boolean", "default": true },
                    "jailed_workspaces": { "type": "boolean", "default": true },
                    "writable_paths": non_empty_string_list_schema()
                },
                "additionalProperties": false
            },
            "network": {
                "type": "object",
                "required": ["mode", "allowed_hosts"],
                "properties": {
                    "mode": {
                        "type": "string",
                        "default": "providers-only",
                        "enum": ["disabled", "providers-only", "allowed"]
                    },
                    "allowed_hosts": non_empty_string_list_schema()
                },
                "additionalProperties": false
            },
            "approval": {
                "type": "object",
                "required": ["require_for_risky_actions", "risky_patterns"],
                "properties": {
                    "require_for_risky_actions": { "type": "boolean", "default": true },
                    "risky_patterns": non_empty_string_list_schema()
                },
                "additionalProperties": false
            },
            "autonomy": {
                "type": "string",
                "default": "execute-with-approval",
                "enum": ["observe-only", "suggest", "execute-with-approval", "execute-freely"]
            },
            "rules": non_empty_string_list_schema()
        },
        "additionalProperties": false
    })
}

fn provider_settings_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "kind",
            "model",
            "endpoint",
            "api_key_env",
            "request_timeout_seconds",
            "max_retries",
            "retry_backoff_ms",
            "adapter",
            "request_options",
            "response_schema",
            "plugin_command",
            "plugin_args",
            "plugin_env"
        ],
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
            },
            "max_retries": {
                "type": "integer",
                "minimum": 0,
                "maximum": MAX_PROVIDER_RETRIES,
                "default": 2
            },
            "retry_backoff_ms": {
                "type": "integer",
                "minimum": 1,
                "default": 250
            },
            "adapter": {
                "type": ["string", "null"],
                "pattern": r".*\S.*"
            },
            "request_options": {
                "type": "object",
                "description": "Additional provider request JSON fields such as temperature, top_p, or max_tokens. model and messages are controlled by Agent OS and cannot be overridden.",
                "propertyNames": {
                    "minLength": 1,
                    "not": {
                        "enum": ["model", "messages"]
                    }
                },
                "additionalProperties": true
            },
            "response_schema": {
                "type": ["object", "null"],
                "description": "Optional lightweight response schema. Agent OS currently enforces required keys before parsing provider JSON content."
            },
            "plugin_command": {
                "type": ["string", "null"],
                "description": "Executable command for provider kind `plugin`. Agent OS sends ProviderRequest JSON on stdin and expects AgentResponse JSON or text on stdout.",
                "pattern": r".*\S.*"
            },
            "plugin_args": {
                "type": "array",
                "items": persisted_non_empty_string_schema()
            },
            "plugin_env": {
                "type": "object",
                "description": "Environment variables passed to provider plugin commands.",
                "propertyNames": { "minLength": 1, "pattern": r"^[A-Za-z_][A-Za-z0-9_]*$" },
                "additionalProperties": { "type": "string" }
            },
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
                "force": { "type": "boolean", "default": false },
                "profile": config_profile_schema()
            },
            "additionalProperties": false
        }),
    );
    schemas.insert(
        "WriteConfigResponse".into(),
        json!({
            "type": "object",
            "required": ["path", "written", "profile", "config"],
            "properties": {
                "path": { "type": "string" },
                "written": { "type": "boolean" },
                "profile": config_profile_schema(),
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

fn config_profile_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "default": ConfigProfile::Safe.as_str(),
        "enum": ConfigProfile::VALUES
    })
}

fn metrics_response_schema() -> serde_json::Value {
    let required = vec![
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
        "oldest_active_run_age_ms",
        "oldest_queued_task_age_ms",
        "task_queue_age_ms_count",
        "task_queue_age_ms_sum",
        "task_queue_age_ms_buckets",
        "run_duration_ms_count",
        "run_duration_ms_sum",
        "run_duration_ms_buckets",
        "tools_total",
        "events_total",
        "memories_total",
        "daemon_status",
        "daemon_ticks",
    ];
    let mut properties = serde_json::Map::new();
    for key in [
        "state_issue_count",
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
        "oldest_active_run_age_ms",
        "oldest_queued_task_age_ms",
        "task_queue_age_ms_count",
        "task_queue_age_ms_sum",
        "run_duration_ms_count",
        "run_duration_ms_sum",
        "tools_total",
        "events_total",
        "memories_total",
    ] {
        properties.insert(key.into(), non_negative_integer());
    }
    properties.insert("ok".into(), json!({ "type": "boolean" }));
    properties.insert("service".into(), json!({ "type": "string" }));
    properties.insert("agent_os_version".into(), json!({ "type": "string" }));
    properties.insert("name".into(), json!({ "type": ["string", "null"] }));
    properties.insert(
        "version".into(),
        json!({ "type": ["integer", "null"], "minimum": 0 }),
    );
    properties.insert("state_loads".into(), json!({ "type": "boolean" }));
    properties.insert("state_valid".into(), json!({ "type": ["boolean", "null"] }));
    properties.insert("state_error".into(), json!({ "type": ["string", "null"] }));
    properties.insert("run_duration_ms_buckets".into(), duration_buckets_schema());
    properties.insert(
        "task_queue_age_ms_buckets".into(),
        duration_buckets_schema(),
    );
    properties.insert(
        "daemon_status".into(),
        json!({ "type": ["string", "null"] }),
    );
    properties.insert(
        "daemon_ticks".into(),
        json!({ "type": ["integer", "null"], "minimum": 0 }),
    );
    json!({
        "type": "object",
        "required": required.to_vec(),
        "properties": properties,
        "additionalProperties": false
    })
}

fn duration_buckets_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["le_1000", "le_5000", "le_30000", "le_60000", "le_300000", "le_inf"],
        "properties": {
            "le_1000": { "type": "integer", "minimum": 0 },
            "le_5000": { "type": "integer", "minimum": 0 },
            "le_30000": { "type": "integer", "minimum": 0 },
            "le_60000": { "type": "integer", "minimum": 0 },
            "le_300000": { "type": "integer", "minimum": 0 },
            "le_inf": { "type": "integer", "minimum": 0 }
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
        "required": [
            "assignments",
            "completed_tasks",
            "recovered_tasks",
            "recovered_runs",
            "recovered_daemon",
            "expired_agents",
            "deadlocked_tasks",
            "unscheduled_tasks",
            "notes"
        ],
        "properties": {
            "assignments": {
                "type": "array",
                "items": schema_ref("Assignment")
            },
            "completed_tasks": slug_list_schema(),
            "recovered_tasks": slug_list_schema(),
            "recovered_runs": slug_list_schema(),
            "recovered_daemon": { "type": "boolean" },
            "expired_agents": slug_list_schema(),
            "deadlocked_tasks": slug_list_schema(),
            "unscheduled_tasks": {
                "type": "array",
                "items": schema_ref("UnscheduledTask")
            },
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
            "trace_id",
            "task_id",
            "agent_id",
            "command",
            "cwd",
            "status",
            "exit_code",
            "log_path",
            "artifacts",
            "started_at",
            "finished_at"
        ],
        "properties": {
            "id": slug_string_schema(),
            "trace_id": slug_string_schema(),
            "task_id": slug_string_schema(),
            "agent_id": nullable_slug_string_schema(),
            "command": persisted_non_empty_string_schema(),
            "cwd": persisted_non_empty_string_schema(),
            "status": run_status_schema(),
            "exit_code": { "type": ["integer", "null"] },
            "log_path": nullable_persisted_non_empty_string_schema(),
            "artifacts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["kind", "path", "bytes", "content_type"],
                    "properties": {
                        "kind": {
                            "type": "string",
                            "enum": ["stdout", "stderr", "summary", "diff", "file"]
                        },
                        "path": persisted_non_empty_string_schema(),
                        "bytes": { "type": ["integer", "null"], "minimum": 0 },
                        "content_type": nullable_persisted_non_empty_string_schema()
                    },
                    "additionalProperties": false
                }
            },
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

fn run_debug_response_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "run",
            "task",
            "agent",
            "workflows",
            "approvals",
            "events",
            "log",
            "log_tail_bytes",
            "log_truncated",
            "log_error",
            "artifact_status",
            "diagnostics"
        ],
        "properties": {
            "run": schema_ref("RunRecord"),
            "task": nullable_schema(schema_ref("Task")),
            "agent": nullable_schema(schema_ref("Agent")),
            "workflows": {
                "type": "array",
                "items": schema_ref("Workflow")
            },
            "approvals": {
                "type": "array",
                "items": approval_request_schema()
            },
            "events": {
                "type": "array",
                "items": schema_ref("Event")
            },
            "log": { "type": ["string", "null"] },
            "log_tail_bytes": { "type": ["integer", "null"], "minimum": 1 },
            "log_truncated": { "type": "boolean" },
            "log_error": { "type": ["string", "null"] },
            "artifact_status": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["artifact", "exists"],
                    "properties": {
                        "artifact": run_artifact_schema(),
                        "exists": { "type": "boolean" }
                    },
                    "additionalProperties": false
                }
            },
            "diagnostics": {
                "type": "object",
                "required": [
                    "log_path",
                    "related_events",
                    "related_workflows",
                    "related_approvals",
                    "artifacts"
                ],
                "properties": {
                    "log_path": { "type": ["string", "null"], "minLength": 1 },
                    "related_events": { "type": "integer", "minimum": 0 },
                    "related_workflows": { "type": "integer", "minimum": 0 },
                    "related_approvals": { "type": "integer", "minimum": 0 },
                    "artifacts": { "type": "integer", "minimum": 0 }
                },
                "additionalProperties": false
            }
        },
        "additionalProperties": false
    })
}

fn run_artifacts_response_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["run_id", "artifacts"],
        "properties": {
            "run_id": slug_string_schema(),
            "artifacts": {
                "type": "array",
                "items": run_artifact_entry_schema()
            }
        },
        "additionalProperties": false
    })
}

fn run_artifact_entry_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "id",
            "index",
            "name",
            "artifact",
            "exists",
            "readable",
            "bytes",
            "checksum",
            "error"
        ],
        "properties": {
            "id": persisted_non_empty_string_schema(),
            "index": { "type": "integer", "minimum": 0 },
            "name": {
                "type": "string",
                "enum": ["stdout", "stderr", "summary", "diff", "file"]
            },
            "artifact": run_artifact_schema(),
            "exists": { "type": "boolean" },
            "readable": { "type": "boolean" },
            "bytes": { "type": ["integer", "null"], "minimum": 0 },
            "checksum": nullable_persisted_non_empty_string_schema(),
            "error": { "type": ["string", "null"], "minLength": 1 }
        },
        "additionalProperties": false
    })
}

fn run_artifact_read_response_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "run_id",
            "artifact_id",
            "index",
            "artifact",
            "exists",
            "bytes",
            "checksum",
            "content_type",
            "tail_bytes",
            "truncated",
            "body",
            "error"
        ],
        "properties": {
            "run_id": slug_string_schema(),
            "artifact_id": persisted_non_empty_string_schema(),
            "index": { "type": "integer", "minimum": 0 },
            "artifact": run_artifact_schema(),
            "exists": { "type": "boolean" },
            "bytes": { "type": ["integer", "null"], "minimum": 0 },
            "checksum": nullable_persisted_non_empty_string_schema(),
            "content_type": nullable_persisted_non_empty_string_schema(),
            "tail_bytes": { "type": ["integer", "null"], "minimum": 1 },
            "truncated": { "type": "boolean" },
            "body": { "type": ["string", "null"] },
            "error": { "type": ["string", "null"], "minLength": 1 }
        },
        "additionalProperties": false
    })
}

fn approval_request_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": [
            "id",
            "task_id",
            "run_id",
            "action",
            "reason",
            "status",
            "requested_at",
            "resolved_at",
            "resolved_by"
        ],
        "properties": {
            "id": slug_string_schema(),
            "task_id": slug_string_schema(),
            "run_id": nullable_slug_string_schema(),
            "action": persisted_non_empty_string_schema(),
            "reason": persisted_non_empty_string_schema(),
            "status": {
                "type": "string",
                "enum": ["pending", "approved", "denied"]
            },
            "requested_at": {
                "type": "string",
                "format": "date-time"
            },
            "resolved_at": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "resolved_by": nullable_persisted_non_empty_string_schema()
        },
        "additionalProperties": false
    })
}

fn run_artifact_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["kind", "path", "bytes", "content_type"],
        "properties": {
            "kind": {
                "type": "string",
                "enum": ["stdout", "stderr", "summary", "diff", "file"]
            },
            "path": persisted_non_empty_string_schema(),
            "bytes": { "type": ["integer", "null"], "minimum": 0 },
            "content_type": nullable_persisted_non_empty_string_schema()
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

fn secret_reference_map(description: &str) -> serde_json::Value {
    json!({
        "type": "object",
        "description": description,
        "additionalProperties": {
            "type": "string",
            "minLength": 1,
            "pattern": r"^(?:[A-Za-z_][A-Za-z0-9_]*|[A-Za-z0-9][A-Za-z0-9_-]*:[^\s\u0000][^\u0000]*)$"
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

fn secrets_backend_kind_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "enum": SECRETS_BACKEND_KIND_VALUES,
    })
}

fn secrets_backend_kind_input_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "enum": SECRETS_BACKEND_KIND_INPUT_VALUES,
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

fn get_endpoint(summary: &str, response_schema: serde_json::Value) -> serde_json::Value {
    let mut endpoint = endpoint(summary);
    endpoint["get"]["responses"]["200"]["content"] = json_response(response_schema);
    endpoint
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

fn prometheus_metrics_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Aggregate monitor metrics in Prometheus text format");
    let content = json!({
        "text/plain; version=0.0.4": {
            "schema": {
                "type": "string"
            }
        }
    });
    endpoint["get"]["responses"]["200"]["content"] = content.clone();
    endpoint["get"]["responses"]["503"] = json!({
        "description": "State unavailable",
        "content": json_response(schema_ref("ErrorResponse"))
    });
    endpoint
}

fn dashboard_html_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Render the local operator dashboard");
    endpoint["get"]["responses"]["200"]["content"] = json!({
        "text/html": {
            "schema": {
                "type": "string"
            }
        }
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

fn workflow_dag_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Get workflow DAG nodes and dependency edges");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("WorkflowDag"));
    endpoint
}

fn workflow_add_task_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_body(
        "Add a task stage to a workflow DAG",
        schema_ref("WorkflowAddTaskRequest"),
    );
    if let Some(responses) = endpoint["post"]["responses"].as_object_mut() {
        responses.remove("200");
    }
    endpoint["post"]["responses"]["201"] = json!({
        "description": "Created",
        "content": json_response(schema_ref("WorkflowAddTaskResponse"))
    });
    endpoint["post"]["responses"]["422"] = error_response_with_description("Unprocessable request");
    endpoint
}

fn workflow_edge_endpoint(summary: &str) -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_body(summary, schema_ref("WorkflowEdgeRequest"));
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("WorkflowEdgeResponse"));
    endpoint
}

fn workflow_transition_endpoint(summary: &str) -> serde_json::Value {
    let mut endpoint =
        mutation_endpoint_with_optional_body(summary, schema_ref("WorkflowNoteRequest"));
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("WorkflowTransitionResponse"));
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

fn approvals_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("List approval gates");
    endpoint["get"]["responses"]["200"]["content"] = array_response(schema_ref("ApprovalRequest"));
    endpoint
}

fn approval_resolve_endpoint(summary: &str) -> serde_json::Value {
    let mut endpoint =
        mutation_endpoint_with_optional_body(summary, schema_ref("ApprovalResolveRequest"));
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("ApprovalResolveResponse"));
    endpoint
}

fn workers_endpoint() -> serde_json::Value {
    let mut endpoint = collection_endpoint_with_body(
        "List distributed worker nodes",
        "Register a distributed worker node",
        schema_ref("WorkerRegisterRequest"),
    );
    endpoint["get"]["responses"]["200"]["content"] = array_response(schema_ref("WorkerNode"));
    if let Some(responses) = endpoint["post"]["responses"].as_object_mut() {
        responses.remove("200");
    }
    endpoint["post"]["responses"]["201"] = json!({
        "description": "Created",
        "content": json_response(schema_ref("WorkerMutationResponse"))
    });
    endpoint["get"]["parameters"] = json!([
        {
            "name": "status",
            "in": "query",
            "required": false,
            "description": "Filter workers by status. Aliases `up`, `pause`, and `down` map to `online`, `paused`, and `offline`.",
            "schema": agent_status_input_schema()
        },
        {
            "name": "since",
            "in": "query",
            "required": false,
            "description": "Filter workers seen at or after this RFC3339 timestamp.",
            "schema": date_time_schema()
        },
        {
            "name": "until",
            "in": "query",
            "required": false,
            "description": "Filter workers seen at or before this RFC3339 timestamp.",
            "schema": date_time_schema()
        },
        {
            "name": "query",
            "in": "query",
            "required": false,
            "description": "Case-insensitive worker text search over ID, endpoint, and status.",
            "schema": non_empty_string("Worker search query.")
        },
        {
            "name": "limit",
            "in": "query",
            "required": false,
            "description": "Maximum number of recently seen workers to return.",
            "schema": positive_integer()
        }
    ]);
    endpoint["get"]["responses"]["400"] =
        error_response_with_description("Invalid query parameter");
    endpoint
}

fn worker_detail_endpoint() -> serde_json::Value {
    let mut endpoint = detail_delete_endpoint("Get one distributed worker", "Remove one worker");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("WorkerNode"));
    endpoint["delete"]["responses"]["200"]["content"] =
        json_response(schema_ref("WorkerDeleteResponse"));
    endpoint
}

fn worker_heartbeat_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Record a worker heartbeat",
        schema_ref("WorkerHeartbeatRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("WorkerMutationResponse"));
    endpoint
}

fn worker_claim_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Claim the next ready task for a matching worker agent",
        schema_ref("ClaimTaskRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("WorkerClaimResponse"));
    endpoint
}

fn worker_report_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_body(
        "Report completion or failure for a claimed worker task",
        schema_ref("WorkerReportRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("WorkerReportResponse"));
    endpoint
}

fn evals_endpoint() -> serde_json::Value {
    let mut endpoint = collection_endpoint_with_body(
        "List eval records",
        "Record an eval result",
        schema_ref("EvalRecordRequest"),
    );
    endpoint["get"]["responses"]["200"]["content"] = array_response(schema_ref("EvalRecord"));
    if let Some(responses) = endpoint["post"]["responses"].as_object_mut() {
        responses.remove("200");
    }
    endpoint["post"]["responses"]["201"] = json!({
        "description": "Created",
        "content": json_response(schema_ref("EvalMutationResponse"))
    });
    endpoint["get"]["parameters"] = json!([
        {
            "name": "target",
            "in": "query",
            "required": false,
            "description": "Filter evals by target.",
            "schema": non_empty_string("Eval target.")
        },
        {
            "name": "success",
            "in": "query",
            "required": false,
            "description": "Filter evals by success value.",
            "schema": { "type": "boolean" }
        },
        {
            "name": "since",
            "in": "query",
            "required": false,
            "description": "Filter evals recorded at or after this RFC3339 timestamp.",
            "schema": date_time_schema()
        },
        {
            "name": "until",
            "in": "query",
            "required": false,
            "description": "Filter evals recorded at or before this RFC3339 timestamp.",
            "schema": date_time_schema()
        },
        {
            "name": "query",
            "in": "query",
            "required": false,
            "description": "Case-insensitive eval text search over ID, target, and success.",
            "schema": non_empty_string("Eval search query.")
        },
        {
            "name": "limit",
            "in": "query",
            "required": false,
            "description": "Maximum number of recent evals to return.",
            "schema": positive_integer()
        }
    ]);
    endpoint["get"]["responses"]["400"] =
        error_response_with_description("Invalid query parameter");
    endpoint
}

fn eval_run_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint("Run and record a local eval command");
    endpoint["post"]["requestBody"] = request_body(schema_ref("EvalRunRequest"));
    if let Some(responses) = endpoint["post"]["responses"].as_object_mut() {
        responses.remove("200");
    }
    endpoint["post"]["responses"]["201"] = json!({
        "description": "Created",
        "content": json_response(schema_ref("EvalRunResponse"))
    });
    endpoint
}

fn eval_detail_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Get one eval record");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("EvalRecord"));
    endpoint
}

fn git_status_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Inspect git status for a local workspace");
    endpoint["get"]["responses"]["200"]["content"] =
        json_response(schema_ref("GitCommandResponse"));
    endpoint["get"]["parameters"] = json!([
        {
            "name": "cwd",
            "in": "query",
            "required": false,
            "description": "Workspace directory. Defaults to the API process current directory.",
            "schema": non_empty_string("Git working directory.")
        }
    ]);
    endpoint["get"]["responses"]["400"] = error_response_with_description("Invalid git request");
    endpoint
}

fn git_review_task_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Create an Agent OS code-review task for a git diff",
        schema_ref("GitReviewTaskRequest"),
    );
    if let Some(responses) = endpoint["post"]["responses"].as_object_mut() {
        responses.remove("200");
    }
    endpoint["post"]["responses"]["201"] = json!({
        "description": "Created",
        "content": json_response(schema_ref("TaskMutationResponse"))
    });
    endpoint["post"]["responses"]["400"] = error_response_with_description("Invalid git request");
    endpoint
}

fn secrets_endpoint() -> serde_json::Value {
    let mut endpoint = collection_endpoint_with_body(
        "List secret manager backends",
        "Register a secret manager backend",
        schema_ref("SecretsRegisterRequest"),
    );
    endpoint["get"]["responses"]["200"]["content"] = array_response(schema_ref("SecretsBackend"));
    if let Some(responses) = endpoint["post"]["responses"].as_object_mut() {
        responses.remove("200");
    }
    endpoint["post"]["responses"]["201"] = json!({
        "description": "Created",
        "content": json_response(schema_ref("SecretsMutationResponse"))
    });
    endpoint["get"]["parameters"] = json!([
        {
            "name": "kind",
            "in": "query",
            "required": false,
            "description": "Filter secret manager backends by kind.",
            "schema": secrets_backend_kind_input_schema()
        },
        {
            "name": "query",
            "in": "query",
            "required": false,
            "description": "Case-insensitive secrets backend search over ID, kind, and reference.",
            "schema": non_empty_string("Secrets backend search query.")
        },
        {
            "name": "limit",
            "in": "query",
            "required": false,
            "description": "Maximum number of backends to return.",
            "schema": positive_integer()
        }
    ]);
    endpoint["get"]["responses"]["400"] =
        error_response_with_description("Invalid query parameter");
    endpoint
}

fn secrets_detail_endpoint() -> serde_json::Value {
    let mut endpoint = detail_delete_endpoint(
        "Get one secret manager backend",
        "Remove one secret manager backend",
    );
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("SecretsBackend"));
    endpoint["delete"]["responses"]["200"]["content"] =
        json_response(schema_ref("SecretsDeleteResponse"));
    endpoint
}

fn secrets_check_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Check task secret environment references without exposing values");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("SecretCheckReport"));
    endpoint
}

fn registry_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("List reusable registry entries");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("RegistryResponse"));
    endpoint
}

fn registry_profiles_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("List reusable agent profiles");
    endpoint["get"]["responses"]["200"]["content"] =
        json_response(string_keyed_map_schema(schema_ref("AgentProfile")));
    endpoint
}

fn registry_profile_detail_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Get one reusable agent profile");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("AgentProfile"));
    endpoint
}

fn registry_profile_agents_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Install an agent from a reusable profile",
        schema_ref("InstallAgentProfileRequest"),
    );
    if let Some(responses) = endpoint["post"]["responses"].as_object_mut() {
        responses.remove("200");
    }
    endpoint["post"]["responses"]["201"] = json!({
        "description": "Created",
        "content": json_response(schema_ref("AgentMutationResponse"))
    });
    endpoint["post"]["responses"]["409"]["content"] = json_response(schema_ref("ErrorResponse"));
    endpoint
}

fn registry_templates_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("List reusable workflow templates");
    endpoint["get"]["responses"]["200"]["content"] =
        json_response(string_keyed_map_schema(schema_ref("WorkflowTemplate")));
    endpoint
}

fn registry_template_detail_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Get one reusable workflow template");
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("WorkflowTemplate"));
    endpoint
}

fn registry_template_workflows_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_body(
        "Create a workflow from a reusable template",
        schema_ref("TemplateWorkflowRequest"),
    );
    if let Some(responses) = endpoint["post"]["responses"].as_object_mut() {
        responses.remove("200");
    }
    endpoint["post"]["responses"]["201"] = json!({
        "description": "Created",
        "content": json_response(schema_ref("TemplateWorkflowResponse"))
    });
    endpoint
}

fn registry_marketplace_import_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_body(
        "Import marketplace registry entries",
        schema_ref("MarketplaceImportRequest"),
    );
    if let Some(responses) = endpoint["post"]["responses"].as_object_mut() {
        responses.remove("200");
    }
    endpoint["post"]["responses"]["201"] = json!({
        "description": "Imported",
        "content": json_response(schema_ref("MarketplaceImportResponse"))
    });
    endpoint
}

fn registry_mcp_servers_endpoint() -> serde_json::Value {
    let mut endpoint = collection_endpoint_with_body(
        "List registered MCP servers",
        "Register an MCP server",
        schema_ref("RegisterMcpServerRequest"),
    );
    endpoint["get"]["responses"]["200"]["content"] =
        json_response(string_keyed_map_schema(schema_ref("McpServer")));
    if let Some(responses) = endpoint["post"]["responses"].as_object_mut() {
        responses.remove("200");
    }
    endpoint["post"]["responses"]["201"] = json!({
        "description": "Created",
        "content": json_response(schema_ref("McpServerMutationResponse"))
    });
    endpoint
}

fn registry_mcp_server_detail_endpoint() -> serde_json::Value {
    let mut endpoint = detail_delete_endpoint(
        "Get one registered MCP server",
        "Remove one registered MCP server",
    );
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("McpServer"));
    endpoint["post"] = mutation_endpoint_with_body(
        "Update one registered MCP server",
        schema_ref("UpdateMcpServerRequest"),
    )["post"]
        .clone();
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("McpServerMutationResponse"));
    endpoint["delete"]["responses"]["200"]["content"] =
        json_response(schema_ref("McpServerDeleteResponse"));
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
            "name": "visibility",
            "in": "query",
            "required": false,
            "description": "Filter memory records by visibility.",
            "schema": memory_visibility_schema()
        },
        {
            "name": "scope",
            "in": "query",
            "required": false,
            "description": "Filter memory records by scope. Empty values are rejected.",
            "schema": persisted_non_empty_string_schema()
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

fn memory_recall_endpoint() -> serde_json::Value {
    let mut endpoint = get_endpoint(
        "Recall scored memory snippets",
        json!({
            "type": "array",
            "items": schema_ref("MemoryRecallHit")
        }),
    );
    endpoint["get"]["parameters"] = json!([
        {
            "name": "query",
            "in": "query",
            "required": true,
            "description": "Memory recall query. Empty values are rejected.",
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
            "description": "Filter recall records that have this tag. Comma-separated values require all listed tags.",
            "schema": normalized_list_string_schema()
        },
        {
            "name": "visibility",
            "in": "query",
            "required": false,
            "description": "Filter recall records by visibility.",
            "schema": memory_visibility_schema()
        },
        {
            "name": "scope",
            "in": "query",
            "required": false,
            "description": "Filter recall records by scope. Empty values are rejected.",
            "schema": persisted_non_empty_string_schema()
        },
        {
            "name": "since",
            "in": "query",
            "required": false,
            "description": "Filter recall records updated at or after this RFC3339 timestamp.",
            "schema": {
                "type": "string",
                "format": "date-time"
            }
        },
        {
            "name": "until",
            "in": "query",
            "required": false,
            "description": "Filter recall records updated at or before this RFC3339 timestamp.",
            "schema": {
                "type": "string",
                "format": "date-time"
            }
        },
        {
            "name": "limit",
            "in": "query",
            "required": false,
            "description": "Maximum number of scored recall hits to return. Must be greater than 0.",
            "schema": positive_integer()
        }
    ]);
    endpoint["get"]["responses"]["400"] =
        error_response_with_description("Invalid memory recall query parameter");
    endpoint
}

fn memory_prune_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Prune expired memory records",
        schema_ref("PruneMemoryRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("PruneMemoryResponse"));
    endpoint["post"]["responses"]["400"] =
        error_response_with_description("Invalid memory prune request");
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

fn run_debug_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Debug run with replay, agent, workflow, approvals, and artifacts");
    endpoint["get"]["parameters"] = log_tail_parameters();
    endpoint["get"]["responses"]["200"]["content"] = json_response(schema_ref("RunDebugResponse"));
    endpoint["get"]["responses"]["400"] =
        error_response_with_description("Invalid query parameter");
    endpoint
}

fn run_artifacts_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("List run artifacts");
    endpoint["get"]["responses"]["200"]["content"] =
        json_response(schema_ref("RunArtifactsResponse"));
    endpoint
}

fn run_artifact_endpoint() -> serde_json::Value {
    let mut endpoint = endpoint("Read one run artifact");
    endpoint["get"]["parameters"] = log_tail_parameters();
    endpoint["get"]["responses"]["200"]["content"] =
        json_response(schema_ref("RunArtifactReadResponse"));
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

fn with_artifact_id_parameter(mut endpoint: serde_json::Value) -> serde_json::Value {
    for method in ["get", "post", "delete"] {
        if let Some(operation) = endpoint.get_mut(method) {
            let mut parameters = operation
                .get("parameters")
                .and_then(|parameters| parameters.as_array())
                .cloned()
                .unwrap_or_default();
            parameters.insert(
                0,
                json!({
                    "name": "artifact_id",
                    "in": "path",
                    "required": true,
                    "description": "Run artifact ID, artifact kind, or zero-based artifact index.",
                    "schema": {
                        "type": "string",
                        "minLength": 1,
                        "pattern": ".*\\S.*"
                    }
                }),
            );
            operation["parameters"] = json!(parameters);
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

fn service_systemd_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Render systemd user service unit",
        schema_ref("SystemdServiceRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("SystemdServiceResponse"));
    endpoint
}

fn service_systemd_install_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Install systemd user service unit",
        schema_ref("SystemdServiceRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("InstallSystemdServiceResponse"));
    endpoint
}

fn service_systemd_uninstall_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Uninstall systemd user service unit",
        schema_ref("UninstallSystemdServiceRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("UninstallSystemdServiceResponse"));
    endpoint
}

fn service_systemd_start_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Start systemd user service",
        schema_ref("SystemdServiceControlRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("SystemdServiceStartResponse"));
    endpoint["post"]["responses"]["409"]["content"] =
        json_response(schema_ref("SystemctlCommandErrorResponse"));
    endpoint
}

fn service_systemd_stop_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Stop systemd user service",
        schema_ref("SystemdServiceControlRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("SystemdServiceStopResponse"));
    endpoint["post"]["responses"]["409"]["content"] =
        json_response(schema_ref("SystemctlCommandErrorResponse"));
    endpoint
}

fn service_systemd_status_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Read systemd user service status",
        schema_ref("SystemdServiceControlRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("SystemdServiceStatusResponse"));
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

fn state_sqlite_endpoint() -> serde_json::Value {
    let mut endpoint = mutation_endpoint_with_optional_body(
        "Initialize, sync, or restore a SQLite state mirror",
        schema_ref("StateSqliteRequest"),
    );
    endpoint["post"]["responses"]["200"]["content"] =
        json_response(schema_ref("StateSqliteResponse"));
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
        ["workflows", id, "dag"] => Some(match parse_workflow_path_id(id) {
            Ok(id) => workflow_dag_json(os, &id, query),
            Err(response) => response,
        }),
        ["registry", "profiles", id] => Some(match parse_registry_path_id("profile id", id) {
            Ok(id) => json_detail(os.agent_profiles.get(&id), "agent profile not found"),
            Err(response) => response,
        }),
        ["registry", "templates", id] => Some(match parse_registry_path_id("template id", id) {
            Ok(id) => json_detail(
                os.workflow_templates.get(&id),
                "workflow template not found",
            ),
            Err(response) => response,
        }),
        ["registry", "mcp-servers", id] => {
            Some(match parse_registry_path_id("mcp server id", id) {
                Ok(id) => json_detail(os.mcp_servers.get(&id), "mcp server not found"),
                Err(response) => response,
            })
        }
        ["workers", id] => Some(match parse_worker_path_id(id) {
            Ok(id) => json_detail(os.workers.get(&id), "worker not found"),
            Err(response) => response,
        }),
        ["evals", id] => Some(match parse_eval_path_id(id) {
            Ok(id) => json_detail(
                os.evals.iter().find(|record| record.id == id),
                "eval not found",
            ),
            Err(response) => response,
        }),
        ["secrets", "check"] => Some(("200 OK", json!(secret_check_report(os)).to_string())),
        ["secrets", id] => Some(match parse_secrets_backend_path_id(id) {
            Ok(id) => json_detail(os.secrets_backends.get(&id), "secrets backend not found"),
            Err(response) => response,
        }),
        ["tools", id] => Some(match parse_tool_path_id(id) {
            Ok(id) => json_detail(os.tools.get(&id), "tool not found"),
            Err(response) => response,
        }),
        ["memory", "recall"] => Some(match memory_recall_json(os, query) {
            Ok(recall) => ("200 OK", recall.to_string()),
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
        ["runs", id, "debug"] => Some(match parse_run_path_id(id) {
            Ok(id) => run_debug_response(store, os, &id, query),
            Err(response) => response,
        }),
        ["runs", id, "artifacts"] => Some(match parse_run_path_id(id) {
            Ok(id) => run_artifacts_response(os, &id, query),
            Err(response) => response,
        }),
        ["runs", id, "artifacts", artifact_id] => Some(match parse_run_path_id(id) {
            Ok(id) => run_artifact_response(os, &id, artifact_id, query),
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

fn run_debug_response(
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
    let (log, log_error, log_path) = match store {
        Some(store) => {
            let log_path = store.run_log_path(id);
            match std::fs::read_to_string(&log_path) {
                Ok(body) => {
                    log_truncated = text_tail_was_truncated(&body, tail_bytes);
                    (
                        Some(tail_text_by_bytes(&body, tail_bytes)),
                        None,
                        Some(log_path.display().to_string()),
                    )
                }
                Err(error) => (
                    None,
                    Some(format!(
                        "could not read run log: {}: {error}",
                        log_path.display()
                    )),
                    Some(log_path.display().to_string()),
                ),
            }
        }
        None => (None, Some("store unavailable".to_owned()), None),
    };
    let related_events = os
        .events
        .iter()
        .filter(|event| {
            event.message.contains(&run.id.to_string())
                || event.message.contains(&run.task_id.to_string())
        })
        .collect::<Vec<_>>();
    let workflows = os
        .workflows
        .values()
        .filter(|workflow| {
            workflow
                .tasks
                .values()
                .any(|task_id| task_id == &run.task_id)
        })
        .collect::<Vec<_>>();
    let approvals = os
        .approvals
        .values()
        .filter(|approval| approval.task_id == run.task_id || approval.run_id.as_ref() == Some(id))
        .collect::<Vec<_>>();
    let artifact_status = run
        .artifacts
        .iter()
        .map(|artifact| {
            json!({
                "artifact": artifact,
                "exists": Path::new(&artifact.path).exists(),
            })
        })
        .collect::<Vec<_>>();

    (
        "200 OK",
        json!({
            "run": run,
            "task": os.tasks.get(&run.task_id),
            "agent": run.agent_id.as_ref().and_then(|agent_id| os.agents.get(agent_id)),
            "workflows": workflows,
            "approvals": approvals,
            "events": related_events,
            "log": log,
            "log_tail_bytes": tail_bytes,
            "log_truncated": log_truncated,
            "log_error": log_error,
            "artifact_status": artifact_status,
            "diagnostics": {
                "log_path": log_path,
                "related_events": related_events.len(),
                "related_workflows": workflows.len(),
                "related_approvals": approvals.len(),
                "artifacts": run.artifacts.len(),
            }
        })
        .to_string(),
    )
}

fn run_artifacts_response(os: &OperatingSystem, id: &RunId, query: &str) -> (&'static str, String) {
    let Some(run) = os.runs.get(id) else {
        return (
            "404 Not Found",
            json!({ "error": "run not found" }).to_string(),
        );
    };
    if let Err(response) = reject_unknown_query_keys(query, &[]) {
        return response;
    }
    (
        "200 OK",
        json!({
            "run_id": id,
            "artifacts": run_artifact_entries(run),
        })
        .to_string(),
    )
}

fn run_artifact_response(
    os: &OperatingSystem,
    id: &RunId,
    artifact_id: &str,
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
    let Some((index, artifact)) = find_run_artifact(run, artifact_id) else {
        return (
            "404 Not Found",
            json!({ "error": "run artifact not found" }).to_string(),
        );
    };
    let artifact_id = run_artifact_id(run, index, artifact);
    (
        "200 OK",
        run_artifact_read_json(run, index, &artifact_id, artifact, tail_bytes).to_string(),
    )
}

fn run_artifact_entries(run: &RunRecord) -> Vec<serde_json::Value> {
    run.artifacts
        .iter()
        .enumerate()
        .map(|(index, artifact)| {
            let id = run_artifact_id(run, index, artifact);
            let status = run_artifact_file_status(artifact);
            json!({
                "id": id,
                "index": index,
                "name": run_artifact_kind_name(&artifact.kind),
                "artifact": artifact,
                "exists": status.exists,
                "readable": status.readable,
                "bytes": status.bytes.or(artifact.bytes),
                "checksum": status.checksum,
                "error": status.error,
            })
        })
        .collect()
}

fn run_artifact_read_json(
    run: &RunRecord,
    index: usize,
    artifact_id: &str,
    artifact: &RunArtifact,
    tail_bytes: Option<usize>,
) -> serde_json::Value {
    match run_artifact_body(run, artifact, tail_bytes) {
        Ok(body) => json!({
            "run_id": run.id,
            "artifact_id": artifact_id,
            "index": index,
            "artifact": artifact,
            "exists": true,
            "bytes": body.bytes,
            "checksum": body.checksum,
            "content_type": body.content_type,
            "tail_bytes": tail_bytes,
            "truncated": body.truncated,
            "body": body.body,
            "error": serde_json::Value::Null,
        }),
        Err(error) => json!({
            "run_id": run.id,
            "artifact_id": artifact_id,
            "index": index,
            "artifact": artifact,
            "exists": false,
            "bytes": artifact.bytes,
            "checksum": serde_json::Value::Null,
            "content_type": artifact.content_type,
            "tail_bytes": tail_bytes,
            "truncated": false,
            "body": serde_json::Value::Null,
            "error": error,
        }),
    }
}

struct ArtifactBody {
    body: String,
    bytes: u64,
    checksum: String,
    content_type: String,
    truncated: bool,
}

fn run_artifact_body(
    run: &RunRecord,
    artifact: &RunArtifact,
    tail_bytes: Option<usize>,
) -> Result<ArtifactBody, String> {
    if matches!(artifact.kind, RunArtifactKind::Summary) && artifact.path.starts_with("run:") {
        let body = format!(
            "run: {}\nstatus: {}\ntask: {}\nagent: {}\ncommand: {}\ncwd: {}\nexit: {}\nstarted: {}\nfinished: {}\n",
            run.id,
            run.status,
            run.task_id,
            run.agent_id
                .as_ref()
                .map(AgentId::to_string)
                .unwrap_or_else(|| "-".into()),
            run.command,
            run.cwd,
            run.exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "-".into()),
            run.started_at.to_rfc3339(),
            run.finished_at
                .map(|at| at.to_rfc3339())
                .unwrap_or_else(|| "-".into()),
        );
        return Ok(artifact_body_from_string(
            body,
            tail_bytes,
            artifact
                .content_type
                .clone()
                .unwrap_or_else(|| "text/plain".into()),
        ));
    }

    let bytes = std::fs::read(&artifact.path)
        .map_err(|error| format!("could not read artifact {}: {error}", artifact.path))?;
    let body = String::from_utf8_lossy(&bytes).into_owned();
    Ok(artifact_body_from_string(
        body,
        tail_bytes,
        artifact
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".into()),
    ))
}

fn artifact_body_from_string(
    body: String,
    tail_bytes: Option<usize>,
    content_type: String,
) -> ArtifactBody {
    let bytes = body.len() as u64;
    let checksum = fnv1a64_checksum(body.as_bytes());
    let truncated = text_tail_was_truncated(&body, tail_bytes);
    let body = tail_text_by_bytes(&body, tail_bytes);
    ArtifactBody {
        bytes,
        checksum,
        content_type,
        truncated,
        body,
    }
}

struct ArtifactFileStatus {
    exists: bool,
    readable: bool,
    bytes: Option<u64>,
    checksum: Option<String>,
    error: Option<String>,
}

fn run_artifact_file_status(artifact: &RunArtifact) -> ArtifactFileStatus {
    if matches!(artifact.kind, RunArtifactKind::Summary) && artifact.path.starts_with("run:") {
        return ArtifactFileStatus {
            exists: true,
            readable: true,
            bytes: None,
            checksum: None,
            error: None,
        };
    }
    match std::fs::read(&artifact.path) {
        Ok(bytes) => ArtifactFileStatus {
            exists: true,
            readable: true,
            bytes: Some(bytes.len() as u64),
            checksum: Some(fnv1a64_checksum(&bytes)),
            error: None,
        },
        Err(error) => ArtifactFileStatus {
            exists: Path::new(&artifact.path).exists(),
            readable: false,
            bytes: artifact.bytes,
            checksum: None,
            error: Some(error.to_string()),
        },
    }
}

fn fnv1a64_checksum(bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("fnv1a64:{hash:016x}")
}

fn find_run_artifact<'a>(
    run: &'a RunRecord,
    artifact_id: &str,
) -> Option<(usize, &'a RunArtifact)> {
    let artifact_id = artifact_id.trim();
    if artifact_id.is_empty() {
        return None;
    }
    if let Ok(index) = artifact_id.parse::<usize>() {
        return run.artifacts.get(index).map(|artifact| (index, artifact));
    }
    run.artifacts.iter().enumerate().find(|(index, artifact)| {
        run_artifact_id(run, *index, artifact) == artifact_id
            || run_artifact_kind_name(&artifact.kind) == artifact_id
    })
}

fn run_artifact_id(run: &RunRecord, index: usize, artifact: &RunArtifact) -> String {
    let kind = run_artifact_kind_name(&artifact.kind);
    let count = run
        .artifacts
        .iter()
        .filter(|candidate| candidate.kind == artifact.kind)
        .count();
    if count == 1 {
        kind.to_owned()
    } else {
        format!("{kind}-{index}")
    }
}

fn run_artifact_kind_name(kind: &RunArtifactKind) -> &'static str {
    match kind {
        RunArtifactKind::Stdout => "stdout",
        RunArtifactKind::Stderr => "stderr",
        RunArtifactKind::Summary => "summary",
        RunArtifactKind::Diff => "diff",
        RunArtifactKind::File => "file",
    }
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
    let state_path = store.path().display().to_string();
    let config_path_text = config["config_path"].as_str().unwrap_or("").to_string();
    let config_exists = config["config_exists"].as_bool().unwrap_or(false);
    let config_loads = config["config_loads"].as_bool().unwrap_or(false);
    let config_valid = config["config_valid"].as_bool();
    let next_steps = doctor_next_steps(
        &state_directory,
        &state_path,
        state_exists,
        state_loads,
        state_valid,
        &config_path_text,
        config_exists,
        config_loads,
        config_valid,
    );

    json!({
        "agent_os_version": env!("CARGO_PKG_VERSION"),
        "platform": doctor_platform(),
        "service_manager": doctor_service_manager(),
        "service_recommendation": doctor_service_recommendation(),
        "shell_execution_supported": doctor_shell_execution_supported(),
        "shell_execution_note": doctor_shell_execution_note(),
        "state_path": state_path,
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
        "next_steps": next_steps,
    })
}

fn doctor_platform() -> &'static str {
    std::env::consts::OS
}

fn doctor_service_manager() -> &'static str {
    if cfg!(target_os = "macos") {
        "launchd"
    } else if cfg!(target_os = "linux") {
        "systemd"
    } else {
        "manual"
    }
}

fn doctor_service_recommendation() -> &'static str {
    if cfg!(target_os = "macos") {
        "agent-os service install && agent-os service start"
    } else if cfg!(target_os = "linux") {
        "agent-os service install-systemd && agent-os service start-systemd"
    } else if cfg!(target_os = "windows") {
        "run agent-os daemon run from a supervised terminal or external Windows service wrapper"
    } else {
        "run agent-os daemon run under the local platform supervisor"
    }
}

fn doctor_shell_execution_supported() -> bool {
    cfg!(unix)
}

fn doctor_shell_execution_note() -> &'static str {
    if cfg!(unix) {
        "shell tasks execute with sh -c and can use Unix process-group cancellation when enabled"
    } else {
        "shell tasks require a Unix-like sh; use provider planning, built-in file tools, or an external supervisor on this platform"
    }
}

fn shell_arg(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':'))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

#[allow(clippy::too_many_arguments)]
fn doctor_next_steps(
    state_directory: &str,
    state_path: &str,
    state_exists: bool,
    state_loads: bool,
    state_valid: Option<bool>,
    config_path: &str,
    config_exists: bool,
    config_loads: bool,
    config_valid: Option<bool>,
) -> Vec<String> {
    let mut steps = Vec::new();
    let state_arg = shell_arg(state_directory);

    if !state_exists {
        steps.push(format!("agent-os --state {state_arg} init"));
    } else if !state_loads {
        steps.push(format!(
            "Restore or repair {}, then rerun agent-os --state {state_arg} doctor",
            shell_arg(state_path)
        ));
    } else if state_valid == Some(false) {
        steps.push(format!(
            "agent-os --state {state_arg} state repair --dry-run"
        ));
    }

    if !config_path.is_empty() {
        let config_arg = shell_arg(config_path);
        if !config_exists {
            steps.push(format!(
                "agent-os --config {config_arg} config init --profile safe"
            ));
        } else if !config_loads || config_valid == Some(false) {
            steps.push(format!(
                "Fix config issues, then run agent-os --config {config_arg} config validate"
            ));
        }
    }

    steps
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
    let run_duration = run_duration_metrics(os);
    let task_queue_age = task_queue_age_metrics(os);

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
        "oldest_active_run_age_ms": oldest_active_run_age_ms(os),
        "oldest_queued_task_age_ms": oldest_queued_task_age_ms(os),
        "task_queue_age_ms_count": task_queue_age.count,
        "task_queue_age_ms_sum": task_queue_age.sum,
        "task_queue_age_ms_buckets": task_queue_age.buckets,
        "run_duration_ms_count": run_duration.count,
        "run_duration_ms_sum": run_duration.sum,
        "run_duration_ms_buckets": run_duration.buckets,
        "tools_total": os.tools.len(),
        "events_total": os.events.len(),
        "memories_total": os.memory.len(),
        "daemon_status": os.daemon.as_ref().map(|daemon| daemon.status.to_string()),
        "daemon_ticks": os.daemon.as_ref().map(|daemon| daemon.ticks),
    })
}

fn metrics_response_body(metrics: serde_json::Value, path: &str) -> String {
    if is_prometheus_metrics_path(path) {
        metrics_prometheus(&metrics)
    } else {
        metrics.to_string()
    }
}

pub fn metrics_prometheus(metrics: &serde_json::Value) -> String {
    let mut lines = Vec::new();
    lines.push("# HELP agent_os_build_info Agent OS build information.".to_owned());
    lines.push("# TYPE agent_os_build_info gauge".to_owned());
    lines.push(format!(
        "agent_os_build_info{{version=\"{}\"}} 1",
        prometheus_label_value(
            metrics
                .get("agent_os_version")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(env!("CARGO_PKG_VERSION"))
        )
    ));

    for (name, key, help) in [
        ("agent_os_up", "ok", "Whether Agent OS state is healthy."),
        (
            "agent_os_state_loads",
            "state_loads",
            "Whether durable state loaded.",
        ),
        (
            "agent_os_state_valid",
            "state_valid",
            "Whether durable state validates.",
        ),
        (
            "agent_os_state_issue_count",
            "state_issue_count",
            "State validation issue count.",
        ),
        ("agent_os_agents_total", "agents_total", "Total agents."),
        ("agent_os_agents_online", "agents_online", "Online agents."),
        ("agent_os_agents_busy", "agents_busy", "Busy agents."),
        ("agent_os_agents_paused", "agents_paused", "Paused agents."),
        (
            "agent_os_agents_offline",
            "agents_offline",
            "Offline agents.",
        ),
        ("agent_os_tasks_total", "tasks_total", "Total tasks."),
        ("agent_os_tasks_pending", "tasks_pending", "Pending tasks."),
        ("agent_os_tasks_running", "tasks_running", "Running tasks."),
        ("agent_os_tasks_blocked", "tasks_blocked", "Blocked tasks."),
        (
            "agent_os_tasks_complete",
            "tasks_complete",
            "Complete tasks.",
        ),
        ("agent_os_tasks_failed", "tasks_failed", "Failed tasks."),
        (
            "agent_os_tasks_cancelled",
            "tasks_cancelled",
            "Cancelled tasks.",
        ),
        (
            "agent_os_workflows_total",
            "workflows_total",
            "Total workflows.",
        ),
        ("agent_os_runs_total", "runs_total", "Total runs."),
        ("agent_os_runs_running", "runs_running", "Running runs."),
        (
            "agent_os_runs_cancel_requested",
            "runs_cancel_requested",
            "Runs with cancellation requested.",
        ),
        (
            "agent_os_runs_cancelled",
            "runs_cancelled",
            "Cancelled runs.",
        ),
        ("agent_os_runs_success", "runs_success", "Successful runs."),
        ("agent_os_runs_failed", "runs_failed", "Failed runs."),
        ("agent_os_runs_rejected", "runs_rejected", "Rejected runs."),
        (
            "agent_os_oldest_active_run_age_ms",
            "oldest_active_run_age_ms",
            "Age of the oldest running or cancel-requested run in milliseconds.",
        ),
        (
            "agent_os_oldest_queued_task_age_ms",
            "oldest_queued_task_age_ms",
            "Age of the oldest pending or blocked task in milliseconds.",
        ),
        ("agent_os_tools_total", "tools_total", "Total tools."),
        ("agent_os_events_total", "events_total", "Total events."),
        (
            "agent_os_memories_total",
            "memories_total",
            "Total memory records.",
        ),
        (
            "agent_os_daemon_ticks",
            "daemon_ticks",
            "Daemon tick count.",
        ),
    ] {
        push_prometheus_gauge(&mut lines, name, key, help, metrics);
    }

    push_prometheus_histogram(
        &mut lines,
        "agent_os_task_queue_age_ms",
        "Pending and blocked task age histogram in milliseconds.",
        "task_queue_age_ms_buckets",
        "task_queue_age_ms_sum",
        "task_queue_age_ms_count",
        metrics,
    );
    push_prometheus_histogram(
        &mut lines,
        "agent_os_run_duration_ms",
        "Finished run duration histogram in milliseconds.",
        "run_duration_ms_buckets",
        "run_duration_ms_sum",
        "run_duration_ms_count",
        metrics,
    );
    lines.push(String::new());
    lines.join("\n")
}

fn push_prometheus_histogram(
    lines: &mut Vec<String>,
    name: &str,
    help: &str,
    buckets_key: &str,
    sum_key: &str,
    count_key: &str,
    metrics: &serde_json::Value,
) {
    lines.push(format!("# HELP {name} {help}"));
    lines.push(format!("# TYPE {name} histogram"));
    let buckets = metrics
        .get(buckets_key)
        .and_then(serde_json::Value::as_object);
    for (key, le) in [
        ("le_1000", "1000"),
        ("le_5000", "5000"),
        ("le_30000", "30000"),
        ("le_60000", "60000"),
        ("le_300000", "300000"),
        ("le_inf", "+Inf"),
    ] {
        let value = buckets
            .and_then(|buckets| buckets.get(key))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        lines.push(format!("{name}_bucket{{le=\"{le}\"}} {value}"));
    }
    lines.push(format!("{name}_sum {}", metric_value(metrics, sum_key)));
    lines.push(format!("{name}_count {}", metric_value(metrics, count_key)));
}

fn push_prometheus_gauge(
    lines: &mut Vec<String>,
    name: &str,
    key: &str,
    help: &str,
    metrics: &serde_json::Value,
) {
    lines.push(format!("# HELP {name} {help}"));
    lines.push(format!("# TYPE {name} gauge"));
    lines.push(format!("{name} {}", metric_value(metrics, key)));
}

fn metric_value(metrics: &serde_json::Value, key: &str) -> u64 {
    match metrics.get(key) {
        Some(value) if value.is_boolean() => u64::from(value.as_bool().unwrap_or(false)),
        Some(value) => value.as_u64().unwrap_or(0),
        None => 0,
    }
}

fn prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('\n', r"\n")
        .replace('"', r#"\""#)
}

struct DurationMetrics {
    count: u64,
    sum: u64,
    buckets: serde_json::Value,
}

fn run_duration_metrics(os: &OperatingSystem) -> DurationMetrics {
    duration_metrics(os.runs.values().filter_map(|run| {
        let finished_at = run.finished_at?;
        Some((finished_at - run.started_at).num_milliseconds().max(0))
    }))
}

fn task_queue_age_metrics(os: &OperatingSystem) -> DurationMetrics {
    let now = chrono::Utc::now();
    duration_metrics(
        os.tasks
            .values()
            .filter(|task| matches!(task.status, TaskStatus::Pending | TaskStatus::Blocked))
            .map(|task| (now - task.created_at).num_milliseconds().max(0)),
    )
}

fn duration_metrics(durations: impl IntoIterator<Item = i64>) -> DurationMetrics {
    let bucket_limits = [1_000_i64, 5_000, 30_000, 60_000, 300_000];
    let mut durations = durations.into_iter().collect::<Vec<_>>();
    durations.sort_unstable();
    let sum = durations.iter().copied().sum::<i64>().max(0) as u64;
    let mut buckets = serde_json::Map::new();
    for limit in bucket_limits {
        let count = durations
            .iter()
            .filter(|duration| **duration <= limit)
            .count() as u64;
        buckets.insert(format!("le_{limit}"), json!(count));
    }
    buckets.insert("le_inf".into(), json!(durations.len() as u64));
    DurationMetrics {
        count: durations.len() as u64,
        sum,
        buckets: serde_json::Value::Object(buckets),
    }
}

fn oldest_active_run_age_ms(os: &OperatingSystem) -> u64 {
    let now = chrono::Utc::now();
    os.runs
        .values()
        .filter(|run| matches!(run.status, RunStatus::Running | RunStatus::CancelRequested))
        .map(|run| (now - run.started_at).num_milliseconds().max(0) as u64)
        .max()
        .unwrap_or(0)
}

fn oldest_queued_task_age_ms(os: &OperatingSystem) -> u64 {
    let now = chrono::Utc::now();
    os.tasks
        .values()
        .filter(|task| matches!(task.status, TaskStatus::Pending | TaskStatus::Blocked))
        .map(|task| (now - task.created_at).num_milliseconds().max(0) as u64)
        .max()
        .unwrap_or(0)
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
        "oldest_active_run_age_ms": 0,
        "oldest_queued_task_age_ms": 0,
        "task_queue_age_ms_count": 0,
        "task_queue_age_ms_sum": 0,
        "task_queue_age_ms_buckets": {
            "le_1000": 0,
            "le_5000": 0,
            "le_30000": 0,
            "le_60000": 0,
            "le_300000": 0,
            "le_inf": 0
        },
        "run_duration_ms_count": 0,
        "run_duration_ms_sum": 0,
        "run_duration_ms_buckets": {
            "le_1000": 0,
            "le_5000": 0,
            "le_30000": 0,
            "le_60000": 0,
            "le_300000": 0,
            "le_inf": 0
        },
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
        Agent, AgentKind, AgentStatus, ApprovalRequest, EvalRecord, EventKind, MemoryRecord,
        OperatingSystem, Priority, RunArtifact, RunArtifactKind, RunRecord, Task, TaskStatus,
        ToolDefinition, ToolId, ToolInvocation, ToolKind, WorkerNode,
    };
    use chrono::Utc;
    use proptest::prelude::*;
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
    fn git_status_endpoint_reports_workspace_status() {
        let git_available = std::process::Command::new("git")
            .arg("--version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false);
        if !git_available {
            return;
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let init = std::process::Command::new("git")
            .arg("init")
            .current_dir(dir.path())
            .output()
            .expect("git init");
        assert!(
            init.status.success(),
            "{}",
            String::from_utf8_lossy(&init.stderr)
        );
        fs::write(dir.path().join("note.txt"), "hello\n").expect("write file");

        let cwd = url::form_urlencoded::byte_serialize(dir.path().to_string_lossy().as_bytes())
            .collect::<String>();
        let os = OperatingSystem::new("api-test");
        let (status, body) = response_for_path(&os, &format!("/git/status?cwd={cwd}"));
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(
            value["command"],
            serde_json::json!(["git", "status", "--short", "--branch"])
        );
        assert_eq!(value["dry_run"], false);
        assert_eq!(value["status"], 0);
        assert!(
            value["stdout"]
                .as_str()
                .expect("stdout")
                .contains("note.txt")
        );
    }

    #[test]
    fn git_review_task_endpoint_creates_review_task() {
        let git_available = std::process::Command::new("git")
            .arg("--version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false);
        if !git_available {
            return;
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let repo = dir.path().join("repo");
        fs::create_dir_all(&repo).expect("repo dir");
        let init = std::process::Command::new("git")
            .arg("init")
            .current_dir(&repo)
            .output()
            .expect("git init");
        assert!(
            init.status.success(),
            "{}",
            String::from_utf8_lossy(&init.stderr)
        );

        let store = Store::new(dir.path().join("state.json"));
        store
            .save(&OperatingSystem::new("api-test"))
            .expect("save state");
        let body = serde_json::to_vec(&json!({
            "cwd": repo,
            "base": "main",
            "title": "API git review",
            "priority": "high"
        }))
        .expect("review request");

        let (status, body) = response_for_mutation(&store, "POST", "/git/review-task", &body);
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "201 Created");
        assert_eq!(value["task"]["title"], "API git review");
        assert_eq!(value["task"]["priority"], "high");
        assert_eq!(value["task"]["required_capabilities"][0], "review");
        assert_eq!(
            value["task"]["cwd"],
            fs::canonicalize(&repo)
                .expect("canonical repo")
                .display()
                .to_string()
        );
        assert!(
            value["task"]["command"]
                .as_str()
                .expect("review command")
                .contains("git diff --stat main...HEAD")
        );

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/git/review-task",
            br#"{"base":"bad branch"}"#,
        );
        let invalid: serde_json::Value = serde_json::from_str(&body).expect("invalid json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            invalid["error"],
            "base contains characters git refs cannot safely use"
        );
    }

    #[test]
    fn dashboard_endpoint_includes_workers_and_evals() {
        let mut os = OperatingSystem::new("api-test");
        os.workers.insert(
            "remote-a".into(),
            WorkerNode {
                id: "remote-a".into(),
                endpoint: "http://127.0.0.1:9000".into(),
                status: AgentStatus::Online,
                last_seen_at: Utc::now(),
            },
        );
        os.evals.push(EvalRecord {
            id: "eval-test".into(),
            target: "ci-fix".into(),
            success: true,
            cost_micros: Some(123),
            latency_ms: Some(456),
            run: None,
            recorded_at: Utc::now(),
        });

        let (status, body) = response_for_path(&os, "/dashboard");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(value["workers"][0]["id"], "remote-a");
        assert_eq!(value["evals"][0]["id"], "eval-test");
    }

    #[test]
    fn dashboard_html_endpoint_renders_operator_sections() {
        let mut os = OperatingSystem::new("api-test");
        os.register_agent(Agent::new(
            "Dashboard agent",
            AgentKind::Builder,
            Some("planner".into()),
            vec!["ui".into()],
            2,
        ));
        let task = Task::new(
            "Build dashboard",
            "Render operator UI",
            Priority::High,
            vec!["ui".into()],
        );
        let task_id = task.id.clone();
        os.create_task(task);
        let review = Task::new(
            "Review dashboard",
            "Review operator UI",
            Priority::High,
            vec![],
        );
        let review_id = review.id.clone();
        os.create_task(review);
        if let Some(review) = os.tasks.get_mut(&review_id) {
            review.dependencies.push(task_id.clone());
        }
        os.register_tool(ToolDefinition::new(
            "Dashboard writer",
            ToolKind::FileWrite,
            "Write dashboard notes",
            vec!["rust".into()],
            "notes/{name}.txt",
            Some("workspace".into()),
        ));
        os.create_workflow(Workflow::new(
            "Dashboard DAG",
            Priority::High,
            BTreeMap::from([
                ("build".into(), task_id.clone()),
                ("review".into(), review_id),
            ]),
        ));
        os.request_approval(ApprovalRequest::new(
            task_id.clone(),
            None,
            "deploy production",
            "requires operator review",
        ));
        os.workers.insert(
            "remote-dashboard".into(),
            WorkerNode {
                id: "remote-dashboard".into(),
                endpoint: "http://127.0.0.1:9200".into(),
                status: AgentStatus::Online,
                last_seen_at: Utc::now(),
            },
        );
        os.evals.push(EvalRecord {
            id: "eval-dashboard".into(),
            target: "dashboard-smoke".into(),
            success: false,
            cost_micros: None,
            latency_ms: Some(42),
            run: None,
            recorded_at: Utc::now(),
        });
        let mut run = RunRecord::new(task_id, None, "printf dashboard", ".");
        run.artifacts
            .push(RunArtifact::new(RunArtifactKind::Stdout, "stdout.log"));
        os.runs.insert(run.id.clone(), run);
        os.write_memory(MemoryRecord::new(
            "release context",
            "dashboard should show memory",
            vec!["release".into()],
        ));
        os.policy.autonomy = AutonomyLevel::ExecuteWithApproval;
        os.policy.allow_shell = false;
        os.policy.allowed_workspaces = vec![".".into(), "src".into()];
        os.policy.rules = vec!["deny writes outside src".into()];
        os.memory_policy.semantic_recall = true;
        os.memory_policy.scope = Some("dashboard".into());
        os.provider.kind = ProviderKind::Plugin;
        os.provider.model = "planner-plugin".into();
        os.provider.plugin_command = Some("agent-os-provider-plugin".into());
        os.provider.plugin_args = vec!["--mode".into(), "strict".into()];
        os.provider.plugin_env = BTreeMap::from([("PLUGIN_TOKEN".into(), "env:token".into())]);
        os.provider.request_options = BTreeMap::from([("temperature".into(), json!(0.1))]);
        os.provider.response_schema = Some(json!({
            "type": "object",
            "required": ["summary"]
        }));
        os.mcp_servers.insert(
            "local-tools".into(),
            McpServer {
                id: "local-tools".into(),
                command: "agent-os-mcp".into(),
                args: vec!["--stdio".into()],
                env: BTreeMap::from([("AGENT_OS_TOKEN".into(), "test-token".into())]),
                enabled: true,
            },
        );

        let (status, body) = response_for_path(&os, "/dashboard.html");

        assert_eq!(status, "200 OK");
        assert_eq!(
            response_content_type("GET", "/dashboard.html", status),
            "text/html; charset=utf-8"
        );
        assert!(body.contains("<h1>api-test</h1>"), "{body}");
        for section in [
            "Metrics",
            "Agents",
            "Tasks",
            "Tools",
            "Runs",
            "Scheduler &amp; Daemon",
            "Workflows",
            "Workflow DAG Editor",
            "Registry Templates",
            "MCP Servers",
            "Git Workspace",
            "Service Definitions",
            "State Maintenance",
            "Policy Posture",
            "Provider Boundary",
            "Approvals",
            "Workers",
            "Evals",
            "Run Inspector",
            "Run Artifacts",
            "Secrets",
            "Memory",
            "Timeline",
        ] {
            assert!(body.contains(section), "{section} missing from dashboard");
        }
        assert!(body.contains("Dashboard agent"), "{body}");
        assert!(body.contains("data-endpoint=\"/agents\""), "{body}");
        assert!(
            body.contains("data-endpoint-template=\"/agents/{agent_id}\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint-template=\"/agents/{agent_id}/heartbeat\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint-template=\"/agents/{agent_id}/claim\""),
            "{body}"
        );
        assert!(body.contains("Create Agent"), "{body}");
        assert!(body.contains("Inspect Agent"), "{body}");
        assert!(body.contains("Update Agent"), "{body}");
        assert!(body.contains("Heartbeat Agent"), "{body}");
        assert!(body.contains("Claim Agent Task"), "{body}");
        assert!(body.contains("Remove Agent"), "{body}");
        assert!(body.contains("name=\"agent_id\""), "{body}");
        assert!(body.contains("name=\"clear_model\""), "{body}");
        assert!(body.contains("Build dashboard"), "{body}");
        assert!(body.contains("data-endpoint=\"/tasks\""), "{body}");
        assert!(
            body.contains("data-endpoint-template=\"/tasks/{task_id}\""),
            "{body}"
        );
        for endpoint in [
            "/assign",
            "/priority",
            "/dependencies",
            "/plan",
            "/complete",
            "/fail",
            "/block",
            "/cancel",
            "/retry",
            "/unblock",
        ] {
            assert!(
                body.contains(&format!(
                    "data-endpoint-template=\"/tasks/{{task_id}}{endpoint}\""
                )),
                "{endpoint} missing from task dashboard controls: {body}"
            );
        }
        assert!(body.contains("data-endpoint=\"/tasks/recover\""), "{body}");
        assert!(body.contains("Create Task"), "{body}");
        assert!(body.contains("Inspect Task"), "{body}");
        assert!(body.contains("Update Task"), "{body}");
        assert!(body.contains("Assign Task"), "{body}");
        assert!(body.contains("Set Priority"), "{body}");
        assert!(body.contains("Set Dependencies"), "{body}");
        assert!(body.contains("Set Plan"), "{body}");
        assert!(body.contains("Complete Task"), "{body}");
        assert!(body.contains("Retry Task"), "{body}");
        assert!(body.contains("Remove Task"), "{body}");
        assert!(body.contains("Recover Tasks"), "{body}");
        assert!(body.contains("name=\"task_id\""), "{body}");
        assert!(body.contains("name=\"max_attempts\""), "{body}");
        assert!(body.contains("name=\"clear_command\""), "{body}");
        assert!(
            body.contains("name=\"clear_required_capabilities\""),
            "{body}"
        );
        assert!(body.contains("data-endpoint=\"/run\""), "{body}");
        assert!(body.contains("data-endpoint=\"/daemon\""), "{body}");
        assert!(body.contains("data-endpoint=\"/daemon/stop\""), "{body}");
        assert!(body.contains("Run Scheduler"), "{body}");
        assert!(body.contains("Inspect Daemon"), "{body}");
        assert!(body.contains("Request Stop"), "{body}");
        assert!(body.contains("name=\"dry_run\""), "{body}");
        assert!(body.contains("name=\"recover_stale_seconds\""), "{body}");
        assert!(body.contains("not-started"), "{body}");
        assert!(body.contains("Dashboard writer"), "{body}");
        assert!(body.contains("notes/{name}.txt"), "{body}");
        assert!(body.contains("data-endpoint=\"/tools\""), "{body}");
        assert!(
            body.contains("data-endpoint-template=\"/tools/{tool_id}\""),
            "{body}"
        );
        assert!(body.contains("Create Tool"), "{body}");
        assert!(body.contains("Inspect Tool"), "{body}");
        assert!(body.contains("Update Tool"), "{body}");
        assert!(body.contains("Remove Tool"), "{body}");
        assert!(body.contains("name=\"tool_id\""), "{body}");
        assert!(body.contains("name=\"command_template\""), "{body}");
        assert!(
            body.contains("name=\"clear_required_capabilities\""),
            "{body}"
        );
        assert!(body.contains("name=\"clear_cwd\""), "{body}");
        assert!(body.contains("build -&gt; review"), "{body}");
        assert!(body.contains("data-dashboard-form"), "{body}");
        assert!(body.contains("/workflows/"), "{body}");
        assert!(body.contains("/tasks"), "{body}");
        assert!(body.contains("/link"), "{body}");
        assert!(body.contains("/unlink"), "{body}");
        assert!(
            body.contains("data-endpoint=\"/registry/templates/"),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint=\"/registry/marketplace-import\""),
            "{body}"
        );
        assert!(body.contains("Agent Profiles"), "{body}");
        assert!(
            body.contains("data-endpoint=\"/registry/profiles/"),
            "{body}"
        );
        assert!(body.contains("/agents\""), "{body}");
        assert!(body.contains("Install Agent"), "{body}");
        assert!(body.contains("data-json=\"true\""), "{body}");
        assert!(body.contains("Import Marketplace"), "{body}");
        assert!(body.contains("Workflow Templates"), "{body}");
        assert!(body.contains("/workflows\""), "{body}");
        assert!(body.contains("Create Workflow"), "{body}");
        assert!(
            body.contains("data-endpoint=\"/registry/mcp-servers\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint=\"/registry/mcp-servers/local-tools\""),
            "{body}"
        );
        assert!(body.contains("Register MCP Server"), "{body}");
        assert!(body.contains("Update MCP"), "{body}");
        assert!(body.contains("Remove MCP"), "{body}");
        assert!(body.contains("data-method=\"DELETE\""), "{body}");
        assert!(body.contains("AGENT_OS_TOKEN"), "{body}");
        assert!(
            body.contains("data-endpoint=\"/git/review-task\""),
            "{body}"
        );
        assert!(body.contains("data-dashboard-query"), "{body}");
        assert!(body.contains("data-endpoint=\"/git/status\""), "{body}");
        assert!(body.contains("Inspect Status"), "{body}");
        assert!(body.contains("Review Task"), "{body}");
        assert!(body.contains("Create Review Task"), "{body}");
        assert!(
            body.contains("data-endpoint=\"/service/launchd\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint=\"/service/launchd/install\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint=\"/service/launchd/uninstall\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint=\"/service/launchd/start\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint=\"/service/launchd/stop\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint=\"/service/launchd/status\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint=\"/service/systemd\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint=\"/service/systemd/install\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint=\"/service/systemd/uninstall\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint=\"/service/systemd/start\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint=\"/service/systemd/stop\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint=\"/service/systemd/status\""),
            "{body}"
        );
        assert!(body.contains("data-dashboard-result=\"json\""), "{body}");
        assert!(body.contains("Render Launchd"), "{body}");
        assert!(body.contains("Install Launchd"), "{body}");
        assert!(body.contains("Uninstall Launchd"), "{body}");
        assert!(body.contains("Start Launchd"), "{body}");
        assert!(body.contains("Stop Launchd"), "{body}");
        assert!(body.contains("Launchd Status"), "{body}");
        assert!(body.contains("Render Systemd"), "{body}");
        assert!(body.contains("Install Systemd"), "{body}");
        assert!(body.contains("Uninstall Systemd"), "{body}");
        assert!(body.contains("Start Systemd"), "{body}");
        assert!(body.contains("Stop Systemd"), "{body}");
        assert!(body.contains("Systemd Status"), "{body}");
        assert!(body.contains("name=\"recover_stale_seconds\""), "{body}");
        assert!(body.contains("name=\"no_logs\""), "{body}");
        assert!(body.contains("name=\"launchctl_path\""), "{body}");
        assert!(body.contains("name=\"systemctl_path\""), "{body}");
        assert!(body.contains("name=\"unit_path\""), "{body}");
        assert!(body.contains("data-endpoint=\"/state/validate\""), "{body}");
        assert!(body.contains("data-endpoint=\"/state/export\""), "{body}");
        assert!(body.contains("data-endpoint=\"/state/import\""), "{body}");
        assert!(body.contains("data-endpoint=\"/state/migrate\""), "{body}");
        assert!(body.contains("data-endpoint=\"/state/sqlite\""), "{body}");
        assert!(body.contains("data-endpoint=\"/state/backup\""), "{body}");
        assert!(body.contains("data-endpoint=\"/state/repair\""), "{body}");
        assert!(body.contains("data-endpoint=\"/state/prune\""), "{body}");
        assert!(body.contains("Validate State"), "{body}");
        assert!(body.contains("Read Snapshot"), "{body}");
        assert!(body.contains("Export State"), "{body}");
        assert!(body.contains("Import State"), "{body}");
        assert!(body.contains("Migrate State"), "{body}");
        assert!(body.contains("SQLite Mirror"), "{body}");
        assert!(body.contains("Sync SQLite"), "{body}");
        assert!(body.contains("Backup State"), "{body}");
        assert!(body.contains("Repair State"), "{body}");
        assert!(body.contains("Prune State"), "{body}");
        assert!(body.contains("name=\"path\""), "{body}");
        assert!(body.contains("name=\"input\""), "{body}");
        assert!(body.contains("name=\"init_only\""), "{body}");
        assert!(body.contains("name=\"restore\""), "{body}");
        assert!(body.contains("name=\"keep_runs\""), "{body}");
        assert!(body.contains("name=\"keep_events\""), "{body}");
        assert!(body.contains("execute-with-approval"), "{body}");
        assert!(body.contains("Shell disabled"), "{body}");
        assert!(body.contains("Workspace jail enabled"), "{body}");
        assert!(body.contains("semantic recall"), "{body}");
        assert!(body.contains("deny writes outside src"), "{body}");
        assert!(
            body.contains("data-endpoint=\"/config/validate\""),
            "{body}"
        );
        assert!(body.contains("data-endpoint=\"/config\""), "{body}");
        assert!(body.contains("Inspect Config"), "{body}");
        assert!(body.contains("Validate Config"), "{body}");
        assert!(body.contains("Write Config Profile"), "{body}");
        assert!(body.contains("Write Config"), "{body}");
        assert!(body.contains("name=\"profile\""), "{body}");
        assert!(body.contains("name=\"force\""), "{body}");
        assert!(body.contains("autonomous"), "{body}");
        assert!(body.contains("ci"), "{body}");
        assert!(body.contains("planner-plugin"), "{body}");
        assert!(body.contains("agent-os-provider-plugin"), "{body}");
        assert!(body.contains("schema configured"), "{body}");
        assert!(body.contains("temperature"), "{body}");
        assert!(body.contains("PLUGIN_TOKEN"), "{body}");
        assert!(body.contains("data-list=\"true\""), "{body}");
        assert!(body.contains("deploy production"), "{body}");
        assert!(body.contains("data-endpoint=\"/approvals/"), "{body}");
        assert!(body.contains("/approve\""), "{body}");
        assert!(body.contains("/deny\""), "{body}");
        assert!(body.contains("name=\"by\""), "{body}");
        assert!(body.contains("Approve"), "{body}");
        assert!(body.contains("Deny"), "{body}");
        assert!(body.contains("data-endpoint=\"/workers\""), "{body}");
        assert!(body.contains("Register Worker"), "{body}");
        assert!(
            body.contains("data-endpoint-template=\"/workers/{worker_id}/heartbeat\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint-template=\"/workers/{worker_id}/claim\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint-template=\"/workers/{worker_id}/report\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint-template=\"/workers/{worker_id}\""),
            "{body}"
        );
        assert!(body.contains("Heartbeat Worker"), "{body}");
        assert!(body.contains("Claim Task"), "{body}");
        assert!(body.contains("Report Task"), "{body}");
        assert!(body.contains("Remove Worker"), "{body}");
        assert!(body.contains("name=\"worker_id\""), "{body}");
        assert!(body.contains("name=\"lease_seconds\""), "{body}");
        assert!(body.contains("name=\"artifacts\""), "{body}");
        assert!(body.contains("data-endpoint=\"/evals\""), "{body}");
        assert!(body.contains("data-endpoint=\"/evals/run\""), "{body}");
        assert!(body.contains("Record Eval"), "{body}");
        assert!(body.contains("Run Eval"), "{body}");
        assert!(body.contains("name=\"success_pattern\""), "{body}");
        assert!(body.contains("name=\"output_schema\""), "{body}");
        assert!(body.contains("data-bool=\"true\""), "{body}");
        assert!(body.contains("data-number=\"true\""), "{body}");
        assert!(body.contains("remote-dashboard"), "{body}");
        assert!(body.contains("dashboard-smoke"), "{body}");
        assert!(
            body.contains("data-endpoint-template=\"/runs/{run_id}/debug\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint-template=\"/runs/{run_id}/replay\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint-template=\"/runs/{run_id}/artifacts\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint-template=\"/runs/{run_id}/artifacts/{artifact_id}\""),
            "{body}"
        );
        assert!(
            body.contains("data-endpoint-template=\"/runs/{run_id}/cancel\""),
            "{body}"
        );
        assert!(body.contains("data-path=\"true\""), "{body}");
        assert!(body.contains("Debug Run"), "{body}");
        assert!(body.contains("Replay Run"), "{body}");
        assert!(body.contains("List Artifacts"), "{body}");
        assert!(body.contains("Read Artifact"), "{body}");
        assert!(body.contains("Cancel Run"), "{body}");
        assert!(body.contains("name=\"artifact_id\""), "{body}");
        assert!(body.contains("stdout.log"), "{body}");
        assert!(body.contains("data-endpoint=\"/secrets\""), "{body}");
        assert!(body.contains("Register Secret Backend"), "{body}");
        assert!(
            body.contains("data-endpoint-template=\"/secrets/{backend_id}\""),
            "{body}"
        );
        assert!(body.contains("Inspect Backend"), "{body}");
        assert!(body.contains("Remove Backend"), "{body}");
        assert!(body.contains("name=\"backend_id\""), "{body}");
        assert!(body.contains("data-endpoint=\"/secrets/check\""), "{body}");
        assert!(body.contains("Check Secrets"), "{body}");
        assert!(body.contains("data-endpoint=\"/memory\""), "{body}");
        assert!(body.contains("Add Memory"), "{body}");
        assert!(body.contains("data-endpoint=\"/memory/recall\""), "{body}");
        assert!(
            body.contains("data-endpoint-template=\"/memory/{memory_id}\""),
            "{body}"
        );
        assert!(body.contains("data-endpoint=\"/memory/prune\""), "{body}");
        assert!(body.contains("Recall Memory"), "{body}");
        assert!(body.contains("Inspect Memory"), "{body}");
        assert!(body.contains("Update Memory"), "{body}");
        assert!(body.contains("Prune Memory"), "{body}");
        assert!(body.contains("Remove Memory"), "{body}");
        assert!(body.contains("name=\"memory_id\""), "{body}");
        assert!(body.contains("name=\"clear_tags\""), "{body}");
        assert!(body.contains("name=\"clear_scope\""), "{body}");
        assert!(body.contains("release context"), "{body}");
    }

    #[test]
    fn metrics_reports_run_duration_histogram() {
        let mut os = OperatingSystem::new("api-test");
        let task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);
        let mut fast = RunRecord::new(task_id.clone(), None, "fast", ".");
        fast.status = RunStatus::Success;
        fast.exit_code = Some(0);
        fast.finished_at = Some(fast.started_at + chrono::Duration::milliseconds(750));
        let mut slow = RunRecord::new(task_id, None, "slow", ".");
        slow.status = RunStatus::Failed;
        slow.exit_code = Some(1);
        slow.finished_at = Some(slow.started_at + chrono::Duration::milliseconds(45_000));
        os.runs.insert(fast.id.clone(), fast);
        os.runs.insert(slow.id.clone(), slow);

        let metrics = metrics_json(&os);

        assert_eq!(metrics["run_duration_ms_count"], 2);
        assert_eq!(metrics["run_duration_ms_sum"], 45_750);
        assert_eq!(metrics["run_duration_ms_buckets"]["le_1000"], 1);
        assert_eq!(metrics["run_duration_ms_buckets"]["le_30000"], 1);
        assert_eq!(metrics["run_duration_ms_buckets"]["le_60000"], 2);
        assert_eq!(metrics["run_duration_ms_buckets"]["le_inf"], 2);
    }

    #[test]
    fn metrics_reports_task_queue_age_histogram() {
        let mut os = OperatingSystem::new("api-test");
        let mut fresh = Task::new("Fresh", "Objective", Priority::Normal, vec![]);
        fresh.created_at = chrono::Utc::now() - chrono::Duration::milliseconds(750);
        let mut stale = Task::new("Stale", "Objective", Priority::Normal, vec![]);
        stale.status = TaskStatus::Blocked;
        stale.created_at = chrono::Utc::now() - chrono::Duration::milliseconds(45_000);
        let mut finished = Task::new("Finished", "Objective", Priority::Normal, vec![]);
        finished.status = TaskStatus::Complete;
        finished.created_at = chrono::Utc::now() - chrono::Duration::milliseconds(300_000);
        os.create_task(fresh);
        os.create_task(stale);
        os.create_task(finished);

        let metrics = metrics_json(&os);

        assert_eq!(metrics["task_queue_age_ms_count"], 2);
        assert!(metrics["task_queue_age_ms_sum"].as_u64().unwrap() >= 45_750);
        assert!(metrics["oldest_queued_task_age_ms"].as_u64().unwrap() >= 45_000);
        assert_eq!(metrics["task_queue_age_ms_buckets"]["le_1000"], 1);
        assert_eq!(metrics["task_queue_age_ms_buckets"]["le_30000"], 1);
        assert_eq!(metrics["task_queue_age_ms_buckets"]["le_60000"], 2);
        assert_eq!(metrics["task_queue_age_ms_buckets"]["le_inf"], 2);
    }

    #[test]
    fn metrics_reports_oldest_active_run_age() {
        let mut os = OperatingSystem::new("api-test");
        let task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);
        let mut active = RunRecord::new(task_id.clone(), None, "active", ".");
        active.started_at = chrono::Utc::now() - chrono::Duration::milliseconds(2_500);
        let mut cancelling = RunRecord::new(task_id.clone(), None, "cancelling", ".");
        cancelling.status = RunStatus::CancelRequested;
        cancelling.started_at = chrono::Utc::now() - chrono::Duration::milliseconds(5_000);
        let mut finished = RunRecord::new(task_id, None, "finished", ".");
        finished.status = RunStatus::Success;
        finished.started_at = chrono::Utc::now() - chrono::Duration::milliseconds(30_000);
        finished.finished_at = Some(finished.started_at + chrono::Duration::milliseconds(1_000));
        os.runs.insert(active.id.clone(), active);
        os.runs.insert(cancelling.id.clone(), cancelling);
        os.runs.insert(finished.id.clone(), finished);

        let metrics = metrics_json(&os);

        assert!(metrics["oldest_active_run_age_ms"].as_u64().unwrap() >= 5_000);
    }

    #[test]
    fn prometheus_metrics_endpoint_reports_counter_snapshot() {
        let mut os = OperatingSystem::new("api-test");
        os.create_task(Task::new(
            "Task",
            "Objective",
            Priority::Normal,
            vec!["plan".into()],
        ));

        let (status, body) = response_for_path(&os, "/metrics/prometheus");

        assert_eq!(status, "200 OK");
        assert!(body.contains("# TYPE agent_os_tasks_total gauge"));
        assert!(body.contains("agent_os_tasks_total 1"));
        assert!(body.contains("# TYPE agent_os_oldest_active_run_age_ms gauge"));
        assert!(body.contains("# TYPE agent_os_oldest_queued_task_age_ms gauge"));
        assert!(body.contains("agent_os_task_queue_age_ms_bucket{le=\"+Inf\"} 1"));
        assert!(body.contains("agent_os_run_duration_ms_bucket{le=\"+Inf\"} 0"));
        assert!(body.contains("agent_os_build_info{version=\""));
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
        os.write_memory(MemoryRecord::with_access(
            "Private Rust",
            "Scheduler notes",
            vec!["OpsTag".into()],
            MemoryVisibility::Private,
            Some("client-a".into()),
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
        assert_eq!(records.as_array().expect("records").len(), 3);
        assert!(
            records
                .as_array()
                .expect("records")
                .iter()
                .any(|record| record["topic"] == "Latest Rust")
        );
        assert!(
            records
                .as_array()
                .expect("records")
                .iter()
                .any(|record| record["topic"] == "Rust")
        );

        let (status, body) = response_for_path(
            &os,
            "/memory/recall?query=scheduler&tag=opsTag&visibility=shared&limit=1",
        );
        let recall: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(recall.as_array().expect("recall").len(), 1);
        assert_eq!(recall[0]["record"]["topic"], "Latest Rust");
        assert!(recall[0]["score"].as_u64().expect("score") > 0);
        assert!(
            recall[0]["snippet"]
                .as_str()
                .expect("snippet")
                .contains("Scheduler")
        );

        let (status, body) = response_for_path(&os, "/memory/recall");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "query is required for memory recall");

        let (status, body) = response_for_path(&os, "/memory?visibility=private&scope=client-a");
        let records: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(records.as_array().expect("records").len(), 1);
        assert_eq!(records[0]["topic"], "Private Rust");

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

        let (status, body) = response_for_path(&os, "/memory?visibility=secret");
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(value["error"], "visibility must be shared or private");

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
            br#"{"visibility":"private","scope":"client-a"}"#,
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(value["memory"]["visibility"], "private");
        assert_eq!(value["memory"]["scope"], "client-a");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/memory/{memory_id}"),
            br#"{"clear_scope":true}"#,
        );
        let value: serde_json::Value = serde_json::from_str(&body).expect("json");

        assert_eq!(status, "200 OK");
        assert_eq!(value["memory"]["scope"], serde_json::Value::Null);

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
    fn malformed_request_lines_are_rejected_by_parser_boundary() {
        for request in [
            b"\r\nhost: localhost\r\n\r\n".as_slice(),
            b"GET status HTTP/1.1\r\nhost: localhost\r\n\r\n".as_slice(),
            b"G ET /status HTTP/1.1\r\nhost: localhost\r\n\r\n".as_slice(),
            b"GET /status HTTP/2\r\nhost: localhost\r\n\r\n".as_slice(),
        ] {
            let mut reader = Cursor::new(request);

            let error = match read_http_request(&mut reader) {
                Ok(_) => panic!("malformed request line should fail"),
                Err(error) => error,
            };

            assert!(matches!(
                error,
                HttpRequestError::MissingRequestLine
                    | HttpRequestError::MalformedRequestLine { .. }
                    | HttpRequestError::UnsupportedHttpVersion { .. }
            ));
        }
    }

    #[test]
    fn non_utf8_http_headers_are_rejected() {
        let mut reader = Cursor::new(b"GET /status HTTP/1.1\r\nx-binary: \xFF\r\n\r\n".as_slice());

        let error = match read_http_request(&mut reader) {
            Ok(_) => panic!("non-utf8 headers should fail"),
            Err(error) => error,
        };

        assert!(matches!(error, HttpRequestError::InvalidHeaderUtf8));
    }

    proptest! {
        #[test]
        fn http_parser_handles_arbitrary_bounded_bytes_without_panicking(bytes in proptest::collection::vec(any::<u8>(), 0..8192)) {
            let mut reader = Cursor::new(bytes);

            match read_http_request(&mut reader) {
                Ok(request) => {
                    prop_assert!(request.headers.len() <= MAX_HTTP_HEADER_BYTES);
                    prop_assert!(request.body.len() <= MAX_HTTP_BODY_BYTES);
                    prop_assert!(parse_http_request_line(request.request_line.as_deref()).is_ok());
                }
                Err(error) => {
                    let (status, body) = http_request_error_response(&error);
                    prop_assert!(
                        matches!(
                            status,
                            "400 Bad Request"
                                | "413 Payload Too Large"
                                | "431 Request Header Fields Too Large"
                                | "500 Internal Server Error"
                        ),
                        "unexpected status: {status}"
                    );
                    prop_assert!(body.contains("\"error\":\"invalid http request\""));
                }
            }
        }

        #[test]
        fn http_parser_round_trips_generated_valid_requests(
            path in "/[A-Za-z0-9_./-]{0,64}",
            body in proptest::collection::vec(any::<u8>(), 0..4096),
            host in "[A-Za-z0-9.-]{1,64}"
        ) {
            let mut request = format!(
                "POST {path} HTTP/1.1\r\nhost: {host}\r\ncontent-length: {}\r\ncontent-type: application/json\r\n\r\n",
                body.len()
            ).into_bytes();
            request.extend_from_slice(&body);
            request.extend_from_slice(b"ignored trailing bytes");
            let mut reader = Cursor::new(request);

            let parsed = read_http_request(&mut reader).expect("generated request should parse");
            let (method, parsed_path) = parse_http_request_line(parsed.request_line.as_deref())
                .expect("generated request line should parse");

            prop_assert_eq!(method, "POST");
            prop_assert_eq!(parsed_path, path.as_str());
            prop_assert_eq!(parsed.body, body);
        }
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
    fn declared_oversized_bodies_are_rejected_without_body_bytes() {
        let request = format!(
            "POST /memory HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n",
            MAX_HTTP_BODY_BYTES + 1
        );
        let mut reader = Cursor::new(request.into_bytes());

        let error = match read_http_request(&mut reader) {
            Ok(_) => panic!("oversized declared body should fail"),
            Err(error) => error,
        };

        assert!(matches!(error, HttpRequestError::PayloadTooLarge { .. }));
    }

    #[test]
    fn duplicate_matching_content_length_headers_are_rejected() {
        let request =
            b"POST /memory HTTP/1.1\r\nhost: localhost\r\ncontent-length: 2\r\ncontent-length: 2\r\n\r\n{}";
        let mut reader = Cursor::new(request.as_slice());

        let error = match read_http_request(&mut reader) {
            Ok(_) => panic!("duplicate content-length headers should fail"),
            Err(error) => error,
        };

        assert!(matches!(error, HttpRequestError::DuplicateContentLength));
    }

    #[test]
    fn transfer_encoding_headers_are_rejected() {
        let request =
            b"POST /memory HTTP/1.1\r\nhost: localhost\r\ntransfer-encoding: chunked\r\n\r\n0\r\n\r\n";
        let mut reader = Cursor::new(request.as_slice());

        let error = match read_http_request(&mut reader) {
            Ok(_) => panic!("unsupported transfer-encoding should fail"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            HttpRequestError::UnsupportedTransferEncoding { .. }
        ));
    }

    #[test]
    fn malformed_header_lines_are_rejected() {
        let mut reader = Cursor::new(b"GET /status HTTP/1.1\r\nhost localhost\r\n\r\n".as_slice());

        let error = match read_http_request(&mut reader) {
            Ok(_) => panic!("malformed header line should fail"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            HttpRequestError::MalformedHeaderLine { .. }
        ));
    }

    #[test]
    fn invalid_header_names_are_rejected() {
        let mut reader =
            Cursor::new(b"GET /status HTTP/1.1\r\nbad header: value\r\n\r\n".as_slice());

        let error = match read_http_request(&mut reader) {
            Ok(_) => panic!("invalid header name should fail"),
            Err(error) => error,
        };

        assert!(matches!(error, HttpRequestError::InvalidHeaderName { .. }));
    }

    #[test]
    fn duplicate_host_headers_are_rejected() {
        let mut reader = Cursor::new(
            b"GET /status HTTP/1.1\r\nhost: localhost\r\nhost: 127.0.0.1\r\n\r\n".as_slice(),
        );

        let error = match read_http_request(&mut reader) {
            Ok(_) => panic!("duplicate host header should fail"),
            Err(error) => error,
        };

        assert!(matches!(error, HttpRequestError::DuplicateHostHeader));
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
    fn state_sqlite_api_syncs_and_restores_mirror() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let sqlite_path = dir.path().join("mirror.sqlite");
        let mut os = OperatingSystem::new("api-test");
        os.create_task(Task::new(
            "Mirror task",
            "Check SQLite sync",
            Priority::Normal,
            vec!["rust".into()],
        ));
        store.save(&os).expect("save state");

        let request = serde_json::json!({
            "output": sqlite_path.display().to_string()
        })
        .to_string();
        let (status, body) =
            response_for_mutation(&store, "POST", "/state/sqlite", request.as_bytes());
        let synced: serde_json::Value = serde_json::from_str(&body).expect("sync json");

        assert_eq!(status, "200 OK");
        assert_eq!(synced["path"], sqlite_path.display().to_string());
        assert_eq!(synced["state_path"], store.path().display().to_string());
        assert_eq!(synced["initialized"], true);
        assert_eq!(synced["imported"], true);
        assert_eq!(synced["import"]["imported_snapshot"], true);
        assert_eq!(synced["import"]["imported_runs"], 0);
        assert!(synced["restore"].is_null());

        let sqlite = SqliteStore::new(&sqlite_path);
        assert_eq!(sqlite.task_record_count().expect("task count"), 1);
        assert_eq!(sqlite.load_snapshot().expect("snapshot").tasks.len(), 1);

        let request = serde_json::json!({
            "output": sqlite_path.display().to_string(),
            "restore": true,
            "force": true,
            "dry_run": true
        })
        .to_string();
        let (status, body) =
            response_for_mutation(&store, "POST", "/state/sqlite", request.as_bytes());
        let restored: serde_json::Value = serde_json::from_str(&body).expect("restore json");

        assert_eq!(status, "200 OK");
        assert_eq!(restored["imported"], false);
        assert_eq!(restored["dry_run"], true);
        assert!(restored["import"].is_null());
        assert_eq!(restored["restore"]["restored_snapshot"], false);
        assert_eq!(restored["restore"]["restored_runs"], 0);
        assert_eq!(restored["restore"]["validation"]["valid"], true);

        let request = serde_json::json!({
            "init_only": true,
            "restore": true
        })
        .to_string();
        let (status, body) =
            response_for_mutation(&store, "POST", "/state/sqlite", request.as_bytes());
        let conflict: serde_json::Value = serde_json::from_str(&body).expect("conflict json");

        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            conflict["error"],
            "init_only cannot be combined with restore"
        );
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
    fn workflow_api_edits_dag_and_transitions_stages() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("api-test");
        let mut plan = Task::new("Plan", "Plan the work", Priority::Normal, vec![]);
        let plan_id = plan.id.clone();
        let mut build = Task::new("Build", "Build the work", Priority::Normal, vec![]);
        build.dependencies.push(plan_id.clone());
        let build_id = build.id.clone();
        os.ensure_unique_task_id(&mut plan);
        os.ensure_unique_task_id(&mut build);
        os.create_task(plan);
        os.create_task(build);
        let workflow = Workflow::new(
            "Ship workflow",
            Priority::Normal,
            BTreeMap::from([
                ("plan".into(), plan_id.clone()),
                ("build".into(), build_id.clone()),
            ]),
        );
        let workflow_id = workflow.id.clone();
        os.create_workflow(workflow);
        store.save(&os).expect("save state");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/workflows/{workflow_id}/tasks"),
            br#"{"stage":"review","title":"Review","dependencies":["build"],"required_capabilities":["review"],"priority":"high"}"#,
        );
        let add: serde_json::Value = serde_json::from_str(&body).expect("add json");
        let review_id = TaskId::from_slug(add["task_id"].as_str().expect("review id"));
        assert_eq!(status, "201 Created");
        assert_eq!(add["stage"], "review");
        assert_eq!(add["task"]["priority"], "high");
        assert_eq!(add["progress"]["total_tasks"], 3);

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/workflows/{workflow_id}/link"),
            br#"{"from":"plan","to":"review"}"#,
        );
        let link: serde_json::Value = serde_json::from_str(&body).expect("link json");
        assert_eq!(status, "200 OK");
        assert_eq!(link["linked"], true);
        assert!(
            store
                .load()
                .expect("load")
                .tasks
                .get(&review_id)
                .expect("review task")
                .dependencies
                .contains(&plan_id)
        );

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/workflows/{workflow_id}/unlink"),
            br#"{"from":"build","to":"review"}"#,
        );
        let unlink: serde_json::Value = serde_json::from_str(&body).expect("unlink json");
        assert_eq!(status, "200 OK");
        assert_eq!(unlink["linked"], false);
        assert!(
            !store
                .load()
                .expect("load")
                .tasks
                .get(&review_id)
                .expect("review task")
                .dependencies
                .contains(&build_id)
        );

        let loaded = store.load().expect("load");
        let (status, body) = response_for_path(&loaded, &format!("/workflows/{workflow_id}/dag"));
        let dag: serde_json::Value = serde_json::from_str(&body).expect("dag json");
        assert_eq!(status, "200 OK");
        assert_eq!(dag["id"], workflow_id.to_string());
        assert_eq!(dag["nodes"].as_array().expect("nodes").len(), 3);
        assert_eq!(dag["edges"].as_array().expect("edges").len(), 2);
        assert!(dag["edges"].as_array().expect("edges").iter().any(|edge| {
            edge["from"] == "plan"
                && edge["to"] == "build"
                && edge["from_task_id"] == plan_id.to_string()
                && edge["to_task_id"] == build_id.to_string()
        }));
        assert!(dag["edges"].as_array().expect("edges").iter().any(|edge| {
            edge["from"] == "plan"
                && edge["to"] == "review"
                && edge["from_task_id"] == plan_id.to_string()
                && edge["to_task_id"] == review_id.to_string()
        }));
        assert_eq!(
            dag["external_dependencies"]
                .as_array()
                .expect("external")
                .len(),
            0
        );
        assert_eq!(dag["progress"]["total_tasks"], 3);

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/workflows/{workflow_id}/pause"),
            b"",
        );
        let pause: serde_json::Value = serde_json::from_str(&body).expect("pause json");
        assert_eq!(status, "200 OK");
        assert_eq!(pause["action"], "paused");
        assert_eq!(
            pause["affected_tasks"].as_array().expect("affected").len(),
            3
        );

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/workflows/{workflow_id}/resume"),
            br#"{"note":"continue"}"#,
        );
        let resume: serde_json::Value = serde_json::from_str(&body).expect("resume json");
        assert_eq!(status, "200 OK");
        assert_eq!(resume["action"], "resumed");
        assert_eq!(
            resume["affected_tasks"].as_array().expect("affected").len(),
            3
        );

        let (status, body) =
            response_for_mutation(&store, "POST", &format!("/tasks/{review_id}/block"), b"");
        assert_eq!(status, "200 OK", "{body}");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/workflows/{workflow_id}/retry"),
            b"",
        );
        let retry: serde_json::Value = serde_json::from_str(&body).expect("retry json");
        assert_eq!(status, "200 OK");
        assert_eq!(retry["action"], "retried");
        assert_eq!(
            retry["affected_tasks"].as_array().expect("affected").len(),
            1
        );

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/workflows/{workflow_id}/link"),
            br#"{"from":"review","to":"review"}"#,
        );
        let invalid: serde_json::Value = serde_json::from_str(&body).expect("invalid json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(invalid["error"], "workflow stage cannot depend on itself");
        assert!(validate_state(&store.load().expect("load")).valid);
    }

    #[test]
    fn registry_api_lists_templates_and_creates_workflow_from_template() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("api-test");
        os.workflow_templates.insert(
            "release".into(),
            crate::models::WorkflowTemplate {
                id: "release".into(),
                name: "Release".into(),
                description: "Release workflow.".into(),
                stages: vec!["plan".into(), "build".into(), "ship".into()],
                tasks: vec![
                    crate::models::WorkflowTemplateTask {
                        stage: "build".into(),
                        title: Some("Build {objective}".into()),
                        objective: Some("Compile {objective}".into()),
                        command: Some("printf build-{objective}".into()),
                        capabilities: vec!["rust".into()],
                    },
                    crate::models::WorkflowTemplateTask {
                        stage: "ship".into(),
                        title: Some("Ship {objective}".into()),
                        objective: Some("Publish {objective}".into()),
                        command: None,
                        capabilities: vec!["ops".into()],
                    },
                ],
                edges: vec![
                    crate::models::WorkflowTemplateEdge {
                        from: "plan".into(),
                        to: "build".into(),
                    },
                    crate::models::WorkflowTemplateEdge {
                        from: "build".into(),
                        to: "ship".into(),
                    },
                ],
            },
        );
        store.save(&os).expect("save state");

        let loaded = store.load().expect("load");
        let (status, body) = response_for_path(&loaded, "/registry/templates");
        let templates: serde_json::Value = serde_json::from_str(&body).expect("templates json");
        assert_eq!(status, "200 OK");
        assert_eq!(templates["ci-fix"]["stages"][0], "reproduce");

        let (status, body) = response_for_path(&loaded, "/registry/templates/ci-fix");
        let template: serde_json::Value = serde_json::from_str(&body).expect("template json");
        assert_eq!(status, "200 OK");
        assert_eq!(template["id"], "ci-fix");
        assert_eq!(template["stages"].as_array().expect("stages").len(), 4);

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/registry/templates/ci-fix/workflows",
            br#"{"objective":"Fix failing CI","priority":"high"}"#,
        );
        let created: serde_json::Value = serde_json::from_str(&body).expect("created json");
        assert_eq!(status, "201 Created");
        assert_eq!(created["template"], "ci-fix");
        assert_eq!(created["workflow"]["objective"], "Fix failing CI");
        assert_eq!(created["workflow"]["priority"], "high");
        assert_eq!(created["tasks"].as_object().expect("tasks").len(), 4);
        assert_eq!(created["dag"]["nodes"].as_array().expect("nodes").len(), 4);
        assert_eq!(created["dag"]["edges"].as_array().expect("edges").len(), 3);
        assert_eq!(created["dag"]["progress"]["total_tasks"], 4);

        let loaded = store.load().expect("load");
        assert_eq!(loaded.workflows.len(), 1);
        assert_eq!(loaded.tasks.len(), 4);
        assert!(validate_state(&loaded).valid);

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/registry/templates/release/workflows",
            br#"{"objective":"v1"}"#,
        );
        let created: serde_json::Value = serde_json::from_str(&body).expect("created release json");
        assert_eq!(status, "201 Created");
        let build_id = created["tasks"]["build"].as_str().expect("build id");
        let ship_id = created["tasks"]["ship"].as_str().expect("ship id");
        let loaded = store.load().expect("load release workflow");
        let build_task_id = TaskId::from_slug(build_id);
        let ship_task_id = TaskId::from_slug(ship_id);
        let build_task = loaded.tasks.get(&build_task_id).expect("build task");
        let ship_task = loaded.tasks.get(&ship_task_id).expect("ship task");
        assert_eq!(build_task.title, "Build v1");
        assert_eq!(build_task.objective, "Compile v1");
        assert_eq!(build_task.command.as_deref(), Some("printf build-v1"));
        assert_eq!(build_task.required_capabilities, vec!["rust".to_owned()]);
        assert_eq!(ship_task.dependencies, vec![build_task_id]);
    }

    #[test]
    fn registry_api_imports_marketplace_manifest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        store
            .save(&OperatingSystem::new("api-test"))
            .expect("save state");
        let manifest = MarketplaceManifest {
            metadata: Some(MarketplaceManifestMetadata {
                id: "api-market".into(),
                version: "1.0.0".into(),
                publisher: Some("Agent OS tests".into()),
                homepage: None,
            }),
            agent_profiles: vec![AgentProfile {
                id: "api-reviewer".into(),
                name: "API Reviewer".into(),
                kind: AgentKind::Reviewer,
                model: Some("review-model".into()),
                capabilities: vec!["review".into()],
                system_prompt: Some("Review imported work.".into()),
            }],
            workflow_templates: vec![WorkflowTemplate {
                id: "api-release".into(),
                name: "API Release".into(),
                description: "Ship from API manifest.".into(),
                stages: vec!["plan".into(), "build".into()],
                tasks: vec![crate::models::WorkflowTemplateTask {
                    stage: "build".into(),
                    title: Some("Build {objective}".into()),
                    objective: Some("Compile {objective}".into()),
                    command: Some("printf build".into()),
                    capabilities: vec!["rust".into()],
                }],
                edges: vec![WorkflowTemplateEdge {
                    from: "plan".into(),
                    to: "build".into(),
                }],
            }],
            mcp_servers: vec![McpServer {
                id: "api-mcp".into(),
                command: "printf".into(),
                args: vec!["{}".into()],
                env: BTreeMap::from([("API_TOKEN".into(), "secret".into())]),
                enabled: true,
            }],
        };
        let checksum = fnv1a64_checksum(&serde_json::to_vec(&manifest).expect("manifest json"));
        let request = serde_json::to_vec(&json!({
            "manifest": manifest,
            "source": "api-test",
            "expect_checksum": checksum
        }))
        .expect("request json");

        let (status, body) =
            response_for_mutation(&store, "POST", "/registry/marketplace-import", &request);
        let imported: serde_json::Value = serde_json::from_str(&body).expect("imported json");
        assert_eq!(status, "201 Created");
        assert_eq!(imported["source"], "api-test");
        assert_eq!(imported["verified_checksum"], true);
        assert_eq!(imported["imported_agent_profiles"], 1);
        assert_eq!(imported["imported_workflow_templates"], 1);
        assert_eq!(imported["imported_mcp_servers"], 1);
        assert_eq!(imported["overwritten"], 0);
        assert_eq!(imported["checksum"], checksum);

        let loaded = store.load().expect("load imported");
        assert_eq!(
            loaded
                .agent_profiles
                .get("api-reviewer")
                .expect("profile")
                .capabilities,
            vec!["review".to_owned()]
        );
        assert!(loaded.workflow_templates.contains_key("api-release"));
        assert!(loaded.mcp_servers.contains_key("api-mcp"));
        assert!(
            loaded
                .events
                .iter()
                .any(|event| event.kind == EventKind::MarketplaceImported)
        );
        assert!(validate_state(&loaded).valid);

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/registry/profiles/api-reviewer/agents",
            br#"{"name":"API Installed Reviewer","model":"override-model","parallel":2}"#,
        );
        let installed: serde_json::Value =
            serde_json::from_str(&body).expect("installed profile agent json");
        assert_eq!(status, "201 Created");
        assert_eq!(installed["profile"], "api-reviewer");
        assert_eq!(installed["agent"]["name"], "API Installed Reviewer");
        assert_eq!(installed["agent"]["model"], "override-model");
        assert_eq!(installed["agent"]["max_parallel_tasks"], 2);
        assert_eq!(installed["agent"]["capabilities"][0], "review");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/registry/profiles/api-reviewer/agents",
            br#"{"name":"API Installed Reviewer"}"#,
        );
        let duplicate_agent: serde_json::Value =
            serde_json::from_str(&body).expect("duplicate installed agent json");
        assert_eq!(status, "409 Conflict");
        assert_eq!(duplicate_agent["error"], "agent already exists");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/registry/profiles/missing/agents",
            br#"{"parallel":1}"#,
        );
        let missing_profile: serde_json::Value =
            serde_json::from_str(&body).expect("missing profile json");
        assert_eq!(status, "404 Not Found");
        assert_eq!(missing_profile["error"], "agent profile not found");

        let loaded = store.load().expect("load installed profile agent");
        assert!(
            loaded
                .agents
                .contains_key(&AgentId::new("api-installed-reviewer"))
        );
        assert!(validate_state(&loaded).valid);

        let (status, body) =
            response_for_mutation(&store, "POST", "/registry/marketplace-import", &request);
        let duplicate: serde_json::Value = serde_json::from_str(&body).expect("duplicate json");
        assert_eq!(status, "409 Conflict");
        assert_eq!(duplicate["error"], "agent profile already exists");

        let forced_request: serde_json::Value =
            serde_json::from_slice(&request).expect("forced request value");
        let mut forced_request = forced_request.as_object().expect("request object").clone();
        forced_request.insert("force".into(), json!(true));
        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/registry/marketplace-import",
            &serde_json::to_vec(&forced_request).expect("forced request json"),
        );
        let forced: serde_json::Value = serde_json::from_str(&body).expect("forced json");
        assert_eq!(status, "201 Created");
        assert_eq!(forced["overwritten"], 3);

        let bad_checksum_request = serde_json::to_vec(&json!({
            "manifest": forced_request["manifest"],
            "expect_checksum": "fnv1a64:0000000000000000"
        }))
        .expect("bad checksum request json");
        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/registry/marketplace-import",
            &bad_checksum_request,
        );
        let mismatch: serde_json::Value = serde_json::from_str(&body).expect("mismatch json");
        assert_eq!(status, "409 Conflict");
        assert_eq!(mismatch["error"], "marketplace checksum mismatch");
    }

    #[test]
    fn registry_api_manages_mcp_servers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        store
            .save(&OperatingSystem::new("api-test"))
            .expect("save state");

        let request = serde_json::json!({
            "id": "local-tools",
            "command": "agent-os-mcp",
            "args": ["--stdio"],
            "env": { "AGENT_OS_TOKEN": "test-token" },
            "enabled": false
        })
        .to_string();
        let (status, body) =
            response_for_mutation(&store, "POST", "/registry/mcp-servers", request.as_bytes());
        let created: serde_json::Value = serde_json::from_str(&body).expect("created json");

        assert_eq!(status, "201 Created");
        assert_eq!(created["id"], "local-tools");
        assert_eq!(created["mcp_server"]["command"], "agent-os-mcp");
        assert_eq!(created["mcp_server"]["args"][0], "--stdio");
        assert_eq!(created["mcp_server"]["env"]["AGENT_OS_TOKEN"], "test-token");
        assert_eq!(created["mcp_server"]["enabled"], false);

        let (status, body) =
            response_for_mutation(&store, "POST", "/registry/mcp-servers", request.as_bytes());
        let duplicate: serde_json::Value = serde_json::from_str(&body).expect("duplicate json");
        assert_eq!(status, "409 Conflict");
        assert_eq!(duplicate["error"], "mcp server already exists");

        let request = serde_json::json!({
            "enabled": true,
            "args": [],
            "env": {}
        })
        .to_string();
        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/registry/mcp-servers/local-tools",
            request.as_bytes(),
        );
        let updated: serde_json::Value = serde_json::from_str(&body).expect("updated json");

        assert_eq!(status, "200 OK");
        assert_eq!(updated["mcp_server"]["enabled"], true);
        assert_eq!(updated["mcp_server"]["args"], serde_json::json!([]));
        assert_eq!(updated["mcp_server"]["env"], serde_json::json!({}));

        let loaded = store.load().expect("load state");
        assert_eq!(
            loaded
                .mcp_servers
                .get("local-tools")
                .expect("mcp server")
                .enabled,
            true
        );
        assert!(
            loaded
                .events
                .iter()
                .any(|event| event.kind == EventKind::McpServerUpdated)
        );

        let (status, body) =
            response_for_mutation(&store, "DELETE", "/registry/mcp-servers/local-tools", b"");
        let deleted: serde_json::Value = serde_json::from_str(&body).expect("deleted json");

        assert_eq!(status, "200 OK");
        assert_eq!(deleted["removed"], true);
        assert_eq!(deleted["mcp_server"]["id"], "local-tools");
        assert!(
            !store
                .load()
                .expect("reload")
                .mcp_servers
                .contains_key("local-tools")
        );

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/registry/mcp-servers",
            br#"{"id":"bad-env","command":"agent-os-mcp","env":{"1BAD":"x"}}"#,
        );
        let invalid: serde_json::Value = serde_json::from_str(&body).expect("invalid json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            invalid["error"],
            "mcp environment key must be a valid environment variable name"
        );
    }

    #[test]
    fn approval_api_lists_and_resolves_gates() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("api-test");
        let mut task = Task::new(
            "Approve deploy",
            "Deploy after review",
            Priority::Normal,
            vec![],
        );
        task.status = TaskStatus::Blocked;
        let task_id = task.id.clone();
        os.create_task(task);
        let approval = ApprovalRequest::new(
            task_id.clone(),
            None,
            "git push origin main",
            "risky command matched policy",
        );
        let approval_id = approval.id.clone();
        os.request_approval(approval);
        store.save(&os).expect("save state");

        let loaded = store.load().expect("load");
        let (status, body) = response_for_path(&loaded, "/approvals");
        let approvals: serde_json::Value = serde_json::from_str(&body).expect("approvals json");
        assert_eq!(status, "200 OK");
        assert_eq!(approvals.as_array().expect("approvals").len(), 1);
        assert_eq!(approvals[0]["status"], "pending");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/approvals/{approval_id}/approve"),
            br#"{"by":"operator"}"#,
        );
        let approved: serde_json::Value = serde_json::from_str(&body).expect("approved json");
        assert_eq!(status, "200 OK");
        assert_eq!(approved["id"], approval_id);
        assert_eq!(approved["approval"]["status"], "approved");
        assert_eq!(approved["approval"]["resolved_by"], "operator");
        assert_eq!(
            store
                .load()
                .expect("load approved")
                .tasks
                .get(&task_id)
                .expect("task")
                .status,
            TaskStatus::Pending
        );

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            &format!("/approvals/{approval_id}/deny"),
            b"",
        );
        let denied: serde_json::Value = serde_json::from_str(&body).expect("denied json");
        assert_eq!(status, "200 OK");
        assert_eq!(denied["approval"]["status"], "approved");

        let (status, body) = response_for_mutation(&store, "POST", "/approvals/!!!/approve", b"");
        let invalid: serde_json::Value = serde_json::from_str(&body).expect("invalid json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            invalid["error"],
            "approval id must contain at least one ASCII letter, digit, or hyphen"
        );

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/approvals/missing/approve",
            br#"{"by":"operator"}"#,
        );
        let missing: serde_json::Value = serde_json::from_str(&body).expect("missing json");
        assert_eq!(status, "404 Not Found");
        assert_eq!(missing["error"], "approval not found");
        assert!(validate_state(&store.load().expect("load")).valid);
    }

    #[test]
    fn worker_and_eval_api_manage_distributed_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("api-test");
        os.policy.allow_shell = true;
        os.policy.command_timeout_seconds = 1;
        os.register_agent(Agent::new(
            "builder",
            AgentKind::Builder,
            None,
            vec!["rust".into()],
            1,
        ));
        let task = Task::new(
            "Remote worker task",
            "claim through worker node",
            Priority::Normal,
            vec!["rust".into()],
        );
        let task_id = task.id.clone();
        os.create_task(task);
        store.save(&os).expect("save state");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/workers",
            br#"{"id":"remote-a","endpoint":"http://127.0.0.1:9000"}"#,
        );
        let worker: serde_json::Value = serde_json::from_str(&body).expect("worker json");
        assert_eq!(status, "201 Created");
        assert_eq!(worker["id"], "remote-a");
        assert_eq!(worker["worker"]["status"], "online");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/workers/remote-a/heartbeat",
            br#"{"status":"busy","endpoint":"http://127.0.0.1:9001"}"#,
        );
        let heartbeat: serde_json::Value = serde_json::from_str(&body).expect("heartbeat json");
        assert_eq!(status, "200 OK");
        assert_eq!(heartbeat["worker"]["status"], "busy");
        assert_eq!(heartbeat["worker"]["endpoint"], "http://127.0.0.1:9001");

        let loaded = store.load().expect("load");
        let (status, body) = response_for_path(&loaded, "/workers?status=busy&query=9001&limit=1");
        let workers: serde_json::Value = serde_json::from_str(&body).expect("workers json");
        assert_eq!(status, "200 OK");
        assert_eq!(workers.as_array().expect("workers").len(), 1);
        assert_eq!(workers[0]["id"], "remote-a");

        let (status, body) = response_for_path(&loaded, "/workers/remote-a");
        let worker_detail: serde_json::Value = serde_json::from_str(&body).expect("worker detail");
        assert_eq!(status, "200 OK");
        assert_eq!(worker_detail["id"], "remote-a");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/workers",
            br#"{"id":"builder","endpoint":"http://127.0.0.1:9100"}"#,
        );
        assert_eq!(status, "201 Created");
        let _: serde_json::Value = serde_json::from_str(&body).expect("builder worker json");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/workers/builder/claim",
            br#"{"lease_seconds":30}"#,
        );
        let claim: serde_json::Value = serde_json::from_str(&body).expect("worker claim json");
        assert_eq!(status, "200 OK");
        assert_eq!(claim["claimed"], true);
        assert_eq!(claim["assignment"]["agent_id"], "builder");
        assert_eq!(claim["assignment"]["task_id"], task_id.to_string());
        assert_eq!(claim["task"]["title"], "Remote worker task");
        assert_eq!(claim["worker"]["status"], "online");

        let report_request = serde_json::to_vec(&serde_json::json!({
            "task_id": task_id,
            "status": "complete",
            "note": "worker finished remotely",
            "command": "printf worker-claim",
            "cwd": "/tmp/remote-worker",
            "exit_code": 0,
            "artifacts": [{
                "kind": "stdout",
                "path": "artifacts/stdout.log",
                "bytes": 18,
                "content_type": "text/plain"
            }]
        }))
        .expect("worker report request");
        let (status, body) =
            response_for_mutation(&store, "POST", "/workers/builder/report", &report_request);
        let report: serde_json::Value = serde_json::from_str(&body).expect("worker report json");
        assert_eq!(status, "200 OK");
        assert_eq!(report["reported"], true);
        assert_eq!(report["task"]["status"], "complete");
        assert_eq!(report["task"]["output"], "worker finished remotely");
        assert_eq!(report["run"]["status"], "success");
        assert_eq!(report["run"]["command"], "printf worker-claim");
        assert_eq!(report["run"]["cwd"], "/tmp/remote-worker");
        assert_eq!(report["run"]["exit_code"], 0);
        assert_eq!(report["run"]["artifacts"][0]["kind"], "stdout");
        assert_eq!(
            report["run"]["artifacts"][0]["path"],
            "artifacts/stdout.log"
        );

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/evals",
            br#"{"target":"ci-fix-workflow","success":true,"cost_micros":1234,"latency_ms":567}"#,
        );
        let eval: serde_json::Value = serde_json::from_str(&body).expect("eval json");
        let eval_id = eval["id"].as_str().expect("eval id").to_owned();
        assert_eq!(status, "201 Created");
        assert_eq!(eval["eval"]["target"], "ci-fix-workflow");
        assert_eq!(eval["eval"]["success"], true);

        let loaded = store.load().expect("load");
        let (status, body) = response_for_path(
            &loaded,
            "/evals?target=ci-fix-workflow&success=true&limit=1",
        );
        let evals: serde_json::Value = serde_json::from_str(&body).expect("evals json");
        assert_eq!(status, "200 OK");
        assert_eq!(evals.as_array().expect("evals").len(), 1);
        assert_eq!(evals[0]["id"], eval_id);

        let (status, body) = response_for_path(&loaded, &format!("/evals/{eval_id}"));
        let eval_detail: serde_json::Value = serde_json::from_str(&body).expect("eval detail");
        assert_eq!(status, "200 OK");
        assert_eq!(eval_detail["id"], eval_id);

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/evals/run",
            br#"{"target":"local-smoke","command":"printf api-eval-run","success_pattern":"api-eval-run"}"#,
        );
        let eval_run: serde_json::Value = serde_json::from_str(&body).expect("eval run json");
        assert_eq!(status, "201 Created");
        assert_eq!(eval_run["eval"]["target"], "local-smoke");
        assert_eq!(eval_run["eval"]["success"], true);
        assert_eq!(eval_run["eval"]["run"]["command"], "printf api-eval-run");
        assert_eq!(eval_run["eval"]["run"]["stdout"], "api-eval-run");
        assert_eq!(eval_run["eval"]["run"]["success_pattern"], "api-eval-run");
        assert_eq!(eval_run["stdout"], "api-eval-run");
        assert_eq!(eval_run["success_pattern_matched"], true);
        assert_eq!(eval_run["output_schema_valid"], true);
        assert_eq!(eval_run["output_schema_error"], serde_json::Value::Null);
        assert_eq!(eval_run["timed_out"], false);
        let eval_run_id = eval_run["id"].as_str().expect("eval run id").to_owned();
        let loaded = store.load().expect("load eval run");
        let (status, body) = response_for_path(&loaded, &format!("/evals/{eval_run_id}"));
        let eval_run_detail: serde_json::Value =
            serde_json::from_str(&body).expect("eval run detail json");
        assert_eq!(status, "200 OK");
        assert_eq!(eval_run_detail["run"]["command"], "printf api-eval-run");

        let (status, body) = response_for_path(&loaded, "/evals?query=api-eval-run&limit=1");
        let queried_evals: serde_json::Value =
            serde_json::from_str(&body).expect("queried evals json");
        assert_eq!(status, "200 OK");
        assert_eq!(queried_evals.as_array().expect("queried evals").len(), 1);
        assert_eq!(queried_evals[0]["id"], eval_run_id);

        let output_schema = serde_json::json!({
            "type": "object",
            "required": ["message", "ok", "count"],
            "properties": {
                "message": { "type": "string" },
                "ok": { "type": "boolean" },
                "count": { "type": "integer" }
            },
            "additionalProperties": false
        });
        let valid_schema_request = serde_json::to_vec(&serde_json::json!({
            "target": "schema-smoke",
            "command": "printf '{\"message\":\"api-schema-run\",\"ok\":true,\"count\":2}'",
            "output_schema": output_schema
        }))
        .expect("schema request json");
        let (status, body) =
            response_for_mutation(&store, "POST", "/evals/run", &valid_schema_request);
        let eval_schema_run: serde_json::Value =
            serde_json::from_str(&body).expect("eval schema run json");
        assert_eq!(status, "201 Created");
        assert_eq!(eval_schema_run["eval"]["success"], true);
        assert_eq!(eval_schema_run["output_schema_valid"], true);
        assert_eq!(
            eval_schema_run["output_schema_error"],
            serde_json::Value::Null
        );
        assert_eq!(eval_schema_run["eval"]["run"]["output_schema_valid"], true);

        let invalid_schema_request = serde_json::to_vec(&serde_json::json!({
            "target": "schema-smoke-invalid",
            "command": "printf '{\"message\":\"api-schema-run\",\"ok\":\"yes\",\"count\":2}'",
            "output_schema": {
                "type": "object",
                "required": ["message", "ok", "count"],
                "properties": {
                    "message": { "type": "string" },
                    "ok": { "type": "boolean" },
                    "count": { "type": "integer" }
                },
                "additionalProperties": false
            }
        }))
        .expect("invalid schema request json");
        let (status, body) =
            response_for_mutation(&store, "POST", "/evals/run", &invalid_schema_request);
        let eval_schema_failure: serde_json::Value =
            serde_json::from_str(&body).expect("eval schema failure json");
        assert_eq!(status, "201 Created");
        assert_eq!(eval_schema_failure["eval"]["success"], false);
        assert_eq!(eval_schema_failure["output_schema_valid"], false);
        assert!(
            eval_schema_failure["output_schema_error"]
                .as_str()
                .expect("schema error")
                .contains("output.ok must match output_schema.type boolean")
        );

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/evals/run",
            br#"{"target":"timeout-smoke","command":"sleep 3; printf late"}"#,
        );
        let eval_timeout: serde_json::Value =
            serde_json::from_str(&body).expect("eval timeout json");
        assert_eq!(status, "201 Created");
        assert_eq!(eval_timeout["eval"]["success"], false);
        assert_eq!(eval_timeout["eval"]["run"]["timed_out"], true);
        assert_eq!(
            eval_timeout["eval"]["run"]["status"],
            serde_json::Value::Null
        );
        assert_eq!(eval_timeout["timed_out"], true);
        assert_eq!(eval_timeout["status"], serde_json::Value::Null);

        let (status, body) = response_for_mutation(&store, "DELETE", "/workers/remote-a", b"");
        let removed: serde_json::Value = serde_json::from_str(&body).expect("removed json");
        assert_eq!(status, "200 OK");
        assert_eq!(removed["removed"], true);

        let (status, body) = response_for_mutation(&store, "DELETE", "/workers/builder", b"");
        let removed: serde_json::Value = serde_json::from_str(&body).expect("removed builder json");
        assert_eq!(status, "200 OK");
        assert_eq!(removed["removed"], true);

        let loaded = store.load().expect("load");
        assert!(loaded.workers.is_empty());
        assert_eq!(loaded.evals.len(), 5);
        assert!(validate_state(&loaded).valid);
    }

    #[test]
    fn secrets_backend_api_manages_backend_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("api-test");
        os.register_tool(ToolDefinition::new(
            "secret-tool",
            ToolKind::Shell,
            "uses a secret",
            vec![],
            "printf {token}",
            None,
        ));
        let mut task = Task::new("Needs token", "check secret", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        task.tool = Some(ToolInvocation::with_secret_env_args(
            ToolId::new("secret-tool"),
            BTreeMap::new(),
            BTreeMap::from([("token".into(), "AGENT_OS_TEST_MISSING_SECRET".into())]),
        ));
        os.create_task(task);
        store.save(&os).expect("save state");

        let loaded = store.load().expect("load");
        let (status, body) = response_for_path(&loaded, "/secrets?kind=env&limit=1");
        let defaults: serde_json::Value = serde_json::from_str(&body).expect("defaults json");
        assert_eq!(status, "200 OK");
        assert_eq!(defaults.as_array().expect("defaults").len(), 1);
        assert_eq!(defaults[0]["id"], "environment");

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/secrets",
            br#"{"id":"prod-op","kind":"1password","reference":"op://vault/item"}"#,
        );
        let created: serde_json::Value = serde_json::from_str(&body).expect("created json");
        assert_eq!(status, "201 Created");
        assert_eq!(created["id"], "prod-op");
        assert_eq!(created["secrets_backend"]["kind"], "one-password");

        let loaded = store.load().expect("load");
        let (status, body) = response_for_path(&loaded, "/secrets?kind=one-password&query=vault");
        let filtered: serde_json::Value = serde_json::from_str(&body).expect("filtered json");
        assert_eq!(status, "200 OK");
        assert_eq!(filtered.as_array().expect("filtered").len(), 1);
        assert_eq!(filtered[0]["id"], "prod-op");

        let (status, body) = response_for_path(&loaded, "/secrets/prod-op");
        let detail: serde_json::Value = serde_json::from_str(&body).expect("detail json");
        assert_eq!(status, "200 OK");
        assert_eq!(detail["reference"], "op://vault/item");

        let loaded = store.load().expect("load");
        let (status, body) = response_for_path(&loaded, "/secrets/check");
        let check: serde_json::Value = serde_json::from_str(&body).expect("check json");
        assert_eq!(status, "200 OK");
        assert_eq!(check["total"], 1);
        assert_eq!(check["present"], 0);
        assert_eq!(check["missing"], 1);
        assert_eq!(check["references"][0]["task_id"], task_id.to_string());
        assert_eq!(
            check["references"][0]["env"],
            "AGENT_OS_TEST_MISSING_SECRET"
        );
        assert_eq!(check["references"][0]["present"], false);

        let (status, body) =
            response_for_mutation(&store, "POST", "/secrets", br#"{"id":"!!!","kind":"env"}"#);
        let invalid: serde_json::Value = serde_json::from_str(&body).expect("invalid json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(
            invalid["error"],
            "secrets backend id must contain at least one ASCII letter, digit, or hyphen"
        );

        let (status, body) = response_for_mutation(
            &store,
            "POST",
            "/secrets",
            br#"{"id":"bad-kind","kind":"mystery"}"#,
        );
        let invalid_kind: serde_json::Value =
            serde_json::from_str(&body).expect("invalid kind json");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(invalid_kind["error"], "invalid kind");

        let (status, body) = response_for_mutation(&store, "DELETE", "/secrets/prod-op", b"");
        let removed: serde_json::Value = serde_json::from_str(&body).expect("removed json");
        assert_eq!(status, "200 OK");
        assert_eq!(removed["removed"], true);

        let loaded = store.load().expect("load");
        assert!(!loaded.secrets_backends.contains_key("prod-op"));
        assert!(validate_state(&loaded).valid);
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

    #[test]
    fn cors_origin_policy_allows_loopback_and_rejects_remote_origins() {
        let cors = ApiCors::default();
        assert_eq!(
            allowed_cors_origin("GET / HTTP/1.1\r\n", &cors).expect("missing origin"),
            None
        );
        assert_eq!(
            allowed_cors_origin("GET / HTTP/1.1\r\nOrigin: http://localhost:3000\r\n", &cors)
                .expect("localhost"),
            Some("http://localhost:3000".into())
        );
        assert_eq!(
            allowed_cors_origin("GET / HTTP/1.1\r\nOrigin: http://127.0.0.1:3000\r\n", &cors)
                .expect("ipv4"),
            Some("http://127.0.0.1:3000".into())
        );
        assert_eq!(
            allowed_cors_origin("GET / HTTP/1.1\r\nOrigin: http://[::1]:3000\r\n", &cors)
                .expect("ipv6"),
            Some("http://[::1]:3000".into())
        );

        let denied =
            allowed_cors_origin("GET / HTTP/1.1\r\nOrigin: https://evil.example\r\n", &cors)
                .expect_err("remote origin rejected");
        assert_eq!(denied.0, "403 Forbidden");

        let repeated = allowed_cors_origin(
            "GET / HTTP/1.1\r\nOrigin: http://localhost:3000\r\nOrigin: https://evil.example\r\n",
            &cors,
        )
        .expect_err("repeated origin rejected");
        assert_eq!(repeated.0, "400 Bad Request");
        let repeated_body: serde_json::Value =
            serde_json::from_str(&repeated.1).expect("repeated origin body");
        assert_eq!(repeated_body["error"], "origin must not be repeated");
    }

    #[test]
    fn cors_origin_policy_allows_configured_remote_origins() {
        let cors = ApiCors::allow_origins(vec!["https://dashboard.example".into()]);

        assert_eq!(
            allowed_cors_origin(
                "GET / HTTP/1.1\r\nOrigin: https://dashboard.example\r\n",
                &cors,
            )
            .expect("configured origin"),
            Some("https://dashboard.example".into())
        );
        let denied =
            allowed_cors_origin("GET / HTTP/1.1\r\nOrigin: https://other.example\r\n", &cors)
                .expect_err("unconfigured remote origin rejected");
        assert_eq!(denied.0, "403 Forbidden");
        assert_eq!(
            normalize_cors_origin("https://dashboard.example/path"),
            None
        );
    }

    #[test]
    fn bearer_token_authorization_uses_exact_token_match() {
        let handler = ApiHandler {
            store: Store::new("unused-state.json"),
            auth: ApiAuth::bearer(Some("secret-token".into())),
            cors: ApiCors::default(),
            config_path: None,
        };

        assert_eq!(
            handler.authorize(
                "GET",
                "GET / HTTP/1.1\r\nAuthorization: Bearer secret-token\r\n"
            ),
            AuthDecision::Allowed
        );
        assert_eq!(
            handler.authorize("GET", "GET / HTTP/1.1\r\nAuthorization: Bearer secret\r\n"),
            AuthDecision::MissingOrInvalid
        );
        assert_eq!(
            handler.authorize(
                "GET",
                "GET / HTTP/1.1\r\nAuthorization: Bearer secret-token\r\nAuthorization: Bearer secret-token\r\n"
            ),
            AuthDecision::MissingOrInvalid
        );
        assert_eq!(
            handler.authorize(
                "GET",
                "GET / HTTP/1.1\r\nAuthorization: Bearer secret-token-extra\r\n"
            ),
            AuthDecision::MissingOrInvalid
        );
        assert_eq!(
            handler.authorize(
                "GET",
                "GET / HTTP/1.1\r\nAuthorization: Basic secret-token\r\n"
            ),
            AuthDecision::MissingOrInvalid
        );
    }

    #[test]
    fn scoped_tokens_authorize_only_matching_methods() {
        let handler = ApiHandler {
            store: Store::new("unused-state.json"),
            auth: ApiAuth::scoped(
                Some("full-token".into()),
                Some("read-token".into()),
                Some("write-token".into()),
            ),
            cors: ApiCors::default(),
            config_path: None,
        };

        assert_eq!(
            handler.authorize(
                "GET",
                "GET / HTTP/1.1\r\nAuthorization: Bearer read-token\r\n"
            ),
            AuthDecision::Allowed
        );
        assert_eq!(
            handler.authorize(
                "POST",
                "POST / HTTP/1.1\r\nAuthorization: Bearer read-token\r\n"
            ),
            AuthDecision::InsufficientScope
        );
        assert_eq!(
            handler.authorize(
                "DELETE",
                "DELETE / HTTP/1.1\r\nAuthorization: Bearer write-token\r\n"
            ),
            AuthDecision::Allowed
        );
        assert_eq!(
            handler.authorize(
                "GET",
                "GET / HTTP/1.1\r\nAuthorization: Bearer write-token\r\n"
            ),
            AuthDecision::InsufficientScope
        );
        assert_eq!(
            handler.authorize(
                "POST",
                "POST / HTTP/1.1\r\nAuthorization: Bearer full-token\r\n"
            ),
            AuthDecision::Allowed
        );
    }
}
