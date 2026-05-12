use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const DEFAULT_LAUNCHD_LABEL: &str = "com.infinite-apps.agent-os";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LaunchdServiceOptions {
    pub label: String,
    pub program: Option<PathBuf>,
    pub interval_ms: u64,
    pub limit: usize,
    pub execute: bool,
    pub recover_stale_seconds: Option<i64>,
    pub no_logs: bool,
    pub plist_path: Option<PathBuf>,
}

impl Default for LaunchdServiceOptions {
    fn default() -> Self {
        Self {
            label: DEFAULT_LAUNCHD_LABEL.into(),
            program: None,
            interval_ms: 1000,
            limit: 1,
            execute: false,
            recover_stale_seconds: None,
            no_logs: false,
            plist_path: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("service label must not be empty")]
    EmptyLabel,
    #[error("{field} must not be empty")]
    EmptyText { field: &'static str },
    #[error("{field} must not be empty")]
    EmptyPath { field: &'static str },
    #[error("interval_ms must be greater than 0")]
    InvalidInterval,
    #[error("limit must be greater than 0")]
    InvalidLimit,
    #[error("recover_stale_seconds must be greater than or equal to 0")]
    InvalidRecoverStaleSeconds,
    #[error("could not determine current exe")]
    CurrentExe { source: std::io::Error },
    #[error("could not run {path}: {source}")]
    CommandIo {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("could not determine current uid for launchd domain: {source}")]
    DomainIo { source: std::io::Error },
    #[error("could not determine current uid for launchd domain")]
    EmptyDomainUid,
    #[error("could not determine current uid for launchd domain: {stderr}")]
    DomainCommandFailed { stderr: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LaunchdService {
    pub label: String,
    pub program: String,
    pub state_path: String,
    pub interval_ms: u64,
    pub limit: usize,
    pub execute: bool,
    pub recover_stale_seconds: Option<i64>,
    pub stdout_path: Option<String>,
    pub stderr_path: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LaunchctlCommandOutput {
    pub success: bool,
    pub status: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

impl LaunchdService {
    pub fn program_arguments(&self) -> Vec<String> {
        let mut args = vec![
            self.program.clone(),
            "--state".into(),
            self.state_path.clone(),
            "daemon".into(),
            "run".into(),
            "--interval-ms".into(),
            self.interval_ms.to_string(),
            "--limit".into(),
            self.limit.to_string(),
        ];
        if self.execute {
            args.push("--execute".into());
        }
        if let Some(seconds) = self.recover_stale_seconds {
            args.push("--recover-stale-seconds".into());
            args.push(seconds.to_string());
        }
        args
    }

    pub fn render_plist(&self) -> String {
        let mut body = String::new();
        body.push_str(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
        body.push('\n');
        body.push_str(
            r#"<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">"#,
        );
        body.push('\n');
        body.push_str(r#"<plist version="1.0">"#);
        body.push('\n');
        body.push_str("<dict>\n");
        push_key_string(&mut body, "Label", &self.label);
        body.push_str("  <key>ProgramArguments</key>\n");
        body.push_str("  <array>\n");
        for arg in self.program_arguments() {
            body.push_str("    <string>");
            body.push_str(&escape_xml(&arg));
            body.push_str("</string>\n");
        }
        body.push_str("  </array>\n");
        push_key_bool(&mut body, "RunAtLoad", true);
        push_key_bool(&mut body, "KeepAlive", true);
        if let Some(path) = &self.stdout_path {
            push_key_string(&mut body, "StandardOutPath", path);
        }
        if let Some(path) = &self.stderr_path {
            push_key_string(&mut body, "StandardErrorPath", path);
        }
        body.push_str("</dict>\n</plist>\n");
        body
    }
}

pub fn build_launchd_service(
    state_path: &Path,
    options: LaunchdServiceOptions,
) -> Result<(LaunchdService, PathBuf), ServiceError> {
    validate_service_label(&options.label)?;
    validate_optional_path("bin_path", options.program.as_deref())?;
    validate_optional_path("plist_path", options.plist_path.as_deref())?;
    if options.interval_ms == 0 {
        return Err(ServiceError::InvalidInterval);
    }
    if options.limit == 0 {
        return Err(ServiceError::InvalidLimit);
    }
    if matches!(options.recover_stale_seconds, Some(seconds) if seconds < 0) {
        return Err(ServiceError::InvalidRecoverStaleSeconds);
    }

    let program = match options.program {
        Some(program) => program,
        None => std::env::current_exe().map_err(|source| ServiceError::CurrentExe { source })?,
    };
    let (stdout_path, stderr_path) = if options.no_logs {
        (None, None)
    } else {
        let (stdout, stderr) = default_launchd_log_paths(state_path);
        (Some(stdout), Some(stderr))
    };
    let plist_path = options
        .plist_path
        .unwrap_or_else(|| default_launchd_plist_path(&options.label));
    Ok((
        LaunchdService {
            label: options.label,
            program: program.display().to_string(),
            state_path: state_path.display().to_string(),
            interval_ms: options.interval_ms,
            limit: options.limit,
            execute: options.execute,
            recover_stale_seconds: options.recover_stale_seconds,
            stdout_path,
            stderr_path,
        },
        plist_path,
    ))
}

pub fn default_launchd_plist_path(label: &str) -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("Library")
        .join("LaunchAgents")
        .join(format!("{label}.plist"))
}

pub fn default_launchd_log_paths(state_path: &Path) -> (String, String) {
    let directory = if state_path.extension().is_some() {
        state_path.parent().unwrap_or_else(|| Path::new("."))
    } else {
        state_path
    };
    (
        directory.join("daemon.out.log").display().to_string(),
        directory.join("daemon.err.log").display().to_string(),
    )
}

pub fn validate_service_label(label: &str) -> Result<(), ServiceError> {
    if label.trim().is_empty() {
        return Err(ServiceError::EmptyLabel);
    }
    Ok(())
}

pub fn validate_optional_path(
    field: &'static str,
    path: Option<&Path>,
) -> Result<(), ServiceError> {
    if let Some(path) = path
        && (path.as_os_str().is_empty() || path.to_string_lossy().trim().is_empty())
    {
        return Err(ServiceError::EmptyPath { field });
    }
    Ok(())
}

pub fn validate_optional_text(
    field: &'static str,
    value: Option<&str>,
) -> Result<(), ServiceError> {
    if let Some(value) = value
        && value.trim().is_empty()
    {
        return Err(ServiceError::EmptyText { field });
    }
    Ok(())
}

pub fn validate_service_control_inputs(
    label: &str,
    domain: Option<&str>,
    plist_path: Option<&Path>,
    launchctl_path: &Path,
) -> Result<(), ServiceError> {
    validate_service_label(label)?;
    validate_optional_text("launchd domain", domain)?;
    validate_optional_path("plist_path", plist_path)?;
    validate_optional_path("launchctl_path", Some(launchctl_path))
}

pub fn run_launchctl(
    launchctl_path: &Path,
    args: &[&str],
) -> Result<LaunchctlCommandOutput, ServiceError> {
    let output = std::process::Command::new(launchctl_path)
        .args(args)
        .output()
        .map_err(|source| ServiceError::CommandIo {
            path: launchctl_path.to_path_buf(),
            source,
        })?;
    Ok(LaunchctlCommandOutput {
        success: output.status.success(),
        status: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

pub fn resolve_launchd_domain(domain: Option<String>) -> Result<String, ServiceError> {
    if let Some(domain) = domain {
        return Ok(domain);
    }

    let output = std::process::Command::new("id")
        .arg("-u")
        .output()
        .map_err(|source| ServiceError::DomainIo { source })?;
    if !output.status.success() {
        return Err(ServiceError::DomainCommandFailed {
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    let uid = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if uid.is_empty() {
        return Err(ServiceError::EmptyDomainUid);
    }
    Ok(format!("gui/{uid}"))
}

fn push_key_string(body: &mut String, key: &str, value: &str) {
    body.push_str("  <key>");
    body.push_str(&escape_xml(key));
    body.push_str("</key>\n  <string>");
    body.push_str(&escape_xml(value));
    body.push_str("</string>\n");
}

fn push_key_bool(body: &mut String, key: &str, value: bool) {
    body.push_str("  <key>");
    body.push_str(&escape_xml(key));
    body.push_str("</key>\n  ");
    body.push_str(if value { "<true/>" } else { "<false/>" });
    body.push('\n');
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_launchd_plist_escapes_arguments() {
        let service = LaunchdService {
            label: "com.example.agent-os".into(),
            program: "/tmp/agent-os".into(),
            state_path: "/tmp/agent&os/state.json".into(),
            interval_ms: 500,
            limit: 2,
            execute: true,
            recover_stale_seconds: Some(30),
            stdout_path: Some("/tmp/out.log".into()),
            stderr_path: Some("/tmp/err.log".into()),
        };

        let plist = service.render_plist();

        assert!(plist.contains("<key>ProgramArguments</key>"));
        assert!(plist.contains("/tmp/agent&amp;os/state.json"));
        assert!(plist.contains("--recover-stale-seconds"));
        assert!(plist.contains("<key>KeepAlive</key>"));
    }

    #[test]
    fn build_launchd_service_applies_defaults_and_validates_inputs() {
        let state_path = Path::new("/tmp/agent-os/state.json");
        let (service, plist_path) = build_launchd_service(
            state_path,
            LaunchdServiceOptions {
                label: "com.example.agent-os".into(),
                program: Some(PathBuf::from("/usr/local/bin/agent-os")),
                interval_ms: 500,
                limit: 2,
                execute: true,
                recover_stale_seconds: Some(30),
                no_logs: false,
                plist_path: None,
            },
        )
        .expect("service");

        assert_eq!(service.state_path, "/tmp/agent-os/state.json");
        assert_eq!(service.program, "/usr/local/bin/agent-os");
        assert_eq!(
            service.stdout_path.as_deref(),
            Some("/tmp/agent-os/daemon.out.log")
        );
        assert_eq!(
            service.stderr_path.as_deref(),
            Some("/tmp/agent-os/daemon.err.log")
        );
        assert!(plist_path.ends_with("Library/LaunchAgents/com.example.agent-os.plist"));

        let error = build_launchd_service(
            state_path,
            LaunchdServiceOptions {
                label: " ".into(),
                ..LaunchdServiceOptions::default()
            },
        )
        .expect_err("empty label should fail");
        assert_eq!(error.to_string(), "service label must not be empty");
    }

    #[test]
    fn service_control_validation_rejects_empty_inputs() {
        let error = validate_service_control_inputs(
            "com.example.agent-os",
            Some(" "),
            Some(Path::new("/tmp/test.plist")),
            Path::new("launchctl"),
        )
        .expect_err("empty domain should fail");
        assert_eq!(error.to_string(), "launchd domain must not be empty");

        let error = validate_service_control_inputs(
            "com.example.agent-os",
            Some("gui/test"),
            None,
            Path::new(" "),
        )
        .expect_err("empty launchctl path should fail");
        assert_eq!(error.to_string(), "launchctl_path must not be empty");
    }
}
