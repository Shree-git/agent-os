use crate::models::Policy;
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyDecision {
    pub allowed: bool,
    pub reason: String,
}

#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("shell execution is disabled by policy")]
    ShellDisabled,
    #[error("command rejected by policy: {0}")]
    Rejected(String),
    #[error("workspace rejected by policy: {0}")]
    WorkspaceRejected(String),
}

pub fn check_shell_command(policy: &Policy, command: &str) -> Result<PolicyDecision, PolicyError> {
    if !policy.allow_shell {
        return Err(PolicyError::ShellDisabled);
    }

    let normalized = command.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(PolicyError::Rejected("command is empty".into()));
    }
    for pattern in &policy.denied_patterns {
        let pattern = pattern.trim().to_ascii_lowercase();
        if pattern.is_empty() {
            continue;
        }
        if normalized.contains(&pattern) {
            return Err(PolicyError::Rejected(format!(
                "matched denied pattern `{pattern}`"
            )));
        }
    }

    let allowed_commands = policy
        .allowed_commands
        .iter()
        .map(|command| command.trim())
        .filter(|command| !command.is_empty())
        .collect::<Vec<_>>();

    if !allowed_commands.is_empty() {
        if has_shell_control_operator(command) {
            return Err(PolicyError::Rejected(
                "allowed_commands mode rejects shell control operators".into(),
            ));
        }
        let executable = normalized.split_whitespace().next().unwrap_or_default();
        let allowed = allowed_commands
            .iter()
            .any(|command| command.eq_ignore_ascii_case(executable));
        if !allowed {
            return Err(PolicyError::Rejected(format!(
                "`{executable}` is not in allowed_commands"
            )));
        }
    }

    Ok(PolicyDecision {
        allowed: true,
        reason: "command allowed by policy".into(),
    })
}

fn has_shell_control_operator(command: &str) -> bool {
    let mut single_quoted = false;
    let mut double_quoted = false;
    let mut escaped = false;
    let mut chars = command.chars().peekable();

    while let Some(ch) = chars.next() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        match ch {
            '\'' if !double_quoted => single_quoted = !single_quoted,
            '"' if !single_quoted => double_quoted = !double_quoted,
            ';' | '|' | '&' | '<' | '>' | '\n' if !single_quoted && !double_quoted => return true,
            '$' if !single_quoted && chars.peek() == Some(&'(') => return true,
            '`' if !single_quoted => return true,
            _ => {}
        }
    }

    false
}

pub fn check_workspace(policy: &Policy, cwd: &Path) -> Result<PolicyDecision, PolicyError> {
    let cwd = cwd
        .canonicalize()
        .map_err(|error| PolicyError::WorkspaceRejected(format!("{} ({error})", cwd.display())))?;

    let allowed = policy.allowed_workspaces.iter().any(|workspace| {
        Path::new(workspace)
            .canonicalize()
            .map(|allowed| cwd.starts_with(allowed))
            .unwrap_or(false)
    });

    if !allowed {
        return Err(PolicyError::WorkspaceRejected(format!(
            "{} is outside allowed_workspaces",
            cwd.display()
        )));
    }

    Ok(PolicyDecision {
        allowed: true,
        reason: "workspace allowed by policy".into(),
    })
}

pub fn check_file_write_path(policy: &Policy, path: &Path) -> Result<PolicyDecision, PolicyError> {
    match path.symlink_metadata() {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            let resolved = path.canonicalize().map_err(|error| {
                PolicyError::WorkspaceRejected(format!(
                    "{} symlink target could not be resolved ({error})",
                    path.display()
                ))
            })?;
            check_workspace(policy, &resolved)
        }
        Ok(_) => check_workspace(policy, path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            check_workspace(policy, path.parent().unwrap_or_else(|| Path::new(".")))
        }
        Err(error) => Err(PolicyError::WorkspaceRejected(format!(
            "{} could not be inspected ({error})",
            path.display()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_denied_shell_patterns() {
        let policy = Policy::default();
        let error = check_shell_command(&policy, "rm -rf /tmp/example").expect_err("rejected");

        assert!(error.to_string().contains("denied pattern"));
    }

    #[test]
    fn rejects_empty_shell_commands() {
        let policy = Policy::default();
        let error = check_shell_command(&policy, "   ").expect_err("rejected");

        assert!(error.to_string().contains("command is empty"));
    }

    #[test]
    fn supports_allowlist_mode() {
        let policy = Policy {
            allowed_commands: vec!["printf".into()],
            ..Policy::default()
        };

        assert!(check_shell_command(&policy, "printf hello").is_ok());
        assert!(check_shell_command(&policy, "echo hello").is_err());
        assert!(check_shell_command(&policy, "printf ok; uname -s").is_err());
        assert!(check_shell_command(&policy, "printf 'ok; still argument'").is_ok());
    }

    #[test]
    fn policy_patterns_are_case_and_space_tolerant() {
        let policy = Policy {
            allowed_commands: vec!["  PRINTF  ".into()],
            denied_patterns: vec!["  SuDo  ".into()],
            ..Policy::default()
        };

        assert!(check_shell_command(&policy, "printf hello").is_ok());
        assert!(check_shell_command(&policy, "SUDO whoami").is_err());
    }

    #[test]
    fn empty_denied_patterns_are_ignored_at_runtime() {
        let policy = Policy {
            denied_patterns: vec![" ".into()],
            ..Policy::default()
        };

        assert!(check_shell_command(&policy, "printf hello").is_ok());
    }

    #[test]
    fn empty_allowed_commands_do_not_enable_allowlist_mode() {
        let policy = Policy {
            allowed_commands: vec![" ".into()],
            ..Policy::default()
        };

        assert!(check_shell_command(&policy, "printf hello").is_ok());
    }

    #[test]
    fn empty_allowed_commands_are_ignored_when_allowlist_has_entries() {
        let policy = Policy {
            allowed_commands: vec![" ".into(), "printf".into()],
            ..Policy::default()
        };

        assert!(check_shell_command(&policy, "printf hello").is_ok());
        assert!(check_shell_command(&policy, "echo hello").is_err());
    }

    #[test]
    fn rejects_workspaces_outside_allowlist() {
        let dir = tempfile::tempdir().expect("tempdir");
        let allowed = dir.path().join("allowed");
        let denied = dir.path().join("denied");
        std::fs::create_dir_all(&allowed).expect("allowed");
        std::fs::create_dir_all(&denied).expect("denied");
        let policy = Policy {
            allowed_workspaces: vec![allowed.display().to_string()],
            ..Policy::default()
        };

        assert!(check_workspace(&policy, &allowed).is_ok());
        assert!(check_workspace(&policy, &denied).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn file_write_policy_rejects_existing_symlink_outside_allowlist() {
        let dir = tempfile::tempdir().expect("tempdir");
        let allowed = dir.path().join("allowed");
        let denied = dir.path().join("denied");
        std::fs::create_dir_all(&allowed).expect("allowed");
        std::fs::create_dir_all(&denied).expect("denied");
        let outside = denied.join("target.txt");
        std::fs::write(&outside, "outside").expect("outside");
        let link = allowed.join("link.txt");
        std::os::unix::fs::symlink(&outside, &link).expect("symlink");
        let policy = Policy {
            allowed_workspaces: vec![allowed.display().to_string()],
            ..Policy::default()
        };

        let error = check_file_write_path(&policy, &link).expect_err("rejected");

        assert!(error.to_string().contains("outside allowed_workspaces"));
    }
}
