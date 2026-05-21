use crate::models::{AutonomyLevel, NetworkMode, Policy};
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::path::Path;
use thiserror::Error;
use url::Url;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyDecision {
    pub allowed: bool,
    pub reason: String,
}

#[derive(Debug, Error)]
pub enum PolicyError {
    #[error(
        "shell execution is disabled by policy; opt into a shell-enabled profile such as dev, ci, or autonomous, or set policy.allow_shell = true in config"
    )]
    ShellDisabled,
    #[error("action requires human approval: {0}")]
    ApprovalRequired(String),
    #[error("execution is disabled by autonomy level: {0}")]
    AutonomyRestricted(String),
    #[error("command rejected by policy: {0}")]
    Rejected(String),
    #[error("workspace rejected by policy: {0}")]
    WorkspaceRejected(String),
}

pub fn check_shell_command(policy: &Policy, command: &str) -> Result<PolicyDecision, PolicyError> {
    if !policy.allow_shell {
        return Err(PolicyError::ShellDisabled);
    }
    match policy.autonomy {
        AutonomyLevel::ObserveOnly | AutonomyLevel::Suggest => {
            return Err(PolicyError::AutonomyRestricted(policy.autonomy_label()));
        }
        AutonomyLevel::ExecuteWithApproval | AutonomyLevel::ExecuteFreely => {}
    }

    let normalized = command.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(PolicyError::Rejected("command is empty".into()));
    }
    let rule_clauses = policy_rule_clauses(&policy.rules);
    for rule in &rule_clauses {
        apply_shell_rule(rule, &normalized)?;
    }
    check_dangerous_shell_invocations(command)?;
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
    if matches!(
        policy.network.mode,
        NetworkMode::Disabled | NetworkMode::ProvidersOnly
    ) && shell_command_looks_networked(&normalized)
    {
        return Err(PolicyError::Rejected(
            "network access is disabled for shell commands".into(),
        ));
    }
    if matches!(policy.network.mode, NetworkMode::Allowed)
        && shell_command_looks_networked(&normalized)
    {
        check_network_allowed_hosts(policy, command)?;
    }
    if matches!(policy.autonomy, AutonomyLevel::ExecuteWithApproval)
        || policy.approval.require_for_risky_actions
    {
        for pattern in &policy.approval.risky_patterns {
            let pattern = pattern.trim().to_ascii_lowercase();
            if !pattern.is_empty() && normalized.contains(&pattern) {
                return Err(PolicyError::ApprovalRequired(format!(
                    "matched risky pattern `{pattern}`"
                )));
            }
        }
    }
    for pattern in approval_rule_patterns(&rule_clauses) {
        if normalized.contains(&pattern) {
            return Err(PolicyError::ApprovalRequired(format!(
                "matched policy rule `require approval for {pattern}`"
            )));
        }
    }

    let allow_rules = rule_clauses
        .iter()
        .filter_map(|rule| rule.strip_prefix("allow ").map(str::trim))
        .filter(|pattern| !pattern.is_empty())
        .collect::<Vec<_>>();
    if !allow_rules.is_empty() {
        if has_shell_control_operator(command) {
            return Err(PolicyError::Rejected(
                "policy allow rules reject shell control operators".into(),
            ));
        }
        if !allow_rules
            .iter()
            .any(|pattern| command_matches_rule_pattern(&normalized, pattern))
        {
            return Err(PolicyError::Rejected(format!(
                "command is not allowed by policy rules: {}",
                allow_rules.join(", ")
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
        let allowed = allowed_commands.iter().any(|command| {
            let command = command.to_ascii_lowercase();
            if command.split_whitespace().count() > 1 {
                command_matches_rule_pattern(&normalized, &command)
            } else {
                command.eq_ignore_ascii_case(executable)
            }
        });
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

fn check_dangerous_shell_invocations(command: &str) -> Result<(), PolicyError> {
    let tokens = shell_tokens(command);
    for segment in shell_command_segments(&tokens) {
        let Some(executable) = segment.first().map(|token| shell_basename(token)) else {
            continue;
        };
        match executable.as_str() {
            "rm" if rm_force_recursive(&segment) => {
                return Err(PolicyError::Rejected(
                    "matched dangerous rm recursive-force invocation".into(),
                ));
            }
            "dd" if dd_uses_raw_device_operands(&segment) => {
                return Err(PolicyError::Rejected(
                    "matched dangerous dd raw-device operand".into(),
                ));
            }
            _ => {
                if let Some(reason) = dangerous_shell_executable_reason(&executable) {
                    return Err(PolicyError::Rejected(reason));
                }
            }
        }
    }
    Ok(())
}

fn dangerous_shell_executable_reason(executable: &str) -> Option<String> {
    let reason = match executable {
        "sudo" | "doas" | "pkexec" | "su" => "matched dangerous privilege-escalation executable",
        "mkfs" | "mke2fs" => "matched dangerous filesystem-format executable",
        "shutdown" | "reboot" | "halt" | "poweroff" => "matched dangerous system power executable",
        _ if executable.starts_with("mkfs.") => "matched dangerous filesystem-format executable",
        _ => return None,
    };
    Some(reason.into())
}

fn rm_force_recursive(command: &[String]) -> bool {
    let mut force = false;
    let mut recursive = false;
    for token in command.iter().skip(1) {
        if token == "--" {
            break;
        }
        match token.as_str() {
            "-f" | "--force" => force = true,
            "-r" | "-R" | "--recursive" | "--directories" => recursive = true,
            _ if token.starts_with('-') && !token.starts_with("--") => {
                force |= token[1..].chars().any(|ch| ch == 'f');
                recursive |= token[1..].chars().any(|ch| matches!(ch, 'r' | 'R'));
            }
            _ => {}
        }
    }
    force && recursive
}

fn dd_uses_raw_device_operands(command: &[String]) -> bool {
    command
        .iter()
        .skip(1)
        .take_while(|token| token.as_str() != "--")
        .any(|token| {
            let lower = token.to_ascii_lowercase();
            lower.starts_with("if=") || lower.starts_with("of=")
        })
}

trait PolicyAutonomyLabel {
    fn autonomy_label(&self) -> String;
}

impl PolicyAutonomyLabel for Policy {
    fn autonomy_label(&self) -> String {
        match self.autonomy {
            AutonomyLevel::ObserveOnly => "observe-only".into(),
            AutonomyLevel::Suggest => "suggest".into(),
            AutonomyLevel::ExecuteWithApproval => "execute-with-approval".into(),
            AutonomyLevel::ExecuteFreely => "execute-freely".into(),
        }
    }
}

fn apply_shell_rule(rule: &str, normalized_command: &str) -> Result<(), PolicyError> {
    let rule = rule.trim().to_ascii_lowercase();
    if rule.is_empty() {
        return Ok(());
    }
    if rule == "deny writes outside src" {
        return Ok(());
    }
    if let Some(pattern) = rule.strip_prefix("deny ") {
        let pattern = pattern.trim();
        if !pattern.is_empty() && normalized_command.contains(pattern) {
            return Err(PolicyError::Rejected(format!(
                "matched policy rule `{rule}`"
            )));
        }
    }
    Ok(())
}

fn policy_rule_clauses(rules: &[String]) -> Vec<String> {
    rules
        .iter()
        .flat_map(|rule| rule.split(','))
        .map(|rule| rule.trim().to_ascii_lowercase())
        .filter(|rule| !rule.is_empty())
        .collect()
}

fn approval_rule_patterns(rules: &[String]) -> Vec<String> {
    rules
        .iter()
        .filter_map(|rule| {
            rule.strip_prefix("require approval for ")
                .or_else(|| rule.strip_prefix("approval required for "))
                .map(str::trim)
        })
        .filter(|pattern| !pattern.is_empty())
        .map(str::to_owned)
        .collect()
}

fn command_matches_rule_pattern(command: &str, pattern: &str) -> bool {
    command == pattern || command.starts_with(&format!("{pattern} "))
}

fn shell_command_looks_networked(command: &str) -> bool {
    shell_command_segments(&shell_tokens(command))
        .iter()
        .any(|segment| {
            let Some(executable) = segment.first().map(|token| shell_basename(token)) else {
                return false;
            };
            match executable.as_str() {
                "curl" | "wget" | "ssh" | "scp" | "rsync" | "nc" | "ncat" | "telnet" | "ftp" => {
                    true
                }
                "git" => git_segment_looks_networked(segment),
                _ => false,
            }
        })
}

fn git_segment_looks_networked(segment: &[String]) -> bool {
    let Some(subcommand_index) = segment
        .iter()
        .enumerate()
        .skip(1)
        .find_map(|(index, token)| (!token.starts_with('-')).then_some(index))
    else {
        return false;
    };
    let subcommand = segment[subcommand_index].to_ascii_lowercase();
    match subcommand.as_str() {
        "clone" | "fetch" | "pull" | "push" | "ls-remote" => true,
        "remote" => segment
            .iter()
            .skip(subcommand_index + 1)
            .any(|token| token.eq_ignore_ascii_case("update")),
        "submodule" => segment
            .iter()
            .skip(subcommand_index + 1)
            .any(|token| token.eq_ignore_ascii_case("update")),
        _ => false,
    }
}

fn check_network_allowed_hosts(policy: &Policy, command: &str) -> Result<(), PolicyError> {
    let allowed_hosts = policy
        .network
        .allowed_hosts
        .iter()
        .map(|host| host.trim().trim_end_matches('.').to_ascii_lowercase())
        .filter(|host| !host.is_empty())
        .collect::<Vec<_>>();
    if allowed_hosts.is_empty() {
        return Ok(());
    }
    if has_shell_control_operator(command) {
        return Err(PolicyError::Rejected(
            "network allowed_hosts rejects shell control operators".into(),
        ));
    }

    let hosts = network_hosts_in_command(command);
    if hosts.is_empty() {
        return Err(PolicyError::Rejected(
            "network command host could not be verified against allowed_hosts".into(),
        ));
    }
    if let Some(host) = hosts
        .iter()
        .find(|host| !allowed_hosts.iter().any(|allowed| allowed == *host))
    {
        return Err(PolicyError::Rejected(format!(
            "host `{host}` is not in network allowed_hosts"
        )));
    }

    Ok(())
}

fn network_hosts_in_command(command: &str) -> Vec<String> {
    let mut hosts = Vec::new();
    for token in shell_tokens(command)
        .into_iter()
        .filter(|token| !is_shell_separator(token) && !is_write_redirection(token))
        .map(|token| clean_network_token(&token))
    {
        if token.is_empty() {
            continue;
        }
        if let Some(host) = url_host(&token).or_else(|| scp_like_host(&token)) {
            hosts.push(host);
        }
    }
    hosts.sort();
    hosts.dedup();
    hosts
}

fn clean_network_token(token: &str) -> String {
    token
        .trim_matches(|ch| matches!(ch, '\'' | '"' | '(' | ')' | '[' | ']' | '{' | '}' | ','))
        .to_owned()
}

fn url_host(token: &str) -> Option<String> {
    let url = Url::parse(token).ok()?;
    if !matches!(url.scheme(), "http" | "https" | "ssh" | "git") {
        return None;
    }
    url.host_str()
        .map(|host| host.trim_end_matches('.').to_ascii_lowercase())
        .filter(|host| !host.is_empty())
}

fn scp_like_host(token: &str) -> Option<String> {
    let (_, after_user) = token.rsplit_once('@')?;
    let host = after_user
        .split([':', '/'])
        .next()
        .unwrap_or_default()
        .trim()
        .trim_end_matches('.');
    if host.is_empty() {
        return None;
    }
    Some(host.to_ascii_lowercase())
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
    if !policy.sandbox.jailed_workspaces {
        return Ok(PolicyDecision {
            allowed: true,
            reason: "workspace jail disabled by policy".into(),
        });
    }
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
    for rule_path in write_rule_paths(policy) {
        enforce_write_rule(policy, path, &rule_path, None)?;
    }
    check_file_write_path_without_rules(policy, path)
}

fn check_file_write_path_without_rules(
    policy: &Policy,
    path: &Path,
) -> Result<PolicyDecision, PolicyError> {
    let workspace_decision = match path.symlink_metadata() {
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
    }?;

    check_writable_paths(policy, path)?;

    Ok(workspace_decision)
}

pub fn check_shell_writes(
    policy: &Policy,
    command: &str,
    cwd: &Path,
) -> Result<PolicyDecision, PolicyError> {
    let writable_paths = policy
        .sandbox
        .writable_paths
        .iter()
        .map(|path| path.trim())
        .filter(|path| !path.is_empty())
        .collect::<Vec<_>>();
    let write_rules = write_rule_paths(policy);
    if writable_paths.is_empty() && write_rules.is_empty() {
        return Ok(PolicyDecision {
            allowed: true,
            reason: "shell write sandbox disabled by empty writable_paths and write rules".into(),
        });
    }

    let tokens = shell_tokens(command);
    check_shell_redirections(policy, cwd, &tokens)?;
    check_shell_write_commands(policy, cwd, &tokens)?;

    Ok(PolicyDecision {
        allowed: true,
        reason: "shell write targets allowed by sandbox writable_paths".into(),
    })
}

fn check_shell_redirections(
    policy: &Policy,
    cwd: &Path,
    tokens: &[String],
) -> Result<(), PolicyError> {
    for (index, token) in tokens.iter().enumerate() {
        if !is_write_redirection(token) {
            continue;
        }
        let Some(target) = tokens
            .iter()
            .skip(index + 1)
            .find(|candidate| !is_shell_separator(candidate))
        else {
            return Err(PolicyError::WorkspaceRejected(
                "shell write redirection target could not be verified against sandbox writable_paths"
                    .into(),
            ));
        };
        if target.starts_with('&') {
            continue;
        }
        check_shell_write_target(policy, cwd, target)?;
    }
    Ok(())
}

fn check_shell_write_commands(
    policy: &Policy,
    cwd: &Path,
    tokens: &[String],
) -> Result<(), PolicyError> {
    for command in shell_command_segments(tokens) {
        let Some(executable) = command.first().map(|token| shell_basename(token)) else {
            continue;
        };
        match executable.as_str() {
            "touch" | "mkdir" | "rmdir" | "unlink" | "truncate" | "tee" => {
                for operand in command
                    .iter()
                    .skip(1)
                    .filter(|token| shell_path_operand(token))
                {
                    check_shell_write_target(policy, cwd, operand)?;
                }
            }
            "cp" | "mv" | "install" => {
                if let Some(destination) = command
                    .iter()
                    .skip(1)
                    .filter(|token| shell_path_operand(token))
                    .last()
                {
                    check_shell_write_target(policy, cwd, destination)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn shell_tokens(command: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut single_quoted = false;
    let mut double_quoted = false;
    let mut escaped = false;
    let mut chars = command.chars().peekable();
    while let Some(ch) = chars.next() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == '\'' && !double_quoted {
            single_quoted = !single_quoted;
            continue;
        }
        if ch == '"' && !single_quoted {
            double_quoted = !double_quoted;
            continue;
        }
        if !single_quoted && !double_quoted && ch.is_whitespace() {
            push_shell_token(&mut tokens, &mut current);
            continue;
        }
        if !single_quoted && !double_quoted && matches!(ch, ';' | '|' | '&' | '>' | '<') {
            push_shell_token(&mut tokens, &mut current);
            let mut operator = ch.to_string();
            if matches!(ch, '|' | '&' | '>') && chars.peek() == Some(&ch) {
                operator.push(chars.next().unwrap_or(ch));
            }
            tokens.push(operator);
            continue;
        }
        current.push(ch);
    }
    push_shell_token(&mut tokens, &mut current);
    tokens
}

fn push_shell_token(tokens: &mut Vec<String>, current: &mut String) {
    if !current.is_empty() {
        tokens.push(std::mem::take(current));
    }
}

fn is_shell_separator(token: &str) -> bool {
    matches!(token, ";" | "|" | "||" | "&" | "&&")
}

fn is_write_redirection(token: &str) -> bool {
    matches!(token, ">" | ">>" | "&>")
}

fn shell_command_segments(tokens: &[String]) -> Vec<Vec<String>> {
    let mut commands = Vec::new();
    let mut current = Vec::new();
    let mut skip_next_redirection_target = false;
    for token in tokens {
        if skip_next_redirection_target && !is_shell_separator(token) {
            skip_next_redirection_target = false;
            continue;
        }
        if is_write_redirection(token) {
            skip_next_redirection_target = true;
            continue;
        }
        if is_shell_separator(token) {
            if !current.is_empty() {
                commands.push(std::mem::take(&mut current));
            }
            continue;
        }
        current.push(token.clone());
    }
    if !current.is_empty() {
        commands.push(current);
    }
    commands
}

fn shell_basename(token: &str) -> String {
    Path::new(token)
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or(token)
        .to_ascii_lowercase()
}

fn shell_path_operand(token: &&String) -> bool {
    let token = token.as_str();
    !token.is_empty()
        && !token.starts_with('-')
        && !token.contains('=')
        && !matches!(token, "." | "..")
}

fn check_shell_write_target(policy: &Policy, cwd: &Path, target: &str) -> Result<(), PolicyError> {
    if shell_target_is_dynamic(target) {
        return Err(PolicyError::WorkspaceRejected(format!(
            "shell write target `{target}` could not be verified against sandbox writable_paths"
        )));
    }
    let target = Path::new(target);
    let path = if target.is_absolute() {
        target.to_path_buf()
    } else {
        cwd.join(target)
    };
    for rule_path in write_rule_paths(policy) {
        enforce_write_rule(policy, &path, &rule_path, Some(cwd))?;
    }
    check_file_write_path_without_rules(policy, &path).map(|_| ())
}

fn shell_target_is_dynamic(target: &str) -> bool {
    target.starts_with('$')
        || target.starts_with('~')
        || target.contains('*')
        || target.contains('?')
        || target.contains('[')
        || target.contains(']')
        || target.contains('{')
        || target.contains('}')
}

fn write_rule_paths(policy: &Policy) -> Vec<String> {
    policy_rule_clauses(&policy.rules)
        .into_iter()
        .filter_map(|rule| {
            rule.strip_prefix("deny writes outside ")
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .map(ToOwned::to_owned)
        })
        .collect()
}

fn enforce_write_rule(
    policy: &Policy,
    path: &Path,
    rule_path: &str,
    cwd: Option<&Path>,
) -> Result<(), PolicyError> {
    let target = canonical_write_target(path)?;
    let allowed_roots = write_rule_allowed_roots(policy, rule_path, cwd);
    let allowed = allowed_roots.iter().any(|root| {
        canonical_configured_path(root)
            .map(|allowed| target.starts_with(allowed))
            .unwrap_or(false)
    });
    if !allowed {
        return Err(PolicyError::WorkspaceRejected(format!(
            "policy rule `deny writes outside {rule_path}` rejected write"
        )));
    }
    Ok(())
}

fn write_rule_allowed_roots(
    policy: &Policy,
    rule_path: &str,
    cwd: Option<&Path>,
) -> Vec<std::path::PathBuf> {
    let rule = Path::new(rule_path);
    if rule.is_absolute() {
        return vec![rule.to_path_buf()];
    }
    let mut roots = Vec::new();
    if let Some(cwd) = cwd {
        roots.push(cwd.join(rule));
    }
    for workspace in policy
        .allowed_workspaces
        .iter()
        .map(|workspace| workspace.trim())
        .filter(|workspace| !workspace.is_empty())
    {
        roots.push(Path::new(workspace).join(rule));
    }
    if roots.is_empty() {
        roots.push(Path::new(".").join(rule));
    }
    roots
}

fn check_writable_paths(policy: &Policy, path: &Path) -> Result<(), PolicyError> {
    let writable_paths = policy
        .sandbox
        .writable_paths
        .iter()
        .map(|path| path.trim())
        .filter(|path| !path.is_empty())
        .collect::<Vec<_>>();
    if writable_paths.is_empty() {
        return Ok(());
    }

    let target = canonical_write_target(path)?;
    let allowed = writable_paths.iter().any(|writable| {
        canonical_configured_path(Path::new(writable))
            .map(|allowed| target.starts_with(allowed))
            .unwrap_or(false)
    });
    if !allowed {
        return Err(PolicyError::WorkspaceRejected(format!(
            "{} is outside sandbox writable_paths",
            path.display()
        )));
    }

    Ok(())
}

fn canonical_write_target(path: &Path) -> Result<std::path::PathBuf, PolicyError> {
    match path.symlink_metadata() {
        Ok(metadata) if metadata.file_type().is_symlink() => path.canonicalize().map_err(|error| {
            PolicyError::WorkspaceRejected(format!(
                "{} symlink target could not be resolved ({error})",
                path.display()
            ))
        }),
        Ok(_) => path.canonicalize().map_err(|error| {
            PolicyError::WorkspaceRejected(format!(
                "{} could not be resolved ({error})",
                path.display()
            ))
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            let parent = parent.canonicalize().map_err(|error| {
                PolicyError::WorkspaceRejected(format!(
                    "{} parent could not be resolved ({error})",
                    path.display()
                ))
            })?;
            Ok(path
                .file_name()
                .map(|file_name| parent.join(file_name))
                .unwrap_or(parent))
        }
        Err(error) => Err(PolicyError::WorkspaceRejected(format!(
            "{} could not be inspected ({error})",
            path.display()
        ))),
    }
}

fn canonical_configured_path(path: &Path) -> Result<std::path::PathBuf, std::io::Error> {
    match path.symlink_metadata() {
        Ok(metadata) if metadata.file_type().is_symlink() => path.canonicalize(),
        Ok(_) => path.canonicalize(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            let parent = parent.canonicalize()?;
            Ok(path
                .file_name()
                .map(|file_name| parent.join(file_name))
                .unwrap_or(parent))
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shell_policy() -> Policy {
        Policy {
            allow_shell: true,
            ..Policy::default()
        }
    }

    #[test]
    fn default_policy_disables_shell_execution() {
        let error = check_shell_command(&Policy::default(), "printf hello").expect_err("disabled");

        assert!(matches!(error, PolicyError::ShellDisabled));
        assert!(
            error
                .to_string()
                .contains("set policy.allow_shell = true in config")
        );
    }

    #[test]
    fn rejects_denied_shell_patterns() {
        let policy = Policy {
            denied_patterns: vec!["custom-danger".into()],
            ..shell_policy()
        };
        let error = check_shell_command(&policy, "printf custom-danger").expect_err("rejected");

        assert!(error.to_string().contains("denied pattern"));
    }

    #[test]
    fn rejects_tokenized_dangerous_shell_invocations() {
        let policy = shell_policy();
        for command in [
            "rm -rf /tmp/example",
            "rm -fr /tmp/example",
            "rm -fR /tmp/example",
            "rm -r -f /tmp/example",
            "rm --recursive --force /tmp/example",
            "/bin/rm -R --force /tmp/example",
            "dd of=/dev/disk0 if=image.raw",
            "dd bs=1m if=image.raw of=/dev/disk0",
        ] {
            let error = check_shell_command(&policy, command).expect_err("rejected");

            assert!(error.to_string().contains("dangerous"), "{error}");
        }
    }

    #[test]
    fn rejects_tokenized_dangerous_system_executables() {
        let policy = Policy {
            denied_patterns: Vec::new(),
            ..shell_policy()
        };
        for command in [
            "sudo whoami",
            "/usr/bin/doas id",
            "pkexec sh",
            "su root -c id",
            "mkfs.ext4 /dev/sda1",
            "mke2fs /dev/sda1",
            "shutdown -h now",
            "reboot",
            "halt",
            "poweroff",
        ] {
            let error = check_shell_command(&policy, command).expect_err("rejected");

            assert!(error.to_string().contains("dangerous"), "{error}");
        }

        assert!(check_shell_command(&policy, "printf 'sudo mkfs reboot'").is_ok());
    }

    #[test]
    fn rejects_empty_shell_commands() {
        let policy = shell_policy();
        let error = check_shell_command(&policy, "   ").expect_err("rejected");

        assert!(error.to_string().contains("command is empty"));
    }

    #[test]
    fn supports_allowlist_mode() {
        let policy = Policy {
            allowed_commands: vec!["printf".into()],
            ..shell_policy()
        };

        assert!(check_shell_command(&policy, "printf hello").is_ok());
        assert!(check_shell_command(&policy, "echo hello").is_err());
        assert!(check_shell_command(&policy, "printf ok; uname -s").is_err());
        assert!(check_shell_command(&policy, "printf 'ok; still argument'").is_ok());
    }

    #[test]
    fn allowed_commands_support_full_command_prefixes() {
        let policy = Policy {
            allowed_commands: vec!["cargo test".into()],
            ..shell_policy()
        };

        assert!(check_shell_command(&policy, "cargo test --workspace").is_ok());
        assert!(check_shell_command(&policy, "cargo build").is_err());
        assert!(check_shell_command(&policy, "cargo test; cargo publish").is_err());
    }

    #[test]
    fn readable_policy_rules_allow_and_deny_shell_commands() {
        let policy = Policy {
            rules: vec!["allow cargo test, deny cargo publish".into()],
            ..shell_policy()
        };

        assert!(check_shell_command(&policy, "cargo test --workspace").is_ok());
        assert!(check_shell_command(&policy, "cargo publish").is_err());
        assert!(check_shell_command(&policy, "cargo build").is_err());
        assert!(check_shell_command(&policy, "cargo test; cargo publish").is_err());
    }

    #[test]
    fn readable_policy_rules_can_require_approval_for_matching_commands() {
        let policy = Policy {
            approval: crate::models::ApprovalPolicy {
                require_for_risky_actions: false,
                risky_patterns: Vec::new(),
            },
            network: crate::models::NetworkPolicy {
                mode: NetworkMode::Allowed,
                allowed_hosts: Vec::new(),
            },
            rules: vec!["allow git, require approval for git push".into()],
            ..shell_policy()
        };

        assert!(check_shell_command(&policy, "git status").is_ok());
        let error =
            check_shell_command(&policy, "git push origin main").expect_err("approval required");
        assert!(matches!(error, PolicyError::ApprovalRequired(_)));
        assert!(error.to_string().contains("require approval for git push"));
    }

    #[test]
    fn approval_required_rule_alias_is_supported() {
        let policy = Policy {
            approval: crate::models::ApprovalPolicy {
                require_for_risky_actions: false,
                risky_patterns: Vec::new(),
            },
            rules: vec!["approval required for cargo publish".into()],
            ..shell_policy()
        };

        assert!(matches!(
            check_shell_command(&policy, "cargo publish").expect_err("approval required"),
            PolicyError::ApprovalRequired(_)
        ));
    }

    #[test]
    fn readable_write_rule_can_be_composed_with_shell_rules() {
        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("src");
        let docs = dir.path().join("docs");
        std::fs::create_dir_all(&src).expect("src");
        std::fs::create_dir_all(&docs).expect("docs");
        let policy = Policy {
            allowed_workspaces: vec![dir.path().display().to_string()],
            rules: vec!["allow cargo test, deny writes outside src".into()],
            ..shell_policy()
        };

        assert!(check_shell_command(&policy, "cargo test").is_ok());
        assert!(check_file_write_path(&policy, &src.join("lib.rs")).is_ok());
        assert!(check_file_write_path(&policy, &docs.join("guide.md")).is_err());
        assert!(
            check_file_write_path(&policy, &src.join("..").join("docs").join("guide.md")).is_err()
        );
    }

    #[test]
    fn readable_write_rules_support_parameterized_paths_and_shell_writes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let workspace = dir.path().join("workspace");
        let generated = workspace.join("src").join("generated");
        let docs = workspace.join("docs");
        std::fs::create_dir_all(&generated).expect("generated");
        std::fs::create_dir_all(&docs).expect("docs");
        let policy = Policy {
            allowed_workspaces: vec![workspace.display().to_string()],
            rules: vec!["deny writes outside src/generated".into()],
            ..shell_policy()
        };

        assert!(check_file_write_path(&policy, &generated.join("file.rs")).is_ok());
        assert!(check_file_write_path(&policy, &workspace.join("src").join("lib.rs")).is_err());
        assert!(check_shell_writes(&policy, "touch src/generated/file.rs", &workspace).is_ok());

        let error = check_shell_writes(&policy, "printf no > docs/out.txt", &workspace)
            .expect_err("outside redirection rejected");
        assert!(
            error
                .to_string()
                .contains("deny writes outside src/generated")
        );
    }

    #[test]
    fn file_write_policy_enforces_writable_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        let workspace = dir.path().join("workspace");
        let src = workspace.join("src");
        let docs = workspace.join("docs");
        std::fs::create_dir_all(&src).expect("src");
        std::fs::create_dir_all(&docs).expect("docs");
        let policy = Policy {
            allowed_workspaces: vec![workspace.display().to_string()],
            sandbox: crate::models::SandboxPolicy {
                writable_paths: vec![src.display().to_string()],
                ..crate::models::SandboxPolicy::default()
            },
            ..Policy::default()
        };

        assert!(check_file_write_path(&policy, &src.join("lib.rs")).is_ok());
        let error = check_file_write_path(&policy, &docs.join("guide.md")).expect_err("rejected");

        assert!(error.to_string().contains("outside sandbox writable_paths"));
    }

    #[test]
    fn file_write_policy_allows_new_files_under_writable_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let workspace = dir.path().join("workspace");
        let generated = workspace.join("src").join("generated");
        std::fs::create_dir_all(&generated).expect("generated");
        let policy = Policy {
            allowed_workspaces: vec![workspace.display().to_string()],
            sandbox: crate::models::SandboxPolicy {
                writable_paths: vec![generated.display().to_string()],
                ..crate::models::SandboxPolicy::default()
            },
            ..Policy::default()
        };

        assert!(check_file_write_path(&policy, &generated.join("file.rs")).is_ok());
        assert!(check_file_write_path(&policy, &workspace.join("src").join("lib.rs")).is_err());
    }

    #[test]
    fn shell_write_policy_enforces_writable_paths_for_common_writes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let workspace = dir.path().join("workspace");
        let src = workspace.join("src");
        let docs = workspace.join("docs");
        std::fs::create_dir_all(&src).expect("src");
        std::fs::create_dir_all(&docs).expect("docs");
        let policy = Policy {
            allowed_workspaces: vec![workspace.display().to_string()],
            sandbox: crate::models::SandboxPolicy {
                writable_paths: vec![src.display().to_string()],
                ..crate::models::SandboxPolicy::default()
            },
            ..shell_policy()
        };

        assert!(check_shell_writes(&policy, "touch src/lib.rs", &workspace).is_ok());
        assert!(check_shell_writes(&policy, "printf ok > src/out.txt", &workspace).is_ok());
        assert!(
            check_shell_writes(&policy, "cp docs/input.txt src/output.txt", &workspace).is_ok()
        );

        let error = check_shell_writes(&policy, "touch docs/guide.md", &workspace)
            .expect_err("outside write rejected");
        assert!(error.to_string().contains("outside sandbox writable_paths"));

        let error = check_shell_writes(&policy, "printf no > docs/out.txt", &workspace)
            .expect_err("outside redirection rejected");
        assert!(error.to_string().contains("outside sandbox writable_paths"));
    }

    #[test]
    fn shell_write_policy_rejects_dynamic_targets_when_writable_paths_are_configured() {
        let dir = tempfile::tempdir().expect("tempdir");
        let workspace = dir.path().join("workspace");
        let src = workspace.join("src");
        std::fs::create_dir_all(&src).expect("src");
        let policy = Policy {
            allowed_workspaces: vec![workspace.display().to_string()],
            sandbox: crate::models::SandboxPolicy {
                writable_paths: vec![src.display().to_string()],
                ..crate::models::SandboxPolicy::default()
            },
            ..shell_policy()
        };

        let error = check_shell_writes(&policy, "touch $OUTPUT", &workspace)
            .expect_err("dynamic target rejected");
        assert!(error.to_string().contains("could not be verified"));
    }

    #[test]
    fn network_allowed_hosts_allow_matching_urls_and_git_ssh_hosts() {
        let policy = Policy {
            network: crate::models::NetworkPolicy {
                mode: NetworkMode::Allowed,
                allowed_hosts: vec!["example.com".into(), "github.com".into()],
            },
            approval: crate::models::ApprovalPolicy {
                require_for_risky_actions: false,
                risky_patterns: Vec::new(),
            },
            autonomy: AutonomyLevel::ExecuteFreely,
            ..shell_policy()
        };

        assert!(check_shell_command(&policy, "curl https://example.com/api").is_ok());
        assert!(check_shell_command(&policy, "git clone git@github.com:owner/repo.git").is_ok());
        assert!(
            check_shell_command(&policy, "git ls-remote https://github.com/owner/repo.git").is_ok()
        );
    }

    #[test]
    fn network_allowed_hosts_reject_unlisted_hosts() {
        let policy = Policy {
            network: crate::models::NetworkPolicy {
                mode: NetworkMode::Allowed,
                allowed_hosts: vec!["example.com".into()],
            },
            approval: crate::models::ApprovalPolicy {
                require_for_risky_actions: false,
                risky_patterns: Vec::new(),
            },
            autonomy: AutonomyLevel::ExecuteFreely,
            ..shell_policy()
        };

        let error =
            check_shell_command(&policy, "curl https://evil.example/api").expect_err("rejected");

        assert!(error.to_string().contains("not in network allowed_hosts"));
    }

    #[test]
    fn network_policy_detection_is_token_aware() {
        let policy = Policy {
            approval: crate::models::ApprovalPolicy {
                require_for_risky_actions: false,
                risky_patterns: Vec::new(),
            },
            autonomy: AutonomyLevel::ExecuteFreely,
            ..shell_policy()
        };

        let error = check_shell_command(&policy, "git ls-remote https://github.com/owner/repo.git")
            .expect_err("network command rejected");
        assert!(
            error
                .to_string()
                .contains("network access is disabled for shell commands")
        );

        assert!(check_shell_command(&policy, "printf 'curl https://evil.example/api'").is_ok());
    }

    #[test]
    fn network_allowed_hosts_reject_unverifiable_network_commands() {
        let policy = Policy {
            network: crate::models::NetworkPolicy {
                mode: NetworkMode::Allowed,
                allowed_hosts: vec!["github.com".into()],
            },
            approval: crate::models::ApprovalPolicy {
                require_for_risky_actions: false,
                risky_patterns: Vec::new(),
            },
            autonomy: AutonomyLevel::ExecuteFreely,
            ..shell_policy()
        };

        let error = check_shell_command(&policy, "git fetch origin").expect_err("rejected");

        assert!(error.to_string().contains("could not be verified"));

        let error = check_shell_command(&policy, "ssh").expect_err("rejected");

        assert!(error.to_string().contains("could not be verified"));
    }

    #[test]
    fn network_allowed_hosts_reject_shell_control_operators() {
        let policy = Policy {
            network: crate::models::NetworkPolicy {
                mode: NetworkMode::Allowed,
                allowed_hosts: vec!["example.com".into()],
            },
            approval: crate::models::ApprovalPolicy {
                require_for_risky_actions: false,
                risky_patterns: Vec::new(),
            },
            autonomy: AutonomyLevel::ExecuteFreely,
            ..shell_policy()
        };

        let error = check_shell_command(
            &policy,
            "curl https://example.com/api; curl https://evil.example/api",
        )
        .expect_err("rejected");

        assert!(error.to_string().contains("shell control operators"));
    }

    #[test]
    fn policy_patterns_are_case_and_space_tolerant() {
        let policy = Policy {
            allowed_commands: vec!["  PRINTF  ".into()],
            denied_patterns: vec!["  SuDo  ".into()],
            ..shell_policy()
        };

        assert!(check_shell_command(&policy, "printf hello").is_ok());
        assert!(check_shell_command(&policy, "SUDO whoami").is_err());
    }

    #[test]
    fn empty_denied_patterns_are_ignored_at_runtime() {
        let policy = Policy {
            denied_patterns: vec![" ".into()],
            ..shell_policy()
        };

        assert!(check_shell_command(&policy, "printf hello").is_ok());
    }

    #[test]
    fn empty_allowed_commands_do_not_enable_allowlist_mode() {
        let policy = Policy {
            allowed_commands: vec![" ".into()],
            ..shell_policy()
        };

        assert!(check_shell_command(&policy, "printf hello").is_ok());
    }

    #[test]
    fn empty_allowed_commands_are_ignored_when_allowlist_has_entries() {
        let policy = Policy {
            allowed_commands: vec![" ".into(), "printf".into()],
            ..shell_policy()
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
