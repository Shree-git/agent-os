use crate::models::{ToolDefinition, ToolInvocation, ToolKind, is_valid_env_var_name};
use crate::secrets::{EnvironmentSecretResolver, SecretResolveError, SecretResolver};
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ToolError {
    #[error("tool not found: {0}")]
    NotFound(String),
    #[error("unsupported tool kind: {0}")]
    UnsupportedKind(ToolKind),
    #[error("tool `{tool}` has malformed template placeholder `{placeholder}`")]
    InvalidTemplatePlaceholder { tool: String, placeholder: String },
    #[error("tool `{tool}` has unclosed template placeholder `{placeholder}`")]
    UnclosedTemplatePlaceholder { tool: String, placeholder: String },
    #[error("missing required tool argument `{arg}` for tool `{tool}`")]
    MissingArgument { tool: String, arg: String },
    #[error("invalid tool argument key `{arg}` for tool `{tool}`")]
    InvalidArgumentKey { tool: String, arg: String },
    #[error("tool argument `{arg}` for tool `{tool}` cannot be both plain and secret")]
    ConflictingArgument { tool: String, arg: String },
    #[error("unexpected tool argument `{arg}` for tool `{tool}`")]
    UnexpectedArgument { tool: String, arg: String },
    #[error("secret tool argument `{arg}` for tool `{tool}` references unset env var `{env}`")]
    MissingSecretEnv {
        tool: String,
        arg: String,
        env: String,
    },
    #[error("secret tool argument `{arg}` for tool `{tool}` references invalid env var `{env}`")]
    InvalidSecretEnv {
        tool: String,
        arg: String,
        env: String,
    },
    #[error(
        "secret tool argument `{arg}` for tool `{tool}` could not resolve `{reference}`: {message}"
    )]
    SecretResolve {
        tool: String,
        arg: String,
        reference: String,
        message: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenderedToolCommand {
    pub command: String,
    pub default_cwd: Option<String>,
    pub redacted_values: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenderedToolText {
    pub text: String,
    pub default_cwd: Option<String>,
    pub redacted_values: Vec<String>,
}

pub fn render_tool_command(
    tool: &ToolDefinition,
    invocation: &ToolInvocation,
) -> Result<RenderedToolCommand, ToolError> {
    render_tool_command_with_redaction_patterns(tool, invocation, &[])
}

pub fn render_tool_command_with_redaction_patterns(
    tool: &ToolDefinition,
    invocation: &ToolInvocation,
    redacted_key_patterns: &[String],
) -> Result<RenderedToolCommand, ToolError> {
    render_tool_command_with_resolver_and_redaction_patterns(
        tool,
        invocation,
        &EnvironmentSecretResolver,
        redacted_key_patterns,
    )
}

pub fn render_tool_command_with_resolver_and_redaction_patterns(
    tool: &ToolDefinition,
    invocation: &ToolInvocation,
    secret_resolver: &impl SecretResolver,
    redacted_key_patterns: &[String],
) -> Result<RenderedToolCommand, ToolError> {
    match &tool.kind {
        ToolKind::Shell => {}
        other => return Err(ToolError::UnsupportedKind(other.clone())),
    }
    let rendered = render_template(
        tool,
        invocation,
        secret_resolver,
        redacted_key_patterns,
        true,
    )?;

    Ok(RenderedToolCommand {
        command: rendered.text,
        default_cwd: rendered.default_cwd,
        redacted_values: rendered.redacted_values,
    })
}

pub fn render_tool_text_with_redaction_patterns(
    tool: &ToolDefinition,
    invocation: &ToolInvocation,
    redacted_key_patterns: &[String],
) -> Result<RenderedToolText, ToolError> {
    render_tool_text_with_resolver_and_redaction_patterns(
        tool,
        invocation,
        &EnvironmentSecretResolver,
        redacted_key_patterns,
    )
}

pub fn render_tool_text_with_resolver_and_redaction_patterns(
    tool: &ToolDefinition,
    invocation: &ToolInvocation,
    secret_resolver: &impl SecretResolver,
    redacted_key_patterns: &[String],
) -> Result<RenderedToolText, ToolError> {
    render_template(
        tool,
        invocation,
        secret_resolver,
        redacted_key_patterns,
        false,
    )
}

pub fn validate_tool_template(tool: &ToolDefinition) -> Result<(), ToolError> {
    template_placeholders(tool)?;
    Ok(())
}

pub fn validate_tool_invocation(
    tool: &ToolDefinition,
    invocation: &ToolInvocation,
) -> Result<(), ToolError> {
    let allowed_args = allowed_tool_arg_keys(tool)?;
    if let Some(key) = invocation
        .args
        .keys()
        .find(|key| invocation.secret_env_args.contains_key(*key))
    {
        return Err(ToolError::ConflictingArgument {
            tool: tool.id.to_string(),
            arg: key.to_owned(),
        });
    }
    for key in invocation
        .args
        .keys()
        .chain(invocation.secret_env_args.keys())
    {
        if !is_valid_tool_arg_key(key) {
            return Err(ToolError::InvalidArgumentKey {
                tool: tool.id.to_string(),
                arg: key.to_owned(),
            });
        }
        if !allowed_args.contains(key) {
            return Err(ToolError::UnexpectedArgument {
                tool: tool.id.to_string(),
                arg: key.to_owned(),
            });
        }
    }
    Ok(())
}

pub fn allowed_tool_arg_keys(tool: &ToolDefinition) -> Result<BTreeSet<String>, ToolError> {
    let mut allowed_args = template_placeholders(tool)?;
    if tool.kind == ToolKind::FileWrite {
        allowed_args.insert("body".into());
    }
    Ok(allowed_args)
}

pub fn resolve_tool_arg(
    tool: &ToolDefinition,
    invocation: &ToolInvocation,
    key: &str,
) -> Result<String, ToolError> {
    resolve_tool_arg_with_resolver(tool, invocation, key, &EnvironmentSecretResolver)
}

pub fn resolve_tool_arg_with_resolver(
    tool: &ToolDefinition,
    invocation: &ToolInvocation,
    key: &str,
    secret_resolver: &impl SecretResolver,
) -> Result<String, ToolError> {
    resolve_arg(tool, invocation, key, secret_resolver).map(|(value, _)| value)
}

fn template_placeholders(tool: &ToolDefinition) -> Result<BTreeSet<String>, ToolError> {
    let mut placeholders = BTreeSet::new();
    let mut chars = tool.command_template.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '}' {
            return Err(ToolError::InvalidTemplatePlaceholder {
                tool: tool.id.to_string(),
                placeholder: ch.to_string(),
            });
        }
        if ch != '{' {
            continue;
        }

        let mut key = String::new();
        let mut closed = false;
        for next in chars.by_ref() {
            if next == '}' {
                closed = true;
                break;
            }
            key.push(next);
        }

        if !closed {
            return Err(ToolError::UnclosedTemplatePlaceholder {
                tool: tool.id.to_string(),
                placeholder: format!("{{{key}"),
            });
        }
        validate_template_placeholder(tool, &key)?;
        placeholders.insert(key);
    }
    Ok(placeholders)
}

fn render_template(
    tool: &ToolDefinition,
    invocation: &ToolInvocation,
    secret_resolver: &impl SecretResolver,
    redacted_key_patterns: &[String],
    quote_values: bool,
) -> Result<RenderedToolText, ToolError> {
    let mut text = String::new();
    let mut redacted_values = Vec::new();
    let mut chars = tool.command_template.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '}' {
            return Err(ToolError::InvalidTemplatePlaceholder {
                tool: tool.id.to_string(),
                placeholder: ch.to_string(),
            });
        }
        if ch != '{' {
            text.push(ch);
            continue;
        }

        let mut key = String::new();
        let mut closed = false;
        for next in chars.by_ref() {
            if next == '}' {
                closed = true;
                break;
            }
            key.push(next);
        }

        if !closed {
            return Err(ToolError::UnclosedTemplatePlaceholder {
                tool: tool.id.to_string(),
                placeholder: format!("{{{key}"),
            });
        }
        validate_template_placeholder(tool, &key)?;

        let (value, secret) = resolve_arg(tool, invocation, &key, secret_resolver)?;
        if (secret || should_redact_key(&key, redacted_key_patterns)) && !value.is_empty() {
            redacted_values.push(value.clone());
        }
        if quote_values {
            text.push_str(&shell_quote(&value));
        } else {
            text.push_str(&value);
        }
    }

    Ok(RenderedToolText {
        text,
        default_cwd: tool.default_cwd.clone(),
        redacted_values,
    })
}

fn resolve_arg(
    tool: &ToolDefinition,
    invocation: &ToolInvocation,
    key: &str,
    secret_resolver: &impl SecretResolver,
) -> Result<(String, bool), ToolError> {
    if let Some(value) = invocation.args.get(key) {
        return Ok((value.clone(), false));
    }

    if let Some(reference) = invocation.secret_env_args.get(key) {
        if !is_valid_env_var_name(reference) && !reference.contains(':') {
            return Err(ToolError::InvalidSecretEnv {
                tool: tool.id.to_string(),
                arg: key.to_owned(),
                env: reference.clone(),
            });
        }
        let value = secret_resolver
            .resolve_secret(reference)
            .map_err(|error| tool_secret_error(tool, key, reference, error))?;
        return Ok((value, true));
    }

    Err(ToolError::MissingArgument {
        tool: tool.id.to_string(),
        arg: key.to_owned(),
    })
}

fn tool_secret_error(
    tool: &ToolDefinition,
    arg: &str,
    reference: &str,
    error: SecretResolveError,
) -> ToolError {
    match error {
        SecretResolveError::InvalidEnvironmentName(env) => ToolError::InvalidSecretEnv {
            tool: tool.id.to_string(),
            arg: arg.to_owned(),
            env,
        },
        SecretResolveError::MissingEnvironment(env) => ToolError::MissingSecretEnv {
            tool: tool.id.to_string(),
            arg: arg.to_owned(),
            env,
        },
        other => ToolError::SecretResolve {
            tool: tool.id.to_string(),
            arg: arg.to_owned(),
            reference: reference.to_owned(),
            message: other.to_string(),
        },
    }
}

fn should_redact_key(key: &str, redacted_key_patterns: &[String]) -> bool {
    let key = key.to_ascii_uppercase();
    redacted_key_patterns.iter().any(|pattern| {
        let pattern = pattern.trim();
        !pattern.is_empty() && key.contains(&pattern.to_ascii_uppercase())
    })
}

fn validate_template_placeholder(tool: &ToolDefinition, key: &str) -> Result<(), ToolError> {
    if !is_valid_tool_arg_key(key) || key.contains(['{', '}']) {
        return Err(ToolError::InvalidTemplatePlaceholder {
            tool: tool.id.to_string(),
            placeholder: key.to_owned(),
        });
    }
    Ok(())
}

pub fn is_valid_tool_arg_key(key: &str) -> bool {
    !key.trim().is_empty() && key.trim() == key && !key.contains('=') && !key.contains('\0')
}

fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".into();
    }

    let mut quoted = String::from("'");
    for ch in value.chars() {
        if ch == '\'' {
            quoted.push_str("'\\''");
        } else {
            quoted.push(ch);
        }
    }
    quoted.push('\'');
    quoted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ToolDefinition, ToolId, ToolInvocation, ToolKind};
    use std::collections::BTreeMap;

    #[test]
    fn renders_template_with_args() {
        let tool = ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "print",
            vec!["rust".into()],
            "printf {message}",
            None,
        );
        let invocation = ToolInvocation::new(
            ToolId::new("say"),
            BTreeMap::from([("message".into(), "hello; false".into())]),
        );

        let rendered = render_tool_command(&tool, &invocation).expect("render");

        assert_eq!(rendered.command, "printf 'hello; false'");
    }

    #[test]
    fn rejects_missing_template_arg() {
        let tool = ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "print",
            vec!["rust".into()],
            "printf {message}",
            None,
        );
        let invocation = ToolInvocation::new(ToolId::new("say"), BTreeMap::new());

        let error = render_tool_command(&tool, &invocation).expect_err("missing arg");

        assert!(error.to_string().contains("missing required tool argument"));
    }

    #[test]
    fn rejects_unexpected_tool_invocation_arg() {
        let tool = ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "print",
            vec!["rust".into()],
            "printf {message}",
            None,
        );
        let invocation = ToolInvocation::new(
            ToolId::new("say"),
            BTreeMap::from([
                ("message".into(), "hello".into()),
                ("unused".into(), "ignored".into()),
            ]),
        );

        let error = validate_tool_invocation(&tool, &invocation).expect_err("unexpected arg");

        assert!(
            error
                .to_string()
                .contains("unexpected tool argument `unused`")
        );
    }

    #[test]
    fn rejects_invalid_tool_invocation_arg_keys() {
        let tool = ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "print",
            vec!["rust".into()],
            "printf {message}",
            None,
        );
        let invocation = ToolInvocation::new(
            ToolId::new("say"),
            BTreeMap::from([(" message ".into(), "hello".into())]),
        );

        let error = validate_tool_invocation(&tool, &invocation).expect_err("invalid key");

        assert!(error.to_string().contains("invalid tool argument key"));
    }

    #[test]
    fn rejects_invalid_template_placeholder_names() {
        let tool = ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "print",
            vec!["rust".into()],
            "printf {bad=key}",
            None,
        );

        let error = validate_tool_template(&tool).expect_err("invalid placeholder");

        assert!(error.to_string().contains("malformed template placeholder"));
    }

    #[test]
    fn rejects_conflicting_plain_and_secret_tool_args() {
        let tool = ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "print",
            vec!["rust".into()],
            "printf {message}",
            None,
        );
        let invocation = ToolInvocation::with_secret_env_args(
            ToolId::new("say"),
            BTreeMap::from([("message".into(), "plain".into())]),
            BTreeMap::from([("message".into(), "MESSAGE_ENV".into())]),
        );

        let error = validate_tool_invocation(&tool, &invocation).expect_err("conflicting arg");

        assert!(
            error
                .to_string()
                .contains("cannot be both plain and secret")
        );
    }

    #[test]
    fn allows_file_write_body_arg_outside_path_template() {
        let tool = ToolDefinition::new(
            "write-note",
            ToolKind::FileWrite,
            "write",
            vec!["rust".into()],
            "notes/{name}.txt",
            None,
        );
        let invocation = ToolInvocation::new(
            ToolId::new("write-note"),
            BTreeMap::from([
                ("name".into(), "todo".into()),
                ("body".into(), "remember".into()),
            ]),
        );

        validate_tool_invocation(&tool, &invocation).expect("valid invocation");
    }

    #[test]
    fn resolves_plain_tool_arg() {
        let tool = ToolDefinition::new(
            "write-note",
            ToolKind::FileWrite,
            "write",
            vec!["rust".into()],
            "notes/{name}.txt",
            None,
        );
        let invocation = ToolInvocation::new(
            ToolId::new("write-note"),
            BTreeMap::from([("body".into(), "remember".into())]),
        );

        let value = resolve_tool_arg(&tool, &invocation, "body").expect("body");

        assert_eq!(value, "remember");
    }

    #[test]
    fn rejects_malformed_template_placeholder() {
        let tool = ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "print",
            vec!["rust".into()],
            "printf { message }",
            None,
        );
        let invocation = ToolInvocation::new(
            ToolId::new("say"),
            BTreeMap::from([("message".into(), "hello".into())]),
        );

        let error = render_tool_command(&tool, &invocation).expect_err("malformed placeholder");

        assert!(error.to_string().contains("malformed template placeholder"));
    }

    #[test]
    fn rejects_nested_template_placeholder_braces() {
        let tool = ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "print",
            vec!["rust".into()],
            "printf {{message}}",
            None,
        );
        let invocation = ToolInvocation::new(
            ToolId::new("say"),
            BTreeMap::from([("message".into(), "hello".into())]),
        );

        let error = render_tool_command(&tool, &invocation).expect_err("nested placeholder");

        assert!(error.to_string().contains("malformed template placeholder"));
    }

    #[test]
    fn rejects_unmatched_closing_template_brace() {
        let tool = ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "print",
            vec!["rust".into()],
            "printf {message}}",
            None,
        );
        let invocation = ToolInvocation::new(
            ToolId::new("say"),
            BTreeMap::from([("message".into(), "hello".into())]),
        );

        let error = render_tool_command(&tool, &invocation).expect_err("unmatched brace");

        assert!(error.to_string().contains("malformed template placeholder"));
    }

    #[test]
    fn rejects_unclosed_template_placeholder() {
        let tool = ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "print",
            vec!["rust".into()],
            "printf {message",
            None,
        );
        let invocation = ToolInvocation::new(
            ToolId::new("say"),
            BTreeMap::from([("message".into(), "hello".into())]),
        );

        let error = render_tool_command(&tool, &invocation).expect_err("unclosed placeholder");

        assert!(error.to_string().contains("unclosed template placeholder"));
    }

    #[test]
    fn rejects_invalid_secret_env_name_before_lookup() {
        let tool = ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "print",
            vec!["rust".into()],
            "printf {token}",
            None,
        );
        let invocation = ToolInvocation::with_secret_env_args(
            ToolId::new("say"),
            BTreeMap::new(),
            BTreeMap::from([("token".into(), " BAD_TOKEN ".into())]),
        );

        let error = render_tool_command(&tool, &invocation).expect_err("invalid env");

        assert!(error.to_string().contains("references invalid env var"));
    }

    #[test]
    fn empty_redaction_patterns_are_ignored_at_runtime() {
        let tool = ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "print",
            vec!["rust".into()],
            "printf {message}",
            None,
        );
        let invocation = ToolInvocation::new(
            ToolId::new("say"),
            BTreeMap::from([("message".into(), "hello".into())]),
        );

        let rendered =
            render_tool_command_with_redaction_patterns(&tool, &invocation, &[" ".into()])
                .expect("render");

        assert!(rendered.redacted_values.is_empty());
    }
}
