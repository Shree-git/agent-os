use crate::models::{SecretsBackend, SecretsBackendKind, is_valid_env_var_name};
use std::collections::BTreeMap;
use std::process::Command;
use thiserror::Error;

const DEFAULT_ENV_VAULT_VAR: &str = "AGENT_OS_ENV_VAULT";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedSecretReference {
    pub backend: Option<String>,
    pub name: String,
}

#[derive(Debug, Error)]
pub enum SecretResolveError {
    #[error("secret reference is empty")]
    EmptyReference,
    #[error("secret reference `{0}` is invalid")]
    InvalidReference(String),
    #[error("secret backend `{0}` was not found")]
    BackendNotFound(String),
    #[error("environment secret `{0}` is not a valid environment variable name")]
    InvalidEnvironmentName(String),
    #[error("environment secret `{0}` is not set")]
    MissingEnvironment(String),
    #[error(
        "env-vault backend `{backend}` reference `{reference}` is not a valid environment variable name"
    )]
    InvalidEnvVaultReference { backend: String, reference: String },
    #[error("env-vault backend `{backend}` environment `{reference}` is not set")]
    MissingEnvVault { backend: String, reference: String },
    #[error(
        "env-vault backend `{backend}` environment `{reference}` did not contain a JSON object"
    )]
    InvalidEnvVault { backend: String, reference: String },
    #[error("env-vault backend `{backend}` did not contain secret `{name}`")]
    MissingEnvVaultSecret { backend: String, name: String },
    #[error("secret backend `{backend}` command failed: {message}")]
    BackendCommandFailed { backend: String, message: String },
}

pub trait SecretResolver {
    fn resolve_secret(&self, reference: &str) -> Result<String, SecretResolveError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct EnvironmentSecretResolver;

impl SecretResolver for EnvironmentSecretResolver {
    fn resolve_secret(&self, reference: &str) -> Result<String, SecretResolveError> {
        let parsed = parse_secret_reference(reference)?;
        let name = parsed.name;
        resolve_environment_secret(&name)
    }
}

#[derive(Clone, Debug)]
pub struct OperatingSystemSecretResolver {
    backends: BTreeMap<String, SecretsBackend>,
}

impl OperatingSystemSecretResolver {
    pub fn new(backends: BTreeMap<String, SecretsBackend>) -> Self {
        Self { backends }
    }
}

impl SecretResolver for OperatingSystemSecretResolver {
    fn resolve_secret(&self, reference: &str) -> Result<String, SecretResolveError> {
        let parsed = parse_secret_reference(reference)?;
        match parsed.backend.as_deref() {
            None | Some("env") | Some("environment") => resolve_environment_secret(&parsed.name),
            Some(backend_id) => {
                let backend = self
                    .backends
                    .get(backend_id)
                    .ok_or_else(|| SecretResolveError::BackendNotFound(backend_id.to_owned()))?;
                resolve_backend_secret(backend, &parsed.name)
            }
        }
    }
}

pub fn parse_secret_reference(
    reference: &str,
) -> Result<ParsedSecretReference, SecretResolveError> {
    let reference = reference.trim();
    if reference.is_empty() {
        return Err(SecretResolveError::EmptyReference);
    }
    if reference.contains('\0') {
        return Err(SecretResolveError::InvalidReference(reference.to_owned()));
    }
    let Some((backend, name)) = reference.split_once(':') else {
        return Ok(ParsedSecretReference {
            backend: None,
            name: reference.to_owned(),
        });
    };
    let backend = backend.trim();
    let name = name.trim();
    if backend.is_empty() || name.is_empty() || backend.contains('/') {
        return Err(SecretResolveError::InvalidReference(reference.to_owned()));
    }
    Ok(ParsedSecretReference {
        backend: Some(backend.to_owned()),
        name: name.to_owned(),
    })
}

pub fn is_valid_secret_reference(reference: &str) -> bool {
    if reference != reference.trim() {
        return false;
    }
    let Ok(parsed) = parse_secret_reference(reference) else {
        return false;
    };
    match parsed.backend.as_deref() {
        None | Some("env") | Some("environment") => is_valid_env_var_name(&parsed.name),
        Some(_) => true,
    }
}

fn resolve_backend_secret(
    backend: &SecretsBackend,
    name: &str,
) -> Result<String, SecretResolveError> {
    match backend.kind {
        SecretsBackendKind::Environment => resolve_environment_secret(name),
        SecretsBackendKind::EnvVault => resolve_env_vault_secret(backend, name),
        SecretsBackendKind::OnePassword => resolve_one_password_secret(backend, name),
        SecretsBackendKind::OsKeychain => resolve_os_keychain_secret(backend, name),
    }
}

fn resolve_environment_secret(name: &str) -> Result<String, SecretResolveError> {
    if !is_valid_env_var_name(name) {
        return Err(SecretResolveError::InvalidEnvironmentName(name.to_owned()));
    }
    std::env::var(name).map_err(|_| SecretResolveError::MissingEnvironment(name.to_owned()))
}

fn resolve_env_vault_secret(
    backend: &SecretsBackend,
    name: &str,
) -> Result<String, SecretResolveError> {
    let reference = backend
        .reference
        .as_deref()
        .unwrap_or(DEFAULT_ENV_VAULT_VAR)
        .trim();
    if !is_valid_env_var_name(reference) {
        return Err(SecretResolveError::InvalidEnvVaultReference {
            backend: backend.id.clone(),
            reference: reference.to_owned(),
        });
    }
    let body = std::env::var(reference).map_err(|_| SecretResolveError::MissingEnvVault {
        backend: backend.id.clone(),
        reference: reference.to_owned(),
    })?;
    let value = serde_json::from_str::<serde_json::Value>(&body).map_err(|_| {
        SecretResolveError::InvalidEnvVault {
            backend: backend.id.clone(),
            reference: reference.to_owned(),
        }
    })?;
    let Some(object) = value.as_object() else {
        return Err(SecretResolveError::InvalidEnvVault {
            backend: backend.id.clone(),
            reference: reference.to_owned(),
        });
    };
    object
        .get(name)
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| SecretResolveError::MissingEnvVaultSecret {
            backend: backend.id.clone(),
            name: name.to_owned(),
        })
}

fn resolve_one_password_secret(
    backend: &SecretsBackend,
    name: &str,
) -> Result<String, SecretResolveError> {
    let reference = if name.starts_with("op://") {
        name.to_owned()
    } else if let Some(prefix) = backend.reference.as_deref() {
        format!("{}/{}", prefix.trim_end_matches('/'), name)
    } else {
        name.to_owned()
    };
    let output = Command::new("op")
        .args(["read", "--no-newline", &reference])
        .output()
        .map_err(|error| SecretResolveError::BackendCommandFailed {
            backend: backend.id.clone(),
            message: error.to_string(),
        })?;
    command_output_to_secret(backend, output)
}

fn resolve_os_keychain_secret(
    backend: &SecretsBackend,
    name: &str,
) -> Result<String, SecretResolveError> {
    let service = backend.reference.as_deref().unwrap_or(&backend.id);
    let output = Command::new("security")
        .args(["find-generic-password", "-s", service, "-a", name, "-w"])
        .output()
        .map_err(|error| SecretResolveError::BackendCommandFailed {
            backend: backend.id.clone(),
            message: error.to_string(),
        })?;
    command_output_to_secret(backend, output)
}

fn command_output_to_secret(
    backend: &SecretsBackend,
    output: std::process::Output,
) -> Result<String, SecretResolveError> {
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout)
            .trim_end_matches(['\r', '\n'])
            .to_owned());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    Err(SecretResolveError::BackendCommandFailed {
        backend: backend.id.clone(),
        message: if stderr.is_empty() {
            format!("exit status {}", output.status)
        } else {
            stderr
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_reference_validation_rejects_padded_environment_names() {
        assert!(is_valid_secret_reference("AGENT_OS_TOKEN"));
        assert!(!is_valid_secret_reference(" AGENT_OS_TOKEN"));
        assert!(!is_valid_secret_reference("AGENT_OS_TOKEN "));
    }
}
