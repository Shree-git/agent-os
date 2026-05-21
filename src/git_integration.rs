use serde::Serialize;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GitCommandOutput {
    pub command: Vec<String>,
    pub cwd: String,
    pub status: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub dry_run: bool,
}

#[derive(Debug, Error)]
pub enum GitIntegrationError {
    #[error("git cwd must not be empty")]
    EmptyCwd,
    #[error("could not resolve current directory: {0}")]
    CurrentDir(#[source] std::io::Error),
    #[error("could not run {program} in {cwd}: {source}")]
    CommandIo {
        program: String,
        cwd: String,
        source: std::io::Error,
    },
    #[error("{program} failed in {cwd}: {stderr}")]
    CommandFailed {
        program: String,
        cwd: String,
        stderr: String,
    },
}

impl GitIntegrationError {
    pub fn stderr(&self) -> Option<&str> {
        match self {
            Self::CommandFailed { stderr, .. } => Some(stderr),
            _ => None,
        }
    }
}

pub fn resolve_git_cwd(cwd: Option<PathBuf>) -> Result<PathBuf, GitIntegrationError> {
    let cwd = match cwd {
        Some(cwd) => cwd,
        None => std::env::current_dir().map_err(GitIntegrationError::CurrentDir)?,
    };
    validate_git_cwd(&cwd)?;
    Ok(cwd)
}

pub fn validate_git_cwd(cwd: &Path) -> Result<(), GitIntegrationError> {
    if cwd.as_os_str().is_empty() || cwd.to_string_lossy().trim().is_empty() {
        return Err(GitIntegrationError::EmptyCwd);
    }
    Ok(())
}

pub fn run_git_capture(cwd: &Path, args: &[&str]) -> Result<String, GitIntegrationError> {
    let output = run_git_command(cwd, args, false)?;
    Ok(output.stdout)
}

pub fn run_git_command(
    cwd: &Path,
    args: &[&str],
    dry_run: bool,
) -> Result<GitCommandOutput, GitIntegrationError> {
    run_external_command(cwd, "git", args, dry_run)
}

pub fn run_external_command(
    cwd: &Path,
    program: &str,
    args: &[&str],
    dry_run: bool,
) -> Result<GitCommandOutput, GitIntegrationError> {
    validate_git_cwd(cwd)?;
    let command = std::iter::once(program.to_owned())
        .chain(args.iter().map(|arg| (*arg).to_owned()))
        .collect::<Vec<_>>();
    if dry_run {
        return Ok(GitCommandOutput {
            command,
            cwd: cwd.display().to_string(),
            status: None,
            stdout: String::new(),
            stderr: String::new(),
            dry_run: true,
        });
    }
    let output = std::process::Command::new(program)
        .args(args)
        .current_dir(cwd)
        .output()
        .map_err(|source| GitIntegrationError::CommandIo {
            program: program.to_owned(),
            cwd: cwd.display().to_string(),
            source,
        })?;
    let result = GitCommandOutput {
        command,
        cwd: cwd.display().to_string(),
        status: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        dry_run: false,
    };
    if !output.status.success() {
        return Err(GitIntegrationError::CommandFailed {
            program: program.to_owned(),
            cwd: cwd.display().to_string(),
            stderr: result.stderr.trim().to_owned(),
        });
    }
    Ok(result)
}
