use crate::models::{
    AgentId, ApprovalRequest, AutonomyLevel, EventKind, NetworkMode, OperatingSystem, Policy,
    Priority, ProviderKind, RunArtifact, RunArtifactKind, RunId, RunRecord, RunStatus, Task,
    TaskId, TaskStatus, ToolDefinition, ToolId, ToolInvocation, ToolKind,
};
use crate::policy::{
    PolicyError, check_file_write_path, check_shell_command, check_shell_writes, check_workspace,
};
use crate::process_tree::terminate_child_process_tree;
use crate::providers::{
    AgentProvider, AgentResponse, ProviderError, ProviderRequest, ProviderRuntime, ProviderToolCall,
};
use crate::runtime::{Runtime, RuntimeError};
use crate::secrets::OperatingSystemSecretResolver;
use crate::store::{Store, StoreError};
use crate::tools::{
    RenderedToolText, ToolError, render_tool_command_with_resolver_and_redaction_patterns,
    render_tool_text_with_resolver_and_redaction_patterns, resolve_tool_arg_with_resolver,
    validate_tool_invocation,
};
use chrono::Utc;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use thiserror::Error;

const LIVE_LOG_TRUNCATED_MARKER: &str = "\n[output truncated]\n";

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("task not found: {0}")]
    TaskNotFound(TaskId),
    #[error("task has no command: {0}")]
    MissingCommand(TaskId),
    #[error(transparent)]
    Tool(#[from] ToolError),
    #[error(transparent)]
    Policy(#[from] PolicyError),
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error("failed to execute command for task {task_id}: {source}")]
    Io {
        task_id: TaskId,
        source: std::io::Error,
    },
    #[error("execution worker panicked")]
    WorkerPanicked,
}

pub struct CommandExecutor;

enum PreparedWork {
    Shell(PreparedShellRun),
    FileRead(PreparedFileRun),
    FileWrite(PreparedFileWriteRun),
    Provider(PreparedProviderRun),
}

struct PreparedShellRun {
    run: RunRecord,
    store: Store,
    log_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    command: String,
    cwd_path: PathBuf,
    max_output_bytes: usize,
    timeout: Duration,
    env: Vec<(String, String)>,
    redacted_values: Vec<String>,
    process_isolation: bool,
}

struct PreparedProviderRun {
    run: RunRecord,
    request: ProviderRequest,
    provider: ProviderRuntime,
}

struct PreparedFileRun {
    run: RunRecord,
    path: PathBuf,
    redacted_path: String,
    max_output_bytes: usize,
}

struct PreparedFileWriteRun {
    run: RunRecord,
    store: Store,
    policy: Policy,
    path: PathBuf,
    redacted_path: String,
    body: String,
    max_output_bytes: usize,
}

struct LiveLog {
    file: File,
    body: String,
    max_bytes: usize,
    truncated: bool,
}

impl LiveLog {
    fn new(file: File, max_bytes: usize) -> Self {
        Self {
            file,
            body: String::new(),
            max_bytes,
            truncated: false,
        }
    }

    fn append(&mut self, text: &str) -> std::io::Result<()> {
        if text.is_empty() || self.truncated {
            return Ok(());
        }
        let remaining = self.max_bytes.saturating_sub(self.body.len());
        let (text, truncated) = if text.len() <= remaining {
            (text, false)
        } else {
            let mut end = remaining.min(text.len());
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            (&text[..end], true)
        };
        if !text.is_empty() {
            self.file.write_all(text.as_bytes())?;
            self.body.push_str(text);
        }
        if truncated {
            self.file.write_all(LIVE_LOG_TRUNCATED_MARKER.as_bytes())?;
            self.body.push_str(LIVE_LOG_TRUNCATED_MARKER);
            self.truncated = true;
        }
        self.file.flush()
    }
}

struct FinishedRun {
    run: RunRecord,
    log: String,
    response: Option<AgentResponse>,
    failure_message: Option<String>,
}

impl CommandExecutor {
    pub fn execute_task(
        os: &mut OperatingSystem,
        store: &Store,
        task_id: &TaskId,
    ) -> Result<RunRecord, ExecutorError> {
        let mut results = Self::execute_tasks_parallel(os, store, std::slice::from_ref(task_id));
        results
            .pop()
            .unwrap_or_else(|| Err(ExecutorError::TaskNotFound(task_id.clone())))
    }

    pub fn execute_tasks_parallel(
        os: &mut OperatingSystem,
        store: &Store,
        task_ids: &[TaskId],
    ) -> Vec<Result<RunRecord, ExecutorError>> {
        let mut results = Vec::new();
        let mut prepared_runs = Vec::new();

        for task_id in task_ids {
            match prepare_work(os, store, task_id) {
                Ok(prepared) => prepared_runs.push(prepared),
                Err(error) => results.push(Err(error)),
            }
        }

        if let Err(error) = store.save_validated(os) {
            results.push(Err(error.into()));
            return results;
        }

        let handles = prepared_runs
            .into_iter()
            .map(|prepared| thread::spawn(move || execute_prepared_work(prepared)))
            .collect::<Vec<_>>();

        for handle in handles {
            let finished = match handle.join() {
                Ok(result) => result,
                Err(_) => {
                    results.push(Err(ExecutorError::WorkerPanicked));
                    continue;
                }
            };

            match finished.and_then(|finished| finish_run(os, store, finished)) {
                Ok(run) => results.push(Ok(run)),
                Err(error) => results.push(Err(error)),
            }
        }

        results
    }
}

fn prepare_work(
    os: &mut OperatingSystem,
    store: &Store,
    task_id: &TaskId,
) -> Result<PreparedWork, ExecutorError> {
    let task = os
        .tasks
        .get(task_id)
        .cloned()
        .ok_or_else(|| ExecutorError::TaskNotFound(task_id.clone()))?;
    if task.status == TaskStatus::Pending {
        if let Some(task) = os.tasks.get_mut(task_id) {
            task.attempts = task.attempts.saturating_add(1);
            task.updated_at = Utc::now();
        }
        os.record(
            EventKind::TaskUpdated,
            format!("started task {} direct execution attempt", task_id),
        );
    }
    let agent_id = task.assigned_to.clone();
    let command_override = task.command.clone();
    let cwd_override = task.cwd.clone();
    let tool_invocation = task.tool.clone();
    let secret_resolver = OperatingSystemSecretResolver::new(os.secrets_backends.clone());
    let tool_work = if let Some(invocation) = tool_invocation {
        let tool = os.tools.get(&invocation.tool_id).cloned();
        let Some(tool) = tool else {
            let message = format!("tool not found: {}", invocation.tool_id);
            reject_run_message(
                os,
                store,
                task_id,
                agent_id.clone(),
                format!("tool:{}", invocation.tool_id),
                cwd_override.clone().unwrap_or_else(|| ".".into()),
                &message,
            )?;
            return Err(ToolError::NotFound(invocation.tool_id.to_string()).into());
        };
        if let Err(error) = validate_tool_invocation(&tool, &invocation) {
            let message = error.to_string();
            reject_run_message(
                os,
                store,
                task_id,
                agent_id.clone(),
                format!("tool:{}", invocation.tool_id),
                cwd_override
                    .clone()
                    .or_else(|| tool.default_cwd.clone())
                    .unwrap_or_else(|| ".".into()),
                &message,
            )?;
            return Err(error.into());
        }
        match tool.kind {
            ToolKind::Shell => match render_tool_command_with_resolver_and_redaction_patterns(
                &tool,
                &invocation,
                &secret_resolver,
                &os.policy.redacted_env_patterns,
            ) {
                Ok(rendered) => Some(ToolWork::Shell(rendered)),
                Err(error) => {
                    let message = error.to_string();
                    reject_run_message(
                        os,
                        store,
                        task_id,
                        agent_id.clone(),
                        format!("tool:{}", invocation.tool_id),
                        cwd_override
                            .clone()
                            .or_else(|| tool.default_cwd.clone())
                            .unwrap_or_else(|| ".".into()),
                        &message,
                    )?;
                    return Err(error.into());
                }
            },
            ToolKind::FileRead | ToolKind::FileWrite => {
                match render_tool_text_with_resolver_and_redaction_patterns(
                    &tool,
                    &invocation,
                    &secret_resolver,
                    &os.policy.redacted_env_patterns,
                ) {
                    Ok(rendered) => {
                        let body = if tool.kind == ToolKind::FileWrite {
                            match resolve_tool_arg_with_resolver(
                                &tool,
                                &invocation,
                                "body",
                                &secret_resolver,
                            ) {
                                Ok(body) => Some(body),
                                Err(ToolError::MissingArgument { .. }) => None,
                                Err(error) => {
                                    let message = error.to_string();
                                    reject_run_message(
                                        os,
                                        store,
                                        task_id,
                                        agent_id.clone(),
                                        format!("tool:{}", invocation.tool_id),
                                        cwd_override
                                            .clone()
                                            .or_else(|| tool.default_cwd.clone())
                                            .unwrap_or_else(|| ".".into()),
                                        &message,
                                    )?;
                                    return Err(error.into());
                                }
                            }
                        } else {
                            None
                        };
                        Some(ToolWork::File {
                            tool_kind: tool.kind.clone(),
                            rendered,
                            body,
                        })
                    }
                    Err(error) => {
                        let message = error.to_string();
                        reject_run_message(
                            os,
                            store,
                            task_id,
                            agent_id.clone(),
                            format!("tool:{}", invocation.tool_id),
                            cwd_override
                                .clone()
                                .or_else(|| tool.default_cwd.clone())
                                .unwrap_or_else(|| ".".into()),
                            &message,
                        )?;
                        return Err(error.into());
                    }
                }
            }
        }
    } else {
        None
    };

    if let Some(ToolWork::File {
        tool_kind,
        rendered,
        body,
    }) = tool_work.clone()
    {
        return prepare_file_work(
            os,
            store,
            FileToolPreparation {
                task_id: task_id.clone(),
                agent_id,
                cwd_override,
                tool_kind,
                rendered,
                body,
            },
        );
    }

    let shell_work = tool_work.and_then(|work| match work {
        ToolWork::Shell(rendered) => Some(rendered),
        ToolWork::File { .. } => None,
    });

    let Some(command) = shell_work
        .as_ref()
        .map(|work| work.command.clone())
        .or_else(|| command_override.clone())
    else {
        let agent = agent_id
            .as_ref()
            .and_then(|agent_id| os.agents.get(agent_id));
        let tools = os.tools.values().cloned().collect::<Vec<_>>();
        let memory = os.provider_memory_for_task(&task, chrono::Utc::now());
        let request = ProviderRequest::with_ordered_context_limited(
            agent,
            &task,
            &tools,
            &memory,
            os.memory_policy.max_provider_memories,
        );
        let provider_command = format!("provider:{}", os.provider.kind);
        if matches!(
            os.policy.autonomy,
            AutonomyLevel::ObserveOnly | AutonomyLevel::Suggest
        ) {
            let message = format!(
                "provider execution is disabled by autonomy level {}",
                autonomy_label(&os.policy.autonomy)
            );
            reject_run_message_with_event(
                os,
                store,
                RunRejection {
                    task_id: task_id.clone(),
                    agent_id,
                    command: provider_command,
                    cwd: cwd_override.clone().unwrap_or_else(|| ".".into()),
                    message,
                    event_kind: EventKind::RunFinished,
                },
            )?;
            return Err(
                PolicyError::Rejected("provider execution is disabled by autonomy".into()).into(),
            );
        }
        if os.policy.network.mode == NetworkMode::Disabled && os.provider.kind != ProviderKind::Mock
        {
            let message = "network access is disabled for providers".to_string();
            reject_run_message_with_event(
                os,
                store,
                RunRejection {
                    task_id: task_id.clone(),
                    agent_id,
                    command: provider_command,
                    cwd: cwd_override.clone().unwrap_or_else(|| ".".into()),
                    message,
                    event_kind: EventKind::RunFinished,
                },
            )?;
            return Err(
                PolicyError::Rejected("network access is disabled for providers".into()).into(),
            );
        }
        let provider = match ProviderRuntime::from_settings(&os.provider) {
            Ok(provider) => provider,
            Err(error) => {
                let message = error.to_string();
                reject_run_message_with_event(
                    os,
                    store,
                    RunRejection {
                        task_id: task_id.clone(),
                        agent_id,
                        command: provider_command,
                        cwd: cwd_override.clone().unwrap_or_else(|| ".".into()),
                        message,
                        event_kind: EventKind::RunFinished,
                    },
                )?;
                return Err(error.into());
            }
        };
        let mut run = RunRecord::new(
            task_id.clone(),
            agent_id,
            provider_command,
            cwd_override.clone().unwrap_or_else(|| ".".into()),
        );
        os.ensure_unique_run_id(&mut run);
        os.record(
            EventKind::RunStarted,
            format!("started provider run {} for task {}", run.id, task_id),
        );
        os.runs.insert(run.id.clone(), run.clone());
        return Ok(PreparedWork::Provider(PreparedProviderRun {
            run,
            request,
            provider,
        }));
    };

    let cwd_path = cwd_override
        .as_ref()
        .or_else(|| {
            shell_work
                .as_ref()
                .and_then(|work| work.default_cwd.as_ref())
        })
        .map(PathBuf::from)
        .unwrap_or(std::env::current_dir().map_err(|source| ExecutorError::Io {
            task_id: task_id.clone(),
            source,
        })?);
    let cwd = cwd_path.display().to_string();
    let redacted_values = shell_work
        .as_ref()
        .map(|work| work.redacted_values.clone())
        .unwrap_or_default();
    let recorded_command = redact_values(command.clone(), &redacted_values);

    if let Err(error) = check_shell_command(&os.policy, &command) {
        if let PolicyError::ApprovalRequired(_) = &error
            && approval_already_granted(os, task_id, &recorded_command)
        {
            os.record(
                EventKind::ApprovalResolved,
                format!("approved action reused for task {}", task_id),
            );
        } else {
            if let PolicyError::ApprovalRequired(reason) = &error {
                os.request_approval(ApprovalRequest::new(
                    task_id.clone(),
                    None,
                    recorded_command.clone(),
                    reason.clone(),
                ));
            }
            reject_run(os, store, task_id, agent_id, recorded_command, cwd, &error)?;
            return Err(error.into());
        }
    }

    if let Err(error) = check_workspace(&os.policy, &cwd_path) {
        reject_run(os, store, task_id, agent_id, recorded_command, cwd, &error)?;
        return Err(error.into());
    }
    if let Err(error) = check_shell_writes(&os.policy, &command, &cwd_path) {
        reject_run(os, store, task_id, agent_id, recorded_command, cwd, &error)?;
        return Err(error.into());
    }

    let mut run = RunRecord::new(task_id.clone(), agent_id, recorded_command, cwd);
    os.ensure_unique_run_id(&mut run);
    let log_path = store.run_log_path(&run.id);
    let stdout_path = store.run_artifact_path(&run.id, "stdout.log");
    let stderr_path = store.run_artifact_path(&run.id, "stderr.log");
    run.log_path = Some(log_path.display().to_string());
    os.record(
        EventKind::RunStarted,
        format!("started run {} for task {}", run.id, task_id),
    );
    os.runs.insert(run.id.clone(), run.clone());

    Ok(PreparedWork::Shell(PreparedShellRun {
        run,
        store: store.clone(),
        log_path,
        stdout_path,
        stderr_path,
        command,
        cwd_path,
        max_output_bytes: os.policy.max_output_bytes,
        timeout: Duration::from_secs(os.policy.command_timeout_seconds),
        env: build_task_environment(&os.policy),
        redacted_values: redacted_environment_values(&os.policy)
            .into_iter()
            .chain(redacted_values)
            .collect(),
        process_isolation: os.policy.sandbox.process_isolation,
    }))
}

#[derive(Clone)]
enum ToolWork {
    Shell(crate::tools::RenderedToolCommand),
    File {
        tool_kind: ToolKind,
        rendered: RenderedToolText,
        body: Option<String>,
    },
}

struct FileToolPreparation {
    task_id: TaskId,
    agent_id: Option<AgentId>,
    cwd_override: Option<String>,
    tool_kind: ToolKind,
    rendered: RenderedToolText,
    body: Option<String>,
}

fn prepare_file_work(
    os: &mut OperatingSystem,
    store: &Store,
    preparation: FileToolPreparation,
) -> Result<PreparedWork, ExecutorError> {
    let FileToolPreparation {
        task_id,
        agent_id,
        cwd_override,
        tool_kind,
        rendered,
        body,
    } = preparation;
    let redacted_values = rendered.redacted_values.clone();
    let base = cwd_override
        .as_ref()
        .or(rendered.default_cwd.as_ref())
        .map(PathBuf::from)
        .unwrap_or(std::env::current_dir().map_err(|source| ExecutorError::Io {
            task_id: task_id.clone(),
            source,
        })?);
    let path = base.join(&rendered.text);
    let path_display = path.display().to_string();
    let redacted_path = redact_values(path_display.clone(), &redacted_values);
    let command = format!("{} {}", tool_kind, path_display);
    let recorded_command = redact_values(command.clone(), &redacted_values);

    if tool_kind == ToolKind::Shell {
        let error = ToolError::UnsupportedKind(tool_kind);
        reject_run_message(
            os,
            store,
            &task_id,
            agent_id,
            recorded_command,
            redacted_path,
            &error.to_string(),
        )?;
        return Err(error.into());
    }

    let workspace_check = if tool_kind == ToolKind::FileWrite {
        check_file_write_path(&os.policy, &path)
    } else {
        check_workspace(&os.policy, &path)
    };
    if let Err(error) = workspace_check {
        let error = redact_policy_error(error, &redacted_values);
        reject_run(
            os,
            store,
            &task_id,
            agent_id,
            recorded_command,
            redacted_path,
            &error,
        )?;
        return Err(error.into());
    }

    let file_write_body = if tool_kind == ToolKind::FileWrite {
        match body {
            Some(body) => Some(body),
            None => {
                let message = "missing required file-write tool argument `body`";
                reject_run_message(
                    os,
                    store,
                    &task_id,
                    agent_id,
                    recorded_command,
                    redacted_path,
                    message,
                )?;
                return Err(ToolError::MissingArgument {
                    tool: "file-write".into(),
                    arg: "body".into(),
                }
                .into());
            }
        }
    } else {
        None
    };

    let mut run = RunRecord::new(
        task_id.clone(),
        agent_id,
        recorded_command,
        redacted_path.clone(),
    );
    os.ensure_unique_run_id(&mut run);
    os.record(
        EventKind::RunStarted,
        format!("started file tool run {} for task {}", run.id, task_id),
    );
    os.runs.insert(run.id.clone(), run.clone());

    match tool_kind {
        ToolKind::FileRead => Ok(PreparedWork::FileRead(PreparedFileRun {
            run,
            path,
            redacted_path,
            max_output_bytes: os.policy.max_output_bytes,
        })),
        ToolKind::FileWrite => {
            let body = file_write_body.ok_or_else(|| ToolError::MissingArgument {
                tool: "file-write".into(),
                arg: "body".into(),
            })?;
            Ok(PreparedWork::FileWrite(PreparedFileWriteRun {
                run,
                store: store.clone(),
                policy: os.policy.clone(),
                path,
                redacted_path,
                body,
                max_output_bytes: os.policy.max_output_bytes,
            }))
        }
        ToolKind::Shell => Err(ToolError::UnsupportedKind(ToolKind::Shell).into()),
    }
}

fn execute_prepared_work(prepared: PreparedWork) -> Result<FinishedRun, ExecutorError> {
    match prepared {
        PreparedWork::Shell(run) => execute_shell(run),
        PreparedWork::FileRead(run) => execute_file_read(run),
        PreparedWork::FileWrite(run) => execute_file_write(run),
        PreparedWork::Provider(run) => execute_provider(run),
    }
}

fn execute_shell(mut prepared: PreparedShellRun) -> Result<FinishedRun, ExecutorError> {
    let started = Instant::now();
    create_parent_dir(&prepared.run.task_id, &prepared.log_path)?;
    create_parent_dir(&prepared.run.task_id, &prepared.stdout_path)?;
    create_parent_dir(&prepared.run.task_id, &prepared.stderr_path)?;
    let initial_log = format!(
        "$ {}\n{}\n[stdout]\n",
        prepared.run.command,
        run_trace_log_line(&prepared.run)
    );
    let live_log = open_live_sink(
        &prepared.run.task_id,
        &prepared.log_path,
        prepared.max_output_bytes,
    )?;
    let stdout_artifact = open_live_sink(
        &prepared.run.task_id,
        &prepared.stdout_path,
        prepared.max_output_bytes,
    )?;
    let stderr_artifact = open_live_sink(
        &prepared.run.task_id,
        &prepared.stderr_path,
        prepared.max_output_bytes,
    )?;
    append_live_log(&live_log, &initial_log).map_err(|source| ExecutorError::Io {
        task_id: prepared.run.task_id.clone(),
        source,
    })?;

    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(&prepared.command)
        .current_dir(&prepared.cwd_path)
        .env_clear()
        .envs(prepared.env.iter().map(|(key, value)| (key, value)))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    if prepared.process_isolation {
        command.process_group(0);
    }
    let mut child = command.spawn().map_err(|source| ExecutorError::Io {
        task_id: prepared.run.task_id.clone(),
        source,
    })?;
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdout_reader = stdout.map(|stdout| {
        spawn_pipe_reader(
            stdout,
            live_log.clone(),
            Some(stdout_artifact.clone()),
            prepared.redacted_values.clone(),
            None,
        )
    });
    let stderr_reader = stderr.map(|stderr| {
        spawn_pipe_reader(
            stderr,
            live_log.clone(),
            Some(stderr_artifact.clone()),
            prepared.redacted_values.clone(),
            Some("\n[stderr]\n"),
        )
    });
    let mut timed_out = false;
    let mut cancelled = false;
    loop {
        if child
            .try_wait()
            .map_err(|source| ExecutorError::Io {
                task_id: prepared.run.task_id.clone(),
                source,
            })?
            .is_some()
        {
            break;
        }
        if started.elapsed() >= prepared.timeout {
            timed_out = true;
            terminate_child_process_tree(&mut child, prepared.process_isolation).map_err(
                |source| ExecutorError::Io {
                    task_id: prepared.run.task_id.clone(),
                    source,
                },
            )?;
            break;
        }
        if cancel_requested(&prepared.store, &prepared.run.id) {
            cancelled = true;
            terminate_child_process_tree(&mut child, prepared.process_isolation).map_err(
                |source| ExecutorError::Io {
                    task_id: prepared.run.task_id.clone(),
                    source,
                },
            )?;
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let status = child.wait().map_err(|source| ExecutorError::Io {
        task_id: prepared.run.task_id.clone(),
        source,
    })?;
    if !timed_out && !cancelled {
        join_pipe_reader(stdout_reader);
        join_pipe_reader(stderr_reader);
    }

    if timed_out {
        append_live_log(
            &live_log,
            &format!(
                "\n[timeout]\ncommand exceeded {} second(s)\n",
                prepared.timeout.as_secs()
            ),
        )
        .map_err(|source| ExecutorError::Io {
            task_id: prepared.run.task_id.clone(),
            source,
        })?;
    }
    if cancelled {
        append_live_log(&live_log, "\n[cancelled]\nrun cancellation requested\n").map_err(
            |source| ExecutorError::Io {
                task_id: prepared.run.task_id.clone(),
                source,
            },
        )?;
    }

    prepared.run.exit_code = if timed_out || cancelled {
        None
    } else {
        status.code()
    };
    prepared.run.finished_at = Some(Utc::now());
    prepared.run.status = if cancelled {
        RunStatus::Cancelled
    } else if !timed_out && status.success() {
        RunStatus::Success
    } else {
        RunStatus::Failed
    };
    let log = live_log
        .lock()
        .map(|state| state.body.clone())
        .unwrap_or_default();
    prepared.run.artifacts =
        shell_run_artifacts(&prepared.run, &prepared.stdout_path, &prepared.stderr_path);

    Ok(FinishedRun {
        run: prepared.run,
        log: truncate_bytes(&log, prepared.max_output_bytes),
        response: None,
        failure_message: None,
    })
}

fn spawn_pipe_reader<R: Read + Send + 'static>(
    mut reader: R,
    live_log: Arc<Mutex<LiveLog>>,
    artifact: Option<Arc<Mutex<LiveLog>>>,
    redacted_values: Vec<String>,
    header: Option<&'static str>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        if let Some(header) = header {
            let _ = append_live_log(&live_log, header);
        }
        let mut buffer = [0u8; 8192];
        loop {
            let Ok(read) = reader.read(&mut buffer) else {
                break;
            };
            if read == 0 {
                break;
            }
            let chunk = String::from_utf8_lossy(&buffer[..read]).to_string();
            let chunk = redact_values(chunk, &redacted_values);
            if let Some(artifact) = &artifact {
                let _ = append_live_log(artifact, &chunk);
            }
            let _ = append_live_log(&live_log, &chunk);
        }
    })
}

fn create_parent_dir(task_id: &TaskId, path: &Path) -> Result<(), ExecutorError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| ExecutorError::Io {
            task_id: task_id.clone(),
            source,
        })?;
    }
    Ok(())
}

fn open_live_sink(
    task_id: &TaskId,
    path: &Path,
    max_output_bytes: usize,
) -> Result<Arc<Mutex<LiveLog>>, ExecutorError> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|source| ExecutorError::Io {
            task_id: task_id.clone(),
            source,
        })?;
    Ok(Arc::new(Mutex::new(LiveLog::new(file, max_output_bytes))))
}

fn append_live_log(live_log: &Arc<Mutex<LiveLog>>, text: &str) -> std::io::Result<()> {
    if let Ok(mut state) = live_log.lock() {
        state.append(text)?;
    }
    Ok(())
}

fn join_pipe_reader(reader: Option<thread::JoinHandle<()>>) {
    if let Some(reader) = reader {
        let _ = reader.join();
    }
}

fn cancel_requested(store: &Store, run_id: &RunId) -> bool {
    store
        .load()
        .ok()
        .and_then(|os| os.runs.get(run_id).map(|run| run.status.clone()))
        .map(|status| matches!(status, RunStatus::CancelRequested | RunStatus::Cancelled))
        .unwrap_or(false)
}

fn run_trace_log_line(run: &RunRecord) -> String {
    format!("trace_id: {}\n", run.trace_id)
}

fn execute_file_read(mut prepared: PreparedFileRun) -> Result<FinishedRun, ExecutorError> {
    let result = std::fs::read_to_string(&prepared.path);
    let (status, exit_code, log) = match result {
        Ok(body) => (
            RunStatus::Success,
            Some(0),
            format!(
                "file-read {}\n{}\n[content]\n{}",
                prepared.redacted_path,
                run_trace_log_line(&prepared.run),
                body
            ),
        ),
        Err(error) => (
            RunStatus::Failed,
            None,
            format!(
                "file-read {}\n{}\n[error]\n{}",
                prepared.redacted_path,
                run_trace_log_line(&prepared.run),
                error
            ),
        ),
    };
    prepared.run.status = status;
    prepared.run.exit_code = exit_code;
    prepared.run.finished_at = Some(Utc::now());

    Ok(FinishedRun {
        run: prepared.run,
        log: truncate_bytes(&log, prepared.max_output_bytes),
        response: None,
        failure_message: None,
    })
}

fn execute_file_write(mut prepared: PreparedFileWriteRun) -> Result<FinishedRun, ExecutorError> {
    let mut preflight = prepared.store.load()?;
    let mut candidate_run = prepared.run.clone();
    candidate_run.status = RunStatus::Success;
    candidate_run.exit_code = Some(0);
    candidate_run.finished_at = Some(Utc::now());
    candidate_run.log_path = Some(
        prepared
            .store
            .run_log_path(&candidate_run.id)
            .display()
            .to_string(),
    );
    Runtime::complete_task(
        &mut preflight,
        &candidate_run.task_id,
        Some(format!(
            "run {} succeeded with exit code 0",
            candidate_run.id
        )),
    )?;
    preflight
        .runs
        .insert(candidate_run.id.clone(), candidate_run.clone());
    preflight.record(
        EventKind::RunFinished,
        format!(
            "finished run {} with status {}",
            candidate_run.id, candidate_run.status
        ),
    );
    prepared.store.validate_for_save(&preflight)?;

    let result = write_file_tool_body(&prepared.policy, &prepared.path, prepared.body.as_bytes());
    let (status, exit_code, log) = match result {
        Ok(()) => (
            RunStatus::Success,
            Some(0),
            format!(
                "file-write {}\n{}\n[wrote]\n{} bytes\n",
                prepared.redacted_path,
                run_trace_log_line(&prepared.run),
                prepared.body.len()
            ),
        ),
        Err(error) => (
            RunStatus::Failed,
            None,
            format!(
                "file-write {}\n{}\n[error]\n{}",
                prepared.redacted_path,
                run_trace_log_line(&prepared.run),
                error
            ),
        ),
    };
    prepared.run.status = status;
    prepared.run.exit_code = exit_code;
    prepared.run.finished_at = Some(Utc::now());

    Ok(FinishedRun {
        run: prepared.run,
        log: truncate_bytes(&log, prepared.max_output_bytes),
        response: None,
        failure_message: None,
    })
}

fn write_file_tool_body(policy: &Policy, path: &Path, body: &[u8]) -> Result<(), ExecutorError> {
    check_file_write_path(policy, path)?;
    let destination = match path.symlink_metadata() {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            path.canonicalize().map_err(|source| StoreError::Io {
                path: path.to_path_buf(),
                source,
            })?
        }
        _ => path.to_path_buf(),
    };
    Store::write_file_atomic(&destination, body)?;
    Ok(())
}

fn build_task_environment(policy: &crate::models::Policy) -> Vec<(String, String)> {
    let mut env = std::env::vars()
        .filter(|(key, _)| {
            policy.inherit_environment
                || policy.allowed_env_vars.iter().any(|allowed| allowed == key)
        })
        .collect::<Vec<_>>();
    env.sort_by(|left, right| left.0.cmp(&right.0));
    env
}

fn redacted_environment_values(policy: &crate::models::Policy) -> Vec<String> {
    let env = build_task_environment(policy);
    env.into_iter()
        .filter(|(key, value)| {
            !value.is_empty()
                && policy.redacted_env_patterns.iter().any(|pattern| {
                    let pattern = pattern.trim();
                    !pattern.is_empty()
                        && key
                            .to_ascii_uppercase()
                            .contains(&pattern.to_ascii_uppercase())
                })
        })
        .map(|(_, value)| value)
        .collect()
}

fn redact_values(mut value: String, redacted_values: &[String]) -> String {
    for secret in redacted_values {
        if !secret.is_empty() {
            value = value.replace(secret, "[redacted]");
        }
    }
    value
}

fn redact_policy_error(error: PolicyError, redacted_values: &[String]) -> PolicyError {
    match error {
        PolicyError::ShellDisabled => PolicyError::ShellDisabled,
        PolicyError::ApprovalRequired(message) => {
            PolicyError::ApprovalRequired(redact_values(message, redacted_values))
        }
        PolicyError::AutonomyRestricted(message) => {
            PolicyError::AutonomyRestricted(redact_values(message, redacted_values))
        }
        PolicyError::Rejected(message) => {
            PolicyError::Rejected(redact_values(message, redacted_values))
        }
        PolicyError::WorkspaceRejected(message) => {
            PolicyError::WorkspaceRejected(redact_values(message, redacted_values))
        }
    }
}

fn shell_run_artifacts(
    run: &RunRecord,
    stdout_path: &Path,
    stderr_path: &Path,
) -> Vec<RunArtifact> {
    vec![
        RunArtifact {
            kind: RunArtifactKind::Stdout,
            path: stdout_path.display().to_string(),
            bytes: std::fs::metadata(stdout_path)
                .ok()
                .map(|metadata| metadata.len()),
            content_type: Some("text/plain".into()),
        },
        RunArtifact {
            kind: RunArtifactKind::Stderr,
            path: stderr_path.display().to_string(),
            bytes: std::fs::metadata(stderr_path)
                .ok()
                .map(|metadata| metadata.len()),
            content_type: Some("text/plain".into()),
        },
        RunArtifact::new(RunArtifactKind::Summary, format!("run:{}:summary", run.id)),
    ]
}

fn autonomy_label(level: &AutonomyLevel) -> &'static str {
    match level {
        AutonomyLevel::ObserveOnly => "observe-only",
        AutonomyLevel::Suggest => "suggest",
        AutonomyLevel::ExecuteWithApproval => "execute-with-approval",
        AutonomyLevel::ExecuteFreely => "execute-freely",
    }
}

fn approval_already_granted(os: &OperatingSystem, task_id: &TaskId, action: &str) -> bool {
    os.approvals.values().any(|approval| {
        approval.task_id == *task_id
            && approval.action == action
            && approval.status == crate::models::ApprovalStatus::Approved
    })
}

fn approval_action_granted(os: &OperatingSystem, action: &str) -> bool {
    os.approvals.values().any(|approval| {
        approval.action == action && approval.status == crate::models::ApprovalStatus::Approved
    })
}

fn approval_already_pending(os: &OperatingSystem, task_id: &TaskId, action: &str) -> bool {
    os.approvals.values().any(|approval| {
        approval.task_id == *task_id
            && approval.action == action
            && approval.status == crate::models::ApprovalStatus::Pending
    })
}

fn execute_provider(mut prepared: PreparedProviderRun) -> Result<FinishedRun, ExecutorError> {
    let provider = prepared
        .run
        .command
        .strip_prefix("provider:")
        .unwrap_or(&prepared.run.command)
        .to_owned();
    let response = match prepared.provider.complete(&prepared.request) {
        Ok(response) => response,
        Err(error) => {
            let message = error.to_string();
            prepared.run.status = RunStatus::Failed;
            prepared.run.exit_code = None;
            prepared.run.finished_at = Some(Utc::now());
            let log = format!(
                "provider: {}\n{}\nagent: {} ({})\ntask: {}\n\n[error]\n{}\n",
                provider,
                run_trace_log_line(&prepared.run),
                prepared.request.agent_name,
                prepared.request.agent_kind,
                prepared.request.task_title,
                message
            );
            return Ok(FinishedRun {
                run: prepared.run,
                log,
                response: None,
                failure_message: Some(message),
            });
        }
    };
    prepared.run.status = RunStatus::Success;
    prepared.run.exit_code = Some(0);
    prepared.run.finished_at = Some(Utc::now());

    let log = format!(
        "provider: {}\n{}\nagent: {} ({})\ntask: {}\nconfidence: {}\n\nsummary:\n{}\n\nplan:\n{}\n",
        provider,
        run_trace_log_line(&prepared.run),
        prepared.request.agent_name,
        prepared.request.agent_kind,
        prepared.request.task_title,
        response.confidence,
        response.summary,
        response.plan.join("\n"),
    );
    let log = if response.tool_calls.is_empty() {
        log
    } else {
        format!(
            "{}\ntool calls:\n{}\n",
            log,
            response
                .tool_calls
                .iter()
                .map(|call| format!("- {}", call.tool))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };

    Ok(FinishedRun {
        run: prepared.run,
        log,
        response: Some(response),
        failure_message: None,
    })
}

fn finish_run(
    os: &mut OperatingSystem,
    store: &Store,
    finished: FinishedRun,
) -> Result<RunRecord, ExecutorError> {
    let mut run = finished.run;
    let run_id = run.id.clone();
    let task_id = run.task_id.clone();
    let log_path = store.run_log_path(&run_id);
    run.log_path = Some(log_path.display().to_string());
    let log = finished.log;
    let run_for_update = run.clone();
    let response = finished.response;
    let failure_message = finished.failure_message;
    let (updated_run, updated_os) = store.update(|latest| {
        if let Some(response) = &response
            && let Some(task) = latest.tasks.get_mut(&task_id)
        {
            task.plan = response.plan.clone();
        }

        match run_for_update.status {
            RunStatus::Success => {
                let tool_calls = response
                    .as_ref()
                    .map(|response| response.tool_calls.clone())
                    .unwrap_or_default();
                Runtime::complete_task(
                    latest,
                    &task_id,
                    Some(
                        response
                            .as_ref()
                            .map(|response| response.summary.clone())
                            .unwrap_or_else(|| {
                                format!("run {} succeeded with exit code 0", run_id)
                            }),
                    ),
                )?;
                materialize_provider_tool_calls(latest, &task_id, &tool_calls);
            }
            RunStatus::Cancelled => {
                Runtime::cancel_task(
                    latest,
                    &task_id,
                    Some(format!("run {} was cancelled", run_id)),
                )?;
            }
            _ => {
                let failure_message = failure_message.unwrap_or_else(|| {
                    format!(
                        "run {} failed with exit code {:?}",
                        run_id, run_for_update.exit_code
                    )
                });
                Runtime::fail_task_or_retry(latest, &task_id, Some(failure_message))?;
            }
        }

        latest.runs.insert(run_id.clone(), run_for_update.clone());
        latest.record(
            EventKind::RunFinished,
            format!(
                "finished run {} with status {}",
                run_id, run_for_update.status
            ),
        );
        Ok::<_, ExecutorError>((run_for_update.clone(), latest.clone()))
    })?;
    store.write_run_log(&run_id, &log)?;
    *os = updated_os;

    Ok(updated_run)
}

fn materialize_provider_tool_calls(
    os: &mut OperatingSystem,
    parent_task_id: &TaskId,
    tool_calls: &[ProviderToolCall],
) {
    if tool_calls.is_empty() {
        return;
    }
    let priority = os
        .tasks
        .get(parent_task_id)
        .map(|task| task.priority)
        .unwrap_or(Priority::Normal);
    for call in tool_calls {
        let tool_id = ToolId::new(&call.tool);
        if tool_id.as_str().is_empty() {
            os.record(
                EventKind::PolicyRejected,
                format!(
                    "provider requested malformed tool id `{}` for task {}",
                    call.tool, parent_task_id
                ),
            );
            continue;
        }
        if let Some(empty_key) = call.args.keys().find(|key| key.trim().is_empty()) {
            os.record(
                EventKind::PolicyRejected,
                format!(
                    "provider requested tool {} for task {} with empty argument key `{}`",
                    tool_id, parent_task_id, empty_key
                ),
            );
            continue;
        }
        if let Some(secret_key) = call
            .args
            .keys()
            .find(|key| key_matches_redacted_patterns(key, &os.policy.redacted_env_patterns))
        {
            os.record(
                EventKind::PolicyRejected,
                format!(
                    "provider requested tool {} for task {} with redacted argument key `{}`",
                    tool_id, parent_task_id, secret_key
                ),
            );
            continue;
        }
        let Some(tool) = os.tools.get(&tool_id) else {
            os.record(
                EventKind::TaskUpdated,
                format!(
                    "provider requested missing tool {} for task {}",
                    tool_id, parent_task_id
                ),
            );
            continue;
        };
        let invocation = ToolInvocation::new(tool_id, call.args.clone());
        if let Err(error) = validate_tool_invocation(tool, &invocation) {
            os.record(
                EventKind::PolicyRejected,
                format!(
                    "provider requested invalid tool invocation for task {}: {}",
                    parent_task_id, error
                ),
            );
            continue;
        }
        let action = provider_tool_call_action(&tool.id, &call.args);
        if let Err(error) = preflight_provider_tool_call_policy(os, tool, &invocation, &action) {
            os.record(
                EventKind::PolicyRejected,
                format!(
                    "provider requested tool {} for task {} rejected by policy: {}",
                    tool.id, parent_task_id, error
                ),
            );
            continue;
        }
        if provider_tool_call_requires_approval(&os.policy)
            && !approval_already_granted(os, parent_task_id, &action)
        {
            if !approval_already_pending(os, parent_task_id, &action) {
                os.request_approval(ApprovalRequest::new(
                    parent_task_id.clone(),
                    None,
                    action,
                    format!("provider requested tool task {}", tool.id),
                ));
            }
            continue;
        }
        let mut task = Task::new(
            format!("Tool call: {}", tool.name),
            format!(
                "Run provider-requested tool {} after task {}",
                tool.id, parent_task_id
            ),
            priority,
            tool.required_capabilities.clone(),
        );
        task.dependencies.push(parent_task_id.clone());
        task.tool = Some(invocation);
        os.create_task(task);
    }
}

fn preflight_provider_tool_call_policy(
    os: &OperatingSystem,
    tool: &ToolDefinition,
    invocation: &ToolInvocation,
    action: &str,
) -> Result<(), String> {
    let secret_resolver = OperatingSystemSecretResolver::new(os.secrets_backends.clone());
    match tool.kind {
        ToolKind::Shell => {
            let rendered = render_tool_command_with_resolver_and_redaction_patterns(
                tool,
                invocation,
                &secret_resolver,
                &os.policy.redacted_env_patterns,
            )
            .map_err(|error| error.to_string())?;
            match check_shell_command(&os.policy, &rendered.command) {
                Ok(_) => {}
                Err(PolicyError::ApprovalRequired(_)) if approval_action_granted(os, action) => {}
                Err(PolicyError::ApprovalRequired(_)) => {}
                Err(error) => return Err(error.to_string()),
            }
            let cwd = rendered
                .default_cwd
                .as_deref()
                .map(PathBuf::from)
                .map(Ok)
                .unwrap_or_else(std::env::current_dir)
                .map_err(|error| error.to_string())?;
            check_workspace(&os.policy, &cwd).map_err(|error| error.to_string())?;
            check_shell_writes(&os.policy, &rendered.command, &cwd)
                .map_err(|error| error.to_string())?;
        }
        ToolKind::FileRead | ToolKind::FileWrite => {
            let rendered = render_tool_text_with_resolver_and_redaction_patterns(
                tool,
                invocation,
                &secret_resolver,
                &os.policy.redacted_env_patterns,
            )
            .map_err(|error| error.to_string())?;
            if tool.kind == ToolKind::FileWrite && !invocation.args.contains_key("body") {
                return Err("missing required file-write tool argument `body`".into());
            }
            let base = rendered
                .default_cwd
                .as_deref()
                .map(PathBuf::from)
                .map(Ok)
                .unwrap_or_else(std::env::current_dir)
                .map_err(|error| error.to_string())?;
            let path = base.join(rendered.text);
            let decision = if tool.kind == ToolKind::FileWrite {
                check_file_write_path(&os.policy, &path)
            } else {
                check_workspace(&os.policy, &path)
            };
            decision.map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

fn key_matches_redacted_patterns(key: &str, patterns: &[String]) -> bool {
    let key = key.to_ascii_uppercase();
    patterns.iter().any(|pattern| {
        let pattern = pattern.trim();
        !pattern.is_empty() && key.contains(&pattern.to_ascii_uppercase())
    })
}

fn provider_tool_call_requires_approval(policy: &Policy) -> bool {
    matches!(policy.autonomy, AutonomyLevel::ExecuteWithApproval)
        || policy.approval.require_for_risky_actions
}

fn provider_tool_call_action(
    tool_id: &ToolId,
    args: &std::collections::BTreeMap<String, String>,
) -> String {
    let args = args
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("provider-tool:{tool_id} args:{args}")
}

fn reject_run(
    os: &mut OperatingSystem,
    store: &Store,
    task_id: &TaskId,
    agent_id: Option<AgentId>,
    command: String,
    cwd: String,
    error: &PolicyError,
) -> Result<(), ExecutorError> {
    reject_run_message(
        os,
        store,
        task_id,
        agent_id,
        command,
        cwd,
        &error.to_string(),
    )
}

fn reject_run_message(
    os: &mut OperatingSystem,
    store: &Store,
    task_id: &TaskId,
    agent_id: Option<AgentId>,
    command: String,
    cwd: String,
    message: &str,
) -> Result<(), ExecutorError> {
    reject_run_message_with_event(
        os,
        store,
        RunRejection {
            task_id: task_id.clone(),
            agent_id,
            command,
            cwd,
            message: message.to_owned(),
            event_kind: EventKind::PolicyRejected,
        },
    )
}

struct RunRejection {
    task_id: TaskId,
    agent_id: Option<AgentId>,
    command: String,
    cwd: String,
    message: String,
    event_kind: EventKind,
}

fn reject_run_message_with_event(
    os: &mut OperatingSystem,
    store: &Store,
    rejection: RunRejection,
) -> Result<(), ExecutorError> {
    let mut candidate = os.clone();
    let mut run = RunRecord::new(
        rejection.task_id.clone(),
        rejection.agent_id,
        rejection.command,
        rejection.cwd,
    );
    candidate.ensure_unique_run_id(&mut run);
    run.status = RunStatus::Rejected;
    run.finished_at = Some(Utc::now());
    candidate.runs.insert(run.id.clone(), run.clone());
    Runtime::block_task(
        &mut candidate,
        &rejection.task_id,
        Some(rejection.message.clone()),
    )?;
    candidate.record(
        rejection.event_kind,
        format!(
            "rejected run {} for task {}: {}",
            run.id, rejection.task_id, rejection.message
        ),
    );
    store.save_validated(&candidate)?;
    *os = candidate;
    Ok(())
}

fn truncate_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }

    let mut end = max_bytes.saturating_sub(32).min(value.len());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}\n[output truncated]\n", &value[..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        AutonomyLevel, MemoryRecord, OperatingSystem, Priority, ProviderKind, ProviderSettings,
        Task, TaskStatus, ToolDefinition,
    };
    #[cfg(unix)]
    use crate::process_tree::{
        collect_descendant_pids, collect_process_group_pids, process_exists,
    };
    use std::collections::BTreeMap;

    #[cfg(unix)]
    #[test]
    fn process_tree_termination_kills_spawned_children() {
        let mut child = std::process::Command::new("sh")
            .arg("-c")
            .arg("sleep 30 & wait")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn shell");
        std::thread::sleep(std::time::Duration::from_millis(100));
        let descendants = collect_descendant_pids(child.id());
        assert!(
            !descendants.is_empty(),
            "test command should have a child process"
        );

        terminate_child_process_tree(&mut child, false).expect("terminate tree");
        let _ = child.wait();
        std::thread::sleep(std::time::Duration::from_millis(100));

        assert!(
            descendants.into_iter().all(|pid| !process_exists(pid)),
            "descendant process should be gone"
        );
    }

    #[cfg(unix)]
    #[test]
    fn process_isolation_terminates_spawned_process_group() {
        let mut command = std::process::Command::new("sh");
        command
            .arg("-c")
            .arg("sleep 30 & wait")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .process_group(0);
        let mut child = command.spawn().expect("spawn isolated shell");
        std::thread::sleep(std::time::Duration::from_millis(100));
        let group_members = collect_process_group_pids(child.id());
        assert!(
            group_members.contains(&child.id()),
            "isolated shell should become its own process group leader"
        );
        assert!(
            group_members.len() > 1,
            "isolated command should have a child in its process group"
        );

        terminate_child_process_tree(&mut child, true).expect("terminate process group");
        let _ = child.wait();
        std::thread::sleep(std::time::Duration::from_millis(100));

        assert!(
            group_members.into_iter().all(|pid| !process_exists(pid)),
            "process group members should be gone"
        );
    }

    #[test]
    fn shell_logs_include_trace_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let task = Task::new("Shell trace", "trace shell logs", Priority::Normal, vec![]);
        let run = RunRecord::new(task.id.clone(), None, "printf shell-trace", ".");
        let trace_id = run.trace_id.clone();

        let finished = execute_shell(PreparedShellRun {
            run,
            store,
            log_path: dir.path().join("run.log"),
            stdout_path: dir.path().join("stdout.txt"),
            stderr_path: dir.path().join("stderr.txt"),
            command: "printf shell-trace".into(),
            cwd_path: dir.path().to_path_buf(),
            max_output_bytes: 1024,
            timeout: Duration::from_secs(5),
            env: Vec::new(),
            redacted_values: Vec::new(),
            process_isolation: false,
        })
        .expect("shell execution");

        assert_eq!(finished.run.status, RunStatus::Success);
        assert!(finished.log.contains(&format!("trace_id: {trace_id}")));
        assert!(finished.log.contains("shell-trace"));
    }

    #[test]
    fn provider_run_records_configured_provider_kind() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("test");
        os.provider = ProviderSettings {
            kind: ProviderKind::OpenAiCompatible,
            endpoint: Some("http://127.0.0.1:1/v1/chat/completions".into()),
            ..ProviderSettings::default()
        };
        let task = Task::new("Plan", "plan work", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);

        let prepared = prepare_work(&mut os, &store, &task_id).expect("prepared work");
        let PreparedWork::Provider(provider_run) = prepared else {
            panic!("expected provider work");
        };

        assert_eq!(provider_run.run.command, "provider:openai-compatible");
        assert!(
            os.runs.values().any(|run| {
                run.task_id == task_id && run.command == "provider:openai-compatible"
            })
        );
    }

    #[test]
    fn misconfigured_provider_rejection_does_not_leave_partial_memory_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("test");
        os.provider = ProviderSettings {
            kind: ProviderKind::OpenAiCompatible,
            endpoint: None,
            ..ProviderSettings::default()
        };
        let task = Task::new("Plan", "plan work", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);

        let error = match prepare_work(&mut os, &store, &task_id) {
            Ok(_) => panic!("expected provider config error"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            ExecutorError::Store(StoreError::InvalidState { .. })
        ));
        assert!(error.to_string().contains("provider endpoint is required"));
        let task = os.tasks.get(&task_id).expect("task");
        assert_eq!(task.status, TaskStatus::Pending);
        assert!(task.output.is_none());
        assert!(os.runs.is_empty());
    }

    #[test]
    fn file_write_prepare_rejects_missing_body_without_started_run() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let workspace = dir.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let mut os = OperatingSystem::new("test");
        os.policy.allowed_workspaces = vec![workspace.display().to_string()];
        os.register_tool(ToolDefinition::new(
            "write-note",
            ToolKind::FileWrite,
            "write a note",
            vec![],
            "note.txt",
            Some(workspace.display().to_string()),
        ));
        let mut task = Task::new("Write", "write file", Priority::Normal, vec![]);
        task.tool = Some(ToolInvocation::new(
            ToolId::new("write-note"),
            BTreeMap::new(),
        ));
        let task_id = task.id.clone();
        os.create_task(task);
        store.save_validated(&os).expect("save state");

        let error = match prepare_work(&mut os, &store, &task_id) {
            Ok(_) => panic!("missing body should reject file-write preparation"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            ExecutorError::Tool(ToolError::MissingArgument { tool, arg })
                if tool == "file-write" && arg == "body"
        ));
        assert_eq!(os.runs.len(), 1);
        assert!(
            os.runs
                .values()
                .all(|run| run.status == RunStatus::Rejected)
        );
        let task = os.tasks.get(&task_id).expect("task");
        assert_eq!(task.status, TaskStatus::Blocked);
        assert!(!workspace.join("note.txt").exists());
    }

    #[test]
    fn execution_prepare_rejects_invalid_state_without_overwriting_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        store
            .save(&OperatingSystem::new("persisted"))
            .expect("initial save");

        let mut os = OperatingSystem::new("invalid");
        os.name = " ".into();
        let mut task = Task::new("Run", "run command", Priority::Normal, vec![]);
        task.command = Some("printf ok".into());
        let task_id = task.id.clone();
        os.create_task(task);

        let mut results = CommandExecutor::execute_tasks_parallel(&mut os, &store, &[task_id]);

        let error = results.pop().expect("result").expect_err("invalid state");
        assert!(matches!(
            error,
            ExecutorError::Store(StoreError::InvalidState { .. })
        ));
        let loaded = store.load().expect("load");
        assert_eq!(loaded.name, "persisted");
        assert!(loaded.runs.is_empty());
        assert!(!dir.path().join("runs").exists());
    }

    #[test]
    fn finish_run_rejects_invalid_response_without_writing_log() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("test");
        let task = Task::new("Plan", "plan work", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);
        store.save(&os).expect("save initial state");

        let mut run = RunRecord::new(task_id.clone(), None, "provider:mock", ".");
        run.status = RunStatus::Success;
        run.exit_code = Some(0);
        run.finished_at = Some(Utc::now());
        let run_id = run.id.clone();
        let response = AgentResponse {
            summary: "done".into(),
            plan: vec![" ".into()],
            confidence: 80,
            tool_calls: vec![],
        };

        let error = finish_run(
            &mut os,
            &store,
            FinishedRun {
                run,
                log: "provider log".into(),
                response: Some(response),
                failure_message: None,
            },
        )
        .expect_err("invalid response should not persist");

        assert!(matches!(
            error,
            ExecutorError::Store(StoreError::InvalidState { .. })
        ));
        assert!(!store.run_log_path(&run_id).exists());
        assert!(os.runs.is_empty());
        let task = os.tasks.get(&task_id).expect("task");
        assert!(task.plan.is_empty());
        assert_eq!(task.status, TaskStatus::Pending);
        let loaded = store.load().expect("load");
        assert!(loaded.runs.is_empty());
        assert!(loaded.tasks.get(&task_id).expect("task").plan.is_empty());
    }

    #[test]
    fn finish_run_preserves_concurrent_state_changes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut scheduled_os = OperatingSystem::new("test");
        let task = Task::new("Run", "run work", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        scheduled_os.create_task(task);
        store.save(&scheduled_os).expect("save initial state");

        store
            .update(|latest| {
                latest.write_memory(MemoryRecord::new(
                    "operator-note",
                    "created while run was active",
                    vec![],
                ));
                Ok::<_, StoreError>(())
            })
            .expect("concurrent update");

        let mut run = RunRecord::new(task_id.clone(), None, "printf ok", ".");
        run.status = RunStatus::Success;
        run.exit_code = Some(0);
        run.finished_at = Some(Utc::now());

        finish_run(
            &mut scheduled_os,
            &store,
            FinishedRun {
                run,
                log: "ok".into(),
                response: None,
                failure_message: None,
            },
        )
        .expect("finish run");

        let loaded = store.load().expect("load");
        assert_eq!(loaded.memory.len(), 1);
        assert_eq!(loaded.memory[0].topic, "operator-note");
        assert_eq!(
            loaded.tasks.get(&task_id).expect("task").status,
            TaskStatus::Complete
        );
        assert_eq!(loaded.runs.len(), 1);
    }

    #[test]
    fn file_write_rejects_invalid_state_without_writing_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let target = dir.path().join("workspace").join("note.txt");
        std::fs::create_dir_all(target.parent().expect("target parent")).expect("workspace");
        let mut os = OperatingSystem::new(" ");
        let task = Task::new("Write", "write file", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);
        store
            .save_unchecked(&os)
            .expect("save invalid state fixture");

        let run = RunRecord::new(
            task_id,
            None,
            "file-write note",
            target.display().to_string(),
        );

        let error = match execute_file_write(PreparedFileWriteRun {
            run,
            store,
            policy: Policy {
                allowed_workspaces: vec![dir.path().join("workspace").display().to_string()],
                ..Policy::default()
            },
            path: target.clone(),
            redacted_path: target.display().to_string(),
            body: "should not write".into(),
            max_output_bytes: 1024,
        }) {
            Ok(_) => panic!("invalid state should block file write"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            ExecutorError::Store(StoreError::InvalidState { .. })
        ));
        assert!(!target.exists());
    }

    #[test]
    fn file_write_rejects_missing_task_without_writing_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let target = dir.path().join("workspace").join("note.txt");
        std::fs::create_dir_all(target.parent().expect("target parent")).expect("workspace");
        let os = OperatingSystem::new("agent-os");
        store.save_validated(&os).expect("save state");

        let task = Task::new("Write", "write file", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        let run = RunRecord::new(
            task_id.clone(),
            None,
            "file-write note",
            target.display().to_string(),
        );

        let error = match execute_file_write(PreparedFileWriteRun {
            run,
            store,
            policy: Policy {
                allowed_workspaces: vec![dir.path().join("workspace").display().to_string()],
                ..Policy::default()
            },
            path: target.clone(),
            redacted_path: target.display().to_string(),
            body: "should not write".into(),
            max_output_bytes: 1024,
        }) {
            Ok(_) => panic!("missing task should block file write"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            ExecutorError::Runtime(RuntimeError::TaskNotFound(missing)) if missing == task_id
        ));
        assert!(!target.exists());
    }

    #[test]
    fn file_tool_logs_include_trace_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let workspace = dir.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let source = workspace.join("note.txt");
        std::fs::write(&source, "hello trace").expect("source");
        let mut os = OperatingSystem::new("test");
        let task = Task::new("Trace files", "read and write", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);
        store.save_validated(&os).expect("save state");

        let read_run = RunRecord::new(task_id.clone(), None, "file-read note", ".");
        let read_trace = read_run.trace_id.clone();
        let read = execute_file_read(PreparedFileRun {
            run: read_run,
            path: source,
            redacted_path: "note.txt".into(),
            max_output_bytes: 1024,
        })
        .expect("file read");
        assert!(read.log.contains(&format!("trace_id: {read_trace}")));

        let target = workspace.join("written.txt");
        let write_run = RunRecord::new(task_id, None, "file-write written", ".");
        let write_trace = write_run.trace_id.clone();
        let write = execute_file_write(PreparedFileWriteRun {
            run: write_run,
            store,
            policy: Policy {
                allowed_workspaces: vec![workspace.display().to_string()],
                ..Policy::default()
            },
            path: target.clone(),
            redacted_path: "written.txt".into(),
            body: "hello".into(),
            max_output_bytes: 1024,
        })
        .expect("file write");
        assert!(write.log.contains(&format!("trace_id: {write_trace}")));
        assert_eq!(std::fs::read_to_string(target).expect("written"), "hello");
    }

    #[test]
    fn provider_log_uses_run_provider_kind() {
        let task = Task::new("Plan", "plan work", Priority::Normal, vec![]);
        let run = RunRecord::new(task.id.clone(), None, "provider:openai-compatible", ".");
        let trace_id = run.trace_id.clone();
        let request = ProviderRequest::new(None, &task);

        let finished = execute_provider(PreparedProviderRun {
            run,
            request,
            provider: ProviderRuntime::Mock(crate::providers::MockProvider),
        })
        .expect("provider execution");

        assert!(finished.log.contains("provider: openai-compatible"));
        assert!(finished.log.contains(&format!("trace_id: {trace_id}")));
    }

    #[test]
    fn provider_request_failure_finishes_run_and_fails_task() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("test");
        os.provider = ProviderSettings {
            kind: ProviderKind::OpenAiCompatible,
            endpoint: Some("http://127.0.0.1:1/v1/chat/completions".into()),
            ..ProviderSettings::default()
        };
        let task = Task::new("Plan", "plan work", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);

        let run = CommandExecutor::execute_task(&mut os, &store, &task_id)
            .expect("provider failure should be finalized");

        assert_eq!(run.status, RunStatus::Failed);
        assert_eq!(run.command, "provider:openai-compatible");
        let task = os.tasks.get(&task_id).expect("task");
        assert_eq!(task.status, TaskStatus::Failed);
        assert!(
            task.output
                .as_deref()
                .is_some_and(|output| output.contains("provider request failed"))
        );
        let log_path = run.log_path.as_ref().expect("log path");
        let log = std::fs::read_to_string(log_path).expect("run log");
        assert!(log.contains("provider: openai-compatible"));
        assert!(log.contains(&format!("trace_id: {}", run.trace_id)));
        assert!(log.contains("[error]"));
    }

    #[test]
    fn provider_tool_calls_create_dependent_tool_tasks() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("test");
        os.policy.allow_shell = true;
        os.policy.autonomy = AutonomyLevel::ExecuteFreely;
        os.policy.approval.require_for_risky_actions = false;
        os.register_tool(ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "say something",
            vec!["plan".into()],
            "printf {message}",
            None,
        ));
        let parent = Task::new("Plan", "plan work", Priority::High, vec!["plan".into()]);
        let parent_id = parent.id.clone();
        os.create_task(parent);
        store.save(&os).expect("save state");

        let response = AgentResponse {
            summary: "done".into(),
            plan: vec!["call a tool".into()],
            confidence: 91,
            tool_calls: vec![ProviderToolCall {
                tool: "say".into(),
                args: BTreeMap::from([("message".into(), "hello".into())]),
            }],
        };
        let mut run = RunRecord::new(parent_id.clone(), None, "provider:mock", ".");
        run.status = RunStatus::Success;
        run.exit_code = Some(0);
        run.finished_at = Some(Utc::now());
        finish_run(
            &mut os,
            &store,
            FinishedRun {
                run,
                log: "provider log".into(),
                response: Some(response),
                failure_message: None,
            },
        )
        .expect("finish run");

        let parent = os.tasks.get(&parent_id).expect("parent task");
        assert_eq!(parent.status, TaskStatus::Complete);
        assert_eq!(parent.plan, vec!["call a tool"]);

        let created_tool_task = os
            .tasks
            .values()
            .find(|task| task.id != parent_id && task.tool.is_some())
            .expect("tool task");
        assert_eq!(created_tool_task.priority, Priority::High);
        assert_eq!(created_tool_task.required_capabilities, vec!["plan"]);
        assert_eq!(created_tool_task.dependencies, vec![parent_id]);
        let invocation = created_tool_task.tool.as_ref().expect("invocation");
        assert_eq!(invocation.tool_id, ToolId::new("say"));
        assert_eq!(invocation.args.get("message"), Some(&"hello".to_owned()));
    }

    #[test]
    fn provider_tool_calls_reject_shell_tools_disabled_by_policy_before_creating_task() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("test");
        os.register_tool(ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "say something",
            vec![],
            "printf {message}",
            None,
        ));
        let parent = Task::new("Plan", "plan work", Priority::Normal, vec![]);
        let parent_id = parent.id.clone();
        os.create_task(parent);
        store.save(&os).expect("save state");

        let response = AgentResponse {
            summary: "done".into(),
            plan: vec!["call a tool".into()],
            confidence: 91,
            tool_calls: vec![ProviderToolCall {
                tool: "say".into(),
                args: BTreeMap::from([("message".into(), "hello".into())]),
            }],
        };
        let mut run = RunRecord::new(parent_id.clone(), None, "provider:mock", ".");
        run.status = RunStatus::Success;
        run.exit_code = Some(0);
        run.finished_at = Some(Utc::now());

        finish_run(
            &mut os,
            &store,
            FinishedRun {
                run,
                log: "provider log".into(),
                response: Some(response),
                failure_message: None,
            },
        )
        .expect("finish run");

        assert!(os.tasks.values().all(|task| task.tool.is_none()));
        assert!(os.events.iter().any(|event| {
            event.kind == EventKind::PolicyRejected
                && event
                    .message
                    .contains("shell execution is disabled by policy")
        }));
    }

    #[test]
    fn provider_tool_calls_queue_approval_before_materializing_tool_tasks() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("test");
        os.policy.allow_shell = true;
        os.policy.approval.require_for_risky_actions = true;
        os.register_tool(ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "say something",
            vec!["plan".into()],
            "printf {message}",
            None,
        ));
        let parent = Task::new("Plan", "plan work", Priority::High, vec!["plan".into()]);
        let parent_id = parent.id.clone();
        os.create_task(parent);
        store.save(&os).expect("save state");

        let response = AgentResponse {
            summary: "done".into(),
            plan: vec!["call a tool".into()],
            confidence: 91,
            tool_calls: vec![ProviderToolCall {
                tool: "say".into(),
                args: BTreeMap::from([("message".into(), "hello".into())]),
            }],
        };
        let mut run = RunRecord::new(parent_id.clone(), None, "provider:mock", ".");
        run.status = RunStatus::Success;
        run.exit_code = Some(0);
        run.finished_at = Some(Utc::now());

        finish_run(
            &mut os,
            &store,
            FinishedRun {
                run,
                log: "provider log".into(),
                response: Some(response),
                failure_message: None,
            },
        )
        .expect("finish run");

        assert!(os.tasks.values().all(|task| task.tool.is_none()));
        let approval = os
            .approvals
            .values()
            .find(|approval| approval.task_id == parent_id)
            .expect("approval request");
        assert_eq!(approval.status, crate::models::ApprovalStatus::Pending);
        assert_eq!(
            approval.action,
            "provider-tool:say args:message=hello".to_owned()
        );
        assert!(approval.reason.contains("provider requested tool task say"));
    }

    #[test]
    fn provider_tool_calls_materialize_after_matching_approval() {
        let mut os = OperatingSystem::new("test");
        os.policy.allow_shell = true;
        os.policy.approval.require_for_risky_actions = true;
        os.register_tool(ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "say something",
            vec!["plan".into()],
            "printf {message}",
            None,
        ));
        let parent = Task::new("Plan", "plan work", Priority::High, vec!["plan".into()]);
        let parent_id = parent.id.clone();
        os.create_task(parent);
        let action = "provider-tool:say args:message=hello";
        os.request_approval(ApprovalRequest::new(
            parent_id.clone(),
            None,
            action,
            "provider requested tool task say",
        ));
        let approval_id = os
            .approvals
            .values()
            .find(|approval| approval.action == action)
            .expect("approval")
            .id
            .clone();
        os.resolve_approval(&approval_id, true, Some("operator".into()))
            .expect("resolve approval");

        materialize_provider_tool_calls(
            &mut os,
            &parent_id,
            &[ProviderToolCall {
                tool: "say".into(),
                args: BTreeMap::from([("message".into(), "hello".into())]),
            }],
        );

        assert!(os.tasks.values().any(|task| task.tool.is_some()));
    }

    #[test]
    fn provider_tool_calls_reject_file_write_policy_before_creating_task() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("test");
        os.policy.rules = vec!["deny writes outside src".into()];
        os.register_tool(ToolDefinition::new(
            "write-note",
            ToolKind::FileWrite,
            "write a note",
            vec![],
            "{name}",
            Some(dir.path().display().to_string()),
        ));
        let parent = Task::new("Plan", "plan work", Priority::Normal, vec![]);
        let parent_id = parent.id.clone();
        os.create_task(parent);
        store.save(&os).expect("save state");

        let response = AgentResponse {
            summary: "done".into(),
            plan: vec![],
            confidence: 80,
            tool_calls: vec![ProviderToolCall {
                tool: "write-note".into(),
                args: BTreeMap::from([
                    ("name".into(), "guide.md".into()),
                    ("body".into(), "hello".into()),
                ]),
            }],
        };
        let mut run = RunRecord::new(parent_id.clone(), None, "provider:mock", ".");
        run.status = RunStatus::Success;
        run.exit_code = Some(0);
        run.finished_at = Some(Utc::now());

        finish_run(
            &mut os,
            &store,
            FinishedRun {
                run,
                log: "provider log".into(),
                response: Some(response),
                failure_message: None,
            },
        )
        .expect("finish run");

        assert!(os.tasks.values().all(|task| task.tool.is_none()));
        assert!(os.events.iter().any(|event| {
            event.kind == EventKind::PolicyRejected
                && event.message.contains("deny writes outside src")
        }));
        assert!(!dir.path().join("guide.md").exists());
    }

    #[test]
    fn provider_tool_calls_reject_secret_like_args() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("test");
        os.register_tool(ToolDefinition::new(
            "send",
            ToolKind::Shell,
            "send with auth",
            vec![],
            "printf {api_key}",
            None,
        ));
        let parent = Task::new("Plan", "plan work", Priority::Normal, vec![]);
        let parent_id = parent.id.clone();
        os.create_task(parent);
        store.save(&os).expect("save state");

        let response = AgentResponse {
            summary: "done".into(),
            plan: vec![],
            confidence: 80,
            tool_calls: vec![ProviderToolCall {
                tool: "send".into(),
                args: BTreeMap::from([("api_key".into(), "secret-value".into())]),
            }],
        };
        let mut run = RunRecord::new(parent_id.clone(), None, "provider:mock", ".");
        run.status = RunStatus::Success;
        run.exit_code = Some(0);
        run.finished_at = Some(Utc::now());

        finish_run(
            &mut os,
            &store,
            FinishedRun {
                run,
                log: "provider log".into(),
                response: Some(response),
                failure_message: None,
            },
        )
        .expect("finish run");

        assert!(os.tasks.values().all(|task| task.tool.is_none()));
        assert!(os.events.iter().any(|event| {
            event.kind == EventKind::PolicyRejected
                && event.message.contains("redacted argument key `api_key`")
        }));
    }

    #[test]
    fn provider_tool_calls_reject_malformed_tool_ids() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("test");
        let parent = Task::new("Plan", "plan work", Priority::Normal, vec![]);
        let parent_id = parent.id.clone();
        os.create_task(parent);
        store.save(&os).expect("save state");

        let response = AgentResponse {
            summary: "done".into(),
            plan: vec![],
            confidence: 80,
            tool_calls: vec![ProviderToolCall {
                tool: "!!!".into(),
                args: BTreeMap::from([("message".into(), "hello".into())]),
            }],
        };
        let mut run = RunRecord::new(parent_id.clone(), None, "provider:mock", ".");
        run.status = RunStatus::Success;
        run.exit_code = Some(0);
        run.finished_at = Some(Utc::now());

        finish_run(
            &mut os,
            &store,
            FinishedRun {
                run,
                log: "provider log".into(),
                response: Some(response),
                failure_message: None,
            },
        )
        .expect("finish run");

        assert!(os.tasks.values().all(|task| task.tool.is_none()));
        assert!(os.events.iter().any(|event| {
            event.kind == EventKind::PolicyRejected
                && event.message.contains("malformed tool id `!!!`")
        }));
    }

    #[test]
    fn provider_tool_calls_reject_empty_arg_keys() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("test");
        os.register_tool(ToolDefinition::new(
            "send",
            ToolKind::Shell,
            "send something",
            vec![],
            "printf {message}",
            None,
        ));
        let parent = Task::new("Plan", "plan work", Priority::Normal, vec![]);
        let parent_id = parent.id.clone();
        os.create_task(parent);
        store.save(&os).expect("save state");

        let response = AgentResponse {
            summary: "done".into(),
            plan: vec![],
            confidence: 80,
            tool_calls: vec![ProviderToolCall {
                tool: "send".into(),
                args: BTreeMap::from([(" ".into(), "hello".into())]),
            }],
        };
        let mut run = RunRecord::new(parent_id.clone(), None, "provider:mock", ".");
        run.status = RunStatus::Success;
        run.exit_code = Some(0);
        run.finished_at = Some(Utc::now());

        finish_run(
            &mut os,
            &store,
            FinishedRun {
                run,
                log: "provider log".into(),
                response: Some(response),
                failure_message: None,
            },
        )
        .expect("finish run");

        assert!(os.tasks.values().all(|task| task.tool.is_none()));
        assert!(os.events.iter().any(|event| {
            event.kind == EventKind::PolicyRejected
                && event.message.contains("empty argument key ` `")
        }));
    }

    #[test]
    fn provider_tool_calls_reject_unexpected_args() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("test");
        os.register_tool(ToolDefinition::new(
            "send",
            ToolKind::Shell,
            "send something",
            vec![],
            "printf {message}",
            None,
        ));
        let parent = Task::new("Plan", "plan work", Priority::Normal, vec![]);
        let parent_id = parent.id.clone();
        os.create_task(parent);
        store.save(&os).expect("save state");

        let response = AgentResponse {
            summary: "done".into(),
            plan: vec![],
            confidence: 80,
            tool_calls: vec![ProviderToolCall {
                tool: "send".into(),
                args: BTreeMap::from([
                    ("message".into(), "hello".into()),
                    ("unused".into(), "ignored".into()),
                ]),
            }],
        };
        let mut run = RunRecord::new(parent_id.clone(), None, "provider:mock", ".");
        run.status = RunStatus::Success;
        run.exit_code = Some(0);
        run.finished_at = Some(Utc::now());

        finish_run(
            &mut os,
            &store,
            FinishedRun {
                run,
                log: "provider log".into(),
                response: Some(response),
                failure_message: None,
            },
        )
        .expect("finish run");

        assert!(os.tasks.values().all(|task| task.tool.is_none()));
        assert!(os.events.iter().any(|event| {
            event.kind == EventKind::PolicyRejected
                && event.message.contains("unexpected tool argument `unused`")
        }));
    }

    #[test]
    fn empty_redaction_patterns_do_not_reject_provider_tool_args() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("test");
        os.policy.allow_shell = true;
        os.policy.autonomy = AutonomyLevel::ExecuteFreely;
        os.policy.approval.require_for_risky_actions = false;
        os.policy.redacted_env_patterns = vec![];
        os.register_tool(ToolDefinition::new(
            "send",
            ToolKind::Shell,
            "send with auth",
            vec![],
            "printf {api_key}",
            None,
        ));
        let parent = Task::new("Plan", "plan work", Priority::Normal, vec![]);
        let parent_id = parent.id.clone();
        os.create_task(parent);
        store.save(&os).expect("save state");

        let response = AgentResponse {
            summary: "done".into(),
            plan: vec![],
            confidence: 80,
            tool_calls: vec![ProviderToolCall {
                tool: "send".into(),
                args: BTreeMap::from([("api_key".into(), "plain-value".into())]),
            }],
        };
        let mut run = RunRecord::new(parent_id.clone(), None, "provider:mock", ".");
        run.status = RunStatus::Success;
        run.exit_code = Some(0);
        run.finished_at = Some(Utc::now());

        finish_run(
            &mut os,
            &store,
            FinishedRun {
                run,
                log: "provider log".into(),
                response: Some(response),
                failure_message: None,
            },
        )
        .expect("finish run");

        assert!(os.tasks.values().any(|task| task.tool.is_some()));
        assert!(!os.events.iter().any(|event| {
            event.kind == EventKind::PolicyRejected
                && event.message.contains("redacted argument key `api_key`")
        }));
    }
}
