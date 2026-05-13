use crate::migrations::CURRENT_STATE_VERSION;
use crate::models::{
    AgentId, AgentKind, AgentStatus, DaemonStatus, EventKind, OperatingSystem, Policy,
    ProviderKind, ProviderSettings, RunStatus, TaskId, TaskStatus, ToolDefinition, ToolInvocation,
    is_valid_env_var_name, is_valid_provider_endpoint, is_valid_slug, normalize_list,
};
use crate::tools::{
    allowed_tool_arg_keys, is_valid_tool_arg_key, validate_tool_invocation, validate_tool_template,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const DEFAULT_REPAIRED_OS_NAME: &str = "Agent OS";

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ValidationReport {
    pub valid: bool,
    pub issues: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RepairReport {
    pub changed: bool,
    pub persisted: bool,
    pub repairs: Vec<String>,
    pub validation: ValidationReport,
}

pub fn repair_state(os: &mut OperatingSystem) -> RepairReport {
    let mut repairs = Vec::new();
    let mut additions = Vec::<(AgentId, TaskId)>::new();
    let now = Utc::now();

    repair_updated_at_order("OS", os.created_at, &mut os.updated_at, &mut repairs);
    if os.name.trim().is_empty() {
        repairs.push(format!("reset empty OS name to {DEFAULT_REPAIRED_OS_NAME}"));
        os.name = DEFAULT_REPAIRED_OS_NAME.into();
        os.updated_at = now;
    }

    for agent in os.agents.values_mut() {
        let mut seen = BTreeSet::new();
        let agent_id = agent.id.clone();
        if agent.max_parallel_tasks == 0 {
            repairs.push(format!("raised agent {} max_parallel_tasks to 1", agent_id));
            agent.max_parallel_tasks = 1;
            agent.updated_at = Utc::now();
        }
        if agent.lease_expires_at.is_some() && agent.last_heartbeat_at.is_none() {
            repairs.push(format!("cleared orphan lease from agent {}", agent_id));
            agent.lease_expires_at = None;
            agent.updated_at = now;
        }
        if agent
            .model
            .as_ref()
            .map(|model| model.trim().is_empty())
            .unwrap_or(false)
        {
            repairs.push(format!("cleared empty model from agent {}", agent_id));
            agent.model = None;
            agent.updated_at = Utc::now();
        }
        if agent.status == AgentStatus::Online
            && agent
                .lease_expires_at
                .map(|expires_at| expires_at <= now)
                .unwrap_or(false)
        {
            repairs.push(format!("expired lease for agent {}", agent_id));
            agent.status = AgentStatus::Offline;
            agent.updated_at = now;
        }
        if repair_normalized_list(
            &format!("agent {}", agent_id),
            "capabilities",
            &mut agent.capabilities,
            true,
            &mut repairs,
        ) {
            agent.updated_at = Utc::now();
        }
        let before = agent.current_tasks.clone();
        agent.current_tasks.retain(|task_id| {
            if !seen.insert(task_id.clone()) {
                repairs.push(format!(
                    "removed duplicate current task {} from agent {}",
                    task_id, agent_id
                ));
                return false;
            }
            match os.tasks.get(task_id) {
                Some(task)
                    if task.status == TaskStatus::Running
                        && task.assigned_to.as_ref() == Some(&agent_id) =>
                {
                    true
                }
                Some(task) => {
                    repairs.push(format!(
                        "removed stale current task {} from agent {} with task status {}",
                        task_id, agent_id, task.status
                    ));
                    false
                }
                None => {
                    repairs.push(format!(
                        "removed missing current task {} from agent {}",
                        task_id, agent_id
                    ));
                    false
                }
            }
        });
        if agent.current_tasks != before {
            agent.updated_at = Utc::now();
        }
        repair_updated_at_order(
            &format!("agent {}", agent_id),
            agent.created_at,
            &mut agent.updated_at,
            &mut repairs,
        );
    }

    let task_ids = os.tasks.keys().cloned().collect::<BTreeSet<_>>();
    let agent_ids = os.agents.keys().cloned().collect::<BTreeSet<_>>();
    let tool_definitions = os.tools.clone();
    for task in os.tasks.values_mut() {
        if let Some(agent_id) = &task.assigned_to
            && task.status != TaskStatus::Running
            && !agent_ids.contains(agent_id)
        {
            repairs.push(format!(
                "cleared missing assigned agent {} from non-running task {}",
                agent_id, task.id
            ));
            task.assigned_to = None;
            task.updated_at = Utc::now();
        }

        let mut seen = BTreeSet::new();
        let task_id = task.id.clone();
        let before = task.dependencies.clone();
        task.dependencies.retain(|dependency| {
            if !seen.insert(dependency.clone()) {
                repairs.push(format!(
                    "removed duplicate dependency {} from task {}",
                    dependency, task_id
                ));
                return false;
            }
            if !task_ids.contains(dependency) {
                repairs.push(format!(
                    "removed missing dependency {} from task {}",
                    dependency, task_id
                ));
                return false;
            }
            true
        });
        if task.dependencies != before {
            task.updated_at = Utc::now();
        }

        if repair_normalized_list(
            &format!("task {}", task_id),
            "required_capabilities",
            &mut task.required_capabilities,
            false,
            &mut repairs,
        ) {
            task.updated_at = Utc::now();
        }

        let before_plan = task.plan.clone();
        task.plan.retain(|step| {
            if step.trim().is_empty() {
                repairs.push(format!("removed empty plan step from task {}", task_id));
                return false;
            }
            true
        });
        if task.plan != before_plan {
            task.updated_at = Utc::now();
        }

        if task
            .cwd
            .as_ref()
            .map(|cwd| cwd.trim().is_empty())
            .unwrap_or(false)
        {
            repairs.push(format!("cleared empty cwd from task {}", task_id));
            task.cwd = None;
            task.updated_at = Utc::now();
        }
        if let Some(tool) = &mut task.tool
            && let Some(definition) = tool_definitions.get(&tool.tool_id)
            && repair_tool_invocation(&task_id, tool, definition, &mut repairs)
        {
            task.updated_at = Utc::now();
        }
        if task
            .output
            .as_ref()
            .map(|output| output.trim().is_empty())
            .unwrap_or(false)
        {
            repairs.push(format!("cleared empty output from task {}", task_id));
            task.output = None;
            task.updated_at = Utc::now();
        }
        repair_updated_at_order(
            &format!("task {}", task_id),
            task.created_at,
            &mut task.updated_at,
            &mut repairs,
        );
    }

    for task in os.tasks.values() {
        if task.status != TaskStatus::Running {
            continue;
        }
        let Some(agent_id) = &task.assigned_to else {
            continue;
        };
        let Some(agent) = os.agents.get(agent_id) else {
            continue;
        };
        if !agent
            .current_tasks
            .iter()
            .any(|task_id| task_id == &task.id)
        {
            additions.push((agent_id.clone(), task.id.clone()));
        }
    }

    for (agent_id, task_id) in additions {
        if let Some(agent) = os.agents.get_mut(&agent_id) {
            agent.current_tasks.push(task_id.clone());
            agent.updated_at = Utc::now();
            repairs.push(format!(
                "added running task {} to agent {} current tasks",
                task_id, agent_id
            ));
        }
    }

    for workflow in os.workflows.values_mut() {
        let workflow_id = workflow.id.clone();
        let before = workflow.tasks.clone();
        let mut seen_tasks = BTreeSet::new();
        workflow.tasks.retain(|stage, task_id| {
            if stage.trim().is_empty() {
                repairs.push(format!("removed empty stage from workflow {}", workflow_id));
                return false;
            }
            if !is_valid_slug(task_id.as_str()) {
                repairs.push(format!(
                    "removed invalid task {} from workflow {} stage {}",
                    task_id, workflow_id, stage
                ));
                return false;
            }
            if !task_ids.contains(task_id) {
                repairs.push(format!(
                    "removed missing task {} from workflow {} stage {}",
                    task_id, workflow_id, stage
                ));
                return false;
            }
            if !seen_tasks.insert(task_id.clone()) {
                repairs.push(format!(
                    "removed duplicate task {} from workflow {} stage {}",
                    task_id, workflow_id, stage
                ));
                return false;
            }
            true
        });
        if workflow.tasks != before {
            workflow.updated_at = Utc::now();
        }
        repair_updated_at_order(
            &format!("workflow {}", workflow_id),
            workflow.created_at,
            &mut workflow.updated_at,
            &mut repairs,
        );
    }

    for tool in os.tools.values_mut() {
        if repair_normalized_list(
            &format!("tool {}", tool.id),
            "required_capabilities",
            &mut tool.required_capabilities,
            false,
            &mut repairs,
        ) {
            tool.updated_at = Utc::now();
        }
        if tool
            .default_cwd
            .as_ref()
            .map(|cwd| cwd.trim().is_empty())
            .unwrap_or(false)
        {
            repairs.push(format!("cleared empty default_cwd from tool {}", tool.id));
            tool.default_cwd = None;
            tool.updated_at = Utc::now();
        }
        repair_updated_at_order(
            &format!("tool {}", tool.id),
            tool.created_at,
            &mut tool.updated_at,
            &mut repairs,
        );
    }

    for record in &mut os.memory {
        if repair_normalized_list(
            &format!("memory record {}", record.id),
            "tags",
            &mut record.tags,
            false,
            &mut repairs,
        ) {
            record.updated_at = Utc::now();
        }
        repair_updated_at_order(
            &format!("memory record {}", record.id),
            record.created_at,
            &mut record.updated_at,
            &mut repairs,
        );
    }

    let before_events = os.events.len();
    os.events.retain(|event| {
        if event.id.trim().is_empty() {
            repairs.push("removed event with empty id".into());
            return false;
        }
        if !is_valid_slug(&event.id) {
            repairs.push(format!("removed event {} with invalid id", event.id));
            return false;
        }
        if event.message.trim().is_empty() {
            repairs.push(format!("removed event {} with empty message", event.id));
            return false;
        }
        true
    });
    if os.events.len() != before_events {
        os.touch();
    }

    let mut seen_event_ids = BTreeSet::new();
    let before_events = os.events.len();
    os.events.retain(|event| {
        if !seen_event_ids.insert(event.id.clone()) {
            repairs.push(format!("removed duplicate event {}", event.id));
            return false;
        }
        true
    });
    if os.events.len() != before_events {
        os.touch();
    }

    let before_order = os
        .events
        .iter()
        .map(|event| event.id.clone())
        .collect::<Vec<_>>();
    os.events.sort_by_key(|event| event.at);
    let after_order = os
        .events
        .iter()
        .map(|event| event.id.clone())
        .collect::<Vec<_>>();
    if before_order != after_order {
        repairs.push("sorted events by timestamp".into());
        os.touch();
    }

    if os.events.len() > 500 {
        let removed = os.events.len() - 500;
        os.events.drain(0..removed);
        repairs.push(format!("trimmed {removed} old event(s)"));
        os.touch();
    }

    for run in os.runs.values_mut() {
        if run.command.trim().is_empty()
            && let Some(command) = os
                .tasks
                .get(&run.task_id)
                .and_then(|task| task.command.as_ref())
                .filter(|command| !command.trim().is_empty())
        {
            repairs.push(format!(
                "restored command for run {} from task {}",
                run.id, run.task_id
            ));
            run.command = command.clone();
        }
        if run
            .log_path
            .as_ref()
            .map(|path| path.trim().is_empty())
            .unwrap_or(false)
        {
            repairs.push(format!("cleared empty log_path from run {}", run.id));
            run.log_path = None;
        }
        if run.cwd.trim().is_empty() {
            repairs.push(format!("reset empty cwd from run {} to .", run.id));
            run.cwd = ".".into();
        }
        match run.status {
            RunStatus::Running | RunStatus::CancelRequested => {
                if run.finished_at.take().is_some() {
                    repairs.push(format!(
                        "cleared finished_at from active run {} with status {}",
                        run.id, run.status
                    ));
                }
                if run.exit_code.take().is_some() {
                    repairs.push(format!(
                        "cleared exit_code from active run {} with status {}",
                        run.id, run.status
                    ));
                }
            }
            RunStatus::Cancelled | RunStatus::Success | RunStatus::Failed | RunStatus::Rejected => {
                match run.finished_at {
                    Some(finished_at) if finished_at < run.started_at => {
                        repairs.push(format!(
                            "clamped finished_at for terminal run {} to started_at",
                            run.id
                        ));
                        run.finished_at = Some(run.started_at);
                    }
                    None => {
                        repairs.push(format!(
                            "set missing finished_at for terminal run {} to started_at",
                            run.id
                        ));
                        run.finished_at = Some(run.started_at);
                    }
                    Some(_) => {}
                }
                match run.status {
                    RunStatus::Success if run.exit_code != Some(0) => {
                        repairs.push(format!("set exit_code for successful run {} to 0", run.id));
                        run.exit_code = Some(0);
                    }
                    RunStatus::Failed if run.exit_code == Some(0) => {
                        repairs.push(format!(
                            "cleared successful exit_code from failed run {}",
                            run.id
                        ));
                        run.exit_code = None;
                    }
                    RunStatus::Cancelled | RunStatus::Rejected if run.exit_code.is_some() => {
                        repairs.push(format!(
                            "cleared exit_code from {} run {}",
                            run.status, run.id
                        ));
                        run.exit_code = None;
                    }
                    _ => {}
                }
            }
        }
    }

    let default_provider = ProviderSettings::default();
    if os.provider.model.trim().is_empty() {
        repairs.push(format!(
            "reset provider model to {}",
            default_provider.model
        ));
        os.provider.model = default_provider.model.clone();
        os.touch();
    }
    if os.provider.request_timeout_seconds == 0 {
        repairs.push(format!(
            "reset provider request_timeout_seconds to {}",
            default_provider.request_timeout_seconds
        ));
        os.provider.request_timeout_seconds = default_provider.request_timeout_seconds;
        os.touch();
    }
    if os.provider.api_key_env.trim().is_empty() {
        repairs.push(format!(
            "reset provider api_key_env to {}",
            default_provider.api_key_env
        ));
        os.provider.api_key_env = default_provider.api_key_env.clone();
        os.touch();
    } else if !is_valid_env_var_name(&os.provider.api_key_env) {
        let trimmed = os.provider.api_key_env.trim();
        if is_valid_env_var_name(trimmed) {
            repairs.push(format!(
                "trimmed provider api_key_env value {}",
                os.provider.api_key_env
            ));
            os.provider.api_key_env = trimmed.to_owned();
        } else {
            repairs.push(format!(
                "reset provider api_key_env to {}",
                default_provider.api_key_env
            ));
            os.provider.api_key_env = default_provider.api_key_env.clone();
        }
        os.touch();
    }
    if os
        .provider
        .endpoint
        .as_ref()
        .map(|endpoint| endpoint.trim().is_empty())
        .unwrap_or(false)
    {
        repairs.push("cleared empty provider endpoint".into());
        os.provider.endpoint = None;
        os.touch();
    } else if matches!(os.provider.kind, ProviderKind::Mock)
        && os
            .provider
            .endpoint
            .as_ref()
            .map(|endpoint| !is_valid_provider_endpoint(endpoint))
            .unwrap_or(false)
    {
        repairs.push("cleared invalid mock provider endpoint".into());
        os.provider.endpoint = None;
        os.touch();
    }

    if repair_policy_list(
        "policy allowed_commands",
        &mut os.policy.allowed_commands,
        &mut repairs,
    ) {
        os.touch();
    }
    if repair_policy_list(
        "policy allowed_workspaces",
        &mut os.policy.allowed_workspaces,
        &mut repairs,
    ) {
        os.touch();
    }
    if repair_policy_list(
        "policy denied_patterns",
        &mut os.policy.denied_patterns,
        &mut repairs,
    ) {
        os.touch();
    }
    if repair_policy_env_vars(&mut os.policy.allowed_env_vars, &mut repairs) {
        os.touch();
    }
    if repair_policy_list(
        "policy redacted_env_patterns",
        &mut os.policy.redacted_env_patterns,
        &mut repairs,
    ) {
        os.touch();
    }
    let default_policy = Policy::default();
    if os.policy.max_output_bytes == 0 {
        repairs.push(format!(
            "reset policy max_output_bytes to {}",
            default_policy.max_output_bytes
        ));
        os.policy.max_output_bytes = default_policy.max_output_bytes;
        os.touch();
    }
    if os.policy.command_timeout_seconds == 0 {
        repairs.push(format!(
            "reset policy command_timeout_seconds to {}",
            default_policy.command_timeout_seconds
        ));
        os.policy.command_timeout_seconds = default_policy.command_timeout_seconds;
        os.touch();
    }

    if let Some(daemon) = &mut os.daemon {
        if daemon.limit == 0 {
            repairs.push("raised daemon limit to 1".into());
            daemon.limit = 1;
        }
        if daemon
            .last_message
            .as_ref()
            .map(|message| message.trim().is_empty())
            .unwrap_or(false)
        {
            repairs.push("cleared empty daemon last_message".into());
            daemon.last_message = None;
        }
        if daemon.status == DaemonStatus::Stopped {
            if daemon.pid.is_some() {
                repairs.push("cleared pid from stopped daemon state".into());
                daemon.pid = None;
            }
            if daemon.stop_requested {
                repairs.push("cleared stop request from stopped daemon state".into());
                daemon.stop_requested = false;
            }
        }
    }

    for repair in &repairs {
        os.record(EventKind::StateRepaired, repair);
    }

    RepairReport {
        changed: !repairs.is_empty(),
        persisted: false,
        repairs,
        validation: validate_state(os),
    }
}

fn repair_tool_invocation(
    task_id: &TaskId,
    invocation: &mut ToolInvocation,
    definition: &ToolDefinition,
    repairs: &mut Vec<String>,
) -> bool {
    let Ok(allowed_args) = allowed_tool_arg_keys(definition) else {
        return false;
    };
    let mut changed = false;

    invocation.args.retain(|key, _| {
        if !is_valid_tool_arg_key(key) {
            repairs.push(format!(
                "removed invalid tool argument key {} from task {}",
                key, task_id
            ));
            changed = true;
            return false;
        }
        if !allowed_args.contains(key) {
            repairs.push(format!(
                "removed unexpected tool argument {} from task {}",
                key, task_id
            ));
            changed = true;
            return false;
        }
        true
    });

    invocation.secret_env_args.retain(|key, env| {
        if !is_valid_tool_arg_key(key) {
            repairs.push(format!(
                "removed invalid secret tool argument key {} from task {}",
                key, task_id
            ));
            changed = true;
            return false;
        }
        if invocation.args.contains_key(key) {
            repairs.push(format!(
                "removed conflicting secret tool argument {} from task {}",
                key, task_id
            ));
            changed = true;
            return false;
        }
        if !allowed_args.contains(key) {
            repairs.push(format!(
                "removed unexpected secret tool argument {} from task {}",
                key, task_id
            ));
            changed = true;
            return false;
        }
        if !is_valid_env_var_name(env) {
            repairs.push(format!(
                "removed invalid secret env arg {} from task {}",
                key, task_id
            ));
            changed = true;
            return false;
        }
        true
    });

    changed
}

fn repair_policy_list(field: &str, values: &mut Vec<String>, repairs: &mut Vec<String>) -> bool {
    let before = values.clone();
    values.retain(|value| {
        if value.trim().is_empty() {
            repairs.push(format!("removed empty {field} value"));
            return false;
        }
        true
    });
    *values != before
}

fn repair_policy_env_vars(values: &mut Vec<String>, repairs: &mut Vec<String>) -> bool {
    let before = values.clone();
    let mut repaired = Vec::new();
    for value in values.iter() {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            repairs.push("removed empty policy allowed_env_vars value".into());
        } else if is_valid_env_var_name(value) {
            repaired.push(value.clone());
        } else if is_valid_env_var_name(trimmed) {
            repairs.push(format!("trimmed policy allowed_env_vars value {}", value));
            repaired.push(trimmed.to_owned());
        } else {
            repairs.push(format!(
                "removed invalid policy allowed_env_vars value {}",
                value
            ));
        }
    }
    *values = repaired;
    *values != before
}

fn repair_updated_at_order(
    owner: &str,
    created_at: DateTime<Utc>,
    updated_at: &mut DateTime<Utc>,
    repairs: &mut Vec<String>,
) -> bool {
    if *updated_at >= created_at {
        return false;
    }

    repairs.push(format!("clamped {owner} updated_at to created_at"));
    *updated_at = created_at;
    true
}

fn repair_normalized_list(
    owner: &str,
    field: &str,
    values: &mut Vec<String>,
    require_non_empty: bool,
    repairs: &mut Vec<String>,
) -> bool {
    let normalized = normalize_list(values.clone());
    if require_non_empty && normalized.is_empty() {
        return false;
    }
    if normalized == *values {
        return false;
    }
    *values = normalized;
    repairs.push(format!("normalized {} for {}", field, owner));
    true
}

pub fn validate_state(os: &OperatingSystem) -> ValidationReport {
    let mut issues = Vec::new();

    if os.version == 0 {
        issues.push("state version is missing or zero".into());
    } else if os.version > CURRENT_STATE_VERSION {
        issues.push(format!(
            "state version {} is newer than this binary supports ({})",
            os.version, CURRENT_STATE_VERSION
        ));
    } else if os.version < CURRENT_STATE_VERSION {
        issues.push(format!(
            "state version {} is older than this binary supports ({}); run state migrate",
            os.version, CURRENT_STATE_VERSION
        ));
    }

    if os.name.trim().is_empty() {
        issues.push("OS name is empty".into());
    }
    validate_updated_at_order(&mut issues, "OS", os.created_at, os.updated_at);

    for (task_key, task) in &os.tasks {
        if task_key != &task.id {
            issues.push(format!(
                "task map key {} does not match task id {}",
                task_key, task.id
            ));
        }
        if !is_valid_slug(task_key.as_str()) {
            issues.push(format!("task map key {} is not a valid id", task_key));
        }
        if task.id.as_str().is_empty() {
            issues.push("task has empty id".into());
        } else if !is_valid_slug(task.id.as_str()) {
            issues.push(format!("task {} has invalid id", task.id));
        }
        if task.title.trim().is_empty() {
            issues.push(format!("task {} has empty title", task.id));
        }
        if task.objective.trim().is_empty() {
            issues.push(format!("task {} has empty objective", task.id));
        }
        validate_updated_at_order(
            &mut issues,
            &format!("task {}", task.id),
            task.created_at,
            task.updated_at,
        );
        if has_empty_list_part(&task.required_capabilities) {
            issues.push(format!(
                "task {} required_capabilities contains an empty capability",
                task.id
            ));
        } else if has_normalization_drift(&task.required_capabilities) {
            issues.push(format!(
                "task {} required_capabilities are not normalized",
                task.id
            ));
        }
        if let Some(command) = &task.command
            && command.trim().is_empty()
        {
            issues.push(format!("task {} has empty command", task.id));
        }
        if let Some(cwd) = &task.cwd
            && cwd.trim().is_empty()
        {
            issues.push(format!("task {} has empty cwd", task.id));
        }
        if task.plan.iter().any(|step| step.trim().is_empty()) {
            issues.push(format!("task {} plan contains an empty step", task.id));
        }
        if let Some(output) = &task.output
            && output.trim().is_empty()
        {
            issues.push(format!("task {} has empty output", task.id));
        }
        if let Some(agent_id) = &task.assigned_to
            && !is_valid_slug(agent_id.as_str())
        {
            issues.push(format!(
                "task {} assigned to invalid agent id {}",
                task.id, agent_id
            ));
        }
        if let Some(agent_id) = &task.assigned_to
            && !os.agents.contains_key(agent_id)
        {
            issues.push(format!(
                "task {} assigned to missing agent {}",
                task.id, agent_id
            ));
        }
        if task.status == TaskStatus::Running {
            match &task.assigned_to {
                Some(agent_id) => {
                    if let Some(agent) = os.agents.get(agent_id)
                        && !agent.current_tasks.iter().any(|id| id == &task.id)
                    {
                        issues.push(format!(
                            "running task {} assigned to agent {} but missing from agent current tasks",
                            task.id, agent_id
                        ));
                    }
                }
                None => issues.push(format!(
                    "running task {} is missing an assigned agent",
                    task.id
                )),
            }
        }
        let mut seen_dependencies = BTreeSet::new();
        for dependency in &task.dependencies {
            if !is_valid_slug(dependency.as_str()) {
                issues.push(format!(
                    "task {} depends on invalid task id {}",
                    task.id, dependency
                ));
            }
            if !seen_dependencies.insert(dependency) {
                issues.push(format!(
                    "task {} has duplicate dependency {}",
                    task.id, dependency
                ));
                continue;
            }
            if !os.tasks.contains_key(dependency) {
                issues.push(format!(
                    "task {} depends on missing task {}",
                    task.id, dependency
                ));
            }
        }
        if let Some(tool) = &task.tool
            && !is_valid_slug(tool.tool_id.as_str())
        {
            issues.push(format!(
                "task {} invokes invalid tool id {}",
                task.id, tool.tool_id
            ));
        }
        if let Some(tool) = &task.tool
            && !os.tools.contains_key(&tool.tool_id)
        {
            issues.push(format!(
                "task {} invokes missing tool {}",
                task.id, tool.tool_id
            ));
        }
        if let Some(tool) = &task.tool {
            if tool.tool_id.as_str().is_empty() {
                issues.push(format!("task {} has empty tool id", task.id));
            }
            if let Some(definition) = os.tools.get(&tool.tool_id)
                && let Err(error) = validate_tool_invocation(definition, tool)
            {
                issues.push(format!(
                    "task {} has invalid tool invocation: {}",
                    task.id, error
                ));
            }
            if tool.args.keys().any(|key| key.trim().is_empty())
                || tool.secret_env_args.keys().any(|key| key.trim().is_empty())
            {
                issues.push(format!("task {} tool args contain an empty key", task.id));
            }
            if tool
                .secret_env_args
                .iter()
                .any(|(_, env)| env.trim().is_empty())
            {
                issues.push(format!(
                    "task {} secret tool args contain an empty environment variable",
                    task.id
                ));
            }
            if tool
                .secret_env_args
                .iter()
                .any(|(_, env)| !env.trim().is_empty() && !is_valid_env_var_name(env))
            {
                issues.push(format!(
                    "task {} secret tool args contain an invalid environment variable name",
                    task.id
                ));
            }
            if let Some(key) = tool
                .args
                .keys()
                .find(|key| tool.secret_env_args.contains_key(*key))
            {
                issues.push(format!(
                    "task {} tool argument {} appears in both args and secret args",
                    task.id, key
                ));
            }
        }
    }

    issues.extend(dependency_cycle_issues(os));

    for (workflow_key, workflow) in &os.workflows {
        if workflow_key != &workflow.id {
            issues.push(format!(
                "workflow map key {} does not match workflow id {}",
                workflow_key, workflow.id
            ));
        }
        if !is_valid_slug(workflow_key.as_str()) {
            issues.push(format!(
                "workflow map key {} is not a valid id",
                workflow_key
            ));
        }
        if workflow.id.as_str().is_empty() {
            issues.push("workflow has empty id".into());
        } else if !is_valid_slug(workflow.id.as_str()) {
            issues.push(format!("workflow {} has invalid id", workflow.id));
        }
        if workflow.objective.trim().is_empty() {
            issues.push(format!("workflow {} has empty objective", workflow.id));
        }
        validate_updated_at_order(
            &mut issues,
            &format!("workflow {}", workflow.id),
            workflow.created_at,
            workflow.updated_at,
        );
        let mut seen_stages = BTreeSet::new();
        let mut seen_tasks = BTreeSet::new();
        for (stage, task_id) in &workflow.tasks {
            if stage.trim().is_empty() {
                issues.push(format!("workflow {} has empty stage name", workflow.id));
            }
            if !seen_stages.insert(stage) {
                issues.push(format!(
                    "workflow {} has duplicate stage {}",
                    workflow.id, stage
                ));
            }
            if !is_valid_slug(task_id.as_str()) {
                issues.push(format!(
                    "workflow {} stage {} references invalid task id {}",
                    workflow.id, stage, task_id
                ));
            }
            if !seen_tasks.insert(task_id) {
                issues.push(format!(
                    "workflow {} references duplicate task {}",
                    workflow.id, task_id
                ));
            }
            if !os.tasks.contains_key(task_id) {
                issues.push(format!(
                    "workflow {} stage {} references missing task {}",
                    workflow.id, stage, task_id
                ));
            }
        }
    }

    for (agent_key, agent) in &os.agents {
        if agent_key != &agent.id {
            issues.push(format!(
                "agent map key {} does not match agent id {}",
                agent_key, agent.id
            ));
        }
        if !is_valid_slug(agent_key.as_str()) {
            issues.push(format!("agent map key {} is not a valid id", agent_key));
        }
        if agent.id.as_str().is_empty() {
            issues.push("agent has empty id".into());
        } else if !is_valid_slug(agent.id.as_str()) {
            issues.push(format!("agent {} has invalid id", agent.id));
        }
        if agent.name.trim().is_empty() {
            issues.push(format!("agent {} has empty name", agent.id));
        }
        if matches!(&agent.kind, AgentKind::Custom(kind) if kind.trim().is_empty()) {
            issues.push(format!("agent {} has empty kind", agent.id));
        }
        if let Some(model) = &agent.model
            && model.trim().is_empty()
        {
            issues.push(format!("agent {} has empty model", agent.id));
        }
        validate_updated_at_order(
            &mut issues,
            &format!("agent {}", agent.id),
            agent.created_at,
            agent.updated_at,
        );
        if agent.capabilities.is_empty() {
            issues.push(format!(
                "agent {} capabilities must include at least one capability",
                agent.id
            ));
        }
        if has_empty_list_part(&agent.capabilities) {
            issues.push(format!(
                "agent {} capabilities contains an empty capability",
                agent.id
            ));
        } else if has_normalization_drift(&agent.capabilities) {
            issues.push(format!(
                "agent {} capabilities are not normalized",
                agent.id
            ));
        }
        if agent.max_parallel_tasks == 0 {
            issues.push(format!("agent {} has zero max_parallel_tasks", agent.id));
        }
        if agent.lease_expires_at.is_some() && agent.last_heartbeat_at.is_none() {
            issues.push(format!(
                "agent {} has lease_expires_at but missing last_heartbeat_at",
                agent.id
            ));
        }
        if agent.status == AgentStatus::Online
            && agent
                .lease_expires_at
                .map(|expires_at| expires_at <= Utc::now())
                .unwrap_or(false)
        {
            issues.push(format!("agent {} is online with expired lease", agent.id));
        }
        if agent.current_tasks.len() > agent.max_parallel_tasks {
            issues.push(format!(
                "agent {} has {} current tasks but max_parallel_tasks is {}",
                agent.id,
                agent.current_tasks.len(),
                agent.max_parallel_tasks
            ));
        }
        let mut seen = BTreeSet::new();
        for task_id in &agent.current_tasks {
            if !is_valid_slug(task_id.as_str()) {
                issues.push(format!(
                    "agent {} has invalid current task id {}",
                    agent.id, task_id
                ));
            }
            if !seen.insert(task_id) {
                issues.push(format!(
                    "agent {} has duplicate current task {}",
                    agent.id, task_id
                ));
                continue;
            }
            match os.tasks.get(task_id) {
                Some(task)
                    if task.assigned_to.as_ref() == Some(&agent.id)
                        && task.status == TaskStatus::Running => {}
                Some(task) if task.assigned_to.as_ref() == Some(&agent.id) => issues.push(format!(
                    "agent {} has non-running current task {} with status {}",
                    agent.id, task_id, task.status
                )),
                Some(_) => issues.push(format!(
                    "agent {} has task {} but task is assigned elsewhere",
                    agent.id, task_id
                )),
                None => issues.push(format!(
                    "agent {} has missing current task {}",
                    agent.id, task_id
                )),
            }
        }
    }

    for (tool_key, tool) in &os.tools {
        if tool_key != &tool.id {
            issues.push(format!(
                "tool map key {} does not match tool id {}",
                tool_key, tool.id
            ));
        }
        if !is_valid_slug(tool_key.as_str()) {
            issues.push(format!("tool map key {} is not a valid id", tool_key));
        }
        if tool.id.as_str().is_empty() {
            issues.push("tool has empty id".into());
        } else if !is_valid_slug(tool.id.as_str()) {
            issues.push(format!("tool {} has invalid id", tool.id));
        }
        if tool.name.trim().is_empty() {
            issues.push(format!("tool {} has empty name", tool.id));
        }
        if tool.command_template.trim().is_empty() {
            issues.push(format!("tool {} has empty command_template", tool.id));
        }
        validate_updated_at_order(
            &mut issues,
            &format!("tool {}", tool.id),
            tool.created_at,
            tool.updated_at,
        );
        if let Err(error) = validate_tool_template(tool) {
            issues.push(error.to_string());
        }
        if let Some(cwd) = &tool.default_cwd
            && cwd.trim().is_empty()
        {
            issues.push(format!("tool {} has empty default_cwd", tool.id));
        }
        if has_empty_list_part(&tool.required_capabilities) {
            issues.push(format!(
                "tool {} required_capabilities contains an empty capability",
                tool.id
            ));
        } else if has_normalization_drift(&tool.required_capabilities) {
            issues.push(format!(
                "tool {} required_capabilities are not normalized",
                tool.id
            ));
        }
    }

    for (run_key, run) in &os.runs {
        if run_key != &run.id {
            issues.push(format!(
                "run map key {} does not match run id {}",
                run_key, run.id
            ));
        }
        if !is_valid_slug(run_key.as_str()) {
            issues.push(format!("run map key {} is not a valid id", run_key));
        }
        if run.id.as_str().is_empty() {
            issues.push("run has empty id".into());
        } else if !is_valid_slug(run.id.as_str()) {
            issues.push(format!("run {} has invalid id", run.id));
        }
        if !is_valid_slug(run.task_id.as_str()) {
            issues.push(format!(
                "run {} references invalid task id {}",
                run.id, run.task_id
            ));
        }
        if !os.tasks.contains_key(&run.task_id) {
            issues.push(format!(
                "run {} references missing task {}",
                run.id, run.task_id
            ));
        }
        if let Some(agent_id) = &run.agent_id
            && !is_valid_slug(agent_id.as_str())
        {
            issues.push(format!(
                "run {} references invalid agent id {}",
                run.id, agent_id
            ));
        }
        if let Some(agent_id) = &run.agent_id
            && !os.agents.contains_key(agent_id)
        {
            issues.push(format!(
                "run {} references missing agent {}",
                run.id, agent_id
            ));
        }
        if let Some(finished_at) = run.finished_at
            && finished_at < run.started_at
        {
            issues.push(format!("run {} finished before it started", run.id));
        }
        if run.command.trim().is_empty() {
            issues.push(format!("run {} has empty command", run.id));
        }
        if run.cwd.trim().is_empty() {
            issues.push(format!("run {} has empty cwd", run.id));
        }
        if let Some(log_path) = &run.log_path
            && log_path.trim().is_empty()
        {
            issues.push(format!("run {} has empty log_path", run.id));
        }
        match run.status {
            RunStatus::Running | RunStatus::CancelRequested => {
                if run.finished_at.is_some() {
                    issues.push(format!(
                        "run {} has active status {} but has finished_at",
                        run.id, run.status
                    ));
                }
                if run.exit_code.is_some() {
                    issues.push(format!(
                        "run {} has active status {} but has exit_code",
                        run.id, run.status
                    ));
                }
            }
            RunStatus::Cancelled | RunStatus::Success | RunStatus::Failed | RunStatus::Rejected => {
                if run.finished_at.is_none() {
                    issues.push(format!(
                        "run {} has terminal status {} but missing finished_at",
                        run.id, run.status
                    ));
                }
                match run.status {
                    RunStatus::Success if run.exit_code != Some(0) => {
                        issues.push(format!(
                            "run {} has success status but exit_code is not 0",
                            run.id
                        ));
                    }
                    RunStatus::Failed if run.exit_code == Some(0) => {
                        issues.push(format!(
                            "run {} has failed status but successful exit_code 0",
                            run.id
                        ));
                    }
                    RunStatus::Cancelled | RunStatus::Rejected if run.exit_code.is_some() => {
                        issues.push(format!(
                            "run {} has {} status but has exit_code",
                            run.id, run.status
                        ));
                    }
                    _ => {}
                }
            }
        }
    }

    let mut seen_memory_ids = BTreeSet::new();
    for record in &os.memory {
        if record.id.trim().is_empty() {
            issues.push("memory record has empty id".into());
        } else if !is_valid_slug(&record.id) {
            issues.push(format!("memory record {} has invalid id", record.id));
        }
        if !seen_memory_ids.insert(&record.id) {
            issues.push(format!("memory records have duplicate id {}", record.id));
        }
        if record.topic.trim().is_empty() {
            issues.push(format!("memory record {} has empty topic", record.id));
        }
        if record.body.trim().is_empty() {
            issues.push(format!("memory record {} has empty body", record.id));
        }
        validate_updated_at_order(
            &mut issues,
            &format!("memory record {}", record.id),
            record.created_at,
            record.updated_at,
        );
        if has_empty_list_part(&record.tags) {
            issues.push(format!(
                "memory record {} tags contains an empty tag",
                record.id
            ));
        } else if has_normalization_drift(&record.tags) {
            issues.push(format!(
                "memory record {} tags are not normalized",
                record.id
            ));
        }
    }

    if os.events.len() > 500 {
        issues.push(format!(
            "event log has {} events but maximum is 500",
            os.events.len()
        ));
    }
    let mut previous_event_at = None;
    let mut seen_event_ids = BTreeSet::new();
    for event in &os.events {
        if event.id.trim().is_empty() {
            issues.push("event has empty id".into());
        } else if !is_valid_slug(&event.id) {
            issues.push(format!("event {} has invalid id", event.id));
        }
        if !seen_event_ids.insert(&event.id) {
            issues.push(format!("event log has duplicate event id {}", event.id));
        }
        if event.message.trim().is_empty() {
            issues.push(format!("event {} has empty message", event.id));
        }
        if let Some(previous) = previous_event_at
            && event.at < previous
        {
            issues.push("event log is not sorted by timestamp".into());
            break;
        }
        previous_event_at = Some(event.at);
    }

    if os.provider.model.trim().is_empty() {
        issues.push("provider model is empty".into());
    }
    if os.provider.api_key_env.trim().is_empty() {
        issues.push("provider api_key_env is empty".into());
    } else if !is_valid_env_var_name(&os.provider.api_key_env) {
        issues.push("provider api_key_env must be a valid environment variable name".into());
    }
    if let Some(endpoint) = &os.provider.endpoint
        && endpoint.trim().is_empty()
    {
        issues.push("provider endpoint is empty".into());
    }
    if let Some(endpoint) = &os.provider.endpoint
        && !endpoint.trim().is_empty()
        && !is_valid_provider_endpoint(endpoint)
    {
        issues.push("provider endpoint must be an absolute http(s) URL".into());
    }
    if matches!(os.provider.kind, ProviderKind::OpenAiCompatible) && os.provider.endpoint.is_none()
    {
        issues.push("provider endpoint is required for openai-compatible provider".into());
    }
    if os.provider.request_timeout_seconds == 0 {
        issues.push("provider request_timeout_seconds must be greater than 0".into());
    }
    if has_empty_entry(&os.policy.allowed_commands) {
        issues.push("policy allowed_commands contains an empty value".into());
    }
    if has_empty_entry(&os.policy.allowed_workspaces) {
        issues.push("policy allowed_workspaces contains an empty value".into());
    }
    if has_empty_entry(&os.policy.denied_patterns) {
        issues.push("policy denied_patterns contains an empty value".into());
    }
    if has_empty_entry(&os.policy.allowed_env_vars) {
        issues.push("policy allowed_env_vars contains an empty value".into());
    }
    if os
        .policy
        .allowed_env_vars
        .iter()
        .any(|value| !value.trim().is_empty() && !is_valid_env_var_name(value))
    {
        issues.push("policy allowed_env_vars contains an invalid environment variable name".into());
    }
    if has_empty_entry(&os.policy.redacted_env_patterns) {
        issues.push("policy redacted_env_patterns contains an empty value".into());
    }
    if os.policy.max_output_bytes == 0 {
        issues.push("policy max_output_bytes must be greater than 0".into());
    }
    if os.policy.command_timeout_seconds == 0 {
        issues.push("policy command_timeout_seconds must be greater than 0".into());
    }

    if let Some(daemon) = &os.daemon {
        if daemon.limit == 0 {
            issues.push("daemon limit must be greater than 0".into());
        }
        if let Some(last_message) = &daemon.last_message
            && last_message.trim().is_empty()
        {
            issues.push("daemon last_message is empty".into());
        }
        match daemon.status {
            DaemonStatus::Running => {
                if daemon.pid.is_none() {
                    issues.push("daemon is running but missing pid".into());
                }
            }
            DaemonStatus::Stopped => {
                if daemon.pid.is_some() {
                    issues.push("daemon is stopped but still has pid".into());
                }
                if daemon.stop_requested {
                    issues.push("daemon is stopped but stop request is still set".into());
                }
            }
        }
    }

    ValidationReport {
        valid: issues.is_empty(),
        issues,
    }
}

fn has_empty_entry(values: &[String]) -> bool {
    values.iter().any(|value| value.trim().is_empty())
}

fn has_empty_list_part(values: &[String]) -> bool {
    values
        .iter()
        .any(|value| value.trim().is_empty() || value.split(',').any(|part| part.trim().is_empty()))
}

fn has_normalization_drift(values: &[String]) -> bool {
    normalize_list(values.to_vec()).as_slice() != values
}

fn validate_updated_at_order(
    issues: &mut Vec<String>,
    owner: &str,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
) {
    if updated_at < created_at {
        issues.push(format!("{owner} updated_at is before created_at"));
    }
}

fn dependency_cycle_issues(os: &OperatingSystem) -> Vec<String> {
    let mut issues = Vec::new();
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut stack = Vec::new();

    for task_id in os.tasks.keys() {
        find_dependency_cycles(
            os,
            task_id,
            &mut visiting,
            &mut visited,
            &mut stack,
            &mut issues,
        );
    }

    issues
}

fn find_dependency_cycles(
    os: &OperatingSystem,
    task_id: &TaskId,
    visiting: &mut BTreeSet<TaskId>,
    visited: &mut BTreeSet<TaskId>,
    stack: &mut Vec<TaskId>,
    issues: &mut Vec<String>,
) {
    if visited.contains(task_id) {
        return;
    }
    if visiting.contains(task_id) {
        if let Some(position) = stack.iter().position(|id| id == task_id) {
            let mut cycle = stack[position..].to_vec();
            cycle.push(task_id.clone());
            issues.push(format!(
                "dependency cycle detected: {}",
                cycle
                    .iter()
                    .map(TaskId::to_string)
                    .collect::<Vec<_>>()
                    .join(" -> ")
            ));
        }
        return;
    }

    let Some(task) = os.tasks.get(task_id) else {
        return;
    };

    visiting.insert(task_id.clone());
    stack.push(task_id.clone());
    for dependency in &task.dependencies {
        if os.tasks.contains_key(dependency) {
            find_dependency_cycles(os, dependency, visiting, visited, stack, issues);
        }
    }
    stack.pop();
    visiting.remove(task_id);
    visited.insert(task_id.clone());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        Agent, AgentId, AgentKind, DaemonState, Event, MemoryRecord, OperatingSystem, Priority,
        ProviderKind, RunRecord, Task, ToolDefinition, ToolId, ToolInvocation, ToolKind, Workflow,
    };
    use std::collections::BTreeMap;

    #[test]
    fn reports_missing_assigned_agent() {
        let mut os = OperatingSystem::new("test");
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        task.assigned_to = Some(AgentId::new("missing"));
        os.create_task(task);

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("missing agent"))
        );
    }

    #[test]
    fn reports_running_task_missing_agent_backreference() {
        let mut os = OperatingSystem::new("test");
        let agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        os.register_agent(agent);
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        task.status = TaskStatus::Running;
        task.assigned_to = Some(agent_id);
        os.create_task(task);

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(report.issues.iter().any(|issue| {
            issue.contains("assigned to agent builder but missing from agent current tasks")
        }));
    }

    #[test]
    fn reports_workflow_references_missing_task() {
        let mut os = OperatingSystem::new("test");
        let workflow = Workflow::new(
            "Ship it",
            Priority::Normal,
            BTreeMap::from([("plan".into(), TaskId::from_slug("missing-task"))]),
        );
        os.create_workflow(workflow);

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("references missing task"))
        );
    }

    #[test]
    fn repairs_workflow_stage_drift() {
        let mut os = OperatingSystem::new("test");
        let task = Task::new("Plan", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        let blank_stage_task = Task::new("Blank stage", "Objective", Priority::Normal, vec![]);
        let blank_stage_task_id = blank_stage_task.id.clone();
        os.create_task(task);
        os.create_task(blank_stage_task);
        let workflow = Workflow::new(
            "Ship it",
            Priority::Normal,
            BTreeMap::from([("plan".into(), task_id.clone())]),
        );
        let workflow_id = workflow.id.clone();
        os.create_workflow(workflow);

        let mut value = serde_json::to_value(&os).expect("state value");
        let tasks = value["workflows"][workflow_id.as_str()]["tasks"]
            .as_object_mut()
            .expect("workflow tasks");
        tasks.insert(" ".into(), serde_json::json!(blank_stage_task_id));
        tasks.insert("bad".into(), serde_json::json!("bad task id"));
        tasks.insert("missing".into(), serde_json::json!("missing-task"));
        tasks.insert("zz-duplicate".into(), serde_json::json!(task_id));
        let mut os: OperatingSystem = serde_json::from_value(value).expect("state");

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        let tasks = &os.workflows.get(&workflow_id).expect("workflow").tasks;
        assert_eq!(tasks.len(), 1);
        assert!(tasks.contains_key("plan"));
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("removed empty stage from workflow"))
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("removed invalid task"))
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("removed missing task"))
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("removed duplicate task"))
        );
    }

    #[test]
    fn reports_unsupported_state_versions() {
        let mut zero = OperatingSystem::new("test");
        zero.version = 0;
        let zero_report = validate_state(&zero);

        assert!(!zero_report.valid);
        assert!(
            zero_report
                .issues
                .iter()
                .any(|issue| issue.contains("state version is missing or zero"))
        );

        let mut future = OperatingSystem::new("test");
        future.version = CURRENT_STATE_VERSION + 1;
        let future_report = validate_state(&future);

        assert!(!future_report.valid);
        assert!(
            future_report
                .issues
                .iter()
                .any(|issue| issue.contains("newer than this binary supports"))
        );
    }

    #[test]
    fn repairs_empty_os_name() {
        let mut os = OperatingSystem::new(" ");

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        assert_eq!(os.name, "Agent OS");
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair == "reset empty OS name to Agent OS")
        );
    }

    #[test]
    fn reports_empty_durable_fields() {
        let mut os = OperatingSystem::new("test");
        os.name = " ".into();
        os.provider.kind = ProviderKind::OpenAiCompatible;
        os.provider.model = " ".into();
        os.provider.api_key_env = " ".into();
        os.provider.endpoint = None;
        os.policy.denied_patterns.push(" ".into());
        os.policy.allowed_env_vars.push(" ".into());
        os.policy.max_output_bytes = 0;
        os.policy.command_timeout_seconds = 0;

        let mut agent = Agent::new(
            "Builder",
            AgentKind::Custom(" ".into()),
            None,
            vec!["rust".into()],
            1,
        );
        agent.name = " ".into();
        agent.model = Some(" ".into());
        agent.capabilities.push(" ".into());
        os.register_agent(agent);

        let mut tool = ToolDefinition::new(
            "Tool",
            ToolKind::Shell,
            "",
            vec!["rust".into()],
            "printf hi",
            None,
        );
        tool.command_template = " ".into();
        tool.default_cwd = Some(" ".into());
        tool.required_capabilities.push(" ".into());
        os.register_tool(tool);

        let mut task = Task::new(" ", " ", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        task.required_capabilities.push(" ".into());
        task.command = Some(" ".into());
        task.cwd = Some(" ".into());
        task.plan.push(" ".into());
        task.output = Some(" ".into());
        task.tool = Some(ToolInvocation::with_secret_env_args(
            ToolId::new("tool"),
            BTreeMap::from([("message".into(), "value".into())]),
            BTreeMap::from([("message".into(), " ".into())]),
        ));
        os.create_task(task);

        let run = RunRecord::new(task_id, None, "command", " ");
        os.runs.insert(run.id.clone(), run);

        let mut memory = MemoryRecord::new(" ", " ", vec![]);
        memory.tags.push(" ".into());
        memory.tags.push("rust,".into());
        os.write_memory(memory);

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("OS name is empty"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("has empty title"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("has empty kind"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("agent") && issue.contains("empty model"))
        );
        assert!(report.issues.iter().any(|issue| {
            issue.contains("tool argument message appears in both args and secret args")
        }));
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("task") && issue.contains("empty cwd"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("task") && issue.contains("empty output"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("tool") && issue.contains("empty default_cwd"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("run") && issue.contains("empty cwd"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("memory record") && issue.contains("empty body"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("provider model is empty"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("provider endpoint is required"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("policy denied_patterns contains an empty value"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| { issue.contains("policy max_output_bytes must be greater than 0") })
        );
        assert!(report.issues.iter().any(|issue| {
            issue.contains("policy command_timeout_seconds must be greater than 0")
        }));
    }

    #[test]
    fn reports_updated_at_before_created_at() {
        let mut os = OperatingSystem::new("test");
        os.updated_at = os.created_at - chrono::Duration::seconds(1);

        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        agent.updated_at = agent.created_at - chrono::Duration::seconds(1);
        os.register_agent(agent);

        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        task.updated_at = task.created_at - chrono::Duration::seconds(1);
        os.create_task(task);

        let mut tool = ToolDefinition::new(
            "Tool",
            ToolKind::Shell,
            "",
            vec!["rust".into()],
            "printf hi",
            None,
        );
        tool.updated_at = tool.created_at - chrono::Duration::seconds(1);
        os.register_tool(tool);
        let mut memory = crate::models::MemoryRecord::new("Memory", "Body", vec![]);
        let memory_id = memory.id.clone();
        memory.updated_at = memory.created_at - chrono::Duration::seconds(1);
        os.write_memory(memory);
        os.updated_at = os.created_at - chrono::Duration::seconds(1);

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("OS updated_at is before created_at"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("agent builder updated_at is before created_at"))
        );
        assert!(report.issues.iter().any(|issue| {
            issue.contains(&format!("task {task_id} updated_at is before created_at"))
        }));
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("tool tool updated_at is before created_at"))
        );
        assert!(report.issues.iter().any(|issue| {
            issue.contains(&format!(
                "memory record {memory_id} updated_at is before created_at"
            ))
        }));
    }

    #[test]
    fn repairs_updated_at_before_created_at() {
        let mut os = OperatingSystem::new("test");

        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        agent.updated_at = agent.created_at - chrono::Duration::seconds(1);
        os.register_agent(agent);

        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        task.updated_at = task.created_at - chrono::Duration::seconds(1);
        os.create_task(task);

        let mut tool = ToolDefinition::new(
            "Tool",
            ToolKind::Shell,
            "",
            vec!["rust".into()],
            "printf hi",
            None,
        );
        let tool_id = tool.id.clone();
        tool.updated_at = tool.created_at - chrono::Duration::seconds(1);
        os.register_tool(tool);
        let mut memory = crate::models::MemoryRecord::new("Memory", "Body", vec![]);
        let memory_id = memory.id.clone();
        memory.updated_at = memory.created_at - chrono::Duration::seconds(1);
        os.write_memory(memory);
        os.updated_at = os.created_at - chrono::Duration::seconds(1);

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        assert!(os.updated_at >= os.created_at);
        let agent = os.agents.get(&agent_id).expect("agent");
        assert_eq!(agent.updated_at, agent.created_at);
        let task = os.tasks.get(&task_id).expect("task");
        assert_eq!(task.updated_at, task.created_at);
        let tool = os.tools.get(&tool_id).expect("tool");
        assert_eq!(tool.updated_at, tool.created_at);
        let memory = os
            .memory
            .iter()
            .find(|record| record.id == memory_id)
            .expect("memory");
        assert_eq!(memory.updated_at, memory.created_at);
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("clamped OS updated_at to created_at"))
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("clamped agent builder updated_at to created_at"))
        );
        assert!(report.repairs.iter().any(|repair| {
            repair.contains(&format!("clamped task {task_id} updated_at to created_at"))
        }));
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("clamped tool tool updated_at to created_at"))
        );
        assert!(report.repairs.iter().any(|repair| {
            repair.contains(&format!(
                "clamped memory record {memory_id} updated_at to created_at"
            ))
        }));
    }

    #[test]
    fn reports_comma_empty_capability_parts() {
        let mut os = OperatingSystem::new("test");
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        agent.capabilities = vec!["rust,".into()];
        os.register_agent(agent);

        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        task.required_capabilities = vec![",test".into()];
        os.create_task(task);

        let mut tool = ToolDefinition::new(
            "Tool",
            ToolKind::Shell,
            "",
            vec!["rust".into()],
            "printf hi",
            None,
        );
        tool.required_capabilities = vec!["code,,review".into()];
        os.register_tool(tool);

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(report.issues.iter().any(|issue| {
            issue.contains("agent builder capabilities contains an empty capability")
        }));
        assert!(report.issues.iter().any(|issue| {
            issue.contains("task")
                && issue.contains("required_capabilities contains an empty capability")
        }));
        assert!(report.issues.iter().any(|issue| {
            issue.contains("tool tool required_capabilities contains an empty capability")
        }));
    }

    #[test]
    fn repairs_normalized_list_drift() {
        let mut os = OperatingSystem::new("test");
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        agent.capabilities = vec![" Rust ".into(), "code,rust".into()];
        os.register_agent(agent);

        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        task.required_capabilities = vec![" Rust, test ".into(), "test".into()];
        os.create_task(task);

        let mut tool = ToolDefinition::new(
            "Tool",
            ToolKind::Shell,
            "",
            vec!["rust".into()],
            "printf hi",
            None,
        );
        let tool_id = tool.id.clone();
        tool.required_capabilities = vec![" Plan,plan ".into()];
        os.register_tool(tool);

        os.write_memory(MemoryRecord::new(
            "Topic",
            "Body",
            vec![" Alpha, beta ".into(), "alpha".into()],
        ));
        os.memory[0].tags = vec![" Alpha, beta ".into(), "alpha".into()];

        let validation = validate_state(&os);
        assert!(!validation.valid);
        assert!(
            validation
                .issues
                .iter()
                .any(|issue| issue.contains("agent builder capabilities are not normalized"))
        );
        assert!(
            validation
                .issues
                .iter()
                .any(|issue| issue.contains("required_capabilities are not normalized"))
        );
        assert!(
            validation
                .issues
                .iter()
                .any(|issue| issue.contains("tool tool required_capabilities are not normalized"))
        );
        assert!(
            validation
                .issues
                .iter()
                .any(|issue| issue.contains("memory record")
                    && issue.contains("tags are not normalized"))
        );

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        assert_eq!(
            os.agents.get(&agent_id).expect("agent").capabilities,
            vec!["code", "rust"]
        );
        assert_eq!(
            os.tasks.get(&task_id).expect("task").required_capabilities,
            vec!["rust", "test"]
        );
        assert_eq!(
            os.tools.get(&tool_id).expect("tool").required_capabilities,
            vec!["plan"]
        );
        assert_eq!(os.memory[0].tags, vec!["alpha", "beta"]);
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("normalized capabilities for agent builder"))
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| { repair.contains("normalized required_capabilities for task") })
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("normalized tags for memory record"))
        );
    }

    #[test]
    fn reports_malformed_tool_template_placeholders() {
        let mut os = OperatingSystem::new("test");
        os.register_tool(ToolDefinition::new(
            "Bad Placeholder",
            ToolKind::Shell,
            "",
            vec![],
            "printf { message }",
            None,
        ));

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(report.issues.iter().any(|issue| {
            issue.contains("bad-placeholder") && issue.contains("malformed template placeholder")
        }));
    }

    #[test]
    fn reports_unclosed_tool_template_placeholders() {
        let mut os = OperatingSystem::new("test");
        os.register_tool(ToolDefinition::new(
            "Unclosed Placeholder",
            ToolKind::Shell,
            "",
            vec![],
            "printf {message",
            None,
        ));

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(report.issues.iter().any(|issue| {
            issue.contains("unclosed-placeholder")
                && issue.contains("unclosed template placeholder")
        }));
    }

    #[test]
    fn reports_invalid_environment_variable_names() {
        let mut os = OperatingSystem::new("test");
        os.provider.api_key_env = " API_KEY ".into();
        os.policy.allowed_env_vars.push(" BAD_ENV ".into());
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        task.tool = Some(ToolInvocation::with_secret_env_args(
            ToolId::new("tool"),
            BTreeMap::new(),
            BTreeMap::from([("token".into(), " BAD_TOKEN ".into())]),
        ));
        os.create_task(task);

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(report.issues.iter().any(|issue| {
            issue.contains("provider api_key_env must be a valid environment variable name")
        }));
        assert!(report.issues.iter().any(|issue| {
            issue.contains("policy allowed_env_vars contains an invalid environment variable name")
        }));
        assert!(report.issues.iter().any(|issue| {
            issue.contains("secret tool args contain an invalid environment variable name")
        }));
    }

    #[test]
    fn reports_unexpected_tool_invocation_args() {
        let mut os = OperatingSystem::new("test");
        os.register_tool(ToolDefinition::new(
            "Say",
            ToolKind::Shell,
            "",
            vec![],
            "printf {message}",
            None,
        ));
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        task.tool = Some(ToolInvocation::new(
            ToolId::new("say"),
            BTreeMap::from([
                ("message".into(), "hello".into()),
                ("unused".into(), "ignored".into()),
            ]),
        ));
        os.create_task(task);

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(report.issues.iter().any(|issue| {
            issue.contains("invalid tool invocation")
                && issue.contains("unexpected tool argument `unused`")
        }));
    }

    #[test]
    fn reports_invalid_tool_invocation_arg_keys() {
        let mut os = OperatingSystem::new("test");
        os.register_tool(ToolDefinition::new(
            "Say",
            ToolKind::Shell,
            "",
            vec![],
            "printf {message}",
            None,
        ));
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        task.tool = Some(ToolInvocation::new(
            ToolId::new("say"),
            BTreeMap::from([(" message ".into(), "hello".into())]),
        ));
        os.create_task(task);

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(report.issues.iter().any(|issue| {
            issue.contains("invalid tool invocation") && issue.contains("invalid tool argument key")
        }));
    }

    #[test]
    fn repairs_invalid_tool_invocation_args() {
        let mut os = OperatingSystem::new("test");
        os.register_tool(ToolDefinition::new(
            "Say",
            ToolKind::Shell,
            "",
            vec![],
            "printf {message} {secret}",
            None,
        ));
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        task.tool = Some(ToolInvocation::with_secret_env_args(
            ToolId::new("say"),
            BTreeMap::from([
                ("message".into(), "hello".into()),
                ("unused".into(), "ignored".into()),
                (" bad ".into(), "bad".into()),
            ]),
            BTreeMap::from([
                ("message".into(), "MESSAGE_ENV".into()),
                ("token".into(), "TOKEN_ENV".into()),
                ("secret".into(), " BAD_ENV ".into()),
            ]),
        ));
        os.create_task(task);

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        let tool = os
            .tasks
            .get(&task_id)
            .expect("task")
            .tool
            .as_ref()
            .expect("tool");
        assert_eq!(
            tool.args,
            BTreeMap::from([("message".into(), "hello".into())])
        );
        assert!(tool.secret_env_args.is_empty());
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("removed unexpected tool argument unused"))
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("removed invalid tool argument key  bad "))
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("removed conflicting secret tool argument message"))
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("removed unexpected secret tool argument token"))
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("removed invalid secret env arg secret"))
        );
    }

    #[test]
    fn reports_invalid_provider_endpoint_url() {
        let mut os = OperatingSystem::new("test");
        os.provider.endpoint = Some("file:///tmp/provider".into());

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(
            report.issues.iter().any(|issue| {
                issue.contains("provider endpoint must be an absolute http(s) URL")
            })
        );
    }

    #[test]
    fn reports_current_task_that_is_not_running() {
        let mut os = OperatingSystem::new("test");
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        task.assigned_to = Some(agent_id.clone());
        agent.current_tasks.push(task_id);
        os.register_agent(agent);
        os.create_task(task);

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("non-running current task"))
        );
    }

    #[test]
    fn reports_duplicate_agent_current_tasks() {
        let mut os = OperatingSystem::new("test");
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 2);
        let agent_id = agent.id.clone();
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        task.status = TaskStatus::Running;
        task.assigned_to = Some(agent_id);
        agent.current_tasks.push(task_id.clone());
        agent.current_tasks.push(task_id);
        os.register_agent(agent);
        os.create_task(task);

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("duplicate current task"))
        );
    }

    #[test]
    fn repairs_zero_agent_parallelism() {
        let mut os = OperatingSystem::new("test");
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        agent.max_parallel_tasks = 0;
        os.register_agent(agent);

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        assert_eq!(
            os.agents.get(&agent_id).expect("agent").max_parallel_tasks,
            1
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("max_parallel_tasks to 1"))
        );
    }

    #[test]
    fn reports_agent_over_parallel_capacity() {
        let mut os = OperatingSystem::new("test");
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        for title in ["One", "Two"] {
            let mut task = Task::new(title, "Objective", Priority::Normal, vec![]);
            task.status = TaskStatus::Running;
            task.assigned_to = Some(agent_id.clone());
            agent.current_tasks.push(task.id.clone());
            os.create_task(task);
        }
        os.register_agent(agent);

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("current tasks but max_parallel_tasks"))
        );
    }

    #[test]
    fn repairs_orphan_and_expired_agent_leases() {
        let mut os = OperatingSystem::new("test");
        let mut orphan = Agent::new("Orphan", AgentKind::Builder, None, vec!["rust".into()], 1);
        let orphan_id = orphan.id.clone();
        orphan.lease_expires_at = Some(Utc::now() + chrono::Duration::seconds(60));
        let mut expired = Agent::new("Expired", AgentKind::Builder, None, vec!["rust".into()], 1);
        let expired_id = expired.id.clone();
        expired.last_heartbeat_at = Some(Utc::now() - chrono::Duration::seconds(120));
        expired.lease_expires_at = Some(Utc::now() - chrono::Duration::seconds(60));
        os.register_agent(orphan);
        os.register_agent(expired);

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("missing last_heartbeat_at"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("online with expired lease"))
        );

        let repair = repair_state(&mut os);

        assert!(repair.changed);
        assert!(repair.validation.valid);
        assert!(
            os.agents
                .get(&orphan_id)
                .expect("orphan")
                .lease_expires_at
                .is_none()
        );
        assert_eq!(
            os.agents.get(&expired_id).expect("expired").status,
            AgentStatus::Offline
        );
    }

    #[test]
    fn reports_duplicate_task_dependencies() {
        let mut os = OperatingSystem::new("test");
        let dependency = Task::new("Dependency", "Objective", Priority::Normal, vec![]);
        let dependency_id = dependency.id.clone();
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        task.dependencies.push(dependency_id.clone());
        task.dependencies.push(dependency_id);
        os.create_task(dependency);
        os.create_task(task);

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("duplicate dependency"))
        );
    }

    #[test]
    fn repairs_duplicate_task_dependencies() {
        let mut os = OperatingSystem::new("test");
        let dependency = Task::new("Dependency", "Objective", Priority::Normal, vec![]);
        let dependency_id = dependency.id.clone();
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        task.dependencies.push(dependency_id.clone());
        task.dependencies.push(dependency_id.clone());
        os.create_task(dependency);
        os.create_task(task);

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        assert_eq!(
            os.tasks.get(&task_id).expect("task").dependencies,
            vec![dependency_id]
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("removed duplicate dependency"))
        );
    }

    #[test]
    fn repairs_missing_task_dependencies() {
        let mut os = OperatingSystem::new("test");
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        task.dependencies.push(TaskId::from_slug("missing"));
        os.create_task(task);

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        assert!(
            os.tasks
                .get(&task_id)
                .expect("task")
                .dependencies
                .is_empty()
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("removed missing dependency"))
        );
    }

    #[test]
    fn repairs_missing_assigned_agent_on_non_running_task() {
        let mut os = OperatingSystem::new("test");
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        task.status = TaskStatus::Complete;
        task.assigned_to = Some(AgentId::new("missing"));
        os.create_task(task);

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        assert!(os.tasks.get(&task_id).expect("task").assigned_to.is_none());
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("cleared missing assigned agent"))
        );
    }

    #[test]
    fn repairs_empty_task_output() {
        let mut os = OperatingSystem::new("test");
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        task.output = Some(" ".into());
        os.create_task(task);

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        assert!(os.tasks.get(&task_id).expect("task").output.is_none());
        assert!(report.repairs.iter().any(|repair| {
            repair.contains(&format!("cleared empty output from task {task_id}"))
        }));
    }

    #[test]
    fn repairs_empty_task_plan_steps() {
        let mut os = OperatingSystem::new("test");
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        task.plan = vec!["first".into(), " ".into(), "second".into()];
        os.create_task(task);

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        assert_eq!(
            os.tasks.get(&task_id).expect("task").plan,
            vec!["first", "second"]
        );
        assert!(report.repairs.iter().any(|repair| {
            repair.contains(&format!("removed empty plan step from task {task_id}"))
        }));
    }

    #[test]
    fn repairs_empty_optional_metadata() {
        let mut os = OperatingSystem::new("test");
        let mut agent = Agent::new("Builder", AgentKind::Builder, Some(" ".into()), vec![], 1);
        let agent_id = agent.id.clone();
        agent.capabilities = vec!["rust".into()];
        os.register_agent(agent);

        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        task.cwd = Some(" ".into());
        os.create_task(task);

        let mut tool = ToolDefinition::new(
            "Formatter",
            ToolKind::Shell,
            "",
            vec![],
            "cargo fmt",
            Some(" ".into()),
        );
        let tool_id = tool.id.clone();
        tool.required_capabilities = vec!["rust".into()];
        os.register_tool(tool);

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        assert!(os.agents.get(&agent_id).expect("agent").model.is_none());
        assert!(os.tasks.get(&task_id).expect("task").cwd.is_none());
        assert!(os.tools.get(&tool_id).expect("tool").default_cwd.is_none());
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("cleared empty model from agent"))
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("cleared empty cwd from task"))
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("cleared empty default_cwd from tool"))
        );
    }

    #[test]
    fn repairs_policy_drift() {
        let mut os = OperatingSystem::new("test");
        os.policy.allowed_commands = vec![" ".into(), "printf".into()];
        os.policy.allowed_workspaces = vec![" ".into(), ".".into()];
        os.policy.denied_patterns = vec!["rm -rf".into(), " ".into()];
        os.policy.allowed_env_vars = vec!["PATH".into(), " GOOD_ENV ".into(), "bad-env".into()];
        os.policy.redacted_env_patterns = vec!["SECRET".into(), " ".into()];
        os.policy.max_output_bytes = 0;
        os.policy.command_timeout_seconds = 0;

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        assert_eq!(os.policy.allowed_commands, vec!["printf"]);
        assert_eq!(os.policy.allowed_workspaces, vec!["."]);
        assert_eq!(os.policy.denied_patterns, vec!["rm -rf"]);
        assert_eq!(os.policy.allowed_env_vars, vec!["PATH", "GOOD_ENV"]);
        assert_eq!(os.policy.redacted_env_patterns, vec!["SECRET"]);
        assert_eq!(
            os.policy.max_output_bytes,
            Policy::default().max_output_bytes
        );
        assert_eq!(
            os.policy.command_timeout_seconds,
            Policy::default().command_timeout_seconds
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| { repair.contains("removed empty policy allowed_commands value") })
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| { repair.contains("trimmed policy allowed_env_vars value") })
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| { repair.contains("removed invalid policy allowed_env_vars value") })
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("reset policy max_output_bytes"))
        );
    }

    #[test]
    fn repairs_provider_drift() {
        let mut os = OperatingSystem::new("test");
        os.provider.model = " ".into();
        os.provider.api_key_env = " OPENAI_API_KEY ".into();
        os.provider.endpoint = Some(" ".into());

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        assert_eq!(os.provider.model, ProviderSettings::default().model);
        assert_eq!(os.provider.api_key_env, "OPENAI_API_KEY");
        assert!(os.provider.endpoint.is_none());
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("reset provider model"))
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("trimmed provider api_key_env"))
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("cleared empty provider endpoint"))
        );

        let mut mock = OperatingSystem::new("test");
        mock.provider.endpoint = Some("not-a-url".into());

        let report = repair_state(&mut mock);

        assert!(report.changed);
        assert!(report.validation.valid);
        assert!(mock.provider.endpoint.is_none());
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("cleared invalid mock provider endpoint"))
        );
    }

    #[test]
    fn reports_invalid_persisted_ids() {
        let mut os = OperatingSystem::new("test");
        let agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        os.register_agent(agent);
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec!["rust".into()]);
        task.status = TaskStatus::Running;
        task.assigned_to = Some(agent_id.clone());
        let task_id = task.id.clone();
        os.create_task(task);
        os.agents
            .get_mut(&agent_id)
            .expect("agent")
            .current_tasks
            .push(task_id.clone());
        let run = RunRecord::new(task_id.clone(), Some(agent_id.clone()), "command", ".");
        let run_id = run.id.clone();
        os.runs.insert(run_id.clone(), run);

        let mut value = serde_json::to_value(&os).expect("state value");
        let mut agent = value["agents"]
            .as_object_mut()
            .expect("agents")
            .remove(agent_id.as_str())
            .expect("agent");
        agent["id"] = serde_json::json!("bad agent id");
        value["agents"]
            .as_object_mut()
            .expect("agents")
            .insert("bad agent id".into(), agent);

        let mut task = value["tasks"]
            .as_object_mut()
            .expect("tasks")
            .remove(task_id.as_str())
            .expect("task");
        task["id"] = serde_json::json!("bad task id");
        task["assigned_to"] = serde_json::json!("bad agent id");
        value["tasks"]
            .as_object_mut()
            .expect("tasks")
            .insert("bad task id".into(), task);

        let mut run = value["runs"]
            .as_object_mut()
            .expect("runs")
            .remove(run_id.as_str())
            .expect("run");
        run["id"] = serde_json::json!("bad run id");
        run["task_id"] = serde_json::json!("bad task id");
        run["agent_id"] = serde_json::json!("bad agent id");
        value["runs"]
            .as_object_mut()
            .expect("runs")
            .insert("bad run id".into(), run);
        let mut memory = MemoryRecord::new("Topic", "Body", vec![]);
        memory.id = "bad memory id".into();
        value["memory"] = serde_json::json!([
            memory,
            {
                "id": "duplicate-memory",
                "topic": "First duplicate",
                "body": "Body",
                "tags": [],
                "created_at": Utc::now(),
                "updated_at": Utc::now(),
            },
            {
                "id": "duplicate-memory",
                "topic": "Second duplicate",
                "body": "Body",
                "tags": [],
                "created_at": Utc::now(),
                "updated_at": Utc::now(),
            }
        ]);

        let os: OperatingSystem = serde_json::from_value(value).expect("state");
        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("agent map key bad agent id is not a valid id"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("task bad task id has invalid id"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("run bad run id has invalid id"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("run bad run id references invalid task id"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("memory record bad memory id has invalid id"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("memory records have duplicate id duplicate-memory"))
        );
    }

    #[test]
    fn reports_dependency_cycles() {
        let mut os = OperatingSystem::new("test");
        let mut first = Task::new("First", "Objective", Priority::Normal, vec![]);
        let mut second = Task::new("Second", "Objective", Priority::Normal, vec![]);
        let first_id = first.id.clone();
        let second_id = second.id.clone();
        first.dependencies.push(second_id);
        second.dependencies.push(first_id);
        os.create_task(first);
        os.create_task(second);

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("dependency cycle detected"))
        );
    }

    #[test]
    fn reports_inconsistent_run_lifecycle_fields() {
        let mut os = OperatingSystem::new("test");
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        task.command = Some("printf ok".into());
        let task_id = task.id.clone();
        let other_task = Task::new("Other", "Objective", Priority::Normal, vec![]);
        let other_task_id = other_task.id.clone();
        os.create_task(task);
        os.create_task(other_task);

        let mut active = RunRecord::new(task_id.clone(), None, "command", ".");
        active.finished_at = Some(Utc::now());
        active.exit_code = Some(0);
        active.command = " ".into();
        active.log_path = Some(" ".into());
        let mut terminal = RunRecord::new(task_id.clone(), None, "command", ".");
        terminal.status = RunStatus::Success;
        terminal.exit_code = Some(1);
        let mut inverted = RunRecord::new(other_task_id, None, "command", ".");
        inverted.status = RunStatus::Failed;
        inverted.finished_at = Some(inverted.started_at - chrono::Duration::seconds(1));
        inverted.exit_code = Some(0);
        let mut cancelled = RunRecord::new(task_id, None, "command", ".");
        cancelled.status = RunStatus::Cancelled;
        cancelled.finished_at = Some(cancelled.started_at);
        cancelled.exit_code = Some(143);
        os.runs.insert(active.id.clone(), active);
        os.runs.insert(terminal.id.clone(), terminal);
        os.runs.insert(inverted.id.clone(), inverted);
        os.runs.insert(cancelled.id.clone(), cancelled);

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("active status running but has finished_at"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("active status running but has exit_code"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("terminal status success but missing finished_at"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("finished before it started"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("has empty command"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("has empty log_path"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| { issue.contains("has success status but exit_code is not 0") })
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| { issue.contains("has failed status but successful exit_code 0") })
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| { issue.contains("has cancelled status but has exit_code") })
        );
    }

    #[test]
    fn repairs_inconsistent_run_lifecycle_fields() {
        let mut os = OperatingSystem::new("test");
        let mut task = Task::new("Task", "Objective", Priority::Normal, vec![]);
        task.command = Some("printf ok".into());
        let task_id = task.id.clone();
        let other_task = Task::new("Other", "Objective", Priority::Normal, vec![]);
        let other_task_id = other_task.id.clone();
        os.create_task(task);
        os.create_task(other_task);

        let mut active = RunRecord::new(task_id.clone(), None, "command", ".");
        active.finished_at = Some(Utc::now());
        active.exit_code = Some(0);
        active.command = " ".into();
        active.log_path = Some(" ".into());
        active.cwd = " ".into();
        let active_id = active.id.clone();
        let mut missing_finished = RunRecord::new(task_id.clone(), None, "command", ".");
        missing_finished.status = RunStatus::Success;
        missing_finished.exit_code = Some(1);
        let missing_finished_id = missing_finished.id.clone();
        let missing_finished_started_at = missing_finished.started_at;
        let mut inverted = RunRecord::new(other_task_id, None, "command", ".");
        inverted.status = RunStatus::Failed;
        inverted.finished_at = Some(inverted.started_at - chrono::Duration::seconds(1));
        inverted.exit_code = Some(0);
        let inverted_id = inverted.id.clone();
        let inverted_started_at = inverted.started_at;
        let mut rejected = RunRecord::new(task_id, None, "command", ".");
        rejected.status = RunStatus::Rejected;
        rejected.finished_at = Some(rejected.started_at);
        rejected.exit_code = Some(1);
        let rejected_id = rejected.id.clone();
        os.runs.insert(active.id.clone(), active);
        os.runs
            .insert(missing_finished.id.clone(), missing_finished);
        os.runs.insert(inverted.id.clone(), inverted);
        os.runs.insert(rejected.id.clone(), rejected);

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        let active = os.runs.get(&active_id).expect("active run");
        assert!(active.finished_at.is_none());
        assert!(active.exit_code.is_none());
        assert_eq!(active.command, "printf ok");
        assert!(active.log_path.is_none());
        assert_eq!(active.cwd, ".");
        assert_eq!(
            os.runs
                .get(&missing_finished_id)
                .expect("missing finished run")
                .finished_at,
            Some(missing_finished_started_at)
        );
        assert_eq!(
            os.runs
                .get(&missing_finished_id)
                .expect("missing finished run")
                .exit_code,
            Some(0)
        );
        assert_eq!(
            os.runs.get(&inverted_id).expect("inverted run").finished_at,
            Some(inverted_started_at)
        );
        assert_eq!(
            os.runs.get(&inverted_id).expect("inverted run").exit_code,
            None
        );
        assert_eq!(
            os.runs.get(&rejected_id).expect("rejected run").exit_code,
            None
        );
        assert!(report.repairs.iter().any(|repair| {
            repair.contains(&format!("restored command for run {active_id} from task"))
        }));
        assert!(report.repairs.iter().any(|repair| {
            repair.contains(&format!("cleared empty log_path from run {active_id}"))
        }));
        assert!(report.repairs.iter().any(|repair| {
            repair.contains(&format!("reset empty cwd from run {active_id} to ."))
        }));
        assert!(report.repairs.iter().any(|repair| {
            repair.contains(&format!(
                "set exit_code for successful run {missing_finished_id} to 0"
            ))
        }));
        assert!(report.repairs.iter().any(|repair| {
            repair.contains(&format!(
                "cleared successful exit_code from failed run {inverted_id}"
            ))
        }));
        assert!(report.repairs.iter().any(|repair| {
            repair.contains(&format!(
                "cleared exit_code from rejected run {rejected_id}"
            ))
        }));
    }

    #[test]
    fn reports_invalid_event_log_fields() {
        let mut os = OperatingSystem::new("test");
        let now = Utc::now();
        os.events = vec![
            Event {
                id: "later".into(),
                kind: EventKind::SystemBooted,
                message: "later".into(),
                at: now,
            },
            Event {
                id: "".into(),
                kind: EventKind::SystemBooted,
                message: "missing id".into(),
                at: now + chrono::Duration::seconds(1),
            },
            Event {
                id: "duplicate".into(),
                kind: EventKind::SystemBooted,
                message: "first".into(),
                at: now + chrono::Duration::seconds(2),
            },
            Event {
                id: "duplicate".into(),
                kind: EventKind::SystemBooted,
                message: "second".into(),
                at: now + chrono::Duration::seconds(3),
            },
            Event {
                id: "bad event id".into(),
                kind: EventKind::SystemBooted,
                message: "bad id".into(),
                at: now + chrono::Duration::seconds(4),
            },
            Event {
                id: "empty-message".into(),
                kind: EventKind::SystemBooted,
                message: "   ".into(),
                at: now + chrono::Duration::seconds(5),
            },
            Event {
                id: "earlier".into(),
                kind: EventKind::SystemBooted,
                message: "earlier".into(),
                at: now - chrono::Duration::seconds(1),
            },
        ];

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue == "event has empty id")
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("duplicate event id duplicate"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("event bad event id has invalid id"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("empty-message has empty message"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("event log is not sorted"))
        );
    }

    #[test]
    fn repairs_invalid_event_log_fields() {
        let mut os = OperatingSystem::new("test");
        let now = Utc::now();
        os.events = (0..501)
            .rev()
            .map(|index| Event {
                id: format!("event-{index}"),
                kind: EventKind::SystemBooted,
                message: format!("event {index}"),
                at: now + chrono::Duration::seconds(index),
            })
            .collect();
        os.events.push(Event {
            id: "event-500".into(),
            kind: EventKind::SystemBooted,
            message: "duplicate".into(),
            at: now + chrono::Duration::seconds(600),
        });
        os.events.push(Event {
            id: "".into(),
            kind: EventKind::SystemBooted,
            message: "missing id".into(),
            at: now + chrono::Duration::seconds(601),
        });
        os.events.push(Event {
            id: "bad event id".into(),
            kind: EventKind::SystemBooted,
            message: "bad id".into(),
            at: now + chrono::Duration::seconds(602),
        });
        os.events.push(Event {
            id: "empty-message".into(),
            kind: EventKind::SystemBooted,
            message: " ".into(),
            at: now + chrono::Duration::seconds(603),
        });

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        assert!(os.events.len() <= 500);
        assert!(!os.events.iter().any(|event| event.id.trim().is_empty()));
        assert!(os.events.iter().all(|event| is_valid_slug(&event.id)));
        assert!(
            !os.events
                .iter()
                .any(|event| event.message.trim().is_empty())
        );
        assert!(
            os.events
                .windows(2)
                .all(|events| events[0].at <= events[1].at)
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("removed duplicate event event-500"))
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("removed event bad event id with invalid id"))
        );
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("sorted events by timestamp"))
        );
    }

    #[test]
    fn validates_and_repairs_daemon_state_consistency() {
        let mut os = OperatingSystem::new("test");
        os.daemon = Some(DaemonState {
            status: DaemonStatus::Stopped,
            pid: Some(42),
            ticks: 1,
            limit: 0,
            execute: false,
            stop_requested: true,
            last_tick_at: None,
            last_message: Some(" ".into()),
        });

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("stopped but still has pid"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("stop request is still set"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("daemon limit must be greater than 0"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("daemon last_message is empty"))
        );

        let repair = repair_state(&mut os);

        assert!(repair.changed);
        assert!(repair.validation.valid);
        let daemon = os.daemon.as_ref().expect("daemon");
        assert!(daemon.pid.is_none());
        assert!(!daemon.stop_requested);
        assert_eq!(daemon.limit, 1);
        assert!(daemon.last_message.is_none());
        assert!(
            repair
                .repairs
                .iter()
                .any(|repair| repair.contains("raised daemon limit to 1"))
        );
        assert!(
            repair
                .repairs
                .iter()
                .any(|repair| repair.contains("cleared empty daemon last_message"))
        );
    }

    #[test]
    fn reports_running_daemon_missing_pid() {
        let mut os = OperatingSystem::new("test");
        os.daemon = Some(DaemonState {
            status: DaemonStatus::Running,
            pid: None,
            ticks: 1,
            limit: 1,
            execute: false,
            stop_requested: false,
            last_tick_at: None,
            last_message: None,
        });

        let report = validate_state(&os);

        assert!(!report.valid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("running but missing pid"))
        );
    }

    #[test]
    fn repairs_assignment_indexes() {
        let mut os = OperatingSystem::new("test");
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        let mut running = Task::new("Run", "run", Priority::Normal, vec![]);
        let running_id = running.id.clone();
        running.status = TaskStatus::Running;
        running.assigned_to = Some(agent_id.clone());
        let stale = Task::new("Stale", "stale", Priority::Normal, vec![]);
        let stale_id = stale.id.clone();
        agent.current_tasks.push(stale_id);
        agent.current_tasks.push(TaskId::from_slug("missing"));
        os.register_agent(agent);
        os.create_task(running);
        os.create_task(stale);

        let report = repair_state(&mut os);

        assert!(report.changed);
        assert!(report.validation.valid);
        assert_eq!(
            os.agents.get(&agent_id).expect("agent").current_tasks,
            vec![running_id]
        );
        assert!(
            os.events
                .iter()
                .any(|event| event.kind == EventKind::StateRepaired)
        );
    }
}
