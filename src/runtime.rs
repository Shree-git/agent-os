use crate::models::{
    Agent, AgentId, AgentKind, AgentStatus, DaemonStatus, EventKind, OperatingSystem, Priority,
    RunArtifact, RunId, RunRecord, RunStatus, Task, TaskId, TaskStatus, ToolDefinition, ToolId,
    ToolInvocation, ToolKind, WorkerNode, WorkflowId, normalize_list,
};
use crate::scheduler::{Assignment, Scheduler};
use crate::tools::{validate_tool_invocation, validate_tool_template};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::process::{Command, Stdio};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("task not found: {0}")]
    TaskNotFound(TaskId),
    #[error("task {task_id} is invalid: {reason}")]
    TaskInvalid { task_id: TaskId, reason: String },
    #[error("task {task_id} cannot be edited while {status}")]
    TaskNotEditable { task_id: TaskId, status: TaskStatus },
    #[error("agent not found for task: {0}")]
    AgentMissing(TaskId),
    #[error("agent not found: {0}")]
    AgentNotFound(AgentId),
    #[error("agent {agent_id} is invalid: {reason}")]
    AgentInvalid { agent_id: AgentId, reason: String },
    #[error("agent {agent_id} update is incompatible with task {task_id}: {reason}")]
    AgentIncompatible {
        agent_id: AgentId,
        task_id: TaskId,
        reason: String,
    },
    #[error("task dependency {dependency} is not complete for task {task_id}")]
    TaskDependencyIncomplete { task_id: TaskId, dependency: TaskId },
    #[error("dependency task not found: {0}")]
    TaskDependencyNotFound(TaskId),
    #[error("duplicate dependency task: {0}")]
    DuplicateTaskDependency(TaskId),
    #[error("task cannot depend on itself: {0}")]
    SelfDependency(TaskId),
    #[error("agent {agent_id} cannot accept task {task_id}: {reason}")]
    AgentCannotAccept {
        agent_id: AgentId,
        task_id: TaskId,
        reason: String,
    },
    #[error(
        "task {task_id} is still referenced; delete dependent tasks, remove referencing workflows, or prune referencing runs first: {references}"
    )]
    TaskReferenced { task_id: TaskId, references: String },
    #[error("tool not found: {0}")]
    ToolNotFound(ToolId),
    #[error("workflow not found: {0}")]
    WorkflowNotFound(WorkflowId),
    #[error("worker not found: {0}")]
    WorkerNotFound(String),
    #[error("worker {worker_id} cannot report task {task_id}: {reason}")]
    WorkerReportRejected {
        worker_id: String,
        task_id: TaskId,
        reason: String,
    },
    #[error("tool {tool_id} is invalid: {reason}")]
    ToolInvalid { tool_id: ToolId, reason: String },
    #[error("tool {tool_id} update is incompatible with task {task_id}: {reason}")]
    ToolIncompatible {
        tool_id: ToolId,
        task_id: TaskId,
        reason: String,
    },
    #[error("task cannot transition from {from} to {to}: {task_id}")]
    InvalidTransition {
        task_id: TaskId,
        from: TaskStatus,
        to: TaskStatus,
    },
}

#[derive(Clone, Debug, Default)]
pub struct ToolUpdate {
    pub kind: Option<ToolKind>,
    pub description: Option<String>,
    pub required_capabilities: Option<Vec<String>>,
    pub command_template: Option<String>,
    pub default_cwd: Option<Option<String>>,
}

#[derive(Clone, Debug, Default)]
pub struct AgentUpdate {
    pub name: Option<String>,
    pub kind: Option<AgentKind>,
    pub model: Option<Option<String>>,
    pub capabilities: Option<Vec<String>>,
    pub max_parallel_tasks: Option<usize>,
}

#[derive(Clone, Debug, Default)]
pub struct TaskUpdate {
    pub title: Option<String>,
    pub objective: Option<String>,
    pub command: Option<Option<String>>,
    pub tool: Option<Option<ToolInvocation>>,
    pub cwd: Option<Option<String>>,
    pub required_capabilities: Option<Vec<String>>,
    pub max_attempts: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        Agent, AgentKind, AgentStatus, DaemonState, DaemonStatus, OperatingSystem, Priority,
        RunRecord, Task, ToolDefinition, ToolInvocation, ToolKind, WorkerNode, Workflow,
    };
    use std::collections::BTreeMap;

    #[test]
    fn recovers_stale_running_tasks_and_releases_agent_capacity() {
        let mut os = OperatingSystem::new("test");
        let agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        os.register_agent(agent);
        let task = Task::new("Build", "build", Priority::Normal, vec!["rust".into()]);
        let task_id = task.id.clone();
        os.create_task(task);
        let assignment = Scheduler::assign_next(&mut os).expect("assignment");
        assert_eq!(assignment.task_id, task_id);

        let recovered = Runtime::recover_stale_tasks(&mut os, Duration::zero());

        assert_eq!(recovered, vec![task_id.clone()]);
        let task = os.tasks.get(&task_id).expect("task");
        assert_eq!(task.status, TaskStatus::Pending);
        assert!(task.assigned_to.is_none());
        assert!(
            os.agents
                .get(&agent_id)
                .expect("agent")
                .current_tasks
                .is_empty()
        );
    }

    #[test]
    fn recovery_removes_stale_task_from_all_agent_current_lists() {
        let mut os = OperatingSystem::new("test");
        let mut first = Agent::new("First", AgentKind::Builder, None, vec!["rust".into()], 1);
        let mut second = Agent::new("Second", AgentKind::Reviewer, None, vec!["rust".into()], 1);
        let first_id = first.id.clone();
        let second_id = second.id.clone();
        let mut task = Task::new("Build", "build", Priority::Normal, vec!["rust".into()]);
        let task_id = task.id.clone();
        task.status = TaskStatus::Running;
        task.assigned_to = Some(first_id.clone());
        first.current_tasks.push(task_id.clone());
        second.current_tasks.push(task_id.clone());
        os.register_agent(first);
        os.register_agent(second);
        os.create_task(task);

        let recovered = Runtime::recover_stale_tasks(&mut os, Duration::zero());

        assert_eq!(recovered, vec![task_id.clone()]);
        assert!(
            os.agents
                .get(&first_id)
                .expect("first")
                .current_tasks
                .is_empty()
        );
        assert!(
            os.agents
                .get(&second_id)
                .expect("second")
                .current_tasks
                .is_empty()
        );
        let task = os.tasks.get(&task_id).expect("task");
        assert_eq!(task.status, TaskStatus::Pending);
        assert!(task.assigned_to.is_none());
    }

    #[test]
    fn tick_expires_agent_leases_before_scheduling() {
        let mut os = OperatingSystem::new("test");
        let mut expired_agent =
            Agent::new("Expired", AgentKind::Builder, None, vec!["rust".into()], 1);
        let expired_agent_id = expired_agent.id.clone();
        expired_agent.lease_expires_at = Some(Utc::now() - Duration::seconds(1));
        os.register_agent(expired_agent);
        os.create_task(Task::new(
            "Build",
            "build",
            Priority::Normal,
            vec!["rust".into()],
        ));

        let report = Runtime::tick(&mut os, 1);

        assert_eq!(report.expired_agents, vec![expired_agent_id.clone()]);
        assert!(report.assignments.is_empty());
        assert_eq!(
            os.agents.get(&expired_agent_id).expect("agent").status,
            AgentStatus::Offline
        );
        assert!(
            os.events
                .iter()
                .any(|event| event.message.contains("expired lease for agent"))
        );
    }

    #[test]
    fn worker_heartbeat_refreshes_matching_agent_lease() {
        let mut os = OperatingSystem::new("test");
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        agent.lease_expires_at = Some(Utc::now() - Duration::seconds(60));
        let agent_id = agent.id.clone();
        os.register_agent(agent);
        os.workers.insert(
            agent_id.to_string(),
            WorkerNode {
                id: agent_id.to_string(),
                endpoint: "http://127.0.0.1:9100".into(),
                status: AgentStatus::Online,
                last_seen_at: Utc::now() - Duration::seconds(60),
            },
        );

        let worker = Runtime::heartbeat_worker(
            &mut os,
            agent_id.as_str(),
            Some("http://127.0.0.1:9101".into()),
            Some(AgentStatus::Busy),
            Some(120),
        )
        .expect("worker heartbeat");

        assert_eq!(worker.endpoint, "http://127.0.0.1:9101");
        assert_eq!(worker.status, AgentStatus::Busy);
        let agent = os.agents.get(&agent_id).expect("agent");
        assert_eq!(agent.status, AgentStatus::Busy);
        assert!(agent.last_heartbeat_at.is_some());
        assert!(
            agent
                .lease_expires_at
                .expect("lease")
                .signed_duration_since(Utc::now())
                .num_seconds()
                > 0
        );
    }

    #[test]
    fn worker_heartbeat_preserves_matching_agent_lease_when_omitted() {
        let mut os = OperatingSystem::new("test");
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let lease = Utc::now() + Duration::seconds(60);
        let agent_id = agent.id.clone();
        agent.lease_expires_at = Some(lease);
        os.register_agent(agent);
        os.workers.insert(
            agent_id.to_string(),
            WorkerNode {
                id: agent_id.to_string(),
                endpoint: "http://127.0.0.1:9100".into(),
                status: AgentStatus::Online,
                last_seen_at: Utc::now(),
            },
        );

        Runtime::heartbeat_worker(&mut os, agent_id.as_str(), None, None, None)
            .expect("worker heartbeat");

        assert_eq!(
            os.agents.get(&agent_id).expect("agent").lease_expires_at,
            Some(lease)
        );
    }

    #[test]
    fn recovery_marks_active_runs_failed() {
        let mut os = OperatingSystem::new("test");
        let agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        os.register_agent(agent);
        let mut task = Task::new("Build", "build", Priority::Normal, vec!["rust".into()]);
        let task_id = task.id.clone();
        task.status = TaskStatus::Running;
        task.assigned_to = Some(agent_id.clone());
        os.create_task(task);
        os.agents
            .get_mut(&agent_id)
            .expect("agent")
            .current_tasks
            .push(task_id.clone());
        let run = RunRecord::new(task_id.clone(), Some(agent_id), "sleep 60", ".");
        let run_id = run.id.clone();
        os.runs.insert(run_id.clone(), run);

        let recovered = Runtime::recover_stale_tasks(&mut os, Duration::zero());

        assert_eq!(recovered, vec![task_id]);
        let run = os.runs.get(&run_id).expect("run");
        assert_eq!(run.status, RunStatus::Failed);
        assert!(run.finished_at.is_some());
    }

    #[test]
    fn recovery_uses_stale_active_run_age_when_task_was_recently_updated() {
        let mut os = OperatingSystem::new("test");
        let agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        os.register_agent(agent);
        let mut task = Task::new("Build", "build", Priority::Normal, vec!["rust".into()]);
        let task_id = task.id.clone();
        task.status = TaskStatus::Running;
        task.assigned_to = Some(agent_id.clone());
        task.updated_at = Utc::now();
        os.create_task(task);
        os.agents
            .get_mut(&agent_id)
            .expect("agent")
            .current_tasks
            .push(task_id.clone());
        let mut run = RunRecord::new(task_id.clone(), Some(agent_id.clone()), "sleep 60", ".");
        run.status = RunStatus::CancelRequested;
        run.started_at = Utc::now() - Duration::seconds(60);
        let run_id = run.id.clone();
        os.runs.insert(run_id.clone(), run);

        let recovery = Runtime::recover_stale_work(&mut os, Duration::seconds(30));

        assert_eq!(recovery.recovered_tasks, vec![task_id.clone()]);
        assert_eq!(recovery.recovered_runs, vec![run_id.clone()]);
        let task = os.tasks.get(&task_id).expect("task");
        assert_eq!(task.status, TaskStatus::Pending);
        assert!(task.assigned_to.is_none());
        assert!(
            os.agents
                .get(&agent_id)
                .expect("agent")
                .current_tasks
                .is_empty()
        );
        let run = os.runs.get(&run_id).expect("run");
        assert_eq!(run.status, RunStatus::Failed);
        assert!(run.finished_at.is_some());
    }

    #[test]
    fn stale_recovery_reports_runs_and_orphan_active_runs() {
        let mut os = OperatingSystem::new("test");
        let agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        os.register_agent(agent);
        let mut stale_task = Task::new("Build", "build", Priority::Normal, vec!["rust".into()]);
        let stale_task_id = stale_task.id.clone();
        stale_task.status = TaskStatus::Running;
        stale_task.assigned_to = Some(agent_id.clone());
        stale_task.updated_at = Utc::now() - Duration::seconds(60);
        os.create_task(stale_task);
        os.agents
            .get_mut(&agent_id)
            .expect("agent")
            .current_tasks
            .push(stale_task_id.clone());
        let mut stale_run = RunRecord::new(
            stale_task_id.clone(),
            Some(agent_id.clone()),
            "sleep 60",
            ".",
        );
        stale_run.started_at = Utc::now() - Duration::seconds(60);
        let stale_run_id = stale_run.id.clone();
        os.runs.insert(stale_run_id.clone(), stale_run);

        let mut complete_task = Task::new("Done", "done", Priority::Normal, vec![]);
        let complete_task_id = complete_task.id.clone();
        complete_task.assigned_to = Some(agent_id.clone());
        os.create_task(complete_task);
        Runtime::complete_task(&mut os, &complete_task_id, None).expect("complete");
        os.agents
            .get_mut(&agent_id)
            .expect("agent")
            .current_tasks
            .push(complete_task_id.clone());
        let mut orphan_run = RunRecord::new(
            complete_task_id.clone(),
            Some(agent_id.clone()),
            "old command",
            ".",
        );
        orphan_run.started_at = Utc::now() - Duration::seconds(60);
        let orphan_run_id = orphan_run.id.clone();
        os.runs.insert(orphan_run_id.clone(), orphan_run);

        let recovery = Runtime::recover_stale_work(&mut os, Duration::seconds(30));

        assert_eq!(recovery.recovered_tasks, vec![stale_task_id]);
        assert_eq!(
            recovery.recovered_runs,
            vec![stale_run_id.clone(), orphan_run_id.clone()]
        );
        assert!(
            recovery
                .notes
                .iter()
                .any(|note| note == "recovered 2 active run record(s)")
        );
        assert_eq!(
            os.runs.get(&stale_run_id).expect("stale run").status,
            RunStatus::Failed
        );
        assert_eq!(
            os.runs.get(&orphan_run_id).expect("orphan run").status,
            RunStatus::Failed
        );
        assert!(
            os.tasks
                .get(&complete_task_id)
                .expect("complete task")
                .assigned_to
                .is_none()
        );
        assert!(
            !os.agents
                .get(&agent_id)
                .expect("agent")
                .current_tasks
                .contains(&complete_task_id)
        );
    }

    #[test]
    fn crashed_daemon_recovery_clears_dead_pid() {
        let mut os = OperatingSystem::new("test");
        os.daemon = Some(DaemonState::running(42, 1, true));

        assert!(Runtime::recover_crashed_daemon_with(&mut os, |_| false));

        let daemon = os.daemon.as_ref().expect("daemon");
        assert_eq!(daemon.status, DaemonStatus::Stopped);
        assert!(daemon.pid.is_none());
        assert!(!daemon.stop_requested);
        assert_eq!(
            daemon.last_message.as_deref(),
            Some("recovered crashed daemon state for pid 42")
        );
    }

    #[test]
    fn windows_tasklist_parser_detects_matching_pid() {
        let output = br#""Image Name","PID","Session Name","Session#","Mem Usage"
"agent-os.exe","4242","Console","1","12,340 K"
"other.exe","5151","Console","1","4,000 K"
"#;

        assert!(windows_tasklist_contains_pid(output, 4242));
        assert!(!windows_tasklist_contains_pid(output, 42));
    }

    #[test]
    fn windows_tasklist_parser_treats_no_task_output_as_dead() {
        assert!(!windows_tasklist_contains_pid(
            b"INFO: No tasks are running which match the specified criteria.\r\n",
            4242
        ));
    }

    #[test]
    fn cancel_task_requests_active_run_cancellation() {
        let mut os = OperatingSystem::new("test");
        let mut task = Task::new("Run", "run", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        task.status = TaskStatus::Running;
        os.create_task(task);
        let mut run = RunRecord::new(task_id.clone(), None, "sleep 60", ".");
        run.finished_at = Some(Utc::now());
        run.exit_code = Some(0);
        let run_id = run.id.clone();
        os.runs.insert(run_id.clone(), run);

        Runtime::cancel_task(&mut os, &task_id, Some("stop".into())).expect("cancel");

        let run = os.runs.get(&run_id).expect("run");
        assert_eq!(run.status, RunStatus::CancelRequested);
        assert!(run.finished_at.is_none());
        assert!(run.exit_code.is_none());
    }

    #[test]
    fn lifecycle_transitions_tolerate_missing_assigned_agent() {
        let mut os = OperatingSystem::new("test");
        let mut task = Task::new("Complete", "finish", Priority::Normal, vec!["rust".into()]);
        let task_id = task.id.clone();
        task.status = TaskStatus::Running;
        task.assigned_to = Some(AgentId::new("missing-agent"));
        task.attempts = 1;
        os.create_task(task);

        Runtime::complete_task(&mut os, &task_id, Some("done".into())).expect("complete");

        let task = os.tasks.get(&task_id).expect("task");
        assert_eq!(task.status, TaskStatus::Complete);
        assert_eq!(task.output.as_deref(), Some("done"));

        Runtime::retry_task(&mut os, &task_id, Some("try again".into())).expect("retry");

        let task = os.tasks.get(&task_id).expect("task");
        assert_eq!(task.status, TaskStatus::Pending);
        assert!(task.assigned_to.is_none());
        assert_eq!(task.output.as_deref(), Some("try again"));
        assert_eq!(task.attempts, 1);
    }

    #[test]
    fn failed_attempt_requeues_until_max_attempts_then_fails() {
        let mut os = OperatingSystem::new("test");
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        let mut task = Task::new("Retry", "retry once", Priority::Normal, vec!["rust".into()]);
        task.max_attempts = 2;
        task.status = TaskStatus::Running;
        task.attempts = 1;
        task.assigned_to = Some(agent_id.clone());
        let task_id = task.id.clone();
        agent.current_tasks.push(task_id.clone());
        os.register_agent(agent);
        os.create_task(task);

        Runtime::fail_task_or_retry(&mut os, &task_id, Some("exit 1".into()))
            .expect("first failure requeues");

        let task = os.tasks.get(&task_id).expect("task");
        assert_eq!(task.status, TaskStatus::Pending);
        assert_eq!(task.attempts, 1);
        assert!(task.assigned_to.is_none());
        assert!(
            os.agents
                .get(&agent_id)
                .expect("agent")
                .current_tasks
                .is_empty()
        );
        assert!(
            task.output
                .as_deref()
                .expect("output")
                .contains("retrying after attempt 1 of 2")
        );

        let task = os.tasks.get_mut(&task_id).expect("task");
        task.status = TaskStatus::Running;
        task.attempts = 2;
        task.assigned_to = Some(agent_id.clone());
        os.agents
            .get_mut(&agent_id)
            .expect("agent")
            .current_tasks
            .push(task_id.clone());

        Runtime::fail_task_or_retry(&mut os, &task_id, Some("exit 1".into()))
            .expect("second failure fails");

        let task = os.tasks.get(&task_id).expect("task");
        assert_eq!(task.status, TaskStatus::Failed);
        assert_eq!(task.attempts, 2);
        assert!(
            os.agents
                .get(&agent_id)
                .expect("agent")
                .current_tasks
                .is_empty()
        );
    }

    #[test]
    fn request_daemon_stop_only_marks_running_daemon() {
        let mut os = OperatingSystem::new("test");

        assert!(!Runtime::request_daemon_stop(&mut os));

        os.daemon = Some(DaemonState {
            status: DaemonStatus::Stopped,
            pid: None,
            ticks: 1,
            limit: 1,
            execute: false,
            stop_requested: false,
            last_tick_at: None,
            last_message: Some("stopped".into()),
        });
        assert!(!Runtime::request_daemon_stop(&mut os));
        assert!(!os.daemon.as_ref().expect("stopped daemon").stop_requested);

        os.daemon = Some(DaemonState::running(42, 1, true));
        assert!(Runtime::request_daemon_stop(&mut os));
        let daemon = os.daemon.as_ref().expect("running daemon");
        assert!(daemon.stop_requested);
        assert_eq!(daemon.last_message.as_deref(), Some("stop requested"));
    }

    #[test]
    fn cancel_workflow_cancels_only_active_workflow_tasks() {
        let mut os = OperatingSystem::new("test");
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 2);

        let mut done = Task::new("Done", "done", Priority::Normal, vec![]);
        done.status = TaskStatus::Complete;
        let done_id = done.id.clone();

        let mut pending = Task::new("Pending", "pending", Priority::Normal, vec![]);
        pending.status = TaskStatus::Pending;
        let pending_id = pending.id.clone();

        let mut running = Task::new("Running", "running", Priority::Normal, vec![]);
        running.status = TaskStatus::Running;
        running.assigned_to = Some(agent.id.clone());
        let running_id = running.id.clone();
        agent.current_tasks.push(running_id.clone());

        let mut failed = Task::new("Failed", "failed", Priority::Normal, vec![]);
        failed.status = TaskStatus::Failed;
        let failed_id = failed.id.clone();

        os.register_agent(agent);
        os.create_task(done);
        os.create_task(pending);
        os.create_task(running);
        os.create_task(failed);

        let workflow = Workflow::new(
            "Release",
            Priority::Normal,
            BTreeMap::from([
                ("done".into(), done_id.clone()),
                ("pending".into(), pending_id.clone()),
                ("running".into(), running_id.clone()),
                ("failed".into(), failed_id.clone()),
            ]),
        );
        let workflow_id = workflow.id.clone();
        os.create_workflow(workflow);

        let cancelled =
            Runtime::cancel_workflow(&mut os, &workflow_id, Some("stop".into())).expect("cancel");

        assert_eq!(cancelled, vec![pending_id.clone(), running_id.clone()]);
        assert_eq!(
            os.tasks.get(&done_id).expect("done").status,
            TaskStatus::Complete
        );
        assert_eq!(
            os.tasks.get(&pending_id).expect("pending").status,
            TaskStatus::Cancelled
        );
        assert_eq!(
            os.tasks.get(&running_id).expect("running").status,
            TaskStatus::Cancelled
        );
        assert_eq!(
            os.tasks.get(&failed_id).expect("failed").status,
            TaskStatus::Failed
        );
        assert_eq!(
            os.tasks
                .get(&running_id)
                .expect("running")
                .output
                .as_deref(),
            Some("stop")
        );
        assert!(
            os.agents
                .values()
                .all(|agent| !agent.current_tasks.contains(&running_id))
        );
    }

    #[test]
    fn manual_assignment_checks_readiness_and_agent_fit() {
        let mut os = OperatingSystem::new("test");
        let agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        os.register_agent(agent);
        let dependency = Task::new("Plan", "plan", Priority::Normal, vec!["rust".into()]);
        let dependency_id = dependency.id.clone();
        os.create_task(dependency);
        let mut task = Task::new("Build", "build", Priority::Normal, vec!["rust".into()]);
        let task_id = task.id.clone();
        task.dependencies.push(dependency_id.clone());
        os.create_task(task);

        let error =
            Runtime::assign_task(&mut os, &task_id, &agent_id).expect_err("dependency blocks");
        assert!(matches!(
            error,
            RuntimeError::TaskDependencyIncomplete { .. }
        ));

        Runtime::complete_task(&mut os, &dependency_id, None).expect("complete dependency");
        let assignment = Runtime::assign_task(&mut os, &task_id, &agent_id).expect("assign");

        assert_eq!(assignment.task_id, task_id);
        let task = os.tasks.get(&assignment.task_id).expect("task");
        assert_eq!(task.status, TaskStatus::Running);
        assert_eq!(task.assigned_to.as_ref(), Some(&agent_id));
        assert_eq!(
            os.agents.get(&agent_id).expect("agent").current_tasks,
            vec![assignment.task_id]
        );
    }

    #[test]
    fn reprioritize_task_updates_priority_and_records_event() {
        let mut os = OperatingSystem::new("test");
        let task = Task::new("Queued", "queue", Priority::Low, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);

        Runtime::reprioritize_task(&mut os, &task_id, Priority::Critical).expect("reprioritize");

        assert_eq!(
            os.tasks.get(&task_id).expect("task").priority,
            Priority::Critical
        );
        assert!(
            os.events
                .iter()
                .any(|event| event.message.contains("set priority for task"))
        );
    }

    #[test]
    fn reprioritize_task_rejects_non_editable_tasks() {
        let mut os = OperatingSystem::new("test");
        let mut task = Task::new("Done", "done", Priority::Normal, vec![]);
        task.status = TaskStatus::Complete;
        let task_id = task.id.clone();
        os.create_task(task);

        let error = Runtime::reprioritize_task(&mut os, &task_id, Priority::Critical)
            .expect_err("completed task priority should be immutable");

        assert!(matches!(error, RuntimeError::TaskNotEditable { .. }));
        assert_eq!(
            os.tasks.get(&task_id).expect("task").priority,
            Priority::Normal
        );
    }

    #[test]
    fn set_task_dependencies_replaces_dependency_list() {
        let mut os = OperatingSystem::new("test");
        let dependency = Task::new("Plan", "plan", Priority::Normal, vec![]);
        let dependency_id = dependency.id.clone();
        os.create_task(dependency);
        let task = Task::new("Build", "build", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);

        Runtime::set_task_dependencies(&mut os, &task_id, vec![dependency_id.clone()])
            .expect("set dependencies");

        assert_eq!(
            os.tasks.get(&task_id).expect("task").dependencies,
            vec![dependency_id]
        );
        assert!(
            os.events
                .iter()
                .any(|event| event.message.contains("updated dependencies for task"))
        );

        Runtime::set_task_dependencies(&mut os, &task_id, Vec::new()).expect("clear dependencies");
        assert!(
            os.tasks
                .get(&task_id)
                .expect("task")
                .dependencies
                .is_empty()
        );
    }

    #[test]
    fn set_task_dependencies_rejects_non_editable_tasks() {
        let mut os = OperatingSystem::new("test");
        let dependency = Task::new("Plan", "plan", Priority::Normal, vec![]);
        let dependency_id = dependency.id.clone();
        os.create_task(dependency);
        let mut task = Task::new("Done", "done", Priority::Normal, vec![]);
        task.status = TaskStatus::Complete;
        let task_id = task.id.clone();
        os.create_task(task);

        let error = Runtime::set_task_dependencies(&mut os, &task_id, vec![dependency_id])
            .expect_err("completed task dependencies should be immutable");

        assert!(matches!(error, RuntimeError::TaskNotEditable { .. }));
        assert!(
            os.tasks
                .get(&task_id)
                .expect("task")
                .dependencies
                .is_empty()
        );
    }

    #[test]
    fn set_task_plan_replaces_steps_and_rejects_non_editable_tasks() {
        let mut os = OperatingSystem::new("test");
        let task = Task::new("Plan", "plan", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);

        let updated = Runtime::set_task_plan(&mut os, &task_id, vec!["one".into(), "two".into()])
            .expect("set plan");

        assert_eq!(updated.plan, vec!["one", "two"]);
        assert!(
            os.events
                .iter()
                .any(|event| event.message.contains("updated plan for task"))
        );

        os.tasks.get_mut(&task_id).expect("task").status = TaskStatus::Complete;
        let error = Runtime::set_task_plan(&mut os, &task_id, vec!["changed".into()])
            .expect_err("completed task plan should be immutable");

        assert!(matches!(error, RuntimeError::TaskNotEditable { .. }));
        assert_eq!(
            os.tasks.get(&task_id).expect("task").plan,
            vec!["one", "two"]
        );
    }

    #[test]
    fn update_agent_rewrites_metadata_and_records_event() {
        let mut os = OperatingSystem::new("test");
        let agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 2);
        let agent_id = agent.id.clone();
        os.register_agent(agent);

        let updated = Runtime::update_agent(
            &mut os,
            &agent_id,
            AgentUpdate {
                name: Some("Senior Builder".into()),
                kind: Some(AgentKind::Reviewer),
                model: Some(Some("model-a".into())),
                capabilities: Some(vec!["code".into(), "rust".into()]),
                max_parallel_tasks: Some(3),
            },
        )
        .expect("update agent");

        assert_eq!(updated.id, agent_id);
        assert_eq!(updated.name, "Senior Builder");
        assert_eq!(updated.kind, AgentKind::Reviewer);
        assert_eq!(updated.model.as_deref(), Some("model-a"));
        assert_eq!(updated.capabilities, vec!["code", "rust"]);
        assert_eq!(updated.max_parallel_tasks, 3);
        assert!(os.events.iter().any(|event| {
            event.kind == EventKind::AgentUpdated && event.message.contains("updated agent builder")
        }));
    }

    #[test]
    fn update_agent_rejects_current_work_incompatibility() {
        let mut os = OperatingSystem::new("test");
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 2);
        let agent_id = agent.id.clone();
        let mut task = Task::new("Build", "build", Priority::Normal, vec!["rust".into()]);
        let task_id = task.id.clone();
        task.status = TaskStatus::Running;
        task.assigned_to = Some(agent_id.clone());
        agent.current_tasks.push(task_id.clone());
        os.register_agent(agent);
        os.create_task(task);

        let error = Runtime::update_agent(
            &mut os,
            &agent_id,
            AgentUpdate {
                capabilities: Some(vec!["docs".into()]),
                ..AgentUpdate::default()
            },
        )
        .expect_err("capability regression");
        assert!(matches!(
            error,
            RuntimeError::AgentIncompatible {
                agent_id: ref seen_agent,
                task_id: ref seen_task,
                ..
            } if seen_agent == &agent_id && seen_task == &task_id
        ));

        let error = Runtime::update_agent(
            &mut os,
            &agent_id,
            AgentUpdate {
                max_parallel_tasks: Some(0),
                ..AgentUpdate::default()
            },
        )
        .expect_err("zero capacity");
        assert!(matches!(error, RuntimeError::AgentInvalid { .. }));
    }

    #[test]
    fn update_tool_rewrites_definition_and_records_event() {
        let mut os = OperatingSystem::new("test");
        os.register_tool(ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "old",
            vec!["rust".into()],
            "printf {message}",
            None,
        ));
        let tool_id = ToolId::new("say");

        let updated = Runtime::update_tool(
            &mut os,
            &tool_id,
            ToolUpdate {
                description: Some("new".into()),
                required_capabilities: Some(vec!["code".into(), "rust".into()]),
                command_template: Some("printf updated:{message}".into()),
                default_cwd: Some(Some("/tmp".into())),
                ..ToolUpdate::default()
            },
        )
        .expect("update tool");

        assert_eq!(updated.description, "new");
        assert_eq!(updated.required_capabilities, vec!["code", "rust"]);
        assert_eq!(updated.command_template, "printf updated:{message}");
        assert_eq!(updated.default_cwd.as_deref(), Some("/tmp"));
        assert!(os.events.iter().any(|event| {
            event.kind == EventKind::ToolUpdated && event.message.contains("updated tool say")
        }));
    }

    #[test]
    fn update_tool_rejects_incompatible_existing_task_args() {
        let mut os = OperatingSystem::new("test");
        os.register_tool(ToolDefinition::new(
            "say",
            ToolKind::Shell,
            "",
            vec![],
            "printf {message}",
            None,
        ));
        let tool_id = ToolId::new("say");
        let mut task = Task::new("Use tool", "use tool", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        task.tool = Some(ToolInvocation::new(
            tool_id.clone(),
            [("message".into(), "hello".into())].into(),
        ));
        os.create_task(task);

        let error = Runtime::update_tool(
            &mut os,
            &tool_id,
            ToolUpdate {
                command_template: Some("printf static".into()),
                ..ToolUpdate::default()
            },
        )
        .expect_err("incompatible update");

        assert!(matches!(
            error,
            RuntimeError::ToolIncompatible {
                tool_id: ref seen_tool,
                task_id: ref seen_task,
                ..
            } if seen_tool == &tool_id && seen_task == &task_id
        ));
    }

    #[test]
    fn update_task_rewrites_pending_task_spec_and_records_event() {
        let mut os = OperatingSystem::new("test");
        let mut task = Task::new(
            "Old",
            "old objective",
            Priority::Normal,
            vec!["rust".into()],
        );
        task.command = Some("printf old".into());
        let task_id = task.id.clone();
        os.create_task(task);

        let updated = Runtime::update_task(
            &mut os,
            &task_id,
            TaskUpdate {
                title: Some("New".into()),
                objective: Some("new objective".into()),
                command: Some(Some("printf new".into())),
                tool: None,
                cwd: Some(Some("/tmp".into())),
                required_capabilities: Some(vec!["code".into(), "rust".into()]),
                max_attempts: None,
            },
        )
        .expect("update task");

        assert_eq!(updated.title, "New");
        assert_eq!(updated.objective, "new objective");
        assert_eq!(updated.command.as_deref(), Some("printf new"));
        assert_eq!(updated.cwd.as_deref(), Some("/tmp"));
        assert_eq!(updated.required_capabilities, vec!["code", "rust"]);
        assert!(os.events.iter().any(|event| {
            event.kind == EventKind::TaskUpdated && event.message.contains("updated task")
        }));
    }

    #[test]
    fn update_task_replaces_tool_invocation_and_inherits_capabilities_when_empty() {
        let mut os = OperatingSystem::new("test");
        let tool = ToolDefinition::new(
            "Say",
            ToolKind::Shell,
            "say something",
            vec!["shell".into()],
            "printf {message}",
            None,
        );
        let tool_id = tool.id.clone();
        os.register_tool(tool);
        let task = Task::new("Pending", "pending", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);

        let updated = Runtime::update_task(
            &mut os,
            &task_id,
            TaskUpdate {
                tool: Some(Some(ToolInvocation::new(
                    tool_id.clone(),
                    BTreeMap::from([("message".into(), "hello".into())]),
                ))),
                ..TaskUpdate::default()
            },
        )
        .expect("update task tool");

        assert!(updated.command.is_none());
        assert_eq!(updated.required_capabilities, vec!["shell"]);
        assert_eq!(updated.tool.expect("tool").tool_id, tool_id);
    }

    #[test]
    fn update_task_rejects_unknown_or_invalid_tool_invocation() {
        let mut os = OperatingSystem::new("test");
        let tool = ToolDefinition::new(
            "Say",
            ToolKind::Shell,
            "say something",
            vec![],
            "printf {message}",
            None,
        );
        let tool_id = tool.id.clone();
        os.register_tool(tool);
        let task = Task::new("Pending", "pending", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);

        let error = Runtime::update_task(
            &mut os,
            &task_id,
            TaskUpdate {
                tool: Some(Some(ToolInvocation::new(
                    ToolId::new("missing"),
                    BTreeMap::new(),
                ))),
                ..TaskUpdate::default()
            },
        )
        .expect_err("unknown tool rejected");
        assert!(matches!(error, RuntimeError::ToolNotFound(_)));

        let error = Runtime::update_task(
            &mut os,
            &task_id,
            TaskUpdate {
                tool: Some(Some(ToolInvocation::new(
                    tool_id,
                    BTreeMap::from([("unused".into(), "value".into())]),
                ))),
                ..TaskUpdate::default()
            },
        )
        .expect_err("invalid args rejected");
        assert!(matches!(error, RuntimeError::ToolInvalid { .. }));
    }

    #[test]
    fn update_task_rejects_running_or_invalid_specs() {
        let mut os = OperatingSystem::new("test");
        let mut task = Task::new("Run", "run", Priority::Normal, vec![]);
        task.status = TaskStatus::Running;
        let task_id = task.id.clone();
        os.create_task(task);

        let error = Runtime::update_task(
            &mut os,
            &task_id,
            TaskUpdate {
                title: Some("Renamed".into()),
                ..TaskUpdate::default()
            },
        )
        .expect_err("running task rejected");
        assert!(matches!(error, RuntimeError::TaskNotEditable { .. }));

        let mut pending = Task::new("Pending", "pending", Priority::Normal, vec![]);
        pending.tool = Some(ToolInvocation::new(ToolId::new("tool"), Default::default()));
        let pending_id = pending.id.clone();
        os.create_task(pending);
        let error = Runtime::update_task(
            &mut os,
            &pending_id,
            TaskUpdate {
                command: Some(Some("printf nope".into())),
                ..TaskUpdate::default()
            },
        )
        .expect_err("command with tool rejected");
        assert!(matches!(error, RuntimeError::TaskInvalid { .. }));
    }

    #[test]
    fn delete_task_refuses_run_history_references() {
        let mut os = OperatingSystem::new("test");
        let task = Task::new("Executed", "keep replayable", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);
        let run = RunRecord::new(task_id.clone(), None, "printf ok", ".");
        let run_id = run.id.clone();
        os.runs.insert(run_id.clone(), run);

        let error = Runtime::delete_task(&mut os, &task_id).expect_err("referenced task");

        assert!(matches!(error, RuntimeError::TaskReferenced { .. }));
        assert!(os.tasks.contains_key(&task_id));
        assert!(os.runs.contains_key(&run_id));
    }

    #[test]
    fn tick_reports_dependency_deadlocks_without_mutating_tasks() {
        let mut os = OperatingSystem::new("test");
        let agent = Agent::new("Builder", AgentKind::Builder, None, vec![], 1);
        os.register_agent(agent);

        let first = Task::new("Cycle A", "waits on b", Priority::Normal, vec![]);
        let first_id = first.id.clone();
        let second = Task::new("Cycle B", "waits on a", Priority::Normal, vec![]);
        let second_id = second.id.clone();
        os.create_task(first);
        os.create_task(second);
        os.tasks
            .get_mut(&first_id)
            .expect("first")
            .dependencies
            .push(second_id.clone());
        os.tasks
            .get_mut(&second_id)
            .expect("second")
            .dependencies
            .push(first_id.clone());

        let report = Runtime::tick(&mut os, 1);

        assert!(report.assignments.is_empty());
        assert_eq!(
            report
                .deadlocked_tasks
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([first_id.clone(), second_id.clone()])
        );
        assert!(
            report
                .notes
                .iter()
                .any(|note| note.contains("dependency deadlock detected"))
        );
        let reasons = report
            .unscheduled_tasks
            .iter()
            .map(|task| (task.task_id.clone(), task.reason.as_str()))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            reasons.get(&first_id),
            Some(&"task is in a dependency deadlock")
        );
        assert_eq!(
            reasons.get(&second_id),
            Some(&"task is in a dependency deadlock")
        );
        assert_eq!(
            os.tasks.get(&first_id).expect("first").status,
            TaskStatus::Pending
        );
        assert_eq!(
            os.tasks.get(&second_id).expect("second").status,
            TaskStatus::Pending
        );
    }

    #[test]
    fn tick_reports_unscheduled_task_reasons() {
        let mut os = OperatingSystem::new("test");
        let mut busy = Agent::new("Busy", AgentKind::Builder, None, vec!["rust".into()], 1);
        busy.current_tasks
            .push(TaskId::from_slug("already-running"));
        os.register_agent(busy);

        let blocked = Task::new(
            "Blocked",
            "operator hold",
            Priority::Normal,
            vec!["rust".into()],
        );
        let blocked_id = blocked.id.clone();
        os.create_task(blocked);
        Runtime::block_task(&mut os, &blocked_id, Some("waiting on approval".into()))
            .expect("block task");

        let missing_capability = Task::new(
            "Missing capability",
            "needs docs",
            Priority::Normal,
            vec!["docs".into()],
        );
        let missing_capability_id = missing_capability.id.clone();
        os.create_task(missing_capability);

        let capacity = Task::new(
            "Capacity",
            "needs rust",
            Priority::Normal,
            vec!["rust".into()],
        );
        let capacity_id = capacity.id.clone();
        os.create_task(capacity);

        let report = Runtime::tick(&mut os, 1);

        assert!(report.assignments.is_empty());
        let reasons = report
            .unscheduled_tasks
            .iter()
            .map(|task| (task.task_id.clone(), task.reason.as_str()))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(reasons.get(&blocked_id), Some(&"task is blocked"));
        assert_eq!(
            reasons.get(&missing_capability_id),
            Some(&"no agent has required capabilities [docs]")
        );
        assert_eq!(
            reasons.get(&capacity_id),
            Some(&"matching agents are at capacity")
        );
        assert!(
            report
                .notes
                .iter()
                .any(|note| note.contains("task(s) could not be scheduled"))
        );
    }

    #[test]
    fn delete_task_refuses_dependent_task_references() {
        let mut os = OperatingSystem::new("test");
        let dependency = Task::new("Dependency", "must finish first", Priority::Normal, vec![]);
        let dependency_id = dependency.id.clone();
        os.create_task(dependency);
        let mut dependent = Task::new("Dependent", "waits", Priority::Normal, vec![]);
        let dependent_id = dependent.id.clone();
        dependent.dependencies.push(dependency_id.clone());
        os.create_task(dependent);

        let error = Runtime::delete_task(&mut os, &dependency_id).expect_err("referenced task");

        assert!(matches!(error, RuntimeError::TaskReferenced { .. }));
        assert!(os.tasks.contains_key(&dependency_id));
        assert_eq!(
            os.tasks.get(&dependent_id).expect("dependent").dependencies,
            vec![dependency_id]
        );
    }

    #[test]
    fn delete_task_refuses_workflow_references() {
        let mut os = OperatingSystem::new("test");
        let task = Task::new("Workflow task", "stage work", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);
        let workflow = Workflow::new(
            "Ship",
            Priority::Normal,
            BTreeMap::from([("build".into(), task_id.clone())]),
        );
        let workflow_id = workflow.id.clone();
        os.create_workflow(workflow);

        let error = Runtime::delete_task(&mut os, &task_id).expect_err("referenced task");

        assert!(matches!(error, RuntimeError::TaskReferenced { .. }));
        assert!(os.tasks.contains_key(&task_id));
        assert!(os.workflows.contains_key(&workflow_id));
    }

    #[test]
    fn delete_task_refuses_running_task_without_history() {
        let mut os = OperatingSystem::new("test");
        let agent = Agent::new("Builder", AgentKind::Builder, None, vec!["rust".into()], 1);
        let agent_id = agent.id.clone();
        os.register_agent(agent);
        let task = Task::new(
            "Running",
            "active work",
            Priority::Normal,
            vec!["rust".into()],
        );
        let task_id = task.id.clone();
        os.create_task(task);
        Runtime::assign_task(&mut os, &task_id, &agent_id).expect("assign task");

        let error = Runtime::delete_task(&mut os, &task_id).expect_err("running task rejected");

        assert!(matches!(error, RuntimeError::TaskNotEditable { .. }));
        assert!(os.tasks.contains_key(&task_id));
        assert!(
            os.agents
                .get(&agent_id)
                .expect("agent")
                .current_tasks
                .contains(&task_id)
        );
        assert!(os.runs.is_empty());
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuntimeReport {
    pub assignments: Vec<Assignment>,
    pub completed_tasks: Vec<TaskId>,
    pub recovered_tasks: Vec<TaskId>,
    pub recovered_runs: Vec<RunId>,
    pub recovered_daemon: bool,
    pub expired_agents: Vec<AgentId>,
    pub deadlocked_tasks: Vec<TaskId>,
    pub unscheduled_tasks: Vec<UnscheduledTask>,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UnscheduledTask {
    pub task_id: TaskId,
    pub reason: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecoveryReport {
    pub recovered_tasks: Vec<TaskId>,
    pub recovered_runs: Vec<RunId>,
    pub recovered_daemon: bool,
    pub notes: Vec<String>,
}

impl RuntimeReport {
    pub fn absorb_recovery(&mut self, recovery: RecoveryReport) {
        self.recovered_tasks.extend(recovery.recovered_tasks);
        self.recovered_runs.extend(recovery.recovered_runs);
        self.recovered_daemon |= recovery.recovered_daemon;
        self.notes.extend(recovery.notes);
    }

    pub fn absorb_tick(&mut self, tick: RuntimeReport) {
        self.assignments = tick.assignments;
        self.completed_tasks = tick.completed_tasks;
        self.expired_agents = tick.expired_agents;
        self.deadlocked_tasks = tick.deadlocked_tasks;
        self.unscheduled_tasks = tick.unscheduled_tasks;
        self.notes.extend(tick.notes);
    }
}

fn detect_dependency_deadlocks_from(
    os: &OperatingSystem,
    active: &BTreeSet<TaskId>,
    task_id: &TaskId,
    visiting: &mut BTreeSet<TaskId>,
    visited: &mut BTreeSet<TaskId>,
    stack: &mut Vec<TaskId>,
    deadlocked: &mut BTreeSet<TaskId>,
) {
    if visited.contains(task_id) {
        return;
    }
    if !visiting.insert(task_id.clone()) {
        if let Some(position) = stack.iter().position(|stacked| stacked == task_id) {
            deadlocked.extend(stack[position..].iter().cloned());
        }
        return;
    }

    stack.push(task_id.clone());
    if let Some(task) = os.tasks.get(task_id) {
        for dependency in &task.dependencies {
            if active.contains(dependency) {
                detect_dependency_deadlocks_from(
                    os, active, dependency, visiting, visited, stack, deadlocked,
                );
            }
        }
    }
    stack.pop();
    visiting.remove(task_id);
    visited.insert(task_id.clone());
}

fn unscheduled_reason(
    os: &OperatingSystem,
    task: &Task,
    now: chrono::DateTime<Utc>,
    limit_reached: bool,
    deadlocked_tasks: &BTreeSet<TaskId>,
) -> Option<String> {
    match task.status {
        TaskStatus::Pending => {}
        TaskStatus::Blocked => return Some("task is blocked".into()),
        _ => return None,
    }

    if deadlocked_tasks.contains(&task.id) {
        return Some("task is in a dependency deadlock".into());
    }

    let incomplete_dependencies = task
        .dependencies
        .iter()
        .filter(|dependency| {
            os.tasks
                .get(*dependency)
                .map(|task| task.status != TaskStatus::Complete)
                .unwrap_or(true)
        })
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if !incomplete_dependencies.is_empty() {
        return Some(format!(
            "waiting for incomplete dependencies: {}",
            incomplete_dependencies.join(", ")
        ));
    }

    if os
        .agents
        .values()
        .any(|agent| agent.can_accept_at(&task.required_capabilities, now))
    {
        return limit_reached
            .then(|| "scheduler limit reached before this ready task was assigned".into());
    }

    if os.agents.is_empty() {
        return Some("no agents registered".into());
    }

    let matching_agents = os
        .agents
        .values()
        .filter(|agent| {
            task.required_capabilities
                .iter()
                .all(|need| agent.capabilities.iter().any(|cap| cap == need))
        })
        .collect::<Vec<_>>();
    if matching_agents.is_empty() {
        return Some(format!(
            "no agent has required capabilities [{}]",
            task.required_capabilities.join(", ")
        ));
    }
    if matching_agents
        .iter()
        .all(|agent| agent.status != AgentStatus::Online)
    {
        return Some("matching agents are not online".into());
    }
    if matching_agents.iter().all(|agent| {
        agent
            .lease_expires_at
            .map(|expires_at| expires_at <= now)
            .unwrap_or(false)
    }) {
        return Some("matching agents have expired leases".into());
    }
    if matching_agents
        .iter()
        .all(|agent| agent.current_tasks.len() >= agent.max_parallel_tasks)
    {
        return Some("matching agents are at capacity".into());
    }

    Some("no matching agent can currently accept this task".into())
}

pub struct Runtime;

#[cfg(unix)]
fn process_is_running(pid: u32) -> bool {
    if pid == std::process::id() {
        return true;
    }
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(true)
}

#[cfg(windows)]
fn process_is_running(pid: u32) -> bool {
    let output = Command::new("tasklist")
        .arg("/FI")
        .arg(format!("PID eq {pid}"))
        .arg("/FO")
        .arg("CSV")
        .arg("/NH")
        .stderr(Stdio::null())
        .output();
    match output {
        Ok(output) if output.status.success() => windows_tasklist_contains_pid(&output.stdout, pid),
        _ => true,
    }
}

#[cfg(all(not(unix), not(windows)))]
fn process_is_running(_pid: u32) -> bool {
    true
}

#[cfg(any(test, windows))]
fn windows_tasklist_contains_pid(output: &[u8], pid: u32) -> bool {
    let pid = pid.to_string();
    String::from_utf8_lossy(output).lines().any(|line| {
        parse_windows_tasklist_pid(line)
            .as_deref()
            .is_some_and(|reported_pid| reported_pid == pid)
    })
}

#[cfg(any(test, windows))]
fn parse_windows_tasklist_pid(line: &str) -> Option<String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut chars = line.trim().chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' if quoted && chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            '"' => quoted = !quoted,
            ',' if !quoted => {
                fields.push(field.trim().to_owned());
                field.clear();
            }
            _ => field.push(ch),
        }
    }
    fields.push(field.trim().to_owned());
    fields.get(1).filter(|pid| !pid.is_empty()).cloned()
}

impl Runtime {
    fn ensure_task_editable(task_id: &TaskId, status: &TaskStatus) -> Result<(), RuntimeError> {
        if !matches!(status, TaskStatus::Pending | TaskStatus::Blocked) {
            return Err(RuntimeError::TaskNotEditable {
                task_id: task_id.clone(),
                status: status.clone(),
            });
        }
        Ok(())
    }

    pub fn tick(os: &mut OperatingSystem, limit: usize) -> RuntimeReport {
        let limit = limit.max(1);
        let mut report = RuntimeReport {
            expired_agents: Self::expire_agent_leases(os),
            deadlocked_tasks: Self::detect_dependency_deadlocks(os),
            ..RuntimeReport::default()
        };
        for _ in 0..limit {
            match Scheduler::assign_next(os) {
                Some(assignment) => report.assignments.push(assignment),
                None => break,
            }
        }
        let limit_reached = report.assignments.len() >= limit;
        report.unscheduled_tasks = Self::explain_unscheduled_tasks(os, limit_reached);

        if report.assignments.is_empty() {
            report
                .notes
                .push("no pending task could be scheduled".into());
        }
        if !report.unscheduled_tasks.is_empty() {
            report.notes.push(format!(
                "{} task(s) could not be scheduled: {}",
                report.unscheduled_tasks.len(),
                report
                    .unscheduled_tasks
                    .iter()
                    .take(3)
                    .map(|task| format!("{} ({})", task.task_id, task.reason))
                    .collect::<Vec<_>>()
                    .join("; ")
            ));
        }
        if !report.expired_agents.is_empty() {
            report.notes.push(format!(
                "expired {} agent lease(s)",
                report.expired_agents.len()
            ));
        }
        if !report.deadlocked_tasks.is_empty() {
            report.notes.push(format!(
                "dependency deadlock detected among {} task(s): {}",
                report.deadlocked_tasks.len(),
                report
                    .deadlocked_tasks
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        report
    }

    pub fn explain_unscheduled_tasks(
        os: &OperatingSystem,
        limit_reached: bool,
    ) -> Vec<UnscheduledTask> {
        let now = Utc::now();
        let deadlocked_tasks = Self::detect_dependency_deadlocks(os)
            .into_iter()
            .collect::<BTreeSet<_>>();
        os.tasks
            .values()
            .filter_map(|task| {
                unscheduled_reason(os, task, now, limit_reached, &deadlocked_tasks).map(|reason| {
                    UnscheduledTask {
                        task_id: task.id.clone(),
                        reason,
                    }
                })
            })
            .collect()
    }

    pub fn detect_dependency_deadlocks(os: &OperatingSystem) -> Vec<TaskId> {
        let active = os
            .tasks
            .values()
            .filter(|task| matches!(task.status, TaskStatus::Pending | TaskStatus::Blocked))
            .map(|task| task.id.clone())
            .collect::<BTreeSet<_>>();
        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        let mut stack = Vec::new();
        let mut deadlocked = BTreeSet::new();

        for task_id in &active {
            detect_dependency_deadlocks_from(
                os,
                &active,
                task_id,
                &mut visiting,
                &mut visited,
                &mut stack,
                &mut deadlocked,
            );
        }

        deadlocked.into_iter().collect()
    }

    pub fn expire_agent_leases(os: &mut OperatingSystem) -> Vec<AgentId> {
        let now = Utc::now();
        let expired = os
            .agents
            .values()
            .filter(|agent| {
                agent.status == AgentStatus::Online
                    && agent
                        .lease_expires_at
                        .map(|expires_at| expires_at <= now)
                        .unwrap_or(false)
            })
            .map(|agent| agent.id.clone())
            .collect::<Vec<_>>();

        for agent_id in &expired {
            if let Some(agent) = os.agents.get_mut(agent_id) {
                agent.status = AgentStatus::Offline;
                agent.updated_at = now;
            }
            os.record(
                EventKind::AgentHeartbeat,
                format!("expired lease for agent {}", agent_id),
            );
        }

        expired
    }

    pub fn heartbeat_agent(
        os: &mut OperatingSystem,
        agent_id: &AgentId,
        status: AgentStatus,
        lease_seconds: Option<i64>,
    ) -> Result<(), RuntimeError> {
        let now = Utc::now();
        let agent = os
            .agents
            .get_mut(agent_id)
            .ok_or_else(|| RuntimeError::AgentNotFound(agent_id.clone()))?;
        agent.status = status;
        agent.last_heartbeat_at = Some(now);
        agent.lease_expires_at = lease_seconds.map(|seconds| now + Duration::seconds(seconds));
        agent.updated_at = now;
        os.record(
            EventKind::AgentHeartbeat,
            format!("heartbeat from agent {}", agent_id),
        );
        Ok(())
    }

    pub fn heartbeat_worker(
        os: &mut OperatingSystem,
        worker_id: &str,
        endpoint: Option<String>,
        status: Option<AgentStatus>,
        lease_seconds: Option<i64>,
    ) -> Result<WorkerNode, RuntimeError> {
        let now = Utc::now();
        let worker = os
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| RuntimeError::WorkerNotFound(worker_id.to_owned()))?;
        if let Some(endpoint) = endpoint {
            worker.endpoint = endpoint;
        }
        if let Some(status) = status {
            worker.status = status;
        }
        worker.last_seen_at = now;
        let worker = worker.clone();

        let agent_id = AgentId::new(worker_id);
        if let Some(agent) = os.agents.get_mut(&agent_id) {
            agent.status = worker.status.clone();
            agent.last_heartbeat_at = Some(now);
            if let Some(seconds) = lease_seconds {
                agent.lease_expires_at = Some(now + Duration::seconds(seconds));
            }
            agent.updated_at = now;
            os.record(
                EventKind::AgentHeartbeat,
                format!("heartbeat from worker {} for agent {}", worker_id, agent_id),
            );
        }
        os.record(
            EventKind::WorkerUpdated,
            format!("updated worker {}", worker.id),
        );
        Ok(worker)
    }

    pub fn update_agent(
        os: &mut OperatingSystem,
        agent_id: &AgentId,
        update: AgentUpdate,
    ) -> Result<Agent, RuntimeError> {
        let mut agent = os
            .agents
            .get(agent_id)
            .cloned()
            .ok_or_else(|| RuntimeError::AgentNotFound(agent_id.clone()))?;
        if let Some(name) = update.name {
            if AgentId::new(&name).as_str().is_empty() {
                return Err(RuntimeError::AgentInvalid {
                    agent_id: agent_id.clone(),
                    reason: "agent name must contain at least one ASCII letter, digit, or hyphen"
                        .into(),
                });
            }
            agent.name = name;
        }
        if let Some(kind) = update.kind {
            if kind.to_string().trim().is_empty() {
                return Err(RuntimeError::AgentInvalid {
                    agent_id: agent_id.clone(),
                    reason: "agent kind must not be empty".into(),
                });
            }
            agent.kind = kind;
        }
        if let Some(model) = update.model {
            if let Some(model) = &model
                && model.trim().is_empty()
            {
                return Err(RuntimeError::AgentInvalid {
                    agent_id: agent_id.clone(),
                    reason: "agent model must not be empty".into(),
                });
            }
            agent.model = model;
        }
        if let Some(capabilities) = update.capabilities {
            let capabilities = normalize_list(capabilities);
            if capabilities.is_empty() {
                return Err(RuntimeError::AgentInvalid {
                    agent_id: agent_id.clone(),
                    reason: "agent capabilities must include at least one capability".into(),
                });
            }
            agent.capabilities = capabilities;
        }
        if let Some(max_parallel_tasks) = update.max_parallel_tasks {
            if max_parallel_tasks == 0 {
                return Err(RuntimeError::AgentInvalid {
                    agent_id: agent_id.clone(),
                    reason: "parallel must be greater than 0".into(),
                });
            }
            if agent.current_tasks.len() > max_parallel_tasks {
                return Err(RuntimeError::AgentInvalid {
                    agent_id: agent_id.clone(),
                    reason: format!(
                        "parallel cannot be lower than current task count {}",
                        agent.current_tasks.len()
                    ),
                });
            }
            agent.max_parallel_tasks = max_parallel_tasks;
        }

        for task_id in &agent.current_tasks {
            let Some(task) = os.tasks.get(task_id) else {
                continue;
            };
            let missing_capabilities = task
                .required_capabilities
                .iter()
                .filter(|required| {
                    !agent
                        .capabilities
                        .iter()
                        .any(|capability| capability == *required)
                })
                .cloned()
                .collect::<Vec<_>>();
            if !missing_capabilities.is_empty() {
                return Err(RuntimeError::AgentIncompatible {
                    agent_id: agent_id.clone(),
                    task_id: task_id.clone(),
                    reason: format!("missing capabilities [{}]", missing_capabilities.join(", ")),
                });
            }
        }

        agent.updated_at = Utc::now();
        os.agents.insert(agent_id.clone(), agent.clone());
        os.record(
            EventKind::AgentUpdated,
            format!("updated agent {}", agent_id),
        );
        Ok(agent)
    }

    pub fn assign_task(
        os: &mut OperatingSystem,
        task_id: &TaskId,
        agent_id: &AgentId,
    ) -> Result<Assignment, RuntimeError> {
        let now = Utc::now();
        let required_capabilities = {
            let task = os
                .tasks
                .get(task_id)
                .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?;
            if task.status != TaskStatus::Pending {
                return Err(RuntimeError::InvalidTransition {
                    task_id: task_id.clone(),
                    from: task.status.clone(),
                    to: TaskStatus::Running,
                });
            }
            for dependency in &task.dependencies {
                if os
                    .tasks
                    .get(dependency)
                    .map(|task| task.status != TaskStatus::Complete)
                    .unwrap_or(true)
                {
                    return Err(RuntimeError::TaskDependencyIncomplete {
                        task_id: task_id.clone(),
                        dependency: dependency.clone(),
                    });
                }
            }
            task.required_capabilities.clone()
        };

        let agent = os
            .agents
            .get(agent_id)
            .ok_or_else(|| RuntimeError::AgentNotFound(agent_id.clone()))?;
        if agent.current_tasks.iter().any(|id| id == task_id) {
            return Err(RuntimeError::AgentCannotAccept {
                agent_id: agent_id.clone(),
                task_id: task_id.clone(),
                reason: "agent already has this task".into(),
            });
        }
        if agent.status != AgentStatus::Online {
            return Err(RuntimeError::AgentCannotAccept {
                agent_id: agent_id.clone(),
                task_id: task_id.clone(),
                reason: format!("agent is {}", agent.status),
            });
        }
        if agent
            .lease_expires_at
            .map(|expires_at| expires_at <= now)
            .unwrap_or(false)
        {
            return Err(RuntimeError::AgentCannotAccept {
                agent_id: agent_id.clone(),
                task_id: task_id.clone(),
                reason: "agent lease is expired".into(),
            });
        }
        if agent.current_tasks.len() >= agent.max_parallel_tasks {
            return Err(RuntimeError::AgentCannotAccept {
                agent_id: agent_id.clone(),
                task_id: task_id.clone(),
                reason: "agent is at capacity".into(),
            });
        }
        let missing_capabilities = required_capabilities
            .iter()
            .filter(|required| {
                !agent
                    .capabilities
                    .iter()
                    .any(|capability| capability == *required)
            })
            .cloned()
            .collect::<Vec<_>>();
        if !missing_capabilities.is_empty() {
            return Err(RuntimeError::AgentCannotAccept {
                agent_id: agent_id.clone(),
                task_id: task_id.clone(),
                reason: format!("missing capabilities [{}]", missing_capabilities.join(", ")),
            });
        }

        if let Some(task) = os.tasks.get_mut(task_id) {
            task.status = TaskStatus::Running;
            task.assigned_to = Some(agent_id.clone());
            task.updated_at = now;
        }
        if let Some(agent) = os.agents.get_mut(agent_id) {
            agent.current_tasks.push(task_id.clone());
            agent.updated_at = now;
        }
        os.record(
            EventKind::TaskAssigned,
            format!("assigned task {} to {}", task_id, agent_id),
        );
        Ok(Assignment {
            task_id: task_id.clone(),
            agent_id: agent_id.clone(),
            reason: "manual assignment".into(),
        })
    }

    pub fn reprioritize_task(
        os: &mut OperatingSystem,
        task_id: &TaskId,
        priority: Priority,
    ) -> Result<(), RuntimeError> {
        let task = os
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?;
        Self::ensure_task_editable(task_id, &task.status)?;
        task.priority = priority;
        task.updated_at = Utc::now();
        os.record(
            EventKind::TaskUpdated,
            format!("set priority for task {} to {}", task_id, priority),
        );
        Ok(())
    }

    pub fn update_task(
        os: &mut OperatingSystem,
        task_id: &TaskId,
        update: TaskUpdate,
    ) -> Result<Task, RuntimeError> {
        let mut task = os
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?;
        Self::ensure_task_editable(task_id, &task.status)?;
        if let Some(title) = update.title {
            if title.trim().is_empty() {
                return Err(RuntimeError::TaskInvalid {
                    task_id: task_id.clone(),
                    reason: "task title must not be empty".into(),
                });
            }
            task.title = title;
        }
        if let Some(objective) = update.objective {
            if objective.trim().is_empty() {
                return Err(RuntimeError::TaskInvalid {
                    task_id: task_id.clone(),
                    reason: "task objective must not be empty".into(),
                });
            }
            task.objective = objective;
        }
        if let Some(command) = update.command {
            if let Some(command) = &command {
                if command.trim().is_empty() {
                    return Err(RuntimeError::TaskInvalid {
                        task_id: task_id.clone(),
                        reason: "task command must not be empty".into(),
                    });
                }
            }
            task.command = command;
        }
        if let Some(tool) = update.tool {
            if let Some(invocation) = &tool {
                let definition = os
                    .tools
                    .get(&invocation.tool_id)
                    .ok_or_else(|| RuntimeError::ToolNotFound(invocation.tool_id.clone()))?;
                validate_tool_invocation(definition, invocation).map_err(|error| {
                    RuntimeError::ToolInvalid {
                        tool_id: invocation.tool_id.clone(),
                        reason: error.to_string(),
                    }
                })?;
                if task.required_capabilities.is_empty() && update.required_capabilities.is_none() {
                    task.required_capabilities = definition.required_capabilities.clone();
                }
            }
            task.tool = tool;
        }
        if let Some(cwd) = update.cwd {
            if let Some(cwd) = &cwd
                && cwd.trim().is_empty()
            {
                return Err(RuntimeError::TaskInvalid {
                    task_id: task_id.clone(),
                    reason: "task cwd must not be empty".into(),
                });
            }
            task.cwd = cwd;
        }
        if let Some(required_capabilities) = update.required_capabilities {
            if required_capabilities.iter().any(|value| {
                value.trim().is_empty() || value.split(',').any(|part| part.trim().is_empty())
            }) {
                return Err(RuntimeError::TaskInvalid {
                    task_id: task_id.clone(),
                    reason: "task required capabilities must not contain empty capabilities".into(),
                });
            }
            task.required_capabilities = normalize_list(required_capabilities);
        }
        if let Some(max_attempts) = update.max_attempts {
            if max_attempts == 0 {
                return Err(RuntimeError::TaskInvalid {
                    task_id: task_id.clone(),
                    reason: "task max_attempts must be greater than 0".into(),
                });
            }
            task.max_attempts = max_attempts;
        }
        if task.command.is_some() && task.tool.is_some() {
            return Err(RuntimeError::TaskInvalid {
                task_id: task_id.clone(),
                reason: "task cannot define both command and tool".into(),
            });
        }

        task.updated_at = Utc::now();
        os.tasks.insert(task_id.clone(), task.clone());
        os.record(EventKind::TaskUpdated, format!("updated task {}", task_id));
        Ok(task)
    }

    pub fn set_task_dependencies(
        os: &mut OperatingSystem,
        task_id: &TaskId,
        dependencies: Vec<TaskId>,
    ) -> Result<(), RuntimeError> {
        let status = os
            .tasks
            .get(task_id)
            .map(|task| task.status.clone())
            .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?;
        Self::ensure_task_editable(task_id, &status)?;
        let mut seen = std::collections::BTreeSet::new();
        for dependency in &dependencies {
            if dependency == task_id {
                return Err(RuntimeError::SelfDependency(task_id.clone()));
            }
            if !seen.insert(dependency.clone()) {
                return Err(RuntimeError::DuplicateTaskDependency(dependency.clone()));
            }
            if !os.tasks.contains_key(dependency) {
                return Err(RuntimeError::TaskDependencyNotFound(dependency.clone()));
            }
        }
        let task = os
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?;
        task.dependencies = dependencies;
        task.updated_at = Utc::now();
        os.record(
            EventKind::TaskUpdated,
            format!("updated dependencies for task {}", task_id),
        );
        Ok(())
    }

    pub fn set_task_plan(
        os: &mut OperatingSystem,
        task_id: &TaskId,
        steps: Vec<String>,
    ) -> Result<Task, RuntimeError> {
        let status = os
            .tasks
            .get(task_id)
            .map(|task| task.status.clone())
            .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?;
        Self::ensure_task_editable(task_id, &status)?;
        if steps.is_empty() {
            return Err(RuntimeError::TaskInvalid {
                task_id: task_id.clone(),
                reason: "plan steps must not be empty".into(),
            });
        }
        if steps.iter().any(|step| step.trim().is_empty()) {
            return Err(RuntimeError::TaskInvalid {
                task_id: task_id.clone(),
                reason: "plan steps must not contain empty steps".into(),
            });
        }

        let task = os
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?;
        task.plan = steps;
        task.updated_at = Utc::now();
        let task = task.clone();
        os.record(
            EventKind::TaskUpdated,
            format!("updated plan for task {}", task_id),
        );
        Ok(task)
    }

    pub fn update_tool(
        os: &mut OperatingSystem,
        tool_id: &ToolId,
        update: ToolUpdate,
    ) -> Result<ToolDefinition, RuntimeError> {
        let mut tool = os
            .tools
            .get(tool_id)
            .cloned()
            .ok_or_else(|| RuntimeError::ToolNotFound(tool_id.clone()))?;
        if let Some(kind) = update.kind {
            tool.kind = kind;
        }
        if let Some(description) = update.description {
            tool.description = description;
        }
        if let Some(required_capabilities) = update.required_capabilities {
            tool.required_capabilities = normalize_list(required_capabilities);
        }
        if let Some(command_template) = update.command_template {
            tool.command_template = command_template;
        }
        if let Some(default_cwd) = update.default_cwd {
            tool.default_cwd = default_cwd;
        }
        tool.updated_at = Utc::now();

        validate_tool_template(&tool).map_err(|error| RuntimeError::ToolInvalid {
            tool_id: tool_id.clone(),
            reason: error.to_string(),
        })?;
        for task in os.tasks.values() {
            let Some(invocation) = &task.tool else {
                continue;
            };
            if &invocation.tool_id != tool_id {
                continue;
            }
            validate_tool_invocation(&tool, invocation).map_err(|error| {
                RuntimeError::ToolIncompatible {
                    tool_id: tool_id.clone(),
                    task_id: task.id.clone(),
                    reason: error.to_string(),
                }
            })?;
        }

        os.tools.insert(tool_id.clone(), tool.clone());
        os.record(EventKind::ToolUpdated, format!("updated tool {}", tool_id));
        Ok(tool)
    }

    pub fn complete_task(
        os: &mut OperatingSystem,
        task_id: &TaskId,
        output: Option<String>,
    ) -> Result<(), RuntimeError> {
        let agent_id = {
            let task = os
                .tasks
                .get_mut(task_id)
                .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?;
            task.status = TaskStatus::Complete;
            task.output = output;
            task.updated_at = Utc::now();
            task.assigned_to.clone()
        };

        if let Some(agent_id) = agent_id {
            Self::release_specific_agent_capacity(os, task_id, &agent_id);
        }

        os.record(
            EventKind::TaskUpdated,
            format!("completed task {}", task_id),
        );
        Ok(())
    }

    pub fn recover_stale_tasks(os: &mut OperatingSystem, older_than: Duration) -> Vec<TaskId> {
        Self::recover_stale_work(os, older_than).recovered_tasks
    }

    pub fn recover_stale_state(os: &mut OperatingSystem, older_than: Duration) -> RecoveryReport {
        let mut report = Self::recover_stale_work(os, older_than);
        if Self::recover_crashed_daemon(os) {
            report.recovered_daemon = true;
            report.notes.push("recovered crashed daemon state".into());
        }
        report
    }

    pub fn recover_stale_work(os: &mut OperatingSystem, older_than: Duration) -> RecoveryReport {
        let cutoff = Utc::now() - older_than;
        let stale_task_ids = os
            .tasks
            .values()
            .filter(|task| {
                task.status == TaskStatus::Running
                    && (task.updated_at <= cutoff
                        || os.runs.values().any(|run| {
                            run.task_id == task.id
                                && matches!(
                                    run.status,
                                    RunStatus::Running | RunStatus::CancelRequested
                                )
                                && run.started_at <= cutoff
                        }))
            })
            .map(|task| task.id.clone())
            .collect::<Vec<_>>();
        let mut report = RecoveryReport::default();

        for task_id in &stale_task_ids {
            let now = Utc::now();
            let mut recovered_run_ids = Vec::new();
            {
                let Some(task) = os.tasks.get_mut(task_id) else {
                    continue;
                };
                task.status = TaskStatus::Pending;
                task.output = Some(format!("recovered stale running task at {now}"));
                task.updated_at = now;
                task.assigned_to.take();
            }
            for run in os.runs.values_mut().filter(|run| {
                run.task_id == *task_id
                    && matches!(run.status, RunStatus::Running | RunStatus::CancelRequested)
            }) {
                recovered_run_ids.push(run.id.clone());
                run.status = RunStatus::Failed;
                run.exit_code = None;
                run.finished_at = Some(now);
            }
            Self::release_task_from_all_agents(os, task_id);

            os.record(
                EventKind::TaskUpdated,
                format!("recovered stale running task {}", task_id),
            );
            for run_id in &recovered_run_ids {
                os.record(
                    EventKind::RunFinished,
                    format!("recovered stale active run {run_id} for task {task_id}"),
                );
            }
            report.recovered_tasks.push(task_id.clone());
            report.recovered_runs.extend(recovered_run_ids);
        }

        let recovered_task_set = report
            .recovered_tasks
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let orphan_run_ids = os
            .runs
            .values()
            .filter(|run| {
                matches!(run.status, RunStatus::Running | RunStatus::CancelRequested)
                    && run.started_at <= cutoff
                    && !recovered_task_set.contains(&run.task_id)
                    && !matches!(
                        os.tasks.get(&run.task_id).map(|task| &task.status),
                        Some(TaskStatus::Running)
                    )
            })
            .map(|run| run.id.clone())
            .collect::<Vec<_>>();
        for run_id in orphan_run_ids {
            let now = Utc::now();
            let task_id = {
                let Some(run) = os.runs.get_mut(&run_id) else {
                    continue;
                };
                let task_id = run.task_id.clone();
                run.status = RunStatus::Failed;
                run.exit_code = None;
                run.finished_at = Some(now);
                task_id
            };
            if let Some(task) = os.tasks.get_mut(&task_id)
                && task.status != TaskStatus::Running
            {
                task.assigned_to = None;
            }
            Self::release_task_from_all_agents(os, &task_id);
            os.record(
                EventKind::RunFinished,
                format!("recovered stale active run {run_id} without a running task {task_id}"),
            );
            report.recovered_runs.push(run_id);
        }

        if !report.recovered_runs.is_empty() {
            report.notes.push(format!(
                "recovered {} active run record(s)",
                report.recovered_runs.len()
            ));
        }
        report
    }

    pub fn recover_crashed_daemon(os: &mut OperatingSystem) -> bool {
        Self::recover_crashed_daemon_with(os, process_is_running)
    }

    pub fn recover_crashed_daemon_with(
        os: &mut OperatingSystem,
        process_is_running: impl Fn(u32) -> bool,
    ) -> bool {
        let Some(daemon) = &mut os.daemon else {
            return false;
        };
        if daemon.status != DaemonStatus::Running {
            return false;
        }
        let Some(pid) = daemon.pid else {
            return false;
        };
        if process_is_running(pid) {
            return false;
        }
        daemon.status = DaemonStatus::Stopped;
        daemon.pid = None;
        daemon.stop_requested = false;
        daemon.last_message = Some(format!("recovered crashed daemon state for pid {pid}"));
        os.record(
            EventKind::DaemonStopped,
            format!("recovered crashed daemon state for pid {pid}"),
        );
        true
    }

    pub fn request_daemon_stop(os: &mut OperatingSystem) -> bool {
        let Some(daemon) = &mut os.daemon else {
            return false;
        };
        if daemon.status != DaemonStatus::Running {
            return false;
        }
        daemon.stop_requested = true;
        daemon.last_message = Some("stop requested".into());
        os.record(EventKind::DaemonStopped, "daemon stop requested");
        true
    }

    pub fn fail_task(
        os: &mut OperatingSystem,
        task_id: &TaskId,
        reason: Option<String>,
    ) -> Result<(), RuntimeError> {
        Self::finish_with_status(os, task_id, TaskStatus::Failed, reason)
    }

    pub fn fail_task_or_retry(
        os: &mut OperatingSystem,
        task_id: &TaskId,
        reason: Option<String>,
    ) -> Result<(), RuntimeError> {
        let retrying_agent = {
            let task = os
                .tasks
                .get_mut(task_id)
                .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?;
            if task.attempts >= task.max_attempts {
                return Self::finish_with_status(os, task_id, TaskStatus::Failed, reason);
            }
            let agent_id = task.assigned_to.take();
            task.status = TaskStatus::Pending;
            task.output = Some(format!(
                "{}; retrying after attempt {} of {}",
                reason.unwrap_or_else(|| "task attempt failed".into()),
                task.attempts,
                task.max_attempts
            ));
            task.updated_at = Utc::now();
            agent_id
        };
        if let Some(agent_id) = retrying_agent {
            Self::release_specific_agent_capacity(os, task_id, &agent_id);
        }
        os.record(
            EventKind::TaskUpdated,
            format!("requeued task {} for retry", task_id),
        );
        Ok(())
    }

    pub fn report_worker_task(
        os: &mut OperatingSystem,
        worker_id: &str,
        task_id: &TaskId,
        status: TaskStatus,
        output: Option<String>,
        command: Option<String>,
        cwd: Option<String>,
        exit_code: Option<i32>,
        artifacts: Vec<RunArtifact>,
    ) -> Result<(WorkerNode, Task, RunRecord), RuntimeError> {
        if !matches!(status, TaskStatus::Complete | TaskStatus::Failed) {
            return Err(RuntimeError::WorkerReportRejected {
                worker_id: worker_id.into(),
                task_id: task_id.clone(),
                reason: format!("status must be complete or failed, got {status}"),
            });
        }
        let agent_id = AgentId::new(worker_id);
        let now = Utc::now();
        {
            let worker = os
                .workers
                .get_mut(worker_id)
                .ok_or_else(|| RuntimeError::WorkerNotFound(worker_id.into()))?;
            worker.status = AgentStatus::Online;
            worker.last_seen_at = now;
        }
        if !os.agents.contains_key(&agent_id) {
            return Err(RuntimeError::AgentNotFound(agent_id));
        }
        {
            let task = os
                .tasks
                .get(task_id)
                .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?;
            if task.status != TaskStatus::Running {
                return Err(RuntimeError::InvalidTransition {
                    task_id: task_id.clone(),
                    from: task.status.clone(),
                    to: status,
                });
            }
            if task.assigned_to.as_ref() != Some(&agent_id) {
                return Err(RuntimeError::WorkerReportRejected {
                    worker_id: worker_id.into(),
                    task_id: task_id.clone(),
                    reason: format!("task is not assigned to matching agent {agent_id}"),
                });
            }
        }

        let run_status = match status {
            TaskStatus::Complete => RunStatus::Success,
            TaskStatus::Failed => RunStatus::Failed,
            _ => unreachable!("validated worker report status"),
        };
        let mut run = RunRecord::new(
            task_id.clone(),
            Some(agent_id),
            command.unwrap_or_else(|| format!("worker:{worker_id}")),
            cwd.unwrap_or_else(|| ".".into()),
        );
        run.started_at = now;
        run.status = run_status;
        run.exit_code = Some(exit_code.unwrap_or(match status {
            TaskStatus::Complete => 0,
            TaskStatus::Failed => 1,
            _ => unreachable!("validated worker report status"),
        }));
        run.finished_at = Some(now);
        run.artifacts = artifacts;
        let run_id = run.id.clone();
        os.runs.insert(run_id.clone(), run.clone());

        match status {
            TaskStatus::Complete => Self::complete_task(os, task_id, output)?,
            TaskStatus::Failed => Self::fail_task_or_retry(os, task_id, output)?,
            _ => unreachable!("validated worker report status"),
        }
        os.record(
            EventKind::RunFinished,
            format!("worker {worker_id} reported run {run_id} for task {task_id}"),
        );
        let worker = os
            .workers
            .get(worker_id)
            .cloned()
            .ok_or_else(|| RuntimeError::WorkerNotFound(worker_id.into()))?;
        let task = os
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?;
        Ok((worker, task, run))
    }

    pub fn block_task(
        os: &mut OperatingSystem,
        task_id: &TaskId,
        reason: Option<String>,
    ) -> Result<(), RuntimeError> {
        Self::finish_with_status(os, task_id, TaskStatus::Blocked, reason)
    }

    pub fn cancel_task(
        os: &mut OperatingSystem,
        task_id: &TaskId,
        reason: Option<String>,
    ) -> Result<(), RuntimeError> {
        Self::finish_with_status(os, task_id, TaskStatus::Cancelled, reason)?;
        let requested_runs = Self::request_active_runs_for_task_cancellation(os, task_id);
        if requested_runs > 0 {
            os.record(
                EventKind::RunFinished,
                format!(
                    "cancel requested for {requested_runs} active run(s) on task {}",
                    task_id
                ),
            );
        }
        Ok(())
    }

    pub fn cancel_workflow(
        os: &mut OperatingSystem,
        workflow_id: &WorkflowId,
        reason: Option<String>,
    ) -> Result<Vec<TaskId>, RuntimeError> {
        let workflow = os
            .workflows
            .get(workflow_id)
            .ok_or_else(|| RuntimeError::WorkflowNotFound(workflow_id.clone()))?;
        let task_ids = workflow.tasks.values().cloned().collect::<Vec<_>>();
        let mut cancelled = Vec::new();
        for task_id in task_ids {
            let status = os
                .tasks
                .get(&task_id)
                .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?
                .status
                .clone();
            if matches!(
                status,
                TaskStatus::Complete | TaskStatus::Failed | TaskStatus::Cancelled
            ) {
                continue;
            }
            Self::cancel_task(os, &task_id, reason.clone())?;
            cancelled.push(task_id);
        }
        Ok(cancelled)
    }

    pub fn retry_task(
        os: &mut OperatingSystem,
        task_id: &TaskId,
        note: Option<String>,
    ) -> Result<(), RuntimeError> {
        let status = os
            .tasks
            .get(task_id)
            .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?
            .status
            .clone();
        if status == TaskStatus::Running {
            return Err(RuntimeError::InvalidTransition {
                task_id: task_id.clone(),
                from: status,
                to: TaskStatus::Pending,
            });
        }
        Self::reset_to_pending(os, task_id, note)
    }

    pub fn unblock_task(
        os: &mut OperatingSystem,
        task_id: &TaskId,
        note: Option<String>,
    ) -> Result<(), RuntimeError> {
        let status = os
            .tasks
            .get(task_id)
            .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?
            .status
            .clone();
        if status != TaskStatus::Blocked {
            return Err(RuntimeError::InvalidTransition {
                task_id: task_id.clone(),
                from: status,
                to: TaskStatus::Pending,
            });
        }
        Self::reset_to_pending(os, task_id, note)
    }

    pub fn delete_task(os: &mut OperatingSystem, task_id: &TaskId) -> Result<(), RuntimeError> {
        let status = os
            .tasks
            .get(task_id)
            .map(|task| task.status.clone())
            .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?;
        if status == TaskStatus::Running {
            return Err(RuntimeError::TaskNotEditable {
                task_id: task_id.clone(),
                status,
            });
        }
        let blockers = os.task_removal_blockers(task_id);
        if !blockers.is_empty() {
            return Err(RuntimeError::TaskReferenced {
                task_id: task_id.clone(),
                references: blockers.join("; "),
            });
        }
        Self::release_agent_capacity(os, task_id)?;
        os.tasks
            .remove(task_id)
            .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?;
        for task in os.tasks.values_mut() {
            task.dependencies.retain(|dependency| dependency != task_id);
            task.updated_at = Utc::now();
        }
        os.record(EventKind::TaskUpdated, format!("deleted task {}", task_id));
        Ok(())
    }

    fn reset_to_pending(
        os: &mut OperatingSystem,
        task_id: &TaskId,
        output: Option<String>,
    ) -> Result<(), RuntimeError> {
        Self::release_agent_capacity(os, task_id)?;
        let task = os
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?;
        task.status = TaskStatus::Pending;
        task.assigned_to = None;
        task.output = output;
        task.updated_at = Utc::now();
        os.record(
            EventKind::TaskUpdated,
            format!("reset task {} to pending", task_id),
        );
        Ok(())
    }

    fn finish_with_status(
        os: &mut OperatingSystem,
        task_id: &TaskId,
        status: TaskStatus,
        output: Option<String>,
    ) -> Result<(), RuntimeError> {
        let agent_id = {
            let task = os
                .tasks
                .get_mut(task_id)
                .ok_or_else(|| RuntimeError::TaskNotFound(task_id.clone()))?;
            task.status = status.clone();
            task.output = output;
            task.updated_at = Utc::now();
            task.assigned_to.clone()
        };

        if let Some(agent_id) = agent_id {
            Self::release_specific_agent_capacity(os, task_id, &agent_id);
        }

        os.record(
            EventKind::TaskUpdated,
            format!("marked task {} as {}", task_id, status),
        );
        Ok(())
    }

    fn request_active_runs_for_task_cancellation(
        os: &mut OperatingSystem,
        task_id: &TaskId,
    ) -> usize {
        let mut requested = 0;
        for run in os
            .runs
            .values_mut()
            .filter(|run| run.task_id == *task_id && run.status == RunStatus::Running)
        {
            run.status = RunStatus::CancelRequested;
            run.finished_at = None;
            run.exit_code = None;
            requested += 1;
        }
        requested
    }

    fn release_agent_capacity(
        os: &mut OperatingSystem,
        task_id: &TaskId,
    ) -> Result<(), RuntimeError> {
        let agent_id = os
            .tasks
            .get(task_id)
            .and_then(|task| task.assigned_to.clone());
        if let Some(agent_id) = agent_id {
            Self::release_specific_agent_capacity(os, task_id, &agent_id);
        }
        Ok(())
    }

    fn release_specific_agent_capacity(
        os: &mut OperatingSystem,
        task_id: &TaskId,
        agent_id: &crate::models::AgentId,
    ) {
        if let Some(agent) = os.agents.get_mut(agent_id) {
            agent.current_tasks.retain(|id| id != task_id);
            agent.updated_at = Utc::now();
        }
    }

    fn release_task_from_all_agents(os: &mut OperatingSystem, task_id: &TaskId) {
        for agent in os.agents.values_mut() {
            let before = agent.current_tasks.len();
            agent.current_tasks.retain(|id| id != task_id);
            if agent.current_tasks.len() != before {
                agent.updated_at = Utc::now();
            }
        }
    }
}
