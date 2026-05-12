use crate::models::{AgentId, EventKind, OperatingSystem, TaskId, TaskStatus};
use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub task_id: TaskId,
    pub agent_id: AgentId,
    pub reason: String,
}

pub struct Scheduler;

impl Scheduler {
    pub fn next(os: &OperatingSystem) -> Option<Assignment> {
        let task = next_ready_task(os)?;
        let now = Utc::now();

        let agent = os
            .agents
            .values()
            .filter(|agent| agent.can_accept_at(&task.required_capabilities, now))
            .min_by_key(|agent| (agent.current_tasks.len(), agent.created_at))?;

        Some(Assignment {
            task_id: task.id.clone(),
            agent_id: agent.id.clone(),
            reason: format!(
                "{} can satisfy [{}]",
                agent.name,
                task.required_capabilities.join(", ")
            ),
        })
    }

    pub fn assign_next(os: &mut OperatingSystem) -> Option<Assignment> {
        let assignment = Self::next(os)?;
        assign(os, assignment)
    }

    pub fn assign_ready_task(os: &mut OperatingSystem, task_id: &TaskId) -> Option<Assignment> {
        let task = os.tasks.get(task_id)?;
        if task.status != TaskStatus::Pending || !dependencies_complete(os, task) {
            return None;
        }
        let now = Utc::now();
        let agent = os
            .agents
            .values()
            .filter(|agent| agent.can_accept_at(&task.required_capabilities, now))
            .min_by_key(|agent| (agent.current_tasks.len(), agent.created_at))?;
        assign(
            os,
            Assignment {
                task_id: task.id.clone(),
                agent_id: agent.id.clone(),
                reason: format!(
                    "{} can satisfy [{}]",
                    agent.name,
                    task.required_capabilities.join(", ")
                ),
            },
        )
    }

    pub fn assign_next_for_agent(
        os: &mut OperatingSystem,
        agent_id: &AgentId,
    ) -> Option<Assignment> {
        let agent = os.agents.get(agent_id)?;
        let now = Utc::now();
        let task = next_ready_tasks(os)
            .filter(|task| agent.can_accept_at(&task.required_capabilities, now))
            .max_by(|left, right| {
                left.priority
                    .cmp(&right.priority)
                    .then_with(|| right.created_at.cmp(&left.created_at))
            })?;
        assign(
            os,
            Assignment {
                task_id: task.id.clone(),
                agent_id: agent_id.clone(),
                reason: format!(
                    "{} claimed task with [{}]",
                    agent.name,
                    task.required_capabilities.join(", ")
                ),
            },
        )
    }
}

fn next_ready_task(os: &OperatingSystem) -> Option<&crate::models::Task> {
    next_ready_tasks(os).max_by(|left, right| {
        left.priority
            .cmp(&right.priority)
            .then_with(|| right.created_at.cmp(&left.created_at))
    })
}

fn next_ready_tasks(os: &OperatingSystem) -> impl Iterator<Item = &crate::models::Task> {
    os.tasks
        .values()
        .filter(|task| task.status == TaskStatus::Pending)
        .filter(|task| dependencies_complete(os, task))
}

fn dependencies_complete(os: &OperatingSystem, task: &crate::models::Task) -> bool {
    task.dependencies.iter().all(|dependency| {
        os.tasks
            .get(dependency)
            .map(|task| task.status == TaskStatus::Complete)
            .unwrap_or(false)
    })
}

fn assign(os: &mut OperatingSystem, assignment: Assignment) -> Option<Assignment> {
    let now = Utc::now();
    let required_capabilities = {
        let task = os.tasks.get(&assignment.task_id)?;
        if task.status != TaskStatus::Pending {
            return None;
        }
        task.required_capabilities.clone()
    };

    let agent = os.agents.get(&assignment.agent_id)?;
    if agent
        .current_tasks
        .iter()
        .any(|task_id| task_id == &assignment.task_id)
        || !agent.can_accept_at(&required_capabilities, now)
    {
        return None;
    }

    if let Some(task) = os.tasks.get_mut(&assignment.task_id) {
        task.status = TaskStatus::Running;
        task.assigned_to = Some(assignment.agent_id.clone());
        task.updated_at = now;
    }

    if let Some(agent) = os.agents.get_mut(&assignment.agent_id) {
        agent.current_tasks.push(assignment.task_id.clone());
        agent.updated_at = now;
    }

    os.record(
        EventKind::TaskAssigned,
        format!(
            "assigned task {} to {}",
            assignment.task_id, assignment.agent_id
        ),
    );
    Some(assignment)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Agent, AgentKind, OperatingSystem, Priority, Task};

    #[test]
    fn picks_highest_priority_matching_agent() {
        let mut os = OperatingSystem::new("test");
        os.register_agent(Agent::new(
            "Builder",
            AgentKind::Builder,
            None,
            vec!["code".into(), "rust".into()],
            1,
        ));
        os.create_task(Task::new(
            "Low task",
            "minor work",
            Priority::Low,
            vec!["rust".into()],
        ));
        let critical = Task::new(
            "Critical task",
            "ship it",
            Priority::Critical,
            vec!["rust".into(), "code".into()],
        );
        let expected_id = critical.id.clone();
        os.create_task(critical);

        let assignment = Scheduler::assign_next(&mut os).expect("assignment");

        assert_eq!(assignment.task_id, expected_id);
        assert_eq!(
            os.tasks.get(&expected_id).expect("task").status,
            TaskStatus::Running
        );
    }

    #[test]
    fn waits_for_dependencies_before_assignment() {
        let mut os = OperatingSystem::new("test");
        os.register_agent(Agent::new(
            "Builder",
            AgentKind::Builder,
            None,
            vec!["code".into()],
            2,
        ));
        let dependency = Task::new("Plan", "plan", Priority::Normal, vec!["code".into()]);
        let dependency_id = dependency.id.clone();
        os.create_task(dependency);

        let mut dependent = Task::new("Build", "build", Priority::Critical, vec!["code".into()]);
        let dependent_id = dependent.id.clone();
        dependent.dependencies.push(dependency_id.clone());
        os.create_task(dependent);

        let assignment = Scheduler::assign_next(&mut os).expect("assignment");

        assert_eq!(assignment.task_id, dependency_id);
        assert_eq!(
            os.tasks.get(&dependent_id).expect("dependent").status,
            TaskStatus::Pending
        );
    }

    #[test]
    fn agent_can_claim_only_matching_ready_work() {
        let mut os = OperatingSystem::new("test");
        let agent = Agent::new(
            "Reviewer",
            AgentKind::Reviewer,
            None,
            vec!["review".into()],
            1,
        );
        let agent_id = agent.id.clone();
        os.register_agent(agent);
        os.create_task(Task::new(
            "Build",
            "build",
            Priority::Critical,
            vec!["code".into()],
        ));
        let review = Task::new("Review", "review", Priority::Normal, vec!["review".into()]);
        let review_id = review.id.clone();
        os.create_task(review);

        let assignment = Scheduler::assign_next_for_agent(&mut os, &agent_id).expect("assignment");

        assert_eq!(assignment.task_id, review_id);
        assert_eq!(
            os.tasks.get(&review_id).expect("review").status,
            TaskStatus::Running
        );
    }

    #[test]
    fn can_assign_specific_ready_task_without_global_priority_ordering() {
        let mut os = OperatingSystem::new("test");
        os.register_agent(Agent::new(
            "Planner",
            AgentKind::Planner,
            None,
            vec!["plan".into()],
            1,
        ));
        os.register_agent(Agent::new(
            "Builder",
            AgentKind::Builder,
            None,
            vec!["code".into()],
            1,
        ));
        os.create_task(Task::new(
            "Critical build",
            "global work",
            Priority::Critical,
            vec!["code".into()],
        ));
        let workflow_plan = Task::new(
            "Workflow plan",
            "scoped work",
            Priority::Normal,
            vec!["plan".into()],
        );
        let workflow_plan_id = workflow_plan.id.clone();
        os.create_task(workflow_plan);

        let assignment =
            Scheduler::assign_ready_task(&mut os, &workflow_plan_id).expect("assignment");

        assert_eq!(assignment.task_id, workflow_plan_id);
        assert_eq!(
            os.tasks
                .get(&workflow_plan_id)
                .expect("workflow task")
                .status,
            TaskStatus::Running
        );
    }

    #[test]
    fn skips_agents_with_expired_leases() {
        let mut os = OperatingSystem::new("test");
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["code".into()], 1);
        agent.lease_expires_at = Some(Utc::now() - chrono::Duration::seconds(1));
        os.register_agent(agent);
        os.create_task(Task::new(
            "Build",
            "build",
            Priority::Critical,
            vec!["code".into()],
        ));

        assert!(Scheduler::assign_next(&mut os).is_none());
    }

    #[test]
    fn stale_assignment_does_not_overfill_agent() {
        let mut os = OperatingSystem::new("test");
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["code".into()], 1);
        let agent_id = agent.id.clone();
        let mut running = Task::new("Running", "run", Priority::Normal, vec!["code".into()]);
        let running_id = running.id.clone();
        running.status = TaskStatus::Running;
        running.assigned_to = Some(agent_id.clone());
        agent.current_tasks.push(running_id);
        os.register_agent(agent);
        os.create_task(running);
        let pending = Task::new("Pending", "wait", Priority::Normal, vec!["code".into()]);
        let pending_id = pending.id.clone();
        os.create_task(pending);

        let assignment = assign(
            &mut os,
            Assignment {
                task_id: pending_id.clone(),
                agent_id: agent_id.clone(),
                reason: "stale assignment".into(),
            },
        );

        assert!(assignment.is_none());
        assert_eq!(
            os.tasks.get(&pending_id).expect("task").status,
            TaskStatus::Pending
        );
        assert_eq!(
            os.agents.get(&agent_id).expect("agent").current_tasks.len(),
            1
        );
    }

    #[test]
    fn stale_assignment_does_not_duplicate_current_task() {
        let mut os = OperatingSystem::new("test");
        let mut agent = Agent::new("Builder", AgentKind::Builder, None, vec!["code".into()], 2);
        let agent_id = agent.id.clone();
        let pending = Task::new("Pending", "wait", Priority::Normal, vec!["code".into()]);
        let pending_id = pending.id.clone();
        agent.current_tasks.push(pending_id.clone());
        os.register_agent(agent);
        os.create_task(pending);

        let assignment = assign(
            &mut os,
            Assignment {
                task_id: pending_id.clone(),
                agent_id: agent_id.clone(),
                reason: "duplicate assignment".into(),
            },
        );

        assert!(assignment.is_none());
        assert_eq!(
            os.tasks.get(&pending_id).expect("task").status,
            TaskStatus::Pending
        );
        assert_eq!(
            os.agents.get(&agent_id).expect("agent").current_tasks,
            vec![pending_id]
        );
    }

    #[test]
    fn stale_assignment_does_not_reassign_non_pending_task() {
        let mut os = OperatingSystem::new("test");
        let agent = Agent::new("Builder", AgentKind::Builder, None, vec!["code".into()], 1);
        let agent_id = agent.id.clone();
        os.register_agent(agent);
        let mut complete = Task::new("Complete", "done", Priority::Normal, vec!["code".into()]);
        let complete_id = complete.id.clone();
        complete.status = TaskStatus::Complete;
        os.create_task(complete);

        let assignment = assign(
            &mut os,
            Assignment {
                task_id: complete_id.clone(),
                agent_id: agent_id.clone(),
                reason: "stale assignment".into(),
            },
        );

        assert!(assignment.is_none());
        assert_eq!(
            os.tasks.get(&complete_id).expect("task").status,
            TaskStatus::Complete
        );
        assert!(
            os.agents
                .get(&agent_id)
                .expect("agent")
                .current_tasks
                .is_empty()
        );
    }
}
