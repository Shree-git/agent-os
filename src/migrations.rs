use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub const CURRENT_STATE_VERSION: u32 = 4;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MigrationReport {
    pub from_version: u32,
    pub to_version: u32,
    pub changed: bool,
    pub steps: Vec<String>,
    pub downgrade_notes: Vec<String>,
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("state root must be a JSON object")]
    InvalidRoot,
    #[error("state version must be a non-negative integer")]
    InvalidVersion,
    #[error("state version {0} is newer than this binary supports ({CURRENT_STATE_VERSION})")]
    UnsupportedFutureVersion(u64),
}

pub fn migrate_state_value(value: &mut Value) -> Result<MigrationReport, MigrationError> {
    let object = value.as_object_mut().ok_or(MigrationError::InvalidRoot)?;
    let raw_version = match object.get("version") {
        Some(Value::Number(number)) => number.as_u64().ok_or(MigrationError::InvalidVersion)?,
        Some(_) => return Err(MigrationError::InvalidVersion),
        None => 0,
    };
    if raw_version > u64::from(CURRENT_STATE_VERSION) {
        return Err(MigrationError::UnsupportedFutureVersion(raw_version));
    }
    let from_version = raw_version as u32;
    let mut current_version = from_version;

    let mut steps = Vec::new();
    if current_version == 0 {
        object.insert("version".into(), Value::from(1));
        current_version = 1;
        steps.push("set missing state version to 1".into());
    }
    if current_version < 2 {
        backfill_memory_updated_at(object);
        object.insert("version".into(), Value::from(2));
        steps.push("backfilled memory updated_at timestamps".into());
        current_version = 2;
    }
    if current_version < 3 {
        object
            .entry("workflows")
            .or_insert_with(|| Value::Object(Default::default()));
        object.insert("version".into(), Value::from(3));
        steps.push("initialized workflow registry".into());
        current_version = 3;
    }
    if current_version < 4 {
        object
            .entry("approvals")
            .or_insert_with(|| Value::Object(Default::default()));
        object
            .entry("mcp_servers")
            .or_insert_with(|| Value::Object(Default::default()));
        object
            .entry("agent_profiles")
            .or_insert_with(|| Value::Object(Default::default()));
        object
            .entry("workflow_templates")
            .or_insert_with(|| Value::Object(Default::default()));
        object.entry("memory_policy").or_insert_with(|| {
            serde_json::json!({
                "scope": null,
                "semantic_recall": false,
                "max_provider_memories": 5,
                "max_age_days": null
            })
        });
        if let Some(memory_policy) = object
            .get_mut("memory_policy")
            .and_then(Value::as_object_mut)
        {
            memory_policy.entry("scope").or_insert(Value::Null);
            memory_policy
                .entry("semantic_recall")
                .or_insert(Value::Bool(false));
            memory_policy
                .entry("max_provider_memories")
                .or_insert(Value::from(5));
            memory_policy.entry("max_age_days").or_insert(Value::Null);
        }
        object
            .entry("evals")
            .or_insert_with(|| Value::Array(Default::default()));
        object
            .entry("workers")
            .or_insert_with(|| Value::Object(Default::default()));
        object
            .entry("secrets_backends")
            .or_insert_with(|| Value::Object(Default::default()));
        backfill_policy_runtime_controls(object);
        backfill_provider_plugins(object);
        backfill_run_artifacts(object);
        object.insert("version".into(), Value::from(4));
        steps.push("initialized platform runtime registries and controls".into());
    }

    Ok(MigrationReport {
        from_version,
        to_version: CURRENT_STATE_VERSION,
        changed: !steps.is_empty(),
        steps,
        downgrade_notes: downgrade_notes(from_version, CURRENT_STATE_VERSION),
    })
}

fn downgrade_notes(from_version: u32, to_version: u32) -> Vec<String> {
    if from_version == to_version {
        return vec![
            "State is already at the current schema; no downgrade action is available or required."
                .into(),
        ];
    }
    vec![
        format!(
            "Automatic downgrade from state version {to_version} to {from_version} is not supported."
        ),
        "Keep or create a pre-migration backup/export and restore that file with an older compatible binary if rollback is needed.".into(),
    ]
}

fn backfill_policy_runtime_controls(object: &mut serde_json::Map<String, Value>) {
    let policy = object
        .entry("policy")
        .or_insert_with(|| Value::Object(Default::default()));
    let Some(policy) = policy.as_object_mut() else {
        return;
    };
    policy.entry("sandbox").or_insert_with(|| {
        serde_json::json!({
            "process_isolation": true,
            "jailed_workspaces": true,
            "writable_paths": []
        })
    });
    if let Some(sandbox) = policy.get_mut("sandbox").and_then(Value::as_object_mut) {
        sandbox
            .entry("process_isolation")
            .or_insert(Value::Bool(true));
        sandbox
            .entry("jailed_workspaces")
            .or_insert(Value::Bool(true));
        sandbox
            .entry("writable_paths")
            .or_insert_with(|| Value::Array(Default::default()));
    }
    policy.entry("network").or_insert_with(|| {
        serde_json::json!({
            "mode": "providers-only",
            "allowed_hosts": []
        })
    });
    if let Some(network) = policy.get_mut("network").and_then(Value::as_object_mut) {
        network
            .entry("mode")
            .or_insert_with(|| Value::String("providers-only".into()));
        network
            .entry("allowed_hosts")
            .or_insert_with(|| Value::Array(Default::default()));
    }
    policy
        .entry("approval")
        .or_insert_with(|| serde_json::json!({
            "require_for_risky_actions": true,
            "risky_patterns": ["git push", "git commit", "rm ", "mv ", "chmod", "curl ", "wget ", "ssh "]
        }));
    if let Some(approval) = policy.get_mut("approval").and_then(Value::as_object_mut) {
        approval
            .entry("require_for_risky_actions")
            .or_insert(Value::Bool(true));
        approval.entry("risky_patterns").or_insert_with(|| {
            serde_json::json!([
                "git push",
                "git commit",
                "rm ",
                "mv ",
                "chmod",
                "curl ",
                "wget ",
                "ssh "
            ])
        });
    }
    policy
        .entry("autonomy")
        .or_insert_with(|| Value::String("execute-with-approval".into()));
    policy
        .entry("rules")
        .or_insert_with(|| Value::Array(Default::default()));
}

fn backfill_provider_plugins(object: &mut serde_json::Map<String, Value>) {
    let Some(provider) = object.get_mut("provider").and_then(Value::as_object_mut) else {
        return;
    };
    provider.entry("max_retries").or_insert(Value::from(2));
    provider
        .entry("retry_backoff_ms")
        .or_insert(Value::from(250));
    provider.entry("adapter").or_insert(Value::Null);
    provider
        .entry("request_options")
        .or_insert_with(|| Value::Object(Default::default()));
    provider.entry("response_schema").or_insert(Value::Null);
    provider.entry("plugin_command").or_insert(Value::Null);
    provider
        .entry("plugin_args")
        .or_insert_with(|| Value::Array(Default::default()));
    provider
        .entry("plugin_env")
        .or_insert_with(|| Value::Object(Default::default()));
}

fn backfill_run_artifacts(object: &mut serde_json::Map<String, Value>) {
    let Some(runs) = object.get_mut("runs").and_then(Value::as_object_mut) else {
        return;
    };
    for (run_id, run) in runs.iter_mut() {
        let Some(run) = run.as_object_mut() else {
            continue;
        };
        run.entry("trace_id")
            .or_insert_with(|| Value::String(format!("trace-{run_id}")));
        run.entry("artifacts")
            .or_insert_with(|| Value::Array(Default::default()));
    }
}

fn backfill_memory_updated_at(object: &mut serde_json::Map<String, Value>) {
    let Some(memory) = object.get_mut("memory").and_then(Value::as_array_mut) else {
        return;
    };
    for record in memory {
        let Some(record) = record.as_object_mut() else {
            continue;
        };
        if record.contains_key("updated_at") {
            continue;
        }
        if let Some(created_at) = record.get("created_at").cloned() {
            record.insert("updated_at".into(), created_at);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::OperatingSystem;

    #[test]
    fn migrates_versioned_state_fixtures_to_current_schema() {
        for (name, raw, from_version, expected_steps) in [
            (
                "state-v0",
                include_str!("../tests/fixtures/migrations/state-v0.json"),
                0,
                vec![
                    "set missing state version to 1",
                    "backfilled memory updated_at timestamps",
                    "initialized workflow registry",
                    "initialized platform runtime registries and controls",
                ],
            ),
            (
                "state-v1",
                include_str!("../tests/fixtures/migrations/state-v1.json"),
                1,
                vec![
                    "backfilled memory updated_at timestamps",
                    "initialized workflow registry",
                    "initialized platform runtime registries and controls",
                ],
            ),
            (
                "state-v2",
                include_str!("../tests/fixtures/migrations/state-v2.json"),
                2,
                vec![
                    "initialized workflow registry",
                    "initialized platform runtime registries and controls",
                ],
            ),
            (
                "state-v3",
                include_str!("../tests/fixtures/migrations/state-v3.json"),
                3,
                vec!["initialized platform runtime registries and controls"],
            ),
        ] {
            let mut value: Value =
                serde_json::from_str(raw).unwrap_or_else(|error| panic!("{name}: {error}"));

            let report =
                migrate_state_value(&mut value).unwrap_or_else(|error| panic!("{name}: {error}"));

            assert_eq!(report.from_version, from_version, "{name}");
            assert_eq!(report.to_version, CURRENT_STATE_VERSION, "{name}");
            assert!(report.changed, "{name}");
            assert_eq!(value["version"], CURRENT_STATE_VERSION, "{name}");
            for step in expected_steps {
                assert!(
                    report.steps.iter().any(|actual| actual == step),
                    "{name} missing migration step `{step}` in {:?}",
                    report.steps
                );
            }
            assert!(
                report
                    .downgrade_notes
                    .iter()
                    .any(|note| note.contains("Automatic downgrade")),
                "{name} missing downgrade note"
            );
            let migrated: OperatingSystem = serde_json::from_value(value.clone())
                .unwrap_or_else(|error| panic!("{name} should deserialize: {error}"));
            assert_eq!(migrated.version, CURRENT_STATE_VERSION, "{name}");
            assert_eq!(migrated.name, format!("fixture-v{from_version}"), "{name}");
            assert!(migrated.approvals.is_empty(), "{name}");
            assert!(migrated.mcp_servers.is_empty(), "{name}");
            assert!(migrated.workers.is_empty(), "{name}");
            assert!(migrated.secrets_backends.is_empty(), "{name}");
            if from_version <= 1 && !migrated.memory.is_empty() {
                assert_eq!(migrated.memory[0].created_at, migrated.memory[0].updated_at);
            }
            if from_version == 3 {
                let run = migrated
                    .runs
                    .values()
                    .next()
                    .unwrap_or_else(|| panic!("{name} missing migrated run"));
                assert_eq!(run.trace_id, "trace-run-one");
                assert!(run.artifacts.is_empty());
            }
        }
    }

    #[test]
    fn migrates_legacy_state_without_version() {
        let mut value = serde_json::json!({
            "name": "legacy",
            "agents": {},
            "tasks": {},
            "memory": [],
            "events": [],
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        });

        let report = migrate_state_value(&mut value).expect("migrate");

        assert_eq!(report.from_version, 0);
        assert!(report.changed);
        assert!(
            report
                .downgrade_notes
                .iter()
                .any(|note| note.contains("Automatic downgrade"))
        );
        assert!(
            report
                .downgrade_notes
                .iter()
                .any(|note| note.contains("pre-migration backup/export"))
        );
        assert_eq!(value["version"], 4);
        assert!(
            value["workflows"]
                .as_object()
                .expect("workflows")
                .is_empty()
        );
    }

    #[test]
    fn migrates_version_one_memory_updated_at() {
        let mut value = serde_json::json!({
            "version": 1,
            "name": "legacy",
            "agents": {},
            "tasks": {},
            "memory": [
                {
                    "id": "memory-1",
                    "topic": "topic",
                    "body": "body",
                    "tags": [],
                    "created_at": "2026-01-01T00:00:00Z"
                }
            ],
            "events": [],
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        });

        let report = migrate_state_value(&mut value).expect("migrate");

        assert_eq!(report.from_version, 1);
        assert!(report.changed);
        assert_eq!(report.to_version, 4);
        assert_eq!(value["version"], 4);
        assert_eq!(value["memory"][0]["updated_at"], "2026-01-01T00:00:00Z");
        assert!(
            value["workflows"]
                .as_object()
                .expect("workflows")
                .is_empty()
        );
    }

    #[test]
    fn migrates_version_two_workflows_registry() {
        let mut value = serde_json::json!({
            "version": 2,
            "name": "legacy",
            "agents": {},
            "tasks": {},
            "runs": {},
            "tools": {},
            "memory": [],
            "events": [],
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        });

        let report = migrate_state_value(&mut value).expect("migrate");

        assert_eq!(report.from_version, 2);
        assert_eq!(report.to_version, 4);
        assert!(report.changed);
        assert_eq!(value["version"], 4);
        assert!(
            value["workflows"]
                .as_object()
                .expect("workflows")
                .is_empty()
        );
    }

    #[test]
    fn migrates_version_three_runtime_registries_and_partial_controls() {
        let mut value = serde_json::json!({
            "version": 3,
            "name": "legacy",
            "agents": {},
            "tasks": {},
            "runs": {
                "run-one": {
                    "id": "run-one",
                    "task_id": "task-one",
                    "agent_id": null,
                    "command": "printf ok",
                    "cwd": ".",
                    "status": "success",
                    "exit_code": 0,
                    "started_at": "2026-01-01T00:00:00Z",
                    "finished_at": "2026-01-01T00:00:01Z",
                    "log_path": "runs/run-one.log"
                }
            },
            "tools": {},
            "workflows": {},
            "memory": [],
            "events": [],
            "policy": {
                "sandbox": {},
                "network": {},
                "approval": {}
            },
            "provider": {},
            "memory_policy": {},
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        });

        let report = migrate_state_value(&mut value).expect("migrate");

        assert_eq!(report.from_version, 3);
        assert_eq!(report.to_version, 4);
        assert!(report.changed);
        assert_eq!(value["version"], 4);
        assert!(
            value["approvals"]
                .as_object()
                .expect("approvals")
                .is_empty()
        );
        assert!(
            value["mcp_servers"]
                .as_object()
                .expect("mcp servers")
                .is_empty()
        );
        assert!(
            value["agent_profiles"]
                .as_object()
                .expect("agent profiles")
                .is_empty()
        );
        assert!(
            value["workflow_templates"]
                .as_object()
                .expect("workflow templates")
                .is_empty()
        );
        assert!(value["evals"].as_array().expect("evals").is_empty());
        assert!(value["workers"].as_object().expect("workers").is_empty());
        assert!(
            value["secrets_backends"]
                .as_object()
                .expect("secrets backends")
                .is_empty()
        );
        assert_eq!(value["policy"]["sandbox"]["process_isolation"], true);
        assert_eq!(value["policy"]["sandbox"]["jailed_workspaces"], true);
        assert_eq!(
            value["policy"]["sandbox"]["writable_paths"],
            serde_json::json!([])
        );
        assert_eq!(value["policy"]["network"]["mode"], "providers-only");
        assert_eq!(
            value["policy"]["network"]["allowed_hosts"],
            serde_json::json!([])
        );
        assert_eq!(
            value["policy"]["approval"]["require_for_risky_actions"],
            true
        );
        assert!(
            value["policy"]["approval"]["risky_patterns"]
                .as_array()
                .expect("risky patterns")
                .iter()
                .any(|pattern| pattern == "git push")
        );
        assert_eq!(value["policy"]["autonomy"], "execute-with-approval");
        assert_eq!(value["policy"]["rules"], serde_json::json!([]));
        assert_eq!(value["memory_policy"]["scope"], Value::Null);
        assert_eq!(value["memory_policy"]["semantic_recall"], false);
        assert_eq!(value["memory_policy"]["max_provider_memories"], 5);
        assert_eq!(value["memory_policy"]["max_age_days"], Value::Null);
        assert_eq!(value["provider"]["max_retries"], 2);
        assert_eq!(value["provider"]["retry_backoff_ms"], 250);
        assert_eq!(value["provider"]["adapter"], Value::Null);
        assert_eq!(value["provider"]["request_options"], serde_json::json!({}));
        assert_eq!(value["provider"]["response_schema"], Value::Null);
        assert_eq!(value["provider"]["plugin_command"], Value::Null);
        assert_eq!(value["provider"]["plugin_args"], serde_json::json!([]));
        assert_eq!(value["provider"]["plugin_env"], serde_json::json!({}));
        assert_eq!(value["runs"]["run-one"]["trace_id"], "trace-run-one");
        assert_eq!(value["runs"]["run-one"]["artifacts"], serde_json::json!([]));
    }

    #[test]
    fn rejects_future_state_versions_without_truncating() {
        let mut value = serde_json::json!({
            "version": u64::MAX,
            "name": "future",
            "agents": {},
            "tasks": {},
            "memory": [],
            "events": [],
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        });

        let error = migrate_state_value(&mut value).expect_err("future version");

        assert!(matches!(
            error,
            MigrationError::UnsupportedFutureVersion(u64::MAX)
        ));
    }

    #[test]
    fn rejects_malformed_state_versions() {
        for version in [
            serde_json::json!(-1),
            serde_json::json!(1.5),
            serde_json::json!("1"),
        ] {
            let mut value = serde_json::json!({
                "version": version,
                "name": "malformed",
                "agents": {},
                "tasks": {},
                "memory": [],
                "events": [],
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z"
            });

            let error = migrate_state_value(&mut value).expect_err("malformed version");

            assert!(matches!(error, MigrationError::InvalidVersion));
        }
    }
}
