use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub const CURRENT_STATE_VERSION: u32 = 3;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MigrationReport {
    pub from_version: u32,
    pub to_version: u32,
    pub changed: bool,
    pub steps: Vec<String>,
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
    }

    Ok(MigrationReport {
        from_version,
        to_version: CURRENT_STATE_VERSION,
        changed: !steps.is_empty(),
        steps,
    })
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
        assert_eq!(value["version"], 3);
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
        assert_eq!(report.to_version, 3);
        assert_eq!(value["version"], 3);
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
        assert_eq!(report.to_version, 3);
        assert!(report.changed);
        assert_eq!(value["version"], 3);
        assert!(
            value["workflows"]
                .as_object()
                .expect("workflows")
                .is_empty()
        );
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
