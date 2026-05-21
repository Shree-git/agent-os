use crate::models::{Event, MemoryRecord, OperatingSystem, RunId, RunRecord, Task, TaskId};
use crate::store::{Store, StoreError};
use crate::validation::{ValidationReport, validate_state};
use chrono::Utc;
use rusqlite::{Connection, params};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct SqliteImportReport {
    pub imported_snapshot: bool,
    pub imported_runs: usize,
    pub imported_run_logs: usize,
    pub skipped_run_logs: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct SqliteRestoreReport {
    pub restored_snapshot: bool,
    pub restored_runs: usize,
    pub validation: ValidationReport,
}

#[derive(Debug, Error)]
pub enum SqliteStoreError {
    #[error("sqlite error at {path}: {source}")]
    Sqlite {
        path: PathBuf,
        source: rusqlite::Error,
    },
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("invalid sqlite state payload at {path}: {source}")]
    Json {
        path: PathBuf,
        source: serde_json::Error,
    },
}

#[derive(Clone, Debug)]
pub struct SqliteStore {
    path: PathBuf,
}

impl SqliteStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn init(&self) -> Result<(), SqliteStoreError> {
        let connection = self.connection()?;
        connection
            .execute_batch(
                "
                PRAGMA journal_mode = WAL;
                CREATE TABLE IF NOT EXISTS state_snapshots (
                    id TEXT PRIMARY KEY,
                    body TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS run_logs (
                    run_id TEXT PRIMARY KEY,
                    body TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS run_records (
                    run_id TEXT PRIMARY KEY,
                    body TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS task_records (
                    task_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    status TEXT NOT NULL,
                    priority TEXT NOT NULL,
                    assigned_to TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 1,
                    body TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS task_records_status_idx ON task_records(status);
                CREATE INDEX IF NOT EXISTS task_records_assigned_to_idx ON task_records(assigned_to);
                CREATE TABLE IF NOT EXISTS event_records (
                    event_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    at TEXT NOT NULL,
                    message TEXT NOT NULL,
                    body TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS event_records_kind_idx ON event_records(kind);
                CREATE INDEX IF NOT EXISTS event_records_at_idx ON event_records(at);
                CREATE TABLE IF NOT EXISTS memory_records (
                    memory_id TEXT PRIMARY KEY,
                    topic TEXT NOT NULL,
                    visibility TEXT NOT NULL,
                    scope TEXT,
                    updated_at_source TEXT NOT NULL,
                    body TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS memory_records_topic_idx ON memory_records(topic);
                CREATE INDEX IF NOT EXISTS memory_records_visibility_idx ON memory_records(visibility);
                CREATE INDEX IF NOT EXISTS memory_records_scope_idx ON memory_records(scope);
                CREATE INDEX IF NOT EXISTS memory_records_updated_at_source_idx ON memory_records(updated_at_source);
                ",
            )
            .map_err(|source| self.sqlite_error(source))?;
        self.ensure_column(
            &connection,
            "task_records",
            "attempts",
            "ALTER TABLE task_records ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0",
        )?;
        self.ensure_column(
            &connection,
            "task_records",
            "max_attempts",
            "ALTER TABLE task_records ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 1",
        )?;
        Ok(())
    }

    pub fn save_snapshot(&self, os: &OperatingSystem) -> Result<(), SqliteStoreError> {
        self.init()?;
        let body = serde_json::to_string_pretty(os).map_err(|source| SqliteStoreError::Json {
            path: self.path.clone(),
            source,
        })?;
        let connection = self.connection()?;
        connection
            .execute(
                "
                INSERT INTO state_snapshots (id, body, updated_at)
                VALUES ('current', ?1, ?2)
                ON CONFLICT(id) DO UPDATE SET body = excluded.body, updated_at = excluded.updated_at
                ",
                params![body, Utc::now().to_rfc3339()],
            )
            .map_err(|source| self.sqlite_error(source))?;
        Ok(())
    }

    pub fn save_snapshot_with_records(&self, os: &OperatingSystem) -> Result<(), SqliteStoreError> {
        self.init()?;
        let snapshot_body =
            serde_json::to_string_pretty(os).map_err(|source| SqliteStoreError::Json {
                path: self.path.clone(),
                source,
            })?;
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction()
            .map_err(|source| self.sqlite_error(source))?;
        let updated_at = Utc::now().to_rfc3339();
        transaction
            .execute(
                "
                INSERT INTO state_snapshots (id, body, updated_at)
                VALUES ('current', ?1, ?2)
                ON CONFLICT(id) DO UPDATE SET body = excluded.body, updated_at = excluded.updated_at
                ",
                params![snapshot_body, updated_at],
            )
            .map_err(|source| self.sqlite_error(source))?;

        let mut current_run_ids = BTreeSet::new();
        for run in os.runs.values() {
            current_run_ids.insert(run.id.to_string());
            let body =
                serde_json::to_string_pretty(run).map_err(|source| SqliteStoreError::Json {
                    path: self.path.clone(),
                    source,
                })?;
            transaction
                .execute(
                    "
                    INSERT INTO run_records (run_id, body, updated_at)
                    VALUES (?1, ?2, ?3)
                    ON CONFLICT(run_id) DO UPDATE SET
                        body = excluded.body,
                        updated_at = excluded.updated_at
                    WHERE run_records.body IS NOT excluded.body
                    ",
                    params![run.id.to_string(), body, &updated_at],
                )
                .map_err(|source| self.sqlite_error(source))?;
        }

        let existing_run_ids = {
            let mut statement = transaction
                .prepare("SELECT run_id FROM run_records")
                .map_err(|source| self.sqlite_error(source))?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|source| self.sqlite_error(source))?;
            let mut run_ids = Vec::new();
            for row in rows {
                run_ids.push(row.map_err(|source| self.sqlite_error(source))?);
            }
            run_ids
        };
        for run_id in existing_run_ids {
            if !current_run_ids.contains(&run_id) {
                transaction
                    .execute("DELETE FROM run_records WHERE run_id = ?1", params![run_id])
                    .map_err(|source| self.sqlite_error(source))?;
            }
        }

        let mut current_task_ids = BTreeSet::new();
        for task in os.tasks.values() {
            current_task_ids.insert(task.id.to_string());
            let body =
                serde_json::to_string_pretty(task).map_err(|source| SqliteStoreError::Json {
                    path: self.path.clone(),
                    source,
                })?;
            transaction
                .execute(
                    "
                    INSERT INTO task_records (task_id, title, status, priority, assigned_to, attempts, max_attempts, body, updated_at)
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                    ON CONFLICT(task_id) DO UPDATE SET
                        title = excluded.title,
                        status = excluded.status,
                        priority = excluded.priority,
                        assigned_to = excluded.assigned_to,
                        attempts = excluded.attempts,
                        max_attempts = excluded.max_attempts,
                        body = excluded.body,
                        updated_at = excluded.updated_at
                    WHERE task_records.body IS NOT excluded.body
                    ",
                    params![
                        task.id.to_string(),
                        &task.title,
                        task.status.to_string(),
                        task.priority.to_string(),
                        task.assigned_to.as_ref().map(ToString::to_string),
                        i64::from(task.attempts),
                        i64::from(task.max_attempts),
                        body,
                        &updated_at
                    ],
                )
                .map_err(|source| self.sqlite_error(source))?;
        }

        let existing_task_ids = {
            let mut statement = transaction
                .prepare("SELECT task_id FROM task_records")
                .map_err(|source| self.sqlite_error(source))?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|source| self.sqlite_error(source))?;
            let mut task_ids = Vec::new();
            for row in rows {
                task_ids.push(row.map_err(|source| self.sqlite_error(source))?);
            }
            task_ids
        };
        for task_id in existing_task_ids {
            if !current_task_ids.contains(&task_id) {
                transaction
                    .execute(
                        "DELETE FROM task_records WHERE task_id = ?1",
                        params![task_id],
                    )
                    .map_err(|source| self.sqlite_error(source))?;
            }
        }

        let mut current_event_ids = BTreeSet::new();
        for event in &os.events {
            current_event_ids.insert(event.id.clone());
            let body =
                serde_json::to_string_pretty(event).map_err(|source| SqliteStoreError::Json {
                    path: self.path.clone(),
                    source,
                })?;
            transaction
                .execute(
                    "
                    INSERT INTO event_records (event_id, kind, at, message, body, updated_at)
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                    ON CONFLICT(event_id) DO UPDATE SET
                        kind = excluded.kind,
                        at = excluded.at,
                        message = excluded.message,
                        body = excluded.body,
                        updated_at = excluded.updated_at
                    WHERE event_records.body IS NOT excluded.body
                    ",
                    params![
                        &event.id,
                        event.kind.to_string(),
                        event.at.to_rfc3339(),
                        &event.message,
                        body,
                        &updated_at
                    ],
                )
                .map_err(|source| self.sqlite_error(source))?;
        }

        let existing_event_ids = {
            let mut statement = transaction
                .prepare("SELECT event_id FROM event_records")
                .map_err(|source| self.sqlite_error(source))?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|source| self.sqlite_error(source))?;
            let mut event_ids = Vec::new();
            for row in rows {
                event_ids.push(row.map_err(|source| self.sqlite_error(source))?);
            }
            event_ids
        };
        for event_id in existing_event_ids {
            if !current_event_ids.contains(&event_id) {
                transaction
                    .execute(
                        "DELETE FROM event_records WHERE event_id = ?1",
                        params![event_id],
                    )
                    .map_err(|source| self.sqlite_error(source))?;
            }
        }

        let mut current_memory_ids = BTreeSet::new();
        for memory in &os.memory {
            current_memory_ids.insert(memory.id.clone());
            let body =
                serde_json::to_string_pretty(memory).map_err(|source| SqliteStoreError::Json {
                    path: self.path.clone(),
                    source,
                })?;
            transaction
                .execute(
                    "
                    INSERT INTO memory_records (memory_id, topic, visibility, scope, updated_at_source, body, updated_at)
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                    ON CONFLICT(memory_id) DO UPDATE SET
                        topic = excluded.topic,
                        visibility = excluded.visibility,
                        scope = excluded.scope,
                        updated_at_source = excluded.updated_at_source,
                        body = excluded.body,
                        updated_at = excluded.updated_at
                    WHERE memory_records.body IS NOT excluded.body
                    ",
                    params![
                        &memory.id,
                        &memory.topic,
                        memory.visibility.to_string(),
                        memory.scope.as_deref(),
                        memory.updated_at.to_rfc3339(),
                        body,
                        &updated_at
                    ],
                )
                .map_err(|source| self.sqlite_error(source))?;
        }

        let existing_memory_ids = {
            let mut statement = transaction
                .prepare("SELECT memory_id FROM memory_records")
                .map_err(|source| self.sqlite_error(source))?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|source| self.sqlite_error(source))?;
            let mut memory_ids = Vec::new();
            for row in rows {
                memory_ids.push(row.map_err(|source| self.sqlite_error(source))?);
            }
            memory_ids
        };
        for memory_id in existing_memory_ids {
            if !current_memory_ids.contains(&memory_id) {
                transaction
                    .execute(
                        "DELETE FROM memory_records WHERE memory_id = ?1",
                        params![memory_id],
                    )
                    .map_err(|source| self.sqlite_error(source))?;
            }
        }

        transaction
            .commit()
            .map_err(|source| self.sqlite_error(source))?;
        Ok(())
    }

    pub fn save_snapshot_with_run_records(
        &self,
        os: &OperatingSystem,
    ) -> Result<(), SqliteStoreError> {
        self.save_snapshot_with_records(os)
    }

    pub fn load_snapshot(&self) -> Result<OperatingSystem, SqliteStoreError> {
        self.init()?;
        let connection = self.connection()?;
        let body: String = connection
            .query_row(
                "SELECT body FROM state_snapshots WHERE id = 'current'",
                [],
                |row| row.get(0),
            )
            .map_err(|source| self.sqlite_error(source))?;
        serde_json::from_str(&body).map_err(|source| SqliteStoreError::Json {
            path: self.path.clone(),
            source,
        })
    }

    pub fn import_json_store(&self, store: &Store) -> Result<SqliteImportReport, SqliteStoreError> {
        let os = store.load()?;
        self.save_snapshot_with_records(&os)?;
        let mut report = SqliteImportReport {
            imported_snapshot: true,
            imported_runs: os.runs.len(),
            ..SqliteImportReport::default()
        };
        for run in os.runs.values() {
            self.save_run_record(run)?;
            let Some(path) = &run.log_path else {
                report.skipped_run_logs += 1;
                continue;
            };
            let Ok(body) = std::fs::read_to_string(path) else {
                report.skipped_run_logs += 1;
                continue;
            };
            self.save_run_log(&run.id, &body)?;
            report.imported_run_logs += 1;
        }
        Ok(report)
    }

    pub fn restore_json_store(
        &self,
        store: &Store,
        force: bool,
        dry_run: bool,
    ) -> Result<SqliteRestoreReport, SqliteStoreError> {
        let os = self.load_snapshot()?;
        let validation = validate_state(&os);
        if !validation.valid {
            return Err(StoreError::InvalidState {
                path: self.path.clone(),
                issues: validation.issues.join("; "),
            }
            .into());
        }
        if dry_run {
            if store.exists() && !force {
                return Err(StoreError::AlreadyExists {
                    path: store.path().to_path_buf(),
                }
                .into());
            }
        } else {
            store.save_validated_checked(&os, force)?;
        }
        Ok(SqliteRestoreReport {
            restored_snapshot: !dry_run,
            restored_runs: os.runs.len(),
            validation,
        })
    }

    pub fn save_run_log(&self, run_id: &RunId, body: &str) -> Result<(), SqliteStoreError> {
        self.init()?;
        let connection = self.connection()?;
        connection
            .execute(
                "
                INSERT INTO run_logs (run_id, body, updated_at)
                VALUES (?1, ?2, ?3)
                ON CONFLICT(run_id) DO UPDATE SET body = excluded.body, updated_at = excluded.updated_at
                ",
                params![run_id.to_string(), body, Utc::now().to_rfc3339()],
            )
            .map_err(|source| self.sqlite_error(source))?;
        Ok(())
    }

    pub fn save_run_record(&self, run: &RunRecord) -> Result<(), SqliteStoreError> {
        self.init()?;
        let body = serde_json::to_string_pretty(run).map_err(|source| SqliteStoreError::Json {
            path: self.path.clone(),
            source,
        })?;
        let connection = self.connection()?;
        connection
            .execute(
                "
                INSERT INTO run_records (run_id, body, updated_at)
                VALUES (?1, ?2, ?3)
                ON CONFLICT(run_id) DO UPDATE SET body = excluded.body, updated_at = excluded.updated_at
                ",
                params![run.id.to_string(), body, Utc::now().to_rfc3339()],
            )
            .map_err(|source| self.sqlite_error(source))?;
        Ok(())
    }

    pub fn load_run_record(&self, run_id: &RunId) -> Result<RunRecord, SqliteStoreError> {
        self.init()?;
        let connection = self.connection()?;
        let body: String = connection
            .query_row(
                "SELECT body FROM run_records WHERE run_id = ?1",
                params![run_id.to_string()],
                |row| row.get(0),
            )
            .map_err(|source| self.sqlite_error(source))?;
        serde_json::from_str(&body).map_err(|source| SqliteStoreError::Json {
            path: self.path.clone(),
            source,
        })
    }

    pub fn load_task_record(&self, task_id: &TaskId) -> Result<Task, SqliteStoreError> {
        self.init()?;
        let connection = self.connection()?;
        let body: String = connection
            .query_row(
                "SELECT body FROM task_records WHERE task_id = ?1",
                params![task_id.to_string()],
                |row| row.get(0),
            )
            .map_err(|source| self.sqlite_error(source))?;
        serde_json::from_str(&body).map_err(|source| SqliteStoreError::Json {
            path: self.path.clone(),
            source,
        })
    }

    pub fn load_event_record(&self, event_id: &str) -> Result<Event, SqliteStoreError> {
        self.init()?;
        let connection = self.connection()?;
        let body: String = connection
            .query_row(
                "SELECT body FROM event_records WHERE event_id = ?1",
                params![event_id],
                |row| row.get(0),
            )
            .map_err(|source| self.sqlite_error(source))?;
        serde_json::from_str(&body).map_err(|source| SqliteStoreError::Json {
            path: self.path.clone(),
            source,
        })
    }

    pub fn load_memory_record(&self, memory_id: &str) -> Result<MemoryRecord, SqliteStoreError> {
        self.init()?;
        let connection = self.connection()?;
        let body: String = connection
            .query_row(
                "SELECT body FROM memory_records WHERE memory_id = ?1",
                params![memory_id],
                |row| row.get(0),
            )
            .map_err(|source| self.sqlite_error(source))?;
        serde_json::from_str(&body).map_err(|source| SqliteStoreError::Json {
            path: self.path.clone(),
            source,
        })
    }

    pub fn load_run_log(&self, run_id: &RunId) -> Result<String, SqliteStoreError> {
        self.init()?;
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT body FROM run_logs WHERE run_id = ?1",
                params![run_id.to_string()],
                |row| row.get(0),
            )
            .map_err(|source| self.sqlite_error(source))
    }

    pub fn run_log_count(&self) -> Result<usize, SqliteStoreError> {
        self.init()?;
        let connection = self.connection()?;
        connection
            .query_row("SELECT COUNT(*) FROM run_logs", [], |row| row.get(0))
            .map_err(|source| self.sqlite_error(source))
    }

    pub fn run_record_count(&self) -> Result<usize, SqliteStoreError> {
        self.init()?;
        let connection = self.connection()?;
        connection
            .query_row("SELECT COUNT(*) FROM run_records", [], |row| row.get(0))
            .map_err(|source| self.sqlite_error(source))
    }

    pub fn task_record_count(&self) -> Result<usize, SqliteStoreError> {
        self.init()?;
        let connection = self.connection()?;
        connection
            .query_row("SELECT COUNT(*) FROM task_records", [], |row| row.get(0))
            .map_err(|source| self.sqlite_error(source))
    }

    pub fn event_record_count(&self) -> Result<usize, SqliteStoreError> {
        self.init()?;
        let connection = self.connection()?;
        connection
            .query_row("SELECT COUNT(*) FROM event_records", [], |row| row.get(0))
            .map_err(|source| self.sqlite_error(source))
    }

    pub fn memory_record_count(&self) -> Result<usize, SqliteStoreError> {
        self.init()?;
        let connection = self.connection()?;
        connection
            .query_row("SELECT COUNT(*) FROM memory_records", [], |row| row.get(0))
            .map_err(|source| self.sqlite_error(source))
    }

    fn connection(&self) -> Result<Connection, SqliteStoreError> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| StoreError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        Connection::open(&self.path).map_err(|source| self.sqlite_error(source))
    }

    fn ensure_column(
        &self,
        connection: &Connection,
        table: &str,
        column: &str,
        alter_sql: &str,
    ) -> Result<(), SqliteStoreError> {
        let mut statement = connection
            .prepare(&format!("PRAGMA table_info({table})"))
            .map_err(|source| self.sqlite_error(source))?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|source| self.sqlite_error(source))?;
        for row in rows {
            if row.map_err(|source| self.sqlite_error(source))? == column {
                return Ok(());
            }
        }
        connection
            .execute(alter_sql, [])
            .map_err(|source| self.sqlite_error(source))?;
        Ok(())
    }

    fn sqlite_error(&self, source: rusqlite::Error) -> SqliteStoreError {
        SqliteStoreError::Sqlite {
            path: self.path.clone(),
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        EventKind, MemoryRecord, MemoryVisibility, OperatingSystem, Priority, RunRecord, RunStatus,
        Task,
    };

    #[test]
    fn imports_state_snapshot_and_available_run_logs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let sqlite = SqliteStore::new(dir.path().join("state.sqlite"));
        let mut os = OperatingSystem::new("sqlite-test");
        let mut task = Task::new("Run", "sync log", Priority::Normal, vec![]);
        task.attempts = 2;
        task.max_attempts = 3;
        let task_id = task.id.clone();
        os.create_task(task);
        os.record(EventKind::DaemonTick, "sqlite import event");
        let event_id = os.events.last().expect("event").id.clone();
        os.write_memory(MemoryRecord::with_access(
            "sqlite-memory",
            "sync memory",
            vec!["sqlite".into()],
            MemoryVisibility::Private,
            Some("local".into()),
        ));
        let memory_id = os.memory.last().expect("memory").id.clone();
        let expected_event_count = os.events.len();

        let mut logged_run = RunRecord::new(task_id.clone(), None, "printf ok", ".");
        logged_run.status = RunStatus::Success;
        let logged_run_id = logged_run.id.clone();
        let logged_path = store
            .write_run_log(&logged_run_id, "available log")
            .expect("write log");
        logged_run.log_path = Some(logged_path.display().to_string());
        os.runs.insert(logged_run_id.clone(), logged_run);

        let mut missing_log_run = RunRecord::new(task_id.clone(), None, "printf missing", ".");
        missing_log_run.status = RunStatus::Failed;
        let missing_run_id = missing_log_run.id.clone();
        missing_log_run.log_path = Some(dir.path().join("missing.log").display().to_string());
        os.runs.insert(missing_run_id, missing_log_run);
        store.save_unchecked(&os).expect("save state");

        let report = sqlite.import_json_store(&store).expect("sqlite import");

        assert_eq!(
            report,
            SqliteImportReport {
                imported_snapshot: true,
                imported_runs: 2,
                imported_run_logs: 1,
                skipped_run_logs: 1,
            }
        );
        assert_eq!(
            sqlite.load_snapshot().expect("snapshot").name,
            "sqlite-test"
        );
        assert_eq!(sqlite.run_record_count().expect("run count"), 2);
        assert_eq!(sqlite.task_record_count().expect("task count"), 1);
        assert_eq!(sqlite.memory_record_count().expect("memory count"), 1);
        assert_eq!(
            sqlite.event_record_count().expect("event count"),
            expected_event_count
        );
        assert_eq!(
            sqlite
                .load_task_record(&task_id)
                .expect("task record")
                .title,
            "Run"
        );
        let connection = sqlite.connection().expect("sqlite connection");
        let task_attempts: (i64, i64) = connection
            .query_row(
                "SELECT attempts, max_attempts FROM task_records WHERE task_id = ?1",
                [task_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("query task attempts");
        assert_eq!(task_attempts, (2, 3));
        assert_eq!(
            sqlite
                .load_event_record(&event_id)
                .expect("event record")
                .message,
            "sqlite import event"
        );
        let memory = sqlite
            .load_memory_record(&memory_id)
            .expect("memory record");
        assert_eq!(memory.topic, "sqlite-memory");
        assert_eq!(memory.visibility, MemoryVisibility::Private);
        assert_eq!(memory.scope.as_deref(), Some("local"));
        assert_eq!(
            sqlite
                .load_run_record(&logged_run_id)
                .expect("logged run record")
                .command,
            "printf ok"
        );
        assert_eq!(sqlite.run_log_count().expect("log count"), 1);
        assert_eq!(
            sqlite.load_run_log(&logged_run_id).expect("run log"),
            "available log"
        );
    }

    #[test]
    fn init_adds_task_attempt_columns_to_existing_sqlite_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sqlite = SqliteStore::new(dir.path().join("state.sqlite"));
        let connection = Connection::open(sqlite.path()).expect("open sqlite");
        connection
            .execute_batch(
                "
                CREATE TABLE task_records (
                    task_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    status TEXT NOT NULL,
                    priority TEXT NOT NULL,
                    assigned_to TEXT,
                    body TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                ",
            )
            .expect("create old task records");

        sqlite.init().expect("migrate sqlite schema");

        let columns = {
            let connection = Connection::open(sqlite.path()).expect("open migrated sqlite");
            let mut statement = connection
                .prepare("PRAGMA table_info(task_records)")
                .expect("task table info");
            let rows = statement
                .query_map([], |row| row.get::<_, String>(1))
                .expect("query task columns");
            rows.collect::<Result<BTreeSet<_>, _>>()
                .expect("collect task columns")
        };
        assert!(columns.contains("attempts"));
        assert!(columns.contains("max_attempts"));
    }

    #[test]
    fn mirror_upserts_skip_unchanged_record_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sqlite = SqliteStore::new(dir.path().join("state.sqlite"));
        let mut os = OperatingSystem::new("sqlite-upsert");
        let task = Task::new("Mirror task", "keep row stable", Priority::Normal, vec![]);
        let task_id = task.id.clone();
        os.create_task(task);
        os.record(EventKind::DaemonTick, "stable event");
        let event_id = os.events.last().expect("event").id.clone();
        os.write_memory(MemoryRecord::new("stable memory", "body", vec![]));
        let memory_id = os.memory.last().expect("memory").id.clone();
        let mut run = RunRecord::new(task_id.clone(), None, "printf stable", ".");
        run.status = RunStatus::Success;
        let run_id = run.id.clone();
        os.runs.insert(run_id.clone(), run);

        sqlite.save_snapshot_with_records(&os).expect("first save");
        let first_task_updated =
            sqlite_row_updated_at(sqlite.path(), "task_records", "task_id", task_id.as_str());
        let first_event_updated =
            sqlite_row_updated_at(sqlite.path(), "event_records", "event_id", &event_id);
        let first_memory_updated =
            sqlite_row_updated_at(sqlite.path(), "memory_records", "memory_id", &memory_id);
        let first_run_updated =
            sqlite_row_updated_at(sqlite.path(), "run_records", "run_id", run_id.as_str());

        sqlite
            .save_snapshot_with_records(&os)
            .expect("unchanged second save");

        assert_eq!(
            sqlite_row_updated_at(sqlite.path(), "task_records", "task_id", task_id.as_str()),
            first_task_updated
        );
        assert_eq!(
            sqlite_row_updated_at(sqlite.path(), "event_records", "event_id", &event_id),
            first_event_updated
        );
        assert_eq!(
            sqlite_row_updated_at(sqlite.path(), "memory_records", "memory_id", &memory_id),
            first_memory_updated
        );
        assert_eq!(
            sqlite_row_updated_at(sqlite.path(), "run_records", "run_id", run_id.as_str()),
            first_run_updated
        );

        let mut changed = os.clone();
        changed.tasks.get_mut(&task_id).expect("task").title = "Changed mirror task".into();
        sqlite
            .save_snapshot_with_records(&changed)
            .expect("changed save");

        assert_ne!(
            sqlite_row_updated_at(sqlite.path(), "task_records", "task_id", task_id.as_str()),
            first_task_updated
        );
        assert_eq!(
            sqlite_row_updated_at(sqlite.path(), "event_records", "event_id", &event_id),
            first_event_updated
        );
    }

    #[test]
    fn restores_valid_snapshot_to_json_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = Store::new(dir.path().join("source.json"));
        let target = Store::new(dir.path().join("target.json"));
        let sqlite = SqliteStore::new(dir.path().join("state.sqlite"));
        let mut os = OperatingSystem::new("sqlite-restore");
        os.create_task(Task::new(
            "Restore me",
            "from sqlite",
            Priority::Normal,
            vec![],
        ));
        source.save_unchecked(&os).expect("save source");
        sqlite.import_json_store(&source).expect("import sqlite");

        let preview = sqlite
            .restore_json_store(&target, false, true)
            .expect("preview restore");
        assert_eq!(preview.restored_snapshot, false);
        assert_eq!(preview.restored_runs, 0);
        assert!(preview.validation.valid);
        assert!(!target.exists());

        let report = sqlite
            .restore_json_store(&target, false, false)
            .expect("restore sqlite");
        assert_eq!(report.restored_snapshot, true);
        assert!(report.validation.valid);
        let restored = target.load().expect("load target");
        assert_eq!(restored.name, "sqlite-restore");
        assert_eq!(restored.tasks.len(), 1);
    }

    fn sqlite_row_updated_at(path: &Path, table: &str, id_column: &str, id: &str) -> String {
        let connection = Connection::open(path).expect("open sqlite");
        let sql = format!("SELECT updated_at FROM {table} WHERE {id_column} = ?1");
        connection
            .query_row(&sql, [id], |row| row.get::<_, String>(0))
            .expect("row updated_at")
    }
}
