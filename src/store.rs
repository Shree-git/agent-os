use crate::durable_io;
use crate::migrations::{MigrationError, MigrationReport, migrate_state_value};
use crate::models::{OperatingSystem, RunId, RunStatus};
use crate::validation::{RepairReport, ValidationReport, repair_state, validate_state};
use chrono::Utc;
use directories::ProjectDirs;
use fs2::FileExt;
use serde::Serialize;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("could not determine a state directory; set AGENT_OS_HOME")]
    MissingHome,
    #[error("AGENT_OS_HOME must not be empty")]
    EmptyHome,
    #[error("state already exists at {path}; pass --force to replace it")]
    AlreadyExists { path: PathBuf },
    #[error("io error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("invalid state file at {path}: {source}")]
    Json {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("state migration failed at {path}: {source}")]
    Migration {
        path: PathBuf,
        source: MigrationError,
    },
    #[error("state validation failed at {path}: {issues}")]
    InvalidState { path: PathBuf, issues: String },
}

#[derive(Clone, Debug, Serialize)]
pub struct PruneReport {
    pub dry_run: bool,
    pub removed_runs: Vec<RunId>,
    pub removed_log_paths: Vec<String>,
    pub removed_events: usize,
}

#[derive(Clone, Debug)]
pub struct Store {
    path: PathBuf,
}

struct StoreLock {
    file: File,
}

impl Drop for StoreLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

impl Store {
    pub fn from_environment() -> Result<Self, StoreError> {
        if let Ok(home) = std::env::var("AGENT_OS_HOME") {
            if home.trim().is_empty() {
                return Err(StoreError::EmptyHome);
            }
            return Ok(Self::new(PathBuf::from(home).join("state.json")));
        }

        let dirs =
            ProjectDirs::from("com", "infinite-apps", "agent-os").ok_or(StoreError::MissingHome)?;
        Ok(Self::new(dirs.data_local_dir().join("state.json")))
    }

    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn default_backup_path(&self) -> PathBuf {
        let timestamp = Utc::now().format("%Y%m%d%H%M%S");
        self.path.with_extension(format!("backup-{timestamp}.json"))
    }

    pub fn run_log_path(&self, run_id: &RunId) -> PathBuf {
        self.path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("runs")
            .join(format!("{run_id}.log"))
    }

    fn lock_path(&self) -> PathBuf {
        self.path.with_extension("lock")
    }

    pub fn exists(&self) -> bool {
        self.path.exists()
    }

    pub fn load(&self) -> Result<OperatingSystem, StoreError> {
        let _lock = self.lock_shared()?;
        self.load_unlocked()
    }

    fn load_unlocked(&self) -> Result<OperatingSystem, StoreError> {
        let body = fs::read_to_string(&self.path).map_err(|source| StoreError::Io {
            path: self.path.clone(),
            source,
        })?;
        Self::parse_state(&body, &self.path).map(|(os, _)| os)
    }

    pub fn load_or_create(&self, name: impl Into<String>) -> Result<OperatingSystem, StoreError> {
        if self.exists() {
            self.load()
        } else {
            Ok(OperatingSystem::new(name))
        }
    }

    pub fn save(&self, os: &OperatingSystem) -> Result<(), StoreError> {
        self.validate_for_save(os)?;
        self.save_unchecked(os)
    }

    pub(crate) fn save_unchecked(&self, os: &OperatingSystem) -> Result<(), StoreError> {
        let _lock = self.lock_exclusive()?;
        self.save_unlocked(os)
    }

    pub fn save_validated(&self, os: &OperatingSystem) -> Result<(), StoreError> {
        self.save(os)
    }

    pub fn save_validated_checked(
        &self,
        os: &OperatingSystem,
        force: bool,
    ) -> Result<(), StoreError> {
        let _lock = self.lock_exclusive()?;
        if self.path.exists() && !force {
            return Err(StoreError::AlreadyExists {
                path: self.path.clone(),
            });
        }
        self.validate_for_save(os)?;
        self.save_unlocked(os)
    }

    pub fn save_to_path_validated(path: &Path, os: &OperatingSystem) -> Result<(), StoreError> {
        Self::validate_path_for_save(path, os)?;
        Self::save_to_path(path, os)
    }

    pub fn write_file_atomic(path: &Path, body: &[u8]) -> Result<(), StoreError> {
        write_file_atomic_creating_parent(path, body)
    }

    pub fn validate_for_save(&self, os: &OperatingSystem) -> Result<(), StoreError> {
        Self::validate_path_for_save(&self.path, os)
    }

    fn validate_path_for_save(path: &Path, os: &OperatingSystem) -> Result<(), StoreError> {
        let report = validate_state(os);
        if !report.valid {
            return Err(StoreError::InvalidState {
                path: path.to_path_buf(),
                issues: report.issues.join("; "),
            });
        }
        Ok(())
    }

    pub fn export_json(&self) -> Result<String, StoreError> {
        let _lock = self.lock_shared()?;
        fs::read_to_string(&self.path).map_err(|source| StoreError::Io {
            path: self.path.clone(),
            source,
        })
    }

    pub fn export_to_path(&self, destination_path: &Path) -> Result<PathBuf, StoreError> {
        if path_targets_same_file(&self.path, destination_path) {
            return Err(StoreError::Io {
                path: destination_path.to_path_buf(),
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "export destination must differ from state path",
                ),
            });
        }
        let _lock = self.lock_shared()?;
        let body = fs::read(&self.path).map_err(|source| StoreError::Io {
            path: self.path.clone(),
            source,
        })?;
        write_file_atomic_creating_parent(destination_path, &body)?;
        Ok(destination_path.to_path_buf())
    }

    pub fn preview_export_to_path(&self, destination_path: &Path) -> Result<PathBuf, StoreError> {
        if path_targets_same_file(&self.path, destination_path) {
            return Err(StoreError::Io {
                path: destination_path.to_path_buf(),
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "export destination must differ from state path",
                ),
            });
        }
        let _lock = self.lock_shared()?;
        fs::read(&self.path).map_err(|source| StoreError::Io {
            path: self.path.clone(),
            source,
        })?;
        Ok(destination_path.to_path_buf())
    }

    pub fn import_from_path(&self, source_path: &Path) -> Result<OperatingSystem, StoreError> {
        let os = Self::read_state_from_path(source_path)?;
        let report = validate_state(&os);
        if !report.valid {
            return Err(StoreError::InvalidState {
                path: source_path.to_path_buf(),
                issues: report.issues.join("; "),
            });
        }
        self.save(&os)?;
        Ok(os)
    }

    pub fn import_from_path_checked(
        &self,
        source_path: &Path,
        force: bool,
    ) -> Result<(OperatingSystem, crate::validation::ValidationReport), StoreError> {
        let _lock = self.lock_exclusive()?;
        if self.path.exists() && !force {
            return Err(StoreError::AlreadyExists {
                path: self.path.clone(),
            });
        }
        let os = Self::read_state_from_path(source_path)?;
        let report = validate_state(&os);
        if !report.valid {
            return Err(StoreError::InvalidState {
                path: source_path.to_path_buf(),
                issues: report.issues.join("; "),
            });
        }
        self.save_unlocked(&os)?;
        Ok((os, report))
    }

    pub fn preview_import_from_path_checked(
        &self,
        source_path: &Path,
        force: bool,
    ) -> Result<(OperatingSystem, crate::validation::ValidationReport), StoreError> {
        let _lock = self.lock_shared()?;
        if self.path.exists() && !force {
            return Err(StoreError::AlreadyExists {
                path: self.path.clone(),
            });
        }
        let os = Self::read_state_from_path(source_path)?;
        let report = validate_state(&os);
        if !report.valid {
            return Err(StoreError::InvalidState {
                path: source_path.to_path_buf(),
                issues: report.issues.join("; "),
            });
        }
        Ok((os, report))
    }

    pub fn repair_state(&self) -> Result<RepairReport, StoreError> {
        let _lock = self.lock_exclusive()?;
        let mut os = self.load_unlocked()?;
        let mut report = repair_state(&mut os);
        if report.changed && report.validation.valid {
            self.save_unlocked(&os)?;
            report.persisted = true;
        }
        Ok(report)
    }

    pub fn read_state_from_path(source_path: &Path) -> Result<OperatingSystem, StoreError> {
        let body = fs::read_to_string(source_path).map_err(|source| StoreError::Io {
            path: source_path.to_path_buf(),
            source,
        })?;
        Self::parse_state(&body, source_path).map(|(os, _)| os)
    }

    pub fn migrate_state_body(
        body: &str,
        path: &Path,
    ) -> Result<(String, crate::migrations::MigrationReport), StoreError> {
        let mut value =
            serde_json::from_str::<serde_json::Value>(body).map_err(|source| StoreError::Json {
                path: path.to_path_buf(),
                source,
            })?;
        let report = migrate_state_value(&mut value).map_err(|source| StoreError::Migration {
            path: path.to_path_buf(),
            source,
        })?;
        let migrated = serde_json::to_string_pretty(&value).map_err(|source| StoreError::Json {
            path: path.to_path_buf(),
            source,
        })?;
        Ok((migrated, report))
    }

    pub fn migrate_path_to(
        &self,
        input: &Path,
        output: &Path,
    ) -> Result<(MigrationReport, ValidationReport), StoreError> {
        if path_targets_same_file(&self.path, output) {
            let _lock = self.lock_exclusive()?;
            let body = fs::read_to_string(input).map_err(|source| StoreError::Io {
                path: input.to_path_buf(),
                source,
            })?;
            let (os, migration) = Self::parse_state(&body, input)?;
            let validation = validate_state(&os);
            if !validation.valid {
                return Err(StoreError::InvalidState {
                    path: input.to_path_buf(),
                    issues: validation.issues.join("; "),
                });
            }
            self.save_unlocked(&os)?;
            return Ok((migration, validation));
        }

        let body = fs::read_to_string(input).map_err(|source| StoreError::Io {
            path: input.to_path_buf(),
            source,
        })?;
        let (os, migration) = Self::parse_state(&body, input)?;
        let validation = validate_state(&os);
        if !validation.valid {
            return Err(StoreError::InvalidState {
                path: input.to_path_buf(),
                issues: validation.issues.join("; "),
            });
        }
        Self::save_to_path_validated(output, &os)?;
        Ok((migration, validation))
    }

    pub fn preview_migrate_path(
        &self,
        input: &Path,
    ) -> Result<(MigrationReport, ValidationReport), StoreError> {
        let _lock = if path_targets_same_file(&self.path, input) {
            Some(self.lock_shared()?)
        } else {
            None
        };
        let body = fs::read_to_string(input).map_err(|source| StoreError::Io {
            path: input.to_path_buf(),
            source,
        })?;
        let (os, migration) = Self::parse_state(&body, input)?;
        let validation = validate_state(&os);
        if !validation.valid {
            return Err(StoreError::InvalidState {
                path: input.to_path_buf(),
                issues: validation.issues.join("; "),
            });
        }
        Ok((migration, validation))
    }

    fn parse_state(
        body: &str,
        path: &Path,
    ) -> Result<(OperatingSystem, crate::migrations::MigrationReport), StoreError> {
        let (migrated, report) = Self::migrate_state_body(body, path)?;
        let os = serde_json::from_str::<OperatingSystem>(&migrated).map_err(|source| {
            StoreError::Json {
                path: path.to_path_buf(),
                source,
            }
        })?;
        Ok((os, report))
    }

    pub fn backup_to_path(&self, destination_path: &Path) -> Result<PathBuf, StoreError> {
        if path_targets_same_file(&self.path, destination_path) {
            return Err(StoreError::Io {
                path: destination_path.to_path_buf(),
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "backup destination must differ from state path",
                ),
            });
        }
        let _lock = self.lock_shared()?;
        let body = fs::read(&self.path).map_err(|source| StoreError::Io {
            path: self.path.clone(),
            source,
        })?;
        write_file_atomic_creating_parent(destination_path, &body)?;
        Ok(destination_path.to_path_buf())
    }

    pub fn preview_backup_to_path(&self, destination_path: &Path) -> Result<PathBuf, StoreError> {
        if path_targets_same_file(&self.path, destination_path) {
            return Err(StoreError::Io {
                path: destination_path.to_path_buf(),
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "backup destination must differ from state path",
                ),
            });
        }
        let _lock = self.lock_shared()?;
        fs::read(&self.path).map_err(|source| StoreError::Io {
            path: self.path.clone(),
            source,
        })?;
        Ok(destination_path.to_path_buf())
    }

    pub fn update<T, E, F>(&self, f: F) -> Result<T, E>
    where
        E: From<StoreError>,
        F: FnOnce(&mut OperatingSystem) -> Result<T, E>,
    {
        let _lock = self.lock_exclusive().map_err(E::from)?;
        let mut os = self.load_unlocked().map_err(E::from)?;
        let original = os.clone();
        let output = f(&mut os)?;
        let report = validate_state(&os);
        if !report.valid {
            return Err(E::from(StoreError::InvalidState {
                path: self.path.clone(),
                issues: report.issues.join("; "),
            }));
        }
        if os != original {
            self.save_unlocked(&os).map_err(E::from)?;
        }
        Ok(output)
    }

    fn save_unlocked(&self, os: &OperatingSystem) -> Result<(), StoreError> {
        Self::save_to_path(&self.path, os)
    }

    fn save_to_path(path: &Path, os: &OperatingSystem) -> Result<(), StoreError> {
        let body = serde_json::to_string_pretty(os).map_err(|source| StoreError::Json {
            path: path.to_path_buf(),
            source,
        })?;
        write_file_atomic_creating_parent(path, body.as_bytes())?;
        Ok(())
    }

    fn lock_shared(&self) -> Result<StoreLock, StoreError> {
        let file = self.open_lock_file()?;
        FileExt::lock_shared(&file).map_err(|source| StoreError::Io {
            path: self.lock_path(),
            source,
        })?;
        Ok(StoreLock { file })
    }

    fn lock_exclusive(&self) -> Result<StoreLock, StoreError> {
        let file = self.open_lock_file()?;
        FileExt::lock_exclusive(&file).map_err(|source| StoreError::Io {
            path: self.lock_path(),
            source,
        })?;
        Ok(StoreLock { file })
    }

    fn open_lock_file(&self) -> Result<File, StoreError> {
        let lock_path = self.lock_path();
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).map_err(|source| StoreError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|source| StoreError::Io {
                path: lock_path,
                source,
            })
    }

    pub fn write_run_log(&self, run_id: &RunId, body: &str) -> Result<PathBuf, StoreError> {
        let path = self.run_log_path(run_id);
        write_file_atomic_creating_parent(&path, body.as_bytes())?;
        Ok(path)
    }

    pub fn prune(
        &self,
        keep_runs: usize,
        keep_events: usize,
        dry_run: bool,
    ) -> Result<PruneReport, StoreError> {
        if dry_run {
            let os = self.load()?;
            return Ok(prune_report(self, &os, keep_runs, keep_events, true));
        }

        let report = self.update(|os| {
            let report = prune_report(self, os, keep_runs, keep_events, false);
            for run_id in &report.removed_runs {
                os.runs.remove(run_id);
            }
            if report.removed_events > 0 {
                os.events.drain(0..report.removed_events);
            }
            if !report.removed_runs.is_empty() || report.removed_events > 0 {
                os.touch();
            }
            Ok::<_, StoreError>(report)
        })?;

        for path in &report.removed_log_paths {
            let path = PathBuf::from(path);
            if path.exists() {
                fs::remove_file(&path).map_err(|source| StoreError::Io { path, source })?;
            }
        }

        Ok(report)
    }
}

fn prune_report(
    store: &Store,
    os: &OperatingSystem,
    keep_runs: usize,
    keep_events: usize,
    dry_run: bool,
) -> PruneReport {
    let mut removable_runs = os
        .runs
        .values()
        .filter(|run| !matches!(run.status, RunStatus::Running | RunStatus::CancelRequested))
        .cloned()
        .collect::<Vec<_>>();
    removable_runs.sort_by_key(|run| run.finished_at.unwrap_or(run.started_at));
    let remove_count = removable_runs.len().saturating_sub(keep_runs);
    let runs_to_remove = removable_runs
        .into_iter()
        .take(remove_count)
        .collect::<Vec<_>>();
    let removed_runs = runs_to_remove
        .iter()
        .map(|run| run.id.clone())
        .collect::<Vec<_>>();
    let removed_log_paths = runs_to_remove
        .iter()
        .map(|run| store.run_log_path(&run.id).display().to_string())
        .collect::<Vec<_>>();
    let removed_events = os.events.len().saturating_sub(keep_events);

    PruneReport {
        dry_run,
        removed_runs,
        removed_log_paths,
        removed_events,
    }
}

#[cfg(test)]
fn temp_state_path(path: &Path) -> PathBuf {
    let mut extension = path
        .extension()
        .map(|extension| extension.to_os_string())
        .unwrap_or_default();
    if extension.is_empty() {
        extension.push("tmp");
    } else {
        extension.push(".tmp");
    }
    path.with_extension(extension)
}

fn path_targets_same_file(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }

    if normalize_path_lexically(left) == normalize_path_lexically(right) {
        return true;
    }

    if let (Ok(left), Ok(right)) = (fs::canonicalize(left), fs::canonicalize(right)) {
        return left == right;
    }

    let Some(right_parent) = right.parent() else {
        return false;
    };
    let Some(left_parent) = left.parent() else {
        return false;
    };
    let Some(right_name) = right.file_name() else {
        return false;
    };

    if left.file_name() != Some(right_name) {
        return false;
    }

    match (
        fs::canonicalize(left_parent),
        fs::canonicalize(right_parent),
    ) {
        (Ok(left_parent), Ok(right_parent)) => left_parent == right_parent,
        _ => false,
    }
}

fn normalize_path_lexically(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push(component.as_os_str());
                }
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn write_file_atomic(path: &Path, body: &[u8]) -> Result<(), StoreError> {
    durable_io::write_file_atomic_creating_parent(path, body).map_err(|error| StoreError::Io {
        path: error.path,
        source: error.source,
    })
}

fn write_file_atomic_creating_parent(path: &Path, body: &[u8]) -> Result<(), StoreError> {
    write_file_atomic(path, body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Agent, AgentKind, MemoryRecord, Priority, RunRecord, Task, TaskStatus};

    #[test]
    fn round_trips_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("test-os");
        os.register_agent(Agent::new(
            "Planner",
            AgentKind::Planner,
            Some("local".into()),
            vec!["plan".into()],
            2,
        ));

        store.save(&os).expect("save");
        let loaded = store.load().expect("load");

        assert_eq!(loaded.name, "test-os");
        assert_eq!(loaded.agents.len(), 1);
    }

    #[test]
    fn import_rejects_invalid_state_without_overwriting_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = Store::new(dir.path().join("target.json"));
        target
            .save(&OperatingSystem::new("target-os"))
            .expect("initial save");

        let source = dir.path().join("invalid.json");
        let mut invalid = OperatingSystem::new("source-os");
        invalid.name = " ".into();
        fs::write(
            &source,
            serde_json::to_string_pretty(&invalid).expect("json"),
        )
        .expect("write invalid source");

        let error = target
            .import_from_path(&source)
            .expect_err("invalid import");

        assert!(matches!(error, StoreError::InvalidState { .. }));
        assert_eq!(target.load().expect("load target").name, "target-os");
    }

    #[test]
    fn checked_import_rejects_existing_state_without_overwriting_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = Store::new(dir.path().join("target.json"));
        target
            .save(&OperatingSystem::new("target-os"))
            .expect("initial save");
        let source = dir.path().join("source.json");
        Store::save_to_path_validated(&source, &OperatingSystem::new("source-os"))
            .expect("source save");

        let error = target
            .import_from_path_checked(&source, false)
            .expect_err("existing target");

        assert!(matches!(error, StoreError::AlreadyExists { .. }));
        assert_eq!(target.load().expect("load target").name, "target-os");

        let (imported, report) = target
            .import_from_path_checked(&source, true)
            .expect("forced import");
        assert!(report.valid);
        assert_eq!(imported.name, "source-os");
        assert_eq!(target.load().expect("load target").name, "source-os");
    }

    #[test]
    fn migrate_path_to_updates_active_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let os = OperatingSystem::new("legacy-os");
        store.save(&os).expect("save state");
        let mut legacy = serde_json::to_value(&os).expect("state json");
        legacy.as_object_mut().expect("object").remove("version");
        fs::write(
            store.path(),
            serde_json::to_string_pretty(&legacy).expect("legacy json"),
        )
        .expect("write legacy state");

        let (migration, validation) = store
            .migrate_path_to(store.path(), store.path())
            .expect("migrate active");

        assert_eq!(migration.from_version, 0);
        assert!(migration.changed);
        assert!(validation.valid);
        assert_eq!(store.load().expect("load").version, 3);
        let persisted: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(store.path()).expect("state body"))
                .expect("persisted json");
        assert_eq!(persisted["version"], 3);
    }

    #[test]
    fn export_and_backup_write_atomically_without_leaving_temp_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        store.save(&OperatingSystem::new("test-os")).expect("save");

        let export_path = dir.path().join("nested").join("export.json");
        let backup_path = dir.path().join("nested").join("backup.json");
        store.export_to_path(&export_path).expect("export");
        store.backup_to_path(&backup_path).expect("backup");

        assert_eq!(
            Store::read_state_from_path(&export_path)
                .expect("export state")
                .name,
            "test-os"
        );
        assert_eq!(
            Store::read_state_from_path(&backup_path)
                .expect("backup state")
                .name,
            "test-os"
        );
        assert!(!temp_state_path(&export_path).exists());
        assert!(!temp_state_path(&backup_path).exists());
    }

    #[test]
    fn export_and_backup_reject_state_path_destination() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        store.save(&OperatingSystem::new("test-os")).expect("save");

        let export_error = store
            .export_to_path(store.path())
            .expect_err("export to state path");
        let backup_error = store
            .backup_to_path(store.path())
            .expect_err("backup to state path");
        let preview_export_error = store
            .preview_export_to_path(store.path())
            .expect_err("preview export to state path");
        let preview_backup_error = store
            .preview_backup_to_path(store.path())
            .expect_err("preview backup to state path");

        assert!(matches!(export_error, StoreError::Io { .. }));
        assert!(export_error.to_string().contains("must differ"));
        assert!(matches!(backup_error, StoreError::Io { .. }));
        assert!(backup_error.to_string().contains("must differ"));
        assert!(matches!(preview_export_error, StoreError::Io { .. }));
        assert!(preview_export_error.to_string().contains("must differ"));
        assert!(matches!(preview_backup_error, StoreError::Io { .. }));
        assert!(preview_backup_error.to_string().contains("must differ"));

        let canonical_error = store
            .export_to_path(&fs::canonicalize(store.path()).expect("canonical state path"))
            .expect_err("export to canonical state path");
        let normalized_state_path = dir.path().join("nested").join("..").join("state.json");
        let normalized_error = store
            .backup_to_path(&normalized_state_path)
            .expect_err("backup to normalized state path");
        let normalized_preview_error = store
            .preview_backup_to_path(&normalized_state_path)
            .expect_err("preview backup to normalized state path");

        assert!(matches!(canonical_error, StoreError::Io { .. }));
        assert!(canonical_error.to_string().contains("must differ"));
        assert!(matches!(normalized_error, StoreError::Io { .. }));
        assert!(normalized_error.to_string().contains("must differ"));
        assert!(matches!(normalized_preview_error, StoreError::Io { .. }));
        assert!(normalized_preview_error.to_string().contains("must differ"));
        assert_eq!(store.load().expect("load").name, "test-os");
    }

    #[test]
    fn update_serializes_concurrent_writers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        store
            .save(&OperatingSystem::new("test-os"))
            .expect("initial save");

        let mut handles = Vec::new();
        for index in 0..12 {
            let store = store.clone();
            handles.push(std::thread::spawn(move || {
                store
                    .update::<_, StoreError, _>(|os| {
                        os.write_memory(MemoryRecord::new(
                            format!("topic-{index}"),
                            "body",
                            vec!["concurrent".into()],
                        ));
                        Ok(())
                    })
                    .expect("update");
            }));
        }

        for handle in handles {
            handle.join().expect("join");
        }

        let loaded = store.load().expect("load");
        let concurrent_records = loaded
            .memory
            .iter()
            .filter(|record| record.tags.iter().any(|tag| tag == "concurrent"))
            .count();
        assert_eq!(concurrent_records, 12);
    }

    #[test]
    fn update_rejects_invalid_mutation_without_overwriting_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        store
            .save(&OperatingSystem::new("test-os"))
            .expect("initial save");

        let error = store
            .update::<_, StoreError, _>(|os| {
                os.name = " ".into();
                os.write_memory(MemoryRecord::new("bad mutation", "body", vec![]));
                Ok(())
            })
            .expect_err("invalid mutation");

        assert!(matches!(error, StoreError::InvalidState { .. }));
        let loaded = store.load().expect("load");
        assert_eq!(loaded.name, "test-os");
        assert!(
            !loaded
                .memory
                .iter()
                .any(|record| record.topic == "bad mutation")
        );
    }

    #[test]
    fn update_does_not_rewrite_unchanged_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let os = OperatingSystem::new("test-os");
        let minified = serde_json::to_string(&os).expect("json");
        fs::write(store.path(), &minified).expect("write minified state");

        store
            .update::<_, StoreError, _>(|_| Ok(()))
            .expect("no-op update");

        assert_eq!(
            fs::read_to_string(store.path()).expect("state body"),
            minified
        );
    }

    #[test]
    fn save_validated_rejects_invalid_state_without_overwriting_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        store
            .save(&OperatingSystem::new("test-os"))
            .expect("initial save");

        let mut invalid = OperatingSystem::new("invalid-os");
        invalid.name = " ".into();

        let error = store.save_validated(&invalid).expect_err("invalid save");

        assert!(matches!(error, StoreError::InvalidState { .. }));
        assert_eq!(store.load().expect("load").name, "test-os");
    }

    #[test]
    fn checked_save_rejects_existing_state_without_overwriting_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        store
            .save(&OperatingSystem::new("target-os"))
            .expect("initial save");

        let error = store
            .save_validated_checked(&OperatingSystem::new("replacement-os"), false)
            .expect_err("existing target");

        assert!(matches!(error, StoreError::AlreadyExists { .. }));
        assert_eq!(store.load().expect("load").name, "target-os");

        store
            .save_validated_checked(&OperatingSystem::new("replacement-os"), true)
            .expect("forced save");
        assert_eq!(store.load().expect("load").name, "replacement-os");
    }

    #[test]
    fn save_to_path_validated_rejects_invalid_state_without_overwriting_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("exported.json");
        Store::save_to_path_validated(&target, &OperatingSystem::new("target-os"))
            .expect("initial save");

        let mut invalid = OperatingSystem::new("invalid");
        invalid.name = " ".into();
        let error = Store::save_to_path_validated(&target, &invalid).expect_err("invalid save");

        assert!(matches!(error, StoreError::InvalidState { .. }));
        let loaded = Store::read_state_from_path(&target).expect("load");
        assert_eq!(loaded.name, "target-os");
    }

    #[test]
    fn repair_state_does_not_persist_when_remaining_issues_are_unrepairable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("test-os");
        let agent = Agent::new(
            "Builder",
            AgentKind::Builder,
            Some("local".into()),
            vec!["rust".into()],
            1,
        );
        let agent_id = agent.id.clone();
        os.register_agent(agent);
        os.agents
            .get_mut(&agent_id)
            .expect("agent")
            .max_parallel_tasks = 0;
        let mut task = Task::new("Running", "missing assignee", Priority::Normal, vec![]);
        task.status = TaskStatus::Running;
        os.create_task(task);
        store.save_unchecked(&os).expect("save invalid state");

        let report = store.repair_state().expect("repair report");

        assert!(report.changed);
        assert!(!report.validation.valid);
        assert!(
            report
                .repairs
                .iter()
                .any(|repair| repair.contains("max_parallel_tasks"))
        );
        let loaded = store.load().expect("load");
        assert_eq!(
            loaded
                .agents
                .get(&agent_id)
                .expect("agent")
                .max_parallel_tasks,
            0
        );
    }

    #[test]
    fn write_run_log_overwrites_atomically_without_leaving_temp_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let run_id = RunId::from_slug("run-log-test");

        let path = store.write_run_log(&run_id, "first").expect("first log");
        store.write_run_log(&run_id, "second").expect("second log");

        assert_eq!(fs::read_to_string(&path).expect("log"), "second");
        assert!(!temp_state_path(&path).exists());
    }

    #[test]
    fn prune_keeps_active_runs_and_their_logs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().join("state.json"));
        let mut os = OperatingSystem::new("test-os");
        let agent = Agent::new(
            "Builder",
            AgentKind::Builder,
            Some("local".into()),
            vec!["rust".into()],
            3,
        );
        let agent_id = agent.id.clone();
        os.register_agent(agent);

        let mut running_task = Task::new("Running", "active work", Priority::Normal, vec![]);
        running_task.status = TaskStatus::Running;
        running_task.assigned_to = Some(agent_id.clone());
        let running_task_id = running_task.id.clone();
        os.create_task(running_task);

        let mut cancel_requested_task = Task::new(
            "Cancelling",
            "active cancellation",
            Priority::Normal,
            vec![],
        );
        cancel_requested_task.status = TaskStatus::Running;
        cancel_requested_task.assigned_to = Some(agent_id.clone());
        let cancel_requested_task_id = cancel_requested_task.id.clone();
        os.create_task(cancel_requested_task);

        let mut finished_task = Task::new("Finished", "old work", Priority::Normal, vec![]);
        finished_task.status = TaskStatus::Complete;
        let finished_task_id = finished_task.id.clone();
        os.create_task(finished_task);

        let agent = os.agents.get_mut(&agent_id).expect("agent");
        agent.current_tasks.push(running_task_id.clone());
        agent.current_tasks.push(cancel_requested_task_id.clone());

        let running_run = RunRecord::new(running_task_id, Some(agent_id.clone()), "sleep 10", ".");
        let running_run_id = running_run.id.clone();
        let mut cancel_requested_run = RunRecord::new(
            cancel_requested_task_id,
            Some(agent_id.clone()),
            "sleep 20",
            ".",
        );
        cancel_requested_run.status = RunStatus::CancelRequested;
        let cancel_requested_run_id = cancel_requested_run.id.clone();
        let mut finished_run = RunRecord::new(finished_task_id, Some(agent_id), "printf done", ".");
        finished_run.status = RunStatus::Success;
        finished_run.exit_code = Some(0);
        finished_run.finished_at = Some(finished_run.started_at + chrono::Duration::seconds(1));
        let finished_run_id = finished_run.id.clone();

        os.runs.insert(running_run_id.clone(), running_run);
        os.runs
            .insert(cancel_requested_run_id.clone(), cancel_requested_run);
        os.runs.insert(finished_run_id.clone(), finished_run);
        store.save_validated(&os).expect("save");

        let running_log = store
            .write_run_log(&running_run_id, "running")
            .expect("running log");
        let cancel_requested_log = store
            .write_run_log(&cancel_requested_run_id, "cancel requested")
            .expect("cancel requested log");
        let finished_log = store
            .write_run_log(&finished_run_id, "finished")
            .expect("finished log");

        let report = store.prune(0, usize::MAX, false).expect("prune");

        assert_eq!(report.removed_runs, vec![finished_run_id.clone()]);
        assert_eq!(
            report.removed_log_paths,
            vec![finished_log.display().to_string()]
        );
        let loaded = store.load().expect("load");
        assert!(loaded.runs.contains_key(&running_run_id));
        assert!(loaded.runs.contains_key(&cancel_requested_run_id));
        assert!(!loaded.runs.contains_key(&finished_run_id));
        assert!(running_log.exists());
        assert!(cancel_requested_log.exists());
        assert!(!finished_log.exists());
    }

    #[test]
    fn atomic_write_preserves_existing_legacy_temp_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let destination = dir.path().join("state.json");
        let legacy_temp_path = temp_state_path(&destination);
        fs::write(&legacy_temp_path, "sentinel").expect("legacy temp");

        Store::write_file_atomic(&destination, b"state body").expect("write");

        assert_eq!(
            fs::read_to_string(&destination).expect("state"),
            "state body"
        );
        assert_eq!(
            fs::read_to_string(&legacy_temp_path).expect("legacy temp"),
            "sentinel"
        );
    }

    #[test]
    fn atomic_write_removes_temp_file_when_rename_fails() {
        let dir = tempfile::tempdir().expect("tempdir");
        let destination = dir.path().join("destination");
        fs::create_dir(&destination).expect("destination directory");

        let error = Store::write_file_atomic(&destination, b"body")
            .expect_err("rename over directory should fail");

        assert!(matches!(error, StoreError::Io { .. }));
        assert!(destination.is_dir());
        assert_eq!(fs::read_dir(dir.path()).expect("read dir").count(), 1);
    }
}
