use std::fs;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub(crate) struct DurableWriteError {
    pub(crate) path: PathBuf,
    pub(crate) source: io::Error,
}

pub(crate) fn write_file_atomic_creating_parent(
    path: &Path,
    body: &[u8],
) -> Result<(), DurableWriteError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|source| DurableWriteError {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    write_file_atomic(path, body)
}

fn write_file_atomic(path: &Path, body: &[u8]) -> Result<(), DurableWriteError> {
    let temp_path = unique_temp_path(path);
    let mut temp_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .map_err(|source| DurableWriteError {
            path: temp_path.clone(),
            source,
        })?;
    if let Err(source) = temp_file.write_all(body) {
        let _ = fs::remove_file(&temp_path);
        return Err(DurableWriteError {
            path: temp_path,
            source,
        });
    }
    if let Err(source) = temp_file.sync_all() {
        let _ = fs::remove_file(&temp_path);
        return Err(DurableWriteError {
            path: temp_path,
            source,
        });
    }
    drop(temp_file);
    if let Err(source) = fs::rename(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(DurableWriteError {
            path: path.to_path_buf(),
            source,
        });
    }
    sync_parent_dir(path).map_err(|source| DurableWriteError {
        path: parent_dir_for_sync(path).to_path_buf(),
        source,
    })?;
    Ok(())
}

fn sync_parent_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        OpenOptions::new()
            .read(true)
            .open(parent_dir_for_sync(path))?
            .sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

fn parent_dir_for_sync(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn unique_temp_path(path: &Path) -> PathBuf {
    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "durable".into());
    let process_id = std::process::id();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);

    parent.join(format!(".{file_name}.{process_id}.{nanos}.{counter}.tmp"))
}
