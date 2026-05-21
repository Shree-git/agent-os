use std::io;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub fn terminate_child_process_tree(child: &mut Child, process_isolation: bool) -> io::Result<()> {
    #[cfg(unix)]
    {
        if process_isolation {
            terminate_unix_process_group(child.id());
        } else {
            terminate_unix_process_tree(child);
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        let _ = process_isolation;
        terminate_windows_process_tree(child)
    }
    #[cfg(all(not(unix), not(windows)))]
    {
        let _ = process_isolation;
        kill_child_directly(child)
    }
}

#[cfg(unix)]
pub(crate) fn terminate_unix_process_group(root: u32) {
    send_signal_to_process_group("TERM", root);

    let grace_deadline = Instant::now() + Duration::from_millis(200);
    while Instant::now() < grace_deadline {
        let mut pids = collect_process_group_pids(root);
        pids.retain(|pid| *pid != std::process::id());
        if pids.is_empty() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }

    send_signal_to_process_group("KILL", root);
}

#[cfg(unix)]
pub(crate) fn terminate_unix_process_tree(child: &mut Child) {
    let root = child.id();
    let mut pids = collect_descendant_pids(root);
    pids.push(root);
    pids.sort_unstable();
    pids.dedup();
    send_signal_to_pids("TERM", &pids);

    let grace_deadline = Instant::now() + Duration::from_millis(200);
    while Instant::now() < grace_deadline {
        if pids.iter().all(|pid| !process_exists(*pid)) {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }

    send_signal_to_pids("KILL", &pids);
}

#[cfg(unix)]
pub(crate) fn collect_process_group_pids(pgid: u32) -> Vec<u32> {
    let output = Command::new("pgrep")
        .arg("-g")
        .arg(pgid.to_string())
        .output();
    let Ok(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.trim().parse::<u32>().ok())
        .collect()
}

#[cfg(unix)]
pub(crate) fn collect_descendant_pids(root: u32) -> Vec<u32> {
    let output = Command::new("pgrep")
        .arg("-P")
        .arg(root.to_string())
        .output();
    let Ok(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    let children = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.trim().parse::<u32>().ok())
        .collect::<Vec<_>>();
    let mut descendants = children.clone();
    for child in children {
        descendants.extend(collect_descendant_pids(child));
    }
    descendants
}

#[cfg(unix)]
fn send_signal_to_process_group(signal: &str, pgid: u32) {
    let _ = Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(format!("-{pgid}"))
        .stderr(Stdio::null())
        .status();
}

#[cfg(unix)]
fn send_signal_to_pids(signal: &str, pids: &[u32]) {
    for pid in pids {
        let _ = Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(pid.to_string())
            .stderr(Stdio::null())
            .status();
    }
}

#[cfg(unix)]
pub(crate) fn process_exists(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[cfg(windows)]
fn terminate_windows_process_tree(child: &mut Child) -> io::Result<()> {
    let status = Command::new("taskkill")
        .args(windows_taskkill_args(child.id()))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    match status {
        Ok(status) if status.success() => Ok(()),
        Ok(_) | Err(_) => kill_child_directly(child),
    }
}

#[cfg(any(windows, test))]
fn windows_taskkill_args(pid: u32) -> [String; 4] {
    ["/PID".into(), pid.to_string(), "/T".into(), "/F".into()]
}

#[cfg(any(windows, all(not(unix), not(windows))))]
fn kill_child_directly(child: &mut Child) -> io::Result<()> {
    match child.kill() {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_taskkill_args_request_full_process_tree() {
        assert_eq!(windows_taskkill_args(42), ["/PID", "42", "/T", "/F"]);
    }
}
