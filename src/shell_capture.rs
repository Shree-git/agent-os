use crate::process_tree::terminate_child_process_tree;
use std::io::Read;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub struct ShellCaptureOutput {
    pub status_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub timed_out: bool,
}

pub fn run_shell_capture(
    command_text: &str,
    cwd: &Path,
    env: &[(String, String)],
    timeout: Duration,
    process_isolation: bool,
    max_output_bytes: usize,
) -> std::io::Result<ShellCaptureOutput> {
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(command_text)
        .current_dir(cwd)
        .env_clear()
        .envs(env.iter().map(|(key, value)| (key, value)))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    if process_isolation {
        command.process_group(0);
    }

    let mut child = command.spawn()?;
    let stdout_reader = child
        .stdout
        .take()
        .map(|stdout| thread::spawn(move || read_capped(stdout, max_output_bytes)));
    let stderr_reader = child
        .stderr
        .take()
        .map(|stderr| thread::spawn(move || read_capped(stderr, max_output_bytes)));

    let started = Instant::now();
    let mut timed_out = false;
    loop {
        if child.try_wait()?.is_some() {
            break;
        }
        if started.elapsed() >= timeout {
            timed_out = true;
            terminate_child_process_tree(&mut child, process_isolation)?;
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }

    let status = child.wait()?;
    let stdout = join_reader(stdout_reader)?;
    let stderr = join_reader(stderr_reader)?;
    Ok(ShellCaptureOutput {
        status_code: if timed_out { None } else { status.code() },
        stdout,
        stderr,
        timed_out,
    })
}

fn read_capped<R>(mut reader: R, max_output_bytes: usize) -> std::io::Result<Vec<u8>>
where
    R: Read,
{
    let mut output = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        if output.len() < max_output_bytes {
            let remaining = max_output_bytes.saturating_sub(output.len());
            output.extend_from_slice(&buffer[..read.min(remaining)]);
        }
    }
    Ok(output)
}

fn join_reader(
    handle: Option<thread::JoinHandle<std::io::Result<Vec<u8>>>>,
) -> std::io::Result<Vec<u8>> {
    match handle {
        Some(handle) => match handle.join() {
            Ok(result) => result,
            Err(_) => Err(std::io::Error::other("eval output reader panicked")),
        },
        None => Ok(Vec::new()),
    }
}
