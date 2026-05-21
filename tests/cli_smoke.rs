use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::Value;
use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::{Child, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn workspace_tempdir() -> std::io::Result<tempfile::TempDir> {
    let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp");
    std::fs::create_dir_all(&base)?;
    tempfile::Builder::new()
        .prefix("cli-smoke-")
        .tempdir_in(base)
}

#[cfg(unix)]
fn make_executable(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = std::fs::metadata(path)
        .expect("executable metadata")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(path, permissions).expect("chmod executable");
}

#[cfg(unix)]
fn make_private_file(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = std::fs::metadata(path)
        .expect("private file metadata")
        .permissions();
    permissions.set_mode(0o600);
    std::fs::set_permissions(path, permissions).expect("chmod private file");
}

#[cfg(not(unix))]
fn make_private_file(_path: &std::path::Path) {}

#[cfg(unix)]
fn make_group_readable_file(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = std::fs::metadata(path)
        .expect("group-readable file metadata")
        .permissions();
    permissions.set_mode(0o640);
    std::fs::set_permissions(path, permissions).expect("chmod group-readable file");
}

fn rewrite_memory_timestamp(state_dir: &std::path::Path, topic: &str, timestamp: &str) {
    let state_file = state_dir.join("state.json");
    let mut state: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("read state"))
            .expect("state json");
    let records = state["memory"].as_array_mut().expect("memory array");
    let record = records
        .iter_mut()
        .find(|record| record["topic"] == topic)
        .expect("memory record");
    record["created_at"] = serde_json::json!(timestamp);
    record["updated_at"] = serde_json::json!(timestamp);
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&state).expect("state json"),
    )
    .expect("write state");
}

fn inject_pending_approval(state_dir: &std::path::Path, task_id: &str, approval_id: &str) {
    let state_file = state_dir.join("state.json");
    let mut state: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("read state"))
            .expect("state json");
    let task = state["tasks"]
        .get_mut(task_id)
        .expect("approval task in state");
    task["status"] = serde_json::json!("blocked");
    task["assigned_to"] = Value::Null;
    task["output"] = Value::Null;
    let requested_at = task["updated_at"]
        .as_str()
        .expect("task updated_at")
        .to_owned();
    state["approvals"][approval_id] = serde_json::json!({
        "id": approval_id,
        "task_id": task_id,
        "run_id": null,
        "action": "git push origin main",
        "reason": "mcp approval smoke",
        "status": "pending",
        "requested_at": requested_at,
        "resolved_at": null,
        "resolved_by": null
    });
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&state).expect("state json"),
    )
    .expect("write state");
}

#[test]
fn init_status_and_schedule_task() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "init", "--force"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Initialized"))
        .stdout(predicate::str::contains("Next: agent-os --state"))
        .stdout(predicate::str::contains("Create a task:"))
        .stdout(predicate::str::contains("Preview scheduling:"));
    let initialized: Value = serde_json::from_str(
        &std::fs::read_to_string(std::path::Path::new(state).join("state.json"))
            .expect("read initialized state"),
    )
    .expect("initialized state json");
    assert_eq!(initialized["policy"]["allow_shell"], false);
    assert_eq!(
        initialized["policy"]["network"]["mode"],
        serde_json::json!("providers-only")
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "run", "--limit", "0"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("limit must be greater than 0"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "run", "--recover-stale-seconds=-1"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "recover_stale_seconds must be greater than or equal to 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "events", "--limit", "0"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "events limit must be greater than 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "events", "--kind", "system-booted"])
        .assert()
        .success()
        .stdout(predicate::str::contains("system-booted"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "events", "--query", "initialized"])
        .assert()
        .success()
        .stdout(predicate::str::contains("system-booted"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "events",
            "--since",
            "1970-01-01T00:00:00Z",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("system-booted"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "events",
            "--until",
            "2999-01-01T00:00:00Z",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("system-booted"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "events", "--kind", "magic"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid event kind `magic`"))
        .stderr(predicate::str::contains("expected one of:"))
        .stderr(predicate::str::contains("state-repaired"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "events", "--since", "not-a-time"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid event since timestamp"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "events", "--until", "not-a-time"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid event until timestamp"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "events", "--query", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains("event query must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "agent", "list", "--status", "online"])
        .assert()
        .success()
        .stdout(predicate::str::contains("builder"));

    let limited_agents = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "--json", "agent", "list", "--limit", "1"])
        .output()
        .expect("agent list");
    assert!(limited_agents.status.success(), "agent list failed");
    let limited_agents_body: Value =
        serde_json::from_slice(&limited_agents.stdout).expect("agent list json");
    assert_eq!(limited_agents_body.as_array().expect("agents").len(), 1);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "agent",
            "list",
            "--since",
            "1970-01-01T00:00:00Z",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("builder"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "agent",
            "list",
            "--until",
            "2999-01-01T00:00:00Z",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("builder"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state, "agent", "list", "--status", "online", "--kind", "builder", "--cap",
            "rust",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("builder"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "agent", "list", "--query", "build"])
        .assert()
        .success()
        .stdout(predicate::str::contains("builder"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "--json",
            "agent",
            "update",
            "builder",
            "--name",
            "Build Captain",
            "--model",
            "agent-model",
            "--cap",
            "rust,code,test",
            "--parallel",
            "3",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"id\": \"builder\""))
        .stdout(predicate::str::contains("Build Captain"))
        .stdout(predicate::str::contains("agent-model"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "--json",
            "agent",
            "update",
            "builder",
            "--clear-model",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"model\": null"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "agent", "update", "builder"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "agent update must include at least one field",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "agent", "list", "--kind", " "])
        .assert()
        .failure()
        .stderr(predicate::str::contains("agent kind must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "agent", "list", "--cap", ","])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "agent capability filter must not contain empty capabilities",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "agent", "list", "--limit", "0"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "agent list limit must be greater than 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "agent", "list", "--since", "not-a-time"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid agent since timestamp"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "agent", "list", "--until", "not-a-time"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid agent until timestamp"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "agent", "list", "--query", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains("agent query must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "agent", "list", "--status", "sleeping"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid agent status"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "tool", "list", "--kind", "shell"])
        .assert()
        .success()
        .stdout(predicate::str::contains("cargo-test"));

    let limited_tools = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "--json", "tool", "list", "--limit", "1"])
        .output()
        .expect("tool list");
    assert!(limited_tools.status.success(), "tool list failed");
    let limited_tools_body: Value =
        serde_json::from_slice(&limited_tools.stdout).expect("tool list json");
    assert_eq!(limited_tools_body.as_array().expect("tools").len(), 1);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "tool",
            "list",
            "--since",
            "1970-01-01T00:00:00Z",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("cargo-test"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "tool",
            "list",
            "--until",
            "2999-01-01T00:00:00Z",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("cargo-test"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state, "tool", "list", "--kind", "shell", "--cap", "rust",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("cargo-test"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "tool", "list", "--query", "cargo"])
        .assert()
        .success()
        .stdout(predicate::str::contains("cargo-test"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "tool", "list", "--cap", ","])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "tool capability filter must not contain empty capabilities",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "tool", "list", "--limit", "0"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "tool list limit must be greater than 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "tool", "list", "--since", "not-a-time"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid tool since timestamp"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "tool", "list", "--until", "not-a-time"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid tool until timestamp"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "tool", "list", "--query", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains("tool query must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "tool", "list", "--kind", "magic"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid tool kind"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "agent",
            "add",
            "zero-capacity",
            "--cap",
            "rust",
            "--parallel",
            "0",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("parallel must be greater than 0"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "agent", "add", "!!!", "--cap", "rust"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("agent name must contain"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "agent", "add", "empty-cap", "--cap", ""])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "agent capabilities must not contain empty capabilities",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "agent", "add", "builder", "--cap", "rust"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("agent already exists: builder"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "tool",
            "add",
            "cargo-test",
            "--command-template",
            "cargo test",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("tool already exists: cargo-test"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "create", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains("task title must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Blank objective",
            "--objective",
            "   ",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("task objective must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Blank command",
            "--command",
            "   ",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("task command must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Empty need",
            "--need",
            "",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "task required capabilities must not contain empty capabilities",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "recover",
            "--older-than-seconds=-1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "older_than_seconds must be greater than or equal to 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "tool",
            "add",
            "!!!",
            "--command-template",
            "printf hi",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("tool name must contain"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "tool",
            "add",
            "empty-template",
            "--command-template",
            "   ",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "tool command template must not be empty",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "tool",
            "add",
            "bad-placeholder",
            "--command-template",
            "printf { message }",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("malformed template placeholder"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "tool",
            "add",
            "invalid-placeholder-name",
            "--command-template",
            "printf {bad=key}",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("malformed template placeholder"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "tool",
            "add",
            "unclosed-placeholder",
            "--command-template",
            "printf {message",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unclosed template placeholder"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "tool",
            "add",
            "nested-placeholder",
            "--command-template",
            "printf {{message}}",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("malformed template placeholder"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "tool",
            "add",
            "empty-need",
            "--command-template",
            "printf hi",
            "--need",
            "",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "tool required capabilities must not contain empty capabilities",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Duplicate tool arg",
            "--tool",
            "cargo-test",
            "--arg",
            "message=one",
            "--arg",
            "message=two",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "duplicate tool argument key: message",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Padded tool arg",
            "--tool",
            "cargo-test",
            "--arg",
            " message=hello",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid tool argument key"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Unexpected tool arg",
            "--tool",
            "cargo-test",
            "--arg",
            "message=hello",
            "--arg",
            "unused=ignored",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "unexpected tool argument `message`",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Empty secret env",
            "--tool",
            "cargo-test",
            "--secret-arg",
            "token=",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "secret tool argument `token` must name an environment variable",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Padded secret arg key",
            "--tool",
            "cargo-test",
            "--secret-arg",
            " token=MESSAGE_ENV",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid tool argument key"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Invalid secret env",
            "--tool",
            "cargo-test",
            "--secret-arg",
            "token=BAD=ENV",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "secret tool argument `token` must name a valid environment variable",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Padded secret env",
            "--tool",
            "cargo-test",
            "--secret-arg",
            "token= TOKEN ",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "secret tool argument `token` must name a valid environment variable",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Overlapping tool arg",
            "--tool",
            "cargo-test",
            "--arg",
            "message=value",
            "--secret-arg",
            "message=MESSAGE_ENV",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "tool argument `message` cannot be both --arg and --secret-arg",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Arg without tool",
            "--arg",
            "message=value",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--arg requires --tool"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Secret arg without tool",
            "--secret-arg",
            "token=TOKEN_ENV",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--secret-arg requires --tool"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Write tests",
            "--need",
            "rust,test",
            "--priority",
            "high",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Created task"));

    let task_id = first_task_id(state);
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "plan", &task_id, "--step", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "plan steps must not contain empty steps",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state, "task", "complete", &task_id, "--note", "   ",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("note must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "run", "--dry-run", "--execute"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "--dry-run cannot be combined with --execute",
        ));

    let dry_run = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "--json", "run", "--dry-run"])
        .output()
        .expect("dry-run schedule");
    assert!(dry_run.status.success(), "dry-run schedule failed");
    let dry_run_body: Value = serde_json::from_slice(&dry_run.stdout).expect("dry-run json");
    assert_eq!(dry_run_body["dry_run"], true);
    assert_eq!(
        dry_run_body["scheduler"]["assignments"][0]["task_id"],
        task_id
    );
    assert_eq!(dry_run_body["runs"].as_array().expect("runs").len(), 0);
    assert_eq!(dry_run_body["errors"].as_array().expect("errors").len(), 0);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "list", "--status", "pending"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Write tests"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Assigned task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "list", "--status", "running"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Write tests"));

    let limited_tasks = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "--json", "task", "list", "--limit", "1"])
        .output()
        .expect("task list");
    assert!(limited_tasks.status.success(), "task list failed");
    let limited_tasks_body: Value =
        serde_json::from_slice(&limited_tasks.stdout).expect("task list json");
    assert_eq!(limited_tasks_body.as_array().expect("tasks").len(), 1);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "list",
            "--since",
            "1970-01-01T00:00:00Z",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Write tests"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "list",
            "--until",
            "2999-01-01T00:00:00Z",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Write tests"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "list", "--query", "write"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Write tests"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "list",
            "--status",
            "running",
            "--priority",
            "high",
            "--agent",
            "builder",
            "--cap",
            "test",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Write tests"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "list", "--agent", "!!!"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "agent id must contain at least one ASCII letter, digit, or hyphen",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "list", "--priority", "eventually"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid priority `eventually`"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "list", "--cap", ","])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "task capability filter must not contain empty capabilities",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "list", "--limit", "0"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "task list limit must be greater than 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "list", "--since", "not-a-time"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid task since timestamp"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "list", "--until", "not-a-time"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid task until timestamp"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "list", "--query", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains("task query must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Tool filtered task",
            "--tool",
            "cargo-test",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Created task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "list", "--tool", "cargo-test"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Tool filtered task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "list", "--tool", "!!!"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "tool id must contain at least one ASCII letter, digit, or hyphen",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Dependency filtered task",
            "--after",
            &task_id,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Created task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "list", "--after", &task_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("Dependency filtered task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "list", "--after", "!!!"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "task id must contain at least one ASCII letter, digit, or hyphen",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "task", "list", "--status", "waiting"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid task status"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_running\": 1"));
}

#[test]
fn status_output_uses_color_environment_conventions() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "init", "--force"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Color pending task",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env_remove("NO_COLOR")
        .env("CLICOLOR_FORCE", "1")
        .args(["--state", state, "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\u{1b}[33m1\u{1b}[0m pending"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("NO_COLOR", "1")
        .env("CLICOLOR_FORCE", "1")
        .args(["--state", state, "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\u{1b}[").not())
        .stdout(predicate::str::contains("1 pending"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env_remove("NO_COLOR")
        .env("CLICOLOR", "0")
        .env("CLICOLOR_FORCE", "1")
        .args(["--state", state, "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\u{1b}[33m1\u{1b}[0m pending"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env_remove("NO_COLOR")
        .env("CLICOLOR", "0")
        .env("CLICOLOR_FORCE", "0")
        .args(["--state", state, "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\u{1b}[").not())
        .stdout(predicate::str::contains("1 pending"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env_remove("NO_COLOR")
        .env("CLICOLOR_FORCE", "1")
        .args(["--state", state, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\u{1b}[").not())
        .stdout(predicate::str::contains("\"tasks_pending\": 1"));
}

#[test]
fn state_directory_may_contain_dots() {
    let dir = workspace_tempdir().expect("tempdir");
    let state_dir = dir.path().join("agent.os.state");
    std::fs::create_dir(&state_dir).expect("state dir");
    let state = state_dir.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "init", "--force"])
        .assert()
        .success();

    assert!(state_dir.join("state.json").exists());
}

#[test]
fn json_create_commands_return_created_records() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    let init_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "init", "--force"])
        .output()
        .expect("json init");
    assert!(init_output.status.success(), "json init failed");
    let init_body: Value = serde_json::from_slice(&init_output.stdout).expect("init json");
    assert_eq!(
        init_body["state_path"],
        state.join("state.json").to_str().expect("state file")
    );
    assert_eq!(init_body["os"]["name"], "Agent OS");

    let blank_name_state = dir.path().join("blank-name");
    let blank_name_state_arg = blank_name_state.to_str().expect("blank name state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            blank_name_state_arg,
            "init",
            "--force",
            "--name",
            "   ",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("OS name must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "agent",
            "add",
            "json-worker",
            "--cap",
            "rust",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"id\": \"json-worker\""))
        .stdout(predicate::str::contains("\"agent\""));

    let task_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "task",
            "create",
            "JSON task",
            "--need",
            "rust",
        ])
        .output()
        .expect("json task create");
    assert!(task_output.status.success(), "json task create failed");
    let task_body: Value = serde_json::from_slice(&task_output.stdout).expect("task json");
    let task_id = task_body["id"].as_str().expect("task id");
    assert_eq!(task_body["task"]["title"], "JSON task");

    let plan_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--json", "task", "plan", task_id, "--step", "one",
        ])
        .output()
        .expect("json task plan");
    assert!(plan_output.status.success(), "json task plan failed");
    let plan_body: Value = serde_json::from_slice(&plan_output.stdout).expect("plan json");
    assert_eq!(plan_body["id"], task_id);
    assert_eq!(plan_body["task"]["id"], task_id);
    assert_eq!(plan_body["task"]["plan"], serde_json::json!(["one"]));

    let complete_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--json", "task", "complete", task_id, "--note", "done",
        ])
        .output()
        .expect("json task complete");
    assert!(
        complete_output.status.success(),
        "json task complete failed"
    );
    let complete_body: Value =
        serde_json::from_slice(&complete_output.stdout).expect("complete json");
    assert_eq!(complete_body["id"], task_id);
    assert_eq!(complete_body["task"]["id"], task_id);
    assert_eq!(complete_body["task"]["status"], "complete");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "plan",
            task_id,
            "--step",
            "changed after completion",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be edited while complete"));

    let completed_show = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "task", "show", task_id])
        .output()
        .expect("completed task show");
    assert!(
        completed_show.status.success(),
        "completed task show failed"
    );
    let completed_show: Value =
        serde_json::from_slice(&completed_show.stdout).expect("completed task json");
    assert_eq!(completed_show["plan"], serde_json::json!(["one"]));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "task", "delete", task_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"deleted\": true"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "tool",
            "add",
            "json-tool",
            "--command-template",
            "printf hi",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"id\": \"json-tool\""))
        .stdout(predicate::str::contains("\"tool\""));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "memory",
            "add",
            "json-topic",
            "json-body",
            "--tag",
            "docs,rust",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"id\""))
        .stdout(predicate::str::contains("\"memory\""))
        .stdout(predicate::str::contains("\"topic\": \"json-topic\""));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "memory",
            "add",
            "json-latest-topic",
            "json-latest-body",
            "--tag",
            "docs,rust",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"topic\": \"json-latest-topic\""));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "memory",
            "add",
            "db-note",
            "Pool idle postgres connections before runtime requests",
            "--tag",
            "rust",
        ])
        .assert()
        .success();
    rewrite_memory_timestamp(&state, "db-note", "2020-01-01T00:00:00Z");

    let limited_memory = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--json", "memory", "list", "--tag", "rust", "--limit", "1",
        ])
        .output()
        .expect("memory list");
    assert!(limited_memory.status.success(), "memory list failed");
    let limited_memory_body: Value =
        serde_json::from_slice(&limited_memory.stdout).expect("memory list json");
    assert_eq!(
        limited_memory_body
            .as_array()
            .expect("memory records")
            .len(),
        1
    );
    assert_eq!(limited_memory_body[0]["topic"], "json-latest-topic");

    let memory_by_recency = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--json", "memory", "list", "--tag", "rust",
        ])
        .output()
        .expect("memory list");
    assert!(memory_by_recency.status.success(), "memory list failed");
    let memory_by_recency_body: Value =
        serde_json::from_slice(&memory_by_recency.stdout).expect("memory list json");
    assert_eq!(memory_by_recency_body[0]["topic"], "json-latest-topic");

    let private_memory = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "memory",
            "add",
            "private-client-note",
            "Keep this out of provider recall",
            "--visibility",
            "private",
            "--scope",
            "client-a",
            "--tag",
            "private",
        ])
        .output()
        .expect("private memory add");
    assert!(private_memory.status.success(), "private memory add failed");
    let private_memory_body: Value =
        serde_json::from_slice(&private_memory.stdout).expect("private memory json");
    assert_eq!(private_memory_body["memory"]["visibility"], "private");
    assert_eq!(private_memory_body["memory"]["scope"], "client-a");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "memory",
            "list",
            "--visibility",
            "private",
            "--scope",
            "client-a",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("private-client-note"))
        .stdout(predicate::str::contains("private"))
        .stdout(predicate::str::contains("client-a"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "memory",
            "list",
            "--since",
            "1970-01-01T00:00:00Z",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("json-topic"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "memory",
            "list",
            "--until",
            "2999-01-01T00:00:00Z",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("json-topic"));

    let limited_search = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "memory",
            "search",
            "json",
            "--tag",
            "docs,rust",
            "--limit",
            "1",
        ])
        .output()
        .expect("memory search");
    assert!(limited_search.status.success(), "memory search failed");
    let limited_search_body: Value =
        serde_json::from_slice(&limited_search.stdout).expect("memory search json");
    assert_eq!(
        limited_search_body
            .as_array()
            .expect("memory records")
            .len(),
        1
    );
    assert_eq!(limited_search_body[0]["topic"], "json-latest-topic");

    let search_by_recency = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "memory",
            "search",
            "json",
            "--tag",
            "docs,rust",
        ])
        .output()
        .expect("memory search");
    assert!(search_by_recency.status.success(), "memory search failed");
    let search_by_recency_body: Value =
        serde_json::from_slice(&search_by_recency.stdout).expect("memory search json");
    assert_eq!(search_by_recency_body[0]["topic"], "json-latest-topic");

    let search_by_relevance = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "memory",
            "search",
            "connection pooling postgres",
            "--tag",
            "rust",
            "--limit",
            "1",
        ])
        .output()
        .expect("memory search relevance");
    assert!(
        search_by_relevance.status.success(),
        "memory relevance search failed"
    );
    let search_by_relevance_body: Value =
        serde_json::from_slice(&search_by_relevance.stdout).expect("memory relevance json");
    assert_eq!(search_by_relevance_body[0]["topic"], "db-note");

    let recall = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "memory",
            "recall",
            "connection pooling postgres",
            "--tag",
            "rust",
            "--limit",
            "1",
        ])
        .output()
        .expect("memory recall");
    assert!(recall.status.success(), "memory recall failed");
    let recall_body: Value = serde_json::from_slice(&recall.stdout).expect("memory recall json");
    assert_eq!(recall_body.as_array().expect("recall hits").len(), 1);
    assert_eq!(recall_body[0]["record"]["topic"], "db-note");
    assert!(
        recall_body[0]["score"].as_u64().expect("recall score") > 0,
        "{recall_body}"
    );
    assert!(
        recall_body[0]["snippet"]
            .as_str()
            .expect("recall snippet")
            .contains("postgres"),
        "{recall_body}"
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "memory",
            "search",
            "json",
            "--since",
            "1970-01-01T00:00:00Z",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("json-topic"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "memory",
            "search",
            "json",
            "--until",
            "2999-01-01T00:00:00Z",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("json-topic"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "memory", "list", "--tag", "rust"])
        .assert()
        .success()
        .stdout(predicate::str::contains("json-topic"))
        .stdout(predicate::str::contains("docs, rust"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "memory",
            "search",
            "json",
            "--tag",
            "docs,rust",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("json-topic"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "memory", "list", "--tag", ","])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "memory tag filter must not contain empty tags",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "memory", "list", "--limit", "0"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "memory limit must be greater than 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "memory",
            "list",
            "--since",
            "not-a-time",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid memory since timestamp"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "memory",
            "list",
            "--until",
            "not-a-time",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid memory until timestamp"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "memory", "search", "json", "--limit", "0",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "memory limit must be greater than 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "memory",
            "search",
            "json",
            "--since",
            "not-a-time",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid memory since timestamp"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "memory",
            "search",
            "json",
            "--until",
            "not-a-time",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid memory until timestamp"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "memory",
            "list",
            "--visibility",
            "secret",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "memory visibility must be one of: shared, private",
        ));
}

#[test]
fn invalid_enum_like_inputs_are_rejected() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "agent",
            "heartbeat",
            "builder",
            "--status",
            "awake",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid agent status `awake`"))
        .stderr(predicate::str::contains("online/up"))
        .stderr(predicate::str::contains("offline/down"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "agent",
            "add",
            "blank-kind",
            "--kind",
            " ",
            "--cap",
            "rust",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("agent kind must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "agent",
            "add",
            "blank-model",
            "--model",
            " ",
            "--cap",
            "rust",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("agent model must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Bad priority",
            "--priority",
            "eventually",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid priority `eventually`"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Blank cwd",
            "--cwd",
            "   ",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("task cwd must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Empty tool reference",
            "--tool",
            "",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "tool id must contain at least one ASCII letter, digit, or hyphen",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Empty dependency",
            "--after",
            "",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "dependency task id must contain at least one ASCII letter, digit, or hyphen",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "bad-kind",
            "--kind",
            "magic",
            "--command-template",
            "printf hi",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid tool kind `magic`"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "blank-cwd",
            "--command-template",
            "printf hi",
            "--cwd",
            "   ",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("tool cwd must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "workflow",
            "create",
            "Bad workflow",
            "--priority",
            "whenever",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid priority `whenever`"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "memory", "add", "   ", "body"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("memory topic must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "memory", "add", "topic", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains("memory body must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "memory", "add", "topic", "body", "--tag", "",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "memory tags must not contain empty tags",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "memory", "search", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "memory search query must not be empty",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "memory", "show", "!!!"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "memory id must contain at least one ASCII letter, digit, or hyphen",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "memory", "update", "!!!", "--body", "updated",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "memory id must contain at least one ASCII letter, digit, or hyphen",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "memory", "remove", "!!!"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "memory id must contain at least one ASCII letter, digit, or hyphen",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "workflow", "create", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "workflow objective must not be empty",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "workflow", "list", "--limit", "0"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "workflow list limit must be greater than 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "workflow",
            "list",
            "--since",
            "not-a-time",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "invalid workflow since timestamp `not-a-time`; expected RFC3339",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "workflow", "list", "--query", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains("workflow query must not be empty"));
}

#[test]
fn operator_command_ids_must_be_valid() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "agent", "show", ""])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "agent id must contain at least one ASCII letter, digit, or hyphen",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "show", ""])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "task id must contain at least one ASCII letter, digit, or hyphen",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "tool", "show", ""])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "tool id must contain at least one ASCII letter, digit, or hyphen",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "show", ""])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "run id must contain at least one ASCII letter, digit, or hyphen",
        ));
}

#[test]
fn agent_heartbeat_cli_controls_scheduler_availability() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "agent",
            "heartbeat",
            "builder",
            "--status",
            "paused",
            "--lease-seconds",
            "60",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Updated agent builder: paused"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "agent", "show", "builder"])
        .assert()
        .success()
        .stdout(predicate::str::contains("builder paused"))
        .stdout(predicate::str::contains("lease expires:"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "agent", "show", "builder"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"status\": \"paused\""))
        .stdout(predicate::str::contains("\"lease_expires_at\": \""));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Wait for available agent",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("No runnable tasks found"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_pending\": 1"))
        .stdout(predicate::str::contains("\"tasks_running\": 0"));

    let heartbeat = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "agent",
            "heartbeat",
            "builder",
            "--status",
            "online",
        ])
        .output()
        .expect("agent heartbeat json");
    assert!(heartbeat.status.success(), "agent heartbeat failed");
    let heartbeat: Value = serde_json::from_slice(&heartbeat.stdout).expect("heartbeat json");
    assert_eq!(heartbeat["id"], "builder");
    assert_eq!(heartbeat["agent"]["id"], "builder");
    assert_eq!(heartbeat["agent"]["status"], "online");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Assigned task"));
}

#[test]
fn agent_claim_cli_assigns_ready_work() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "agent",
            "claim",
            "builder",
            "--lease-seconds=0",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "lease_seconds must be greater than 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Claimable CLI task",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    let claim_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "agent",
            "claim",
            "builder",
            "--lease-seconds",
            "60",
        ])
        .output()
        .expect("agent claim");
    assert!(claim_output.status.success(), "agent claim failed");
    let claim: Value = serde_json::from_slice(&claim_output.stdout).expect("claim json");
    assert_eq!(claim["claimed"], true);
    assert_eq!(claim["assignment"]["agent_id"], "builder");
    assert_eq!(claim["task"]["title"], "Claimable CLI task");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_running\": 1"));

    let empty_claim = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "agent", "claim", "builder"])
        .output()
        .expect("empty claim");
    assert!(empty_claim.status.success(), "empty claim failed");
    let empty: Value = serde_json::from_slice(&empty_claim.stdout).expect("empty claim json");
    assert_eq!(empty["claimed"], false);
    assert_eq!(empty["assignment"], Value::Null);
    assert_eq!(empty["task"], Value::Null);
}

#[test]
fn state_repair_expires_online_agents_with_expired_leases() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "agent",
            "heartbeat",
            "builder",
            "--lease-seconds=0",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "lease_seconds must be greater than 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "agent",
            "heartbeat",
            "builder",
            "--status",
            "online",
            "--lease-seconds=60",
        ])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    value["agents"]["builder"]["lease_expires_at"] = serde_json::json!("2000-01-01T00:00:00Z");
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("online with expired lease"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains("expired lease for agent builder"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "agent", "show", "builder"])
        .assert()
        .success()
        .stdout(predicate::str::contains("builder offline"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn agent_remove_refuses_referenced_agents_and_removes_idle_agents() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "agent",
            "add",
            "scratch-worker",
            "--cap",
            "rust",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Registered agent scratch-worker"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "agent",
            "remove",
            "scratch-worker",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"removed\": true"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "agent", "show", "scratch-worker"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("agent not found"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Keep builder referenced",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Assigned task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "agent", "remove", "builder"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "agent builder is still referenced",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn execute_task_command_records_run_and_log() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Run smoke command",
            "--need",
            "rust",
            "--command",
            "printf agent-os; printf err-os >&2",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Executed run"))
        .stdout(predicate::str::contains("success"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_complete\": 1"))
        .stdout(predicate::str::contains("\"runs\": 1"));

    let log_count = std::fs::read_dir(state.join("runs"))
        .expect("runs dir")
        .count();
    assert_eq!(log_count, 1);

    let output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "runs", "list"])
        .output()
        .expect("runs list");
    assert!(output.status.success());
    let runs: Value = serde_json::from_slice(&output.stdout).expect("runs json");
    let run_id = runs[0]["id"].as_str().expect("run id");
    let run_task_id = runs[0]["task_id"].as_str().expect("run task id");
    let artifacts = runs[0]["artifacts"].as_array().expect("run artifacts");
    let stdout_artifact = artifacts
        .iter()
        .find(|artifact| artifact["kind"] == "stdout")
        .expect("stdout artifact");
    let stderr_artifact = artifacts
        .iter()
        .find(|artifact| artifact["kind"] == "stderr")
        .expect("stderr artifact");
    let stdout_path = stdout_artifact["path"].as_str().expect("stdout path");
    let stderr_path = stderr_artifact["path"].as_str().expect("stderr path");
    assert_eq!(
        std::fs::read_to_string(stdout_path).expect("stdout artifact"),
        "agent-os"
    );
    assert_eq!(
        std::fs::read_to_string(stderr_path).expect("stderr artifact"),
        "err-os"
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "runs",
            "list",
            "--status",
            "success",
            "--task",
            run_task_id,
            "--agent",
            "builder",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"))
        .stdout(predicate::str::contains("printf agent-os"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "list", "--limit", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"))
        .stdout(predicate::str::contains("printf agent-os"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "list", "--query", "agent-os"])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"))
        .stdout(predicate::str::contains("printf agent-os"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "runs",
            "list",
            "--since",
            "1970-01-01T00:00:00Z",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"))
        .stdout(predicate::str::contains("printf agent-os"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "runs",
            "list",
            "--until",
            "2999-01-01T00:00:00Z",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"))
        .stdout(predicate::str::contains("printf agent-os"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "list", "--task", "!!!"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "task id must contain at least one ASCII letter, digit, or hyphen",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "list", "--agent", "!!!"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "agent id must contain at least one ASCII letter, digit, or hyphen",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "list", "--status", "waiting"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid run status"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "runs",
            "list",
            "--since",
            "not-a-time",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid run since timestamp"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "runs",
            "list",
            "--until",
            "not-a-time",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid run until timestamp"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "list", "--query", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains("run query must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "list", "--limit", "0"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "runs limit must be greater than 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "logs", run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("agent-os"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "runs",
            "logs",
            run_id,
            "--tail-bytes",
            "2",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("os"))
        .stdout(predicate::str::contains("agent-").not());

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "runs",
            "logs",
            run_id,
            "--tail-bytes",
            "0",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "tail_bytes must be greater than 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "runs",
            "logs",
            run_id,
            "--tail-bytes",
            "2",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tail_bytes\": 2"))
        .stdout(predicate::str::contains("\"truncated\": true"))
        .stdout(predicate::str::contains("\"body\": \"os\""));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "replay", run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("replay"))
        .stdout(predicate::str::contains("Run smoke command"))
        .stdout(predicate::str::contains("agent-os"))
        .stdout(predicate::str::contains("run-finished"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "runs",
            "replay",
            run_id,
            "--tail-bytes",
            "2",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("log:\nos"))
        .stdout(predicate::str::contains("log:\nagent-os").not());

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "runs",
            "replay",
            run_id,
            "--tail-bytes",
            "2",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"log_tail_bytes\": 2"))
        .stdout(predicate::str::contains("\"log_truncated\": true"))
        .stdout(predicate::str::contains("\"log\": \"os\""));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "debug", run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("debug"))
        .stdout(predicate::str::contains("artifacts: 3"))
        .stdout(predicate::str::contains("exists=true"))
        .stdout(predicate::str::contains("Run smoke command"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "runs",
            "debug",
            run_id,
            "--tail-bytes",
            "2",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"log_tail_bytes\": 2"))
        .stdout(predicate::str::contains("\"log\": \"os\""))
        .stdout(predicate::str::contains("\"agent\""))
        .stdout(predicate::str::contains("\"artifact_status\""))
        .stdout(predicate::str::contains("\"diagnostics\""));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "artifacts", run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("stdout"))
        .stdout(predicate::str::contains("stderr"))
        .stdout(predicate::str::contains("fnv1a64:"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "runs",
            "artifacts",
            run_id,
            "stdout",
            "--tail-bytes",
            "2",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"artifact_id\": \"stdout\""))
        .stdout(predicate::str::contains("\"tail_bytes\": 2"))
        .stdout(predicate::str::contains("\"truncated\": true"))
        .stdout(predicate::str::contains("\"body\": \"os\""))
        .stdout(predicate::str::contains("fnv1a64:"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "artifacts", run_id, "stderr"])
        .assert()
        .success()
        .stdout(predicate::str::contains("artifact stderr"))
        .stdout(predicate::str::contains("err-os"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "runs",
            "replay",
            run_id,
            "--tail-bytes",
            "0",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "tail_bytes must be greater than 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "runs",
            "debug",
            run_id,
            "--tail-bytes",
            "0",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "tail_bytes must be greater than 0",
        ));
}

#[test]
fn state_sqlite_initializes_and_imports_json_state() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");
    let sqlite_init = dir.path().join("init-only.sqlite");
    let sqlite_init_arg = sqlite_init.to_str().expect("sqlite init");
    let sqlite_import = dir.path().join("import.sqlite");
    let sqlite_import_arg = sqlite_import.to_str().expect("sqlite import");
    let restored_state = dir.path().join("restored-agent-os");
    let restored_state_arg = restored_state.to_str().expect("restored state");
    let active_sqlite = dir.path().join("active.sqlite");
    let active_sqlite_arg = active_sqlite.to_str().expect("active sqlite");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "state",
            "sqlite",
            "--output",
            sqlite_init_arg,
            "--init-only",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Initialized SQLite backend"));

    let init_connection = rusqlite::Connection::open(&sqlite_init).expect("open init sqlite");
    let table_count: i64 = init_connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('state_snapshots', 'run_logs', 'run_records', 'task_records')",
            [],
            |row| row.get(0),
        )
        .expect("query sqlite tables");
    assert_eq!(table_count, 4);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Persist sqlite run",
            "--need",
            "rust",
            "--command",
            "printf sqlite-log",
            "--max-attempts",
            "3",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success();

    let import_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "state",
            "sqlite",
            "--output",
            sqlite_import_arg,
        ])
        .output()
        .expect("state sqlite import");
    assert!(import_output.status.success(), "sqlite import failed");
    let import_report: Value =
        serde_json::from_slice(&import_output.stdout).expect("sqlite import json");
    assert_eq!(import_report["initialized"], true);
    assert_eq!(import_report["imported"], true);
    assert_eq!(import_report["import"]["imported_snapshot"], true);
    assert_eq!(import_report["import"]["imported_runs"], 1);
    assert_eq!(import_report["import"]["imported_run_logs"], 1);
    assert_eq!(import_report["import"]["skipped_run_logs"], 0);

    let import_connection = rusqlite::Connection::open(&sqlite_import).expect("open import sqlite");
    let snapshot_body: String = import_connection
        .query_row(
            "SELECT body FROM state_snapshots WHERE id = 'current'",
            [],
            |row| row.get(0),
        )
        .expect("query sqlite snapshot");
    let snapshot: Value = serde_json::from_str(&snapshot_body).expect("snapshot json");
    assert_eq!(snapshot["tasks"].as_object().expect("tasks").len(), 1);
    assert_eq!(snapshot["runs"].as_object().expect("runs").len(), 1);

    let run_record_body: String = import_connection
        .query_row("SELECT body FROM run_records LIMIT 1", [], |row| row.get(0))
        .expect("query sqlite run record");
    let run_record: Value = serde_json::from_str(&run_record_body).expect("run record json");
    assert_eq!(run_record["command"], "printf sqlite-log");

    let task_record: (String, String, String, i64, i64) = import_connection
        .query_row(
            "SELECT title, status, priority, attempts, max_attempts FROM task_records LIMIT 1",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .expect("query sqlite task record");
    assert_eq!(task_record.0, "Persist sqlite run");
    assert_eq!(task_record.1, "complete");
    assert_eq!(task_record.2, "normal");
    assert_eq!(task_record.3, 1);
    assert_eq!(task_record.4, 3);

    let log_body: String = import_connection
        .query_row("SELECT body FROM run_logs LIMIT 1", [], |row| row.get(0))
        .expect("query sqlite log");
    assert!(log_body.contains("sqlite-log"));

    let restore_preview = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            restored_state_arg,
            "--json",
            "state",
            "sqlite",
            "--output",
            sqlite_import_arg,
            "--restore",
            "--dry-run",
        ])
        .output()
        .expect("state sqlite restore preview");
    assert!(
        restore_preview.status.success(),
        "sqlite restore preview failed"
    );
    let restore_preview: Value =
        serde_json::from_slice(&restore_preview.stdout).expect("sqlite restore preview json");
    assert_eq!(restore_preview["dry_run"], true);
    assert_eq!(restore_preview["restored"], false);
    assert_eq!(restore_preview["restore"]["restored_snapshot"], false);
    assert!(!restored_state.join("state.json").exists());

    let restore_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            restored_state_arg,
            "--json",
            "state",
            "sqlite",
            "--output",
            sqlite_import_arg,
            "--restore",
        ])
        .output()
        .expect("state sqlite restore");
    assert!(restore_output.status.success(), "sqlite restore failed");
    let restore_report: Value =
        serde_json::from_slice(&restore_output.stdout).expect("sqlite restore json");
    assert_eq!(restore_report["restored"], true);
    assert_eq!(restore_report["restore"]["restored_snapshot"], true);
    assert_eq!(restore_report["restore"]["restored_runs"], 1);

    let restored_body =
        std::fs::read_to_string(restored_state.join("state.json")).expect("restored state");
    let restored_snapshot: Value = serde_json::from_str(&restored_body).expect("restored json");
    assert_eq!(
        restored_snapshot["tasks"]
            .as_object()
            .expect("restored tasks")
            .len(),
        1
    );
    assert_eq!(
        restored_snapshot["runs"]
            .as_object()
            .expect("restored runs")
            .len(),
        1
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            active_sqlite_arg,
            "init",
            "--force",
            "--profile",
            "dev",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            active_sqlite_arg,
            "task",
            "create",
            "Active sqlite task",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    let active_status = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", active_sqlite_arg, "--json", "status"])
        .output()
        .expect("active sqlite status");
    assert!(
        active_status.status.success(),
        "active sqlite status failed"
    );
    let active_status: Value =
        serde_json::from_slice(&active_status.stdout).expect("active sqlite status json");
    assert_eq!(active_status["tasks_pending"], 1);

    let active_connection = rusqlite::Connection::open(&active_sqlite).expect("active sqlite db");
    let active_snapshot_body: String = active_connection
        .query_row(
            "SELECT body FROM state_snapshots WHERE id = 'current'",
            [],
            |row| row.get(0),
        )
        .expect("active sqlite snapshot");
    let active_snapshot: Value =
        serde_json::from_str(&active_snapshot_body).expect("active snapshot json");
    assert_eq!(
        active_snapshot["tasks"]
            .as_object()
            .expect("active sqlite tasks")
            .len(),
        1
    );
    let active_task_count: i64 = active_connection
        .query_row("SELECT COUNT(*) FROM task_records", [], |row| row.get(0))
        .expect("active sqlite task record count");
    assert_eq!(active_task_count, 1);
    let active_task_attempts: (i64, i64) = active_connection
        .query_row(
            "SELECT attempts, max_attempts FROM task_records LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("active sqlite task attempts");
    assert_eq!(active_task_attempts, (0, 1));
}

#[test]
fn config_can_seed_policy_that_rejects_execution() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let config = dir.path().join("agent-os.toml");
    let state_arg = state.to_str().expect("state");
    let config_arg = config.to_str().expect("config");

    std::fs::write(
        &config,
        r#"
name = "Locked OS"

[policy]
allow_shell = false
allowed_commands = []
denied_patterns = ["rm -rf"]
max_output_bytes = 1024

[[agents]]
name = "builder"
kind = "builder"
capabilities = ["rust"]
parallel = 1
"#,
    )
    .expect("write config");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--config", config_arg, "init", "--force",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Blocked command",
            "--need",
            "rust",
            "--command",
            "printf blocked",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stderr(predicate::str::contains("shell execution is disabled"))
        .stderr(predicate::str::contains(
            "set policy.allow_shell = true in config",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_blocked\": 1"))
        .stdout(predicate::str::contains("\"runs\": 1"));
}

#[test]
fn config_rejects_invalid_seed_definitions() {
    let dir = workspace_tempdir().expect("tempdir");
    let config = dir.path().join("agent-os.toml");
    let config_arg = config.to_str().expect("config");

    std::fs::write(&config, "nam = \"Typo\"\n").expect("write unknown top-level config");
    let unknown_top_level_state = dir.path().join("unknown-top-level");
    let unknown_top_level_state_arg = unknown_top_level_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            unknown_top_level_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown field"));

    std::fs::write(&config, "name = \"   \"\n").expect("write empty os name config");
    let empty_name_state = dir.path().join("empty-name");
    let empty_name_state_arg = empty_name_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            empty_name_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("OS name must not be empty"));

    std::fs::write(
        &config,
        r#"
[policy]
allow_shel = false
"#,
    )
    .expect("write unknown policy config");
    let unknown_policy_state = dir.path().join("unknown-policy");
    let unknown_policy_state_arg = unknown_policy_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            unknown_policy_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown field"));

    std::fs::write(
        &config,
        r#"
[provider]
kind = "openai-compatible"
"#,
    )
    .expect("write missing provider endpoint config");
    let missing_provider_endpoint_state = dir.path().join("missing-provider-endpoint");
    let missing_provider_endpoint_state_arg =
        missing_provider_endpoint_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            missing_provider_endpoint_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "provider endpoint is required for openai-compatible provider",
        ));

    std::fs::write(
        &config,
        r#"
[provider]
kind = "openai-compatible"
endpoint = "/v1/chat/completions"
"#,
    )
    .expect("write invalid provider endpoint config");
    let invalid_provider_endpoint_state = dir.path().join("invalid-provider-endpoint");
    let invalid_provider_endpoint_state_arg =
        invalid_provider_endpoint_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            invalid_provider_endpoint_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "provider endpoint must be an absolute http(s) URL",
        ));

    std::fs::write(
        &config,
        r#"
[provider]
model = " "
"#,
    )
    .expect("write empty provider model config");
    let empty_provider_model_state = dir.path().join("empty-provider-model");
    let empty_provider_model_state_arg = empty_provider_model_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            empty_provider_model_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("provider model must not be empty"));

    std::fs::write(
        &config,
        r#"
[provider]
api_key_env = " BAD_KEY "
"#,
    )
    .expect("write invalid provider env config");
    let invalid_provider_env_state = dir.path().join("invalid-provider-env");
    let invalid_provider_env_state_arg = invalid_provider_env_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            invalid_provider_env_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "provider api_key_env must be a valid environment variable name",
        ));

    std::fs::write(
        &config,
        r#"
[provider]
kind = "openai-compatible"
endpoint = "https://api.openai.com/v1/chat/completions"
max_retries = 9
"#,
    )
    .expect("write unbounded provider retry config");
    let unbounded_provider_retries_state = dir.path().join("unbounded-provider-retries");
    let unbounded_provider_retries_state_arg =
        unbounded_provider_retries_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            unbounded_provider_retries_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "provider max_retries must be less than or equal to 8",
        ));

    std::fs::write(
        &config,
        r#"
[policy]
denied_patterns = ["rm -rf", " "]
"#,
    )
    .expect("write empty policy list config");
    let empty_policy_value_state = dir.path().join("empty-policy-value");
    let empty_policy_value_state_arg = empty_policy_value_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            empty_policy_value_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "policy denied_patterns must not contain empty values",
        ));

    std::fs::write(
        &config,
        r#"
[policy]
allowed_env_vars = ["PATH", " BAD_ENV "]
"#,
    )
    .expect("write invalid policy env config");
    let invalid_policy_env_state = dir.path().join("invalid-policy-env");
    let invalid_policy_env_state_arg = invalid_policy_env_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            invalid_policy_env_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "policy allowed_env_vars must contain valid environment variable names",
        ));

    std::fs::write(
        &config,
        r#"
[policy]
max_output_bytes = 0
"#,
    )
    .expect("write zero max output config");
    let zero_max_output_state = dir.path().join("zero-max-output");
    let zero_max_output_state_arg = zero_max_output_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            zero_max_output_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "policy max_output_bytes must be greater than 0",
        ));

    std::fs::write(
        &config,
        r#"
[policy]
command_timeout_seconds = 0
"#,
    )
    .expect("write zero timeout config");
    let zero_timeout_state = dir.path().join("zero-timeout");
    let zero_timeout_state_arg = zero_timeout_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            zero_timeout_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "policy command_timeout_seconds must be greater than 0",
        ));

    std::fs::write(
        &config,
        r#"
[[agents]]
name = "builder"
parallel = 0
"#,
    )
    .expect("write zero parallel config");
    let zero_parallel_state = dir.path().join("zero-parallel");
    let zero_parallel_state_arg = zero_parallel_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            zero_parallel_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "agent builder parallel must be greater than 0",
        ));

    std::fs::write(
        &config,
        r#"
[[agents]]
name = "!!!"
"#,
    )
    .expect("write invalid agent name config");
    let invalid_agent_name_state = dir.path().join("invalid-agent-name");
    let invalid_agent_name_state_arg = invalid_agent_name_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            invalid_agent_name_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("agent name must contain"));

    std::fs::write(
        &config,
        r#"
[[agents]]
name = "builder"
capabilities = ["rust", " "]
"#,
    )
    .expect("write empty agent capability config");
    let empty_agent_capability_state = dir.path().join("empty-agent-capability");
    let empty_agent_capability_state_arg = empty_agent_capability_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            empty_agent_capability_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "agent builder capabilities must not contain empty capabilities",
        ));

    std::fs::write(
        &config,
        r#"
[[agents]]
name = "builder"
kind = " "
"#,
    )
    .expect("write empty agent kind config");
    let empty_agent_kind_state = dir.path().join("empty-agent-kind");
    let empty_agent_kind_state_arg = empty_agent_kind_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            empty_agent_kind_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "agent builder kind must not be empty",
        ));

    std::fs::write(
        &config,
        r#"
[[agents]]
name = "builder"
model = " "
"#,
    )
    .expect("write empty agent model config");
    let empty_agent_model_state = dir.path().join("empty-agent-model");
    let empty_agent_model_state_arg = empty_agent_model_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            empty_agent_model_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "agent builder model must not be empty",
        ));

    std::fs::write(
        &config,
        r#"
[[agents]]
name = "Builder"

[[agents]]
name = "builder"
"#,
    )
    .expect("write duplicate agent config");
    let duplicate_agent_state = dir.path().join("duplicate-agent");
    let duplicate_agent_state_arg = duplicate_agent_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            duplicate_agent_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "duplicate agent id in config: builder",
        ));

    std::fs::write(
        &config,
        r#"
[[tools]]
name = "bad-tool"
kind = "magic"
command_template = "printf hi"
"#,
    )
    .expect("write bad tool config");
    let bad_tool_state = dir.path().join("bad-tool");
    let bad_tool_state_arg = bad_tool_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            bad_tool_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid tool kind `magic`"));

    std::fs::write(
        &config,
        r#"
[[tools]]
name = "empty-template"
command_template = "   "
"#,
    )
    .expect("write empty tool template config");
    let empty_template_state = dir.path().join("empty-template");
    let empty_template_state_arg = empty_template_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            empty_template_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "tool command template must not be empty",
        ));

    std::fs::write(
        &config,
        r#"
[[tools]]
name = "bad-placeholder"
command_template = "printf { message }"
"#,
    )
    .expect("write bad placeholder tool config");
    let bad_placeholder_state = dir.path().join("bad-placeholder");
    let bad_placeholder_state_arg = bad_placeholder_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            bad_placeholder_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("malformed template placeholder"));

    std::fs::write(
        &config,
        r#"
[[tools]]
name = "unclosed-placeholder"
command_template = "printf {message"
"#,
    )
    .expect("write unclosed placeholder tool config");
    let unclosed_placeholder_state = dir.path().join("unclosed-placeholder");
    let unclosed_placeholder_state_arg = unclosed_placeholder_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            unclosed_placeholder_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unclosed template placeholder"));

    std::fs::write(
        &config,
        r#"
[[tools]]
name = "empty-cwd"
command_template = "printf hi"
default_cwd = "   "
"#,
    )
    .expect("write empty tool cwd config");
    let empty_tool_cwd_state = dir.path().join("empty-tool-cwd");
    let empty_tool_cwd_state_arg = empty_tool_cwd_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            empty_tool_cwd_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "tool empty-cwd default_cwd must not be empty",
        ));

    std::fs::write(
        &config,
        r#"
[[tools]]
name = "cap-tool"
command_template = "printf hi"
required_capabilities = ["rust", ","]
"#,
    )
    .expect("write empty tool capability config");
    let empty_tool_capability_state = dir.path().join("empty-tool-capability");
    let empty_tool_capability_state_arg = empty_tool_capability_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            empty_tool_capability_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "tool cap-tool required capabilities must not contain empty capabilities",
        ));

    std::fs::write(
        &config,
        r#"
[[tools]]
name = "Cargo Test"
command_template = "cargo test"

[[tools]]
name = "cargo-test"
command_template = "cargo test"
"#,
    )
    .expect("write duplicate tool config");
    let duplicate_tool_state = dir.path().join("duplicate-tool");
    let duplicate_tool_state_arg = duplicate_tool_state.to_str().expect("state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            duplicate_tool_state_arg,
            "--config",
            config_arg,
            "init",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "duplicate tool id in config: cargo-test",
        ));
}

#[test]
fn allowed_command_policy_rejects_shell_chaining() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let config = dir.path().join("agent-os.toml");
    let state_arg = state.to_str().expect("state");
    let config_arg = config.to_str().expect("config");

    std::fs::write(
        &config,
        r#"
name = "Allowlisted OS"

[policy]
allow_shell = true
allowed_commands = ["printf"]

[[agents]]
name = "builder"
kind = "builder"
capabilities = ["rust"]
parallel = 1
"#,
    )
    .expect("write config");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--config", config_arg, "init", "--force",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Chained command",
            "--need",
            "rust",
            "--command",
            "printf ok; uname -s",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stderr(predicate::str::contains("shell control operators"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_blocked\": 1"));
}

#[test]
fn policy_timeout_fails_runaway_command() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let config = dir.path().join("agent-os.toml");
    let state_arg = state.to_str().expect("state");
    let config_arg = config.to_str().expect("config");

    std::fs::write(
        &config,
        r#"
name = "Timeout OS"

[policy]
allow_shell = true
command_timeout_seconds = 1

[[agents]]
name = "builder"
kind = "builder"
capabilities = ["rust"]
parallel = 1
"#,
    )
    .expect("write config");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--config", config_arg, "init", "--force",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Runaway command",
            "--need",
            "rust",
            "--command",
            "sleep 3; printf late",
        ])
        .assert()
        .success();

    let start = Instant::now();
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("failed"));
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "timeout did not stop command promptly: {:?}",
        start.elapsed()
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_failed\": 1"));

    let eval_start = Instant::now();
    let eval_timeout = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "eval",
            "run",
            "timeout-smoke",
            "--command",
            "sleep 3; printf late",
        ])
        .output()
        .expect("eval timeout");
    assert!(eval_timeout.status.success(), "agent-os eval run failed");
    assert!(
        eval_start.elapsed() < Duration::from_secs(3),
        "eval timeout did not stop command promptly: {:?}",
        eval_start.elapsed()
    );
    let eval_timeout: Value =
        serde_json::from_slice(&eval_timeout.stdout).expect("eval timeout json");
    assert_eq!(eval_timeout["eval"]["success"], false);
    assert_eq!(eval_timeout["timed_out"], true);
    assert_eq!(eval_timeout["status"], Value::Null);
}

#[test]
fn policy_environment_is_minimal_by_default() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Check env isolation",
            "--need",
            "rust",
            "--command",
            "printf \"${AGENT_OS_SECRET:-empty}\"",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_SECRET", "supersecret")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"));

    let run_id = first_run_id(state_arg);
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "logs", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("empty"))
        .stdout(predicate::str::contains("supersecret").not());
}

#[test]
fn allowed_secret_environment_values_are_redacted_from_logs() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let config = dir.path().join("agent-os.toml");
    let state_arg = state.to_str().expect("state");
    let config_arg = config.to_str().expect("config");

    std::fs::write(
        &config,
        r#"
name = "Redacted OS"

[policy]
allow_shell = true
allowed_env_vars = ["PATH", "AGENT_OS_SECRET"]
redacted_env_patterns = ["SECRET"]

[[agents]]
name = "builder"
kind = "builder"
capabilities = ["rust"]
parallel = 1
"#,
    )
    .expect("write config");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--config", config_arg, "init", "--force",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Check log redaction",
            "--need",
            "rust",
            "--command",
            "printf \"$AGENT_OS_SECRET\"",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_SECRET", "supersecret")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"));

    let run_id = first_run_id(state_arg);
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "logs", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("[redacted]"))
        .stdout(predicate::str::contains("supersecret").not());
}

#[test]
fn config_init_and_doctor_report_config() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let config = dir.path().join("agent-os.toml");
    let state_arg = state.to_str().expect("state");
    let config_arg = config.to_str().expect("config");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["init", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("missing config defaults to safe"))
        .stdout(predicate::str::contains("missing config defaults to dev").not());

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", "   ", "doctor"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("state must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "status"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("state unavailable at"))
        .stderr(predicate::str::contains("agent-os --state"))
        .stderr(predicate::str::contains(state_arg))
        .stderr(predicate::str::contains("init --profile safe"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--config", "   ", "config", "init"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("config must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_HOME", "   ")
        .args(["doctor"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("AGENT_OS_HOME must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_CONFIG", "   ")
        .args(["--state", state_arg, "doctor"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("config must not be empty"));

    let config_init = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--config", config_arg, "--json", "config", "init"])
        .output()
        .expect("config init json");
    assert!(config_init.status.success(), "config init failed");
    let config_init: Value =
        serde_json::from_slice(&config_init.stdout).expect("config init json body");
    assert_eq!(config_init["path"], config_arg);
    assert_eq!(config_init["written"], true);
    assert_eq!(config_init["profile"], "safe");
    assert_eq!(config_init["config"]["name"], "Agent OS");
    assert_eq!(config_init["config"]["policy"]["allow_shell"], false);
    assert!(!config.with_extension("toml.tmp").exists());

    let dev_config = dir.path().join("dev-agent-os.toml");
    let dev_config_arg = dev_config.to_str().expect("dev config");
    let dev_config_init = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--config",
            dev_config_arg,
            "--json",
            "config",
            "init",
            "--profile",
            "dev",
        ])
        .output()
        .expect("dev config init json");
    assert!(dev_config_init.status.success(), "dev config init failed");
    let dev_report: Value =
        serde_json::from_slice(&dev_config_init.stdout).expect("dev config init json body");
    assert_eq!(dev_report["profile"], "dev");
    assert_eq!(dev_report["config"]["policy"]["allow_shell"], true);

    let safe_config = dir.path().join("safe-agent-os.toml");
    let safe_config_arg = safe_config.to_str().expect("safe config");
    let safe_config_init = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--config",
            safe_config_arg,
            "--json",
            "config",
            "init",
            "--profile",
            "safe",
        ])
        .output()
        .expect("safe config init json");
    assert!(safe_config_init.status.success(), "safe config init failed");
    let safe_report: Value =
        serde_json::from_slice(&safe_config_init.stdout).expect("safe config init json body");
    assert_eq!(safe_report["profile"], "safe");
    assert_eq!(safe_report["config"]["policy"]["allow_shell"], false);
    assert_eq!(
        safe_report["config"]["policy"]["network"]["mode"],
        "providers-only"
    );
    assert_eq!(
        safe_report["config"]["policy"]["approval"]["require_for_risky_actions"],
        true
    );

    let human_config = dir.path().join("human-agent-os.toml");
    let human_config_arg = human_config.to_str().expect("human config");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--config", human_config_arg, "config", "init"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Wrote safe config"))
        .stdout(predicate::str::contains("Next: agent-os --config"))
        .stdout(predicate::str::contains("config validate"));

    let config_show = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--config", config_arg, "--json", "config", "show"])
        .output()
        .expect("config show");
    assert!(config_show.status.success(), "config show failed");
    let config_report: Value =
        serde_json::from_slice(&config_show.stdout).expect("config show json");
    assert_eq!(config_report["path"], config_arg);
    assert_eq!(config_report["exists"], true);
    assert_eq!(config_report["config"]["name"], "Agent OS");

    let missing_config = dir.path().join("missing-agent-os.toml");
    let missing_config_arg = missing_config.to_str().expect("missing config");
    let default_show = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--config", missing_config_arg, "--json", "config", "show"])
        .output()
        .expect("default config show");
    assert!(
        default_show.status.success(),
        "default config show should succeed"
    );
    let default_report: Value =
        serde_json::from_slice(&default_show.stdout).expect("default config show json");
    assert_eq!(default_report["path"], missing_config_arg);
    assert_eq!(default_report["exists"], false);
    assert_eq!(default_report["config"]["name"], "Agent OS");
    assert_eq!(default_report["config"]["policy"]["allow_shell"], false);

    let default_validation = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--config",
            missing_config_arg,
            "--json",
            "config",
            "validate",
        ])
        .output()
        .expect("default config validate");
    assert!(
        default_validation.status.success(),
        "missing config validate should not fail"
    );
    let default_validation_report: Value =
        serde_json::from_slice(&default_validation.stdout).expect("default validate json");
    assert_eq!(default_validation_report["config_path"], missing_config_arg);
    assert_eq!(default_validation_report["config_exists"], false);
    assert_eq!(default_validation_report["config_loads"], false);
    assert_eq!(default_validation_report["config_valid"], Value::Null);
    assert_eq!(default_validation_report["config_error"], Value::Null);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--config",
            missing_config_arg,
            "doctor",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("config exists: no"))
        .stdout(predicate::str::contains("config init --profile safe"))
        .stdout(predicate::str::contains("config init --profile dev").not());

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--config", config_arg, "doctor"])
        .assert()
        .success()
        .stdout(predicate::str::contains("state valid: n/a"))
        .stdout(predicate::str::contains("config exists: yes"))
        .stdout(predicate::str::contains("config loads: yes"))
        .stdout(predicate::str::contains("config valid: yes"))
        .stdout(predicate::str::contains("next steps:"))
        .stdout(predicate::str::contains("agent-os --state"))
        .stdout(predicate::str::contains(" init"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--config", config_arg, "config", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("config valid: yes"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--config", config_arg, "--json", "config", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"config_valid\": true"))
        .stdout(predicate::str::contains("\"config_error\": null"));

    std::fs::write(&config, "name = '   '\n").expect("invalid config");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--config", config_arg, "config", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("config valid: no"))
        .stdout(predicate::str::contains("OS name must not be empty"))
        .stderr(predicate::str::contains("config invalid"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--config", config_arg, "--json", "config", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("\"config_valid\": false"))
        .stdout(predicate::str::contains("\"config_error\": null"))
        .stderr(predicate::str::contains("config invalid"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--config", config_arg, "doctor"])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "platform: {}",
            std::env::consts::OS
        )))
        .stdout(predicate::str::contains("service manager:"))
        .stdout(predicate::str::contains("service recommendation:"))
        .stdout(predicate::str::contains("shell execution supported:"))
        .stdout(predicate::str::contains("config loads: yes"))
        .stdout(predicate::str::contains("config valid: no"))
        .stdout(predicate::str::contains("OS name must not be empty"))
        .stdout(predicate::str::contains("next steps:"))
        .stdout(predicate::str::contains("config validate"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--config", config_arg, "--json", "doctor",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "\"agent_os_version\": \"{}\"",
            env!("CARGO_PKG_VERSION")
        )))
        .stdout(predicate::str::contains(format!(
            "\"platform\": \"{}\"",
            std::env::consts::OS
        )))
        .stdout(predicate::str::contains("\"service_manager\":"))
        .stdout(predicate::str::contains("\"service_recommendation\":"))
        .stdout(predicate::str::contains("\"shell_execution_supported\":"))
        .stdout(predicate::str::contains("\"shell_execution_note\":"))
        .stdout(predicate::str::contains("\"config_loads\": true"))
        .stdout(predicate::str::contains("\"config_valid\": false"))
        .stdout(predicate::str::contains("\"config_error\": null"))
        .stdout(predicate::str::contains("\"next_steps\": ["))
        .stdout(predicate::str::contains("config validate"))
        .stdout(predicate::str::contains("OS name must not be empty"));
}

#[test]
fn doctor_reports_state_validation_issues() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Diagnose me",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Assigned task"));

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    value["agents"]["builder"]["current_tasks"] = serde_json::json!([]);
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "doctor"])
        .assert()
        .success()
        .stdout(predicate::str::contains("state valid: no"))
        .stdout(predicate::str::contains("state issues:"))
        .stdout(predicate::str::contains("missing from agent current tasks"))
        .stdout(predicate::str::contains("state repair --dry-run"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "doctor"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"state_valid\": false"))
        .stdout(predicate::str::contains("\"next_steps\": ["))
        .stdout(predicate::str::contains("state repair --dry-run"))
        .stdout(predicate::str::contains("missing from agent current tasks"));

    std::fs::write(&state_file, "{not-json").expect("write malformed state");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "doctor"])
        .assert()
        .success()
        .stdout(predicate::str::contains("state loads: no"))
        .stdout(predicate::str::contains("state error:"))
        .stdout(predicate::str::contains("Restore or repair"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "doctor"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"state_loads\": false"))
        .stdout(predicate::str::contains("Restore or repair"))
        .stdout(predicate::str::contains("\"state_error\":"));
}

#[test]
fn invalid_state_update_does_not_persist_partial_mutation() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    value["name"] = serde_json::json!(" ");
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write invalid state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "memory",
            "add",
            "partial mutation",
            "should not persist",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("state validation failed"));

    let after: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    assert!(
        after["memory"]
            .as_array()
            .expect("memory")
            .iter()
            .all(|record| record["topic"] != "partial mutation")
    );
}

#[test]
fn invalid_state_run_does_not_persist_partial_assignment() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Do not assign",
            "--need",
            "rust",
            "--command",
            "printf no",
        ])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let task_id = value["tasks"]
        .as_object()
        .expect("tasks")
        .keys()
        .next()
        .expect("task id")
        .to_owned();
    value["name"] = serde_json::json!(" ");
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write invalid state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("state validation failed"));

    let after: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    assert_eq!(after["tasks"][&task_id]["status"], "pending");
    assert!(after["tasks"][&task_id]["assigned_to"].is_null());
    assert!(after["runs"].as_object().expect("runs").is_empty());
}

#[test]
fn invalid_state_workflow_does_not_persist_partial_tasks() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    value["name"] = serde_json::json!(" ");
    let original_task_count = value["tasks"].as_object().expect("tasks").len();
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write invalid state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "workflow",
            "create",
            "build transactional workflows",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("state validation failed"));

    let after: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    assert_eq!(
        after["tasks"].as_object().expect("tasks").len(),
        original_task_count
    );
}

#[test]
fn state_export_backup_import_and_validate_round_trip() {
    let dir = workspace_tempdir().expect("tempdir");
    let source_state = dir.path().join("source");
    let target_state = dir.path().join("target");
    let export_path = dir.path().join("export.json");
    let backup_path = dir.path().join("backup.json");
    let source_arg = source_state.to_str().expect("source");
    let target_arg = target_state.to_str().expect("target");
    let export_arg = export_path.to_str().expect("export");
    let backup_arg = backup_path.to_str().expect("backup");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", source_arg, "init", "--force"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            source_arg,
            "task",
            "create",
            "Persist me",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", source_arg, "state", "export", "--output", export_arg,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Exported state"));
    assert!(export_path.exists());

    let dry_run_export_path = dir.path().join("dry-run-export.json");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            source_arg,
            "state",
            "export",
            "--output",
            dry_run_export_path.to_str().expect("dry-run export path"),
            "--dry-run",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Would export state"));
    assert!(
        !dry_run_export_path.exists(),
        "dry-run export should not write a file"
    );

    let source_state_file = source_state.join("state.json");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            source_arg,
            "state",
            "export",
            "--output",
            source_state_file.to_str().expect("source state file"),
            "--dry-run",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "export destination must differ from state path",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", source_arg, "state", "export", "--dry-run"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--dry-run requires --output"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", source_arg, "state", "export", "--output", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains("output must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", source_arg, "state", "backup", "--output", backup_arg,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Backed up state"));
    assert!(backup_path.exists());

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", source_arg, "state", "backup", "--output", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains("output must not be empty"));

    let dry_run_backup_path = dir.path().join("dry-run-backup.json");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            source_arg,
            "state",
            "backup",
            "--output",
            dry_run_backup_path.to_str().expect("dry-run backup path"),
            "--dry-run",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Would back up state"));
    assert!(
        !dry_run_backup_path.exists(),
        "dry-run backup should not write a file"
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            source_arg,
            "state",
            "backup",
            "--output",
            source_state_file.to_str().expect("source state file"),
            "--dry-run",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "backup destination must differ from state path",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", source_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));

    let blank_import_target = dir.path().join("blank-import-target");
    let blank_import_target_arg = blank_import_target.to_str().expect("blank target");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", blank_import_target_arg, "state", "import", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains("import path must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            target_arg,
            "state",
            "import",
            export_arg,
            "--dry-run",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Would import"));
    assert!(
        !target_state.join("state.json").exists(),
        "dry-run import should not create target state"
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", target_arg, "state", "import", export_arg])
        .assert()
        .success()
        .stdout(predicate::str::contains("Imported"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", target_arg, "--json", "task", "list", "--all"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Persist me"));
}

#[test]
fn api_can_backup_state_to_requested_path() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let backup_path = dir.path().join("api-backup.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Back me up",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "backup-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "4",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");
    let auth = [("authorization", "Bearer backup-token")];

    let invalid = http_request(addr, "POST", "/state/backup", r#"{"output":"   "}"#, &auth);
    assert!(invalid.contains("HTTP/1.1 400 Bad Request"), "{invalid}");
    assert!(invalid.contains("output must not be empty"), "{invalid}");

    let dry_run_body = serde_json::json!({
        "output": backup_path.display().to_string(),
        "dry_run": true
    })
    .to_string();
    let dry_run = http_request(addr, "POST", "/state/backup", &dry_run_body, &auth);
    assert!(dry_run.contains("HTTP/1.1 200 OK"), "{dry_run}");
    let dry_run_response: Value =
        serde_json::from_str(http_body(&dry_run)).expect("backup dry-run body");
    assert_eq!(dry_run_response["dry_run"], true);
    assert_eq!(
        dry_run_response["backup"].as_str(),
        Some(backup_path.to_str().expect("backup path"))
    );
    assert!(
        !backup_path.exists(),
        "dry-run backup should not write a file"
    );

    let backup_body = serde_json::json!({
        "output": backup_path.display().to_string()
    })
    .to_string();
    let backup = http_request(addr, "POST", "/state/backup", &backup_body, &auth);
    assert!(backup.contains("HTTP/1.1 200 OK"), "{backup}");
    let backup_response: Value = serde_json::from_str(http_body(&backup)).expect("backup body");
    assert_eq!(
        backup_response["backup"].as_str(),
        Some(backup_path.to_str().expect("backup path"))
    );
    assert_eq!(backup_response["dry_run"], false);
    assert!(backup_path.exists());

    let backed_up: Value =
        serde_json::from_str(&std::fs::read_to_string(&backup_path).expect("backup json"))
            .expect("backup state");
    assert_eq!(backed_up["tasks"].as_object().expect("tasks").len(), 1);

    let default_backup = http_request(addr, "POST", "/state/backup", "", &auth);
    assert!(
        default_backup.contains("HTTP/1.1 200 OK"),
        "{default_backup}"
    );
    let default_backup_response: Value =
        serde_json::from_str(http_body(&default_backup)).expect("default backup body");
    assert_eq!(default_backup_response["dry_run"], false);
    let default_backup_path = std::path::PathBuf::from(
        default_backup_response["backup"]
            .as_str()
            .expect("default backup path"),
    );
    assert_eq!(default_backup_path.parent(), Some(state.as_path()));
    let default_backup_name = default_backup_path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("default backup filename");
    assert!(default_backup_name.starts_with("state.backup-"));
    assert!(default_backup_name.ends_with(".json"));
    assert!(default_backup_path.exists());
    let default_backed_up: Value = serde_json::from_str(
        &std::fs::read_to_string(&default_backup_path).expect("default backup json"),
    )
    .expect("default backup state");
    assert_eq!(
        default_backed_up["tasks"].as_object().expect("tasks").len(),
        1
    );

    wait_for_api_success(&mut child);
}

#[test]
fn api_can_import_state_with_force() {
    let dir = workspace_tempdir().expect("tempdir");
    let source_state = dir.path().join("source-agent-os");
    let target_state = dir.path().join("target-agent-os");
    let export_path = dir.path().join("api-import-source.json");
    let source_arg = source_state.to_str().expect("source state");
    let target_arg = target_state.to_str().expect("target state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", source_arg, "init", "--force"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            source_arg,
            "task",
            "create",
            "Import me",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            source_arg,
            "state",
            "export",
            "--output",
            export_path.to_str().expect("export path"),
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", target_arg, "init", "--force"])
        .assert()
        .success();

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "import-token")
        .args([
            "--state",
            target_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "4",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");
    let auth = [("authorization", "Bearer import-token")];

    let invalid = http_request(addr, "POST", "/state/import", r#"{"path":"   "}"#, &auth);
    assert!(invalid.contains("HTTP/1.1 400 Bad Request"), "{invalid}");
    assert!(
        invalid.contains("import path must not be empty"),
        "{invalid}"
    );

    let import_body = serde_json::json!({
        "path": export_path.display().to_string()
    })
    .to_string();
    let conflict = http_request(addr, "POST", "/state/import", &import_body, &auth);
    assert!(conflict.contains("HTTP/1.1 409 Conflict"), "{conflict}");
    assert!(conflict.contains("state conflict"), "{conflict}");

    let dry_run_body = serde_json::json!({
        "path": export_path.display().to_string(),
        "force": true,
        "dry_run": true
    })
    .to_string();
    let dry_run = http_request(addr, "POST", "/state/import", &dry_run_body, &auth);
    assert!(dry_run.contains("HTTP/1.1 200 OK"), "{dry_run}");
    let dry_run_body: Value = serde_json::from_str(http_body(&dry_run)).expect("import dry-run");
    assert_eq!(dry_run_body["dry_run"], true);
    assert_eq!(dry_run_body["validation"]["valid"], true);
    let target_before_import =
        std::fs::read_to_string(target_state.join("state.json")).expect("target before import");
    assert!(
        !target_before_import.contains("Import me"),
        "dry-run import should not overwrite target state"
    );

    let forced_body = serde_json::json!({
        "path": export_path.display().to_string(),
        "force": true
    })
    .to_string();
    let imported = http_request(addr, "POST", "/state/import", &forced_body, &auth);
    assert!(imported.contains("HTTP/1.1 200 OK"), "{imported}");
    let imported_body: Value = serde_json::from_str(http_body(&imported)).expect("import body");
    assert_eq!(imported_body["dry_run"], false);
    assert_eq!(
        imported_body["imported"].as_str(),
        Some(export_path.to_str().expect("export path"))
    );
    assert_eq!(imported_body["validation"]["valid"], true);

    wait_for_api_success(&mut child);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", target_arg, "--json", "task", "list", "--all"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Import me"));
}

#[test]
fn api_can_migrate_state_paths() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let legacy_path = dir.path().join("api-legacy-state.json");
    let migrated_path = dir.path().join("api-migrated-state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "state",
            "export",
            "--output",
            legacy_path.to_str().expect("legacy path"),
        ])
        .assert()
        .success();

    let mut legacy: Value =
        serde_json::from_str(&std::fs::read_to_string(&legacy_path).expect("legacy state"))
            .expect("legacy json");
    legacy
        .as_object_mut()
        .expect("legacy object")
        .remove("version");
    std::fs::write(
        &legacy_path,
        serde_json::to_string_pretty(&legacy).expect("legacy json"),
    )
    .expect("write legacy state");

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "migrate-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "5",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");
    let auth = [("authorization", "Bearer migrate-token")];

    let invalid_input = http_request(addr, "POST", "/state/migrate", r#"{"input":"   "}"#, &auth);
    assert!(
        invalid_input.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_input}"
    );
    assert!(
        invalid_input.contains("input must not be empty"),
        "{invalid_input}"
    );

    let invalid_output = http_request(addr, "POST", "/state/migrate", r#"{"output":"   "}"#, &auth);
    assert!(
        invalid_output.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_output}"
    );
    assert!(
        invalid_output.contains("output must not be empty"),
        "{invalid_output}"
    );

    let active_noop = http_request(addr, "POST", "/state/migrate", "", &auth);
    assert!(active_noop.contains("HTTP/1.1 200 OK"), "{active_noop}");
    let active_noop_body: Value =
        serde_json::from_str(http_body(&active_noop)).expect("active migrate body");
    assert_eq!(active_noop_body["dry_run"], false);
    assert_eq!(
        active_noop_body["input"].as_str(),
        Some(state.join("state.json").to_str().expect("state path"))
    );
    assert_eq!(
        active_noop_body["output"].as_str(),
        Some(state.join("state.json").to_str().expect("state path"))
    );
    assert_eq!(active_noop_body["output_preexisting"], true);
    assert_eq!(active_noop_body["migration"]["changed"], false);
    assert_eq!(active_noop_body["validation"]["valid"], true);

    let dry_run_body = serde_json::json!({
        "input": legacy_path.display().to_string(),
        "output": migrated_path.display().to_string(),
        "dry_run": true
    })
    .to_string();
    let dry_run = http_request(addr, "POST", "/state/migrate", &dry_run_body, &auth);
    assert!(dry_run.contains("HTTP/1.1 200 OK"), "{dry_run}");
    let dry_run_body: Value = serde_json::from_str(http_body(&dry_run)).expect("migrate dry-run");
    assert_eq!(dry_run_body["dry_run"], true);
    assert_eq!(dry_run_body["migration"]["from_version"], 0);
    assert_eq!(dry_run_body["migration"]["to_version"], 4);
    assert_eq!(dry_run_body["output_preexisting"], false);
    assert_eq!(dry_run_body["validation"]["valid"], true);
    assert!(
        !migrated_path.exists(),
        "dry-run should not write migrated output"
    );

    let migrate_body = serde_json::json!({
        "input": legacy_path.display().to_string(),
        "output": migrated_path.display().to_string()
    })
    .to_string();
    let migrated = http_request(addr, "POST", "/state/migrate", &migrate_body, &auth);
    assert!(migrated.contains("HTTP/1.1 200 OK"), "{migrated}");
    let migrated_body: Value = serde_json::from_str(http_body(&migrated)).expect("migrate body");
    assert_eq!(migrated_body["dry_run"], false);
    assert_eq!(migrated_body["migration"]["from_version"], 0);
    assert_eq!(migrated_body["migration"]["to_version"], 4);
    assert_eq!(migrated_body["output_preexisting"], false);
    assert_eq!(migrated_body["validation"]["valid"], true);

    wait_for_api_success(&mut child);

    let migrated_state: Value =
        serde_json::from_str(&std::fs::read_to_string(&migrated_path).expect("migrated state"))
            .expect("migrated json");
    assert_eq!(migrated_state["version"], 4);
}

#[test]
fn api_can_export_state_snapshot() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let export_path = dir.path().join("api-export.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Export me",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "export-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "4",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");
    let auth = [("authorization", "Bearer export-token")];

    let invalid = http_request(addr, "POST", "/state/export", r#"{"output":"   "}"#, &auth);
    assert!(invalid.contains("HTTP/1.1 400 Bad Request"), "{invalid}");
    assert!(invalid.contains("output must not be empty"), "{invalid}");

    let dry_run_body = serde_json::json!({
        "output": export_path.display().to_string(),
        "dry_run": true
    })
    .to_string();
    let dry_run = http_request(addr, "POST", "/state/export", &dry_run_body, &auth);
    assert!(dry_run.contains("HTTP/1.1 200 OK"), "{dry_run}");
    let dry_run_response: Value =
        serde_json::from_str(http_body(&dry_run)).expect("export dry-run body");
    assert_eq!(dry_run_response["dry_run"], true);
    assert_eq!(
        dry_run_response["exported"].as_str(),
        Some(export_path.to_str().expect("export path"))
    );
    assert!(
        !export_path.exists(),
        "dry-run export should not write a file"
    );

    let export_body = serde_json::json!({
        "output": export_path.display().to_string()
    })
    .to_string();
    let exported_to_path = http_request(addr, "POST", "/state/export", &export_body, &auth);
    assert!(
        exported_to_path.contains("HTTP/1.1 200 OK"),
        "{exported_to_path}"
    );
    let exported_to_path_body: Value =
        serde_json::from_str(http_body(&exported_to_path)).expect("export path body");
    assert_eq!(
        exported_to_path_body["exported"].as_str(),
        Some(export_path.to_str().expect("export path"))
    );
    assert_eq!(exported_to_path_body["dry_run"], false);
    assert!(export_path.exists());
    let exported_file: Value =
        serde_json::from_str(&std::fs::read_to_string(&export_path).expect("exported file"))
            .expect("exported file json");
    assert!(exported_file["tasks"].to_string().contains("Export me"));

    let exported = http_get(addr, "/state/export", &auth);
    assert!(exported.contains("HTTP/1.1 200 OK"), "{exported}");
    let exported_body: Value = serde_json::from_str(http_body(&exported)).expect("export body");
    assert_eq!(exported_body["version"], 4);
    assert!(exported_body["tasks"].to_string().contains("Export me"));
    assert!(exported_body["events"].as_array().expect("events").len() >= 2);

    wait_for_api_success(&mut child);
}

#[test]
fn state_repair_resets_empty_os_name() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    value["name"] = serde_json::json!(" ");
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");
    let before_dry_run = std::fs::read_to_string(&state_file).expect("state before dry-run");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("OS name is empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair", "--dry-run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Would repair 1 state issue"))
        .stdout(predicate::str::contains("reset empty OS name to Agent OS"));
    let after_dry_run = std::fs::read_to_string(&state_file).expect("state after dry-run");
    assert_eq!(after_dry_run, before_dry_run);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains("reset empty OS name to Agent OS"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));

    let after: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    assert_eq!(after["name"], "Agent OS");
}

#[test]
fn state_repair_fixes_assignment_index_drift() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Repair me",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Assigned task"));

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    value["agents"]["builder"]["current_tasks"] = serde_json::json!([]);
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("State is invalid"))
        .stdout(predicate::str::contains("missing from agent current tasks"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Repaired 1 state issue"))
        .stdout(predicate::str::contains("added running task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "events", "--limit", "5"])
        .assert()
        .success()
        .stdout(predicate::str::contains("state-repaired"));
}

#[test]
fn state_validate_and_repair_handle_duplicate_current_tasks() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Duplicate index",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Assigned task"));

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let assigned = value["agents"]["builder"]["current_tasks"][0].clone();
    value["agents"]["builder"]["current_tasks"] = serde_json::json!([assigned.clone(), assigned]);
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("duplicate current task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains("removed duplicate current task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn state_repair_raises_zero_agent_parallelism() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    value["agents"]["builder"]["max_parallel_tasks"] = serde_json::json!(0);
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("zero max_parallel_tasks"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "raised agent builder max_parallel_tasks to 1",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn failed_state_repair_does_not_persist_partial_repairs() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "create", "Unrepairable"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let task_id = value["tasks"]
        .as_object()
        .expect("tasks object")
        .keys()
        .next()
        .expect("task id")
        .to_owned();
    value["agents"]["builder"]["max_parallel_tasks"] = serde_json::json!(0);
    value["tasks"][&task_id]["status"] = serde_json::json!("running");
    value["tasks"][&task_id]["assigned_to"] = serde_json::Value::Null;
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("did not persist"))
        .stdout(predicate::str::contains(
            "raised agent builder max_parallel_tasks to 1",
        ))
        .stderr(predicate::str::contains("missing an assigned agent"));

    let after: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    assert_eq!(after["agents"]["builder"]["max_parallel_tasks"], 0);
}

#[test]
fn state_repair_dedupes_duplicate_task_dependencies() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    for title in ["Dependency", "Dependent"] {
        Command::cargo_bin("agent-os")
            .expect("binary")
            .args(["--state", state_arg, "task", "create", title])
            .assert()
            .success();
    }

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let task_ids = value["tasks"]
        .as_object()
        .expect("tasks object")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(task_ids.len(), 2);
    value["tasks"][&task_ids[1]]["dependencies"] =
        serde_json::json!([task_ids[0].clone(), task_ids[0].clone()]);
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("duplicate dependency"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains("removed duplicate dependency"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn state_repair_removes_missing_task_dependencies() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "create", "Missing dependency"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let task_id = value["tasks"]
        .as_object()
        .expect("tasks object")
        .keys()
        .next()
        .expect("task id")
        .to_owned();
    value["tasks"][&task_id]["dependencies"] = serde_json::json!(["missing-task"]);
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("depends on missing task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains("removed missing dependency"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn state_repair_removes_empty_task_plan_steps() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "create", "Plan drift"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let task_id = value["tasks"]
        .as_object()
        .expect("tasks object")
        .keys()
        .next()
        .expect("task id")
        .to_owned();
    value["tasks"][&task_id]["plan"] = serde_json::json!(["first", " ", "second"]);
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted task plan");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("plan contains an empty step"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "removed empty plan step from task {task_id}"
        )));

    let after: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    assert_eq!(
        after["tasks"][&task_id]["plan"],
        serde_json::json!(["first", "second"])
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn state_repair_removes_invalid_workflow_stages() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "workflow",
            "create",
            "Repair workflow",
        ])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let workflow_id = value["workflows"]
        .as_object()
        .expect("workflows object")
        .keys()
        .next()
        .expect("workflow id")
        .to_owned();
    let plan_task_id = value["workflows"][&workflow_id]["tasks"]["plan"]
        .as_str()
        .expect("plan task id")
        .to_owned();
    let build_task_id = value["workflows"][&workflow_id]["tasks"]["build"]
        .as_str()
        .expect("build task id")
        .to_owned();
    let tasks = value["workflows"][&workflow_id]["tasks"]
        .as_object_mut()
        .expect("workflow tasks");
    tasks.insert(" ".into(), Value::from(build_task_id));
    tasks.insert("bad".into(), Value::from("bad task id"));
    tasks.insert("missing".into(), Value::from("missing-task"));
    tasks.insert("zz-duplicate".into(), Value::from(plan_task_id));
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted workflow");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("empty stage name"))
        .stdout(predicate::str::contains("references invalid task id"))
        .stdout(predicate::str::contains("references missing task"))
        .stdout(predicate::str::contains("references duplicate task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "removed empty stage from workflow",
        ))
        .stdout(predicate::str::contains("removed invalid task"))
        .stdout(predicate::str::contains("removed missing task"))
        .stdout(predicate::str::contains("removed duplicate task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn state_repair_fixes_policy_drift() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    value["policy"]["allowed_commands"] = serde_json::json!([" ", "printf"]);
    value["policy"]["allowed_workspaces"] = serde_json::json!([" ", "."]);
    value["policy"]["denied_patterns"] = serde_json::json!(["rm -rf", " "]);
    value["policy"]["allowed_env_vars"] = serde_json::json!(["PATH", " GOOD_ENV ", "bad-env"]);
    value["policy"]["redacted_env_patterns"] = serde_json::json!(["SECRET", " "]);
    value["policy"]["max_output_bytes"] = Value::from(0);
    value["policy"]["command_timeout_seconds"] = Value::from(0);
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted policy");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains(
            "policy allowed_commands contains an empty value",
        ))
        .stdout(predicate::str::contains(
            "policy allowed_env_vars contains an invalid environment variable name",
        ))
        .stdout(predicate::str::contains(
            "policy max_output_bytes must be greater than 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "removed empty policy allowed_commands value",
        ))
        .stdout(predicate::str::contains(
            "trimmed policy allowed_env_vars value",
        ))
        .stdout(predicate::str::contains(
            "removed invalid policy allowed_env_vars value",
        ))
        .stdout(predicate::str::contains("reset policy max_output_bytes"));

    let after: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    assert_eq!(
        after["policy"]["allowed_commands"],
        serde_json::json!(["printf"])
    );
    assert_eq!(
        after["policy"]["allowed_env_vars"],
        serde_json::json!(["PATH", "GOOD_ENV"])
    );
    assert_eq!(
        after["policy"]["redacted_env_patterns"],
        serde_json::json!(["SECRET"])
    );
    assert!(
        after["policy"]["max_output_bytes"]
            .as_u64()
            .expect("max output")
            > 0
    );
    assert!(
        after["policy"]["command_timeout_seconds"]
            .as_u64()
            .expect("timeout")
            > 0
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn state_repair_fixes_provider_drift() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    value["provider"]["model"] = Value::from(" ");
    value["provider"]["api_key_env"] = Value::from(" OPENAI_API_KEY ");
    value["provider"]["endpoint"] = Value::from(" ");
    value["provider"]["max_retries"] = Value::from(u64::from(agent_os::MAX_PROVIDER_RETRIES + 1));
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted provider");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("provider model is empty"))
        .stdout(predicate::str::contains(
            "provider api_key_env must be a valid environment variable name",
        ))
        .stdout(predicate::str::contains("provider endpoint is empty"))
        .stdout(predicate::str::contains(
            "provider max_retries must be less than or equal to 8",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains("reset provider model"))
        .stdout(predicate::str::contains("trimmed provider api_key_env"))
        .stdout(predicate::str::contains("cleared empty provider endpoint"))
        .stdout(predicate::str::contains("reset provider max_retries"));

    let after: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    assert_eq!(after["provider"]["model"], "mock-agent");
    assert_eq!(after["provider"]["api_key_env"], "OPENAI_API_KEY");
    assert!(after["provider"]["endpoint"].is_null());
    assert_eq!(after["provider"]["max_retries"], 2);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn state_repair_clears_missing_assigned_agent_on_finished_task() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "create", "Missing agent"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let task_id = value["tasks"]
        .as_object()
        .expect("tasks object")
        .keys()
        .next()
        .expect("task id")
        .to_owned();
    value["tasks"][&task_id]["status"] = serde_json::json!("complete");
    value["tasks"][&task_id]["assigned_to"] = serde_json::json!("missing-agent");
    value["tasks"][&task_id]["cwd"] = serde_json::json!(" ");
    value["tasks"][&task_id]["output"] = serde_json::json!(" ");
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("assigned to missing agent"))
        .stdout(predicate::str::contains("has empty cwd"))
        .stdout(predicate::str::contains("has empty output"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains("cleared missing assigned agent"))
        .stdout(predicate::str::contains("cleared empty cwd from task"))
        .stdout(predicate::str::contains("cleared empty output from task"));

    let after: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    assert!(after["tasks"][&task_id]["cwd"].is_null());
    assert!(after["tasks"][&task_id]["output"].is_null());

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn state_validate_reports_inconsistent_run_lifecycle() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Inconsistent run",
            "--need",
            "rust",
            "--command",
            "printf ok",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let run_id = value["runs"]
        .as_object()
        .expect("runs object")
        .keys()
        .next()
        .expect("run id")
        .to_owned();
    value["runs"][&run_id]["status"] = serde_json::json!("running");
    value["runs"][&run_id]["exit_code"] = serde_json::json!(0);
    value["runs"][&run_id]["finished_at"] = serde_json::json!("2026-05-11T00:00:00Z");
    value["runs"][&run_id]["started_at"] = serde_json::json!("2026-05-11T00:00:01Z");
    value["runs"][&run_id]["command"] = serde_json::json!(" ");
    value["runs"][&run_id]["log_path"] = serde_json::json!(" ");
    value["runs"][&run_id]["cwd"] = serde_json::json!(" ");
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains(
            "active status running but has finished_at",
        ))
        .stdout(predicate::str::contains(
            "active status running but has exit_code",
        ))
        .stdout(predicate::str::contains("finished before it started"))
        .stdout(predicate::str::contains("has empty command"))
        .stdout(predicate::str::contains("has empty cwd"))
        .stdout(predicate::str::contains("has empty log_path"));
}

#[test]
fn state_repair_fixes_inconsistent_run_lifecycle() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Repair inconsistent run",
            "--need",
            "rust",
            "--command",
            "printf ok",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let run_id = value["runs"]
        .as_object()
        .expect("runs object")
        .keys()
        .next()
        .expect("run id")
        .to_owned();
    value["runs"][&run_id]["status"] = serde_json::json!("running");
    value["runs"][&run_id]["exit_code"] = serde_json::json!(0);
    value["runs"][&run_id]["finished_at"] = serde_json::json!("2026-05-11T00:00:00Z");
    value["runs"][&run_id]["started_at"] = serde_json::json!("2026-05-11T00:00:01Z");
    value["runs"][&run_id]["command"] = serde_json::json!(" ");
    value["runs"][&run_id]["log_path"] = serde_json::json!(" ");
    value["runs"][&run_id]["cwd"] = serde_json::json!(" ");
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Repaired 5 state issue"))
        .stdout(predicate::str::contains(
            "cleared finished_at from active run",
        ))
        .stdout(predicate::str::contains(
            "cleared exit_code from active run",
        ))
        .stdout(predicate::str::contains("restored command for run"))
        .stdout(predicate::str::contains("reset empty cwd from run"))
        .stdout(predicate::str::contains("cleared empty log_path from run"));

    let after: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    assert_eq!(after["runs"][&run_id]["status"], "running");
    assert_eq!(after["runs"][&run_id]["command"], "printf ok");
    assert_eq!(after["runs"][&run_id]["cwd"], ".");
    assert!(after["runs"][&run_id]["log_path"].is_null());
    assert!(after["runs"][&run_id]["exit_code"].is_null());
    assert!(after["runs"][&run_id]["finished_at"].is_null());

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn state_validate_and_repair_run_exit_code_drift() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Exit code drift",
            "--need",
            "rust",
            "--command",
            "printf ok",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let run_id = value["runs"]
        .as_object()
        .expect("runs object")
        .keys()
        .next()
        .expect("run id")
        .to_owned();
    value["runs"][&run_id]["status"] = serde_json::json!("success");
    value["runs"][&run_id]["exit_code"] = serde_json::json!(7);
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write drifted exit code");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains(
            "success status but exit_code is not 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains("set exit_code for successful run"));

    let after: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    assert_eq!(after["runs"][&run_id]["exit_code"], 0);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn state_validate_and_repair_timestamp_order_drift() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "create", "Timestamp drift"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "Timestamp Tool",
            "--command-template",
            "printf hi",
        ])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    value["created_at"] = serde_json::json!("2026-01-02T00:00:00Z");
    value["updated_at"] = serde_json::json!("2026-01-01T00:00:00Z");
    value["agents"]["builder"]["created_at"] = serde_json::json!("2026-01-02T00:00:00Z");
    value["agents"]["builder"]["updated_at"] = serde_json::json!("2026-01-01T00:00:00Z");

    let task_id = value["tasks"]
        .as_object()
        .expect("tasks object")
        .keys()
        .next()
        .expect("task id")
        .to_owned();
    value["tasks"][&task_id]["created_at"] = serde_json::json!("2026-01-02T00:00:00Z");
    value["tasks"][&task_id]["updated_at"] = serde_json::json!("2026-01-01T00:00:00Z");
    value["tools"]["timestamp-tool"]["created_at"] = serde_json::json!("2026-01-02T00:00:00Z");
    value["tools"]["timestamp-tool"]["updated_at"] = serde_json::json!("2026-01-01T00:00:00Z");
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write drifted timestamps");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains(
            "OS updated_at is before created_at",
        ))
        .stdout(predicate::str::contains(
            "agent builder updated_at is before created_at",
        ))
        .stdout(predicate::str::contains(format!(
            "task {task_id} updated_at is before created_at"
        )))
        .stdout(predicate::str::contains(
            "tool timestamp-tool updated_at is before created_at",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "clamped OS updated_at to created_at",
        ))
        .stdout(predicate::str::contains(
            "clamped agent builder updated_at to created_at",
        ))
        .stdout(predicate::str::contains(format!(
            "clamped task {task_id} updated_at to created_at"
        )))
        .stdout(predicate::str::contains(
            "clamped tool timestamp-tool updated_at to created_at",
        ));

    let after: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    assert_eq!(
        after["agents"]["builder"]["updated_at"],
        after["agents"]["builder"]["created_at"]
    );
    assert_eq!(
        after["tasks"][&task_id]["updated_at"],
        after["tasks"][&task_id]["created_at"]
    );
    assert_eq!(
        after["tools"]["timestamp-tool"]["updated_at"],
        after["tools"]["timestamp-tool"]["created_at"]
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn state_repair_fixes_invalid_event_log() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    value["events"] = serde_json::json!([
        {
            "id": "later",
            "kind": "system-booted",
            "message": "later",
            "at": "2026-05-11T00:00:03Z"
        },
        {
            "id": "",
            "kind": "system-booted",
            "message": "missing id",
            "at": "2026-05-11T00:00:04Z"
        },
        {
            "id": "duplicate",
            "kind": "system-booted",
            "message": "first duplicate",
            "at": "2026-05-11T00:00:05Z"
        },
        {
            "id": "duplicate",
            "kind": "system-booted",
            "message": "second duplicate",
            "at": "2026-05-11T00:00:06Z"
        },
        {
            "id": "empty-message",
            "kind": "system-booted",
            "message": " ",
            "at": "2026-05-11T00:00:07Z"
        },
        {
            "id": "earlier",
            "kind": "system-booted",
            "message": "earlier",
            "at": "2026-05-11T00:00:02Z"
        }
    ]);
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted events");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("event has empty id"))
        .stdout(predicate::str::contains("duplicate event id duplicate"))
        .stdout(predicate::str::contains("empty-message has empty message"))
        .stdout(predicate::str::contains("event log is not sorted"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains("removed event with empty id"))
        .stdout(predicate::str::contains(
            "removed duplicate event duplicate",
        ))
        .stdout(predicate::str::contains("sorted events by timestamp"));

    let after: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let events = after["events"].as_array().expect("events");
    assert!(events.iter().all(|event| {
        event["id"]
            .as_str()
            .map(|id| !id.trim().is_empty())
            .unwrap_or(false)
    }));
    assert!(events.iter().all(|event| {
        event["message"]
            .as_str()
            .map(|message| !message.trim().is_empty())
            .unwrap_or(false)
    }));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn state_validate_reports_invalid_persisted_ids() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Invalid id drift",
            "--need",
            "rust",
            "--command",
            "printf ok",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let mut agent = value["agents"]
        .as_object_mut()
        .expect("agents")
        .remove("builder")
        .expect("builder agent");
    agent["id"] = serde_json::json!("bad agent id");
    agent["capabilities"] = serde_json::json!([" Rust ", "rust,code"]);
    value["agents"]
        .as_object_mut()
        .expect("agents")
        .insert("bad agent id".into(), agent);

    let task_id = value["tasks"]
        .as_object()
        .expect("tasks object")
        .keys()
        .next()
        .expect("task id")
        .to_owned();
    let mut task = value["tasks"]
        .as_object_mut()
        .expect("tasks")
        .remove(&task_id)
        .expect("task");
    task["id"] = serde_json::json!("bad task id");
    task["required_capabilities"] = serde_json::json!([" Rust, test ", "test"]);
    value["tasks"]
        .as_object_mut()
        .expect("tasks")
        .insert("bad task id".into(), task);

    let run_id = value["runs"]
        .as_object()
        .expect("runs object")
        .keys()
        .next()
        .expect("run id")
        .to_owned();
    let mut run = value["runs"]
        .as_object_mut()
        .expect("runs")
        .remove(&run_id)
        .expect("run");
    run["id"] = serde_json::json!("bad run id");
    run["task_id"] = serde_json::json!("bad task id");
    run["agent_id"] = serde_json::json!("bad agent id");
    value["runs"]
        .as_object_mut()
        .expect("runs")
        .insert("bad run id".into(), run);
    value["memory"] = serde_json::json!([
        {
            "id": "bad memory id",
            "topic": "Topic",
            "body": "Body",
            "tags": [" Alpha, beta ", "alpha"],
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        },
        {
            "id": "duplicate-memory",
            "topic": "First duplicate",
            "body": "Body",
            "tags": [],
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        },
        {
            "id": "duplicate-memory",
            "topic": "Second duplicate",
            "body": "Body",
            "tags": [],
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        }
    ]);
    value["events"]
        .as_array_mut()
        .expect("events")
        .push(serde_json::json!({
            "id": "bad event id",
            "kind": "system-booted",
            "message": "Bad event id",
            "at": "2999-01-01T00:00:00Z"
        }));

    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write invalid ids");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains(
            "agent map key bad agent id is not a valid id",
        ))
        .stdout(predicate::str::contains(
            "agent bad agent id capabilities are not normalized",
        ))
        .stdout(predicate::str::contains("task bad task id has invalid id"))
        .stdout(predicate::str::contains(
            "task bad task id required_capabilities are not normalized",
        ))
        .stdout(predicate::str::contains("run bad run id has invalid id"))
        .stdout(predicate::str::contains(
            "run bad run id references invalid task id",
        ))
        .stdout(predicate::str::contains(
            "memory record bad memory id has invalid id",
        ))
        .stdout(predicate::str::contains(
            "memory record bad memory id tags are not normalized",
        ))
        .stdout(predicate::str::contains(
            "memory records have duplicate id duplicate-memory",
        ))
        .stdout(predicate::str::contains(
            "event bad event id has invalid id",
        ));
}

#[test]
fn runs_cancel_clears_active_run_terminal_fields() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Cancel drifted run",
            "--need",
            "rust",
            "--command",
            "printf ok",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let run_id = value["runs"]
        .as_object()
        .expect("runs object")
        .keys()
        .next()
        .expect("run id")
        .to_owned();
    value["runs"][&run_id]["status"] = serde_json::json!("running");
    value["runs"][&run_id]["exit_code"] = serde_json::json!(0);
    value["runs"][&run_id]["finished_at"] = serde_json::json!("2026-05-11T00:00:00Z");
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write drifted run");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "cancel", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("Cancel requested"));

    let repeated_cancel = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "runs", "cancel", &run_id])
        .output()
        .expect("json cancel");
    assert!(repeated_cancel.status.success(), "json cancel failed");
    let repeated_cancel_body: Value =
        serde_json::from_slice(&repeated_cancel.stdout).expect("json cancel body");
    assert_eq!(repeated_cancel_body["id"], run_id);
    assert_eq!(repeated_cancel_body["cancel_requested"], true);
    assert_eq!(
        repeated_cancel_body["run"]["status"],
        serde_json::json!("cancel-requested")
    );

    let after: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    assert_eq!(after["runs"][&run_id]["status"], "cancel-requested");
    assert!(after["runs"][&run_id]["exit_code"].is_null());
    assert!(after["runs"][&run_id]["finished_at"].is_null());

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn state_validate_reports_dependency_cycles() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    for title in ["Cycle A", "Cycle B"] {
        Command::cargo_bin("agent-os")
            .expect("binary")
            .args(["--state", state_arg, "task", "create", title])
            .assert()
            .success();
    }

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let task_ids = value["tasks"]
        .as_object()
        .expect("tasks object")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(task_ids.len(), 2);
    value["tasks"][&task_ids[0]]["dependencies"] = serde_json::json!([task_ids[1].clone()]);
    value["tasks"][&task_ids[1]]["dependencies"] = serde_json::json!([task_ids[0].clone()]);
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("dependency cycle detected"));
}

#[test]
fn run_dry_run_reports_dependency_deadlocks() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    for title in ["Cycle A", "Cycle B"] {
        Command::cargo_bin("agent-os")
            .expect("binary")
            .args(["--state", state_arg, "task", "create", title])
            .assert()
            .success();
    }

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let task_ids = value["tasks"]
        .as_object()
        .expect("tasks object")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(task_ids.len(), 2);
    value["tasks"][&task_ids[0]]["dependencies"] = serde_json::json!([task_ids[1].clone()]);
    value["tasks"][&task_ids[1]]["dependencies"] = serde_json::json!([task_ids[0].clone()]);
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");

    let dry_run = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "run", "--dry-run"])
        .output()
        .expect("run dry-run");
    assert!(dry_run.status.success(), "run dry-run failed");
    let dry_run: Value = serde_json::from_slice(&dry_run.stdout).expect("dry-run json");
    assert_eq!(dry_run["dry_run"], true);
    assert_eq!(
        dry_run["scheduler"]["deadlocked_tasks"]
            .as_array()
            .expect("deadlocked tasks")
            .len(),
        2
    );
    assert!(
        dry_run["scheduler"]["notes"]
            .as_array()
            .expect("notes")
            .iter()
            .any(|note| note
                .as_str()
                .expect("note")
                .contains("dependency deadlock detected")),
        "dry-run should include dependency deadlock note: {dry_run}"
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--dry-run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("dependency deadlock detected"));
}

#[test]
fn run_dry_run_reports_unscheduled_task_reasons() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Needs uncommon capability",
            "--need",
            "capability-that-no-agent-has",
        ])
        .assert()
        .success();

    let dry_run = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "run", "--dry-run"])
        .output()
        .expect("run dry-run");
    assert!(dry_run.status.success(), "run dry-run failed");
    let dry_run: Value = serde_json::from_slice(&dry_run.stdout).expect("dry-run json");
    let unscheduled = dry_run["scheduler"]["unscheduled_tasks"]
        .as_array()
        .expect("unscheduled tasks");
    assert_eq!(unscheduled.len(), 1, "{dry_run}");
    assert!(
        unscheduled[0]["reason"]
            .as_str()
            .expect("reason")
            .contains("no agent has required capabilities"),
        "{dry_run}"
    );
    assert!(
        dry_run["scheduler"]["notes"]
            .as_array()
            .expect("notes")
            .iter()
            .any(|note| note
                .as_str()
                .expect("note")
                .contains("task(s) could not be scheduled")),
        "dry-run should include unscheduled note: {dry_run}"
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--dry-run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("task(s) could not be scheduled"));
}

#[test]
fn state_migrate_upgrades_legacy_state_without_version() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let legacy = dir.path().join("legacy.json");
    let state_arg = state.to_str().expect("state");
    let legacy_arg = legacy.to_str().expect("legacy");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let state_file = state.join("state.json");
    let mut current_state: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state")).expect("json");
    current_state
        .as_object_mut()
        .expect("object")
        .remove("version");
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&current_state).expect("json"),
    )
    .expect("write legacy current state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "migrate"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Migrated state from version 0 to 4",
        ))
        .stdout(predicate::str::contains("Migration steps:"))
        .stdout(predicate::str::contains("set missing state version to 1"))
        .stdout(predicate::str::contains(
            "Migrated state validation passed.",
        ))
        .stdout(predicate::str::contains("Downgrade note:"))
        .stdout(predicate::str::contains("Automatic downgrade"));

    let migrated_current: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("migrated state"))
            .expect("json");
    assert_eq!(migrated_current["version"], 4);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "state", "export", "--output", legacy_arg,
        ])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&legacy).expect("legacy")).expect("json");
    value.as_object_mut().expect("object").remove("version");
    std::fs::write(&legacy, serde_json::to_string_pretty(&value).expect("json"))
        .expect("write legacy");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "migrate", "--input", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains("input must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "state", "migrate", "--input", legacy_arg, "--output", "   ",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("output must not be empty"));

    let before_dry_run = std::fs::read_to_string(&legacy).expect("legacy before dry-run");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "state",
            "migrate",
            "--input",
            legacy_arg,
            "--output",
            legacy_arg,
            "--dry-run",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Would migrate state from version 0 to 4",
        ))
        .stdout(predicate::str::contains(
            "Output exists and would be overwritten",
        ))
        .stdout(predicate::str::contains("Migration steps:"))
        .stdout(predicate::str::contains("set missing state version to 1"))
        .stdout(predicate::str::contains(
            "Migrated state validation passed.",
        ))
        .stdout(predicate::str::contains("Downgrade note:"))
        .stdout(predicate::str::contains("pre-migration backup/export"));
    let after_dry_run = std::fs::read_to_string(&legacy).expect("legacy after dry-run");
    assert_eq!(after_dry_run, before_dry_run);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--json", "state", "migrate", "--input", legacy_arg, "--output",
            legacy_arg,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"dry_run\": false"))
        .stdout(predicate::str::contains("\"output_preexisting\": true"))
        .stdout(predicate::str::contains("\"from_version\": 0"))
        .stdout(predicate::str::contains("\"to_version\": 4"))
        .stdout(predicate::str::contains("\"downgrade_notes\""));

    let migrated: Value =
        serde_json::from_str(&std::fs::read_to_string(&legacy).expect("migrated")).expect("json");
    assert_eq!(migrated["version"], 4);

    let mut future = migrated;
    future["version"] = Value::from(u64::MAX);
    std::fs::write(
        &legacy,
        serde_json::to_string_pretty(&future).expect("json"),
    )
    .expect("write future version");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "state", "migrate", "--input", legacy_arg,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "state version 18446744073709551615 is newer",
        ));
}

#[test]
fn state_prune_removes_old_finished_runs_and_logs() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    for index in 0..3 {
        Command::cargo_bin("agent-os")
            .expect("binary")
            .args([
                "--state",
                state_arg,
                "task",
                "create",
                &format!("Prune run {index}"),
                "--need",
                "rust",
                "--command",
                &format!("printf run-{index}"),
            ])
            .assert()
            .success();
        Command::cargo_bin("agent-os")
            .expect("binary")
            .args(["--state", state_arg, "run", "--execute"])
            .assert()
            .success();
    }

    let outside_log = dir.path().join("outside-prune.log");
    std::fs::write(&outside_log, "do not remove").expect("outside log");
    let state_file = state.join("state.json");
    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    for run in value["runs"].as_object_mut().expect("runs").values_mut() {
        run["log_path"] = Value::from(outside_log.display().to_string());
    }
    std::fs::write(
        &state_file,
        serde_json::to_string(&value).expect("json body"),
    )
    .expect("write untrusted log paths");
    let before_dry_run = std::fs::read_to_string(&state_file).expect("state before dry-run");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "state",
            "prune",
            "--keep-runs",
            "1",
            "--keep-events",
            "4",
            "--dry-run",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Would remove 2 run"));

    let after_dry_run = std::fs::read_to_string(&state_file).expect("state after dry-run");
    assert_eq!(after_dry_run, before_dry_run);
    assert_eq!(
        std::fs::read_dir(state.join("runs"))
            .expect("runs dir")
            .count(),
        3
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "state",
            "prune",
            "--keep-runs",
            "1",
            "--keep-events",
            "4",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Removed 2 run"));

    let output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "runs", "list"])
        .output()
        .expect("runs list");
    assert!(output.status.success());
    let runs: Value = serde_json::from_slice(&output.stdout).expect("runs json");
    assert_eq!(runs.as_array().expect("runs array").len(), 1);

    let log_count = std::fs::read_dir(state.join("runs"))
        .expect("runs dir")
        .count();
    assert_eq!(log_count, 1);
    assert!(outside_log.exists());
}

#[test]
fn api_can_prune_old_finished_runs_and_events() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    for index in 0..3 {
        Command::cargo_bin("agent-os")
            .expect("binary")
            .args([
                "--state",
                state_arg,
                "task",
                "create",
                &format!("API prune run {index}"),
                "--need",
                "rust",
                "--command",
                &format!("printf api-prune-{index}"),
            ])
            .assert()
            .success();
        Command::cargo_bin("agent-os")
            .expect("binary")
            .args(["--state", state_arg, "run", "--execute"])
            .assert()
            .success();
    }

    let before_dry_run =
        std::fs::read_to_string(state.join("state.json")).expect("state before dry-run");

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "prune-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "3",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");
    let auth = [("authorization", "Bearer prune-token")];

    let default_prune = http_request(addr, "POST", "/state/prune", "", &auth);
    assert!(default_prune.contains("HTTP/1.1 200 OK"), "{default_prune}");
    let default_prune_body: Value =
        serde_json::from_str(http_body(&default_prune)).expect("default prune body");
    assert_eq!(default_prune_body["dry_run"], false);
    assert_eq!(
        default_prune_body["removed_runs"]
            .as_array()
            .expect("default removed runs")
            .len(),
        0
    );
    assert_eq!(default_prune_body["removed_events"], 0);
    let after_default_prune =
        std::fs::read_to_string(state.join("state.json")).expect("state after default prune");
    assert_eq!(after_default_prune, before_dry_run);

    let dry_run = http_request(
        addr,
        "POST",
        "/state/prune",
        r#"{"keep_runs":1,"keep_events":4,"dry_run":true}"#,
        &auth,
    );
    assert!(dry_run.contains("HTTP/1.1 200 OK"), "{dry_run}");
    let dry_run_body: Value = serde_json::from_str(http_body(&dry_run)).expect("dry-run body");
    assert_eq!(dry_run_body["dry_run"], true);
    assert_eq!(
        dry_run_body["removed_runs"]
            .as_array()
            .expect("dry-run removed runs")
            .len(),
        2
    );
    let after_dry_run =
        std::fs::read_to_string(state.join("state.json")).expect("state after dry-run");
    assert_eq!(after_dry_run, before_dry_run);

    let prune = http_request(
        addr,
        "POST",
        "/state/prune",
        r#"{"keep_runs":1,"keep_events":4}"#,
        &auth,
    );
    assert!(prune.contains("HTTP/1.1 200 OK"), "{prune}");
    let prune_body: Value = serde_json::from_str(http_body(&prune)).expect("prune body");
    assert_eq!(prune_body["dry_run"], false);
    assert_eq!(
        prune_body["removed_runs"]
            .as_array()
            .expect("removed runs")
            .len(),
        2
    );

    wait_for_api_success(&mut child);

    let output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "runs", "list"])
        .output()
        .expect("runs list");
    assert!(output.status.success());
    let runs: Value = serde_json::from_slice(&output.stdout).expect("runs json");
    assert_eq!(runs.as_array().expect("runs array").len(), 1);
}

#[test]
fn workspace_policy_blocks_unapproved_cwd() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let config = dir.path().join("agent-os.toml");
    let allowed = dir.path().join("allowed");
    let denied = dir.path().join("denied");
    std::fs::create_dir_all(&allowed).expect("allowed");
    std::fs::create_dir_all(&denied).expect("denied");
    let state_arg = state.to_str().expect("state");
    let config_arg = config.to_str().expect("config");
    let allowed_arg = allowed.to_str().expect("allowed");
    let denied_arg = denied.to_str().expect("denied");

    std::fs::write(
        &config,
        format!(
            r#"
name = "Workspace OS"

[policy]
allow_shell = true
allowed_workspaces = ["{allowed_arg}"]

[[agents]]
name = "builder"
kind = "builder"
capabilities = ["rust"]
parallel = 1
"#
        ),
    )
    .expect("write config");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--config", config_arg, "init", "--force",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Denied workspace",
            "--need",
            "rust",
            "--command",
            "printf no",
            "--cwd",
            denied_arg,
        ])
        .assert()
        .success();

    let run = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "run", "--execute"])
        .output()
        .expect("json run");
    assert!(run.status.success(), "json run failed");
    let run_body: Value = serde_json::from_slice(&run.stdout).expect("json run body");
    assert_eq!(run_body["dry_run"], false);
    assert_eq!(run_body["runs"].as_array().expect("runs").len(), 0);
    assert!(
        run_body["errors"][0]
            .as_str()
            .expect("run error")
            .contains("outside allowed_workspaces"),
        "{run_body}"
    );
}

#[test]
fn sandbox_writable_paths_block_shell_writes_outside_allowed_paths() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let config = dir.path().join("agent-os.toml");
    let workspace = dir.path().join("workspace");
    let src = workspace.join("src");
    let docs = workspace.join("docs");
    std::fs::create_dir_all(&src).expect("src");
    std::fs::create_dir_all(&docs).expect("docs");
    let state_arg = state.to_str().expect("state");
    let config_arg = config.to_str().expect("config");
    let workspace_arg = workspace.to_str().expect("workspace");
    let src_arg = src.to_str().expect("src");

    std::fs::write(
        &config,
        format!(
            r#"
name = "Sandbox OS"

[policy]
allow_shell = true
allowed_workspaces = ["{workspace_arg}"]

[policy.sandbox]
writable_paths = ["{src_arg}"]

[[agents]]
name = "builder"
kind = "builder"
capabilities = ["rust"]
parallel = 1
"#
        ),
    )
    .expect("write config");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--config", config_arg, "init", "--force",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Denied shell write",
            "--need",
            "rust",
            "--command",
            "touch docs/out.txt",
            "--cwd",
            workspace_arg,
        ])
        .assert()
        .success();

    let run = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "run", "--execute"])
        .output()
        .expect("json run");
    assert!(run.status.success(), "json run failed");
    let run_body: Value = serde_json::from_slice(&run.stdout).expect("json run body");
    assert_eq!(run_body["runs"].as_array().expect("runs").len(), 0);
    assert!(
        run_body["errors"][0]
            .as_str()
            .expect("run error")
            .contains("outside sandbox writable_paths"),
        "{run_body}"
    );
    assert!(!docs.join("out.txt").exists());

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Allowed shell write",
            "--need",
            "rust",
            "--command",
            "touch src/out.txt",
            "--cwd",
            workspace_arg,
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"));
    assert!(src.join("out.txt").exists());
}

#[test]
fn registered_tool_invocation_executes_with_quoted_args() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "say",
            "--need",
            "rust",
            "--description",
            "Print a message",
            "--command-template",
            "printf {message}",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Registered tool say"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "tool",
            "update",
            "say",
            "--description",
            "Print an updated message",
            "--command-template",
            "printf updated:{message}",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"id\": \"say\""))
        .stdout(predicate::str::contains("updated:{message}"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "tool", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("say"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Use tool",
            "--tool",
            "say",
            "--arg",
            "message=hello; false",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_complete\": 1"))
        .stdout(predicate::str::contains("\"tools\": 2"));

    let output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "runs", "list"])
        .output()
        .expect("runs list");
    assert!(output.status.success());
    let runs: Value = serde_json::from_slice(&output.stdout).expect("runs json");
    let run_id = runs[0]["id"].as_str().expect("run id");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "logs", run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("updated:hello; false"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "update",
            "say",
            "--command-template",
            "printf static",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("update is incompatible with task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "unused",
            "--need",
            "rust",
            "--command-template",
            "printf unused",
        ])
        .assert()
        .success();

    let remove_unused = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "tool", "remove", "unused"])
        .output()
        .expect("tool remove json");
    assert!(
        remove_unused.status.success(),
        "tool remove failed: {}",
        String::from_utf8_lossy(&remove_unused.stderr)
    );
    let remove_unused_body: Value =
        serde_json::from_slice(&remove_unused.stdout).expect("tool remove json body");
    assert_eq!(remove_unused_body["id"], "unused");
    assert_eq!(remove_unused_body["removed"], true);
    assert_eq!(remove_unused_body["tool"]["id"], "unused");
    assert_eq!(
        remove_unused_body["tool"]["command_template"],
        "printf unused"
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "tool", "remove", "say"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("tool say is still referenced"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn tool_secret_arg_resolves_from_env_and_is_redacted() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "call-api",
            "--need",
            "rust",
            "--command-template",
            "printf {api_key}",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Use secret tool",
            "--tool",
            "call-api",
            "--secret-arg",
            "api_key=AGENT_TOOL_SECRET",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_TOOL_SECRET", "qz")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"));

    let run_id = first_run_id(state_arg);

    for command in [["runs", "show"], ["runs", "logs"], ["runs", "replay"]] {
        Command::cargo_bin("agent-os")
            .expect("binary")
            .args(["--state", state_arg, command[0], command[1], &run_id])
            .assert()
            .success()
            .stdout(predicate::str::contains("[redacted]"))
            .stdout(predicate::str::contains("qz").not());
    }

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "secrets",
            "register",
            "vault",
            "--kind",
            "env-vault",
            "--reference",
            "AGENT_TEST_VAULT",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Use vault secret tool",
            "--tool",
            "call-api",
            "--secret-arg",
            "api_key=vault:api_key",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_TEST_VAULT", r#"{"api_key":"vault-secret"}"#)
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"));

    let vault_run_id = first_run_id(state_arg);
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "logs", &vault_run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("[redacted]"))
        .stdout(predicate::str::contains("vault-secret").not());
}

#[test]
fn file_tools_read_and_write_inside_allowed_workspace_without_shell() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let config = dir.path().join("agent-os.toml");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let state_arg = state.to_str().expect("state");
    let config_arg = config.to_str().expect("config");
    let workspace_arg = workspace.to_str().expect("workspace");

    std::fs::write(
        &config,
        format!(
            r#"
name = "File Tool OS"

[policy]
allow_shell = false
allowed_workspaces = ["{workspace_arg}"]

[[agents]]
name = "builder"
kind = "builder"
capabilities = ["rust"]
parallel = 1
"#
        ),
    )
    .expect("write config");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--config", config_arg, "init", "--force",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "write-note",
            "--kind",
            "file-write",
            "--need",
            "rust",
            "--command-template",
            "note.txt",
            "--cwd",
            workspace_arg,
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Write note",
            "--tool",
            "write-note",
            "--arg",
            "body=hello from file tool",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"));

    assert_eq!(
        std::fs::read_to_string(workspace.join("note.txt")).expect("note"),
        "hello from file tool"
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "write-secret-note",
            "--kind",
            "file-write",
            "--need",
            "rust",
            "--command-template",
            "secret-note.txt",
            "--cwd",
            workspace_arg,
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Write secret note",
            "--tool",
            "write-secret-note",
            "--secret-arg",
            "body=AGENT_FILE_BODY",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_FILE_BODY", "secret file body")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"));

    assert_eq!(
        std::fs::read_to_string(workspace.join("secret-note.txt")).expect("secret note"),
        "secret file body"
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "write-secret-path",
            "--kind",
            "file-write",
            "--need",
            "rust",
            "--command-template",
            "{name}.txt",
            "--cwd",
            workspace_arg,
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Write secret path",
            "--tool",
            "write-secret-path",
            "--secret-arg",
            "name=AGENT_FILE_NAME",
            "--arg",
            "body=secret path body",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_FILE_NAME", "hidden-note")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"));

    assert_eq!(
        std::fs::read_to_string(workspace.join("hidden-note.txt")).expect("hidden note"),
        "secret path body"
    );

    let output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "runs", "list"])
        .output()
        .expect("runs list");
    assert!(output.status.success());
    let runs: Value = serde_json::from_slice(&output.stdout).expect("runs json");
    let secret_path_run_id = runs
        .as_array()
        .expect("run array")
        .iter()
        .find(|run| {
            run["status"] == "success"
                && run["log_path"].is_string()
                && run["command"]
                    .as_str()
                    .map(|command| {
                        command.starts_with("file-write") && command.contains("[redacted]")
                    })
                    .unwrap_or(false)
        })
        .and_then(|run| run["id"].as_str())
        .expect("secret path run id");

    for command in [["runs", "show"], ["runs", "logs"], ["runs", "replay"]] {
        Command::cargo_bin("agent-os")
            .expect("binary")
            .args([
                "--state",
                state_arg,
                command[0],
                command[1],
                secret_path_run_id,
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("[redacted]"))
            .stdout(predicate::str::contains("hidden-note").not());
    }

    std::fs::write(dir.path().join("hidden-denied.txt"), "outside").expect("outside secret file");
    let outside_cwd_arg = dir.path().to_str().expect("outside cwd");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "read-secret-outside",
            "--kind",
            "file-read",
            "--need",
            "rust",
            "--command-template",
            "{name}.txt",
            "--cwd",
            outside_cwd_arg,
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Read secret outside",
            "--tool",
            "read-secret-outside",
            "--secret-arg",
            "name=AGENT_DENIED_FILE",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_DENIED_FILE", "hidden-denied")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stderr(predicate::str::contains("outside allowed_workspaces"))
        .stderr(predicate::str::contains("[redacted]"))
        .stderr(predicate::str::contains("hidden-denied").not());

    let output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "runs", "list"])
        .output()
        .expect("runs list");
    assert!(output.status.success());
    let runs: Value = serde_json::from_slice(&output.stdout).expect("runs json");
    let rejected_secret_path_run_id = runs
        .as_array()
        .expect("run array")
        .iter()
        .find(|run| {
            run["status"] == "rejected"
                && run["command"]
                    .as_str()
                    .map(|command| {
                        command.starts_with("file-read") && command.contains("[redacted]")
                    })
                    .unwrap_or(false)
        })
        .and_then(|run| run["id"].as_str())
        .expect("rejected secret path run id");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "runs",
            "show",
            rejected_secret_path_run_id,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("[redacted]"))
        .stdout(predicate::str::contains("hidden-denied").not());

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "read-note",
            "--kind",
            "file-read",
            "--need",
            "rust",
            "--command-template",
            "note.txt",
            "--cwd",
            workspace_arg,
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Read note",
            "--tool",
            "read-note",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"));

    let output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "runs", "list"])
        .output()
        .expect("runs list");
    assert!(output.status.success());
    let runs: Value = serde_json::from_slice(&output.stdout).expect("runs json");
    let read_run_id = runs
        .as_array()
        .expect("run array")
        .iter()
        .find(|run| {
            run["status"] == "success"
                && run["log_path"].is_string()
                && run["command"]
                    .as_str()
                    .map(|command| command.starts_with("file-read"))
                    .unwrap_or(false)
        })
        .and_then(|run| run["id"].as_str())
        .expect("read run id");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "logs", read_run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello from file tool"));
}

#[cfg(unix)]
#[test]
fn file_write_tool_rejects_symlink_escape_from_allowed_workspace() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let config = dir.path().join("agent-os.toml");
    let workspace = dir.path().join("workspace");
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(&workspace).expect("workspace");
    std::fs::create_dir_all(&outside).expect("outside");
    let outside_file = outside.join("target.txt");
    std::fs::write(&outside_file, "do not overwrite").expect("outside file");
    std::os::unix::fs::symlink(&outside_file, workspace.join("link.txt")).expect("symlink");
    let state_arg = state.to_str().expect("state");
    let config_arg = config.to_str().expect("config");
    let workspace_arg = workspace.to_str().expect("workspace");

    std::fs::write(
        &config,
        format!(
            r#"
name = "File Tool OS"

[policy]
allow_shell = false
allowed_workspaces = ["{workspace_arg}"]

[[agents]]
name = "builder"
kind = "builder"
capabilities = ["rust"]
parallel = 1
"#
        ),
    )
    .expect("write config");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--config", config_arg, "init", "--force",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "write-link",
            "--kind",
            "file-write",
            "--need",
            "rust",
            "--command-template",
            "link.txt",
            "--cwd",
            workspace_arg,
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Write link",
            "--tool",
            "write-link",
            "--arg",
            "body=escape",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stderr(predicate::str::contains("outside allowed_workspaces"));

    assert_eq!(
        std::fs::read_to_string(outside_file).expect("outside file"),
        "do not overwrite"
    );
}

#[cfg(unix)]
#[test]
fn file_write_tool_follows_allowed_symlink_target_atomically() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let config = dir.path().join("agent-os.toml");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let target_file = workspace.join("target.txt");
    let link_file = workspace.join("link.txt");
    std::fs::write(&target_file, "old").expect("target file");
    std::os::unix::fs::symlink(&target_file, &link_file).expect("symlink");
    let state_arg = state.to_str().expect("state");
    let config_arg = config.to_str().expect("config");
    let workspace_arg = workspace.to_str().expect("workspace");

    std::fs::write(
        &config,
        format!(
            r#"
name = "File Tool OS"

[policy]
allow_shell = false
allowed_workspaces = ["{workspace_arg}"]

[[agents]]
name = "builder"
kind = "builder"
capabilities = ["rust"]
parallel = 1
"#
        ),
    )
    .expect("write config");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--config", config_arg, "init", "--force",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "write-link",
            "--kind",
            "file-write",
            "--need",
            "rust",
            "--command-template",
            "link.txt",
            "--cwd",
            workspace_arg,
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Write allowed link",
            "--tool",
            "write-link",
            "--arg",
            "body=new body",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"));

    assert_eq!(
        std::fs::read_to_string(&target_file).expect("target file"),
        "new body"
    );
    assert!(
        std::fs::symlink_metadata(&link_file)
            .expect("link metadata")
            .file_type()
            .is_symlink()
    );
}

#[test]
fn plain_secret_like_tool_arg_is_rejected_before_persistence() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "call-api",
            "--need",
            "rust",
            "--command-template",
            "printf {api_key}",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Reject secret arg",
            "--tool",
            "call-api",
            "--arg",
            "api_key=dummy-tool-secret",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("use --secret-arg api_key=ENV_VAR"));
}

#[test]
fn tool_task_missing_arg_is_blocked_and_releases_capacity() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "needs-message",
            "--need",
            "rust",
            "--command-template",
            "printf {message}",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Missing arg",
            "--tool",
            "needs-message",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "missing required tool argument `message`",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_blocked\": 1"))
        .stdout(predicate::str::contains("\"tasks_running\": 0"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "agent", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("0/2"));
}

#[test]
fn metrics_command_reports_counter_snapshot() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "init", "--force"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state,
            "task",
            "create",
            "Inspect metrics",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    let output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "--json", "metrics"])
        .output()
        .expect("metrics json");
    assert!(output.status.success(), "metrics command failed");
    let metrics: Value = serde_json::from_slice(&output.stdout).expect("metrics json");
    assert_eq!(metrics["ok"], true);
    assert_eq!(metrics["agent_os_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(metrics["state_loads"], true);
    assert_eq!(metrics["state_valid"], true);
    assert_eq!(metrics["state_issue_count"], 0);
    assert!(metrics["agents_total"].as_u64().expect("agents_total") >= 1);
    assert!(metrics["agents_online"].as_u64().expect("agents_online") >= 1);
    assert_eq!(metrics["tasks_total"], 1);
    assert_eq!(metrics["tasks_pending"], 1);
    assert_eq!(metrics["task_queue_age_ms_count"], 1);
    assert!(metrics["oldest_queued_task_age_ms"].as_u64().is_some());
    assert_eq!(metrics["task_queue_age_ms_buckets"]["le_inf"], 1);
    assert_eq!(metrics["runs_total"], 0);
    assert_eq!(metrics["oldest_active_run_age_ms"], 0);
    assert_eq!(metrics["run_duration_ms_count"], 0);
    assert_eq!(metrics["run_duration_ms_sum"], 0);
    assert_eq!(metrics["run_duration_ms_buckets"]["le_inf"], 0);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "metrics"])
        .assert()
        .success()
        .stdout(predicate::str::contains("agent-os metrics"))
        .stdout(predicate::str::contains("agents:"))
        .stdout(predicate::str::contains("tasks: 1 total"))
        .stdout(predicate::str::contains("oldest queued"))
        .stdout(predicate::str::contains("oldest active"))
        .stdout(predicate::str::contains("task queue ages:"))
        .stdout(predicate::str::contains("run durations:"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state, "metrics", "--prometheus"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "# TYPE agent_os_tasks_total gauge",
        ))
        .stdout(predicate::str::contains("agent_os_tasks_total 1"))
        .stdout(predicate::str::contains(
            "agent_os_oldest_active_run_age_ms 0",
        ))
        .stdout(predicate::str::contains(
            "agent_os_task_queue_age_ms_bucket{le=\"+Inf\"} 1",
        ))
        .stdout(predicate::str::contains(
            "agent_os_run_duration_ms_bucket{le=\"+Inf\"} 0",
        ));

    let missing_state = dir.path().join("missing-agent-os");
    let missing_state = missing_state.to_str().expect("missing state");
    let output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", missing_state, "--json", "metrics"])
        .output()
        .expect("missing metrics json");
    assert!(
        output.status.success(),
        "metrics should report unavailable state as JSON"
    );
    let unavailable: Value =
        serde_json::from_slice(&output.stdout).expect("unavailable metrics json");
    assert_eq!(unavailable["ok"], false);
    assert_eq!(unavailable["agent_os_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(unavailable["state_loads"], false);
    assert_eq!(unavailable["state_valid"], Value::Null);
    assert_eq!(unavailable["oldest_active_run_age_ms"], 0);
    assert_eq!(unavailable["oldest_queued_task_age_ms"], 0);
    assert_eq!(unavailable["task_queue_age_ms_count"], 0);
    assert_eq!(unavailable["task_queue_age_ms_buckets"]["le_inf"], 0);
    assert_eq!(unavailable["run_duration_ms_count"], 0);
    assert_eq!(unavailable["run_duration_ms_buckets"]["le_inf"], 0);
    assert!(
        unavailable["state_error"]
            .as_str()
            .expect("state error")
            .contains("io error"),
        "{unavailable}"
    );
}

#[test]
fn execute_limit_runs_commands_in_parallel() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");
    let marker = dir.path().join("parallel.log");
    let marker_arg = marker.to_str().expect("marker path");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    for (title, label) in [("Parallel one", "one"), ("Parallel two", "two")] {
        let command = format!(
            "printf '{label}-start\\n' >> '{marker_arg}'; sleep 0.8; printf '{label}-end\\n' >> '{marker_arg}'"
        );
        Command::cargo_bin("agent-os")
            .expect("binary")
            .args([
                "--state",
                state_arg,
                "task",
                "create",
                title,
                "--need",
                "rust",
                "--command",
                &command,
            ])
            .assert()
            .success();
    }

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--limit", "2", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Executed run").count(2));

    let marker_body = std::fs::read_to_string(&marker).expect("parallel marker");
    let one_start = marker_body.find("one-start").expect("one start");
    let two_start = marker_body.find("two-start").expect("two start");
    let one_end = marker_body.find("one-end").expect("one end");
    let two_end = marker_body.find("two-end").expect("two end");
    assert!(
        one_start.max(two_start) < one_end.min(two_end),
        "expected both tasks to start before either ended; marker log:\n{marker_body}"
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_complete\": 2"))
        .stdout(predicate::str::contains("\"runs\": 2"));
}

#[test]
fn execute_non_command_task_uses_mock_provider() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Design provider runtime",
            "--need",
            "plan",
            "--objective",
            "Create a deterministic provider abstraction",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Executed run"))
        .stdout(predicate::str::contains("success"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_complete\": 1"))
        .stdout(predicate::str::contains("\"runs\": 1"));

    let output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "task", "list", "--all"])
        .output()
        .expect("task list");
    assert!(output.status.success());
    let tasks: Value = serde_json::from_slice(&output.stdout).expect("tasks json");
    let task_id = tasks[0]["id"].as_str().expect("task id");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "show", task_id])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "completed `Design provider runtime`",
        ));
}

#[test]
fn execute_non_command_task_uses_provider_plugin() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let config = dir.path().join("agent-os.toml");
    let state_arg = state.to_str().expect("state");
    let config_arg = config.to_str().expect("config");

    std::fs::write(
        &config,
        r#"
name = "Plugin Provider OS"

[provider]
kind = "plugin"
model = "plugin-model"
plugin_command = "sh"
plugin_args = [
  "-c",
  "read request; case \"$request\" in *Plugin*) printf '{\"summary\":\"plugin provider completed\",\"plan\":[\"plugin step\"],\"confidence\":92}' ;; *) printf 'bad request' >&2; exit 3 ;; esac"
]

[policy]
allow_shell = true
allowed_workspaces = ["."]
"#,
    )
    .expect("write plugin config");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--config", config_arg, "init", "--force",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Plugin provider task",
            "--objective",
            "Exercise provider plugin contract",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Executed run"))
        .stdout(predicate::str::contains("success"));

    let output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "task", "list", "--all"])
        .output()
        .expect("task list");
    assert!(output.status.success());
    let tasks: Value = serde_json::from_slice(&output.stdout).expect("tasks json");
    let task_id = tasks[0]["id"].as_str().expect("task id");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "show", task_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("plugin provider completed"))
        .stdout(predicate::str::contains("plugin step"));
}

#[test]
fn daemon_run_ticks_and_persists_status() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "daemon",
            "run",
            "--max-ticks",
            "1",
            "--interval-ms",
            "0",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "interval_ms must be greater than 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "service", "launchd", "--label", ""])
        .assert()
        .failure()
        .stderr(predicate::str::contains("service label must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "service",
            "launchd",
            "--bin-path",
            "   ",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("bin_path must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "daemon",
            "run",
            "--max-ticks",
            "0",
            "--interval-ms",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("max_ticks must be greater than 0"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Daemon provider task",
            "--need",
            "plan",
            "--objective",
            "Exercise daemon mode",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "daemon",
            "run",
            "--execute",
            "--max-ticks",
            "1",
            "--interval-ms",
            "1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("tick 1 assigned 1 executed 1"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "daemon", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("daemon: stopped"))
        .stdout(predicate::str::contains("ticks: 1"));

    let daemon_status = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "daemon", "status"])
        .output()
        .expect("daemon status json");
    assert!(daemon_status.status.success(), "daemon status failed");
    let daemon_status: Value =
        serde_json::from_slice(&daemon_status.stdout).expect("daemon status json body");
    assert_eq!(daemon_status["daemon"]["status"], "stopped");
    assert_eq!(daemon_status["daemon"]["ticks"], 1);

    let daemon_run = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "daemon",
            "run",
            "--max-ticks",
            "2",
            "--interval-ms",
            "1",
        ])
        .output()
        .expect("daemon run json");
    assert!(daemon_run.status.success(), "daemon run json failed");
    let daemon_run: Value =
        serde_json::from_slice(&daemon_run.stdout).expect("daemon run json body");
    assert_eq!(daemon_run["tick_count"], 2);
    assert_eq!(daemon_run["totals"]["assigned"], 0);
    assert_eq!(daemon_run["totals"]["executed"], 0);
    assert_eq!(daemon_run["totals"]["recovered"], 0);
    assert_eq!(daemon_run["totals"]["errors"], 0);
    assert_eq!(daemon_run["ticks_truncated"], false);
    assert!(
        daemon_run["tick_history_limit"]
            .as_u64()
            .expect("history limit")
            > 0
    );
    assert_eq!(
        daemon_run["ticks"].as_array().expect("tick reports").len(),
        2
    );
    assert_eq!(daemon_run["ticks"][0]["tick"], 1);
    assert_eq!(daemon_run["ticks"][1]["tick"], 2);
    assert_eq!(daemon_run["daemon"]["status"], "stopped");
    assert_eq!(daemon_run["daemon"]["ticks"], 2);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "\"agent_os_version\": \"{}\"",
            env!("CARGO_PKG_VERSION")
        )))
        .stdout(predicate::str::contains("\"daemon_status\": \"stopped\""))
        .stdout(predicate::str::contains("\"tasks_complete\": 1"));

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    value["daemon"]["pid"] = serde_json::json!(12345);
    value["daemon"]["stop_requested"] = serde_json::json!(true);
    value["daemon"]["limit"] = serde_json::json!(0);
    value["daemon"]["last_message"] = serde_json::json!(" ");
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .failure()
        .stdout(predicate::str::contains(
            "daemon is stopped but still has pid",
        ))
        .stdout(predicate::str::contains(
            "daemon is stopped but stop request is still set",
        ))
        .stdout(predicate::str::contains(
            "daemon limit must be greater than 0",
        ))
        .stdout(predicate::str::contains("daemon last_message is empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "repair"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "cleared pid from stopped daemon state",
        ))
        .stdout(predicate::str::contains(
            "cleared stop request from stopped daemon state",
        ))
        .stdout(predicate::str::contains("raised daemon limit to 1"))
        .stdout(predicate::str::contains(
            "cleared empty daemon last_message",
        ));

    let after: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    assert_eq!(after["daemon"]["limit"], 1);
    assert!(after["daemon"]["last_message"].is_null());

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State is valid"));
}

#[test]
fn daemon_soak_processes_multiple_ticks_and_tasks() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    for index in 0..4 {
        Command::cargo_bin("agent-os")
            .expect("binary")
            .args([
                "--state",
                state_arg,
                "task",
                "create",
                &format!("Daemon soak task {index}"),
                "--need",
                "rust",
                "--command",
                &format!("printf soak-{index}"),
            ])
            .assert()
            .success();
    }

    let daemon_run = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "daemon",
            "run",
            "--execute",
            "--limit",
            "2",
            "--max-ticks",
            "5",
            "--interval-ms",
            "5",
        ])
        .output()
        .expect("daemon soak");
    assert!(daemon_run.status.success(), "daemon soak failed");
    let daemon_run: Value = serde_json::from_slice(&daemon_run.stdout).expect("daemon soak json");
    assert_eq!(daemon_run["tick_count"], 5);
    assert_eq!(daemon_run["totals"]["assigned"], 4);
    assert_eq!(daemon_run["totals"]["executed"], 4);
    assert_eq!(daemon_run["totals"]["errors"], 0);
    assert_eq!(daemon_run["daemon"]["status"], "stopped");
    assert_eq!(daemon_run["daemon"]["ticks"], 5);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_complete\": 4"))
        .stdout(predicate::str::contains("\"runs\": 4"));
}

#[test]
fn daemon_stop_requests_running_daemon_to_exit() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "0",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "max_requests must be greater than 0",
        ));

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "daemon",
            "run",
            "--max-ticks",
            "50",
            "--interval-ms",
            "50",
        ])
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn daemon");

    wait_for_daemon_status(state_arg, "daemon: running");

    let stop = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "daemon", "stop"])
        .output()
        .expect("daemon stop");
    assert!(stop.status.success(), "daemon stop failed");
    let stop_body: Value = serde_json::from_slice(&stop.stdout).expect("daemon stop json");
    assert_eq!(stop_body["stop_requested"], true);
    assert_eq!(stop_body["daemon"]["status"], "running");
    assert_eq!(stop_body["daemon"]["stop_requested"], true);

    let status = child.wait().expect("daemon wait");
    assert!(status.success(), "daemon process failed with {status}");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "daemon", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("daemon: stopped"))
        .stdout(predicate::str::contains("stop requested: no"));
}

#[test]
fn api_can_read_and_stop_daemon() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let mut daemon = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "daemon",
            "run",
            "--max-ticks",
            "50",
            "--interval-ms",
            "50",
        ])
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn daemon");

    wait_for_daemon_status(state_arg, "daemon: running");

    let mut api = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "daemon-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "2",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = api.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");
    let auth = [("authorization", "Bearer daemon-token")];

    let status = http_get(addr, "/daemon", &auth);
    assert!(status.contains("HTTP/1.1 200 OK"), "{status}");
    assert!(status.contains("\"status\":\"running\""), "{status}");
    assert!(status.contains("\"stop_requested\":false"), "{status}");

    let stop = http_request(addr, "POST", "/daemon/stop", "", &auth);
    assert!(stop.contains("HTTP/1.1 200 OK"), "{stop}");
    assert!(stop.contains("\"stop_requested\":true"), "{stop}");

    wait_for_api_success(&mut api);

    let daemon_status = daemon.wait().expect("daemon wait");
    assert!(
        daemon_status.success(),
        "daemon process failed with {daemon_status}"
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "daemon", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("daemon: stopped"))
        .stdout(predicate::str::contains("stop requested: no"));
}

#[test]
fn service_launchd_renders_plist_manifest() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let plist = dir.path().join("com.example.agent-os.plist");
    let state_arg = state.to_str().expect("state");
    let plist_arg = plist.to_str().expect("plist");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "service",
            "launchd",
            "--interval-ms",
            "0",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "interval_ms must be greater than 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "service",
            "launchd",
            "--bin-path",
            "/usr/local/bin/agent-os",
            "--label",
            "com.example.agent-os",
            "--interval-ms",
            "250",
            "--limit",
            "3",
            "--execute",
            "--recover-stale-seconds",
            "60",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("<key>Label</key>"))
        .stdout(predicate::str::contains("com.example.agent-os"))
        .stdout(predicate::str::contains("/usr/local/bin/agent-os"))
        .stdout(predicate::str::contains("--state"))
        .stdout(predicate::str::contains(state_arg))
        .stdout(predicate::str::contains("--execute"))
        .stdout(predicate::str::contains("--recover-stale-seconds"))
        .stdout(predicate::str::contains("daemon.out.log"));

    let output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "service",
            "launchd",
            "--bin-path",
            "/usr/local/bin/agent-os",
            "--label",
            "com.example.agent-os",
            "--plist-path",
            plist_arg,
        ])
        .output()
        .expect("service launchd json");
    assert!(output.status.success(), "service launchd json failed");
    let response: Value = serde_json::from_slice(&output.stdout).expect("service launchd json");
    assert_eq!(response["platform"], "launchd");
    assert_eq!(response["plist_path"], plist_arg);
    assert_eq!(response["service"]["label"], "com.example.agent-os");
    assert!(
        response["plist"]
            .as_str()
            .expect("plist")
            .contains("<key>ProgramArguments</key>")
    );
}

#[test]
fn git_cli_reports_status_and_creates_review_task() {
    let dir = workspace_tempdir().expect("tempdir");
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo).expect("repo dir");
    let init = std::process::Command::new("git")
        .arg("init")
        .current_dir(&repo)
        .output()
        .expect("git init");
    assert!(init.status.success(), "git init failed");
    std::fs::write(repo.join("lib.rs"), "fn main() {}\n").expect("write file");

    let status = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--json",
            "git",
            "status",
            "--cwd",
            repo.to_str().expect("repo path"),
        ])
        .output()
        .expect("git status");
    assert!(status.status.success(), "agent-os git status failed");
    let status: Value = serde_json::from_slice(&status.stdout).expect("status json");
    assert_eq!(status["command"][0], "git");
    assert!(
        status["stdout"]
            .as_str()
            .expect("stdout")
            .contains("lib.rs")
    );

    let mut mcp = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args(["mcp", "serve", "--max-requests", "3"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn mcp git status");
    let mut stdin = mcp.stdin.take().expect("mcp stdin");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":1,"method":"initialize","params":{{}}}}"#
    )
    .expect("write mcp initialize");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{{}}}}"#
    )
    .expect("write mcp tools list");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{{"name":"agent_os_git_status","arguments":{{"cwd":{}}}}}}}"#,
        serde_json::to_string(repo.to_str().expect("repo path")).expect("repo json")
    )
    .expect("write mcp git status");
    drop(stdin);
    let output = mcp.wait_with_output().expect("mcp git status output");
    assert!(output.status.success(), "mcp git status failed");
    let stdout = String::from_utf8(output.stdout).expect("mcp stdout");
    let responses = stdout
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("mcp response json"))
        .collect::<Vec<_>>();
    assert_eq!(responses.len(), 3);
    assert!(
        responses[1]["result"]["tools"]
            .as_array()
            .expect("tools")
            .iter()
            .any(|tool| tool["name"] == "agent_os_git_status")
    );
    assert_eq!(responses[2]["result"]["isError"], false);
    let git_status_text = responses[2]["result"]["content"][0]["text"]
        .as_str()
        .expect("git status text");
    let git_status: Value = serde_json::from_str(git_status_text).expect("git status json");
    assert_eq!(
        git_status["command"],
        serde_json::json!(["git", "status", "--short", "--branch"])
    );
    assert!(
        git_status["stdout"]
            .as_str()
            .expect("mcp git stdout")
            .contains("lib.rs")
    );

    let pr = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--json",
            "git",
            "pr",
            "--title",
            "Review",
            "--body",
            "Body",
            "--base",
            "main",
            "--head",
            "codex/test",
            "--dry-run",
            "--cwd",
            repo.to_str().expect("repo path"),
        ])
        .output()
        .expect("git pr dry run");
    assert!(pr.status.success(), "agent-os git pr dry-run failed");
    let pr: Value = serde_json::from_slice(&pr.stdout).expect("pr json");
    assert_eq!(pr["dry_run"], true);
    assert_eq!(pr["command"][0], "gh");
    assert_eq!(pr["command"][1], "pr");

    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state path");
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let review = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "git",
            "review-task",
            "--cwd",
            repo.to_str().expect("repo path"),
            "--base",
            "main",
        ])
        .output()
        .expect("git review task");
    assert!(review.status.success(), "agent-os git review-task failed");
    let review: Value = serde_json::from_slice(&review.stdout).expect("review json");
    assert_eq!(review["task"]["required_capabilities"][0], "review");
    assert!(
        review["task"]["command"]
            .as_str()
            .expect("review command")
            .contains("git diff --stat main...HEAD")
    );
}

#[test]
fn registry_cli_installs_profiles_templates_and_mcp_servers() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state path");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let registry = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "registry", "list"])
        .output()
        .expect("registry list");
    assert!(registry.status.success(), "agent-os registry list failed");
    let registry: Value = serde_json::from_slice(&registry.stdout).expect("registry json");
    assert_eq!(
        registry["agent_profiles"]["rust-project-maintainer"]["kind"],
        "builder"
    );
    assert_eq!(
        registry["workflow_templates"]["ci-fix"]["stages"][0],
        "reproduce"
    );

    let marketplace = dir.path().join("marketplace.json");
    std::fs::write(
        &marketplace,
        r#"
{
  "metadata": {
    "id": "market-core",
    "version": "1.2.3",
    "publisher": "Agent OS test marketplace",
    "homepage": "https://example.invalid/agent-os-market"
  },
  "agent_profiles": [
    {
      "id": "market-reviewer",
      "name": "Marketplace reviewer",
      "kind": "reviewer",
      "model": "market-model",
      "capabilities": ["review", "market"],
      "system_prompt": "Review imported marketplace work."
    }
  ],
  "workflow_templates": [
    {
      "id": "market-release",
      "name": "Marketplace release",
      "description": "Imported release workflow.",
      "stages": ["plan", "build", "ship"],
      "tasks": [
        {
          "stage": "build",
          "title": "Build {objective}",
          "objective": "Compile release for {objective}",
          "command": "printf build-{objective}",
          "capabilities": ["rust"]
        },
        {
          "stage": "ship",
          "title": "Ship {objective}",
          "objective": "Publish release for {objective}",
          "capabilities": ["ops"]
        }
      ],
      "edges": [
        {"from": "plan", "to": "build"},
        {"from": "build", "to": "ship"}
      ]
    }
  ],
  "mcp_servers": [
    {
      "id": "market-mcp",
      "command": "market-mcp",
      "args": ["--stdio"],
      "env": {"MARKET_TOKEN": "token"},
      "enabled": true
    }
  ]
}
"#,
    )
    .expect("write marketplace manifest");
    let marketplace_arg = marketplace.to_str().expect("marketplace path");

    let import = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "registry",
            "marketplace-import",
            marketplace_arg,
        ])
        .output()
        .expect("marketplace import");
    assert!(
        import.status.success(),
        "agent-os registry marketplace-import failed"
    );
    let import: Value = serde_json::from_slice(&import.stdout).expect("marketplace import json");
    assert_eq!(import["imported_agent_profiles"], 1);
    assert_eq!(import["imported_workflow_templates"], 1);
    assert_eq!(import["imported_mcp_servers"], 1);
    assert_eq!(import["overwritten"], 0);
    assert_eq!(import["manifest_id"], "market-core");
    assert_eq!(import["manifest_version"], "1.2.3");
    let marketplace_checksum = import["checksum"]
        .as_str()
        .expect("marketplace checksum")
        .to_owned();
    assert!(marketplace_checksum.starts_with("fnv1a64:"));
    assert_eq!(import["verified_checksum"], false);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "registry",
            "marketplace-import",
            marketplace_arg,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "agent profile already exists: market-reviewer",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "registry",
            "marketplace-import",
            marketplace_arg,
            "--expect-checksum",
            "fnv1a64:deadbeef",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("marketplace checksum mismatch"));

    let forced_import = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "registry",
            "marketplace-import",
            marketplace_arg,
            "--expect-checksum",
            marketplace_checksum.as_str(),
            "--force",
        ])
        .output()
        .expect("forced marketplace import");
    assert!(
        forced_import.status.success(),
        "agent-os registry marketplace-import --force failed"
    );
    let forced_import: Value =
        serde_json::from_slice(&forced_import.stdout).expect("forced marketplace import json");
    assert_eq!(forced_import["overwritten"], 3);
    assert_eq!(forced_import["verified_checksum"], true);

    let imported_registry = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "registry", "list"])
        .output()
        .expect("registry list after marketplace import");
    assert!(
        imported_registry.status.success(),
        "agent-os registry list after import failed"
    );
    let imported_registry: Value =
        serde_json::from_slice(&imported_registry.stdout).expect("imported registry json");
    assert_eq!(
        imported_registry["agent_profiles"]["market-reviewer"]["model"],
        "market-model"
    );
    assert_eq!(
        imported_registry["workflow_templates"]["market-release"]["stages"][2],
        "ship"
    );
    assert_eq!(
        imported_registry["workflow_templates"]["market-release"]["tasks"][0]["command"],
        "printf build-{objective}"
    );
    assert_eq!(
        imported_registry["workflow_templates"]["market-release"]["edges"][1]["to"],
        "ship"
    );
    assert_eq!(
        imported_registry["mcp_servers"]["market-mcp"]["enabled"],
        true
    );

    let install = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "registry",
            "install-agent",
            "rust-project-maintainer",
            "--name",
            "Maintainer",
            "--model",
            "local-model",
            "--parallel",
            "2",
        ])
        .output()
        .expect("install registry agent");
    assert!(
        install.status.success(),
        "agent-os registry install-agent failed"
    );
    let install: Value = serde_json::from_slice(&install.stdout).expect("install json");
    assert_eq!(install["agent"]["name"], "Maintainer");
    assert_eq!(install["agent"]["model"], "local-model");
    assert_eq!(install["agent"]["max_parallel_tasks"], 2);
    assert_eq!(install["agent"]["capabilities"][0], "code");

    let workflow = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "registry",
            "create-workflow",
            "ci-fix",
            "Fix failing CI",
            "--priority",
            "high",
        ])
        .output()
        .expect("create registry workflow");
    assert!(
        workflow.status.success(),
        "agent-os registry create-workflow failed"
    );
    let workflow: Value = serde_json::from_slice(&workflow.stdout).expect("workflow json");
    assert_eq!(workflow["template"], "ci-fix");
    assert_eq!(workflow["workflow"]["priority"], "high");
    assert!(workflow["tasks"]["reproduce"].is_string());
    assert!(workflow["tasks"]["patch"].is_string());
    assert!(workflow["tasks"]["verify"].is_string());
    assert!(workflow["tasks"]["summarize"].is_string());

    let templated_workflow = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "registry",
            "create-workflow",
            "market-release",
            "v1",
        ])
        .output()
        .expect("create marketplace workflow");
    assert!(
        templated_workflow.status.success(),
        "agent-os registry create-workflow for imported template failed"
    );
    let templated_workflow: Value =
        serde_json::from_slice(&templated_workflow.stdout).expect("templated workflow json");
    let build_task_id = templated_workflow["tasks"]["build"]
        .as_str()
        .expect("build task id");
    let ship_task_id = templated_workflow["tasks"]["ship"]
        .as_str()
        .expect("ship task id");
    let state_body = std::fs::read_to_string(state.join("state.json")).expect("state json");
    let state_json: Value = serde_json::from_str(&state_body).expect("state json");
    assert_eq!(state_json["tasks"][build_task_id]["title"], "Build v1");
    assert_eq!(
        state_json["tasks"][build_task_id]["objective"],
        "Compile release for v1"
    );
    assert_eq!(
        state_json["tasks"][build_task_id]["command"],
        "printf build-v1"
    );
    assert_eq!(
        state_json["tasks"][build_task_id]["required_capabilities"][0],
        "rust"
    );
    assert_eq!(
        state_json["tasks"][ship_task_id]["dependencies"][0],
        build_task_id
    );

    let mcp = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "registry",
            "mcp-add",
            "local-tools",
            "--command",
            "agent-os-mcp",
            "--arg=--stdio",
            "--env",
            "AGENT_OS_TOKEN=test-token",
            "--disabled",
        ])
        .output()
        .expect("mcp add");
    assert!(mcp.status.success(), "agent-os registry mcp-add failed");
    let mcp: Value = serde_json::from_slice(&mcp.stdout).expect("mcp json");
    assert_eq!(mcp["id"], "local-tools");
    assert_eq!(mcp["args"][0], "--stdio");
    assert_eq!(mcp["env"]["AGENT_OS_TOKEN"], "test-token");
    assert_eq!(mcp["enabled"], false);

    let enable = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "registry",
            "mcp-enable",
            "local-tools",
        ])
        .output()
        .expect("mcp enable");
    assert!(
        enable.status.success(),
        "agent-os registry mcp-enable failed"
    );
    let enable: Value = serde_json::from_slice(&enable.stdout).expect("enable json");
    assert_eq!(enable["enabled"], true);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "registry",
            "mcp-disable",
            "local-tools",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Disabled MCP server local-tools"));

    let mcp_list = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "registry", "mcp-list"])
        .output()
        .expect("mcp list");
    assert!(
        mcp_list.status.success(),
        "agent-os registry mcp-list failed"
    );
    let mcp_list: Value = serde_json::from_slice(&mcp_list.stdout).expect("mcp-list json");
    assert_eq!(mcp_list["local-tools"]["enabled"], false);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "registry",
            "mcp-remove",
            "local-tools",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Removed MCP server local-tools"));

    let events = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "events",
            "--kind",
            "mcp-server-removed",
        ])
        .output()
        .expect("mcp removed event");
    assert!(events.status.success(), "agent-os events failed");
    let events = String::from_utf8(events.stdout).expect("events utf8");
    assert!(events.contains("mcp-server-removed"), "{events}");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "events",
            "--kind",
            "marketplace-imported",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("marketplace-imported"));
}

#[test]
fn mcp_stdio_server_exposes_agent_os_tools() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state path");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let approval_task_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "task",
            "create",
            "MCP approval task",
        ])
        .output()
        .expect("create approval task");
    assert!(
        approval_task_output.status.success(),
        "approval task create failed"
    );
    let approval_task: Value =
        serde_json::from_slice(&approval_task_output.stdout).expect("approval task json");
    let approval_task_id = approval_task["id"].as_str().expect("approval task id");
    inject_pending_approval(&state, approval_task_id, "mcp-approval");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "mcp-secret-tool",
            "--command-template",
            "printf {token}",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "MCP secret task",
            "--tool",
            "mcp-secret-tool",
            "--secret-arg",
            "token=AGENT_OS_MCP_SECRET_CHECK_MISSING",
        ])
        .assert()
        .success();

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env_remove("AGENT_OS_MCP_SECRET_CHECK_MISSING")
        .args(["--state", state_arg, "mcp", "serve", "--max-requests", "6"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn mcp server");

    let mut stdin = child.stdin.take().expect("mcp stdin");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":1,"method":"initialize","params":{{}}}}"#
    )
    .expect("write initialize");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{{}}}}"#
    )
    .expect("write tools/list");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{{"name":"agent_os_approval_list","arguments":{{"status":"pending"}}}}}}"#
    )
    .expect("write approval list");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{{"name":"agent_os_resolve_approval","arguments":{{"id":"mcp-approval","approved":true,"by":"mcp-client"}}}}}}"#
    )
    .expect("write approval resolve");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{{"name":"agent_os_secrets_check","arguments":{{}}}}}}"#
    )
    .expect("write secrets check");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{{"name":"agent_os_create_task","arguments":{{"title":"MCP task","command":"printf mcp","need":["rust"]}}}}}}"#
    )
    .expect("write tools/call");
    drop(stdin);

    let output = child.wait_with_output().expect("mcp output");
    assert!(output.status.success(), "mcp server failed");
    let stdout = String::from_utf8(output.stdout).expect("mcp stdout");
    let responses = stdout
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("mcp response json"))
        .collect::<Vec<_>>();
    assert_eq!(responses.len(), 6);
    assert_eq!(responses[0]["result"]["serverInfo"]["name"], "agent-os");
    assert!(
        responses[1]["result"]["tools"]
            .as_array()
            .expect("tools")
            .iter()
            .any(|tool| tool["name"] == "agent_os_create_task")
    );
    assert!(
        responses[1]["result"]["tools"]
            .as_array()
            .expect("tools")
            .iter()
            .any(|tool| tool["name"] == "agent_os_resolve_approval")
    );
    assert!(
        responses[1]["result"]["tools"]
            .as_array()
            .expect("tools")
            .iter()
            .any(|tool| tool["name"] == "agent_os_secrets_check")
    );
    assert_eq!(responses[2]["result"]["isError"], false);
    let approvals_text = responses[2]["result"]["content"][0]["text"]
        .as_str()
        .expect("approvals text");
    let approvals: Value = serde_json::from_str(approvals_text).expect("approvals json");
    assert_eq!(approvals["count"], 1);
    assert_eq!(approvals["approvals"][0]["id"], "mcp-approval");
    assert_eq!(approvals["approvals"][0]["status"], "pending");
    assert_eq!(responses[3]["result"]["isError"], false);
    let resolved_text = responses[3]["result"]["content"][0]["text"]
        .as_str()
        .expect("resolved text");
    let resolved: Value = serde_json::from_str(resolved_text).expect("resolved approval json");
    assert_eq!(resolved["approval"]["status"], "approved");
    assert_eq!(resolved["approval"]["resolved_by"], "mcp-client");
    assert_eq!(responses[4]["result"]["isError"], false);
    let secrets_text = responses[4]["result"]["content"][0]["text"]
        .as_str()
        .expect("secrets check text");
    let secrets: Value = serde_json::from_str(secrets_text).expect("secrets check json");
    assert_eq!(secrets["total"], 1);
    assert_eq!(secrets["present"], 0);
    assert_eq!(secrets["missing"], 1);
    assert_eq!(
        secrets["references"][0]["env"],
        "AGENT_OS_MCP_SECRET_CHECK_MISSING"
    );
    assert_eq!(responses[5]["result"]["isError"], false);
    let created_text = responses[5]["result"]["content"][0]["text"]
        .as_str()
        .expect("tool text");
    let created: Value = serde_json::from_str(created_text).expect("created task json");
    assert_eq!(created["task"]["title"], "MCP task");
    assert_eq!(created["task"]["command"], "printf mcp");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "list", "--query", "MCP task"])
        .assert()
        .success()
        .stdout(predicate::str::contains("MCP task"));

    let approvals = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "approval", "list"])
        .output()
        .expect("approval list");
    assert!(approvals.status.success(), "approval list failed");
    let approvals: Value = serde_json::from_slice(&approvals.stdout).expect("approvals json");
    assert_eq!(approvals[0]["id"], "mcp-approval");
    assert_eq!(approvals[0]["status"], "approved");

    let state_body = std::fs::read_to_string(state.join("state.json")).expect("read state");
    let state_json: Value = serde_json::from_str(&state_body).expect("state json");
    assert_eq!(state_json["tasks"][approval_task_id]["status"], "pending");
}

#[test]
fn mcp_stdio_server_exposes_resources_and_prompts() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state path");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "worker",
            "register",
            "mcp-worker",
            "--endpoint",
            "http://127.0.0.1:9200",
        ])
        .assert()
        .success();
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "eval",
            "record",
            "mcp-eval",
            "--success",
        ])
        .assert()
        .success();
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "secrets",
            "register",
            "mcp-env",
            "--kind",
            "environment",
            "--reference",
            "MCP_SECRET",
        ])
        .assert()
        .success();
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "registry",
            "mcp-add",
            "mcp-resource",
            "--command",
            assert_cmd::cargo::cargo_bin("agent-os")
                .to_str()
                .expect("binary path"),
        ])
        .assert()
        .success();

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args(["--state", state_arg, "mcp", "serve", "--max-requests", "11"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn mcp server");

    let mut stdin = child.stdin.take().expect("mcp stdin");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":1,"method":"initialize","params":{{}}}}"#
    )
    .expect("write initialize");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":2,"method":"resources/list","params":{{}}}}"#
    )
    .expect("write resources/list");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":3,"method":"resources/read","params":{{"uri":"agent-os://status"}}}}"#
    )
    .expect("write resources/read");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":4,"method":"resources/read","params":{{"uri":"agent-os://workers"}}}}"#
    )
    .expect("write workers resource");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":5,"method":"resources/read","params":{{"uri":"agent-os://evals"}}}}"#
    )
    .expect("write evals resource");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":6,"method":"resources/read","params":{{"uri":"agent-os://registry"}}}}"#
    )
    .expect("write registry resource");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":7,"method":"resources/read","params":{{"uri":"agent-os://secrets"}}}}"#
    )
    .expect("write secrets resource");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":8,"method":"resources/read","params":{{"uri":"agent-os://approvals"}}}}"#
    )
    .expect("write approvals resource");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":9,"method":"resources/read","params":{{"uri":"agent-os://policy"}}}}"#
    )
    .expect("write policy resource");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":10,"method":"prompts/list","params":{{}}}}"#
    )
    .expect("write prompts/list");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":11,"method":"prompts/get","params":{{"name":"agent_os_plan_task","arguments":{{"objective":"Ship MCP resources"}}}}}}"#
    )
    .expect("write prompts/get");
    drop(stdin);

    let output = child.wait_with_output().expect("mcp output");
    assert!(output.status.success(), "mcp server failed");
    let stdout = String::from_utf8(output.stdout).expect("mcp stdout");
    let responses = stdout
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("mcp response json"))
        .collect::<Vec<_>>();
    assert_eq!(responses.len(), 11);
    assert_eq!(
        responses[0]["result"]["capabilities"]["resources"],
        serde_json::json!({})
    );
    assert_eq!(
        responses[0]["result"]["capabilities"]["prompts"],
        serde_json::json!({})
    );
    assert!(
        responses[1]["result"]["resources"]
            .as_array()
            .expect("resources")
            .iter()
            .any(|resource| resource["uri"] == "agent-os://status")
    );
    assert!(
        responses[1]["result"]["resources"]
            .as_array()
            .expect("resources")
            .iter()
            .any(|resource| resource["uri"] == "agent-os://workers")
    );
    assert!(
        responses[1]["result"]["resources"]
            .as_array()
            .expect("resources")
            .iter()
            .any(|resource| resource["uri"] == "agent-os://secrets")
    );
    assert!(
        responses[1]["result"]["resources"]
            .as_array()
            .expect("resources")
            .iter()
            .any(|resource| resource["uri"] == "agent-os://policy")
    );
    let status_text = responses[2]["result"]["contents"][0]["text"]
        .as_str()
        .expect("resource text");
    let status: Value = serde_json::from_str(status_text).expect("status resource json");
    assert_eq!(
        status["state_path"],
        state.join("state.json").display().to_string()
    );
    let workers_text = responses[3]["result"]["contents"][0]["text"]
        .as_str()
        .expect("workers resource text");
    let workers: Value = serde_json::from_str(workers_text).expect("workers resource json");
    assert_eq!(
        workers["workers"]["mcp-worker"]["endpoint"],
        "http://127.0.0.1:9200"
    );
    assert_eq!(workers["count"], 1);

    let evals_text = responses[4]["result"]["contents"][0]["text"]
        .as_str()
        .expect("evals resource text");
    let evals: Value = serde_json::from_str(evals_text).expect("evals resource json");
    assert_eq!(evals["evals"][0]["target"], "mcp-eval");
    assert_eq!(evals["count"], 1);

    let registry_text = responses[5]["result"]["contents"][0]["text"]
        .as_str()
        .expect("registry resource text");
    let registry: Value = serde_json::from_str(registry_text).expect("registry resource json");
    assert_eq!(
        registry["mcp_servers"]["mcp-resource"]["command"],
        assert_cmd::cargo::cargo_bin("agent-os")
            .display()
            .to_string()
    );
    assert_eq!(registry["counts"]["mcp_servers"], 1);

    let secrets_text = responses[6]["result"]["contents"][0]["text"]
        .as_str()
        .expect("secrets resource text");
    let secrets: Value = serde_json::from_str(secrets_text).expect("secrets resource json");
    assert_eq!(
        secrets["secrets_backends"]["mcp-env"]["reference"],
        "MCP_SECRET"
    );
    assert_eq!(secrets["count"], 2);

    let approvals_text = responses[7]["result"]["contents"][0]["text"]
        .as_str()
        .expect("approvals resource text");
    let approvals: Value = serde_json::from_str(approvals_text).expect("approvals resource json");
    assert_eq!(approvals["count"], 0);
    let policy_text = responses[8]["result"]["contents"][0]["text"]
        .as_str()
        .expect("policy resource text");
    let policy: Value = serde_json::from_str(policy_text).expect("policy resource json");
    assert_eq!(policy["policy"]["autonomy"], "execute-freely");
    assert_eq!(policy["policy"]["sandbox"]["process_isolation"], true);
    assert_eq!(policy["policy"]["network"]["mode"], "allowed");
    assert_eq!(policy["memory_policy"]["semantic_recall"], false);
    assert!(
        responses[9]["result"]["prompts"]
            .as_array()
            .expect("prompts")
            .iter()
            .any(|prompt| prompt["name"] == "agent_os_plan_task")
    );
    let prompt_text = responses[10]["result"]["messages"][0]["content"]["text"]
        .as_str()
        .expect("prompt text");
    assert!(prompt_text.contains("Ship MCP resources"), "{prompt_text}");
}

#[test]
fn mcp_stdio_server_proxies_enabled_registered_servers() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let remote_state = dir.path().join("remote-agent-os");
    let state_arg = state.to_str().expect("state path");
    let remote_state_arg = remote_state.to_str().expect("remote state path");
    let binary = assert_cmd::cargo::cargo_bin("agent-os");
    let binary_arg = binary.to_str().expect("binary path");

    for state_arg in [state_arg, remote_state_arg] {
        Command::cargo_bin("agent-os")
            .expect("binary")
            .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
            .assert()
            .success();
    }

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "registry",
            "mcp-add",
            "remote",
            "--command",
            binary_arg,
            "--arg=--state",
            "--arg",
            remote_state_arg,
            "--arg",
            "mcp",
            "--arg",
            "serve",
            "--arg=--max-requests",
            "--arg",
            "2",
        ])
        .assert()
        .success();
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "registry",
            "mcp-add",
            "disabled",
            "--command",
            binary_arg,
            "--arg=--state",
            "--arg",
            remote_state_arg,
            "--arg",
            "mcp",
            "--arg",
            "serve",
            "--arg=--max-requests",
            "--arg",
            "2",
            "--disabled",
        ])
        .assert()
        .success();

    let mut child = std::process::Command::new(binary)
        .args(["--state", state_arg, "mcp", "serve", "--max-requests", "3"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn mcp server");

    let mut stdin = child.stdin.take().expect("mcp stdin");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":1,"method":"initialize","params":{{}}}}"#
    )
    .expect("write initialize");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{{}}}}"#
    )
    .expect("write tools/list");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{{"name":"mcp_remote__agent_os_status","arguments":{{}}}}}}"#
    )
    .expect("write tools/call");
    drop(stdin);

    let output = child.wait_with_output().expect("mcp output");
    assert!(output.status.success(), "mcp server failed");
    let stdout = String::from_utf8(output.stdout).expect("mcp stdout");
    let responses = stdout
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("mcp response json"))
        .collect::<Vec<_>>();
    assert_eq!(responses.len(), 3);
    let tools = responses[1]["result"]["tools"]
        .as_array()
        .expect("tools list");
    assert!(
        tools
            .iter()
            .any(|tool| tool["name"] == "mcp_remote__agent_os_status"),
        "{tools:?}"
    );
    assert!(
        !tools
            .iter()
            .any(|tool| tool["name"] == "mcp_disabled__agent_os_status"),
        "{tools:?}"
    );
    assert_eq!(responses[2]["result"]["isError"], false);
    let proxied_text = responses[2]["result"]["content"][0]["text"]
        .as_str()
        .expect("proxied text");
    let proxied: Value = serde_json::from_str(proxied_text).expect("proxied status json");
    assert_eq!(
        proxied["state_path"],
        remote_state.join("state.json").display().to_string()
    );
}

#[test]
fn worker_and_eval_cli_manage_distributed_runtime_records() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state path");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let worker = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "worker",
            "register",
            "remote-a",
            "--endpoint",
            "http://127.0.0.1:9000",
            "--status",
            "online",
        ])
        .output()
        .expect("worker register");
    assert!(worker.status.success(), "agent-os worker register failed");
    let worker: Value = serde_json::from_slice(&worker.stdout).expect("worker json");
    assert_eq!(worker["id"], "remote-a");
    assert_eq!(worker["endpoint"], "http://127.0.0.1:9000");
    assert_eq!(worker["status"], "online");

    let heartbeat = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "worker",
            "heartbeat",
            "remote-a",
            "--endpoint",
            "http://127.0.0.1:9001",
            "--status",
            "busy",
            "--lease-seconds",
            "60",
        ])
        .output()
        .expect("worker heartbeat");
    assert!(
        heartbeat.status.success(),
        "agent-os worker heartbeat failed"
    );
    let heartbeat: Value = serde_json::from_slice(&heartbeat.stdout).expect("heartbeat json");
    assert_eq!(heartbeat["endpoint"], "http://127.0.0.1:9001");
    assert_eq!(heartbeat["status"], "busy");

    let workers = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--json", "worker", "list", "--status", "busy", "--query",
            "remote", "--limit", "1",
        ])
        .output()
        .expect("worker list");
    assert!(workers.status.success(), "agent-os worker list failed");
    let workers: Value = serde_json::from_slice(&workers.stdout).expect("workers json");
    assert_eq!(workers.as_array().expect("workers").len(), 1);
    assert_eq!(workers[0]["id"], "remote-a");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "worker",
            "register",
            "builder",
            "--endpoint",
            "http://127.0.0.1:9100",
        ])
        .assert()
        .success();
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Remote worker task",
            "--need",
            "rust",
            "--command",
            "printf worker-claim",
        ])
        .assert()
        .success();
    let worker_claim = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "worker",
            "claim",
            "builder",
            "--lease-seconds",
            "30",
        ])
        .output()
        .expect("worker claim");
    assert!(
        worker_claim.status.success(),
        "agent-os worker claim failed"
    );
    let worker_claim: Value =
        serde_json::from_slice(&worker_claim.stdout).expect("worker claim json");
    assert_eq!(worker_claim["claimed"], true);
    assert_eq!(worker_claim["worker"]["id"], "builder");
    assert_eq!(worker_claim["assignment"]["agent_id"], "builder");
    assert_eq!(worker_claim["task"]["title"], "Remote worker task");
    let worker_task_id = worker_claim["assignment"]["task_id"]
        .as_str()
        .expect("worker task id");

    let worker_report = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "worker",
            "report",
            "builder",
            worker_task_id,
            "--status",
            "complete",
            "--note",
            "worker finished remotely",
            "--command",
            "printf worker-claim",
            "--cwd",
            "/tmp/remote-worker",
            "--exit-code",
            "0",
            "--artifact",
            "stdout=artifacts/stdout.log",
        ])
        .output()
        .expect("worker report");
    assert!(
        worker_report.status.success(),
        "agent-os worker report failed"
    );
    let worker_report: Value =
        serde_json::from_slice(&worker_report.stdout).expect("worker report json");
    assert_eq!(worker_report["reported"], true);
    assert_eq!(worker_report["task"]["status"], "complete");
    assert_eq!(worker_report["task"]["output"], "worker finished remotely");
    assert_eq!(worker_report["run"]["status"], "success");
    assert_eq!(worker_report["run"]["command"], "printf worker-claim");
    assert_eq!(worker_report["run"]["cwd"], "/tmp/remote-worker");
    assert_eq!(worker_report["run"]["artifacts"][0]["kind"], "stdout");

    let eval = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "eval",
            "record",
            "ci-fix-workflow",
            "--success",
            "--cost-micros",
            "1234",
            "--latency-ms",
            "567",
        ])
        .output()
        .expect("eval record");
    assert!(eval.status.success(), "agent-os eval record failed");
    let eval: Value = serde_json::from_slice(&eval.stdout).expect("eval json");
    let eval_id = eval["id"].as_str().expect("eval id");
    assert_eq!(eval["target"], "ci-fix-workflow");
    assert_eq!(eval["success"], true);
    assert_eq!(eval["cost_micros"], 1234);
    assert_eq!(eval["latency_ms"], 567);

    let eval_output_schema = dir.path().join("eval-output-schema.json");
    std::fs::write(
        &eval_output_schema,
        r#"{"type":"object","required":["message","ok","count"],"properties":{"message":{"type":"string"},"ok":{"type":"boolean"},"count":{"type":"integer"}},"additionalProperties":false}"#,
    )
    .expect("write eval output schema");
    let eval_output_schema_arg = eval_output_schema
        .to_str()
        .expect("eval output schema path");
    let eval_run = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "eval",
            "run",
            "local-smoke",
            "--command",
            "printf '{\"message\":\"cli-eval-run\",\"ok\":true,\"count\":2}'",
            "--success-pattern",
            "cli-eval-run",
            "--output-schema",
            eval_output_schema_arg,
        ])
        .output()
        .expect("eval run");
    assert!(eval_run.status.success(), "agent-os eval run failed");
    let eval_run: Value = serde_json::from_slice(&eval_run.stdout).expect("eval run json");
    assert_eq!(eval_run["eval"]["target"], "local-smoke");
    assert_eq!(eval_run["eval"]["success"], true);
    assert_eq!(
        eval_run["eval"]["run"]["command"],
        "printf '{\"message\":\"cli-eval-run\",\"ok\":true,\"count\":2}'"
    );
    assert_eq!(
        eval_run["eval"]["run"]["stdout"],
        r#"{"message":"cli-eval-run","ok":true,"count":2}"#
    );
    assert_eq!(eval_run["eval"]["run"]["success_pattern"], "cli-eval-run");
    assert_eq!(
        eval_run["stdout"],
        r#"{"message":"cli-eval-run","ok":true,"count":2}"#
    );
    assert_eq!(eval_run["success_pattern_matched"], true);
    assert_eq!(eval_run["output_schema_valid"], true);
    assert_eq!(eval_run["output_schema_error"], Value::Null);
    let eval_run_id = eval_run["id"].as_str().expect("eval run id");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "eval", "record", "bad-eval"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "eval record requires exactly one of --success or --failure",
        ));

    let eval_show = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "eval", "show", eval_id])
        .output()
        .expect("eval show");
    assert!(eval_show.status.success(), "agent-os eval show failed");
    let eval_show: Value = serde_json::from_slice(&eval_show.stdout).expect("eval show json");
    assert_eq!(eval_show["id"], eval_id);

    let evals = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "eval",
            "list",
            "--target",
            "ci-fix-workflow",
            "--success",
            "true",
            "--limit",
            "1",
        ])
        .output()
        .expect("eval list");
    assert!(evals.status.success(), "agent-os eval list failed");
    let evals: Value = serde_json::from_slice(&evals.stdout).expect("evals json");
    assert_eq!(evals.as_array().expect("evals").len(), 1);
    assert_eq!(evals[0]["id"], eval_id);

    let eval_run_list = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "eval",
            "list",
            "--query",
            "cli-eval-run",
            "--limit",
            "1",
        ])
        .output()
        .expect("eval run list");
    assert!(
        eval_run_list.status.success(),
        "agent-os eval run list failed"
    );
    let eval_run_list: Value =
        serde_json::from_slice(&eval_run_list.stdout).expect("eval run list json");
    assert_eq!(eval_run_list.as_array().expect("eval run list").len(), 1);
    assert_eq!(eval_run_list[0]["id"], eval_run_id);
    assert_eq!(
        eval_run_list[0]["run"]["stdout"],
        r#"{"message":"cli-eval-run","ok":true,"count":2}"#
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "worker", "remove", "remote-a"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Removed worker remote-a"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "events", "--kind", "worker-removed"])
        .assert()
        .success()
        .stdout(predicate::str::contains("worker-removed"));
}

#[test]
fn secrets_cli_manages_secret_manager_backends_without_values() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state path");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let defaults = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "secrets", "list"])
        .output()
        .expect("secrets list");
    assert!(defaults.status.success(), "agent-os secrets list failed");
    let defaults: Value = serde_json::from_slice(&defaults.stdout).expect("secrets list json");
    assert_eq!(defaults[0]["id"], "environment");
    assert_eq!(defaults[0]["kind"], "environment");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "secret-tool",
            "--command-template",
            "printf {token}",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Use secret token",
            "--tool",
            "secret-tool",
            "--secret-arg",
            "token=AGENT_OS_CLI_SECRET_CHECK",
        ])
        .assert()
        .success();

    let missing_check = Command::cargo_bin("agent-os")
        .expect("binary")
        .env_remove("AGENT_OS_CLI_SECRET_CHECK")
        .args(["--state", state_arg, "--json", "secrets", "check"])
        .output()
        .expect("secrets missing check");
    assert!(
        missing_check.status.success(),
        "agent-os secrets missing check failed"
    );
    let missing_check: Value =
        serde_json::from_slice(&missing_check.stdout).expect("missing check json");
    assert_eq!(missing_check["total"], 1);
    assert_eq!(missing_check["present"], 0);
    assert_eq!(missing_check["missing"], 1);
    assert_eq!(
        missing_check["references"][0]["env"],
        "AGENT_OS_CLI_SECRET_CHECK"
    );
    assert_eq!(missing_check["references"][0]["present"], false);

    let present_check = Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_CLI_SECRET_CHECK", "not printed")
        .args(["--state", state_arg, "--json", "secrets", "check"])
        .output()
        .expect("secrets present check");
    assert!(
        present_check.status.success(),
        "agent-os secrets present check failed"
    );
    let present_check: Value =
        serde_json::from_slice(&present_check.stdout).expect("present check json");
    assert_eq!(present_check["total"], 1);
    assert_eq!(present_check["present"], 1);
    assert_eq!(present_check["missing"], 0);
    assert_eq!(present_check["references"][0]["present"], true);

    let backend = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "secrets",
            "register",
            "prod-op",
            "--kind",
            "1password",
            "--reference",
            "vault/Agent OS",
        ])
        .output()
        .expect("secrets register");
    assert!(backend.status.success(), "agent-os secrets register failed");
    let backend: Value = serde_json::from_slice(&backend.stdout).expect("backend json");
    assert_eq!(backend["id"], "prod-op");
    assert_eq!(backend["kind"], "one-password");
    assert_eq!(backend["reference"], "vault/Agent OS");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "secrets", "register", "bad", "--kind", "mystery",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "invalid secrets backend kind `mystery`",
        ));

    let filtered = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "secrets",
            "list",
            "--kind",
            "one-password",
            "--query",
            "vault",
            "--limit",
            "1",
        ])
        .output()
        .expect("filtered secrets list");
    assert!(
        filtered.status.success(),
        "agent-os secrets list filtered failed"
    );
    let filtered: Value = serde_json::from_slice(&filtered.stdout).expect("filtered json");
    assert_eq!(filtered.as_array().expect("filtered").len(), 1);
    assert_eq!(filtered[0]["id"], "prod-op");

    let shown = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "secrets", "show", "prod-op"])
        .output()
        .expect("secrets show");
    assert!(shown.status.success(), "agent-os secrets show failed");
    let shown: Value = serde_json::from_slice(&shown.stdout).expect("show json");
    assert_eq!(shown["kind"], "one-password");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "secrets", "remove", "prod-op"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Removed secrets backend prod-op"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "events",
            "--kind",
            "secrets-backend-removed",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("secrets-backend-removed"));
}

#[test]
fn service_install_and_uninstall_manage_launchd_plist() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let plist = dir
        .path()
        .join("LaunchAgents")
        .join("com.example.agent-os.plist");
    let state_arg = state.to_str().expect("state");
    let plist_arg = plist.to_str().expect("plist");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "service",
            "install",
            "--plist-path",
            "   ",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("plist_path must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "service",
            "install",
            "--bin-path",
            "/usr/local/bin/agent-os",
            "--label",
            "com.example.agent-os",
            "--interval-ms",
            "250",
            "--limit",
            "3",
            "--execute",
            "--plist-path",
            plist_arg,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Installed launchd service plist"));
    let body = std::fs::read_to_string(&plist).expect("plist body");
    assert!(body.contains("com.example.agent-os"));
    assert!(body.contains("/usr/local/bin/agent-os"));
    assert!(body.contains("--execute"));
    assert!(!plist.with_extension("plist.tmp").exists());

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "service",
            "uninstall",
            "--label",
            "",
            "--plist-path",
            plist_arg,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("service label must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "service",
            "uninstall",
            "--label",
            "com.example.agent-os",
            "--plist-path",
            plist_arg,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Removed launchd service plist"));
    assert!(!plist.exists());
}

#[test]
fn service_windows_task_renders_powershell_without_installing() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "service",
            "windows-task",
            "--bin-path",
            r"C:\Program Files\Agent OS\agent-os.exe",
            "--task-name",
            "Agent OS Test",
            "--interval-ms",
            "2500",
            "--limit",
            "2",
            "--execute",
            "--recover-stale-seconds",
            "60",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("New-ScheduledTaskAction"))
        .stdout(predicate::str::contains("Register-ScheduledTask"))
        .stdout(predicate::str::contains("-TaskName 'Agent OS Test'"))
        .stdout(predicate::str::contains("-Milliseconds 2500"))
        .stdout(predicate::str::contains("--execute"))
        .stdout(predicate::str::contains("--recover-stale-seconds 60"));

    let rendered = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "service",
            "windows-task",
            "--bin-path",
            r"C:\Agent OS\agent-os.exe",
            "--task-name",
            "Agent OS JSON",
        ])
        .output()
        .expect("windows task json");
    assert!(rendered.status.success(), "windows task json failed");
    let rendered: Value = serde_json::from_slice(&rendered.stdout).expect("windows task json body");
    assert_eq!(rendered["platform"], "windows-scheduled-task");
    assert_eq!(rendered["task"]["task_name"], "Agent OS JSON");
    assert!(
        rendered["powershell"]
            .as_str()
            .expect("powershell")
            .contains("Register-ScheduledTask")
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "service",
            "windows-task",
            "--task-name",
            " ",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "windows task name must not be empty",
        ));
}

#[test]
#[cfg(unix)]
fn service_start_stop_and_status_call_launchctl() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let plist = dir.path().join("com.example.agent-os.plist");
    let fake_launchctl = dir.path().join("launchctl");
    let log = dir.path().join("launchctl.log");
    std::fs::write(&plist, "<plist/>").expect("plist");
    std::fs::write(
        &fake_launchctl,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$AGENT_OS_LAUNCHCTL_LOG\"\nif [ \"$1\" = \"print\" ]; then echo 'state = running'; fi\nexit 0\n",
    )
    .expect("fake launchctl");
    make_executable(&fake_launchctl);

    let state_arg = state.to_str().expect("state");
    let plist_arg = plist.to_str().expect("plist");
    let launchctl_arg = fake_launchctl.to_str().expect("launchctl");
    let log_arg = log.to_str().expect("log");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "service",
            "start",
            "--label",
            "com.example.agent-os",
            "--plist-path",
            plist_arg,
            "--domain",
            "",
            "--launchctl-path",
            launchctl_arg,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("launchd domain must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "service",
            "status",
            "--label",
            "com.example.agent-os",
            "--launchctl-path",
            "   ",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("launchctl_path must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_LAUNCHCTL_LOG", log_arg)
        .args([
            "--state",
            state_arg,
            "service",
            "start",
            "--label",
            "com.example.agent-os",
            "--plist-path",
            plist_arg,
            "--domain",
            "gui/test",
            "--launchctl-path",
            launchctl_arg,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Started launchd service"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_LAUNCHCTL_LOG", log_arg)
        .args([
            "--state",
            state_arg,
            "service",
            "status",
            "--label",
            "com.example.agent-os",
            "--domain",
            "gui/test",
            "--launchctl-path",
            launchctl_arg,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("is loaded"))
        .stdout(predicate::str::contains("state = running"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_LAUNCHCTL_LOG", log_arg)
        .args([
            "--state",
            state_arg,
            "service",
            "stop",
            "--label",
            "com.example.agent-os",
            "--plist-path",
            plist_arg,
            "--domain",
            "gui/test",
            "--launchctl-path",
            launchctl_arg,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Stopped launchd service"));

    let calls = std::fs::read_to_string(log).expect("launchctl log");
    assert!(calls.contains(&format!("bootstrap gui/test {plist_arg}")));
    assert!(calls.contains("print gui/test/com.example.agent-os"));
    assert!(calls.contains(&format!("bootout gui/test {plist_arg}")));
}

#[test]
#[cfg(unix)]
fn service_systemd_controls_call_systemctl() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let unit = dir.path().join("agent-os.service");
    let fake_systemctl = dir.path().join("systemctl");
    let log = dir.path().join("systemctl.log");
    std::fs::write(
        &fake_systemctl,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$AGENT_OS_SYSTEMCTL_LOG\"\nif [ \"$2\" = \"is-active\" ]; then echo active; fi\nexit 0\n",
    )
    .expect("fake systemctl");
    make_executable(&fake_systemctl);

    let state_arg = state.to_str().expect("state");
    let unit_arg = unit.to_str().expect("unit");
    let systemctl_arg = fake_systemctl.to_str().expect("systemctl");
    let log_arg = log.to_str().expect("log");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "service",
            "install-systemd",
            "--unit-name",
            "agent-os-test.service",
            "--unit-path",
            unit_arg,
            "--bin-path",
            "/usr/local/bin/agent-os",
            "--execute",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Installed systemd user service unit",
        ));
    let unit_body = std::fs::read_to_string(&unit).expect("systemd unit");
    assert!(unit_body.contains("[Service]"));
    assert!(unit_body.contains("ExecStart=/usr/local/bin/agent-os"));
    assert!(unit_body.contains("--execute"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "service",
            "start-systemd",
            "--unit-name",
            "",
            "--systemctl-path",
            systemctl_arg,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("service label must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "service",
            "status-systemd",
            "--unit-name",
            "agent-os-test.service",
            "--systemctl-path",
            "   ",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("systemctl_path must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_SYSTEMCTL_LOG", log_arg)
        .args([
            "--state",
            state_arg,
            "service",
            "start-systemd",
            "--unit-name",
            "agent-os-test.service",
            "--systemctl-path",
            systemctl_arg,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Started systemd user service"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_SYSTEMCTL_LOG", log_arg)
        .args([
            "--state",
            state_arg,
            "service",
            "status-systemd",
            "--unit-name",
            "agent-os-test.service",
            "--systemctl-path",
            systemctl_arg,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("is active"))
        .stdout(predicate::str::contains("active"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_SYSTEMCTL_LOG", log_arg)
        .args([
            "--state",
            state_arg,
            "service",
            "stop-systemd",
            "--unit-name",
            "agent-os-test.service",
            "--systemctl-path",
            systemctl_arg,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Stopped systemd user service"));

    let calls = std::fs::read_to_string(log).expect("systemctl log");
    assert!(calls.contains("--user start agent-os-test.service"));
    assert!(calls.contains("--user is-active agent-os-test.service"));
    assert!(calls.contains("--user stop agent-os-test.service"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "service",
            "uninstall-systemd",
            "--unit-name",
            "agent-os-test.service",
            "--unit-path",
            unit_arg,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Removed systemd user service unit",
        ));
    assert!(!unit.exists());
}

#[test]
fn completions_command_generates_shell_completion() {
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Examples:"))
        .stdout(predicate::str::contains(
            "agent-os config init --profile safe",
        ))
        .stdout(predicate::str::contains(
            "agent-os --state ./sandbox run --dry-run",
        ))
        .stdout(predicate::str::contains(
            "Run state and config preflight diagnostics",
        ))
        .stdout(predicate::str::contains(
            "Register agents, heartbeats, claims, and capacity",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["agent", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Claim the next ready task for an agent",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["agent", "list", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("online/up"))
        .stdout(predicate::str::contains("offline/down"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["agent", "heartbeat", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("online/up"))
        .stdout(predicate::str::contains("offline/down"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["state", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Repair safe state consistency issues",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["events", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("task-created"))
        .stdout(predicate::str::contains("state-repaired"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["task", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Recover stale running tasks"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["task", "create", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("critical/urgent"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["task", "list", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("pending, running, blocked"))
        .stdout(predicate::str::contains("complete/completed"))
        .stdout(predicate::str::contains("cancelled/canceled"))
        .stdout(predicate::str::contains("critical/urgent"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["task", "priority", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("critical/urgent"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["tool", "add", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("file-read/read-file"))
        .stdout(predicate::str::contains("file-write/write-file"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["tool", "list", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("file-read/read-file"))
        .stdout(predicate::str::contains("file-write/write-file"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["tool", "update", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("file-read/read-file"))
        .stdout(predicate::str::contains("file-write/write-file"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["runs", "list", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "cancel-requested/cancel_requested",
        ))
        .stdout(predicate::str::contains("success/succeeded"))
        .stdout(predicate::str::contains("rejected"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["workflow", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Advance workflow tasks"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["workflow", "create", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("critical/urgent"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["workflow", "list", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("critical/urgent"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["api", "serve", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Bind address for the local API listener",
        ))
        .stdout(predicate::str::contains("--allow-origin"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["completions", "bash"])
        .assert()
        .success()
        .stdout(predicate::str::contains("_agent-os"))
        .stdout(predicate::str::contains("service"))
        .stdout(predicate::str::contains("daemon"));
}

#[test]
fn readme_agent_add_synopsis_documents_supported_flags() {
    let readme = include_str!("../README.md");
    let synopsis = readme
        .lines()
        .find(|line| line.starts_with("agent-os agent add NAME "))
        .expect("README agent add synopsis");

    assert!(synopsis.contains("[--kind KIND]"), "{synopsis}");
    assert!(synopsis.contains("[--model MODEL]"), "{synopsis}");
    assert!(synopsis.contains("--cap CAP"), "{synopsis}");
    assert!(synopsis.contains("[--parallel N]"), "{synopsis}");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["agent", "add", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--kind"))
        .stdout(predicate::str::contains("--model"))
        .stdout(predicate::str::contains("--cap"))
        .stdout(predicate::str::contains("--parallel"));
}

#[test]
fn readme_agent_status_synopses_document_aliases() {
    let readme = include_str!("../README.md");
    let list_synopsis = readme
        .lines()
        .find(|line| line.starts_with("agent-os agent list "))
        .expect("README agent list synopsis");
    let heartbeat_synopsis = readme
        .lines()
        .find(|line| line.starts_with("agent-os agent heartbeat AGENT_ID "))
        .expect("README agent heartbeat synopsis");

    for synopsis in [list_synopsis, heartbeat_synopsis] {
        assert!(synopsis.contains("online|up"), "{synopsis}");
        assert!(synopsis.contains("paused|pause"), "{synopsis}");
        assert!(synopsis.contains("offline|down"), "{synopsis}");
    }
}

#[test]
fn readme_documents_systemd_argument_escaping() {
    let readme = include_str!("../README.md");
    let service_notes = readme
        .lines()
        .find(|line| line.starts_with("- `service launchd` renders"))
        .expect("README service notes");

    assert!(
        service_notes.contains("service systemd` renders a Linux systemd user unit"),
        "{service_notes}"
    );
    assert!(
        service_notes.contains("service windows-task` renders a PowerShell script"),
        "{service_notes}"
    );
    assert!(
        service_notes.contains("escapes `%` specifiers"),
        "{service_notes}"
    );
}

#[test]
fn readme_documents_policy_rule_approval_gates() {
    let readme = include_str!("../README.md");
    let policy_notes = readme
        .lines()
        .find(|line| line.starts_with("Policy `rules` accept"))
        .expect("README policy rule notes");

    assert!(
        policy_notes.contains("require approval for git push"),
        "{policy_notes}"
    );
    assert!(
        policy_notes.contains("require approval for PATTERN"),
        "{policy_notes}"
    );
    assert!(policy_notes.contains("approval gate"), "{policy_notes}");
}

#[test]
fn readme_filter_synopses_document_accepted_aliases() {
    let readme = include_str!("../README.md");
    let task_list = readme
        .lines()
        .find(|line| line.starts_with("agent-os task list "))
        .expect("README task list synopsis");
    let task_create = readme
        .lines()
        .find(|line| line.starts_with("agent-os task create "))
        .expect("README task create synopsis");
    let task_update = readme
        .lines()
        .find(|line| line.starts_with("agent-os task update "))
        .expect("README task update synopsis");
    let tool_add = readme
        .lines()
        .find(|line| line.starts_with("agent-os tool add "))
        .expect("README tool add synopsis");
    let tool_list = readme
        .lines()
        .find(|line| line.starts_with("agent-os tool list "))
        .expect("README tool list synopsis");
    let tool_update = readme
        .lines()
        .find(|line| line.starts_with("agent-os tool update "))
        .expect("README tool update synopsis");
    let runs_list = readme
        .lines()
        .find(|line| line.starts_with("agent-os runs list "))
        .expect("README runs list synopsis");

    assert!(task_list.contains("complete|completed"), "{task_list}");
    assert!(task_list.contains("cancelled|canceled"), "{task_list}");
    assert!(task_create.contains("[--max-attempts N]"), "{task_create}");
    assert!(task_update.contains("[--max-attempts N]"), "{task_update}");
    for synopsis in [tool_add, tool_list, tool_update] {
        assert!(synopsis.contains("file-read|read-file"), "{synopsis}");
        assert!(synopsis.contains("file-write|write-file"), "{synopsis}");
    }
    assert!(
        runs_list.contains("cancel-requested|cancel_requested"),
        "{runs_list}"
    );
    assert!(runs_list.contains("success|succeeded"), "{runs_list}");
}

#[test]
fn readme_documents_generated_id_collision_retries() {
    let readme = include_str!("../README.md");
    let id_notes = readme
        .lines()
        .find(|line| line.starts_with("- Agent names must normalize"))
        .expect("README ID notes");

    assert!(
        id_notes.contains("Generated task, run, workflow, memory, and approval IDs"),
        "{id_notes}"
    );
    assert!(
        id_notes.contains("retried against current state"),
        "{id_notes}"
    );
}

#[test]
fn readme_state_repair_documents_workflow_drift() {
    let readme = include_str!("../README.md");
    let export_synopsis = readme
        .lines()
        .find(|line| line.starts_with("agent-os state export "))
        .expect("README state export synopsis");
    let import_synopsis = readme
        .lines()
        .find(|line| line.starts_with("agent-os state import "))
        .expect("README state import synopsis");
    let backup_synopsis = readme
        .lines()
        .find(|line| line.starts_with("agent-os state backup "))
        .expect("README state backup synopsis");
    let migrate_synopsis = readme
        .lines()
        .find(|line| line.starts_with("agent-os state migrate "))
        .expect("README state migrate synopsis");
    let state_notes = readme
        .lines()
        .find(|line| line.starts_with("- State can be exported"))
        .expect("README state maintenance notes");
    let repair_notes = readme
        .lines()
        .find(|line| line.starts_with("- `state repair` fixes"))
        .expect("README state repair notes");

    assert!(export_synopsis.contains("[--dry-run]"), "{export_synopsis}");
    assert!(import_synopsis.contains("[--dry-run]"), "{import_synopsis}");
    assert!(backup_synopsis.contains("[--dry-run]"), "{backup_synopsis}");
    assert!(
        migrate_synopsis.contains("[--dry-run]"),
        "{migrate_synopsis}"
    );
    assert!(
        state_notes
            .contains(r#"POST /state/export` accepts `{"output":"state.json","dry_run":true}`"#),
        "{state_notes}"
    );
    assert!(
        state_notes.contains(
            r#"POST /state/import` accepts `{"path":"state.json","force":true,"dry_run":true}`"#
        ),
        "{state_notes}"
    );
    assert!(
        state_notes.contains(r#"POST /state/migrate` accepts `{"input":"legacy.json","output":"state.json","dry_run":true}`"#),
        "{state_notes}"
    );
    assert!(
        state_notes
            .contains("migration dry-runs print planned schema steps plus validation success"),
        "{state_notes}"
    );
    assert!(
        state_notes.contains("migration responses include `output_preexisting`"),
        "{state_notes}"
    );
    assert!(
        state_notes.contains("migration reports include downgrade notes"),
        "{state_notes}"
    );
    assert!(
        state_notes.contains("pre-migration backup/export"),
        "{state_notes}"
    );
    assert!(
        state_notes
            .contains(r#"POST /state/backup` accepts `{"output":"backup.json","dry_run":true}`"#),
        "{state_notes}"
    );
    assert!(
        state_notes.contains(r#"POST /state/repair` accepts `{"dry_run":true}`"#),
        "{state_notes}"
    );
    assert!(
        state_notes.contains(r#"POST /state/sqlite` accepts `{"output":"state.sqlite"}`"#),
        "{state_notes}"
    );
    assert!(repair_notes.contains("OS name drift"), "{repair_notes}");
    assert!(
        repair_notes.contains("workflow stage/task drift"),
        "{repair_notes}"
    );
    assert!(
        repair_notes.contains("policy list/env/limit drift"),
        "{repair_notes}"
    );
    assert!(
        repair_notes.contains("provider default/env/empty-endpoint drift"),
        "{repair_notes}"
    );
    assert!(
        repair_notes.contains("task plan/output drift"),
        "{repair_notes}"
    );
    assert!(
        repair_notes.contains("run command/cwd/exit-code drift"),
        "{repair_notes}"
    );
}

#[test]
fn crate_manifest_documents_release_metadata() {
    let manifest: toml::Value = include_str!("../Cargo.toml")
        .parse()
        .expect("Cargo.toml parses");
    let package = manifest["package"]
        .as_table()
        .expect("Cargo.toml package table");
    assert_eq!(
        package["name"].as_str(),
        Some("agent_os"),
        "crate name is part of the published package identity"
    );
    assert_eq!(
        package["version"].as_str(),
        Some(env!("CARGO_PKG_VERSION")),
        "manifest version should match the compiled package version"
    );
    assert!(
        package["description"]
            .as_str()
            .is_some_and(|description| description.contains("AI agents")),
        "crate description should be useful on crates.io"
    );
    assert_eq!(package["readme"].as_str(), Some("README.md"));
    assert_eq!(package["license"].as_str(), Some("MIT"));
    assert_eq!(
        package["documentation"].as_str(),
        Some("https://docs.rs/agent_os")
    );
    assert_manifest_string_array_contains(package, "keywords", "agents");
    assert_manifest_string_array_contains(package, "keywords", "scheduler");
    assert_manifest_string_array_contains(package, "categories", "command-line-utilities");
    assert_manifest_string_array_contains(package, "categories", "development-tools");

    let binaries = manifest["bin"].as_array().expect("Cargo.toml bin table");
    assert!(
        binaries.iter().any(|binary| {
            binary["name"].as_str() == Some("agent-os")
                && binary["path"].as_str() == Some("src/main.rs")
        }),
        "published package should expose the documented agent-os binary"
    );

    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(
        manifest_dir.join("README.md").is_file(),
        "README.md missing"
    );
    let license = std::fs::read_to_string(manifest_dir.join("LICENSE")).expect("LICENSE");
    assert!(
        license.contains("MIT License"),
        "LICENSE should match Cargo.toml license metadata"
    );
}

#[test]
fn ci_script_uses_locked_dependency_resolution() {
    let ci = include_str!("../scripts/ci.sh");
    for command in [
        "cargo test --locked",
        "cargo doc --locked --no-deps",
        "cargo clippy --locked --all-targets",
        "cargo publish --dry-run --locked --allow-dirty",
        "cargo package --locked --allow-dirty",
    ] {
        assert!(
            ci.contains(command),
            "scripts/ci.sh should keep `{command}` pinned to Cargo.lock"
        );
    }
    for command in [
        "cargo test\n",
        "cargo doc --no-deps",
        "cargo clippy --all-targets",
        "cargo publish --dry-run --allow-dirty",
        "cargo package --allow-dirty",
    ] {
        assert!(
            !ci.contains(command),
            "scripts/ci.sh should not use unlocked `{}`",
            command.trim()
        );
    }
}

#[test]
fn github_ci_matches_manifest_rust_version_and_runs_canonical_ci() {
    let manifest: toml::Value = include_str!("../Cargo.toml")
        .parse()
        .expect("Cargo.toml parses");
    let rust_version = manifest["package"]["rust-version"]
        .as_str()
        .expect("package rust-version");
    let ci = include_str!("../.github/workflows/ci.yml");
    assert!(
        ci.contains(&format!("dtolnay/rust-toolchain@{rust_version}.0")),
        "GitHub CI should install the Cargo.toml rust-version"
    );
    assert!(
        ci.contains("components: rustfmt, clippy"),
        "GitHub CI should install rustfmt and clippy components"
    );
    assert!(
        ci.contains("run: ./scripts/ci.sh"),
        "GitHub CI should run the canonical local CI script"
    );
    for expected in [
        "runs-on: windows-latest",
        "runs-on: macos-latest",
        "cargo check --locked --all-targets",
        "cargo test --locked --lib",
    ] {
        assert!(
            ci.contains(expected),
            "GitHub CI should include cross-platform check `{expected}`"
        );
    }
}

#[test]
fn release_packaging_artifacts_cover_archives_completions_and_attestation() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let package_script = manifest_dir.join("scripts").join("package-release.sh");
    assert!(package_script.is_file(), "release packaging script missing");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = std::fs::metadata(&package_script)
            .expect("package script metadata")
            .permissions()
            .mode();
        assert_ne!(
            mode & 0o111,
            0,
            "release packaging script must be executable"
        );
    }

    let script = include_str!("../scripts/package-release.sh");
    for expected in [
        "cargo build --locked --release",
        "completions \"$shell\"",
        "tar -czf \"$archive\"",
        "shasum -a 256",
        "sha256sum",
        "target_exe_suffix",
        "*windows*|*mingw*|*msvc*) printf '.exe'",
        "exe=\"$(target_exe_suffix \"$target\")\"",
        "exe=\"$(host_exe_suffix)\"",
        "AGENT_OS_SIGN_RELEASES",
        "cosign sign-blob --yes",
        "--output-signature",
        "--output-certificate",
    ] {
        assert!(
            script.contains(expected),
            "package-release.sh should include `{expected}`"
        );
    }

    let release = include_str!("../.github/workflows/release.yml");
    for expected in [
        "tags: [\"v*\"]",
        "contents: write",
        "id-token: write",
        "attestations: write",
        "actions/attest-build-provenance@v2",
        "sigstore/cosign-installer@v3",
        "AGENT_OS_SIGN_RELEASES: \"1\"",
        "./scripts/package-release.sh",
        "softprops/action-gh-release@v2",
        "target/dist/*.tar.gz.sha256",
        "target/dist/*.tar.gz.sig",
        "target/dist/*.tar.gz.pem",
    ] {
        assert!(
            release.contains(expected),
            "release workflow should include `{expected}`"
        );
    }

    let formula = include_str!("../packaging/homebrew/agent-os.rb");
    for expected in [
        "class AgentOs < Formula",
        "on_macos do",
        "on_linux do",
        "agent-os-0.1.0-aarch64-apple-darwin.tar.gz",
        "agent-os-0.1.0-x86_64-apple-darwin.tar.gz",
        "agent-os-0.1.0-x86_64-unknown-linux-gnu.tar.gz",
        "bin.install \"bin/agent-os\"",
        "bash_completion.install",
        "zsh_completion.install",
        "fish_completion.install",
        "pkgshare/\"completions\"",
        "agent-os.elvish",
        "agent-os.powershell",
        "REPLACE_WITH_AARCH64_APPLE_DARWIN_SHA256",
        "REPLACE_WITH_X86_64_APPLE_DARWIN_SHA256",
        "REPLACE_WITH_X86_64_UNKNOWN_LINUX_GNU_SHA256",
    ] {
        assert!(
            formula.contains(expected),
            "Homebrew formula should include `{expected}`"
        );
    }

    let checklist = include_str!("../docs/PUBLIC_RELEASE_CHECKLIST.md");
    assert!(checklist.contains("./scripts/package-release.sh"));
    assert!(checklist.contains("Sigstore `.sig`/`.pem` signature files"));
    assert!(checklist.contains("build provenance attestations"));
    assert!(checklist.contains("packaging/homebrew/agent-os.rb"));
    assert!(checklist.contains("macOS Apple Silicon"));
    assert!(checklist.contains("Linux x86_64"));
}

#[test]
fn product_polish_artifacts_cover_landing_demo_and_screenshots() {
    let readme = include_str!("../README.md");
    let examples = include_str!("../examples/README.md");
    let landing = include_str!("../docs/landing.html");
    let preview = include_str!("../docs/assets/agent-os-dashboard-preview.svg");

    assert!(
        readme.contains("[landing page](docs/landing.html)"),
        "README should link the product landing page"
    );
    assert!(
        examples.contains("docs/landing.html"),
        "examples should point demo users to the landing page"
    );
    for expected in [
        "Why Agent OS",
        "Local demo",
        "Screenshot storyboard",
        "agent-os --state ./sandbox run --execute --limit 1",
        "agent-os-dashboard-preview.svg",
        "Agent OS dashboard preview",
    ] {
        assert!(
            landing.contains(expected),
            "landing page should include `{expected}`"
        );
    }
    for expected in [
        "<svg",
        "Agent OS dashboard preview",
        "Workflow Queue",
        "Run Log",
        "Approvals",
        "Memory",
        "Event Timeline",
    ] {
        assert!(
            preview.contains(expected),
            "dashboard preview asset should include `{expected}`"
        );
    }
}

#[test]
fn architecture_doc_contains_runtime_diagrams() {
    let architecture = include_str!("../docs/ARCHITECTURE.md");
    for expected in [
        "## Architecture Diagrams",
        "```mermaid",
        "flowchart LR",
        "sequenceDiagram",
        "CLI[\"CLI operator commands\"] --> Runtime",
        "API[\"Local HTTP API\"] --> Runtime",
        "Executor->>Store: finish run and task lifecycle",
    ] {
        assert!(
            architecture.contains(expected),
            "docs/ARCHITECTURE.md should include `{expected}`"
        );
    }
}

#[test]
fn api_smoke_tests_use_bounded_api_waits() {
    let source = include_str!("cli_smoke.rs");
    assert!(
        !source.contains(".wait().expect(\"api wait\")"),
        "API smoke tests should use wait_for_api_success so request-count drift fails fast"
    );
}

#[test]
fn readme_api_endpoint_list_matches_openapi_contract() {
    let readme = include_str!("../README.md");
    let documented = documented_readme_api_operations(readme);
    let schema = agent_os::openapi_schema();
    let expected = openapi_documented_operations(&schema);

    assert_eq!(documented, expected);
}

#[test]
fn readme_command_list_matches_top_level_cli_help() {
    let readme = include_str!("../README.md");
    let documented = documented_readme_top_level_commands(readme);
    let help = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--help"])
        .output()
        .expect("help");
    assert!(help.status.success(), "agent-os --help failed");
    let help = String::from_utf8(help.stdout).expect("help utf8");
    let actual = top_level_commands_from_help(&help);

    assert_eq!(documented, actual);
}

#[test]
fn readme_documents_global_cli_options_and_env_vars() {
    let readme = include_str!("../README.md");
    let help = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--help"])
        .output()
        .expect("help");
    assert!(help.status.success(), "agent-os --help failed");
    let help = String::from_utf8(help.stdout).expect("help utf8");
    let options = global_long_options_from_help(&help);

    assert_eq!(
        options,
        BTreeSet::from([
            "--config".to_owned(),
            "--json".to_owned(),
            "--state".to_owned()
        ])
    );
    for option in options {
        assert!(
            readme.contains(&format!("`{option}`")),
            "README should document global option `{option}`"
        );
    }
    for env_var in [
        "AGENT_OS_HOME",
        "AGENT_OS_CONFIG",
        "NO_COLOR",
        "CLICOLOR",
        "CLICOLOR_FORCE",
    ] {
        assert!(
            readme.contains(&format!("`{env_var}`")),
            "README should document {env_var}"
        );
    }
}

#[test]
fn readme_subcommand_lists_match_cli_help() {
    let readme = include_str!("../README.md");
    for command in [
        "config", "state", "agent", "task", "tool", "memory", "registry", "worker", "eval",
        "secrets", "approval", "git", "runs", "daemon", "service", "api", "workflow",
    ] {
        let documented = documented_readme_subcommands(readme, command);
        let help = Command::cargo_bin("agent-os")
            .expect("binary")
            .args([command, "--help"])
            .output()
            .unwrap_or_else(|error| panic!("agent-os {command} --help failed: {error}"));
        assert!(help.status.success(), "agent-os {command} --help failed");
        let help = String::from_utf8(help.stdout).expect("help utf8");
        let actual = commands_from_help(&help);

        assert_eq!(
            documented, actual,
            "README drift for `{command}` subcommands"
        );
    }
}

#[test]
fn readme_command_synopses_include_cli_help_options() {
    let readme = include_str!("../README.md");
    for synopsis in readme_command_synopsis_lines(readme) {
        let command_path = readme_synopsis_command_path(synopsis);
        let mut args = command_path.clone();
        args.push("--help".to_owned());
        let help = Command::cargo_bin("agent-os")
            .expect("binary")
            .args(&args)
            .output()
            .unwrap_or_else(|error| {
                panic!("agent-os {} --help failed: {error}", command_path.join(" "))
            });
        assert!(
            help.status.success(),
            "agent-os {} --help failed",
            command_path.join(" ")
        );
        let help = String::from_utf8(help.stdout).expect("help utf8");
        let help_options = local_long_options_from_help(&help);
        let documented_options = documented_long_options_from_synopsis(synopsis);

        assert_eq!(
            documented_options, help_options,
            "README synopsis `{synopsis}` local options drifted from CLI help"
        );
    }
}

#[test]
fn readme_completions_shells_match_cli_help() {
    let readme = include_str!("../README.md");
    let documented = documented_readme_completion_shells(readme);
    let help = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["completions", "--help"])
        .output()
        .expect("completions help");
    assert!(help.status.success(), "agent-os completions --help failed");
    let help = String::from_utf8(help.stdout).expect("completions help utf8");
    let actual = possible_values_from_help(&help, "<SHELL>");

    assert_eq!(documented, actual);

    for shell in actual {
        let output = Command::cargo_bin("agent-os")
            .expect("binary")
            .args(["completions", &shell])
            .output()
            .unwrap_or_else(|error| panic!("agent-os completions {shell} failed: {error}"));
        assert!(
            output.status.success(),
            "agent-os completions {shell} failed"
        );
        assert!(
            !output.stdout.is_empty(),
            "agent-os completions {shell} should emit a completion script"
        );
    }
}

#[test]
fn workflow_create_respects_task_dependencies() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let unrelated = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "task",
            "create",
            "Preexisting critical review",
            "--priority",
            "critical",
            "--need",
            "review",
        ])
        .output()
        .expect("create unrelated task");
    assert!(unrelated.status.success(), "unrelated task create failed");
    let unrelated: Value = serde_json::from_slice(&unrelated.stdout).expect("unrelated task json");
    let unrelated_task_id = unrelated["id"].as_str().expect("unrelated task id");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "workflow",
            "create",
            "Ship dependency-aware orchestration",
            "--execute",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Created workflow"))
        .stdout(predicate::str::contains("Executed run"));

    let workflows = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "workflow",
            "list",
            "--priority",
            "normal",
            "--query",
            "review",
            "--limit",
            "1",
        ])
        .output()
        .expect("workflow list");
    assert!(workflows.status.success(), "workflow list failed");
    let workflows: Value = serde_json::from_slice(&workflows.stdout).expect("workflows json");
    let workflow_id = workflows[0]["id"].as_str().expect("workflow id");
    let review_task_id = workflows[0]["tasks"]["review"]
        .as_str()
        .expect("review task id");
    assert_eq!(
        workflows[0]["objective"],
        "Ship dependency-aware orchestration"
    );
    let plan_task_id = workflows[0]["tasks"]["plan"]
        .as_str()
        .expect("plan task id");

    let runs = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--json", "runs", "list", "--limit", "1",
        ])
        .output()
        .expect("runs list");
    assert!(runs.status.success(), "runs list failed");
    let runs: Value = serde_json::from_slice(&runs.stdout).expect("runs json");
    assert_eq!(runs[0]["task_id"].as_str(), Some(plan_task_id));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "delete", unrelated_task_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("Deleted task"));

    let task_filtered_workflows = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "workflow",
            "list",
            "--task",
            review_task_id,
        ])
        .output()
        .expect("workflow list by task");
    assert!(
        task_filtered_workflows.status.success(),
        "workflow task filter failed"
    );
    let task_filtered_workflows: Value =
        serde_json::from_slice(&task_filtered_workflows.stdout).expect("workflows json");
    assert_eq!(task_filtered_workflows[0]["id"].as_str(), Some(workflow_id));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "workflow", "show", workflow_id])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "objective: Ship dependency-aware orchestration",
        ))
        .stdout(predicate::str::contains("plan:"))
        .stdout(predicate::str::contains("review:"));

    let workflow_status = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "workflow",
            "status",
            workflow_id,
        ])
        .output()
        .expect("workflow status");
    assert!(workflow_status.status.success(), "workflow status failed");
    let workflow_status: Value =
        serde_json::from_slice(&workflow_status.stdout).expect("workflow status json");
    assert_eq!(workflow_status["total_tasks"], 3);
    assert_eq!(workflow_status["tasks_complete"], 1);
    assert_eq!(workflow_status["current_stage"], "build");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_pending\": 2"))
        .stdout(predicate::str::contains("\"tasks_complete\": 1"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Executed run"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_pending\": 1"))
        .stdout(predicate::str::contains("\"tasks_complete\": 2"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "workflow", "status", workflow_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("tasks: 2/3 complete"))
        .stdout(predicate::str::contains("current stage: review"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "delete", review_task_id])
        .assert()
        .failure()
        .stderr(predicate::str::contains("workflow"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "workflow", "remove", workflow_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("Removed workflow"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "delete", review_task_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("Deleted task"));
}

#[test]
fn workflow_dag_editor_adds_edges_and_controls_stages() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let workflow = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "workflow",
            "create",
            "Edit workflow graph",
        ])
        .output()
        .expect("workflow create");
    assert!(workflow.status.success(), "workflow create failed");
    let workflow: Value = serde_json::from_slice(&workflow.stdout).expect("workflow json");
    let workflow_id = workflow["id"].as_str().expect("workflow id");

    let add = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "workflow",
            "add-task",
            workflow_id,
            "deploy",
            "Deploy release",
            "--after",
            "review",
            "--command",
            "printf deploy",
            "--need",
            "ops",
        ])
        .output()
        .expect("workflow add-task");
    assert!(add.status.success(), "workflow add-task failed");
    let add: Value = serde_json::from_slice(&add.stdout).expect("add json");
    assert_eq!(add["stage"], "deploy");
    assert_eq!(add["progress"]["total_tasks"], 4);

    let link = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "workflow",
            "link",
            workflow_id,
            "--from",
            "plan",
            "--to",
            "deploy",
        ])
        .output()
        .expect("workflow link");
    assert!(link.status.success(), "workflow link failed");
    let link: Value = serde_json::from_slice(&link.stdout).expect("link json");
    assert_eq!(link["linked"], true);

    let unlink = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "workflow",
            "unlink",
            workflow_id,
            "--from",
            "plan",
            "--to",
            "deploy",
        ])
        .output()
        .expect("workflow unlink");
    assert!(unlink.status.success(), "workflow unlink failed");
    let unlink: Value = serde_json::from_slice(&unlink.stdout).expect("unlink json");
    assert_eq!(unlink["linked"], false);

    let pause = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "workflow",
            "pause",
            workflow_id,
        ])
        .output()
        .expect("workflow pause");
    assert!(pause.status.success(), "workflow pause failed");
    let pause: Value = serde_json::from_slice(&pause.stdout).expect("pause json");
    assert_eq!(
        pause["affected_tasks"].as_array().expect("affected").len(),
        4
    );
    assert_eq!(pause["progress"]["tasks_blocked"], 4);

    let retry = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "workflow",
            "retry",
            workflow_id,
        ])
        .output()
        .expect("workflow retry");
    assert!(retry.status.success(), "workflow retry failed");
    let retry: Value = serde_json::from_slice(&retry.stdout).expect("retry json");
    assert_eq!(retry["progress"]["tasks_pending"], 4);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "workflow", "pause", workflow_id])
        .assert()
        .success();

    let resume = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "workflow",
            "resume",
            workflow_id,
        ])
        .output()
        .expect("workflow resume");
    assert!(resume.status.success(), "workflow resume failed");
    let resume: Value = serde_json::from_slice(&resume.stdout).expect("resume json");
    assert_eq!(resume["progress"]["tasks_pending"], 4);
}

#[test]
fn workflow_run_advances_next_ready_stage() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let workflow = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "workflow",
            "create",
            "Advance staged workflow",
            "--execute",
        ])
        .output()
        .expect("workflow create");
    assert!(workflow.status.success(), "workflow create failed");
    let workflow: Value = serde_json::from_slice(&workflow.stdout).expect("workflow json");
    let workflow_id = workflow["id"].as_str().expect("workflow id");
    assert!(workflow.get("objective").is_none());
    assert_eq!(
        workflow["errors"]
            .as_array()
            .expect("workflow errors")
            .len(),
        0
    );
    let build_task_id = workflow["tasks"]["build"].as_str().expect("build task id");
    let review_task_id = workflow["tasks"]["review"]
        .as_str()
        .expect("review task id");

    let remaining_run = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "workflow",
            "run",
            workflow_id,
            "--all",
        ])
        .output()
        .expect("workflow run all");
    assert!(remaining_run.status.success(), "workflow run all failed");
    let remaining_run: Value =
        serde_json::from_slice(&remaining_run.stdout).expect("workflow run all json");
    assert_eq!(
        remaining_run["errors"]
            .as_array()
            .expect("workflow run errors")
            .len(),
        0
    );
    assert_eq!(
        remaining_run["runs"][0]["task_id"].as_str(),
        Some(build_task_id)
    );
    assert_eq!(
        remaining_run["runs"][1]["task_id"].as_str(),
        Some(review_task_id)
    );
    assert_eq!(remaining_run["progress"]["tasks_complete"], 3);
    assert_eq!(remaining_run["progress"]["current_stage"], Value::Null);

    let cancellable = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "workflow",
            "create",
            "Cancel staged workflow",
            "--execute",
        ])
        .output()
        .expect("workflow create for cancel");
    assert!(
        cancellable.status.success(),
        "workflow create for cancel failed"
    );
    let cancellable: Value =
        serde_json::from_slice(&cancellable.stdout).expect("cancellable workflow json");
    assert_eq!(
        cancellable["errors"]
            .as_array()
            .expect("cancellable workflow errors")
            .len(),
        0
    );
    let cancellable_id = cancellable["id"].as_str().expect("cancellable workflow id");
    let cancellable_build_id = cancellable["tasks"]["build"]
        .as_str()
        .expect("cancellable build task id");
    let cancellable_review_id = cancellable["tasks"]["review"]
        .as_str()
        .expect("cancellable review task id");

    let cancelled = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "workflow",
            "cancel",
            cancellable_id,
            "--note",
            "stop",
        ])
        .output()
        .expect("workflow cancel");
    assert!(cancelled.status.success(), "workflow cancel failed");
    let cancelled: Value = serde_json::from_slice(&cancelled.stdout).expect("workflow cancel json");
    assert_eq!(
        cancelled["cancelled_tasks"]
            .as_array()
            .expect("cancelled tasks"),
        &vec![
            Value::String(cancellable_build_id.into()),
            Value::String(cancellable_review_id.into()),
        ]
    );
    assert_eq!(cancelled["progress"]["tasks_complete"], 1);
    assert_eq!(cancelled["progress"]["tasks_cancelled"], 2);
    assert_eq!(cancelled["progress"]["current_stage"], "build");
}

#[test]
fn end_to_end_operator_workflow_covers_cli_api_daemon_tools_memory_and_run_logs() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let workspace_arg = workspace.to_str().expect("workspace");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "memory",
            "add",
            "release-context",
            "Use the durable workflow smoke path.",
            "--tag",
            "release",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "write-handoff",
            "--kind",
            "file-write",
            "--need",
            "rust",
            "--cwd",
            workspace_arg,
            "--command-template",
            "{name}.txt",
        ])
        .assert()
        .success();

    let workflow = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "workflow",
            "create",
            "Prove planner builder reviewer release loop",
            "--execute",
        ])
        .output()
        .expect("workflow create");
    assert!(workflow.status.success(), "workflow create failed");
    let workflow: Value = serde_json::from_slice(&workflow.stdout).expect("workflow json");
    let workflow_id = workflow["id"].as_str().expect("workflow id");
    let plan_run_id = workflow["runs"][0]["id"].as_str().expect("plan run id");

    let mut api = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "e2e-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "4",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");
    let stdout = api.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");
    let auth = [("authorization", "Bearer e2e-token")];

    let memory = http_get(addr, "/memory?query=release-context&tag=release", &auth);
    assert!(memory.contains("HTTP/1.1 200 OK"), "{memory}");
    assert!(memory.contains("durable workflow smoke"), "{memory}");

    let run_workflow = http_request(
        addr,
        "POST",
        &format!("/workflows/{workflow_id}/run"),
        r#"{"all":true}"#,
        &auth,
    );
    assert!(run_workflow.contains("HTTP/1.1 200 OK"), "{run_workflow}");
    let run_workflow_body: Value =
        serde_json::from_str(http_body(&run_workflow)).expect("run workflow body");
    assert_eq!(run_workflow_body["progress"]["tasks_complete"], 3);
    assert_eq!(
        run_workflow_body["errors"]
            .as_array()
            .expect("errors")
            .len(),
        0
    );

    let workflow_status = http_get(addr, &format!("/workflows/{workflow_id}/status"), &auth);
    assert!(
        workflow_status.contains("HTTP/1.1 200 OK"),
        "{workflow_status}"
    );
    assert!(
        workflow_status.contains("\"tasks_complete\":3"),
        "{workflow_status}"
    );

    let api_logs = http_get(
        addr,
        &format!("/runs/{plan_run_id}/logs?tail_bytes=4096"),
        &auth,
    );
    assert!(api_logs.contains("HTTP/1.1 200 OK"), "{api_logs}");
    assert!(api_logs.contains("provider:"), "{api_logs}");
    wait_for_api_success(&mut api);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Write daemon handoff",
            "--need",
            "rust",
            "--tool",
            "write-handoff",
            "--arg",
            "name=handoff",
            "--arg",
            "body=daemon-note",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "daemon",
            "run",
            "--execute",
            "--limit",
            "1",
            "--interval-ms",
            "10",
            "--max-ticks",
            "2",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("tick 1 assigned 1 executed 1"))
        .stdout(predicate::str::contains("tick 2 assigned 0 executed 0"));

    let handoff = std::fs::read_to_string(workspace.join("handoff.txt")).expect("handoff file");
    assert_eq!(handoff, "daemon-note");

    let daemon_status = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "daemon", "status"])
        .output()
        .expect("daemon status");
    assert!(daemon_status.status.success(), "daemon status failed");
    let daemon_status: Value =
        serde_json::from_slice(&daemon_status.stdout).expect("daemon status json");
    assert_eq!(daemon_status["daemon"]["status"], "stopped");
    assert_eq!(daemon_status["daemon"]["ticks"], 2);

    let runs = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--json", "runs", "list", "--limit", "1",
        ])
        .output()
        .expect("runs list");
    assert!(runs.status.success(), "runs list failed");
    let runs: Value = serde_json::from_slice(&runs.stdout).expect("runs json");
    let daemon_run_id = runs[0]["id"].as_str().expect("daemon run id");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "logs", daemon_run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("file-write"))
        .stdout(predicate::str::contains("[wrote]"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "replay", daemon_run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("events:"))
        .stdout(predicate::str::contains("log:"));
}

#[test]
fn task_recover_requeues_stale_running_tasks() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Recover me",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Assigned task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_running\": 1"));

    let recovered = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "task",
            "recover",
            "--older-than-seconds",
            "0",
        ])
        .output()
        .expect("task recover json");
    assert!(recovered.status.success(), "task recover failed");
    let recovered: Value = serde_json::from_slice(&recovered.stdout).expect("recover json");
    assert_eq!(recovered["older_than_seconds"], 0);
    assert_eq!(
        recovered["recovered"]
            .as_array()
            .expect("recovered ids")
            .len(),
        1
    );
    assert_eq!(
        recovered["recovered_runs"]
            .as_array()
            .expect("recovered run ids")
            .len(),
        0
    );
    assert_eq!(recovered["recovered_daemon"], false);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_pending\": 1"))
        .stdout(predicate::str::contains("\"tasks_running\": 0"));
}

#[test]
fn api_can_recover_stale_running_tasks() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Recover through API",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Assigned task"));

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "recover-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "5",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");
    let auth = [("authorization", "Bearer recover-token")];

    let invalid = http_request(
        addr,
        "POST",
        "/tasks/recover",
        r#"{"older_than_seconds":-1}"#,
        &auth,
    );
    assert!(invalid.contains("HTTP/1.1 400 Bad Request"), "{invalid}");
    assert!(
        invalid.contains("older_than_seconds must be greater than or equal to 0"),
        "{invalid}"
    );

    let default_recovery = http_request(addr, "POST", "/tasks/recover", "", &auth);
    assert!(
        default_recovery.contains("HTTP/1.1 200 OK"),
        "{default_recovery}"
    );
    let default_recovery_body: Value =
        serde_json::from_str(http_body(&default_recovery)).expect("default recover body");
    assert_eq!(default_recovery_body["older_than_seconds"], 1800);
    assert_eq!(
        default_recovery_body["recovered"]
            .as_array()
            .expect("default recovered ids")
            .len(),
        0
    );
    assert_eq!(
        default_recovery_body["recovered_runs"]
            .as_array()
            .expect("default recovered run ids")
            .len(),
        0
    );
    assert_eq!(default_recovery_body["recovered_daemon"], false);
    let status_after_default = http_get(addr, "/status", &auth);
    assert!(
        status_after_default.contains("HTTP/1.1 200 OK"),
        "{status_after_default}"
    );
    assert!(
        status_after_default.contains("\"tasks_running\":1"),
        "{status_after_default}"
    );

    let recovered = http_request(
        addr,
        "POST",
        "/tasks/recover",
        r#"{"older_than_seconds":0}"#,
        &auth,
    );
    assert!(recovered.contains("HTTP/1.1 200 OK"), "{recovered}");
    let recovered_body: Value = serde_json::from_str(http_body(&recovered)).expect("recover body");
    assert_eq!(recovered_body["older_than_seconds"], 0);
    assert_eq!(
        recovered_body["recovered"]
            .as_array()
            .expect("recovered ids")
            .len(),
        1
    );
    assert_eq!(
        recovered_body["recovered_runs"]
            .as_array()
            .expect("recovered run ids")
            .len(),
        0
    );
    assert_eq!(recovered_body["recovered_daemon"], false);

    let status = http_get(addr, "/status", &auth);
    assert!(status.contains("HTTP/1.1 200 OK"), "{status}");
    assert!(status.contains("\"tasks_pending\":1"), "{status}");
    assert!(status.contains("\"tasks_running\":0"), "{status}");

    wait_for_api_success(&mut child);
}

#[test]
fn task_lifecycle_controls_validate_dependencies_and_release_capacity() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Bad dependency",
            "--after",
            "missing-task",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("dependency task not found"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "create", "Dependency"])
        .assert()
        .success();
    let dependency_id = first_task_id(state_arg);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Duplicate dependency",
            "--after",
            &format!("{dependency_id},{dependency_id}"),
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("duplicate dependency task"));

    let dependent_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "task",
            "create",
            "Depends on dependency",
            "--after",
            &dependency_id,
        ])
        .output()
        .expect("dependent task create");
    assert!(
        dependent_output.status.success(),
        "dependent task create failed"
    );
    let dependent: Value = serde_json::from_slice(&dependent_output.stdout).expect("task json");
    let dependent_id = dependent["id"].as_str().expect("dependent task id");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "delete", &dependency_id])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "delete dependent tasks, remove referencing workflows, or prune referencing runs first",
        ))
        .stderr(predicate::str::contains("depends on task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "delete", dependent_id])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "delete", &dependency_id])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Lifecycle task",
            "--need",
            "rust",
        ])
        .assert()
        .success();
    let task_id = first_task_id(state_arg);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Assigned task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "cancel", &task_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("Cancelled task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_cancelled\": 1"))
        .stdout(predicate::str::contains("\"tasks_running\": 0"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "retry",
            &task_id,
            "--note",
            "try again",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Retried task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "block", &task_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("Blocked task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "unblock", &task_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("Unblocked task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "delete", &task_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("Deleted task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "task", "list", "--all"])
        .assert()
        .success()
        .stdout(predicate::str::contains(&task_id).not());
}

#[test]
fn memory_records_can_be_shown_and_removed() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "memory",
            "add",
            "cleanup-topic",
            "cleanup-body",
            "--tag",
            "ops",
        ])
        .output()
        .expect("memory add");
    assert!(output.status.success(), "memory add failed");
    let body: Value = serde_json::from_slice(&output.stdout).expect("memory json");
    let memory_id = body["id"].as_str().expect("memory id");
    assert!(body["memory"]["updated_at"].as_str().is_some());

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "memory", "show", memory_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("cleanup-topic"))
        .stdout(predicate::str::contains("updated:"))
        .stdout(predicate::str::contains("cleanup-body"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "memory",
            "update",
            memory_id,
            "--topic",
            "updated-topic",
            "--body",
            "updated-body",
            "--tag",
            "reviewed",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"topic\": \"updated-topic\""))
        .stdout(predicate::str::contains("\"body\": \"updated-body\""))
        .stdout(predicate::str::contains("\"reviewed\""));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "memory",
            "update",
            memory_id,
            "--clear-tags",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Updated memory"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--json", "memory", "remove", memory_id,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"removed\": true"))
        .stdout(predicate::str::contains("\"topic\": \"updated-topic\""));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "memory", "show", memory_id])
        .assert()
        .failure()
        .stderr(predicate::str::contains("memory not found"));
}

#[test]
fn memory_prune_dry_run_reports_without_removing_then_prunes() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let old_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "memory",
            "add",
            "old-prune-topic",
            "old body",
        ])
        .output()
        .expect("old memory add");
    assert!(old_output.status.success(), "old memory add failed");
    let old_body: Value = serde_json::from_slice(&old_output.stdout).expect("old memory json");
    let old_id = old_body["id"].as_str().expect("old memory id");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "memory",
            "add",
            "fresh-prune-topic",
            "fresh body",
        ])
        .assert()
        .success();

    rewrite_memory_timestamp(&state, "old-prune-topic", "2020-01-01T00:00:00Z");

    let dry_run = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "memory",
            "prune",
            "--max-age-days",
            "1",
            "--dry-run",
        ])
        .output()
        .expect("memory prune dry run");
    assert!(dry_run.status.success(), "memory prune dry run failed");
    let dry_run_body: Value = serde_json::from_slice(&dry_run.stdout).expect("dry run json");
    assert_eq!(dry_run_body["dry_run"], true);
    assert_eq!(dry_run_body["expired"][0]["topic"], "old-prune-topic");
    assert_eq!(
        dry_run_body["removed"].as_array().expect("removed").len(),
        0
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "memory", "show", old_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("old-prune-topic"));

    let prune = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "memory",
            "prune",
            "--max-age-days",
            "1",
        ])
        .output()
        .expect("memory prune");
    assert!(prune.status.success(), "memory prune failed");
    let prune_body: Value = serde_json::from_slice(&prune.stdout).expect("prune json");
    assert_eq!(prune_body["dry_run"], false);
    assert_eq!(prune_body["removed"][0]["id"], old_id);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "memory", "show", old_id])
        .assert()
        .failure()
        .stderr(predicate::str::contains("memory not found"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "memory", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("fresh-prune-topic"))
        .stdout(predicate::str::contains("old-prune-topic").not());
}

#[test]
fn task_assign_manually_schedules_ready_task() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let dependency_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "task",
            "create",
            "Prepare manual assignment",
        ])
        .output()
        .expect("dependency task create");
    assert!(
        dependency_output.status.success(),
        "dependency create failed"
    );
    let dependency: Value =
        serde_json::from_slice(&dependency_output.stdout).expect("dependency json");
    let dependency_id = dependency["id"].as_str().expect("dependency id");

    let task_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "task",
            "create",
            "Manual assignment target",
            "--need",
            "rust",
            "--after",
            dependency_id,
        ])
        .output()
        .expect("target task create");
    assert!(task_output.status.success(), "target create failed");
    let task: Value = serde_json::from_slice(&task_output.stdout).expect("task json");
    let task_id = task["id"].as_str().expect("task id");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "assign", task_id, "builder"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("dependency"))
        .stderr(predicate::str::contains("is not complete"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "complete", dependency_id])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "assign", task_id, "architect"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("missing capabilities [rust]"));

    let assignment_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state", state_arg, "--json", "task", "assign", task_id, "builder",
        ])
        .output()
        .expect("assign task");
    assert!(assignment_output.status.success(), "assignment failed");
    let assignment: Value =
        serde_json::from_slice(&assignment_output.stdout).expect("assignment json");
    assert_eq!(assignment["assignment"]["task_id"], task_id);
    assert_eq!(assignment["assignment"]["agent_id"], "builder");
    assert_eq!(assignment["task"]["status"], "running");
    assert_eq!(assignment["task"]["assigned_to"], "builder");
    assert!(
        assignment["agent"]["current_tasks"]
            .as_array()
            .expect("current tasks")
            .iter()
            .any(|value| value.as_str() == Some(task_id))
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "delete", task_id])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be edited while running"));
}

#[test]
fn task_priority_updates_backlog_ordering_signal() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let task_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "task",
            "create",
            "Reprioritize me",
            "--priority",
            "low",
        ])
        .output()
        .expect("task create");
    assert!(task_output.status.success(), "task create failed");
    let task: Value = serde_json::from_slice(&task_output.stdout).expect("task json");
    let task_id = task["id"].as_str().expect("task id");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "task",
            "update",
            task_id,
            "--title",
            "Reprioritize me now",
            "--objective",
            "Updated objective",
            "--command",
            "printf updated",
            "--cwd",
            ".",
            "--need",
            "rust,test",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Reprioritize me now"))
        .stdout(predicate::str::contains("printf updated"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "update",
            task_id,
            "--clear-command",
            "--clear-cwd",
            "--clear-needs",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Updated task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "update", task_id])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "task update must include at least one field",
        ));

    let priority_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "task",
            "priority",
            task_id,
            "--priority",
            "urgent",
        ])
        .output()
        .expect("task priority json");
    assert!(priority_output.status.success(), "task priority failed");
    let priority_body: Value =
        serde_json::from_slice(&priority_output.stdout).expect("priority json");
    assert_eq!(priority_body["id"], task_id);
    assert_eq!(priority_body["task"]["id"], task_id);
    assert_eq!(priority_body["task"]["priority"], "critical");

    let updated = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "task", "show", task_id])
        .output()
        .expect("task show");
    assert!(updated.status.success(), "task show failed");
    let updated: Value = serde_json::from_slice(&updated.stdout).expect("updated task json");
    assert_eq!(updated["priority"], "critical");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "priority",
            task_id,
            "--priority",
            "urgent-ish",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid priority"))
        .stderr(predicate::str::contains("critical/urgent"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "complete", task_id])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "priority",
            task_id,
            "--priority",
            "low",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be edited while complete"));

    let completed = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "task", "show", task_id])
        .output()
        .expect("completed task show");
    assert!(completed.status.success(), "completed task show failed");
    let completed: Value = serde_json::from_slice(&completed.stdout).expect("completed task json");
    assert_eq!(completed["priority"], "critical");
}

#[test]
fn task_update_can_replace_and_clear_tool_invocations() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "tool",
            "add",
            "Printer",
            "--need",
            "shell",
            "--command-template",
            "printf {message}",
        ])
        .assert()
        .success();

    let task_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "task",
            "create",
            "Tool task",
            "--tool",
            "printer",
            "--arg",
            "message=hello",
        ])
        .output()
        .expect("task create");
    assert!(task_output.status.success(), "task create failed");
    let task: Value = serde_json::from_slice(&task_output.stdout).expect("task json");
    let task_id = task["id"].as_str().expect("task id");

    let updated_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "task",
            "update",
            task_id,
            "--arg",
            "message=updated",
        ])
        .output()
        .expect("task update");
    assert!(updated_output.status.success(), "task update failed");
    let updated: Value = serde_json::from_slice(&updated_output.stdout).expect("updated task json");
    assert_eq!(updated["task"]["tool"]["args"]["message"], "updated");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "update",
            task_id,
            "--clear-tool",
            "--command",
            "printf fallback",
        ])
        .assert()
        .success();

    let shown = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "task", "show", task_id])
        .output()
        .expect("task show");
    assert!(shown.status.success(), "task show failed");
    let shown: Value = serde_json::from_slice(&shown.stdout).expect("shown task json");
    assert!(shown["tool"].is_null());
    assert_eq!(shown["command"], "printf fallback");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "update",
            task_id,
            "--arg",
            "message=nope",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("task has no tool invocation"));
}

#[test]
fn task_dependencies_can_be_replaced_and_cleared() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let dependency_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "task",
            "create",
            "Dependency source",
        ])
        .output()
        .expect("dependency create");
    assert!(
        dependency_output.status.success(),
        "dependency create failed"
    );
    let dependency: Value =
        serde_json::from_slice(&dependency_output.stdout).expect("dependency json");
    let dependency_id = dependency["id"].as_str().expect("dependency id");

    let task_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "task",
            "create",
            "Dependency target",
        ])
        .output()
        .expect("task create");
    assert!(task_output.status.success(), "task create failed");
    let task: Value = serde_json::from_slice(&task_output.stdout).expect("task json");
    let task_id = task["id"].as_str().expect("task id");

    let dependency_update = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "task",
            "dependencies",
            task_id,
            "--after",
            dependency_id,
        ])
        .output()
        .expect("task dependencies json");
    assert!(
        dependency_update.status.success(),
        "dependency update failed"
    );
    let dependency_update: Value =
        serde_json::from_slice(&dependency_update.stdout).expect("dependency update json");
    assert_eq!(dependency_update["id"], task_id);
    assert_eq!(dependency_update["task"]["id"], task_id);
    assert_eq!(
        dependency_update["task"]["dependencies"]
            .as_array()
            .expect("dependencies")[0],
        dependency_id
    );

    let updated = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "task", "show", task_id])
        .output()
        .expect("task show");
    assert!(updated.status.success(), "task show failed");
    let updated: Value = serde_json::from_slice(&updated.stdout).expect("updated task json");
    assert_eq!(
        updated["dependencies"].as_array().expect("dependencies")[0],
        dependency_id
    );

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "dependencies",
            task_id,
            "--after",
            task_id,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("task cannot depend on itself"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "dependencies",
            task_id,
            "--clear",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Cleared dependencies"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "complete", task_id])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "dependencies",
            task_id,
            "--after",
            dependency_id,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be edited while complete"));
}

#[test]
fn api_can_manually_assign_task() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let task_output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "--json",
            "task",
            "create",
            "API manual assignment",
            "--need",
            "rust",
        ])
        .output()
        .expect("task create");
    assert!(task_output.status.success(), "task create failed");
    let task: Value = serde_json::from_slice(&task_output.stdout).expect("task json");
    let task_id = task["id"].as_str().expect("task id");

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "5",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let priority = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/priority"),
        r#"{"priority":"urgent"}"#,
        &[],
    );
    assert!(priority.contains("HTTP/1.1 200 OK"), "{priority}");
    let priority_body: Value = serde_json::from_str(http_body(&priority)).expect("priority json");
    assert_eq!(priority_body["task"]["priority"], "critical");

    let self_dependency = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/dependencies"),
        &format!(r#"{{"dependencies":["{task_id}"]}}"#),
        &[],
    );
    assert!(
        self_dependency.contains("HTTP/1.1 400 Bad Request"),
        "{self_dependency}"
    );
    assert!(
        self_dependency.contains("task cannot depend on itself"),
        "{self_dependency}"
    );

    let clear_dependencies = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/dependencies"),
        r#"{"dependencies":[]}"#,
        &[],
    );
    assert!(
        clear_dependencies.contains("HTTP/1.1 200 OK"),
        "{clear_dependencies}"
    );

    let response = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/assign"),
        r#"{"agent":"builder"}"#,
        &[],
    );
    assert!(response.contains("HTTP/1.1 200 OK"), "{response}");
    let body: Value = serde_json::from_str(http_body(&response)).expect("assignment json");
    assert_eq!(body["assignment"]["task_id"], task_id);
    assert_eq!(body["assignment"]["agent_id"], "builder");
    assert_eq!(body["task"]["status"], "running");

    let delete_running = http_request(addr, "DELETE", &format!("/tasks/{task_id}"), "", &[]);
    assert!(
        delete_running.contains("HTTP/1.1 409 Conflict"),
        "{delete_running}"
    );
    assert!(
        delete_running.contains("cannot be edited while running"),
        "{delete_running}"
    );

    wait_for_api_success(&mut child);
}

#[test]
fn api_serve_exposes_status_json() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "7",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let health = http_get(addr, "/health", &[]);
    assert!(health.contains("HTTP/1.1 200 OK"), "{health}");
    assert!(health.contains("\"ok\":true"), "{health}");
    assert!(
        health.contains(&format!(
            "\"agent_os_version\":\"{}\"",
            env!("CARGO_PKG_VERSION")
        )),
        "{health}"
    );
    assert!(health.contains("\"state_loads\":true"), "{health}");
    assert!(health.contains("\"state_valid\":true"), "{health}");
    assert!(health.contains("\"state_issue_count\":0"), "{health}");
    assert!(health.contains("\"state_error\":null"), "{health}");
    assert!(health.contains("\"config_exists\":false"), "{health}");
    assert!(health.contains("\"config_loads\":false"), "{health}");
    assert!(health.contains("\"config_valid\":null"), "{health}");

    let metrics = http_get(addr, "/metrics", &[]);
    assert!(metrics.contains("HTTP/1.1 200 OK"), "{metrics}");
    assert!(metrics.contains("\"ok\":true"), "{metrics}");
    assert!(
        metrics.contains(&format!(
            "\"agent_os_version\":\"{}\"",
            env!("CARGO_PKG_VERSION")
        )),
        "{metrics}"
    );
    assert!(metrics.contains("\"agents_total\""), "{metrics}");
    assert!(metrics.contains("\"tasks_pending\""), "{metrics}");
    assert!(metrics.contains("\"runs_success\""), "{metrics}");

    let prometheus = http_get(addr, "/metrics/prometheus", &[]);
    assert!(prometheus.contains("HTTP/1.1 200 OK"), "{prometheus}");
    assert!(
        prometheus.contains("content-type: text/plain; version=0.0.4; charset=utf-8"),
        "{prometheus}"
    );
    assert!(
        prometheus.contains("# TYPE agent_os_tasks_total gauge"),
        "{prometheus}"
    );
    assert!(
        prometheus.contains("agent_os_run_duration_ms_bucket{le=\"+Inf\"}"),
        "{prometheus}"
    );

    let mut stream = connect_with_retry(addr);
    stream
        .write_all(b"GET /status HTTP/1.1\r\nhost: localhost\r\n\r\n")
        .expect("write request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");

    assert!(response.contains("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains("\r\nx-trace-id: "), "{response}");
    assert!(response.contains("\"tasks_pending\""), "{response}");
    assert!(
        response.contains(&format!(
            "\"agent_os_version\":\"{}\"",
            env!("CARGO_PKG_VERSION")
        )),
        "{response}"
    );
    assert!(response.contains("\"daemon_status\""), "{response}");

    let daemon = http_get(addr, "/daemon", &[]);
    assert!(daemon.contains("HTTP/1.1 200 OK"), "{daemon}");
    assert!(daemon.contains("\"daemon\""), "{daemon}");

    let events = http_get(addr, "/events?limit=1", &[]);
    assert!(events.contains("HTTP/1.1 200 OK"), "{events}");
    assert!(events.contains("\"kind\""), "{events}");
    assert!(events.contains("\"message\""), "{events}");

    let schema = http_get(addr, "/openapi.json", &[]);
    assert!(schema.contains("HTTP/1.1 200 OK"), "{schema}");
    assert!(schema.contains("Agent OS Local API"), "{schema}");
    let live_schema: Value = serde_json::from_str(http_body(&schema)).expect("live schema json");
    assert_eq!(live_schema, agent_os::openapi_schema());

    wait_for_api_success(&mut child);
}

#[test]
fn api_health_reports_state_load_errors() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("missing-state");
    let state_arg = state.to_str().expect("state");

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "4",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let health = http_get(addr, "/health", &[]);
    assert!(
        health.contains("HTTP/1.1 503 Service Unavailable"),
        "{health}"
    );
    assert!(health.contains("\"ok\":false"), "{health}");
    assert!(
        health.contains(&format!(
            "\"agent_os_version\":\"{}\"",
            env!("CARGO_PKG_VERSION")
        )),
        "{health}"
    );
    assert!(health.contains("\"state_loads\":false"), "{health}");
    assert!(health.contains("\"state_valid\":null"), "{health}");
    assert!(health.contains("\"state_error\":"), "{health}");
    assert!(health.contains("\"config_exists\":false"), "{health}");
    assert!(health.contains("\"config_valid\":null"), "{health}");

    let metrics = http_get(addr, "/metrics", &[]);
    assert!(
        metrics.contains("HTTP/1.1 503 Service Unavailable"),
        "{metrics}"
    );
    assert!(metrics.contains("\"ok\":false"), "{metrics}");
    assert!(
        metrics.contains(&format!(
            "\"agent_os_version\":\"{}\"",
            env!("CARGO_PKG_VERSION")
        )),
        "{metrics}"
    );
    assert!(metrics.contains("\"state_loads\":false"), "{metrics}");
    assert!(metrics.contains("\"agents_total\":0"), "{metrics}");

    let schema = http_get(addr, "/openapi.json", &[]);
    assert!(schema.contains("HTTP/1.1 200 OK"), "{schema}");
    assert!(schema.contains("Agent OS Local API"), "{schema}");
    assert!(schema.contains("\"/health\""), "{schema}");
    let live_schema: Value =
        serde_json::from_str(http_body(&schema)).expect("unavailable live schema json");
    assert_eq!(live_schema, agent_os::openapi_schema());

    let status_response = http_get(addr, "/status", &[]);
    assert!(
        status_response.contains("HTTP/1.1 500 Internal Server Error"),
        "{status_response}"
    );
    assert!(
        status_response.contains("\"error\":\"state unavailable\""),
        "{status_response}"
    );

    wait_for_api_success(&mut child);
}

#[test]
fn api_health_reports_config_validation() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let config = dir.path().join("agent-os.toml");
    let state_arg = state.to_str().expect("state");
    let config_arg = config.to_str().expect("config");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    std::fs::write(&config, "name = '   '\n").expect("write invalid config");

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "--config",
            config_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "1",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let health = http_get(addr, "/health", &[]);
    assert!(health.contains("HTTP/1.1 200 OK"), "{health}");
    assert!(health.contains("\"ok\":false"), "{health}");
    assert!(health.contains("\"state_loads\":true"), "{health}");
    assert!(health.contains("\"state_valid\":true"), "{health}");
    assert!(health.contains("\"config_exists\":true"), "{health}");
    assert!(health.contains("\"config_loads\":true"), "{health}");
    assert!(health.contains("\"config_valid\":false"), "{health}");
    assert!(health.contains("OS name must not be empty"), "{health}");

    wait_for_api_success(&mut child);
}

#[test]
fn api_config_endpoints_do_not_require_state() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("missing-state");
    let config = dir.path().join("agent-os.toml");
    let state_arg = state.to_str().expect("state");
    let config_arg = config.to_str().expect("config");

    std::fs::write(
        &config,
        r#"
name = "Config API OS"

[[agents]]
name = "api-planner"
kind = "planner"
capabilities = ["plan"]
parallel = 1
"#,
    )
    .expect("write config");

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "--config",
            config_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "2",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let config_response = http_get(addr, "/config", &[]);
    assert!(
        config_response.contains("HTTP/1.1 200 OK"),
        "{config_response}"
    );
    assert!(
        config_response.contains("\"exists\":true"),
        "{config_response}"
    );
    assert!(
        config_response.contains("\"name\":\"Config API OS\""),
        "{config_response}"
    );
    assert!(config_response.contains("api-planner"), "{config_response}");

    let validation = http_get(addr, "/config/validate", &[]);
    assert!(validation.contains("HTTP/1.1 200 OK"), "{validation}");
    assert!(
        validation.contains("\"config_exists\":true"),
        "{validation}"
    );
    assert!(validation.contains("\"config_loads\":true"), "{validation}");
    assert!(validation.contains("\"config_valid\":true"), "{validation}");
    assert!(validation.contains("\"config_issues\":[]"), "{validation}");

    wait_for_api_success(&mut child);
}

#[test]
fn api_can_write_default_config() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("missing-state");
    let config = dir.path().join("agent-os.toml");
    let state_arg = state.to_str().expect("state");
    let config_arg = config.to_str().expect("config");

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "--config",
            config_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "4",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let written = http_request(addr, "POST", "/config", "", &[]);
    assert!(written.contains("HTTP/1.1 201 Created"), "{written}");
    assert!(written.contains("\"written\":true"), "{written}");
    assert!(written.contains("\"profile\":\"safe\""), "{written}");
    assert!(written.contains("\"allow_shell\":false"), "{written}");
    assert!(written.contains("\"name\":\"Agent OS\""), "{written}");
    assert!(config.exists());

    let conflict = http_request(addr, "POST", "/config", "{}", &[]);
    assert!(conflict.contains("HTTP/1.1 409 Conflict"), "{conflict}");
    assert!(conflict.contains("config conflict"), "{conflict}");

    std::fs::write(&config, "name = 'Custom'\n").expect("custom config");
    let forced = http_request(
        addr,
        "POST",
        "/config",
        r#"{"force":true,"profile":"ci"}"#,
        &[],
    );
    assert!(forced.contains("HTTP/1.1 201 Created"), "{forced}");
    assert!(forced.contains("\"written\":true"), "{forced}");
    assert!(forced.contains("\"profile\":\"ci\""), "{forced}");
    assert!(
        forced.contains("\"allowed_commands\":[\"cargo\",\"rustc\",\"git\"]"),
        "{forced}"
    );

    let loaded = http_get(addr, "/config", &[]);
    assert!(loaded.contains("HTTP/1.1 200 OK"), "{loaded}");
    assert!(loaded.contains("\"exists\":true"), "{loaded}");
    assert!(loaded.contains("\"name\":\"Agent OS\""), "{loaded}");

    wait_for_api_success(&mut child);
}

#[test]
fn api_can_render_launchd_service() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("missing-state");
    let bin_path = dir.path().join("bin").join("agent-os");
    let plist_path = dir.path().join("agent-os.plist");
    let state_arg = state.to_str().expect("state");

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "3",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let default_rendered = http_request(addr, "POST", "/service/launchd", "", &[]);
    assert!(
        default_rendered.contains("HTTP/1.1 200 OK"),
        "{default_rendered}"
    );
    assert!(
        default_rendered.contains("\"label\":\"com.infinite-apps.agent-os\""),
        "{default_rendered}"
    );
    assert!(
        default_rendered.contains("\"interval_ms\":1000"),
        "{default_rendered}"
    );
    assert!(
        default_rendered.contains("\"limit\":1"),
        "{default_rendered}"
    );
    assert!(
        default_rendered.contains("\"execute\":false"),
        "{default_rendered}"
    );
    assert!(
        !default_rendered.contains("--execute"),
        "{default_rendered}"
    );
    assert!(
        default_rendered.contains("daemon.out.log"),
        "{default_rendered}"
    );
    assert!(
        default_rendered.contains("com.infinite-apps.agent-os.plist"),
        "{default_rendered}"
    );

    let body = serde_json::json!({
        "label": "com.example.agent-os",
        "bin_path": bin_path,
        "interval_ms": 250,
        "limit": 2,
        "execute": true,
        "recover_stale_seconds": 30,
        "plist_path": plist_path,
    })
    .to_string();
    let rendered = http_request(addr, "POST", "/service/launchd", &body, &[]);
    assert!(rendered.contains("HTTP/1.1 200 OK"), "{rendered}");
    assert!(rendered.contains("\"platform\":\"launchd\""), "{rendered}");
    assert!(
        rendered.contains("\"label\":\"com.example.agent-os\""),
        "{rendered}"
    );
    assert!(rendered.contains("\"interval_ms\":250"), "{rendered}");
    assert!(rendered.contains("\"limit\":2"), "{rendered}");
    assert!(rendered.contains("--recover-stale-seconds"), "{rendered}");
    assert!(
        rendered.contains("<key>ProgramArguments</key>"),
        "{rendered}"
    );
    assert!(rendered.contains("daemon.out.log"), "{rendered}");

    let invalid = http_request(
        addr,
        "POST",
        "/service/launchd",
        r#"{"label":" ","interval_ms":250}"#,
        &[],
    );
    assert!(invalid.contains("HTTP/1.1 400 Bad Request"), "{invalid}");
    assert!(
        invalid.contains("service label must not be empty"),
        "{invalid}"
    );

    wait_for_api_success(&mut child);
}

#[test]
fn api_can_install_and_uninstall_launchd_service() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("missing-state");
    let home = dir.path().join("home");
    let default_plist_path = home
        .join("Library")
        .join("LaunchAgents")
        .join("com.infinite-apps.agent-os.plist");
    let bin_path = dir.path().join("bin").join("agent-os");
    let plist_path = dir.path().join("LaunchAgents").join("agent-os.plist");
    let state_arg = state.to_str().expect("state");

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("HOME", &home)
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "5",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let default_installed = http_request(addr, "POST", "/service/launchd/install", "", &[]);
    assert!(
        default_installed.contains("HTTP/1.1 200 OK"),
        "{default_installed}"
    );
    assert!(
        default_installed.contains("\"installed\":true"),
        "{default_installed}"
    );
    assert!(
        default_installed.contains("com.infinite-apps.agent-os.plist"),
        "{default_installed}"
    );
    assert!(default_plist_path.exists());

    let default_removed = http_request(addr, "POST", "/service/launchd/uninstall", "", &[]);
    assert!(
        default_removed.contains("HTTP/1.1 200 OK"),
        "{default_removed}"
    );
    assert!(
        default_removed.contains("\"removed\":true"),
        "{default_removed}"
    );
    assert!(!default_plist_path.exists());

    let body = serde_json::json!({
        "label": "com.example.agent-os",
        "bin_path": bin_path,
        "interval_ms": 250,
        "limit": 2,
        "execute": true,
        "plist_path": plist_path,
    })
    .to_string();
    let installed = http_request(addr, "POST", "/service/launchd/install", &body, &[]);
    assert!(installed.contains("HTTP/1.1 200 OK"), "{installed}");
    assert!(installed.contains("\"installed\":true"), "{installed}");
    assert!(plist_path.exists());
    let plist_body = std::fs::read_to_string(&plist_path).expect("plist body");
    assert!(plist_body.contains("com.example.agent-os"));
    assert!(plist_body.contains("--execute"));
    assert!(!plist_path.with_extension("plist.tmp").exists());

    let uninstall_body = serde_json::json!({
        "label": "com.example.agent-os",
        "plist_path": plist_path,
    })
    .to_string();
    let removed = http_request(
        addr,
        "POST",
        "/service/launchd/uninstall",
        &uninstall_body,
        &[],
    );
    assert!(removed.contains("HTTP/1.1 200 OK"), "{removed}");
    assert!(removed.contains("\"removed\":true"), "{removed}");
    assert!(!plist_path.exists());

    let missing = http_request(
        addr,
        "POST",
        "/service/launchd/uninstall",
        &uninstall_body,
        &[],
    );
    assert!(missing.contains("HTTP/1.1 200 OK"), "{missing}");
    assert!(missing.contains("\"removed\":false"), "{missing}");

    wait_for_api_success(&mut child);
}

#[test]
#[cfg(unix)]
fn api_can_control_launchd_service_with_launchctl() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("missing-state");
    let home = dir.path().join("home");
    let default_plist = home
        .join("Library")
        .join("LaunchAgents")
        .join("com.infinite-apps.agent-os.plist");
    let plist = dir.path().join("com.example.agent-os.plist");
    let fake_bin = dir.path().join("bin");
    let fake_launchctl = fake_bin.join("launchctl");
    let failing_launchctl = dir.path().join("launchctl-fail");
    let log = dir.path().join("launchctl.log");
    std::fs::create_dir_all(default_plist.parent().expect("default plist parent"))
        .expect("default plist parent");
    std::fs::write(&plist, "<plist/>").expect("plist");
    std::fs::write(&default_plist, "<plist/>").expect("default plist");
    std::fs::create_dir_all(&fake_bin).expect("fake launchctl bin");
    std::fs::write(
        &fake_launchctl,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$AGENT_OS_LAUNCHCTL_LOG\"\nif [ \"$1\" = \"print\" ]; then echo 'state = running'; fi\nexit 0\n",
    )
    .expect("fake launchctl");
    make_executable(&fake_launchctl);
    std::fs::write(
        &failing_launchctl,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$AGENT_OS_LAUNCHCTL_LOG\"\necho 'bootout failed' >&2\nexit 42\n",
    )
    .expect("failing launchctl");
    make_executable(&failing_launchctl);

    let state_arg = state.to_str().expect("state");
    let log_arg = log.to_str().expect("log");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("HOME", &home)
        .env("AGENT_OS_LAUNCHCTL_LOG", log_arg)
        .env("PATH", path)
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "8",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let default_started = http_request(addr, "POST", "/service/launchd/start", "", &[]);
    assert!(
        default_started.contains("HTTP/1.1 200 OK"),
        "{default_started}"
    );
    assert!(
        default_started.contains("\"label\":\"com.infinite-apps.agent-os\""),
        "{default_started}"
    );
    assert!(
        default_started.contains("\"domain\":\"gui/"),
        "{default_started}"
    );

    let default_status = http_request(addr, "POST", "/service/launchd/status", "", &[]);
    assert!(
        default_status.contains("HTTP/1.1 200 OK"),
        "{default_status}"
    );
    assert!(
        default_status.contains("\"loaded\":true"),
        "{default_status}"
    );
    assert!(
        default_status.contains("state = running"),
        "{default_status}"
    );

    let default_stopped = http_request(addr, "POST", "/service/launchd/stop", "", &[]);
    assert!(
        default_stopped.contains("HTTP/1.1 200 OK"),
        "{default_stopped}"
    );
    assert!(
        default_stopped.contains("\"stopped\":true"),
        "{default_stopped}"
    );

    let body = serde_json::json!({
        "label": "com.example.agent-os",
        "plist_path": plist,
        "domain": "gui/test",
        "launchctl_path": fake_launchctl,
    })
    .to_string();
    let started = http_request(addr, "POST", "/service/launchd/start", &body, &[]);
    assert!(started.contains("HTTP/1.1 200 OK"), "{started}");
    assert!(started.contains("\"started\":true"), "{started}");
    assert!(started.contains("\"domain\":\"gui/test\""), "{started}");

    let status = http_request(addr, "POST", "/service/launchd/status", &body, &[]);
    assert!(status.contains("HTTP/1.1 200 OK"), "{status}");
    assert!(status.contains("\"loaded\":true"), "{status}");
    assert!(status.contains("state = running"), "{status}");

    let stopped = http_request(addr, "POST", "/service/launchd/stop", &body, &[]);
    assert!(stopped.contains("HTTP/1.1 200 OK"), "{stopped}");
    assert!(stopped.contains("\"stopped\":true"), "{stopped}");

    let failing_body = serde_json::json!({
        "label": "com.example.agent-os",
        "plist_path": plist,
        "domain": "gui/test",
        "launchctl_path": failing_launchctl,
    })
    .to_string();
    let failed_stop = http_request(addr, "POST", "/service/launchd/stop", &failing_body, &[]);
    assert!(
        failed_stop.contains("HTTP/1.1 409 Conflict"),
        "{failed_stop}"
    );
    assert!(
        failed_stop.contains("\"error\":\"service command failed\""),
        "{failed_stop}"
    );
    assert!(failed_stop.contains("\"launchctl\":"), "{failed_stop}");
    assert!(failed_stop.contains("\"success\":false"), "{failed_stop}");
    assert!(failed_stop.contains("\"status\":42"), "{failed_stop}");
    assert!(failed_stop.contains("bootout failed"), "{failed_stop}");

    let invalid = http_request(
        addr,
        "POST",
        "/service/launchd/status",
        r#"{"domain":" ","launchctl_path":"launchctl"}"#,
        &[],
    );
    assert!(invalid.contains("HTTP/1.1 400 Bad Request"), "{invalid}");
    assert!(
        invalid.contains("launchd domain must not be empty"),
        "{invalid}"
    );

    wait_for_api_success(&mut child);

    let calls = std::fs::read_to_string(log).expect("launchctl log");
    let plist_arg = plist.to_str().expect("plist");
    assert!(calls.contains(&format!("bootstrap gui/test {plist_arg}")));
    assert!(calls.contains("print gui/test/com.example.agent-os"));
    assert!(calls.contains(&format!("bootout gui/test {plist_arg}")));
}

#[test]
fn api_can_render_install_and_uninstall_systemd_service() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("missing-state");
    let home = dir.path().join("home");
    let default_unit_path = home
        .join(".config")
        .join("systemd")
        .join("user")
        .join("agent-os.service");
    let bin_path = dir.path().join("bin").join("agent-os");
    let unit_path = dir.path().join("systemd").join("agent-os-test.service");
    let state_arg = state.to_str().expect("state");

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("HOME", &home)
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "7",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let default_rendered = http_request(addr, "POST", "/service/systemd", "", &[]);
    assert!(
        default_rendered.contains("HTTP/1.1 200 OK"),
        "{default_rendered}"
    );
    assert!(
        default_rendered.contains("\"unit_name\":\"agent-os.service\""),
        "{default_rendered}"
    );
    assert!(
        default_rendered.contains("\"interval_ms\":1000"),
        "{default_rendered}"
    );
    assert!(
        default_rendered.contains("\"limit\":1"),
        "{default_rendered}"
    );
    assert!(default_rendered.contains("[Service]"), "{default_rendered}");
    assert!(
        default_rendered.contains(".config/systemd/user/agent-os.service"),
        "{default_rendered}"
    );

    let body = serde_json::json!({
        "unit_name": "agent-os-test.service",
        "bin_path": bin_path,
        "interval_ms": 250,
        "limit": 2,
        "execute": true,
        "recover_stale_seconds": 30,
        "unit_path": unit_path,
    })
    .to_string();
    let rendered = http_request(addr, "POST", "/service/systemd", &body, &[]);
    assert!(rendered.contains("HTTP/1.1 200 OK"), "{rendered}");
    assert!(rendered.contains("\"platform\":\"systemd\""), "{rendered}");
    assert!(
        rendered.contains("\"unit_name\":\"agent-os-test.service\""),
        "{rendered}"
    );
    assert!(rendered.contains("\"interval_ms\":250"), "{rendered}");
    assert!(rendered.contains("\"limit\":2"), "{rendered}");
    assert!(rendered.contains("--execute"), "{rendered}");
    assert!(rendered.contains("--recover-stale-seconds"), "{rendered}");

    let invalid = http_request(
        addr,
        "POST",
        "/service/systemd",
        r#"{"unit_name":" ","interval_ms":250}"#,
        &[],
    );
    assert!(invalid.contains("HTTP/1.1 400 Bad Request"), "{invalid}");
    assert!(
        invalid.contains("service label must not be empty"),
        "{invalid}"
    );

    let default_installed = http_request(addr, "POST", "/service/systemd/install", "", &[]);
    assert!(
        default_installed.contains("HTTP/1.1 200 OK"),
        "{default_installed}"
    );
    assert!(
        default_installed.contains("\"installed\":true"),
        "{default_installed}"
    );
    assert!(default_unit_path.exists());

    let default_removed = http_request(addr, "POST", "/service/systemd/uninstall", "", &[]);
    assert!(
        default_removed.contains("HTTP/1.1 200 OK"),
        "{default_removed}"
    );
    assert!(
        default_removed.contains("\"removed\":true"),
        "{default_removed}"
    );
    assert!(!default_unit_path.exists());

    let installed = http_request(addr, "POST", "/service/systemd/install", &body, &[]);
    assert!(installed.contains("HTTP/1.1 200 OK"), "{installed}");
    assert!(installed.contains("\"installed\":true"), "{installed}");
    assert!(unit_path.exists());
    let unit_body = std::fs::read_to_string(&unit_path).expect("systemd unit");
    assert!(unit_body.contains("agent-os-test.service"));
    assert!(unit_body.contains("--execute"));
    assert!(!unit_path.with_extension("service.tmp").exists());

    let uninstall_body = serde_json::json!({
        "unit_name": "agent-os-test.service",
        "unit_path": unit_path,
    })
    .to_string();
    let removed = http_request(
        addr,
        "POST",
        "/service/systemd/uninstall",
        &uninstall_body,
        &[],
    );
    assert!(removed.contains("HTTP/1.1 200 OK"), "{removed}");
    assert!(removed.contains("\"removed\":true"), "{removed}");
    assert!(!unit_path.exists());

    wait_for_api_success(&mut child);
}

#[test]
#[cfg(unix)]
fn api_can_control_systemd_service_with_systemctl() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("missing-state");
    let fake_systemctl = dir.path().join("systemctl");
    let failing_systemctl = dir.path().join("systemctl-fail");
    let log = dir.path().join("systemctl.log");
    std::fs::write(
        &fake_systemctl,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$AGENT_OS_SYSTEMCTL_LOG\"\nif [ \"$2\" = \"is-active\" ]; then echo active; fi\nexit 0\n",
    )
    .expect("fake systemctl");
    make_executable(&fake_systemctl);
    std::fs::write(
        &failing_systemctl,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$AGENT_OS_SYSTEMCTL_LOG\"\necho 'stop failed' >&2\nexit 42\n",
    )
    .expect("failing systemctl");
    make_executable(&failing_systemctl);

    let state_arg = state.to_str().expect("state");
    let log_arg = log.to_str().expect("log");

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_SYSTEMCTL_LOG", log_arg)
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "5",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let body = serde_json::json!({
        "unit_name": "agent-os-test.service",
        "systemctl_path": fake_systemctl,
    })
    .to_string();
    let started = http_request(addr, "POST", "/service/systemd/start", &body, &[]);
    assert!(started.contains("HTTP/1.1 200 OK"), "{started}");
    assert!(started.contains("\"started\":true"), "{started}");
    assert!(
        started.contains("\"unit_name\":\"agent-os-test.service\""),
        "{started}"
    );

    let status = http_request(addr, "POST", "/service/systemd/status", &body, &[]);
    assert!(status.contains("HTTP/1.1 200 OK"), "{status}");
    assert!(status.contains("\"active\":true"), "{status}");
    assert!(status.contains("active"), "{status}");

    let stopped = http_request(addr, "POST", "/service/systemd/stop", &body, &[]);
    assert!(stopped.contains("HTTP/1.1 200 OK"), "{stopped}");
    assert!(stopped.contains("\"stopped\":true"), "{stopped}");

    let failing_body = serde_json::json!({
        "unit_name": "agent-os-test.service",
        "systemctl_path": failing_systemctl,
    })
    .to_string();
    let failed_stop = http_request(addr, "POST", "/service/systemd/stop", &failing_body, &[]);
    assert!(
        failed_stop.contains("HTTP/1.1 409 Conflict"),
        "{failed_stop}"
    );
    assert!(
        failed_stop.contains("\"error\":\"service command failed\""),
        "{failed_stop}"
    );
    assert!(failed_stop.contains("\"systemctl\":"), "{failed_stop}");
    assert!(failed_stop.contains("\"success\":false"), "{failed_stop}");
    assert!(failed_stop.contains("\"status\":42"), "{failed_stop}");
    assert!(failed_stop.contains("stop failed"), "{failed_stop}");

    let invalid = http_request(
        addr,
        "POST",
        "/service/systemd/status",
        r#"{"unit_name":"agent-os-test.service","systemctl_path":" "}"#,
        &[],
    );
    assert!(invalid.contains("HTTP/1.1 400 Bad Request"), "{invalid}");
    assert!(
        invalid.contains("systemctl_path must not be empty"),
        "{invalid}"
    );

    wait_for_api_success(&mut child);

    let calls = std::fs::read_to_string(log).expect("systemctl log");
    assert!(calls.contains("--user start agent-os-test.service"));
    assert!(calls.contains("--user is-active agent-os-test.service"));
    assert!(calls.contains("--user stop agent-os-test.service"));
}

#[test]
fn api_doctor_reports_state_and_config_preflight() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("missing-state");
    let config = dir.path().join("agent-os.toml");
    let missing_config = dir.path().join("missing-agent-os.toml");
    let state_arg = state.to_str().expect("state");
    let config_arg = config.to_str().expect("config");
    let missing_config_arg = missing_config.to_str().expect("missing config");

    std::fs::write(&config, "name = '   '\n").expect("write invalid config");

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "--config",
            config_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "1",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let doctor = http_get(addr, "/doctor", &[]);
    assert!(doctor.contains("HTTP/1.1 200 OK"), "{doctor}");
    assert!(
        doctor.contains(&format!(
            "\"agent_os_version\":\"{}\"",
            env!("CARGO_PKG_VERSION")
        )),
        "{doctor}"
    );
    assert!(doctor.contains("\"state_path\":"), "{doctor}");
    assert!(
        doctor.contains(&format!("\"platform\":\"{}\"", std::env::consts::OS)),
        "{doctor}"
    );
    assert!(doctor.contains("\"service_manager\":"), "{doctor}");
    assert!(doctor.contains("\"service_recommendation\":"), "{doctor}");
    assert!(
        doctor.contains("\"shell_execution_supported\":"),
        "{doctor}"
    );
    assert!(doctor.contains("\"shell_execution_note\":"), "{doctor}");
    assert!(doctor.contains("\"state_exists\":false"), "{doctor}");
    assert!(doctor.contains("\"state_loads\":false"), "{doctor}");
    assert!(doctor.contains("\"state_valid\":null"), "{doctor}");
    assert!(doctor.contains("\"config_exists\":true"), "{doctor}");
    assert!(doctor.contains("\"config_loads\":true"), "{doctor}");
    assert!(doctor.contains("\"config_valid\":false"), "{doctor}");
    assert!(doctor.contains("\"next_steps\":["), "{doctor}");
    assert!(doctor.contains("agent-os --state"), "{doctor}");
    assert!(doctor.contains("config validate"), "{doctor}");
    assert!(doctor.contains("OS name must not be empty"), "{doctor}");

    wait_for_api_success(&mut child);

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "--config",
            missing_config_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "1",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let doctor = http_get(addr, "/doctor", &[]);
    assert!(doctor.contains("HTTP/1.1 200 OK"), "{doctor}");
    assert!(doctor.contains("\"config_exists\":false"), "{doctor}");
    assert!(doctor.contains("config init --profile safe"), "{doctor}");
    assert!(!doctor.contains("config init --profile dev"), "{doctor}");

    wait_for_api_success(&mut child);
}

#[test]
fn api_can_initialize_state_from_config() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let config = dir.path().join("agent-os.toml");
    let state_arg = state.to_str().expect("state");
    let config_arg = config.to_str().expect("config");

    std::fs::write(
        &config,
        r#"
name = "API Init OS"

[[agents]]
name = "api-builder"
kind = "builder"
capabilities = ["rust"]
parallel = 1
"#,
    )
    .expect("write config");

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "--config",
            config_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "4",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let invalid = http_request(addr, "POST", "/init", r#"{"name":"   "}"#, &[]);
    assert!(invalid.contains("HTTP/1.1 400 Bad Request"), "{invalid}");
    assert!(invalid.contains("OS name must not be empty"), "{invalid}");

    let initialized = http_request(addr, "POST", "/init", "", &[]);
    assert!(
        initialized.contains("HTTP/1.1 201 Created"),
        "{initialized}"
    );
    assert!(
        initialized.contains("\"name\":\"API Init OS\""),
        "{initialized}"
    );
    assert!(initialized.contains("api-builder"), "{initialized}");

    let conflict = http_request(addr, "POST", "/init", "{}", &[]);
    assert!(conflict.contains("HTTP/1.1 409 Conflict"), "{conflict}");
    assert!(conflict.contains("state conflict"), "{conflict}");

    let forced = http_request(
        addr,
        "POST",
        "/init",
        r#"{"name":"Forced API OS","force":true}"#,
        &[],
    );
    assert!(forced.contains("HTTP/1.1 201 Created"), "{forced}");
    assert!(forced.contains("\"name\":\"Forced API OS\""), "{forced}");

    wait_for_api_success(&mut child);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"name\": \"Forced API OS\""));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "agent", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("api-builder"));
}

#[test]
fn api_schema_command_prints_contract() {
    let output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["api", "schema"])
        .output()
        .expect("api schema");
    assert!(output.status.success(), "api schema failed");
    let schema: Value = serde_json::from_slice(&output.stdout).expect("schema json");
    assert_eq!(schema, agent_os::openapi_schema());
    assert_schema_refs_resolve(&schema);
    assert_optional_bearer_security_declared(&schema);
    assert_component_schemas_are_reachable(&schema);
    assert_operations_and_responses_have_descriptions(&schema);
    assert_parameters_are_documented(&schema);
    assert_path_parameters_declared(&schema);
    assert_query_parameters_match_runtime_filters(&schema);
    assert_request_bodies_match_http_methods(&schema);
    assert_success_responses_have_json_schemas(&schema);
    assert_error_responses_have_json_schemas(&schema);
    assert_mutation_operations_declare_unsupported_media_type(&schema);
    assert_create_post_success_statuses(&schema);
    assert_content_uses_only_application_json(&schema);
    assert_json_content_entries_have_schemas(&schema);
    assert_request_body_requirements(&schema);
    assert_array_schemas_declare_items(&schema);
    assert_component_object_schemas_are_closed(&schema);
    assert_non_request_object_schemas_require_declared_properties(&schema);
    assert_operation_ids_present_and_unique(&schema);
    assert_operation_tags_present_and_declared(&schema);
    assert_standard_response_headers_declared(&schema);
    assert_string_inputs_are_constrained(&schema);
    assert_nullable_string_variants_are_constrained(&schema);

    assert_eq!(schema["openapi"], "3.1.0");
    assert_eq!(schema["tags"][0]["name"], "system");
    assert_eq!(
        schema["security"],
        serde_json::json!([{}, {"bearerAuth": []}])
    );
    assert_eq!(
        schema["paths"]["/status"]["get"]["responses"]["401"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/status"]["get"]["responses"]["401"]["headers"]["WWW-Authenticate"]["schema"]
            ["example"],
        "Bearer"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["post"]["responses"]["401"]["description"],
        "Authentication required"
    );
    assert_eq!(
        schema["paths"]["/status"]["get"]["operationId"],
        "getStatus"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["post"]["operationId"],
        "postTasks"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/logs"]["options"]["operationId"],
        "optionsRunsIdLogs"
    );
    assert_eq!(
        schema["paths"]["/status"]["get"]["tags"],
        serde_json::json!(["system"])
    );
    assert_eq!(
        schema["paths"]["/tasks"]["post"]["tags"],
        serde_json::json!(["tasks"])
    );
    assert_eq!(
        schema["paths"]["/workflows"]["post"]["tags"],
        serde_json::json!(["workflows"])
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}/status"]["get"]["tags"],
        serde_json::json!(["workflows"])
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/logs"]["options"]["tags"],
        serde_json::json!(["runs"])
    );
    assert_eq!(
        schema["paths"]["/openapi.json"]["get"]["tags"],
        serde_json::json!(["schema"])
    );
    assert_eq!(
        schema["paths"]["/status"]["options"]["responses"]["204"]["description"],
        "No content"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["options"]["summary"],
        "CORS preflight"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["options"]["security"],
        serde_json::json!([])
    );
    assert!(schema["paths"]["/tasks"]["options"]["parameters"].is_null());
    assert!(schema["paths"]["/tasks"]["options"]["responses"]["401"].is_null());
    assert_eq!(
        schema["paths"]["/status"]["get"]["responses"]["405"]["description"],
        "Method not allowed"
    );
    assert_eq!(
        schema["paths"]["/status"]["get"]["responses"]["200"]["headers"]["Allow"]["schema"]["example"],
        "GET, POST, DELETE, OPTIONS"
    );
    assert_eq!(
        schema["paths"]["/status"]["get"]["responses"]["200"]["headers"]["Cache-Control"]["schema"]
            ["example"],
        "no-store"
    );
    assert_eq!(
        schema["paths"]["/status"]["get"]["responses"]["200"]["headers"]["X-Content-Type-Options"]
            ["schema"]["example"],
        "nosniff"
    );
    assert_eq!(
        schema["paths"]["/status"]["get"]["responses"]["200"]["headers"]["X-Trace-Id"]["schema"]["pattern"],
        r"^[0-9a-fA-F-]{36}$"
    );
    assert_eq!(
        schema["paths"]["/status"]["options"]["responses"]["204"]["headers"]["Access-Control-Allow-Headers"]
            ["schema"]["example"],
        "authorization, content-type"
    );
    assert_eq!(
        schema["components"]["schemas"]["ErrorResponse"]["properties"]["method"]["type"],
        "string"
    );
    assert_eq!(
        schema["paths"]["/health"]["get"]["responses"]["431"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/status"]["get"]["responses"]["500"]["description"],
        "Server error"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["post"]["responses"]["413"]["description"],
        "Request body too large"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["post"]["responses"]["415"]["description"],
        "Unsupported media type"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/cancel"]["post"]["responses"]["500"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/openapi.json"]["get"]["responses"]["401"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/openapi.json"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/OpenApiDocument"
    );
    assert_eq!(
        schema["components"]["schemas"]["OpenApiDocument"]["required"],
        serde_json::json!(["openapi", "info", "paths", "components"])
    );
    assert_eq!(
        schema["components"]["schemas"]["OpenApiDocument"]["properties"]["paths"]["type"],
        "object"
    );
    assert_eq!(
        schema["paths"]["/health"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/HealthResponse"
    );
    assert_eq!(
        schema["paths"]["/doctor"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/DoctorResponse"
    );
    assert_eq!(
        schema["paths"]["/doctor"]["get"]["tags"],
        serde_json::json!(["system"])
    );
    assert_eq!(
        schema["paths"]["/init"]["post"]["requestBody"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/InitRequest"
    );
    assert_eq!(
        schema["paths"]["/init"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/init"]["post"]["responses"]["201"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/InitResponse"
    );
    assert_eq!(
        schema["paths"]["/config"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ConfigResponse"
    );
    assert_eq!(
        schema["paths"]["/config"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/WriteConfigRequest"
    );
    assert_eq!(
        schema["paths"]["/config"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/config"]["post"]["responses"]["201"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/WriteConfigResponse"
    );
    assert_eq!(
        schema["paths"]["/config/validate"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ConfigValidationResponse"
    );
    assert_eq!(
        schema["paths"]["/config"]["get"]["tags"],
        serde_json::json!(["config"])
    );
    assert_eq!(
        schema["components"]["schemas"]["HealthResponse"]["properties"]["state_issue_count"]["minimum"],
        0
    );
    assert_eq!(
        schema["components"]["schemas"]["HealthResponse"]["properties"]["agent_os_version"]["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["DoctorResponse"]["properties"]["state_exists"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["DoctorResponse"]["properties"]["agent_os_version"]["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["DoctorResponse"]["properties"]["platform"]["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["DoctorResponse"]["properties"]["service_manager"]["enum"],
        serde_json::json!(["launchd", "systemd", "manual"])
    );
    assert_eq!(
        schema["components"]["schemas"]["DoctorResponse"]["properties"]["service_recommendation"]["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["DoctorResponse"]["properties"]["shell_execution_supported"]
            ["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["DoctorResponse"]["properties"]["shell_execution_note"]["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["DoctorResponse"]["properties"]["config_valid"]["type"],
        serde_json::json!(["boolean", "null"])
    );
    assert_eq!(
        schema["components"]["schemas"]["DoctorResponse"]["properties"]["next_steps"]["items"]["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["InitRequest"]["properties"]["force"]["default"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["InitRequest"]["properties"]["profile"]["default"],
        "safe"
    );
    assert_eq!(
        schema["components"]["schemas"]["InitRequest"]["properties"]["profile"]["enum"],
        serde_json::json!(["safe", "dev", "autonomous", "ci"])
    );
    assert_eq!(
        schema["components"]["schemas"]["InitResponse"]["properties"]["os"]["$ref"],
        "#/components/schemas/OperatingSystem"
    );
    assert_eq!(
        schema["paths"]["/health"]["get"]["responses"]["503"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/HealthResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["HealthResponse"]["properties"]["state_loads"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["HealthResponse"]["properties"]["state_error"]["type"],
        serde_json::json!(["string", "null"])
    );
    assert_eq!(
        schema["components"]["schemas"]["HealthResponse"]["properties"]["config_loads"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["HealthResponse"]["properties"]["config_valid"]["type"],
        serde_json::json!(["boolean", "null"])
    );
    assert_eq!(
        schema["components"]["schemas"]["HealthResponse"]["properties"]["config_issues"]["items"]["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["ConfigResponse"]["properties"]["config"]["$ref"],
        "#/components/schemas/AppConfig"
    );
    assert_eq!(
        schema["components"]["schemas"]["WriteConfigRequest"]["properties"]["force"]["default"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["WriteConfigRequest"]["properties"]["profile"]["enum"],
        serde_json::json!(["safe", "dev", "autonomous", "ci"])
    );
    assert_eq!(
        schema["components"]["schemas"]["WriteConfigRequest"]["properties"]["profile"]["default"],
        "safe"
    );
    assert_eq!(
        schema["components"]["schemas"]["WriteConfigResponse"]["properties"]["profile"]["enum"],
        serde_json::json!(["safe", "dev", "autonomous", "ci"])
    );
    assert_eq!(
        schema["components"]["schemas"]["WriteConfigResponse"]["properties"]["config"]["$ref"],
        "#/components/schemas/AppConfig"
    );
    assert_eq!(
        schema["components"]["schemas"]["ConfigValidationResponse"]["properties"]["config_valid"]["type"],
        serde_json::json!(["boolean", "null"])
    );
    assert_eq!(
        schema["components"]["schemas"]["AppConfig"]["properties"]["agents"]["items"]["$ref"],
        "#/components/schemas/AgentConfig"
    );
    assert_eq!(
        schema["components"]["schemas"]["AppConfig"]["properties"]["provider"]["$ref"],
        "#/components/schemas/ProviderSettings"
    );
    assert_eq!(
        schema["components"]["schemas"]["ProviderSettings"]["properties"]["max_retries"]["maximum"],
        agent_os::MAX_PROVIDER_RETRIES
    );
    assert_eq!(
        schema["components"]["schemas"]["AppConfig"]["properties"]["name"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["AppConfig"]["properties"]["policy"]["$ref"],
        "#/components/schemas/Policy"
    );
    assert_eq!(
        schema["components"]["schemas"]["AppConfig"]["properties"]["memory_policy"]["$ref"],
        "#/components/schemas/MemoryPolicy"
    );
    assert_eq!(
        schema["components"]["schemas"]["OperatingSystem"]["properties"]["policy"]["$ref"],
        "#/components/schemas/Policy"
    );
    assert_eq!(
        schema["components"]["schemas"]["OperatingSystem"]["properties"]["memory_policy"]["$ref"],
        "#/components/schemas/MemoryPolicy"
    );
    assert_eq!(
        schema["components"]["schemas"]["MemoryPolicy"]["properties"]["max_provider_memories"]["minimum"],
        1
    );
    assert_eq!(
        schema["components"]["schemas"]["MemoryPolicy"]["properties"]["max_age_days"]["type"],
        serde_json::json!(["integer", "null"])
    );
    assert_eq!(
        schema["components"]["schemas"]["Policy"]["additionalProperties"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["Policy"]["properties"]["allow_shell"]["default"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["Policy"]["properties"]["max_output_bytes"]["minimum"],
        1
    );
    assert_eq!(
        schema["components"]["schemas"]["Policy"]["properties"]["max_output_bytes"]["default"],
        131072
    );
    assert_eq!(
        schema["components"]["schemas"]["Policy"]["properties"]["command_timeout_seconds"]["minimum"],
        1
    );
    assert_eq!(
        schema["components"]["schemas"]["Policy"]["properties"]["command_timeout_seconds"]["default"],
        300
    );
    assert_eq!(
        schema["components"]["schemas"]["Policy"]["properties"]["allowed_env_vars"]["items"]["pattern"],
        r"^[A-Za-z_][A-Za-z0-9_]*$"
    );
    assert_eq!(
        schema["components"]["schemas"]["Policy"]["properties"]["allowed_commands"]["items"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["Policy"]["properties"]["allowed_workspaces"]["items"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["Policy"]["properties"]["denied_patterns"]["items"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["Policy"]["properties"]["redacted_env_patterns"]["items"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["Policy"]["properties"]["network"]["properties"]["mode"]["default"],
        "providers-only"
    );
    assert_eq!(
        schema["components"]["schemas"]["Policy"]["properties"]["approval"]["properties"]["require_for_risky_actions"]
            ["default"],
        true
    );
    assert_eq!(
        schema["components"]["schemas"]["Policy"]["properties"]["autonomy"]["default"],
        "execute-with-approval"
    );
    assert_eq!(
        schema["components"]["schemas"]["ProviderSettings"]["properties"]["kind"]["enum"],
        serde_json::json!(agent_os::ProviderKind::INPUT_VALUES)
    );
    assert_eq!(
        schema["components"]["schemas"]["ProviderSettings"]["properties"]["model"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["ProviderSettings"]["properties"]["endpoint"]["pattern"],
        r"^https?://.*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["ProviderSettings"]["properties"]["api_key_env"]["pattern"],
        r"^[A-Za-z_][A-Za-z0-9_]*$"
    );
    assert_eq!(
        schema["components"]["schemas"]["ProviderSettings"]["properties"]["plugin_command"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["ProviderSettings"]["properties"]["plugin_args"]["items"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["ProviderSettings"]["properties"]["plugin_env"]["propertyNames"]
            ["pattern"],
        r"^[A-Za-z_][A-Za-z0-9_]*$"
    );
    assert_eq!(
        schema["components"]["schemas"]["AgentConfig"]["properties"]["name"]["pattern"],
        r".*[A-Za-z0-9-].*"
    );
    assert_eq!(
        schema["components"]["schemas"]["AgentConfig"]["properties"]["kind"]["default"],
        "builder"
    );
    assert_eq!(
        schema["components"]["schemas"]["AgentConfig"]["properties"]["model"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["AgentConfig"]["properties"]["capabilities"]["items"]["minLength"],
        1
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolConfig"]["properties"]["name"]["pattern"],
        r".*[A-Za-z0-9-].*"
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolConfig"]["properties"]["kind"]["enum"],
        serde_json::json!(agent_os::ToolKind::INPUT_VALUES)
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolConfig"]["properties"]["command_template"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolConfig"]["properties"]["default_cwd"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolConfig"]["properties"]["required_capabilities"]["items"]
            ["minLength"],
        1
    );
    assert_eq!(
        schema["paths"]["/metrics"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/MetricsResponse"
    );
    assert_eq!(
        schema["paths"]["/metrics"]["get"]["responses"]["503"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/MetricsResponse"
    );
    assert_eq!(
        schema["paths"]["/metrics/prometheus"]["get"]["responses"]["200"]["content"]["text/plain; version=0.0.4"]
            ["schema"]["type"],
        "string"
    );
    assert_eq!(
        schema["paths"]["/metrics"]["get"]["tags"],
        serde_json::json!(["system"])
    );
    assert_eq!(
        schema["components"]["schemas"]["MetricsResponse"]["properties"]["agents_total"]["minimum"],
        0
    );
    assert_eq!(
        schema["components"]["schemas"]["MetricsResponse"]["properties"]["state_valid"]["type"],
        serde_json::json!(["boolean", "null"])
    );
    assert_eq!(
        schema["components"]["schemas"]["MetricsResponse"]["properties"]["agent_os_version"]["type"],
        "string"
    );
    assert_eq!(
        schema["paths"]["/status"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/StatusResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["StatusResponse"]["properties"]["tasks_pending"]["minimum"],
        0
    );
    assert_eq!(
        schema["components"]["schemas"]["StatusResponse"]["properties"]["workflows"]["minimum"],
        0
    );
    assert_eq!(
        schema["components"]["schemas"]["MetricsResponse"]["properties"]["workflows_total"]["minimum"],
        0
    );
    assert_eq!(
        schema["components"]["schemas"]["MetricsResponse"]["properties"]["oldest_active_run_age_ms"]
            ["minimum"],
        0
    );
    assert_eq!(
        schema["components"]["schemas"]["StatusResponse"]["properties"]["version"]["type"],
        "integer"
    );
    assert_eq!(
        schema["components"]["schemas"]["StatusResponse"]["properties"]["agent_os_version"]["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["StatusResponse"]["properties"]["daemon_status"]["type"],
        serde_json::json!(["string", "null"])
    );
    assert_eq!(
        schema["paths"]["/daemon"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/DaemonResponse"
    );
    assert_eq!(
        schema["paths"]["/daemon/stop"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/DaemonStopResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["DaemonState"]["properties"]["status"]["enum"],
        serde_json::json!(["running", "stopped"])
    );
    assert_eq!(
        schema["components"]["schemas"]["DaemonStopResponse"]["required"],
        serde_json::json!(["stop_requested", "daemon"])
    );
    assert_eq!(
        schema["paths"]["/service/launchd"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/LaunchdServiceRequest"
    );
    assert_eq!(
        schema["paths"]["/service/launchd"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/service/launchd"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/LaunchdServiceResponse"
    );
    assert_eq!(
        schema["paths"]["/service/launchd"]["post"]["tags"],
        serde_json::json!(["service"])
    );
    assert_eq!(
        schema["components"]["schemas"]["LaunchdServiceRequest"]["properties"]["interval_ms"]["default"],
        1000
    );
    assert_eq!(
        schema["components"]["schemas"]["LaunchdServiceRequest"]["properties"]["label"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["LaunchdServiceRequest"]["properties"]["bin_path"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["LaunchdServiceRequest"]["properties"]["plist_path"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["LaunchdServiceResponse"]["properties"]["service"]["$ref"],
        "#/components/schemas/LaunchdService"
    );
    assert_eq!(
        schema["paths"]["/service/launchd/install"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/LaunchdServiceRequest"
    );
    assert_eq!(
        schema["paths"]["/service/launchd/install"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/service/launchd/install"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/InstallLaunchdServiceResponse"
    );
    assert_eq!(
        schema["paths"]["/service/launchd/uninstall"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/UninstallLaunchdServiceRequest"
    );
    assert_eq!(
        schema["paths"]["/service/launchd/uninstall"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/service/launchd/uninstall"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/UninstallLaunchdServiceResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["InstallLaunchdServiceResponse"]["properties"]["service"]["$ref"],
        "#/components/schemas/LaunchdService"
    );
    assert_eq!(
        schema["components"]["schemas"]["UninstallLaunchdServiceResponse"]["properties"]["removed"]
            ["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["UninstallLaunchdServiceRequest"]["properties"]["label"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["UninstallLaunchdServiceRequest"]["properties"]["plist_path"]
            ["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["paths"]["/service/launchd/start"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/LaunchdServiceControlRequest"
    );
    assert_eq!(
        schema["paths"]["/service/launchd/start"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/service/launchd/start"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/LaunchdServiceStartResponse"
    );
    assert_eq!(
        schema["paths"]["/service/launchd/stop"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/LaunchdServiceStopResponse"
    );
    assert_eq!(
        schema["paths"]["/service/launchd/stop"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/service/launchd/status"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/LaunchdServiceStatusResponse"
    );
    assert_eq!(
        schema["paths"]["/service/launchd/status"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["LaunchdServiceControlRequest"]["properties"]["launchctl_path"]
            ["default"],
        "launchctl"
    );
    assert_eq!(
        schema["components"]["schemas"]["LaunchdServiceControlRequest"]["properties"]["label"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["LaunchdServiceControlRequest"]["properties"]["plist_path"]
            ["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["LaunchdServiceControlRequest"]["properties"]["domain"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["LaunchdServiceControlRequest"]["properties"]["launchctl_path"]
            ["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["LaunchdServiceStartResponse"]["properties"]["launchctl"]["$ref"],
        "#/components/schemas/LaunchctlCommandOutput"
    );
    assert_eq!(
        schema["components"]["schemas"]["LaunchctlCommandErrorResponse"]["properties"]["launchctl"]
            ["$ref"],
        "#/components/schemas/LaunchctlCommandOutput"
    );
    assert_eq!(
        schema["paths"]["/service/launchd/start"]["post"]["responses"]["409"]["content"]["application/json"]
            ["schema"]["oneOf"][0]["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/service/launchd/start"]["post"]["responses"]["409"]["content"]["application/json"]
            ["schema"]["oneOf"][1]["$ref"],
        "#/components/schemas/LaunchctlCommandErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/service/launchd/stop"]["post"]["responses"]["409"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/LaunchctlCommandErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/service/systemd"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/SystemdServiceRequest"
    );
    assert_eq!(
        schema["paths"]["/service/systemd"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/service/systemd"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/SystemdServiceResponse"
    );
    assert_eq!(
        schema["paths"]["/service/systemd"]["post"]["tags"],
        serde_json::json!(["service"])
    );
    assert_eq!(
        schema["components"]["schemas"]["SystemdServiceRequest"]["properties"]["interval_ms"]["default"],
        1000
    );
    assert_eq!(
        schema["components"]["schemas"]["SystemdServiceRequest"]["properties"]["unit_name"]["default"],
        "agent-os.service"
    );
    assert_eq!(
        schema["components"]["schemas"]["SystemdServiceRequest"]["properties"]["unit_name"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["SystemdServiceRequest"]["properties"]["bin_path"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["SystemdServiceRequest"]["properties"]["unit_path"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["SystemdServiceResponse"]["properties"]["service"]["$ref"],
        "#/components/schemas/SystemdService"
    );
    assert_eq!(
        schema["paths"]["/service/systemd/install"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/SystemdServiceRequest"
    );
    assert_eq!(
        schema["paths"]["/service/systemd/install"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/service/systemd/install"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/InstallSystemdServiceResponse"
    );
    assert_eq!(
        schema["paths"]["/service/systemd/uninstall"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/UninstallSystemdServiceRequest"
    );
    assert_eq!(
        schema["paths"]["/service/systemd/uninstall"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/service/systemd/uninstall"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/UninstallSystemdServiceResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["InstallSystemdServiceResponse"]["properties"]["service"]["$ref"],
        "#/components/schemas/SystemdService"
    );
    assert_eq!(
        schema["components"]["schemas"]["UninstallSystemdServiceResponse"]["properties"]["removed"]
            ["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["UninstallSystemdServiceRequest"]["properties"]["unit_name"]
            ["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["UninstallSystemdServiceRequest"]["properties"]["unit_path"]
            ["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["paths"]["/service/systemd/start"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/SystemdServiceControlRequest"
    );
    assert_eq!(
        schema["paths"]["/service/systemd/start"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/service/systemd/start"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/SystemdServiceStartResponse"
    );
    assert_eq!(
        schema["paths"]["/service/systemd/stop"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/SystemdServiceStopResponse"
    );
    assert_eq!(
        schema["paths"]["/service/systemd/stop"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/service/systemd/status"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/SystemdServiceStatusResponse"
    );
    assert_eq!(
        schema["paths"]["/service/systemd/status"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["SystemdServiceControlRequest"]["properties"]["systemctl_path"]
            ["default"],
        "systemctl"
    );
    assert_eq!(
        schema["components"]["schemas"]["SystemdServiceControlRequest"]["properties"]["unit_name"]
            ["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["SystemdServiceControlRequest"]["properties"]["systemctl_path"]
            ["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["SystemdServiceStartResponse"]["properties"]["systemctl"]["$ref"],
        "#/components/schemas/SystemctlCommandOutput"
    );
    assert_eq!(
        schema["components"]["schemas"]["SystemctlCommandErrorResponse"]["properties"]["systemctl"]
            ["$ref"],
        "#/components/schemas/SystemctlCommandOutput"
    );
    assert_eq!(
        schema["paths"]["/service/systemd/start"]["post"]["responses"]["409"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/SystemctlCommandErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/service/systemd/stop"]["post"]["responses"]["409"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/SystemctlCommandErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/state/validate"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ValidationReport"
    );
    assert_eq!(
        schema["paths"]["/state/export"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/OperatingSystem"
    );
    assert_eq!(
        schema["paths"]["/state/export"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ExportStateRequest"
    );
    assert_eq!(
        schema["paths"]["/state/export"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ExportStateResponse"
    );
    assert_eq!(
        schema["paths"]["/state/import"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ImportStateRequest"
    );
    assert_eq!(
        schema["paths"]["/state/import"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ImportStateResponse"
    );
    assert_eq!(
        schema["paths"]["/state/migrate"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/MigrateStateRequest"
    );
    assert_eq!(
        schema["paths"]["/state/migrate"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/MigrateStateResponse"
    );
    assert_eq!(
        schema["paths"]["/state/sqlite"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/state/sqlite"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/StateSqliteRequest"
    );
    assert_eq!(
        schema["paths"]["/state/sqlite"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/StateSqliteResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["OperatingSystem"]["properties"]["tasks"]["additionalProperties"]
            ["$ref"],
        "#/components/schemas/Task"
    );
    assert_eq!(
        schema["components"]["schemas"]["ExportStateRequest"]["required"],
        serde_json::json!(["output"])
    );
    assert_eq!(
        schema["components"]["schemas"]["BackupStateRequest"]["properties"]["output"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["ExportStateRequest"]["properties"]["output"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["ExportStateRequest"]["properties"]["dry_run"]["default"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["ExportStateResponse"]["required"],
        serde_json::json!(["dry_run", "exported"])
    );
    assert_eq!(
        schema["components"]["schemas"]["ExportStateResponse"]["properties"]["dry_run"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["ImportStateRequest"]["required"],
        serde_json::json!(["path"])
    );
    assert_eq!(
        schema["components"]["schemas"]["ImportStateRequest"]["properties"]["path"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["ImportStateRequest"]["properties"]["dry_run"]["default"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["ImportStateResponse"]["required"],
        serde_json::json!(["dry_run", "imported", "state_path", "validation"])
    );
    assert_eq!(
        schema["components"]["schemas"]["ImportStateResponse"]["properties"]["dry_run"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["ImportStateResponse"]["properties"]["validation"]["$ref"],
        "#/components/schemas/ValidationReport"
    );
    assert_eq!(
        schema["components"]["schemas"]["MigrateStateRequest"]["properties"]["input"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["MigrateStateRequest"]["properties"]["output"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["MigrateStateRequest"]["properties"]["dry_run"]["default"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["MigrateStateResponse"]["required"],
        serde_json::json!([
            "dry_run",
            "input",
            "output",
            "output_preexisting",
            "migration",
            "validation"
        ])
    );
    assert_eq!(
        schema["components"]["schemas"]["MigrateStateResponse"]["properties"]["dry_run"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["MigrateStateResponse"]["properties"]["output_preexisting"]
            ["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["MigrateStateResponse"]["properties"]["migration"]["$ref"],
        "#/components/schemas/MigrationReport"
    );
    assert_eq!(
        schema["components"]["schemas"]["MigrationReport"]["required"],
        serde_json::json!([
            "from_version",
            "to_version",
            "changed",
            "steps",
            "downgrade_notes"
        ])
    );
    assert_eq!(
        schema["components"]["schemas"]["MigrationReport"]["properties"]["downgrade_notes"]["type"],
        "array"
    );
    assert_eq!(
        schema["components"]["schemas"]["StateSqliteRequest"]["properties"]["output"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["StateSqliteRequest"]["properties"]["init_only"]["default"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["StateSqliteRequest"]["properties"]["restore"]["default"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["StateSqliteResponse"]["required"],
        serde_json::json!([
            "path",
            "state_path",
            "initialized",
            "imported",
            "dry_run",
            "force",
            "import",
            "restore"
        ])
    );
    assert_eq!(
        schema["components"]["schemas"]["StateSqliteResponse"]["properties"]["import"]["anyOf"][0]
            ["$ref"],
        "#/components/schemas/SqliteImportReport"
    );
    assert_eq!(
        schema["components"]["schemas"]["StateSqliteResponse"]["properties"]["restore"]["anyOf"][0]
            ["$ref"],
        "#/components/schemas/SqliteRestoreReport"
    );
    assert_eq!(
        schema["components"]["schemas"]["SqliteRestoreReport"]["properties"]["validation"]["$ref"],
        "#/components/schemas/ValidationReport"
    );
    assert_eq!(
        schema["components"]["schemas"]["OperatingSystem"]["properties"]["daemon"]["anyOf"][0]["$ref"],
        "#/components/schemas/DaemonState"
    );
    assert_eq!(
        schema["components"]["schemas"]["ErrorResponse"]["required"],
        serde_json::json!(["error"])
    );
    assert_eq!(
        schema["components"]["schemas"]["ErrorResponse"]["properties"]["detail"]["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["ErrorResponse"]["properties"]["id"]["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["ErrorResponse"]["properties"]["parallel"]["minimum"],
        0
    );
    assert_eq!(
        schema["components"]["schemas"]["ErrorResponse"]["properties"]["tail_bytes"]["maximum"],
        0
    );
    assert!(schema["paths"]["/runs/{id}/replay"]["get"].is_object());
    assert!(schema["paths"]["/runs/{id}/debug"]["get"].is_object());
    assert!(schema["paths"]["/agents/{id}"]["get"].is_object());
    assert!(schema["paths"]["/tasks/{id}"]["get"].is_object());
    assert!(schema["paths"]["/workflows/{id}"]["get"].is_object());
    assert!(schema["paths"]["/workflows/{id}/status"]["get"].is_object());
    assert!(schema["paths"]["/workflows/{id}/dag"]["get"].is_object());
    assert!(schema["paths"]["/workflows/{id}/tasks"]["post"].is_object());
    assert!(schema["paths"]["/workflows/{id}/link"]["post"].is_object());
    assert!(schema["paths"]["/workflows/{id}/unlink"]["post"].is_object());
    assert!(schema["paths"]["/workflows/{id}/pause"]["post"].is_object());
    assert!(schema["paths"]["/workflows/{id}/resume"]["post"].is_object());
    assert!(schema["paths"]["/workflows/{id}/retry"]["post"].is_object());
    assert!(schema["paths"]["/approvals"]["get"].is_object());
    assert!(schema["paths"]["/approvals/{id}/approve"]["post"].is_object());
    assert!(schema["paths"]["/approvals/{id}/deny"]["post"].is_object());
    assert!(schema["paths"]["/workers"]["get"].is_object());
    assert!(schema["paths"]["/workers"]["post"].is_object());
    assert!(schema["paths"]["/workers/{id}"]["get"].is_object());
    assert!(schema["paths"]["/workers/{id}"]["delete"].is_object());
    assert!(schema["paths"]["/workers/{id}/heartbeat"]["post"].is_object());
    assert!(schema["paths"]["/workers/{id}/claim"]["post"].is_object());
    assert!(schema["paths"]["/evals"]["get"].is_object());
    assert!(schema["paths"]["/evals"]["post"].is_object());
    assert!(schema["paths"]["/evals/run"]["post"].is_object());
    assert!(schema["paths"]["/evals/{id}"]["get"].is_object());
    assert!(schema["paths"]["/git/status"]["get"].is_object());
    assert!(schema["paths"]["/git/review-task"]["post"].is_object());
    assert!(schema["paths"]["/registry"]["get"].is_object());
    assert!(schema["paths"]["/registry/profiles"]["get"].is_object());
    assert!(schema["paths"]["/registry/profiles/{id}"]["get"].is_object());
    assert!(schema["paths"]["/registry/templates"]["get"].is_object());
    assert!(schema["paths"]["/registry/templates/{id}"]["get"].is_object());
    assert!(schema["paths"]["/registry/templates/{id}/workflows"]["post"].is_object());
    assert!(schema["paths"]["/registry/marketplace-import"]["post"].is_object());
    assert!(schema["paths"]["/registry/mcp-servers"]["get"].is_object());
    assert!(schema["paths"]["/registry/mcp-servers"]["post"].is_object());
    assert!(schema["paths"]["/registry/mcp-servers/{id}"]["get"].is_object());
    assert!(schema["paths"]["/registry/mcp-servers/{id}"]["post"].is_object());
    assert!(schema["paths"]["/registry/mcp-servers/{id}"]["delete"].is_object());
    assert!(schema["paths"]["/secrets"]["get"].is_object());
    assert!(schema["paths"]["/secrets"]["post"].is_object());
    assert!(schema["paths"]["/secrets/check"]["get"].is_object());
    assert!(schema["paths"]["/secrets/{id}"]["get"].is_object());
    assert!(schema["paths"]["/secrets/{id}"]["delete"].is_object());
    assert!(schema["paths"]["/tools/{id}"]["get"].is_object());
    assert_eq!(
        schema["paths"]["/agents/{id}"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/Agent"
    );
    assert_eq!(
        schema["paths"]["/agents/{id}"]["get"]["responses"]["400"]["description"],
        "Invalid path parameter"
    );
    assert_eq!(
        schema["paths"]["/agents/{id}"]["get"]["responses"]["404"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/Task"
    );
    assert_eq!(
        schema["paths"]["/tools/{id}"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ToolDefinition"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/RunRecord"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}"]["get"]["responses"]["400"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}"]["get"]["responses"]["404"]["description"],
        "Record not found"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["post"]["requestBody"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/CreateTaskRequest"
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateTaskRequest"]["properties"]["priority"]["enum"],
        serde_json::json!(agent_os::Priority::INPUT_VALUES)
    );
    assert_eq!(
        schema["paths"]["/workflows"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/CreateWorkflowRequest"
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateWorkflowRequest"]["properties"]["priority"]["enum"],
        serde_json::json!(agent_os::Priority::INPUT_VALUES)
    );
    assert_eq!(
        schema["paths"]["/workflows"]["post"]["responses"]["201"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/WorkflowMutationResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateWorkflowRequest"]["properties"]["execute"]["default"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowMutationResponse"]["properties"]["runs"]["items"]
            ["$ref"],
        "#/components/schemas/RunRecord"
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}/status"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/WorkflowProgress"
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}/dag"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/WorkflowDag"
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowDag"]["required"],
        serde_json::json!([
            "id",
            "objective",
            "priority",
            "nodes",
            "edges",
            "external_dependencies",
            "progress"
        ])
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowDagEdge"]["required"],
        serde_json::json!(["from", "to", "from_task_id", "to_task_id"])
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}/tasks"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/WorkflowAddTaskRequest"
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}/tasks"]["post"]["responses"]["201"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/WorkflowAddTaskResponse"
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}/link"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/WorkflowEdgeRequest"
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}/unlink"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/WorkflowEdgeResponse"
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}/pause"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/WorkflowTransitionResponse"
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}/resume"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/WorkflowNoteRequest"
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}/retry"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowAddTaskRequest"]["required"],
        serde_json::json!(["stage", "title"])
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowAddTaskRequest"]["properties"]["stage"]["pattern"],
        r"^[^\u0000-\u001F/\\]*\S[^\u0000-\u001F/\\]*$"
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowTransitionResponse"]["properties"]["action"]["enum"],
        serde_json::json!(["paused", "resumed", "retried"])
    );
    assert_eq!(
        schema["paths"]["/approvals"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["items"]["$ref"],
        "#/components/schemas/ApprovalRequest"
    );
    assert_eq!(
        schema["paths"]["/approvals/{id}/approve"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ApprovalResolveRequest"
    );
    assert_eq!(
        schema["paths"]["/approvals/{id}/approve"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/approvals/{id}/deny"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ApprovalResolveResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["ApprovalResolveRequest"]["properties"]["by"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["ApprovalResolveResponse"]["properties"]["approval"]["$ref"],
        "#/components/schemas/ApprovalRequest"
    );
    assert_eq!(
        schema["paths"]["/workers"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/WorkerRegisterRequest"
    );
    assert_eq!(
        schema["paths"]["/workers"]["post"]["responses"]["201"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/WorkerMutationResponse"
    );
    assert_eq!(
        schema["paths"]["/workers/{id}/heartbeat"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/WorkerHeartbeatRequest"
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkerHeartbeatRequest"]["properties"]["lease_seconds"]["minimum"],
        1
    );
    assert_eq!(
        schema["paths"]["/workers/{id}/claim"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ClaimTaskRequest"
    );
    assert_eq!(
        schema["paths"]["/workers/{id}/claim"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/WorkerClaimResponse"
    );
    assert_eq!(
        schema["paths"]["/workers/{id}/report"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/WorkerReportRequest"
    );
    assert_eq!(
        schema["paths"]["/workers/{id}/report"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/WorkerReportResponse"
    );
    assert_eq!(
        schema["paths"]["/workers/{id}"]["delete"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/WorkerDeleteResponse"
    );
    assert_eq!(
        schema["paths"]["/evals"]["post"]["requestBody"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/EvalRecordRequest"
    );
    assert_eq!(
        schema["paths"]["/evals"]["post"]["responses"]["201"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/EvalMutationResponse"
    );
    assert_eq!(
        schema["paths"]["/evals/run"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/EvalRunRequest"
    );
    assert_eq!(
        schema["paths"]["/evals/run"]["post"]["responses"]["201"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/EvalRunResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["EvalRunResponse"]["properties"]["timed_out"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["EvalRunResponse"]["properties"]["output_schema_valid"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["EvalRunResponse"]["properties"]["output_schema_error"]["anyOf"]
            [1]["type"],
        "null"
    );
    assert_eq!(
        schema["components"]["schemas"]["EvalRecord"]["properties"]["run"]["anyOf"][0]["$ref"],
        "#/components/schemas/EvalRunDetails"
    );
    assert_eq!(
        schema["components"]["schemas"]["EvalRecord"]["properties"]["run"]["anyOf"][1]["type"],
        "null"
    );
    assert_eq!(
        schema["components"]["schemas"]["EvalRunDetails"]["required"],
        serde_json::json!([
            "command",
            "cwd",
            "status",
            "stdout",
            "stderr",
            "success_pattern",
            "success_pattern_matched",
            "output_schema",
            "output_schema_valid",
            "output_schema_error",
            "timed_out"
        ])
    );
    assert_eq!(
        schema["components"]["schemas"]["EvalRunDetails"]["properties"]["success_pattern"]["anyOf"]
            [1]["type"],
        "null"
    );
    assert_eq!(
        schema["components"]["schemas"]["EvalRunDetails"]["properties"]["output_schema"]["anyOf"]
            [0]["type"],
        "object"
    );
    assert_eq!(
        schema["components"]["schemas"]["EvalRunDetails"]["properties"]["output_schema"]["anyOf"]
            [1]["type"],
        "null"
    );
    assert_eq!(
        schema["paths"]["/evals/{id}"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/EvalRecord"
    );
    assert_eq!(
        schema["paths"]["/git/status"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/GitCommandResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["GitCommandResponse"]["required"],
        serde_json::json!(["command", "cwd", "status", "stdout", "stderr", "dry_run"])
    );
    assert_eq!(
        schema["paths"]["/git/status"]["get"]["parameters"][0]["name"],
        "cwd"
    );
    assert_eq!(
        schema["paths"]["/git/review-task"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/GitReviewTaskRequest"
    );
    assert_eq!(
        schema["paths"]["/git/review-task"]["post"]["responses"]["201"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/TaskMutationResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["GitReviewTaskRequest"]["properties"]["base"]["default"],
        "main"
    );
    assert_eq!(
        schema["paths"]["/registry"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/RegistryResponse"
    );
    assert_eq!(
        schema["paths"]["/registry/templates/{id}/workflows"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/TemplateWorkflowRequest"
    );
    assert_eq!(
        schema["paths"]["/registry/templates/{id}/workflows"]["post"]["responses"]["201"]["content"]
            ["application/json"]["schema"]["$ref"],
        "#/components/schemas/TemplateWorkflowResponse"
    );
    assert_eq!(
        schema["paths"]["/registry/marketplace-import"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/MarketplaceImportRequest"
    );
    assert_eq!(
        schema["paths"]["/registry/marketplace-import"]["post"]["responses"]["201"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/MarketplaceImportResponse"
    );
    assert_eq!(
        schema["paths"]["/registry/mcp-servers"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/RegisterMcpServerRequest"
    );
    assert_eq!(
        schema["paths"]["/registry/mcp-servers"]["post"]["responses"]["201"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/McpServerMutationResponse"
    );
    assert_eq!(
        schema["paths"]["/registry/mcp-servers/{id}"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/UpdateMcpServerRequest"
    );
    assert_eq!(
        schema["paths"]["/registry/mcp-servers/{id}"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/McpServerMutationResponse"
    );
    assert_eq!(
        schema["paths"]["/registry/mcp-servers/{id}"]["delete"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/McpServerDeleteResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["RegisterMcpServerRequest"]["required"],
        serde_json::json!(["id", "command"])
    );
    assert_eq!(
        schema["components"]["schemas"]["RegisterMcpServerRequest"]["properties"]["enabled"]["default"],
        true
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateMcpServerRequest"]["properties"]["env"]["propertyNames"]
            ["pattern"],
        r"^[A-Za-z_][A-Za-z0-9_]*$"
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowTemplate"]["required"],
        serde_json::json!(["id", "name", "description", "stages", "tasks", "edges"])
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowTemplate"]["properties"]["tasks"]["items"]["$ref"],
        "#/components/schemas/WorkflowTemplateTask"
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowTemplate"]["properties"]["edges"]["items"]["$ref"],
        "#/components/schemas/WorkflowTemplateEdge"
    );
    assert_eq!(
        schema["components"]["schemas"]["TemplateWorkflowResponse"]["properties"]["dag"]["$ref"],
        "#/components/schemas/WorkflowDag"
    );
    assert_eq!(
        schema["paths"]["/secrets"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/SecretsRegisterRequest"
    );
    assert_eq!(
        schema["paths"]["/secrets"]["post"]["responses"]["201"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/SecretsMutationResponse"
    );
    assert_eq!(
        schema["paths"]["/secrets/check"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/SecretCheckReport"
    );
    assert_eq!(
        schema["paths"]["/secrets/{id}"]["delete"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/SecretsDeleteResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkerRegisterRequest"]["required"],
        serde_json::json!(["id", "endpoint"])
    );
    assert_eq!(
        schema["components"]["schemas"]["EvalRecordRequest"]["required"],
        serde_json::json!(["target", "success"])
    );
    assert_eq!(
        schema["components"]["schemas"]["EvalRunRequest"]["required"],
        serde_json::json!(["target", "command"])
    );
    assert_eq!(
        schema["components"]["schemas"]["EvalRunRequest"]["properties"]["output_schema"]["type"],
        "object"
    );
    assert_eq!(
        schema["components"]["schemas"]["SecretsRegisterRequest"]["required"],
        serde_json::json!(["id", "kind"])
    );
    assert_eq!(
        schema["components"]["schemas"]["SecretsRegisterRequest"]["properties"]["kind"]["enum"],
        serde_json::json!([
            "environment",
            "env",
            "one-password",
            "1password",
            "1-password",
            "op",
            "os-keychain",
            "keychain",
            "macos-keychain",
            "env-vault",
            "envvault"
        ])
    );
    assert_eq!(
        schema["components"]["schemas"]["SecretsBackend"]["properties"]["kind"]["enum"],
        serde_json::json!(["environment", "one-password", "os-keychain", "env-vault"])
    );
    assert_eq!(
        schema["components"]["schemas"]["SecretCheckReport"]["required"],
        serde_json::json!(["total", "present", "missing", "invalid", "references"])
    );
    assert_eq!(
        schema["components"]["schemas"]["SecretCheckReference"]["required"],
        serde_json::json!([
            "task_id",
            "task_title",
            "tool_id",
            "arg",
            "env",
            "valid_env",
            "present"
        ])
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}/run"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/WorkflowRunResponse"
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}/run"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/RunWorkflowRequest"
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}/run"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}/cancel"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/WorkflowCancelResponse"
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}/cancel"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/FinishTaskRequest"
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}/cancel"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["RunWorkflowRequest"]["properties"]["all"]["default"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowRunResponse"]["properties"]["progress"]["$ref"],
        "#/components/schemas/WorkflowProgress"
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowCancelResponse"]["required"][1],
        "cancelled_tasks"
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowProgress"]["properties"]["stages"]["items"]["$ref"],
        "#/components/schemas/WorkflowStageProgress"
    );
    assert_eq!(
        schema["paths"]["/workflows"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["items"]["$ref"],
        "#/components/schemas/Workflow"
    );
    assert_eq!(
        schema["paths"]["/workflows"]["get"]["parameters"][0]["name"],
        "priority"
    );
    assert_eq!(
        schema["paths"]["/workflows"]["get"]["parameters"][0]["schema"]["enum"],
        serde_json::json!(agent_os::Priority::INPUT_VALUES)
    );
    assert_eq!(
        schema["paths"]["/workflows"]["get"]["parameters"][1]["name"],
        "task"
    );
    assert_eq!(
        schema["paths"]["/workflows"]["get"]["parameters"][1]["schema"]["pattern"],
        r".*[A-Za-z0-9-].*"
    );
    assert_eq!(
        schema["paths"]["/workflows"]["get"]["parameters"][2]["name"],
        "since"
    );
    assert_eq!(
        schema["paths"]["/workflows"]["get"]["parameters"][2]["schema"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["paths"]["/workflows"]["get"]["parameters"][3]["name"],
        "until"
    );
    assert_eq!(
        schema["paths"]["/workflows"]["get"]["parameters"][3]["schema"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["paths"]["/workflows"]["get"]["parameters"][4]["name"],
        "query"
    );
    assert_eq!(
        schema["paths"]["/workflows"]["get"]["parameters"][4]["description"],
        "Case-insensitive search over workflow ID, objective, stage names, and task IDs."
    );
    assert_eq!(
        schema["paths"]["/workflows"]["get"]["parameters"][4]["schema"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["paths"]["/workflows"]["get"]["parameters"][5]["name"],
        "limit"
    );
    assert_eq!(
        schema["paths"]["/workflows"]["get"]["parameters"][5]["schema"]["minimum"],
        1
    );
    assert_eq!(
        schema["paths"]["/workflows"]["get"]["responses"]["400"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/workflows/{id}"]["delete"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/WorkflowDeleteResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowDeleteResponse"]["required"],
        serde_json::json!(["id", "removed", "workflow"])
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowDeleteResponse"]["properties"]["removed"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["Workflow"]["properties"]["tasks"]["additionalProperties"]
            ["type"],
        "string"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/retry"]["post"]["responses"]["409"]["description"],
        "Conflict"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/retry"]["post"]["responses"]["409"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/complete"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/TaskMutationResponse"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/UpdateTaskRequest"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}"]["post"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/TaskMutationResponse"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}"]["post"]["responses"]["422"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateTaskRequest"]["properties"]["clear_command"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateTaskRequest"]["anyOf"][0]["required"],
        serde_json::json!(["title"])
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateTaskRequest"]["anyOf"][10]["properties"]["clear_tool"]
            ["const"],
        true
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateTaskRequest"]["allOf"][0]["not"]["required"],
        serde_json::json!(["clear_command", "command"])
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateTaskRequest"]["allOf"][4]["not"]["properties"]["clear_args"]
            ["const"],
        true
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateTaskRequest"]["properties"]["tool"]["pattern"],
        ".*[A-Za-z0-9-].*"
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateTaskRequest"]["properties"]["args"]["additionalProperties"]
            ["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateTaskRequest"]["properties"]["clear_tool"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateTaskRequest"]["properties"]["clear_required_capabilities"]
            ["type"],
        "boolean"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/plan"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/TaskMutationResponse"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/assign"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/TaskAssignmentResponse"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/assign"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/AssignTaskRequest"
    );
    assert_eq!(
        schema["components"]["schemas"]["TaskAssignmentResponse"]["properties"]["assignment"]["$ref"],
        "#/components/schemas/Assignment"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/priority"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/TaskMutationResponse"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/priority"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/TaskPriorityRequest"
    );
    assert_eq!(
        schema["components"]["schemas"]["TaskPriorityRequest"]["properties"]["priority"]["enum"],
        serde_json::json!(agent_os::Priority::INPUT_VALUES)
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/dependencies"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/TaskMutationResponse"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/dependencies"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/TaskDependenciesRequest"
    );
    assert_eq!(
        schema["components"]["schemas"]["TaskDependenciesRequest"]["required"],
        serde_json::json!(["dependencies"])
    );
    assert_eq!(
        schema["components"]["schemas"]["TaskDependenciesRequest"]["properties"]["dependencies"]["items"]
            ["pattern"],
        ".*[A-Za-z0-9-].*"
    );
    assert_eq!(
        schema["components"]["schemas"]["TaskMutationResponse"]["required"][0],
        "id"
    );
    assert_eq!(
        schema["components"]["schemas"]["TaskMutationResponse"]["properties"]["task"]["$ref"],
        "#/components/schemas/Task"
    );
    assert_eq!(
        schema["paths"]["/agents/{id}/claim"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/AgentClaimResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["AgentClaimResponse"]["required"],
        serde_json::json!(["claimed", "assignment", "task"])
    );
    assert_eq!(
        schema["components"]["schemas"]["AgentClaimResponse"]["properties"]["claimed"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["AgentClaimResponse"]["properties"]["task"]["anyOf"][1]["type"],
        "null"
    );
    assert_eq!(
        schema["components"]["schemas"]["AgentClaimResponse"]["properties"]["assignment"]["anyOf"]
            [0]["$ref"],
        "#/components/schemas/Assignment"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/retry"]["post"]["parameters"][0]["in"],
        "path"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/retry"]["post"]["parameters"][0]["required"],
        true
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/retry"]["post"]["parameters"][0]["schema"]["pattern"],
        ".*[A-Za-z0-9].*"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/logs"]["get"]["parameters"][0]["name"],
        "id"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/logs"]["options"]["parameters"][0]["name"],
        "id"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/logs"]["options"]["parameters"][0]["in"],
        "path"
    );
    assert!(schema["paths"]["/runs/{id}/logs"]["options"]["parameters"][1].is_null());
    assert_eq!(
        schema["paths"]["/runs/{id}/logs"]["get"]["parameters"][1]["name"],
        "tail_bytes"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/logs"]["get"]["parameters"][1]["schema"]["minimum"],
        1
    );
    assert!(
        schema["paths"]["/runs/{id}/logs"]["get"]["description"]
            .as_str()
            .expect("run logs description")
            .contains("Non-following API equivalent")
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/logs"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/RunLogsResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["RunLogsResponse"]["properties"]["truncated"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["RunLogsResponse"]["properties"]["tail_bytes"]["minimum"],
        1
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/logs"]["get"]["responses"]["400"]["description"],
        "Invalid query parameter"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/logs"]["get"]["responses"]["404"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/replay"]["get"]["parameters"][1]["name"],
        "tail_bytes"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/replay"]["get"]["parameters"][1]["schema"]["minimum"],
        1
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/replay"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/RunReplayResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["RunReplayResponse"]["properties"]["log_truncated"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["RunReplayResponse"]["properties"]["log_tail_bytes"]["minimum"],
        1
    );
    assert_eq!(
        schema["components"]["schemas"]["RunReplayResponse"]["properties"]["run"]["$ref"],
        "#/components/schemas/RunRecord"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/replay"]["get"]["responses"]["400"]["description"],
        "Invalid query parameter"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/replay"]["get"]["responses"]["404"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/debug"]["get"]["parameters"][1]["name"],
        "tail_bytes"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/debug"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/RunDebugResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["RunDebugResponse"]["properties"]["agent"]["anyOf"][0]["$ref"],
        "#/components/schemas/Agent"
    );
    assert_eq!(
        schema["components"]["schemas"]["RunDebugResponse"]["properties"]["artifact_status"]["items"]
            ["properties"]["exists"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["RunDebugResponse"]["properties"]["diagnostics"]["properties"]
            ["related_events"]["minimum"],
        0
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/artifacts"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/RunArtifactsResponse"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/artifacts/{artifact_id}"]["get"]["parameters"][1]["name"],
        "artifact_id"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/artifacts/{artifact_id}"]["get"]["parameters"][2]["name"],
        "tail_bytes"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/artifacts/{artifact_id}"]["get"]["responses"]["200"]["content"]
            ["application/json"]["schema"]["$ref"],
        "#/components/schemas/RunArtifactReadResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["RunArtifactsResponse"]["properties"]["artifacts"]["items"]
            ["properties"]["checksum"]["anyOf"][0]["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["RunArtifactReadResponse"]["properties"]["body"]["type"],
        serde_json::json!(["string", "null"])
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/cancel"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/RunCancelResponse"
    );
    assert_eq!(
        schema["paths"]["/runs/{id}/cancel"]["post"]["responses"]["409"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/RunCancelResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["RunCancelResponse"]["properties"]["cancel_requested"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["RunCancelResponse"]["properties"]["run"]["$ref"],
        "#/components/schemas/RunRecord"
    );
    assert_eq!(
        schema["paths"]["/tools/{id}"]["delete"]["responses"]["409"]["description"],
        "Conflict"
    );
    assert_eq!(
        schema["paths"]["/tools/{id}"]["delete"]["responses"]["409"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/tools/{id}"]["delete"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ToolDeleteResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolDeleteResponse"]["required"],
        serde_json::json!(["id", "removed", "tool"])
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolDeleteResponse"]["properties"]["removed"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolDeleteResponse"]["properties"]["tool"]["$ref"],
        "#/components/schemas/ToolDefinition"
    );
    assert_eq!(
        schema["paths"]["/tools/{id}"]["delete"]["parameters"][0]["schema"]["minLength"],
        1
    );
    assert_eq!(
        schema["paths"]["/agents"]["post"]["responses"]["409"]["description"],
        "Conflict"
    );
    assert_eq!(
        schema["paths"]["/agents"]["post"]["responses"]["409"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/agents"]["post"]["responses"]["201"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/AgentMutationResponse"
    );
    assert_eq!(
        schema["paths"]["/agents/{id}"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/UpdateAgentRequest"
    );
    assert_eq!(
        schema["paths"]["/agents/{id}"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/AgentMutationResponse"
    );
    assert_eq!(
        schema["paths"]["/agents/{id}/heartbeat"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/AgentMutationResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["AgentMutationResponse"]["required"],
        serde_json::json!(["id", "agent"])
    );
    assert_eq!(
        schema["components"]["schemas"]["AgentMutationResponse"]["properties"]["agent"]["$ref"],
        "#/components/schemas/Agent"
    );
    assert_eq!(
        schema["components"]["schemas"]["Agent"]["properties"]["status"]["enum"],
        serde_json::json!(agent_os::AgentStatus::VALUES)
    );
    assert_eq!(
        schema["components"]["schemas"]["Agent"]["properties"]["max_parallel_tasks"]["minimum"],
        1
    );
    assert_eq!(
        schema["paths"]["/agents/{id}"]["delete"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/AgentDeleteResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["AgentDeleteResponse"]["required"],
        serde_json::json!(["id", "removed", "agent"])
    );
    assert_eq!(
        schema["components"]["schemas"]["AgentDeleteResponse"]["properties"]["removed"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["parameters"][0]["name"],
        "status"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["type"],
        "array"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["responses"]["400"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["items"]["$ref"],
        "#/components/schemas/Agent"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["parameters"][0]["in"],
        "query"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["parameters"][0]["schema"]["enum"],
        serde_json::json!(agent_os::AgentStatus::INPUT_VALUES)
    );
    assert_eq!(
        schema["components"]["schemas"]["HeartbeatRequest"]["properties"]["status"]["enum"],
        serde_json::json!(agent_os::AgentStatus::INPUT_VALUES)
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["parameters"][1]["name"],
        "kind"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["parameters"][1]["schema"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["parameters"][2]["name"],
        "capability"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["parameters"][2]["schema"]["pattern"],
        r"^\s*[^,\s][^,]*(?:,\s*[^,\s][^,]*)*\s*$"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["parameters"][3]["name"],
        "since"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["parameters"][3]["schema"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["parameters"][4]["name"],
        "until"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["parameters"][4]["schema"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["parameters"][5]["name"],
        "query"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["parameters"][5]["schema"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["parameters"][6]["name"],
        "limit"
    );
    assert_eq!(
        schema["paths"]["/agents"]["get"]["parameters"][6]["schema"]["minimum"],
        1
    );
    assert_eq!(
        schema["paths"]["/tasks"]["post"]["responses"]["404"]["description"],
        "Referenced record not found"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["post"]["responses"]["404"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["post"]["responses"]["201"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/TaskMutationResponse"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["post"]["responses"]["422"]["description"],
        "Unprocessable request"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["post"]["responses"]["422"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}"]["delete"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/TaskDeleteResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["TaskDeleteResponse"]["required"],
        serde_json::json!(["id", "deleted"])
    );
    assert_eq!(
        schema["components"]["schemas"]["TaskDeleteResponse"]["properties"]["deleted"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["Task"]["properties"]["tool"]["anyOf"][0]["$ref"],
        "#/components/schemas/ToolInvocation"
    );
    assert_eq!(
        schema["components"]["schemas"]["Task"]["properties"]["created_at"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["components"]["schemas"]["Task"]["properties"]["attempts"]["minimum"],
        0
    );
    assert_eq!(
        schema["components"]["schemas"]["Task"]["properties"]["max_attempts"]["minimum"],
        1
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][0]["name"],
        "status"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["type"],
        "array"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["items"]["$ref"],
        "#/components/schemas/Task"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][0]["in"],
        "query"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][0]["schema"]["enum"],
        serde_json::json!(agent_os::TaskStatus::INPUT_VALUES)
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][1]["name"],
        "priority"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][1]["schema"]["enum"],
        serde_json::json!(agent_os::Priority::INPUT_VALUES)
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][2]["name"],
        "agent"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][2]["schema"]["pattern"],
        ".*[A-Za-z0-9-].*"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][3]["name"],
        "tool"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][3]["schema"]["pattern"],
        ".*[A-Za-z0-9-].*"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][4]["name"],
        "after"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][4]["schema"]["pattern"],
        ".*[A-Za-z0-9-].*"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][5]["name"],
        "capability"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][5]["schema"]["pattern"],
        r"^\s*[^,\s][^,]*(?:,\s*[^,\s][^,]*)*\s*$"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][6]["name"],
        "since"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][6]["schema"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][7]["name"],
        "until"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][7]["schema"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][8]["name"],
        "query"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][8]["schema"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][9]["name"],
        "limit"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["get"]["parameters"][9]["schema"]["minimum"],
        1
    );
    assert_eq!(
        schema["paths"]["/tools"]["post"]["responses"]["422"]["description"],
        "Unprocessable request"
    );
    assert_eq!(
        schema["paths"]["/tools"]["post"]["responses"]["201"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ToolMutationResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolMutationResponse"]["required"],
        serde_json::json!(["id", "tool"])
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolMutationResponse"]["properties"]["tool"]["$ref"],
        "#/components/schemas/ToolDefinition"
    );
    assert_eq!(
        schema["paths"]["/tools/{id}"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/UpdateToolRequest"
    );
    assert_eq!(
        schema["paths"]["/tools/{id}"]["post"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/ToolMutationResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateToolRequest"]["properties"]["clear_cwd"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateToolRequest"]["anyOf"][0]["required"],
        serde_json::json!(["kind"])
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateToolRequest"]["anyOf"][5]["properties"]["clear_description"]
            ["const"],
        true
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateToolRequest"]["allOf"][0]["not"]["required"],
        serde_json::json!(["clear_description", "description"])
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateToolRequest"]["properties"]["clear_description"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateToolRequest"]["properties"]["clear_required_capabilities"]
            ["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolDefinition"]["properties"]["kind"]["enum"],
        serde_json::json!(agent_os::ToolKind::VALUES)
    );
    let slug_pattern = r"^[a-z0-9]+(?:-[a-z0-9]+)*$";
    let normalized_entry_pattern = r"^(?!\s)(?!.*\s$)(?!.*,)[^A-Z]+$";
    let non_empty_pattern = r".*\S.*";
    assert_eq!(
        schema["components"]["schemas"]["OperatingSystem"]["properties"]["agents"]["propertyNames"]
            ["pattern"],
        slug_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["OperatingSystem"]["properties"]["version"]["minimum"],
        agent_os::CURRENT_STATE_VERSION
    );
    assert_eq!(
        schema["components"]["schemas"]["OperatingSystem"]["properties"]["version"]["maximum"],
        agent_os::CURRENT_STATE_VERSION
    );
    assert_eq!(
        schema["components"]["schemas"]["OperatingSystem"]["properties"]["name"]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["OperatingSystem"]["properties"]["events"]["maxItems"],
        500
    );
    assert_eq!(
        schema["components"]["schemas"]["Agent"]["properties"]["id"]["pattern"],
        slug_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Agent"]["properties"]["name"]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Agent"]["properties"]["kind"]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Agent"]["properties"]["model"]["anyOf"][0]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Agent"]["properties"]["capabilities"]["minItems"],
        1
    );
    assert_eq!(
        schema["components"]["schemas"]["Agent"]["properties"]["capabilities"]["items"]["pattern"],
        normalized_entry_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Agent"]["properties"]["current_tasks"]["items"]["pattern"],
        slug_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Task"]["properties"]["id"]["pattern"],
        slug_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Task"]["properties"]["title"]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Task"]["properties"]["objective"]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Task"]["properties"]["command"]["anyOf"][0]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Task"]["properties"]["dependencies"]["items"]["pattern"],
        slug_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Task"]["properties"]["required_capabilities"]["items"]["pattern"],
        normalized_entry_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Task"]["properties"]["assigned_to"]["anyOf"][0]["pattern"],
        slug_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Task"]["properties"]["plan"]["items"]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Workflow"]["properties"]["tasks"]["additionalProperties"]
            ["pattern"],
        slug_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Workflow"]["properties"]["objective"]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowProgress"]["properties"]["current_stage"]["anyOf"]
            [0]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowStageProgress"]["properties"]["stage"]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["WorkflowStageProgress"]["properties"]["title"]["anyOf"][0]
            ["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolDefinition"]["properties"]["name"]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolDefinition"]["properties"]["command_template"]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolDefinition"]["properties"]["default_cwd"]["anyOf"][0]
            ["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolDefinition"]["properties"]["required_capabilities"]["items"]
            ["pattern"],
        normalized_entry_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["MemoryRecord"]["properties"]["topic"]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["MemoryRecord"]["properties"]["body"]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["MemoryRecord"]["properties"]["tags"]["items"]["pattern"],
        normalized_entry_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Event"]["properties"]["message"]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Assignment"]["properties"]["agent_id"]["pattern"],
        slug_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["Assignment"]["properties"]["reason"]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["RunRecord"]["properties"]["task_id"]["pattern"],
        slug_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["RunRecord"]["properties"]["trace_id"]["pattern"],
        slug_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["RunRecord"]["properties"]["command"]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["RunRecord"]["properties"]["cwd"]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["RunRecord"]["properties"]["log_path"]["anyOf"][0]["pattern"],
        non_empty_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["RunRecord"]["properties"]["agent_id"]["anyOf"][0]["pattern"],
        slug_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolInvocation"]["properties"]["args"]["additionalProperties"]
            ["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolInvocation"]["properties"]["tool_id"]["pattern"],
        slug_pattern
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolInvocation"]["properties"]["args"]["propertyNames"]["pattern"],
        r"^(?!\s)(?!.*\s$)[^=\u0000]+$"
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolInvocation"]["properties"]["secret_env_args"]["propertyNames"]
            ["pattern"],
        r"^(?!\s)(?!.*\s$)[^=\u0000]+$"
    );
    assert_eq!(
        schema["components"]["schemas"]["ToolInvocation"]["properties"]["secret_env_args"]["additionalProperties"]
            ["pattern"],
        r"^(?:[A-Za-z_][A-Za-z0-9_]*|[A-Za-z0-9][A-Za-z0-9_-]*:[^\s\u0000][^\u0000]*)$"
    );
    assert_eq!(
        schema["paths"]["/tools"]["get"]["parameters"][0]["name"],
        "kind"
    );
    assert_eq!(
        schema["paths"]["/tools"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["type"],
        "array"
    );
    assert_eq!(
        schema["paths"]["/tools"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["items"]["$ref"],
        "#/components/schemas/ToolDefinition"
    );
    assert_eq!(
        schema["paths"]["/tools"]["get"]["parameters"][0]["in"],
        "query"
    );
    assert_eq!(
        schema["paths"]["/tools"]["get"]["parameters"][0]["schema"]["enum"],
        serde_json::json!(agent_os::ToolKind::INPUT_VALUES)
    );
    assert_eq!(
        schema["paths"]["/tools"]["get"]["parameters"][1]["name"],
        "capability"
    );
    assert_eq!(
        schema["paths"]["/tools"]["get"]["parameters"][1]["schema"]["pattern"],
        r"^\s*[^,\s][^,]*(?:,\s*[^,\s][^,]*)*\s*$"
    );
    assert_eq!(
        schema["paths"]["/tools"]["get"]["parameters"][2]["name"],
        "since"
    );
    assert_eq!(
        schema["paths"]["/tools"]["get"]["parameters"][2]["schema"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["paths"]["/tools"]["get"]["parameters"][3]["name"],
        "until"
    );
    assert_eq!(
        schema["paths"]["/tools"]["get"]["parameters"][3]["schema"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["paths"]["/tools"]["get"]["parameters"][4]["name"],
        "query"
    );
    assert_eq!(
        schema["paths"]["/tools"]["get"]["parameters"][4]["schema"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["paths"]["/tools"]["get"]["parameters"][5]["name"],
        "limit"
    );
    assert_eq!(
        schema["paths"]["/tools"]["get"]["parameters"][5]["schema"]["minimum"],
        1
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateAgentRequest"]["required"],
        serde_json::json!(["name", "capabilities"])
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateAgentRequest"]["properties"]["capabilities"]["minItems"],
        1
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateAgentRequest"]["properties"]["capabilities"]["items"]
            ["pattern"],
        r"^\s*[^,\s][^,]*(?:,\s*[^,\s][^,]*)*\s*$"
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateAgentRequest"]["properties"]["name"]["pattern"],
        ".*[A-Za-z0-9-].*"
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateAgentRequest"]["properties"]["model"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateAgentRequest"]["properties"]["clear_model"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateAgentRequest"]["anyOf"][0]["required"],
        serde_json::json!(["name"])
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateAgentRequest"]["anyOf"][5]["properties"]["clear_model"]
            ["const"],
        true
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateAgentRequest"]["allOf"][0]["not"]["required"],
        serde_json::json!(["clear_model", "model"])
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateAgentRequest"]["properties"]["capabilities"]["minItems"],
        1
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateAgentRequest"]["properties"]["capabilities"]["items"]
            ["pattern"],
        r"^\s*[^,\s][^,]*(?:,\s*[^,\s][^,]*)*\s*$"
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateTaskRequest"]["properties"]["title"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateTaskRequest"]["properties"]["tool"]["pattern"],
        ".*[A-Za-z0-9-].*"
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateTaskRequest"]["dependentRequired"]["args"],
        serde_json::json!(["tool"])
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateTaskRequest"]["dependentRequired"]["secret_args"],
        serde_json::json!(["tool"])
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateTaskRequest"]["not"]["required"],
        serde_json::json!(["command", "tool"])
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateTaskRequest"]["properties"]["required_capabilities"]
            ["items"]["pattern"],
        r"^\s*[^,\s][^,]*(?:,\s*[^,\s][^,]*)*\s*$"
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateTaskRequest"]["properties"]["dependencies"]["items"]
            ["pattern"],
        ".*[A-Za-z0-9-].*"
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateTaskRequest"]["properties"]["max_attempts"]["minimum"],
        1
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateTaskRequest"]["properties"]["max_attempts"]["minimum"],
        1
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateToolRequest"]["properties"]["name"]["pattern"],
        ".*[A-Za-z0-9-].*"
    );
    assert_eq!(
        schema["paths"]["/memory"]["post"]["responses"]["201"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/MemoryMutationResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["MemoryMutationResponse"]["required"],
        serde_json::json!(["id", "memory"])
    );
    assert_eq!(
        schema["components"]["schemas"]["MemoryMutationResponse"]["properties"]["memory"]["$ref"],
        "#/components/schemas/MemoryRecord"
    );
    assert_eq!(
        schema["components"]["schemas"]["MemoryRecord"]["properties"]["tags"]["items"]["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["MemoryRecord"]["properties"]["visibility"]["enum"],
        serde_json::json!(["shared", "private"])
    );
    assert_eq!(
        schema["components"]["schemas"]["MemoryRecord"]["properties"]["scope"]["anyOf"][0]["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["MemoryRecord"]["properties"]["scope"]["anyOf"][1]["type"],
        "null"
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateMemoryRequest"]["properties"]["tags"]["items"]["pattern"],
        r"^\s*[^,\s][^,]*(?:,\s*[^,\s][^,]*)*\s*$"
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateMemoryRequest"]["properties"]["visibility"]["default"],
        "shared"
    );
    assert_eq!(
        schema["components"]["schemas"]["MemoryRecord"]["properties"]["updated_at"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["components"]["schemas"]["Event"]["properties"]["at"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["paths"]["/tasks"]["post"]["requestBody"]["required"],
        true
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/plan"]["post"]["requestBody"]["required"],
        true
    );
    assert_eq!(
        schema["components"]["schemas"]["PlanTaskRequest"]["properties"]["steps"]["items"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/assign"]["post"]["requestBody"]["required"],
        true
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/priority"]["post"]["requestBody"]["required"],
        true
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/dependencies"]["post"]["requestBody"]["required"],
        true
    );
    assert_eq!(
        schema["paths"]["/agents/{id}/heartbeat"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/agents/{id}/claim"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/tasks/{id}/complete"]["post"]["requestBody"]["required"],
        false
    );
    for endpoint in ["fail", "block", "cancel", "retry", "unblock"] {
        assert_eq!(
            schema["paths"][format!("/tasks/{{id}}/{endpoint}")]["post"]["requestBody"]["required"],
            false
        );
    }
    assert_eq!(
        schema["paths"]["/state/repair"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/state/repair"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/RepairStateResponse"
    );
    assert_eq!(
        schema["paths"]["/state/repair"]["post"]["responses"]["409"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/RepairStateResponse"
    );
    assert_eq!(
        schema["paths"]["/state/backup"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/state/backup"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/BackupStateRequest"
    );
    assert_eq!(
        schema["paths"]["/state/backup"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/BackupStateResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["BackupStateResponse"]["required"],
        serde_json::json!(["dry_run", "backup"])
    );
    assert_eq!(
        schema["components"]["schemas"]["BackupStateRequest"]["properties"]["dry_run"]["default"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["BackupStateResponse"]["properties"]["dry_run"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["paths"]["/state/prune"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/state/prune"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/PruneStateRequest"
    );
    assert_eq!(
        schema["paths"]["/state/prune"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/PruneReport"
    );
    assert_eq!(
        schema["components"]["schemas"]["PruneStateRequest"]["properties"]["keep_runs"]["default"],
        100
    );
    assert_eq!(
        schema["components"]["schemas"]["PruneReport"]["required"],
        serde_json::json!([
            "dry_run",
            "removed_runs",
            "removed_log_paths",
            "removed_artifact_paths",
            "removed_events"
        ])
    );
    assert_eq!(
        schema["components"]["schemas"]["RepairReport"]["required"],
        serde_json::json!(["changed", "persisted", "repairs", "validation"])
    );
    assert_eq!(
        schema["components"]["schemas"]["RepairReport"]["properties"]["persisted"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["paths"]["/run"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/run"]["post"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/RunOnceResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["RunRequest"]["properties"]["dry_run"]["default"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["RunRequest"]["allOf"][0]["not"]["required"],
        serde_json::json!(["dry_run", "execute"])
    );
    assert_eq!(
        schema["components"]["schemas"]["RunRequest"]["allOf"][0]["not"]["properties"]["dry_run"]["const"],
        true
    );
    assert_eq!(
        schema["components"]["schemas"]["RunOnceResponse"]["required"],
        serde_json::json!(["dry_run", "scheduler", "runs", "errors"])
    );
    assert_eq!(
        schema["components"]["schemas"]["RunOnceResponse"]["properties"]["dry_run"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["RunOnceResponse"]["properties"]["scheduler"]["$ref"],
        "#/components/schemas/RuntimeReport"
    );
    assert_eq!(
        schema["components"]["schemas"]["RunOnceResponse"]["properties"]["runs"]["items"]["$ref"],
        "#/components/schemas/RunRecord"
    );
    assert_eq!(
        schema["components"]["schemas"]["RuntimeReport"]["properties"]["assignments"]["items"]["$ref"],
        "#/components/schemas/Assignment"
    );
    assert_eq!(
        schema["components"]["schemas"]["RuntimeReport"]["properties"]["expired_agents"]["items"]["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["RuntimeReport"]["properties"]["deadlocked_tasks"]["items"]
            ["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["RuntimeReport"]["properties"]["unscheduled_tasks"]["items"]
            ["$ref"],
        "#/components/schemas/UnscheduledTask"
    );
    assert_eq!(
        schema["components"]["schemas"]["UnscheduledTask"]["required"],
        serde_json::json!(["task_id", "reason"])
    );
    assert_eq!(
        schema["components"]["schemas"]["UnscheduledTask"]["properties"]["reason"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["components"]["schemas"]["RuntimeReport"]["properties"]["recovered_runs"]["items"]["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["RuntimeReport"]["properties"]["recovered_daemon"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["RunRecord"]["properties"]["status"]["enum"],
        serde_json::json!(agent_os::RunStatus::VALUES)
    );
    assert_eq!(
        schema["components"]["schemas"]["RunRecord"]["properties"]["finished_at"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["parameters"][0]["name"],
        "status"
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["type"],
        "array"
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["items"]["$ref"],
        "#/components/schemas/RunRecord"
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["parameters"][0]["in"],
        "query"
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["parameters"][0]["schema"]["enum"],
        serde_json::json!(agent_os::RunStatus::INPUT_VALUES)
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["parameters"][1]["name"],
        "task"
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["parameters"][1]["schema"]["pattern"],
        ".*[A-Za-z0-9-].*"
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["parameters"][2]["name"],
        "agent"
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["parameters"][2]["schema"]["pattern"],
        ".*[A-Za-z0-9-].*"
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["parameters"][3]["name"],
        "since"
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["parameters"][3]["schema"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["parameters"][4]["name"],
        "until"
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["parameters"][4]["schema"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["parameters"][5]["name"],
        "query"
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["parameters"][5]["schema"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["parameters"][6]["name"],
        "limit"
    );
    assert_eq!(
        schema["paths"]["/runs"]["get"]["parameters"][6]["schema"]["minimum"],
        1
    );
    assert_eq!(
        schema["paths"]["/events"]["get"]["parameters"][0]["name"],
        "limit"
    );
    assert_eq!(
        schema["paths"]["/events"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["type"],
        "array"
    );
    assert_eq!(
        schema["paths"]["/events"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["items"]["$ref"],
        "#/components/schemas/Event"
    );
    assert_eq!(
        schema["paths"]["/events"]["get"]["parameters"][0]["in"],
        "query"
    );
    assert_eq!(
        schema["paths"]["/events"]["get"]["parameters"][0]["schema"]["minimum"],
        1
    );
    assert_eq!(
        schema["paths"]["/events"]["get"]["parameters"][1]["name"],
        "kind"
    );
    assert_eq!(
        schema["paths"]["/events"]["get"]["parameters"][1]["schema"]["enum"],
        serde_json::json!(agent_os::EventKind::VALUES)
    );
    assert_eq!(
        schema["paths"]["/events"]["get"]["parameters"][2]["name"],
        "since"
    );
    assert_eq!(
        schema["paths"]["/events"]["get"]["parameters"][2]["schema"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["paths"]["/events"]["get"]["parameters"][3]["name"],
        "until"
    );
    assert_eq!(
        schema["paths"]["/events"]["get"]["parameters"][3]["schema"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["paths"]["/events"]["get"]["parameters"][4]["name"],
        "query"
    );
    assert_eq!(
        schema["paths"]["/events"]["get"]["parameters"][4]["schema"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["parameters"][0]["name"],
        "query"
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["type"],
        "array"
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["items"]["$ref"],
        "#/components/schemas/MemoryRecord"
    );
    assert_eq!(
        schema["paths"]["/memory/recall"]["get"]["parameters"][0]["name"],
        "query"
    );
    assert_eq!(
        schema["paths"]["/memory/recall"]["get"]["parameters"][0]["required"],
        true
    );
    assert_eq!(
        schema["paths"]["/memory/recall"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["items"]["$ref"],
        "#/components/schemas/MemoryRecallHit"
    );
    assert_eq!(
        schema["components"]["schemas"]["MemoryRecallHit"]["properties"]["record"]["$ref"],
        "#/components/schemas/MemoryRecord"
    );
    assert_eq!(
        schema["components"]["schemas"]["MemoryRecallHit"]["properties"]["score"]["minimum"],
        0
    );
    assert_eq!(
        schema["paths"]["/memory/{id}"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/MemoryRecord"
    );
    assert_eq!(
        schema["paths"]["/memory/{id}"]["delete"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/MemoryDeleteResponse"
    );
    assert_eq!(
        schema["paths"]["/memory/{id}"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/MemoryMutationResponse"
    );
    assert_eq!(
        schema["paths"]["/memory/{id}"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/UpdateMemoryRequest"
    );
    assert_eq!(
        schema["paths"]["/memory/prune"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/memory/prune"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/PruneMemoryRequest"
    );
    assert_eq!(
        schema["paths"]["/memory/prune"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/PruneMemoryResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["MemoryDeleteResponse"]["required"],
        serde_json::json!(["id", "removed", "memory"])
    );
    assert_eq!(
        schema["components"]["schemas"]["PruneMemoryRequest"]["properties"]["max_age_days"]["minimum"],
        1
    );
    assert_eq!(
        schema["components"]["schemas"]["PruneMemoryRequest"]["properties"]["dry_run"]["default"],
        false
    );
    assert_eq!(
        schema["components"]["schemas"]["PruneMemoryResponse"]["required"],
        serde_json::json!(["dry_run", "max_age_days", "removed", "expired"])
    );
    assert_eq!(
        schema["components"]["schemas"]["PruneMemoryResponse"]["properties"]["removed"]["items"]["$ref"],
        "#/components/schemas/MemoryRecord"
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateMemoryRequest"]["properties"]["tags"]["type"],
        "array"
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateMemoryRequest"]["properties"]["tags"]["items"]["pattern"],
        r"^\s*[^,\s][^,]*(?:,\s*[^,\s][^,]*)*\s*$"
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateMemoryRequest"]["properties"]["visibility"]["enum"],
        serde_json::json!(["shared", "private"])
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateMemoryRequest"]["properties"]["clear_scope"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateMemoryRequest"]["properties"]["clear_tags"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateMemoryRequest"]["anyOf"][0]["required"],
        serde_json::json!(["topic"])
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateMemoryRequest"]["anyOf"][5]["properties"]["clear_tags"]
            ["const"],
        true
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateMemoryRequest"]["anyOf"][6]["properties"]["clear_scope"]
            ["const"],
        true
    );
    assert_eq!(
        schema["components"]["schemas"]["UpdateMemoryRequest"]["allOf"][0]["not"]["required"],
        serde_json::json!(["clear_tags", "tags"])
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["parameters"][0]["in"],
        "query"
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["parameters"][0]["schema"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["parameters"][1]["name"],
        "tag"
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["parameters"][1]["schema"]["pattern"],
        r"^\s*[^,\s][^,]*(?:,\s*[^,\s][^,]*)*\s*$"
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["parameters"][2]["name"],
        "visibility"
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["parameters"][2]["schema"]["enum"],
        serde_json::json!(["shared", "private"])
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["parameters"][3]["name"],
        "scope"
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["parameters"][3]["schema"]["pattern"],
        r".*\S.*"
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["parameters"][4]["name"],
        "since"
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["parameters"][4]["schema"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["parameters"][5]["name"],
        "until"
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["parameters"][5]["schema"]["format"],
        "date-time"
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["parameters"][6]["name"],
        "limit"
    );
    assert_eq!(
        schema["paths"]["/memory"]["get"]["parameters"][6]["schema"]["minimum"],
        1
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateTaskRequest"]["properties"]["secret_args"]["propertyNames"]
            ["minLength"],
        1
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateTaskRequest"]["properties"]["args"]["propertyNames"]
            ["pattern"],
        r"^(?!\s)(?!.*\s$)[^=\u0000]+$"
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateTaskRequest"]["properties"]["secret_args"]["propertyNames"]
            ["pattern"],
        r"^(?!\s)(?!.*\s$)[^=\u0000]+$"
    );
    assert_eq!(
        schema["components"]["schemas"]["CreateTaskRequest"]["properties"]["secret_args"]["additionalProperties"]
            ["pattern"],
        r"^(?:[A-Za-z_][A-Za-z0-9_]*|[A-Za-z0-9][A-Za-z0-9_-]*:[^\s\u0000][^\u0000]*)$"
    );
    assert_eq!(
        schema["components"]["schemas"]["HeartbeatRequest"]["properties"]["lease_seconds"]["minimum"],
        1
    );
    assert_eq!(
        schema["components"]["schemas"]["RunRequest"]["properties"]["recover_stale_seconds"]["minimum"],
        0
    );
    assert_eq!(
        schema["paths"]["/tasks/recover"]["post"]["requestBody"]["required"],
        false
    );
    assert_eq!(
        schema["paths"]["/tasks/recover"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/RecoverTasksRequest"
    );
    assert_eq!(
        schema["paths"]["/tasks/recover"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/RecoverTasksResponse"
    );
    assert_eq!(
        schema["components"]["schemas"]["RecoverTasksRequest"]["properties"]["older_than_seconds"]
            ["default"],
        1800
    );
    assert_eq!(
        schema["components"]["schemas"]["RecoverTasksResponse"]["properties"]["recovered_runs"]["items"]
            ["type"],
        "string"
    );
    assert_eq!(
        schema["components"]["schemas"]["RecoverTasksResponse"]["properties"]["recovered_daemon"]["type"],
        "boolean"
    );
}

#[test]
fn api_serve_can_require_bearer_token() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "",
            "--max-requests",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("token_env must not be empty"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            " BAD_TOKEN ",
            "--max-requests",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "token_env must be a valid environment variable name",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_EMPTY_TOKEN", " ")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_EMPTY_TOKEN",
            "--max-requests",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "API token from token_env must not be empty",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_SHORT_TOKEN", "short")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_SHORT_TOKEN",
            "--max-requests",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "API token from token_env must be at least 8 bytes",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-file",
            " ",
            "--max-requests",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("token_file must not be empty"));

    let empty_token_file = dir.path().join("empty-token.txt");
    std::fs::write(&empty_token_file, "  \n").expect("empty token file");
    make_private_file(&empty_token_file);
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-file",
            empty_token_file.to_str().expect("empty token path"),
            "--max-requests",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "API token from token_file must not be empty",
        ));

    let whitespace_token_file = dir.path().join("whitespace-token.txt");
    std::fs::write(&whitespace_token_file, "file token\n").expect("whitespace token file");
    make_private_file(&whitespace_token_file);
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-file",
            whitespace_token_file
                .to_str()
                .expect("whitespace token path"),
            "--max-requests",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "API token from token_file must not contain whitespace or control characters",
        ));

    let token_file = dir.path().join("api-token.txt");
    std::fs::write(&token_file, "file-token\n").expect("token file");
    make_private_file(&token_file);
    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_API_TOKEN", "test-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--token-file",
            token_file.to_str().expect("token path"),
            "--max-requests",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "use either --token-env or --token-file, not both",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "0.0.0.0:0",
            "--max-requests",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "non-loopback addresses requires --token-env, --token-file, --read-token-env, --read-token-file, --write-token-env, --write-token-file, or --unsafe-no-token",
        ));

    #[cfg(unix)]
    {
        let group_readable_token_file = dir.path().join("group-readable-token.txt");
        std::fs::write(&group_readable_token_file, "group-token\n")
            .expect("group readable token file");
        make_group_readable_file(&group_readable_token_file);

        Command::cargo_bin("agent-os")
            .expect("binary")
            .args([
                "--state",
                state_arg,
                "api",
                "serve",
                "--addr",
                "127.0.0.1:0",
                "--token-file",
                group_readable_token_file
                    .to_str()
                    .expect("group readable token path"),
                "--max-requests",
                "1",
            ])
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "API token file token_file must not be accessible by group or others",
            ));

        let symlink_target_token_file = dir.path().join("symlink-target-token.txt");
        std::fs::write(&symlink_target_token_file, "symlink-token\n")
            .expect("symlink target token file");
        make_private_file(&symlink_target_token_file);
        let symlink_token_file = dir.path().join("symlink-token.txt");
        std::os::unix::fs::symlink(&symlink_target_token_file, &symlink_token_file)
            .expect("token file symlink");

        Command::cargo_bin("agent-os")
            .expect("binary")
            .args([
                "--state",
                state_arg,
                "api",
                "serve",
                "--addr",
                "127.0.0.1:0",
                "--token-file",
                symlink_token_file.to_str().expect("symlink token path"),
                "--max-requests",
                "1",
            ])
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "API token file token_file must be a regular file, not a symlink",
            ));
    }

    let mut file_child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-file",
            token_file.to_str().expect("token path"),
            "--max-requests",
            "2",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn token file api");

    let stdout = file_child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let unauthorized = http_get(addr, "/status", &[]);
    assert!(
        unauthorized.contains("HTTP/1.1 401 Unauthorized"),
        "{unauthorized}"
    );
    let authorized = http_get(addr, "/status", &[("authorization", "Bearer file-token")]);
    assert!(authorized.contains("HTTP/1.1 200 OK"), "{authorized}");
    wait_for_api_success(&mut file_child);

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "test-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "5",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let unauthorized = http_get(addr, "/status", &[]);
    assert!(
        unauthorized.contains("HTTP/1.1 401 Unauthorized"),
        "{unauthorized}"
    );
    assert!(
        unauthorized.contains("www-authenticate: Bearer"),
        "{unauthorized}"
    );
    assert_eq!(
        serde_json::from_str::<Value>(http_body(&unauthorized)).expect("unauthorized json")["error"],
        "unauthorized"
    );

    let wrong_token = http_get(addr, "/status", &[("authorization", "Bearer wrong-token")]);
    assert!(
        wrong_token.contains("HTTP/1.1 401 Unauthorized"),
        "{wrong_token}"
    );
    assert_eq!(
        serde_json::from_str::<Value>(http_body(&wrong_token)).expect("wrong token json")["error"],
        "unauthorized"
    );

    let duplicate_auth = http_get(
        addr,
        "/status",
        &[
            ("authorization", "Bearer test-token"),
            ("authorization", "Bearer test-token"),
        ],
    );
    assert!(
        duplicate_auth.contains("HTTP/1.1 401 Unauthorized"),
        "{duplicate_auth}"
    );
    assert_eq!(
        serde_json::from_str::<Value>(http_body(&duplicate_auth)).expect("duplicate auth json")["error"],
        "unauthorized"
    );

    let authorized = http_get(addr, "/status", &[("Authorization", "Bearer test-token")]);
    assert!(authorized.contains("HTTP/1.1 200 OK"), "{authorized}");
    assert!(authorized.contains("\"tasks_pending\""), "{authorized}");

    let health = http_get(addr, "/health", &[("authorization", "Bearer test-token")]);
    assert!(health.contains("HTTP/1.1 200 OK"), "{health}");
    assert!(health.contains("cache-control: no-store"), "{health}");
    assert!(
        health.contains("x-content-type-options: nosniff"),
        "{health}"
    );
    assert!(health.contains("\"ok\":true"), "{health}");

    wait_for_api_success(&mut child);
}

#[test]
fn api_serve_supports_scoped_read_and_write_tokens() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let read_token_file = dir.path().join("read-token.txt");
    let write_token_file = dir.path().join("write-token.txt");
    let empty_scoped_token_file = dir.path().join("empty-scoped-token.txt");
    std::fs::write(&read_token_file, "file-read-token\n").expect("read token file");
    std::fs::write(&write_token_file, "file-write-token\n").expect("write token file");
    std::fs::write(&empty_scoped_token_file, " \n").expect("empty scoped token file");
    make_private_file(&read_token_file);
    make_private_file(&write_token_file);
    make_private_file(&empty_scoped_token_file);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_READ_TOKEN", "read-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--read-token-env",
            "AGENT_OS_READ_TOKEN",
            "--read-token-file",
            read_token_file.to_str().expect("read token path"),
            "--max-requests",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "use either --read-token-env or --read-token-file, not both",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--write-token-file",
            empty_scoped_token_file
                .to_str()
                .expect("empty scoped token path"),
            "--max-requests",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "API token from write_token_file must not be empty",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_READ_TOKEN", "same-token")
        .env("AGENT_OS_WRITE_TOKEN", "same-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--read-token-env",
            "AGENT_OS_READ_TOKEN",
            "--write-token-env",
            "AGENT_OS_WRITE_TOKEN",
            "--max-requests",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "read and write API tokens must be distinct",
        ));

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_READ_TOKEN", "read-token")
        .env("AGENT_OS_WRITE_TOKEN", "write-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--read-token-env",
            "AGENT_OS_READ_TOKEN",
            "--write-token-env",
            "AGENT_OS_WRITE_TOKEN",
            "--max-requests",
            "5",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn scoped api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let missing = http_get(addr, "/status", &[]);
    assert!(missing.contains("HTTP/1.1 401 Unauthorized"), "{missing}");

    let read = [("authorization", "Bearer read-token")];
    let write = [("authorization", "Bearer write-token")];

    let status = http_get(addr, "/status", &read);
    assert!(status.contains("HTTP/1.1 200 OK"), "{status}");
    assert!(status.contains("\"tasks_pending\""), "{status}");

    let read_mutation = http_request(addr, "POST", "/tasks", r#"{"title":"blocked"}"#, &read);
    assert!(
        read_mutation.contains("HTTP/1.1 403 Forbidden"),
        "{read_mutation}"
    );
    let read_mutation_body: Value =
        serde_json::from_str(http_body(&read_mutation)).expect("read mutation json");
    assert_eq!(read_mutation_body["error"], "forbidden");

    let write_mutation = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"scoped token task"}"#,
        &write,
    );
    assert!(
        write_mutation.contains("HTTP/1.1 201 Created"),
        "{write_mutation}"
    );

    let write_read = http_get(addr, "/status", &write);
    assert!(
        write_read.contains("HTTP/1.1 403 Forbidden"),
        "{write_read}"
    );

    wait_for_api_success(&mut child);

    let mut file_child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--read-token-file",
            read_token_file.to_str().expect("read token path"),
            "--write-token-file",
            write_token_file.to_str().expect("write token path"),
            "--max-requests",
            "5",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn scoped file api");

    let stdout = file_child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let missing = http_get(addr, "/status", &[]);
    assert!(missing.contains("HTTP/1.1 401 Unauthorized"), "{missing}");

    let read = [("authorization", "Bearer file-read-token")];
    let write = [("authorization", "Bearer file-write-token")];

    let status = http_get(addr, "/status", &read);
    assert!(status.contains("HTTP/1.1 200 OK"), "{status}");

    let read_mutation = http_request(addr, "POST", "/tasks", r#"{"title":"blocked"}"#, &read);
    assert!(
        read_mutation.contains("HTTP/1.1 403 Forbidden"),
        "{read_mutation}"
    );

    let write_mutation = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"scoped file token task"}"#,
        &write,
    );
    assert!(
        write_mutation.contains("HTTP/1.1 201 Created"),
        "{write_mutation}"
    );

    let write_read = http_get(addr, "/status", &write);
    assert!(
        write_read.contains("HTTP/1.1 403 Forbidden"),
        "{write_read}"
    );

    wait_for_api_success(&mut file_child);
}

#[test]
fn api_serve_allows_browser_preflight() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "browser-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "3",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let response = http_request(
        addr,
        "OPTIONS",
        "/tasks",
        "",
        &[
            ("origin", "http://localhost:3000"),
            ("access-control-request-method", "POST"),
            (
                "access-control-request-headers",
                "authorization,content-type",
            ),
        ],
    );
    assert!(response.contains("HTTP/1.1 204 No Content"), "{response}");
    assert!(
        response.contains("access-control-allow-methods: GET, POST, DELETE, OPTIONS"),
        "{response}"
    );
    assert!(
        response.contains("access-control-allow-origin: http://localhost:3000"),
        "{response}"
    );
    assert!(
        response.contains("access-control-allow-headers: authorization, content-type"),
        "{response}"
    );

    let rejected = http_request(
        addr,
        "OPTIONS",
        "/tasks",
        "",
        &[
            ("origin", "https://evil.example"),
            ("access-control-request-method", "POST"),
        ],
    );
    assert!(rejected.contains("HTTP/1.1 403 Forbidden"), "{rejected}");
    assert!(
        !rejected.contains("access-control-allow-origin: https://evil.example"),
        "{rejected}"
    );

    let repeated_origin = http_request(
        addr,
        "OPTIONS",
        "/tasks",
        "",
        &[
            ("origin", "http://localhost:3000"),
            ("origin", "https://evil.example"),
            ("access-control-request-method", "POST"),
        ],
    );
    assert!(
        repeated_origin.contains("HTTP/1.1 400 Bad Request"),
        "{repeated_origin}"
    );
    assert!(
        repeated_origin.contains("origin must not be repeated"),
        "{repeated_origin}"
    );

    wait_for_api_success(&mut child);
}

#[test]
fn api_serve_supports_explicit_cors_origin_allowlist() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .env("AGENT_OS_API_TOKEN", "browser-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--allow-origin",
            "https://dashboard.example/path",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "allow_origin must be an http(s) origin",
        ));

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "browser-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--allow-origin",
            "https://dashboard.example",
            "--max-requests",
            "2",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let allowed = http_request(
        addr,
        "OPTIONS",
        "/tasks",
        "",
        &[
            ("origin", "https://dashboard.example"),
            ("access-control-request-method", "POST"),
        ],
    );
    assert!(allowed.contains("HTTP/1.1 204 No Content"), "{allowed}");
    assert!(
        allowed.contains("access-control-allow-origin: https://dashboard.example"),
        "{allowed}"
    );

    let rejected = http_request(
        addr,
        "OPTIONS",
        "/tasks",
        "",
        &[
            ("origin", "https://other.example"),
            ("access-control-request-method", "POST"),
        ],
    );
    assert!(rejected.contains("HTTP/1.1 403 Forbidden"), "{rejected}");

    wait_for_api_success(&mut child);
}

#[test]
fn api_serve_rejects_malformed_request_framing_without_exiting() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "12",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let invalid_length = http_raw_request(
        addr,
        "POST /tasks HTTP/1.1\r\nhost: localhost\r\ncontent-length: nope\r\n\r\n{}",
    );
    assert!(
        invalid_length.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_length}"
    );
    assert!(
        invalid_length.contains("invalid content-length header"),
        "{invalid_length}"
    );

    let oversized_body = http_raw_request(
        addr,
        "POST /tasks HTTP/1.1\r\nhost: localhost\r\ncontent-length: 1048577\r\n\r\n",
    );
    assert!(
        oversized_body.contains("HTTP/1.1 413 Payload Too Large"),
        "{oversized_body}"
    );
    assert!(
        oversized_body.contains("request body exceeded"),
        "{oversized_body}"
    );

    let oversized_headers = http_raw_request(
        addr,
        &format!(
            "GET /status HTTP/1.1\r\nhost: localhost\r\nx-large: {}\r\n\r\n",
            "x".repeat(65 * 1024)
        ),
    );
    assert!(
        oversized_headers.contains("HTTP/1.1 431 Request Header Fields Too Large"),
        "{oversized_headers}"
    );
    assert!(
        oversized_headers.contains("http headers exceeded"),
        "{oversized_headers}"
    );

    let malformed_line = http_raw_request(addr, "GET\r\nhost: localhost\r\n\r\n");
    assert!(
        malformed_line.contains("HTTP/1.1 400 Bad Request"),
        "{malformed_line}"
    );
    assert!(
        malformed_line.contains("malformed http request line"),
        "{malformed_line}"
    );

    let malformed_header = http_raw_request(addr, "GET /status HTTP/1.1\r\nhost localhost\r\n\r\n");
    assert!(
        malformed_header.contains("HTTP/1.1 400 Bad Request"),
        "{malformed_header}"
    );
    assert!(
        malformed_header.contains("malformed http header line"),
        "{malformed_header}"
    );

    let duplicate_host = http_raw_request(
        addr,
        "GET /status HTTP/1.1\r\nhost: localhost\r\nhost: 127.0.0.1\r\n\r\n",
    );
    assert!(
        duplicate_host.contains("HTTP/1.1 400 Bad Request"),
        "{duplicate_host}"
    );
    assert!(
        duplicate_host.contains("duplicate host header"),
        "{duplicate_host}"
    );

    let transfer_encoding = http_raw_request(
        addr,
        "POST /tasks HTTP/1.1\r\nhost: localhost\r\ntransfer-encoding: chunked\r\ncontent-type: application/json\r\n\r\n0\r\n\r\n",
    );
    assert!(
        transfer_encoding.contains("HTTP/1.1 400 Bad Request"),
        "{transfer_encoding}"
    );
    assert!(
        transfer_encoding.contains("unsupported transfer-encoding header"),
        "{transfer_encoding}"
    );

    let unsupported_method = http_raw_request(
        addr,
        "PATCH /status HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\n\r\n",
    );
    assert!(
        unsupported_method.contains("HTTP/1.1 405 Method Not Allowed"),
        "{unsupported_method}"
    );
    assert!(
        unsupported_method.contains("allow: GET, POST, DELETE, OPTIONS"),
        "{unsupported_method}"
    );
    assert!(
        unsupported_method.contains("\"error\":\"method not allowed\""),
        "{unsupported_method}"
    );
    assert!(
        unsupported_method.contains("\"method\":\"PATCH\""),
        "{unsupported_method}"
    );

    let unsupported_media_type = http_raw_request(
        addr,
        "POST /tasks HTTP/1.1\r\nhost: localhost\r\ncontent-length: 2\r\ncontent-type: text/plain\r\n\r\n{}",
    );
    assert!(
        unsupported_media_type.contains("HTTP/1.1 415 Unsupported Media Type"),
        "{unsupported_media_type}"
    );
    assert!(
        unsupported_media_type.contains("\"error\":\"unsupported media type\""),
        "{unsupported_media_type}"
    );
    assert!(
        unsupported_media_type.contains("application/json"),
        "{unsupported_media_type}"
    );

    let missing_content_type = http_raw_request(
        addr,
        "POST /tasks HTTP/1.1\r\nhost: localhost\r\ncontent-length: 2\r\n\r\n{}",
    );
    assert!(
        missing_content_type.contains("HTTP/1.1 415 Unsupported Media Type"),
        "{missing_content_type}"
    );
    assert!(
        missing_content_type.contains("missing content-type"),
        "{missing_content_type}"
    );

    let json_with_charset = http_raw_request(
        addr,
        "POST /tasks/recover HTTP/1.1\r\nhost: localhost\r\ncontent-length: 24\r\ncontent-type: application/json; charset=utf-8\r\n\r\n{\"older_than_seconds\":0}",
    );
    assert!(
        json_with_charset.contains("HTTP/1.1 200 OK"),
        "{json_with_charset}"
    );
    assert!(
        json_with_charset.contains("\"older_than_seconds\":0"),
        "{json_with_charset}"
    );

    let status_response = http_get(addr, "/status", &[]);
    assert!(
        status_response.contains("HTTP/1.1 200 OK"),
        "{status_response}"
    );
    assert!(
        status_response.contains("\"tasks_pending\""),
        "{status_response}"
    );

    wait_for_api_success(&mut child);
}

#[test]
fn api_path_ids_must_be_valid() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "11",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let agent = http_get(addr, "/agents/!!!", &[]);
    assert!(agent.contains("HTTP/1.1 400 Bad Request"), "{agent}");
    assert!(agent.contains("agent id must contain"), "{agent}");

    let task = http_get(addr, "/tasks/!!!", &[]);
    assert!(task.contains("HTTP/1.1 400 Bad Request"), "{task}");
    assert!(task.contains("task id must contain"), "{task}");

    let tool = http_request(addr, "DELETE", "/tools/!!!", "", &[]);
    assert!(tool.contains("HTTP/1.1 400 Bad Request"), "{tool}");
    assert!(tool.contains("tool id must contain"), "{tool}");

    let memory = http_get(addr, "/memory/!!!", &[]);
    assert!(memory.contains("HTTP/1.1 400 Bad Request"), "{memory}");
    assert!(memory.contains("memory id must contain"), "{memory}");

    let memory_update = http_request(addr, "POST", "/memory/!!!", r#"{"body":"updated"}"#, &[]);
    assert!(
        memory_update.contains("HTTP/1.1 400 Bad Request"),
        "{memory_update}"
    );
    assert!(
        memory_update.contains("memory id must contain"),
        "{memory_update}"
    );

    let memory_delete = http_request(addr, "DELETE", "/memory/!!!", "", &[]);
    assert!(
        memory_delete.contains("HTTP/1.1 400 Bad Request"),
        "{memory_delete}"
    );
    assert!(
        memory_delete.contains("memory id must contain"),
        "{memory_delete}"
    );

    let workflow = http_get(addr, "/workflows/!!!", &[]);
    assert!(workflow.contains("HTTP/1.1 400 Bad Request"), "{workflow}");
    assert!(workflow.contains("workflow id must contain"), "{workflow}");

    let plan = http_request(addr, "POST", "/tasks/!!!/plan", r#"{"steps":["one"]}"#, &[]);
    assert!(plan.contains("HTTP/1.1 400 Bad Request"), "{plan}");
    assert!(plan.contains("task id must contain"), "{plan}");

    let run_logs = http_get(addr, "/runs/!!!/logs", &[]);
    assert!(run_logs.contains("HTTP/1.1 400 Bad Request"), "{run_logs}");
    assert!(run_logs.contains("run id must contain"), "{run_logs}");

    let run_cancel = http_request(addr, "POST", "/runs/!!!/cancel", "", &[]);
    assert!(
        run_cancel.contains("HTTP/1.1 400 Bad Request"),
        "{run_cancel}"
    );
    assert!(run_cancel.contains("run id must contain"), "{run_cancel}");

    let status_response = http_get(addr, "/status", &[]);
    assert!(
        status_response.contains("HTTP/1.1 200 OK"),
        "{status_response}"
    );

    wait_for_api_success(&mut child);
}

#[test]
fn api_can_validate_and_repair_state() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Repair through API",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run"])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    value["agents"]["builder"]["current_tasks"] = serde_json::json!([]);
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "repair-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "5",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");
    let auth = [("authorization", "Bearer repair-token")];

    let invalid = http_get(addr, "/state/validate", &auth);
    assert!(invalid.contains("HTTP/1.1 200 OK"), "{invalid}");
    assert!(invalid.contains("\"valid\":false"), "{invalid}");
    assert!(
        invalid.contains("missing from agent current tasks"),
        "{invalid}"
    );

    let dry_run = http_request(addr, "POST", "/state/repair", r#"{"dry_run":true}"#, &auth);
    assert!(dry_run.contains("HTTP/1.1 200 OK"), "{dry_run}");
    assert!(dry_run.contains("\"dry_run\":true"), "{dry_run}");
    assert!(dry_run.contains("\"changed\":true"), "{dry_run}");
    assert!(dry_run.contains("\"persisted\":false"), "{dry_run}");
    assert!(dry_run.contains("added running task"), "{dry_run}");

    let still_invalid = http_get(addr, "/state/validate", &auth);
    assert!(still_invalid.contains("HTTP/1.1 200 OK"), "{still_invalid}");
    assert!(still_invalid.contains("\"valid\":false"), "{still_invalid}");
    assert!(
        still_invalid.contains("missing from agent current tasks"),
        "{still_invalid}"
    );

    let repair = http_request(addr, "POST", "/state/repair", "", &auth);
    assert!(repair.contains("HTTP/1.1 200 OK"), "{repair}");
    assert!(repair.contains("\"dry_run\":false"), "{repair}");
    assert!(repair.contains("\"changed\":true"), "{repair}");
    assert!(repair.contains("\"persisted\":true"), "{repair}");
    assert!(repair.contains("added running task"), "{repair}");

    let valid = http_get(addr, "/state/validate", &auth);
    assert!(valid.contains("HTTP/1.1 200 OK"), "{valid}");
    assert!(valid.contains("\"valid\":true"), "{valid}");

    wait_for_api_success(&mut child);
}

#[test]
fn api_state_repair_reports_conflict_for_unrepairable_state() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Unrepairable",
            "--need",
            "rust",
        ])
        .assert()
        .success();

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let task_id = value["tasks"]
        .as_object()
        .expect("tasks object")
        .keys()
        .next()
        .expect("task id")
        .to_owned();
    value["agents"]["builder"]["max_parallel_tasks"] = serde_json::json!(0);
    value["tasks"][&task_id]["status"] = serde_json::json!("running");
    value["tasks"][&task_id]["assigned_to"] = serde_json::Value::Null;
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write corrupted state");

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "repair-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "1",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let response = http_request(
        addr,
        "POST",
        "/state/repair",
        "{}",
        &[("authorization", "Bearer repair-token")],
    );
    assert!(response.contains("HTTP/1.1 409 Conflict"), "{response}");
    assert!(response.contains("\"persisted\":false"), "{response}");
    assert!(response.contains("\"valid\":false"), "{response}");
    assert!(response.contains("missing an assigned agent"), "{response}");

    wait_for_api_success(&mut child);

    let after: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    assert_eq!(after["agents"]["builder"]["max_parallel_tasks"], 0);
}

#[test]
fn api_mutating_routes_create_and_update_state_with_auth() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "write-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "104",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let unauthorized = http_request(addr, "POST", "/tasks", "{}", &[]);
    assert!(
        unauthorized.contains("HTTP/1.1 401 Unauthorized"),
        "{unauthorized}"
    );
    assert!(
        unauthorized.contains("www-authenticate: Bearer"),
        "{unauthorized}"
    );
    assert_eq!(
        serde_json::from_str::<Value>(http_body(&unauthorized)).expect("unauthorized json")["error"],
        "unauthorized"
    );

    let auth = [("authorization", "Bearer write-token")];
    let invalid_agent = http_request(
        addr,
        "POST",
        "/agents",
        r#"{"name":"api-zero","capabilities":["rust"],"parallel":0}"#,
        &auth,
    );
    assert!(
        invalid_agent.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_agent}"
    );
    assert!(
        invalid_agent.contains("parallel must be greater than 0"),
        "{invalid_agent}"
    );

    let invalid_agent_name = http_request(
        addr,
        "POST",
        "/agents",
        r#"{"name":"!!!","capabilities":["rust"]}"#,
        &auth,
    );
    assert!(
        invalid_agent_name.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_agent_name}"
    );
    assert!(
        invalid_agent_name.contains("agent name must contain"),
        "{invalid_agent_name}"
    );

    let invalid_agent_capability = http_request(
        addr,
        "POST",
        "/agents",
        r#"{"name":"api-empty-cap","capabilities":["rust"," "]}"#,
        &auth,
    );
    assert!(
        invalid_agent_capability.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_agent_capability}"
    );
    assert!(
        invalid_agent_capability.contains("capabilities must not contain empty capabilities"),
        "{invalid_agent_capability}"
    );

    let invalid_agent_kind = http_request(
        addr,
        "POST",
        "/agents",
        r#"{"name":"api-empty-kind","kind":" ","capabilities":["rust"]}"#,
        &auth,
    );
    assert!(
        invalid_agent_kind.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_agent_kind}"
    );
    assert!(
        invalid_agent_kind.contains("kind must not be empty"),
        "{invalid_agent_kind}"
    );

    let invalid_agent_model = http_request(
        addr,
        "POST",
        "/agents",
        r#"{"name":"api-empty-model","model":" ","capabilities":["rust"]}"#,
        &auth,
    );
    assert!(
        invalid_agent_model.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_agent_model}"
    );
    assert!(
        invalid_agent_model.contains("model must not be empty"),
        "{invalid_agent_model}"
    );

    let agent = http_request(
        addr,
        "POST",
        "/agents",
        r#"{"name":"api-builder","capabilities":["rust"],"parallel":2}"#,
        &auth,
    );
    assert!(agent.contains("HTTP/1.1 201 Created"), "{agent}");
    assert!(agent.contains("api-builder"), "{agent}");

    let update_agent = http_request(
        addr,
        "POST",
        "/agents/api-builder",
        r#"{"name":"API Builder Prime","model":"agent-api-model","capabilities":["rust","plan"],"parallel":3}"#,
        &auth,
    );
    assert!(update_agent.contains("HTTP/1.1 200 OK"), "{update_agent}");
    assert!(update_agent.contains("API Builder Prime"), "{update_agent}");

    let online_agents = http_get(addr, "/agents?status=online&capability=rust", &auth);
    assert!(online_agents.contains("HTTP/1.1 200 OK"), "{online_agents}");
    assert!(online_agents.contains("api-builder"), "{online_agents}");

    let up_agents = http_get(addr, "/agents?status=up&capability=rust", &auth);
    assert!(up_agents.contains("HTTP/1.1 200 OK"), "{up_agents}");
    assert!(up_agents.contains("api-builder"), "{up_agents}");

    let duplicate_agent = http_request(
        addr,
        "POST",
        "/agents",
        r#"{"name":"api-builder","capabilities":["rust"],"parallel":1}"#,
        &auth,
    );
    assert!(
        duplicate_agent.contains("HTTP/1.1 409 Conflict"),
        "{duplicate_agent}"
    );
    assert!(
        duplicate_agent.contains("agent already exists"),
        "{duplicate_agent}"
    );

    let idle_agent = http_request(
        addr,
        "POST",
        "/agents",
        r#"{"name":"api-idle","capabilities":["rust"]}"#,
        &auth,
    );
    assert!(idle_agent.contains("HTTP/1.1 201 Created"), "{idle_agent}");
    let delete_agent = http_request(addr, "DELETE", "/agents/api-idle", "", &auth);
    assert!(delete_agent.contains("HTTP/1.1 200 OK"), "{delete_agent}");
    let deleted_agent = http_get(addr, "/agents/api-idle", &auth);
    assert!(
        deleted_agent.contains("HTTP/1.1 404 Not Found"),
        "{deleted_agent}"
    );

    let invalid_tool = http_request(
        addr,
        "POST",
        "/tools",
        r#"{"name":"api-bad-tool","kind":"magic","command_template":"printf hi"}"#,
        &auth,
    );
    assert!(
        invalid_tool.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_tool}"
    );
    assert!(invalid_tool.contains("invalid kind"), "{invalid_tool}");

    let invalid_tool_name = http_request(
        addr,
        "POST",
        "/tools",
        r#"{"name":"!!!","command_template":"printf hi"}"#,
        &auth,
    );
    assert!(
        invalid_tool_name.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_tool_name}"
    );
    assert!(
        invalid_tool_name.contains("tool name must contain"),
        "{invalid_tool_name}"
    );

    let invalid_tool_template = http_request(
        addr,
        "POST",
        "/tools",
        r#"{"name":"empty-template","command_template":"   "}"#,
        &auth,
    );
    assert!(
        invalid_tool_template.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_tool_template}"
    );
    assert!(
        invalid_tool_template.contains("command_template must not be empty"),
        "{invalid_tool_template}"
    );

    let malformed_tool_template = http_request(
        addr,
        "POST",
        "/tools",
        r#"{"name":"bad-placeholder","command_template":"printf { message }"}"#,
        &auth,
    );
    assert!(
        malformed_tool_template.contains("HTTP/1.1 422 Unprocessable Entity"),
        "{malformed_tool_template}"
    );
    assert!(
        malformed_tool_template.contains("malformed template placeholder"),
        "{malformed_tool_template}"
    );

    let unclosed_tool_template = http_request(
        addr,
        "POST",
        "/tools",
        r#"{"name":"unclosed-placeholder","command_template":"printf {message"}"#,
        &auth,
    );
    assert!(
        unclosed_tool_template.contains("HTTP/1.1 422 Unprocessable Entity"),
        "{unclosed_tool_template}"
    );
    assert!(
        unclosed_tool_template.contains("unclosed template placeholder"),
        "{unclosed_tool_template}"
    );

    let nested_tool_template = http_request(
        addr,
        "POST",
        "/tools",
        r#"{"name":"nested-placeholder","command_template":"printf {{message}}"}"#,
        &auth,
    );
    assert!(
        nested_tool_template.contains("HTTP/1.1 422 Unprocessable Entity"),
        "{nested_tool_template}"
    );
    assert!(
        nested_tool_template.contains("malformed template placeholder"),
        "{nested_tool_template}"
    );

    let invalid_tool_cwd = http_request(
        addr,
        "POST",
        "/tools",
        r#"{"name":"bad-cwd","command_template":"printf hi","cwd":"   "}"#,
        &auth,
    );
    assert!(
        invalid_tool_cwd.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_tool_cwd}"
    );
    assert!(
        invalid_tool_cwd.contains("cwd must not be empty"),
        "{invalid_tool_cwd}"
    );

    let invalid_tool_capability = http_request(
        addr,
        "POST",
        "/tools",
        r#"{"name":"api-empty-tool-cap","required_capabilities":["rust",","],"command_template":"printf hi"}"#,
        &auth,
    );
    assert!(
        invalid_tool_capability.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_tool_capability}"
    );
    assert!(
        invalid_tool_capability
            .contains("required_capabilities must not contain empty capabilities"),
        "{invalid_tool_capability}"
    );

    let tool = http_request(
        addr,
        "POST",
        "/tools",
        r#"{"name":"api-printf","required_capabilities":["rust"],"command_template":"printf {message}"}"#,
        &auth,
    );
    assert!(tool.contains("HTTP/1.1 201 Created"), "{tool}");

    let update_tool = http_request(
        addr,
        "POST",
        "/tools/api-printf",
        r#"{"description":"Updated through API","command_template":"printf api:{message}","required_capabilities":["rust"]}"#,
        &auth,
    );
    assert!(update_tool.contains("HTTP/1.1 200 OK"), "{update_tool}");
    assert!(update_tool.contains("api:{message}"), "{update_tool}");

    let shell_tools = http_get(addr, "/tools?kind=shell&capability=rust", &auth);
    assert!(shell_tools.contains("HTTP/1.1 200 OK"), "{shell_tools}");
    assert!(shell_tools.contains("api-printf"), "{shell_tools}");

    let duplicate_tool = http_request(
        addr,
        "POST",
        "/tools",
        r#"{"name":"api-printf","command_template":"printf again"}"#,
        &auth,
    );
    assert!(
        duplicate_tool.contains("HTTP/1.1 409 Conflict"),
        "{duplicate_tool}"
    );
    assert!(
        duplicate_tool.contains("tool already exists"),
        "{duplicate_tool}"
    );

    let invalid_priority = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Bad priority","priority":"eventually"}"#,
        &auth,
    );
    assert!(
        invalid_priority.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_priority}"
    );
    assert!(
        invalid_priority.contains("invalid priority"),
        "{invalid_priority}"
    );

    let invalid_title = http_request(addr, "POST", "/tasks", r#"{"title":"   "}"#, &auth);
    assert!(
        invalid_title.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_title}"
    );
    assert!(
        invalid_title.contains("title must not be empty"),
        "{invalid_title}"
    );

    let invalid_command = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Bad command","command":"   "}"#,
        &auth,
    );
    assert!(
        invalid_command.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_command}"
    );
    assert!(
        invalid_command.contains("command must not be empty"),
        "{invalid_command}"
    );

    let invalid_task_cwd = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Bad cwd","cwd":"   "}"#,
        &auth,
    );
    assert!(
        invalid_task_cwd.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_task_cwd}"
    );
    assert!(
        invalid_task_cwd.contains("cwd must not be empty"),
        "{invalid_task_cwd}"
    );

    let invalid_objective = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Bad objective","objective":"   "}"#,
        &auth,
    );
    assert!(
        invalid_objective.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_objective}"
    );
    assert!(
        invalid_objective.contains("objective must not be empty"),
        "{invalid_objective}"
    );

    let invalid_task_capability = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Bad capability","required_capabilities":["rust"," "]}"#,
        &auth,
    );
    assert!(
        invalid_task_capability.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_task_capability}"
    );
    assert!(
        invalid_task_capability
            .contains("required_capabilities must not contain empty capabilities"),
        "{invalid_task_capability}"
    );

    let invalid_tool_reference = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Bad tool reference","tool":"   "}"#,
        &auth,
    );
    assert!(
        invalid_tool_reference.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_tool_reference}"
    );
    assert!(
        invalid_tool_reference
            .contains("tool id must contain at least one ASCII letter, digit, or hyphen"),
        "{invalid_tool_reference}"
    );

    let invalid_args_without_tool = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Args without tool","args":{"message":"hello"}}"#,
        &auth,
    );
    assert!(
        invalid_args_without_tool.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_args_without_tool}"
    );
    assert!(
        invalid_args_without_tool.contains("args require a tool"),
        "{invalid_args_without_tool}"
    );

    let invalid_secret_args_without_tool = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Secret args without tool","secret_args":{"message":"MESSAGE_ENV"}}"#,
        &auth,
    );
    assert!(
        invalid_secret_args_without_tool.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_secret_args_without_tool}"
    );
    assert!(
        invalid_secret_args_without_tool.contains("secret_args require a tool"),
        "{invalid_secret_args_without_tool}"
    );

    let invalid_dependency_id = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Bad dependency","dependencies":["   "]}"#,
        &auth,
    );
    assert!(
        invalid_dependency_id.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_dependency_id}"
    );
    assert!(
        invalid_dependency_id.contains(
            "dependency task id must contain at least one ASCII letter, digit, or hyphen"
        ),
        "{invalid_dependency_id}"
    );

    let unknown_task_field = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Unknown field","surprise":true}"#,
        &auth,
    );
    assert!(
        unknown_task_field.contains("HTTP/1.1 400 Bad Request"),
        "{unknown_task_field}"
    );
    assert!(
        unknown_task_field.contains("unknown field"),
        "{unknown_task_field}"
    );

    let invalid_empty_arg_key = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Empty arg key","tool":"api-printf","args":{"":"value"}}"#,
        &auth,
    );
    assert!(
        invalid_empty_arg_key.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_empty_arg_key}"
    );
    assert!(
        invalid_empty_arg_key.contains("tool argument key cannot be empty"),
        "{invalid_empty_arg_key}"
    );

    let invalid_unexpected_arg = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Unexpected arg","tool":"api-printf","args":{"message":"hello","unused":"ignored"}}"#,
        &auth,
    );
    assert!(
        invalid_unexpected_arg.contains("HTTP/1.1 422 Unprocessable Entity"),
        "{invalid_unexpected_arg}"
    );
    assert!(
        invalid_unexpected_arg.contains("unexpected tool argument `unused`"),
        "{invalid_unexpected_arg}"
    );

    let invalid_padded_arg_key = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Padded arg key","tool":"api-printf","args":{" message ":"hello"}}"#,
        &auth,
    );
    assert!(
        invalid_padded_arg_key.contains("HTTP/1.1 422 Unprocessable Entity"),
        "{invalid_padded_arg_key}"
    );
    assert!(
        invalid_padded_arg_key.contains("invalid tool argument key"),
        "{invalid_padded_arg_key}"
    );

    let invalid_overlapping_arg = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Overlapping arg","tool":"api-printf","args":{"message":"hello"},"secret_args":{"message":"MESSAGE_ENV"}}"#,
        &auth,
    );
    assert!(
        invalid_overlapping_arg.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_overlapping_arg}"
    );
    assert!(
        invalid_overlapping_arg.contains("cannot be both args and secret_args"),
        "{invalid_overlapping_arg}"
    );

    let invalid_empty_secret_env = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Empty secret env","tool":"api-printf","secret_args":{"message":"   "}}"#,
        &auth,
    );
    assert!(
        invalid_empty_secret_env.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_empty_secret_env}"
    );
    assert!(
        invalid_empty_secret_env.contains("must name an environment variable"),
        "{invalid_empty_secret_env}"
    );

    let invalid_secret_env_name = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Invalid secret env","tool":"api-printf","secret_args":{"message":"1BAD"}}"#,
        &auth,
    );
    assert!(
        invalid_secret_env_name.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_secret_env_name}"
    );
    assert!(
        invalid_secret_env_name.contains("must name a valid environment variable"),
        "{invalid_secret_env_name}"
    );

    let task = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"API task","tool":"api-printf","args":{"message":"hello"},"priority":"high"}"#,
        &auth,
    );
    assert!(task.contains("HTTP/1.1 201 Created"), "{task}");
    let task_body: Value = serde_json::from_str(http_body(&task)).expect("task body");
    let task_id = task_body["id"].as_str().expect("task id");

    let update_task = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}"),
        r#"{"title":"API task updated","objective":"updated through api","required_capabilities":["rust"]}"#,
        &auth,
    );
    assert!(update_task.contains("HTTP/1.1 200 OK"), "{update_task}");
    assert!(update_task.contains("API task updated"), "{update_task}");

    let update_task_args = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}"),
        r#"{"args":{"message":"updated through api"}}"#,
        &auth,
    );
    assert!(
        update_task_args.contains("HTTP/1.1 200 OK"),
        "{update_task_args}"
    );
    assert!(
        update_task_args.contains("updated through api"),
        "{update_task_args}"
    );

    let invalid_update_task_args = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}"),
        r#"{"args":{"message":"updated through api","unused":"ignored"}}"#,
        &auth,
    );
    assert!(
        invalid_update_task_args.contains("HTTP/1.1 422 Unprocessable Entity"),
        "{invalid_update_task_args}"
    );
    assert!(
        invalid_update_task_args.contains("unexpected tool argument `unused`"),
        "{invalid_update_task_args}"
    );

    let unrelated_critical = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Preexisting critical review","priority":"critical","required_capabilities":["review"]}"#,
        &auth,
    );
    assert!(
        unrelated_critical.contains("HTTP/1.1 201 Created"),
        "{unrelated_critical}"
    );

    let workflow = http_request(
        addr,
        "POST",
        "/workflows",
        r#"{"objective":"Coordinate API workflow","priority":"critical","execute":true}"#,
        &auth,
    );
    assert!(workflow.contains("HTTP/1.1 201 Created"), "{workflow}");
    assert!(workflow.contains("Coordinate API workflow"), "{workflow}");
    let workflow_body: Value = serde_json::from_str(http_body(&workflow)).expect("workflow body");
    let workflow_id = workflow_body["id"].as_str().expect("workflow id");
    assert_eq!(workflow_body["runs"][0]["status"], "success");
    assert_eq!(
        workflow_body["runs"][0]["task_id"],
        workflow_body["tasks"]["plan"]
    );
    let workflow_review_task_id = workflow_body["tasks"]["review"]
        .as_str()
        .expect("workflow review task id");

    let workflows = http_get(addr, "/workflows?priority=critical&limit=1", &auth);
    assert!(workflows.contains("HTTP/1.1 200 OK"), "{workflows}");
    assert!(workflows.contains("Coordinate API workflow"), "{workflows}");

    let workflows_by_task = http_get(
        addr,
        &format!("/workflows?query={workflow_review_task_id}"),
        &auth,
    );
    assert!(
        workflows_by_task.contains("HTTP/1.1 200 OK"),
        "{workflows_by_task}"
    );
    assert!(
        workflows_by_task.contains(workflow_id),
        "{workflows_by_task}"
    );

    let workflow_detail = http_get(addr, &format!("/workflows/{workflow_id}"), &auth);
    assert!(
        workflow_detail.contains("HTTP/1.1 200 OK"),
        "{workflow_detail}"
    );
    assert!(workflow_detail.contains(workflow_id), "{workflow_detail}");

    let workflow_status = http_get(addr, &format!("/workflows/{workflow_id}/status"), &auth);
    assert!(
        workflow_status.contains("HTTP/1.1 200 OK"),
        "{workflow_status}"
    );
    let workflow_status_body: Value =
        serde_json::from_str(http_body(&workflow_status)).expect("workflow status body");
    assert_eq!(workflow_status_body["total_tasks"], 3);
    assert_eq!(workflow_status_body["tasks_complete"], 1);
    assert_eq!(workflow_status_body["current_stage"], "build");

    let default_run_workflow = http_request(
        addr,
        "POST",
        "/workflows",
        r#"{"objective":"Default API workflow run","priority":"high","execute":true}"#,
        &auth,
    );
    assert!(
        default_run_workflow.contains("HTTP/1.1 201 Created"),
        "{default_run_workflow}"
    );
    let default_run_workflow_body: Value =
        serde_json::from_str(http_body(&default_run_workflow)).expect("default run workflow body");
    let default_run_workflow_id = default_run_workflow_body["id"]
        .as_str()
        .expect("default run workflow id");
    let default_workflow_run = http_request(
        addr,
        "POST",
        &format!("/workflows/{default_run_workflow_id}/run"),
        "",
        &auth,
    );
    assert!(
        default_workflow_run.contains("HTTP/1.1 200 OK"),
        "{default_workflow_run}"
    );
    let default_workflow_run_body: Value =
        serde_json::from_str(http_body(&default_workflow_run)).expect("default workflow run body");
    assert_eq!(default_workflow_run_body["progress"]["tasks_complete"], 2);
    assert_eq!(
        default_workflow_run_body["progress"]["current_stage"],
        "review"
    );
    assert_eq!(
        default_workflow_run_body["runs"][0]["task_id"],
        default_run_workflow_body["tasks"]["build"]
    );

    let workflow_run = http_request(
        addr,
        "POST",
        &format!("/workflows/{workflow_id}/run"),
        r#"{"all":true}"#,
        &auth,
    );
    assert!(workflow_run.contains("HTTP/1.1 200 OK"), "{workflow_run}");
    let workflow_run_body: Value =
        serde_json::from_str(http_body(&workflow_run)).expect("workflow run body");
    assert_eq!(workflow_run_body["progress"]["tasks_complete"], 3);
    assert_eq!(workflow_run_body["progress"]["current_stage"], Value::Null);
    assert_eq!(
        workflow_run_body["runs"][0]["task_id"],
        workflow_body["tasks"]["build"]
    );
    assert_eq!(
        workflow_run_body["runs"][1]["task_id"],
        workflow_body["tasks"]["review"]
    );

    let cancellable_workflow = http_request(
        addr,
        "POST",
        "/workflows",
        r#"{"objective":"Cancel API workflow","priority":"high","execute":true}"#,
        &auth,
    );
    assert!(
        cancellable_workflow.contains("HTTP/1.1 201 Created"),
        "{cancellable_workflow}"
    );
    let cancellable_workflow_body: Value =
        serde_json::from_str(http_body(&cancellable_workflow)).expect("cancellable workflow body");
    let cancellable_workflow_id = cancellable_workflow_body["id"]
        .as_str()
        .expect("cancellable workflow id");
    let workflow_cancel = http_request(
        addr,
        "POST",
        &format!("/workflows/{cancellable_workflow_id}/cancel"),
        r#"{"note":"stop"}"#,
        &auth,
    );
    assert!(
        workflow_cancel.contains("HTTP/1.1 200 OK"),
        "{workflow_cancel}"
    );
    let workflow_cancel_body: Value =
        serde_json::from_str(http_body(&workflow_cancel)).expect("workflow cancel body");
    assert_eq!(
        workflow_cancel_body["cancelled_tasks"],
        serde_json::json!([
            cancellable_workflow_body["tasks"]["build"],
            cancellable_workflow_body["tasks"]["review"]
        ])
    );
    assert_eq!(workflow_cancel_body["progress"]["tasks_complete"], 1);
    assert_eq!(workflow_cancel_body["progress"]["tasks_cancelled"], 2);
    assert_eq!(workflow_cancel_body["progress"]["current_stage"], "build");

    let default_cancel_workflow = http_request(
        addr,
        "POST",
        "/workflows",
        r#"{"objective":"Default API workflow cancel","priority":"high","execute":true}"#,
        &auth,
    );
    assert!(
        default_cancel_workflow.contains("HTTP/1.1 201 Created"),
        "{default_cancel_workflow}"
    );
    let default_cancel_workflow_body: Value =
        serde_json::from_str(http_body(&default_cancel_workflow))
            .expect("default cancel workflow body");
    let default_cancel_workflow_id = default_cancel_workflow_body["id"]
        .as_str()
        .expect("default cancel workflow id");
    let default_workflow_cancel = http_request(
        addr,
        "POST",
        &format!("/workflows/{default_cancel_workflow_id}/cancel"),
        "",
        &auth,
    );
    assert!(
        default_workflow_cancel.contains("HTTP/1.1 200 OK"),
        "{default_workflow_cancel}"
    );
    let default_workflow_cancel_body: Value =
        serde_json::from_str(http_body(&default_workflow_cancel))
            .expect("default workflow cancel body");
    assert_eq!(
        default_workflow_cancel_body["cancelled_tasks"],
        serde_json::json!([
            default_cancel_workflow_body["tasks"]["build"],
            default_cancel_workflow_body["tasks"]["review"]
        ])
    );
    assert_eq!(
        default_workflow_cancel_body["progress"]["tasks_cancelled"],
        2
    );

    let delete_workflow = http_request(
        addr,
        "DELETE",
        &format!("/workflows/{workflow_id}"),
        "",
        &auth,
    );
    assert!(
        delete_workflow.contains("HTTP/1.1 200 OK"),
        "{delete_workflow}"
    );
    assert!(
        delete_workflow.contains("\"removed\":true"),
        "{delete_workflow}"
    );

    let high_priority_tasks = http_get(addr, "/tasks?priority=high&capability=rust", &auth);
    assert!(
        high_priority_tasks.contains("HTTP/1.1 200 OK"),
        "{high_priority_tasks}"
    );
    assert!(
        high_priority_tasks.contains("API task updated"),
        "{high_priority_tasks}"
    );

    let initial_plan = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/plan"),
        r#"{"steps":["one","two"]}"#,
        &auth,
    );
    assert!(initial_plan.contains("HTTP/1.1 200 OK"), "{initial_plan}");
    assert!(initial_plan.contains("one"), "{initial_plan}");

    let empty_plan = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/plan"),
        r#"{"steps":[]}"#,
        &auth,
    );
    assert!(
        empty_plan.contains("HTTP/1.1 400 Bad Request"),
        "{empty_plan}"
    );
    assert!(
        empty_plan.contains("plan steps must not be empty"),
        "{empty_plan}"
    );

    let blank_plan_step = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/plan"),
        r#"{"steps":["one","   "]}"#,
        &auth,
    );
    assert!(
        blank_plan_step.contains("HTTP/1.1 400 Bad Request"),
        "{blank_plan_step}"
    );
    assert!(
        blank_plan_step.contains("plan steps must not contain empty steps"),
        "{blank_plan_step}"
    );

    let duplicate_dependency = http_request(
        addr,
        "POST",
        "/tasks",
        &format!(r#"{{"title":"Duplicate dependency","dependencies":["{task_id}","{task_id}"]}}"#),
        &auth,
    );
    assert!(
        duplicate_dependency.contains("HTTP/1.1 400 Bad Request"),
        "{duplicate_dependency}"
    );
    assert!(
        duplicate_dependency.contains("duplicate dependency task"),
        "{duplicate_dependency}"
    );

    let blank_note = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/complete"),
        r#"{"note":"   "}"#,
        &auth,
    );
    assert!(
        blank_note.contains("HTTP/1.1 400 Bad Request"),
        "{blank_note}"
    );
    assert!(
        blank_note.contains("note must not be empty"),
        "{blank_note}"
    );

    let heartbeat = http_request(
        addr,
        "POST",
        "/agents/api-builder/heartbeat",
        r#"{"status":"online","lease_seconds":60}"#,
        &auth,
    );
    assert!(heartbeat.contains("HTTP/1.1 200 OK"), "{heartbeat}");
    assert!(heartbeat.contains("last_heartbeat_at"), "{heartbeat}");

    let invalid_status = http_request(
        addr,
        "POST",
        "/agents/api-builder/heartbeat",
        r#"{"status":"awake"}"#,
        &auth,
    );
    assert!(
        invalid_status.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_status}"
    );
    assert!(
        invalid_status.contains("invalid status"),
        "{invalid_status}"
    );

    let invalid_claim = http_request(
        addr,
        "POST",
        "/agents/api-builder/claim",
        r#"{"lease_seconds":0}"#,
        &auth,
    );
    assert!(
        invalid_claim.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_claim}"
    );
    assert!(
        invalid_claim.contains("lease_seconds must be greater than 0"),
        "{invalid_claim}"
    );

    let claim = http_request(
        addr,
        "POST",
        "/agents/api-builder/claim",
        r#"{"lease_seconds":60}"#,
        &auth,
    );
    assert!(claim.contains("HTTP/1.1 200 OK"), "{claim}");
    assert!(claim.contains("\"claimed\":true"), "{claim}");
    assert!(claim.contains(task_id), "{claim}");
    let agent_after_claim = http_get(addr, "/agents/api-builder", &auth);
    assert!(
        agent_after_claim.contains("\"lease_expires_at\"")
            && !agent_after_claim.contains("\"lease_expires_at\":null"),
        "{agent_after_claim}"
    );
    let empty_claim = http_request(addr, "POST", "/agents/api-builder/claim", "{}", &auth);
    assert!(empty_claim.contains("HTTP/1.1 200 OK"), "{empty_claim}");
    assert!(empty_claim.contains("\"task\":null"), "{empty_claim}");
    let agent_after_empty_claim = http_get(addr, "/agents/api-builder", &auth);
    assert!(
        agent_after_empty_claim.contains("\"lease_expires_at\"")
            && !agent_after_empty_claim.contains("\"lease_expires_at\":null"),
        "{agent_after_empty_claim}"
    );

    let retry_running = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/retry"),
        r#"{"note":"too soon"}"#,
        &auth,
    );
    assert!(
        retry_running.contains("HTTP/1.1 409 Conflict"),
        "{retry_running}"
    );
    assert!(
        retry_running.contains("cannot transition"),
        "{retry_running}"
    );

    let running_plan = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/plan"),
        r#"{"steps":["changed while running"]}"#,
        &auth,
    );
    assert!(
        running_plan.contains("HTTP/1.1 409 Conflict"),
        "{running_plan}"
    );
    assert!(
        running_plan.contains("cannot be edited while running"),
        "{running_plan}"
    );

    let complete = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/complete"),
        r#"{"note":"done through api"}"#,
        &auth,
    );
    assert!(complete.contains("HTTP/1.1 200 OK"), "{complete}");
    assert!(complete.contains("complete"), "{complete}");

    let completed_plan = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/plan"),
        r#"{"steps":["changed after completion"]}"#,
        &auth,
    );
    assert!(
        completed_plan.contains("HTTP/1.1 409 Conflict"),
        "{completed_plan}"
    );
    assert!(
        completed_plan.contains("cannot be edited while complete"),
        "{completed_plan}"
    );

    let completed_priority = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/priority"),
        r#"{"priority":"low"}"#,
        &auth,
    );
    assert!(
        completed_priority.contains("HTTP/1.1 409 Conflict"),
        "{completed_priority}"
    );
    assert!(
        completed_priority.contains("cannot be edited while complete"),
        "{completed_priority}"
    );

    let completed_detail = http_get(addr, &format!("/tasks/{task_id}"), &auth);
    assert!(
        completed_detail.contains(r#""plan":["one","two"]"#),
        "{completed_detail}"
    );
    assert!(
        completed_detail.contains(r#""priority":"high""#),
        "{completed_detail}"
    );

    let complete_tasks = http_get(addr, "/tasks?status=complete", &auth);
    assert!(
        complete_tasks.contains("HTTP/1.1 200 OK"),
        "{complete_tasks}"
    );
    assert!(complete_tasks.contains("API task"), "{complete_tasks}");

    let retry = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/retry"),
        r#"{"note":"run again"}"#,
        &auth,
    );
    assert!(retry.contains("HTTP/1.1 200 OK"), "{retry}");
    assert!(retry.contains("pending"), "{retry}");

    let block = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/block"),
        r#"{"note":"needs input"}"#,
        &auth,
    );
    assert!(block.contains("HTTP/1.1 200 OK"), "{block}");
    assert!(block.contains("blocked"), "{block}");

    let unblock = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/unblock"),
        r#"{"note":"input supplied"}"#,
        &auth,
    );
    assert!(unblock.contains("HTTP/1.1 200 OK"), "{unblock}");
    assert!(unblock.contains("pending"), "{unblock}");

    let cancel = http_request(
        addr,
        "POST",
        &format!("/tasks/{task_id}/cancel"),
        r#"{"note":"not needed"}"#,
        &auth,
    );
    assert!(cancel.contains("HTTP/1.1 200 OK"), "{cancel}");
    assert!(cancel.contains("cancelled"), "{cancel}");

    let delete_referenced_tool = http_request(addr, "DELETE", "/tools/api-printf", "", &auth);
    assert!(
        delete_referenced_tool.contains("HTTP/1.1 409 Conflict"),
        "{delete_referenced_tool}"
    );
    assert!(
        delete_referenced_tool.contains("tool is still referenced"),
        "{delete_referenced_tool}"
    );

    let delete_task = http_request(addr, "DELETE", &format!("/tasks/{task_id}"), "", &auth);
    assert!(delete_task.contains("HTTP/1.1 200 OK"), "{delete_task}");

    let memory = http_request(
        addr,
        "POST",
        "/memory",
        r#"{"topic":"api","body":"mutating routes work","tags":["api","ops"]}"#,
        &auth,
    );
    assert!(memory.contains("HTTP/1.1 201 Created"), "{memory}");
    let memory_body: Value = serde_json::from_str(http_body(&memory)).expect("memory body");
    let memory_id = memory_body["id"].as_str().expect("memory id");

    let memory_search = http_get(addr, "/memory?query=API&tag=ops", &auth);
    assert!(memory_search.contains("HTTP/1.1 200 OK"), "{memory_search}");
    assert!(
        memory_search.contains("mutating routes work"),
        "{memory_search}"
    );

    let memory_recall = http_get(addr, "/memory/recall?query=mutating&tag=ops", &auth);
    assert!(memory_recall.contains("HTTP/1.1 200 OK"), "{memory_recall}");
    assert!(memory_recall.contains("\"score\":"), "{memory_recall}");
    assert!(
        memory_recall.contains("\"snippet\":\"mutating routes work\""),
        "{memory_recall}"
    );

    let memory_detail = http_get(addr, &format!("/memory/{memory_id}"), &auth);
    assert!(memory_detail.contains("HTTP/1.1 200 OK"), "{memory_detail}");
    assert!(
        memory_detail.contains("mutating routes work"),
        "{memory_detail}"
    );

    let update_memory = http_request(
        addr,
        "POST",
        &format!("/memory/{memory_id}"),
        r#"{"body":"updated through api","clear_tags":true}"#,
        &auth,
    );
    assert!(update_memory.contains("HTTP/1.1 200 OK"), "{update_memory}");
    assert!(
        update_memory.contains("updated through api"),
        "{update_memory}"
    );
    assert!(update_memory.contains("\"tags\":[]"), "{update_memory}");

    let conflicting_memory_update = http_request(
        addr,
        "POST",
        &format!("/memory/{memory_id}"),
        r#"{"tags":["ops"],"clear_tags":true}"#,
        &auth,
    );
    assert!(
        conflicting_memory_update.contains("HTTP/1.1 400 Bad Request"),
        "{conflicting_memory_update}"
    );
    assert!(
        conflicting_memory_update.contains("use either tags or clear_tags, not both"),
        "{conflicting_memory_update}"
    );

    let prune_memory = http_request(
        addr,
        "POST",
        "/memory",
        r#"{"topic":"old-api-prune","body":"api prune target"}"#,
        &auth,
    );
    assert!(
        prune_memory.contains("HTTP/1.1 201 Created"),
        "{prune_memory}"
    );
    let prune_memory_body: Value =
        serde_json::from_str(http_body(&prune_memory)).expect("prune memory body");
    let prune_memory_id = prune_memory_body["id"].as_str().expect("prune memory id");
    rewrite_memory_timestamp(&state, "old-api-prune", "2020-01-01T00:00:00Z");

    let prune_dry_run = http_request(
        addr,
        "POST",
        "/memory/prune",
        r#"{"max_age_days":1,"dry_run":true}"#,
        &auth,
    );
    assert!(prune_dry_run.contains("HTTP/1.1 200 OK"), "{prune_dry_run}");
    let prune_dry_run_body: Value =
        serde_json::from_str(http_body(&prune_dry_run)).expect("prune dry run body");
    assert_eq!(prune_dry_run_body["dry_run"], true);
    assert_eq!(prune_dry_run_body["expired"][0]["id"], prune_memory_id);
    assert_eq!(
        prune_dry_run_body["removed"]
            .as_array()
            .expect("dry run removed")
            .len(),
        0
    );

    let prune_memory = http_request(
        addr,
        "POST",
        "/memory/prune",
        r#"{"max_age_days":1}"#,
        &auth,
    );
    assert!(prune_memory.contains("HTTP/1.1 200 OK"), "{prune_memory}");
    let prune_memory_body: Value =
        serde_json::from_str(http_body(&prune_memory)).expect("prune response body");
    assert_eq!(prune_memory_body["dry_run"], false);
    assert_eq!(prune_memory_body["removed"][0]["id"], prune_memory_id);

    let pruned_memory_detail = http_get(addr, &format!("/memory/{prune_memory_id}"), &auth);
    assert!(
        pruned_memory_detail.contains("HTTP/1.1 404 Not Found"),
        "{pruned_memory_detail}"
    );

    let delete_memory = http_request(addr, "DELETE", &format!("/memory/{memory_id}"), "", &auth);
    assert!(delete_memory.contains("HTTP/1.1 200 OK"), "{delete_memory}");
    assert!(
        delete_memory.contains("\"removed\":true"),
        "{delete_memory}"
    );

    let invalid_memory_tag = http_get(addr, "/memory?tag=,", &auth);
    assert!(
        invalid_memory_tag.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_memory_tag}"
    );
    assert!(
        invalid_memory_tag.contains("tag must include at least one tag"),
        "{invalid_memory_tag}"
    );

    let invalid_memory_query = http_get(addr, "/memory?query=%20%20", &auth);
    assert!(
        invalid_memory_query.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_memory_query}"
    );
    assert!(
        invalid_memory_query.contains("query must not be empty"),
        "{invalid_memory_query}"
    );

    let invalid_memory_topic = http_request(
        addr,
        "POST",
        "/memory",
        r#"{"topic":"   ","body":"body"}"#,
        &auth,
    );
    assert!(
        invalid_memory_topic.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_memory_topic}"
    );
    assert!(
        invalid_memory_topic.contains("topic must not be empty"),
        "{invalid_memory_topic}"
    );

    let invalid_memory_body = http_request(
        addr,
        "POST",
        "/memory",
        r#"{"topic":"topic","body":"   "}"#,
        &auth,
    );
    assert!(
        invalid_memory_body.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_memory_body}"
    );
    assert!(
        invalid_memory_body.contains("body must not be empty"),
        "{invalid_memory_body}"
    );

    let invalid_memory_tags = http_request(
        addr,
        "POST",
        "/memory",
        r#"{"topic":"topic","body":"body","tags":["api"," "]}"#,
        &auth,
    );
    assert!(
        invalid_memory_tags.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_memory_tags}"
    );
    assert!(
        invalid_memory_tags.contains("tags must not contain empty tags"),
        "{invalid_memory_tags}"
    );

    let delete_tool = http_request(addr, "DELETE", "/tools/api-printf", "", &auth);
    assert!(delete_tool.contains("HTTP/1.1 200 OK"), "{delete_tool}");

    wait_for_api_success(&mut child);
}

#[test]
fn api_run_endpoint_schedules_and_executes_work() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "run-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "12",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");
    let auth = [("authorization", "Bearer run-token")];

    let default_run = http_request(addr, "POST", "/run", "", &auth);
    assert!(default_run.contains("HTTP/1.1 200 OK"), "{default_run}");
    let default_run_body: Value =
        serde_json::from_str(http_body(&default_run)).expect("default run body");
    assert_eq!(default_run_body["dry_run"], false);
    assert_eq!(
        default_run_body["scheduler"]["assignments"]
            .as_array()
            .expect("default assignments")
            .len(),
        0
    );
    assert_eq!(
        default_run_body["runs"]
            .as_array()
            .expect("default runs")
            .len(),
        0
    );
    assert_eq!(
        default_run_body["errors"]
            .as_array()
            .expect("default errors")
            .len(),
        0
    );

    let agent = http_request(
        addr,
        "POST",
        "/agents",
        r#"{"name":"api-runner","capabilities":["rust"],"parallel":1}"#,
        &auth,
    );
    assert!(agent.contains("HTTP/1.1 201 Created"), "{agent}");

    let task = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"API run task","required_capabilities":["rust"],"command":"printf api-run"}"#,
        &auth,
    );
    assert!(task.contains("HTTP/1.1 201 Created"), "{task}");
    let task_body: Value = serde_json::from_str(http_body(&task)).expect("task body");
    let task_id = task_body["id"].as_str().expect("task id");

    let heartbeat = http_request(
        addr,
        "POST",
        "/agents/api-runner/heartbeat",
        r#"{"status":"online","lease_seconds":60}"#,
        &auth,
    );
    assert!(heartbeat.contains("HTTP/1.1 200 OK"), "{heartbeat}");

    let invalid_limit = http_request(addr, "POST", "/run", r#"{"limit":0}"#, &auth);
    assert!(
        invalid_limit.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_limit}"
    );
    assert!(
        invalid_limit.contains("limit must be greater than 0"),
        "{invalid_limit}"
    );

    let invalid_recovery = http_request(
        addr,
        "POST",
        "/run",
        r#"{"recover_stale_seconds":-1}"#,
        &auth,
    );
    assert!(
        invalid_recovery.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_recovery}"
    );
    assert!(
        invalid_recovery.contains("recover_stale_seconds must be greater than or equal to 0"),
        "{invalid_recovery}"
    );

    let invalid_dry_run_execute = http_request(
        addr,
        "POST",
        "/run",
        r#"{"dry_run":true,"execute":true}"#,
        &auth,
    );
    assert!(
        invalid_dry_run_execute.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_dry_run_execute}"
    );
    assert!(
        invalid_dry_run_execute.contains("dry_run cannot be combined with execute"),
        "{invalid_dry_run_execute}"
    );

    let dry_run = http_request(addr, "POST", "/run", r#"{"limit":1,"dry_run":true}"#, &auth);
    assert!(dry_run.contains("HTTP/1.1 200 OK"), "{dry_run}");
    assert!(dry_run.contains("\"dry_run\":true"), "{dry_run}");
    assert!(dry_run.contains(task_id), "{dry_run}");
    let pending_after_dry_run = http_get(addr, "/tasks?status=pending", &auth);
    assert!(
        pending_after_dry_run.contains(task_id),
        "{pending_after_dry_run}"
    );

    let run = http_request(addr, "POST", "/run", r#"{"limit":1,"execute":true}"#, &auth);
    assert!(run.contains("HTTP/1.1 200 OK"), "{run}");
    assert!(run.contains("\"dry_run\":false"), "{run}");
    assert!(run.contains("\"assignments\""), "{run}");
    assert!(run.contains("\"status\":\"success\""), "{run}");
    assert!(run.contains("\"errors\":[]"), "{run}");

    let successful_runs = http_get(addr, &format!("/runs?status=success&task={task_id}"), &auth);
    assert!(
        successful_runs.contains("HTTP/1.1 200 OK"),
        "{successful_runs}"
    );
    assert!(
        successful_runs.contains("printf api-run"),
        "{successful_runs}"
    );

    let delete_task_with_run =
        http_request(addr, "DELETE", &format!("/tasks/{task_id}"), "", &auth);
    assert!(
        delete_task_with_run.contains("HTTP/1.1 409 Conflict"),
        "{delete_task_with_run}"
    );
    assert!(
        delete_task_with_run.contains(
            "delete dependent tasks, remove referencing workflows, or prune referencing runs first"
        ),
        "{delete_task_with_run}"
    );

    wait_for_api_success(&mut child);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "show", task_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("API run task"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("success"))
        .stdout(predicate::str::contains("printf api-run"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "state", "prune", "--keep-runs", "0"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Removed 1 run"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "task", "delete", task_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("Deleted task"));
}

#[test]
fn api_run_endpoint_respects_expired_agent_lease() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_file = state.join("state.json");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "lease-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "5",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");
    let auth = [("authorization", "Bearer lease-token")];

    let agent = http_request(
        addr,
        "POST",
        "/agents",
        r#"{"name":"short-lease","capabilities":["lease-only"],"parallel":1}"#,
        &auth,
    );
    assert!(agent.contains("HTTP/1.1 201 Created"), "{agent}");

    let invalid_heartbeat = http_request(
        addr,
        "POST",
        "/agents/short-lease/heartbeat",
        r#"{"status":"online","lease_seconds":0}"#,
        &auth,
    );
    assert!(
        invalid_heartbeat.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_heartbeat}"
    );
    assert!(
        invalid_heartbeat.contains("lease_seconds must be greater than 0"),
        "{invalid_heartbeat}"
    );

    let heartbeat = http_request(
        addr,
        "POST",
        "/agents/short-lease/heartbeat",
        r#"{"status":"online","lease_seconds":60}"#,
        &auth,
    );
    assert!(heartbeat.contains("HTTP/1.1 200 OK"), "{heartbeat}");

    let task = http_request(
        addr,
        "POST",
        "/tasks",
        r#"{"title":"Lease task","required_capabilities":["lease-only"],"command":"printf lease"}"#,
        &auth,
    );
    assert!(task.contains("HTTP/1.1 201 Created"), "{task}");

    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    value["agents"]["short-lease"]["lease_expires_at"] = serde_json::json!("2000-01-01T00:00:00Z");
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write expired lease");

    let run = http_request(addr, "POST", "/run", r#"{"limit":1,"execute":true}"#, &auth);
    assert!(run.contains("HTTP/1.1 200 OK"), "{run}");
    assert!(run.contains("\"assignments\":[]"), "{run}");
    assert!(
        run.contains("\"expired_agents\":[\"short-lease\"]"),
        "{run}"
    );

    wait_for_api_success(&mut child);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "agent", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"status\": \"offline\""));
}

#[test]
fn api_exposes_run_logs_and_replay_with_explicit_missing_log_errors() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Run API-visible command",
            "--need",
            "rust",
            "--command",
            "printf api-log",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success();
    let run_id = first_run_id(state_arg);
    let outside_log = dir.path().join("api-outside.log");
    std::fs::write(&outside_log, "api-outside-secret").expect("outside log");
    let state_file = state.join("state.json");
    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    value["runs"][&run_id]["log_path"] = Value::from(outside_log.display().to_string());
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write untrusted run state");

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "15",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");

    let stdout = child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let logs = http_get(addr, &format!("/runs/{run_id}/logs"), &[]);
    assert!(logs.contains("HTTP/1.1 200 OK"), "{logs}");
    assert!(logs.contains("api-log"), "{logs}");
    assert!(logs.contains("\"truncated\":false"), "{logs}");
    assert!(!logs.contains("api-outside-secret"), "{logs}");

    let tailed_logs = http_get(addr, &format!("/runs/{run_id}/logs?tail_bytes=3"), &[]);
    assert!(tailed_logs.contains("HTTP/1.1 200 OK"), "{tailed_logs}");
    assert!(tailed_logs.contains("\"tail_bytes\":3"), "{tailed_logs}");
    assert!(tailed_logs.contains("\"truncated\":true"), "{tailed_logs}");
    assert!(tailed_logs.contains("\"body\":\"log\""), "{tailed_logs}");
    assert!(!tailed_logs.contains("api-"), "{tailed_logs}");

    let invalid_tail = http_get(addr, &format!("/runs/{run_id}/logs?tail_bytes=0"), &[]);
    assert!(
        invalid_tail.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_tail}"
    );
    assert!(
        invalid_tail.contains("tail_bytes must be greater than 0"),
        "{invalid_tail}"
    );

    let unknown_query = http_get(addr, &format!("/runs/{run_id}/logs?offset=1"), &[]);
    assert!(
        unknown_query.contains("HTTP/1.1 400 Bad Request"),
        "{unknown_query}"
    );
    assert!(
        unknown_query.contains("unsupported query parameter `offset`"),
        "{unknown_query}"
    );

    let replay = http_get(addr, &format!("/runs/{run_id}/replay"), &[]);
    assert!(replay.contains("HTTP/1.1 200 OK"), "{replay}");
    assert!(replay.contains("Run API-visible command"), "{replay}");
    assert!(replay.contains("api-log"), "{replay}");
    assert!(!replay.contains("api-outside-secret"), "{replay}");

    let tailed_replay = http_get(addr, &format!("/runs/{run_id}/replay?tail_bytes=3"), &[]);
    assert!(tailed_replay.contains("HTTP/1.1 200 OK"), "{tailed_replay}");
    assert!(
        tailed_replay.contains("\"log_tail_bytes\":3"),
        "{tailed_replay}"
    );
    assert!(
        tailed_replay.contains("\"log_truncated\":true"),
        "{tailed_replay}"
    );
    assert!(tailed_replay.contains("\"log\":\"log\""), "{tailed_replay}");
    assert!(
        !tailed_replay.contains("\"log\":\"api-log\""),
        "{tailed_replay}"
    );

    let debug = http_get(addr, &format!("/runs/{run_id}/debug?tail_bytes=3"), &[]);
    assert!(debug.contains("HTTP/1.1 200 OK"), "{debug}");
    assert!(debug.contains("\"artifact_status\""), "{debug}");
    assert!(debug.contains("\"diagnostics\""), "{debug}");
    assert!(debug.contains("\"log_tail_bytes\":3"), "{debug}");
    assert!(debug.contains("\"log\":\"log\""), "{debug}");
    assert!(debug.contains("\"agent\""), "{debug}");

    let artifacts = http_get(addr, &format!("/runs/{run_id}/artifacts"), &[]);
    assert!(artifacts.contains("HTTP/1.1 200 OK"), "{artifacts}");
    assert!(artifacts.contains("\"id\":\"stdout\""), "{artifacts}");
    assert!(artifacts.contains("\"checksum\":\"fnv1a64:"), "{artifacts}");

    let stdout_artifact = http_get(
        addr,
        &format!("/runs/{run_id}/artifacts/stdout?tail_bytes=3"),
        &[],
    );
    assert!(
        stdout_artifact.contains("HTTP/1.1 200 OK"),
        "{stdout_artifact}"
    );
    assert!(
        stdout_artifact.contains("\"artifact_id\":\"stdout\""),
        "{stdout_artifact}"
    );
    assert!(
        stdout_artifact.contains("\"tail_bytes\":3"),
        "{stdout_artifact}"
    );
    assert!(
        stdout_artifact.contains("\"body\":\"log\""),
        "{stdout_artifact}"
    );
    assert!(
        stdout_artifact.contains("\"checksum\":\"fnv1a64:"),
        "{stdout_artifact}"
    );

    let invalid_artifact_tail = http_get(
        addr,
        &format!("/runs/{run_id}/artifacts/stdout?tail_bytes=0"),
        &[],
    );
    assert!(
        invalid_artifact_tail.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_artifact_tail}"
    );
    assert!(
        invalid_artifact_tail.contains("tail_bytes must be greater than 0"),
        "{invalid_artifact_tail}"
    );

    let missing_artifact = http_get(addr, &format!("/runs/{run_id}/artifacts/missing"), &[]);
    assert!(
        missing_artifact.contains("HTTP/1.1 404 Not Found"),
        "{missing_artifact}"
    );
    assert!(
        missing_artifact.contains("run artifact not found"),
        "{missing_artifact}"
    );

    let invalid_debug_tail = http_get(addr, &format!("/runs/{run_id}/debug?tail_bytes=0"), &[]);
    assert!(
        invalid_debug_tail.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_debug_tail}"
    );
    assert!(
        invalid_debug_tail.contains("tail_bytes must be greater than 0"),
        "{invalid_debug_tail}"
    );

    let invalid_replay_tail = http_get(addr, &format!("/runs/{run_id}/replay?tail_bytes=0"), &[]);
    assert!(
        invalid_replay_tail.contains("HTTP/1.1 400 Bad Request"),
        "{invalid_replay_tail}"
    );
    assert!(
        invalid_replay_tail.contains("tail_bytes must be greater than 0"),
        "{invalid_replay_tail}"
    );

    std::fs::remove_file(state.join("runs").join(format!("{run_id}.log"))).expect("remove log");
    let missing_replay = http_get(addr, &format!("/runs/{run_id}/replay"), &[]);
    assert!(
        missing_replay.contains("HTTP/1.1 200 OK"),
        "{missing_replay}"
    );
    assert!(missing_replay.contains("\"log\":null"), "{missing_replay}");
    assert!(
        missing_replay.contains("\"log_error\":\"could not read run log:"),
        "{missing_replay}"
    );
    let missing_debug = http_get(addr, &format!("/runs/{run_id}/debug"), &[]);
    assert!(missing_debug.contains("HTTP/1.1 200 OK"), "{missing_debug}");
    assert!(missing_debug.contains("\"log\":null"), "{missing_debug}");
    assert!(
        missing_debug.contains("\"log_error\":\"could not read run log:"),
        "{missing_debug}"
    );

    wait_for_api_success(&mut child);
}

#[test]
fn runs_log_commands_fall_back_to_canonical_log_path() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Run legacy-log command",
            "--need",
            "rust",
            "--command",
            "printf legacy-log",
        ])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "run", "--execute"])
        .assert()
        .success();

    let run_id = first_run_id(state_arg);
    let state_file = state.join("state.json");
    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).expect("state body"))
            .expect("state json");
    let outside_log = dir.path().join("outside.log");
    std::fs::write(&outside_log, "outside-secret").expect("outside log");
    value["runs"][&run_id]["log_path"] = Value::from(outside_log.display().to_string());
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write untrusted run state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "logs", &run_id])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("legacy-log")
                .and(predicate::str::contains("outside-secret").not()),
        );

    value["runs"][&run_id]["log_path"] = Value::Null;
    std::fs::write(
        &state_file,
        serde_json::to_string_pretty(&value).expect("json body"),
    )
    .expect("write legacy run state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "logs", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("legacy-log"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "tail", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("legacy-log"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "runs",
            "tail",
            &run_id,
            "--tail-bytes",
            "3",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("log"))
        .stdout(predicate::str::contains("legacy-").not());

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "runs",
            "tail",
            &run_id,
            "--tail-bytes",
            "0",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "tail_bytes must be greater than 0",
        ));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "replay", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("legacy-log"));

    std::fs::remove_file(state.join("runs").join(format!("{run_id}.log"))).expect("remove run log");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "tail", &run_id])
        .assert()
        .failure()
        .stderr(predicate::str::contains("could not read run log"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "replay", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("log unavailable"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "runs", "replay", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"log_error\""));
}

#[test]
fn runs_cancel_stops_running_shell_command() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Cancel long command",
            "--need",
            "rust",
            "--command",
            "sleep 20",
        ])
        .assert()
        .success();

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args(["--state", state_arg, "run", "--execute"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn run");

    let run_id = wait_for_first_run_id(state_arg);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "cancel", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("Cancel requested"));

    wait_for_child_exit(&mut child);
    wait_for_run_status(state_arg, &run_id, "cancelled");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "logs", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("[cancelled]"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tasks_cancelled\": 1"));
}

#[test]
fn concurrent_cli_and_api_mutations_survive_running_execution() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Concurrent mutation command",
            "--need",
            "rust",
            "--command",
            "printf started; sleep 1; printf finished",
        ])
        .assert()
        .success();

    let mut run = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args(["--state", state_arg, "run", "--execute"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn run");
    let run_id = wait_for_first_run_id(state_arg);
    wait_for_run_tail_contains(state_arg, &run_id, "started");

    let mut api = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--max-requests",
            "1",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");
    let stdout = api.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let api_memory = http_request(
        addr,
        "POST",
        "/memory",
        r#"{"topic":"api-concurrent","body":"api mutation while run active","tags":["race"]}"#,
        &[],
    );
    assert!(api_memory.contains("HTTP/1.1 201 Created"), "{api_memory}");
    wait_for_api_success(&mut api);

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "memory",
            "add",
            "cli-concurrent",
            "cli mutation while run active",
            "--tag",
            "race",
        ])
        .assert()
        .success();

    wait_for_child_exit(&mut run);
    wait_for_run_status(state_arg, &run_id, "success");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "memory", "list", "--tag", "race"])
        .assert()
        .success()
        .stdout(predicate::str::contains("api-concurrent"))
        .stdout(predicate::str::contains("cli-concurrent"));

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "logs", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("finished"));
}

#[test]
fn runs_tail_reads_shell_log_before_completion() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "Tail live command",
            "--need",
            "rust",
            "--command",
            "printf live-log; sleep 20",
        ])
        .assert()
        .success();

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args(["--state", state_arg, "run", "--execute"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn run");

    let run_id = wait_for_first_run_id(state_arg);
    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "runs",
            "tail",
            &run_id,
            "--interval-ms",
            "0",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "interval_ms must be greater than 0",
        ));
    wait_for_run_tail_contains(state_arg, &run_id, "live-log");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "runs", "cancel", &run_id])
        .assert()
        .success();
    wait_for_child_exit(&mut child);
}

#[test]
fn api_can_cancel_running_shell_command() {
    let dir = workspace_tempdir().expect("tempdir");
    let state = dir.path().join("agent-os");
    let state_arg = state.to_str().expect("state");

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "init", "--force", "--profile", "dev"])
        .assert()
        .success();

    Command::cargo_bin("agent-os")
        .expect("binary")
        .args([
            "--state",
            state_arg,
            "task",
            "create",
            "API cancel long command",
            "--need",
            "rust",
            "--command",
            "sleep 20",
        ])
        .assert()
        .success();

    let mut run_child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .args(["--state", state_arg, "run", "--execute"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn run");
    let run_id = wait_for_first_run_id(state_arg);

    let mut api_child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "cancel-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "1",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");
    let stdout = api_child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");

    let response = http_request(
        addr,
        "POST",
        &format!("/runs/{run_id}/cancel"),
        "{}",
        &[("authorization", "Bearer cancel-token")],
    );
    assert!(response.contains("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains("\"cancel_requested\":true"), "{response}");

    wait_for_api_success(&mut api_child);

    wait_for_child_exit(&mut run_child);
    wait_for_run_status(state_arg, &run_id, "cancelled");

    let mut api_child = std::process::Command::new(assert_cmd::cargo::cargo_bin("agent-os"))
        .env("AGENT_OS_API_TOKEN", "cancel-token")
        .args([
            "--state",
            state_arg,
            "api",
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--token-env",
            "AGENT_OS_API_TOKEN",
            "--max-requests",
            "1",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn api");
    let stdout = api_child.stdout.take().expect("api stdout");
    let mut stdout = BufReader::new(stdout);
    let mut listening = String::new();
    stdout
        .read_line(&mut listening)
        .expect("read api listening line");
    let addr = listening
        .trim()
        .strip_prefix("API listening on http://")
        .expect("listening prefix")
        .parse::<SocketAddr>()
        .expect("listening addr");
    let terminal_cancel = http_request(
        addr,
        "POST",
        &format!("/runs/{run_id}/cancel"),
        "{}",
        &[("authorization", "Bearer cancel-token")],
    );
    assert!(
        terminal_cancel.contains("HTTP/1.1 409 Conflict"),
        "{terminal_cancel}"
    );
    assert!(
        terminal_cancel.contains("\"cancel_requested\":false"),
        "{terminal_cancel}"
    );

    wait_for_api_success(&mut api_child);
}

fn connect_with_retry(addr: SocketAddr) -> TcpStream {
    let start = Instant::now();
    loop {
        match TcpStream::connect(addr) {
            Ok(stream) => return stream,
            Err(error) if start.elapsed() < Duration::from_secs(3) => {
                let _ = error;
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => panic!("api did not accept connections at {addr}: {error}"),
        }
    }
}

fn http_get(addr: SocketAddr, path: &str, headers: &[(&str, &str)]) -> String {
    http_request(addr, "GET", path, "", headers)
}

fn http_raw_request(addr: SocketAddr, request: &str) -> String {
    let mut stream = connect_with_retry(addr);
    stream.write_all(request.as_bytes()).expect("write request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
    response
}

fn http_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: &str,
    headers: &[(&str, &str)],
) -> String {
    let mut stream = connect_with_retry(addr);
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\ncontent-type: application/json\r\n",
        body.len()
    );
    for (key, value) in headers {
        request.push_str(key);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream.write_all(request.as_bytes()).expect("write request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
    response
}

fn http_body(response: &str) -> &str {
    response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .expect("http body")
}

fn wait_for_api_success(child: &mut Child) {
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                assert!(status.success(), "api process failed with {status}");
                return;
            }
            Ok(None) if start.elapsed() < Duration::from_secs(5) => {
                thread::sleep(Duration::from_millis(20));
            }
            Ok(None) => {
                let _ = child.kill();
                let status = child.wait().expect("api wait after kill");
                panic!(
                    "api process did not exit within 5s after expected requests; killed with {status}"
                );
            }
            Err(error) => panic!("api wait failed: {error}"),
        }
    }
}

fn wait_for_daemon_status(state_arg: &str, expected: &str) {
    let start = Instant::now();
    loop {
        let output = Command::cargo_bin("agent-os")
            .expect("binary")
            .args(["--state", state_arg, "daemon", "status"])
            .output()
            .expect("daemon status");
        let stdout = String::from_utf8_lossy(&output.stdout);
        if output.status.success() && stdout.contains(expected) {
            return;
        }
        if start.elapsed() > Duration::from_secs(3) {
            panic!("daemon status did not contain {expected}; last stdout: {stdout}");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_first_run_id(state_arg: &str) -> String {
    let start = Instant::now();
    loop {
        let output = Command::cargo_bin("agent-os")
            .expect("binary")
            .args(["--state", state_arg, "--json", "runs", "list"])
            .output()
            .expect("runs list");
        if output.status.success() {
            let runs: Value = serde_json::from_slice(&output.stdout).expect("runs json");
            if let Some(run_id) = runs
                .as_array()
                .and_then(|runs| runs.first())
                .and_then(|run| run["id"].as_str())
            {
                return run_id.to_owned();
            }
        }
        if start.elapsed() > Duration::from_secs(3) {
            panic!("run did not start");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_run_status(state_arg: &str, run_id: &str, expected: &str) {
    let start = Instant::now();
    loop {
        let output = Command::cargo_bin("agent-os")
            .expect("binary")
            .args(["--state", state_arg, "--json", "runs", "show", run_id])
            .output()
            .expect("run show");
        if output.status.success() {
            let run: Value = serde_json::from_slice(&output.stdout).expect("run json");
            if run["status"].as_str() == Some(expected) {
                return;
            }
        }
        if start.elapsed() > Duration::from_secs(3) {
            panic!("run {run_id} did not reach status {expected}");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_run_tail_contains(state_arg: &str, run_id: &str, expected: &str) {
    let start = Instant::now();
    loop {
        let output = Command::cargo_bin("agent-os")
            .expect("binary")
            .args(["--state", state_arg, "runs", "tail", run_id])
            .output()
            .expect("run tail");
        let stdout = String::from_utf8_lossy(&output.stdout);
        if output.status.success() && stdout.contains(expected) {
            return;
        }
        if start.elapsed() > Duration::from_secs(3) {
            panic!("run {run_id} tail did not contain {expected}; last stdout: {stdout}");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_child_exit(child: &mut std::process::Child) {
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("try wait child") {
            assert!(status.success(), "child failed with {status}");
            return;
        }
        if start.elapsed() > Duration::from_secs(5) {
            let _ = child.kill();
            panic!("child did not exit after cancellation");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn first_run_id(state_arg: &str) -> String {
    let output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "runs", "list"])
        .output()
        .expect("runs list");
    assert!(output.status.success());
    let runs: Value = serde_json::from_slice(&output.stdout).expect("runs json");
    runs[0]["id"].as_str().expect("run id").to_owned()
}

fn first_task_id(state_arg: &str) -> String {
    let output = Command::cargo_bin("agent-os")
        .expect("binary")
        .args(["--state", state_arg, "--json", "task", "list", "--all"])
        .output()
        .expect("task list");
    assert!(output.status.success());
    let tasks: Value = serde_json::from_slice(&output.stdout).expect("tasks json");
    tasks[0]["id"].as_str().expect("task id").to_owned()
}

fn documented_readme_api_operations(readme: &str) -> BTreeSet<(String, String)> {
    let mut in_api_section = false;
    let mut in_code_block = false;
    let mut operations = BTreeSet::new();
    for line in readme.lines() {
        if line.starts_with("The local API serves JSON") {
            in_api_section = true;
            continue;
        }
        if !in_api_section {
            continue;
        }
        if line.starts_with("```") {
            if in_code_block {
                break;
            }
            in_code_block = true;
            continue;
        }
        if !in_code_block {
            continue;
        }
        let Some((method, path)) = parse_readme_api_operation(line) else {
            continue;
        };
        operations.insert((method, path));
    }
    assert!(
        !operations.is_empty(),
        "README local API endpoint list should not be empty"
    );
    operations
}

fn parse_readme_api_operation(line: &str) -> Option<(String, String)> {
    let mut parts = line.split_whitespace();
    let method = parts.next()?;
    if !matches!(method, "GET" | "POST" | "DELETE") {
        return None;
    }
    let path = parts.next()?.split('?').next()?;
    Some((method.to_owned(), normalize_readme_api_path(path)))
}

fn normalize_readme_api_path(path: &str) -> String {
    path.split('/')
        .map(|segment| {
            if segment.ends_with("_ID") {
                "{id}"
            } else {
                segment
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn openapi_documented_operations(schema: &Value) -> BTreeSet<(String, String)> {
    let paths = schema["paths"].as_object().expect("OpenAPI paths");
    let mut operations = BTreeSet::new();
    for (path, endpoint) in paths {
        let methods = endpoint.as_object().expect("OpenAPI path item");
        for method in methods.keys() {
            if matches!(method.as_str(), "get" | "post" | "delete") {
                operations.insert((method.to_ascii_uppercase(), path.to_owned()));
            }
        }
    }
    operations
}

fn documented_readme_top_level_commands(readme: &str) -> BTreeSet<String> {
    let mut in_commands_block = false;
    let mut in_code_block = false;
    let mut commands = BTreeSet::new();
    for line in readme.lines() {
        if line == "## Commands" {
            in_commands_block = true;
            continue;
        }
        if !in_commands_block {
            continue;
        }
        if line.starts_with("```") {
            if in_code_block {
                break;
            }
            in_code_block = true;
            continue;
        }
        if !in_code_block {
            continue;
        }
        let Some(command) = line
            .strip_prefix("agent-os ")
            .and_then(|command| command.split_whitespace().next())
        else {
            continue;
        };
        commands.insert(command.to_owned());
    }
    assert!(
        !commands.is_empty(),
        "README command synopsis list should not be empty"
    );
    commands
}

fn documented_readme_subcommands(readme: &str, command: &str) -> BTreeSet<String> {
    let mut commands = BTreeSet::new();
    let prefix = format!("agent-os {command} ");
    for line in readme_command_synopsis_lines(readme) {
        let Some(rest) = line.strip_prefix(&prefix) else {
            continue;
        };
        let Some(subcommand) = rest.split_whitespace().next() else {
            continue;
        };
        commands.insert(subcommand.to_owned());
    }
    assert!(
        !commands.is_empty(),
        "README command synopsis list for `{command}` should not be empty"
    );
    commands
}

fn documented_readme_completion_shells(readme: &str) -> BTreeSet<String> {
    let prefix = "agent-os completions ";
    let Some(shells) = readme_command_synopsis_lines(readme)
        .into_iter()
        .find_map(|line| line.strip_prefix(prefix))
    else {
        panic!("README command synopsis should document `{prefix}<SHELL>`");
    };
    let shells = shells
        .split('|')
        .map(str::trim)
        .filter(|shell| !shell.is_empty())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    assert!(
        !shells.is_empty(),
        "README completions synopsis should list supported shells"
    );
    shells
}

fn assert_manifest_string_array_contains(
    package: &toml::map::Map<String, toml::Value>,
    field: &str,
    expected: &str,
) {
    let values = package[field]
        .as_array()
        .unwrap_or_else(|| panic!("Cargo.toml package.{field} should be an array"));
    assert!(
        values.iter().any(|value| value.as_str() == Some(expected)),
        "Cargo.toml package.{field} should contain {expected}"
    );
}

fn readme_command_synopsis_lines(readme: &str) -> Vec<&str> {
    let mut in_commands_block = false;
    let mut in_code_block = false;
    let mut lines = Vec::new();
    for line in readme.lines() {
        if line == "## Commands" {
            in_commands_block = true;
            continue;
        }
        if !in_commands_block {
            continue;
        }
        if line.starts_with("```") {
            if in_code_block {
                break;
            }
            in_code_block = true;
            continue;
        }
        if in_code_block {
            lines.push(line);
        }
    }
    lines
}

fn readme_synopsis_command_path(synopsis: &str) -> Vec<String> {
    let mut parts = synopsis.split_whitespace();
    assert_eq!(parts.next(), Some("agent-os"), "README synopsis prefix");
    let mut command_path = Vec::new();
    for part in parts {
        let part = part.trim_matches(|character| character == '[' || character == ']');
        if part.starts_with("--")
            || part.contains('|')
            || part.chars().any(|character| character.is_ascii_uppercase())
        {
            break;
        }
        command_path.push(part.to_owned());
        if command_path.len() == 2 {
            break;
        }
    }
    assert!(
        !command_path.is_empty(),
        "README synopsis should include a command path: {synopsis}"
    );
    command_path
}

fn documented_long_options_from_synopsis(synopsis: &str) -> BTreeSet<String> {
    synopsis
        .split_whitespace()
        .filter_map(|part| {
            let option = part.trim_matches(|character| character == '[' || character == ']');
            option.starts_with("--").then(|| option.to_owned())
        })
        .collect()
}

fn top_level_commands_from_help(help: &str) -> BTreeSet<String> {
    commands_from_help(help)
}

fn global_long_options_from_help(help: &str) -> BTreeSet<String> {
    let options = long_options_from_help(help);
    assert!(
        !options.is_empty(),
        "CLI global option list should not be empty"
    );
    options
}

fn local_long_options_from_help(help: &str) -> BTreeSet<String> {
    long_options_from_help(help)
        .into_iter()
        .filter(|option| !matches!(option.as_str(), "--state" | "--json" | "--config"))
        .collect()
}

fn long_options_from_help(help: &str) -> BTreeSet<String> {
    let mut in_options = false;
    let mut options = BTreeSet::new();
    for line in help.lines() {
        if line == "Options:" {
            in_options = true;
            continue;
        }
        if !in_options {
            continue;
        }
        let trimmed = line.trim_start();
        if trimmed.is_empty() || !trimmed.starts_with('-') {
            continue;
        }
        for part in trimmed.split_whitespace() {
            let option = part.trim_end_matches(',');
            if matches!(option, "--help" | "--version") {
                break;
            }
            if option.starts_with("--") {
                options.insert(option.to_owned());
                break;
            }
            if !option.starts_with('-') {
                break;
            }
        }
    }
    options
}

fn possible_values_from_help(help: &str, argument: &str) -> BTreeSet<String> {
    for line in help.lines() {
        let line = line.trim_start();
        if !line.starts_with(argument) {
            continue;
        }
        let Some(values) = line
            .split_once("[possible values: ")
            .and_then(|(_, rest)| rest.split_once(']').map(|(values, _)| values))
        else {
            continue;
        };
        let values = values
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect::<BTreeSet<_>>();
        assert!(
            !values.is_empty(),
            "CLI help possible values for `{argument}` should not be empty"
        );
        return values;
    }
    panic!("CLI help should document possible values for `{argument}`");
}

fn commands_from_help(help: &str) -> BTreeSet<String> {
    let mut in_commands = false;
    let mut commands = BTreeSet::new();
    for line in help.lines() {
        if line == "Commands:" {
            in_commands = true;
            continue;
        }
        if !in_commands {
            continue;
        }
        if line.trim().is_empty() {
            break;
        }
        let Some(command) = line.split_whitespace().next() else {
            continue;
        };
        if command != "help" {
            commands.insert(command.to_owned());
        }
    }
    assert!(
        !commands.is_empty(),
        "CLI help command list should not be empty"
    );
    commands
}

fn assert_schema_refs_resolve(schema: &Value) {
    let mut refs = Vec::new();
    collect_schema_refs(schema, &mut refs);
    for reference in refs {
        let Some(name) = reference.strip_prefix("#/components/schemas/") else {
            panic!("unsupported OpenAPI ref: {reference}");
        };
        assert!(
            schema["components"]["schemas"][name].is_object(),
            "missing OpenAPI schema component for {reference}"
        );
    }
}

fn collect_schema_refs(value: &Value, refs: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                refs.push(reference.to_owned());
            }
            for value in object.values() {
                collect_schema_refs(value, refs);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_schema_refs(value, refs);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn assert_optional_bearer_security_declared(schema: &Value) {
    assert_eq!(
        schema["components"]["securitySchemes"]["bearerAuth"]["type"],
        "http"
    );
    assert_eq!(
        schema["components"]["securitySchemes"]["bearerAuth"]["scheme"],
        "bearer"
    );
    assert_eq!(
        schema["security"],
        serde_json::json!([{}, { "bearerAuth": [] }])
    );
}

fn assert_component_schemas_are_reachable(schema: &Value) {
    let schemas = schema["components"]["schemas"]
        .as_object()
        .expect("OpenAPI schemas");
    let mut reachable = BTreeSet::new();
    collect_reachable_component_schemas(&schema["paths"], schemas, &mut reachable);

    let mut unreachable = schemas
        .keys()
        .filter(|schema_name| !reachable.contains(*schema_name))
        .cloned()
        .collect::<Vec<_>>();
    unreachable.sort();
    assert!(
        unreachable.is_empty(),
        "OpenAPI component schemas are not reachable from paths: {unreachable:?}"
    );
}

fn collect_reachable_component_schemas(
    value: &Value,
    schemas: &serde_json::Map<String, Value>,
    reachable: &mut BTreeSet<String>,
) {
    let mut refs = Vec::new();
    collect_schema_refs(value, &mut refs);
    for reference in refs {
        let Some(schema_name) = reference.strip_prefix("#/components/schemas/") else {
            continue;
        };
        if !reachable.insert(schema_name.to_owned()) {
            continue;
        }
        let component = schemas
            .get(schema_name)
            .unwrap_or_else(|| panic!("missing OpenAPI schema component for {reference}"));
        collect_reachable_component_schemas(component, schemas, reachable);
    }
}

fn assert_operations_and_responses_have_descriptions(schema: &Value) {
    let paths = schema["paths"].as_object().expect("OpenAPI paths");
    for (path, endpoint) in paths {
        let operations = endpoint.as_object().expect("OpenAPI path item");
        for (method, operation) in operations {
            if !matches!(method.as_str(), "get" | "post" | "delete" | "options") {
                continue;
            }
            assert!(
                operation["summary"]
                    .as_str()
                    .is_some_and(|summary| !summary.trim().is_empty()),
                "{method} {path} missing operation summary"
            );
            let responses = operation["responses"]
                .as_object()
                .unwrap_or_else(|| panic!("{method} {path} missing responses"));
            for (status, response) in responses {
                assert!(
                    response["description"]
                        .as_str()
                        .is_some_and(|description| !description.trim().is_empty()),
                    "{method} {path} response {status} missing response description"
                );
            }
        }
    }
}

fn assert_parameters_are_documented(schema: &Value) {
    let paths = schema["paths"].as_object().expect("OpenAPI paths");
    for (path, endpoint) in paths {
        let operations = endpoint.as_object().expect("OpenAPI path item");
        for (method, operation) in operations {
            if !matches!(method.as_str(), "get" | "post" | "delete" | "options") {
                continue;
            }
            let Some(parameters) = operation.get("parameters").and_then(Value::as_array) else {
                continue;
            };
            for parameter in parameters {
                let name = parameter["name"]
                    .as_str()
                    .filter(|name| !name.trim().is_empty())
                    .unwrap_or_else(|| panic!("{method} {path} parameter missing name"));
                let location = parameter["in"]
                    .as_str()
                    .unwrap_or_else(|| panic!("{method} {path} parameter {name} missing location"));
                assert!(
                    matches!(location, "path" | "query"),
                    "{method} {path} parameter {name} has unsupported location {location}"
                );
                assert!(
                    parameter["description"]
                        .as_str()
                        .is_some_and(|description| !description.trim().is_empty()),
                    "{method} {path} parameter {name} missing description"
                );
                assert!(
                    parameter["schema"].is_object(),
                    "{method} {path} parameter {name} missing schema"
                );
                let required = parameter["required"].as_bool().unwrap_or_else(|| {
                    panic!("{method} {path} parameter {name} missing required flag")
                });
                if location == "path" {
                    assert!(
                        required,
                        "{method} {path} parameter {name} path parameter must be required"
                    );
                }
            }
        }
    }
}

fn assert_path_parameters_declared(schema: &Value) {
    let paths = schema["paths"].as_object().expect("OpenAPI paths");
    for (path, endpoint) in paths {
        let expected = path_template_parameters(path);
        if expected.is_empty() {
            continue;
        }
        let operations = endpoint.as_object().expect("OpenAPI path item");
        for (method, operation) in operations {
            if !matches!(method.as_str(), "get" | "post" | "delete" | "options") {
                continue;
            }
            let parameters = operation["parameters"]
                .as_array()
                .unwrap_or_else(|| panic!("{method} {path} missing parameters"));
            for name in &expected {
                assert!(
                    parameters.iter().any(|parameter| {
                        parameter["name"].as_str() == Some(name.as_str())
                            && parameter["in"].as_str() == Some("path")
                    }),
                    "{method} {path} missing path parameter {name}"
                );
            }
        }
    }
}

fn assert_query_parameters_match_runtime_filters(schema: &Value) {
    let expected = [
        ("/events", &["limit", "kind", "since", "until", "query"][..]),
        (
            "/agents",
            &[
                "status",
                "kind",
                "capability",
                "since",
                "until",
                "query",
                "limit",
            ][..],
        ),
        (
            "/tasks",
            &[
                "status",
                "priority",
                "agent",
                "tool",
                "after",
                "capability",
                "since",
                "until",
                "query",
                "limit",
            ][..],
        ),
        (
            "/workflows",
            &["priority", "task", "since", "until", "query", "limit"][..],
        ),
        (
            "/workers",
            &["status", "since", "until", "query", "limit"][..],
        ),
        (
            "/evals",
            &["target", "success", "since", "until", "query", "limit"][..],
        ),
        ("/git/status", &["cwd"][..]),
        ("/secrets", &["kind", "query", "limit"][..]),
        (
            "/tools",
            &["kind", "capability", "since", "until", "query", "limit"][..],
        ),
        (
            "/memory",
            &[
                "query",
                "tag",
                "visibility",
                "scope",
                "since",
                "until",
                "limit",
            ][..],
        ),
        (
            "/memory/recall",
            &[
                "query",
                "tag",
                "visibility",
                "scope",
                "since",
                "until",
                "limit",
            ][..],
        ),
        (
            "/runs",
            &[
                "status", "task", "agent", "since", "until", "query", "limit",
            ][..],
        ),
        ("/runs/{id}/logs", &["tail_bytes"][..]),
        ("/runs/{id}/replay", &["tail_bytes"][..]),
        ("/runs/{id}/debug", &["tail_bytes"][..]),
        ("/runs/{id}/artifacts", &[][..]),
        ("/runs/{id}/artifacts/{artifact_id}", &["tail_bytes"][..]),
    ];
    let paths = schema["paths"].as_object().expect("OpenAPI paths");
    for (path, endpoint) in paths {
        let Some(operation) = endpoint.get("get") else {
            continue;
        };
        let actual = query_parameter_names(operation);
        let expected = expected
            .iter()
            .find_map(|(expected_path, parameters)| {
                (*expected_path == path).then(|| {
                    parameters
                        .iter()
                        .map(|parameter| (*parameter).to_owned())
                        .collect::<Vec<_>>()
                })
            })
            .unwrap_or_default();
        assert_eq!(
            actual, expected,
            "OpenAPI query parameters drifted for GET {path}"
        );
    }
}

fn query_parameter_names(operation: &Value) -> Vec<String> {
    operation
        .get("parameters")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|parameter| parameter["in"].as_str() == Some("query"))
        .map(|parameter| {
            parameter["name"]
                .as_str()
                .expect("query parameter name")
                .to_owned()
        })
        .collect()
}

fn assert_request_bodies_match_http_methods(schema: &Value) {
    let paths = schema["paths"].as_object().expect("OpenAPI paths");
    let mut bodyless_posts = BTreeSet::new();
    for (path, endpoint) in paths {
        let operations = endpoint.as_object().expect("OpenAPI path item");
        for (method, operation) in operations {
            if !matches!(method.as_str(), "get" | "post" | "delete" | "options") {
                continue;
            }
            let has_request_body = operation.get("requestBody").is_some();
            if method == "post" {
                if !has_request_body {
                    bodyless_posts.insert(path.to_owned());
                }
                continue;
            }
            assert!(
                !has_request_body,
                "{method} {path} should not declare a request body"
            );
        }
    }

    assert_eq!(
        bodyless_posts,
        BTreeSet::from(["/daemon/stop".to_owned(), "/runs/{id}/cancel".to_owned()])
    );
}

fn assert_success_responses_have_json_schemas(schema: &Value) {
    let paths = schema["paths"].as_object().expect("OpenAPI paths");
    for (path, endpoint) in paths {
        let operations = endpoint.as_object().expect("OpenAPI path item");
        for (method, operation) in operations {
            if matches!(path.as_str(), "/metrics/prometheus" | "/dashboard.html") {
                continue;
            }
            if !matches!(method.as_str(), "get" | "post" | "delete") {
                continue;
            }
            let responses = operation["responses"]
                .as_object()
                .unwrap_or_else(|| panic!("{method} {path} missing responses"));
            for status in ["200", "201"] {
                let Some(response) = responses.get(status) else {
                    continue;
                };
                assert!(
                    response["content"]["application/json"]["schema"].is_object(),
                    "{method} {path} response {status} missing JSON schema"
                );
            }
        }
    }
}

fn assert_error_responses_have_json_schemas(schema: &Value) {
    let paths = schema["paths"].as_object().expect("OpenAPI paths");
    for (path, endpoint) in paths {
        let operations = endpoint.as_object().expect("OpenAPI path item");
        for (method, operation) in operations {
            if matches!(path.as_str(), "/metrics/prometheus" | "/dashboard.html") {
                continue;
            }
            if !matches!(method.as_str(), "get" | "post" | "delete") {
                continue;
            }
            let responses = operation["responses"]
                .as_object()
                .unwrap_or_else(|| panic!("{method} {path} missing responses"));
            for (status, response) in responses {
                if !matches!(status.as_bytes().first(), Some(b'4' | b'5')) {
                    continue;
                }
                assert!(
                    response["content"]["application/json"]["schema"].is_object(),
                    "{method} {path} response {status} missing JSON error schema"
                );
            }
        }
    }
}

fn assert_mutation_operations_declare_unsupported_media_type(schema: &Value) {
    let paths = schema["paths"].as_object().expect("OpenAPI paths");
    for (path, endpoint) in paths {
        let operations = endpoint.as_object().expect("OpenAPI path item");
        for method in ["post", "delete"] {
            let Some(operation) = operations.get(method) else {
                continue;
            };
            let response = &operation["responses"]["415"];
            assert_eq!(
                response["description"], "Unsupported media type",
                "{method} {path} missing unsupported media type response"
            );
            assert_eq!(
                response["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/ErrorResponse",
                "{method} {path} 415 response should use ErrorResponse"
            );
        }
    }
}

fn assert_create_post_success_statuses(schema: &Value) {
    for path in [
        "/init",
        "/config",
        "/agents",
        "/tasks",
        "/workflows",
        "/tools",
        "/memory",
    ] {
        let responses = schema["paths"][path]["post"]["responses"]
            .as_object()
            .unwrap_or_else(|| panic!("POST {path} missing responses"));
        assert!(
            responses.contains_key("201"),
            "POST {path} missing created response"
        );
        assert!(
            !responses.contains_key("200"),
            "POST {path} advertises 200 despite returning 201"
        );
    }
}

fn assert_content_uses_only_application_json(schema: &Value) {
    let paths = schema["paths"].as_object().expect("OpenAPI paths");
    for (path, endpoint) in paths {
        let operations = endpoint.as_object().expect("OpenAPI path item");
        for (method, operation) in operations {
            if !matches!(method.as_str(), "get" | "post" | "delete" | "options") {
                continue;
            }
            if let Some(request_body) = operation.get("requestBody") {
                assert_content_media_types(
                    &request_body["content"],
                    &format!("{method} {path} request body"),
                );
            }
            if let Some(responses) = operation.get("responses").and_then(Value::as_object) {
                for (status, response) in responses {
                    assert_content_media_types(
                        &response["content"],
                        &format!("{method} {path} response {status}"),
                    );
                }
            }
        }
    }
}

fn assert_content_media_types(content: &Value, context: &str) {
    let Some(content) = content.as_object() else {
        return;
    };
    let media_types = content.keys().cloned().collect::<BTreeSet<_>>();
    if context.starts_with("get /metrics/prometheus response ")
        && media_types == BTreeSet::from(["text/plain; version=0.0.4".to_owned()])
    {
        return;
    }
    if context.starts_with("get /dashboard.html response ")
        && media_types == BTreeSet::from(["text/html".to_owned()])
    {
        return;
    }
    assert!(
        media_types.is_empty() || media_types == BTreeSet::from(["application/json".to_owned()]),
        "{context} advertises unsupported media types: {media_types:?}"
    );
}

fn assert_json_content_entries_have_schemas(schema: &Value) {
    let paths = schema["paths"].as_object().expect("OpenAPI paths");
    for (path, endpoint) in paths {
        let operations = endpoint.as_object().expect("OpenAPI path item");
        for (method, operation) in operations {
            if !matches!(method.as_str(), "get" | "post" | "delete" | "options") {
                continue;
            }
            if let Some(request_body) = operation.get("requestBody") {
                assert_json_content_schema(
                    &request_body["content"],
                    &format!("{method} {path} request body"),
                );
            }
            if let Some(responses) = operation.get("responses").and_then(Value::as_object) {
                for (status, response) in responses {
                    assert_json_content_schema(
                        &response["content"],
                        &format!("{method} {path} response {status}"),
                    );
                }
            }
        }
    }
}

fn assert_request_body_requirements(schema: &Value) {
    let paths = schema["paths"].as_object().expect("OpenAPI paths");
    let mut actual = Vec::new();
    for (path, endpoint) in paths {
        let operations = endpoint.as_object().expect("OpenAPI path item");
        for method in ["get", "post", "delete"] {
            let Some(operation) = operations.get(method) else {
                continue;
            };
            let Some(request_body) = operation.get("requestBody") else {
                continue;
            };
            actual.push((
                path.to_owned(),
                method.to_owned(),
                request_body["required"]
                    .as_bool()
                    .unwrap_or_else(|| panic!("{method} {path} requestBody.required missing")),
            ));
        }
    }
    actual.sort();

    let mut expected = [
        ("/agents", "post", true),
        ("/agents/{id}", "post", true),
        ("/agents/{id}/claim", "post", false),
        ("/agents/{id}/heartbeat", "post", false),
        ("/approvals/{id}/approve", "post", false),
        ("/approvals/{id}/deny", "post", false),
        ("/config", "post", false),
        ("/evals", "post", true),
        ("/evals/run", "post", true),
        ("/git/review-task", "post", false),
        ("/init", "post", false),
        ("/memory", "post", true),
        ("/memory/prune", "post", false),
        ("/memory/{id}", "post", true),
        ("/registry/mcp-servers", "post", true),
        ("/registry/mcp-servers/{id}", "post", true),
        ("/registry/marketplace-import", "post", true),
        ("/registry/profiles/{id}/agents", "post", false),
        ("/registry/templates/{id}/workflows", "post", true),
        ("/run", "post", false),
        ("/secrets", "post", true),
        ("/service/launchd", "post", false),
        ("/service/launchd/install", "post", false),
        ("/service/launchd/start", "post", false),
        ("/service/launchd/status", "post", false),
        ("/service/launchd/stop", "post", false),
        ("/service/launchd/uninstall", "post", false),
        ("/service/systemd", "post", false),
        ("/service/systemd/install", "post", false),
        ("/service/systemd/start", "post", false),
        ("/service/systemd/status", "post", false),
        ("/service/systemd/stop", "post", false),
        ("/service/systemd/uninstall", "post", false),
        ("/state/backup", "post", false),
        ("/state/export", "post", true),
        ("/state/import", "post", true),
        ("/state/migrate", "post", false),
        ("/state/prune", "post", false),
        ("/state/repair", "post", false),
        ("/state/sqlite", "post", false),
        ("/tasks", "post", true),
        ("/tasks/recover", "post", false),
        ("/tasks/{id}", "post", true),
        ("/tasks/{id}/assign", "post", true),
        ("/tasks/{id}/block", "post", false),
        ("/tasks/{id}/cancel", "post", false),
        ("/tasks/{id}/complete", "post", false),
        ("/tasks/{id}/dependencies", "post", true),
        ("/tasks/{id}/fail", "post", false),
        ("/tasks/{id}/plan", "post", true),
        ("/tasks/{id}/priority", "post", true),
        ("/tasks/{id}/retry", "post", false),
        ("/tasks/{id}/unblock", "post", false),
        ("/tools", "post", true),
        ("/tools/{id}", "post", true),
        ("/workers", "post", true),
        ("/workers/{id}/claim", "post", false),
        ("/workers/{id}/heartbeat", "post", false),
        ("/workers/{id}/report", "post", true),
        ("/workflows", "post", true),
        ("/workflows/{id}/cancel", "post", false),
        ("/workflows/{id}/link", "post", true),
        ("/workflows/{id}/pause", "post", false),
        ("/workflows/{id}/resume", "post", false),
        ("/workflows/{id}/retry", "post", false),
        ("/workflows/{id}/run", "post", false),
        ("/workflows/{id}/tasks", "post", true),
        ("/workflows/{id}/unlink", "post", true),
    ]
    .into_iter()
    .map(|(path, method, required)| (path.to_owned(), method.to_owned(), required))
    .collect::<Vec<_>>();
    expected.sort();

    assert_eq!(actual, expected);
}

fn assert_component_object_schemas_are_closed(schema: &Value) {
    let schemas = schema["components"]["schemas"]
        .as_object()
        .expect("OpenAPI schemas");
    for (schema_name, schema_value) in schemas {
        if schema_name == "OpenApiDocument" {
            assert_eq!(
                schema_value["additionalProperties"], true,
                "OpenApiDocument should remain an intentionally loose recursive schema"
            );
            continue;
        }
        if schema_value["type"] == "object" {
            assert_eq!(
                schema_value["additionalProperties"], false,
                "{schema_name} should explicitly reject undeclared object properties"
            );
        }
    }
}

fn assert_non_request_object_schemas_require_declared_properties(schema: &Value) {
    let schemas = schema["components"]["schemas"]
        .as_object()
        .expect("OpenAPI schemas");
    for (schema_name, schema_value) in schemas {
        if schema_name == "ErrorResponse"
            || schema_name == "OpenApiDocument"
            || schema_name.ends_with("Request")
            || schema_name.ends_with("Config")
            || schema_value["type"] != "object"
        {
            continue;
        }
        let properties = schema_value["properties"]
            .as_object()
            .unwrap_or_else(|| panic!("{schema_name} missing object properties"));
        let mut property_names = properties.keys().cloned().collect::<Vec<_>>();
        property_names.sort();
        let mut required = schema_value["required"]
            .as_array()
            .unwrap_or_else(|| panic!("{schema_name} missing required properties"))
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .unwrap_or_else(|| panic!("{schema_name} has non-string required property"))
                    .to_owned()
            })
            .collect::<Vec<_>>();
        required.sort();
        assert_eq!(
            required, property_names,
            "{schema_name} should require every declared output property"
        );
    }
}

fn assert_array_schemas_declare_items(schema: &Value) {
    assert_array_schemas_declare_items_in(schema, "$");
}

fn assert_array_schemas_declare_items_in(value: &Value, context: &str) {
    match value {
        Value::Object(object) => {
            if object.get("type").and_then(Value::as_str) == Some("array") {
                assert!(
                    object.get("items").is_some_and(Value::is_object),
                    "{context} declares an array schema without object items"
                );
            }
            for (key, child) in object {
                assert_array_schemas_declare_items_in(child, &format!("{context}.{key}"));
            }
        }
        Value::Array(values) => {
            for (index, child) in values.iter().enumerate() {
                assert_array_schemas_declare_items_in(child, &format!("{context}[{index}]"));
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn assert_json_content_schema(content: &Value, context: &str) {
    if content.is_null() {
        return;
    }
    let Some(json_content) = content.get("application/json") else {
        return;
    };
    assert!(
        json_content["schema"].is_object(),
        "{context} missing JSON content schema"
    );
}

fn assert_operation_ids_present_and_unique(schema: &Value) {
    let paths = schema["paths"].as_object().expect("OpenAPI paths");
    let mut operation_ids = std::collections::BTreeSet::new();
    for (path, endpoint) in paths {
        let operations = endpoint.as_object().expect("OpenAPI path item");
        for (method, operation) in operations {
            if !matches!(method.as_str(), "get" | "post" | "delete" | "options") {
                continue;
            }
            let operation_id = operation["operationId"]
                .as_str()
                .unwrap_or_else(|| panic!("{method} {path} missing operationId"));
            assert_eq!(
                operation_id,
                expected_operation_id(method, path),
                "{method} {path} has non-standard operationId"
            );
            assert!(
                operation_ids.insert(operation_id.to_owned()),
                "duplicate OpenAPI operationId {operation_id}"
            );
        }
    }
}

fn expected_operation_id(method: &str, path: &str) -> String {
    let mut operation_id = method.to_owned();
    for segment in path.trim_matches('/').split('/') {
        let segment = segment.trim_matches(|character| character == '{' || character == '}');
        push_pascal_words_for_operation_id(&mut operation_id, segment);
    }
    operation_id
}

fn push_pascal_words_for_operation_id(operation_id: &mut String, segment: &str) {
    let mut word = String::new();
    for character in segment.chars() {
        if character.is_ascii_alphanumeric() {
            word.push(character);
        } else if !word.is_empty() {
            push_pascal_word_for_operation_id(operation_id, &word);
            word.clear();
        }
    }
    if !word.is_empty() {
        push_pascal_word_for_operation_id(operation_id, &word);
    }
}

fn push_pascal_word_for_operation_id(operation_id: &mut String, word: &str) {
    let mut characters = word.chars();
    let Some(first) = characters.next() else {
        return;
    };
    operation_id.push(first.to_ascii_uppercase());
    for character in characters {
        operation_id.push(character.to_ascii_lowercase());
    }
}

fn assert_operation_tags_present_and_declared(schema: &Value) {
    let declared_tags = schema["tags"]
        .as_array()
        .expect("OpenAPI top-level tags")
        .iter()
        .map(|tag| {
            let name = tag["name"]
                .as_str()
                .filter(|name| !name.trim().is_empty())
                .unwrap_or_else(|| panic!("OpenAPI tag missing name: {tag:?}"));
            assert!(
                tag["description"]
                    .as_str()
                    .is_some_and(|description| !description.trim().is_empty()),
                "OpenAPI tag {name} missing description"
            );
            name.to_owned()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        !declared_tags.is_empty(),
        "OpenAPI schema should declare top-level tags"
    );

    let mut used_tags = std::collections::BTreeSet::new();
    let paths = schema["paths"].as_object().expect("OpenAPI paths");
    for (path, endpoint) in paths {
        let operations = endpoint.as_object().expect("OpenAPI path item");
        for (method, operation) in operations {
            if !matches!(method.as_str(), "get" | "post" | "delete" | "options") {
                continue;
            }
            let tags = operation["tags"]
                .as_array()
                .unwrap_or_else(|| panic!("{method} {path} missing tags"));
            assert!(!tags.is_empty(), "{method} {path} missing operation tags");
            for tag in tags {
                let tag = tag
                    .as_str()
                    .unwrap_or_else(|| panic!("{method} {path} has non-string tag: {tag:?}"));
                assert!(
                    declared_tags.contains(tag),
                    "{method} {path} uses undeclared OpenAPI tag {tag}"
                );
                used_tags.insert(tag.to_owned());
            }
        }
    }
    assert_eq!(
        used_tags, declared_tags,
        "OpenAPI top-level tags should all be used by operations"
    );
}

fn assert_standard_response_headers_declared(schema: &Value) {
    let paths = schema["paths"].as_object().expect("OpenAPI paths");
    for (path, endpoint) in paths {
        let operations = endpoint.as_object().expect("OpenAPI path item");
        for (method, operation) in operations {
            if !matches!(method.as_str(), "get" | "post" | "delete" | "options") {
                continue;
            }
            let responses = operation["responses"]
                .as_object()
                .unwrap_or_else(|| panic!("{method} {path} missing responses"));
            for (status, response) in responses {
                for header in [
                    "Allow",
                    "Cache-Control",
                    "X-Content-Type-Options",
                    "X-Trace-Id",
                    "Access-Control-Allow-Origin",
                    "Access-Control-Allow-Methods",
                    "Access-Control-Allow-Headers",
                ] {
                    assert!(
                        response["headers"][header]["description"]
                            .as_str()
                            .is_some_and(|description| !description.trim().is_empty()),
                        "{method} {path} response {status} missing {header} response header description"
                    );
                    assert!(
                        response["headers"][header]["schema"].is_object(),
                        "{method} {path} response {status} missing {header} response header schema"
                    );
                }
            }
        }
    }
}

fn assert_string_inputs_are_constrained(schema: &Value) {
    let request_schemas = schema["components"]["schemas"]
        .as_object()
        .expect("OpenAPI schemas");
    for (schema_name, schema_value) in request_schemas {
        if !schema_name.ends_with("Request") {
            continue;
        }
        let Some(properties) = schema_value.get("properties").and_then(Value::as_object) else {
            continue;
        };
        for (property_name, property_schema) in properties {
            if request_string_input_allows_free_form(schema_name, property_name) {
                continue;
            }
            assert_string_schema_constrained(
                property_schema,
                &format!("{schema_name}.{property_name}"),
            );
            if let Some(item_schema) = property_schema.get("items") {
                assert_string_schema_constrained(
                    item_schema,
                    &format!("{schema_name}.{property_name}[]"),
                );
            }
        }
    }

    let paths = schema["paths"].as_object().expect("OpenAPI paths");
    for (path, endpoint) in paths {
        let operations = endpoint.as_object().expect("OpenAPI path item");
        for (method, operation) in operations {
            if !matches!(method.as_str(), "get" | "post" | "delete" | "options") {
                continue;
            }
            let Some(parameters) = operation.get("parameters").and_then(Value::as_array) else {
                continue;
            };
            for parameter in parameters {
                let name = parameter["name"]
                    .as_str()
                    .unwrap_or_else(|| panic!("{method} {path} parameter missing name"));
                assert_string_schema_constrained(
                    &parameter["schema"],
                    &format!("{method} {path} parameter {name}"),
                );
            }
        }
    }
}

fn request_string_input_allows_free_form(schema_name: &str, property_name: &str) -> bool {
    matches!(
        (schema_name, property_name),
        ("CreateToolRequest", "description") | ("UpdateToolRequest", "description")
    )
}

fn assert_string_schema_constrained(schema: &Value, context: &str) {
    if !schema_accepts_string(schema) {
        for keyword in ["anyOf", "oneOf", "allOf"] {
            if let Some(variants) = schema.get(keyword).and_then(Value::as_array) {
                for (index, variant) in variants.iter().enumerate() {
                    assert_string_schema_constrained(
                        variant,
                        &format!("{context}.{keyword}[{index}]"),
                    );
                }
            }
        }
        if let Some(item_schema) = schema.get("items") {
            assert_string_schema_constrained(item_schema, &format!("{context}.items"));
        }
        if let Some(property_names_schema) = schema.get("propertyNames") {
            assert_string_schema_constrained(
                property_names_schema,
                &format!("{context}.propertyNames"),
            );
        }
        return;
    }
    assert!(
        schema.get("minLength").is_some()
            || schema.get("pattern").is_some()
            || schema.get("enum").is_some()
            || schema.get("format").is_some()
            || schema.get("const").is_some(),
        "{context} accepts arbitrary strings without a schema constraint"
    );
}

fn assert_nullable_string_variants_are_constrained(schema: &Value) {
    let schemas = schema["components"]["schemas"]
        .as_object()
        .expect("OpenAPI schemas");
    for (schema_name, schema_value) in schemas {
        assert_nullable_string_variants_in_schema(schema_value, schema_name);
    }
}

fn assert_nullable_string_variants_in_schema(schema: &Value, context: &str) {
    if let Some(variants) = schema.get("anyOf").and_then(Value::as_array) {
        for (index, variant) in variants.iter().enumerate() {
            let variant_context = format!("{context}.anyOf[{index}]");
            if schema_accepts_string(variant) {
                assert_string_schema_constrained(variant, &variant_context);
            }
            assert_nullable_string_variants_in_schema(variant, &variant_context);
        }
    }
    if let Some(variants) = schema.get("oneOf").and_then(Value::as_array) {
        for (index, variant) in variants.iter().enumerate() {
            assert_nullable_string_variants_in_schema(
                variant,
                &format!("{context}.oneOf[{index}]"),
            );
        }
    }
    if let Some(variants) = schema.get("allOf").and_then(Value::as_array) {
        for (index, variant) in variants.iter().enumerate() {
            assert_nullable_string_variants_in_schema(
                variant,
                &format!("{context}.allOf[{index}]"),
            );
        }
    }
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (property_name, property_schema) in properties {
            assert_nullable_string_variants_in_schema(
                property_schema,
                &format!("{context}.{property_name}"),
            );
        }
    }
    if let Some(item_schema) = schema.get("items") {
        assert_nullable_string_variants_in_schema(item_schema, &format!("{context}.items"));
    }
    if let Some(property_names_schema) = schema.get("propertyNames") {
        assert_nullable_string_variants_in_schema(
            property_names_schema,
            &format!("{context}.propertyNames"),
        );
    }
}

fn schema_accepts_string(schema: &Value) -> bool {
    match schema.get("type") {
        Some(Value::String(kind)) => kind == "string",
        Some(Value::Array(kinds)) => kinds.iter().any(|kind| kind.as_str() == Some("string")),
        _ => false,
    }
}

fn path_template_parameters(path: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut rest = path;
    while let Some(start) = rest.find('{') {
        let after_start = &rest[start + 1..];
        let Some(end) = after_start.find('}') else {
            break;
        };
        names.push(after_start[..end].to_owned());
        rest = &after_start[end + 1..];
    }
    names
}
