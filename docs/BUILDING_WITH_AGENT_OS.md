# Building With Agent OS

Agent OS is intended to be embedded into other agent products, developer tools, dashboards, and local automation systems. Treat it as a durable local backend with two public control surfaces:

- The `agent-os` CLI for operators, scripts, and shell-first workflows.
- The local HTTP API for dashboards, workers, and integrations.

## Core Pattern

1. Pick a state directory for the project or product.
2. Register agents with capabilities such as `code`, `rust`, `review`, `test`, or `deploy`.
3. Register tools for reusable commands or file operations.
4. Queue tasks through the CLI or API.
5. Run work through a scheduler tick, daemon, or service.
6. Read status, metrics, events, run logs, and replay records.

```bash
agent-os --state ./.agent-os init --name "Project Agent OS"
agent-os --state ./.agent-os agent add builder --kind builder --cap code --cap test --parallel 2
agent-os --state ./.agent-os task create "Run test suite" --need test --command "cargo test"
agent-os --state ./.agent-os run --execute --limit 1
agent-os --state ./.agent-os runs list
```

## Integration Options

### CLI Backend

Use the CLI when your product is shell-first or when the integration is another local script. Add `--json` to commands that return structured output.

```bash
agent-os --state ./.agent-os --json task create "Run lint" --need lint --command "cargo clippy --all-targets"
agent-os --state ./.agent-os --json run --execute --limit 2
```

### Local API Backend

Use the API when building a dashboard, worker process, editor extension, or another local service.

```bash
export AGENT_OS_API_TOKEN=dev-token
agent-os --state ./.agent-os api serve --addr 127.0.0.1:7373 --token-env AGENT_OS_API_TOKEN
```

Then call the API with bearer auth:

```bash
curl -H "Authorization: Bearer dev-token" http://127.0.0.1:7373/status
```

For supervised local services, `--token-file ./api.token` can be used instead of `--token-env`; scoped clients can use `--read-token-file` and `--write-token-file` instead of scoped env vars. Token files are read once at startup, trimmed, and rejected if empty.

Use `agent-os api schema` or `GET /openapi.json` to generate clients or validate your integration.

### Daemon Backend

Use daemon mode when Agent OS should keep processing work without a human running each tick:

```bash
agent-os --state ./.agent-os daemon run --execute --limit 2 --interval-ms 1000
```

On macOS, use the `service launchd`, `service install`, `service start`, and `service status` commands to run the daemon as a LaunchAgent. On Linux, use `service systemd`, `service install-systemd`, `service start-systemd`, and `service status-systemd` to run the daemon as a systemd user service. On Windows, use `service windows-task` to render a PowerShell `Register-ScheduledTask` script, inspect it, then run it from an elevated PowerShell session or your preferred endpoint-management tool.

## Product Ideas

- Coding agent CLI: queue tasks, let an LLM provider plan work, and execute safe tools through Agent OS.
- Local dashboard: show agents, tasks, workflows, DAG editing forms, runs, logs, events, health, and metrics.
- Automation daemon: keep project maintenance tasks moving with recovery and replay.
- Worker fleet on one machine: have separate processes heartbeat, claim tasks, execute work based on capabilities, and report task results with run/artifact metadata.
- Agent framework adapter: use another framework for reasoning and Agent OS for persistence, scheduling, policy, and logs.

## State And Safety

State is intentionally local and portable. A state directory contains the JSON state file, lock file, and run logs. Use one state directory per project or product environment.

Execution is policy-aware. Shell commands, file reads, and file writes are checked before running. Configure allowed workspaces, denied command patterns, environment inheritance, and redaction in `agent-os.toml`.

Use `agent-os doctor` and `agent-os state validate` before running critical automation. Doctor output includes platform-specific service guidance and shell-execution support, which is useful when deciding between launchd, systemd, or a manual supervisor. Use `agent-os state backup` before migrations, pruning, or public demos.
