# Agent OS

[![CI](https://github.com/Shree-git/agent-os/actions/workflows/ci.yml/badge.svg)](https://github.com/Shree-git/agent-os/actions/workflows/ci.yml)
![Rust](https://img.shields.io/badge/rust-1.85%2B-93450a)
![License](https://img.shields.io/badge/license-MIT-blue)
![Local First](https://img.shields.io/badge/local--first-agent%20runtime-2f6f4e)

Agent OS is a local-first control plane for AI agents. It gives builders the durable runtime pieces that most agent projects eventually have to invent: task queues, agent registry, capability-aware scheduling, shared memory, tool execution, policy checks, run logs, daemon execution, and a local HTTP API.

Use Agent OS when you want to build an agent CLI, dashboard, automation daemon, coding worker, or Hermes-style autonomous system without starting from an empty prompt loop and a pile of glue code.

## Why Agent OS

Most agent frameworks help define agent behavior. Agent OS focuses on the operational layer underneath:

- Durable local state for agents, tasks, workflows, tools, memory, events, daemon status, and run history.
- A single portable state directory instead of a required database, broker, or hosted service.
- A CLI for operators and a JSON API for dashboards, workers, and integrations.
- Policy-aware shell and file-tool execution with workspace checks, redaction, cancellation, replay, and logs.
- Deterministic mock-provider mode for offline development plus an OpenAI-compatible provider boundary for real LLM work.
- Rust-native packaging with a small binary and a library API for embedding the runtime.

Agent OS is not a chat UI or a hosted agent platform. It is the local runtime you can build those systems on.

## What You Can Build

- A project-local coding agent queue that runs tests, reviews, migrations, or release checks.
- A dashboard for long-running agent work with live status, logs, metrics, and replay.
- A self-hosted automation daemon for recurring maintenance tasks.
- A multi-agent worker backend with capability routing and task leases.
- A safe tool execution layer for LLM apps that need auditability and recovery.
- A Hermes-like autonomous agent experience with Agent OS as the scheduler, memory, and execution backend.

## Installation

With Cargo after release:

```bash
cargo install agent_os
agent-os --help
```

With Homebrew after publishing a tap formula:

```bash
brew install Shree-git/tap/agent-os
agent-os --help
```

The Homebrew formula selects the matching release archive for Apple Silicon macOS, Intel macOS, or x86_64 Linux. Bash, zsh, and fish completions are installed through Homebrew's native completion paths; elvish and PowerShell completions are included under the formula package share directory.

From a GitHub release archive:

```bash
tar -xzf agent-os-VERSION-TARGET.tar.gz
./agent-os-VERSION-TARGET/bin/agent-os --help
```

From a checkout:

```bash
cargo install --path .
agent-os --help
```

For development, `cargo run -- ...` works without installing the binary. Release archives are built with `./scripts/package-release.sh`, include README/license/changelog plus bash, zsh, fish, elvish, and PowerShell completions, infer `.exe` packaging from Windows target triples, and emit `.sha256` checksum files. Set `AGENT_OS_SIGN_RELEASES=1` with `cosign` on `PATH` to emit Sigstore `.sig` and `.pem` files; the tag release workflow signs archives this way. The crate package builds and verifies with the release check documented below.

## Quick Start

Use `--state ./sandbox` while learning so state and logs stay inside the checkout. `init` seeds a planner agent, a builder agent, and a reusable `cargo-test` tool so the first demo can start from a working local control plane:

```bash
cargo run -- --state ./sandbox init --name "Local Agent OS" --force
cargo run -- --state ./sandbox agent list
cargo run -- --state ./sandbox tool list
cargo run -- --state ./sandbox task create "Show where Agent OS fits" --need rust --command "printf 'coding queue, local dashboard, automation daemon, auditable tools\n'"
cargo run -- --state ./sandbox run --limit 1 --execute
cargo run -- --state ./sandbox runs list
```

Inspect a run:

```bash
cargo run -- --state ./sandbox runs logs RUN_ID
cargo run -- --state ./sandbox runs replay RUN_ID
```

Run the local API:

```bash
export AGENT_OS_API_TOKEN=dev-token
cargo run -- --state ./sandbox api serve --addr 127.0.0.1:7373 --token-env AGENT_OS_API_TOKEN
```

Then query it from another terminal:

```bash
curl -H "Authorization: Bearer dev-token" http://127.0.0.1:7373/status
curl -H "Authorization: Bearer dev-token" http://127.0.0.1:7373/metrics
```

After installation, use `agent-os` directly instead of `cargo run --`.

## Live Demo Tracks

Use the same deterministic local workflow for both audiences. It needs no hosted service, no API key for model providers, and no network beyond the optional localhost API.

### Technical builders

Show Agent OS as a local backend for agent products:

```bash
agent-os --state ./sandbox init --name "Builder Demo" --force
agent-os --state ./sandbox agent list
agent-os --state ./sandbox tool list
agent-os --state ./sandbox task create "Run a local builder task" --need rust --command "printf 'scheduled, executed, logged, replayable\n'"
agent-os --state ./sandbox run --execute --limit 1
agent-os --state ./sandbox runs list
agent-os --state ./sandbox runs logs RUN_ID
agent-os --state ./sandbox runs replay RUN_ID
agent-os --state ./sandbox metrics
```

Then start the API and open the local dashboard or dashboard-ready JSON from another terminal:

```bash
export AGENT_OS_API_TOKEN=dev-token
agent-os --state ./sandbox api serve --addr 127.0.0.1:7373 --token-env AGENT_OS_API_TOKEN
curl -H "Authorization: Bearer dev-token" http://127.0.0.1:7373/status
curl -H "Authorization: Bearer dev-token" http://127.0.0.1:7373/metrics
open http://127.0.0.1:7373/dashboard.html
```

### Business stakeholders

Use the terminal output to tell the product story:

- Coding queue: tasks can be queued, assigned by capability, executed, and inspected.
- Local dashboard: `/dashboard.html` renders agent creation/inspection/update/heartbeat/claim/removal, task creation/inspection/update/assignment/planning/priority/dependency/lifecycle/recovery/removal, tool catalog creation/inspection/update/removal, runs, one-shot scheduler ticks, daemon inspection/stop controls, workflow progress, a lightweight workflow DAG editor, registry marketplace import, registry agent-profile installation, registry template workflow creation, MCP server registration/update/removal, git status inspection, git review-task creation, service definition rendering plus install/uninstall/start/stop/status controls for launchd/systemd, state validation/export/import/migration/backup/repair/prune plus SQLite mirror sync/restore, policy/autonomy posture with config inspection/validation/profile writing, provider/plugin and structured-output posture, approval resolution controls, worker registration/heartbeat/claim/report/removal, eval recording and command/schema eval runs, run debug/replay/artifact inspection, secret backend registration/inspection/removal and redacted checks, memory add/recall/inspect/update/prune/removal, metrics, and the event timeline; `/status` and `/metrics` expose live state for custom UIs.
- Automation daemon: `daemon run` keeps recurring or queued work moving without manual ticks.
- Auditable tool execution: runs have durable logs and replay output.
- Portable local state: the state directory can be backed up, validated, repaired, exported, and moved with the project.

Global `--state` and `--config` paths, plus `AGENT_OS_HOME` and `AGENT_OS_CONFIG`, must not be empty. `--state path.sqlite`, `--state path.sqlite3`, or `--state path.db` uses SQLite as the active state backend; other file paths remain JSON state files, and directory paths use `state.json` inside the directory. When a command needs initialized state, CLI errors include the resolved state path and a copyable `agent-os --state ... init --profile safe` command. Add `--json` to read commands and supported create/update commands when another program needs stable output. Human status output uses terminal colors when stdout is a terminal; set `NO_COLOR` or set `CLICOLOR` to `0` to disable ANSI colors, or set `CLICOLOR_FORCE` to `1` to force them in scripts.

## Build With Agent OS

The fastest way to integrate Agent OS is to treat it as a local backend:

1. Create a project-local state directory such as `.agent-os`.
2. Register agents with the capabilities your worker process can handle.
3. Register reusable tools for commands or file operations.
4. Queue tasks through the CLI or `POST /tasks`.
5. Run work with `agent-os run --execute`, `agent-os daemon run --execute`, or a launchd service.
6. Read status, metrics, logs, and replay data from the CLI or HTTP API.

See the [landing page](docs/landing.html), [Building With Agent OS](docs/BUILDING_WITH_AGENT_OS.md), [Architecture](docs/ARCHITECTURE.md), [Threat Model](docs/THREAT_MODEL.md), [Examples](examples/README.md), and the [Public Release Checklist](docs/PUBLIC_RELEASE_CHECKLIST.md) for copyable positioning, demos, integration patterns, and repository launch steps.

## End-to-End Smoke Workflow

This path exercises the core operator loop across durable state, memory, workflows, API, daemon execution, tools, and run logs:

```bash
agent-os --state ./sandbox init --force
agent-os --state ./sandbox memory add release-context "Use the durable workflow smoke path." --tag release
agent-os --state ./sandbox tool add write-handoff --kind file-write --need rust --cwd ./workspace --command-template "{name}.txt"
agent-os --state ./sandbox --json workflow create "Prove planner builder reviewer release loop" --execute
agent-os --state ./sandbox api serve --addr 127.0.0.1:7373 --token-env AGENT_OS_API_TOKEN
curl -H "Authorization: Bearer $AGENT_OS_API_TOKEN" http://127.0.0.1:7373/memory?query=release-context
curl -H "Authorization: Bearer $AGENT_OS_API_TOKEN" -H "Content-Type: application/json" -d '{"all":true}' http://127.0.0.1:7373/workflows/WORKFLOW_ID/run
agent-os --state ./sandbox task create "Write daemon handoff" --need rust --tool write-handoff --arg name=handoff --arg body=daemon-note
agent-os --state ./sandbox daemon run --execute --limit 1 --interval-ms 10 --max-ticks 2
agent-os --state ./sandbox runs logs RUN_ID
agent-os --state ./sandbox runs replay RUN_ID
```

Use the IDs returned by the JSON commands for `WORKFLOW_ID` and `RUN_ID`. The matching integration test is `end_to_end_operator_workflow_covers_cli_api_daemon_tools_memory_and_run_logs`.

## Commands

```text
agent-os init [--name NAME] [--force] [--profile safe|dev|autonomous|ci]
agent-os status
agent-os metrics [--prometheus]
agent-os doctor
agent-os config init [--force] [--profile safe|dev|autonomous|ci]
agent-os config show
agent-os config validate
agent-os state export [--output PATH] [--dry-run]
agent-os state import PATH [--force] [--dry-run]
agent-os state backup [--output PATH] [--dry-run]
agent-os state migrate [--input PATH] [--output PATH] [--dry-run]
agent-os state prune [--keep-runs 100] [--keep-events 500] [--dry-run]
agent-os state repair [--dry-run]
agent-os state sqlite [--output PATH] [--init-only] [--restore] [--force] [--dry-run]
agent-os state validate
agent-os agent add NAME [--kind KIND] [--model MODEL] --cap CAP [--parallel N]
agent-os agent list [--status online|up|busy|paused|pause|offline|down] [--kind KIND] [--cap CAP] [--since RFC3339] [--until RFC3339] [--query TEXT] [--limit N]
agent-os agent show AGENT_ID
agent-os agent update AGENT_ID [--name NAME] [--kind KIND] [--model MODEL] [--clear-model] [--cap CAP] [--parallel N]
agent-os agent heartbeat AGENT_ID [--status online|up|busy|paused|pause|offline|down] [--lease-seconds N]
agent-os agent claim AGENT_ID [--lease-seconds N]
agent-os agent remove AGENT_ID
agent-os task create TITLE [--objective TEXT] [--command SHELL] [--tool TOOL_ID] [--arg KEY=VALUE] [--secret-arg KEY=ENV_VAR] [--cwd DIR] [--priority low|normal|high|critical|urgent] [--need CAP] [--after TASK_ID] [--max-attempts N]
agent-os task list [--all] [--status pending|running|blocked|complete|completed|failed|cancelled|canceled] [--priority low|normal|high|critical|urgent] [--agent AGENT_ID] [--tool TOOL_ID] [--after TASK_ID] [--cap CAP] [--since RFC3339] [--until RFC3339] [--query TEXT] [--limit N]
agent-os task show TASK_ID
agent-os task update TASK_ID [--title TITLE] [--objective TEXT] [--command SHELL] [--clear-command] [--tool TOOL_ID] [--clear-tool] [--arg KEY=VALUE] [--secret-arg KEY=ENV_VAR] [--clear-args] [--clear-secret-args] [--cwd DIR] [--clear-cwd] [--need CAP] [--clear-needs] [--max-attempts N]
agent-os task assign TASK_ID AGENT_ID
agent-os task priority TASK_ID --priority low|normal|high|critical|urgent
agent-os task dependencies TASK_ID [--after TASK_ID] [--clear]
agent-os task plan TASK_ID --step "first" --step "second"
agent-os task complete TASK_ID [--note TEXT]
agent-os task fail TASK_ID [--note TEXT]
agent-os task block TASK_ID [--note TEXT]
agent-os task cancel TASK_ID [--note TEXT]
agent-os task retry TASK_ID [--note TEXT]
agent-os task unblock TASK_ID [--note TEXT]
agent-os task delete TASK_ID
agent-os task recover [--older-than-seconds 1800]
agent-os tool add NAME --command-template "printf {message}" [--kind shell|file-read|read-file|file-write|write-file] [--description TEXT] [--need CAP] [--cwd DIR]
agent-os tool list [--kind shell|file-read|read-file|file-write|write-file] [--cap CAP] [--since RFC3339] [--until RFC3339] [--query TEXT] [--limit N]
agent-os tool show TOOL_ID
agent-os tool update TOOL_ID [--kind shell|file-read|read-file|file-write|write-file] [--description TEXT] [--clear-description] [--command-template TEMPLATE] [--need CAP] [--clear-needs] [--cwd DIR] [--clear-cwd]
agent-os tool remove TOOL_ID
agent-os memory add TOPIC BODY [--tag TAG] [--visibility shared|private] [--scope SCOPE]
agent-os memory search QUERY [--tag TAG] [--since RFC3339] [--until RFC3339] [--limit N] [--visibility shared|private] [--scope SCOPE]
agent-os memory recall QUERY [--tag TAG] [--since RFC3339] [--until RFC3339] [--limit N] [--visibility shared|private] [--scope SCOPE]
agent-os memory list [--tag TAG] [--since RFC3339] [--until RFC3339] [--limit N] [--visibility shared|private] [--scope SCOPE]
agent-os memory show MEMORY_ID
agent-os memory update MEMORY_ID [--topic TOPIC] [--body BODY] [--tag TAG] [--clear-tags] [--visibility shared|private] [--scope SCOPE] [--clear-scope]
agent-os memory remove MEMORY_ID
agent-os memory prune [--max-age-days N] [--dry-run]
agent-os registry list
agent-os registry profiles
agent-os registry profile ID
agent-os registry install-agent PROFILE [--name NAME] [--model MODEL] [--parallel N]
agent-os registry templates
agent-os registry template ID
agent-os registry create-workflow TEMPLATE OBJECTIVE [--priority low|normal|high|critical|urgent]
agent-os registry mcp-list
agent-os registry mcp-add ID --command COMMAND [--arg ARG] [--env KEY=VALUE] [--disabled]
agent-os registry mcp-enable ID
agent-os registry mcp-disable ID
agent-os registry mcp-remove ID
agent-os registry marketplace-import PATH [--expect-checksum CHECKSUM] [--force]
agent-os worker list [--status online|up|busy|paused|pause|offline|down] [--since RFC3339] [--until RFC3339] [--query TEXT] [--limit N]
agent-os worker register ID --endpoint ENDPOINT [--status online|up|busy|paused|pause|offline|down]
agent-os worker show WORKER_ID
agent-os worker heartbeat WORKER_ID [--endpoint ENDPOINT] [--status online|up|busy|paused|pause|offline|down] [--lease-seconds N]
agent-os worker claim WORKER_ID [--lease-seconds N]
agent-os worker report WORKER_ID TASK_ID [--status complete|failed] [--note TEXT] [--command COMMAND] [--cwd CWD] [--exit-code N] [--artifact KIND=PATH]
agent-os worker remove WORKER_ID
agent-os eval list [--target TARGET] [--success true|false] [--since RFC3339] [--until RFC3339] [--query TEXT] [--limit N]
agent-os eval show EVAL_ID
agent-os eval record TARGET [--success] [--failure] [--cost-micros N] [--latency-ms N]
agent-os eval run TARGET --command SHELL [--cwd PATH] [--success-pattern TEXT] [--output-schema PATH]
agent-os secrets list [--kind environment|1password|os-keychain|env-vault] [--query TEXT] [--limit N]
agent-os secrets check
agent-os secrets register ID --kind environment|1password|os-keychain|env-vault [--reference REF]
agent-os secrets show BACKEND_ID
agent-os secrets remove BACKEND_ID
agent-os approval list
agent-os approval approve APPROVAL_ID [--by NAME]
agent-os approval deny APPROVAL_ID [--by NAME]
agent-os git status [--cwd PATH]
agent-os git branch NAME [--cwd PATH] [--create]
agent-os git commit --message MESSAGE [--cwd PATH] [--all] [--dry-run]
agent-os git pr --title TITLE --body BODY [--base BRANCH] [--head BRANCH] [--draft] [--cwd PATH] [--dry-run]
agent-os git review-task [--base BRANCH] [--cwd PATH] [--title TITLE] [--priority low|normal|high|critical|urgent]
agent-os events [--limit 20] [--kind KIND] [--since RFC3339] [--until RFC3339] [--query TEXT]
agent-os runs list [--status running|cancel-requested|cancel_requested|cancelled|canceled|success|succeeded|failed|rejected] [--task TASK_ID] [--agent AGENT_ID] [--since RFC3339] [--until RFC3339] [--query TEXT] [--limit N]
agent-os runs show RUN_ID
agent-os runs logs RUN_ID [--tail-bytes N]
agent-os runs tail RUN_ID [--follow] [--tail-bytes N] [--interval-ms 200]
agent-os runs replay RUN_ID [--tail-bytes N]
agent-os runs debug RUN_ID [--tail-bytes N]
agent-os runs artifacts RUN_ID [ARTIFACT_ID|KIND|INDEX] [--tail-bytes N]
agent-os runs cancel RUN_ID
agent-os daemon run [--limit 1] [--execute] [--interval-ms 1000] [--max-ticks N] [--recover-stale-seconds N]
agent-os daemon status
agent-os daemon stop
agent-os service launchd [--label LABEL] [--bin-path PATH] [--interval-ms 1000] [--limit 1] [--execute] [--recover-stale-seconds N] [--no-logs] [--plist-path PATH]
agent-os service systemd [--unit-name NAME] [--bin-path PATH] [--interval-ms 1000] [--limit 1] [--execute] [--recover-stale-seconds N] [--unit-path PATH]
agent-os service windows-task [--task-name NAME] [--bin-path PATH] [--interval-ms 1000] [--limit 1] [--execute] [--recover-stale-seconds N]
agent-os service install [--label LABEL] [--bin-path PATH] [--interval-ms 1000] [--limit 1] [--execute] [--recover-stale-seconds N] [--no-logs] [--plist-path PATH]
agent-os service install-systemd [--unit-name NAME] [--bin-path PATH] [--interval-ms 1000] [--limit 1] [--execute] [--recover-stale-seconds N] [--unit-path PATH]
agent-os service uninstall [--label LABEL] [--plist-path PATH]
agent-os service uninstall-systemd [--unit-name NAME] [--unit-path PATH]
agent-os service start [--label LABEL] [--plist-path PATH] [--domain DOMAIN] [--launchctl-path PATH]
agent-os service start-systemd [--unit-name NAME] [--systemctl-path PATH]
agent-os service stop [--label LABEL] [--plist-path PATH] [--domain DOMAIN] [--launchctl-path PATH]
agent-os service stop-systemd [--unit-name NAME] [--systemctl-path PATH]
agent-os service status [--label LABEL] [--domain DOMAIN] [--launchctl-path PATH]
agent-os service status-systemd [--unit-name NAME] [--systemctl-path PATH]
agent-os mcp serve [--max-requests N]
agent-os completions bash|elvish|fish|powershell|zsh
agent-os api serve [--addr 127.0.0.1:7373] [--token-env ENV] [--token-file PATH] [--read-token-env ENV] [--read-token-file PATH] [--write-token-env ENV] [--write-token-file PATH] [--allow-origin ORIGIN] [--unsafe-no-token] [--max-requests N]
agent-os api schema
agent-os workflow create OBJECTIVE [--priority low|normal|high|critical|urgent] [--execute]
agent-os workflow list [--priority low|normal|high|critical|urgent] [--task TASK_ID] [--since RFC3339] [--until RFC3339] [--query TEXT] [--limit N]
agent-os workflow show WORKFLOW_ID
agent-os workflow status WORKFLOW_ID
agent-os workflow add-task WORKFLOW_ID STAGE TITLE [--objective TEXT] [--command SHELL] [--need CAP] [--after STAGE] [--priority low|normal|high|critical|urgent]
agent-os workflow link WORKFLOW_ID --from STAGE --to STAGE
agent-os workflow unlink WORKFLOW_ID --from STAGE --to STAGE
agent-os workflow pause WORKFLOW_ID [--note TEXT]
agent-os workflow resume WORKFLOW_ID [--note TEXT]
agent-os workflow retry WORKFLOW_ID [--note TEXT]
agent-os workflow run WORKFLOW_ID [--all]
agent-os workflow cancel WORKFLOW_ID [--note TEXT]
agent-os workflow remove WORKFLOW_ID
agent-os run [--limit 1] [--execute] [--dry-run] [--recover-stale-seconds N]
```

Add `--json` to read commands and supported create/update commands when another program needs stable output.

The local API serves JSON plus a small HTML dashboard for operators, tools, and local agent integrations:

```text
GET /health
GET /dashboard.html
GET /doctor
GET /config
POST /config
GET /config/validate
POST /init
GET /metrics
GET /metrics/prometheus
GET /status
GET /daemon
POST /daemon/stop
POST /service/launchd
POST /service/launchd/install
POST /service/launchd/uninstall
POST /service/launchd/start
POST /service/launchd/stop
POST /service/launchd/status
POST /service/systemd
POST /service/systemd/install
POST /service/systemd/uninstall
POST /service/systemd/start
POST /service/systemd/stop
POST /service/systemd/status
GET /state/export
POST /state/export
POST /state/import
POST /state/migrate
GET /state/validate
POST /state/backup
POST /state/repair
POST /state/prune
POST /state/sqlite
GET /agents?status=online|up|busy|paused|pause|offline|down&kind=KIND&capability=CAP&since=RFC3339&until=RFC3339&query=TEXT&limit=N
POST /agents
GET /agents/AGENT_ID
POST /agents/AGENT_ID
DELETE /agents/AGENT_ID
POST /agents/AGENT_ID/heartbeat
POST /agents/AGENT_ID/claim
GET /tasks?status=pending|running|blocked|complete|completed|failed|cancelled|canceled&priority=low|normal|high|critical|urgent&agent=AGENT_ID&tool=TOOL_ID&after=TASK_ID&capability=CAP&since=RFC3339&until=RFC3339&query=TEXT&limit=N
POST /tasks
GET /tasks/TASK_ID
POST /tasks/TASK_ID
POST /tasks/TASK_ID/plan
POST /tasks/TASK_ID/complete
POST /tasks/TASK_ID/fail
POST /tasks/TASK_ID/block
POST /tasks/TASK_ID/cancel
POST /tasks/TASK_ID/retry
POST /tasks/TASK_ID/unblock
POST /tasks/TASK_ID/assign
POST /tasks/TASK_ID/priority
POST /tasks/TASK_ID/dependencies
DELETE /tasks/TASK_ID
POST /tasks/recover
GET /tools?kind=shell|file-read|read-file|file-write|write-file&capability=CAP&since=RFC3339&until=RFC3339&query=TEXT&limit=N
POST /tools
GET /tools/TOOL_ID
POST /tools/TOOL_ID
DELETE /tools/TOOL_ID
POST /run
GET /runs?status=running|cancel-requested|cancel_requested|cancelled|canceled|success|succeeded|failed|rejected&task=TASK_ID&agent=AGENT_ID&since=RFC3339&until=RFC3339&query=TEXT&limit=N
GET /runs/RUN_ID
GET /runs/RUN_ID/logs?tail_bytes=N
GET /runs/RUN_ID/replay?tail_bytes=N
GET /runs/RUN_ID/debug?tail_bytes=N
GET /runs/RUN_ID/artifacts
GET /runs/RUN_ID/artifacts/{artifact_id}?tail_bytes=N
POST /runs/RUN_ID/cancel
GET /events?limit=N&kind=KIND&since=RFC3339&until=RFC3339&query=TEXT
GET /workflows?priority=low|normal|high|critical|urgent&task=TASK_ID&since=RFC3339&until=RFC3339&query=TEXT&limit=N
GET /workflows/WORKFLOW_ID
GET /workflows/WORKFLOW_ID/status
GET /workflows/WORKFLOW_ID/dag
POST /workflows/WORKFLOW_ID/tasks
POST /workflows/WORKFLOW_ID/link
POST /workflows/WORKFLOW_ID/unlink
POST /workflows/WORKFLOW_ID/pause
POST /workflows/WORKFLOW_ID/resume
POST /workflows/WORKFLOW_ID/retry
POST /workflows/WORKFLOW_ID/run
POST /workflows/WORKFLOW_ID/cancel
POST /workflows
DELETE /workflows/WORKFLOW_ID
GET /approvals
POST /approvals/APPROVAL_ID/approve
POST /approvals/APPROVAL_ID/deny
GET /workers?status=online|up|busy|paused|pause|offline|down&since=RFC3339&until=RFC3339&query=TEXT&limit=N
POST /workers
GET /workers/WORKER_ID
POST /workers/WORKER_ID/heartbeat
POST /workers/WORKER_ID/claim
POST /workers/WORKER_ID/report
DELETE /workers/WORKER_ID
GET /evals?target=TARGET&success=true|false&since=RFC3339&until=RFC3339&query=TEXT&limit=N
POST /evals
POST /evals/run
GET /evals/EVAL_ID
GET /git/status?cwd=PATH
POST /git/review-task
GET /registry
GET /registry/profiles
GET /registry/profiles/PROFILE_ID
POST /registry/profiles/PROFILE_ID/agents
GET /registry/templates
GET /registry/templates/TEMPLATE_ID
POST /registry/templates/TEMPLATE_ID/workflows
POST /registry/marketplace-import
GET /registry/mcp-servers
POST /registry/mcp-servers
GET /registry/mcp-servers/SERVER_ID
POST /registry/mcp-servers/SERVER_ID
DELETE /registry/mcp-servers/SERVER_ID
GET /secrets?kind=environment|1password|os-keychain|env-vault&query=TEXT&limit=N
GET /secrets/check
POST /secrets
GET /secrets/BACKEND_ID
DELETE /secrets/BACKEND_ID
GET /memory?query=TEXT&tag=TAG&visibility=shared|private&scope=SCOPE&since=RFC3339&until=RFC3339&limit=N
GET /memory/recall?query=TEXT&tag=TAG&visibility=shared|private&scope=SCOPE&since=RFC3339&until=RFC3339&limit=N
POST /memory
POST /memory/prune
GET /memory/MEMORY_ID
POST /memory/MEMORY_ID
DELETE /memory/MEMORY_ID
GET /openapi.json
```

Use `api serve --token-env AGENT_OS_API_TOKEN` or `api serve --token-file ./api.token` to require a full-access `Authorization: Bearer <token>` on API requests. Token files must be regular files, are trimmed, and tokens must be at least 8 bytes, nonempty, and free of whitespace or control characters; on Unix, symlink token files are rejected and token files must not be readable, writable, or executable by group or others, so use `chmod 600 ./api.token`. Use token files for launchd/systemd setups where keeping the token out of process arguments and shell startup files is preferable. Repeated `Authorization` headers are rejected as unauthorized. For scoped local clients, use `--read-token-env` or `--read-token-file` for GET-only access and `--write-token-env` or `--write-token-file` for POST/DELETE mutation access; read and write tokens must be distinct, and a token presented outside its scope receives `403 Forbidden`. Non-loopback binds require one of `--token-env`, `--token-file`, `--read-token-env`, `--read-token-file`, `--write-token-env`, or `--write-token-file` unless `--unsafe-no-token` is passed explicitly. Browser CORS requests are restricted to missing origins or loopback origins such as `http://localhost`, `http://127.0.0.1`, and `http://[::1]`; repeated `Origin` headers receive `400 Bad Request`, and other origins receive `403 Forbidden` unless explicitly added with repeated `--allow-origin https://dashboard.example` flags.
Use `api schema` or `GET /openapi.json` to inspect the supported API contract, including mutation request bodies, validation constraints, stable operation IDs, endpoint tags, local-origin CORS/cache-control headers, and standard error responses. The schema endpoint remains available for client discovery even when state is unavailable.
API mutation bodies with content must use `Content-Type: application/json`, and mutation bodies reject unknown fields so misspelled request keys fail early.
The local HTTP parser rejects oversized headers/bodies, non-UTF-8 headers, malformed request lines, malformed header lines, invalid header names, duplicate `Host` headers, conflicting `Content-Length` headers, and unsupported HTTP versions before routing a request. Parser behavior is covered by targeted malformed-request tests plus bounded property tests over arbitrary request bytes and generated valid requests.

## Runtime Model

- Agents advertise capabilities and a maximum parallel task count.
- OS names from config or `init --name` must not be empty.
- Capability lists reject empty capability entries.
- CLI and API agent, task, and tool collections can be filtered by capability; comma-separated capability filter values require all listed capabilities.
- Agent kinds can be built-in or custom, but they must not be empty; CLI/API agent collections can be filtered by kind, RFC3339 update time window, or agent text, and agent lists return the most recently updated matching agents first.
- Agent-specific model overrides are optional, but provided model values must not be empty.
- Agent `parallel` capacity values must be greater than zero.
- Agent names must normalize to a non-empty ID, and agent IDs are unique. Generated task, run, workflow, memory, and approval IDs are retried against current state before insertion so a random collision cannot overwrite existing records.
- Operators can create and inspect agents, update stable-ID agent metadata/capabilities/capacity, update agent heartbeat/status, claim agent work, and remove unreferenced agents from the CLI, local API, or dashboard.
- Agent updates reject empty names, kinds, models, capability sets, and capacity values; they also reject changes that would leave current running work over capacity or missing required capabilities.
- Agent status values are stored as `online`, `busy`, `paused`, or `offline`; CLI/API status inputs also accept `up`, `pause`, and `down` aliases.
- Operator commands and API path parameters reject empty or malformed resource IDs before reading or mutating state.
- Worker agents can heartbeat, claim ready tasks, and report completion or failure through the CLI or local API. Distributed worker nodes can also be registered, listed, heartbeated, inspected, removed, and connected to scheduling through `worker claim WORKER_ID` or `POST /workers/WORKER_ID/claim`; worker claim uses the existing agent scheduler and requires a matching agent ID. `worker heartbeat WORKER_ID --lease-seconds N` also refreshes the matching agent lease when one exists, so distributed workers can keep scheduler claimability alive without sending a separate agent heartbeat. `worker report WORKER_ID TASK_ID` and `POST /workers/WORKER_ID/report` require that matching agent to own the running task, then finish the task and persist a run record with command, cwd, exit code, and artifact metadata. Eval records can be written and queried through `eval ...` commands or `/evals` API routes with target, success, time-window, query, and limit filters. The local dashboard can register, heartbeat, claim, report, and remove worker nodes, plus record manual eval results or run policy-checked command evals with optional success-pattern and output-schema validation. `eval run` and `POST /evals/run` execute a policy-checked local shell command, enforce `policy.command_timeout_seconds`, optionally require an output substring, optionally validate stdout JSON with `--output-schema` or `output_schema`, measure latency, and persist the resulting eval record with command, cwd, bounded stdout/stderr, exit status, timeout flag, pattern-match result, and output schema validity/error details.
- Git workspace automation is available from the CLI; the local API exposes read-only status through `GET /git/status?cwd=PATH` and can create code-review tasks with `POST /git/review-task` for dashboards and integrations. The local dashboard can inspect workspace status and create review tasks from the same Git workspace panel.
- Secret manager backend metadata can be listed, registered, inspected, and removed through `secrets ...` commands or `/secrets` API routes; the local dashboard can register, inspect, and remove backends, plus run redacted checks. `secrets check` and `GET /secrets/check` scan task tool invocations for secret references and report only task/tool/arg/reference presence metadata, never resolved values. Agent OS stores backend kind/reference metadata and secret argument references, not resolved secret values. Bare `--secret-arg key=ENV_VAR` values still resolve from the process environment; `backend-id:name` values resolve through registered backends. `env-vault` reads a JSON object from its `reference` environment variable, `one-password` shells out to `op read`, and `os-keychain` shells out to the macOS `security` generic-password lookup.
- `agent-os mcp serve` exposes Agent OS as an MCP stdio server for external agents, including status, task creation, memory search, approval listing, approval resolution, read-only git status, and redacted secrets-check tools; JSON resources for status, tasks, workflows, memory, runs, approvals, workers, evals, registry metadata, secret backend metadata, and the current policy/autonomy posture; and reusable task-planning and memory-brief prompts. Enabled registry MCP servers are proxied into the same `tools/list`, `resources/list`, and `prompts/list` responses with namespaced tool or prompt names such as `mcp_SERVER__tool_name` and proxied resource URIs such as `mcp+agent-os://SERVER/remote-uri`; calls, resource reads, and prompt gets route to the registered stdio server command and return its MCP result. Timed-out proxied MCP server commands are terminated with the shared process-tree cleanup path so helper descendants are not left running after a failed proxy call.
- `registry marketplace-import` installs marketplace JSON manifests containing optional `metadata` plus `agent_profiles`, `workflow_templates`, and `mcp_servers`; duplicate IDs require `--force`, imports write audit events with a content checksum, and `--expect-checksum CHECKSUM` rejects tampered or unexpected manifest contents before mutating state. `POST /registry/marketplace-import` accepts the same manifest shape in a `manifest` body, optional `source`, optional canonical manifest `expect_checksum`, and `force` for API-driven marketplace installs. Workflow templates can stay simple with a linear `stages` list, or add per-stage `tasks` metadata plus explicit `edges` for DAG templates; task `title`, `objective`, and `command` values can include `{objective}` placeholders. The API exposes registry inspection through `/registry`, `/registry/profiles`, `/registry/templates`, and `/registry/mcp-servers`, `POST /registry/profiles/PROFILE_ID/agents` installs an agent from a reusable profile with optional name/model/capacity overrides, `POST /registry/mcp-servers` registers MCP server definitions, `POST /registry/mcp-servers/SERVER_ID` updates command, args, env, or enabled state, `DELETE /registry/mcp-servers/SERVER_ID` removes definitions, and `POST /registry/templates/TEMPLATE_ID/workflows` creates a dependency graph from a reusable workflow template. The local dashboard can import marketplace manifests, install agents from profiles, list installed workflow templates, create workflows from them, and manage registered MCP servers.
- Agent heartbeats and claims can include `lease_seconds`; expired leases are marked offline before scheduler ticks.
- Explicit `lease_seconds` values must be positive.
- Tasks declare required capabilities and a priority.
- Task titles must not be empty.
- Task objectives, when provided, must not be empty.
- Task shell commands, when provided, must not be empty.
- Task and workflow priorities are stored as `low`, `normal`, `high`, or `critical`; CLI/API priority inputs also accept `urgent` as a `critical` alias. CLI/API task status filters accept `pending`, `running`, `blocked`, `complete`, `completed`, `failed`, `cancelled`, or `canceled`, and task collections can be filtered by assigned agent.
- The scheduler selects the highest effective-priority pending task and assigns it to the least-loaded online agent that satisfies all required capabilities. Ties between capable agents prefer the least-recently updated agent before creation time, which rotates work across idle agents instead of repeatedly favoring the oldest registration. Ready tasks gain a bounded in-memory scheduling boost as they wait, so older work is not starved by a steady stream of newer high-priority tasks; stored task priority is unchanged, and critical work remains the ceiling. Each assignment increments the task's durable `attempts` counter, and tasks can opt into bounded automatic executor retries with `--max-attempts N` or API `max_attempts`; failed attempts are requeued until the cap is reached, so manual retries and repeated scheduling are auditable across daemon restarts without changing default one-attempt behavior. Scheduler reports include dependency-cycle deadlocks plus structured `unscheduled_tasks` reasons for blocked work, dependency deadlocks, incomplete dependencies, missing capabilities, expired leases, agent capacity, and scheduler limits, so `run --dry-run` and `POST /run` can explain why work did not move without mutating state.
- Operators can manually assign a ready pending task to a specific online agent; the runtime enforces dependency, lease, capacity, and capability checks before mutating state.
- Operators can reprioritize tasks while they are pending or blocked from the CLI or API; the change is audited as a task update event.
- Operators can update pending or blocked task specs from the CLI or API, including title, objective, command, tool invocation, cwd, and required capabilities; command, tool invocation, cwd, and required capabilities can also be cleared. Running or terminal task spec edits are rejected to preserve execution history.
- Operators can replace or clear task dependencies while a task is pending or blocked; state validation rejects missing dependencies, duplicates, self-dependencies, and dependency cycles.
- Task dependencies are respected; a task with `--after` is not scheduled until every dependency is complete.
- Task dependency IDs and tool references are validated when tasks are created, task collections can be filtered by tool, dependency, RFC3339 update time window, or task text, task lists return the most recently updated matching tasks first, and lifecycle controls can cancel, retry, or unblock tasks while releasing agent capacity. Task deletion refuses running tasks and tasks that are still referenced by dependent tasks, workflow stages, or run history.
- Task plans can be replaced while a task is pending or blocked and must include at least one non-empty step.
- Task lifecycle notes are optional, but provided notes must not be empty.
- `workflow create` generates a dependency-aware planner -> builder -> reviewer task chain for an objective and records a durable workflow that can be listed, filtered, searched by workflow/task text, inspected for progress, edited as a DAG with `workflow add-task`, `workflow link`, and `workflow unlink`, paused/resumed/retried by stage status, advanced one stage or all ready stages, or removed from the CLI or API without deleting generated tasks. The API exposes the same DAG editing surface with `GET /workflows/WORKFLOW_ID/dag` plus `POST /workflows/WORKFLOW_ID/tasks`, `/link`, `/unlink`, `/pause`, `/resume`, and `/retry` so dashboards can render and manipulate workflow nodes, dependency edges, and external dependencies directly. CLI JSON and API workflow create/run responses include `runs` and `errors` arrays; API workflow creation can also immediately execute the first runnable stage with `{"execute":true}`.
- Workflow objectives must not be empty.
- `run --execute --limit N` runs scheduled task commands concurrently up to the selected limit, records each run, and writes logs under `runs/`; `run --dry-run` and `POST /run` with `{"dry_run":true}` preview scheduler assignments and stale recovery without mutating state or executing commands. CLI JSON and API scheduler-run responses include `runs`, `errors`, and a scheduler report that lists recovered task IDs, recovered active run IDs, and whether stale daemon state was cleared. CLI/API run status filters accept `running`, `cancel-requested`, `cancel_requested`, `cancelled`, `canceled`, `success`, `succeeded`, `failed`, or `rejected`, and run history can be filtered by task, assigned agent, RFC3339 start time window, or command text with the most recent matching runs first.
- Scheduler run limits must be greater than zero; stale recovery windows must be zero or positive.
- Tools are durable command templates with required capabilities. `task create --tool TOOL --arg key=value` invokes the registered tool with shell-quoted arguments and the normal policy checks; tool args are rejected unless a tool is selected. Pending and blocked tasks can replace or clear their tool invocation through `task update`. Duplicate or unused tool arg keys are rejected. Secret-like inputs should use `--secret-arg key=ENV_VAR` or `--secret-arg key=backend-id:name`, which resolves at execution time and redacts the value from state and logs; secret args cannot share a key with plain args. The local dashboard can create, inspect, update, and remove tool definitions through the same API paths used by integrations.
- Tool kinds and API tool kind filters accept `shell`, `file-read`, `read-file`, `file-write`, or `write-file`, tool collections can be filtered by RFC3339 update time window or tool text, and tool lists return the most recently updated matching tools first.
- Tool names must normalize to a non-empty ID, tool command templates must not be empty, and every brace in a template must be part of a closed `{arg}` placeholder without surrounding whitespace; tool IDs are unique.
- Operators can update tool kind, description, required capabilities, command template, and default cwd from the CLI or API; description, required capabilities, and default cwd can also be cleared. Updates preserve tool IDs, reject incompatible existing task arguments, and write audit events.
- Approval gates can be inspected and resolved from the CLI, API, or local dashboard. `GET /approvals` returns pending and resolved gate records, while `POST /approvals/APPROVAL_ID/approve` and `/deny` accept an optional `{"by":"operator"}` body and write approval-resolution audit events. Approving a gate moves its blocked task back to pending once no other approval for that task is pending; denying a gate fails the blocked task. Resolved gates are final and later resolve calls leave the original decision intact.
- Tool removal refuses tools that are still referenced by tasks, preserving state validity.
- Built-in `file-read` and `file-write` tools use the command template as a path template and enforce workspace policy without invoking a shell. `file-write` expects a `body` argument, which can be supplied as a plain arg or `--secret-arg body=ENV_VAR`.
- Tasks without `--command` are completed through the local mock provider, which gives deterministic agent output for tests and offline workflows. Provider responses can request registered tool calls; the runtime turns those into dependent tool tasks instead of executing them inline.
- Task commands and tools can declare `--cwd`; provided working directories must not be empty, and the runtime rejects work outside configured workspace allowlists.
- Task lifecycle changes release agent capacity and write audit events.
- CLI and API event lists return newest matching events first; limits must be greater than zero, event kind filters are validated, event `since`/`until` filters must be RFC3339 timestamps, and event queries search messages case-insensitively.
- Stale running tasks can be recovered to pending manually with `task recover`, through `POST /tasks/recover`, or before scheduler ticks with `--recover-stale-seconds`. Recovery also fails stale active run records that no longer have a running task, clears stale agent capacity for those orphaned run records, and clears crashed daemon state when the recorded daemon PID is no longer alive, using Unix signal checks or Windows `tasklist` PID checks where available.
- Manual stale recovery windows must be zero or positive.
- Memory records are searchable by topic, body, and tags through the CLI, API, and local dashboard, memory collections can be filtered by exact tag or RFC3339 update time window, and memory searches rank exact and token-overlap matches before recency. `memory recall` and `GET /memory/recall` return RAG-ready scored hits with compact snippets and nested source records for prompt assembly or citation in dashboards.
- Memory records can be added, inspected, updated, pruned by age, and removed by ID from the CLI, API, and local dashboard; prune dry-runs and removals report expired records oldest-first. Updates can replace or clear tags, set `shared`/`private` visibility, and set or clear a scope. Memory writes and updates deduplicate existing records with the same normalized topic, trimmed body, normalized tag set, visibility, and scope so repeated provider or operator memory writes and edits do not grow shared memory indefinitely. Providers only receive non-expired `shared` memory records, and scoped records are included only when their scope matches `memory_policy.scope`; `private` memory stays available to local operators without being sent to providers. `memory_policy.max_provider_memories` caps memory sent to providers, `memory_policy.semantic_recall = true` ranks provider memory by relevance to the current task before prompting, and `memory_policy.max_age_days` can be used by `memory prune` or `POST /memory/prune` for expiry.
- Memory topics, bodies, and tag entries must not be empty.
- Memory search queries must not be empty.
- State can be exported, imported, backed up, migrated, pruned, and validated for unsupported versions, dangling references, malformed durable fields, optional metadata drift, timestamp order drift, dependency cycles, task output drift, daemon metadata drift, run command/exit-code/lifecycle drift, and assignment index consistency. State import/export/backup/migrate paths must not be empty when provided. `GET /state/export` returns the exact durable state snapshot, `POST /state/export` accepts `{"output":"state.json","dry_run":true}` to preview the export path without writing or `{"output":"state.json"}` to write an atomic export file, `POST /state/import` accepts `{"path":"state.json","force":true,"dry_run":true}` with the same validation and overwrite checks as the CLI while dry-run previews without persisting, `POST /state/migrate` accepts `{"input":"legacy.json","output":"state.json","dry_run":true}` with both paths defaulting to the active state and dry-run previewing without persisting, migration dry-runs print planned schema steps plus validation success before persisting, migration responses include `output_preexisting` so dry-runs can distinguish creating from overwriting the target, and migration reports include downgrade notes because automatic schema downgrade is not supported; keep a pre-migration backup/export for rollback with older binaries. `POST /state/backup` accepts `{"output":"backup.json","dry_run":true}` or uses a timestamped default path, with dry-run previewing without writing a backup file; active SQLite-state backups write the validated JSON snapshot rather than raw database bytes. `POST /state/repair` accepts `{"dry_run":true}` to preview repairs without persisting, `POST /state/prune` accepts `{"keep_runs":100,"keep_events":500,"dry_run":false}` for API-driven maintenance, and `POST /state/sqlite` accepts `{"output":"state.sqlite"}`, `{"init_only":true}`, or `{"restore":true,"force":true,"dry_run":true}` for API-driven SQLite mirror sync and recovery.
- `state sqlite` and `POST /state/sqlite` initialize or sync a SQLite backend with the current state snapshot, queryable task records, queryable event records, queryable memory records, per-run records, and available run logs; JSON output reports imported run count, imported log count, and skipped missing-log count so high-run sync jobs can detect incomplete log transfer. The local dashboard exposes state validation, snapshot read/export, import, migration, SQLite mirror, restore, dry-run, force, backup, repair, and prune maintenance actions. Task rows expose status, priority, assignment, `attempts`, and `max_attempts` columns for dashboards without parsing the snapshot body. `state sqlite --restore --output PATH [--force] [--dry-run]` or `POST /state/sqlite` with `{"restore":true}` validates the SQLite `current` snapshot and restores it into the configured state path, making the SQLite backend usable for recovery as well as mirroring. For active SQLite state, point global `--state` at a `.sqlite`, `.sqlite3`, or `.db` path; active saves keep the `current` snapshot plus `task_records`, `event_records`, `memory_records`, and `run_records` tables synchronized with durable state, while run-log writes mirror available logs into `run_logs`.
- `state repair` fixes repairable OS name drift, assignment index drift, dependency drift, workflow stage/task drift, dangling non-running task assignments, optional metadata drift, timestamp order drift, task plan/output drift, provider default/env/empty-endpoint drift, policy list/env/limit drift, zero agent capacity, daemon limit/metadata drift, expired agent leases, repairable run command/cwd/exit-code drift, repairable tool invocation argument drift, capability/tag normalization drift, and stopped daemon state drift.
- `config init`, `config show`, `config validate`, `GET /config`, `POST /config`, and `GET /config/validate` let operators create, inspect, and validate config from the CLI or API. The local dashboard shows the active policy/autonomy posture from state, can inspect or validate config directly, and can write guided config profiles with optional forced replacement. `config init` and `POST /config` default to the locked-down `safe` profile; pass `--profile dev` or `{"profile":"dev"}` only when local shell execution is intentionally needed. `config init --profile safe|dev|autonomous|ci` and `POST /config` with a `profile` value write guided policy profiles for locked-down local use, development, autonomous execution, or CI. `config show --json` and `GET /config` return the config path, existence flag, and loaded config, or the default safe config when no config file exists. `config validate --json` and `GET /config/validate` report `config_valid:null` when no config file exists, and include `config_error` only for load/parse failures. `POST /config` accepts `{"force":true}` for controlled replacement.
- `init` and `POST /init` initialize durable state from the effective config. When no config file exists, the effective default is the locked-down `safe` profile; pass `--profile dev` or `{"profile":"dev"}` only when local shell execution is intentionally needed. API init accepts `{"name":"My OS","force":true}` for an optional name override and controlled replacement of existing state.
- `doctor` and `GET /doctor` report platform/service guidance, shell-execution support, state paths, state load errors, state validation status, config load/semantic validation issues, and actionable next-step commands before operators initialize or run the OS.
- `daemon run` keeps the scheduler alive as a service loop and persists heartbeat/status in state. `daemon run --json` emits one final response with the daemon state, aggregate scheduler totals, and a bounded recent tick history.
- Daemon and service intervals must be greater than zero; daemon `max_ticks`, when provided, must also be greater than zero.
- `daemon stop` and `POST /daemon/stop` record a stop request that a running daemon observes between ticks, and `GET /daemon` returns the durable daemon state for dashboards and supervisors. `daemon stop --json` mirrors the API response with `stop_requested` and the updated daemon state.
- `service launchd` renders a macOS launchd plist for running the daemon under a supervisor, `service systemd` renders a Linux systemd user unit, and `service windows-task` renders a PowerShell script for registering a Windows Scheduled Task. The systemd renderer quotes whitespace-sensitive arguments and escapes `%` specifiers in paths before writing `ExecStart`; the Windows renderer quotes paths and arguments for `New-ScheduledTaskAction`.
- `POST /service/launchd` and `POST /service/systemd` render supervisor definitions and service metadata for API-driven setup without installing or starting the service; the local dashboard exposes the same safe render flow before an operator installs anything, plus install, uninstall, start, stop, and status controls for both supervisors. The matching `/install` endpoints write definitions atomically, and `/uninstall` removes the selected definition when present.
- `service install` and `service uninstall` write or remove the user launchd plist; `service start`, `service stop`, and `service status` control the LaunchAgent through `launchctl`. `service install-systemd` and `service uninstall-systemd` write or remove the user unit, and `service start-systemd`, `service stop-systemd`, and `service status-systemd` control it through `systemctl --user`. The API mirrors both supervisors with `POST /service/launchd/*` and `POST /service/systemd/*` render, install, uninstall, start, stop, and status endpoints. Service labels, launchd domains, launchctl/systemctl paths, binary paths, plist paths, and unit paths must not be empty when provided.
- `api serve` exposes state and authenticated mutation over a small local HTTP server for dashboards, monitors, and agent tools.
- API collection filters reject unsupported query parameters so integration typos fail early.
- API request caps must be greater than zero, and token-protected API serving rejects empty token environment names, non-regular token files, symlink token files on Unix, empty token files, short token values, whitespace/control characters in tokens, Unix token files accessible by group or others, and identical read/write scoped tokens.
- `metrics` and `GET /metrics` expose the same counter snapshot for scripts, dashboards, and monitors, returning `ok=false` with `state_loads=false` when state is missing or unavailable. `metrics --prometheus` and `GET /metrics/prometheus` expose the same counters in Prometheus text format for local scraping, including the age of the oldest active run for stuck-run alerts and queued-task age histograms for backlog alerts.
- `GET /health` includes the running `agent-os` binary version, state load status, state load errors, state validation status, config load status, config semantic validation status, and issue counts for monitors that need a quick health signal.
- `GET /metrics` returns a stable JSON counter snapshot for the running `agent-os` binary version, agents, tasks, queued-task age histogram buckets, runs, run duration histogram buckets, tools, events, memories, daemon status, and state health. Run records include a durable `trace_id`, run logs include that `trace_id` near the top, and every API response includes an `X-Trace-Id` header for request-level client correlation.
- `POST /run` performs one scheduler tick and can execute assigned tasks when sent `{"execute":true,"limit":N}` or preview assignments without changing state when sent `{"dry_run":true,"limit":N}`.
- `workflow run`, `workflow cancel`, `POST /workflows/WORKFLOW_ID/run`, and `POST /workflows/WORKFLOW_ID/cancel` advance or cancel workflow stages without letting unrelated higher-priority work take over the workflow action.
- `runs logs --tail-bytes N`, `runs tail --tail-bytes N`, `runs replay --tail-bytes N`, `runs debug --tail-bytes N`, `GET /runs/RUN_ID/logs?tail_bytes=N`, `GET /runs/RUN_ID/replay?tail_bytes=N`, and `GET /runs/RUN_ID/debug?tail_bytes=N` return bounded log output for large runs. `GET /runs/RUN_ID/logs?tail_bytes=N` is the non-following API equivalent for bounded tail reads; streaming follow mode is intentionally CLI-only through `runs tail --follow`.
- JSON/API log responses include a truncation flag when a byte limit removes part of the log.
- `runs replay` reconstructs a run from task state, run metadata, related events, and log output, and reports log read errors when the log is unavailable.
- `runs debug` and `GET /runs/RUN_ID/debug` extend replay with related agent, workflow, approval, artifact existence, and diagnostic counts for post-failure investigation. `runs artifacts` and `/runs/RUN_ID/artifacts` expose captured stdout, stderr, summary, diff, and file artifacts as first-class records with bounded reads, byte counts, content types, and stable checksums. The local dashboard can call run debug, replay, artifact list/read, and cancellation views by run ID.
- Shell run logs are written while the command is still running, and `runs tail --follow` streams them through the CLI.
- `runs tail --interval-ms` must be greater than zero.
- `runs cancel` and `POST /runs/RUN_ID/cancel` record a durable cancellation request that running shell executions observe and terminate. Cancelling a running task or workflow also marks active runs for cancellation. On Unix, cancellation and command timeouts terminate the spawned shell process group or descendant tree; on Windows they use `taskkill /T /F` to terminate the spawned process tree before falling back to a direct child kill. `runs cancel --json` mirrors the API response with `id`, `cancel_requested`, and the updated run record.
- Mutating CLI commands run through an exclusive store transaction; read-only commands and the API take shared locks.

## Policy

Generated configs use the `safe` profile by default, disabling shell execution until an operator explicitly opts into `dev`, `ci`, `autonomous`, or a custom policy. When shell execution is enabled, commands are checked before launch: the policy allows the current workspace, rejects destructive patterns such as `rm -rf`, `rm -fr`, `sudo`, `shutdown`, `reboot`, `mkfs`, and raw disk writes using `dd if=` or `dd of=`, and applies command allowlist and deny-pattern checks case-insensitively. Common destructive shell shapes such as `rm -r -f`, `rm --recursive --force`, `dd ... of=/dev/disk0`, direct privilege escalation with `sudo`/`doas`/`pkexec`/`su`, filesystem formatters such as `mkfs.ext4`, and system power commands such as `shutdown` or `reboot` are rejected after quote-aware tokenization, even if they do not exactly match a configured denied-pattern string.
If a shell task is rejected because shell execution is disabled, intentionally opt into a shell-enabled profile such as `dev`, `ci`, or `autonomous`, or set `[policy] allow_shell = true` in a reviewed config.

Command execution starts from a minimal allowlisted environment rather than inheriting every parent process variable. Values from allowed environment variables whose names match redaction patterns are replaced with `[redacted]` in run logs.

When `allowed_commands` is set, commands are also rejected if they contain unquoted shell control operators such as `;`, pipes, redirects, background operators, command substitution, or newlines.

Policy `rules` accept readable comma-separated clauses such as `allow cargo test, deny cargo publish, require approval for git push, deny writes outside src`, or `deny writes outside src/generated`. Any `allow ...` shell clause creates a rule allowlist for shell commands, `deny ...` clauses reject matching command text, and `require approval for PATTERN` raises the same approval gate as risky action patterns before shell execution can proceed. `deny writes outside PATH` applies to built-in file-write tools and common shell write preflight, resolving relative paths against allowed workspaces and shell task working directories.

Sandbox settings narrow filesystem effects for built-in tools and common shell writes. `allowed_workspaces` gates command working directories and file reads, while non-empty `sandbox.writable_paths` further restricts built-in file-write tools after symlink and parent resolution. When `sandbox.writable_paths` is set, shell preflight also checks common write commands such as `touch`, `mkdir`, `cp`, `mv`, `install`, `tee`, and output redirections like `>`/`>>`, rejecting targets that are outside the writable set or cannot be resolved statically. On Unix, `sandbox.process_isolation = true` starts shell commands in their own process group so cancellation and timeouts can terminate descendants together; Windows cancellation uses the platform process-tree killer. When `network.mode = "allowed"` and `network.allowed_hosts` is non-empty, common shell network commands such as `curl`, `wget`, `ssh`, `scp`, `rsync`, and remote `git` commands must name a matching host directly in the command. Network-command detection is token-aware, so quoted text does not count as a network command and additional git network subcommands such as `ls-remote` are checked.

## Configuration

`agent-os.toml` can seed OS name, agents, tools, provider settings, and policy before `init` writes state. Use `--config PATH` or `AGENT_OS_CONFIG` to point at a specific file. Seeded agents and tools are validated with the same uniqueness and capacity rules as CLI/API creation.
Unknown config keys are rejected so typos fail before state is initialized.
Provider and policy config values reject empty names, patterns, workspaces, invalid or whitespace-padded environment variable names, and missing or malformed OpenAI-compatible HTTP(S) endpoints.
Provider kind config accepts `mock`, `openai`, `anthropic`, `gemini`, `ollama`, `local`, `plugin`, `openai-compatible`/`open-ai-compatible`, or `custom`, and stores the canonical kind in state. `custom` can set `adapter = "openai-compatible"`, `"anthropic"`, `"gemini"`, or `"ollama"` to reuse a provider request/response shape with a custom endpoint. `request_options` passes model-specific top-level options such as temperature, token limits, or streaming flags, but validation rejects adapter-owned request fields like `model`, `messages`, Anthropic `system`, or Gemini `contents` so config errors do not get silently ignored. `plugin` requires `plugin_command` and can set `plugin_args` plus `plugin_env`.
Tool kind config accepts `shell`, `file-read`, `read-file`, `file-write`, or `write-file`, and stores the canonical kind in state.
Config profiles make the policy posture explicit: `safe` disables shell execution and requires approval for risky actions, `dev` keeps the historical local development defaults, `autonomous` allows unattended local execution, and `ci` restricts shell execution to cargo/rustc/git with provider network disabled. Missing policy fields default to the `safe` posture: provider-only network access, risky-action approval, and `execute-with-approval` autonomy.
Generated configs and no-config initialization default to `safe`; use `agent-os config init --profile dev` or `agent-os init --profile dev` only when you intentionally want local shell execution.
Policy `max_output_bytes` must be greater than zero so run logs remain inspectable.
Policy `command_timeout_seconds` must be greater than zero so commands have a real execution window.

```toml
name = "Agent OS"

[policy]
allow_shell = false
allowed_commands = []
allowed_workspaces = ["."]
denied_patterns = ["rm -rf", "rm -fr", "sudo", "shutdown", "reboot", "mkfs", "dd if=", "dd of="]
max_output_bytes = 131072
command_timeout_seconds = 300
inherit_environment = false
allowed_env_vars = ["PATH", "HOME", "USER", "TMPDIR", "TMP", "TEMP", "RUSTUP_HOME", "CARGO_HOME"]
redacted_env_patterns = ["KEY", "TOKEN", "SECRET", "PASSWORD", "AUTH", "CREDENTIAL"]
autonomy = "execute-with-approval"

[policy.sandbox]
process_isolation = true
jailed_workspaces = true
writable_paths = []

[policy.network]
mode = "providers-only"
allowed_hosts = []

[policy.approval]
require_for_risky_actions = true
risky_patterns = ["git push", "git commit", "rm ", "mv ", "chmod", "curl ", "wget ", "ssh "]

[provider]
kind = "mock"
model = "mock-agent"
# kind = "openai-compatible" # also accepts openai, anthropic, gemini, ollama, local, plugin, custom
# adapter = "anthropic" # optional for custom endpoints; accepts openai-compatible, anthropic, gemini, ollama
# endpoint = "https://api.openai.com/v1/chat/completions"
# model = "gpt-4.1-mini"
# api_key_env = "OPENAI_API_KEY"
# plugin_command = "agent-os-provider-plugin"
# plugin_args = ["--json"]
# plugin_env = { AGENT_OS_PROVIDER_PLUGIN_TOKEN = "local-dev-token" }
request_timeout_seconds = 30
# Provider retries are capped at 8 to keep failed calls bounded.
max_retries = 2
retry_backoff_ms = 250
# request_options = { temperature = 0.2, top_p = 0.9, stream = true }

[memory_policy]
max_provider_memories = 5
semantic_recall = true
max_age_days = 30

[[agents]]
name = "builder"
kind = "builder"
model = "local-default"
capabilities = ["code", "rust", "test"]
parallel = 2

[[tools]]
name = "cargo-test"
kind = "shell"
description = "Run the Rust test suite"
required_capabilities = ["rust", "test"]
command_template = "cargo test"

[[tools]]
name = "read-note"
kind = "file-read"
description = "Read a note from the workspace"
required_capabilities = ["rust"]
command_template = "notes/{name}.txt"
```

## Providers

The runtime has a provider boundary for agent work. The default `provider:mock` path is local and deterministic, so non-command tasks can still be planned, completed, audited, and logged without network access or API keys. External HTTP provider kinds use provider-specific adapters: `openai`/`openai-compatible` use chat completions with bearer auth, `anthropic` uses the Messages API with `x-api-key` and `anthropic-version`, `gemini` uses `generateContent` with `x-goog-api-key`, and `ollama` uses `/api/chat` with `stream=false` and JSON format. `custom` defaults to the OpenAI-compatible adapter unless `adapter` selects another shape. `plugin` runs `plugin_command` with `plugin_args`, sends the normal `ProviderRequest` JSON on stdin, applies `plugin_env`, enforces `request_timeout_seconds`, retries transient plugin process failures with capped `max_retries` and exponential `retry_backoff_ms`, terminates the plugin process tree on timeout where the platform supports it, and parses stdout as the same structured provider response contract. The dashboard exposes the active provider kind, model, adapter, endpoint, plugin command, plugin args, plugin environment keys, request option keys, retry policy, and whether a structured `response_schema` is configured without showing provider secret values. The API key is read from `api_key_env`, network calls use `request_timeout_seconds`, and transient transport, rate-limit, and 5xx failures are retried with capped `max_retries` values, capped exponential `retry_backoff_ms`, and numeric or HTTP-date `Retry-After` response headers when providers send them. HTTP provider response bodies are capped before JSON or streaming chunk parsing so a misbehaving endpoint cannot force unbounded memory growth. `request_options` can pass model-specific request fields such as `temperature`, `top_p`, `max_tokens`, or OpenAI-compatible `stream = true`; streamed chat-completion chunks are aggregated into the final provider response before JSON validation and tool-call materialization. Agent OS owns adapter contract fields such as `model`, `messages`, Anthropic `system`, and Gemini `contents`, and validation rejects overriding values there. Provider requests include the assigned agent, task, matching registered tools, and recent shared memory allowed by visibility/scope policy. Providers are asked for strict JSON with `summary`, `plan`, `confidence`, and optional `tool_calls`; configured `response_schema` values require JSON responses and validate common JSON Schema constraints including `type`, `required`, `properties`, `items`, `enum`, and `additionalProperties:false`. Plain text responses remain supported only when no `response_schema` is configured. Tool calls are materialized as normal dependent tool tasks, so policy checks, approval gates, logs, and scheduling still happen through the runtime. When approval for risky actions or `execute-with-approval` autonomy is enabled, provider-requested tool calls queue approval requests before any dependent tool task is created. If an assigned agent has a model, it overrides the global provider model for that request.

## Troubleshooting

- Run `agent-os doctor` first; it reports state, config, and validation problems in one place.
- Use `--state ./sandbox` while learning so experiments do not touch the default user state.
- If `api serve --addr 0.0.0.0:7373` fails, set a nonempty token env with `--token-env ENV`, pass a nonempty token file with `--token-file PATH`, configure scoped read/write envs or token files, or use `--unsafe-no-token` only for isolated local testing.
- If execution is rejected, inspect the policy section in `agent-os.toml`; `allowed_workspaces`, `allowed_commands`, and `denied_patterns` are enforced before commands or file tools run.
- Use `agent-os runs logs RUN_ID`, `agent-os runs tail RUN_ID`, `agent-os runs replay RUN_ID`, and `agent-os runs debug RUN_ID` to inspect failed or cancelled work.
- For launchd, verify the rendered plist with `agent-os service launchd` before installing, and check the configured binary path plus launchd stdout/stderr log paths.

## Development

```bash
./scripts/ci.sh
AGENT_OS_RELEASE_CHECK=1 ./scripts/ci.sh
```

The default CI script checks formatting, tests, docs with warnings denied, clippy, and package verification against the checked-in lockfile. Set `AGENT_OS_RELEASE_CHECK=1` to run the crates.io publish dry run with the same locked dependency resolution.
