# Agent OS

Agent OS is a local operating system for coordinating AI agents. It gives a team of agents a durable registry, prioritized task queue, capability-aware scheduler, shared memory, and event stream.

The implementation is intentionally local-first: state is a single JSON file, the Rust library owns the runtime behavior, and the CLI is a thin control surface over that runtime.
State reads and writes use a sidecar lock file so CLI commands, daemon ticks, and local API readers can coordinate safely around the same JSON state.

## Installation

From crates.io after release:

```bash
cargo install agent_os
agent-os --help
```

From a checkout:

```bash
cargo install --path .
agent-os --help
```

For development, `cargo run -- ...` works without installing the binary. The crate package builds and verifies with the release check documented below.

## Quick Start

```bash
cargo run -- config init
cargo run -- init --name "Infinite Agent OS"
cargo run -- status
cargo run -- doctor
cargo run -- agent list
cargo run -- task create "Build execution kernel" --need rust,code --priority high --command "cargo test"
cargo run -- run --limit 2 --execute
cargo run -- runs list
cargo run -- task list
cargo run -- api serve --addr 127.0.0.1:7373
```

Use `--state ./sandbox` to keep state inside a project directory. Global `--state` and `--config` paths, plus `AGENT_OS_HOME` and `AGENT_OS_CONFIG`, must not be empty:

```bash
cargo run -- --state ./sandbox init --force
```

## Commands

```text
agent-os init [--name NAME] [--force]
agent-os status
agent-os metrics
agent-os doctor
agent-os config init [--force]
agent-os config show
agent-os config validate
agent-os state export [--output PATH]
agent-os state import PATH [--force]
agent-os state backup [--output PATH]
agent-os state migrate [--input PATH] [--output PATH]
agent-os state prune [--keep-runs 100] [--keep-events 500] [--dry-run]
agent-os state repair [--dry-run]
agent-os state validate
agent-os agent add NAME [--kind KIND] [--model MODEL] --cap CAP [--parallel N]
agent-os agent list [--status online|up|busy|paused|pause|offline|down] [--kind KIND] [--cap CAP] [--since RFC3339] [--until RFC3339] [--query TEXT] [--limit N]
agent-os agent show AGENT_ID
agent-os agent update AGENT_ID [--name NAME] [--kind KIND] [--model MODEL] [--clear-model] [--cap CAP] [--parallel N]
agent-os agent heartbeat AGENT_ID [--status online|up|busy|paused|pause|offline|down] [--lease-seconds N]
agent-os agent claim AGENT_ID [--lease-seconds N]
agent-os agent remove AGENT_ID
agent-os task create TITLE [--objective TEXT] [--command SHELL] [--tool TOOL_ID] [--arg KEY=VALUE] [--secret-arg KEY=ENV_VAR] [--cwd DIR] [--priority low|normal|high|critical|urgent] [--need CAP] [--after TASK_ID]
agent-os task list [--all] [--status pending|running|blocked|complete|completed|failed|cancelled|canceled] [--priority low|normal|high|critical|urgent] [--agent AGENT_ID] [--tool TOOL_ID] [--after TASK_ID] [--cap CAP] [--since RFC3339] [--until RFC3339] [--query TEXT] [--limit N]
agent-os task show TASK_ID
agent-os task update TASK_ID [--title TITLE] [--objective TEXT] [--command SHELL] [--clear-command] [--tool TOOL_ID] [--clear-tool] [--arg KEY=VALUE] [--secret-arg KEY=ENV_VAR] [--clear-args] [--clear-secret-args] [--cwd DIR] [--clear-cwd] [--need CAP] [--clear-needs]
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
agent-os memory add TOPIC BODY [--tag TAG]
agent-os memory search QUERY [--tag TAG] [--since RFC3339] [--until RFC3339] [--limit N]
agent-os memory list [--tag TAG] [--since RFC3339] [--until RFC3339] [--limit N]
agent-os memory show MEMORY_ID
agent-os memory update MEMORY_ID [--topic TOPIC] [--body BODY] [--tag TAG] [--clear-tags]
agent-os memory remove MEMORY_ID
agent-os events [--limit 20] [--kind KIND] [--since RFC3339] [--until RFC3339] [--query TEXT]
agent-os runs list [--status running|cancel-requested|cancel_requested|cancelled|canceled|success|succeeded|failed|rejected] [--task TASK_ID] [--agent AGENT_ID] [--since RFC3339] [--until RFC3339] [--query TEXT] [--limit N]
agent-os runs show RUN_ID
agent-os runs logs RUN_ID [--tail-bytes N]
agent-os runs tail RUN_ID [--follow] [--tail-bytes N] [--interval-ms 200]
agent-os runs replay RUN_ID [--tail-bytes N]
agent-os runs cancel RUN_ID
agent-os daemon run [--limit 1] [--execute] [--interval-ms 1000] [--max-ticks N] [--recover-stale-seconds N]
agent-os daemon status
agent-os daemon stop
agent-os service launchd [--label LABEL] [--bin-path PATH] [--interval-ms 1000] [--limit 1] [--execute] [--recover-stale-seconds N] [--no-logs] [--plist-path PATH]
agent-os service install [--label LABEL] [--bin-path PATH] [--interval-ms 1000] [--limit 1] [--execute] [--recover-stale-seconds N] [--no-logs] [--plist-path PATH]
agent-os service uninstall [--label LABEL] [--plist-path PATH]
agent-os service start [--label LABEL] [--plist-path PATH] [--domain DOMAIN] [--launchctl-path PATH]
agent-os service stop [--label LABEL] [--plist-path PATH] [--domain DOMAIN] [--launchctl-path PATH]
agent-os service status [--label LABEL] [--domain DOMAIN] [--launchctl-path PATH]
agent-os completions bash|elvish|fish|powershell|zsh
agent-os api serve [--addr 127.0.0.1:7373] [--token-env ENV] [--max-requests N]
agent-os api schema
agent-os workflow create OBJECTIVE [--priority low|normal|high|critical|urgent] [--execute]
agent-os workflow list [--priority low|normal|high|critical|urgent] [--task TASK_ID] [--since RFC3339] [--until RFC3339] [--query TEXT] [--limit N]
agent-os workflow show WORKFLOW_ID
agent-os workflow status WORKFLOW_ID
agent-os workflow run WORKFLOW_ID [--all]
agent-os workflow cancel WORKFLOW_ID [--note TEXT]
agent-os workflow remove WORKFLOW_ID
agent-os run [--limit 1] [--execute] [--dry-run] [--recover-stale-seconds N]
```

Add `--json` to read commands and supported create/update commands when another program needs stable output.

The local API serves JSON for dashboards, tools, and local agent integrations:

```text
GET /health
GET /doctor
GET /config
POST /config
GET /config/validate
POST /init
GET /metrics
GET /status
GET /daemon
POST /daemon/stop
POST /service/launchd
POST /service/launchd/install
POST /service/launchd/uninstall
POST /service/launchd/start
POST /service/launchd/stop
POST /service/launchd/status
GET /state/export
POST /state/export
POST /state/import
POST /state/migrate
GET /state/validate
POST /state/backup
POST /state/repair
POST /state/prune
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
POST /runs/RUN_ID/cancel
GET /events?limit=N&kind=KIND&since=RFC3339&until=RFC3339&query=TEXT
GET /workflows?priority=low|normal|high|critical|urgent&task=TASK_ID&since=RFC3339&until=RFC3339&query=TEXT&limit=N
GET /workflows/WORKFLOW_ID
GET /workflows/WORKFLOW_ID/status
POST /workflows/WORKFLOW_ID/run
POST /workflows/WORKFLOW_ID/cancel
POST /workflows
DELETE /workflows/WORKFLOW_ID
GET /memory?query=TEXT&tag=TAG&since=RFC3339&until=RFC3339&limit=N
POST /memory
GET /memory/MEMORY_ID
POST /memory/MEMORY_ID
DELETE /memory/MEMORY_ID
GET /openapi.json
```

Use `api serve --token-env AGENT_OS_API_TOKEN` to require `Authorization: Bearer <token>` on API requests. This is strongly recommended when enabling mutating endpoints.
Use `api schema` or `GET /openapi.json` to inspect the supported API contract, including mutation request bodies, validation constraints, stable operation IDs, endpoint tags, CORS headers, and standard error responses. The schema endpoint remains available for client discovery even when state is unavailable.
API mutation bodies reject unknown fields so misspelled request keys fail early.

## Runtime Model

- Agents advertise capabilities and a maximum parallel task count.
- OS names from config or `init --name` must not be empty.
- Capability lists reject empty capability entries.
- CLI and API agent, task, and tool collections can be filtered by capability; comma-separated capability filter values require all listed capabilities.
- Agent kinds can be built-in or custom, but they must not be empty; CLI/API agent collections can be filtered by kind, RFC3339 update time window, or agent text, and agent lists return the most recently updated matching agents first.
- Agent-specific model overrides are optional, but provided model values must not be empty.
- Agent `parallel` capacity values must be greater than zero.
- Agent names must normalize to a non-empty ID, and agent IDs are unique.
- Operators can inspect agents, update stable-ID agent metadata/capabilities/capacity, update agent heartbeat/status, and remove unreferenced agents from the CLI.
- Agent updates reject empty names, kinds, models, capability sets, and capacity values; they also reject changes that would leave current running work over capacity or missing required capabilities.
- Agent status values are stored as `online`, `busy`, `paused`, or `offline`; CLI/API status inputs also accept `up`, `pause`, and `down` aliases.
- Operator commands and API path parameters reject empty or malformed resource IDs before reading or mutating state.
- Worker agents can heartbeat and claim ready tasks through the CLI or local API.
- Agent heartbeats and claims can include `lease_seconds`; expired leases are marked offline before scheduler ticks.
- Explicit `lease_seconds` values must be positive.
- Tasks declare required capabilities and a priority.
- Task titles must not be empty.
- Task objectives, when provided, must not be empty.
- Task shell commands, when provided, must not be empty.
- Task and workflow priorities are stored as `low`, `normal`, `high`, or `critical`; CLI/API priority inputs also accept `urgent` as a `critical` alias. CLI/API task status filters accept `pending`, `running`, `blocked`, `complete`, `completed`, `failed`, `cancelled`, or `canceled`, and task collections can be filtered by assigned agent.
- The scheduler selects the highest-priority pending task and assigns it to the least-loaded online agent that satisfies all required capabilities.
- Operators can manually assign a ready pending task to a specific online agent; the runtime enforces dependency, lease, capacity, and capability checks before mutating state.
- Operators can reprioritize tasks while they are pending or blocked from the CLI or API; the change is audited as a task update event.
- Operators can update pending or blocked task specs from the CLI or API, including title, objective, command, tool invocation, cwd, and required capabilities; command, tool invocation, cwd, and required capabilities can also be cleared. Running or terminal task spec edits are rejected to preserve execution history.
- Operators can replace or clear task dependencies while a task is pending or blocked; state validation rejects missing dependencies, duplicates, self-dependencies, and dependency cycles.
- Task dependencies are respected; a task with `--after` is not scheduled until every dependency is complete.
- Task dependency IDs and tool references are validated when tasks are created, task collections can be filtered by tool, dependency, RFC3339 update time window, or task text, task lists return the most recently updated matching tasks first, and lifecycle controls can cancel, retry, or unblock tasks while releasing agent capacity. Task deletion refuses running tasks and tasks that are still referenced by dependent tasks, workflow stages, or run history.
- Task plans can be replaced while a task is pending or blocked and must include at least one non-empty step.
- Task lifecycle notes are optional, but provided notes must not be empty.
- `workflow create` generates a dependency-aware planner -> builder -> reviewer task chain for an objective and records a durable workflow that can be listed, filtered, searched by workflow/task text, inspected for progress, advanced one stage or all ready stages, or removed from the CLI or API without deleting generated tasks. CLI JSON and API workflow create/run responses include `runs` and `errors` arrays; API workflow creation can also immediately execute the first runnable stage with `{"execute":true}`.
- Workflow objectives must not be empty.
- `run --execute --limit N` runs scheduled task commands concurrently up to the selected limit, records each run, and writes logs under `runs/`; `run --dry-run` and `POST /run` with `{"dry_run":true}` preview scheduler assignments and stale recovery without mutating state or executing commands. CLI JSON and API scheduler-run responses include `runs` and `errors` arrays. CLI/API run status filters accept `running`, `cancel-requested`, `cancel_requested`, `cancelled`, `canceled`, `success`, `succeeded`, `failed`, or `rejected`, and run history can be filtered by task, assigned agent, RFC3339 start time window, or command text with the most recent matching runs first.
- Scheduler run limits must be greater than zero; stale recovery windows must be zero or positive.
- Tools are durable command templates with required capabilities. `task create --tool TOOL --arg key=value` invokes the registered tool with shell-quoted arguments and the normal policy checks; tool args are rejected unless a tool is selected. Pending and blocked tasks can replace or clear their tool invocation through `task update`. Duplicate or unused tool arg keys are rejected. Secret-like inputs should use `--secret-arg key=ENV_VAR`, which resolves at execution time and redacts the value from state and logs; secret args must name an environment variable and cannot share a key with plain args.
- Tool kinds and API tool kind filters accept `shell`, `file-read`, `read-file`, `file-write`, or `write-file`, tool collections can be filtered by RFC3339 update time window or tool text, and tool lists return the most recently updated matching tools first.
- Tool names must normalize to a non-empty ID, tool command templates must not be empty, and every brace in a template must be part of a closed `{arg}` placeholder without surrounding whitespace; tool IDs are unique.
- Operators can update tool kind, description, required capabilities, command template, and default cwd from the CLI or API; description, required capabilities, and default cwd can also be cleared. Updates preserve tool IDs, reject incompatible existing task arguments, and write audit events.
- Tool removal refuses tools that are still referenced by tasks, preserving state validity.
- Built-in `file-read` and `file-write` tools use the command template as a path template and enforce workspace policy without invoking a shell. `file-write` expects a `body` argument, which can be supplied as a plain arg or `--secret-arg body=ENV_VAR`.
- Tasks without `--command` are completed through the local mock provider, which gives deterministic agent output for tests and offline workflows. Provider responses can request registered tool calls; the runtime turns those into dependent tool tasks instead of executing them inline.
- Task commands and tools can declare `--cwd`; provided working directories must not be empty, and the runtime rejects work outside configured workspace allowlists.
- Task lifecycle changes release agent capacity and write audit events.
- CLI and API event lists return newest matching events first; limits must be greater than zero, event kind filters are validated, event `since`/`until` filters must be RFC3339 timestamps, and event queries search messages case-insensitively.
- Stale running tasks can be recovered to pending manually with `task recover`, through `POST /tasks/recover`, or before scheduler ticks with `--recover-stale-seconds`.
- Manual stale recovery windows must be zero or positive.
- Memory records are searchable by topic, body, and tags through the CLI and API, memory collections can be filtered by exact tag or RFC3339 update time window, and memory list/search results return the most recently updated matching records first.
- Memory records can be inspected, updated, and removed by ID from the CLI and API; updates can replace or clear tags, and updates/removals write audit events.
- Memory topics, bodies, and tag entries must not be empty.
- Memory search queries must not be empty.
- State can be exported, imported, backed up, migrated, pruned, and validated for unsupported versions, dangling references, malformed durable fields, optional metadata drift, timestamp order drift, dependency cycles, task output drift, daemon metadata drift, run command/exit-code/lifecycle drift, and assignment index consistency. State import/export/backup/migrate paths must not be empty when provided. `GET /state/export` returns the exact durable state snapshot, `POST /state/export` accepts `{"output":"state.json"}` to write an atomic export file, `POST /state/import` accepts `{"path":"state.json","force":true}` with the same validation and overwrite checks as the CLI, `POST /state/migrate` accepts `{"input":"legacy.json","output":"state.json"}` with both paths defaulting to the active state, `POST /state/backup` accepts `{"output":"backup.json"}` or uses a timestamped default path, `POST /state/repair` accepts `{"dry_run":true}` to preview repairs without persisting, and `POST /state/prune` accepts `{"keep_runs":100,"keep_events":500,"dry_run":false}` for API-driven maintenance.
- `state repair` fixes repairable OS name drift, assignment index drift, dependency drift, workflow stage/task drift, dangling non-running task assignments, optional metadata drift, timestamp order drift, task plan/output drift, provider default/env/empty-endpoint drift, policy list/env/limit drift, zero agent capacity, daemon limit/metadata drift, expired agent leases, repairable run command/cwd/exit-code drift, repairable tool invocation argument drift, capability/tag normalization drift, and stopped daemon state drift.
- `config init`, `config show`, `config validate`, `GET /config`, `POST /config`, and `GET /config/validate` let operators create, inspect, and validate config from the CLI or API. `config show --json` and `GET /config` return the config path, existence flag, and loaded config, or the default config when no config file exists. `config validate --json` and `GET /config/validate` report `config_valid:null` when no config file exists, and include `config_error` only for load/parse failures. `POST /config` writes the default config and accepts `{"force":true}` for controlled replacement.
- `init` and `POST /init` initialize durable state from the effective config. API init accepts `{"name":"My OS","force":true}` for an optional name override and controlled replacement of existing state.
- `doctor` and `GET /doctor` report state paths, state load errors, state validation status, and config load/semantic validation issues before operators initialize or run the OS.
- `daemon run` keeps the scheduler alive as a service loop and persists heartbeat/status in state. `daemon run --json` emits one final response with the daemon state, aggregate scheduler totals, and a bounded recent tick history.
- Daemon and service intervals must be greater than zero; daemon `max_ticks`, when provided, must also be greater than zero.
- `daemon stop` and `POST /daemon/stop` record a stop request that a running daemon observes between ticks, and `GET /daemon` returns the durable daemon state for dashboards and supervisors. `daemon stop --json` mirrors the API response with `stop_requested` and the updated daemon state.
- `service launchd` renders a macOS launchd plist for running the daemon under a supervisor.
- `POST /service/launchd` renders the same launchd plist and service metadata for API-driven supervisors without installing or starting the service. `POST /service/launchd/install` writes that plist atomically, and `POST /service/launchd/uninstall` removes the selected plist when present.
- `service install` and `service uninstall` write or remove the user launchd plist; `service start`, `service stop`, and `service status` control the LaunchAgent through `launchctl`. The API mirrors those launchctl controls with `POST /service/launchd/start`, `POST /service/launchd/stop`, and `POST /service/launchd/status`. Service labels, launchd domains, launchctl paths, binary paths, and plist paths must not be empty when provided.
- `api serve` exposes state and authenticated mutation over a small local HTTP server for dashboards, monitors, and agent tools.
- API collection filters reject unsupported query parameters so integration typos fail early.
- API request caps must be greater than zero, and token-protected API serving rejects empty token environment names or values.
- `metrics` and `GET /metrics` expose the same counter snapshot for scripts, dashboards, and monitors, returning `ok=false` with `state_loads=false` when state is missing or unavailable.
- `GET /health` includes the running `agent-os` binary version, state load status, state load errors, state validation status, config load status, config semantic validation status, and issue counts for monitors that need a quick health signal.
- `GET /metrics` returns a stable JSON counter snapshot for the running `agent-os` binary version, agents, tasks, runs, tools, events, memories, daemon status, and state health.
- `POST /run` performs one scheduler tick and can execute assigned tasks when sent `{"execute":true,"limit":N}` or preview assignments without changing state when sent `{"dry_run":true,"limit":N}`.
- `workflow run`, `workflow cancel`, `POST /workflows/WORKFLOW_ID/run`, and `POST /workflows/WORKFLOW_ID/cancel` advance or cancel workflow stages without letting unrelated higher-priority work take over the workflow action.
- `runs logs --tail-bytes N`, `runs tail --tail-bytes N`, `runs replay --tail-bytes N`, `GET /runs/RUN_ID/logs?tail_bytes=N`, and `GET /runs/RUN_ID/replay?tail_bytes=N` return bounded log output for large runs. `GET /runs/RUN_ID/logs?tail_bytes=N` is the non-following API equivalent for bounded tail reads; streaming follow mode is intentionally CLI-only through `runs tail --follow`.
- JSON/API log responses include a truncation flag when a byte limit removes part of the log.
- `runs replay` reconstructs a run from task state, run metadata, related events, and log output, and reports log read errors when the log is unavailable.
- Shell run logs are written while the command is still running, and `runs tail --follow` streams them through the CLI.
- `runs tail --interval-ms` must be greater than zero.
- `runs cancel` and `POST /runs/RUN_ID/cancel` record a durable cancellation request that running shell executions observe and terminate. `runs cancel --json` mirrors the API response with `id`, `cancel_requested`, and the updated run record.
- Mutating CLI commands run through an exclusive store transaction; read-only commands and the API take shared locks.

## Policy

Shell execution is enabled by default for local use, but commands are checked before launch. The default policy allows the current workspace, rejects destructive patterns such as `rm -rf`, `sudo`, `shutdown`, `reboot`, `mkfs`, and raw disk writes using `dd if=`. Command allowlist and deny-pattern checks are case-insensitive and ignore surrounding whitespace in configured entries.

Command execution starts from a minimal allowlisted environment rather than inheriting every parent process variable. Values from allowed environment variables whose names match redaction patterns are replaced with `[redacted]` in run logs.

When `allowed_commands` is set, commands are also rejected if they contain unquoted shell control operators such as `;`, pipes, redirects, background operators, command substitution, or newlines.

## Configuration

`agent-os.toml` can seed OS name, agents, tools, provider settings, and policy before `init` writes state. Use `--config PATH` or `AGENT_OS_CONFIG` to point at a specific file. Seeded agents and tools are validated with the same uniqueness and capacity rules as CLI/API creation.
Unknown config keys are rejected so typos fail before state is initialized.
Provider and policy config values reject empty names, patterns, workspaces, invalid or whitespace-padded environment variable names, and missing or malformed OpenAI-compatible HTTP(S) endpoints.
Provider kind config accepts `mock`, `openai-compatible`, or `open-ai-compatible`, and stores the canonical kind in state.
Tool kind config accepts `shell`, `file-read`, `read-file`, `file-write`, or `write-file`, and stores the canonical kind in state.
Policy `max_output_bytes` must be greater than zero so run logs remain inspectable.
Policy `command_timeout_seconds` must be greater than zero so commands have a real execution window.

```toml
name = "Agent OS"

[policy]
allow_shell = true
allowed_commands = []
allowed_workspaces = ["."]
denied_patterns = ["rm -rf", "sudo", "shutdown", "reboot", "mkfs", "dd if="]
max_output_bytes = 131072
command_timeout_seconds = 300
inherit_environment = false
allowed_env_vars = ["PATH", "HOME", "USER", "TMPDIR", "TMP", "TEMP", "RUSTUP_HOME", "CARGO_HOME"]
redacted_env_patterns = ["KEY", "TOKEN", "SECRET", "PASSWORD", "AUTH", "CREDENTIAL"]

[provider]
kind = "mock"
model = "mock-agent"
# kind = "openai-compatible" # also accepts "open-ai-compatible"
# endpoint = "https://api.openai.com/v1/chat/completions"
# model = "gpt-4.1-mini"
# api_key_env = "OPENAI_API_KEY"

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

The runtime has a provider boundary for agent work. The default `provider:mock` path is local and deterministic, so non-command tasks can still be planned, completed, audited, and logged without network access or API keys. Set `[provider].kind = "openai-compatible"` with an OpenAI-compatible chat-completions endpoint to use an external LLM provider; the API key is read from `api_key_env`. Provider requests include the assigned agent, task, matching registered tools, and recent memory. OpenAI-compatible providers are asked for strict JSON with `summary`, `plan`, `confidence`, and optional `tool_calls`; plain text responses remain supported as a fallback. Tool calls are materialized as normal dependent tool tasks, so policy checks, logs, and scheduling still happen through the runtime. If an assigned agent has a model, it overrides the global provider model for that request.

## Development

```bash
./scripts/ci.sh
AGENT_OS_RELEASE_CHECK=1 ./scripts/ci.sh
```

The default CI script checks formatting, tests, docs with warnings denied, clippy, and package verification. Set `AGENT_OS_RELEASE_CHECK=1` to run the crates.io publish dry run.
