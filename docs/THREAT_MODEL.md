# Agent OS Threat Model

Agent OS is a local-first coordinator for agents, tasks, tools, memory, and shell execution. The primary security goal is to keep local automation auditable and bounded by explicit policy.

## Assets

- Durable state: agents, tasks, workflows, runs, memory, policy, provider settings, and event history.
- Workspace files reachable by shell commands and file tools.
- Provider API keys and secret tool arguments loaded from environment variables.
- Run logs, which may include command output and operational metadata.

## Trust Boundaries

- CLI callers are local operators with access to the state path.
- The HTTP API is intended for local clients. Non-loopback binds require bearer-token authentication unless explicitly overridden. Operators can use one full-access bearer token or split read-only and mutation-scoped bearer tokens.
- Browser clients must use missing origins, loopback origins, or exact origins explicitly configured with `api serve --allow-origin`. Other remote origins are rejected before authorization checks.
- Provider responses are untrusted. Tool calls from providers are converted into normal tool tasks and pass through validation, policy, scheduling, and logging.
- Shell commands and file tools are untrusted work. They are checked against policy before launch.

## Current Controls

- Atomic state writes with validation before save.
- Exclusive store transactions for mutations.
- Generated task, run, workflow, memory, approval, eval, and event IDs are retried against current state before insertion. User-derived agent, tool, worker, MCP server, and secrets backend IDs are rejected on collision instead of overwriting existing records.
- Run records carry durable trace IDs, run logs include those trace IDs, API responses include per-request `X-Trace-Id` headers, and metrics expose queued-task age and run duration histogram buckets for operational correlation.
- Shell execution starts with an allowlisted environment, not inherited process state.
- Policy supports disabling shell, allowed workspaces, built-in file-write `writable_paths`, common shell write/redirection checks against `writable_paths` and `deny writes outside PATH` rules, denied command patterns, command allowlists, readable `require approval for PATTERN` approval gates, output limits, and command timeouts. A quote-aware token pass rejects destructive `rm`/`dd` forms, direct privilege-escalation executables, filesystem formatters, and system power commands before launch.
- Shell network policy can disable common network commands or require directly named hosts to match `network.allowed_hosts`. Detection is token-aware for common network executables and remote git subcommands, so quoted text is not treated as execution.
- Unix shell execution can start in an isolated process group, and cancellation/timeout handling terminates that group or the descendant tree so spawned children are stopped together. Windows cancellation uses `taskkill /T /F` for process-tree termination before falling back to direct child termination.
- Task and workflow cancellation request cancellation for active runs.
- Scheduler dry-runs report dependency-cycle deadlocks among pending or blocked tasks without assigning work or mutating state.
- API responses use `no-store` and `nosniff`; bearer tokens are compared without early-exit string equality, startup rejects short or whitespace-bearing token values, scoped read/write tokens must be distinct, and scoped tokens receive `403 Forbidden` when used outside their granted method class.
- The local HTTP parser has explicit size, UTF-8, syntax, duplicate-host, and content-length checks with targeted tests plus bounded property tests for arbitrary request bytes.
- Provider requests use bounded timeouts plus bounded and capped retries for transient transport, rate-limit, 5xx, and provider-plugin process failures. HTTP provider response bodies are capped before parsing.
- Provider memory recall excludes `private` records and only includes scoped shared records when the record scope matches the active `memory_policy.scope`.
- Secret environment-backed tool args are redacted from rendered commands and logs.

## Residual Risks

- Shell execution still uses `sh -c`; policy checks and process-group isolation reduce risk but do not make arbitrary shell commands safe.
- Deny patterns are guardrails, not a complete sandbox.
- Shell network host detection is conservative and string/URL based; commands that hide the destination behind aliases, remotes, variables, or scripts should be represented as narrower registered tools or blocked pending approval.
- Local users with filesystem access to state and logs can read stored operational data.
- Provider-generated plans and tool calls can be wrong or malicious, so approval policy and narrow tool definitions remain important. When risky-action approval or `execute-with-approval` autonomy is enabled, provider-requested tool calls are queued for approval before dependent tool tasks are created.
- Unix process-group and descendant termination depends on Unix process controls and process inspection tools. Windows process-tree termination depends on `taskkill`; if that command fails, Agent OS falls back to killing the direct child process.

## Operator Guidance

- Set `allow_shell = false` unless command execution is required.
- Prefer `agent-os config init --profile safe` for new local deployments, then opt into `dev`, `ci`, or `autonomous` only when the operational need is clear.
- Prefer registered tools with narrow templates over free-form task commands.
- Use `allowed_workspaces` and `allowed_commands` for recurring automation.
- Use `--token-env` for full-access API serving, or `--read-token-env` plus `--write-token-env` when dashboard and automation clients should not share one credential. If using token files, keep them as regular files private to the service account; Unix symlink token files and files with group or other access are rejected.
- Keep `--allow-origin` narrow and exact. Prefer one trusted dashboard origin over broad browser exposure.
- Keep provider API keys in environment variables and use `--secret-arg` for secret tool inputs.
- Run `agent-os doctor` and `agent-os state validate` before unattended daemon runs.
