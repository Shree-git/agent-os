# Architecture

Agent OS is a local-first runtime for coordinating agent work. The binary and library share the same core runtime, so CLI commands, daemon ticks, and API mutations operate on the same durable state model.

## Components

- `OperatingSystem`: the durable state model for agents, tasks, tools, memory, workflows, runs, policy, provider settings, daemon status, and events.
- `Store`: lock-protected persistence around the state file, run logs, import/export, backup, migration, prune, validation, and repair.
- `Scheduler`: capability-aware task assignment. It picks ready pending work and assigns it to online agents with matching capabilities and available capacity.
- `Runtime`: state transitions for registering agents, creating and updating tasks, managing workflows, lifecycle changes, stale recovery, and scheduler ticks.
- `CommandExecutor`: shell execution, file tools, provider-backed tasks, live logs, cancellation, and run completion.
- `ProviderRuntime`: deterministic mock provider for offline development and OpenAI-compatible HTTP provider for real model-backed work.
- `ApiServer`: local HTTP control plane for dashboards, workers, and integrations.

## Durable State

Agent OS stores state in a single JSON file by default. This keeps deployment simple and makes local experiments easy to inspect, copy, back up, and repair.

Writes are guarded by a sidecar lock file. State, config, export, backup, migration, and run-log writes use temporary files and atomic rename. On Unix, parent directories are synced after rename so completed writes survive process and machine failures more reliably.

## Scheduling Model

Agents advertise:

- status
- kind
- optional model override
- capabilities
- parallel capacity
- optional lease expiry

Tasks declare:

- title and objective
- priority
- required capabilities
- optional command or tool invocation
- optional dependencies

The scheduler considers pending tasks whose dependencies are complete, then assigns the highest-priority ready task to the least-loaded capable online agent. Manual assignment and claim flows use the same readiness, capacity, dependency, and lease rules.

## Execution Model

Work can be executed in three ways:

- Shell command tasks.
- Built-in file-read and file-write tool tasks.
- Provider-backed tasks with mock or OpenAI-compatible providers.

Runs create durable run records and log files. Shell logs are written while the command is running, can be tailed, and can be replayed after completion. Cancellation is recorded durably and observed by running shell executions.

Provider tool calls are materialized as normal dependent tool tasks instead of being executed inline. That keeps policy checks, scheduling, logs, and audit history consistent across human-created and provider-created work.

## API Model

The API is intentionally small and local. It exposes the same runtime concepts as the CLI: agents, tasks, tools, workflows, memory, events, runs, health, metrics, daemon status, state maintenance, config, and service control.

Non-loopback binds require a bearer token unless explicitly overridden. Mutation bodies use JSON and reject unknown fields so integration mistakes fail early.

## Tradeoffs

The single-file state model is simple, portable, and excellent for local-first development. It is not meant to replace a distributed queue or database for high-volume multi-host workloads.

The HTTP server is purpose-built for local integrations. It avoids a large web framework dependency, but it is not a general internet-facing API gateway.

