# Changelog

All notable changes to Agent OS will be documented in this file.

The project follows semantic versioning after the first public release. Until then, `0.x` releases may change CLI, API, and state details while the public contract settles.

## 0.1.0 - Unreleased

Initial public release candidate.

### Added

- Local-first state model for agents, tasks, workflows, tools, memory, events, runs, provider settings, policy, and daemon status.
- `agent-os` CLI for initialization, state maintenance, agents, tasks, tools, memory, events, runs, daemon control, launchd service management, completions, workflows, metrics, health, and local API serving.
- Capability-aware scheduler with task dependencies, priorities, leases, and parallel capacity.
- Shell execution with durable run records, live logs, cancellation, replay, redaction, and policy checks.
- Built-in file-read and file-write tools with workspace policy enforcement.
- Deterministic mock provider and OpenAI-compatible provider boundary.
- Local HTTP API with bearer auth, OpenAPI schema, health, metrics, and mutation validation.
- State validation, repair, migration, backup, import, export, and prune flows.
- CI script covering formatting, locked tests, docs, clippy, and package verification.

