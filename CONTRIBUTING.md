# Contributing

Thanks for helping improve Agent OS. This project is still pre-release, so the most useful contributions are focused, reproducible, and tied to concrete local-agent workflows.

## Development Setup

Requirements:

- Rust 1.85 or newer.
- `cargo`, `rustfmt`, and `clippy`.

Run the full local check:

```bash
./scripts/ci.sh
```

For release verification:

```bash
AGENT_OS_RELEASE_CHECK=1 ./scripts/ci.sh
```

## Before Opening A PR

- Keep changes scoped to one behavior, feature, or documentation improvement.
- Add or update tests for runtime, persistence, API, CLI, policy, or migration changes.
- Update `README.md` when CLI flags, API routes, config fields, provider behavior, or operator workflows change.
- Run `./scripts/ci.sh` before submitting.
- Include user-facing context in the PR description: what changed, why, and how it was verified.

## Documentation Expectations

The README is both a landing page and a tested contract for command/API coverage. Several tests validate that README command synopses and API routes match the implementation. If those tests fail, update the docs and implementation together.

Use `docs/` for deeper explanations and `examples/` for copyable workflows.

## Design Principles

- Local-first by default.
- Durable state transitions over hidden in-memory behavior.
- Explicit policy checks before execution.
- Stable JSON output for integrations.
- Small dependencies unless a library clearly improves correctness.
- Operator-visible logs, events, health, metrics, and recovery paths.

## Reporting Security Issues

Do not open public issues for vulnerabilities. Follow `SECURITY.md`.

