# Security Policy

Agent OS can execute local commands, read and write files through configured tools, expose a local HTTP API, and use provider credentials. Treat security reports seriously and avoid posting exploit details publicly.

## Supported Versions

Agent OS is currently pre-release. Security fixes are expected to target the latest `main` branch until the project publishes stable release lines.

## Reporting A Vulnerability

Please report vulnerabilities through GitHub Security Advisories for this repository. If advisories are unavailable, open a minimal public issue that asks for a private disclosure path without including exploit details.

Include:

- Affected command, API route, config field, or runtime behavior.
- Reproduction steps in a temporary state directory.
- Expected impact.
- Whether command execution, file tools, API auth, provider credentials, logs, or state corruption are involved.

## Security-Relevant Areas

- Shell command policy and deny-pattern enforcement.
- Workspace allowlists and file-read/file-write tool path handling.
- Symlink and path traversal behavior.
- Local API authentication and non-loopback binding rules.
- Secret argument handling and log redaction.
- State import, migration, repair, and atomic writes.
- Run cancellation and stale task recovery.
- Provider API key loading and request construction.

## Operational Guidance

- Use project-local state directories for experiments.
- Require `--token-env` for API serving outside loopback.
- Keep `allowed_workspaces` narrow.
- Prefer `--secret-arg` for secret values and avoid storing secrets in plain task args.
- Run `agent-os doctor` and `agent-os state validate` before long-running automation.

