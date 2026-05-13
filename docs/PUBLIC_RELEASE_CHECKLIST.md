# Public Release Checklist

Use this before making the repository public.

## Repository Setup

- Create the GitHub repository and add it as `origin`.
- Add the final repository URL to `Cargo.toml` under `package.repository`.
- Replace static README badges with repository-backed badges if desired.
- Confirm repository description: `The local control plane for AI agents.`
- Add topics: `agents`, `automation`, `rust`, `local-first`, `scheduler`, `developer-tools`, `ai-agents`.

## Project Surface

- Confirm `README.md` landing copy matches the intended positioning.
- Confirm `CONTRIBUTING.md`, `SECURITY.md`, `SUPPORT.md`, `CODE_OF_CONDUCT.md`, and `CHANGELOG.md` are present.
- Confirm issue templates and PR template are visible in GitHub.
- Confirm Dependabot is enabled for Cargo and GitHub Actions.

## Verification

```bash
./scripts/ci.sh
AGENT_OS_RELEASE_CHECK=1 ./scripts/ci.sh
```

## Release

- Tag the first public release after CI passes.
- Publish the crate only after the repository URL, README, license, and package metadata are final.
- Announce Agent OS as a local-first control plane for building durable AI agent systems.

