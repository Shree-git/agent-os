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
./scripts/package-release.sh
```

## Release

- Tag the first public release after CI passes. Tags matching `v*` run `.github/workflows/release.yml`.
- Confirm the GitHub release contains platform archives, `.sha256` checksum files, Sigstore `.sig`/`.pem` signature files, and build provenance attestations.
- Confirm Windows release archives contain `bin/agent-os.exe`; the packaging script infers the executable suffix from the target triple.
- Publish the crate only after the repository URL, README, license, and package metadata are final.
- Update `packaging/homebrew/agent-os.rb` with the released macOS Apple Silicon, macOS Intel, and Linux x86_64 archive URLs and SHA-256 values, then open the tap PR. Keep the bundled elvish and PowerShell completions in `pkgshare` for users whose shells are not installed through Homebrew's native completion directories.
- Announce Agent OS as a local-first control plane for building durable AI agent systems.
