# Examples

These examples show how to use Agent OS as a local backend for agent products and automation.

For product storytelling, open `docs/landing.html` from the repository root. It includes the "Why Agent OS" positioning, a copyable local demo flow, and a dashboard-preview screenshot asset for release notes or repository screenshots.

## Local Coding Queue

Run the example from a checkout after installing the binary:

```bash
cargo install --path .
sh examples/local-coding-queue.sh
```

The script creates a local `.agent-os-example` state directory, uses the seeded planner and builder agents, shows the seeded tools, queues a deterministic local task, executes one scheduler tick, and prints run history.

For live demos, keep state isolated:

```bash
AGENT_OS_EXAMPLE_STATE="$(mktemp -d)" sh examples/local-coding-queue.sh
```

## Demo Tracks

### Technical builders

Show Agent OS as the local backend for agent CLIs, dashboards, and workers:

```bash
agent-os --state ./.agent-os-example init --name "Builder Demo" --force
agent-os --state ./.agent-os-example agent list
agent-os --state ./.agent-os-example tool list
agent-os --state ./.agent-os-example task create "Run a local builder task" --need rust --command "printf 'scheduled, executed, logged, replayable\n'"
agent-os --state ./.agent-os-example run --execute --limit 1
agent-os --state ./.agent-os-example runs list
agent-os --state ./.agent-os-example runs logs RUN_ID
agent-os --state ./.agent-os-example runs replay RUN_ID
agent-os --state ./.agent-os-example metrics
```

### Business stakeholders

Use the same terminal flow to show where the product fits:

- Coding queues that route work by agent capability.
- Local dashboard backends with status, metrics, logs, and replay data.
- Automation daemons that keep maintenance tasks moving.
- Auditable tool execution with durable local state.

## Configuration Seed

`examples/agent-os.toml` is a starter config that seeds agents, tools, provider settings, and policy.

```bash
agent-os --state ./.agent-os-example --config examples/agent-os.toml init --force
agent-os --state ./.agent-os-example status
agent-os --state ./.agent-os-example tool list
```

## API Demo

Start the API with a local token:

```bash
export AGENT_OS_API_TOKEN=dev-token
agent-os --state ./.agent-os-example api serve --addr 127.0.0.1:7373 --token-env AGENT_OS_API_TOKEN
```

Query it from another terminal:

```bash
curl -H "Authorization: Bearer dev-token" http://127.0.0.1:7373/status
curl -H "Authorization: Bearer dev-token" http://127.0.0.1:7373/metrics
curl -H "Authorization: Bearer dev-token" http://127.0.0.1:7373/openapi.json
```
