# Examples

These examples show how to use Agent OS as a local backend for agent products and automation.

## Local Coding Queue

Run the example from a checkout after installing the binary:

```bash
cargo install --path .
sh examples/local-coding-queue.sh
```

The script creates a local `.agent-os-example` state directory, registers a builder agent, registers a few tools, queues tasks, executes one scheduler tick, and prints run history.

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

