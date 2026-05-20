#!/usr/bin/env sh
set -eu

STATE_DIR="${AGENT_OS_EXAMPLE_STATE:-./.agent-os-example}"

agent-os --state "$STATE_DIR" init --name "Agent OS Example" --force

agent-os --state "$STATE_DIR" agent list
agent-os --state "$STATE_DIR" tool list

agent-os --state "$STATE_DIR" task create "Show coding queue handoff" \
  --need rust \
  --command "printf 'scheduled, executed, logged, replayable\n'"

agent-os --state "$STATE_DIR" run --execute --limit 1
agent-os --state "$STATE_DIR" runs list
agent-os --state "$STATE_DIR" status
