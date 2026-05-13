#!/usr/bin/env sh
set -eu

STATE_DIR="${AGENT_OS_EXAMPLE_STATE:-./.agent-os-example}"

agent-os --state "$STATE_DIR" init --name "Agent OS Example" --force

agent-os --state "$STATE_DIR" agent add builder \
  --kind builder \
  --cap code \
  --cap rust \
  --cap test \
  --parallel 2

agent-os --state "$STATE_DIR" tool add cargo-test \
  --kind shell \
  --description "Run the Rust test suite" \
  --need rust \
  --need test \
  --command-template "cargo test"

agent-os --state "$STATE_DIR" task create "Run the test suite" \
  --need rust \
  --need test \
  --tool cargo-test

agent-os --state "$STATE_DIR" run --execute --limit 1
agent-os --state "$STATE_DIR" runs list
agent-os --state "$STATE_DIR" status

