#!/usr/bin/env sh
set -eu

cargo fmt -- --check
cargo test --locked
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
cargo clippy --locked --all-targets -- -D warnings

if [ "${AGENT_OS_RELEASE_CHECK:-0}" = "1" ]; then
  cargo publish --dry-run --locked --allow-dirty
else
  cargo package --locked --allow-dirty
fi
