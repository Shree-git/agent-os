#!/usr/bin/env sh
set -eu

cargo fmt -- --check
cargo test
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
cargo clippy --all-targets -- -D warnings

if [ "${AGENT_OS_RELEASE_CHECK:-0}" = "1" ]; then
  cargo publish --dry-run --allow-dirty
else
  cargo package --allow-dirty
fi
