#!/usr/bin/env bash
# Autoresearch correctness gate (fast, scoped) for bombay-core + fuzz/.
# The single authoritative gate is `nix flake check`; it is run at finalize
# (autoresearch-finalize) and as the final confirmation before keeping the
# branch. This scoped gate runs every iteration so a bad change auto-reverts.
set -euo pipefail

cargo fmt --check -p bombay-core

cargo clippy -p bombay-core --all-targets --all-features \
  -- -D warnings

cargo nextest run -p bombay-core --no-fail-fast

# fuzz/ is its own workspace (bolero, stable replay of the corpus).
( cd fuzz && cargo test )
