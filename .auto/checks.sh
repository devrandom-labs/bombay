#!/usr/bin/env bash
# Autoresearch correctness gate (fast, scoped) for bombay-core + fuzz/.
# The single authoritative gate is `nix flake check`; it is run at finalize
# (autoresearch-finalize) and as the final confirmation before keeping the
# branch. This scoped gate runs every iteration so a bad change auto-reverts.
#
# Scoped to bombay-core's own library + its test run + the fuzz/ workspace.
# We deliberately do NOT build bombay-core's benches / integration tests that
# depend on the `src/` kameo reference oracle: that crate has pre-existing
# clippy-nursery violations that are outside this task's scope AND outside
# `nix flake check`'s fuzz coverage (fuzz/ is its own workspace, not a flake
# member), so pulling it in would fail the gate on unrelated, untouchable code.
set -euo pipefail

cargo fmt --check -p bombay-core

cargo clippy -p bombay-core --lib \
	-- -D warnings

cargo nextest run -p bombay-core --no-fail-fast

# fuzz/ is its own workspace (bolero, stable corpus replay).
(cd fuzz && cargo test)
