#!/usr/bin/env bash
# Autoresearch measure script — bombay-core fuzz/property invariant coverage.
# Fast: counts PASSING invariant-covering tests in bombay-core + fuzz/.
set -euo pipefail

# bombay-core: passing tests via nextest summary line.
core_pass=$(cargo nextest run -p bombay-core --no-fail-fast 2>/dev/null \
  | rg -o '[0-9]+ tests run: [0-9]+ passed' \
  | rg -o '[0-9]+ passed' | rg -o '[0-9]+' | head -1 || true)
core_pass="${core_pass:-0}"

# fuzz/: bolero `check!` targets compile + replay corpus as `#[test]`.
# Runs in its own workspace; build is cached after the first iteration.
fuzz_pass=$( (cd fuzz && cargo test 2>/dev/null \
  | rg -o 'test result: ok\. ([0-9]+) passed' \
  | rg -o '[0-9]+' | head -1) || true )
fuzz_pass="${fuzz_pass:-0}"

total=$(( core_pass + fuzz_pass ))
echo "METRIC invariant_tests=${total}"
echo "METRIC core_tests=${core_pass}"
echo "METRIC fuzz_tests=${fuzz_pass}"
