#!/usr/bin/env bash
# Architecture gate: rule self-tests, the rule scan and the complexity ceiling.
# Run from the repository root. Pass tests or rules to run one part.
# With no argument, run the self-tests before the rules.
set -euo pipefail

ignore_host_git_config() {
  export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null
}

run_tests() (
  ast-grep test --config architecture/sgconfig.yml --skip-snapshot-tests
  ignore_host_git_config
  export GIT_CONFIG_COUNT=3
  export GIT_CONFIG_KEY_0=commit.gpgsign GIT_CONFIG_VALUE_0=false
  export GIT_CONFIG_KEY_1=init.defaultBranch GIT_CONFIG_VALUE_1=main
  export GIT_CONFIG_KEY_2=core.hooksPath GIT_CONFIG_VALUE_2=/dev/null
  source architecture/tests/suites.sh
  local suite
  for suite in "${suites[@]}"; do
    bash "architecture/tests/$suite-test.sh" < /dev/null
  done
)

run_rules() (
  local scope=() dir
  for dir in \
    crates/bombay-engine/src \
    crates/bombay-macros/src \
    crates/bombay-observe/src \
    crates/bombay-domain-auth/src \
    crates/bombay-domain-wire/src \
    crates/bombay-domain-placement/src \
    crates/bombay-domain-durable/src \
    crates/bombay-runtime/src \
    crates/bombay-zenoh/src \
    crates/bombay-axum/src; do
    if [ -d "$dir" ]; then scope+=("$dir"); fi
  done
  ceiling=15
  ignore_host_git_config
  ast-grep scan --config architecture/sgconfig.yml "${scope[@]}"

  if command -v rust-code-analysis-cli >/dev/null 2>&1; then
    local complexity_scope=()
    for dir in \
      crates/bombay-observe/src \
      crates/bombay-domain-auth/src \
      crates/bombay-domain-wire/src \
      crates/bombay-domain-placement/src \
      crates/bombay-domain-durable/src \
      crates/bombay-runtime/src \
      crates/bombay-zenoh/src \
      crates/bombay-axum/src; do
      if [ -d "$dir" ]; then complexity_scope+=("$dir"); fi
    done
    if [ "${#complexity_scope[@]}" -gt 0 ]; then
      report=$(mktemp -d)
      trap 'rm -rf "$report"' EXIT
      for dir in "${complexity_scope[@]}"; do
        rust-code-analysis-cli --metrics --output-format json --output "$report" --paths "$dir" > /dev/null
      done
      over=$(fd --unrestricted --extension json . "$report" --exec-batch jq -r --argjson ceiling "$ceiling" '
        input_filename as $file
        | .. | objects | select(.kind == "function" and .metrics.cognitive.sum > $ceiling)
        | "\($file | sub("\\.json$"; "")):\(.start_line) \(.name): \(.metrics.cognitive.sum)"
      ')
      if [ -n "$over" ]; then
        echo "error[cognitive-complexity]: functions above cognitive complexity $ceiling:"
        echo "$over"
        echo "note: Source: SonarSource S3776; Campbell, Cognitive Complexity (2018)."
        echo "      Fix: name each decision as a pure function or move a rule to its owning domain crate."
        exit 1
      fi
    fi
  fi
  echo "architecture: rules pass for ${scope[*]}"
)

if [ "$#" -eq 0 ]; then
  run_tests
  run_rules
elif [ "$#" -eq 1 ] && [ "$1" = tests ]; then
  run_tests
elif [ "$#" -eq 1 ] && [ "$1" = rules ]; then
  run_rules
else
  echo "Usage: bash architecture/check.sh [tests|rules]" >&2
  exit 2
fi
