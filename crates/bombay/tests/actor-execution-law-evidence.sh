#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${IN_NIX_SHELL:-}" && -z "${NIX_BUILD_TOP:-}" ]]; then
  printf '%s\n' 'Actor-execution law evidence must run inside the pinned Nix shell' >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd "$script_dir/../../.." && pwd)"
output_dir=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output)
      output_dir="${2:?--output requires a directory}"
      shift 2
      ;;
    *)
      printf 'unknown Actor-execution law evidence argument: %s\n' "$1" >&2
      exit 2
      ;;
  esac
done

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/bombay-actor-execution-law.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT
if [[ -z "$output_dir" ]]; then
  output_dir="$(mktemp -d "${TMPDIR:-/tmp}/bombay-incarnation-result.XXXXXX")"
fi
mkdir -p "$output_dir"

source_dir="$work_dir/source"
mkdir -p "$source_dir"
tar -C "$repository_root" \
  --exclude='./.git' \
  --exclude='./.direnv' \
  --exclude='./.serena' \
  --exclude='./target' \
  --exclude='*/target' \
  --exclude='./mutants.out' \
  --exclude='./mutants.out.old' \
  -cf - . | tar -C "$source_dir" -xf -

cd "$source_dir"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$work_dir/target}"

actor_execution='crates/bombay/src/actor_execution.rs'
outcome='crates/bombay/src/actor_outcome.rs'
pristine_actor_execution="$work_dir/actor_execution.rs"
pristine_outcome="$work_dir/actor_outcome.rs"
receipt_rows="$work_dir/receipt.ndjson"
cp "$actor_execution" "$pristine_actor_execution"
cp "$outcome" "$pristine_outcome"
: > "$receipt_rows"

restore_sources() {
  cp "$pristine_actor_execution" "$actor_execution"
  cp "$pristine_outcome" "$outcome"
  touch "$actor_execution" "$outcome"
}

replace_exact() {
  local path="$1"
  local from="$2"
  local to="$3"
  FROM="$from" TO="$to" perl -0pi -e '
    BEGIN {
      $from = $ENV{"FROM"};
      $to = $ENV{"TO"};
      $count = 0;
    }
    $count += s/\Q$from\E/$to/g;
    END {
      die "expected one exact mutation in $ARGV, found $count\n" unless $count == 1;
    }
  ' "$path"
}

apply_mutation() {
  case "$1" in
    terminal-after-driver)
      replace_exact "$actor_execution" \
        '        let terminal = Terminal::<_, B, E::Residual, B::Error, E::Error>::new(retirement);
        let outcome = driver.run().await.into();' \
        '        let outcome = driver.run().await.into();
        let terminal = Terminal::<_, B, E::Residual, B::Error, E::Error>::new(retirement);'
      ;;
    retirement-before-driver-drop)
      replace_exact "$actor_execution" \
        '        let terminal = Terminal::<_, B, E::Residual, B::Error, E::Error>::new(retirement);
        let outcome = driver.run().await.into();
        terminal.complete(outcome)' \
        '        struct Reversed<T, D> {
            terminal: T,
            driver: D,
        }
        let mut reversed = Reversed {
            terminal: Terminal::<_, B, E::Residual, B::Error, E::Error>::new(retirement),
            driver: Box::pin(driver.run()),
        };
        let outcome = reversed.driver.as_mut().await.into();
        reversed.terminal.complete(outcome)'
      ;;
    discard-abnormal-retirement)
      replace_exact "$actor_execution" \
        '        let Some(retirement) = self.retirement.take() else {
            return;
        };
        let outcome = if thread::panicking() {
            ActorExecutionOutcome::Panicked
        } else {
            ActorExecutionOutcome::Cancelled
        };
        drop(retirement.retire(outcome));' \
        '        drop(self.retirement.take());'
      ;;
    duplicate-driver)
      replace_exact "$actor_execution" \
        '        let outcome = driver.run().await.into();' \
        '        let outcome = driver.run().await.into();
        let _ = driver.run().await;'
      ;;
    duplicate-retirement)
      replace_exact "$actor_execution" \
        '        self.retirement
            .take()
            .expect("terminal retirement is affine")
            .retire(outcome)' \
        '        let retirement = self
            .retirement
            .take()
            .expect("terminal retirement is affine");
        drop(retirement.retire(ActorExecutionOutcome::Panicked));
        retirement.retire(outcome)'
      ;;
    panic-as-cancellation)
      replace_exact "$actor_execution" \
        '        let outcome = if thread::panicking() {
            ActorExecutionOutcome::Panicked
        } else {
            ActorExecutionOutcome::Cancelled
        };' \
        '        let outcome = if thread::panicking() {
            ActorExecutionOutcome::Cancelled
        } else {
            ActorExecutionOutcome::Cancelled
        };'
      ;;
    cancellation-as-panic)
      replace_exact "$actor_execution" \
        '        let outcome = if thread::panicking() {
            ActorExecutionOutcome::Panicked
        } else {
            ActorExecutionOutcome::Cancelled
        };' \
        '        let outcome = if thread::panicking() {
            ActorExecutionOutcome::Panicked
        } else {
            ActorExecutionOutcome::Panicked
        };'
      ;;
    erase-behavior-failure)
      replace_exact "$outcome" \
        '            Err(DriverError::Behavior(error)) => Self::BehaviorFailed {
                behavior,
                residual,
                error,
            },' \
        '            Err(DriverError::Behavior(_)) => panic!("mutated Behavior failure"),'
      ;;
    erase-activation-failure)
      replace_exact "$outcome" \
        '            Err(DriverError::Activation(error)) => Self::ActivationFailed {
                behavior,
                residual,
                error,
            },' \
        '            Err(DriverError::Activation(_)) => panic!("mutated activation failure"),'
      ;;
    erase-settlement-failure)
      replace_exact "$outcome" \
        '            Err(DriverError::Settlement(error)) => Self::SettlementFailed {
                behavior,
                residual,
                error,
            },' \
        '            Err(DriverError::Settlement(_)) => panic!("mutated settlement failure"),'
      ;;
    *)
      printf 'unknown actor-execution mutation: %s\n' "$1" >&2
      exit 2
      ;;
  esac
}

run_reference() {
  local test_name="$1"
  restore_sources
  printf 'PASS  %s\n' "$test_name"
  cargo test --locked -p bombay-rs --lib "$test_name" -- --exact
}

kill_mutation() {
  local inversion="$1"
  local killer="$2"
  local output
  local status

  restore_sources
  apply_mutation "$inversion"
  printf 'KILL  %s with %s\n' "$inversion" "$killer"
  set +e
  output="$(cargo test --locked -p bombay-rs --lib "$killer" -- --exact --nocapture 2>&1)"
  status=$?
  set -e
  restore_sources

  if [[ $status -eq 0 ]]; then
    printf '%s\n' "$output" >&2
    printf 'mutation survived: %s\n' "$inversion" >&2
    exit 1
  fi
  if grep -Fq 'error: could not compile' <<< "$output"; then
    printf '%s\n' "$output" >&2
    printf 'mutation was unviable instead of killed: %s\n' "$inversion" >&2
    exit 1
  fi
  if ! grep -Fq "test $killer ... FAILED" <<< "$output"; then
    printf '%s\n' "$output" >&2
    printf 'mutation failed outside its named killer: %s\n' "$inversion" >&2
    exit 1
  fi

  jq -n --arg inversion "$inversion" --arg killer "$killer" \
    '{inversion: $inversion, result: "killed", killer: $killer}' >> "$receipt_rows"
}

deny_mutation() {
  local inversion="$1"
  local moved_value="$2"
  local output
  local status

  restore_sources
  apply_mutation "$inversion"
  printf 'DENY  %s through affine ownership\n' "$inversion"
  set +e
  output="$(cargo test --locked -p bombay-rs --lib "$replay_test" -- --exact 2>&1)"
  status=$?
  set -e
  restore_sources

  if [[ $status -eq 0 ]]; then
    printf '%s\n' "$output" >&2
    printf 'ownership inversion compiled: %s\n' "$inversion" >&2
    exit 1
  fi
  if ! grep -Fq "use of moved value: \`$moved_value\`" <<< "$output"; then
    printf '%s\n' "$output" >&2
    printf 'ownership inversion lacked its exact affine diagnostic: %s\n' "$inversion" >&2
    exit 1
  fi

  jq -n --arg inversion "$inversion" --arg diagnostic "use of moved value: $moved_value" \
    '{inversion: $inversion, result: "denied", diagnostic: $diagnostic}' >> "$receipt_rows"
}

panic_test='actor_execution::tests::panic_drops_driver_before_exactly_one_terminal_classification'
cancellation_test='actor_execution::tests::cancellation_drops_driver_before_exactly_one_terminal_classification'
failure_test='actor_execution::tests::exact_driver_failures_remain_distinct'
settlement_test='actor_outcome::tests::settlement_retirement_preserves_each_exact_failure_and_owned_payload'
replay_test='actor_execution::tests::replayed_abnormal_terminations_are_each_accepted_once'

run_reference "$panic_test"
run_reference "$cancellation_test"
run_reference "$failure_test"
run_reference "$settlement_test"
run_reference "$replay_test"

kill_mutation terminal-after-driver "$panic_test"
kill_mutation retirement-before-driver-drop "$cancellation_test"
kill_mutation discard-abnormal-retirement "$panic_test"
kill_mutation panic-as-cancellation "$panic_test"
kill_mutation cancellation-as-panic "$cancellation_test"
kill_mutation erase-behavior-failure "$failure_test"
kill_mutation erase-activation-failure "$failure_test"
kill_mutation erase-settlement-failure "$settlement_test"
deny_mutation duplicate-driver driver
deny_mutation duplicate-retirement retirement

restore_sources
jq -s \
  --arg revision '804b2bf25325a523884ec49d8a4ae6d2d2b6e9da' \
  '{
    schema: 1,
    behavior_revision: $revision,
    production_sources: [
      "crates/bombay/src/actor_execution.rs",
      "crates/bombay/src/actor_outcome.rs"
    ],
    results: .
  }' "$receipt_rows" > "$output_dir/actor-execution-law-evidence.json"

printf 'Actor-execution law evidence: %s\n' "$output_dir/actor-execution-law-evidence.json"
