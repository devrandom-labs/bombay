#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${IN_NIX_SHELL:-}" && -z "${NIX_BUILD_TOP:-}" ]]; then
  printf '%s\n' 'Actor-execution law evidence must run inside the pinned Nix shell' >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd "$script_dir/../../.." && pwd -P)"
output_dir=""
cargo_profile=()
cargo_target=(--lib)
cargo_capture=()
native_phase=''
native_profile=''

while [[ $# -gt 0 ]]; do
  case "$1" in
    --release)
      cargo_profile=(--release)
      shift
      ;;
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
work_dir="$(cd "$work_dir" && pwd -P)"
trap 'rm -rf "$work_dir"' EXIT
if [[ -z "$output_dir" ]]; then
  output_dir="$(mktemp -d "${TMPDIR:-/tmp}/bombay-incarnation-result.XXXXXX")"
fi
output_dir="$(realpath -m "$output_dir")"
case "$output_dir" in
  "$repository_root"|"$repository_root"/*) printf '%s\n' 'Evidence output must be outside the caller checkout' >&2; exit 2 ;;
esac
mkdir -p "$output_dir"
mkdir "$output_dir/child-preparation"

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
export CARGO_TARGET_DIR="$(realpath -m "$CARGO_TARGET_DIR")"
case "$CARGO_TARGET_DIR" in
  "$repository_root"|"$repository_root"/*) printf '%s\n' 'Build artifacts must be outside the caller checkout' >&2; exit 2 ;;
esac

actor_execution='crates/bombay/src/actor_execution.rs'
outcome='crates/bombay/src/actor_outcome.rs'
pristine_actor_execution="$work_dir/actor_execution.rs"
pristine_outcome="$work_dir/actor_outcome.rs"
launch='crates/bombay/src/launch.rs'
pristine_launch="$work_dir/launch.rs"
child_test='joined_child_retirement_waits_for_owned_preparation_while_parent_and_sibling_progress'
child_sources=("$launch" crates/bombay/tests/fixed_supervisor_recovery.rs Cargo.lock Cargo.toml crates/bombay/Cargo.toml)
for child_source in "${child_sources[@]}"; do
  case "$(realpath "$child_source")" in
    "$source_dir"/*) ;;
    *) printf '%s\n' 'Child preparation source escaped the snapshot' >&2; exit 2 ;;
  esac
done
child_checksums="$output_dir/child-preparation/original-sources.sha256"
(cd "$repository_root" && sha256sum "${child_sources[@]}") > "$child_checksums"
sha256sum --status --check "$child_checksums"
receipt_rows="$work_dir/receipt.ndjson"
cp "$actor_execution" "$pristine_actor_execution"
cp "$outcome" "$pristine_outcome"
cp "$launch" "$pristine_launch"
: > "$receipt_rows"

restore_sources() {
  if ! cmp -s "$pristine_actor_execution" "$actor_execution"; then
    cp "$pristine_actor_execution" "$actor_execution"
  fi
  if ! cmp -s "$pristine_outcome" "$outcome"; then
    cp "$pristine_outcome" "$outcome"
  fi
  if ! cmp -s "$pristine_launch" "$launch"; then
    cp "$pristine_launch" "$launch"
  fi
}

finish_evidence() {
  local status=$?
  restore_sources
  if ! sha256sum --status --check "$child_checksums" \
    || ! (cd "$repository_root" && sha256sum --status --check "$child_checksums"); then
    printf '%s\n' 'Child preparation snapshot or caller source conservation failed' >&2
    status=1
  fi
  rm -rf "$work_dir"
  exit "$status"
}
trap finish_evidence EXIT

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
    omit-child-preparation-retirement)
      replace_exact "$launch" '    if let Some(residual) = residual {
        residual.receive_activation_tasks().await;
    }
' ''
      ;;
    terminal-after-driver)
      replace_exact "$actor_execution" \
        '        let terminal = Terminal::<_, B, E::Residual, B::Error, E::Error, E::RetirementRequest>::new(
            retirement,
        );
        // The original Driver and terminal capability stay owned until the
        // completion barrier yields a genuine residual. An incomplete advanced
        // host cannot trigger a replay of its arbitrary retirement callback.
        let mut driver = Some(driver);
        let mut received = None;
        Driver::receive_run(&mut driver, &mut received).await;' \
        '        // The original Driver and terminal capability stay owned until the
        // completion barrier yields a genuine residual. An incomplete advanced
        // host cannot trigger a replay of its arbitrary retirement callback.
        let mut driver = Some(driver);
        let mut received = None;
        Driver::receive_run(&mut driver, &mut received).await;
        let terminal = Terminal::<_, B, E::Residual, B::Error, E::Error, E::RetirementRequest>::new(
            retirement,
        );'
      ;;
    retirement-before-driver-drop)
      replace_exact "$actor_execution" \
        '        let terminal = Terminal::<_, B, E::Residual, B::Error, E::Error, E::RetirementRequest>::new(
            retirement,
        );
        // The original Driver and terminal capability stay owned until the
        // completion barrier yields a genuine residual. An incomplete advanced
        // host cannot trigger a replay of its arbitrary retirement callback.
        let mut driver = Some(driver);
        let mut received = None;
        Driver::receive_run(&mut driver, &mut received).await;' \
        '        let mut driver = Some(driver);
        let terminal = Terminal::<_, B, E::Residual, B::Error, E::Error, E::RetirementRequest>::new(
            retirement,
        );
        // The original Driver and terminal capability stay owned until the
        // completion barrier yields a genuine residual. An incomplete advanced
        // host cannot trigger a replay of its arbitrary retirement callback.
        let mut received = None;
        Driver::receive_run(&mut driver, &mut received).await;'
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
        '        let mut driver = Some(driver);' \
        '        let original_driver = driver;
        let mut driver = Some(driver);'
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
                additional_failures,
                error,
            },' \
        '            Err(DriverError::Behavior(_)) => panic!("mutated Behavior failure"),'
      ;;
    erase-activation-failure)
      replace_exact "$outcome" \
        '            Err(DriverError::Activation(error)) => Self::ActivationFailed {
                behavior,
                residual,
                additional_failures,
                error,
            },' \
        '            Err(DriverError::Activation(_)) => panic!("mutated activation failure"),'
      ;;
    erase-settlement-failure)
      replace_exact "$outcome" \
        '            Err(DriverError::Settlement(error)) => Self::SettlementFailed {
                behavior,
                residual,
                additional_failures,
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
  local label="${2:-reference-${test_name//::/-}}"
  local output
  local status

  restore_sources
  printf 'PASS  %s\n' "$test_name"
  set +e
  output="$(cargo test "${cargo_profile[@]}" --locked -p bombay-rs "${cargo_target[@]}" "$test_name" -- --exact "${cargo_capture[@]}" 2>&1)"
  status=$?
  set -e
  printf '%s\n' "$output" > "$output_dir/$label.log"
  if [[ $status -ne 0 ]] || ! NAMED_REFERENCE="$test_name" perl -0ne '
    exit(/(?:\A|\n)running 1 test\n(?:(?!^test result:).)*^test \Q$ENV{NAMED_REFERENCE}\E \.\.\. ok\n(?:(?!^test result:).)*^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out; finished in [^\n]*\n(?:(?!^test result:).)*\z/ms ? 0 : 1);
  ' <<< "$output"; then
    printf '%s\n' "$output" >&2
    printf 'healthy reference did not pass exactly one named test: %s\n' "$test_name" >&2
    exit 1
  fi
  if [[ -n "$native_phase" ]]; then
    jq -n --arg phase "$native_phase" --arg profile "$native_profile" --arg test "$test_name" --arg log "$label.log" \
      '{phase:$phase,profile:$profile,test:$test,log:$log,result:"passed"}' >> "$receipt_rows"
  fi
  printf '%s\n' "$output"
}

kill_mutation() {
  local inversion="$1"
  local killer="$2"
  local label="${3:-kill-$inversion}"
  local output
  local status

  restore_sources
  apply_mutation "$inversion"
  printf 'KILL  %s with %s\n' "$inversion" "$killer"
  set +e
  output="$(cargo test "${cargo_profile[@]}" --locked -p bombay-rs "${cargo_target[@]}" "$killer" -- --exact "${cargo_capture[@]}" 2>&1)"
  status=$?
  set -e
  printf '%s\n' "$output" > "$output_dir/$label.log"
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
  if ! NAMED_KILLER="$killer" perl -0ne '
    exit(/(?:\A|\n)failures:\n    \Q$ENV{NAMED_KILLER}\E\n\ntest result: FAILED\. 0 passed; 1 failed; 0 ignored; 0 measured; [0-9]+ filtered out; finished in [^\n]*\n(?:(?!^test result:).)*\z/ms ? 0 : 1);
  ' <<< "$output"; then
    printf '%s\n' "$output" >&2
    printf 'mutation failed outside its named killer: %s\n' "$inversion" >&2
    exit 1
  fi
  if [[ "$native_phase" == inverse ]]; then
    if [[ $status -ne 101 ]] || ! grep -Fxq "test $killer ... FAILED" <<< "$output" \
      || ! grep -Eq 'joined child report precedes no unfinished owned preparation|joined child report stays absent across parent and sibling progress' <<< "$output" \
      || grep -Eq 'resumed after completion|called after complete' <<< "$output"; then
      printf '%s\n' "$output" >&2
      printf '%s\n' 'Child preparation failed outside its intended native oracle' >&2
      exit 1
    fi
  fi

  jq -n --arg inversion "$inversion" --arg killer "$killer" \
    --arg phase "$native_phase" --arg profile "$native_profile" --arg log "$label.log" \
    '{inversion: $inversion, result: "killed", killer: $killer}
      + (if $phase == "" then {} else {phase:$phase,profile:$profile,log:$log} end)' >> "$receipt_rows"
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
  output="$(cargo test ${cargo_profile[@]+"${cargo_profile[@]}"} --locked -p bombay-rs --lib "$replay_test" -- --exact 2>&1)"
  status=$?
  set -e
  printf '%s\n' "$output" > "$output_dir/deny-$inversion.log"
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
original_profile=("${cargo_profile[@]}")
cargo_target=(--test fixed_supervisor_recovery)
cargo_capture=(--nocapture)
for native_phase in healthy inverse restored; do
  for native_profile in debug release; do
    cargo_profile=()
    if [[ "$native_profile" == release ]]; then cargo_profile=(--release); fi
    if [[ "$native_phase" == inverse ]]; then
      kill_mutation omit-child-preparation-retirement "$child_test" "child-preparation/$native_phase-$native_profile"
    else
      run_reference "$child_test" "child-preparation/$native_phase-$native_profile"
    fi
    sha256sum --status --check "$child_checksums"
  done
done
cargo_profile=("${original_profile[@]}")
behavior_revision="$(jq -r '.behavior.revision' docs/driver-law-manifest.json)"
jq -s \
  --arg revision "$behavior_revision" \
  '{
    schema: 1,
    behavior_revision: $revision,
    production_sources: [
      "crates/bombay/src/actor_execution.rs",
      "crates/bombay/src/actor_outcome.rs",
      "crates/bombay/src/launch.rs"
    ],
    results: .
  }' "$receipt_rows" > "$output_dir/actor-execution-law-evidence.json"

printf 'Actor-execution law evidence: %s\n' "$output_dir/actor-execution-law-evidence.json"
