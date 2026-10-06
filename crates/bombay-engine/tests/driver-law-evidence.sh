#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${IN_NIX_SHELL:-}" && -z "${NIX_BUILD_TOP:-}" ]]; then
  printf '%s\n' 'driver-law evidence must run inside the pinned Nix shell' >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd "$script_dir/../../.." && pwd)"
selected_law=""
output_dir=""
profile_arguments=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --law)
      selected_law="${2:?--law requires a canonical law id}"
      shift 2
      ;;
    --release)
      profile_arguments=(--release)
      shift
      ;;
    --output)
      output_dir="${2:?--output requires a directory}"
      shift 2
      ;;
    *)
      printf 'unknown driver-law evidence argument: %s\n' "$1" >&2
      exit 2
      ;;
  esac
done

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/bombay-driver-law.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT
if [[ -z "$output_dir" ]]; then
  output_dir="$(mktemp -d "${TMPDIR:-/tmp}/bombay-driver-law-result.XXXXXX")"
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

manifest='docs/driver-law-manifest.json'
driver='crates/bombay-engine/src/driver.rs'
environment='crates/bombay-engine/src/environment.rs'
pristine_driver="$work_dir/driver.rs"
pristine_environment="$work_dir/environment.rs"
pristine_manifest="$work_dir/driver-law-manifest.json"
receipt_rows="$work_dir/receipt.ndjson"
cp "$driver" "$pristine_driver"
cp "$environment" "$pristine_environment"
cp "$manifest" "$pristine_manifest"
: > "$receipt_rows"

restore_sources() {
  cp "$pristine_driver" "$driver"
  cp "$pristine_environment" "$environment"
  cp "$pristine_manifest" "$manifest"
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

run_reference() {
  local reference="$1"
  local suite="${reference%%::*}"
  local test_name="${reference#*::}"
  printf 'PASS  %s\n' "$reference"
  cargo test "${profile_arguments[@]}" --locked -p bombay-engine --test "$suite" "$test_name" -- --exact
}

apply_mutation() {
  local law="$1"
  local inversion="$2"
  case "$law:$inversion" in
    D-INIT-1:duplicate-initialization)
      replace_exact "$driver" \
        '                        behavior::initialize(&mut self.behavior)' \
        '                        drop(behavior::initialize(&mut self.behavior));
                        behavior::initialize(&mut self.behavior)'
      ;;
    D-TURN-1:prefetch-before-apply)
      replace_exact "$driver" \
        '                    let failures = receive_operation(
                        || active.apply(&mut self.actions, &mut self.interpretation),' \
        '                    drop(active.next().await);
                    let failures = receive_operation(
                        || active.apply(&mut self.actions, &mut self.interpretation),'
      ;;
    D-SETTLE-1:progress-retained-settlement)
      replace_exact "$driver" \
        '                            SourceCustody::Retained(settlement) => {
                                self.settlements
                                    .insert(index, SettlementTurn::Retained(settlement));' \
        '                            SourceCustody::Retained(settlement) => {
                                self.settlements
                                    .insert(index, SettlementTurn::Offer(settlement));'
      ;;
    D-TERM-1:corruption-as-stop)
      replace_exact "$driver" \
        '                            (SettlementStatus::Corrupt, _) => {
                                self.reject(DriverError::Settlement(SettlementFailure::Corrupt));' \
        '                            (SettlementStatus::Corrupt, _) => {
                                self.complete(Completion::Stopped);'
      ;;
    D-RETIRE-1:erase-retirement-settlements)
      replace_exact "$driver" \
        '                                .map(SettlementTurn::into_settlement)
                                .collect(),' \
        '                                .map(SettlementTurn::into_settlement)
                                .take(0)
                                .collect(),'
      ;;
    D-PORT-1:prepared-environment-next)
      replace_exact "$environment" \
        'pub trait Environment<B: Behavior<Ph = Never>> {' \
        'pub trait Environment<B: Behavior<Ph = Never>> {
    /// Invalid mutation: prepared environments must not expose ingress.
    fn next(&mut self) {}'
      ;;
    D-SURFACE-1:driver-reset-control)
      replace_exact "$driver" \
        '    fn reject(&mut self, error: DriverError<B::Error, E::Error>) {' \
        '    /// Invalid mutation: the Driver must not expose a reset control.
    pub fn reset(&mut self) {}

    fn reject(&mut self, error: DriverError<B::Error, E::Error>) {'
      ;;
    D-EVIDENCE-1:remove-first-law-row)
      jq '.laws = .laws[1:]' "$manifest" > "$work_dir/mutated-manifest.json"
      mv "$work_dir/mutated-manifest.json" "$manifest"
      ;;
    *)
      printf 'no production mutation implements %s:%s\n' "$law" "$inversion" >&2
      exit 1
      ;;
  esac
}

kill_mutation() {
  local law="$1"
  local inversion="$2"
  local killer="$3"
  local suite="${killer%%::*}"
  local test_name="${killer#*::}"
  local output
  local status

  restore_sources
  apply_mutation "$law" "$inversion"
  printf 'KILL  %s with %s\n' "$inversion" "$killer"
  set +e
  output="$(cargo test "${profile_arguments[@]}" --locked -p bombay-engine --test "$suite" "$test_name" -- --exact 2>&1)"
  status=$?
  set -e
  restore_sources
  printf '%s\n' "$output"

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
  if ! NAMED_KILLER="$test_name" perl -0ne '
    exit(/(?:\A|\n)failures:\n    \Q$ENV{NAMED_KILLER}\E\n\ntest result: FAILED\. 0 passed; 1 failed; 0 ignored; 0 measured; [0-9]+ filtered out; finished in [^\n]*\n(?:(?!^test result:).)*\z/ms ? 0 : 1);
  ' <<< "$output"; then
    printf '%s\n' "$output" >&2
    printf 'mutation failed outside its named killer: %s\n' "$inversion" >&2
    exit 1
  fi
}

if [[ -n "$selected_law" ]] && ! jq -e --arg law "$selected_law" \
  '.laws[] | select(.law == $law)' "$manifest" >/dev/null; then
  printf 'unknown canonical Driver law: %s\n' "$selected_law" >&2
  exit 2
fi

while IFS= read -r row; do
  restore_sources
  law="$(jq -r '.law' <<< "$row")"
  positive="$(jq -r '.positive.test' <<< "$row")"
  boundary="$(jq -r '.boundary.test' <<< "$row")"
  inversion="$(jq -r '.inversion.id' <<< "$row")"
  killer="$(jq -r '.inversion.killer' <<< "$row")"
  revision="$(jq -r '.inversion.revision' <<< "$row")"
  positive_command="$(jq -r '.positive.command' <<< "$row")"
  boundary_command="$(jq -r '.boundary.command' <<< "$row")"
  inversion_command="$(jq -r '.inversion.command' <<< "$row")"

  if [[ ${#profile_arguments[@]} -ne 0 ]]; then
    positive_command="${positive_command/cargo test --locked/cargo test --release --locked}"
    boundary_command="${boundary_command/cargo test --locked/cargo test --release --locked}"
    inversion_command="$inversion_command --release"
  fi

  run_reference "$positive"
  run_reference "$boundary"
  kill_mutation "$law" "$inversion" "$killer"
  jq -n \
    --arg law "$law" \
    --arg revision "$revision" \
    --arg positive "$positive" \
    --arg positive_command "$positive_command" \
    --arg boundary "$boundary" \
    --arg boundary_command "$boundary_command" \
    --arg inversion "$inversion" \
    --arg inversion_command "$inversion_command" \
    --arg killer "$killer" \
    '{
      law: $law,
      revision: $revision,
      positive: {reference: $positive, command: $positive_command, result: "passed"},
      boundary: {reference: $boundary, command: $boundary_command, result: "passed"},
      inversion: {id: $inversion, command: $inversion_command, killer: $killer, result: "killed"}
    }' >> "$receipt_rows"
done < <(
  if [[ -n "$selected_law" ]]; then
    jq -c --arg law "$selected_law" '.laws[] | select(.law == $law)' "$manifest"
  else
    jq -c '.laws[]' "$manifest"
  fi
)

restore_sources
behavior_revision="$(jq -r '.behavior.revision' "$manifest")"
jq -s \
  --arg revision "$behavior_revision" \
  --arg rustc "$(rustc --version)" \
  --arg cargo "$(cargo --version)" \
  '{
    schema: 1,
    behavior_revision: $revision,
    toolchain: {rustc: $rustc, cargo: $cargo},
    laws: .
  }' "$receipt_rows" > "$output_dir/driver-law-evidence.json"

printf 'Driver-law evidence receipt: %s\n' "$output_dir/driver-law-evidence.json"
