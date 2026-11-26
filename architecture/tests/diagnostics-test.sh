#!/usr/bin/env bash
set -euo pipefail

probe=$PWD/architecture/diagnostics/kalloc-fast-probe.sh
check=$PWD/architecture/diagnostics/check-with-zone.sh
source architecture/tests/suites.sh
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
fail() { echo "diagnostics-test: FAILED: $*" >&2; exit 1; }
mkdir -p "$work/bin" "$work/tmp" "$work/clone/architecture/tests" "$work/logs"

cp architecture/tests/suites.sh "$work/clone/architecture/tests/suites.sh"
chmod -R u+w "$work/clone/architecture/tests/suites.sh"
printf '%s\n' "${suites[@]}" host > "$work/expected-suites"
while IFS= read -r suite; do
  cat > "$work/clone/architecture/tests/$suite-test.sh" <<'SUITE'
set -eu
suite=${0##*/}
suite=${suite%-test.sh}
[ -f "$TMPDIR/current" ]
wait_for_sample() {
  for ((attempt = 0; attempt < 200; attempt++)); do
    if grep -Fq "$1" "$DIAG_LOGS/zone-fine.tsv"; then return 0; fi
    "$DIAG_SLEEP" 0.02
  done
  return 1
}
case "$suite" in
  step)
    touch "$DIAG_LOGS/missing-zone"
    wait_for_sample $'\t?\tstep\t'
    unlink "$DIAG_LOGS/missing-zone"
    ;;
  space) wait_for_sample $'\t100\tspace\t' ;;
esac
echo "$suite" >> "$DIAG_SUITE_CALLS"
echo "fixture output for $suite"
echo "suite=$suite" >> "$GITHUB_OUTPUT"
[ "${DIAG_FAIL_SUITE:-}" != "$suite" ] || exit 7
SUITE
done < "$work/expected-suites"
cat > "$work/bin/zprint" <<'ZPRINT_STUB'
#!/bin/sh
[ "$#" = 2 ] && [ "$1" = -L ] && [ "$2" = data.kalloc.1024 ] || exit 9
[ ! -e "$DIAG_LOGS/missing-zone" ] || exit 8
echo 'data.kalloc.1024 a b c d e 100'
ZPRINT_STUB
cat > "$work/bin/ps" <<'PS_STUB'
#!/bin/sh
echo '00:00:00 XprotectService'
PS_STUB
cat > "$work/bin/sleep" <<'SLEEP_STUB'
#!/bin/sh
if [ "${1:-}" = 2 ]; then exec "$DIAG_SLEEP" 0.02; fi
SLEEP_STUB
cat > "$work/bin/yq" <<'YQ_STUB'
#!/bin/sh
exit 0
YQ_STUB
chmod +x "$work/bin/zprint" "$work/bin/ps" "$work/bin/sleep" "$work/bin/yq"

if "$BASH" "$probe" > "$work/kalloc-usage" 2>&1; then fail 'kalloc probe accepted no arguments'; fi
grep -q '^usage: kalloc-fast-probe.sh <count>$' "$work/kalloc-usage" || fail 'kalloc usage is missing'
if "$BASH" "$check" > "$work/check-usage" 2>&1; then fail 'suite diagnostic accepted no arguments'; fi
grep -q '^usage: check-with-zone.sh <clone> <log folder>$' "$work/check-usage" || fail 'suite diagnostic usage is missing'

DIAG_SLEEP=$(command -v sleep)
export DIAG_SLEEP PATH="$work/bin:$PATH" TMPDIR="$work/tmp"
export DIAG_SUITE_CALLS="$work/suite-calls"
export DIAG_LOGS="$work/logs"
: > "$DIAG_SUITE_CALLS"
if "$BASH" "$probe" invalid > "$work/kalloc-invalid" 2>&1; then fail 'kalloc probe accepted an invalid count'; fi
grep -q '^usage: ' "$work/kalloc-invalid" || fail 'invalid kalloc count gives no usage'
if [ -x /bin/bash ] && [ -x /usr/bin/env ] && [ -x /usr/bin/true ]; then
  "$BASH" "$probe" 1 > "$work/kalloc-output"
  grep -q '^start .*  count 1  nix bash ' "$work/kalloc-output" || fail 'kalloc probe did not report the count'
  [ "$(grep -c 'zone  *+0$' "$work/kalloc-output")" -eq 30 ] || fail 'kalloc did not report all phases'
else
  # Nix on Linux lacks the system binaries used in the macOS comparison.
  if "$BASH" "$probe" 1 > "$work/kalloc-output" 2>&1; then fail 'probe succeeded without system binaries'; fi
fi
[ -z "$(find "$work/tmp" -mindepth 1 -print -quit)" ] || fail 'kalloc private scratch was retained'

git init -q "$work/clone"
git -C "$work/clone" -c user.name=Fixture -c user.email=fixture@example.test \
  -c commit.gpgsign=false -c core.hooksPath=/dev/null commit --allow-empty -qm fixture
export DIAG_FAIL_SUITE=''
"$BASH" "$check" "$work/clone" "$work/logs" > "$work/check-output"
diff -u "$work/expected-suites" "$DIAG_SUITE_CALLS"
for suite in "${suites[@]}" host; do
  grep -q "^$suite-test[[:space:]]exit 0[[:space:]]" "$work/check-output" || fail "$suite status was not reported"
  grep -q "fixture output for $suite" "$work/logs/$suite.log" || fail "$suite evidence log was not retained"
done
awk -F '\t' '$2 == "?" && $3 == "step" { gap = 1 }
  gap && $2 == 100 { recovered = 1 }
  END { exit !recovered }' "$work/logs/zone-fine.tsv" || fail 'sampler did not recover after a missing zone reading'
[ -f "$work/logs/github-output" ] || fail 'GitHub output file was not retained'
[ ! -e "$work/logs/current" ] || fail 'diagnostic bookkeeping leaked into the evidence folder'
[ -z "$(find "$work/tmp" -mindepth 1 -print -quit)" ] || fail 'suite diagnostic private scratch was retained'

: > "$DIAG_SUITE_CALLS"
export DIAG_FAIL_SUITE=gate
export DIAG_LOGS="$work/failed-logs"
if "$BASH" "$check" "$work/clone" "$DIAG_LOGS" > "$work/check-failed"; then fail 'suite failure returned success'; fi
[ "$(cat "$DIAG_LOGS/gate.log")" = 'fixture output for gate' ] || fail 'failed suite evidence was not retained'
grep -q '^end .*status 1$' "$work/check-failed" || fail 'overall failure status was not reported'
[ -z "$(find "$work/tmp" -mindepth 1 -print -quit)" ] || fail 'private scratch survived a failed suite'
echo 'diagnostics-test: usage, probe stubs, suite order, retained evidence and scratch cleanup pass'
