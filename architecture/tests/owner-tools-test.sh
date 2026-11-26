#!/usr/bin/env bash
# Run from the repository root. Pass an architecture directory to test a modified copy.
set -euo pipefail
architecture=${1:-architecture}
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
work=$(cd -- "$work" && pwd -P)
export HOME=$work/home XDG_STATE_HOME=$work/xdg
mkdir -p "$HOME" "$XDG_STATE_HOME"
root="$work/fixture repo"
tree=$root/architecture
state="$work/owner state"
worktrees="$work/lane copies"
mkdir -p "$tree"/{design,quality,cards} "$worktrees" "$work/bin" "$work/temp"
cp -R "$architecture/owner" "$architecture/graph" "$architecture/project.env" "$tree/"
chmod -R u+w "$tree/"
snapshot=$tree/owner/facts-snapshot.sh registry=$tree/owner/tools.sh

printf "#!%s\n" "$BASH" > "$work/bin/gh"
cat >> "$work/bin/gh" <<'STUB'
echo 'owner-tools-test: unexpected gh call' >&2
exit 98
STUB
chmod +x "$work/bin/gh"
export PATH="$work/bin:$PATH" TMPDIR="$work/temp" HARNESS_REPO=fixture/owner-fixture
export HARNESS_OWNER_WORKFLOWS='fixture.yml second.yml'
export ROOT="$root"
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE
git -c init.templateDir= init -q "$root"
commit() {
  git -C "$root" -c user.name=Fixture -c user.email=fixture@example.invalid \
    -c commit.gpgsign=false -c core.hooksPath=/dev/null commit --allow-empty -qm "$1"
  git -C "$root" rev-parse --short=12 HEAD
}
first=$(commit first)

printf '#!%s\n' "$BASH" > "$tree/design/facts.sh"
cat >> "$tree/design/facts.sh" <<'STUB'
set -euo pipefail
[ "$PWD" = "$ROOT" ] && [ "$*" = 'apps/server/src apps/studio crates' ] || exit 98
case "${SNAPSHOT:-first}" in
  first)
    printf '%s\n' '# facts' $'# note\told comment\t999' \
      $'primitive\tA\t9\ta.rs:1' $'primitive\tB\t10\tb.rs:2' \
      $'condition\tGone\t4\tc.rs:3' $'churn\tSame\t20\td.rs:4' ;;
  second)
    printf '%s\n' '# facts' $'# note\tnew comment\t888' \
      $'primitive\tA\t10\ta.rs:1' $'primitive\tB\t9\tb.rs:2' \
      $'condition\tNew\t7\te.rs:5' $'churn\tSame\t20\td.rs:4' ;;
  empty) ;;
  malformed) printf '%s\n' $'primitive\tA\t5\ta.rs:1' $'broken\trow\t1' ;;
  wide)
    for ((i = 1; i <= 27; i++)); do printf 'wide\t%02d\t1\t\n' "$i"; done ;;
  fail)
    echo 'partial result'
    printf 'old error\nfirst error\nlast error\n' >&2
    exit 7 ;;
  *) exit 98 ;;
esac
STUB
printf '#!%s\n' "$BASH" > "$tree/quality/facts.sh"
cat >> "$tree/quality/facts.sh" <<'STUB'
set -euo pipefail
[ "$PWD" = "$ROOT" ] && [ "$*" = 'apps/server apps/studio crates' ] || exit 98
printf '%s\n' $'oracle-edit\tabc Fix\t1\tremoved=1 added=0'
STUB

expect_error() {
  local expected=$1 message=$2 status=0
  shift 2
  "$@" > "$work/out" 2> "$work/err" || status=$?
  if [ "$status" != "$expected" ]; then
    printf 'owner-tools-test: expected exit %s, got %s from %s\n' "$expected" "$status" "$*" >&2
    cat "$work/out" "$work/err" >&2
    return 1
  fi
  if [ -n "$message" ]; then
    [ ! -s "$work/out" ]
    diff -u <(printf '%s\n' "$message") "$work/err"
  else
    [ ! -s "$work/err" ]
  fi
}
snapshot_usage='Usage: facts-snapshot.sh <state folder>'
expect_error 2 "$snapshot_usage" bash "$snapshot"
expect_error 2 "$snapshot_usage" bash "$snapshot" ''
expect_error 2 "$snapshot_usage" bash "$snapshot" "$state" extra

(cd -- "$work" && bash "$snapshot" 'owner state') > "$work/first"
diff -u - "$work/first" <<EXPECTED
design-facts at $first (921ccc4457da):
  churn              rows    1  sum     20
  condition          rows    1  sum      4
  primitive          rows    2  sum     19
  no snapshot before this one
quality-facts at $first (349c371be2ae):
  oracle-edit        rows    1  sum      1
  no snapshot before this one
EXPECTED
facts=$state/facts
[ ! -e "$facts/design-facts-previous.tsv" ]
[ ! -e "$facts/quality-facts-previous.tsv" ]
cp "$facts/design-facts-latest.tsv" "$work/first.tsv"
diff -u <(printf '%s\n' "$first") "$facts/design-facts-latest.commit"

second=$(commit second)
SNAPSHOT=second bash "$snapshot" "$state" > "$work/second"
diff -u - "$work/second" <<EXPECTED
design-facts at $second (58e868ee8d3a):
  churn              rows    1  sum     20
  condition          rows    1  sum      7
  primitive          rows    2  sum     19
  changed since $first:
    down  primitive  B  10 -> 9
    gone  condition  Gone  (was 4)
    new   condition  New  = 7
    up    primitive  A  9 -> 10
quality-facts at $second (349c371be2ae):
  oracle-edit        rows    1  sum      1
  no snapshot before this one
EXPECTED
diff -u "$work/first.tsv" "$facts/design-facts-previous.tsv"
diff -u <(printf '%s\n' "$first") "$facts/design-facts-previous.commit"
diff -u <(printf '%s\n' "$second") "$facts/design-facts-latest.commit"
diff -u <(printf '%s\n' "$second") "$facts/quality-facts-latest.commit"
jq -Rr --arg project owner-fixture --arg source design-facts --arg commit "$second" \
  -f "$tree/graph/facts.jq" "$facts/design-facts-latest.tsv" | diff -u - "$state/design-facts.nq"
jq -Rr --arg project owner-fixture --arg source quality-facts --arg commit "$second" \
  -f "$tree/graph/facts.jq" "$facts/quality-facts-latest.tsv" | diff -u - "$state/quality-facts.nq"
SNAPSHOT=second bash "$snapshot" "$state" | diff -u "$work/second" -

cp "$facts/design-facts-latest.tsv" "$work/second.tsv"
cp "$state/design-facts.nq" "$work/second.nq"
expect_error 1 '' env SNAPSHOT=fail bash "$snapshot" "$state"
printf '%s\n' 'design-facts: the tool failed: first error last error ' \
  "quality-facts at $second (349c371be2ae):" \
  '  oracle-edit        rows    1  sum      1' '  no snapshot before this one' |
  diff -u - "$work/out"
diff -u "$work/second.tsv" "$facts/design-facts-latest.tsv"
diff -u "$work/second.nq" "$state/design-facts.nq"
[ -z "$(ls -A "$TMPDIR")" ]

mv "$tree/quality/facts.sh" "$work/quality.sh"
third=$(commit third)
for invalid in empty malformed; do
  expect_error 1 '' env SNAPSHOT="$invalid" bash "$snapshot" "$state"
  diff -u - "$work/out" <<'EXPECTED'
design-facts: the tool failed: expected at least one row with four tab-separated fields
quality-facts: not in this tree (architecture/quality/facts.sh)
EXPECTED
  diff -u "$work/second.tsv" "$facts/design-facts-latest.tsv"
  diff -u "$work/second.nq" "$state/design-facts.nq"
  diff -u <(printf '%s\n' "$second") "$facts/design-facts-latest.commit"
  diff -u "$work/first.tsv" "$facts/design-facts-previous.tsv"
done
SNAPSHOT=wide bash "$snapshot" "$work/wide state" > /dev/null
rm -- "$work/wide state/facts/design-facts-latest.commit"
SNAPSHOT=second bash "$snapshot" "$work/wide state" | diff -u /dev/fd/3 - 3<<EXPECTED
design-facts at $third (58e868ee8d3a):
  churn              rows    1  sum     20
  condition          rows    1  sum      7
  primitive          rows    2  sum     19
  changed since the snapshot before:
$(for ((i = 1; i <= 25; i++)); do printf '    gone  wide  %02d  (was 1)\n' "$i"; done)
    6 changed rows left out
quality-facts: not in this tree (architecture/quality/facts.sh)
EXPECTED

# Each check records the arguments so the registry's routing and order are visible.
printf '#!%s\n' "$BASH" > "$work/check"
cat >> "$work/check" <<'STUB'
set -euo pipefail
printf '%s' "${0##*/}" >> "$CALLS"
printf '\t%s' "$@" >> "$CALLS"
printf '\n' >> "$CALLS"
case "${0##*/}" in
  q.sh)
    printf '?card\n'
    if [ "$2" = loops ] && [ "${REGISTRY_FAIL:-0}" = 1 ]; then
      echo 'fixture loops error' >&2
      exit 7
    fi
    if [ "$2" != hot-design ]; then
      printf '%s row\n' "$2"
    fi ;;
  host-series.sh)
    if [ "${REGISTRY_FAIL:-0}" = 1 ]; then
      echo 'fixture host error' >&2
      exit 8
    fi
    echo 'host-series is stopped' ;;
  gate-times.sh)
    if [ "${REGISTRY_FAIL:-0}" = gate ]; then
      echo 'fixture gate recorder error' >&2
      exit 9
    fi
    echo 'gate-times is stopped' ;;
  facts-snapshot.sh)
    if [ "${REGISTRY_HANG:-0}" = 1 ]; then
      (sleep 3; : > "$CHILD") &
      wait
    else
      cat > "$INPUT"
      echo 'fixture facts'
    fi ;;
  build.sh) echo 'fixture graph' ;;
  close-merged.sh) echo 'fixture open issue' ;;
  harness.sh)
    [ "$*" = doctor ] && [ -d "$HARNESS_STATE" ] && [ -d "$HARNESS_WORKTREES" ]
    echo "fixture doctor: $HARNESS_STATE $HARNESS_WORKTREES" ;;
  session.sh)
    [ "$*" = 'loop status' ] && [ -d "$HARNESS_STATE" ] && [ -d "$HARNESS_WORKTREES" ]
    echo 'fixture owner loop'
    [ "${REGISTRY_FAIL:-0}" != loop ] ;;
  *) exit 98 ;;
esac
STUB
for path in owner/facts-snapshot.sh graph/build.sh graph/q.sh host-series.sh gate-times.sh harness.sh owner/session.sh; do
  cp "$work/check" "$tree/$path"
done
export CALLS="$work/calls" INPUT="$work/stdin" CHILD="$work/child"
registry_usage='Usage: tools.sh check|list [<state folder> <worktree parent>]'
expect_error 2 "$registry_usage" bash "$registry"
expect_error 2 "$registry_usage" bash "$registry" other "$state" "$worktrees"
expect_error 2 "$registry_usage" bash "$registry" list '' "$worktrees"
expect_error 2 "$registry_usage" bash "$registry" check "$state" "$work/absent"

unset HARNESS_STATE HARNESS_WORKTREES
bash "$registry" list "$work/unused state" "$worktrees" > "$work/list"
diff -u /dev/fd/3 "$work/list" 3<<'EXPECTED'
facts|check|Design and quality facts of HEAD: totals, and what changed since the last check|tool owner/facts-snapshot.sh "$state"
graph|check|Build the graph from the board, the pull requests and both facts outputs|tool graph/build.sh "$state"
loops|check|Open cards that wait for each other (none is the good state)|tool graph/q.sh "$state" loops | tail -n +2
blockers|check|Open cards by the number of open cards that wait for them, through other cards too|tool graph/q.sh "$state" blockers 8 | tail -n +2
oracle|check|Removed and added assertions by card (which work touched the tests the wrong way round)|tool graph/q.sh "$state" oracle | tail -n +2
hot-design|check|Files with a design signal, by the number of cards that changed them|tool graph/q.sh "$state" hot-design | tail -n +2
merged-open|check|Cards that are Merged with an open issue|tool cards/close-merged.sh
recorders|check|The host recorder and the gate time recorder are alive|status=0; tool host-series.sh status "$state" || status=$?; tool gate-times.sh status "$state" "$worktrees" || status=$?; exit "$status"
doctor|check|The machine can start the harness|HARNESS_STATE="$state" HARNESS_WORKTREES="$worktrees" tool harness.sh doctor
owner-loop|check|The owner check loop is running|HARNESS_STATE="$state" HARNESS_WORKTREES="$worktrees" tool owner/session.sh loop status
releases|on-demand|What an answer or a merge releases|architecture/graph/q.sh <state folder> releases <card>
ready|on-demand|What a lane can take, in its order|architecture/graph/q.sh <state folder> ready <workstream>
touched|on-demand|Merged changes that touched a path since a time|architecture/graph/q.sh <state folder> touched <path> <since>
overlap|on-demand|Cards whose changes touched the same files|architecture/graph/q.sh <state folder> overlap <card>
lookup|on-demand|Symbols, similar names and applicable gate rules on origin/main|architecture/lookup.sh {exists|like|rules} <name or path>
design-facts|on-demand|The design signals of one scope with every place|architecture/design/facts.sh <scope>
fitness|on-demand|Book-grounded AST fitness counts for a revision|architecture/design/fitness.sh <rev> [scope...]
quality-facts|on-demand|Test honesty and rework of one scope|architecture/quality/facts.sh [--commits r] [--window a b] <scope>
release-dep|on-demand|A card stops waiting for the whole of another card|architecture/cards/release-dep.sh <card> <dependency> <new text line> <note file>
new-card|on-demand|Make a card with the board fields that make it selectable|architecture/cards/new.sh <title> <body file> <workstream> <sequence> [--phase <phase>]
harness|on-demand|Check the machine and start or show the harness|architecture/harness.sh up|status|doctor
lane|on-demand|Start, stop or show a lane|architecture/lane.sh start|stop <lane>|status
session|on-demand|Run the owner session or control its check loop|architecture/owner/session.sh talk|check|loop start|status|stop
land|on-demand|Land an accepted harness step under the merge turn|architecture/land.sh [--dry-run] <clone> <branch> <title> <body file>
turns|on-demand|Report gate time and merge-turn waits|architecture/turns.sh <worktree parent> [hours]
watch-parks|on-demand|Wait for a park on a named card or a park without a dependency|architecture/watch-parks.sh <worktree parent> [card ...]
sequence|on-demand|Change the place of a card in its lane|architecture/cards/sequence.sh <card> <sequence> [--dry-run]
status|on-demand|Change a card's board status|architecture/cards/status.sh <card> <status> [--dry-run]
guard|on-demand|Claude pre-tool hook for the owner session. Claude runs it|architecture/owner/guard.sh
owner-check|on-demand|Measure the lanes and run the registered checks|architecture/owner/check.sh [hours]
measure|on-demand|Report lane activity, board progress and host state|architecture/measure.sh <worktree parent> <card assignee> [hours]
tools|on-demand|List the tools or run the checks|architecture/owner/tools.sh list|check [<state folder> <worktree parent>]
workflow-fixture.yml|on-demand|Start an owner workflow by hand|gh workflow run fixture.yml -R fixture/owner-fixture --ref main
workflow-second.yml|on-demand|Start an owner workflow by hand|gh workflow run second.yml -R fixture/owner-fixture --ref main
EXPECTED
[ ! -e "$CALLS" ]
[ ! -e "$work/unused state" ]
bash "$registry" list | diff -u "$work/list" -
if command -v sha256sum >/dev/null; then
  digest=$(printf '%s' "$root" | sha256sum)
else
  digest=$(printf '%s' "$root" | shasum -a 256)
fi
default_state=$XDG_STATE_HOME/harness/${root##*/}-${digest:0:8}
[ ! -e "$default_state" ]
[ ! -e "$root-worktrees" ]

expect_error 1 '' bash "$registry" check "$state" "$worktrees"
cp "$work/out" "$work/check-output"
diff -u - "$work/check-output" <<EXPECTED
== tool: facts — Design and quality facts of HEAD: totals, and what changed since the last check ==
  fixture facts
== tool: graph — Build the graph from the board, the pull requests and both facts outputs ==
  fixture graph
== tool: loops — Open cards that wait for each other (none is the good state) ==
  loops row
== tool: blockers — Open cards by the number of open cards that wait for them, through other cards too ==
  blockers row
== tool: oracle — Removed and added assertions by card (which work touched the tests the wrong way round) ==
  oracle row
== tool: hot-design — Files with a design signal, by the number of cards that changed them ==
  none
== tool: merged-open — Cards that are Merged with an open issue ==
  not in this tree (architecture/cards/close-merged.sh)
== tool: recorders — The host recorder and the gate time recorder are alive ==
  host-series is stopped
  gate-times is stopped
== tool: doctor — The machine can start the harness ==
  fixture doctor: $state $worktrees
== tool: owner-loop — The owner check loop is running ==
  fixture owner loop
== tools run: 9 of 10; passed: 9; failed: 0; absent: 1 (merged-open) ==
EXPECTED
{
  printf '%s\t%s\n' facts-snapshot.sh "$state" build.sh "$state"
  printf '%s\t%s\t%s\n' q.sh "$state" loops
  printf '%s\t%s\t%s\t%s\n' q.sh "$state" blockers 8
  printf '%s\t%s\t%s\n' q.sh "$state" oracle q.sh "$state" hot-design
  printf '%s\t%s\t%s\n' host-series.sh status "$state"
  printf '%s\t%s\t%s\t%s\n' gate-times.sh status "$state" "$worktrees"
  printf 'harness.sh\tdoctor\nsession.sh\tloop\tstatus\n'
} > "$work/expected-calls"
diff -u "$work/expected-calls" "$CALLS"

: > "$CALLS"
cp "$work/check" "$tree/cards/close-merged.sh"
awk 'NR == 7 { printf "close-merged.sh\t\n" } { print }' "$work/expected-calls" > "$work/all-calls"
sed -e 's@  not in this tree (architecture/cards/close-merged.sh)@  fixture open issue@' \
  -e 's/== tools run:.*/== tools run: 10 of 10; passed: 10; failed: 0; absent: 0 ==/' \
  "$work/check-output" > "$work/passed"
expect_error 0 '' bash "$registry" check "$state" "$worktrees"
diff -u "$work/passed" "$work/out"
diff -u "$work/all-calls" "$CALLS"
[ ! -s "$INPUT" ]

: > "$CALLS"
mkdir "$root-worktrees"
expect_error 0 '' bash "$registry" check
sed -e "s@$state@$default_state@g" -e "s@$worktrees@$root-worktrees@g" "$work/passed" | diff -u - "$work/out"
sed -e "s@$state@$default_state@g" -e "s@$worktrees@$root-worktrees@g" "$work/all-calls" | diff -u - "$CALLS"
[ -n "$(find "$default_state" -prune -perm 700 -print)" ]

expect_error 1 '' env REGISTRY_FAIL=loop bash "$registry" check "$state" "$worktrees"
sed 's/== tools run:.*/== tools run: 10 of 10; passed: 9; failed: 1 (owner-loop); absent: 0 ==/' \
  "$work/passed" | diff -u - "$work/out"

export REAL_AWK
REAL_AWK=$(command -v awk)
printf "#!%s\n" "$BASH" > "$work/bin/awk"
cat >> "$work/bin/awk" <<'STUB'
if [ "${REGISTRY_COUNT_MISMATCH:-0}" = 1 ]; then
  "$REAL_AWK" '{ next } END { print 11 }'
else
  exec "$REAL_AWK" "$@"
fi
STUB
chmod +x "$work/bin/awk"
expect_error 1 '' env REGISTRY_COUNT_MISMATCH=1 bash "$registry" check "$state" "$worktrees"
sed 's/tools run: 10 of 10/tools run: 10 of 11/' "$work/passed" | diff -u - "$work/out"

: > "$CALLS"
expect_error 1 '' env REGISTRY_FAIL=1 bash "$registry" check "$state" "$worktrees"
sed -e 's/  loops row/  fixture loops error/' \
  -e 's/  host-series is stopped/  fixture host error/' \
  -e 's/== tools run:.*/== tools run: 10 of 10; passed: 8; failed: 2 (loops recorders); absent: 0 ==/' "$work/passed" |
  diff -u - "$work/out"
diff -u "$work/all-calls" "$CALLS"

: > "$CALLS"
expect_error 1 '' env REGISTRY_FAIL=gate bash "$registry" check "$state" "$worktrees"
sed -e 's/  gate-times is stopped/  fixture gate recorder error/' \
  -e 's/== tools run:.*/== tools run: 10 of 10; passed: 9; failed: 1 (recorders); absent: 0 ==/' "$work/passed" |
  diff -u - "$work/out"
diff -u "$work/all-calls" "$CALLS"

: > "$CALLS"
expect_error 1 '' env REGISTRY_HANG=1 OWNER_TOOL_TIMEOUT=2 bash "$registry" check "$state" "$worktrees"
sed -e 's/  fixture facts/  timeout/' \
  -e 's/== tools run:.*/== tools run: 10 of 10; passed: 9; failed: 1 (facts); absent: 0 ==/' "$work/passed" |
  diff -u - "$work/out"
diff -u "$work/all-calls" "$CALLS"
sleep 1.2
if [ -e "$CHILD" ]; then
  echo 'owner-tools-test: the timed-out tool left its child running' >&2
  exit 1
fi
echo 'owner-tools-test: registry, snapshot changes and errors pass'
