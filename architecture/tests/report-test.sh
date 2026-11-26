#!/usr/bin/env bash
# Self-test of the lane report, with a fixed history, fixed pull requests,
# fixed commits, fixed Checks runs, a scripted rule scan and stand-ins for Git
# and GitHub. Run from the repository root.
set -euo pipefail

report=$PWD/architecture/report.sh
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
fail() { echo "report-test: FAILED: $1"; cat "$work/out" 2> /dev/null; exit 1; }
value() { grep -F "  $1" "$work/out" | head -n 1 | sed -E 's/^  .{44} //'; }
expect() { [ "$(value "$1")" = "$2" ] || fail "$1: got '$(value "$1")', wanted '$2'"; }

# Stand-ins, first on PATH. The rule scan prints the file `scan` and returns
# the code in `scan.code`. Git and GitHub answer from the fixtures only when
# STUB_TOOLS is `answer`, and fail when it is `fail`. In every other run a
# call to them is a defect of the report: it is written down and fails.
mkdir "$work/bin"
stub() { printf '#!%s\n' "$BASH" > "$work/bin/$1"; cat >> "$work/bin/$1"; chmod +x "$work/bin/$1"; }
stub ast-grep << 'STUB'
cat "$STUB_DIR/scan" 2> /dev/null
exit "$(cat "$STUB_DIR/scan.code" 2> /dev/null || echo 2)"
STUB
stub git << 'STUB'
case "${STUB_TOOLS:-}" in
  answer) echo "$*" >> "$STUB_DIR/git.calls" ;;
  fail) [ "$1" = rev-parse ] && exit 0; exit 1 ;;
  *) echo "git $*" >> "$STUB_DIR/unexpected.calls"; exit 99 ;;
esac
case "$*" in
  "rev-parse origin/main") ;;
  log*--numstat*) cat "$STUB_DIR/numstat" ;;
  log*--format=%B*) cat "$STUB_DIR/commits" ;;
  *) exit 98 ;;
esac
STUB
stub gh << 'STUB'
case "${STUB_TOOLS:-}" in
  answer) echo "$*" >> "$STUB_DIR/gh.calls" ;;
  fail) exit 1 ;;
  *) echo "gh $*" >> "$STUB_DIR/unexpected.calls"; exit 99 ;;
esac
case "$1 $2" in
  "auth token") echo stand-in-token ;;
  "pr list") cat "$STUB_DIR/prs.json" ;;
  "issue list") echo 4 ;;
  "run list") cat "$STUB_DIR/runs.json" ;;
  *) exit 98 ;;
esac
STUB

# History. Lines before the report's date do not count, but a start on the
# evening before explains the failure after midnight. Card #2 has three starts
# without a lead (the failure follows within two minutes) and two long sessions
# without a result, one of them over midnight.
printf '%s\t%s\n' \
  '2026-11-01 10:00' 'session on #1' \
  '2026-11-01 10:30' '#1 STEP merged 5' \
  '2026-11-04 23:59' 'session on #2' \
  '2026-11-05 00:00' '#2 STEP failed the lead wrote no result line' \
  '2026-11-08 23:00' 'session on #2' \
  '2026-11-09 00:30' '#2 STEP failed the lead wrote no result line' \
  '2026-11-09 23:59' 'session on #2' \
  '2026-11-10 00:00' '#2 STEP failed the lead wrote no result line' \
  '2026-11-10 07:00' 'session on #2' \
  '2026-11-10 07:01' '#2 STEP failed the lead wrote no result line' \
  '2026-11-10 07:21' 'session on #2' \
  '2026-11-10 07:51' '#2 STEP failed the lead wrote no result line' \
  '2026-11-10 08:00' 'ended: no ready card' \
  '2026-11-10 08:10' 'session on the weekly design review' \
  '2026-11-10 08:20' 'the weekly design review STEP failed no judge' \
  '2026-11-10 08:21' 'session on the weekly design review' \
  '2026-11-10 08:40' 'the weekly design review STEP reviewed 2 apps/studio/src/program' \
  '2026-11-10 09:00' 'session on #2' \
  '2026-11-10 09:20' '#2 STEP waiting 10 Linux GPU host busy' \
  '2026-11-10 09:30' 'session on #2' \
  '2026-11-10 09:50' '#2 STEP waiting 10 Linux GPU host busy' \
  '2026-11-10 10:00' 'session on #2' \
  '2026-11-10 10:40' '#2 STEP merged 7' \
  '2026-11-10 10:41' 'session on #3' \
  '2026-11-10 10:50' '#3 STEP planned' \
  '2026-11-10 10:51' 'session on #3' \
  '2026-11-10 11:20' '#3 STEP failed tests' \
  '2026-11-10 11:21' 'session on #3' \
  '2026-11-10 11:40' '#3 STEP failed tests' \
  '2026-11-10 11:40' 'parked #3: failed tests' \
  '2026-11-10 12:00' 'stopped: GitHub could not be read 36 times in a row' > "$work/history"

# Merged pull requests. #7 changes one scope and its tests. #8 changes two
# scopes. #10 changes two modules in one file each and the application file.
# #11 changes the two files of the application itself, which are one scope.
# #12 changes only a design document of the Studio lane. #13 changes two files
# of the execution crate, which is one scope. #9 belongs to the Server lane.
cat > "$work/prs.json" << 'JSON'
[
  {"number": 7, "body": "Text.\nReview rounds: 3\nMore.", "files": [
    {"path": "apps/studio/src/program/engine/mod.rs"}, {"path": "apps/studio/src/program/schedule.rs"},
    {"path": "apps/studio/tests/engine/mod.rs"}]},
  {"number": 8, "body": "Review rounds: 1", "files": [
    {"path": "apps/studio/src/timing/clock.rs"}, {"path": "apps/studio/src/formats/time.rs"},
    {"path": "docs/arch/studio/architecture.md"}]},
  {"number": 9, "body": "No record.", "files": [{"path": "apps/server/src/models/events.rs"}]},
  {"number": 10, "body": "No record.", "files": [
    {"path": "apps/studio/src/output.rs"}, {"path": "apps/studio/src/telemetry.rs"},
    {"path": "apps/studio/src/lib.rs"}, {"path": "apps/studio/Cargo.toml"}]},
  {"number": 11, "body": "Review rounds: 2", "files": [
    {"path": "apps/studio/src/lib.rs"}, {"path": "apps/studio/src/main.rs"}]},
  {"number": 12, "body": "Review rounds: 2", "files": [{"path": "docs/arch/studio/timing.md"}]},
  {"number": 13, "body": "Review rounds: 2", "files": [
    {"path": "apps/studio/execution/src/lib.rs"}, {"path": "apps/studio/execution/build.rs"}]}
]
JSON
printf '%s\n' 'feat: one' '' 'Reuse: std' '' 'fix: two' '' 'Step-size: one move' 'Rules: with its code' \
  'Reuse: none fills the gap' > "$work/commits"
# Changed lines: Rust code of both lanes, tests, a binary file and files that are no Rust code.
printf '%s\t%s\t%s\n' 40 10 apps/studio/src/program/engine/mod.rs 5 5 apps/studio/src/timing/clock.rs \
  300 0 apps/studio/tests/engine/mod.rs - - apps/studio/tests/fixture.bin 7 0 crates/editorial/src/lib.rs \
  3 1 apps/studio/src/output.rs 2 0 apps/studio/src/lib.rs 20 0 apps/studio/domain/timing/src/lib.rs \
  90 0 apps/studio/domain/timing/tests/a.rs 500 0 apps/studio/Cargo.toml 9 0 apps/server/migration/src/m1.rs \
  600 0 apps/server/assets/views/a.html > "$work/numstat"
# Checks runs, newest first: a cancelled run, a skipped night, a green manual
# run and a night that timed out in the days of the report, a failed push check
# of the earlier workflow, and a failed night before the date.
cat > "$work/runs.json" << 'JSON'
[
  {"conclusion": "cancelled", "event": "workflow_dispatch", "createdAt": "2026-11-11T09:00:00Z"},
  {"conclusion": "skipped", "event": "schedule", "createdAt": "2026-11-11T02:17:00Z"},
  {"conclusion": "success", "event": "workflow_dispatch", "createdAt": "2026-11-10T12:00:00Z"},
  {"conclusion": "failure", "event": "push", "createdAt": "2026-11-10T11:00:00Z"},
  {"conclusion": "timed_out", "event": "schedule", "createdAt": "2026-11-10T02:17:00Z"},
  {"conclusion": "failure", "event": "schedule", "createdAt": "2026-11-01T02:17:00Z"}
]
JSON
# The code of the lane: a tests folder with three tests, and with comments and
# an attribute that only look like tests; an execution folder without Rust files;
# the source folder; no domain folder yet.
mkdir -p "$work/apps/studio/tests/engine" "$work/apps/studio/src" "$work/apps/studio/execution"
printf '%s\n' '#[test]' 'fn one() {}' '#[tokio::test]' 'async fn two() {}' \
  '    #[tokio::test(flavor = "multi_thread")]' '    async fn three() {}' \
  '// #[test] in a comment is no test' '#[test_case(1)]' 'fn four(_: u8) {}' \
  '/*' '#[test]' 'fn old() {}' '*/' '/* #[test] */' \
  > "$work/apps/studio/tests/engine/mod.rs"

# A report that waits for input or does not end is stopped after a minute.
limit=()
if command -v timeout > /dev/null; then limit=(timeout 60); fi
# Runs the report for a lane with the fixture files. More settings follow the lane.
run() {
  local lane=$1; shift
  got=0
  env PATH="$work/bin:$PATH" STUB_DIR="$work" REPORT_SINCE=2026-11-05 REPORT_HISTORY="$work/history" \
    REPORT_PRS="$work/prs.json" REPORT_COMMITS="$work/commits" REPORT_NUMSTAT="$work/numstat" \
    REPORT_RUNS="$work/runs.json" REPORT_OFFLINE=1 "$@" \
    ${limit[@]+"${limit[@]}"} "$BASH" "$report" "$lane" < /dev/null > "$work/out" 2>&1 || got=$?
  [ "$got" -ne 124 ] || fail "the report did not end in a minute"
  [ ! -e "$work/unexpected.calls" ] || fail "the report called a tool that it must not call: $(cat "$work/unexpected.calls")"
}
# Runs it with the stand-ins for Git and GitHub in place of the fixture files.
run_tools() {
  local lane=$1 tools=$2; shift 2
  got=0; rm -f "$work/git.calls" "$work/gh.calls"
  env -u GH_TOKEN PATH="$work/bin:$PATH" STUB_DIR="$work" STUB_TOOLS="$tools" REPORT_SINCE=2026-11-05 \
    REPORT_HISTORY="$work/history" "$@" \
    ${limit[@]+"${limit[@]}"} "$BASH" "$report" "$lane" < /dev/null > "$work/out" 2>&1 || got=$?
  [ "$got" -ne 124 ] || fail "the report did not end in a minute"
}
ranked() { sed -n '/^Code scopes/,$p' "$work/out" | tail -n +2 | awk '{ print $1 ":" $2 }' | tr '\n' ' '; }
# The numbers that the fixture files and the stand-ins must both give.
studio_numbers() {
  expect "pull requests merged" 6
  expect "files for each pull request (median)" 2.5
  expect "pull requests that touch several scopes" 2
  expect "review rounds for each step (mean)" "2 (5 of 6 recorded)"
  expect "override trailers Step-size" 1
  expect "override trailers Lanes" 0
  expect "override trailers Rules" 1
  expect "reuse notes" 2
  expect "Checks runs of main that failed" "1 of 2"
  expect "last Checks run of main" success
  # Tests, binary files, files that are no Rust code and the other lane do not count.
  [ "$(ranked)" = "apps/studio/src/program:50 apps/studio/domain/timing:20 apps/studio/src/timing:10 crates/editorial:7 apps/studio/src/output:4 " ] \
    || fail "$name: scope ranking: $(ranked)"
}

cd "$work"
name="the Studio lane"
printf 'one\ntwo\n' > scan; echo 1 > scan.code
run studio; [ "$got" -eq 0 ] || fail "$name: the report returned $got"
expect "sessions " 8
expect "steps merged" 1
expect "sessions for each merge (all sessions)" 8.0
expect "sessions that only planned" 1
expect "sessions that ended in a wait" 2
expect "  wait: Linux GPU host busy" 2
expect "sessions that failed" 4
expect "starts without a lead (no session)" 3
expect "cards parked" 1
expect "stops of the run" 1
expect "weekly design reviews" 1
studio_numbers
expect "rule findings in apps/studio/src" 2
expect "tests in pure domain crates" 0
if command -v fd > /dev/null; then
  expect "tests of the assembled application" 3
  expect "tests in the execution shell" 0
fi
expect "open cards that wait for Joel" n/a
grep -q "Flow of the run ($work/history)" "$work/out" || fail "$name: the history file is not named"

name="waiting and later both count as waits"
printf '%s\t%s\n' \
  '2026-11-10 09:20' '#2 STEP waiting 10 host busy' \
  '2026-11-10 09:50' '#2 STEP later 120 nightly Checks evidence on main' > "$work/waits.history"
run studio REPORT_HISTORY="$work/waits.history"
expect "sessions that ended in a wait" 2
expect "  wait: host busy" 1
expect "  wait: nightly Checks evidence on main" 1

name="gate requests count as waits and gate history does not add sessions or failures"
printf '%s\t%s\n' \
  '2026-11-10 09:00' 'session on #2' \
  '2026-11-10 09:20' '#2 STEP gate' \
  '2026-11-10 09:30' 'full gate for #2 passed in 10 minutes' \
  '2026-11-10 09:31' 'session on #2' \
  '2026-11-10 09:50' '#2 STEP gate reviewed step' \
  '2026-11-10 10:00' 'full gate for #2 failed in 10 minutes' \
  '2026-11-10 10:01' 'session on #2' \
  '2026-11-10 10:02' '#2 STEP gate 17 files' \
  '2026-11-10 10:10' 'full gate for #2 did not end in 360 minutes' > "$work/gates.history"
run studio REPORT_HISTORY="$work/gates.history"
expect "sessions " 3
expect "sessions that ended in a wait" 3
expect "  wait: full gate" 1
expect "  wait: reviewed step" 1
expect "  wait: 17 files" 1
expect "sessions that failed" 0

name="the rule scan: no finding, and a scan that could not run"
: > scan; echo 0 > scan.code
run studio; expect "rule findings in apps/studio/src" 0
echo 1 > scan.code
run studio; expect "rule findings in apps/studio/src" n/a
echo 2 > scan.code
run studio; expect "rule findings in apps/studio/src" n/a

name="Git and GitHub give the same numbers, and they are asked the right questions"
run_tools studio answer; [ "$got" -eq 0 ] || fail "$name: the report returned $got"
studio_numbers
expect "open cards that wait for Joel" 4
asked() { grep -qF -- "$2" "$work/$1.calls" || fail "$name: $1 was not asked: $2"; }
asked git "log origin/main --since=2026-11-05 00:00 --format=%B -- apps/studio crates/editorial docs/arch/studio"
asked git "log origin/main --since=2026-11-05 00:00 --numstat --no-renames --format= -- apps/studio crates/editorial docs/arch/studio"
asked gh "auth token --user joeldsouzax"
asked gh "pr list -R labcast/labcast-server --state merged --search merged:>=2026-11-05 --limit 200 --json number,files,body"
asked gh "issue list -R labcast/labcast-server --label needs-joel --state open --limit 500"
asked gh "run list -R labcast/labcast-server --workflow checks.yml --branch main --status completed --limit 300"

name="tools that fail give no number"
run_tools studio fail REPORT_FD=false
expect "pull requests" n/a
expect "override trailers and reuse notes" n/a
expect "open cards that wait for Joel" n/a
expect "Checks runs of main" n/a
expect "changed lines" n/a
expect "tests of the assembled application" n/a
expect "tests in the execution shell" n/a
expect "tests in pure domain crates" 0

name="an odd number of pull requests, lists that are cut off, an empty ranking"
jq -c '[.[] | select(.number != 7)]' prs.json > five.json
run studio REPORT_PRS="$work/five.json"
expect "files for each pull request (median)" 2
jq -cn '[range(200) | {number: ., body: "", files: [{path: "apps/studio/src/a.rs"}]}]' > many.json
jq -cn '[range(300) | {conclusion: "success", event: "schedule", createdAt: "2026-11-10T00:00:00Z"}]' > nights.json
run studio REPORT_PRS="$work/many.json" REPORT_RUNS="$work/nights.json"
expect "pull requests merged" "200 (at least)"
expect "Checks runs of main that failed" "0 of 300 (at least)"
# A full list that reaches back before the date is not cut off in the days of the report.
jq -cn '[range(299) | {conclusion: "success", event: "schedule", createdAt: "2026-11-10T00:00:00Z"}] + [{conclusion: "success", event: "schedule", createdAt: "2026-10-01T00:00:00Z"}]' > older.json
run studio REPORT_RUNS="$work/older.json"
expect "Checks runs of main that failed" "0 of 299"
# Only tests and files that are no Rust code changed: the ranking says none.
printf '%s\t%s\t%s\n' 5 0 apps/studio/tests/a.rs 9 0 apps/studio/Cargo.toml > plain.numstat
run studio REPORT_NUMSTAT="$work/plain.numstat"
[ "$(sed -n '/^Code scopes/,$p' "$work/out" | tail -n +2)" = "  none" ] || fail "$name: an empty ranking does not say none"

name="the first day is today minus the days less one, with either date tool"
mkdir "$work/datebin"
printf '#!%s\n' "$BASH" > "$work/datebin/date"
cat >> "$work/datebin/date" << 'STUB'
echo "$*" >> "$STUB_DIR/date.calls"
case "$STUB_DATE $1" in
  "gnu -d") echo 2026-11-06 ;;
  bsd\ -v*) echo 2026-11-07 ;;
  *) exit 1 ;;
esac
STUB
chmod +x "$work/datebin/date"
since_of() { # the date tool, then the arguments after the lane
  local kind=$1; shift
  rm -f "$work/date.calls"
  env PATH="$work/datebin:$work/bin:$PATH" STUB_DIR="$work" STUB_DATE="$kind" REPORT_HISTORY="$work/history" \
    REPORT_PRS="$work/prs.json" REPORT_COMMITS="$work/commits" REPORT_NUMSTAT="$work/numstat" \
    REPORT_RUNS="$work/runs.json" REPORT_OFFLINE=1 "$BASH" "$report" studio "$@" < /dev/null > "$work/out" 2>&1 || true
  head -n 1 "$work/out"
}
[ "$(since_of gnu)" = "Lane studio since 2026-11-06" ] || fail "$name: the default days"
grep -qxF -- "-d -6 days +%Y-%m-%d" "$work/date.calls" || fail "$name: seven days are not today and six days before"
[ "$(since_of gnu 3)" = "Lane studio since 2026-11-06" ] || fail "$name: three days"
grep -qxF -- "-d -2 days +%Y-%m-%d" "$work/date.calls" || fail "$name: three days are not today and two days before"
[ "$(since_of bsd)" = "Lane studio since 2026-11-07" ] || fail "$name: the BSD date tool"
grep -qxF -- "-v-6d +%Y-%m-%d" "$work/date.calls" || fail "$name: the BSD form was not asked"

name="the Server lane counts its own pull request and its own code only"
run server REPORT_HISTORY="$work/none"; [ "$got" -eq 0 ] || fail "$name: the report returned $got"
expect "pull requests merged" 1
expect "review rounds for each step (mean)" "n/a (0 of 1 recorded)"
grep -q 'run history .* n/a' "$work/out" || fail "$name: a missing history is not shown as n/a"
[ "$(ranked)" = "apps/server/migration:9 " ] || fail "$name: scope ranking: $(ranked)"

name="wrong arguments"
run nix; [ "$got" -eq 2 ] || fail "$name: an unknown lane returned $got"
got=0
env PATH="$work/bin:$PATH" STUB_DIR="$work" REPORT_OFFLINE=1 "$BASH" "$report" studio many < /dev/null > "$work/out" 2>&1 || got=$?
[ "$got" -eq 2 ] || fail "$name: days that are no number returned $got"
got=0
env PATH="$work/bin:$PATH" STUB_DIR="$work" REPORT_OFFLINE=1 "$BASH" "$report" studio 0 < /dev/null > "$work/out" 2>&1 || got=$?
[ "$got" -eq 2 ] || fail "$name: zero days returned $got"
[ ! -e "$work/unexpected.calls" ] || fail "$name: the report called a tool: $(cat "$work/unexpected.calls")"

echo "report-test: 10 cases pass"
