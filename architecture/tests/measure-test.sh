#!/usr/bin/env bash
# Run from the repository root. The optional script path permits guard tests.
set -euo pipefail

measure=${1:-$PWD/architecture/measure.sh}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
fail() { echo "measure-test: $1"; cat "$work/out"; exit 1; }
expect() { grep -Fx -- "$1" "$work/out" > /dev/null || fail "missing: $1"; }
reject() { if grep -F -- "$1" "$work/out" > /dev/null; then fail "unexpected: $1"; fi; }

mkdir -p "$work/bin" "$work/lane copies"/{server,studio,nix}/.cache
cat > "$work/bin/date" <<'STUB'
#!/bin/sh
case "$*" in
  '+%s') echo 1762783200 ;;
  '-d @1762783200 +%Y-%m-%d %H:%M') echo '2025-11-10 15:00' ;;
  '-u -d @1762783200 +%Y-%m-%dT%H:%M:%SZ') echo '2025-11-10T14:00:00Z' ;;
  '-d @1762768800 +%Y-%m-%d %H:%M') echo '2025-11-10 11:00' ;;
  '-u -d @1762768800 +%Y-%m-%dT%H:%M:%SZ') echo '2025-11-10T10:00:00Z' ;;
  *) exit 98 ;;
esac
STUB
cat > "$work/bin/gh" <<'STUB'
#!/bin/sh
case "$*" in
  'api graphql --paginate -F query=@'*)
    [ "${BOARD_FAIL:-0}" = 0 ] || exit 1
    cat "$BOARD_PAGES" ;;
  'pr list -R labcast/labcast-server --state merged --search merged:2025-11-10T10:00:00Z..2025-11-10T14:00:00Z --limit 100 --json number,title')
    echo '[{"number":50,"title":"Merged pull request"}]'
    exit "${PR_FAIL:-0}" ;;
  'issue list -R labcast/labcast-server --state closed --search closed:2025-11-10T10:00:00Z..2025-11-10T14:00:00Z --limit 100 --json number,title')
    echo '[{"number":20,"title":"Closed card"}]' ;;
  'run list -R labcast/labcast-server --workflow checks.yml --branch main --limit 3 --json databaseId,event,status,conclusion,createdAt,headSha')
    echo '[{"databaseId":7,"event":"schedule","status":"completed","conclusion":"success","createdAt":"2025-11-10T09:00:00Z","headSha":"abcd1234abcd"}]' ;;
  *) exit 98 ;;
esac
STUB
chmod +x "$work/bin/"{gh,date}

cat > "$work/board.json" <<'FIXTURE'
[
  {"n":1,"task":"T1","title":"Open blocker","kind":"Task","phase":"Phase 1","workstream":"Server","status":"Active","assignee":"fixture-owner","labels":["needs-joel"]},
  {"n":2,"task":"T2","title":"Merged prerequisite","kind":"Task","phase":"Phase 1","workstream":"Studio","status":"Merged"},
  {"n":3,"task":"","kind":"Task","phase":"Phase 1","workstream":"Server","status":"Active","body":"## Dependencies\n- /issues/1\n- /issues/1\n- /issues/2\n\n## Notes\n- /issues/70"},
  {"n":4,"kind":"Task","phase":"Phase 1","workstream":"Studio","status":"Active","body":"## Dependencies\n- /issues/1\n- /issues/9\n- /issues/81\n- /issues/82"},
  {"n":5,"title":"Deferred candidate","kind":"Task","status":"Candidate","assignee":"fixture-owner"},
  {"n":6,"title":"Missing phase","kind":"Task","workstream":"Server","status":"Active","assignee":"fixture-owner"},
  {"n":7,"title":"Another owner","kind":"Task","status":"Active","assignee":"someone-else"},
  {"n":9,"task":"T9","kind":"Task","phase":"Phase 2","workstream":"Studio","status":"Active","labels":["linux-owed"]},
  {"n":10,"task":"T10","kind":"Milestone","phase":"Phase 1","workstream":"Server","status":"Merged"},
  {"n":12,"kind":"Task","phase":"Phase 1","workstream":"DevOps","status":"Merged"},
  {"n":82,"kind":"Task","phase":"Phase 2","workstream":"Studio"}
]
FIXTURE
# Feed a GraphQL page to the real board reader, as a runner does.
jq '{data: {organization: {projectV2: {items: {totalCount: length, nodes: map({
  id: (.n | tostring),
  content: {number: .n, title, body, assignees: {nodes: (if .assignee then [{login: .assignee}] else [] end)},
    labels: {nodes: [(.labels // [])[] | {name: .}]}},
  kind: {name: .kind}, phase: {name: .phase}, workstream: {name: .workstream}, status: {name: .status},
  taskId: {text: .task}
})}}}}}' "$work/board.json" > "$work/pages.json"
export PATH="$work/bin:$PATH" BOARD_PAGES="$work/pages.json"

printf '%s\t%s\n' \
  '2025-11-10 10:59' '#4 STEP merged 40 (the run)' \
  '2025-11-10 11:00' 'session on #4' \
  '2025-11-10 11:01' '#4 STEP merged 40' \
  '2025-11-10 12:00' '#5 STEP gate reviewed' \
  '2025-11-10 12:01' 'waited 8 minutes for the merge turn' \
  '2025-11-10 12:05' 'full gate for #5 passed in 4 minutes' \
  '2025-11-10 12:06' 'main advanced during the gate for #5; the gate runs again' \
  '2025-11-10 12:10' 'full gate for #5 failed in 3 minutes' \
  '2025-11-10 13:00' 'full gate for #5 did not end in 30 minutes' \
  '2025-11-10 13:03' '#5 STEP merged 50 (the run)' \
  '2025-11-10 13:04' '#5 STEP merged 50' \
  '2025-11-10 15:01' 'session on #8' > "$work/lane copies/server/.cache/run.history"
printf '%s\t%s\n' \
  '2025-11-10 12:01' 'session on #6' \
  '2025-11-10 12:02' '#6 STEP later checks GPU proof' \
  '2025-11-10 12:02' 'set aside #6 until the next Checks run: GPU proof' \
  '2025-11-10 12:03' '#7 STEP blocked missing package' \
  '2025-11-10 15:00' 'parked #7: blocked missing package' > "$work/lane copies/studio/.cache/run.history"
echo 'full gate for #8' > "$work/lane copies/server/.cache/run.status"

mkdir -p "$work/elsewhere/lane copies"
(cd "$work" && CDPATH="$work/elsewhere" bash "$measure" 'lane copies' fixture-owner 04) > "$work/out" || fail 'report failed'
expect 'window: 2025-11-10 11:00 to 2025-11-10 15:00 (4 hours)'
diff -u - <(sed -n '/^Phase 1 task cards Merged:/,/^open cards of fixture-owner with missing board fields:$/p' "$work/out") <<'EXPECTED' || fail 'wrong Phase 1 progress'
Phase 1 task cards Merged: 2 of 5
  DevOps: 1 of 1 Merged, Active 0
  Server: 0 of 2 Merged, Active 2
  Studio: 1 of 2 Merged, Active 1
Phase 1 plan cards (with a Task ID) Merged: 1 of 2
  Server: 0 of 1 Merged
  Studio: 1 of 1 Merged
Phase 1 cards made during the work (no Task ID) Merged: 1 of 3
open cards of fixture-owner with missing board fields:
EXPECTED
expect '  #6 kind=Task phase=null workstream=Server status=Active Missing phase'
expect '  #1 Server Open blocker'
diff -u - <(sed -n '/^cards that block the most open Phase 1 tasks:$/,/^$/p' "$work/out") <<'EXPECTED' || fail 'wrong blockers or empty loop section'
cards that block the most open Phase 1 tasks:
  #1 (Active) blocks 2
  #81 (not on the board) blocks 1
  #82 (no status) blocks 1
open cards in a dependency loop:
  none

EXPECTED
reject 'Deferred candidate'; reject 'Another owner'; reject 'blocks 3'; reject '  #2 ('; reject '  #9 ('; reject '  #70 ('
expect 'pull requests merged: 1'
expect 'cards closed: 1'
expect '  7 schedule completed success 2025-11-10T09:00:00Z abcd1234'
expect 'server: session starts 1, merged 1 (by the run 1, without a PR 0), planned 0, gate requests 1, gates 3 = 37 minutes (failed 2), main advanced during a gate 1, merge turn waits 1 = 8 minutes, later/waiting 0, set aside 0, failed/blocked 0, parks 0'
expect '  recorded status: full gate for #8'
expect 'studio: session starts 1, merged 0 (by the run 0, without a PR 0), planned 0, gate requests 0, gates 0 = 0 minutes (failed 0), main advanced during a gate 0, merge turn waits 0 = 0 minutes, later/waiting 1, set aside 1, failed/blocked 1, parks 1'
expect '    2025-11-10 15:00 parked #7: blocked missing package'
expect '  last line: 2025-11-10 15:00 parked #7: blocked missing package'
expect 'nix: no history'
reject 'nix: session starts'
reject '15:01'

# Two rings joined by #15 and a self-loop. The tails, broken rings and
# missing dependency must disappear. These cards are outside Phase 1.
jq -er --arg mode measure --arg lane '' --arg skip '' --arg assignee fixture-owner \
  -f architecture/backlog.jq > "$work/out" <<'FIXTURE' || fail 'loop report failed'
{"items":[
  {"content":{"number":27,"body":"## Dependencies\n/issues/17"}},
  {"content":{"number":9,"body":"## Dependencies\n/issues/11\n/issues/90\n/issues/91"}},
  {"content":{"number":11,"body":"## Dependencies\n/issues/9\n/issues/15"}},
  {"content":{"number":17,"body":"## Dependencies\n/issues/18"}},
  {"content":{"number":18,"body":"## Dependencies\n/issues/27"}},
  {"content":{"number":15,"body":"## Dependencies\n/issues/17"}},
  {"content":{"number":3,"body":"## Dependencies\n/issues/3"}},
  {"content":{"number":6,"body":"## Dependencies\n/issues/7"}},
  {"content":{"number":7,"body":"## Dependencies\n/issues/9"}},
  {"content":{"number":90,"body":"## Dependencies\n/issues/92"}},
  {"content":{"number":91}},
  {"content":{"number":92}},
  {"content":{"number":36,"body":"## Dependencies\n/issues/37"}},
  {"content":{"number":37,"body":"## Dependencies\n/issues/36"},"status":"Merged"},
  {"content":{"number":46,"body":"## Dependencies\n/issues/47"}},
  {"content":{"number":47,"body":"## Dependencies\n/issues/46"},"labels":["linux-owed"]},
  {"content":{"number":50,"body":"## Dependencies\n/issues/999\n\n## Notes\n/issues/50"}}
]}
FIXTURE
diff -u - <(sed -n '/^open cards in a dependency loop:$/,$p' "$work/out") <<'EXPECTED' || fail 'wrong cards in dependency loops'
open cards in a dependency loop:
  #3 #9 #11 #15 #17 #18 #27
EXPECTED

printf '%s\t%s\n' \
  '2025-11-10 13:00' 'session on #8' \
  '2025-11-10 13:02' '#8 STEP later 10 shared builder' \
  '2025-11-10 13:02' 'set aside #8 for 10 minutes: shared builder' \
  '2025-11-10 13:10' 'session on #9' \
  '2025-11-10 13:15' '#9 STEP planned' \
  '2025-11-10 14:01' '#10 STEP merged no-PR-qualification-7' \
  '2025-11-10 14:02' '#11 STEP merged no-PR-qualification-7' > "$work/lane copies/nix/.cache/run.history"
if BOARD_FAIL=1 PR_FAIL=1 bash "$measure" "$work/lane copies" fixture-owner > "$work/out"; then fail 'failed read returned success'; fi
expect 'the board could not be read'
expect 'pull requests merged: unavailable'
expect 'nix: session starts 2, merged 2 (by the run 0, without a PR 2), planned 1, gate requests 0, gates 0 = 0 minutes (failed 0), main advanced during a gate 0, merge turn waits 0 = 0 minutes, later/waiting 1, set aside 1, failed/blocked 0, parks 0'
reject 'Phase 1 task cards Merged'
reject 'pull requests merged: 1'

if bash "$measure" "$work/lane copies" fixture-owner 0 > "$work/out" 2>&1; then fail 'invalid hours returned success'; fi
grep -q '^Usage:' "$work/out" || fail 'invalid hours gave no usage'
echo 'measure-test: history window, missing lane, board, plan progress, dependency loops, GitHub and input errors pass'
