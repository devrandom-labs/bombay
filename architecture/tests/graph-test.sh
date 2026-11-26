#!/usr/bin/env bash
# Run from the repository root. Pass an architecture directory to test a modified copy.
set -euo pipefail

missing='graph: oxigraph is not on PATH (the Nix shell of the repository provides it)'
if ! command -v oxigraph > /dev/null; then
  echo "$missing" >&2
  exit 3
fi
architecture=${1:-architecture}
work=$(mktemp -d)
export HOME=$work/home XDG_STATE_HOME=$work/xdg
mkdir -p "$HOME" "$XDG_STATE_HOME"
children=()
cleanup() {
  touch "$work/release" "$work/read-release" "$work/swap-release"
  local child
  for child in "${children[@]}"; do wait "$child" || true; done
  rm -rf -- "$work"
}
trap cleanup EXIT
work=$(cd -- "$work" && pwd)
tree=$work/architecture
data="$work/graph data"
mkdir -p "$tree" "$work/bin" "$work/temp" "$work/no tools" "$data"
cp -R "$architecture/graph" "$architecture/project.env" "$architecture/backlog.jq" "$tree/"
chmod -R u+w "$tree/"
build=$tree/graph/build.sh q=$tree/graph/q.sh

cat > "$tree/board.sh" <<'STUB'
set -euo pipefail
gh api graphql --paginate -F query=@"$(dirname "$0")/board.graphql" -f login=original -F number=0
STUB
printf "#!%s\n" "$BASH" > "$work/bin/gh"
cat >> "$work/bin/gh" <<'STUB'
set -euo pipefail
printf '%s\n' "$*" >> "$CALLS"
case "$*" in
  "api graphql --paginate -F query=@$TREE/board.graphql -f login=${HARNESS_PROJECT_OWNER:-labcast} -F number=${HARNESS_PROJECT_NUMBER:-2}")
    [ "${GH_TOKEN:-}" = "$TOKEN" ] || exit 98
    if [ "${BLOCK_BOARD:-0}" = 1 ]; then
      : > "$READY"
      for ((i = 0; i < 100; i++)); do
        [ ! -e "$RELEASE" ] || break
        sleep 0.05
      done
      [ -e "$RELEASE" ] || exit 7
    fi
    cat "$BOARD"
    [ "${BOARD_FAIL:-0}" = 0 ] || exit 7 ;;
  "pr list -R ${HARNESS_REPO:-labcast/labcast-server} --state merged --limit ${GRAPH_PULLS:-400} --json number,title,mergedAt,mergeCommit,headRefName,files")
    [ "${GH_TOKEN:-}" = "$TOKEN" ] || exit 98
    [ "${PR_FAIL:-0}" = 0 ] || exit 7
    cat "$PULLS" ;;
  *) exit 98 ;;
esac
STUB
chmod +x "$work/bin/gh"
export PATH="$work/bin:$PATH" TMPDIR="$work/temp" TREE="$tree" CALLS="$work/calls"
export BOARD="$work/board" PULLS="$work/pulls"
export HARNESS_PROJECT_OWNER=fixture HARNESS_PROJECT_NUMBER=9 HARNESS_REPO=fixture/graph-fixture
export TOKEN=fixture-token GH_TOKEN=fixture-token HARNESS_ASSIGNEE=operator
unset GRAPH_PULLS

cat > "$BOARD" <<'INPUT'
{"items":[
  {"content":{"number":1,"title":"Loop A","body":"## Dependencies\n/issues/2"},"workstream":"Server","kind":"Task"},
  {"content":{"number":2,"title":"Loop B","body":"## Dependencies\n/issues/1"},"workstream":"Studio","kind":"Task"},
  {"content":{"number":3,"title":"Chain top","body":"## Dependencies\n/issues/4"},"workstream":"Server","kind":"Task"},
  {"content":{"number":4,"title":"Chain middle","body":"## Dependencies\n/issues/5"},"workstream":"Studio","kind":"Task"},
  {"content":{"number":5,"title":"Chain bottom"},"workstream":"Server","kind":"Task","phase":"Phase 1","sequence":3},
  {"content":{"number":6,"title":"Already merged","body":"## Dependencies\n/issues/5"},"status":"Merged","workstream":"Server","kind":"Task"},
  {"content":{"number":7,"title":"Linux proof owed","body":"## Dependencies\n/issues/5"},"labels":["linux-owed"],"workstream":"Server","kind":"Task"},
  {"content":{"number":8,"title":"A \"quoted\" title and \\","body":"## Dependencies\n/issues/6 /issues/7 /issues/99"},"workstream":"Server","kind":"Task","phase":"Phase 1","sequence":2},
  {"content":{"number":9,"title":"Next phase"},"workstream":"Server","kind":"Task","phase":"Phase 2","sequence":1},
  {"content":{"number":10,"title":"A \"quoted\" title and \\"},"workstream":"Server","kind":"Task","phase":"Phase 1","sequence":2},
  {"content":{"number":101,"title":"Fourth tie"},"workstream":"Server","kind":"Task","phase":"Phase 1","sequence":2},
  {"content":{"number":16,"title":"Third tie"},"workstream":"Server","kind":"Task","phase":"Phase 1","sequence":2},
  {"content":{"number":11,"title":"No sequence"},"workstream":"Server","kind":"Task","phase":"Phase 1"},
  {"content":{"number":12,"title":"Studio task"},"workstream":"Studio","kind":"Task","phase":"Phase 1"},
  {"content":{"number":13,"title":"Needs a decision"},"workstream":"Server","kind":"Task","labels":["needs-joel"],"phase":"Phase 1","sequence":0},
  {"content":{"number":14,"title":"Milestone"},"workstream":"Server","kind":"Milestone","phase":"Phase 1","sequence":0},
  {"content":{"number":15,"title":"Workstream with quotes"},"workstream":"Studio \"New\"","kind":"Task","phase":"Phase 2","sequence":2},
  {"content":{"number":20,"title":"Diamond top","body":"## Dependencies\n/issues/21 /issues/22"}},
  {"content":{"number":21,"title":"Diamond left","body":"## Dependencies\n/issues/23"}},
  {"content":{"number":22,"title":"Diamond right","body":"## Dependencies\n/issues/23"}},
  {"content":{"number":23,"title":"Diamond bottom"}}
]}
INPUT
jq --arg assignee "$HARNESS_ASSIGNEE" '.items |= map(if .phase != null then .assignees = [$assignee] else . end)' "$BOARD" > "$work/assigned"
mv "$work/assigned" "$BOARD"
cat > "$PULLS" <<'INPUT'
[
  {"number":101,"title":"Top change","mergedAt":"2026-11-20T10:00:00Z","headRefName":"feat/3-top",
   "mergeCommit":{"oid":"111111111111aa"},"files":[{"path":"shared/with space.rs"},{"path":"shared/also.rs"},{"path":"other/file.rs"}]},
  {"number":102,"title":"Middle change","mergedAt":"2026-11-20T11:00:00Z","headRefName":"feat/4-middle",
   "mergeCommit":{"oid":"222222222222bb"},"files":[{"path":"shared/with space.rs"}]},
  {"number":103,"title":"Earlier change","mergedAt":"2026-11-19T10:00:00Z","headRefName":"feat/5-bottom",
   "files":[{"path":"shared/with space.rs"},{"path":"shared/also.rs"}]},
  {"number":104,"title":"Change for missing card","mergedAt":"2026-11-20T12:00:00Z","headRefName":"fix/99-spelling","mergeCommit":{"oid":"69bcddc133553f78d45f1d4f745376c6737a70ea"},
   "files":[{"path":"shared/with space.rs"}]},
  {"number":100,"title":"Another change for card 3","mergedAt":"2026-11-18T10:00:00Z","headRefName":"feat/3-other",
   "mergeCommit":{"oid":"444444444444dd"},"files":[{"path":"shared/also.rs"}]}
]
INPUT
# The converters are checked separately. Feed both fact sources into the graph.
printf '%s\n' $'primitive\tString\t5\tshared/also.rs:9 shared/with space.rs:17' $'churn\tfile.rs\t10\tother/file.rs:8' |
  jq -Rr --arg project graph-fixture --arg source design-facts --arg commit head -f "$tree/graph/facts.jq" > "$data/design-facts.nq"
# The 69bcddc13355 row is the real quality facts output for the commit in PR 104's fixture.
printf '%s\n' $'oracle-edit\t111111111111 Top\t13\tremoved=12 added=1' \
  $'oracle-edit\t222222222222 Middle\t4\tremoved=1 added=3' \
  $'oracle-edit\t69bcddc13355 Move Studio session launch order into Execution (#331 step 1\t1\tremoved=1 changed=0 authorized=0 added=1 apps/studio/execution/tests/contract/pipeline.rs' \
  $'oracle-edit\t555555555555 Unlinked\t20\tremoved=18 added=2' \
  $'oracle-edit\t444444444444 Other\t5\tremoved=1 added=4' |
  jq -Rr --arg project graph-fixture --arg source quality-facts --arg commit head -f "$tree/graph/facts.jq" > "$data/quality-facts.nq"

bash "$build" "$data" 2> "$work/first-error" |
  diff -u <(printf 'pulls: read 5 (limit 400)\ngraph: 174 quads from the board, 57 from the pull requests, store %s/store\n' "$data") -
[ ! -s "$work/first-error" ]
diff -u "$PULLS" "$data/pulls.json"
diff -u - "$CALLS" <<EXPECTED
api graphql --paginate -F query=@$tree/board.graphql -f login=fixture -F number=9
pr list -R fixture/graph-fixture --state merged --limit 400 --json number,title,mergedAt,mergeCommit,headRefName,files
EXPECTED

bash "$q" "$data" loops | diff -u <(printf '?card\n#1\n#2\n') -
bash "$q" "$data" releases 5 | diff -u <(printf '?card\t?workstream\t?title\n#3\tServer\tChain top\n#4\tStudio\tChain middle\n') -
bash "$q" "$data" releases 6 | diff -u <(printf '?card\t?workstream\t?title\n') -
bash "$q" "$data" blockers | diff -u <(printf '?card\t?waiting\t?title\n#23\t3\tDiamond bottom\n#5\t2\tChain bottom\n#1\t1\tLoop A\n#2\t1\tLoop B\n#21\t1\tDiamond left\n#22\t1\tDiamond right\n#4\t1\tChain middle\n') -
bash "$q" "$data" blockers 3 | diff -u <(printf '?card\t?waiting\t?title\n#23\t3\tDiamond bottom\n#5\t2\tChain bottom\n#1\t1\tLoop A\n') -
bash "$q" "$data" ready Server | diff -u <(printf '%s\n' $'?card\t?phase\t?sequence\t?title' \
  $'#10\tPhase 1\t2\tA "quoted" title and \\' \
  $'#16\tPhase 1\t2\tThird tie' $'#101\tPhase 1\t2\tFourth tie' \
  $'#5\tPhase 1\t3\tChain bottom' $'#11\tPhase 1\t\tNo sequence' $'#9\tPhase 2\t1\tNext phase') -
bash "$q" "$data" ready Studio | diff -u <(printf '?card\t?phase\t?sequence\t?title\n#12\tPhase 1\t\tStudio task\n') -
bash "$q" "$data" ready 'Studio "New"' | diff -u <(printf '?card\t?phase\t?sequence\t?title\n') -
bash "$q" "$data" touched shared/ 2026-11-20T10:00:00Z | diff -u <(printf '%s\n' $'?change\t?mergedAt\t?title\t?files' \
  $'PR 101\t2026-11-20T10:00:00Z\tTop change\t2' $'PR 102\t2026-11-20T11:00:00Z\tMiddle change\t1' \
  $'PR 104\t2026-11-20T12:00:00Z\tChange for missing card\t1') -
bash "$q" "$data" touched 'shared/with space' 2026-11-20T12:00:00Z |
  diff -u <(printf '?change\t?mergedAt\t?title\t?files\nPR 104\t2026-11-20T12:00:00Z\tChange for missing card\t1\n') -
bash "$q" "$data" touched 'shared/"' 2026-11-20T10:00:00Z | diff -u <(printf '?change\t?mergedAt\t?title\t?files\n') -
bash "$q" "$data" overlap 3 | diff -u <(printf '?other\t?sharedFiles\t?title\n#5\t2\tChain bottom\n#4\t1\tChain middle\n#99\t1\t(not on the board)\n') -
bash "$q" "$data" overlap 6 | diff -u <(printf '?other\t?sharedFiles\t?title\n') -
bash "$q" "$data" oracle | diff -u <(printf '?card\t?changes\t?removed\t?added\t?title\n#3\t2\t13\t5\tChain top\n#4\t1\t1\t3\tChain middle\n#99\t1\t1\t1\t(not on the board)\n') -
bash "$q" "$data" oracle 2 | diff -u <(printf '?card\t?changes\t?removed\t?added\t?title\n#3\t2\t13\t5\tChain top\n#4\t1\t1\t3\tChain middle\n') -
bash "$q" "$data" hot-design | diff -u <(printf '?path\t?cards\t?changes\t?signals\nshared/with space.rs\t4\t4\tprimitive\nshared/also.rs\t2\t3\tprimitive\n') -
bash "$q" "$data" hot-design 0 | diff -u <(printf '?path\t?cards\t?changes\t?signals\n') -

cat > "$work/custom query" <<'QUERY'
PREFIX h: <urn:harness:v0:>
SELECT ?card ?status WHERE { ?card h:status ?status }
QUERY
bash "$q" "$data" sparql "$work/custom query" |
  diff -u <(printf '?card\t?status\n<urn:harness:graph-fixture:card:6>\t"Merged"\n') -

expect_error() {
  local expected=$1 message=$2 status=0
  shift 2
  "$@" > "$work/out" 2> "$work/err" || status=$?
  if [ "$status" != "$expected" ] || [ -s "$work/out" ]; then
    printf 'graph-test: expected exit %s, got %s from %s\n' "$expected" "$status" "$*" >&2
    cat "$work/out" "$work/err" >&2
    return 1
  fi
  if [ -n "$message" ]; then
    diff -u <(printf '%s\n' "$message") "$work/err"
  fi
}
no_staging() {
  local path
  for path in "$data"/.build* "$data"/.query.* "$data"/.publish.*; do
    if [ -e "$path" ]; then
      echo 'graph-test: a failed build left staging files or a lock' >&2
      return 1
    fi
  done
}
await_file() {
  local i
  for ((i = 0; i < 100; i++)); do
    [ ! -e "$1" ] || return 0
    sleep 0.05
  done
  printf 'graph-test: did not reach %s\n' "${1##*/}" >&2
  return 1
}
expire_board() { printf '1 1970-01-01T00:00:01Z\n' > "$data/store/board-read"; }
usage='Usage: q.sh <data folder> {loops|releases|blockers|ready|touched|overlap|oracle|hot-design|sparql} [arguments]'
expect_error 2 'Usage: build.sh <data folder>' bash "$build"
expect_error 2 'Usage: build.sh <data folder>' bash "$build" ''
expect_error 2 'Usage: build.sh <data folder>' bash "$build" "$data" extra
expect_error 2 "$usage" bash "$q" "$data"
expect_error 2 "$usage" bash "$q" '' loops
expect_error 2 "$usage" bash "$q" "$data" unknown
expect_error 2 "$usage" bash "$q" "$data" loops extra
expect_error 2 "$usage" bash "$q" "$data" releases
expect_error 2 "$usage" bash "$q" "$data" releases 0
expect_error 2 "$usage" bash "$q" "$data" releases 1 extra
expect_error 2 "$usage" bash "$q" "$data" overlap a
expect_error 2 "$usage" bash "$q" "$data" blockers -1
expect_error 2 "$usage" bash "$q" "$data" oracle 1 extra
expect_error 2 "$usage" bash "$q" "$data" hot-design ''
expect_error 2 "$usage" bash "$q" "$data" ready ''
expect_error 2 "$usage" bash "$q" "$data" ready Server extra
expect_error 2 "$usage" bash "$q" "$data" touched shared/
expect_error 2 "$usage" bash "$q" "$data" touched shared/ 'bad date'
expect_error 2 "$usage" bash "$q" "$data" touched shared/ 2026-11-20T10:00:00Z extra
expect_error 2 "$usage" bash "$q" "$data" sparql "$work/absent"
expect_error 2 "$usage" bash "$q" "$data" sparql "$work"
expect_error 2 "$usage" bash "$q" "$data" sparql "$work/custom query" extra
touch "$work/unreadable"
chmod a-r "$work/unreadable"
if [ ! -r "$work/unreadable" ]; then
  expect_error 2 "$usage" bash "$q" "$data" sparql "$work/unreadable"
fi
expect_error 1 'q: no store; run graph/build.sh' bash "$q" "$work/absent" loops
expect_error 3 "$missing" env PATH="$work/no tools" "$BASH" "$build" "$work/no graph"
expect_error 3 "$missing" env PATH="$work/no tools" "$BASH" "$q" "$data" loops
[ ! -e "$work/no graph" ]
touch "$data/.build.lock"
expect_error 4 'graph: another build holds the lock' bash "$build" "$data"
rm -- "$data/.build.lock"

cat > "$work/date-env" <<'STUB'
date() { printf '%s\n' "$TEST_NOW"; }
STUB
read -r board_at board_time < "$data/store/board-read"
cp "$CALLS" "$work/first-calls"
env TEST_NOW=$((board_at + 599)) BASH_ENV="$work/date-env" bash "$build" "$data" |
  diff -u <(printf 'graph: board read at %s is less than 10 minutes old; using %s/store\n' "$board_time" "$data") -
cmp "$work/first-calls" "$CALLS"
expect_error 1 'graph: the board could not be read' env TEST_NOW=$((board_at + 600)) BASH_ENV="$work/date-env" BOARD_FAIL=1 bash "$build" "$data"
expect_error 1 'graph: the board could not be read' env TEST_NOW=$((board_at - 1)) BASH_ENV="$work/date-env" BOARD_FAIL=1 bash "$build" "$data"
printf 'bad timestamp\n' > "$data/store/board-read"
expect_error 1 'graph: the board could not be read' env BOARD_FAIL=1 bash "$build" "$data"
expire_board
echo '{"items":[]}' > "$work/empty-board"
expect_error 1 'graph: the board could not be read' env BOARD="$work/empty-board" bash "$build" "$data"
echo '{"items":{"not-an-array":{"content":{"number":16}}}}' > "$work/invalid-board"
expect_error 1 'graph: the board could not be read' env BOARD="$work/invalid-board" bash "$build" "$data"
expect_error 7 '' env PR_FAIL=1 bash "$build" "$data"
bash "$q" "$data" loops > "$work/old-query"
cp "$data/board.nq" "$work/old-board.nq"
printf '{"items":[{"content":{"number":16,"title":"New board"}}]}\n' > "$work/new-board"
printf '<bad nq\n' > "$data/design-facts.nq"
expect_error 1 '' env BOARD="$work/new-board" bash "$build" "$data"
[ -s "$work/err" ]
bash "$q" "$data" loops | diff -u "$work/old-query" -
cmp "$data/board.nq" "$work/old-board.nq"
no_staging
rm -- "$data/design-facts.nq" "$data/quality-facts.nq"

export READY="$work/ready" RELEASE="$work/release" WAITING="$work/waiting"
cat > "$work/wait-env" <<'STUB'
sleep() { : > "$WAITING"; command sleep "$@"; }
STUB
cp "$CALLS" "$work/before-lock"
BLOCK_BOARD=1 bash "$build" "$data" > "$work/locked-build" &
builder=$!
children+=("$builder")
await_file "$READY"
staged=("$data"/.build.??????)
[ "${#staged[@]}" -eq 1 ]
[ -d "${staged[0]}" ]
BASH_ENV="$work/wait-env" bash "$build" "$data" > "$work/wait-output" 2> "$work/wait-error" &
waiter=$!
children+=("$waiter")
await_file "$WAITING"
kill -0 "$waiter"
export TIMEOUT_CLOCK="$work/timeout-clock"
cat > "$work/timeout-env" <<'STUB'
unset SECONDS
SECONDS=0
sleep() {
  if [ "$SECONDS" = 0 ]; then
    SECONDS=299
  elif [ "$SECONDS" = 299 ]; then
    : > "$TIMEOUT_CLOCK"
    SECONDS=300
  else
    return 98
  fi
}
STUB
BASH_ENV="$work/timeout-env" bash "$build" "$data" > "$work/timeout-output" 2> "$work/timeout-error"
diff -u <(printf 'graph: using board read at 1970-01-01T00:00:01Z from %s/store\n' "$data") "$work/timeout-output"
diff -u <(printf 'graph: another build holds the lock\n') "$work/timeout-error"
[ -f "$TIMEOUT_CLOCK" ]
unbuilt="$work/unbuilt graph"
mkdir -p "$unbuilt/.build.lock/$$"
BASH_ENV="$work/timeout-env" bash "$build" "$unbuilt" > "$work/unbuilt-output" 2> "$work/unbuilt-error"
diff -u <(printf 'graph: no board read recorded in %s/store\n' "$unbuilt") "$work/unbuilt-output"
cmp "$work/timeout-error" "$work/unbuilt-error"
mkdir -- "$unbuilt/store"
printf '1\n' > "$unbuilt/store/board-read"
BASH_ENV="$work/timeout-env" bash "$build" "$unbuilt" > "$work/invalid-read-output" 2> "$work/unbuilt-error"
cmp "$work/unbuilt-output" "$work/invalid-read-output"
cmp "$work/timeout-error" "$work/unbuilt-error"
bash "$q" "$data" loops | diff -u "$work/old-query" -
: > "$RELEASE"
wait "$builder"
wait "$waiter"
children=()
read -r board_at board_time < "$data/store/board-read"
diff -u <(printf 'graph: using board read at %s from %s/store\n' "$board_time" "$data") "$work/wait-output"
[ ! -s "$work/wait-error" ]
[ "$(wc -l < "$CALLS")" -eq "$(( $(wc -l < "$work/before-lock") + 2 ))" ]
expire_board

# Kill the builder after the waiter has seen its live lock. The waiter must
# clear it after the first sleep and report the previous store.
rm -- "$READY" "$RELEASE" "$WAITING"
cat > "$work/stale-wait-env" <<'STUB'
sleep() {
  if [ -e "$WAITING" ]; then
    echo 'graph-test: waiter kept sleeping after the builder died' >&2
    return 98
  fi
  : > "$WAITING"
  while [ ! -e "$RELEASE" ]; do command sleep 0.05; done
}
STUB
BLOCK_BOARD=1 bash "$build" "$data" > "$work/killed-build" &
builder=$!
children+=("$builder")
await_file "$READY"
BASH_ENV="$work/stale-wait-env" bash "$build" "$data" > "$work/stale-wait-output" &
waiter=$!
children+=("$waiter")
await_file "$WAITING"
kill -KILL "$builder"
status=0
wait "$builder" 2>/dev/null || status=$?
[ "$status" = 137 ]
: > "$RELEASE"
wait "$waiter"
children=()
diff -u <(printf 'graph: using board read at 1970-01-01T00:00:01Z from %s/store\n' "$data") "$work/stale-wait-output"
no_staging

# A query started during the first build waits for the initial store.
rm -- "$READY" "$RELEASE" "$WAITING"
first="$work/first graph"
BLOCK_BOARD=1 bash "$build" "$first" > "$work/first-build" &
builder=$!
children+=("$builder")
await_file "$READY"
cat > "$work/initial-wait-env" <<'STUB'
unset SECONDS
SECONDS=0
sleep() {
  if [ "$SECONDS" = 300 ]; then : > "$WAITING"; fi
  SECONDS=300
  command sleep "$@"
}
STUB
BASH_ENV="$work/initial-wait-env" bash "$q" "$first" loops > "$work/first-query" &
reader=$!
children+=("$reader")
await_file "$WAITING"
: > "$RELEASE"
wait "$builder"
wait "$reader"
children=()
diff -u "$work/old-query" "$work/first-query"

true &
dead=$!
wait "$dead"
if kill -0 "$dead" 2>/dev/null; then exit 1; fi
abandoned="$work/abandoned graph"
mkdir -p "$abandoned/.build.lock/$dead"
expect_error 1 'q: no store; run graph/build.sh' env BASH_ENV="$work/timeout-env" bash "$q" "$abandoned" loops
mkdir -p "$data/.build.lock/$dead"
: > "$data/.publish.$dead"
printf 'sleep() { return 98; }\n' > "$work/no-wait-env"
BASH_ENV="$work/no-wait-env" bash "$q" "$data" loops | diff -u "$work/old-query" -
stale="$data/.build.stale1"
mkdir "$stale"
printf '%s\n' "$dead" > "$stale/pid"
mv -- "$data/store" "$stale/previous"
expect_error 1 'graph: the board could not be read' env BOARD_FAIL=1 bash "$build" "$data"
bash "$q" "$data" loops | diff -u "$work/old-query" -
no_staging
mkdir "$data/.query.$dead"
GRAPH_PULLS=5 bash "$build" "$data" | diff -u - 3<<EXPECTED /dev/fd/3
pulls: read 5 (limit 5)
pulls: the limit of 5 was reached; older changes are not in the graph
graph: 174 quads from the board, 57 from the pull requests, store $data/store
EXPECTED
no_staging

cat > "$work/publish-timeout-env" <<'STUB'
unset SECONDS
SECONDS=0
sleep() {
  case "$SECONDS" in
    0) SECONDS=29 ;;
    29) : > "$TIMEOUT_CLOCK"; SECONDS=30 ;;
    *) echo 'graph-test: the publication wait exceeded 30 seconds' >&2; return 98 ;;
  esac
}
STUB
expire_board
mkdir "$data/.query.$$"
TIMEOUT_CLOCK="$work/reader-clock" BASH_ENV="$work/publish-timeout-env" \
  bash "$build" "$data" > "$work/stalled-reader-build"
[ -f "$work/reader-clock" ]
[ -d "$data/.query.$$" ]
read -r board_at board_time < "$data/store/board-read"
[ "$board_at" -gt 1 ]
diff -u <(printf '%s\n' 'graph: published after waiting 30 seconds for readers' \
  'pulls: read 5 (limit 400)' "graph: 174 quads from the board, 57 from the pull requests, store $data/store") \
  "$work/stalled-reader-build"
rmdir -- "$data/.query.$$"

: > "$data/.publish.$$"
expect_error 4 'graph: a build is publishing; try again' env TIMEOUT_CLOCK="$work/publish-clock" \
  BASH_ENV="$work/publish-timeout-env" bash "$q" "$data" loops
[ -f "$work/publish-clock" ]
rm -- "$data/.publish.$$"
no_staging

if [ "$(uname -s)" = Darwin ]; then
  expire_board
  (
    export PATH="/bin:$PATH"
    [ "$(command -v mv)" = /bin/mv ]
    BOARD="$work/new-board" "$BASH" "$build" "$data"
  ) | diff -u <(printf 'pulls: read 5 (limit 400)\ngraph: 4 quads from the board, 57 from the pull requests, store %s/store\n' "$data") -
  bash "$q" "$data" loops | diff -u <(printf '?card\n') -
  no_staging
  expire_board
  bash "$build" "$data" > "$work/restore-fixture"
  bash "$q" "$data" loops | diff -u "$work/old-query" -
fi

# Fail the rename into place. A later rename runs while readers arrive
# on both sides of the publication fence.
expire_board
export REAL_MV REAL_OXIGRAPH
REAL_MV=$(command -v mv)
REAL_OXIGRAPH=$(command -v oxigraph)
export SWAP_READY="$work/swap-ready" SWAP_RELEASE="$work/swap-release"
export READ_READY="$work/read-ready" READ_RELEASE="$work/read-release"
printf "#!%s\n" "$BASH" > "$work/bin/mv"
cat >> "$work/bin/mv" <<'STUB'
set -euo pipefail
case "$2" in
  */.build.??????/store)
    [ "$1" = -- ] && [ -f "$2/CURRENT" ] && [ -f "${2%/store}/previous/CURRENT" ] && [ ! -e "$3" ] || exit 98
    if [ "${FAIL_SWAP:-0}" = 1 ]; then echo 'fixture store rename failed' >&2; exit 9; fi
    : > "$SWAP_READY"
    while [ ! -e "$SWAP_RELEASE" ]; do sleep 0.05; done
    "$REAL_MV" "$@"
    [ ! -e "$2" ] && [ -f "$3/CURRENT" ] && [ -s "$3/board-read" ]
    exit ;;
esac
exec "$REAL_MV" "$@"
STUB
printf "#!%s\n" "$BASH" > "$work/bin/oxigraph"
cat >> "$work/bin/oxigraph" <<'STUB'
if [ "$1" = query ] && [ "${BLOCK_QUERY:-0}" = 1 ]; then
  : > "$READ_READY"
  while [ ! -e "$READ_RELEASE" ]; do sleep 0.05; done
fi
exec "$REAL_OXIGRAPH" "$@"
STUB
chmod +x "$work/bin/mv" "$work/bin/oxigraph"
expect_error 9 'fixture store rename failed' env FAIL_SWAP=1 BOARD="$work/new-board" bash "$build" "$data"
bash "$q" "$data" loops | diff -u "$work/old-query" -
no_staging

BLOCK_QUERY=1 bash "$q" "$data" loops > "$work/active-query" &
reader=$!
children+=("$reader")
await_file "$READ_READY"
WAITING="$work/publisher-wait" BASH_ENV="$work/wait-env" BOARD="$work/new-board" bash "$build" "$data" > "$work/atomic-build" &
builder=$!
children+=("$builder")
await_file "$work/publisher-wait"
[ ! -e "$SWAP_READY" ]
: > "$READ_RELEASE"
wait "$reader"
diff -u "$work/old-query" "$work/active-query"
await_file "$SWAP_READY"
expect_error 4 'graph: a build is publishing; try again' env TIMEOUT_CLOCK="$work/publish-gap-clock" \
  BASH_ENV="$work/publish-timeout-env" bash "$q" "$data" loops
[ -f "$work/publish-gap-clock" ]
WAITING="$work/query-wait" BASH_ENV="$work/wait-env" bash "$q" "$data" loops > "$work/wait-query" &
reader=$!
children+=("$reader")
await_file "$work/query-wait"
[ ! -s "$work/wait-query" ]
: > "$SWAP_RELEASE"
wait "$builder"
wait "$reader"
children=()
diff -u <(printf '?card\n') "$work/wait-query"
no_staging
rm -- "$work/bin/mv" "$work/bin/oxigraph"

# Rebuild in place after deleting the old sources. The old quads must be gone.
expire_board
unset HARNESS_PROJECT_OWNER HARNESS_PROJECT_NUMBER HARNESS_REPO GH_TOKEN
export TOKEN=''
cp "$work/new-board" "$BOARD"
printf '[]\n' > "$PULLS"
(cd -- "$work" && bash "$build" 'graph data') |
  diff -u <(printf 'pulls: read 0 (limit 400)\ngraph: 4 quads from the board, 0 from the pull requests, store %s/store\n' "$data") -
bash "$q" "$data" loops | diff -u <(printf '?card\n') -
bash "$q" "$data" touched '' 2026-11-20T10:00:00Z | diff -u <(printf '?change\t?mergedAt\t?title\t?files\n') -
diff -u "$PULLS" "$data/pulls.json"
[ -z "$(ls -A "$TMPDIR")" ]
echo 'graph-test: builds, all named queries, rebuild and errors pass'
