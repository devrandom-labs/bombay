#!/usr/bin/env bash
# Usage: tools.sh check|list [<state folder> <worktree parent>]
# List the registry or run each check in order. The worktree parent holds the
# lane worktrees. Checks write snapshots and a derived store in the state
# folder. See facts-snapshot.sh and graph/build.sh for how to undo those writes.
# Each check has OWNER_TOOL_TIMEOUT seconds to finish (default 300).
set -uo pipefail
export CDPATH=

case "${1:-}:$#" in
  check:1|list:1) ;;
  check:3|list:3) [ -n "$2" ] && [ -d "$3" ] || set -- ;;
  *) set -- ;;
esac
if [ "$#" = 0 ]; then
  echo 'Usage: tools.sh check|list [<state folder> <worktree parent>]' >&2
  exit 2
fi
architecture=$(cd -- "$(dirname -- "$0")/.." && pwd) || exit 1
# shellcheck source=architecture/project.env
source "$architecture/project.env"
if [ "$#" = 1 ]; then
  # shellcheck source=architecture/owner/where.sh
  source "$architecture/owner/where.sh" || exit 1
  state=$harness_state worktrees=$harness_worktrees
else
  # shellcheck disable=SC2034 # The registry reads worktrees when a check runs.
  state=$2 worktrees=$3
fi

registry() {
  local workflow workflows
  cat <<'REGISTRY'
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
REGISTRY
  read -ra workflows <<< "$HARNESS_OWNER_WORKFLOWS"
  for workflow in "${workflows[@]}"; do
    printf 'workflow-%s|on-demand|Start an owner workflow by hand|gh workflow run %q -R %q --ref %q\n' \
      "$workflow" "$workflow" "$HARNESS_REPO" "$HARNESS_MAIN"
  done
}

tool() {
  local path=$1
  shift
  if [ -f "$architecture/$path" ]; then
    bash "$architecture/$path" "$@"
  else
    : > "$work/absent"
    printf 'not in this tree (architecture/%s)\n' "$path" >&2
    return 1
  fi
}

if [ "$1" = list ]; then
  registry
  exit
fi
if [ "$#" = 1 ]; then (umask 077; mkdir -p -- "$state") || exit 1; fi
limit=${OWNER_TOOL_TIMEOUT:-300}
if ! [[ $limit =~ ^[1-9][0-9]*$ ]]; then
  echo 'tools: OWNER_TOOL_TIMEOUT must be a positive integer' >&2
  exit 2
fi
work=$(mktemp -d) || exit 1
trap 'rm -rf -- "$work"' EXIT

run_check() {
  local job watch result=0
  rm -f -- "$work/absent" "$work/timeout" || return 1
  # Bash job control puts each job and its children in a separate process group.
  set -m
  (eval "$command") </dev/null > "$work/out" 2>&1 &
  job=$!
  (
    sleep "$limit"
    : > "$work/timeout"
    kill -KILL -- "-$job" 2>/dev/null
  ) &
  watch=$!
  set +m
  wait "$job" 2>/dev/null || result=$?
  kill -KILL -- "-$watch" 2>/dev/null || true
  wait "$watch" 2>/dev/null || true
  return "$result"
}

passed=0 failed_count=0 absent_count=0
failed='' absent=''
while IFS='|' read -r id when purpose command; do
  [ "$when" = check ] || continue
  printf '== tool: %s — %s ==\n' "$id" "$purpose"
  result=0
  run_check || result=$?
  if [ -f "$work/timeout" ]; then
    echo '  timeout'
    tail -n 5 "$work/out" | sed 's/^/  /'
    failed="$failed $id"
    failed_count=$((failed_count + 1))
  elif [ -f "$work/absent" ]; then
    sed -n '1,40s/^/  /p' "$work/out"
    absent="$absent $id"
    absent_count=$((absent_count + 1))
  elif [ "$result" = 0 ]; then
    if [ -s "$work/out" ]; then sed -n '1,40s/^/  /p' "$work/out"; else echo '  none'; fi
    passed=$((passed + 1))
  else
    tail -n 5 "$work/out" | sed 's/^/  /'
    failed="$failed $id"
    failed_count=$((failed_count + 1))
  fi
done < <(registry)
total=$(registry | awk -F '|' '$2 == "check" { n++ } END { print n+0 }')
printf '== tools run: %s of %s; passed: %s; failed: %s%s; absent: %s%s ==\n' \
  "$((passed + failed_count))" "$total" "$passed" "$failed_count" "${failed:+ (${failed# })}" \
  "$absent_count" "${absent:+ (${absent# })}"
[ "$passed" -eq "$total" ] && [ "$failed_count" -eq 0 ] && [ "$absent_count" -eq 0 ]
