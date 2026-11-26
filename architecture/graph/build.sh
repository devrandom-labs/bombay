#!/usr/bin/env bash
# Usage: build.sh <data folder>
# Read the project board, merged pull requests and local facts. GH_TOKEN and the
# overrides in project.env are optional. Write board.nq, pulls.json, changes.nq
# and a new store in the data folder. Remove those four outputs to undo the build.
set -euo pipefail
export CDPATH=
umask 077

if [ "$#" != 1 ] || [ -z "$1" ]; then
  echo 'Usage: build.sh <data folder>' >&2
  exit 2
fi
if ! command -v oxigraph > /dev/null; then
  echo 'graph: oxigraph is not on PATH (the Nix shell of the repository provides it)' >&2
  exit 3
fi
here=$(cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=architecture/project.env
source "$here/../project.env"
project=${HARNESS_REPO##*/}
limit=${GRAPH_PULLS:-400}
if ! [[ $limit =~ ^[1-9][0-9]*$ ]]; then
  echo 'graph: GRAPH_PULLS must be a positive integer' >&2
  exit 2
fi
data=$1
mkdir -p -- "$data"
data=$(cd -- "$data" && pwd)
discard_stage() {
  local stage=$1
  if [ -d "$stage/previous" ] && [ ! -e "$data/store" ]; then
    mv -- "$stage/previous" "$data/store" || {
      printf 'graph: previous store is still at %s/previous\n' "$stage" >&2
      return 1
    }
  fi
  rm -rf -- "$stage"
}

discard_stale_stages() {
  local stage pid
  for stage in "$data"/.build.??????; do
    if [ -f "$stage/pid" ] && read -r pid < "$stage/pid" && [ "$pid" = "$1" ]; then
      discard_stage "$stage" || return 1
    fi
  done
}

lock=$data/.build.lock
clear_stale_lock() {
  local owner pid
  # Removing the dead owner's entry claims the stale lock for one builder.
  # While the directory is empty, another builder treats it as held.
  for owner in "$lock"/*; do
    pid=${owner##*/}
    if [[ $pid =~ ^[1-9][0-9]*$ ]] && ! kill -0 "$pid" 2>/dev/null &&
        rmdir -- "$owner" 2>/dev/null; then
      rm -f -- "$data/.publish.$pid" || exit 1
      if ! discard_stale_stages "$pid"; then
        mkdir -- "$owner"
        exit 1
      fi
      rmdir -- "$lock" || exit 1
      return 0
    fi
  done
  return 1
}

read_board_time() {
  [ -f "$data/store/board-read" ] &&
    read -r board_at board_time 2>/dev/null < "$data/store/board-read" &&
    [[ $board_at =~ ^[1-9][0-9]{0,10}$ ]] && [ -n "$board_time" ]
}

if ! mkdir -- "$lock" 2>/dev/null; then
  held=false
  if [ -d "$lock" ] && ! clear_stale_lock; then held=true; fi
  if $held || ! mkdir -- "$lock" 2>/dev/null; then
    if ! $held && [ ! -d "$lock" ]; then
      echo 'graph: another build holds the lock' >&2
      exit 4
    fi
    deadline=$((SECONDS + 300))
    while [ -d "$lock" ] && (( SECONDS < deadline )); do
      if clear_stale_lock; then break; fi
      sleep 1
    done
    if [ -d "$lock" ]; then echo 'graph: another build holds the lock' >&2; fi
    if read_board_time; then
      printf 'graph: using board read at %s from %s/store\n' "$board_time" "$data"
    else
      printf 'graph: no board read recorded in %s/store\n' "$data"
    fi
    exit 0
  fi
fi
work=
cleanup() {
  if [ -n "$work" ]; then discard_stage "$work" || return 1; fi
  rm -f -- "$data/.publish.$$"
  rmdir -- "$lock/$$" "$lock"
}
mkdir -- "$lock/$$"
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
if read_board_time; then
  now=$(date +%s)
  if (( now >= board_at && now - board_at < 600 )); then
    printf 'graph: board read at %s is less than 10 minutes old; using %s\n' "$board_time" "$data/store"
    exit 0
  fi
fi
work=$(mktemp -d "$data/.build.XXXXXX")
printf '%s\n' "$$" > "$work/pid"

read_board() (
  # board.sh fixes these two arguments. Supply the configured project to its
  # GraphQL call and keep its pagination and validation.
  export HARNESS_PROJECT_OWNER HARNESS_PROJECT_NUMBER
  # shellcheck disable=SC2329
  gh() {
    local arg
    local args=()
    for arg; do
      case "$arg" in
        login=*) arg="login=$HARNESS_PROJECT_OWNER" ;;
        number=*) arg="number=$HARNESS_PROJECT_NUMBER" ;;
      esac
      args+=("$arg")
    done
    command gh "${args[@]}"
  }
  export -f gh
  bash "$(dirname -- "$here")/board.sh"
)
if ! read_board > "$work/board.json" ||
    ! jq -e '.items | arrays | length > 0' "$work/board.json" > /dev/null; then
  echo 'graph: the board could not be read' >&2
  exit 1
fi
board_read=$(date -u '+%s %Y-%m-%dT%H:%M:%SZ')
gh pr list -R "$HARNESS_REPO" --state merged --limit "$limit" \
  --json number,title,mergedAt,mergeCommit,headRefName,files > "$work/pulls.json"
pulls=$(jq -er 'arrays | length' "$work/pulls.json")
jq -r --arg project "$project" -f "$here/board.jq" "$work/board.json" > "$work/board.nq"
jq -r --arg project "$project" -f "$here/changes.jq" "$work/pulls.json" > "$work/changes.nq"

files=("$work/board.nq" "$work/changes.nq")
for facts in "$data/design-facts.nq" "$data/quality-facts.nq"; do
  if [ -s "$facts" ]; then
    files+=("$facts")
  fi
done
if ! oxigraph load --location "$work/store" --file "${files[@]}" 2> "$work/load.log"; then
  cat "$work/load.log" >&2
  exit 1
fi
queries_active() {
  local reader pid
  for reader in "$data"/.query.*; do
    [ -d "$reader" ] || continue
    pid=${reader##*.}
    if kill -0 "$pid" 2>/dev/null; then return 0; fi
    rmdir -- "$reader" 2>/dev/null || true
  done
  return 1
}
printf '%s\n' "$board_read" > "$work/store/board-read"
# Block new readers while the old store is moved aside and the new store is
# renamed into place. Cleanup restores the old store if publication fails.
: > "$data/.publish.$$"
deadline=$((SECONDS + 30))
readers_timed_out=false
while queries_active; do
  if (( SECONDS >= deadline )); then
    readers_timed_out=true
    break
  fi
  sleep 0.1
done
if [ -e "$data/store" ]; then
  mv -- "$data/store" "$work/previous"
fi
mv -- "$work/store" "$data/store"
rm -- "$data/.publish.$$"
if $readers_timed_out; then
  echo 'graph: published after waiting 30 seconds for readers'
fi
mv -f -- "$work/board.nq" "$work/pulls.json" "$work/changes.nq" "$data/"
printf 'pulls: read %s (limit %s)\n' "$pulls" "$limit"
if [ "$pulls" -eq "$limit" ]; then
  printf 'pulls: the limit of %s was reached; older changes are not in the graph\n' "$limit"
fi
printf 'graph: %s quads from the board, %s from the pull requests, store %s\n' \
  "$(wc -l < "$data/board.nq" | tr -d ' ')" "$(wc -l < "$data/changes.nq" | tr -d ' ')" "$data/store"
