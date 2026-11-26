#!/usr/bin/env bash
# Usage: harness.sh up|status|doctor|--where
# Check the machine, then start the lanes, both recorders and the owner loop.
# Up makes the missing lane worktrees and starts their supervisors.
# Use the Nix shell for the recorders' tools.
# The individual commands own the logs and PID files. Use their stop commands
# to end an individual run. Status also reports who holds the owner lock.
set -euo pipefail
unset CDPATH
architecture=$(cd -- "$(dirname -- "$0")" && pwd -P)
# shellcheck source=architecture/start-common.sh
. "$architecture/start-common.sh"

role_line() {
  case "$harness_role" in
    owner) printf 'ok       role: owner (the main checkout %s)\n' "$harness_main" ;;
    lane\ *) printf 'note     role: not the owner here (this is the worktree of the lane %s)\n' "${harness_role#lane }" ;;
    *) printf 'note     role: not the owner here (this checkout is not the main checkout %s)\n' "$harness_main" ;;
  esac
}

missing_part() {
  printf 'MISSING  %s: %s -> %s\n' "$1" "$2" "$3"
  missing=$((missing + 1))
}

check_tool() {
  local name=$1 path hint version
  case "$name" in
    codex|claude|agy) hint='install the program and sign in' ;;
    *) hint='run this from the Nix shell of the repository (nix develop, or direnv allow)' ;;
  esac
  if ! path=$(command -v "$name"); then
    missing_part "tools $name" 'not on PATH' "$hint"
  elif [ "$name" = jq ]; then
    version=$(jq --version 2>/dev/null) || version=
    if [[ $version =~ ^jq-([0-9]+)\.([0-9]+) ]] &&
        (( 10#${BASH_REMATCH[1]} > 1 || (10#${BASH_REMATCH[1]} == 1 && 10#${BASH_REMATCH[2]} >= 8) )); then
      printf 'ok       tools jq: %s (%s)\n' "$version" "$path"
    else
      missing_part 'tools jq' "${version:-version unavailable}; need jq 1.8 or newer" "$hint"
    fi
  else
    printf 'ok       tools %s: %s\n' "$name" "$path"
  fi
}

check_github() {
  local repo title query
  if repo=$(gh api "repos/$HARNESS_REPO" -q .full_name 2>/dev/null) &&
      [ -n "$repo" ] && [ "$repo" != null ]; then
    printf 'ok       github: %s\n' "$repo"
  else
    missing_part github "cannot read $HARNESS_REPO" 'gh auth login, or set GH_TOKEN for an account with access'
  fi
  # shellcheck disable=SC2016 # GraphQL variables are bound by gh.
  query='query($login: String!, $number: Int!) {
    organization(login: $login) { projectV2(number: $number) { title } }
  }'
  if title=$(gh api graphql -f "query=$query" -f "login=$HARNESS_PROJECT_OWNER" \
      -F "number=$HARNESS_PROJECT_NUMBER" -q '.data.organization.projectV2.title' 2>/dev/null) &&
      [ -n "$title" ] && [ "$title" != null ]; then
    printf 'ok       board: %s/%s (%s)\n' "$HARNESS_PROJECT_OWNER" "$HARNESS_PROJECT_NUMBER" "$title"
  else
    missing_part board "cannot read $HARNESS_PROJECT_OWNER/$HARNESS_PROJECT_NUMBER" 'gh auth refresh -s project'
  fi
}

check_lead() {
  local first command=${STEP_LEAD-$HARNESS_LEAD} source=architecture/project.env
  if [ "${STEP_LEAD+x}" = x ]; then source=STEP_LEAD; fi
  read -r first _ <<< "$command"
  if [ -n "$first" ] && command -v -- "$first" >/dev/null 2>&1; then
    printf 'ok       lead: %s (from %s)\n' "$command" "$source"
  else
    missing_part lead "${first:-no command} not on PATH (from $source)" 'install the program and sign in'
  fi
}

check_state() {
  local free minimum=${HARNESS_MIN_FREE_GIB:-15}
  if ! [[ $minimum =~ ^(0|[1-9][0-9]{0,8})$ ]]; then
    missing_part state 'HARNESS_MIN_FREE_GIB must be a nonnegative integer' 'export HARNESS_MIN_FREE_GIB=15'
  elif ! create_state 2>/dev/null; then
    missing_part state "cannot create $harness_state" 'set HARNESS_STATE to a writable folder outside the checkout'
  elif ! free=$(free_gib "$harness_state" 2>/dev/null) || [ -z "$free" ]; then
    missing_part state "cannot read free disk space at $harness_state" 'check the filesystem or set HARNESS_STATE to a writable folder outside the checkout'
  elif [ "$free" -lt "$minimum" ]; then
    missing_part state "$harness_state ($free GiB free)" "make space; a session needs $minimum GiB"
  else
    printf 'ok       state: %s (%s GiB free)\n' "$harness_state" "$free"
  fi
}

check_owner_files() {
  local file
  for file in "$@"; do
    if [ ! -f "$harness_here/$file" ]; then
      missing_part agent 'harness-owner or owner settings missing' "git restore -- $*"
      return
    fi
  done
  echo 'ok       agent: harness-owner and owner settings exist'
}

doctor() {
  local missing=0 name stopped lanes
  role_line
  [ "$harness_role" = owner ] || return 0
  for name in git gh jq ast-grep nix just oxigraph codex claude; do check_tool "$name"; done
  case "$HARNESS_OWNER_CLI" in
    claude) ;;
    agy) check_tool agy ;;
    *) missing_part owner "HARNESS_OWNER_CLI must be agy or claude (got $HARNESS_OWNER_CLI)" \
         'export HARNESS_OWNER_CLI=agy' ;;
  esac
  check_github
  check_lead
  if [ "$HARNESS_OWNER_CLI" = agy ]; then
    check_owner_files .claude/agents/harness-owner.md .agents/agents/harness-owner.md .agents/hooks.json \
      architecture/owner/settings.json architecture/owner/agy-settings.json
  else
    check_owner_files .claude/agents/harness-owner.md architecture/owner/settings.json
  fi
  IFS=' ' read -r -a lanes <<< "$HARNESS_LANES"
  for name in "${lanes[@]}"; do
    if stopped=$(stopped_on "$name"); then
      printf 'note     lane %s: stopped by a person on %s\n' "$name" "$stopped"
    elif supervisor_pid "$name" >/dev/null; then
      printf 'ok       lane %s: running\n' "$name"
    elif [ -d "$harness_worktrees/$name" ]; then
      printf 'note     lane %s: not running\n' "$name"
    else
      printf 'note     lane %s: no worktree yet; up makes it\n' "$name"
    fi
  done
  check_state
  if [ "$missing" -gt 0 ]; then
    printf 'doctor: %s missing\n' "$missing"
    return 1
  fi
  echo 'doctor: ready'
}

up_part() {
  local name=$1 tool=$2 output code=0
  shift 2
  output=$("$BASH" "$architecture/$tool" "$@" 2>&1) || code=$?
  if [ "$tool" = lane.sh ]; then
    sed -n '/^lane: [0-9][0-9]* leftover Nix build folders; /p' <<< "$output"
  fi
  if [ "$code" = 0 ]; then
    case "${output%%$'\n'*}" in
      "$name already runs."|'host-series already runs'|'gate-times is running'|'owner loop already runs')
        printf '%s: already runs\n' "$name" ;;
      "$name: stopped by a person on "*) printf '%s\n' "$output" ;;
      *) printf '%s: started\n' "$name" ;;
    esac
  else
    printf '%s: FAILED: %s\n' "$name" "$(last_line <<< "$output")"
    result=1
  fi
}

result=0
case "${1:-}:$#" in
  --where:1) show_paths ;;
  doctor:1) doctor ;;
  up:1)
    doctor || exit 1
    [ "$harness_role" = owner ] || exit 0
    mkdir -p -- "$harness_worktrees"
    IFS=' ' read -r -a lanes <<< "$HARNESS_LANES"
    for lane in "${lanes[@]}"; do
      up_part "$lane" lane.sh start "$lane" --unless-stopped
    done
    up_part host-series host-series.sh start "$harness_state" "$harness_main"
    up_part gate-times gate-times.sh start "$harness_state" "$harness_worktrees"
    up_part owner-loop owner/session.sh loop start
    ;;
  status:1)
    "$BASH" "$architecture/lane.sh" status || result=1
    "$BASH" "$architecture/host-series.sh" status "$harness_state" || result=1
    "$BASH" "$architecture/gate-times.sh" status "$harness_state" "$harness_worktrees" || result=1
    "$BASH" "$architecture/owner/session.sh" loop status || result=1
    if owner_holder; then
      printf 'owner: %s holds the lock (process %s)\n' "$holder_kind" "$holder_pid"
    else
      echo 'owner: no lock holder'
    fi
    ;;
  *) echo 'usage: harness.sh up|status|doctor|--where' >&2; exit 2 ;;
esac
exit "$result"
