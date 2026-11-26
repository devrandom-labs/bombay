#!/usr/bin/env bash
# Shared checkout paths and process helpers for the harness start commands.
# Source this file with the caller's architecture directory in `architecture`.
# Writes nothing on load. Control locks last until the caller closes the descriptor.

unset CDPATH
: "${architecture:?set the architecture directory before sourcing}"
# shellcheck source=architecture/project.env
. "$architecture/project.env"

# shellcheck source=architecture/owner/where.sh
. "$architecture/owner/where.sh" || return 1

show_paths() {
  printf 'main: %s\nhere: %s\nworktrees: %s\nstate: %s\nrole: %s\n' \
    "$harness_main" "$harness_here" "$harness_worktrees" "$harness_state" "$harness_role"
}

create_state() (
  umask 077
  mkdir -p -- "$harness_state"
)

fetch_main() {
  git -C "$harness_main" -c credential.https://github.com.helper='!gh auth git-credential' \
    fetch -q origin "+refs/heads/$HARNESS_MAIN:refs/remotes/origin/$HARNESS_MAIN"
}

checkout_branch() {
  git -C "$harness_main" symbolic-ref --quiet --short HEAD || printf '%s\n' 'detached HEAD'
}

clean_checkout() {
  local changes
  changes=$(git -C "$1" status --porcelain --untracked-files=no) && [ -z "$changes" ]
}

fast_forward() {
  git -C "$1" merge -q --ff-only "origin/$HARNESS_MAIN"
}

control_lock() {
  if [[ $OSTYPE == darwin* ]]; then /usr/bin/lockf -s "$1"
  else flock -x "$1"; fi
}

process_command() {
  [[ $1 =~ ^[1-9][0-9]*$ ]] && ps -ww -p "$1" -o command= 2>/dev/null
}

process_alive() {
  local status
  [[ $1 =~ ^[1-9][0-9]*$ ]] &&
    status=$(ps -p "$1" -o stat= 2>/dev/null) &&
    [[ $status =~ ^[[:space:]]*[^Z[:space:]] ]]
}

process_started() {
  [[ $1 =~ ^[1-9][0-9]*$ ]] && env LC_ALL=C TZ=UTC ps -p "$1" -o lstart= 2>/dev/null
}

agent_in_checkout() {
  local dir=$1 pattern=$2 pid
  while IFS= read -r pid; do
    [ -n "$pid" ] || continue
    [ "$(lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p')" = "$dir" ] || continue
    printf '%s\n' "$pid"
    return 0
  done < <(pgrep -f "$pattern" 2>/dev/null)
  return 1
}

kill_tree() {
  local child
  while IFS= read -r child; do
    [ -n "$child" ] && kill_tree "$child"
  done < <(pgrep -P "$1" 2>/dev/null)
  kill -TERM "$1" 2>/dev/null || true
}

last_line() {
  awk '/[^[:space:]]/ { last = $0 } END { print last }'
}

# shellcheck disable=SC2034 # The caller reads the lock fields.
owner_holder() {
  local lock=$harness_state/owner.lock started command launcher state_arg
  holder_pid='' holder_kind=''
  [ -r "$lock/pid" ] && IFS= read -r holder_pid < "$lock/pid" &&
    [ -r "$lock/kind" ] && IFS= read -r holder_kind < "$lock/kind" &&
    [ -r "$lock/started" ] && IFS= read -r started < "$lock/started" &&
    process_alive "$holder_pid" && [ -n "$started" ] &&
    [ "$(process_started "$holder_pid")" = "$started" ] || return 1
  case "$holder_kind" in talk|check) ;; *) return 1 ;; esac
  command=$(process_command "$holder_pid") || return 1
  launcher=${command%% *}
  [ "${launcher##*/}" = bash ] || return 1
  # One ASCII argument identifies the state path even when it contains spaces.
  state_arg=$(harness_path_hash "$harness_state") || return 1
  case "${command#* } " in
    /*/architecture/owner/session.sh\ run\ "$holder_kind $state_arg "*) return 0 ;;
  esac
  return 1
}

stopped_on() (
  mark=$harness_worktrees/$1/.cache/run.stopped
  [ -f "$mark" ] || return 1
  IFS= read -r stopped 2>/dev/null < "$mark" || true
  printf '%s\n' "${stopped:-an unknown date}"
)

supervisor_pid() {
  local name=$1 dir=$harness_worktrees/$1 pid command runner prefix
  [ -r "$dir/.cache/run.pid" ] && IFS= read -r pid < "$dir/.cache/run.pid" || return 1
  command=$(process_command "$pid") || return 1
  # direnv exec replaces itself. caffeinate can stay as the supervisor's parent.
  case "$command" in
    direnv\ exec\ .\ *|*/direnv\ exec\ .\ *) command=${command#*direnv exec . } ;;
  esac
  case "$command" in
    caffeinate\ -i\ *|*/caffeinate\ -i\ *) command=${command#*caffeinate -i } ;;
  esac
  for runner in "$dir/architecture/run.sh" "$harness_main/architecture/run.sh" architecture/run.sh; do
    prefix="bash $runner $name"
    case "$command" in
      "$prefix"|*/"$prefix") printf '%s\n' "$pid"; return 0 ;;
    esac
  done
  return 1
}

free_gib() {
  df -Pk "$1" | awk 'NR == 2 { print int($4 / 1048576) }'
}
