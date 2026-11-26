#!/usr/bin/env bash
# Usage: session.sh talk [runner arguments ...], session.sh check,
#        session.sh loop start|status|stop, session.sh --where
# Run one harness owner at a time. Writes owner.lock, owner.history,
# owner-checks/*.log, and the loop's PID, next check time and log in the state
# folder. Stop the loop and end any talk before removing the saved records.
set -euo pipefail
unset CDPATH
architecture=$(cd -- "$(dirname -- "$0")/.." && pwd -P)
# shellcheck source=architecture/start-common.sh
. "$architecture/start-common.sh"

script=$architecture/owner/session.sh
settings=$harness_main/architecture/owner/settings.json
agy_settings=$harness_main/architecture/owner/agy-settings.json
owner_cli=${HARNESS_OWNER_CLI:-claude}
owner_model=${HARNESS_OWNER_MODEL:-gemini-4-carbon-high}
lock=$harness_state/owner.lock
history=$harness_state/owner.history
logs=$harness_state/owner-checks
loop_file=$harness_state/owner-loop.pid
next_file=$harness_state/owner-loop.next
child='' watch='' timeout_note='' active_log='' check_started=''
claim_dir=$harness_state/owner.claim
candidate='' claim_token='' claimed=0

clear_lock() {
  mv -- "$lock" "$candidate/old-owner.lock"
}

# Atomically move a nonempty directory into place. A competing move cannot
# replace it. Removing a stale claim's unique marker never removes a new one.
claim_control() {
  local marker pid started identity
  started=$(process_started "$$") && [ -n "$started" ] || return 1
  printf -v identity '%q' "$started"
  candidate=$(mktemp -d "$harness_state/.claim.XXXXXX") || return 1
  claim_token=$$.${candidate##*.}.$identity
  mkdir -p -- "$candidate/owner.claim/$claim_token" || return 1
  until mv -f -- "$candidate/owner.claim" "$harness_state/" 2>/dev/null; do
    if [ ! -d "$claim_dir" ]; then
      # The holder can release between the failed move and this check.
      mv -f -- "$candidate/owner.claim" "$harness_state/" 2>/dev/null && break
      [ -d "$claim_dir" ] || return 1
    fi
    for marker in "$claim_dir"/*; do
      [ -d "$marker" ] || continue
      identity=${marker##*/}
      pid=${identity%%.*}
      if ! process_alive "$pid" || ! started=$(process_started "$pid") ||
          [ "$(printf '%q' "$started")" != "${identity#*.*.}" ]; then
        rmdir -- "$marker" 2>/dev/null || true
      fi
    done
    sleep 0.05
  done
}

release_control() {
  if [ -n "$claim_token" ]; then
    rmdir -- "$claim_dir/$claim_token" "$claim_dir" 2>/dev/null || true
    claim_token=''
  fi
  if [ -n "$candidate" ]; then
    rm -rf -- "$candidate"
    candidate=''
  fi
}

# Serialize changes to the holder record and release this short claim before
# starting the runner. The record stays in place while the session runs.
claim_once() {
  local started record
  claim_control || return 1
  if ! owner_holder; then
    if [ -d "$lock" ]; then clear_lock || return 1; fi
    started=$(process_started "$$") && [ -n "$started" ] || return 1
    record=$candidate/owner.lock
    mkdir -- "$record" || return 1
    printf '%s\n' "$$" > "$record/pid" && printf '%s\n' "$kind" > "$record/kind" &&
      printf '%s\n' "$started" > "$record/started" &&
      mv -f -- "$record" "$harness_state/" || return 1
    claimed=1
    holder_pid=
  fi
  release_control
}

end_session() {
  local code=$?
  if [ -n "$watch" ]; then
    kill -KILL -- "-$watch" 2>/dev/null || true
    wait "$watch" 2>/dev/null || true
  fi
  if [ -n "$child" ]; then
    # A waiting talk has a sleep child. A running check has its own group.
    if [ -n "$active_log" ]; then
      kill -KILL -- "-$child" 2>/dev/null || true
    else
      kill_tree "$child"
    fi
    wait "$child" 2>/dev/null || true
  fi
  if [ -n "$active_log" ]; then record_check "failed $code" "$code" || true; fi
  if [ -n "$timeout_note" ]; then rm -f -- "$timeout_note" || true; fi
  if [ "$claimed" = 1 ]; then
    # Serialize release with the readers that can replace a stale record.
    if [ -z "$claim_token" ]; then claim_control || return 1; fi
    clear_lock || true
  fi
  release_control
}

write_history() {
  local result=$1 minutes=$2 line=$3
  line=${line//$'\t'/ }
  printf '%s\tcheck\t%s\t%s\t%s\n' "$(date '+%Y-%m-%d %H:%M:%S')" \
    "$result" "$minutes" "${line:0:200}" >> "$history"
}

record_check() {
  local result=$1 code=$2 log=$active_log elapsed
  if { grep -iq 'sign-in' "$log" && grep -iEq 'ends|expired' "$log"; } ||
    { [ "$code" != 0 ] && grep -iEq 'not logged in|please run /login|authentication (failed|required)' "$log"; }; then
    result="$result SIGN-IN"
  fi
  elapsed=$(( ($(date +%s) - check_started) / 60 ))
  if [ "$elapsed" -lt 0 ]; then elapsed=0; fi
  write_history "$result" "$elapsed" "$(last_line < "$log")"
}

skip_check() {
  printf '%s\n' "$1"
  write_history skipped 0 "$1"
}

fail_start() {
  printf '%s\n' "$2" >&2
  if [ "$kind" = check ]; then write_history failed 0 "$2" || true; fi
  return "$1"
}

take_session() {
  local announced='' message
  while :; do
    claim_once || { fail_start 1 'owner: could not take the owner lock'; return; }
    [ -n "$holder_pid" ] || return 0
    if [ "$kind" = check ]; then
      case "$holder_kind" in
        talk) message="owner: a session with a terminal is alive (process $holder_pid); it makes its own checks" ;;
        check) message="owner: a check already runs (process $holder_pid)" ;;
      esac
      skip_check "$message"
      exit 0
    fi
    if [ "$announced" != "$holder_kind $holder_pid" ]; then
      case "$holder_kind" in
        talk) message='a session with a terminal is alive' ;;
        check) message='a check runs' ;;
      esac
      printf 'owner: %s (process %s); waiting for it to end\n' "$message" "$holder_pid"
      announced="$holder_kind $holder_pid"
    fi
    sleep 1 & child=$!
    wait "$child"
    child=
  done
}

valid_runner() {
  case "$owner_cli" in
    agy|claude) return 0 ;;
    *) fail_start 2 'owner: HARNESS_OWNER_CLI must be agy or claude' ;;
  esac
}

check_agy_files() {
  local file hooks=$harness_main/.agents/hooks.json
  for file in "$harness_main/.agents/agents/harness-owner.md" "$hooks" "$agy_settings"; do
    [ -f "$file" ] && [ -r "$file" ] ||
      { fail_start 1 'owner: harness-owner or owner settings missing'; return; }
  done
  grep -Fq 'architecture/owner/guard.sh' "$hooks" ||
    fail_start 1 'owner: harness-owner or owner settings missing'
}

# Keep the terminal available to the runner while wait lets this shell handle
# signals. Bash otherwise supplies /dev/null to an asynchronous command.
talk() {
  local code=0 arg skip_value=false model=() permission=()
  valid_runner || return
  if [ "$owner_cli" = agy ]; then
    command -v agy >/dev/null || { fail_start 127 'owner: agy is not on PATH'; return; }
    check_agy_files || return
    model=(--model "$owner_model")
    permission=(--dangerously-skip-permissions)
    for arg in "$@"; do
      if "$skip_value"; then
        skip_value=false
        continue
      fi
      case "$arg" in
        --) break ;;
        -model|--model) model=(); skip_value=true ;;
        -model=*|--model=*) model=() ;;
        -mode|--mode) permission=(); skip_value=true ;;
        -mode=*|--mode=*|-dangerously-skip-permissions|--dangerously-skip-permissions|-dangerously-skip-permissions=*|--dangerously-skip-permissions=*)
          permission=() ;;
        -add-dir|--add-dir|-agent|--agent|-conversation|--conversation|-r|--r|-resume|--resume|-effort|--effort|-i|--i|-input-format|--input-format|-json-schema|--json-schema|-log-file|--log-file|-output-format|--output-format|-p|--p|-print|--print|-print-timeout|--print-timeout|-project|--project|-prompt|--prompt|-prompt-interactive|--prompt-interactive|-settings|--settings)
          skip_value=true ;;
        -?*) ;;
        *) break ;;
      esac
    done
    HARNESS_OWNER_SESSION=1 CLAUDE_PROJECT_DIR=$harness_main \
      agy --agent harness-owner ${model[@]+"${model[@]}"} --settings "$agy_settings" \
      ${permission[@]+"${permission[@]}"} "$@" <&3 3<&- &
  else
    HARNESS_OWNER_SESSION=1 CLAUDE_PROJECT_DIR=$harness_main \
      claude --agent harness-owner --settings "$settings" "$@" <&3 3<&- &
  fi
  child=$!
  wait "$child" || code=$?
  child=
  return "$code"
}

format_epoch() {
  date -d "@$1" "$2" 2>/dev/null || date -r "$1" "$2"
}

check() {
  local stamp log limit=${OWNER_CHECK_LOG_LIMIT:-50} permission=()
  local minutes=${OWNER_CHECK_MINUTES:-60} seconds code=0 result
  valid_runner || return
  if [ "$owner_cli" = agy ]; then
    if ! [[ $owner_model =~ [^[:space:]] ]]; then
      fail_start 2 'owner: HARNESS_OWNER_MODEL must be non-empty'
      return
    fi
    permission=(--dangerously-skip-permissions)
    if [ -n "${OWNER_PERMISSION_MODE:-}" ]; then
      if ! [[ $OWNER_PERMISSION_MODE =~ [^[:space:]] ]]; then
        fail_start 2 'owner: OWNER_PERMISSION_MODE must be non-empty'
        return
      fi
      permission=(--mode "$OWNER_PERMISSION_MODE")
    fi
  fi
  if ! [[ $minutes =~ ^[0-9]+([.][0-9]+)?$ && $minutes =~ [1-9] &&
          $limit =~ ^[1-9][0-9]*$ ]]; then
    fail_start 2 'owner: the check minutes and log limit must be positive'
    return
  fi
  command -v "$owner_cli" >/dev/null || { fail_start 127 "owner: $owner_cli is not on PATH"; return; }
  if [ "$owner_cli" = agy ]; then check_agy_files || return; fi
  mkdir -p -- "$logs" || { fail_start 1 "owner: cannot create $logs"; return; }
  check_started=$(date +%s) || { fail_start 1 'owner: cannot read the clock'; return; }
  stamp=$check_started
  while :; do
    log=$(format_epoch "$stamp" '+%Y%m%d-%H%M%S') ||
      { fail_start 1 'owner: cannot format the check time'; return; }
    log=$logs/$log.log
    [ -e "$log" ] || break
    stamp=$((stamp + 1))
  done
  : > "$log" || { fail_start 1 "owner: cannot write $log"; return; }
  local files=("$logs"/*.log) index
  for ((index = 0; index < ${#files[@]} - limit; index++)); do
    rm -f -- "${files[index]}" ||
      { fail_start 1 "owner: cannot remove ${files[index]}"; return; }
  done
  seconds=$(env LC_ALL=C awk -v minutes="$minutes" 'BEGIN { printf "%.9f", minutes * 60 }') ||
    { fail_start 1 'owner: cannot calculate the check deadline'; return; }
  active_log=$log
  timeout_note=$log.timeout
  # As in tools.sh, job control gives each tree a separate process group.
  set -m
  if [ "$owner_cli" = agy ]; then
    HARNESS_OWNER_SESSION=1 CLAUDE_PROJECT_DIR=$harness_main \
      agy --agent harness-owner --model "$owner_model" --settings "$agy_settings" \
      "${permission[@]}" -p 'Run one harness check and stop.' \
      </dev/null > "$log" 2>&1 &
  else
    HARNESS_OWNER_SESSION=1 CLAUDE_PROJECT_DIR=$harness_main \
      claude -p --agent harness-owner --settings "$settings" \
      --permission-mode "${OWNER_PERMISSION_MODE:-auto}" 'Run one harness check and stop.' \
      </dev/null > "$log" 2>&1 &
  fi
  child=$!
  (
    sleep "$seconds" || exit 1
    : > "$timeout_note"
    kill -KILL -- "-$child" 2>/dev/null
  ) &
  watch=$!
  set +m
  wait "$child" 2>/dev/null || code=$?
  child=
  kill -KILL -- "-$watch" 2>/dev/null || true
  wait "$watch" 2>/dev/null || true
  watch=
  if [ -f "$timeout_note" ]; then
    result=timeout
    code=124
  elif [ "$code" = 0 ]; then
    result=ok
  else
    result="failed $code"
  fi
  record_check "$result" "$code"
  active_log=
  return "$code"
}

loop_pid() {
  local pid command suffix
  [ -r "$loop_file" ] && IFS= read -r pid < "$loop_file" || return 1
  command=$(process_command "$pid") || return 1
  # Every worktree uses the same state folder. Match that folder even if
  # another copy of this script started the loop.
  suffix="/architecture/owner/session.sh loop run $harness_state"
  case "$command" in
    bash\ *"$suffix"|*/bash\ *"$suffix") printf '%s\n' "$pid" ;;
    *) return 1 ;;
  esac
}

end_loop() {
  local pid
  if [ -n "$child" ]; then
    kill -TERM "$child" 2>/dev/null || true
    wait "$child" 2>/dev/null || true
  fi
  if [ -r "$loop_file" ] && IFS= read -r pid < "$loop_file" && [ "$pid" = "$$" ]; then
    rm -f -- "$next_file" "$loop_file"
  fi
}

loop_run() {
  local due now delay pid='' attempt
  trap end_loop EXIT
  # Start publishes the PID before the loop writes its next deadline.
  for ((attempt = 0; attempt < 100; attempt++)); do
    if [ -r "$loop_file" ] && IFS= read -r pid < "$loop_file" && [ "$pid" = "$$" ]; then break; fi
    sleep 0.05
  done
  [ "$pid" = "$$" ] || return 1
  due=$(date +%s)
  while :; do
    now=$(date +%s)
    if [ "$now" -ge "$due" ]; then
      # Keep the original cadence and do at most one check after a clock jump.
      due=$((due + ((now - due) / period + 1) * period))
      printf '%s\n' "$due" > "$next_file"
      "$BASH" "$script" check & child=$!
      wait "$child" || true
      child=
    else
      if [ "$((due - now))" -gt "$period" ]; then
        due=$((now + period))
        printf '%s\n' "$due" > "$next_file"
      fi
      delay=$((due - now))
      if [ "$delay" -gt "$poll" ]; then delay=$poll; fi
      sleep "$delay" & child=$!
      wait "$child"
      child=
    fi
  done
}

loop_start() {
  local pid attempt
  control_lock 8
  if loop_pid >/dev/null; then echo 'owner loop already runs'; return 0; fi
  rm -f -- "$next_file"
  exec 3> "$loop_file"
  nohup "$BASH" "$script" loop run "$harness_state" </dev/null >> "$harness_state/owner-loop.log" 2>&1 3>&- 8>&- &
  pid=$!
  if ! printf '%s\n' "$pid" >&3; then
    kill -TERM "$pid" 2>/dev/null || true
    return 1
  fi
  exec 3>&-
  for ((attempt = 0; attempt < 100; attempt++)); do
    if loop_pid >/dev/null; then
      printf 'owner loop started (process %s)\n' "$pid"
      return 0
    fi
    process_alive "$pid" || break
    sleep 0.05
  done
  kill -TERM "$pid" 2>/dev/null || true
  rm -f -- "$loop_file"
  echo 'owner loop did not start' >&2
  return 1
}

loop_stop() {
  local pid attempt
  control_lock 8
  if pid=$(loop_pid); then
    kill -TERM "$pid" 2>/dev/null || true
    for ((attempt = 0; attempt < 200; attempt++)); do
      process_alive "$pid" || break
      sleep 0.05
    done
    if process_alive "$pid"; then echo 'owner loop did not stop' >&2; return 1; fi
    echo 'owner loop stopped'
  else
    echo 'owner loop is not running'
  fi
  rm -f -- "$next_file" "$loop_file"
}

loop_status() {
  local pid next result=0
  if pid=$(loop_pid); then
    printf 'owner loop is running (process %s)\n' "$pid"
    if [ -r "$next_file" ] && IFS= read -r next < "$next_file"; then
      printf 'next check: %s\n' "$(format_epoch "$next" '+%Y-%m-%d %H:%M:%S')"
    fi
  else
    echo 'owner loop is not running'
    result=1
  fi
  if [ -f "$history" ]; then tail -n 3 "$history"; fi
  return "$result"
}

usage() {
  echo 'usage: session.sh talk [arguments ...], check, loop start|status|stop, or --where' >&2
  exit 2
}

umask 077
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
case "${1:-}" in
  --where) [ "$#" = 1 ] || usage; show_paths ;;
  talk|check)
    [ "$1" = talk ] || [ "$#" = 1 ] || usage
    kind=$1
    state_arg=$(harness_path_hash "$harness_state") ||
      { fail_start 1 'owner: cannot identify the state folder'; exit $?; }
    exec "$BASH" "$script" run "$1" "$state_arg" "${@:2}"
    ;;
  run)
    state_arg=$(harness_path_hash "$harness_state") || exit 1
    [ "$#" -ge 3 ] && [ "$3" = "$state_arg" ] || usage
    shift
    kind=$1
    shift 2
    case "$kind:$#" in talk:*|check:0) ;; *) usage ;; esac
    create_state || { fail_start 1 "owner: cannot create $harness_state"; exit $?; }
    trap end_session EXIT
    take_session
    cd -- "$harness_main" || { fail_start 1 "owner: cannot enter $harness_main"; exit $?; }
    if [ "$kind" = talk ]; then
      talk "$@" 3<&0
    else
      check
    fi
    ;;
  loop)
    if [ "${2:-}" = run ]; then
      [ "$#" = 3 ] && [ "$3" = "$harness_state" ] || usage
    else
      [ "$#" = 2 ] || usage
    fi
    case "$2" in
      start|run)
        hours=${OWNER_CHECK_HOURS:-2}
        poll=${OWNER_POLL_SECONDS:-30}
        if ! [[ $hours =~ ^[1-9][0-9]*$ ]]; then
          echo 'owner loop: hours must be a positive integer' >&2
          exit 2
        fi
        period=${OWNER_CHECK_SECONDS:-$((hours * 3600))}
        if ! [[ $period =~ ^[1-9][0-9]*$ && $poll =~ ^[1-9][0-9]*$ ]]; then
          echo 'owner loop: period and poll seconds must be positive integers' >&2
          exit 2
        fi
        create_state
        if [ "$2" = start ]; then loop_start 8>> "$harness_state/owner-loop.control"; else loop_run; fi
        ;;
      stop)
        if [ -d "$harness_state" ]; then loop_stop 8>> "$harness_state/owner-loop.control"; else echo 'owner loop is not running'; fi
        ;;
      status) loop_status ;;
      *) usage ;;
    esac
    ;;
  *) usage ;;
esac
