#!/usr/bin/env bash
# Merge turns through the real supervisor, local Git and recorded fake clients.
set -euo pipefail
supervisor=${TURN_SUPERVISOR:-$PWD/architecture/run.sh}
backlog=$PWD/architecture/backlog.jq
landing=$PWD/architecture/tests/run-test.sh
gate_script=${TURN_GATE_SCRIPT:-$PWD/architecture/gate.sh}
event_limit=${TURN_TEST_LIMIT:-120}
work=$(mktemp -d)
. "$PWD/architecture/tests/own-processes.sh"
own_init "$work"
real_git=$(command -v git) real_sleep=$(command -v sleep) real_find=$(command -v find)
real_mkdir=$(command -v mkdir) real_rm=$(command -v rm) real_ps=$(command -v ps || true)
real_timeout=$(command -v timeout || true) real_mktemp=$(command -v mktemp)
signal_env=$(command -v env)
export real_git real_sleep real_find real_mkdir real_rm real_timeout real_mktemp real_ps signal_env RUN_TEST_BASH=$BASH
cleanup() {
  local child deadline=$((SECONDS + 2))
  trap '' TERM HUP
  own_cleanup TERM
  own_stop "$deadline" "$real_sleep"
  own_cleanup
  for child in $(jobs -p); do
    wait "$child" 2>/dev/null || true
  done
  [ -z "${TURN_PIDS:-}" ] || { cat "$work"/case*/processes 2>/dev/null || true; } > "$TURN_PIDS"
  [ -z "${TURN_KEEP:-}" ] || cp -R "$work" "$TURN_KEEP"
  chmod -R u+w "$work"
  "$real_rm" -rf "$work"
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 129' HUP
mkdir -p "$work/bin" "$work/no-timeout" "$work/cache"
# An omitted RUN_TURN still uses this cache. Never use the user's lock folders.
export XDG_CACHE_HOME=$work/cache RUN_TURN=$work/unused.lock GATE_LOCK=$work/unused-gate.lock
unset RUN_TURN_POLL RUN_TURN_TELL RUN_TURN_FETCH RUN_STATE
export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=commit.gpgsign GIT_CONFIG_VALUE_0=false
export GIT_AUTHOR_NAME=Test GIT_AUTHOR_EMAIL=test@example.invalid GIT_COMMITTER_NAME=Test GIT_COMMITTER_EMAIL=test@example.invalid
cat > "$work/bin/bash" <<'STUB'
#!/bin/sh
if [ "$1" = architecture/run.sh ] && [ -n "${RUN_STATE:-}" ]; then
  cp "$RUN_STATE" "$TEST_ROOT/$ID/restart.state"
  . "$OWN_HELPER"; own_publish $$ "$TEST_ROOT/$ID/restart.pid"
  owner=$(cat "$TURN_PATH/pid" 2>/dev/null || echo -)
  printf '%s %s exec %s\n' "$(cat "$TEST_ROOT/clock")" "$ID" "$owner" >> "$TEST_ROOT/timeline"
  if [ "${HOLD_RESTART:-0}" = 1 ]; then
    while [ ! -e "$TEST_ROOT/$ID/restart" ]; do "$real_sleep" 0.01; done
  fi
fi
exec "$RUN_TEST_BASH" "$@"
STUB
cat > "$work/bin/env" <<'STUB'
#!/bin/sh
owner=$(cat "$TURN_PATH/pid" 2>/dev/null || echo -)
if [ "${1:-}" = -- ] || { [ "$PWD" = "$TEST_ROOT/C" ] && [ "$owner" = "$PPID" ]; }; then
  [ "$owner" = "$PPID" ] || exit 95
  . "$OWN_HELPER"; own_publish "$owner" "$TEST_ROOT/C/lookup.owner"
fi
exec "$signal_env" "$@"
STUB
cat > "$work/bin/sleep" <<'STUB'
#!/bin/sh
printf '%s %s poll %s\n' "$(cat "$TEST_ROOT/clock")" "$ID" "$1" >> "$TEST_ROOT/timeline"
printf '%s %s\n' "$ID" "$1" >> "$TEST_ROOT/sleeps"
if [ "${CONTROL_POLL:-0}" = 1 ] && [ "$1" = 20 ]; then
  parent=$PPID
  while [ ! -e "$TEST_ROOT/$ID/poll" ]; do
    kill -0 "$parent" 2>/dev/null || exit 0
    "$real_sleep" 0.01
  done
  "$real_rm" "$TEST_ROOT/$ID/poll"
  exit
fi
exec "$real_sleep" 0.02
STUB
cat > "$work/bin/find" <<'STUB'
#!/bin/sh
if [ "$1" = "$TURN_PATH" ] && [ -f "$TEST_ROOT/born" ]; then
  value=$(cat "$TEST_ROOT/clock")
  minutes=${5#+}
  [ "$((value - $(cat "$TEST_ROOT/born")))" -le "$((minutes * 60))" ] || {
    echo "$1"
    if [ -n "${REPLACE_HOLDER:-}" ] && [ ! -e "$TEST_ROOT/replaced" ]; then
      . "$OWN_HELPER"; own_publish "$REPLACE_HOLDER" "$TURN_PATH/pid"
      printf '%s %s replace %s\n' "$value" "$ID" "$REPLACE_HOLDER" >> "$TEST_ROOT/timeline"
      touch "$TEST_ROOT/replaced"
    fi
  }
else exec "$real_find" "$@"; fi
STUB
cat > "$work/bin/mkdir" <<'STUB'
#!/bin/sh
if [ "$#" = 1 ] && [ "$1" = "$TURN_PATH" ]; then
  printf '%s %s mkdir.locale %s\n' "$(cat "$TEST_ROOT/clock")" "$ID" "${LC_ALL:-unset}" >> "$TEST_ROOT/timeline"
  [ -z "${PID_WRITE:-}" ] || { . "$OWN_HELPER"; own_record $$ >> "$TEST_ROOT/processes"; }
  if [ -e "$TEST_ROOT/$ID/hold-mkdir" ]; then
    . "$OWN_HELPER"; own_record $$ >> "$TEST_ROOT/processes"
    touch "$TEST_ROOT/$ID/before-mkdir"
    while [ ! -e "$TEST_ROOT/$ID/resume-mkdir" ]; do "$real_sleep" 0.01; done
  fi
fi
if ! "$real_mkdir" "$@"; then
  if [ "$#" = 1 ] && [ "$1" = "$TURN_PATH" ]; then
    printf '%s %s mkdir.failed\n' "$(cat "$TEST_ROOT/clock")" "$ID" >> "$TEST_ROOT/timeline"
  fi
  if [ "$#" = 1 ] && [ "$1" = "$TURN_PATH" ] && [ "${RELEASE_AT_MKDIR:-0}" = 1 ] && [ ! -e "$TEST_ROOT/mkdir.released" ]; then
    "$real_rm" -rf "$TURN_PATH"
    printf '%s %s mkdir.released\n' "$(cat "$TEST_ROOT/clock")" "$ID" >> "$TEST_ROOT/timeline"
    touch "$TEST_ROOT/mkdir.released"
  fi
  exit 1
fi
if [ "$#" = 1 ] && [ "$1" = "$TURN_PATH" ]; then
  printf '%s %s take\n' "$(cat "$TEST_ROOT/clock")" "$ID" >> "$TEST_ROOT/timeline"
  if [ "${PID_WRITE:-}" = always ]; then
    chmod 555 "$1"
    cat "$TEST_ROOT/clock" > "$TEST_ROOT/born"
    printf '%s %s pid.readonly\n' "$(cat "$TEST_ROOT/clock")" "$ID" >> "$TEST_ROOT/timeline"
  elif [ "${PID_WRITE:-}" = once ] && [ ! -e "$TEST_ROOT/dropped" ]; then
    rmdir "$1"; touch "$TEST_ROOT/dropped"
    printf '%s %s pid.drop\n' "$(cat "$TEST_ROOT/clock")" "$ID" >> "$TEST_ROOT/timeline"
  fi
fi
STUB
cat > "$work/bin/rm" <<'STUB'
#!/bin/sh
if [ "$#" = 2 ] && [ "$1" = -rf ] && [ "$2" = "$TURN_PATH" ]; then
  owner=$(cat "$TURN_PATH/pid" 2>/dev/null || echo -)
  printf '%s %s release %s\n' "$(cat "$TEST_ROOT/clock")" "${ID:-test}" "$owner" >> "$TEST_ROOT/timeline"
  if [ "${COMMAND_CHILD:-0}" = 1 ] && kill -0 "$(cat "$TEST_ROOT/C/command.pid")" 2>/dev/null; then
    touch "$TEST_ROOT/C/release.live"
  fi
  [ "${RM_FAIL:-0}" = 0 ] || exit 1
fi
exec "$real_rm" "$@"
STUB
cat > "$work/bin/mktemp" <<'STUB'
#!/bin/sh
if [ "${HOLD_REGISTRATION:-0}" = 1 ] && [ "${1:-}" = -d ]; then
  case "$2" in "$TURN_PATH.wait/"*)
    parent=$PPID
    touch "$TEST_ROOT/$ID/registering"
    while [ ! -e "$TEST_ROOT/$ID/register" ]; do
      kill -0 "$parent" 2>/dev/null || exit 1
      "$real_sleep" 0.01
    done
    ;;
  esac
fi
exec "$real_mktemp" "$@"
STUB
cat > "$work/ps" <<'STUB'
#!/bin/sh
if [ "${7:-}" = lstart= ]; then
  state=S
  if [ -e "$TEST_ROOT/zombie.$3" ]; then state=Z; fi
  printf '%s start of process %s\n' "$state" "$3"
  if [ -e "$TEST_ROOT/reused.$3" ]; then echo 'reused PID'; fi
  exit
fi
printf '%s %s ps %s\n' "$(cat "$TEST_ROOT/clock")" "$ID" "$*" >> "$TEST_ROOT/timeline"
if [ "$3" = "$(cat "$TEST_ROOT/self.pid" 2>/dev/null)" ]; then
  case "${SELF_PS_MODE:-}" in
    fail) echo 'bash run.sh turn command'; exit 1 ;;
    empty) exit 0 ;;
  esac
  echo "${SELF_COMMAND:-bash architecture/run.sh turn command}"
  exit 0
fi
case "${PS_MODE:-}" in
  fail) exit 1 ;;
  empty) exit 0 ;;
  errorline) echo '/bin/sleep 600'; exit 1 ;;
esac
if [ -n "${REPLACE_HOLDER:-}" ]; then
  if [ ! -e "$TEST_ROOT/replaced" ]; then
    . "$OWN_HELPER"; own_publish "$REPLACE_HOLDER" "$TURN_PATH/pid"
    printf '%s %s replace %s\n' "$(cat "$TEST_ROOT/clock")" "$ID" "$REPLACE_HOLDER" >> "$TEST_ROOT/timeline"
    touch "$TEST_ROOT/replaced"
    echo '/bin/sleep 600'; exit 0
  fi
  echo 'bash architecture/run.sh studio'; exit 0
fi
echo "${FOREIGN_COMMAND:-bash architecture/run.sh studio}"
STUB
if [ -n "$real_timeout" ]; then
printf '#!%s\n' "$BASH" > "$work/bin/timeout"
cat >> "$work/bin/timeout" <<'STUB'
read_clock() {
  clock_status=0; clock=$(cat "$TEST_ROOT/clock") || clock_status=$?
  if [ "${TURN_LAND_EVIDENCE:-0}" = 1 ] && [ "$clock_status" != 0 ]; then
    printf '%s timeout %s clock lookup status=%s\n' "$ID" "$1" "$clock_status" >> "$TEST_ROOT/timeline"
  fi
}
read_clock start
printf '%s %s timeout %s\n' "$clock" "$ID" "$*" >> "$TEST_ROOT/timeline"
owner_status=0
owner=$(cat "$TURN_PATH/pid" 2>/dev/null || {
  status=$?
  if [ "${TURN_LAND_EVIDENCE:-0}" = 1 ]; then
    printf '%s timeout owner read status=%s\n' "$ID" "$status" >> "$TEST_ROOT/timeline" || true
  fi
  echo -
}) || owner_status=$?
export TURN_CAP_ARGS="$*"
options=()
[ "${TURN_LAND_EVIDENCE:-0}" != 1 ] || options=(--verbose)
"$real_timeout" "${options[@]}" "$@" & child=$!
if [ "${TURN_LAND_EVIDENCE:-0}" = 1 ]; then
  printf '%s %s timeout wrapper=%s child=%s clock_status=%s owner_status=%s %s\n' \
    "$clock" "$ID" "$$" "$child" "$clock_status" "$owner_status" "$*" >> "$TEST_ROOT/timeline"
fi
. "$OWN_HELPER"; own_record "$child" group >/dev/null
. "$OWN_HELPER"; own_record "$$" >> "$TEST_ROOT/processes"
. "$OWN_HELPER"; own_record "$child" >> "$TEST_ROOT/processes"
read_clock owner
printf '%s %s timeout.owner %s %s\n' "$clock" "$ID" "$owner" "$*" >> "$TEST_ROOT/timeline"
code=0; wait "$child" || code=$?
if [ "${TURN_LAND_EVIDENCE:-0}" = 1 ] && [ "$code" -ne 0 ] && [ -s .cache/land.log ]; then
  cat .cache/land.log >> "$TEST_ROOT/timeline" || true
fi
own_forget "$child"
read_clock end
printf '%s %s timeout.end %s\n' "$clock" "$ID" "$code" >> "$TEST_ROOT/timeline"
read_clock done
printf '%s %s timeout.done %s\n' "$clock" "$ID" "$*" >> "$TEST_ROOT/timeline"
exit "$code"
STUB
fi
printf '#!%s\n' "$BASH" > "$work/bin/git"
cat >> "$work/bin/git" <<'STUB'
case " $* " in *' push '*)
  echo "$RUN_TEST_NAME: real git push escaped its seam" >> "$TEST_ROOT/leak"; exit 99 ;;
esac
if [ "$1" = fetch ] && [ "${TURN_LAND_CALL:-0}" = 0 ]; then
  owner=$(cat "$TURN_PATH/pid" 2>/dev/null || echo -)
  printf '%s %s sync %s %s %s\n' "$(cat "$TEST_ROOT/clock")" "$ID" "$owner" "$(cat supervisor.pid)" "${TURN_CAP_ARGS:-none}" >> "$TEST_ROOT/timeline"
  [ ! -e requested ] || [ ! -f fetch.sh ] || . ./fetch.sh
fi
exec "$real_git" "$@"
STUB
cat > "$work/bin/gh" <<'STUB'
#!/bin/sh
echo "$RUN_TEST_NAME: real gh escaped its seam: $*" >> "$TEST_ROOT/leak"
exit 99
STUB
cat > "$work/launch" <<'STUB'
#!/bin/sh
. "$OWN_HELPER"; own_write $$ supervisor.pid
if [ "${OWN_LOCK:-0}" = 1 ]; then
  "$real_mkdir" "$TURN_PATH"
  . "$OWN_HELPER"; own_publish $$ "$TURN_PATH/pid"
  printf '%s %s own.pid %s\n' "$(cat "$TEST_ROOT/clock")" "$ID" "$$" >> "$TEST_ROOT/timeline"
fi
if [ "${OLD_OWN_TICKET:-0}" = 1 ]; then mkdir -p "$TURN_PATH.wait/1.$$.unknown.stale"; fi
exec "$@"
STUB
cat > "$work/now" <<'STUB'
cat "$TEST_ROOT/clock"
STUB
cat > "$work/notify" <<'STUB'
. "$OWN_HELPER"; own_record $$ >> "$TEST_ROOT/processes"
if [ "${NOTIFY_HANG:-0}" = 1 ]; then
  case "$*" in *'the supervisor of main does not parse'*)
    # Hold startup before the body; TERM is ignored once env has started.
    while :; do "$real_sleep" 0.01; done ;;
  esac
fi
owner=$(cat "$TURN_PATH/pid" 2>/dev/null || echo -)
printf '%s %s notify.owner %s\n' "$(cat "$TEST_ROOT/clock")" "$ID" "$owner" >> "$TEST_ROOT/timeline"
printf '%s %s notify %s\n' "$(cat "$TEST_ROOT/clock")" "$ID" "$*" >> "$TEST_ROOT/timeline"
echo "$*" >> "$TEST_ROOT/notified"
STUB
printf '#!%s -S --ignore-signal=TERM %s\n' "$signal_env" "$BASH" > "$work/bin/osascript"
cat >> "$work/bin/osascript" <<'STUB'
exec "$RUN_TEST_BASH" "$TURN_TEST_NOTIFY" "$@"
STUB
export TURN_TEST_NOTIFY=$work/notify
cat > "$work/checks" <<'STUB'
. "$OWN_HELPER"; own_record $$ >> "$TEST_ROOT/processes"
count=$(($(cat checks.count 2>/dev/null || echo 0) + 1)); echo "$count" > checks.count
owner=$(cat "$TURN_PATH/pid" 2>/dev/null || echo -)
printf '%s %s checks %s\n' "$(cat "$TEST_ROOT/clock")" "$ID" "$owner" >> "$TEST_ROOT/timeline"
[ ! -f checks.sh ] || . ./checks.sh
echo 4242
STUB
printf '#!%s\n' "$BASH" > "$work/lead"
cat >> "$work/lead" <<'STUB'
set -eu
. "$OWN_HELPER"; own_record $$ >> "$TEST_ROOT/processes"
while [ $# -gt 0 ]; do [ "$1" != -o ] || out=$2; prompt=$1; shift; done
count=$(($(cat calls 2>/dev/null || echo 0) + 1)); echo "$count" > calls
[ "$count" -le 12 ] || { own_signal TERM "$(cat supervisor.pid)"; exit 98; }
echo "$prompt" > "prompt.$count"
owner=$(cat "$TURN_PATH/pid" 2>/dev/null || echo -)
printf '%s %s session.%s %s\n' "$(cat "$TEST_ROOT/clock")" "$ID" "$count" "$owner" >> "$TEST_ROOT/timeline"
reply='blocked done'
if [[ "$prompt" == 'Run the weekly design review'* ]]; then reply='reviewed 0'
elif [ "$count" = 1 ] && [ "${PENDING:-0}" = 0 ]; then reply='gate ready'; fi
[ ! -f "session.$count.sh" ] || . "./session.$count.sh"
if [[ "$reply" == gate* ]]; then
  touch requested
  if [ "${PREPARE:-1}" = 1 ]; then
    mkdir -p .cache
    printf 'Reviewed-head: %s\nA test step\n\nThe test text.\n' "$(git rev-parse HEAD)" > .cache/pr.md
  fi
fi
echo "STEP $reply" > "$out"
touch "session.$count"
STUB
printf '#!%s\n' "$BASH" > "$work/gate"
cat >> "$work/gate" <<'STUB'
set -eu
. "$OWN_HELPER"; own_record $$ >> "$TEST_ROOT/processes"
. "$OWN_HELPER"; own_publish $$ gate.pid
trap 'touch gate.ended' EXIT
trap 'exit 143' TERM
trap 'exit 129' HUP
touch gate.enter
# Model gate.sh's machine lock without touching its real folder.
until mkdir "$TEST_ROOT/machine.lock" 2>/dev/null; do "$real_sleep" 0.01; done
trap 'rmdir "$TEST_ROOT/machine.lock"; touch gate.ended' EXIT
count=$(($(cat gates 2>/dev/null || echo 0) + 1)); echo "$count" > gates
git rev-parse HEAD >> gated
owner=$(cat "$TURN_PATH/pid" 2>/dev/null || echo -)
printf '%s %s gate.%s %s\n' "$(cat "$TEST_ROOT/clock")" "$ID" "$count" "$owner" >> "$TEST_ROOT/timeline"
own_publish "$owner" gate.holder
touch "gate.$count"
while [ -e "$TEST_ROOT/gate-hold.$ID" ]; do "$real_sleep" 0.01; done
[ ! -f gate.sh ] || . ./gate.sh
exit 0
STUB
# Read the base suite's single client fake; its JSON and remote state stay there.
sed -n '/^cat > "\$work\/land.sh" <<.STUB.$/,/^STUB$/p' "$landing" | sed '1d;$d' > "$work/land.sh"
[ -s "$work/land.sh" ] || { echo 'turn-test: shared landing fake missing'; exit 1; }
printf '#!%s\n' "$BASH" > "$work/land"
cat >> "$work/land" <<'STUB'
set -eu
. "$OWN_HELPER"; own_record $$ >> "$TEST_ROOT/processes"
lookup_failed() {
  [ "${TURN_LAND_EVIDENCE:-0}" != 1 ] ||
    printf '%s %s lookup status=%s\n' "$ID" "$2" "$1" >> "$TEST_ROOT/timeline" || true
}
read_or() {
  cat "$1" 2>/dev/null || {
    lookup_failed "$?" "$3 read"
    printf '%s\n' "$2"
  }
}
land_event() {
  local clock
  clock=$(cat "$TEST_ROOT/clock") || lookup_failed "$?" "$1 clock"
  printf '%s %s %s\n' "$clock" "$ID" "$*" >> "$TEST_ROOT/timeline"
}
case "$1 $2" in
  'push -q') action=push ;;
  'fetch -q') action=fetch ;;
  'pr list') action=list ;;
  'pr create') action=create ;;
  'pr edit') action=edit ;;
  'pr merge') action=merge ;;
  'pr view') action=view ;;
  'issue view')
    owner=$(read_or "$TURN_PATH/pid" - 'labels owner') || {
      status=$?; lookup_failed "$status" 'labels owner'; exit "$status"
    }
    land_event labels "$owner"
    exec "$RUN_TEST_BASH" "$TURN_LAND_FAKE" "$@" ;;
  *) echo "$RUN_TEST_NAME: unexpected landing call: $*" >> "$TEST_ROOT/leak"; exit 98 ;;
esac
previous_count=$(read_or "land.$action.count" 0 "$action count") || {
  status=$?; lookup_failed "$status" "$action count"; exit "$status"
}
count=$(($previous_count + 1))
echo "$count" > "land.$action.count"
owner=$(read_or "$TURN_PATH/pid" - "$action owner") || {
  status=$?; lookup_failed "$status" "$action owner"; exit "$status"
}
if [ "${TURN_LAND_EVIDENCE:-0}" = 1 ] && [ "$action.$count" = view.2 ]; then
  printf '%s land.view.2 pid=%s parent=%s\n' "$ID" "$$" "$PPID" >> "$TEST_ROOT/timeline"
  trap 'printf "%s land.view.2 received TERM pid=%s\n" "$ID" "$$" >> "$TEST_ROOT/timeline" || true; trap - TERM; kill -TERM "$$"' TERM
fi
land_event "land.$action" "$owner"
land_event "land.$action.$count"
while [ -e "$TEST_ROOT/land-hold.$ID.$action" ] || [ -e "$TEST_ROOT/land-hold.$ID.$action.$count" ]; do "$real_sleep" 0.01; done
# Only the local fake remote's writers need this; the supervisor owns the turn.
if [ "$action" = push ] || [ "$action" = merge ]; then
  until "$real_mkdir" "$TEST_ROOT/origin.lock" 2>/dev/null; do "$real_sleep" 0.01; done
  trap 'rmdir "$TEST_ROOT/origin.lock"' EXIT
fi
code=0
TURN_LAND_CALL=1 "$RUN_TEST_BASH" "$TURN_LAND_FAKE" "$@" || code=$?
if [ "$code" != 0 ]; then
  if [ "${TURN_LAND_EVIDENCE:-0}" = 1 ]; then
    printf '%s land.%s status=%s\n' "$ID" "$action" "$code" >> "$TEST_ROOT/timeline"
  fi
  land_event land.failed "$action"
else
  land_event "land.$action.end"
  land_event "land.$action.$count.end"
  if [ "$action" = fetch ] && [ -e merged.call ]; then
    git -C origin rev-parse main > "$TEST_ROOT/merged.$ID"
    rm -f requested
  fi
fi
exit "$code"
STUB
export TURN_LAND_FAKE=$work/land.sh
printf '#!%s\n' "$BASH" > "$work/command"
cat >> "$work/command" <<'STUB'
set -eu
. "$OWN_HELPER"; own_record $$ >> "$TEST_ROOT/processes"
[ "$PWD" = "$TEST_ROOT/C" ] && [ "$COMMAND_VALUE" = 'the caller' ] || exit 96
[ "$#" = 3 ] && [ "$1" = 'one argument' ] && [ -z "$2" ] && [ "$3" = '*' ] || exit 97
. "$OWN_HELPER"; own_publish $$ command.pid
. "$OWN_HELPER"; own_record $$ group >/dev/null
trap 'own_forget "$$"' EXIT
owner=$(cat "$TURN_PATH/pid" 2>/dev/null || echo -)
printf '%s C command.start %s\n' "$(cat "$TEST_ROOT/clock")" "$owner" >> "$TEST_ROOT/timeline"
if [ -n "${COMMAND_TRAP:-}" ]; then
  teardown() {
    signals=$((${signals:-0} + 1))
    echo "$signals" > command.signalled
    end=$((SECONDS + event_limit))
    while [ ! -e command.release ]; do
      [ "$SECONDS" -lt "$end" ] || exit 98
      "$real_sleep" 0.01
    done
    if [ "${COMMAND_GROUP:-0}" = 1 ]; then
      wait "$peer"
      kill -TERM "$child"
      wait "$child"
      [ -e command.child.ended ] || exit 99
      own_forget "$child"
    fi
    touch command.cleanup
    exit 31
  }
  trap teardown "$COMMAND_TRAP"
elif [ "${COMMAND_IGNORE:-0}" = 1 ]; then
  trap '' TERM
fi
if [ "${COMMAND_GROUP:-0}" = 1 ]; then
  env --default-signal=INT,TERM,HUP,QUIT "$RUN_TEST_BASH" -c '
    trap "touch command.peer.ended; exit 0" "$COMMAND_TRAP"
    . "$OWN_HELPER"; own_record $$ >> "$TEST_ROOT/processes"
    touch command.peer.ready
    while :; do "$real_sleep" 0.01; done
  ' & peer=$!
  . "$OWN_HELPER"; own_record "$peer" >> "$TEST_ROOT/processes"
  "$real_timeout" "$event_limit" "$RUN_TEST_BASH" -c '
    trap "touch command.child.ended; exit 0" TERM
    . "$OWN_HELPER"; own_record $$ >> "$TEST_ROOT/processes"
    touch command.child.ready
    end=$((SECONDS + event_limit))
    while :; do [ "$SECONDS" -lt "$end" ] || exit 98; "$real_sleep" 0.01; done
  ' & child=$!
  . "$OWN_HELPER"; own_record "$child" >> "$TEST_ROOT/processes"
  . "$OWN_HELPER"; own_record "$child" group >/dev/null
fi
touch command.ready
[ -z "${COMMAND_EXIT_SIGNAL:-}" ] || kill -"$COMMAND_EXIT_SIGNAL" $$
end=$((SECONDS + event_limit))
while [ -e "$TEST_ROOT/command-hold" ]; do
  [ "$SECONDS" -lt "$end" ] || exit 98
  [ ! -e command.check ] || touch command.checked
  "$real_sleep" 0.01
done
printf '%s C command.end\n' "$(cat "$TEST_ROOT/clock")" >> "$TEST_ROOT/timeline"
exit "${COMMAND_CODE:-0}"
STUB
chmod +x "$work/bin/"* "$work/ps" "$work/lead" "$work/gate" "$work/land" "$work/command"
ln -s "$work/ps" "$work/bin/ps"
for tool in bash sh git jq awk basename cat cksum cp date df dirname env grep mkdir mv rm ps sed sort tail tr wc find sleep touch rmdir chmod head ln mktemp cut ls true; do
  path=$work/bin/$tool
  [ -x "$path" ] || path=$(type -P "$tool")
  ln -s "$path" "$work/no-timeout/$tool"
done
if flock_path=$(type -P flock); then ln -s "$flock_path" "$work/no-timeout/flock"; fi
# Exclude host commands such as macOS say from command-lookup scenarios.
cp -R "$work/no-timeout" "$work/commands"
[ -z "$real_timeout" ] || ln -s "$work/bin/timeout" "$work/commands/timeout"
export PATH="$work/bin:$PATH"
export RUN_TURN_PS="$BASH $work/ps"
cases=0 skipped=0 unavailable=0
fail() {
  echo "turn-test: FAILED: $name: $*"
  cat "$TEST_ROOT/timeline" "$TEST_ROOT/A/out" "$TEST_ROOT/B/out" 2>/dev/null || true
  exit 1
}
new_case() {
  name=$1; cases=$((cases + 1)); export TEST_ROOT=$work/case$cases
  export RUN_TEST_NAME="$name" RUN_TEST_GH_SENTINEL=$TEST_ROOT/leak
  [ -z "${TURN_PROGRESS:-}" ] || echo "$name" > "$TURN_PROGRESS"
  mkdir -p "$TEST_ROOT/origin/architecture"
  echo 1000 > "$TEST_ROOT/clock"
  cp "$supervisor" "$TEST_ROOT/origin/architecture/run.sh"
  cp "$backlog" "$TEST_ROOT/origin/architecture/backlog.jq"
  chmod u+w "$TEST_ROOT/origin/architecture/"*
  echo '*' > "$TEST_ROOT/origin/.gitignore"
  "$real_git" -C "$TEST_ROOT/origin" init -q -b main
  "$real_git" -C "$TEST_ROOT/origin" add -f architecture .gitignore
  "$real_git" -C "$TEST_ROOT/origin" commit -qm base
  export RUN_TURN=$TEST_ROOT/turn.lock TURN_PATH=$TEST_ROOT/turn.lock
}
run_case() {
  local needs_timeout=$1 label=$2 before=$unavailable; shift 2
  if [ -n "${TURN_CASE:-}" ] && [[ "$label" != *"$TURN_CASE"* ]]; then return; fi
  if [ "$needs_timeout" = 1 ] && [ -z "$real_timeout" ]; then
    echo "turn-test: unavailable: $label: timeout is not on PATH"
    skipped=$((skipped + 1)); return
  fi
  new_case "$label"
  "$@"
  [ ! -e "$TEST_ROOT/leak" ] || fail "$(cat "$TEST_ROOT/leak")"
  if [[ "$label" = 'turn command'* || "$label" = 'gate command'* ]] && [ "$unavailable" = "$before" ]; then
    echo "turn-test: pass: $label"
  fi
}
clone() {
  "$real_git" clone -q "$TEST_ROOT/origin" "$TEST_ROOT/$1"
  "$real_git" -C "$TEST_ROOT/$1" switch -qc "$1"
  ln -s "$TEST_ROOT/origin" "$TEST_ROOT/$1/origin"
  echo "$1" > "$TEST_ROOT/$1/$1"
  "$real_git" -C "$TEST_ROOT/$1" add -f "$1"
  "$real_git" -C "$TEST_ROOT/$1" commit -qm "$1"
  printf '{"items":[{"content":{"number":1,"body":""},"title":"turn","task ID":"T1","kind":"Task","phase":"Phase 1","workstream":"Studio","status":"Blocked","sequence":1,"assignees":["joeldsouzax"]}]}' > "$TEST_ROOT/$1/items.json"
}
start() {
  local id=$1; shift
  ( cd "$TEST_ROOT/$id"
    exec env ID="$id" STEP_LEAD="bash $work/lead" \
      RUN_ITEMS="$TEST_ROOT/$id/items.json" RUN_SYNC=1 RUN_REVIEW=0 STEP_FREE=0 RUN_FULL=0 \
      RUN_NOW="bash $work/now" RUN_GATE="bash $work/gate" RUN_NOTIFY="bash $work/notify" \
      RUN_PUSH="bash $work/land" RUN_GH="bash $work/land" RUN_CHECKS="bash $work/checks" \
      RUN_TURN_PS="sh $work/ps" RUN_RETRY_WAIT=0 RUN_QUICK=0 "$@" \
      sh "$work/launch" "$BASH" "$TEST_ROOT/$id/architecture/run.sh" studio "${TURN_ONCE:-}"
  ) > "$TEST_ROOT/$id/out" 2>&1 &
  pid=$!; . "$OWN_HELPER"; own_publish "$pid" "$TEST_ROOT/$id/supervisor.pid"
  echo "$pid" >> "$TEST_ROOT/processes"
}
await() {
  local end=$((SECONDS + $event_limit))
  until [ -e "$1" ]; do
    [ -n "$(jobs -pr)" ] || { [ -n "${2:-}" ] && kill -0 "$2" 2>/dev/null; } \
      || [ -e "$1" ] || fail "processes ended before $1"
    [ "$SECONDS" -lt "$end" ] || fail "missing event $1 (bounded test wait)"
    "$real_sleep" 0.01
  done
}
await_line() {
  local end=$((SECONDS + $event_limit)) matches active
  while :; do
    matches=$(count "$1" "$2") || lookup_failed "$?" "count $2"
    [ "$matches" -ge "${3:-1}" ] && break
    active=$(jobs -pr) || lookup_failed "$?" 'supervisor jobs'
    [ -n "$active" ] || fail "supervisors ended before $2"
    [ "$SECONDS" -lt "$end" ] || fail "missing line $2 (bounded test wait)"
    "$real_sleep" 0.01
  done
}
finish() {
  local child=$1 want=${2:-1} code=0 end=$((SECONDS + $event_limit))
  while kill -0 "$child" 2>/dev/null; do
    [ "$SECONDS" -lt "$end" ] || fail "supervisor $child did not finish (bounded test wait)"
    "$real_sleep" 0.01
  done
  wait "$child" || code=$?
  [ "$code" = "$want" ] || fail "exit $code, wanted $want"
}
clock() { echo "$1" > "$TEST_ROOT/clock.new"; mv "$TEST_ROOT/clock.new" "$TEST_ROOT/clock"; }
lookup_failed() {
  if [ "${turn_evidence:-0}" = 1 ]; then printf 'lookup %s status=%s\n' "$2" "$1" >> "$TEST_ROOT/timeline"; fi
}
count() {
  local n status=0
  n=$(grep -c "$2" "$1" 2>/dev/null) || status=$?
  if [ "$status" -ge 2 ]; then lookup_failed "$status" "grep count $2"; fi
  echo "${n:-0}"
}
line() { grep -n "$1" "$TEST_ROOT/timeline" | head -n 1 | cut -d: -f1; }

start_command() {
  mkdir -p "$TEST_ROOT/C"
  ( cd "$TEST_ROOT/C"
    exec env --default-signal=INT,TERM,HUP,QUIT ID=C COMMAND_VALUE='the caller' event_limit="$event_limit" RUN_NOW="bash $work/now" \
      RUN_NOTIFY="bash $work/notify" RUN_TURN_PS="sh $work/ps" "$@" \
      "$BASH" "$TEST_ROOT/origin/architecture/run.sh" turn "$work/command" 'one argument' '' '*'
  ) > "$TEST_ROOT/C/out" 2>&1 &
  pid=$!; echo "$pid" >> "$TEST_ROOT/processes"
}
command_alone() {
  [ ! -e "$TEST_ROOT/C/.cache" ] || fail 'command wrote lane files'
  [ "$(count "$TEST_ROOT/timeline" ' C notify')" = 0 ] || fail 'command sent a notification'
  [ "$(count "$TEST_ROOT/timeline" ' C session.\| C gate.\| C checks\| C sync')" = 0 ] || fail 'command ran lane work'
}
command_after_run() {
  clone A; touch "$TEST_ROOT/gate-hold.A"
  start A; local a=$pid c
  await "$TEST_ROOT/A/gate.1"
  start_command; c=$pid
  await_line "$TEST_ROOT/sleeps" '^C 20$' 3
  [ "$(count "$TEST_ROOT/timeline" ' C command.start ')" = 0 ] || fail 'command started before run released'
  [ "$(count "$TEST_ROOT/C/out" "waiting for the merge turn of process $a$")" = 1 ] || fail 'command must name holder once'
  clock 8260
  await_line "$TEST_ROOT/sleeps" '^C 20$' 5
  . "$OWN_HELPER"; own_publish $$ "$TURN_PATH/pid"
  await_line "$TEST_ROOT/C/out" "waiting for the merge turn of process $$$"
  . "$OWN_HELPER"; own_publish "$a" "$TURN_PATH/pid"
  await_line "$TEST_ROOT/C/out" "waiting for the merge turn of process $a$" 2
  command_alone
  rm "$TEST_ROOT/gate-hold.A"; finish "$a"; finish "$c" 0
  [ "$(line " A release $a$")" -lt "$(line " C command.start $c$")" ] || fail 'command did not follow run release'
  [ ! -e "$TURN_PATH" ] || fail 'successful command left turn'
  command_alone
}
run_case 1 'turn command waits for a run, then starts without lane files or notices' command_after_run

run_after_command() {
  clone A; touch "$TEST_ROOT/command-hold"
  start_command; local c=$pid a
  await_line "$TEST_ROOT/timeline" " C command.start $c$"
  start A FOREIGN_COMMAND="bash $TEST_ROOT/origin/architecture/run.sh turn $work/command"; a=$pid
  await_line "$TEST_ROOT/sleeps" '^A 20$' 3
  [ "$(cat "$TURN_PATH/pid")" = "$c" ] || fail 'run removed command holder'
  [ "$(count "$TEST_ROOT/timeline" ' A sync ')" = 1 ] || fail 'run brought main in before command ended'
  [ ! -e "$TEST_ROOT/A/gate.enter" ] || fail 'run gated before command ended'
  rm "$TEST_ROOT/command-hold"; finish "$c" 0; finish "$a"
  [ "$(line ' C command.end$')" -lt "$(line " A sync $a $a ")" ] || fail 'run sync preceded command end'
  command_alone
}
run_case 1 'turn command holds off a run until the command ends' run_after_command

start_waiter() {
  local id=$1; shift
  case "$id" in
    C) start_command CONTROL_POLL=1 "$@" ;;
    *) start "$id" CONTROL_POLL=1 "$@" ;;
  esac
  await_line "$TEST_ROOT/sleeps" "^$id 20$"
}

next_turn_event() {
  touch "$TEST_ROOT/$1/poll"
  expect_turn_event "$@"
}

expect_turn_event() {
  local id=$1 expected=$2 event
  await_line "$TEST_ROOT/timeline" " $id \(poll 20$\|gate\.1 \|command\.start \)" 2
  event=$(grep " $id \(poll 20$\|gate\.1 \|command\.start \)" "$TEST_ROOT/timeline" | sed -n '2p')
  case "$expected:$event" in
    wait:*" $id poll 20" | take:*" $id gate.1 "* | take:*" $id command.start "*) ;;
    *) fail "$id expected to $expected on its next try, saw $event" ;;
  esac
}

queued_turns() {
  local first=$1 second=$2 order=$3 holder earlier later
  clone A; clone B; clone D
  touch "$TEST_ROOT/gate-hold.A"
  start A; holder=$pid; await "$TEST_ROOT/A/gate.1"
  start_waiter "$first"; earlier=$pid
  clock 1001
  if [ "$order" = slash ]; then start_waiter "$second" RUN_TURN="$TURN_PATH///"
  else start_waiter "$second"; fi
  later=$pid
  if [ "$order" = dead ]; then kill -KILL "$earlier"; finish "$earlier" 137; fi
  case "$order" in reused | zombie) touch "$TEST_ROOT/$order.$earlier" ;; esac
  rm "$TEST_ROOT/gate-hold.A"; finish "$holder"
  [ ! -e "$TURN_PATH" ] || fail 'holder did not free the turn'
  case "$order" in later | slash) next_turn_event "$second" wait ;; esac
  if [[ $order = earlier || $order = later || $order = slash ]]; then
    next_turn_event "$first" take
    if [ "$first" = C ]; then finish "$earlier" 0; else finish "$earlier"; fi
  fi
  if [[ $order = later || $order = slash ]]; then
    touch "$TEST_ROOT/$second/poll"
  else
    next_turn_event "$second" take
  fi
  if [ "$second" = C ]; then finish "$later" 0; else finish "$later"; fi
  case "$order" in reused | zombie) kill -KILL "$earlier"; finish "$earlier" 137 ;; esac
  [ ! -e "$TURN_PATH" ] || fail 'last waiter did not free the turn'
}
run_case 1 'queue: first run wins when it polls first' queued_turns B D earlier
run_case 1 'queue: first run wins when the later run polls first' queued_turns B D later
run_case 1 'queue: a killed waiter lets the next run take a free turn on its first try' queued_turns B D dead
run_case 1 'queue: a reused PID does not keep the earlier place' queued_turns B D reused
run_case 1 'queue: a zombie does not keep the earlier place' queued_turns B D zombie
run_case 1 'queue: a command that arrived first holds its place' queued_turns C B later
run_case 1 'queue: a command that arrived later waits behind a run' queued_turns B C later
run_case 1 'queue: a trailing slash still uses the same queue' queued_turns B C slash

install_gate() {
  local architecture=$1/architecture
  cp "$gate_script" "$architecture/gate.sh"
  printf 'if [ "$1" = --profile ]; then echo .cache/test-profile; fi\n' > "$architecture/space.sh"
}

start_gate_command() {
  local mode=${1:-direct}
  mkdir "$TEST_ROOT/C"
  install_gate "$TEST_ROOT/origin"
  (
    cd "$TEST_ROOT/C"
    export ID=C CONTROL_POLL=1 GATE_LOCK=$TEST_ROOT/gate.lock GATE_FREE=0 PATH="$PWD:$PATH"
    # Use an executable with the same name as a private supervisor function.
    printf '#!%s\n' "$BASH" > say
    cat >> say <<'STUB'
set -eu
owner=$(cat "$TURN_PATH/pid" 2>/dev/null || echo -)
[ "$owner" = "$EXPECTED_OWNER" ] || { echo "expected turn owner $EXPECTED_OWNER, got $owner" >&2; exit 1; }
printf '%s C command.start %s\n' "$(cat "$TEST_ROOT/clock")" "$owner" >> "$TEST_ROOT/timeline"
STUB
    chmod +x say
    set -- "$BASH" "$TEST_ROOT/origin/architecture/gate.sh" say
    case "$mode" in
      recovery)
        EXPECTED_OWNER=$(cat "$TURN_PATH/pid")
        export EXPECTED_OWNER GATE_OWNER=recovery GATE_CLEANUP=1
        echo "$GATE_OWNER" > "$GATE_LOCK.cleanup" ;;
      no-cache)
        touch "$TEST_ROOT/cache-file"
        export RUN_TURN=$TEST_ROOT/cache-file/turn.lock EXPECTED_OWNER=- ;;
    esac
    exec "$BASH" -c '
      case "$1" in recovery | no-cache) shift; exec "$@";; esac
      export EXPECTED_OWNER=$$
      if [ "$1" = nested ]; then
        shift
        exec "$RUN_TEST_BASH" "$TEST_ROOT/origin/architecture/run.sh" turn "$RUN_TEST_BASH" -c '\''
          "$@"
          exit "$?"
        '\'' gate "$@"
      fi
      shift
      exec "$@"
    ' gate "$mode" "$@"
  ) < /dev/null > "$TEST_ROOT/C/out" 2>&1 &
  pid=$!; echo "$pid" >> "$TEST_ROOT/processes"
}

install_full_gate() {
  local checkout=$1
  install_gate "$checkout"
  printf '#!%s\nif [ "$1 $2" = "flake check" ]; then touch checked; fi\n' "$BASH" > "$checkout/nix"
  chmod +x "$checkout/nix"
  cat > "$checkout/gate.sh" <<'STUB'
GATE_LOCK=$TEST_ROOT/gate.lock GATE_AGAIN=1 GATE_FREE=0 \
  GATE_PASSED=$PWD/passed PATH="$PWD:$PATH" bash architecture/gate.sh
STUB
}

gate_command_order() {
  local order=$1 holder earlier command
  clone A; clone B
  install_full_gate "$TEST_ROOT/B"
  touch "$TEST_ROOT/gate-hold.A"
  start A; holder=$pid
  await "$TEST_ROOT/A/gate.1"
  start_waiter B; earlier=$pid
  start_gate_command; command=$pid
  await_line "$TEST_ROOT/sleeps" '^C 20$'
  [ ! -e "$TEST_ROOT/gate.lock" ] || fail 'gate command took machine lock before the turn'
  rm "$TEST_ROOT/gate-hold.A"; finish "$holder"
  if [ "$order" = command-first ]; then next_turn_event C wait; fi
  next_turn_event B take; finish "$earlier"
  [ -e "$TEST_ROOT/B/checked" ] || fail 'full gate did not run under the existing turn'
  touch "$TEST_ROOT/C/poll"; finish "$command" 0
  [ "$(line " B release $earlier$")" -lt "$(line " C command.start $command$")" ] ||
    fail 'gate command started before the earlier run released its turn'
  [ ! -e "$TURN_PATH" ] || fail 'gate command left the turn held'
}
run_case 1 'gate command waits for the earlier run when the run polls first' gate_command_order run-first
run_case 1 'gate command waits for the earlier run when the command polls first' gate_command_order command-first

gate_command_nested() {
  start_gate_command nested; local command=$pid
  finish "$command" 0
  [ "$(count "$TEST_ROOT/timeline" ' C take$')" = 1 ] &&
    [ "$(count "$TEST_ROOT/timeline" " C command.start $command$")" = 1 ] &&
    [ ! -e "$TURN_PATH" ] || fail 'nested gate did not use and release the outer turn'
}
run_case 1 'gate command under run.sh turn uses that turn once' gate_command_nested

gate_command_idle() {
  start_gate_command; local command=$pid
  finish "$command" 0
  [ "$(count "$TEST_ROOT/timeline" " C command.start $command$")" = 1 ] &&
    [ ! -e "$TURN_PATH" ] || fail 'idle gate command did not run or release the turn'
}
run_case 0 'gate command runs and frees the turn without a lane' gate_command_idle

gate_command_no_cache() {
  start_gate_command no-cache
  finish "$pid" 0
  [ "$(count "$TEST_ROOT/timeline" ' C command.start -$')" = 1 ] &&
    [ ! -e "$TURN_PATH" ] || fail 'gate command without a cache did not use the machine lock'
}
run_case 0 'gate command runs when the cache folder cannot be made' gate_command_no_cache

gate_command_recovery() {
  local holder recovery
  clone A; touch "$TEST_ROOT/gate-hold.A"
  start A; holder=$pid
  await "$TEST_ROOT/A/gate.1"
  start_gate_command recovery; recovery=$pid
  finish "$recovery" 0
  [ ! -e "$TEST_ROOT/gate.lock.cleanup" ] &&
    [ "$(cat "$TURN_PATH/pid")" = "$holder" ] || fail 'recovery waited for or released the run turn'
  rm "$TEST_ROOT/gate-hold.A"; finish "$holder"
}
run_case 1 'gate command recovery takes only the machine lock' gate_command_recovery

dependent_card() {
  local id=$1 number=$2 status=$3 stream=$4 body=$5
  local board=$TEST_ROOT/$id/items.json
  jq --arg status "$status" --arg stream "$stream" --arg body "$body" --argjson number "$number" '
    .items += [{content: {number: $number, body: $body}, kind: "Task",
      workstream: $stream, status: $status}]
  ' "$board" > "$board.next"
  mv "$board.next" "$board"
}

priority_turns() {
  local order=$1 holder earlier later winner=D loser=B
  clone A; clone B; clone D
  # The fixture gives each lane a view with one ready card. Only open board
  # cards count, including cards outside the lane and unassigned cards.
  dependent_card B 10 Merged Shared $'## Dependencies\n/issues/1'
  dependent_card D 11 Active Server $'## Dependencies\n/issues/1\n/issues/1'
  if [ "$order" = tie ]; then
    dependent_card B 12 Active DevOps $'## Dependencies\n/issues/1'
    winner=B loser=D
  fi
  touch "$TEST_ROOT/gate-hold.A"
  start A; holder=$pid; await "$TEST_ROOT/A/gate.1"
  start_waiter B; earlier=$pid
  clock 1001
  start_waiter D; later=$pid
  # A poll must use the registered count, even if the next board read
  # would say the card has no dependents.
  if [ "$order" != tie ]; then
    jq '.items |= .[:1]' "$TEST_ROOT/D/items.json" > "$TEST_ROOT/D/items.next"
    mv "$TEST_ROOT/D/items.next" "$TEST_ROOT/D/items.json"
  fi
  rm "$TEST_ROOT/gate-hold.A"; finish "$holder"
  [ ! -e "$TURN_PATH" ] || fail 'holder did not free the turn'
  if [ "$order" != high-first ]; then next_turn_event "$loser" wait; fi
  next_turn_event "$winner" take
  if [ "$winner" = B ]; then finish "$earlier"; else finish "$later"; fi
  touch "$TEST_ROOT/$loser/poll"
  if [ "$loser" = B ]; then finish "$earlier"; else finish "$later"; fi
}
run_case 1 'priority: higher count wins when the lower count polls first' priority_turns low-first
run_case 1 'priority: higher count wins when the higher count polls first' priority_turns high-first
run_case 1 'priority: duplicate links count once and equal counts follow arrival' priority_turns tie

priority_command() {
  local holder command run
  clone A; clone D
  dependent_card D 11 Active DevOps $'## Dependencies\n/issues/1'
  touch "$TEST_ROOT/gate-hold.A"
  start A; holder=$pid; await "$TEST_ROOT/A/gate.1"
  start_waiter C; command=$pid
  clock 1001
  start_waiter D; run=$pid
  rm "$TEST_ROOT/gate-hold.A"; finish "$holder"
  next_turn_event C wait
  next_turn_event D take; finish "$run"
  touch "$TEST_ROOT/C/poll"; finish "$command" 0
  command_alone
}
run_case 1 'priority: a command has zero and yields to a later run with dependents' priority_command

pass_bound() {
  local mode=$1 holder early first last waiter=B exit_code=1
  clone A; clone D; clone E
  jq '.items[0].content.number = 2' "$TEST_ROOT/D/items.json" > "$TEST_ROOT/D/items.next"
  mv "$TEST_ROOT/D/items.next" "$TEST_ROOT/D/items.json"
  dependent_card D 11 Active DevOps $'## Dependencies\n/issues/2'
  dependent_card E 12 Active DevOps $'## Dependencies\n/issues/1'
  dependent_card E 13 Active Server $'## Dependencies\n/issues/1'
  touch "$TEST_ROOT/gate-hold.A" "$TEST_ROOT/gate-hold.D"
  start A; holder=$pid; await "$TEST_ROOT/A/gate.1"
  case "$mode" in
    command) waiter=C; exit_code=0; start_waiter C ;;
    gate) waiter=C; exit_code=0; start_gate_command; await_line "$TEST_ROOT/sleeps" '^C 20$' ;;
    *) clone B; start_waiter B HOLD_RESTART=1 ;;
  esac
  early=$pid
  start_waiter D; first=$pid
  rm "$TEST_ROOT/gate-hold.A"; finish "$holder"
  next_turn_event D take
  start_waiter E; last=$pid
  case "$mode" in
    dead)
      touch "$TEST_ROOT/B/poll"
      await_line "$TEST_ROOT/B/.cache/run.history" 'request for #1 was passed at .* by #2$'
      kill -KILL "$early"; finish "$early" 137 ;;
  esac
  rm "$TEST_ROOT/gate-hold.D"; finish "$first"
  case "$mode" in
    dead) next_turn_event E take; finish "$last"; return ;;
    restart)
      echo '# next supervisor' >> "$TEST_ROOT/origin/architecture/run.sh"
      "$real_git" -C "$TEST_ROOT/origin" commit -qam supervisor
      touch "$TEST_ROOT/B/poll"
      await_line "$TEST_ROOT/timeline" ' B exec -$'
      next_turn_event E wait
      touch "$TEST_ROOT/B/restart"
      expect_turn_event B take ;;
    *)
      if [ "$mode" = later-first ]; then next_turn_event E wait; fi
      next_turn_event "$waiter" take ;;
  esac
  finish "$early" "$exit_code"
  if [ "$waiter" = B ]; then
    [ "$(count "$TEST_ROOT/B/.cache/run.history" 'request for #1 was passed at .* by #2$')" = 1 ] ||
      fail 'lane did not record the pass time and card once'
  fi
  if [[ $mode = later-first || $mode = restart ]]; then touch "$TEST_ROOT/E/poll"
  else next_turn_event E take; fi
  finish "$last"
}
run_case 1 'pass bound: the earlier waiter takes its next turn when it polls first' pass_bound earlier-first
run_case 1 'pass bound: a higher count cannot pass a waiter again when it polls first' pass_bound later-first
run_case 1 'pass bound: a killed passed waiter leaves no place' pass_bound dead
run_case 1 'pass bound: a restarted waiter keeps its pass and history' pass_bound restart
run_case 1 'pass bound: a standalone turn command can be passed once' pass_bound command
run_case 1 'pass bound: a standalone gate command can be passed once' pass_bound gate

passed_arrival_order() {
  local holder earlier later first last
  clone A; clone B; clone D; clone E; clone F
  dependent_card D 11 Active Server $'## Dependencies\n/issues/1'
  dependent_card E 11 Active Server $'## Dependencies\n/issues/1'
  dependent_card E 12 Active DevOps $'## Dependencies\n/issues/1'
  dependent_card F 11 Active Server $'## Dependencies\n/issues/1'
  dependent_card F 12 Active DevOps $'## Dependencies\n/issues/1'
  dependent_card F 13 Active Studio $'## Dependencies\n/issues/1'
  touch "$TEST_ROOT/gate-hold.A" "$TEST_ROOT/gate-hold.E"
  start A; holder=$pid; await "$TEST_ROOT/A/gate.1"
  start_waiter B; earlier=$pid
  start_waiter D; later=$pid
  start_waiter E; first=$pid
  rm "$TEST_ROOT/gate-hold.A"; finish "$holder"
  next_turn_event E take
  start_waiter F; last=$pid
  rm "$TEST_ROOT/gate-hold.E"; finish "$first"
  next_turn_event D wait
  next_turn_event F wait
  next_turn_event B take; finish "$earlier"
  touch "$TEST_ROOT/D/poll"; finish "$later"
  touch "$TEST_ROOT/F/poll"; finish "$last"
  [ "$(line ' B gate.1 ')" -lt "$(line ' D gate.1 ')" ] &&
    [ "$(line ' D gate.1 ')" -lt "$(line ' F gate.1 ')" ] ||
    fail 'passed waiters did not take the turn in arrival order'
}
run_case 1 'pass bound: two passed waiters take turns in arrival order' passed_arrival_order

pass_only_earlier_waiters() {
  local holder first younger last
  clone A; clone B; clone D; clone E
  dependent_card B 11 Active Server $'## Dependencies\n/issues/1'
  dependent_card E 11 Active Server $'## Dependencies\n/issues/1'
  touch "$TEST_ROOT/gate-hold.A" "$TEST_ROOT/gate-hold.B"
  start A; holder=$pid; await "$TEST_ROOT/A/gate.1"
  start_waiter B; first=$pid
  start_waiter D; younger=$pid
  rm "$TEST_ROOT/gate-hold.A"; finish "$holder"
  next_turn_event B take
  start_waiter E; last=$pid
  rm "$TEST_ROOT/gate-hold.B"; finish "$first"
  next_turn_event D wait
  next_turn_event E take; finish "$last"
  touch "$TEST_ROOT/D/poll"; finish "$younger"
}
run_case 1 'pass bound: a winner does not pass a waiter that joined after it' pass_only_earlier_waiters

pass_write_failure() {
  local holder earlier later
  clone A; clone B; clone D
  dependent_card D 11 Active Server $'## Dependencies\n/issues/1'
  touch "$TEST_ROOT/gate-hold.A"
  start A; holder=$pid; await "$TEST_ROOT/A/gate.1"
  start_waiter B; earlier=$pid
  start_waiter D; later=$pid
  rm "$TEST_ROOT/gate-hold.A"; finish "$holder"
  mkdir "$TURN_PATH.wait/passed.next"
  next_turn_event D wait
  [ ! -e "$TURN_PATH" ] || fail 'failed pass write left a claim on the turn'
  rmdir "$TURN_PATH.wait/passed.next"
  next_turn_event B wait
  touch "$TEST_ROOT/D/poll"; finish "$later"
  touch "$TEST_ROOT/B/poll"; finish "$earlier"
  [ "$(line ' D gate.1 ')" -lt "$(line ' B gate.1 ')" ] ||
    fail 'failed pass write changed the queue order'
}
run_case 1 'pass bound: a failed write gives up the turn and leaves the first pass available' pass_write_failure

missing_priority() {
  local holder run higher
  clone A; clone B; clone D
  printf '\n| select($mode != "dependents")\n' >> "$TEST_ROOT/B/architecture/backlog.jq"
  "$real_git" -C "$TEST_ROOT/B" commit -qam 'board output without a count'
  dependent_card D 11 Active DevOps $'## Dependencies\n/issues/1'
  touch "$TEST_ROOT/gate-hold.A"
  start A; holder=$pid; await "$TEST_ROOT/A/gate.1"
  start_waiter B; run=$pid
  [ "$(count "$TEST_ROOT/B/.cache/run.history" 'merge turn for #1: dependency count unavailable; using zero')" = 1 ] \
    || fail 'missing count did not write one fallback line'
  start_waiter D; higher=$pid
  rm "$TEST_ROOT/gate-hold.A"; finish "$holder"
  next_turn_event B wait
  next_turn_event D take; finish "$higher"
  touch "$TEST_ROOT/B/poll"; finish "$run"
}
run_case 1 'priority: a missing count is recorded and does not stop the run' missing_priority

registration_serial() {
  local holder earlier later
  clone A; clone B; clone D
  touch "$TEST_ROOT/gate-hold.A"
  start A; holder=$pid; await "$TEST_ROOT/A/gate.1"
  start B CONTROL_POLL=1 HOLD_REGISTRATION=1; earlier=$pid
  await "$TEST_ROOT/B/registering"
  if ( if [[ $OSTYPE == darwin* ]]; then /usr/bin/lockf -s -t 0 9; else flock -xn 9; fi ) 9<> "$TURN_PATH.wait/lock"; then
    fail 'another registration could enter while the first publishes its ticket'
  fi
  start D CONTROL_POLL=1; later=$pid
  touch "$TEST_ROOT/B/register"
  await_line "$TEST_ROOT/sleeps" '^B 20$'
  await_line "$TEST_ROOT/sleeps" '^D 20$'
  rm "$TEST_ROOT/gate-hold.A"; finish "$holder"
  next_turn_event D wait
  next_turn_event B take; finish "$earlier"
  touch "$TEST_ROOT/D/poll"; finish "$later"
}
run_case 1 'queue: registration holds one lock until its ticket is published' registration_serial

queue_unavailable() {
  local a message='waiting for the merge turn queue to open'
  clone A
  mkdir "$TURN_PATH"; . "$OWN_HELPER"; own_publish $$ "$TURN_PATH/pid"
  touch "$TURN_PATH.wait"
  start A CONTROL_POLL=1; a=$pid
  await_line "$TEST_ROOT/timeline" ' A \(poll 20$\|gate\.1 \)'
  [ ! -e "$TEST_ROOT/A/gate.enter" ] || fail 'run entered a held turn after its queue failed'
  [ "$(cat "$TURN_PATH/pid")" = $$ ] || fail 'queue failure changed the live holder'
  [ "$(cat "$TEST_ROOT/A/.cache/run.status")" = "$message" ] || fail 'status did not say the queue failed'
  grep -q "$message" "$TEST_ROOT/A/.cache/run.history" || fail 'history did not record the queue failure'
  [ "$(count "$TEST_ROOT/notified" "$message")" = 1 ] || fail 'queue failure did not notify once'
  start_command; finish "$pid" 1
  grep -q "$message" "$TEST_ROOT/C/out" || fail 'command did not report the queue failure'
  rm "$TURN_PATH.wait"; rm -rf "$TURN_PATH"
  next_turn_event A take; finish "$a"
}
run_case 1 'queue: a setup failure waits and retries without bypassing the turn' queue_unavailable

restart_queue() {
  local mode=$1 holder earlier later upstream=$TEST_ROOT/origin
  clone A; clone B; clone D
  touch "$TEST_ROOT/gate-hold.A"
  start A; holder=$pid; await "$TEST_ROOT/A/gate.1"
  start_waiter B HOLD_RESTART=1; earlier=$pid
  start_waiter D; later=$pid
  rm "$TEST_ROOT/gate-hold.A"; finish "$holder"
  if [ "$mode" = legacy ]; then
    sed '/^# RUN_TURN_RESUME /d' "$upstream/architecture/run.sh" > "$upstream/run.sh.next"
    mv "$upstream/run.sh.next" "$upstream/architecture/run.sh"
  else echo '# next supervisor' >> "$upstream/architecture/run.sh"; fi
  "$real_git" -C "$upstream" add -f architecture/run.sh
  "$real_git" -C "$upstream" commit -qm supervisor
  if [ "$mode" = race ]; then
    cat > "$TEST_ROOT/B/fetch.sh" <<'STUB'
touch before-fetch
while [ ! -e resume-fetch ]; do "$real_sleep" 0.01; done
STUB
  fi
  # Pause B before the new supervisor starts. D can still poll the turn.
  touch "$TEST_ROOT/B/poll"
  if [ "$mode" = race ]; then
    await "$TEST_ROOT/B/before-fetch"
    touch "$TEST_ROOT/D/hold-mkdir" "$TEST_ROOT/D/poll"
    await "$TEST_ROOT/D/before-mkdir"
    touch "$TEST_ROOT/B/resume-fetch"
  fi
  await_line "$TEST_ROOT/timeline" ' B exec -$'
  if [ "$mode" = lost ]; then
    local board=$TEST_ROOT/B/items.json
    mv "$board" "$board.saved"
    mkfifo "$board"
    printf '(\n' > "$TEST_ROOT/B/.cache/run.state"
    touch "$TEST_ROOT/B/restart"
    await_line "$TEST_ROOT/B/out" 'saved run state could not be read'
    next_turn_event D take; finish "$later"
    "$real_timeout" "$event_limit" "$BASH" -c 'exec 3>"$1"; mv "$1.saved" "$1"; cat "$1" >&3' bash "$board"
    finish "$earlier"
    [ ! -e "$TURN_PATH" ] || fail 'restart left turn locked'
    return
  fi
  case "$mode" in
    legacy) next_turn_event D take; finish "$later" ;;
    compatible) next_turn_event D wait ;;
    race)
      touch "$TEST_ROOT/D/resume-mkdir"
      expect_turn_event D wait
      [ "$(count "$TEST_ROOT/timeline" ' D sync ')" = 1 ] || fail 'later waiter brought main in during the earlier restart'
      ;;
  esac
  touch "$TEST_ROOT/B/restart"; finish "$earlier"
  if [ "$mode" != legacy ]; then touch "$TEST_ROOT/D/poll"; finish "$later"; fi
  [ ! -e "$TURN_PATH" ] || fail 'restart left turn locked'
}
run_case 1 'queue: a compatible restart keeps the earlier place' restart_queue compatible
run_case 1 'queue: a legacy restart drops the earlier place' restart_queue legacy
run_case 1 'queue: a turn attempt across a restart checks the earlier place again' restart_queue race
run_case 1 'queue: an unreadable restart state frees the earlier place' restart_queue lost

priority_restart() {
  local mode=$1 holder earlier later upstream=$TEST_ROOT/origin board=$TEST_ROOT/B/items.json
  clone A; clone B; clone D
  dependent_card B 10 Active Server $'## Dependencies\n/issues/1'
  dependent_card B 11 Active DevOps $'## Dependencies\n/issues/1'
  dependent_card D 12 Active DevOps $'## Dependencies\n/issues/1'
  touch "$TEST_ROOT/gate-hold.A"
  start A; holder=$pid; await "$TEST_ROOT/A/gate.1"
  start_waiter B HOLD_RESTART=1; earlier=$pid
  start_waiter D; later=$pid
  rm "$TEST_ROOT/gate-hold.A"; finish "$holder"
  printf '\n# next supervisor\n' >> "$upstream/architecture/run.sh"
  "$real_git" -C "$upstream" commit -qam supervisor
  touch "$TEST_ROOT/B/poll"
  await_line "$TEST_ROOT/timeline" ' B exec -$'
  if [ "$mode" = same ]; then
    # The new board no longer has the dependents that B registered with.
    jq '.items |= .[:1]' "$board" > "$board.next"
  else
    # The new supervisor finds a different pending gate with zero dependents.
    jq '.items[0].content.number = 2' "$board" > "$board.next"
    printf '\ngate_requests[2]=${gate_requests[1]}\ngate_branches[2]=${gate_branches[1]}\n' >> "$TEST_ROOT/B/.cache/run.state"
  fi
  mv "$board.next" "$board"
  touch "$TEST_ROOT/B/restart"
  if [ "$mode" = same ]; then
    expect_turn_event B take; finish "$earlier"
    next_turn_event D take; finish "$later"
    return
  fi
  expect_turn_event B wait
  next_turn_event D take; finish "$later"
  touch "$TEST_ROOT/B/poll"; finish "$earlier"
  grep -q 'full gate for #2 passed' "$TEST_ROOT/B/.cache/run.history" || fail 'new card did not take the turn'
}
run_case 1 'priority: a different card after restart joins the queue again' priority_restart different
run_case 1 'priority: the same card after restart keeps its registered count' priority_restart same

old_own_ticket() {
  local a
  clone A
  if [ "${1:-}" = priority ]; then dependent_card A 10 Active Server $'## Dependencies\n/issues/1'; fi
  start A OLD_OWN_TICKET=1 CONTROL_POLL=1; a=$pid
  await_line "$TEST_ROOT/timeline" ' A \(poll 20$\|gate\.1 \)'
  [ "$(count "$TEST_ROOT/sleeps" '^A 20$')" = 0 ] || fail 'run waited behind its own old ticket'
  finish "$a"
  [ ! -e "$TURN_PATH.wait/1.$a.unknown.stale" ] || fail 'run left its old ticket behind'
}
run_case 1 'queue: a run does not wait behind its own old ticket' old_own_ticket
run_case 1 'priority: a run removes its own old ticket with lower count' old_own_ticket priority

command_status() {
  local code=$1
  start_command COMMAND_CODE="$code"; local c=$pid
  finish "$c" "$code"
  grep -q " C command.start $c$" "$TEST_ROOT/timeline" || fail 'command did not own turn'
  [ ! -e "$TURN_PATH" ] || fail 'failed command left turn'
  command_alone
}
for code in 0 37; do run_case 1 "turn command returns status $code and releases" command_status "$code"; done

command_wait_signal() {
  local signal=$1 code=$2 handlers=${3:-0} c polls
  mkdir "$TURN_PATH"; . "$OWN_HELPER"; own_publish $$ "$TURN_PATH/pid"
  if [ "$handlers" = 1 ]; then
    # A Bash startup file can install handlers before the supervisor loads.
    echo "trap ':' TERM HUP" > "$TEST_ROOT/handlers"
    start_command BASH_ENV="$TEST_ROOT/handlers"
  else start_command; fi
  c=$pid
  await_line "$TEST_ROOT/sleeps" '^C 20$'
  kill -"$signal" "$c"
  polls=$(count "$TEST_ROOT/sleeps" '^C 20$')
  finish "$c" "$code"
  [ "$(count "$TEST_ROOT/sleeps" '^C 20$')" -le "$((polls + 1))" ] || fail 'signal waited more than one poll'
  [ "$(count "$TEST_ROOT/timeline" ' C command.start ')" = 0 ] || fail 'signalled waiter started command'
  [ -d "$TURN_PATH" ] && [ "$(cat "$TURN_PATH/pid")" = $$ ] || fail 'signalled waiter changed holder'
  rm -rf "$TURN_PATH"
  command_alone
}
for signal in INT TERM HUP QUIT; do
  case "$signal" in TERM) code=143 ;; INT) code=130 ;; HUP) code=129 ;; QUIT) code=131 ;; esac
  run_case 1 "turn command $signal while waiting returns $code and preserves holder" command_wait_signal "$signal" "$code"
done
run_case 1 'turn command TERM replaces caller wait handler' command_wait_signal TERM 143 1
run_case 1 'turn command HUP replaces caller wait handler' command_wait_signal HUP 129 1

command_signal() {
  local signal=$1 c
  touch "$TEST_ROOT/command-hold"
  start_command COMMAND_TRAP="$signal" COMMAND_GROUP=1 COMMAND_CHILD=1; c=$pid
  await "$TEST_ROOT/C/command.child.ready"
  await "$TEST_ROOT/C/command.peer.ready"
  kill -"$signal" "$c"
  await "$TEST_ROOT/C/command.signalled"
  [ -d "$TURN_PATH" ] && [ "$(cat "$TURN_PATH/pid")" = "$c" ] || fail 'signal released turn during cleanup'
  touch "$TEST_ROOT/C/command.release"
  finish "$c" 31
  [ -e "$TEST_ROOT/C/command.cleanup" ] || fail 'command cleanup did not run'
  [ -e "$TEST_ROOT/C/command.child.ended" ] || fail 'command did not stop its separate group'
  [ -e "$TEST_ROOT/C/command.peer.ended" ] || fail 'signal did not reach peer in command group'
  [ ! -e "$TEST_ROOT/C/release.live" ] || fail 'turn released before command ended'
  [ ! -e "$TURN_PATH" ] || fail 'signalled command left turn'
  command_alone
}
for signal in TERM INT HUP QUIT; do
  run_case 1 "turn command $signal runs cleanup before release and returns command status" command_signal "$signal"
done

command_repeat_signals() {
  local c n
  touch "$TEST_ROOT/command-hold"
  start_command COMMAND_TRAP=TERM; c=$pid
  await "$TEST_ROOT/C/command.ready"
  for n in 1 2 3; do
    kill -TERM "$c"
    await_line "$TEST_ROOT/C/command.signalled" "^$n$"
    [ "$(cat "$TURN_PATH/pid")" = "$c" ] || fail 'repeated signal released turn during cleanup'
  done
  touch "$TEST_ROOT/C/command.release"; finish "$c" 31
  [ -e "$TEST_ROOT/C/command.cleanup" ] && [ ! -e "$TURN_PATH" ] || fail 'repeated signal lost cleanup or release'
}
run_case 1 'turn command repeated TERM keeps waiting through cleanup' command_repeat_signals

command_window() {
  local probe_command=$1 probe_signal=$2 c
  export probe_command probe_signal
  mkdir "$TEST_ROOT/C"; touch "$TEST_ROOT/command-hold"
  ( cd "$TEST_ROOT/C"
    exec env --default-signal=INT,TERM,HUP,QUIT ID=C COMMAND_VALUE='the caller' \
      COMMAND_TRAP="$probe_signal" event_limit="$event_limit" RUN_NOW="bash $work/now" \
      probe_ready='[ -z "$pending_signal" ]' "$BASH" -e -c '
        set -T
        trap '\''
          if [ "$BASH_COMMAND" = "$probe_command" ] && [ ! -e injected ]; then
            if [ "$probe_command" = "set -m" ]; then
              [ ! -e command.pid ] || exit 99
            else
              end=$((SECONDS + event_limit))
              while [ ! -e command.ready ]; do
                [ "$SECONDS" -lt "$end" ] || exit 98
                "$real_sleep" 0.01
              done
            fi
            touch injected
            kill -"$probe_signal" "$$"
          fi
          # Let the fixture install its handler before delivering the saved signal.
          if [ "$probe_command" = "set -m" ] && [ "$BASH_COMMAND" = "$probe_ready" ]; then
            end=$((SECONDS + event_limit))
            while [ ! -e command.ready ]; do
              [ "$SECONDS" -lt "$end" ] || exit 98
              "$real_sleep" 0.01
            done
          fi
        '\'' DEBUG
        source "$0" turn "$1" "one argument" "" "*"
      ' "$TEST_ROOT/origin/architecture/run.sh" "$work/command"
  ) > "$TEST_ROOT/C/out" 2>&1 &
  c=$!; echo "$c" >> "$TEST_ROOT/processes"
  await "$TEST_ROOT/C/injected"
  await "$TEST_ROOT/C/command.signalled"
  [ "$(cat "$TURN_PATH/pid")" = "$c" ] || fail 'launch signal released turn during cleanup'
  touch "$TEST_ROOT/C/command.release"; finish "$c" 31
  [ -e "$TEST_ROOT/C/command.cleanup" ] && [ ! -e "$TURN_PATH" ] || fail 'launch signal lost cleanup or turn release'
}
for probe in 'set -m' 'command_pid=$!' 'wait "$command_pid"'; do
  for signal in INT TERM HUP QUIT; do
    run_case 1 "turn command $signal at $probe keeps cleanup locked" command_window "$probe" "$signal"
  done
done

command_signal_status() {
  local signal=$1 code=$2
  start_command COMMAND_EXIT_SIGNAL="$signal"; local c=$pid
  finish "$c" "$code"
  [ ! -e "$TURN_PATH" ] || fail 'signal exit left turn'
}
for signal in INT TERM HUP; do
  case "$signal" in TERM) code=143 ;; INT) code=130 ;; HUP) code=129 ;; esac
  run_case 1 "turn command ended by $signal returns $code" command_signal_status "$signal" "$code"
done

command_ignore() {
  touch "$TEST_ROOT/command-hold"
  start_command COMMAND_IGNORE=1; local c=$pid
  await "$TEST_ROOT/C/command.ready"
  kill -TERM "$c"
  touch "$TEST_ROOT/C/command.check"
  await "$TEST_ROOT/C/command.checked"
  [ -d "$TURN_PATH" ] && [ "$(cat "$TURN_PATH/pid")" = "$c" ] || fail 'ignored TERM released turn'
  rm "$TEST_ROOT/command-hold"; finish "$c" 0
  [ ! -e "$TURN_PATH" ] || fail 'ended command left turn'
}
run_case 1 'turn command that ignores TERM keeps its turn until it ends' command_ignore

command_lookup() {
  local target=$1 want=$2 code=0
  shift 2
  mkdir "$TEST_ROOT/C"
  ( cd "$TEST_ROOT/C"
    PATH="$work/commands" ID=C RUN_NOW="bash $work/now" RUN_NOTIFY="bash $work/notify" \
      "$BASH" "$TEST_ROOT/origin/architecture/run.sh" turn "$target" "$@"
  ) > "$TEST_ROOT/C/out" 2>&1 || code=$?
  [ "$code" = "$want" ] || fail "lookup $target returned $code, wanted $want"
  [ -e "$TEST_ROOT/C/lookup.owner" ] || fail 'lookup bypassed executable env from PATH'
  grep -q " C release $(cat "$TEST_ROOT/C/lookup.owner")$" "$TEST_ROOT/timeline" || fail 'lookup did not release its held turn normally'
  [ ! -e "$TURN_PATH" ] || fail 'lookup failure left turn'
  [ ! -e "$TEST_ROOT/notified" ] || fail 'lookup ran notify function'
  ! grep -q '^run: x$' "$TEST_ROOT/C/out" || fail 'lookup ran say function'
  command_alone
}
for target in notify say; do
  run_case 1 "turn command lookup excludes function $target" command_lookup "$target" 127 x
done
run_case 1 'turn command lookup rejects -v ls with 127' command_lookup -v 127 ls
run_case 1 'turn command lookup rejects exec true with 127' command_lookup exec 127 true
command_missing() { command_lookup "$TEST_ROOT/missing" 127; }
command_permission() { touch "$TEST_ROOT/denied"; command_lookup "$TEST_ROOT/denied" 126; }
run_case 1 'turn command missing executable returns 127' command_missing
run_case 1 'turn command nonexecutable file returns 126' command_permission

command_ended() {
  mkdir "$TEST_ROOT/C"
  # PID reuse cannot be forced safely. Record kill calls instead: EXIT must only
  # release the turn, with no handler that signals a command after wait reaped it.
  ( cd "$TEST_ROOT/C"
    ID=C RUN_NOW="bash $work/now" "$BASH" -c '
      kill() { printf "%s\n" "$*" >> "$TEST_ROOT/C/signals"; builtin kill "$@"; }
      source "$0" turn true
    ' "$TEST_ROOT/origin/architecture/run.sh"
  ) > "$TEST_ROOT/C/out" 2>&1 || fail 'ordinary command failed'
  if [ -e "$TEST_ROOT/C/signals" ]; then
    ! grep -v '^-0 ' "$TEST_ROOT/C/signals" | grep -q . || fail 'ended command group was signalled'
  fi
  [ ! -e "$TURN_PATH" ] || fail 'ended command left turn'
}
run_case 1 'turn command already ended receives no group signal' command_ended

command_completed_signal() {
  mkdir "$TEST_ROOT/C"
  local signal=$1 want=$2 probe=$3 code=0
  ( cd "$TEST_ROOT/C"
    env --default-signal=INT,TERM,HUP,QUIT ID=C probe_signal="$signal" RUN_NOW="bash $work/now" probe_command="$probe" "$BASH" -e -c '
      kill() {
        if [ "$1" = -0 ]; then builtin kill "$@"
        else printf "%s\n" "$*" >> signals; fi
      }
      set -T
      trap '\''
        if [ "$BASH_COMMAND" = "$probe_command" ] && [ ! -e injected ]; then
          touch injected
          builtin kill -"$probe_signal" $$
        fi
      '\'' DEBUG
      source "$0" turn true
    ' "$TEST_ROOT/origin/architecture/run.sh"
  ) > "$TEST_ROOT/C/out" 2>&1 || code=$?
  [ ! -e "$TEST_ROOT/C/signals" ] || fail 'signal after loop reached reaped command group'
  [ "$code" = "$want" ] || fail "signal after loop returned $code, wanted $want"
  [ ! -e "$TURN_PATH" ] || fail 'signal after loop left turn'
}
for probe in 'status=0' 'exit "$status"'; do
  for signal in INT TERM HUP QUIT; do
    case "$signal" in INT) code=130 ;; TERM) code=143 ;; HUP) code=129 ;; QUIT) code=131 ;; esac
    run_case 1 "turn command clears forwarding before $signal at $probe" command_completed_signal "$signal" "$code" "$probe"
  done
done

command_wait_errors() {
  mkdir "$TEST_ROOT/C"; touch "$TEST_ROOT/command-hold"
  ( cd "$TEST_ROOT/C"
    exec env --default-signal=INT,TERM,HUP,QUIT ID=C COMMAND_VALUE='the caller' \
      COMMAND_IGNORE=1 event_limit="$event_limit" RUN_NOW="bash $work/now" "$BASH" -e -c '
        kill() {
          if [ "$1" = -0 ]; then builtin kill "$@"
          else touch forward.failed; return 1; fi
        }
        source "$0" turn "$1" "one argument" "" "*"
      ' "$TEST_ROOT/origin/architecture/run.sh" "$work/command"
  ) > "$TEST_ROOT/C/out" 2>&1 &
  local c=$!; echo "$c" >> "$TEST_ROOT/processes"
  await "$TEST_ROOT/C/command.ready"
  kill -TERM "$c"; await "$TEST_ROOT/C/forward.failed"
  [ "$(cat "$TURN_PATH/pid")" = "$c" ] || fail 'failed forwarding released running command turn'
  rm "$TEST_ROOT/command-hold"; finish "$c" 0
  [ ! -e "$TURN_PATH" ] || fail 'failed forwarding left turn after command ended'
}
run_case 1 'turn command under errexit holds turn after forwarding fails' command_wait_errors

command_wait_probe() {
  mkdir "$TEST_ROOT/C"; touch "$TEST_ROOT/command-hold"
  ( cd "$TEST_ROOT/C"
    exec env ID=C COMMAND_VALUE='the caller' event_limit="$event_limit" RUN_NOW="bash $work/now" \
      probe_command='wait "$command_pid"' "$BASH" -c '
        kill() {
          if [ "$1" = -0 ] && [ "$2" = "${command_pid:-}" ] && [ -e "$TEST_ROOT/command-hold" ]; then
            touch premature.probe
            return 1
          fi
          builtin kill "$@"
        }
        set -T
        trap '\''[ "$BASH_COMMAND" != "$probe_command" ] || touch waiting'\'' DEBUG
        source "$0" turn "$1" "one argument" "" "*"
      ' "$TEST_ROOT/origin/architecture/run.sh" "$work/command"
  ) > "$TEST_ROOT/C/out" 2>&1 &
  local c=$!; echo "$c" >> "$TEST_ROOT/processes"
  await "$TEST_ROOT/C/command.ready"; await "$TEST_ROOT/C/waiting"
  rm "$TEST_ROOT/command-hold"; finish "$c" 0
  [ ! -e "$TEST_ROOT/C/premature.probe" ] || fail 'command lifetime was polled before wait'
  [ ! -e "$TURN_PATH" ] || fail 'wait probe left turn'
}
run_case 1 'turn command waits before probing command lifetime' command_wait_probe

command_input() {
  mkdir "$TEST_ROOT/C"
  ( cd "$TEST_ROOT/C"
    printf 'hello\n' | ID=C RUN_NOW="bash $work/now" \
      "$BASH" "$TEST_ROOT/origin/architecture/run.sh" turn "$BASH" -ec '
        read -r value
        [ "$value" = hello ]
        touch input.read
      '
  ) > "$TEST_ROOT/C/out" 2>&1 || fail 'batch command lost caller input'
  [ -e "$TEST_ROOT/C/input.read" ] && [ ! -e "$TURN_PATH" ] || fail 'batch input or turn release missing'
}
run_case 1 'turn command batch reads caller input' command_input

command_terminal() {
  local script_tool terminal_command stream c code
  script_tool=$(command -v script || true)
  if [ -z "$script_tool" ]; then
    echo "turn-test: unavailable: $name: script is not on PATH"
    unavailable=$((unavailable + 1)); return
  fi
  code=0
  if [ "$(uname -s)" = Darwin ]; then
    "$real_timeout" "$event_limit" "$script_tool" -q /dev/null true > "$TEST_ROOT/pty.log" 2>&1 < /dev/null || code=$?
  else
    "$real_timeout" "$event_limit" "$script_tool" -qec true /dev/null > "$TEST_ROOT/pty.log" 2>&1 < /dev/null || code=$?
  fi
  if [ "$code" != 0 ]; then
    echo "turn-test: unavailable: $name: script cannot allocate a terminal: $(cat "$TEST_ROOT/pty.log")"
    unavailable=$((unavailable + 1)); return
  fi
  mkdir "$TEST_ROOT/C"
  for stream in 0 1 2; do
    printf -v terminal_command '%q ' "$BASH" "$TEST_ROOT/origin/architecture/run.sh" turn "$BASH" -c 'touch "$TEST_ROOT/C/started"'
    case "$stream" in
      0) terminal_command+=' > "$TEST_ROOT/C/refusal" 2>&1' ;;
      1) terminal_command+=' < /dev/null 2> "$TEST_ROOT/C/refusal"' ;;
      2) terminal_command+=' < /dev/null > "$TEST_ROOT/C/refusal"' ;;
    esac
    terminal_command+='; printf "%s\n" "$?" > "$TEST_ROOT/C/terminal.status"'
    if [ "$(uname -s)" = Darwin ]; then
      "$real_timeout" "$event_limit" "$script_tool" -q /dev/null "$BASH" -c "$terminal_command" \
        < /dev/null > "$TEST_ROOT/C/terminal.log" 2>&1 &
    else
      "$real_timeout" "$event_limit" "$script_tool" -qec "$terminal_command" /dev/null \
        < /dev/null > "$TEST_ROOT/C/terminal.log" 2>&1 &
    fi
    c=$!; echo "$c" >> "$TEST_ROOT/processes"; . "$OWN_HELPER"; own_record "$c" group >/dev/null
    finish "$c" 0
    own_forget "$c"
    [ "$(cat "$TEST_ROOT/C/terminal.status")" = 2 ] || fail "terminal stream $stream was not refused with 2"
    cat "$TEST_ROOT/C/refusal" "$TEST_ROOT/C/terminal.log" | tr -d '\r' > "$TEST_ROOT/C/combined"
    [ "$(grep -c 'turn refuses a terminal; redirect all three streams:' "$TEST_ROOT/C/combined")" = 1 ] || fail "terminal stream $stream missing one-line redirection advice"
    [ ! -e "$TEST_ROOT/C/started" ] || fail "terminal stream $stream started command"
    [ "$(count "$TEST_ROOT/timeline" ' take$')" = 0 ] && [ ! -e "$TURN_PATH" ] || fail "terminal stream $stream took turn"
  done
}
run_case 1 'turn command refuses a terminal on each stream before taking turn' command_terminal

command_assignment() {
  local target=$1 code=0
  shift
  mkdir -p "$TEST_ROOT/C/x=y"
  printf '#!%s\ntouch "$TEST_ROOT/C/started"\n' "$BASH" > "$TEST_ROOT/C/x=y/tool"
  chmod +x "$TEST_ROOT/C/x=y/tool"
  ( cd "$TEST_ROOT/C"
    ID=C TURN_SECRET_SENTINEL=private-value "$BASH" "$TEST_ROOT/origin/architecture/run.sh" turn "$target" "$@"
  ) > "$TEST_ROOT/C/out" 2>&1 || code=$?
  [ "$code" = 2 ] || fail "assignment word $target returned $code, wanted 2"
  [ "$(cat "$TEST_ROOT/C/out")" = "run: turn refuses command word containing =: $target" ] || fail 'assignment refusal must name only the word on one line'
  [ ! -e "$TEST_ROOT/C/started" ] || fail 'assignment word started command'
  [ "$(count "$TEST_ROOT/timeline" ' take$')" = 0 ] && [ ! -e "$TURN_PATH" ] || fail 'assignment word took turn'
}
run_case 1 'turn command refuses A=b without exposing environment' command_assignment A=b
run_case 1 'turn command refuses A=b true without starting command' command_assignment A=b true
run_case 1 'turn command refuses path ./x=y/tool without starting command' command_assignment ./x=y/tool

# CPU time of the supervisor alone, never the command or its children.
command_cpu() {
  local stat value
  if [ -r "/proc/$1/stat" ]; then
    stat=$(cat "/proc/$1/stat")
    # Remove pid and comm (which can contain spaces). Linux reports USER_HZ ticks.
    value=$(awk '{ print $12 + $13 }' <<< "${stat##*) }")
    printf '%s ticks\n' "$value"
  elif [ -n "$real_ps" ]; then
    value=$("$real_ps" -p "$1" -o cputime= 2>/dev/null) || value=$("$real_ps" -p "$1" -o time= 2>/dev/null) || return 1
    [ -n "$value" ] || return 1
    awk 'BEGIN { FS="[-:]" } { days=0; hours=0; if (NF==4) days=$1; if (NF>=3) hours=$(NF-2); print 1000 * (days*86400 + hours*3600 + $(NF-1)*60 + $NF), "ms" }' <<< "$value"
  else return 1; fi
}
command_stopped() {
  local c child before after units limit=200 cpu_available=1 waits
  touch "$TEST_ROOT/command-hold"
  mkdir "$TEST_ROOT/C"
  ( cd "$TEST_ROOT/C"
    exec env ID=C COMMAND_VALUE='the caller' COMMAND_CODE=37 event_limit="$event_limit" RUN_NOW="bash $work/now" \
      "$BASH" -c '
        wait() { echo wait >> "$TEST_ROOT/C/waits"; builtin wait "$@"; }
        source "$0" turn "$1" "one argument" "" "*"
      ' "$TEST_ROOT/origin/architecture/run.sh" "$work/command"
  ) > "$TEST_ROOT/C/out" 2>&1 &
  c=$!; echo "$c" >> "$TEST_ROOT/processes"
  await "$TEST_ROOT/C/command.ready"
  child=$(cat "$TEST_ROOT/C/command.pid")
  own_signal STOP "-$child"
  "$real_sleep" 0.1
  waits=$(count "$TEST_ROOT/C/waits" '^wait$')
  before=$(command_cpu "$c") || cpu_available=0
  "$real_sleep" 1.2
  after=$(command_cpu "$c") || cpu_available=0
  waits=$(( $(count "$TEST_ROOT/C/waits" '^wait$') - waits ))
  [ -d "$TURN_PATH" ] && [ "$(cat "$TURN_PATH/pid")" = "$c" ] || fail 'stopped command lost turn'
  # Remove the hold while stopped: after CONT the command ends by itself.
  rm "$TEST_ROOT/command-hold"
  own_signal CONT "-$child"
  finish "$c" 37
  [ ! -e "$TURN_PATH" ] || fail 'continued command left turn'
  [ "$waits" -le 2 ] || fail 'stopped command repeated wait instead of sleeping'
  if [ "$cpu_available" = 0 ]; then
    echo "turn-test: unavailable: $name: no readable /proc/$c/stat or ps CPU time (ps may be denied)"
    unavailable=$((unavailable + 1)); return
  fi
  units=${before#* }
  [ "$units" != ticks ] || limit=20
  before=${before%% *}; after=${after%% *}
  echo "turn-test: CPU: stopped supervisor before=$before after=$after delta=$((after-before)) $units; bound=$limit over 1.2 s (sleep near 0; spin near 120 ticks or 1200 ms)"
  [ "$((after-before))" -le "$limit" ] || fail 'stopped command made supervisor spin'
}
run_case 1 'turn command stopped group keeps turn and supervisor sleeps' command_stopped

command_unavailable() {
  local kind=$1 c
  if [ "$kind" = timeout ]; then
    start_command PATH="$work/no-timeout"
  else
    touch "$TEST_ROOT/parent"
    RUN_TURN="$TEST_ROOT/parent/turn.lock" start_command
  fi
  c=$pid
  finish "$c" 1
  [ "$(count "$TEST_ROOT/timeline" ' C command.start ')" = 0 ] || fail 'unavailable turn ran command'
  if [ "$kind" = timeout ]; then
    grep -q 'timeout is not on PATH' "$TEST_ROOT/C/out" || fail 'missing timeout reason'
  else
    grep -q 'the folder of the merge turn cannot be made' "$TEST_ROOT/C/out" || fail 'missing folder reason'
  fi
  [ ! -e "$TURN_PATH" ] || fail 'unavailable command took turn'
  command_alone
}
run_case 0 'turn command refuses without timeout' command_unavailable timeout
run_case 1 'turn command refuses when the turn folder cannot be made' command_unavailable folder

command_usage() {
  mkdir "$TEST_ROOT/C"
  local code=0
  ( cd "$TEST_ROOT/C"; ID=C "$BASH" "$TEST_ROOT/origin/architecture/run.sh" turn ) > "$TEST_ROOT/C/out" 2>&1 || code=$?
  [ "$code" = 2 ] || fail "missing command returned $code, wanted 2"
  grep -qi 'usage:.*turn.*command' "$TEST_ROOT/C/out" || fail 'missing command usage'
  [ "$(count "$TEST_ROOT/timeline" ' C take$')" = 0 ] || fail 'missing command took turn'
  [ ! -e "$TURN_PATH" ] || fail 'missing command left turn'
  command_alone
}
run_case 0 'turn command needs a command before taking the turn' command_usage

command_identity() {
  local command_line=$1 mode=$2 c want=37 directory script command_path=$PATH
  mkdir "$TEST_ROOT/C"
  if [ "$mode" = seam ]; then
    mkdir "$TEST_ROOT/C/bin"
    printf '#!%s\necho "bash run.sh turn command"\n' "$BASH" > "$TEST_ROOT/C/bin/ps"
    chmod +x "$TEST_ROOT/C/bin/ps"
    command_path=$TEST_ROOT/C/bin:$PATH
  fi
  directory=$TEST_ROOT/C; script=$TEST_ROOT/origin/architecture/run.sh
  case "$mode" in
    reject) want=2 ;;
    inside) want=2; directory=$TEST_ROOT/origin/architecture; script=run.sh ;;
  esac
  ( cd "$directory"
    exec env ID=C SELF_COMMAND="$command_line" SELF_PS_MODE="$mode" PATH="$command_path" \
      "$BASH" -c '. "$OWN_HELPER"; own_publish $$ "$TEST_ROOT/self.pid"; exec "$@"' _ \
      "$BASH" "$script" turn "$BASH" -c 'touch "$TEST_ROOT/C/started"; exit 37'
  ) < /dev/null > "$TEST_ROOT/C/out" 2>&1 &
  c=$!; echo "$c" >> "$TEST_ROOT/processes"
  finish "$c" "$want"
  [ ! -e "$TURN_PATH" ] || fail 'identity check left turn folder'
  if [ "$want" = 2 ]; then
    [ "$(cat "$TEST_ROOT/C/out")" = 'run: turn requires bash [<path>/]architecture/run.sh turn <command> ...; another form would not keep the turn' ] || fail 'identity refusal must explain accepted form on one line'
    [ ! -e "$TEST_ROOT/C/started" ] || fail 'refused identity started command'
    [ "$(count "$TEST_ROOT/timeline" ' C take$')" = 0 ] || fail 'refused identity took turn'
  else
    [ -e "$TEST_ROOT/C/started" ] || fail 'accepted or unreadable identity did not run command'
    [ "$(count "$TEST_ROOT/timeline" ' C take$')" = 1 ] || fail 'accepted or unreadable identity did not take turn'
  fi
  if [ "${RUN_TURN_PS}" != "$real_ps" ]; then
    grep -q " C ps -ww -p $c -o args=$" "$TEST_ROOT/timeline" || fail 'identity read did not use supervisor PID and holder lookup arguments'
  fi
}
run_case 1 'turn command refuses bash run.sh from architecture' command_identity 'bash run.sh turn command' inside
run_case 1 'turn command refuses bash -x architecture/run.sh' command_identity 'bash -x architecture/run.sh turn command' reject
run_case 1 'turn command refuses bash -e architecture/run.sh' command_identity 'bash -e architecture/run.sh turn command' reject
run_case 1 'turn command refuses sh architecture/run.sh' command_identity 'sh architecture/run.sh turn command' reject
run_case 1 'turn command accepts a failed identity read with partial output' command_identity ignored fail
run_case 1 'turn command accepts an empty identity read' command_identity ignored empty
run_case 1 'turn command reads identity through RUN_TURN_PS instead of PATH ps' command_identity 'bash architecture/run.sh turn command' seam
for command_line in 'bash architecture/run.sh turn command' '/bin/bash architecture/run.sh turn command' \
  'bash /work/architecture/run.sh turn command' '/bin/bash /work/architecture/run.sh turn command'; do
  run_case 1 "turn command accepts identity $command_line" command_identity "$command_line" accept
done

command_real_ps() {
  local command_line=''
  if [ -n "$real_ps" ]; then command_line=$("$real_ps" -ww -p $$ -o args= 2>/dev/null) || command_line=''; fi
  if [ -z "$command_line" ]; then
    echo "turn-test: unavailable: $name: real ps is missing, denied, or gives no command line"
    unavailable=$((unavailable + 1)); return 1
  fi
}
command_identity_real() {
  command_real_ps || return 0
  RUN_TURN_PS="$real_ps" command_identity ignored inside
}
run_case 1 'turn command real ps refuses launch from architecture' command_identity_real

command_pair() {
  local reader=$1 mode=${2:-} c d
  touch "$TEST_ROOT/command-hold"
  start_command RUN_TURN_PS="$reader" PS_MODE="$mode"; c=$pid
  await "$TEST_ROOT/C/command.ready"
  mkdir "$TEST_ROOT/D"
  ( cd "$TEST_ROOT/D"
    exec env ID=D RUN_TURN_PS="$reader" PS_MODE="$mode" RUN_TURN_POLL=.01 \
      "$BASH" "$TEST_ROOT/origin/architecture/run.sh" turn "$BASH" -c 'touch "$TEST_ROOT/D/started"'
  ) < /dev/null > "$TEST_ROOT/D/out" 2>&1 &
  d=$!; echo "$d" >> "$TEST_ROOT/processes"
  await_line "$TEST_ROOT/sleeps" '^D .01$' 3
  [ "$(cat "$TURN_PATH/pid")" = "$c" ] || fail 'second command removed accepted live holder'
  [ ! -e "$TEST_ROOT/D/started" ] || fail 'second command overlapped accepted live holder'
  rm "$TEST_ROOT/command-hold"
  finish "$c" 0; finish "$d" 0
  [ -e "$TEST_ROOT/D/started" ] && [ ! -e "$TURN_PATH" ] || fail 'command pair lost second command or release'
}
run_case 1 'turn command second taker preserves accepted live holder' command_pair "$BASH $work/ps"
run_case 1 'turn command second taker keeps holder after failed ps with partial output' command_pair "$BASH $work/ps" errorline
run_case 1 'turn command second taker keeps holder after empty ps' command_pair "$BASH $work/ps" empty
command_pair_real() {
  command_real_ps || return 0
  command_pair "$real_ps"
}
run_case 1 'turn command real ps second taker preserves accepted live holder' command_pair_real

scan_locale() {
  # The supervisor check applies even when the source has a suppression comment.
  sed 's/ast-grep-ignore/ast-grep-check/g' |
    ast-grep scan --rule architecture/locale.yml --stdin --min-severity error "$@"
}
locale_assignments() {
  local findings status=0 rejected=$TEST_ROOT/locale-fail
  local -a supervisor_scan=(scan_locale --off=temporary-locale --report-style short)
  "${supervisor_scan[@]}" < "$supervisor" || fail 'supervisor locale assignment check'

  "${supervisor_scan[@]}" <<'PASS' || fail 'passing locale fixture was rejected'
# The old form was $(LC_ALL=C printf ok).
printf '%s\n' '$(LC_ALL=C printf ok)'
LC_ALL=C :
result=$(env LC_ALL=C printf ok)
result=$(printf ok; LC_ALL=C)
result=$(true && LC_ALL=C | :)
result=$(A="two words" LC_ALL=C)
result=$(A="two words" B=1; LC_ALL=C)
result=$(A=1 && LC_ALL=C :)
result=$(A=1 ; LC_ALL=C :)
PASS

  cat > "$rejected" <<'FAIL'
LC_ALL=C :
result=$(LC_ALL=POSIX :)
result=$(A=1 LC_CTYPE=C)
result=$(LC_COLLATE=C && :) # ast-grep-ignore
result=$(LC_MESSAGES=C >/dev/null)
result=$(LANG=C | : && : >/dev/null)
result=`LC_TIME=C :`
LANGUAGE=C :
result=$(A=1; LC_NUMERIC=C :)
result=$(A=1 B=2; LC_PAPER=C)
result=$(
  A=1
  LC_MONETARY=C | : && : >/dev/null
)
result=$(A=1&& LC_ADDRESS=C :)
result=$(A=1| LC_TELEPHONE=C :)
FAIL
  "${supervisor_scan[@]}" < "$rejected" > "$TEST_ROOT/locale.log" 2>&1 || status=$?
  [ "$status" -eq 1 ] || fail "expected supervisor locale errors, got exit status $status: $(cat "$TEST_ROOT/locale.log")"
  status=0
  findings=$(scan_locale --json=stream < "$rejected" 2> "$TEST_ROOT/locale.log") || status=$?
  [ "$status" -eq 1 ] || fail "expected locale errors, got exit status $status: $(cat "$TEST_ROOT/locale.log")"
  jq -se '
    (map(select(.ruleId == "temporary-locale").text) | sort) ==
      ["LANGUAGE=C", "LC_ADDRESS=C", "LC_ALL=C", "LC_ALL=POSIX", "LC_NUMERIC=C",
       "LC_TELEPHONE=C", "LC_TIME=C"] and
    (map(select(.ruleId == "locale-substitution") | .range.start.line + 1) | sort) ==
      [2, 3, 4, 5, 6, 7, 9, 10, 11, 15, 16]
  ' <<< "$findings" >/dev/null || fail "unexpected locale diagnostics: $findings"
}
run_case 0 'command substitutions use env for locale settings' locale_assignments
state() {
  mkdir -p "$TEST_ROOT/A/.cache"
  local head
  head=$("$real_git" -C "$TEST_ROOT/A" rev-parse HEAD)
  echo "gate_requests[1]=$head" > "$TEST_ROOT/A/.cache/saved"
  echo 'gate_branches[1]=A' >> "$TEST_ROOT/A/.cache/saved"
  case "$1" in
    passed) echo "gate_results[1]='$head The run ran the full gate on commit $head: it passed.'" >> "$TEST_ROOT/A/.cache/saved" ;;
    failed) echo "gate_results[1]='$head The run ran the full gate on commit $head: it failed. Read the end of .cache/gate.log first.'" >> "$TEST_ROOT/A/.cache/saved" ;;
  esac
}

sequence() {
  local baseline=${1:-0} a b id owner
  clone A; clone B
  touch "$TEST_ROOT/gate-hold.A" "$TEST_ROOT/land-hold.A.fetch.3"
  if [ "$baseline" = 1 ]; then start A PATH="$work/no-timeout"; else start A; fi
  a=$pid; await "$TEST_ROOT/A/gate.1"
  if [ "$baseline" = 1 ]; then touch "$TEST_ROOT/gate-hold.B"; start B PATH="$work/no-timeout"; else start B; fi
  b=$pid; await "$TEST_ROOT/B/requested"
  if [ "$baseline" = 0 ]; then
    await_line "$TEST_ROOT/sleeps" '^B 20$' 2
    [ "$(cat "$TEST_ROOT/B/.cache/run.status")" = "waiting for the merge turn of process $a" ] || fail 'status lost the holder'
    [ "$(count "$TEST_ROOT/timeline" ' B sync ')" = 1 ] || fail 'B brought main in while waiting'
    [ ! -e "$TEST_ROOT/B/gate.enter" ] || fail 'B started its gate while A gated'
  else
    await "$TEST_ROOT/B/gate.enter"
    [ "$(count "$TEST_ROOT/timeline" ' B sync - ')" = 2 ] || fail 'base must sync before its gate waits'
  fi
  clock 1061; rm "$TEST_ROOT/gate-hold.A"
  await_line "$TEST_ROOT/timeline" ' A land.fetch.3$'
  if [ "$baseline" = 0 ]; then
    await_line "$TEST_ROOT/sleeps" '^B 20$' 4
    [ ! -e "$TEST_ROOT/B/gate.enter" ] || fail 'B gated while A was still landing'
    [ ! -e "$TEST_ROOT/B/session.2" ] || fail 'B started a lead while waiting'
    [ "$(count "$TEST_ROOT/timeline" ' B sync ')" = 1 ] || fail 'B synced before land_step returned'
  else await "$TEST_ROOT/B/gate.1"; fi
  rm "$TEST_ROOT/land-hold.A.fetch.3"
  [ "$baseline" = 0 ] || rm "$TEST_ROOT/gate-hold.B"
  finish "$a"; finish "$b"
  [ ! -e "$TURN_PATH" ] || fail 'turn left after landing'
  for id in A B; do
    local expected=1
    if [ "$id" = B ] && [ "$baseline" = 1 ]; then expected=2; fi
    [ "$(cat "$TEST_ROOT/$id/gates")" = "$expected" ] || fail "$id got the wrong gate count"
    [ "$(count "$TEST_ROOT/timeline" " $id land.merge ")" = 1 ] || fail "$id did not land once"
    owner=$(cat "$TEST_ROOT/$id/supervisor.pid")
    ! grep -q " $id session.2 $owner$" "$TEST_ROOT/timeline" || fail "$id held the turn in its next session"
  done
  if [ "$baseline" = 0 ]; then
    [ "$(line ' A gate.1 ')" -lt "$(line ' A land.fetch.3.end$')" ] || fail 'A did not gate before landing'
    [ "$(line ' A land.fetch.3.end$')" -lt "$(line " A release $a$")" ] || fail 'A released before landing returned'
    [ "$(line " A release $a$")" -lt "$(line " B sync $b $b ")" ] || fail 'B synced before release'
    [ "$(line " B sync $b $b ")" -lt "$(line ' B gate.1 ')" ] || fail 'B gated before sync'
    for id in A B; do
      owner=$(cat "$TEST_ROOT/$id/supervisor.pid")
      [ "$(count "$TEST_ROOT/timeline" " $id take$")" = 1 ] || fail "$id did not take exactly one turn"
      [ "$(count "$TEST_ROOT/timeline" " $id timeout --signal=TERM --kill-after=10 300 git fetch -q origin main$")" = 1 ] || fail "$id did not record exactly one sync fetch with TERM, KILL 10 and limit 300"
      [ "$(count "$TEST_ROOT/timeline" " $id sync $owner $owner ")" = 1 ] || fail "$id did not sync exactly once while holding its turn"
      [ "$(count "$TEST_ROOT/timeline" " $id checks $owner$")" = 0 ] || fail "$id read Checks before releasing its turn"
      grep -q " $id gate.1 $owner$" "$TEST_ROOT/timeline" || fail "$id gated without its turn"
      [ "$(count "$TEST_ROOT/timeline" " $id land.fetch $owner$")" = 3 ] || fail "$id missed a landing fetch"
      [ "$(count "$TEST_ROOT/timeline" " $id timeout --signal=TERM --kill-after=10 300 bash .*land ")" = 9 ] || fail "$id missed a bounded landing call"
      for action in push list create merge view; do
        grep -q " $id land.$action $owner$" "$TEST_ROOT/timeline" || fail "$id landed $action without its turn"
      done
    done
    "$real_git" -C "$TEST_ROOT/B" merge-base --is-ancestor "$(cat "$TEST_ROOT/merged.A")" "$(cat "$TEST_ROOT/B/gated")" || fail 'B gated without A merge'
    grep -q 'waited 1 minutes for the merge turn$' "$TEST_ROOT/B/.cache/run.history" || fail 'missing minute wait history'
    [ "$(count "$TEST_ROOT/notified" 'merge turn')" = 0 ] || fail 'short wait notified Joel'
  else
    [ "$(line ' B sync - .* none$')" -lt "$(line ' A land.fetch.3.end$')" ] || fail 'base unexpectedly serialized sync'
    if "$real_git" -C "$TEST_ROOT/B" merge-base --is-ancestor "$(cat "$TEST_ROOT/merged.A")" "$(head -n 1 "$TEST_ROOT/B/gated")"; then fail 'base unexpectedly gated A merge'; fi
    [ "$(count "$TEST_ROOT/timeline" ' take$')" = 0 ] || fail 'missing timeout still took a turn'
    for id in A B; do
      [ "$(head -n 1 "$TEST_ROOT/$id/out")" = 'run: timeout is not on PATH; taking no merge turn' ] || fail 'notice missing at startup'
      [ "$(count "$TEST_ROOT/$id/out" 'timeout is not on PATH')" = 1 ] || fail 'startup notice repeated'
    done
  fi
}
run_case 1 'two runs gate and land once; B syncs A merge only after A releases' sequence
run_case 0 'without timeout two runs retain the base sequence and repeats B gate after main advances' sequence 1

main_advance() {
  local stage=$1 a b outside agate bsync brelease async
  clone A; clone B
  touch "$TEST_ROOT/gate-hold.A"
  cat > "$TEST_ROOT/A/checks.sh" <<'STUB'
if [ "$count" = 3 ]; then
  printf '%s A retry.checks %s\n' "$(cat "$TEST_ROOT/clock")" "$owner" >> "$TEST_ROOT/timeline"
  # Give the waiting run this turn. This wait is outside the turn.
  while [ ! -f "$TEST_ROOT/merged.B" ]; do "$real_sleep" 0.01; done
fi
STUB
  if [ "$stage" = before-merge ]; then touch "$TEST_ROOT/land-hold.A.view.1"; fi
  start A; a=$pid; await "$TEST_ROOT/A/gate.1"
  start B; b=$pid; await "$TEST_ROOT/B/requested"
  await_line "$TEST_ROOT/sleeps" '^B 20$' 2
  if [ "$stage" = before-merge ]; then
    rm "$TEST_ROOT/gate-hold.A"
    await_line "$TEST_ROOT/timeline" ' A land.view.1$'
  fi
  echo outside > "$TEST_ROOT/origin/outside-main"
  "$real_git" -C "$TEST_ROOT/origin" add -f outside-main
  "$real_git" -C "$TEST_ROOT/origin" commit -qm outside
  outside=$("$real_git" -C "$TEST_ROOT/origin" rev-parse HEAD)
  printf '%s test outside %s\n' "$(cat "$TEST_ROOT/clock")" "$outside" >> "$TEST_ROOT/timeline"
  if [ "$stage" = before-merge ]; then rm "$TEST_ROOT/land-hold.A.view.1"
  else rm "$TEST_ROOT/gate-hold.A"; fi
  await_line "$TEST_ROOT/timeline" ' A retry.checks '
  ! grep -q " A retry.checks $a$" "$TEST_ROOT/timeline" || fail 'main advance kept the turn before retry'
  [ "$(count "$TEST_ROOT/timeline" ' A session.')" = 1 ] || fail 'main advance started a lead'
  finish "$b"; finish "$a"
  [ "$(cat "$TEST_ROOT/A/gates")" = 2 ] || fail 'A did not gate exactly once more'
  [ "$(cat "$TEST_ROOT/B/gates")" = 1 ] || fail 'B repeated its gate'
  [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 2 ] || fail 'A did not retake for its unanswered request'
  [ "$(count "$TEST_ROOT/timeline" ' B take$')" = 1 ] || fail 'B did not take one turn'
  [ "$(count "$TEST_ROOT/timeline" ' A land.merge ')" = 1 ] || fail 'A landed before its second gate'
  [ "$(count "$TEST_ROOT/timeline" ' B land.merge ')" = 1 ] || fail 'B did not land once'
  [ "$(count "$TEST_ROOT/A/.cache/run.history" 'main advanced during the gate for #1; the gate runs again$')" = 1 ] || fail 'main advance was not one gate wait'
  [ "$(cat "$TEST_ROOT/A/calls")" = 2 ] || fail 'a lead ran between A gates'
  agate=$(line ' A gate.1 '); bsync=$(line " B sync $b $b ")
  brelease=$(line " B release $b$")
  async=$(grep -n " A sync $a $a " "$TEST_ROOT/timeline" | tail -n 1 | cut -d: -f1)
  [ "$agate" -lt "$(line ' test outside ')" ] || fail 'outside merge preceded A gate'
  [ "$(line ' test outside ')" -lt "$(line " A release $a$")" ] || fail 'A released before observing outside main'
  [ "$(line " A release $a$")" -lt "$bsync" ] || fail 'B synced before A gave back the turn'
  [ "$bsync" -lt "$(line ' B gate.1 ')" ] || fail 'B gated before sync'
  [ "$(line ' B land.fetch.3.end$')" -lt "$brelease" ] || fail 'B released before landing returned'
  [ "$brelease" -lt "$async" ] || fail 'A synced before B gave back the turn'
  [ "$async" -lt "$(line ' A gate.2 ')" ] || fail 'A regated before sync'
  [ "$(line ' A gate.2 ')" -lt "$(line ' A land.merge ')" ] || fail 'A merged before its regate'
  "$real_git" -C "$TEST_ROOT/B" merge-base --is-ancestor "$outside" "$(cat "$TEST_ROOT/B/gated")" || fail 'B missed outside main'
  "$real_git" -C "$TEST_ROOT/A" merge-base --is-ancestor "$outside" "$(tail -n 1 "$TEST_ROOT/A/gated")" || fail 'A missed outside main'
  "$real_git" -C "$TEST_ROOT/A" merge-base --is-ancestor "$(cat "$TEST_ROOT/merged.B")" "$(tail -n 1 "$TEST_ROOT/A/gated")" || fail 'A regated without B merge'
  ! grep -q " A session.2 $a$" "$TEST_ROOT/timeline" || fail 'A held the turn in its next lead'
  [ ! -e "$TURN_PATH" ] || fail 'main advance left a turn'
}
run_case 1 'outside main advances during A gate: release, B lands, A retakes and gates both merges' main_advance during-gate
run_case 1 'outside main advances before A merge: release, B lands, A retakes and gates both merges' main_advance before-merge

seventh_advance() {
  clone A; state pending
  local head a
  head=$("$real_git" -C "$TEST_ROOT/A" rev-parse HEAD)
  printf 'saved_counts[1]="6 0 0"\nlast_passed_requests[1]=%s\n' "$head" >> "$TEST_ROOT/A/.cache/saved"
  printf 'Reviewed-head: %s\nA test step\n\nThe test text.\n' "$head" > "$TEST_ROOT/A/.cache/pr.md"
  cat > "$TEST_ROOT/A/gate.sh" <<'STUB'
echo outside > origin/outside-main
"$real_git" -C origin add -f outside-main
"$real_git" -C origin commit -qm outside
STUB
  start A PENDING=1 RUN_STATE=.cache/saved; a=$pid; finish "$a"
  [ "$(count "$TEST_ROOT/timeline" ' A gate.')" = 1 ] || fail 'seventh advance did not gate once'
  [ "$(count "$TEST_ROOT/timeline" ' A session.')" = 0 ] || fail 'seventh advance started a session'
  [ "$(count "$TEST_ROOT/timeline" ' A labels -$')" = 1 ] || fail 'seventh advance held the turn during label read'
  [ "$(line " A release $a$")" -lt "$(line ' A labels -$')" ] || fail 'park read labels before release'
  ! grep -q " A notify.owner $a$" "$TEST_ROOT/timeline" || fail 'park notification held turn'
  grep -q 'parked #1: waited six times: main advanced during the gate' "$TEST_ROOT/A/out" || fail 'seventh advance did not park'
  [ ! -e "$TURN_PATH" ] || fail 'seventh advance left turn'
}
run_case 1 'seventh main advance releases the turn before parking reads labels' seventh_advance

held_notice() {
  local client=$1 a call ended notice='the supervisor of main does not parse; the run keeps its supervisor'
  clone A; state pending
  printf '\nif\n' >> "$TEST_ROOT/origin/architecture/run.sh"
  "$real_git" -C "$TEST_ROOT/origin" add -f architecture/run.sh
  "$real_git" -C "$TEST_ROOT/origin" commit -qm 'invalid supervisor'
  if [ "$client" = override ]; then
    call="$signal_env --ignore-signal=TERM $BASH $work/notify studio: $notice"
    start A PENDING=1 RUN_STATE=.cache/saved NOTIFY_HANG=1 RUN_LAND_LIMIT=1 RUN_LAND_KILL=1 \
      RUN_NOTIFY="$signal_env --ignore-signal=TERM $BASH $work/notify"
  else
    call="osascript -e on run a -e display notification (item 1 of a) with title (item 2 of a) -e end run $notice Labcast run: studio"
    start A PENDING=1 RUN_STATE=.cache/saved NOTIFY_HANG=1 RUN_LAND_LIMIT=1 RUN_LAND_KILL=1 RUN_NOTIFY=
  fi
  a=$pid
  await_line "$TEST_ROOT/timeline" ' A timeout.owner .*the supervisor of main does not parse'
  grep -Fxq "1000 A timeout --signal=TERM --kill-after=1 1 $call" "$TEST_ROOT/timeline" || fail 'held warning has no exact TERM/KILL call'
  grep -Fxq "1000 A timeout.owner $a --signal=TERM --kill-after=1 1 $call" "$TEST_ROOT/timeline" || fail 'warning did not exercise held sync'
  finish "$a"
  ended=$(grep -Fnx "1000 A timeout.done --signal=TERM --kill-after=1 1 $call" "$TEST_ROOT/timeline" | cut -d: -f1 || true)
  [ -n "$ended" ] && [ "$ended" -lt "$(line " A gate.1 $a$")" ] || fail 'warning return did not precede gate'
  grep -q " A gate.1 $a$" "$TEST_ROOT/timeline" || fail 'bounded warning lost the turn before gate'
  [ "$(count "$TEST_ROOT/timeline" ' A gate.')" = 1 ] || fail 'warning repeated the gate'
  [ "$(line " A gate.1 $a$")" -lt "$(line " A release $a$")" ] || fail 'warning released before the gate'
  [ "$(line " A release $a$")" -lt "$(line ' A session.1 -$')" ] || fail 'warning session preceded release'
  ! grep -q " A session\.[0-9][0-9]* $a$" "$TEST_ROOT/timeline" || fail 'warning ran a session inside the turn'
  [ ! -e "$TURN_PATH" ] || fail 'bounded warning left turn'
}
run_case 1 'held supervisor warning through RUN_NOTIFY has a TERM and KILL limit' held_notice override
run_case 1 'held supervisor warning through osascript has a TERM and KILL limit' held_notice default

landing_failure() {
  local action=$1 phase=${2:-} a b hold=$1 turn_evidence=0
  if [ "$phase" = after-merge ]; then hold=view.2; turn_evidence=1; fi
  clone A; clone B; touch "$TEST_ROOT/land-hold.A.$hold"
  if [ "$phase" = after-merge ]; then start A LAND_VIEW_FAIL=merge TURN_LAND_EVIDENCE=1
  else start A LAND_FAIL="$action"; fi
  a=$pid
  if [ "$phase" = after-merge ]; then await_line "$TEST_ROOT/timeline" ' A land.view.2$'
  else await_line "$TEST_ROOT/timeline" " A land.$action "; fi
  start B TURN_LAND_EVIDENCE="$turn_evidence"; b=$pid; await_line "$TEST_ROOT/sleeps" '^B 20$' 2
  local syncs
  syncs=$(count "$TEST_ROOT/timeline" ' B sync ') || lookup_failed "$?" 'B sync count'
  [ "$syncs" = 1 ] || fail 'B synced during failed landing'
  rm "$TEST_ROOT/land-hold.A.$hold"
  finish "$a"; finish "$b"
  local returned released repair synced gates
  returned=$(line " A land.failed $action$") || lookup_failed "$?" 'failed call return'
  released=$(line " A release $a$") || lookup_failed "$?" 'A release'
  [ "$returned" -lt "$released" ] || fail 'release preceded failed call return'
  repair=$(line ' A session.2 ') || lookup_failed "$?" 'A repair session'
  [ "$released" -lt "$repair" ] || fail 'repair session preceded release'
  ! grep -q " A session.2 $a$" "$TEST_ROOT/timeline" || fail 'repair session held turn'
  synced=$(line " B sync $b $b ") || lookup_failed "$?" 'B sync'
  [ "$released" -lt "$synced" ] || fail 'B synced before A released'
  gates=$(cat "$TEST_ROOT/B/gates") || lookup_failed "$?" 'B gate count'
  [ "$gates" = 1 ] || fail 'B did not continue once'
  if [ "$phase" = after-merge ]; then
    local merged gated
    merged=$(jq -r '.[0].mergeCommit.oid' "$TEST_ROOT/A/remote-prs.json") || fail "merge commit lookup status=$?"
    gated=$(cat "$TEST_ROOT/B/gated") || lookup_failed "$?" 'B gated commit'
    "$real_git" -C "$TEST_ROOT/B" merge-base --is-ancestor "$merged" "$gated" || fail 'B missed A merge after failed confirmation'
  fi
  [ ! -e "$TURN_PATH" ] || fail 'failed landing left turn'
}
for action in push list create merge view; do
  run_case 1 "landing $action failure releases before repair session and lets B proceed" landing_failure "$action"
done
run_case 1 'landing confirmation read fails after merge: release before repair and let B gate the merge' landing_failure view after-merge

refusal() {
  local mode=$1 a
  clone A
  case "$mode" in
    failure) echo 'exit 1' > "$TEST_ROOT/A/gate.sh" ;;
    changed) cat > "$TEST_ROOT/A/fetch.sh" <<'STUB'
echo edit >> A
git add -f A; git commit -qm other
STUB
      ;;
    dirty) echo 'echo dirty >> A' > "$TEST_ROOT/A/session.1.sh" ;;
    conflict) cat > "$TEST_ROOT/A/session.1.sh" <<'STUB'
echo conflict > "$TEST_ROOT/origin/A"
git -C "$TEST_ROOT/origin" add -f A; git -C "$TEST_ROOT/origin" commit -qm conflict
STUB
      ;;
  esac
  start A; a=$pid; finish "$a"
  [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 1 ] || fail 'refusal did not take one turn'
  [ "$(line " A release $a$")" -lt "$(line ' A session.2 -$')" ] || fail 'turn not free before repair session'
  if [ "$mode" = failure ]; then
    [ "$(count "$TEST_ROOT/timeline" " A gate.1 $a$")" = 1 ] || fail 'failed gate did not hold turn'
    grep -q 'full gate.*failed' "$TEST_ROOT/A/.cache/run.history" || fail 'failure not retained'
  else [ "$(count "$TEST_ROOT/timeline" ' A gate.')" = 0 ] || fail 'refused gate ran'; fi
  [ ! -e "$TURN_PATH" ] || fail 'refusal left turn'
}
for mode in failure changed dirty conflict; do
  run_case 1 "gate $mode releases before its repair session" refusal "$mode"
done

cached() {
  local outcome=$1 legacy=$2
  clone A; state "$outcome"
  if [ "$legacy" = old ]; then printf 'turn_held=1\nturn_until=1900\n' >> "$TEST_ROOT/A/.cache/saved"; fi
  start A PENDING=1 RUN_STATE=.cache/saved; finish "$pid"
  [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 0 ] || fail 'stored result took turn'
  [ "$(count "$TEST_ROOT/timeline" ' A gate.')" = 0 ] || fail 'stored result repeated gate'
  grep -q ' A sync - .* none$' "$TEST_ROOT/timeline" || fail 'stored result bounded unheld fetch'
  grep -q ' A session.1 -$' "$TEST_ROOT/timeline" || fail 'stored result held session turn'
  grep -q 'last Checks run of main failed: run 4242' "$TEST_ROOT/A/prompt.1" || fail 'stored result lost Checks note'
  grep -q "it $outcome\." "$TEST_ROOT/A/prompt.1" || fail 'stored result note lost'
  ! grep -q 'unbound variable' "$TEST_ROOT/A/out" || fail 'legacy state broke set -u'
  [ ! -e "$TURN_PATH" ] || fail 'stored result left turn'
}
for legacy in current old; do
  for outcome in passed failed; do run_case 0 "stored $outcome takes no turn with $legacy state under set -u" cached "$outcome" "$legacy"; done
done

three() {
  local a b c id owner last_release=0 taken freed
  for id in A B C; do clone "$id"; touch "$TEST_ROOT/gate-hold.$id"; done
  start A; a=$pid; start B; b=$pid; start C; c=$pid
  for id in A B C; do await "$TEST_ROOT/$id/requested"; done
  for id in A B C; do rm "$TEST_ROOT/gate-hold.$id"; done
  finish "$a"; finish "$b"; finish "$c"
  for id in A B C; do
    owner=$(cat "$TEST_ROOT/$id/supervisor.pid")
    [ "$(count "$TEST_ROOT/timeline" " $id take$")" = 1 ] || fail "$id did not get exactly one turn"
    [ "$(count "$TEST_ROOT/timeline" " $id gate.1 $owner$")" = 1 ] || fail "$id gated without its turn"
    [ "$(count "$TEST_ROOT/timeline" " $id land.merge $owner$")" = 1 ] || fail "$id did not land in its turn"
  done
  # The timeline decides the order; there is no assumption about which wins.
  for id in $(sed -n 's/^[0-9]* \([ABC]\) take$/\1/p' "$TEST_ROOT/timeline"); do
    owner=$(cat "$TEST_ROOT/$id/supervisor.pid")
    taken=$(line " $id take$"); freed=$(line " $id release $owner$")
    [ "$last_release" -lt "$taken" ] && [ "$taken" -lt "$freed" ] || fail 'turn intervals overlap'
    last_release=$freed
  done
  [ ! -e "$TURN_PATH" ] || fail 'three runs left turn'
}
run_case 1 'three simultaneous requests each gate and land in one separate turn' three

waiter() {
  local tell=${1:-7200} a n
  clone A; state pending; mkdir "$RUN_TURN"; . "$OWN_HELPER"; own_publish $$ "$RUN_TURN/pid"
  printf 'waits=5\nlast=$\x27#1\tT1\tturn\x27\n' >> "$TEST_ROOT/A/.cache/saved"
  echo 'exit 1' > "$TEST_ROOT/A/gate.sh"
  echo "reply='gate retry'" > "$TEST_ROOT/A/session.1.sh"
  start A PENDING=1 RUN_STATE=.cache/saved RUN_TURN_TELL="${1:-}" RUN_TURN_POLL=7; a=$pid
  await_line "$TEST_ROOT/sleeps" '^A ' 2
  [ "$(count "$TEST_ROOT/sleeps" '^A 7$')" -ge 2 ] || fail 'poll override is not 7 seconds'
  [ "$(cat "$TEST_ROOT/A/.cache/run.status")" = "waiting for the merge turn of process $$" ] || fail 'status must name holder'
  clock "$((1000 + tell - 1))"; n=$(count "$TEST_ROOT/sleeps" '^A 7$')
  await_line "$TEST_ROOT/sleeps" '^A 7$' "$((n + 2))"
  [ "$(count "$TEST_ROOT/timeline" ' A notify .*merge turn')" = 0 ] || fail 'notice before threshold'
  clock "$((1000 + tell))"; n=$(count "$TEST_ROOT/sleeps" '^A 7$')
  await_line "$TEST_ROOT/sleeps" '^A 7$' "$((n + 3))"
  [ "$(count "$TEST_ROOT/timeline" ' A notify .*merge turn')" = 1 ] || fail 'no single notice at threshold'
  clock "$((1000 + tell + 60))"; n=$(count "$TEST_ROOT/sleeps" '^A 7$')
  await_line "$TEST_ROOT/sleeps" '^A 7$' "$((n + 2))"
  [ "$(count "$TEST_ROOT/timeline" ' A notify .*merge turn')" = 1 ] || fail 'wait notice repeated'
  [ "$(count "$TEST_ROOT/timeline" ' A session.')" = 0 ] || fail 'wait started session'
  [ "$(count "$TEST_ROOT/timeline" ' A sync ')" = 0 ] || fail 'wait brought main in'
  [ "$(cat "$RUN_TURN/pid")" = $$ ] || fail 'live run holder removed'
  rm -rf "$RUN_TURN"; finish "$a"
  [ "$(count "$TEST_ROOT/timeline" ' A gate.')" = 2 ] || fail 'turn wait counted as card wait'
  [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 2 ] || fail 'each fresh request must take its turn'
  [ "$(line ' A notify .*merge turn')" -lt "$(line ' A take$')" ] || fail 'notice came after acquisition'
  grep -q "waited $(((tell + 60) / 60)) minutes for the merge turn$" "$TEST_ROOT/A/.cache/run.history" || fail 'minute wait missing'
  [ ! -e "$TURN_PATH" ] || fail 'wait left turn'
}
run_case 1 'live run wait starts no session, costs no card wait and tells once at 7200 seconds' waiter
run_case 1 'turn notice and polling overrides use the file clock' waiter 60

short_wait() {
  clone A; mkdir "$RUN_TURN"; . "$OWN_HELPER"; own_publish $$ "$RUN_TURN/pid"
  start A; local a=$pid
  await_line "$TEST_ROOT/sleeps" '^A 20$' 2
  clock 1059; rm -rf "$RUN_TURN"; finish "$a"
  [ "$(count "$TEST_ROOT/A/.cache/run.history" 'waited .*minutes for the merge turn')" = 0 ] || fail 'short wait wrote history'
  [ "$(count "$TEST_ROOT/timeline" ' A notify .*merge turn')" = 0 ] || fail 'short wait notified'
  [ ! -e "$TURN_PATH" ] || fail 'short wait left turn'
}
run_case 1 'wait under a minute writes no wait history' short_wait

once() {
  clone A
  if [ "${1:-}" = pending ]; then
    state pending
    TURN_ONCE=--once start A PENDING=1 RUN_STATE=.cache/saved
  else TURN_ONCE=--once start A; fi
  finish "$pid" 0
  [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 0 ] || fail '--once took turn'
  [ "$(count "$TEST_ROOT/timeline" ' A gate.')" = 0 ] || fail '--once ran gate'
  grep -q ' A session.1 -$' "$TEST_ROOT/timeline" || fail '--once held session turn'
  [ ! -e "$TURN_PATH" ] || fail '--once left turn'
}
run_case 0 '--once neither takes a turn nor runs a gate' once
run_case 0 '--once with an imported request takes no turn and runs no gate' once pending

review_case() {
  clone A; state pending
  start A PENDING=1 RUN_STATE=.cache/saved RUN_REVIEW=1 RUN_WEEK=2026-46; local a=$pid
  finish "$a"
  [ "$(line ' A session.1 -$')" -lt "$(line ' A take$')" ] || fail 'weekly review took turn'
  [ "$(line ' A session.1 -$')" -lt "$(line ' A checks ')" ] || fail 'review read Checks'
  [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 1 ] || fail 'review took extra turn'
  grep -q " A gate.1 $a$" "$TEST_ROOT/timeline" || fail 'review lost pending gate turn'
  [ ! -e "$TURN_PATH" ] || fail 'review left turn'
}
run_case 1 'weekly review runs without a turn before the pending gate' review_case

parent_failure() {
  local kind=$1 a
  clone A
  if [ "$kind" = directory ]; then
    mkdir "$TEST_ROOT/parent"; chmod 555 "$TEST_ROOT/parent"
    [ ! -w "$TEST_ROOT/parent" ] || fail 'mode 555 parent is still writable by this test user'
  else touch "$TEST_ROOT/parent"; fi
  # A pathname containing the error words must not hide a permission error.
  export RUN_TURN="$TEST_ROOT/parent/File exists.lock" TURN_PATH="$TEST_ROOT/parent/File exists.lock"
  echo "reply='gate again'" > "$TEST_ROOT/A/session.2.sh"
  start A PREPARE=0; a=$pid
  while kill -0 "$a" 2>/dev/null; do
    [ "$(count "$TEST_ROOT/sleeps" '^A 20$')" -lt 8 ] || fail 'unavailable lock kept polling instead of falling back'
    "$real_sleep" 0.01
  done
  finish "$a"
  local message='the folder of the merge turn cannot be made; the run takes no merge turn'
  [ "$(count "$TEST_ROOT/A/.cache/run.history" "$message$")" = 1 ] || fail 'parent failure history repeated'
  [ "$(count "$TEST_ROOT/notified" "$message$")" = 1 ] || fail 'parent failure notice repeated'
  [ "$(count "$TEST_ROOT/timeline" ' A mkdir.failed$')" = 1 ] || fail 'fallback did not follow one actual mkdir failure'
  [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 0 ] || fail 'parent failure tried to take turn'
  [ "$(count "$TEST_ROOT/timeline" ' A gate.')" = 2 ] || fail 'parent failure stopped work'
  [ "$(count "$TEST_ROOT/timeline" ' A session.* -$')" = 3 ] || fail 'fallback held a session'
  [ ! -e "$TURN_PATH" ] || fail 'parent failure left turn'
  [ "$kind" != directory ] || chmod u+w "$TEST_ROOT/parent"
}
run_case 1 'unwritable parent disables turns once and keeps running' parent_failure directory
run_case 1 'regular file parent disables turns once and keeps running' parent_failure file

mkdir_handoff() {
  local a mode=${1:-} expected=1
  clone A; state pending
  mkdir "$TURN_PATH"; . "$OWN_HELPER"; own_publish $$ "$TURN_PATH/pid"
  start A PENDING=1 RUN_STATE=.cache/saved RELEASE_AT_MKDIR=1 PID_WRITE="$mode"; a=$pid; finish "$a"
  [ "$mode" != once ] || expected=2
  [ "$(count "$TEST_ROOT/timeline" ' A mkdir.released$')" = 1 ] || fail 'fixture did not release after failed mkdir'
  [ "$(count "$TEST_ROOT/timeline" ' A take$')" = "$expected" ] || fail 'holder release or failed PID write skipped retrying mkdir'
  [ "$(line ' A mkdir.released$')" -lt "$(line ' A take$')" ] || fail 'retry preceded the holder release'
  [ "$(line ' A mkdir.released$')" -lt "$(line ' A poll 20$')" ] || fail 'contention did not pause after release'
  [ "$(line ' A poll 20$')" -lt "$(line ' A take$')" ] || fail 'contention retried without a pause'
  grep -q " A gate.1 $a$" "$TEST_ROOT/timeline" || fail 'handoff gated without its turn'
  [ "$(count "$TEST_ROOT/notified" 'folder of the merge turn')" = 0 ] || fail 'normal handoff announced filesystem failure'
  [ ! -e "$TURN_PATH" ] || fail 'mkdir handoff left turn'
}
run_case 1 'holder releases after mkdir fails: retry creation and keep the turn for the gate' mkdir_handoff
run_case 1 'PID write after the mkdir retry fails once: acquire only after a successful write' mkdir_handoff once

mkdir_locale() {
  local a
  clone A; state pending
  mkdir "$TURN_PATH"; . "$OWN_HELPER"; own_publish $$ "$TURN_PATH/pid"
  cat > "$TEST_ROOT/A/session.1.sh" <<'STUB'
echo "$LC_ALL" > session.locale
STUB
  start A PENDING=1 RUN_STATE=.cache/saved RELEASE_AT_MKDIR=1 LC_ALL=POSIX; a=$pid; finish "$a"
  [ "$(count "$TEST_ROOT/timeline" ' A mkdir.locale C$')" = 2 ] || fail 'both failed and successful mkdir must use LC_ALL=C'
  [ "$(count "$TEST_ROOT/timeline" ' A mkdir.locale POSIX$')" = 0 ] || fail 'mkdir inherited the session locale'
  [ "$(cat "$TEST_ROOT/A/session.locale")" = POSIX ] || fail 'mkdir changed the session locale'
  [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 1 ] || fail 'released holder disabled the turn'
  [ "$(line ' A mkdir.released$')" -lt "$(line ' A take$')" ] || fail 'take preceded the controlled release'
  grep -q " A gate.1 $a$" "$TEST_ROOT/timeline" || fail 'released holder gated without ownership'
  [ "$(count "$TEST_ROOT/notified" 'folder of the merge turn')" = 0 ] || fail 'File exists announced creation failure'
  [ ! -e "$TURN_PATH" ] || fail 'locale case left turn'
}
run_case 1 'mkdir uses C only for its error; a released holder never disables turns' mkdir_locale

fetch_limit() {
  local a ended
  clone A
  mkdir "$TEST_ROOT/fetch-bin"
  printf '#!%s -S --ignore-signal=TERM %s\n' "$signal_env" "$BASH" > "$TEST_ROOT/fetch-bin/git"
  cat >> "$TEST_ROOT/fetch-bin/git" <<'STUB'
if [ "$1" = fetch ] && [ -e requested ] && [ "${TURN_LAND_CALL:-0}" = 0 ]; then
  . "$OWN_HELPER"; own_record $$ >> "$TEST_ROOT/processes"
  # Hold before the shared fake; TERM is ignored once env has started.
  while :; do "$real_sleep" 0.01; done
fi
exec "$RUN_TEST_BASH" "$TURN_TEST_GIT" "$@"
STUB
  chmod +x "$TEST_ROOT/fetch-bin/git"
  start A RUN_TURN_FETCH=1 RUN_LAND_KILL=1 TURN_TEST_GIT="$work/bin/git" PATH="$TEST_ROOT/fetch-bin:$PATH"; a=$pid
  await_line "$TEST_ROOT/timeline" ' A timeout.owner .*git fetch -q origin main$'
  grep -Fxq '1000 A timeout --signal=TERM --kill-after=1 1 git fetch -q origin main' "$TEST_ROOT/timeline" || fail 'fetch override or KILL grace missing'
  grep -Fxq "1000 A timeout.owner $a --signal=TERM --kill-after=1 1 git fetch -q origin main" "$TEST_ROOT/timeline" || fail 'limited fetch lost holder'
  finish "$a"
  [ "$(count "$TEST_ROOT/timeline" ' A timeout --signal=TERM --kill-after=1 300 bash .*land fetch -q origin main$')" = 3 ] || fail 'sync fetch limit changed a landing fetch limit'
  ended=$(grep -Fnx '1000 A timeout.done --signal=TERM --kill-after=1 1 git fetch -q origin main' "$TEST_ROOT/timeline" | cut -d: -f1 || true)
  [ -n "$ended" ] && [ "$ended" -lt "$(line " A gate.1 $a$")" ] || fail 'fetch return did not precede gate'
  [ "$(count "$TEST_ROOT/timeline" ' A gate.')" = 1 ] || fail 'fetch repeated the gate'
  [ "$(line " A gate.1 $a$")" -lt "$(line " A release $a$")" ] || fail 'fetch released before the gate'
  [ "$(line " A release $a$")" -lt "$(line ' A session.2 -$')" ] || fail 'fetch session preceded release'
  ! grep -q " A session\.[0-9][0-9]* $a$" "$TEST_ROOT/timeline" || fail 'fetch ran a session inside the turn'
  grep -q ' A sync - .* none$' "$TEST_ROOT/timeline" || fail 'unheld fetch got timeout'
  [ ! -e "$TURN_PATH" ] || fail 'fetch timeout left turn'
}
run_case 1 'held fetch passes its TERM and KILL limits then gates; unheld fetch has no timeout' fetch_limit

checks_order() {
  clone A; start A; local a=$pid; finish "$a"
  [ "$(count "$TEST_ROOT/timeline" ' A checks -$')" = 3 ] || fail 'Checks read ran with turn'
  checks=$(grep -n ' A checks ' "$TEST_ROOT/timeline" | sed -n 2p | cut -d: -f1)
  [ "$checks" -lt "$(line ' A take$')" ] || fail 'Checks read after acquisition'
  grep -q " A sync $a $a --signal=TERM --kill-after=10 300 git fetch -q origin main$" "$TEST_ROOT/timeline" || fail 'held fetch lost default limit'
  grep -q 'last Checks run of main failed: run 4242' "$TEST_ROOT/A/prompt.2" || fail 'next lead lost Checks note'
  [ ! -e "$TURN_PATH" ] || fail 'Checks case left turn'
}
run_case 1 'Checks read precedes take; held fetch uses 300 seconds and next lead gets Checks note' checks_order

stale() {
  local kind=$1 command=${2:-/bin/sleep 600} a tries
  clone A; mkdir "$RUN_TURN"
  if [ "$kind" = dead ]; then . "$OWN_HELPER"; own_publish 2147483647 "$RUN_TURN/pid"; else . "$OWN_HELPER"; own_publish $$ "$RUN_TURN/pid"; fi
  start A FOREIGN_COMMAND="$command"; a=$pid
  # A wrong holder test fails by an observed number of polls, not a time limit.
  while kill -0 "$a" 2>/dev/null; do
    tries=$(count "$TEST_ROOT/sleeps" '^A 20$')
    [ "$tries" -lt 8 ] || fail 'stale holder was polled eight times instead of reclaimed'
    "$real_sleep" 0.01
  done
  finish "$a"
  [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 1 ] || fail 'stale lock blocked acquisition'
  [ "$(count "$TEST_ROOT/timeline" ' A release ')" = 2 ] || fail 'stale removal or owned release missing'
  if [ "$kind" = dead ]; then
    [ "$(count "$TEST_ROOT/timeline" ' A ps ')" = 0 ] || fail 'dead process needed ps'
  else
    grep -q " A ps -ww -p $$ -o args=$" "$TEST_ROOT/timeline" || fail 'holder ps arguments changed'
  fi
  [ ! -e "$TURN_PATH" ] || fail 'stale holder left turn'
}
run_case 1 'gone holder is stale without reading ps' stale dead

own_holder() {
  local a
  clone A; state pending
  start A PENDING=1 RUN_STATE=.cache/saved OWN_LOCK=1; a=$pid
  while kill -0 "$a" 2>/dev/null; do
    [ "$(count "$TEST_ROOT/sleeps" '^A 20$')" -lt 8 ] || fail 'own PID was polled instead of reclaimed'
    "$real_sleep" 0.01
  done
  finish "$a"
  [ "$(line " A own.pid $a$")" -lt "$(line ' A take$')" ] || fail 'own PID was not seeded before the first take'
  [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 1 ] || fail 'own PID did not allow one take'
  grep -q " A gate.1 $a$" "$TEST_ROOT/timeline" || fail 'own PID stopped the gate'
  [ ! -e "$TURN_PATH" ] || fail 'own PID case left turn'
}
run_case 1 'own PID before the first take is stale and the run gates' own_holder

replaced_holder() {
  local kind=$1 a b end
  clone A; clone B
  touch "$TEST_ROOT/gate-hold.B"
  start B; b=$pid; await "$TEST_ROOT/B/gate.1"
  if [ "$kind" = pid ]; then . "$OWN_HELPER"; own_publish $$ "$TURN_PATH/pid"
  else rm "$TURN_PATH/pid"; echo 1000 > "$TEST_ROOT/born"; clock 1120; fi
  start A REPLACE_HOLDER="$b"; a=$pid
  await "$TEST_ROOT/replaced"
  end=$((SECONDS + event_limit))
  while [ "$(count "$TEST_ROOT/sleeps" '^A 20$')" -lt 3 ] \
    && [ "$(count "$TEST_ROOT/timeline" ' A release ')" = 0 ] \
    && [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 0 ]; do
    [ "$SECONDS" -lt "$end" ] || fail 'replacement case made no progress (bounded test wait)'
    "$real_sleep" 0.01
  done
  [ "$(cat "$TURN_PATH/pid" 2>/dev/null || echo -)" = "$b" ] || fail 'stale removal deleted the replacement live holder'
  [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 0 ] || fail 'replacement live turn was taken'
  [ "$(count "$TEST_ROOT/timeline" ' A sync ')" = 1 ] || fail 'replacement wait synced after requesting its gate'
  [ "$(count "$TEST_ROOT/timeline" ' A release ')" = 0 ] || fail 'replacement live lock was removed'
  kill -TERM "$a"; finish "$a" 143
  [ "$(cat "$TURN_PATH/pid")" = "$b" ] || fail 'waiting exit removed replacement holder'
  rm "$TEST_ROOT/gate-hold.B"; finish "$b"
  [ "$(line " A replace $b$")" -lt "$(line " B release $b$")" ] || fail 'holder was replaced after its release'
  [ ! -e "$TURN_PATH" ] || fail 'replacement case left turn'
}
run_case 1 'stale PID changes to a live run before removal and its lock stays' replaced_holder pid
run_case 1 'aged pidless lock gains a live run before removal and its lock stays' replaced_holder pidless

run_case 1 'live different program is stale' stale live
run_case 1 'non-shell program with the script as its first argument is stale' stale live 'cat architecture/run.sh studio'
run_case 1 'lead prompt that contains architecture/run.sh is stale' stale live 'codex exec -m sol PROMPT use bash architecture/run.sh studio'
run_case 1 'bash command whose argument only contains the script path is stale' stale live 'bash lead.sh PROMPT=architecture/run.sh studio'
run_case 1 'similar script name is stale' stale live 'bash architecture/runXsh studio'
run_case 1 'script suffix is stale' stale live 'bash architecture/run.sh.backup studio'

live_path() {
  clone A; state pending; mkdir "$RUN_TURN"; . "$OWN_HELPER"; own_publish $$ "$RUN_TURN/pid"
  start A PENDING=1 RUN_STATE=.cache/saved FOREIGN_COMMAND='/bin/bash /tmp/launcher/architecture/run.sh studio'
  local a=$pid
  await_line "$TEST_ROOT/sleeps" '^A ' 3
  [ "$(count "$TEST_ROOT/sleeps" '^A 20$')" -ge 3 ] || fail 'default polling is not 20 seconds'
  [ "$(cat "$RUN_TURN/pid")" = $$ ] || fail 'absolute shell and script paths rejected'
  [ "$(count "$TEST_ROOT/timeline" ' A sync ')" = 0 ] || fail 'absolute run holder allowed sync'
  kill -TERM "$a"; finish "$a" 143
  [ "$(cat "$RUN_TURN/pid")" = $$ ] || fail 'waiter removed absolute run holder'
  rm -rf "$RUN_TURN"
}
run_case 1 'live supervisor with absolute bash and script paths keeps its turn' live_path

uncertain_holder() {
  local mode=$1 a
  clone A; state pending; mkdir "$RUN_TURN"; . "$OWN_HELPER"; own_publish $$ "$RUN_TURN/pid"
  if [ "$mode" = missing ]; then
    start A PENDING=1 RUN_STATE=.cache/saved RUN_TURN_PS="$TEST_ROOT/no-ps"
  else start A PENDING=1 RUN_STATE=.cache/saved PS_MODE="$mode"; fi
  a=$pid; await_line "$TEST_ROOT/sleeps" '^A 20$' 3
  [ "$(cat "$RUN_TURN/pid")" = $$ ] || fail 'unknown live holder removed'
  [ "$(count "$TEST_ROOT/timeline" ' A sync ')" = 0 ] || fail 'unknown holder allowed sync'
  [ "$(count "$TEST_ROOT/timeline" ' A gate.')" = 0 ] || fail 'unknown holder allowed gate'
  [ "$(count "$TEST_ROOT/timeline" ' A session.')" = 0 ] || fail 'unknown holder allowed session'
  kill -TERM "$a"; finish "$a" 143
  [ "$(cat "$RUN_TURN/pid")" = $$ ] || fail 'waiter removed unknown holder at EXIT'
  rm -rf "$RUN_TURN"
}
for mode in fail empty errorline missing; do run_case 1 "live holder remains alive when ps is $mode" uncertain_holder "$mode"; done

real_holder() {
  local a b foreign command
  if [ -z "$real_ps" ]; then
    echo "turn-test: unavailable: $name: ps is not on PATH"
    unavailable=$((unavailable + 1)); return
  fi
  clone A; clone B; touch "$TEST_ROOT/gate-hold.A"
  start A; a=$pid; await "$TEST_ROOT/A/gate.1"
  # This is the installed ps, not either ps seam. A sandbox may deny it.
  if ! command=$("$real_ps" -ww -p "$a" -o args= 2> "$TEST_ROOT/ps.error") || [ -z "$command" ]; then
    echo "turn-test: unavailable: $name: real ps failed or returned no command: $(cat "$TEST_ROOT/ps.error")"
    unavailable=$((unavailable + 1))
    rm "$TEST_ROOT/gate-hold.A"; finish "$a"
    return
  fi
  echo "$command" > "$TEST_ROOT/real-run.args"
  # Wrap only to retain the real output; the holder decision is the supervisor's.
  cat > "$TEST_ROOT/real-ps" <<'STUB'
#!/bin/sh
"$real_ps" "$@" > "$TEST_ROOT/ps.args" || exit $?
cat "$TEST_ROOT/ps.args"
STUB
  start B RUN_TURN_PS="sh $TEST_ROOT/real-ps"; b=$pid
  await_line "$TEST_ROOT/sleeps" '^B 20$' 2
  [ "$(cat "$RUN_TURN/pid")" = "$a" ] || fail 'real bash supervisor rejected'
  [ "$(count "$TEST_ROOT/timeline" ' B sync ')" = 1 ] || fail 'real holder failed to block sync'
  grep -q 'architecture/run.sh studio' "$TEST_ROOT/ps.args" || fail 'real ps did not read supervisor'
  rm "$TEST_ROOT/gate-hold.A"; finish "$a"; finish "$b"
  # A live shell's prompt contains the path, but its executed file is lead.sh.
  printf 'while :; do "$real_sleep" 0.01; done\n' > "$TEST_ROOT/lead.sh"
  "$BASH" "$TEST_ROOT/lead.sh" 'PROMPT read architecture/run.sh studio' & foreign=$!
  echo "$foreign" >> "$TEST_ROOT/processes"
  "$real_ps" -ww -p "$foreign" -o args= > "$TEST_ROOT/real-lead.args"
  grep -q 'architecture/run.sh' "$TEST_ROOT/real-lead.args" || fail 'foreign prompt lost path'
  clone C; mkdir "$RUN_TURN"; . "$OWN_HELPER"; own_publish "$foreign" "$RUN_TURN/pid"
  start C RUN_TURN_PS="sh $TEST_ROOT/real-ps"; finish "$pid"
  [ "$(count "$TEST_ROOT/timeline" " C release $foreign$")" = 1 ] || fail 'real non-supervisor accepted'
  [ "$(count "$TEST_ROOT/timeline" ' C gate.1 ')" = 1 ] || fail 'real non-supervisor blocked gate'
  kill -KILL "$foreign"; wait "$foreign" 2>/dev/null || true
  [ ! -e "$TURN_PATH" ] || fail 'real ps case left turn'
}
run_case 1 'real ps accepts a real bash supervisor and rejects a live shell with the path in its prompt' real_holder

pidless() {
  local a n
  clone A; state pending; mkdir "$RUN_TURN"; echo 1000 > "$TEST_ROOT/born"
  start A PENDING=1 RUN_STATE=.cache/saved; a=$pid
  await_line "$TEST_ROOT/sleeps" '^A 20$' 2
  clock 1060; n=$(count "$TEST_ROOT/sleeps" '^A 20$')
  await_line "$TEST_ROOT/sleeps" '^A 20$' "$((n + 2))"
  [ "$(count "$TEST_ROOT/timeline" ' A release ')" = 0 ] || fail 'pidless lock removed within minute'
  [ "$(count "$TEST_ROOT/timeline" ' A sync ')" = 0 ] || fail 'pidless lock allowed sync'
  clock 1061
  while kill -0 "$a" 2>/dev/null; do
    [ "$(count "$TEST_ROOT/sleeps" '^A 20$')" -lt "$((n + 10))" ] || fail 'old pidless holder not reclaimed'
    "$real_sleep" 0.01
  done
  finish "$a"
  [ "$(line ' A release -$')" -lt "$(line ' A take$')" ] || fail 'pidless removal did not precede take'
  [ ! -e "$TURN_PATH" ] || fail 'pidless lock left turn'
}
run_case 1 'pidless lock stays for a full minute then is reclaimed' pidless

stale_removal_failure() {
  local a releases polls
  clone A; state pending; mkdir "$RUN_TURN"; . "$OWN_HELPER"; own_publish 2147483647 "$RUN_TURN/pid"
  start A PENDING=1 RUN_STATE=.cache/saved RM_FAIL=1; a=$pid
  await_line "$TEST_ROOT/timeline" ' A release ' 4
  releases=$(count "$TEST_ROOT/timeline" ' A release '); polls=$(count "$TEST_ROOT/timeline" ' A poll 20$')
  [ "$polls" -ge "$((releases - 1))" ] || fail 'stale removal retries did not pause'
  clock 8200; await_line "$TEST_ROOT/notified" 'merge turn'
  await_line "$TEST_ROOT/timeline" ' A release ' "$((releases + 4))"
  [ "$(count "$TEST_ROOT/notified" 'merge turn')" = 1 ] || fail 'stale removal notice repeated'
  [ "$(count "$TEST_ROOT/timeline" ' A sync ')" = 0 ] || fail 'failed stale removal allowed sync'
  kill -TERM "$a"; finish "$a" 143
  [ "$(cat "$RUN_TURN/pid")" = 2147483647 ] || fail 'failed stale removal changed holder'
  rm -rf "$RUN_TURN"
}
run_case 1 'stale lock that cannot be removed pauses each retry and notifies once' stale_removal_failure

pid_write() {
  local mode=$1 a n
  clone A; state pending
  start A PENDING=1 RUN_STATE=.cache/saved PID_WRITE="$mode" LC_ALL=C; a=$pid
  if [ "$mode" = once ]; then
    while kill -0 "$a" 2>/dev/null; do
      [ "$(count "$TEST_ROOT/sleeps" '^A 20$')" -lt 8 ] || fail 'successful PID retry kept polling instead of gating'
      "$real_sleep" 0.01
    done
    finish "$a"
    [ "$(count "$TEST_ROOT/timeline" ' A pid.drop$')" = 1 ] || fail 'PID write did not fail once'
    [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 2 ] || fail 'PID write did not retry'
    [ "$(line ' A pid.drop$')" -lt "$(line ' A poll 20$')" ] || fail 'failed write did not pause'
    [ "$(line ' A poll 20$')" -lt "$(line " A sync $a $a ")" ] || fail 'sync preceded successful PID write'
    grep -q " A gate.1 $a$" "$TEST_ROOT/timeline" || fail 'failed write skipped gate or lost turn'
    [ ! -e "$TURN_PATH" ] || fail 'PID retry left turn'
  else
    await_line "$TEST_ROOT/sleeps" '^A 20$' 3
    [ -d "$TURN_PATH" ] && [ ! -w "$TURN_PATH" ] && [ ! -e "$TURN_PATH/pid" ] || fail 'PID write must fail inside a folder that stays'
    grep -Fq "$TURN_PATH/pid: Permission denied" "$TEST_ROOT/A/out" || fail 'a real PID write did not fail'
    [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 1 ] || fail 'PID failure did not leave its first folder'
    [ "$(count "$TEST_ROOT/timeline" ' A release ')" = 0 ] || fail 'PID failure removed a young folder'
    clock 1060; n=$(count "$TEST_ROOT/sleeps" '^A 20$'); await_line "$TEST_ROOT/sleeps" '^A 20$' "$((n + 2))"
    [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 1 ] || fail 'PID failure retried before its minute grace'
    [ "$(count "$TEST_ROOT/timeline" ' A release ')" = 0 ] || fail 'PID failure removed its folder within a minute'
    clock 1061; await_line "$TEST_ROOT/timeline" ' A pid.readonly$' 2
    [ "$(line ' A release -$')" -lt "$(grep -n ' A pid.readonly$' "$TEST_ROOT/timeline" | sed -n 2p | cut -d: -f1)" ] || fail 'PID retry preceded aged folder removal'
    [ -d "$TURN_PATH" ] && [ ! -w "$TURN_PATH" ] && [ ! -e "$TURN_PATH/pid" ] || fail 'PID retry did not keep the failed-write folder'
    [ "$(count "$TEST_ROOT/timeline" ' A sync ')" = 0 ] || fail 'persistent PID failure allowed sync'
    [ "$(count "$TEST_ROOT/timeline" ' A gate.')" = 0 ] || fail 'persistent PID failure allowed gate'
    [ "$(count "$TEST_ROOT/timeline" ' A session.')" = 0 ] || fail 'persistent PID failure allowed session'
    clock 8199; n=$(count "$TEST_ROOT/sleeps" '^A 20$'); await_line "$TEST_ROOT/sleeps" '^A 20$' "$((n + 2))"
    [ "$(count "$TEST_ROOT/notified" 'merge turn')" = 0 ] || fail 'PID failure notified early'
    clock 8200; await_line "$TEST_ROOT/notified" 'merge turn'
    n=$(count "$TEST_ROOT/sleeps" '^A 20$'); await_line "$TEST_ROOT/sleeps" '^A 20$' "$((n + 3))"
    [ "$(count "$TEST_ROOT/notified" 'merge turn')" = 1 ] || fail 'PID failure notice repeated'
    [ -d "$TURN_PATH" ] && [ ! -w "$TURN_PATH" ] && [ ! -e "$TURN_PATH/pid" ] || fail 'PID failure folder disappeared during the wait'
    [ "$(count "$TEST_ROOT/timeline" ' A sync ')" = 0 ] || fail 'persistent PID failure later allowed sync'
    [ "$(count "$TEST_ROOT/timeline" ' A gate.')" = 0 ] || fail 'persistent PID failure later allowed gate'
    [ "$(count "$TEST_ROOT/timeline" ' A session.')" = 0 ] || fail 'persistent PID failure later allowed session'
    kill -TERM "$a"; finish "$a" 143
    [ -d "$TURN_PATH" ] && [ ! -e "$TURN_PATH/pid" ] || fail 'waiting EXIT removed the pidless folder'
    chmod u+w "$TURN_PATH"; rm -rf "$TURN_PATH"
  fi
}
run_case 1 'PID write fails once then succeeds before sync and gate' pid_write once
run_case 1 'PID write always fails: keep waiting without work and tell once' pid_write always

signal_case() {
  local mode=$1 signal=$2 a code=143
  [ "$signal" != HUP ] || code=129
  clone A
  if [ "$mode" = holder ]; then touch "$TEST_ROOT/gate-hold.A"
  else state pending; mkdir "$RUN_TURN"; . "$OWN_HELPER"; own_publish $$ "$RUN_TURN/pid"; fi
  if [ "$mode" = holder ]; then start A; else start A PENDING=1 RUN_STATE=.cache/saved; fi
  a=$pid
  if [ "$mode" = holder ]; then await "$TEST_ROOT/A/gate.1"
  else await_line "$TEST_ROOT/sleeps" '^A 20$' 2; fi
  kill -"$signal" "$a"
  # Bash exits and runs EXIT; its foreground gate can continue as an orphan.
  # End the fake gate separately; suite cleanup also does so on any failure.
  if [ "$mode" = holder ]; then rm "$TEST_ROOT/gate-hold.A"; fi
  finish "$a" "$code"
  if [ "$mode" = waiter ]; then
    [ "$(cat "$RUN_TURN/pid")" = $$ ] || fail 'signalled waiter removed holder'
    [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 0 ] || fail 'signalled waiter acquired'
    rm -rf "$RUN_TURN"
  else
    await "$TEST_ROOT/A/gate.ended" "$(cat "$TEST_ROOT/A/gate.pid")"
    [ "$(count "$TEST_ROOT/timeline" " A release $a$")" = 1 ] || fail 'EXIT did not release owned turn'
  fi
  [ ! -e "$TURN_PATH" ] || fail 'signal left owned turn'
}
for signal in TERM HUP; do
  for mode in holder waiter; do run_case 1 "$signal while $mode leaves no owned turn and ends on failure" signal_case "$mode" "$signal"; done
done

killed_holder() {
  clone A; clone B; touch "$TEST_ROOT/gate-hold.A"
  start A; local a=$pid b
  await "$TEST_ROOT/A/gate.1"
  start B; b=$pid; await_line "$TEST_ROOT/sleeps" '^B 20$' 2
  kill -KILL "$a"; finish "$a" 137
  rm "$TEST_ROOT/gate-hold.A"
  finish "$b"
  [ "$(count "$TEST_ROOT/timeline" " B release $a$")" = 1 ] || fail 'dead supervisor not reclaimed'
  grep -q " B gate.1 $b$" "$TEST_ROOT/timeline" || fail 'waiter did not gate with reclaimed turn'
  [ ! -e "$TURN_PATH" ] || fail 'killed holder left turn'
}
run_case 1 'KILL leaves a dead holder that the waiting run reclaims' killed_holder

error_case() {
  clone A; echo 'echo undefined_clock > "$TEST_ROOT/clock"' > "$TEST_ROOT/A/gate.sh"
  start A; local a=$pid code=1
  [ "$BASH_VERSINFO" -ge 4 ] || code=0
  finish "$a" "$code"
  grep -q undefined_clock "$TEST_ROOT/A/out" || fail 'arithmetic error not reached'
  [ "$(count "$TEST_ROOT/timeline" " A release $a$")" = 1 ] || fail 'error EXIT did not release'
  [ "$(count "$TEST_ROOT/timeline" ' A session.2 ')" = 0 ] || fail 'session started after fatal error'
  [ ! -e "$TURN_PATH" ] || fail 'error left turn'
}
run_case 1 'fatal error after gate runs EXIT and releases without a session' error_case

restart() {
  clone A
  cat > "$TEST_ROOT/A/session.1.sh" <<'STUB'
echo '# next supervisor' >> "$TEST_ROOT/origin/architecture/run.sh"
git -C "$TEST_ROOT/origin" add -f architecture/run.sh
git -C "$TEST_ROOT/origin" commit -qm supervisor
STUB
  start A; local a=$pid
  await_line "$TEST_ROOT/timeline" ' A exec '
  grep -q ' A exec -$' "$TEST_ROOT/timeline" || fail 'exec began while holding the turn'
  finish "$a"
  [ "$(count "$TEST_ROOT/timeline" ' A exec -$')" = 1 ] || fail 'exec did not observe free turn'
  [ "$(cat "$TEST_ROOT/A/restart.pid")" = "$a" ] || fail 'exec changed supervisor pid'
  ! grep -q 'turn_' "$TEST_ROOT/A/restart.state" || fail 'restart saved turn data'
  [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 2 ] || fail 'restarted run failed to reacquire'
  [ "$(line " A release $a$")" -lt "$(line ' A exec -$')" ] || fail 'exec preceded release'
  [ "$(count "$TEST_ROOT/timeline" " A sync $a $a ")" = 2 ] || fail 'restart sync ran without turn'
  [ "$(count "$TEST_ROOT/timeline" " A gate.1 $a$")" = 1 ] || fail 'restart repeated gate or lost holder'
  [ ! -e "$TURN_PATH" ] || fail 'restart left turn'
}
run_case 1 'new supervisor exec releases the open request turn and reacquires before sync' restart

replace_owner() {
  clone A; echo '. "$OWN_HELPER"; own_publish 2147483647 "$TURN_PATH/pid"; exit 1' > "$TEST_ROOT/A/gate.sh"
  start A; finish "$pid"
  [ "$(cat "$RUN_TURN/pid")" = 2147483647 ] || fail 'release removed another owner'
  [ "$(count "$TEST_ROOT/timeline" ' A release ')" = 0 ] || fail 'release tried to remove another owner'
  rm -rf "$RUN_TURN"
}
run_case 1 'release leaves a lock whose pid changed to another owner' replace_owner

default_path() {
  clone A
  TURN_PATH=$XDG_CACHE_HOME/labcast/turn.lock start A RUN_TURN=
  finish "$pid"
  [ "$(count "$TEST_ROOT/timeline" ' A take$')" = 1 ] || fail 'omitted RUN_TURN did not use isolated cache'
  [ ! -e "$XDG_CACHE_HOME/labcast/turn.lock" ] || fail 'default lock left behind'
  [ ! -e "$GATE_LOCK" ] || fail 'fake gate reached gate lock seam'
}
run_case 1 'omitted RUN_TURN uses the isolated cache and never the real lock folders' default_path

[ "$cases" -gt 0 ] || [ "$skipped" -gt 0 ] || { echo 'turn-test: no matching case'; exit 1; }
echo "turn-test: $((cases - unavailable)) cases pass"
if [ "$skipped" -gt 0 ]; then echo "turn-test: skipped $skipped cases: timeout is not on PATH"; fi
if [ "$unavailable" -gt 0 ]; then echo "turn-test: $unavailable case unavailable"; fi
