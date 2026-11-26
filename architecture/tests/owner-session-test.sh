#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=architecture/tests/start-fixture.sh
. "$(dirname -- "$0")/start-fixture.sh"
fixture_init
fixture_session_commands
fixture_copy architecture/owner/session.sh
fixture_copy architecture/owner/agy-settings.json
mkdir -p "$checkout/.agents/agents"
fixture_copy .agents/agents/harness-owner.md
fixture_copy .agents/hooks.json
session=$checkout/architecture/owner/session.sh
cleanup() {
  local code=$? file pid
  if [ "$code" != 0 ] && [ -r "$fixture/loop-launches" ]; then
    while IFS= read -r pid; do kill -TERM "$pid" 2>/dev/null || true; done < "$fixture/loop-launches"
  fi
  for file in "$fixture"/*.parent "$fixture"/claude-child "$fixture"/claude-pid "$state"/owner-loop.pid; do
    if [ -r "$file" ] && IFS= read -r pid < "$file"; then
      kill -TERM "$pid" 2>/dev/null || true
      if [ "$file" = "$fixture/claude-child" ]; then kill -KILL "$pid" 2>/dev/null || true; fi
    fi
  done
  fixture_cleanup "$code"
}
trap cleanup EXIT

mkfifo "$fixture/hold"
export FIXTURE_NOHUP FIXTURE_DATE FIXTURE_SLEEP
FIXTURE_NOHUP=$(command -v nohup)
FIXTURE_DATE=$(command -v date)
FIXTURE_SLEEP=$(command -v sleep)
for name in timeout gtimeout; do ln -s unexpected "$fixture/bin/$name"; done
export FIXTURE_FLOCK
FIXTURE_FLOCK=$(command -v flock || true)
fixture_stub flock <<'STUB'
if [ "${FIXTURE_NO_FLOCK:-0}" = 1 ]; then exec "$FIXTURE/bin/unexpected"; fi
exec "$FIXTURE_FLOCK" "$@"
STUB
cat >> "$BASH_ENV" <<'ENV'
command() {
  if [ "$*" = '-v agy' ] && [ "${FIXTURE_NO_AGY:-0}" = 1 ]; then return 1; fi
  case "$*" in '-v timeout'|'-v gtimeout'|'-v flock') return 1 ;; esac
  builtin command "$@"
}
ENV
fixture_stub nohup <<'STUB'
printf '%s\n' "$*" > "$FIXTURE/commands/$$"
printf '%s\n' "$$" >> "$FIXTURE/loop-launches"
exec "$FIXTURE_NOHUP" "$@"
STUB
fixture_stub sleep <<'STUB'
printf '%s\n' "$1" >> "$FIXTURE/sleeps"
case "$1" in 30) exec "$FIXTURE_SLEEP" 1 ;; *) exec "$FIXTURE_SLEEP" "$1" ;; esac
STUB
fixture_stub pgrep <<'STUB'
case "$1" in
  -f)
    [ -s "$FIXTURE/plain-ready" ] || exit 1
    grep -Eq -- "$2" "$FIXTURE/plain-command" || exit 1
    cat "$FIXTURE/plain-ready"
    ;;
  -P) if [ -s "$FIXTURE/claude-child" ] && [ "$2" = "$(cat "$FIXTURE/claude-pid")" ]; then
    cat "$FIXTURE/claude-child"
  fi ;;
esac
STUB
fixture_stub lsof <<'STUB'
[ "$1 $2 $4 $5 $6" = '-a -p -d cwd -Fn' ]
[ "$3" = "$(cat "$FIXTURE/plain-ready")" ]
printf 'p%s\nn%s\n' "$3" "$(cat "$FIXTURE/plain-cwd")"
STUB
fixture_stub date <<'STUB'
case "$*" in
  '+%Y-%m-%d %H:%M:%S') echo '2026-11-20 10:00:00' ;;
  '+%s') if [ -s "$FIXTURE/clock" ]; then cat "$FIXTURE/clock"; else "$FIXTURE_DATE" "$@"; fi ;;
  *) exec "$FIXTURE_DATE" "$@" ;;
esac
STUB
fixture_stub claude <<'STUB'
set -eu
if [ "$#" = 0 ]; then
  printf '%s\n' "$PWD" > "$FIXTURE/plain-cwd"
  printf '%s\n' "$0" > "$FIXTURE/plain-command"
  exec 6<> "$FIXTURE/hold"
  printf '%s\n' "$$" > "$FIXTURE/plain-ready"
  read -r unused <&6
  exit
fi
input=EOF
if IFS= read -r value; then input=$value; fi
{
  printf 'cwd=<%s> stdin=<%s>\n' "$PWD" "$input"
  printf 'arg=<%s>\n' "$@"
} > "$FIXTURE/claude-args"
printf '%s\n' "${HARNESS_OWNER_SESSION:-unset}" "${CLAUDE_PROJECT_DIR:-unset}" > "$FIXTURE/claude-env"
echo "$$" > "$FIXTURE/claude-pid"
printf '%s\n' "$1" >> "$FIXTURE/claude-runs"
cat "$FIXTURE_STATE/owner.lock/kind" >> "$FIXTURE/claude-lock-kinds"
case "${CLAUDE_TEST_MODE:-ok}" in
  ok) printf 'first output\nlast output\n\n' ;;
  fail) printf 'fixture failure\n' >&2; exit 7 ;;
  code124) echo 'ordinary exit 124'; exit 124 ;;
  expired) echo 'The sign-in window ends soon.' ;;
  signin) echo 'Not logged in. Please run /login.' >&2; exit 1 ;;
  long) printf '%.0sX' {1..220}; printf '\n\t\n' ;;
  elapsed) echo 1795168930 > "$FIXTURE/clock"; echo measured ;;
  hold|timeout)
    trap 'exit 0' TERM HUP
    if [ "$CLAUDE_TEST_MODE" = timeout ]; then
      (
        trap '' TERM
        exec 6<> "$FIXTURE/hold"
        echo ready > "$FIXTURE/claude-child-ready"
        read -r unused <&6
      ) &
      echo "$!" > "$FIXTURE/claude-child"
    fi
    echo 'waiting'
    exec 6<> "$FIXTURE/hold"
    echo ready > "$FIXTURE/claude-ready"
    read -r unused <&6
    ;;
  *) exit 2 ;;
esac
STUB
fixture_stub agy <<'STUB'
set -eu
if [ "$#" = 0 ]; then
  printf '%s\n' "$PWD" > "$FIXTURE/plain-cwd"
  printf '%s\n' "$0" > "$FIXTURE/plain-command"
  exec 6<> "$FIXTURE/hold"
  printf '%s\n' "$$" > "$FIXTURE/plain-ready"
  read -r unused <&6
  exit
fi
input=EOF
if IFS= read -r value; then input=$value; fi
{
  printf 'cwd=<%s> stdin=<%s>\n' "$PWD" "$input"
  printf 'arg=<%s>\n' "$@"
} > "$FIXTURE/agy-args"
printf '%s\n' "${HARNESS_OWNER_SESSION:-unset}" "${CLAUDE_PROJECT_DIR:-unset}" > "$FIXTURE/agy-env"
echo 'Antigravity check output'
STUB
fixture_commit
git -C "$checkout" worktree add -q --detach "$worktrees/another" HEAD
other_session=$worktrees/another/architecture/owner/session.sh

expect "$(where_lines "$checkout" "$checkout" "$worktrees" "$state" owner)" "$("$BASH" "$session" --where)"
[ ! -e "$state" ]
[ ! -e "$fixture/claude-runs" ]
invoke 1 "$BASH" "$session" loop status
expect 'owner loop is not running' "$actual"
[ ! -e "$state" ]

settings=$checkout/architecture/owner/settings.json
talk_args() {
  local input=$1
  shift
  printf 'cwd=<%s> stdin=<%s>\n' "$checkout" "$input"
  printf 'arg=<%s>\n' --agent harness-owner --settings "$settings" "$@"
}

check_args() {
  printf 'cwd=<%s> stdin=<EOF>\n' "$checkout"
  printf 'arg=<%s>\n' -p --agent harness-owner --settings "$settings" \
    --permission-mode "$1" 'Run one harness check and stop.'
}

history_line() {
  printf '2026-11-20 10:00:00\tcheck\t%s\t%s\t%s' "$1" "${3:-0}" "$2"
}

expect_history() {
  expect "$(history_line "$@")" "$(tail -n 1 "$state/owner.history")"
  [ ! -e "$state/owner.lock" ]
}

export CLAUDE_TEST_MODE=hold
"$BASH" "$session" talk --model 'fixture model' 'hello world' <<< 'terminal input' > "$fixture/talk.out" &
talk=$!
echo "$talk" > "$fixture/talk.parent"
await test -s "$fixture/claude-ready"
expect "$(talk_args 'terminal input' --model 'fixture model' 'hello world')" "$(cat "$fixture/claude-args")"
expect "$talk" "$(cat "$state/owner.lock/pid")"
expect talk "$(cat "$state/owner.lock/kind")"
expect 'Fri Nov 20 10:00:00 2026' "$(cat "$state/owner.lock/started")"
message="owner: a session with a terminal is alive (process $talk); it makes its own checks"
CLAUDE_TEST_MODE=ok invoke 0 "$BASH" "$session" check
expect "$message" "$actual"
expect "$(history_line skipped "$message")" "$(cat "$state/owner.history")"
expect '--agent' "$(cat "$fixture/claude-runs")"
CLAUDE_TEST_MODE=ok "$BASH" "$other_session" talk </dev/null > "$fixture/second-talk.out" &
waiting=$!
echo "$waiting" > "$fixture/waiting.parent"
await test -s "$fixture/second-talk.out"
expect "owner: a session with a terminal is alive (process $talk); waiting for it to end" "$(cat "$fixture/second-talk.out")"
expect "$talk" "$(cat "$state/owner.lock/pid")"
talk_child=$(cat "$fixture/claude-pid")
kill -TERM "$talk"
code=0
wait "$talk" || code=$?
[ "$code" = 143 ]
wait "$waiting"
expect "$(printf '%s\n' "owner: a session with a terminal is alive (process $talk); waiting for it to end" 'first output' 'last output')" \
  "$(cat "$fixture/second-talk.out")"
[ ! -e "$state/owner.lock" ]
await fixture_dead "$talk_child"
rm "$fixture/talk.parent" "$fixture/waiting.parent" "$fixture/claude-ready"

for signal in HUP INT; do
  code=129
  command=("$BASH")
  if [ "$signal" = INT ]; then
    # Bash ignores INT for a background command. Model a foreground start.
    if ! env --default-signal=INT true 2>/dev/null; then continue; fi
    command=(env --default-signal=INT "$BASH")
    code=130
  fi
  "${command[@]}" "$session" talk </dev/null > "$fixture/signal.out" &
  talk=$!
  echo "$talk" > "$fixture/talk.parent"
  await test -s "$fixture/claude-ready"
  talk_child=$(cat "$fixture/claude-pid")
  kill "-$signal" "$talk"
  outcome=0
  wait "$talk" || outcome=$?
  [ "$outcome" = "$code" ]
  [ ! -e "$state/owner.lock" ]
  await fixture_dead "$talk_child"
  rm "$fixture/talk.parent" "$fixture/claude-ready"
done

export CLAUDE_TEST_MODE=fail
invoke 7 "$BASH" "$session" talk </dev/null
expect 'fixture failure' "$actual"
[ ! -e "$state/owner.lock" ]
unset CLAUDE_TEST_MODE
invoke 0 "$BASH" "$session" talk </dev/null
expect $'first output\nlast output' "$actual"
[ ! -e "$state/owner.lock" ]

stale_lock() {
  mkdir "$state/owner.lock"
  printf '%s\n' "$1" > "$state/owner.lock/pid"
  echo talk > "$state/owner.lock/kind"
  printf '%s\n' "${2:-Fri Nov 20 10:00:00 2026}" > "$state/owner.lock/started"
}
stale_lock 999999999
echo 1795168800 > "$fixture/clock"
# Stale talk is taken over. Check has /dev/null, even if given input.
invoke 0 "$BASH" "$session" check <<< 'must not reach Claude'
expect '' "$actual"
expect "$(check_args auto)" "$(cat "$fixture/claude-args")"
expect_history ok 'last output'
log_files=("$state"/owner-checks/*.log)
[ "${#log_files[@]}" = 1 ]
diff -u <(printf 'first output\nlast output\n\n') "${log_files[0]}"

# Simulate a PID taken by an unrelated command, by the tool for another
# state folder, and by the same tool with a different process start time.
printf '%s\n' '/bin/sleep 2' > "$fixture/commands/$$"
stale_lock "$$"
invoke 0 "$BASH" "$session" check
expect_history ok 'last output'
printf '%s %s run talk %s\n' "$BASH" "$session" "$(fixture_hash "$fixture/other state")" > "$fixture/commands/$$"
stale_lock "$$"
invoke 0 "$BASH" "$session" check
expect_history ok 'last output'
printf '%s %s run talk %s\n' "$BASH" "$session" "$(fixture_hash "$state")" > "$fixture/commands/$$"
stale_lock "$$" 'Thu Nov 19 10:00:00 2026'
invoke 0 "$BASH" "$session" check
expect_history ok 'last output'

OWNER_CHECK_MINUTES=0 invoke 2 "$BASH" "$session" check
expect 'owner: the check minutes and log limit must be positive' "$actual"
expect_history failed "$actual"
mv "$state/owner-checks" "$fixture/check-logs"
touch "$state/owner-checks"
invoke 1 "$BASH" "$session" check
expect_history failed "owner: cannot create $state/owner-checks"
rm "$state/owner-checks"
mv "$fixture/check-logs" "$state/owner-checks"
touch "$state/owner.lock"
invoke 1 "$BASH" "$session" check
expect "$(history_line failed 'owner: could not take the owner lock')" "$(tail -n 1 "$state/owner.history")"
rm "$state/owner.lock"

export CLAUDE_TEST_MODE=fail OWNER_PERMISSION_MODE=plan
invoke 7 "$BASH" "$session" check
expect "$(check_args plan)" "$(cat "$fixture/claude-args")"
expect_history 'failed 7' 'fixture failure'
unset OWNER_PERMISSION_MODE
export CLAUDE_TEST_MODE=code124
invoke 124 "$BASH" "$session" check
expect_history 'failed 124' 'ordinary exit 124'

export CLAUDE_TEST_MODE=expired
invoke 0 "$BASH" "$session" check
expect_history 'ok SIGN-IN' 'The sign-in window ends soon.'
export CLAUDE_TEST_MODE=signin
invoke 1 "$BASH" "$session" check
expect_history 'failed 1 SIGN-IN' 'Not logged in. Please run /login.'
export CLAUDE_TEST_MODE=long OWNER_CHECK_LOG_LIMIT=3
invoke 0 "$BASH" "$session" check
expect_history ok "$(printf '%.0sX' {1..200})"
log_files=("$state"/owner-checks/*.log)
[ "${#log_files[@]}" = 3 ]
expect 'The sign-in window ends soon.' "$(cat "${log_files[0]}")"
expect 'Not logged in. Please run /login.' "$(cat "${log_files[1]}")"
expect "$(printf '%.0sX' {1..220})" "$(head -n 1 "${log_files[2]}")"
unset OWNER_CHECK_LOG_LIMIT

# Keep the default number of logs, and record elapsed minutes independently
# from the requested deadline.
for ((index = 1; index <= 51; index++)); do
  printf -v name '20200101-0000%02d' "$index"
  touch "$state/owner-checks/$name.log"
done
echo 1795168800 > "$fixture/clock"
export CLAUDE_TEST_MODE=elapsed
invoke 0 "$BASH" "$session" check
expect_history ok measured 2
log_files=("$state"/owner-checks/*.log)
[ "${#log_files[@]}" = 50 ]
[ ! -e "$state/owner-checks/20200101-000001.log" ]
rm "$fixture/clock"

export CLAUDE_TEST_MODE=timeout OWNER_CHECK_MINUTES=0.03
"$BASH" "$session" check > "$fixture/check.out" 2>&1 &
checking=$!
echo "$checking" > "$fixture/check.parent"
await test -s "$fixture/claude-child-ready"
check_child=$(cat "$fixture/claude-child")
check_claude=$(cat "$fixture/claude-pid")
expect "$checking" "$(cat "$state/owner.lock/pid")"
expect check "$(cat "$state/owner.lock/kind")"
CLAUDE_TEST_MODE=ok invoke 0 "$BASH" "$session" check
message="owner: a check already runs (process $checking)"
expect "$message" "$actual"
expect "$(history_line skipped "$message")" "$(tail -n 1 "$state/owner.history")"
# Talk waits across several polls, prints its notice once, then takes the lock.
CLAUDE_TEST_MODE=ok "$BASH" "$session" talk --continue </dev/null > "$fixture/waiting-talk.out" &
waiting=$!
echo "$waiting" > "$fixture/waiting.parent"
await test -s "$fixture/waiting-talk.out"
await grep -q $'\tcheck\ttimeout\t' "$state/owner.history"
code=0
wait "$checking" || code=$?
[ "$code" = 124 ]
await fixture_dead "$check_claude"
await fixture_dead "$check_child"
wait "$waiting"
expect "$(printf '%s\n' "owner: a check runs (process $checking); waiting for it to end" 'first output' 'last output')" "$(cat "$fixture/waiting-talk.out")"
expect talk "$(tail -n 1 "$fixture/claude-lock-kinds")"
expect_history timeout waiting
rm "$fixture/check.parent" "$fixture/waiting.parent" "$fixture/claude-child" "$fixture/claude-child-ready" "$fixture/claude-ready"
unset OWNER_CHECK_MINUTES

export CLAUDE_TEST_MODE=hold
"$BASH" "$session" check > "$fixture/check.out" 2>&1 &
checking=$!
echo "$checking" > "$fixture/check.parent"
await test -s "$fixture/claude-ready"
kill -HUP "$checking"
code=0
wait "$checking" || code=$?
[ "$code" = 129 ]
expect_history 'failed 129' waiting
rm "$fixture/check.parent" "$fixture/claude-ready"

# Use a clock jump to model a sleeping machine. The next check stays on
# the original cadence even if the first check fails.
(exec 6<> "$fixture/hold"; read -r _ <&6) &
impostor=$!
echo "$impostor" > "$fixture/loop-impostor.parent"
echo "$impostor" > "$state/owner-loop.pid"
printf '%s %s loop run %s\n' "$BASH" "$session" "$fixture/other state" > "$fixture/commands/$impostor"
invoke 1 "$BASH" "$session" loop status
expect "$(printf '%s\n' 'owner loop is not running' "$(tail -n 3 "$state/owner.history")")" "$actual"
expect 'owner loop is not running' "$("$BASH" "$session" loop stop)"
fixture_alive "$impostor"
printf 'close\n' > "$fixture/hold"
wait "$impostor"
rm "$fixture/loop-impostor.parent"

export CLAUDE_TEST_MODE=fail OWNER_CHECK_SECONDS=2 OWNER_POLL_SECONDS=1
echo 1795168800 > "$fixture/clock"
sleep_mark=$(awk 'END {print NR}' "$fixture/sleeps")
start_count=$(awk 'END {print NR}' "$state/owner.history")
invoke 0 "$BASH" "$session" loop start
loop=$(cat "$state/owner-loop.pid")
expect "owner loop started (process $loop)" "$actual"
expect 'owner loop already runs' "$("$BASH" "$other_session" loop start)"
expect "$loop" "$(cat "$fixture/loop-launches")"
history_count() {
  local count
  count=$(awk 'END {print NR}' "$state/owner.history")
  [ "$count" -ge "$1" ]
}
await history_count "$((start_count + 1))"
await test ! -e "$state/owner.lock"
expect_history 'failed 7' 'fixture failure'
expect 1795168802 "$(cat "$state/owner-loop.next")"
first_poll() {
  tail -n "+$((sleep_mark + 1))" "$fixture/sleeps" | grep -Fxq 1
}
await first_poll
echo 1795168811 > "$fixture/clock"
await history_count "$((start_count + 2))"
expect 1795168812 "$(cat "$state/owner-loop.next")"
next=$("$FIXTURE_DATE" -d @1795168812 '+%Y-%m-%d %H:%M:%S' 2>/dev/null ||
  "$FIXTURE_DATE" -r 1795168812 '+%Y-%m-%d %H:%M:%S')
expect "$(printf '%s\n' "owner loop is running (process $loop)" "next check: $next" "$(tail -n 3 "$state/owner.history")")" \
  "$("$BASH" "$other_session" loop status)"
echo 1795168800 > "$fixture/clock"
next_is_reset() { [ "$(cat "$state/owner-loop.next")" = 1795168802 ]; }
await next_is_reset
[ "$(awk 'END { print NR }' "$state/owner.history")" = "$((start_count + 2))" ]
echo 1795168802 > "$fixture/clock"
await history_count "$((start_count + 3))"
expect 1795168804 "$(cat "$state/owner-loop.next")"
expect 'owner loop stopped' "$("$BASH" "$other_session" loop stop)"
[ ! -e "$state/owner-loop.pid" ]
[ ! -e "$state/owner-loop.next" ]
[ ! -e "$state/owner.lock" ]
await fixture_dead "$loop"
invoke 1 "$BASH" "$session" loop status
expect "$(printf '%s\n' 'owner loop is not running' "$(tail -n 3 "$state/owner.history")")" "$actual"

unset OWNER_CHECK_SECONDS OWNER_POLL_SECONDS
invoke 0 "$BASH" "$session" loop start
await history_count "$((start_count + 4))"
expect 1795176002 "$(cat "$state/owner-loop.next")"
loop=$(cat "$state/owner-loop.pid")
await grep -Fxq 30 "$fixture/sleeps"
expect 'owner loop stopped' "$("$BASH" "$session" loop stop)"
await fixture_dead "$loop"

export CLAUDE_TEST_MODE=hold
invoke 0 "$BASH" "$session" loop start
await test -s "$fixture/claude-ready"
loop=$(cat "$state/owner-loop.pid")
check_claude=$(cat "$fixture/claude-pid")
expect check "$(cat "$state/owner.lock/kind")"
expect 'owner loop stopped' "$("$BASH" "$session" loop stop)"
await fixture_dead "$loop"
await fixture_dead "$check_claude"
expect_history 'failed 143' waiting
echo 'owner-session-test: session arguments, locking, history, timeout and loop pass'

agy_settings=$checkout/architecture/owner/agy-settings.json
agy_args() {
  local input=$1
  shift
  printf 'cwd=<%s> stdin=<%s>\n' "$checkout" "$input"
  printf 'arg=<%s>\n' --agent harness-owner "$@"
}
expected_env=$(printf '%s\n' 1 "$checkout")
default_agy=(--model gemini-4-carbon-high --settings "$agy_settings")
check_prompt=(-p 'Run one harness check and stop.')
unset CLAUDE_TEST_MODE HARNESS_OWNER_SESSION CLAUDE_PROJECT_DIR
HARNESS_OWNER_CLI='' invoke 0 "$BASH" "$session" talk <<< 'terminal input'
expect 'Antigravity check output' "$actual"
expect "$(agy_args 'terminal input' "${default_agy[@]}" --dangerously-skip-permissions)" "$(cat "$fixture/agy-args")"
expect "$expected_env" "$(cat "$fixture/agy-env")"

# Without a project model, the session supplies the same default.
sed '/^HARNESS_OWNER_MODEL=/d' "$checkout/architecture/project.env" > "$fixture/project.env"
mv "$fixture/project.env" "$checkout/architecture/project.env"
HARNESS_OWNER_CLI=agy invoke 0 "$BASH" "$session" talk </dev/null
expect "$(agy_args EOF "${default_agy[@]}" --dangerously-skip-permissions)" "$(cat "$fixture/agy-args")"
fixture_copy architecture/project.env

HARNESS_OWNER_CLI=agy invoke 0 "$BASH" "$session" talk --model 'custom model' 'hello world' </dev/null
expect "$(agy_args EOF --settings "$agy_settings" --dangerously-skip-permissions --model 'custom model' 'hello world')" \
  "$(cat "$fixture/agy-args")"
HARNESS_OWNER_CLI=agy invoke 0 "$BASH" "$session" talk --model=other </dev/null
expect "$(agy_args EOF --settings "$agy_settings" --dangerously-skip-permissions --model=other)" "$(cat "$fixture/agy-args")"
HARNESS_OWNER_CLI=agy invoke 0 "$BASH" "$session" talk --mode plan </dev/null
expect "$(agy_args EOF "${default_agy[@]}" --mode plan)" "$(cat "$fixture/agy-args")"
HARNESS_OWNER_CLI=agy invoke 0 "$BASH" "$session" talk --model=other --mode=plan </dev/null
expect "$(agy_args EOF --settings "$agy_settings" --model=other --mode=plan)" "$(cat "$fixture/agy-args")"
HARNESS_OWNER_CLI=agy invoke 0 "$BASH" "$session" talk --dangerously-skip-permissions </dev/null
expect "$(agy_args EOF "${default_agy[@]}" --dangerously-skip-permissions)" "$(cat "$fixture/agy-args")"
HARNESS_OWNER_CLI=agy invoke 0 "$BASH" "$session" talk -- --mode --model=other </dev/null
expect "$(agy_args EOF "${default_agy[@]}" --dangerously-skip-permissions -- --mode --model=other)" \
  "$(cat "$fixture/agy-args")"
HARNESS_OWNER_CLI=agy invoke 0 "$BASH" "$session" talk -i '--mode=plan' </dev/null
expect "$(agy_args EOF "${default_agy[@]}" --dangerously-skip-permissions -i '--mode=plan')" \
  "$(cat "$fixture/agy-args")"
HARNESS_OWNER_CLI=agy invoke 0 "$BASH" "$session" talk -i '--' --mode plan </dev/null
expect "$(agy_args EOF "${default_agy[@]}" -i '--' --mode plan)" \
  "$(cat "$fixture/agy-args")"
HARNESS_OWNER_CLI=agy invoke 0 "$BASH" "$session" talk -model custom -mode=plan </dev/null
expect "$(agy_args EOF --settings "$agy_settings" -model custom -mode=plan)" \
  "$(cat "$fixture/agy-args")"

unset OWNER_PERMISSION_MODE
HARNESS_OWNER_CLI=agy invoke 0 "$BASH" "$session" check
expect '' "$actual"
expect "$(agy_args EOF "${default_agy[@]}" --dangerously-skip-permissions "${check_prompt[@]}")" "$(cat "$fixture/agy-args")"
expect_history ok 'Antigravity check output'
HARNESS_OWNER_CLI=agy OWNER_PERMISSION_MODE='' invoke 0 "$BASH" "$session" check
expect '' "$actual"
expect "$(agy_args EOF "${default_agy[@]}" --dangerously-skip-permissions "${check_prompt[@]}")" "$(cat "$fixture/agy-args")"
expect_history ok 'Antigravity check output'
HARNESS_OWNER_CLI=agy HARNESS_OWNER_MODEL='configured model' OWNER_PERMISSION_MODE=fixture \
  HARNESS_OWNER_SESSION=0 CLAUDE_PROJECT_DIR=$fixture \
  invoke 0 "$BASH" "$session" check <<< 'must not reach Antigravity'
expect '' "$actual"
expect "$(agy_args EOF --model 'configured model' --settings "$agy_settings" --mode fixture "${check_prompt[@]}")" \
  "$(cat "$fixture/agy-args")"
expect "$expected_env" "$(cat "$fixture/agy-env")"
expect_history ok 'Antigravity check output'

saved_args=$(cat "$fixture/agy-args")
saved_logs=$(printf '%s\n' "$state"/owner-checks/*.log)
HARNESS_OWNER_CLI=agy OWNER_PERMISSION_MODE=$' \t' invoke 2 "$BASH" "$session" check
expect 'owner: OWNER_PERMISSION_MODE must be non-empty' "$actual"
expect_history failed "$actual"
expect "$saved_args" "$(cat "$fixture/agy-args")"
expect "$saved_logs" "$(printf '%s\n' "$state"/owner-checks/*.log)"

invoke 0 "$BASH" "$session" talk </dev/null
expect "$expected_env" "$(cat "$fixture/claude-env")"
rm "$fixture/claude-env"
OWNER_PERMISSION_MODE='' invoke 0 "$BASH" "$session" check
expect "$expected_env" "$(cat "$fixture/claude-env")"
expect "$(check_args auto)" "$(cat "$fixture/claude-args")"

for action in talk check; do
  HARNESS_OWNER_CLI=unknown invoke 2 "$BASH" "$session" "$action" </dev/null
  expect 'owner: HARNESS_OWNER_CLI must be agy or claude' "$actual"
  HARNESS_OWNER_CLI=agy FIXTURE_NO_AGY=1 invoke 127 "$BASH" "$session" "$action" </dev/null
  expect 'owner: agy is not on PATH' "$actual"
  [ ! -e "$state/owner.lock" ]
done
HARNESS_OWNER_CLI=agy HARNESS_OWNER_MODEL=$' \t' invoke 2 "$BASH" "$session" check
expect 'owner: HARNESS_OWNER_MODEL must be non-empty' "$actual"
expect_history failed "$actual"

missing_files='owner: harness-owner or owner settings missing'
saved_args=$(cat "$fixture/agy-args")
for file in .agents/agents/harness-owner.md .agents/hooks.json architecture/owner/agy-settings.json; do
  mv "$checkout/$file" "$fixture/removed"
  HARNESS_OWNER_CLI=agy invoke 1 "$BASH" "$session" talk </dev/null
  expect "$missing_files" "$actual"
  HARNESS_OWNER_CLI=agy invoke 1 "$BASH" "$session" check
  expect "$missing_files" "$actual"
  expect_history failed "$missing_files"
  expect "$saved_args" "$(cat "$fixture/agy-args")"
  [ ! -e "$state/owner.lock" ]
  mv "$fixture/removed" "$checkout/$file"
done
mv "$agy_settings" "$fixture/removed"
mkdir "$agy_settings"
HARNESS_OWNER_CLI=agy invoke 1 "$BASH" "$session" talk </dev/null
expect "$missing_files" "$actual"
rmdir "$agy_settings"
mv "$fixture/removed" "$agy_settings"
chmod a-r "$agy_settings"
if [ ! -r "$agy_settings" ]; then
  HARNESS_OWNER_CLI=agy invoke 1 "$BASH" "$session" check
  expect "$missing_files" "$actual"
  expect_history failed "$missing_files"
fi
chmod u+r "$agy_settings"
mv "$checkout/.agents/hooks.json" "$fixture/removed"
echo '{}' > "$checkout/.agents/hooks.json"
HARNESS_OWNER_CLI=agy invoke 1 "$BASH" "$session" talk </dev/null
expect "$missing_files" "$actual"
HARNESS_OWNER_CLI=agy invoke 1 "$BASH" "$session" check
expect "$missing_files" "$actual"
expect_history failed "$missing_files"
expect "$saved_args" "$(cat "$fixture/agy-args")"
mv "$fixture/removed" "$checkout/.agents/hooks.json"

for runner in claude agy; do
  (cd "$checkout" && exec "$runner") </dev/null &
  plain=$!
  echo "$plain" > "$fixture/plain.parent"
  await test -s "$fixture/plain-ready"
  HARNESS_OWNER_CLI=$runner invoke 0 "$BASH" "$session" check
  case "$runner" in
    claude) expect_history ok 'last output' ;;
    agy) expect_history ok 'Antigravity check output' ;;
  esac
  fixture_alive "$plain"
  printf 'close\n' > "$fixture/hold"
  wait "$plain"
  rm "$fixture/plain.parent" "$fixture/plain-ready"
done

start_count=$(awk 'END { print NR }' "$state/owner.history")
HARNESS_OWNER_CLI=agy invoke 0 "$BASH" "$session" loop start
loop=$(cat "$state/owner-loop.pid")
await history_count "$((start_count + 1))"
await test ! -e "$state/owner.lock"
expect_history ok 'Antigravity check output'
expect "$(agy_args EOF "${default_agy[@]}" --dangerously-skip-permissions "${check_prompt[@]}")" \
  "$(cat "$fixture/agy-args")"
expect 'owner loop stopped' "$("$BASH" "$session" loop stop)"
await fixture_dead "$loop"
echo 'owner-session-test: Antigravity talk, check, concurrent plain sessions and loop pass'
