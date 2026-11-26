#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=architecture/tests/start-fixture.sh
. "$(dirname -- "$0")/start-fixture.sh"
fixture_init
fixture_copy architecture/lane.sh
lane_script=$checkout/architecture/lane.sh
cleanup() {
  local code=$? file pid
  for file in "$fixture"/children/* "$fixture"/{server,studio,nix}.pid; do
    if [ -r "$file" ] && IFS= read -r pid < "$file"; then
      kill -TERM "$pid" 2>/dev/null || true
      case "${file##*/}" in
        *[!0-9]*) ;;
        *) kill -TERM "${file##*/}" 2>/dev/null || true ;;
      esac
    fi
  done
  fixture_cleanup "$code"
}
trap cleanup EXIT

mkdir "$fixture/children" "$fixture/builds"
mkfifo "$fixture/hold"
export HARNESS_BUILDS="$fixture/builds" FIXTURE_NOHUP
FIXTURE_NOHUP=$(command -v nohup)
fixture_stub nohup <<'STUB'
[ "$1 $2 $3 $4 $6" = "env -i $BASH -c lane" ] || exit 2
printf 'nohup\t%s\n' "${*:7}" >> "$FIXTURE/wrappers"
exec "$FIXTURE_NOHUP" "$@"
STUB
fixture_stub gh <<'STUB'
printf '%s\n' "$*" >> "$FIXTURE/gh"
[ "$#" = 2 ] && [ "$1" = auth ] && [ "$2" = token ] || exit 2
[ "${FIXTURE_NO_LOGIN:-0}" = 0 ] || exit 1
echo fixture-active-token
STUB
fixture_stub sudo <<'STUB'
printf '%s\n' "$@" > "$FIXTURE/sudo"
[ "$#" = 4 ] && [ "$1 $2 $3" = 'rm -rf --' ]
rmdir -- "$4"
STUB
fixture_stub pgrep <<'STUB'
case "$1" in
  -f)
    if [ -s "$FIXTURE/agent" ] && grep -Eq -- "$2" "$FIXTURE/agent-command"; then
      cat "$FIXTURE/agent"
    fi
    ;;
  -P) if [ -s "$FIXTURE/children/$2" ]; then cat "$FIXTURE/children/$2"; fi ;;
  *) exit 2 ;;
esac
STUB
fixture_stub lsof <<'STUB'
[ "$1 $2 $4 $5 $6" = '-a -p -d cwd -Fn' ]
printf 'p%s\nn%s\n' "$3" "$(cat "$FIXTURE/agent-cwd")"
STUB
fixture_stub caffeinate <<'STUB'
FIXTURE=${HOME%/home}
printf 'caffeinate\t%s\n' "$*" >> "$FIXTURE/wrappers"
[ "$1" = -i ] || exit 2
shift
exec "$@"
STUB
fixture_stub direnv <<'STUB'
FIXTURE=${HOME%/home}
printf 'direnv\t%s\n' "$*" >> "$FIXTURE/wrappers"
case "$1 $2" in
  'allow .') exit 0 ;;
  'exec .') shift 2; exec "$@" ;;
  *) exit 2 ;;
esac
STUB
fixture_stub df <<'STUB'
[ "$1" = -Pk ]
printf 'Filesystem 1024-blocks Used Available\nfixture 99999999 100 33554432\n'
STUB
fixture_stub date <<'STUB'
[ "$1" = '+%Y-%m-%d %H:%M' ]
echo '2026-11-20 10:00'
STUB
cat > "$checkout/architecture/run.sh" <<'STUB'
# started again with the supervisor of main
set -eu
FIXTURE=${HOME%/home}
name=$1
printf '%s %s %s\n' "$BASH" "$0" "$1" > "$FIXTURE/commands/$$"
printf '%s\n' "$$" > "$FIXTURE/$name.pid"
(
  trap 'echo stopped > "$FIXTURE/$$.child-ended"; exit' TERM
  exec 4<> "$FIXTURE/hold"
  echo ready > "$FIXTURE/$$.child-ready"
  read -r unused <&4
) &
child=$!
printf '%s\n' "$child" > "$FIXTURE/children/$$"
trap 'wait "$child"; echo stopped > "$FIXTURE/$$.ended"; exit' TERM
printf '%s\t%s\t%s\t%s\t%s\n' "$0" "$1" "$PWD" "$STEP_LEAD" "${GH_TOKEN:-unset}" >> "$FIXTURE/launches"
printf '%s\n' "${LANE_EXTRA-unset}" "${GATE_LOCK-unset}" "${GATE_KEEP-unset}" > "$FIXTURE/$name.environment"
exec 4<> "$FIXTURE/hold"
echo ready > "$FIXTURE/$$.ready"
# Return from the read so Bash can run TERM when the child exits at the same time.
while ! read -r -t 1 unused <&4; do :; done
STUB
echo ':' > "$checkout/.envrc"
fixture_commit
fixture_remote
base=$(git -C "$checkout" rev-parse HEAD)
git -C "$checkout" worktree add -q --detach "$worktrees/server" origin/trunk
touch "$fixture/process-table"

# Resolve the first checkout from a tool in a linked worktree and write nothing.
expect "$(where_lines "$checkout" "$worktrees/server" "$worktrees" "$state" 'lane server')" \
  "$("$BASH" "$worktrees/server/architecture/lane.sh" --where)"
[ ! -e "$state" ]
[ ! -e "$fixture/launches" ]

invoke 2 "$BASH" "$lane_script" start extra
expect 'lane: unknown lane extra (choose from server studio nix)' "$actual"
invoke 2 "$BASH" "$lane_script" stop extra
expect 'lane: unknown lane extra (choose from server studio nix)' "$actual"

export STEP_LEAD='fixture lead'
echo "$$" > "$fixture/agent"
echo "$worktrees/server" > "$fixture/agent-cwd"
echo 'claude fixture' > "$fixture/agent-command"
invoke 1 "$BASH" "$lane_script" start server
expect "$(printf '%s\n' "server: process $$ already works in $worktrees/server:" '00:01 claude fixture' 'Let it finish and close it, then run this again.')" "$actual"
[ ! -e "$fixture/launches" ]
for runner in agy antigravity; do
  printf '/opt/homebrew/bin/%s --agent harness-owner\n' "$runner" > "$fixture/agent-command"
  FIXTURE_AGENT_NAME=$runner invoke 1 "$BASH" "$lane_script" start server
  expect "$(printf '%s\n' "server: process $$ already works in $worktrees/server:" "00:01 $runner fixture" 'Let it finish and close it, then run this again.')" "$actual"
  [ ! -e "$fixture/launches" ]
done
for command_line in 'vim agy-settings.json' 'vim /tmp/agy'; do
  printf '%s\n' "$command_line" > "$fixture/agent-command"
  FIXTURE_NO_LOGIN=1 invoke 1 "$BASH" "$lane_script" start server
  expect $'lane: no leftover Nix build folders\nlane: sign in with gh auth login or set GH_TOKEN' "$actual"
  [ ! -e "$fixture/launches" ]
done
rm "$fixture/gh"
: > "$fixture/agent"

# Only the ended build counts. A live build, a symlink and an unrelated folder stay.
mkdir "$HARNESS_BUILDS/nix-999999999-0" "$HARNESS_BUILDS/nix-123-0" "$HARNESS_BUILDS/nix-other"
ln -s "$HARNESS_BUILDS/nix-other" "$HARNESS_BUILDS/nix-999999998-0"
printf '%s bash\n123 nix-daemon\n' "$$" > "$fixture/process-table"
# Include each launcher PID in the synthetic process listing, like ps -A would.
mv "$fixture/bin/ps" "$fixture/bin/ps-probe"
fixture_stub ps <<'STUB'
if [ "$*" = '-A -o pid=,comm=' ]; then
  if [ "${SWEEP_PARTIAL:-0}" = 0 ]; then
    for file in "$FIXTURE"/lane-probe-pid.*; do
      [ -s "$file" ] && printf '%s bash\n' "${file##*.}"
    done
  fi
fi
exec "$FIXTURE/bin/ps-probe" "$@"
STUB

# Run sweep and start through a shell that records the PID before exec.
launch_lane() {
  # shellcheck disable=SC2016 # The child records its own PID before exec.
  "$BASH" -c 'printf "%s\n" "$$" > "$FIXTURE/lane-probe-pid.$$"; exec "$@"' fixture-lane "$BASH" "$lane_script" "$@"
}
sweep=$(printf 'lane: 1 leftover Nix build folders; run in a terminal: bash %q sweep' "$lane_script")
expect "$sweep" "$(launch_lane sweep </dev/null)"
[ ! -e "$fixture/sudo" ]
[ -d "$HARNESS_BUILDS/nix-999999999-0" ]
expect 'lane: no leftover Nix build folders' "$(SWEEP_PARTIAL=1 launch_lane sweep </dev/null)"
[ ! -e "$fixture/sudo" ]

# Some Nix build sandboxes have no PTY. The nonterminal path always runs.
pty() {
  if [[ $OSTYPE == darwin* ]]; then script -q /dev/null "$@"
  else script -qec "$(printf '%q ' "$@")" /dev/null; fi
}
if command -v script >/dev/null && pty "$BASH" -c '[ -t 0 ]' </dev/null >/dev/null 2>&1; then
  # shellcheck disable=SC2016 # The child records its own PID before exec.
  pty "$BASH" -c 'printf "%s\n" "$$" > "$FIXTURE/lane-probe-pid.$$"; exec "$@"' fixture-lane \
    "$BASH" "$lane_script" sweep </dev/null > "$fixture/sweep-terminal"
  expect $'lane: removing 1 leftover Nix build folders; this needs your password\nlane: 32 GiB are free now (before: 32 GiB)' \
    "$(tr -d '\r' < "$fixture/sweep-terminal" | sed -n 's/^.*lane: /lane: /p')"
  expect "$(printf 'rm\n-rf\n--\n%s' "$HARNESS_BUILDS/nix-999999999-0")" "$(cat "$fixture/sudo")"
  [ ! -e "$HARNESS_BUILDS/nix-999999999-0" ]
  mkdir "$HARNESS_BUILDS/nix-999999999-0"
  rm "$fixture/sudo"
fi

head=$(fixture_advance upstream)
export GH_TOKEN=fixture-token
export LANE_EXTRA=outside GATE_LOCK="$fixture/other gate.lock" GATE_KEEP="$fixture/gate links"
launch_message() {
  printf '%s: started in the background.\n  status:  bash %q status\n  follow:  tail -f %q\n  stop:    bash %q stop %s\n' \
    "$1" "$lane_script" "$worktrees/$1/.cache/run.log" "$lane_script" "$1"
}
ready_to_stop() {
  [ -s "$fixture/$1.ready" ] && [ -s "$fixture/$1.child-ready" ]
}
assert_stopped() {
  local pid=$1 child
  child=$(cat "$fixture/children/$pid")
  await test -s "$fixture/$pid.child-ended"
  await test -s "$fixture/$pid.ended"
  await fixture_dead "$child"
  await fixture_dead "$pid"
}
invoke 0 launch_lane start server </dev/null
expect "$sweep"$'\n'"$(launch_message server)" "$actual"
pid=$(cat "$worktrees/server/.cache/run.pid")
await ready_to_stop "$pid"
await test -s "$fixture/server.environment"
expect "$(printf 'unset\nunset\n%s' "$GATE_KEEP")" "$(cat "$fixture/server.environment")"
expect "$(printf '%s\tserver\t%s\tfixture lead\tfixture-token' "$worktrees/server/architecture/run.sh" "$worktrees/server")" "$(cat "$fixture/launches")"
expect "$(printf 'direnv\tallow .\nnohup\tdirenv exec . caffeinate -i %s %s server\ndirenv\texec . caffeinate -i %s %s server\ncaffeinate\t-i %s %s server' \
  "$BASH" "$worktrees/server/architecture/run.sh" "$BASH" "$worktrees/server/architecture/run.sh" \
  "$BASH" "$worktrees/server/architecture/run.sh")" "$(cat "$fixture/wrappers")"
expect '--- started 2026-11-20 10:00 ---' "$(cat "$worktrees/server/.cache/run.log")"
[ ! -e "$fixture/gh" ]
[ ! -e "$fixture/sudo" ]
[ "$(git -C "$checkout" rev-parse HEAD)" = "$head" ]
[ "$(git -C "$worktrees/server" rev-parse HEAD)" = "$head" ]
[ "$pid" = "$(cat "$fixture/server.pid")" ]
unset STEP_LEAD GH_TOKEN
echo '2026-11-20 09:00' > "$worktrees/server/.cache/run.stopped"
invoke 0 launch_lane start server
expect $'server already runs.\n'"$(launch_lane status)" "$actual"
[ "$(awk 'END {print NR}' "$fixture/launches")" = 1 ]
[ ! -e "$worktrees/server/.cache/run.stopped" ]

echo 'ready card' > "$worktrees/server/.cache/run.status"
printf '%s\n' first second third fourth fifth sixth seventh eighth ninth tenth > "$worktrees/server/.cache/run.history"
disk=$(printf 'disk: 1 leftover Nix build folders; remove them with: bash %q sweep' "$lane_script")
expect "$(
  printf '%s\n' 'server: RUNNING' '  now: ready card' '  last sessions:' \
    '    third' '    fourth' '    fifth' '    sixth' '    seventh' '    eighth' '    ninth' '    tenth' \
    'studio: not running' '  no worktree yet' 'nix: not running' '  no worktree yet' \
    'disk: 32 GiB free; a session needs 15' "$disk"
)" "$(launch_lane status)"

expect 'server: stopped' "$(launch_lane stop server)"
assert_stopped "$pid"
[ ! -e "$worktrees/server/.cache/run.pid" ]
expect '2026-11-20 10:00' "$(cat "$worktrees/server/.cache/run.stopped")"
expect 'server: stopped by a person on 2026-11-20 10:00' "$(launch_lane status | head -n 1)"
invoke 0 launch_lane start server --unless-stopped
expect 'server: stopped by a person on 2026-11-20 10:00; start it with: bash architecture/lane.sh start server' "$actual"
[ "$(awk 'END {print NR}' "$fixture/launches")" = 1 ]

# A stale PID or a live unrelated command may never be signalled.
(exec 4<> "$fixture/hold"; read -r _ <&4) &
unrelated=$!
echo "$unrelated" > "$fixture/children/unrelated"
for impostor in 999999999 "$unrelated"; do
  echo "$impostor" > "$worktrees/server/.cache/run.pid"
  echo "echo architecture/run.sh server" > "$fixture/commands/$impostor"
  rm -f "$worktrees/server/.cache/run.stopped"
  expect 'server: not running' "$(launch_lane status | head -n 1)"
  expect 'server: was not running' "$(launch_lane stop server)"
  [ ! -e "$worktrees/server/.cache/run.pid" ]
done
kill -0 "$unrelated"
kill -TERM "$unrelated"
wait "$unrelated" 2>/dev/null || true
rm "$fixture/children/unrelated"
[ -f "$worktrees/server/.cache/run.stopped" ]
# Hold the first start inside gh, after it has checked the PID. The second
# must wait for that launch and then see the supervisor.
fixture_stub gh <<'STUB'
printf '%s\n' "$*" >> "$FIXTURE/gh"
[ "$*" = 'auth token' ] || exit 2
[ "${FIXTURE_NO_LOGIN:-0}" = 0 ] || exit 1
if [ "${HOLD_LOGIN:-0}" = 1 ]; then
  echo ready > "$FIXTURE/login-ready"
  exec 7<> "$FIXTURE/login"
  read -r ignored <&7
fi
echo fixture-active-token
STUB
mkfifo "$fixture/login"
HOLD_LOGIN=1 launch_lane start server </dev/null > "$fixture/first-start" 2>&1 &
first_start=$!
await test -s "$fixture/login-ready"
launch_lane start server </dev/null > "$fixture/second-start" 2>&1 &
second_start=$!
sleep 0.5
printf 'continue\n' > "$fixture/login"
wait "$first_start"
wait "$second_start"
expect 'server: started in the background.' "$(grep '^server:' "$fixture/first-start")"
expect 'server already runs.' "$(head -n 1 "$fixture/second-start")"
expect 2 "$(awk 'END {print NR}' "$fixture/launches")"
expect "$(printf '%s\tserver\t%s\tcodex exec -m gpt-6.2-sol -s danger-full-access\tfixture-active-token' \
  "$worktrees/server/architecture/run.sh" "$worktrees/server")" "$(tail -n 1 "$fixture/launches")"
[ ! -e "$worktrees/server/.cache/run.stopped" ]
new_pid=$(cat "$worktrees/server/.cache/run.pid")
await ready_to_stop "$new_pid"
expect 'server: stopped' "$(launch_lane stop server)"
assert_stopped "$new_pid"
: > "$fixture/gh"

# A clean, missing worktree uses the configured main; changes in the main
# checkout prevent its update. A dirty pre-marker lane still uses the fallback.
export STEP_LEAD='fixture lead'
printf 'local\n' >> "$checkout/tracked"
new=$(fixture_advance third)
FIXTURE_NO_LOGIN=1 invoke 1 launch_lane start nix </dev/null
expect "$sweep"$'\nlane: sign in with gh auth login or set GH_TOKEN' "$actual"
[ ! -e "$worktrees/nix" ]
invoke 0 launch_lane start nix </dev/null
expect "$sweep"$'\n'"lane: making the worktree of the nix lane at $worktrees/nix"$'\n'"$(launch_message nix)" "$actual"
nix_pid=$(cat "$worktrees/nix/.cache/run.pid")
await ready_to_stop "$nix_pid"
[ "$(git -C "$worktrees/nix" rev-parse HEAD)" = "$new" ]
[ "$(git -C "$worktrees/nix" reflog show --format=%H HEAD | tail -n 1)" = "$new" ]
[ "$(git -C "$checkout" rev-parse HEAD)" = "$head" ]
expect 'nix: stopped' "$(launch_lane stop nix)"
assert_stopped "$nix_pid"

git -C "$checkout" restore tracked
git -C "$checkout" switch -qc work
new=$(fixture_advance fourth)
git -C "$checkout" worktree add -q --detach "$worktrees/studio" "$base"
echo 'exit 88' > "$worktrees/studio/architecture/run.sh"
rm "$worktrees/studio/.envrc"
invoke 0 launch_lane start studio </dev/null
studio_pid=$(cat "$worktrees/studio/.cache/run.pid")
await ready_to_stop "$studio_pid"
rm -f "$worktrees/server/.cache/run.stopped"
printf '%s\n' "$studio_pid" > "$worktrees/server/.cache/run.pid"
expect 'server: not running' "$(launch_lane status | head -n 1)"
expect 'server: was not running' "$(launch_lane stop server)"
fixture_alive "$studio_pid"
expect "$(printf '%s\tstudio\t%s\tfixture lead\tfixture-active-token' "$checkout/architecture/run.sh" "$worktrees/studio")" "$(tail -n 1 "$fixture/launches")"
expect $'auth token\nauth token\nauth token' "$(cat "$fixture/gh")"
expect "$(printf 'caffeinate\t-i %s %s studio' "$BASH" "$checkout/architecture/run.sh")" "$(tail -n 1 "$fixture/wrappers")"
[ "$(grep -c '^direnv' "$fixture/wrappers")" = 6 ]
[ "$(git -C "$checkout" rev-parse HEAD)" = "$head" ]
[ "$(git -C "$checkout" rev-parse origin/trunk)" = "$new" ]
[ "$(git -C "$checkout" branch --show-current)" = work ]
[ "$(git -C "$worktrees/studio" rev-parse HEAD)" = "$base" ]
expect 'studio: stopped' "$(launch_lane stop studio)"
assert_stopped "$studio_pid"

# Hide wrappers that may be available on the host. The lane still starts
# when the shell has neither caffeinate nor direnv.
cat > "$fixture/no-launchers" <<'ENV'
command() {
  case "$*" in '-v caffeinate'|'-v direnv') return 1 ;; esac
  builtin command "$@"
}
ENV
BASH_ENV="$fixture/no-launchers" invoke 0 launch_lane start server </dev/null
pid=$(cat "$worktrees/server/.cache/run.pid")
await ready_to_stop "$pid"
expect "$(printf 'nohup\t%s %s server' "$BASH" "$worktrees/server/architecture/run.sh")" "$(tail -n 1 "$fixture/wrappers")"
expect 'server: stopped' "$(launch_lane stop server)"
assert_stopped "$pid"
echo 'lane-test: start, stop, worktrees, process identity, sweep and status pass'
