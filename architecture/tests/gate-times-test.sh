#!/usr/bin/env bash
# Run from the repository root. An optional script path supports guard tests.
set -euo pipefail

script=${1:-$PWD/architecture/gate-times.sh}
work=$(mktemp -d)
export HOME=$work/home XDG_STATE_HOME=$work/xdg
mkdir -p "$HOME" "$XDG_STATE_HOME"
. "$PWD/architecture/tests/own-processes.sh"
own_init "$work"
root="$work/lane copies"
state="$work/gate state"
short="$work/short run"
custom="$work/custom"
cleanup() {
  local result=$?
  own_cleanup TERM
  # The real implementation waits for the followers to leave before removal.
  bash architecture/gate-times.sh stop "$state" "$root" >/dev/null 2>&1 || true
  bash architecture/gate-times.sh stop "$short" "$root" >/dev/null 2>&1 || true
  bash architecture/gate-times.sh stop "$custom" "$root" >/dev/null 2>&1 || true
  rm -rf "$work"
  if [ "$result" != 0 ]; then echo 'gate-times-test: fixture failed' >&2; fi
}
trap cleanup EXIT

mkdir -p "$work/bin" "$root"/{server,studio,nix}/.cache
export GATE_TEST_TAIL GATE_TEST_TIMEOUT GATE_TEST_WORK=$work
GATE_TEST_TAIL=$(command -v tail)
GATE_TEST_TIMEOUT=$(command -v timeout)
cat > "$work/bin/tail" <<'STUB'
#!/bin/sh
trap 'sleep 0.3; echo stopped > "$GATE_TEST_WORK/follower"; exit 0' TERM
"$GATE_TEST_TIMEOUT" --foreground 15 "$GATE_TEST_TAIL" "$@" | tee "$GATE_TEST_WORK/tail"
STUB
printf '#!%s\n' "$BASH" > "$work/bin/timeout"
cat >> "$work/bin/timeout" <<'STUB'
set -eu
. "$OWN_HELPER"
own_record "$$" >/dev/null
printf '%s\n' "$1" >> "$GATE_TEST_WORK/durations"
[ "$1" = 86400 ]
shift
exec "$GATE_TEST_TIMEOUT" "${GATE_TEST_DURATION:-20}" "$@"
STUB
cat > "$work/bin/date" <<'STUB'
#!/bin/sh
[ "$1" = '+%Y-%m-%d %H:%M:%S' ] || exit 98
echo '2026-11-20 11:12:13'
STUB
chmod +x "$work/bin/"*
export PATH="$work/bin:$PATH"
unset GATE_TEST_DURATION
export HARNESS_LANES='server studio nix'

wait_for() {
  local attempt
  for ((attempt = 0; attempt < 200; attempt++)); do
    if "$@"; then return 0; fi
    sleep 0.05
  done
  printf 'gate-times-test: timed out waiting for %s\n' "$*" >&2
  return 1
}
started() { grep -Fq -- "==> $root/$1/.cache/gate.log <==" "$work/tail" 2>/dev/null; }
stopped() { [ "$(bash "$script" status "$1" "$root")" = 'gate-times is not running' ]; }
recorded() { grep -Fq -- "$1" "$state/gate-times.tsv"; }

server="$root/server/.cache/gate.log"
studio="$root/studio/.cache/gate.log"
nix="$root/nix/.cache/gate.log"
printf 'Finished old target(s) in 1s\n' > "$server"
: > "$nix"
[ "$(bash "$script" status "$state" "$root")" = 'gate-times is not running' ]
[ "$(bash "$script" stop "$state" "$root")" = 'gate-times stopped' ]
[ ! -e "$state" ]
(umask 022; bash "$script" start "$state" "$root") > "$work/start1" &
first=$!
(umask 022; bash "$script" start "$state" "$root") > "$work/start2" &
second=$!
wait "$first"
wait "$second"
read -r pid < "$state/gate-times.pid"
[[ $pid =~ ^[1-9][0-9]*$ ]]
sort "$work/start1" "$work/start2" > "$work/starts"
diff -u <(printf 'gate-times is running\ngate-times started (PID %s), output %s/gate-times.tsv\n' "$pid" "$state") "$work/starts"
[ "$(bash "$script" status "$state" "$root")" = 'gate-times is running' ]
wait_for started nix
private=$(find "$state" -type f \( -perm -040 -o -perm -004 \) -print)
[ -z "$private" ] || { printf 'gate-times-test: readable by others: %s\n' "$private" >&2; exit 1; }
kill -HUP "$pid"

cat >> "$server" <<'LOG'
these 2 derivations will be built:
building '/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-a-check.drv'...
building '/nix/store/short-unrelated.drv'...
building '/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-overlong.drv'...
building '/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-no-extension'...
noise from the full gate
    Finished release [optimized] target(s) in 7.00s
labcast-architecture-tests> Running 2 tests
labcast-architecture-tests> first-test: running
labcast-architecture-tests> first-test: still running
labcast-architecture-tests> first-test: two cases pass
labcast-architecture-tests> first-test: 2 checks pass
labcast-architecture-tests> first-test: 2 cases fail
labcast-architecture-tests> first-test: 2 cases pass
labcast-architecture-tests> test result: ok.
     Summary [  3.220s] 5 tests run: 5 passed
LOG
wait_for recorded $'server\tnextest 5 tests run: 5 passed in 3.220 s'
printf 'labcast-architecture-tests> first-test: running in Studio\n' > "$studio"
wait_for recorded $'studio\tself-tests: first line of first-test'
printf '    Finished dev [unoptimized] target(s) in 3s\n' >> "$nix"
wait_for recorded $'nix\tFinished dev [unoptimized] target(s) in 3s'
printf 'building '\''/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-replaced.drv'\''...\n' > "$work/replacement"
mv "$work/replacement" "$server"
wait_for recorded $'server\tbuild replaced'

printf 'not-a-pid\n' > "$state/gate-times.pid"
result=0
bash "$script" stop "$state" "$root" > "$work/out" 2> "$work/error" || result=$?
printf '%s\n' "$pid" > "$state/gate-times.pid"
[ "$result" -eq 1 ]
[ ! -s "$work/out" ]
diff -u <(echo 'gate-times PID is invalid') "$work/error"
[ "$(bash "$script" status "$state" "$root")" = 'gate-times is running' ]
[ "$(bash "$script" stop "$state" "$root")" = 'gate-times stopped' ]
diff -u <(echo stopped) "$work/follower"
[ "$(bash "$script" status "$state" "$root")" = 'gate-times is not running' ]
[ ! -e "$state/gate-times.pid" ]
printf 'Finished after stop target(s) in 1s\n' >> "$server"
{
  printf 'time\tlane\ttext\n'
  while IFS= read -r line; do printf '2026-11-20 11:12:13\t%s\n' "$line"; done <<'EXPECTED'
server	these 2 derivations will be built
server	build a-check
server	Finished release [optimized] target(s) in 7.00s
server	self-tests: Running 2 tests
server	self-tests: first line of first-test
server	self-tests: first-test 2 cases pass
server	self-tests: test result: ok
server	nextest 5 tests run: 5 passed in 3.220 s
studio	self-tests: first line of first-test
nix	Finished dev [unoptimized] target(s) in 3s
server	build replaced
EXPECTED
} | diff -u - "$state/gate-times.tsv"
diff -u <(echo 86400) "$work/durations"

GATE_TEST_DURATION=0.5 bash "$script" start "$short" "$root" > "$work/short-start"
wait_for stopped "$short"
[ "$(bash "$script" stop "$short" "$root")" = 'gate-times stopped' ]
diff -u <(printf '86400\n86400\n') "$work/durations"
diff -u <(printf 'time\tlane\ttext\n') "$short/gate-times.tsv"

cp "$state/gate-times.tsv" "$work/recording"
GATE_TEST_DURATION=0.5 bash "$script" start "$state" "$root" > "$work/restart"
wait_for stopped "$state"
diff -u "$work/recording" "$state/gate-times.tsv"

# The PID comes from this launch. The real tail must leave when that PID dies.
: > "$work/tail"
bash "$script" start "$short" "$root" > "$work/orphan-start"
wait_for started nix
read -r pid < "$short/gate-times.pid"
[[ $pid =~ ^[1-9][0-9]*$ ]]
kill -KILL "$pid"
wait_for stopped "$short"
[ "$(bash "$script" stop "$short" "$root")" = 'gate-times stopped' ]

for blocked in gate-times.tsv gate-times.pid; do
  mkdir -p "$work/blocked $blocked/$blocked"
  result=0
  bash "$script" start "$work/blocked $blocked" "$root" > "$work/out" 2> "$work/error" || result=$?
  [ "$result" -eq 1 ]
  [ ! -s "$work/out" ]
  grep -Fq -- "$blocked: " "$work/error"
  [ "$(bash "$script" status "$work/blocked $blocked" "$root")" = 'gate-times is not running' ]
done
diff -u <(printf '86400\n86400\n86400\n86400\n') "$work/durations"

mkdir -p "$root/custom/.cache"
: > "$root/custom/.cache/gate.log"
HARNESS_LANES=custom bash "$script" start "$custom" "$root" > "$work/custom-start"
wait_for started custom
echo 'Finished custom target(s) in 1s' >> "$root/custom/.cache/gate.log"
wait_for grep -Fq $'custom\tFinished custom target(s) in 1s' "$custom/gate-times.tsv"
[ "$(bash "$script" stop "$custom" "$root")" = 'gate-times stopped' ]
diff -u - "$custom/gate-times.tsv" <<'EXPECTED'
time	lane	text
2026-11-20 11:12:13	custom	Finished custom target(s) in 1s
EXPECTED

mkdir "$work/no-timeout"
ln -s "$(command -v mkdir)" "$work/no-timeout/mkdir"
ln -s "$(command -v dirname)" "$work/no-timeout/dirname"
if [[ $OSTYPE != darwin* ]]; then ln -s "$(command -v flock)" "$work/no-timeout/flock"; fi
result=0
PATH="$work/no-timeout" "$BASH" "$script" start "$work/unavailable" "$root" > "$work/out" 2> "$work/error" || result=$?
[ "$result" -eq 1 ]
[ ! -s "$work/out" ]
diff -u <(echo 'gate-times requires GNU timeout from the Nix shell') "$work/error"

bad_input() {
  local result=0
  bash "$script" "$@" > "$work/out" 2> "$work/error" || result=$?
  [ "$result" -eq 2 ]
  [ ! -s "$work/out" ]
  diff -u <(echo 'usage: gate-times.sh start|status|stop <state folder> <worktree parent>') "$work/error"
}
bad_input
bad_input start "$state"
bad_input start '' "$root"
bad_input start "$state" "$work/missing"
bad_input status "$state" "$root" extra
bad_input unknown "$state" "$root"
echo 'gate-times-test: appended and replaced logs, one recorder, stop, expiry and bad input pass'
