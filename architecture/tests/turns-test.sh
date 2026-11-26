#!/usr/bin/env bash
# Run from the repository root. An optional script path supports guard tests.
set -euo pipefail

script=${1:-$PWD/architecture/turns.sh}
work=$(mktemp -d)
export HOME=$work/home XDG_STATE_HOME=$work/xdg
mkdir -p "$HOME" "$XDG_STATE_HOME"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/bin" "$work/lane copies"/{server,studio}/.cache
root="$work/lane copies"
cat > "$work/bin/date" <<'STUB'
#!/bin/sh
case "$*" in
  '+%s') echo 1762783200 ;;
  '-d @1762783200 +%Y-%m-%d %H:%M') echo '2025-11-10 15:00' ;;
  '-d @1762776000 +%Y-%m-%d %H:%M') echo '2025-11-10 13:00' ;;
  '-r 1762696800 +%Y-%m-%d %H:%M') echo '2025-11-09 15:00' ;;
  *) exit 98 ;;
esac
STUB
chmod +x "$work/bin/date"
export PATH="$work/bin:$PATH"
export HARNESS_LANES='server studio nix'

printf '%s\t%s\n' \
  '2025-11-09 14:59' 'full gate for #1 failed in 90 minutes' \
  '2025-11-09 15:00' 'full gate for #1 passed in 5 minutes' \
  '2025-11-10 12:59' 'waited 3 minutes for the merge turn' \
  '2025-11-10 13:00' 'full gate for #2 failed in 8 minutes' \
  '2025-11-10 13:01' 'full gate for #2 did not end in 10 minutes' \
  '2025-11-10 13:10' 'waited 90 minutes for a Checks run' \
  '2025-11-10 14:55' 'full gate for #3 passed in 2 minutes' \
  '2025-11-10 15:00' 'waited 4 minutes for the merge turn' \
  '2025-11-10 15:01' 'full gate for #4 passed in 99 minutes' > "$root/server/.cache/run.history"
: > "$root/studio/.cache/run.history"

bash "$script" "$root" > "$work/out"
diff -u - "$work/out" <<'EXPECTED'
== gate lock and merge turn, last 24 hours (from run.history) ==
  server: gates 3 = 15 minutes (failed 1 = 8 minutes); waits for the turn 2 = 7 minutes
  studio: gates 0 = 0 minutes (failed 0 = 0 minutes); waits for the turn 0 = 0 minutes
  nix: gates 0 = 0 minutes (failed 0 = 0 minutes); waits for the turn 0 = 0 minutes
EXPECTED

printf '%s\t%s\n' '2025-11-10 14:00' 'waited 7 minutes for the merge turn' > "$root/studio/.cache/run.history"
bash "$script" "$root" 02 > "$work/out"
diff -u - "$work/out" <<'EXPECTED'
== gate lock and merge turn, last 2 hours (from run.history) ==
  server: gates 2 = 10 minutes (failed 1 = 8 minutes); waits for the turn 1 = 4 minutes
  studio: gates 0 = 0 minutes (failed 0 = 0 minutes); waits for the turn 1 = 7 minutes
  nix: gates 0 = 0 minutes (failed 0 = 0 minutes); waits for the turn 0 = 0 minutes
EXPECTED

HARNESS_LANES='studio other' bash "$script" "$root" 2 > "$work/out"
diff -u - "$work/out" <<'EXPECTED'
== gate lock and merge turn, last 2 hours (from run.history) ==
  studio: gates 0 = 0 minutes (failed 0 = 0 minutes); waits for the turn 1 = 7 minutes
  other: gates 0 = 0 minutes (failed 0 = 0 minutes); waits for the turn 0 = 0 minutes
EXPECTED

bad_input() {
  local result=0
  bash "$script" "$@" > "$work/out" 2> "$work/error" || result=$?
  [ "$result" -eq 2 ]
  [ ! -s "$work/out" ]
  diff -u <(echo 'usage: turns.sh <worktree parent> [positive hours]') "$work/error"
}
bad_input
bad_input "$root" 2 extra
bad_input "$work/missing"
for hours in '' 0 -1 2.5 1000000; do bad_input "$root" "$hours"; done
echo 'turns-test: default and custom windows, empty and absent histories, and bad input pass'
