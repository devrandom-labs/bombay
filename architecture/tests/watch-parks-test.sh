#!/usr/bin/env bash
# Run from the repository root. An optional script path supports guard tests.
set -euo pipefail

script=${1:-$PWD/architecture/watch-parks.sh}
work=$(mktemp -d)
export HOME=$work/home XDG_STATE_HOME=$work/xdg
mkdir -p "$HOME" "$XDG_STATE_HOME"
trap 'rm -rf "$work"' EXIT
export WATCH_FIXTURE="$work/lane copies" WATCH_POLLS="$work/polls"
mkdir -p "$work/bin" "$WATCH_FIXTURE"/{server,studio,nix}/.cache
export WATCH_SED WATCH_READS="$work/reads"
WATCH_SED=$(command -v sed)
cat > "$work/bin/sed" <<'STUB'
#!/bin/sh
set -eu
file=${3%/.cache/run.history}
echo "${file##*/}" >> "$WATCH_READS"
if [ "$WATCH_CASE" = read-race ] && [ "${file##*/}" = studio ]; then rm "$3"; fi
exec "$WATCH_SED" "$@"
STUB
cat > "$work/bin/sleep" <<'STUB'
#!/bin/sh
set -eu
if [ "$WATCH_CASE" = decision ]; then [ "$1" = 2 ]; else [ "$1" = 30 ]; fi
printf 'poll\n' >> "$WATCH_POLLS"
case "$WATCH_CASE:$(wc -l < "$WATCH_POLLS" | tr -d ' ')" in
  named:1)
    printf '2026-11-20 10:00\tparked #17: blocked #9 pending\n' >> "$WATCH_FIXTURE/server/.cache/run.history"
    printf '2026-11-20 10:01\tsession on #7\n' >> "$WATCH_FIXTURE/studio/.cache/run.history"
    ;;
  named:2)
    printf '2026-11-20 10:02\tparked #7: blocked #9 pending\n' >> "$WATCH_FIXTURE/nix/.cache/run.history"
    ;;
  decision:1)
    printf '2026-11-20 10:03\tparked #2: blocked pilot operator credential decision on #2\n' >> "$WATCH_FIXTURE/studio/.cache/run.history"
    ;;
  restart:1)
    : > "$WATCH_FIXTURE/server/.cache/run.history"
    ;;
  restart:2)
    printf '2026-11-20 10:04\tparked #4: failed again\n' >> "$WATCH_FIXTURE/server/.cache/run.history"
    ;;
  read-race:1)
    printf '2026-11-20 10:05\tparked #5: failed early\n' >> "$WATCH_FIXTURE/studio/.cache/run.history"
    ;;
  read-race:2)
    printf '2026-11-20 10:06\tparked #6: failed again\n' >> "$WATCH_FIXTURE/nix/.cache/run.history"
    ;;
  custom:1)
    printf '2026-11-20 10:07\tparked #8: custom decision\n' >> "$WATCH_FIXTURE/custom/.cache/run.history"
    ;;
  *) echo 'watcher did not wake' >&2; exit 97 ;;
esac
STUB
chmod +x "$work/bin/"*
export PATH="$work/bin:$PATH"
unset WATCH_POLL
export HARNESS_LANES='server studio nix'

printf '2026-11-20 08:00\tparked #7: blocked #6 pending\n' > "$WATCH_FIXTURE/server/.cache/run.history"
WATCH_CASE=named bash "$script" "$WATCH_FIXTURE" 7 70 > "$work/out" 2> "$work/error"
diff -u <(printf 'nix: 2026-11-20 10:02\tparked #7: blocked #9 pending\n') "$work/out"
diff -u <(printf 'poll\npoll\n') "$WATCH_POLLS"
diff -u <(printf 'server\nstudio\nnix\n') "$WATCH_READS"
[ ! -s "$work/error" ]

: > "$WATCH_POLLS"
WATCH_POLL=2 WATCH_CASE=decision bash "$script" "$WATCH_FIXTURE" > "$work/out"
diff -u <(printf 'studio: 2026-11-20 10:03\tparked #2: blocked pilot operator credential decision on #2\n') "$work/out"
diff -u <(printf 'poll\n') "$WATCH_POLLS"

: > "$WATCH_POLLS"
WATCH_CASE=restart bash "$script" "$WATCH_FIXTURE" > "$work/out"
diff -u <(printf 'server: 2026-11-20 10:04\tparked #4: failed again\n') "$work/out"
diff -u <(printf 'poll\npoll\n') "$WATCH_POLLS"

: > "$WATCH_POLLS"
WATCH_CASE=read-race bash "$script" "$WATCH_FIXTURE" > "$work/out" 2> "$work/error"
diff -u <(printf 'nix: 2026-11-20 10:06\tparked #6: failed again\n') "$work/out"
diff -u <(printf 'poll\npoll\n') "$WATCH_POLLS"
[ ! -s "$work/error" ]

: > "$WATCH_POLLS"
mkdir -p "$WATCH_FIXTURE/custom/.cache"
HARNESS_LANES=custom WATCH_CASE=custom bash "$script" "$WATCH_FIXTURE" > "$work/out"
diff -u <(printf 'custom: 2026-11-20 10:07\tparked #8: custom decision\n') "$work/out"
diff -u <(printf 'poll\n') "$WATCH_POLLS"

bash "$script" --filter 7 70 > "$work/out" <<'FIXTURE'
parked #70: blocked #5 wait
parked #7: blocked decision
parked #7: blocked decision
parked #17: blocked #5 wait
parked #71: blocked #5 wait
session on #7
FIXTURE
diff -u <(printf 'parked #7: blocked decision\nparked #70: blocked #5 wait\n') "$work/out"
bash "$script" --filter 7 <<< 'parked #17: blocked #5 wait' > "$work/out"
[ ! -s "$work/out" ]
bash "$script" --filter <<< 'session on #7' > "$work/out"
[ ! -s "$work/out" ]

bad_input() {
  local result=0
  bash "$script" "$@" > "$work/out" 2> "$work/error" || result=$?
  [ "$result" -eq 2 ]
  [ ! -s "$work/out" ]
  diff -u - "$work/error" <<'EXPECTED'
usage: watch-parks.sh <worktree parent> [card ...]
       watch-parks.sh --filter [card ...] < history lines
EXPECTED
}
bad_input
bad_input "$work/missing"
bad_input "$WATCH_FIXTURE" 0
bad_input "$WATCH_FIXTURE" '7|.*'
echo 'watch-parks-test: new named parks and decisions wake, dependencies wait, and bad input fails'
