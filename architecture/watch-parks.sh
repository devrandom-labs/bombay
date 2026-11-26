#!/usr/bin/env bash
# Print new parks of named cards and parks that do not wait behind another card.
# Usage: watch-parks.sh <worktree parent> [card ...]
#        watch-parks.sh --filter [card ...] < history lines
# WATCH_POLL sets the seconds between reads (default 30). This tool only reads.
set -euo pipefail

usage() {
  echo 'usage: watch-parks.sh <worktree parent> [card ...]' >&2
  echo '       watch-parks.sh --filter [card ...] < history lines' >&2
  exit 2
}

[ "$#" -gt 0 ] || usage
root=$1
shift
named=
for card in "$@"; do
  [[ $card =~ ^[1-9][0-9]*$ ]] || usage
  named="${named:+$named|}parked #$card:"
done

wake_lines() {
  local parks
  parks=$(grep -E 'parked #[0-9]+:' || true)
  [ -n "$parks" ] || return 0
  {
    printf '%s\n' "$parks" | grep -Ev 'parked #[0-9]+: blocked #[0-9]+' || true
    if [ -n "$named" ]; then printf '%s\n' "$parks" | grep -E "$named" || true; fi
  } | awk '!seen[$0]++'
}

if [ "$root" = --filter ]; then wake_lines; exit 0; fi
[ -d "$root" ] || usage

# shellcheck source=architecture/project.env
source "$(dirname -- "$0")/project.env"
read -ra lanes <<< "$HARNESS_LANES"
seen=()
for index in "${!lanes[@]}"; do
  lane=${lanes[$index]}
  file=$root/$lane/.cache/run.history
  seen[index]=$(wc -l 2>/dev/null < "$file" || echo 0)
done
while true; do
  for index in "${!lanes[@]}"; do
    lane=${lanes[$index]}
    file=$root/$lane/.cache/run.history
    now=$(wc -l 2>/dev/null < "$file") || continue
    now=$((now))
    [ "$now" -ge "${seen[$index]}" ] || seen[index]=0
    [ "$now" -gt "${seen[$index]}" ] || continue
    new=$(sed -n "$((seen[index] + 1)),${now}p;${now}q" "$file" 2>/dev/null | wake_lines) || continue
    seen[index]=$now
    if [ -n "$new" ]; then printf '%s: %s\n' "$lane" "$new"; exit 0; fi
  done
  sleep "${WATCH_POLL:-30}"
done
