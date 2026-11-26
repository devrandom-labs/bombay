#!/usr/bin/env bash
# Report gate time and merge-turn waits for each lane from run.history.
# Usage: turns.sh <worktree parent> [hours] (default 24). This tool only reads.
set -euo pipefail
export LC_ALL=C
# shellcheck source=architecture/project.env
source "$(dirname -- "$0")/project.env"
read -ra lanes <<< "$HARNESS_LANES"

hours=${2-24}
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ] || [ ! -d "$1" ] || ! [[ $hours =~ ^0*[1-9][0-9]{0,5}$ ]]; then
  echo 'usage: turns.sh <worktree parent> [positive hours]' >&2
  exit 2
fi
hours=$((10#$hours))

format_time() {
  date -d "@$1" '+%Y-%m-%d %H:%M' 2>/dev/null || date -r "$1" '+%Y-%m-%d %H:%M'
}
epoch=$(date +%s)
since=$(format_time "$((epoch - hours * 3600))")
now=$(format_time "$epoch")
printf '== gate lock and merge turn, last %s hours (from run.history) ==\n' "$hours"
for lane in "${lanes[@]}"; do
  history=$1/$lane/.cache/run.history
  [ -f "$history" ] || history=/dev/null
  awk -F '\t' -v lane="$lane" -v since="$since" -v now="$now" '
    $1 < since || $1 > now { next }
    $2 ~ /^full gate for .* (passed|failed) in [0-9]+ minutes/ {
      split($2, parts, " in "); gates++; minutes += parts[2] + 0
      if ($2 ~ / failed in /) { failed++; lost += parts[2] + 0 }
    }
    $2 ~ /^waited [0-9]+ minutes for the merge turn/ {
      split($2, words, " "); waits++; waited += words[2]
    }
    END { printf "  %s: gates %d = %d minutes (failed %d = %d minutes); waits for the turn %d = %d minutes\n",
      lane, gates, minutes, failed, lost, waits, waited }
  ' < "$history"
done
