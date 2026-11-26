#!/usr/bin/env bash
# Usage: bash architecture/measure.sh <worktree parent> <card assignee> [hours]
# The parent contains server/, studio/ and nix/ on this machine.
# The assignee filters missing board fields.
# GH_TOKEN is the token for a GitHub account with access to the project board
# and repository. When unset, gh uses its active login.
# This read-only report covers the last four hours by default.
set -uo pipefail
export LC_ALL=C
unset CDPATH

worktrees=${1:-}
assignee=${2:-}
hours=${3:-4}
if [ "$#" -lt 2 ] || [ "$#" -gt 3 ] || [ ! -d "$worktrees" ] || [ -z "$assignee" ] ||
    ! [[ "$hours" =~ ^0*[1-9][0-9]{0,5}$ ]]; then
  echo 'Usage: bash architecture/measure.sh <worktree parent directory> <card assignee> [positive hours]' >&2
  exit 2
fi
hours=$((10#$hours))

here=$(cd -- "$(dirname -- "$0")" > /dev/null && pwd) || exit 1
worktrees=$(cd -- "$worktrees" > /dev/null && pwd) || exit 1
if [ -f "$(dirname "$0")/project.env" ]; then . "$(dirname "$0")/project.env"; fi
repo=${HARNESS_REPO:-devrandom-labs/bombay}
format_time() { # epoch, format, optional -u
  local epoch=$1 format=$2
  shift 2
  date "$@" -d "@$epoch" "$format" 2> /dev/null || date "$@" -r "$epoch" "$format"
}
now_epoch=$(date +%s) || exit 1
since_epoch=$((now_epoch - hours * 3600))
now=$(format_time "$now_epoch" '+%Y-%m-%d %H:%M') || exit 1
since=$(format_time "$since_epoch" '+%Y-%m-%d %H:%M') || exit 1
since_utc=$(format_time "$since_epoch" '+%Y-%m-%dT%H:%M:%SZ' -u) || exit 1
now_utc=$(format_time "$now_epoch" '+%Y-%m-%dT%H:%M:%SZ' -u) || exit 1
window_utc=$since_utc..$now_utc
printf 'window: %s to %s (%s hours)\n' "$since" "$now" "$hours"

result=0
echo
echo '== board =='
if ! bash "$here/board.sh" | jq -er --arg mode measure --arg lane '' --arg skip '' \
    --arg assignee "$assignee" -f "$here/backlog.jq"; then
  echo 'the board could not be read'
  result=1
fi

github_list() {
  local label=$1 items
  shift
  if ! { items=$(gh "$@") && jq -er --arg label "$label" 'arrays |
      "\($label): \(if length == 100 then "at least " else "" end)\(length)",
      (.[] | "  #\(.number) \(.title[0:90])")' <<< "$items"; }; then
    printf '%s: unavailable\n' "$label"
    result=1
  fi
}
echo
echo '== closed and merged in the window =='
github_list 'pull requests merged' pr list -R "$repo" --state merged \
  --search "merged:$window_utc" --limit 100 --json number,title
github_list 'cards closed' issue list -R "$repo" --state closed \
  --search "closed:$window_utc" --limit 100 --json number,title
echo
echo '== Checks runs of main (latest 3) =='
if ! { runs=$(gh run list -R "$repo" --workflow checks.yml --branch main --limit 3 \
    --json databaseId,event,status,conclusion,createdAt,headSha) &&
    jq -er 'arrays | if length == 0 then "  none" else
      .[] | "  \(.databaseId) \(.event) \(.status) \(.conclusion) \(.createdAt) \(.headSha[0:8])" end' <<< "$runs"; }; then
  echo 'Checks runs: unavailable'
  result=1
fi

history() {
  local lane=$1 cache=$worktrees/$1/.cache summary status
  if [ ! -f "$cache/run.history" ]; then
    printf '%s: no history\n' "$lane"
    return
  fi
  if summary=$(awk -F '\t' -v since="$since" -v now="$now" -v lane="$lane" '
    { in_window = ($1 >= since && $1 <= now); event = $2 }
    # The lead can later confirm a merge already logged by the run.
    event ~ /^#[0-9]+ STEP merged ([0-9]+|no-PR-[^ ]+)( |$)/ {
      split(event, words, " ")
      has_pr = (words[4] ~ /^[0-9]+$/)
      # One Checks result can qualify several cards without a merge.
      key = (has_pr ? words[4] : words[1] " " words[4])
      if (!seen[key]++ && in_window) {
        merged++
        if (!has_pr) without_pr++
        if (event ~ / \(the run\)$/) by_run++
      }
    }
    !in_window { next }
    event ~ /^session on / { sessions++ }
    event ~ /^#[0-9]+ STEP planned( |$)/ { planned++ }
    event ~ /^#[0-9]+ STEP gate( |$)/ { requests++ }
    event ~ /^full gate for #[0-9]+ (passed in|failed in|did not end in) [0-9]+ minutes$/ {
      gates++
      count = split(event, words, " ")
      gate_minutes += words[count - 1]
      if (event !~ / passed in /) failed++
    }
    event ~ /^main advanced during the gate for #[0-9]+;/ { advanced++ }
    event ~ /^waited [0-9]+ minutes for the merge turn$/ {
      split(event, words, " ")
      turns++
      turn_minutes += words[2]
    }
    event ~ /^#[0-9]+ STEP (later|waiting)( |$)/ { waits++ }
    event ~ /^set aside #[0-9]+ / { aside++ }
    event ~ /^#[0-9]+ STEP (failed|blocked)( |$)/ { stopped++ }
    event ~ /^parked #[0-9]+:/ { parks++; parked = parked "\n    " $1 " " substr(event, 1, 110) }
    { last = $1 " " substr(event, 1, 110) }
    END {
      printf "%s: session starts %d, merged %d (by the run %d, without a PR %d), planned %d, gate requests %d, gates %d = %d minutes (failed %d), main advanced during a gate %d, merge turn waits %d = %d minutes, later/waiting %d, set aside %d, failed/blocked %d, parks %d\n",
        lane, sessions, merged, by_run, without_pr, planned, requests, gates, gate_minutes, failed, advanced, turns, turn_minutes, waits, aside, stopped, parks
      if (parked != "") printf "  parked:%s\n", parked
      if (last != "") printf "  last line: %s\n", last
    }' "$cache/run.history"); then
    printf '%s\n' "$summary"
  else
    printf '%s: history unavailable\n' "$lane"
    result=1
  fi
  if status=$(head -n 1 "$cache/run.status" 2> /dev/null) && [ -n "$status" ]; then
    printf '  recorded status: %.110s\n' "$status"
  fi
}
echo
echo '== lanes (from run.history) =='
for lane in server studio nix; do history "$lane"; done

echo
echo '== host =='
if ! df -Pk "$worktrees" | awk 'NR == 2 { printf "  free disk: %.1f GiB\n", $4 / 1048576 }'; then
  echo '  free disk: unavailable'
fi
case "$(uname -s)" in
  Darwin)
    vm_stat | awk -F ': *' '
      NR == 1 { split($0, header, " "); page = header[8] }
      /Pages wired down|Pages free|Pages occupied by compressor/ {
        printf "  %-30s %.1f GiB\n", $1, $2 * page / 1073741824
      }' || echo '  memory: unavailable'
    sysctl vm.swapusage kern.memorystatus_vm_pressure_level || true
    zprint 2> /dev/null | awk '$1 == "data.kalloc.1024" {
      printf "  kernel zone data.kalloc.1024: %.2f GiB\n", $7 * 1024 / 1073741824
    }' || true
    ;;
  Linux)
    awk '/^MemAvailable:/ { printf "  available memory: %.1f GiB\n", $2 / 1048576 }
         /^SwapTotal:/ { swap = $2 }
         /^SwapFree:/ { printf "  swap used: %.1f GiB of %.1f GiB\n", (swap - $2) / 1048576, swap / 1048576 }' \
      /proc/meminfo 2> /dev/null || echo '  memory: unavailable'
    ;;
  *) echo '  memory: unavailable' ;;
esac
uptime || true
exit "$result"
