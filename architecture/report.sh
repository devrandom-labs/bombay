#!/usr/bin/env bash
# Report of one lane: how the run flows and where the design costs.
#
#   bash architecture/report.sh <lane> [days]     7 days by default, today included
#
# Run it in the lane's worktree. It reads the run history, the pull requests
# that merged, the commits on main and the rule scan. It changes nothing.
# A number that it cannot read is shown as "n/a".
#
# REPORT_SINCE, REPORT_HISTORY, REPORT_PRS, REPORT_COMMITS, REPORT_NUMSTAT,
# REPORT_RUNS, REPORT_FD and REPORT_OFFLINE serve the test.
set -uo pipefail

lane=${1:?usage: report.sh <lane> [days]}
days=${2:-7}
if [ -f "$(dirname "$0")/project.env" ]; then . "$(dirname "$0")/project.env"; fi
repo=${HARNESS_REPO:-devrandom-labs/bombay}
case "$lane" in
  # The paths that the lane owns (AGENTS.md, Lanes). The first one holds its code.
  server) roots=(apps/server docs/arch/server) ;;
  studio) roots=(apps/studio crates/editorial docs/arch/studio) ;;
  *) echo "report: unknown lane $lane"; exit 2 ;;
esac
[[ "$days" =~ ^[1-9][0-9]*$ ]] || { echo "report: the days must be a number from 1, not $days"; exit 2; }
since=${REPORT_SINCE:-$(date -d "-$((days - 1)) days" +%Y-%m-%d 2> /dev/null || date -v-"$((days - 1))"d +%Y-%m-%d 2> /dev/null)}
[ -n "$since" ] || { echo "report: no date for the last $days days"; exit 2; }
history=${REPORT_HISTORY:-$PWD/.cache/run.history}
online() { [ -z "${REPORT_OFFLINE:-}" ] && command -v gh > /dev/null; }
# GitHub answers only to the account of the project.
if online; then GH_TOKEN=${GH_TOKEN:-$(gh auth token --user joeldsouzax 2> /dev/null)} && export GH_TOKEN; fi
row() { printf '  %-44s %s\n' "$1" "$2"; }
# The most pull requests and Checks runs that it reads. A list of that length
# is cut off, and its count gets the words "at least".
most_prs=200 most_runs=300

# The scope of a path: a module of an application, one of its private crates,
# its tests, or a shared crate. A module in one file (src/output.rs) and a
# module in a folder (src/output/mod.rs) give the same scope. The files lib.rs
# and main.rs belong to the application itself. The private crates follow
# ADR 0040: one crate for each domain context and each adapter, and one
# execution crate.
scope='def scope: split("/") as $p
  | if $p[0] == "crates" then $p[0:2]
    elif ($p[2] // "") == "src" and ($p | length) == 4
      then (if $p[3] == "lib.rs" or $p[3] == "main.rs" then $p[0:3] else $p[0:3] + [$p[3] | rtrimstr(".rs")] end)
    elif ($p[2] // "") == "src" or ($p[2] // "") == "domain" or ($p[2] // "") == "adapters"
      then (if ($p | length) > 4 then $p[0:4] else $p[0:3] end)
    else $p[0:3] end
  | join("/");
# Rust code outside a test folder: only that makes a scope.
def code: endswith(".rs") and (split("/") | index("tests") | not);
# A path below one of the roots of the lane.
def in_lane($roots): . as $path | any($roots[]; . as $root | $path | startswith($root + "/"));'
prefixes=$(printf '%s\n' "${roots[@]}" | jq -R . | jq -cs .)

echo "Lane $lane since $since"
echo
echo "Flow of the run ($history)"
if [ -f "$history" ]; then
  awk -F'\t' -v since="$since" '
    # The minutes from the start of the day of a history line.
    function minutes(stamp,   part) { split(stamp, part, /[ :]/); return part[2] * 60 + part[3] }
    function inside(stamp) { return substr(stamp, 1, 10) >= since }
    # A start is remembered also from the day before the report, because the
    # line that follows it can lie inside the report.
    $2 ~ /^session on #/ { started = $1; counted = inside($1); if (counted) sessions++; next }
    # A lead that ends at once without a result could not start (a usage limit,
    # a login, the network). The run tries again; that try is no session.
    $2 ~ /^#[0-9]+ STEP failed the lead wrote no result line/ && started != "" {
      gap = minutes($1) - minutes(started)
      if (substr($1, 1, 10) != substr(started, 1, 10)) gap += 1440
      if (gap <= 2) {
        if (counted) sessions--
        if (inside($1)) nostart++
        started = ""; next
      }
    }
    !inside($1) { next }
    $2 ~ /^#[0-9]+ STEP merged/ { merged++ }
    $2 ~ /^#[0-9]+ STEP planned/ { planned++ }
    $2 ~ /^#[0-9]+ STEP (waiting|later|gate)/ { waiting++; reason = $2; sub(/^.* STEP (waiting|later|gate) ?/, "", reason); if ($2 !~ / STEP gate/) sub(/^[0-9]+ /, "", reason); if (reason == "") reason = "full gate"; reasons[reason]++ }
    $2 ~ /^#[0-9]+ STEP failed/ { failed++ }
    $2 ~ /^parked / { parked++ }
    $2 ~ /^stopped: / { stopped++ }
    $2 ~ / STEP reviewed/ { reviews++ }
    END {
      printf "  %-44s %d\n", "sessions", sessions
      printf "  %-44s %d\n", "steps merged", merged
      if (merged > 0) printf "  %-44s %.1f\n", "sessions for each merge (all sessions)", sessions / merged
      else printf "  %-44s %s\n", "sessions for each merge (all sessions)", "n/a"
      printf "  %-44s %d\n", "sessions that only planned", planned
      printf "  %-44s %d\n", "sessions that ended in a wait", waiting
      for (reason in reasons) printf "  %-44s %d\n", "  wait: " substr(reason, 1, 36), reasons[reason]
      printf "  %-44s %d\n", "sessions that failed", failed
      printf "  %-44s %d\n", "starts without a lead (no session)", nostart
      printf "  %-44s %d\n", "cards parked", parked
      printf "  %-44s %d\n", "stops of the run", stopped
      printf "  %-44s %d\n", "weekly design reviews", reviews
    }' "$history"
else
  row "run history" "n/a (no $history here)"
fi

# The merged pull requests of the lane, as a JSON list with number, files and body.
if [ -n "${REPORT_PRS:-}" ]; then prs=$(cat "$REPORT_PRS")
elif online; then
  prs=$(gh pr list -R "$repo" --state merged --search "merged:>=$since" --limit "$most_prs" --json number,files,body 2> /dev/null) || prs=""
else prs=""; fi

echo
echo "How far a change spreads (GitHub counts the days in UTC)"
if [ -n "$prs" ]; then
  jq -r --argjson roots "$prefixes" --argjson most "$most_prs" "$scope"'
    (if length >= $most then " (at least)" else "" end) as $cut
    | [ .[] | {number, body, paths: [.files[].path | select(in_lane($roots))]} | select(.paths | length > 0)
      | . + {scopes: (.paths | map(select(code)) | map(scope) | unique)} ] as $steps
    | ($steps | length) as $count
    | ($steps | map(.paths | length) | sort) as $files
    | [ $steps[] | (.body // "" | capture("Review rounds: (?<rounds>[0-9]+)").rounds? // empty) | tonumber ] as $rounds
    | "  \("pull requests merged" | . + (" " * (44 - length))) \($count)\($cut)",
      "  \("files for each pull request (median)" | . + (" " * (44 - length))) \(if $count == 0 then "n/a" elif $count % 2 == 1 then $files[($count / 2 | floor)] else (($files[$count / 2 - 1] + $files[$count / 2]) / 2) end)",
      "  \("pull requests that touch several scopes" | . + (" " * (44 - length))) \($steps | map(select(.scopes | length > 1)) | length)",
      "  \("review rounds for each step (mean)" | . + (" " * (44 - length))) \(if ($rounds | length) > 0 then (($rounds | add) / ($rounds | length) * 10 | round / 10) else "n/a" end) (\($rounds | length) of \($count) recorded)"
  ' <<< "$prs"
else
  row "pull requests" "n/a"
fi

# The commit messages and the changed lines on main since the date.
if [ -n "${REPORT_COMMITS:-}" ]; then commits=$(cat "$REPORT_COMMITS")
elif command -v git > /dev/null && git rev-parse origin/main > /dev/null 2>&1; then
  commits=$(git log origin/main --since="$since 00:00" --format=%B -- "${roots[@]}") || commits="n/a"
else commits="n/a"; fi
if [ "$commits" = n/a ]; then row "override trailers and reuse notes" "n/a"
else
  for trailer in Step-size Lanes Rules; do
    row "override trailers $trailer" "$(grep -c "^$trailer: " <<< "$commits" || true)"
  done
  row "reuse notes" "$(grep -c '^Reuse: ' <<< "$commits" || true)"
fi

echo
echo "Rules and tests now"
if command -v ast-grep > /dev/null && [ -d "${roots[0]}/src" ]; then
  # The scan returns 1 when it has findings and more when it could not run.
  findings=$(ast-grep scan --config architecture/sgconfig.yml "${roots[0]}/src" --json=stream 2> /dev/null) && code=0 || code=$?
  if [ "$code" -eq 0 ] || { [ "$code" -eq 1 ] && [ -n "$findings" ]; }; then
    row "rule findings in ${roots[0]}/src" "$(grep -c . <<< "$findings" || true)"
  else row "rule findings in ${roots[0]}/src" "n/a"; fi
else row "rule findings" "n/a"; fi
# The test functions below a folder; 0 when the folder does not exist yet.
lister=${REPORT_FD:-fd}
count_tests() {
  [ -d "$1" ] || { echo 0; return; }
  command -v "$lister" > /dev/null || { echo n/a; return; }
  local code
  code=$("$lister" --unrestricted --extension rs . "$1" --exec-batch cat 2> /dev/null) || { echo n/a; return; }
  # `#[test]`, `#[tokio::test]` and `#[tokio::test(...)]` at the start of a
  # line, outside a block comment that starts a line.
  awk '
    block { if ($0 ~ /\*\//) block = 0; next }
    /^[[:space:]]*\/\*/ { if ($0 !~ /\*\//) block = 1; next }
    /^[[:space:]]*#\[(tokio::)?test(\]|\()/ { tests++ }
    END { print tests + 0 }' <<< "$code"
}
if [ "$lane" = studio ]; then
  # The layers of ADR 0040: most behaviour should be testable in the pure crates.
  row "tests in pure domain crates" "$(count_tests apps/studio/domain)"
  row "tests in the execution shell" "$(count_tests apps/studio/execution)"
fi
row "tests of the assembled application" "$(count_tests "${roots[0]}/tests")"

echo
echo "Outside the lane"
if online; then
  row "open cards that wait for Joel" "$(gh issue list -R "$repo" --label needs-joel --state open --limit 500 --json number -q length 2> /dev/null || echo n/a)"
else row "open cards that wait for Joel" "n/a"; fi
# The finished Checks runs of main, newest first.
if [ -n "${REPORT_RUNS:-}" ]; then runs=$(cat "$REPORT_RUNS")
elif online; then
  runs=$(gh run list -R "$repo" --workflow checks.yml --branch main --status completed --limit "$most_runs" --json conclusion,event,createdAt 2> /dev/null) || runs=""
else runs=""; fi
if [ -n "$runs" ]; then
  jq -r --arg since "$since" --argjson most "$most_runs" '
      (if length >= $most and .[-1].createdAt[0:10] >= $since then " (at least)" else "" end) as $cut |
      [ .[] | select((.event == "schedule" or .event == "workflow_dispatch") and .conclusion != "cancelled" and .conclusion != "skipped") ] as $runs
      | "  \("Checks runs of main that failed" | . + (" " * (44 - length))) \($runs | map(select(.createdAt[0:10] >= $since and .conclusion != "success")) | length) of \($runs | map(select(.createdAt[0:10] >= $since)) | length)\($cut)",
        "  \("last Checks run of main" | . + (" " * (44 - length))) \($runs[0].conclusion // "n/a")"' <<< "$runs" \
    || row "Checks runs of main" "n/a"
else
  row "Checks runs of main" "n/a"
fi

echo
echo "Code scopes with the most changed Rust lines"
if [ -n "${REPORT_NUMSTAT:-}" ]; then numstat=$(cat "$REPORT_NUMSTAT")
elif command -v git > /dev/null && git rev-parse origin/main > /dev/null 2>&1; then
  if numstat=$(git log origin/main --since="$since 00:00" --numstat --no-renames --format= -- "${roots[@]}"); then
    [ -n "$numstat" ] || numstat=none
  else numstat=""; fi
else numstat=""; fi
if [ "$numstat" = none ]; then
  row "changed lines" "none"
elif [ -n "$numstat" ]; then
  jq -Rr -s --argjson roots "$prefixes" "$scope"'
    [ split("\n")[] | select(length > 0) | split("\t") | select(.[0] != "-") | select(.[2] | code and in_lane($roots))
      | {scope: (.[2] | scope), lines: ((.[0] | tonumber) + (.[1] | tonumber))} ]
    | group_by(.scope) | map({scope: .[0].scope, lines: (map(.lines) | add)})
    | if length == 0 then "  none"
      else (sort_by(-.lines) | .[0:5][] | "  \(.scope | . + (" " * ([44 - length, 1] | max))) \(.lines)") end' <<< "$numstat"
else
  row "changed lines" "n/a"
fi
