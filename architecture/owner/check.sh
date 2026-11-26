#!/usr/bin/env bash
# Usage: check.sh [hours], check.sh --where
# Refresh a clean main checkout and run the two owner reports from that checkout.
# Defaults to two hours. Writes the reports maintained by owner/tools.sh. See
# that tool's header to remove them. Git can restore the pre-fetch checkout.
set -euo pipefail
unset CDPATH
architecture=$(cd -- "$(dirname -- "$0")/.." && pwd -P)
# shellcheck source=architecture/start-common.sh
. "$architecture/start-common.sh"

if [ "$#" = 1 ] && [ "$1" = --where ]; then show_paths; exit; fi
if [ "$#" -gt 1 ] || ! [[ ${1:-2} =~ ^0*[1-9][0-9]{0,5}$ ]]; then
  echo 'usage: check.sh [positive hours], check.sh --where' >&2
  exit 2
fi
hours=${1:-2}
create_state

echo '== main =='
fetched=0
if fetch_main; then fetched=1; else echo "check: could not fetch $HARNESS_MAIN"; fi
branch=$(checkout_branch)
if [ "$branch" != "$HARNESS_MAIN" ]; then
  printf 'check: the checkout is on %s; the tools are of that tree\n' "$branch"
elif ! clean_checkout "$harness_main"; then
  echo 'check: the checkout has changes; the tools are of that tree'
elif [ "$fetched" = 1 ]; then
  if fast_forward "$harness_main"; then
    printf 'check: main is at %s\n' "$(git -C "$harness_main" rev-parse HEAD)"
  else
    echo 'check: the main checkout did not update; the tools are of that tree'
  fi
fi

echo '== measure =='
measure=ok
"$BASH" "$harness_main/architecture/measure.sh" "$harness_worktrees" "$HARNESS_ASSIGNEE" "$hours" || measure=failed

echo '== tools =='
tools_result=0
tools_output=$("$BASH" "$harness_main/architecture/owner/tools.sh" check "$harness_state" "$harness_worktrees" 2>&1) || tools_result=$?
if [ -n "$tools_output" ]; then printf '%s\n' "$tools_output"; fi
printf 'check: measure %s; tools %s\n' "$measure" "$(last_line <<< "$tools_output")"
[ "$measure" = ok ] && [ "$tools_result" = 0 ]
