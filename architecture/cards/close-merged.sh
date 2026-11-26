#!/usr/bin/env bash
# List open Merged issues with no unchecked boxes, or close them with one comment each.
# Usage: close-merged.sh [close] [--dry-run]
# To reverse a close, run gh issue reopen for the issue and delete the comment.
set -euo pipefail
# shellcheck source=architecture/cards/common.sh
. "$(dirname -- "$0")/common.sh"
set -- "${args[@]+"${args[@]}"}"

case "$#:${1:-}" in
  0:|1:close) ;;
  *) usage '[close]' ;;
esac

# Read state with each issue to avoid a capped list of closed issues.
# shellcheck disable=SC2016 # GraphQL variables are passed with gh flags.
pages=$(gh api graphql --paginate -f login="$HARNESS_PROJECT_OWNER" -F number="$HARNESS_PROJECT_NUMBER" -f query='
  query($login: String!, $number: Int!, $endCursor: String) {
    organization(login: $login) {
      projectV2(number: $number) {
        items(first: 100, after: $endCursor) {
          totalCount
          pageInfo { hasNextPage endCursor }
          nodes {
            id
            status: fieldValueByName(name: "Status") { ... on ProjectV2ItemFieldSingleSelectValue { name } }
            content {
              ... on Issue { number title body state repository { nameWithOwner } }
            }
          }
        }
      }
    }
  }') || refuse 'the board could not be read'

candidates=$(jq -rs --arg repo "$HARNESS_REPO" '
  if any(.[]; has("errors")) then error("close-merged: GraphQL errors") else . end
  | map(.data.organization.projectV2.items) as $pages
  | ($pages[0].totalCount // 0) as $total
  | [$pages[].nodes[]?] as $nodes
  | if $total == 0 then error("close-merged: the board could not be read")
    elif any($pages[]; .totalCount != $total)
      or ($nodes | length) != $total then error("close-merged: incomplete item count")
    elif ($nodes | map(.id) | unique | length) != $total then error("close-merged: duplicate item IDs")
    else $nodes end
  | sort_by(.content.number)[]
  | select(.status.name == "Merged" and .content.state == "OPEN" and .content.repository.nameWithOwner == $repo)
  | ((.content.body // "") | [scan("(?m)^[ \t]*(?:>[ \t]*)*(?:[-*+]|[0-9]{1,9}[.)])[ \t]+\\[[ \t]\\]")] | length) as $open
  | "\(.content.number)\t\($open)\t\(.content.title)"
' <<< "$pages") || exit 1

while IFS=$'\t' read -r number open title && [ -n "$number" ]; do
  if [ "$open" != 0 ]; then
    printf '#%s KEPT OPEN: Status Merged with %s open boxes: %s\n' "$number" "$open" "$title"
    continue
  fi
  comment='Closed by the harness owner: the board Status is Merged and every step and box is ticked. The graphs of the project count closed issues. Reopen the issue if a step is missing.'
  if "$dry_run"; then
    printf '#%s would close: %s\n' "$number" "$title"
    plan issue close "$number" -R "$HARNESS_REPO" --reason completed --comment "$comment"
  elif [ "$#" = 1 ]; then
    write_quietly issue close "$number" -R "$HARNESS_REPO" --reason completed --comment "$comment"
    printf '#%s closed: %s\n' "$number" "$title"
  else
    printf '#%s would close: %s\n' "$number" "$title"
  fi
done <<< "$candidates"
