#!/usr/bin/env bash
# Read every project page before printing the gh item-list document.
set -uo pipefail

here=$(cd -- "$(dirname -- "$0")" && pwd -P)
# shellcheck source=architecture/project.env
. "$here/project.env"

pages=$(gh api graphql --paginate -F query=@"$here/board.graphql" \
  -f login="$HARNESS_PROJECT_OWNER" -F number="$HARNESS_PROJECT_NUMBER") || exit 1

jq -s '
  if any(.[]; has("errors")) then error("board: GraphQL errors") else . end
  | map(.data.organization.projectV2.items) as $pages
  | $pages[0].totalCount as $total
  | [$pages[].nodes[]] as $nodes
  | if any($pages[]; .totalCount != $total)
       or ($nodes | length) != $total then error("board: incomplete item count")
    elif ($nodes | map(.id) | unique | length) != $total then error("board: duplicate item IDs")
    else {
      totalCount: $total,
      items: [$nodes[]
        | .content as $content
        | {
            id,
            title: $content.title,
            content: ($content | {
              body, number, repository: .repository.nameWithOwner, title,
              type: .__typename, url
            } | with_entries(select(.value != null))),
            repository: $content.repository.url,
            assignees: [$content.assignees.nodes[]?.login],
            labels: [$content.labels.nodes[]?.name],
            milestone: ($content.milestone | if . == null then null
              else {title, description: (.description // ""), dueOn: (.dueOn // "")} end),
            "linked pull requests": [.linked.pullRequests.nodes[]?.url]
          }
          + (del(.id, .content, .linked) | with_entries(
              .key |= ({startDate: "start date", targetDate: "target date", taskId: "task ID"}[.] // .)
              | .value |= (.name // .text // .number // .date
                  | if type == "number" then . + 0 else . end)))
        | with_entries(select(.value != null and .value != []))]
    } end
' <<< "$pages"
