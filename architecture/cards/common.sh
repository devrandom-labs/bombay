#!/usr/bin/env bash
# Shared arguments and GitHub calls for the card tools. Source from a card script.
architecture=$(dirname -- "$0")/..
# shellcheck source=architecture/project.env
. "$architecture/project.env"

# gh uses GH_TOKEN from the environment or its active login.
dry_run=false
args=()
for arg in "$@"; do
  if [ "$arg" = --dry-run ]; then
    dry_run=true
  else
    args+=("$arg")
  fi
done

usage() {
  printf 'Usage: %s %s [--dry-run]\n' "${0##*/}" "$1" >&2
  exit 2
}

refuse() {
  printf '%s: %s\n' "${0##*/}" "$1" >&2
  exit 1
}

issue_number() { [[ "$1" =~ ^[1-9][0-9]*$ ]]; }

sequence_number() {
  [[ "$1" =~ ^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$ ]] &&
    jq -en --arg value "$1" '$value | tonumber | isfinite' > /dev/null
}

plan() {
  printf 'Would run: gh'
  printf ' %q' "$@"
  printf '\n'
}

write_quietly() {
  if "$dry_run"; then
    plan "$@"
  else
    gh "$@" > /dev/null
  fi
}

edit_field() {
  local field_id=$1
  shift
  write_quietly project item-edit --project-id "$project" --id "$item" --field-id "$field_id" "$@"
}

option_id() {
  jq -er --arg name "$2" '.options[]? | select(.name == $name) | .id | strings' <<< "$1"
}

change_field() {
  local name=$1 number=$2 value=$3 pages board field field_id item_data previous option
  local project item
  # shellcheck disable=SC2016 # GraphQL variables are passed with gh flags.
  pages=$(gh api graphql --paginate -f login="$HARNESS_PROJECT_OWNER" -F project="$HARNESS_PROJECT_NUMBER" \
    -f owner="${HARNESS_REPO%/*}" -f repo="${HARNESS_REPO#*/}" -F number="$number" -f field="$name" -f query='
    query($login: String!, $project: Int!, $owner: String!, $repo: String!, $number: Int!, $field: String!, $endCursor: String) {
      organization(login: $login) {
        projectV2(number: $project) {
          id
          field(name: $field) {
            ... on ProjectV2Field { id dataType }
            ... on ProjectV2SingleSelectField { id options { id name } }
          }
        }
      }
      repository(owner: $owner, name: $repo) {
        issue: issueOrPullRequest(number: $number) {
          __typename
          ... on Issue {
            projectItems(first: 100, after: $endCursor) {
              pageInfo { hasNextPage endCursor }
              nodes {
                id
                project { id }
                value: fieldValueByName(name: $field) {
                  ... on ProjectV2ItemFieldNumberValue { number }
                  ... on ProjectV2ItemFieldSingleSelectValue { name }
                }
              }
            }
          }
        }
      }
    }')
  if jq -se 'any(.[]; .data.repository.issue.__typename == "PullRequest")' <<< "$pages" > /dev/null; then
    refuse "#$number is a pull request; use an issue number"
  fi
  board=$(jq -cse '
    select(any(.[]; has("errors")) | not) | .[0].data.organization.projectV2 // empty' <<< "$pages") ||
    refuse 'the board could not be read'
  project=$(jq -er '.id | strings' <<< "$board")
  field=$(jq -c '.field' <<< "$board")
  item_data=$(jq -cse --arg project "$project" '
    [.[] | .data.repository.issue.projectItems.nodes[]? | select(.project.id == $project)][0] // empty' \
    <<< "$pages") || refuse "#$number is not on the board"
  item=$(jq -er '.id | strings' <<< "$item_data")
  field_id=$(jq -er '.id | strings' <<< "$field") || refuse "no $name field"

  if [ "$name" = Sequence ]; then
    jq -e '.dataType == "NUMBER"' <<< "$field" > /dev/null || refuse 'no numeric Sequence field'
    previous=$(jq -r '.value.number // "(unset)"' <<< "$item_data")
    set -- --number "$value"
  else
    option=$(option_id "$field" "$value") || refuse "no Status option $value"
    previous=$(jq -r '.value.name // "(unset)"' <<< "$item_data")
    set -- --single-select-option-id "$option"
  fi

  printf '#%s %s before: %s\n' "$number" "$name" "$previous"
  edit_field "$field_id" "$@"
  if "$dry_run"; then
    printf '#%s %s would be: %s\n' "$number" "$name" "$value"
  else
    printf '#%s %s %s\n' "$number" "$name" "$value"
  fi
}
