#!/usr/bin/env bash
# Create an assigned task issue, add it to the board and set its fields.
# Usage: new.sh <title> <body file> <workstream> <sequence> [--phase <phase>] [--task-id <id>] [--status <status>] [--dry-run]
set -euo pipefail
# shellcheck source=architecture/cards/common.sh
. "$(dirname -- "$0")/common.sh"
set -- "${args[@]+"${args[@]}"}"

syntax='<title> <body file> <workstream> <sequence> [--phase <phase>] [--task-id <id>] [--status <status>]'
(( $# >= 4 )) || usage "$syntax"
if ! [[ "$1" =~ [^[:space:]] ]] || ! sequence_number "$4"; then usage "$syntax"; fi
title=$1 body=$2 stream=$3 sequence=$4
shift 4

phase=""
task_id=""
status_val="Active"
while (( $# > 0 )); do
  case "$1" in
    --phase)   (( $# >= 2 )) || usage "$syntax"; phase=$2; shift 2 ;;
    --task-id) (( $# >= 2 )) || usage "$syntax"; task_id=$2; shift 2 ;;
    --status)  (( $# >= 2 )) || usage "$syntax"; status_val=$2; shift 2 ;;
    *) usage "$syntax" ;;
  esac
done

if [ -z "$phase" ]; then
  phase=$(sed -n 's/^def phases: \(.*\);$/\1/p' "$architecture/backlog.jq" | jq -er '.[0] | strings') ||
    refuse 'could not find the first backlog phase'
fi
if [ ! -f "$body" ] || [ ! -r "$body" ] || ! grep -q '[^[:space:]]' < "$body"; then
  refuse 'body file must be readable and nonempty'
fi

# shellcheck disable=SC2016
data=$(gh api graphql -f login="$HARNESS_PROJECT_OWNER" -F number="$HARNESS_PROJECT_NUMBER" -f query='
  query($login: String!, $number: Int!) {
    organization(login: $login) {
      projectV2(number: $number) {
        id
        kind: field(name: "Kind") { ...selectOptions }
        phase: field(name: "Phase") { ...selectOptions }
        status: field(name: "Status") { ...selectOptions }
        workstream: field(name: "Workstream") { ...selectOptions }
        sequence: field(name: "Sequence") { ... on ProjectV2Field { id dataType } }
        taskId: field(name: "Task ID") { ... on ProjectV2Field { id dataType } }
      }
    }
  }
  fragment selectOptions on ProjectV2FieldConfiguration {
    ... on ProjectV2SingleSelectField { id options { id name } }
  }')
board=$(jq -ce 'select(has("errors") | not) | .data.organization.projectV2 // empty' <<< "$data") ||
  refuse 'the board could not be read'
project=$(jq -er '.id | strings' <<< "$board")
field_ids=() option_ids=()
for pair in 'Kind:Task' "Phase:$phase" "Status:$status_val" "Workstream:$stream"; do
  field_name=${pair%%:*}
  value=${pair#*:}
  field=$(jq -c --arg field "$field_name" '.[($field | ascii_downcase)]' <<< "$board")
  option=$(option_id "$field" "$value") || refuse "no option $value in field $field_name"
  field_id=$(jq -er '.id | strings' <<< "$field") || refuse "no $field_name field"
  field_ids+=("$field_id")
  option_ids+=("$option")
done
sequence_id=$(jq -er '.sequence | select(.dataType == "NUMBER") | .id | strings' <<< "$board") ||
  refuse 'no numeric Sequence field'
task_id_field=""
if [ -n "$task_id" ]; then
  task_id_field=$(jq -er '.taskId | select(.dataType == "TEXT") | .id | strings' <<< "$board") ||
    refuse 'no text Task ID field'
fi

create=(api "repos/$HARNESS_REPO/issues" -f "title=$title" -F "body=@$body" -f "assignees[]=$HARNESS_ASSIGNEE")
item=
creation_failed() {
  if ! "$dry_run" && [ -n "${item:-}" ] &&
      ! gh project item-delete "$HARNESS_PROJECT_NUMBER" --owner "$HARNESS_PROJECT_OWNER" --id "$item" > /dev/null; then
    printf 'new: could not remove project item %s; it may have partial board fields\n' "$item" >&2
  fi
  printf 'new: issue #%s exists with no board fields; no lane can select it. Set the fields or close it.\n' "${number:-?}" >&2
  exit 1
}
if "$dry_run"; then
  plan "${create[@]}"
  plan project item-add "$HARNESS_PROJECT_NUMBER" --owner "$HARNESS_PROJECT_OWNER" --url '<new issue URL>' --format json
  item='<new project item ID>'
else
  issue=$(gh "${create[@]}")
  number=$(jq -er '.number | numbers' <<< "$issue")
  url=$(jq -er '.html_url | strings' <<< "$issue") || creation_failed
  printf '#%s %s\n' "$number" "$url"
  item=$(gh project item-add "$HARNESS_PROJECT_NUMBER" --owner "$HARNESS_PROJECT_OWNER" --url "$url" --format json |
    jq -er '.id | strings') || creation_failed
fi

# Set Workstream last so no lane selects a partially initialized card.
for index in 0 1 2; do
  edit_field "${field_ids[$index]}" --single-select-option-id "${option_ids[$index]}" || creation_failed
done
edit_field "$sequence_id" --number "$sequence" || creation_failed
if [ -n "$task_id_field" ]; then
  edit_field "$task_id_field" --text "$task_id" || creation_failed
fi
edit_field "${field_ids[3]}" --single-select-option-id "${option_ids[3]}" || creation_failed
