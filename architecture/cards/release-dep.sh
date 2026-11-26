#!/usr/bin/env bash
# Replace one issue link in Dependencies with text, update the body and post a note.
# Usage: release-dep.sh <card> <dependency> <new text line> <note file> [--dry-run]
# To reverse, restore the body from the printed backup path and delete the comment.
set -euo pipefail
# shellcheck source=architecture/cards/common.sh
. "$(dirname -- "$0")/common.sh"
set -- "${args[@]+"${args[@]}"}"

if [ "$#" != 4 ] || ! issue_number "$1" || ! issue_number "$2"; then
  usage '<card> <dependency> <new text line> <note file>'
fi
card=$1 dep=$2 new_line=$3 note=$4
[[ "$new_line" =~ [^[:space:]] && "$new_line" != *$'\n'* && "$new_line" != *$'\r'* ]] ||
  refuse 'replacement must be one nonempty line'
[[ "$new_line" != '## '* && "$new_line" != *'## Dependencies'* ]] ||
  refuse 'replacement changes the Dependencies section'
if [[ "$new_line" =~ (^|[^[:alnum:]_])issues/[0-9]+|#[0-9]+ ]]; then
  refuse 'replacement links to an issue'
fi
[ -f "$note" ] && [ -r "$note" ] && [ -s "$note" ] || refuse 'note file must be readable and nonempty'

work=$(mktemp -d "${TMPDIR:-/tmp}/release-dep.XXXXXXXX")
keep_before=false
trap 'rm -f "$work/after.md"; if ! "$keep_before"; then rm -f "$work/before.md"; rmdir "$work"; fi' EXIT
endpoint=repos/$HARNESS_REPO/issues/$card
issue=$(gh api "$endpoint")
if jq -e '.pull_request != null' <<< "$issue" > /dev/null; then
  refuse "#$card is a pull request; use an issue number"
fi
jq -ej '.body // ""' <<< "$issue" > "$work/before.md"

# Use the backlog's /issues/<number> link convention. Preserve other lines,
# line endings and the presence or absence of the last newline.
change=$(jq -Rsce --arg dep "$dep" --arg new "$new_line" --arg card "$card" '
  def dependencies:
    [to_entries[] | select(.value | startswith("## "))] as $headings
    | [$headings[] | select(.value | startswith("## Dependencies"))][0].key as $start
    | if $start == null then empty else
        ([$headings[] | select(.key > $start)][0].key // length) as $end
        | range($start + 1; $end) as $index | {index: $index, line: .[$index]}
      end;
  def links:
    scan("/issues/([0-9]+)") | .[0] | select(. == $dep);
  split("\n") as $before
  | [$before | dependencies | .index as $i | .line | links | $i] as $matches
  | (if ($matches | length) != 1 then
      error("#\($card) does not link #\($dep) exactly one time in Dependencies")
    else $matches[0] end) as $index
  | $before[$index] as $old
  | if ([$before[] | select(. == $old)] | length) != 1 then
      error("the link line is not in the body exactly one time")
    else $before end
  | .[$index] = ($new + (if $old | endswith("\r") then "\r" else "" end))
  | . as $after
  | [range($before | length) | select($before[.] != $after[.])] as $changed
  | if ($after | length) != ($before | length) or ($changed | length) != 1 then
      error("replacement did not change exactly one line")
    else . end
  | if ([$after | dependencies | .line | links] | length) != 0 then
      error("a link to #\($dep) is still in Dependencies")
    else {old: ($old | sub("\r$"; "")), body: ($after | join("\n"))} end
' "$work/before.md") || exit 1
jq -ej '.body' <<< "$change" > "$work/after.md"

printf '#%s old line: %s\n' "$card" "$(jq -r '.old' <<< "$change")"
patch=(api -X PATCH "$endpoint" -F "body=@$work/after.md" --jq '"#'"$card"' updated " + .updated_at')
comment=(issue comment "$card" -R "$HARNESS_REPO" --body-file "$note")
if "$dry_run"; then
  printf '#%s new line: %s\n' "$card" "$new_line"
  plan "${patch[@]}"
  plan "${comment[@]}"
else
  keep_before=true
  printf '#%s before: %s\n' "$card" "$work/before.md"
  gh "${patch[@]}"
  if ! gh "${comment[@]}"; then
    printf 'release-dep: the body of #%s is changed; the note was NOT posted. Post it, or restore the body from %s.\n' \
      "$card" "$work/before.md" >&2
    exit 1
  fi
fi
