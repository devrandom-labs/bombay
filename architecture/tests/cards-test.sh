#!/usr/bin/env bash
# Test the card tools with offline GitHub fixtures. Pass an architecture path
# to exercise isolated copies of the tools.
set -euo pipefail
unset CDPATH

architecture=${1:-$(dirname -- "$0")/..}
architecture=$(cd -- "$architecture" && pwd)
cards=$architecture/cards
work=$(mktemp -d)
export HOME=$work/home XDG_STATE_HOME=$work/xdg
mkdir -p "$HOME" "$XDG_STATE_HOME"
trap 'rm -rf "$work"' EXIT
mkdir "$work/bin" "$work/tmp"
unset GH_TOKEN
export HARNESS_PROJECT_OWNER=labcast HARNESS_PROJECT_NUMBER=2 HARNESS_REPO=labcast/labcast-server HARNESS_ASSIGNEE=joeldsouzax
export CARDS_TEST_WORK="$work" TMPDIR="$work/tmp" CARDS_TEST_JQ
CARDS_TEST_JQ=$(command -v jq)

printf '#!%s\n' "$BASH" > "$work/bin/gh"
cat >> "$work/bin/gh" <<'STUB'
set -euo pipefail
work=$CARDS_TEST_WORK
owner=${EXPECT_OWNER:-labcast}
project=${EXPECT_PROJECT:-2}
repo=${EXPECT_REPO:-labcast/labcast-server}
"$CARDS_TEST_JQ" -cn --args '$ARGS.positional' -- "$@" >> "$work/calls"

unexpected() {
  printf 'unexpected gh call: %s\n' "$*" >> "$work/unexpected"
  exit 98
}
[ "${GH_TOKEN:-}" = "${EXPECT_TOKEN:-}" ] || unexpected 'GH_TOKEN differs'

case "$*" in
  "api graphql -f login=$owner -F number=$project -f query="*)
    [[ "$8" == *'workstream: field(name: "Workstream")'* ]] || unexpected "$@"
    cat "${FIELDS:-$work/fields.json}" ;;
  "api graphql --paginate -f login=$owner -F project=$project -f owner=${repo%/*} -f repo=${repo#*/} -F number="*' -f field='*' -f query='*)
    [[ "${17}" == *'issueOrPullRequest(number: $number)'* &&
       "${17}" == *'projectItems(first: 100, after: $endCursor)'* &&
       "${17}" == *'fieldValueByName(name: $field)'* ]] || unexpected "$@"
    cat "${MEMBERSHIP:-$work/membership.json}" ;;
  "api graphql --paginate -f login=$owner -F number=$project -f query="*)
    [[ "$9" == *'number title body state repository'* && "$9" == *'pageInfo { hasNextPage endCursor }'* ]] ||
      unexpected "$@"
    cat "${BOARD:-$work/board.json}"
    exit "${BOARD_EXIT:-0}" ;;
  "api repos/$repo/issues/31")
    "$CARDS_TEST_JQ" -n --rawfile body "${ISSUE_BODY:-$work/before.md}" --arg pr "${IS_PULL:-no}" '
      {body: $body} + if $pr == "yes" then {pull_request: {url: "fixture"}} else {} end' ;;
  "api repos/$repo/issues "*|"project item-add "*|"project item-edit "*|"project item-delete "*|"api -X PATCH "*|"issue comment "*|"issue close "*)
    tail -n 1 "$work/calls" >> "$work/writes"
    if [ "$1 $2" = "${FAIL_WRITE:-}" ]; then echo 'fixture write failed' >&2; exit 7; fi
    case "$1 $2" in
      "api repos/$repo/issues") printf '{"number":31,"html_url":"https://github.com/%s/issues/31"}\n' "$repo" ;;
      'project item-add') printf '{"id":"new-item"}\n' ;;
      'project item-edit')
        if [ "$8" = "${FAIL_FIELD:-}" ]; then echo 'fixture write failed' >&2; exit 7; fi ;;
      'api -X')
        [ -f "${6#body=@}" ] || unexpected "$@"
        cp "${6#body=@}" "$work/patched.md"
        echo '#31 updated 2026-11-20T12:00:00Z' ;;
      'issue comment') printf 'https://github.com/%s/issues/31#issuecomment-1\n' "$repo" ;;
    esac ;;
  *) unexpected "$@" ;;
esac
STUB

# Inject a bad transformed body before either proof runs. The other jq calls
# use the installed binary unchanged.
printf '#!%s\n' "$BASH" > "$work/bin/jq"
cat >> "$work/bin/jq" <<'STUB'
set -euo pipefail
marker='| . as $after'
if [ -n "${BODY_FAULT:-}" ] && [[ "$*" == *"$marker"* ]]; then
  case "$BODY_FAULT" in
    two-lines) edit='| .[0] = "Changed the title too"' ;;
    extra-line) edit='| . += ["Appended a line"]' ;;
    link-left) edit='| .[$index] = ($old + " changed")' ;;
  esac
  args=("$@")
  for i in "${!args[@]}"; do
    args[$i]=${args[$i]/"$marker"/"$edit $marker"}
  done
  set -- "${args[@]}"
fi
exec "$CARDS_TEST_JQ" "$@"
STUB
chmod +x "$work/bin/"{gh,jq}
export PATH="$work/bin:$PATH"

cat > "$work/fields.json" <<'FIXTURE'
{"data":{"organization":{"projectV2":{
  "id":"board",
  "kind":{"id":"kind","options":[{"id":"task","name":"Task"}]},
  "phase":{"id":"phase","options":[{"id":"first","name":"Phase 1"},{"id":"second","name":"Phase 2"}]},
  "status":{"id":"status","options":[{"id":"active","name":"Active"}]},
  "workstream":{"id":"stream","options":[{"id":"studio","name":"Studio"}]},
  "sequence":{"id":"sequence","dataType":"NUMBER"}
}}}}
FIXTURE
cat > "$work/membership.json" <<'FIXTURE'
{"data":{"organization":{"projectV2":{"id":"board","field":{"id":"sequence","dataType":"NUMBER","options":[{"id":"active","name":"Active"}]}}},"repository":{"issue":{"projectItems":{"nodes":[{"id":"other-item","project":{"id":"other-board"},"value":{"number":99,"name":"Wrong board"}}]}}}}}
{"data":{"organization":{"projectV2":{"id":"board","field":{"id":"sequence","dataType":"NUMBER","options":[{"id":"active","name":"Active"}]}}},"repository":{"issue":{"projectItems":{"nodes":[{"id":"our-item","project":{"id":"board"},"value":{"number":0,"name":"Needs review"}}]}}}}}
FIXTURE
cat > "$work/board.json" <<'FIXTURE'
{"data":{"organization":{"projectV2":{"items":{"totalCount":6,"nodes":[
  {"id":"one","status":{"name":"Merged"},"content":{"number":8,"title":"Waiting","body":"Start\n  - [ ] one\n- [ ] two","state":"OPEN","repository":{"nameWithOwner":"labcast/labcast-server"}}},
  {"id":"two","status":{"name":"Merged"},"content":{"number":9,"title":"Already closed","state":"CLOSED","repository":{"nameWithOwner":"labcast/labcast-server"}}},
  {"id":"three","status":{"name":"Active"},"content":{"number":7,"title":"Still active","state":"OPEN","repository":{"nameWithOwner":"labcast/labcast-server"}}}
]}}}}}
{"data":{"organization":{"projectV2":{"items":{"totalCount":6,"nodes":[
  {"id":"four","status":{"name":"Merged"},"content":{"number":6,"title":"Done","body":"- [x] finished\nThe text - [ ] is inline.","state":"OPEN","repository":{"nameWithOwner":"labcast/labcast-server"}}},
  {"id":"five","status":{"name":"Merged"},"content":{"number":6,"title":"Other repo","state":"OPEN","repository":{"nameWithOwner":"other/tools"}}},
  {"id":"six","status":{"name":"Merged"},"content":{}}
]}}}}}
FIXTURE
mkdir "$work/inputs"
cd "$work/inputs"
printf 'Body\n' > 'body draft.md'
printf 'Body\n' > './-body.md'
printf 'Owner note\n' > 'note draft.md'
: > empty.md
printf ' \t\r\n\n' > whitespace.md

fail() {
  printf 'cards-test: %s: %s\n' "$test" "$1" >&2
  cat "$work/out" "$work/err" "$work/unexpected" >&2
  exit 1
}
run() {
  test=$1
  local expected=$2 actual=0
  shift 2
  : > "$work/calls"
  : > "$work/writes"
  : > "$work/unexpected"
  "$@" > "$work/out" 2> "$work/err" || actual=$?
  [ ! -s "$work/unexpected" ] || fail 'unexpected gh call'
  [ "$actual" = "$expected" ] || fail "exit $actual, expected $expected"
}
output() { diff -u - "$work/out" || fail 'stdout differs'; }
writes() { diff -u - "$work/writes" || fail 'write calls differ'; }
silent() { [ ! -s "$work/err" ] || fail 'unexpected stderr'; }
read_only() { [ ! -s "$work/writes" ] || fail 'unexpected write'; }
before_reads() { [ ! -s "$work/calls" ] || fail 'unexpected GitHub call'; }
refused() {
  read_only
  output < /dev/null
  # jq's filename and line can differ by version and fixture path.
  sed -E 's/^jq: error \(at .*\): //' "$work/err" > "$work/diagnostic"
  printf '%s\n' "$1" | diff -u - "$work/diagnostic" || fail 'diagnostic differs'
}

run 'create with the first phase and active gh login' 0 bash "$cards/new.sh" 'Owner task' 'body draft.md' Studio -3.5
silent
output <<'EXPECTED'
#31 https://github.com/labcast/labcast-server/issues/31
EXPECTED
writes <<'EXPECTED'
["api","repos/labcast/labcast-server/issues","-f","title=Owner task","-F","body=@body draft.md","-f","assignees[]=joeldsouzax"]
["project","item-add","2","--owner","labcast","--url","https://github.com/labcast/labcast-server/issues/31","--format","json"]
["project","item-edit","--project-id","board","--id","new-item","--field-id","kind","--single-select-option-id","task"]
["project","item-edit","--project-id","board","--id","new-item","--field-id","phase","--single-select-option-id","first"]
["project","item-edit","--project-id","board","--id","new-item","--field-id","status","--single-select-option-id","active"]
["project","item-edit","--project-id","board","--id","new-item","--field-id","sequence","--number","-3.5"]
["project","item-edit","--project-id","board","--id","new-item","--field-id","stream","--single-select-option-id","studio"]
EXPECTED

run 'create with project overrides, token and chosen phase' 0 env \
  HARNESS_REPO=example/tools HARNESS_PROJECT_OWNER=roadmap HARNESS_PROJECT_NUMBER=17 HARNESS_ASSIGNEE=operator GH_TOKEN=test-token \
  EXPECT_REPO=example/tools EXPECT_OWNER=roadmap EXPECT_PROJECT=17 EXPECT_TOKEN=test-token \
  bash "$cards/new.sh" 'Owner task' 'body draft.md' Studio 0 --phase 'Phase 2'
silent
output <<'EXPECTED'
#31 https://github.com/example/tools/issues/31
EXPECTED
writes <<'EXPECTED'
["api","repos/example/tools/issues","-f","title=Owner task","-F","body=@body draft.md","-f","assignees[]=operator"]
["project","item-add","17","--owner","roadmap","--url","https://github.com/example/tools/issues/31","--format","json"]
["project","item-edit","--project-id","board","--id","new-item","--field-id","kind","--single-select-option-id","task"]
["project","item-edit","--project-id","board","--id","new-item","--field-id","phase","--single-select-option-id","second"]
["project","item-edit","--project-id","board","--id","new-item","--field-id","status","--single-select-option-id","active"]
["project","item-edit","--project-id","board","--id","new-item","--field-id","sequence","--number","0"]
["project","item-edit","--project-id","board","--id","new-item","--field-id","stream","--single-select-option-id","studio"]
EXPECTED

for stage in item-add item-edit; do
  run "issue created before $stage fails" 1 env FAIL_WRITE="project $stage" \
    bash "$cards/new.sh" 'Owner task' 'body draft.md' Studio 0
  output <<'EXPECTED'
#31 https://github.com/labcast/labcast-server/issues/31
EXPECTED
  diff -u - "$work/err" <<'EXPECTED'
fixture write failed
new: issue #31 exists with no board fields; no lane can select it. Set the fields or close it.
EXPECTED
  result=0
  grep -Fq '"--field-id","stream"' "$work/writes" || result=$?
  [ "$result" -eq 1 ] || fail 'failed creation must leave the workstream unset'
done
run 'partial board fields are removed before reporting the failed issue' 1 env FAIL_FIELD=sequence \
  bash "$cards/new.sh" 'Owner task' 'body draft.md' Studio 0
diff -u - "$work/err" <<'EXPECTED'
fixture write failed
new: issue #31 exists with no board fields; no lane can select it. Set the fields or close it.
EXPECTED
diff -u - <(tail -n 2 "$work/writes") <<'EXPECTED'
["project","item-edit","--project-id","board","--id","new-item","--field-id","sequence","--number","0"]
["project","item-delete","2","--owner","labcast","--id","new-item"]
EXPECTED

mkdir "$work/alternate"
cp -R "$cards" "$architecture/project.env" "$work/alternate/"
chmod -R u+w "$work/alternate/"
sed 's/^def phases:.*/def phases: ["Phase 2", "Phase 1"];/' "$architecture/backlog.jq" > "$work/alternate/backlog.jq"
run 'new dry run uses the changed backlog default' 0 bash "$work/alternate/cards/new.sh" 'Owner task' 'body draft.md' Studio 1 --dry-run
read_only
silent
output <<'EXPECTED'
Would run: gh api repos/labcast/labcast-server/issues -f title=Owner\ task -F body=@body\ draft.md -f assignees\[\]=joeldsouzax
Would run: gh project item-add 2 --owner labcast --url \<new\ issue\ URL\> --format json
Would run: gh project item-edit --project-id board --id \<new\ project\ item\ ID\> --field-id kind --single-select-option-id task
Would run: gh project item-edit --project-id board --id \<new\ project\ item\ ID\> --field-id phase --single-select-option-id second
Would run: gh project item-edit --project-id board --id \<new\ project\ item\ ID\> --field-id status --single-select-option-id active
Would run: gh project item-edit --project-id board --id \<new\ project\ item\ ID\> --field-id sequence --number 1
Would run: gh project item-edit --project-id board --id \<new\ project\ item\ ID\> --field-id stream --single-select-option-id studio
EXPECTED

run 'unknown phase' 1 bash "$cards/new.sh" Task 'body draft.md' Studio 1 --phase 'Phase 99'
refused 'new.sh: no option Phase 99 in field Phase'
run 'unknown workstream' 1 bash "$cards/new.sh" Task 'body draft.md' Unknown 1
refused 'new.sh: no option Unknown in field Workstream'
printf '%s\n' 'Unreadable' > unreadable.md
chmod 000 unreadable.md
bad_files=(missing.md empty.md .)
if [ ! -r unreadable.md ]; then bad_files+=(unreadable.md); fi
for body in "${bad_files[@]}"; do
  run "invalid body file $body" 1 bash "$cards/new.sh" Task "$body" Studio 1
  refused 'new.sh: body file must be readable and nonempty'
  before_reads
done
run 'whitespace-only body' 1 bash "$cards/new.sh" Task whitespace.md Studio 1
refused 'new.sh: body file must be readable and nonempty'
before_reads
run 'body filename starts with a dash' 0 bash "$cards/new.sh" Task -body.md Studio 1 --dry-run
read_only
silent
new_usage='Usage: new.sh <title> <body file> <workstream> <sequence> [--phase <phase>] [--task-id <id>] [--status <status>] [--dry-run]'
run 'invalid new option' 2 bash "$cards/new.sh" Task 'body draft.md' Studio 1 --bogus 'Phase 1'
refused "$new_usage"
before_reads
run 'missing phase value' 2 bash "$cards/new.sh" Task 'body draft.md' Studio 1 --phase
refused "$new_usage"
before_reads
run 'empty title' 2 bash "$cards/new.sh" ' ' 'body draft.md' Studio 1
refused "$new_usage"
before_reads
run 'invalid new sequence' 2 bash "$cards/new.sh" Task 'body draft.md' Studio NaN
refused "$new_usage"
before_reads
jq '.data.organization.projectV2.sequence.dataType = "TEXT"' "$work/fields.json" > "$work/text.json"
run 'new refuses a nonnumeric Sequence field' 1 env FIELDS="$work/text.json" bash "$cards/new.sh" Task 'body draft.md' Studio 1
refused 'new.sh: no numeric Sequence field'
jq '.errors = [{message:"access denied"}]' "$work/fields.json" > "$work/fields-errors.json"
run 'new stops on GraphQL errors' 1 env FIELDS="$work/fields-errors.json" bash "$cards/new.sh" Task 'body draft.md' Studio 1
refused 'new.sh: the board could not be read'

run 'change Sequence on second project page' 0 bash "$cards/sequence.sh" 31 -2
silent
output <<'EXPECTED'
#31 Sequence before: 0
#31 Sequence -2
EXPECTED
writes <<'EXPECTED'
["project","item-edit","--project-id","board","--id","our-item","--field-id","sequence","--number","-2"]
EXPECTED
jq -c '.data.organization.projectV2.field.id = "status"' "$work/membership.json" > "$work/status.json"
run 'change Status' 0 env MEMBERSHIP="$work/status.json" bash "$cards/status.sh" 31 Active
silent
output <<'EXPECTED'
#31 Status before: Needs review
#31 Status Active
EXPECTED
writes <<'EXPECTED'
["project","item-edit","--project-id","board","--id","our-item","--field-id","status","--single-select-option-id","active"]
EXPECTED

jq -c 'del(.data.repository.issue.projectItems.nodes[].value)' "$work/membership.json" > "$work/unset.json"
run 'plan Sequence from an unset value' 0 env MEMBERSHIP="$work/unset.json" bash "$cards/sequence.sh" --dry-run 31 0
silent
read_only
output <<'EXPECTED'
#31 Sequence before: (unset)
Would run: gh project item-edit --project-id board --id our-item --field-id sequence --number 0
#31 Sequence would be: 0
EXPECTED
run 'plan Status' 0 env MEMBERSHIP="$work/status.json" bash "$cards/status.sh" 31 Active --dry-run
silent
read_only
output <<'EXPECTED'
#31 Status before: Needs review
Would run: gh project item-edit --project-id board --id our-item --field-id status --single-select-option-id active
#31 Status would be: Active
EXPECTED

head -n 1 "$work/membership.json" > "$work/other-board.json"
run 'Sequence on another board' 1 env MEMBERSHIP="$work/other-board.json" bash "$cards/sequence.sh" 31 1
refused 'sequence.sh: #31 is not on the board'
run 'Status on another board' 1 env MEMBERSHIP="$work/other-board.json" bash "$cards/status.sh" 31 Active
refused 'status.sh: #31 is not on the board'
jq -sc '.[0] | .data.repository.issue = {__typename:"PullRequest"}' "$work/membership.json" > "$work/pull.json"
run 'Sequence refuses a pull request' 1 env MEMBERSHIP="$work/pull.json" bash "$cards/sequence.sh" 31 1
refused 'sequence.sh: #31 is a pull request; use an issue number'
run 'Status refuses a pull request' 1 env MEMBERSHIP="$work/pull.json" bash "$cards/status.sh" 31 Active
refused 'status.sh: #31 is a pull request; use an issue number'
run 'unknown Status' 1 env MEMBERSHIP="$work/status.json" bash "$cards/status.sh" 31 Other
refused 'status.sh: no Status option Other'
jq -c '.data.organization.projectV2.field = null' "$work/membership.json" > "$work/no-field.json"
run 'no board field' 1 env MEMBERSHIP="$work/no-field.json" bash "$cards/sequence.sh" 31 1
refused 'sequence.sh: no Sequence field'
jq -c '.data.organization.projectV2.field.dataType = "TEXT"' "$work/membership.json" > "$work/text-field.json"
run 'Sequence field has the wrong type' 1 env MEMBERSHIP="$work/text-field.json" bash "$cards/sequence.sh" 31 1
refused 'sequence.sh: no numeric Sequence field'
jq -sc '.[1].errors = [{message:"unavailable"}] | .[]' "$work/membership.json" > "$work/membership-errors.json"
run 'late field GraphQL error' 1 env MEMBERSHIP="$work/membership-errors.json" bash "$cards/sequence.sh" 31 1
refused 'sequence.sh: the board could not be read'
run 'invalid Sequence card' 2 bash "$cards/sequence.sh" 0 1
refused 'Usage: sequence.sh <card> <sequence> [--dry-run]'
before_reads
run 'Sequence exceeds the board number range' 2 bash "$cards/sequence.sh" 31 1e9999
refused 'Usage: sequence.sh <card> <sequence> [--dry-run]'
before_reads
run 'missing Sequence' 2 bash "$cards/sequence.sh" 31
refused 'Usage: sequence.sh <card> <sequence> [--dry-run]'
before_reads
run 'invalid Status card' 2 bash "$cards/status.sh" '31/32' Active
refused 'Usage: status.sh <card> <status> [--dry-run]'
before_reads
run 'empty Status' 2 bash "$cards/status.sh" 31 ''
refused 'Usage: status.sh <card> <status> [--dry-run]'
before_reads
run 'extra Status argument' 2 bash "$cards/status.sh" 31 Active extra
refused 'Usage: status.sh <card> <status> [--dry-run]'
before_reads

old_line='- [release](https://github.com/labcast/labcast-server/issues/7)'
new_line='- Ready after the first three steps.'
printf '%s\r\n' '## Goal' 'Keep the goal.' '## Dependencies' \
  '- https://github.com/labcast/labcast-server/issues/70' "$old_line" '## Notes' > "$work/before.md"
printf '%s' 'See https://github.com/labcast/labcast-server/issues/7 in these notes.' >> "$work/before.md"
printf '%s\r\n' '## Goal' 'Keep the goal.' '## Dependencies' \
  '- https://github.com/labcast/labcast-server/issues/70' "$new_line" '## Notes' > "$work/after.md"
printf '%s' 'See https://github.com/labcast/labcast-server/issues/7 in these notes.' >> "$work/after.md"

run 'release one dependency' 0 bash "$cards/release-dep.sh" 31 7 "$new_line" 'note draft.md'
silent
backup=$(sed -n 's/^#31 before: //p' "$work/out")
case "$backup" in "$TMPDIR"/release-dep.*/before.md) ;; *) fail 'missing backup path inside TMPDIR' ;; esac
printf '%s\n' "#31 old line: $old_line" "#31 before: $backup" '#31 updated 2026-11-20T12:00:00Z' \
  'https://github.com/labcast/labcast-server/issues/31#issuecomment-1' | output
writes <<EXPECTED
["api","-X","PATCH","repos/labcast/labcast-server/issues/31","-F","body=@${backup%/*}/after.md","--jq","\"#31 updated \" + .updated_at"]
["issue","comment","31","-R","labcast/labcast-server","--body-file","note draft.md"]
EXPECTED
cmp "$work/before.md" "$backup" || fail 'backup differs'
cmp "$work/after.md" "$work/patched.md" || fail 'patched body differs'
[ ! -e "${backup%/*}/after.md" ] || fail 'temporary patch file was kept'
rm "$backup"
rmdir "${backup%/*}"

run 'body written but note failed' 1 env FAIL_WRITE='issue comment' \
  bash "$cards/release-dep.sh" 31 7 "$new_line" 'note draft.md'
backup=$(sed -n 's/^#31 before: //p' "$work/out")
printf '%s\n' "release-dep: the body of #31 is changed; the note was NOT posted. Post it, or restore the body from $backup." |
  diff -u - <(tail -n 1 "$work/err")
cmp "$work/before.md" "$backup"
cmp "$work/after.md" "$work/patched.md"
rm "$backup"
rmdir "${backup%/*}"
run 'release refuses a pull request' 1 env IS_PULL=yes bash "$cards/release-dep.sh" 31 7 "$new_line" 'note draft.md'
refused 'release-dep.sh: #31 is a pull request; use an issue number'

run 'plan release without keeping a backup' 0 bash "$cards/release-dep.sh" 31 7 "$new_line" 'note draft.md' --dry-run
silent
read_only
patch_path=$(sed -n 's/^Would run: gh api -X PATCH .* -F body=@\([^ ]*\) --jq .*$/\1/p' "$work/out")
case "$patch_path" in "$TMPDIR"/release-dep.*/after.md) ;; *) fail 'missing planned patch path' ;; esac
output <<EXPECTED
#31 old line: $old_line
#31 new line: $new_line
Would run: gh api -X PATCH repos/labcast/labcast-server/issues/31 -F body=@$patch_path --jq \\"#31\\ updated\\ \\"\\ +\\ .updated_at
Would run: gh issue comment 31 -R labcast/labcast-server --body-file note\\ draft.md
EXPECTED

printf '%s\n' '## Dependencies' '- https://github.com/labcast/labcast-server/issues/70' '## Notes' "$old_line" > "$work/zero.md"
printf '%s\n' '## Notes' "$old_line" > "$work/no-section.md"
printf '%s\n' '## Dependencies' "$old_line" "- Also: $old_line" > "$work/multi.md"
printf '%s\n' '## Dependencies' "$old_line and $old_line" > "$work/same-line.md"
printf '%s\n' '## Dependencies' "$old_line" '## Notes' "$old_line" > "$work/duplicate.md"
for body in zero no-section multi same-line; do
  run "$body dependency links" 1 env ISSUE_BODY="$work/$body.md" bash "$cards/release-dep.sh" 31 7 "$new_line" 'note draft.md'
  refused '#31 does not link #7 exactly one time in Dependencies'
done
run 'link line duplicated outside Dependencies' 1 env ISSUE_BODY="$work/duplicate.md" bash "$cards/release-dep.sh" 31 7 "$new_line" 'note draft.md'
refused 'the link line is not in the body exactly one time'
for link in 'https://github.com/other/tools/issues/9' '[relative](issues/9)' 'other/tools#9' '#9'; do
  run "replacement contains $link" 1 bash "$cards/release-dep.sh" 31 7 "Ready with $link" 'note draft.md'
  refused 'release-dep.sh: replacement links to an issue'
  before_reads
done
for line in '' '   ' $'two\nlines' $'cr\rline'; do
  run 'replacement must be one nonempty line' 1 bash "$cards/release-dep.sh" 31 7 "$line" 'note draft.md'
  refused 'release-dep.sh: replacement must be one nonempty line'
  before_reads
done
for line in '## Notes' '- This changes ## Dependencies'; do
  run 'replacement would change the scope of Dependencies' 1 bash "$cards/release-dep.sh" 31 7 "$line" 'note draft.md'
  refused 'release-dep.sh: replacement changes the Dependencies section'
  before_reads
done
for note in "${bad_files[@]}"; do
  run "invalid note $note" 1 bash "$cards/release-dep.sh" 31 7 "$new_line" "$note"
  refused 'release-dep.sh: note file must be readable and nonempty'
  before_reads
done
release_usage='Usage: release-dep.sh <card> <dependency> <new text line> <note file> [--dry-run]'
run 'invalid release card' 2 bash "$cards/release-dep.sh" zero 7 "$new_line" 'note draft.md'
refused "$release_usage"
before_reads
run 'invalid dependency number' 2 bash "$cards/release-dep.sh" 31 0 "$new_line" 'note draft.md'
refused "$release_usage"
before_reads
run 'missing note argument' 2 bash "$cards/release-dep.sh" 31 7 "$new_line"
refused "$release_usage"
before_reads

for fault in two-lines extra-line; do
  run "proof detects $fault" 1 env BODY_FAULT="$fault" bash "$cards/release-dep.sh" 31 7 "$new_line" 'note draft.md'
  refused 'replacement did not change exactly one line'
done
run 'proof detects the old link still present' 1 env BODY_FAULT=link-left bash "$cards/release-dep.sh" 31 7 "$new_line" 'note draft.md'
refused 'a link to #7 is still in Dependencies'
[ -z "$(ls -A "$TMPDIR")" ] || fail 'refusals or dry-run left temporary files'

run 'list Merged cards' 0 bash "$cards/close-merged.sh"
read_only
silent
output <<'EXPECTED'
#6 would close: Done
#8 KEPT OPEN: Status Merged with 2 open boxes: Waiting
EXPECTED
run 'close only Merged open issues in this repository with all boxes ticked' 0 bash "$cards/close-merged.sh" close
silent
output <<'EXPECTED'
#6 closed: Done
#8 KEPT OPEN: Status Merged with 2 open boxes: Waiting
EXPECTED
writes <<'EXPECTED'
["issue","close","6","-R","labcast/labcast-server","--reason","completed","--comment","Closed by the harness owner: the board Status is Merged and every step and box is ticked. The graphs of the project count closed issues. Reopen the issue if a step is missing."]
EXPECTED
run 'plan closing eligible cards only' 0 bash "$cards/close-merged.sh" close --dry-run
read_only
silent
output <<'EXPECTED'
#6 would close: Done
Would run: gh issue close 6 -R labcast/labcast-server --reason completed --comment Closed\ by\ the\ harness\ owner:\ the\ board\ Status\ is\ Merged\ and\ every\ step\ and\ box\ is\ ticked.\ The\ graphs\ of\ the\ project\ count\ closed\ issues.\ Reopen\ the\ issue\ if\ a\ step\ is\ missing.
#8 KEPT OPEN: Status Merged with 2 open boxes: Waiting
EXPECTED
for checkbox in '* [ ]' '+ [ ]' '1. [ ]' '2) [ ]' '> - [ ]'; do
  jq -sc --arg checkbox "$checkbox" '.[1].data.organization.projectV2.items.nodes[0].content.body = ($checkbox + " later") | .[]' \
    "$work/board.json" > "$work/open-box.json"
  run "keeps a card with $checkbox open" 0 env BOARD="$work/open-box.json" bash "$cards/close-merged.sh" close
  silent
  read_only
  output <<'EXPECTED'
#6 KEPT OPEN: Status Merged with 1 open boxes: Done
#8 KEPT OPEN: Status Merged with 2 open boxes: Waiting
EXPECTED
done
run 'invalid close action' 2 bash "$cards/close-merged.sh" do-it
refused 'Usage: close-merged.sh [close] [--dry-run]'
before_reads
run 'extra close argument' 2 bash "$cards/close-merged.sh" close close
refused 'Usage: close-merged.sh [close] [--dry-run]'
before_reads

run 'GitHub fails after returning a board' 1 env BOARD_EXIT=3 bash "$cards/close-merged.sh" close
refused 'close-merged.sh: the board could not be read'
jq -sc '.[0].data.organization.projectV2.items |= (.totalCount = 0 | .nodes = []) | .[0]' "$work/board.json" > "$work/empty-board.json"
run 'empty board' 1 env BOARD="$work/empty-board.json" bash "$cards/close-merged.sh" close
refused 'close-merged: the board could not be read'
jq -sc '.[1].errors = [{message:"unavailable"}] | .[]' "$work/board.json" > "$work/board-errors.json"
run 'late board GraphQL error' 1 env BOARD="$work/board-errors.json" bash "$cards/close-merged.sh" close
refused 'close-merged: GraphQL errors'
jq -sc '.[0].data.organization.projectV2.items.nodes |= .[1:] | .[]' "$work/board.json" > "$work/board-short.json"
run 'incomplete board' 1 env BOARD="$work/board-short.json" bash "$cards/close-merged.sh" close
refused 'close-merged: incomplete item count'
jq -sc '.[1].data.organization.projectV2.items.totalCount = 7 | .[]' "$work/board.json" > "$work/board-total.json"
run 'board total changed between pages' 1 env BOARD="$work/board-total.json" bash "$cards/close-merged.sh" close
refused 'close-merged: incomplete item count'
jq -sc '.[1].data.organization.projectV2.items.nodes[2].id = "one" | .[]' "$work/board.json" > "$work/board-dupes.json"
run 'duplicate board items' 1 env BOARD="$work/board-dupes.json" bash "$cards/close-merged.sh" close
refused 'close-merged: duplicate item IDs'

echo 'cards-test: new, sequence, status, release-dep and close-merged pass'
