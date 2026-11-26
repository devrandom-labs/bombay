#!/usr/bin/env bash
# Self-test of the run supervisor, with a scripted lead and a fixed backlog.
# Run from the repository root.
set -euo pipefail

supervisor=$PWD/architecture/run.sh
backlog=$PWD/architecture/backlog.jq
turn_tests=$PWD/architecture/tests/turn-test.sh
gate=$PWD/architecture/gate.sh
work=$(mktemp -d)
. "$PWD/architecture/tests/own-processes.sh"
own_init "$work"
trap 'own_cleanup; rm -rf "$work"' EXIT
# Keep the lead and an exec of the supervisor on the shell that runs this test.
mkdir "$work/bin"
export RUN_TEST_BASH=$BASH
cat > "$work/bin/bash" << 'STUB'
#!/bin/sh
case "${STUB_DIR:+$1}" in
  */run.sh)
    echo "$1" >> "$STUB_DIR/starts"
    . "$OWN_HELPER"; own_record "$$" >> "$STUB_DIR/pids"
    [ ! -r "${RUN_STATE:-}" ] || cp "$RUN_STATE" "$STUB_DIR/restart.state"
    # Bound a broken restart loop by launches, without waiting for a clock.
    starts=$(wc -l < "$STUB_DIR/starts") || {
      status=$?
      [ "${RUN_TEST_EVIDENCE:-0}" != 1 ] ||
        printf 'supervisor start count lookup status=%s\n' "$status" >> "$STUB_DIR/lookup-status"
    }
    [ "$starts" -le 8 ] || exit 98 ;;
esac
exec "$RUN_TEST_BASH" "$@"
STUB
chmod +x "$work/bin/bash"
export PATH="$work/bin:$PATH"
# Keep defaults and overrides in this suite, including gates and restarts.
export XDG_CACHE_HOME="$work/cache" RUN_TURN="$work/turn.lock" GATE_LOCK="$work/gate.lock"
mkdir -p "$XDG_CACHE_HOME"
# Any missed landing seam fails by case name; never reach the installed clients.
RUN_TEST_GIT=$(command -v git)
export RUN_TEST_GIT
cat > "$work/bin/git" << 'STUB'
#!/bin/sh
case " $* " in *' push '*)
  echo "$RUN_TEST_NAME: real git push escaped its seam" >> "$STUB_DIR/seam-leak"
  exit 99 ;;
esac
exec "$RUN_TEST_GIT" "$@"
STUB
chmod +x "$work/bin/git"
export RUN_TEST_SAFE_GIT=$work/bin/git
# An accidental default gh call must fail the named case, even if the caller
# ignores its exit code. The label seam below never reaches the real gh.
export RUN_TEST_GH_SENTINEL="$work/unexpected-gh"
cat > "$work/bin/gh" << 'STUB'
#!/bin/sh
if [ "${STUB_PROJECT:-0}" = 1 ]; then exec $RUN_GH "$@"; fi
echo "${RUN_TEST_NAME:-outside run_case}: $*" >> "$RUN_TEST_GH_SENTINEL"
exit 99
STUB
chmod +x "$work/bin/gh"

export RUN_TEST_HOOKS=$work/hooks.sh
cat > "$RUN_TEST_HOOKS" <<'STUB'
run_fixture_hook() {
  local status=0
  # Give each fixture script its own error handling, even when the stub expects failures.
  bash -e -o pipefail -c '. "$RUN_TEST_HOOKS"; . "$1"' fixture-hook "$1" || status=$?
  if [ "$status" -ne 0 ]; then
    # Keep the inner hook's report when it failed during a setup action.
    if [ ! -s "$STUB_DIR/hook.failure" ]; then
      printf '%s: fixture %s %s failed (exit %s)\n' "$RUN_TEST_NAME" "${2:-hook}" "${1##*/}" "$status" > "$STUB_DIR/hook.failure"
    fi
    exit "$status"
  fi
}
STUB

cat > "$work/land.sh" <<'STUB'
. "$RUN_TEST_HOOKS"
if [ "${LAND_IGNORE_TERM:-0}" = 1 ] && [ "${1:-}" = "${LAND_IGNORE_ACTION:-push}" ]; then
  trap '' TERM
  echo "$1 ignores TERM" >&2
fi
bad_call() {
  printf '%s: %s\n' "${RUN_TEST_NAME:-outside run_case}" "$*" >> "$RUN_TEST_GH_SENTINEL"
  echo "$*" >&2
  exit 98
}
# P6 exercises the board reader through the existing unseamed gh path.
if [ "${STUB_PROJECT:-0}" = 1 ]; then
  echo "$*" >> gh.calls
  case "$1 $2" in
    'auth token') echo test-token; exit ;;
    'api graphql')
      jq '{data:{organization:{projectV2:{items:{
        totalCount: (.items | length), pageInfo: {hasNextPage:false, endCursor:null},
        nodes: [.items[] | {
          id: (.id // ("fixture-" + (.content.number | tostring))),
          status:{name:.status}, kind:{name:.kind}, phase:{name:.phase},
          workstream:{name:.workstream}, sequence:{number:.sequence}, taskId:{text:.["task ID"]},
          content: (.content + {__typename:"Issue", title:.title,
            assignees:{nodes:[(.assignees // [])[] | {login:.}]},
            labels:{nodes:[(.labels // [])[] | {name:.}]}})
        }]
      }}}}}' items.json
      exit ;;
    'run list') exit 0 ;;
  esac
fi
if [ "${1:-} ${2:-}" = 'issue view' ]; then
  echo "$*" >> labels.calls
  [ "$#" = 9 ] && [ "$4 $5 $6 $7 $8" = '-R labcast/labcast-server --json labels -q' ] \
    && [[ "$3" =~ ^[1-9][0-9]*$ ]] && [ "$9" = 'any(.labels[]; .name == "needs-joel")' ] \
    || bad_call "unsupported issue view call: $*"
  number=$3 filter=$9
  case "${LABEL_REPLY:-false}" in
    fail) echo true; exit 1 ;;
    empty) exit 0 ;;
    other) echo TRUE; exit 0 ;;
    items)
      jq --argjson n "$number" '{labels: [.items[] | select(.content.number == $n) | (.labels // [])[] | {name: .}]}' items.json \
        | jq -r "$filter" ;;
    *) printf '{"labels":[]}' | jq -r "$filter" ;;
  esac
  exit
fi
if [ "${1:-} ${2:-}" = 'issue edit' ]; then
  echo "$*" >> edit.calls
  [ "$#" = 7 ] && [ "$4 $5 $6 $7" = '-R labcast/labcast-server --remove-label ready-again' ] \
    && [[ "$3" =~ ^[1-9][0-9]*$ ]] || bad_call "unsupported issue edit call: $*"
  [ "${EDIT_FAIL:-0}" != 1 ] || { echo "gh: could not edit issue" >&2; exit 1; }
  [ "${EDIT_FAIL:-0}" != first ] || [ "$(wc -l < edit.calls)" -gt 1 ] || { echo "gh: could not edit issue" >&2; exit 1; }
  jq --argjson n "$3" '(.items[] | select(.content.number == $n) | .labels) -= ["ready-again"]' items.json > items.new
  mv items.new items.json
  echo "https://github.com/labcast/labcast-server/issues/$3"
  exit
fi
# gh 2.103.3 options supplied in S4. Reject any other option before an act.
case "$1 $2" in
  'push -q') action=push; push_ref=${4#HEAD:}; [ "$#" = 4 ] && [ "$3" = origin ] && [ "$4" = "HEAD:refs/heads/$(git branch --show-current)" ] || bad_call "unsupported $action call: $*" ;;
  'fetch -q') action=fetch; [ "$#" = 4 ] && [ "$3 $4" = 'origin main' ] || bad_call "unsupported $action call: $*" ;;
   'pr list') action=list; allowed=' -R --repo -H --head -B --base -s --state --json -q --jq ' ;;
  'pr create') action=create; allowed=' -R --base --head --title --body-file ' ;;
  'pr edit') action=edit; allowed=' -R --title -F --body-file -B --base ' ;;
  'pr merge') action=merge; allowed=' -R -s --squash --match-head-commit ' ;;
  'pr view') action=view; allowed=' -R --json -q --jq ' ;;
  *) bad_call "unexpected landing call: $*" ;;
esac
printf '%s\n' "$@" > "land.$action.args"
selector='' base='' branch='' state='' title='' body='' match='' json='' query=''
if [ "$1" = pr ]; then
  shift 2
  case "$action" in
    edit | merge | view)
      selector=${1:-}; shift
      [[ "$selector" =~ ^[1-9][0-9]*$ ]] || bad_call 'non-numeric PR selector' ;;
  esac
  while [ "$#" -gt 0 ]; do
    option=$1; shift
    case "$allowed" in *" $option "*) ;; *) bad_call "unsupported pr $action option: $option" ;; esac
    if [ "$action" = merge ] && { [ "$option" = --squash ] || [ "$option" = -s ]; }; then continue; fi
    [ "$#" -gt 0 ] || bad_call "unsupported $action call: $*"
    value=$1; shift
    case "$option" in
      -R | --repo) [ "$value" = labcast/labcast-server ] || bad_call "unsupported $action call: $*" ;;
      -B | --base) base=$value ;;
      -H | --head) branch=$value; [[ "$branch" != *:* ]] || bad_call "unsupported $action call: $*" ;;
      -s | --state) state=$value; case "$state" in open | closed | merged | all) ;; *) bad_call "unsupported pr list state: $state" ;; esac ;;
      --json)
        json=$value
        for field in ${json//,/ }; do
          case "$field" in number | url | state | baseRefName | headRefName | headRefOid | isCrossRepository | headRepositoryOwner | mergeCommit) ;; *) bad_call "unsupported JSON field: $field" ;; esac
        done ;;
      -q | --jq) query=$value ;;
      --title) title=$value ;;
      -F | --body-file) body=$value ;;
      --match-head-commit) match=$value ;;
    esac
  done
fi
# Every fixture has an unmerged step, and the remote owns each PR's full state.
if [ ! -f remote-prs.json ]; then
  echo '[]' > remote-prs.json
  if [ -n "${LAND_OPEN:-}" ]; then
    head=$(git rev-parse HEAD)
    jq -n --arg head "$head" '[{number:42,baseRefName:"main",headRefName:"feat/step-1",headRefOid:$head,isCrossRepository:false,state:"OPEN",mergeCommit:null,title:"old",body:"old"}]' > remote-prs.json
    case "$LAND_OPEN" in
      release) jq '.[0].baseRefName="release"' remote-prs.json > prs.new ;;
      two) jq '. + [.[0] | .number=43]' remote-prs.json > prs.new ;;
      mixed-base) jq '. + [.[0] | .number=43 | .baseRefName="release"]' remote-prs.json > prs.new ;;
      two-fork) jq '. + [.[0] | .number=43 | .isCrossRepository=true]' remote-prs.json > prs.new ;;
      head) jq --arg head "$(git rev-parse origin/main)" '.[0].headRefOid=$head' remote-prs.json > prs.new ;;
      fork) jq '.[0].isCrossRepository=true' remote-prs.json > prs.new ;;
      invalid) jq '.[0].number="invalid"' remote-prs.json > prs.new ;;
      numeric-text) jq '.[0].number="42"' remote-prs.json > prs.new ;;
      zero) jq '.[0].number=0' remote-prs.json > prs.new ;;
      negative) jq '.[0].number=-1' remote-prs.json > prs.new ;;
      fraction) jq '.[0].number=1.5' remote-prs.json > prs.new ;;
      *) cp remote-prs.json prs.new ;;
    esac
    mv prs.new remote-prs.json
  fi
  if [ "${LAND_CLOSED:-0}" = 1 ]; then
    jq --arg head "$(git rev-parse origin/main)" '. + [{number:41,baseRefName:"main",headRefName:"feat/step-1",headRefOid:$head,isCrossRepository:false,state:"CLOSED",mergeCommit:null,title:"old",body:"old"}]' remote-prs.json > prs.new
    mv prs.new remote-prs.json
  fi
fi
# Separate read and mutation logs make the no-act expectations visible.
echo "$action" >> remote.calls
echo "land $action" >> events
case "$action" in fetch | branch) echo "$action" >> land.reads ;; *) echo "$action" >> land.calls ;; esac
case "$action" in push | create | edit | merge) echo "$action" >> mutations ;; esac
if [ "$action" = fetch ] && { [ "${LAND_FETCH_FAIL:-0}" = 1 ] || { [ "${LAND_LAST_FETCH_FAIL:-0}" = 1 ] && [ -f premerge.view ] && [ ! -f merged.call ]; } || { [ -f merged.call ] && [ "${LAND_MERGED_FETCH_FAIL:-0}" = 1 ]; }; }; then
  [ ! -f merged.call ] || git fetch -q origin main
  echo 'fetch failed' >&2; exit 9
fi
if [ "$action" = view ] && [ -f merged.call ] && [ "${LAND_CONFIRM_HANG:-0}" = 1 ]; then echo 'confirmation still running' >&2; sleep 10; fi
if [ "$action" = view ] && [ -f merged.call ] && [ "${LAND_VIEW_FAIL:-}" = merge ]; then echo 'view failed' >&2; exit 9; fi
if [ "$action" = "${LAND_IGNORE_ACTION:-push}" ] && [ "${LAND_IGNORE_TERM:-0}" = 1 ]; then
  # Only the first call waits: a missing timeout must finish below the suite guard.
  if [ ! -f ignored-term-started ]; then
    touch ignored-term-started
    sleep 25
    touch ignored-term-finished
  fi
  exit 0
fi
fault=''
[ "${LAND_FAIL:-}" != "$action" ] || fault=fail
[ "${LAND_HANG:-}" != "$action" ] || fault=limit
if [ -z "$fault" ] || [ "${LAND_EFFECT:-0}" = 1 ]; then
  case "$action" in
    fetch) git fetch -q origin main ;;
    push)
      head=$(git rev-parse HEAD)
      git -C origin fetch -q "$PWD" "$head"
      git -C origin update-ref "$push_ref" "$head"
      jq --arg branch "${push_ref#refs/heads/}" --arg head "$head" '(.[] | select(.headRefName == $branch and .state == "OPEN" and .isCrossRepository == false)).headRefOid=$head' remote-prs.json > prs.new
      mv prs.new remote-prs.json ;;
    list)
      if [ "${LAND_OPEN:-}" = object ]; then
        jq '{pr:.[0]}' remote-prs.json
      elif [ -n "${LAND_LIST_RAW:-}" ]; then printf '%s\n' "$LAND_LIST_RAW"
      else
        jq --arg base "$base" --arg branch "$branch" --arg state "$state" \
          '[.[] | select(.headRefName == $branch and ($base == "" or .baseRefName == $base) and ($state == "all" or .state == ($state | ascii_upcase)))]' remote-prs.json > reply.json
        if [ "${LAND_OPEN:-}" = head ]; then
          jq --arg head "$(git rev-parse origin/main)" 'map(.headRefOid=$head)' reply.json > reply.new; mv reply.new reply.json
        fi
        jq --arg fields "$json" 'map(with_entries(select(.key as $key | $fields | split(",") | index($key))))' reply.json | jq -r "${query:-.}"
      fi ;;
    create)
      if jq -e --arg branch "$branch" --arg base "$base" 'any(.[]; .headRefName == $branch and .baseRefName == $base and .state == "OPEN")' remote-prs.json > /dev/null; then
        touch duplicate.create; echo 'duplicate create' >&2; exit 98
      fi
      number=$(jq '[.[].number,41] | max + 1' remote-prs.json)
      jq --argjson number "$number" --arg head "$(git rev-parse HEAD)" --arg base "$base" --arg branch "$branch" --arg title "$title" --rawfile body "$body" \
        '. + [{number:$number,baseRefName:$base,headRefName:$branch,headRefOid:$head,isCrossRepository:false,state:"OPEN",mergeCommit:null,title:$title,body:$body}]' remote-prs.json > prs.new
      mv prs.new remote-prs.json
      [ "${LAND_LOCAL_CHANGE:-0}" = 0 ] || run_fixture_hook ./local.after
      if [ "${LAND_BAD_CREATE:-0}" = 1 ]; then echo 'not-a-number'; else echo "https://github.com/labcast/labcast-server/pull/$number"; fi ;;
    edit)
      jq --argjson pr "$selector" --arg title "$title" --rawfile body "$body" '(.[] | select(.number == $pr)) |= (.title=$title | .body=$body)' remote-prs.json > prs.new
      mv prs.new remote-prs.json
      printf '%s\n' "$title" > land.title; cp "$body" land.body ;;
    view)
      [ -f merged.call ] || touch premerge.view
      jq --argjson pr "$selector" '.[] | select(.number == $pr)' remote-prs.json > pr.view
      if [ ! -f merged.call ]; then
        view=${LAND_VIEW:-}
        if [ -n "${LAND_STALE_VIEWS:-}" ] && [ "$(grep -c '^view$' remote.calls)" -le "$LAND_STALE_VIEWS" ]; then view=head; fi
        case "$view" in
          closed) jq '.state="CLOSED"' pr.view > view.new ;;
          head) jq --arg head "$(git rev-parse origin/main)" '.headRefOid=$head' pr.view > view.new ;;
          branch) jq '.headRefName="another"' pr.view > view.new ;;
          fork) jq '.isCrossRepository=true' pr.view > view.new ;;
          base) jq '.baseRefName="release"' pr.view > view.new ;;
          number) jq '.number=43' pr.view > view.new ;;
          numeric-text) jq '.number="42"' pr.view > view.new ;;
          invalid) echo 'invalid JSON' > view.new ;;
          *) cp pr.view view.new ;;
        esac
      else
        case "${LAND_CONFIRM:-}" in
          base) jq '.baseRefName="release"' pr.view > view.new ;;
          branch) jq '.headRefName="another"' pr.view > view.new ;;
          head) jq --arg head "$(git rev-parse origin/main)" '.headRefOid=$head' pr.view > view.new ;;
          fork) jq '.isCrossRepository=true' pr.view > view.new ;;
          open) jq '.state="OPEN"' pr.view > view.new ;;
          commit) jq '.mergeCommit=null' pr.view > view.new ;;
          ref) jq '.mergeCommit={oid:"origin/main"}' pr.view > view.new ;;
          *) cp pr.view view.new ;;
        esac
      fi
      jq --arg fields "$json" 'with_entries(select(.key as $key | $fields | split(",") | index($key)))' view.new | jq -r "${query:-.}" ;;
    merge)
      current=$(jq -r --argjson pr "$selector" '.[] | select(.number == $pr) | .state' remote-prs.json)
      [ "$current" = OPEN ] || { touch duplicate.merge; echo 'second merge' >&2; exit 98; }
      head=$(jq -r --argjson pr "$selector" '.[] | select(.number == $pr) | .headRefOid' remote-prs.json)
      [ "$head" = "$match" ] || { echo 'head does not match' >&2; exit 9; }
      if [ "${LAND_MERGE_ADVANCE:-0}" = 1 ]; then
        echo 'main moved inside merge' > origin/main-during-merge
        git -C origin add -f main-during-merge; git -C origin commit -qm during-merge
      fi
      main=$(git -C origin rev-parse main)
      printf '%s %s\n' "$main" "$head" >> merge.inputs
      tree=$(git -C origin merge-tree --write-tree "$main" "$head") || exit 9
      squash=$(printf 'squash PR %s\n' "$selector" | git -C origin -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit-tree "$tree" -p "$main")
      [ "${LAND_UNREACHABLE:-0}" = 1 ] || git -C origin reset -q --hard "$squash"
      jq --argjson pr "$selector" --arg squash "$squash" '(.[] | select(.number == $pr)) |= (.state="MERGED" | .mergeCommit={oid:$squash})' remote-prs.json > prs.new
      mv prs.new remote-prs.json
      touch merged.call
      [ ! -f land.after ] || run_fixture_hook ./land.after ;;
  esac
fi
if [ "${LAND_ADVANCE:-}" = "$action" ] && [ ! -f main.advanced ]; then
  touch main.advanced
  echo "$action moved main" > origin/main-during-call
  git -C origin add -f main-during-call; git -C origin commit -qm during-call
fi
[ -z "${LAND_STDERR:-}" ] || printf '%s\n' "$LAND_STDERR" >&2
if [ "${LAND_TERM:-}" = "$action" ] && [ ! -f term.sent ]; then
  touch term.sent
  . "$OWN_HELPER"; own_signal TERM "$(tail -n 1 pids)"
fi
if [ -n "$fault" ]; then
  touch act.failed
  if [ "$fault" = limit ]; then echo "$action still running" >&2; sleep 10
  else
    if [ "${LAND_FAIL_STDOUT:-0}" = 1 ]; then echo "$action final error"; else echo "$action final error" >&2; fi
    exit 9
  fi
fi
STUB


# A lead that answers from a script: line N is the result of session N.
# "-" writes no result; "sleep S" sleeps and writes nothing. Before a result,
# "full:" fills the scripted disk, "low:" leaves 13 GiB, "nospace:" prints the
# error of a full disk and "nospace28:" its Rust form, "refused:" prints the
# refusal of the gate, "reads:"
# prints source lines that only name those two, and "nogh:" takes the backlog
# away until the next sleep ("nogh2:" until the second sleep). "open:" creates
# the file open before a result. "takes N:" adds N seconds
# to the fake clock before it writes its result. "main:" publishes the next
# supervisor at the local origin; "edit:" replaces only the worktree copy;
# "outside:" replaces only the launcher's copy outside the repository;
# "mainfull:" publishes the next supervisor and fills the scripted disk;
# "mainbad:" publishes the supervisor from invalid.sh. Gate cases use
# "dirty:" for a tracked edit, "untracked:" for an untracked file, "nohead:"
# for an unreadable HEAD, "badindex:" for a failed status read,
# "ahead:" advances main, "aheadloose:" also leaves an untracked file,
# "nomain:" removes origin/main, "conflict:" for conflicting commits on main and
# the step, and "other:" to select card 2 first. "commit:" adds a repair,
# "branch:" switches branches and commits, "empty:" makes an empty commit,
# and "twin:" makes a sibling commit with the same tree. "merge:" merges main.
# "gatecode N:" sets the exit code for the next gates.
printf '#!%s\n' "$BASH" > "$work/lead.sh"
cat >> "$work/lead.sh" << 'EOF'
. "$RUN_TEST_HOOKS"
. "$OWN_HELPER"; own_record "$PPID" >/dev/null
echo "$EPOCHREALTIME" >> "$STUB_DIR/session.starts"
while [ $# -gt 0 ]; do [ "$1" = "-o" ] && out=$2; prompt=$1; shift; done
previous_count=$(cat "$STUB_DIR/calls" 2> /dev/null || echo 0) || {
  status=$?
  [ "${RUN_TEST_EVIDENCE:-0}" != 1 ] || printf 'lead count lookup status=%s\n' "$status" >> "$STUB_DIR/lookup-status"
}
count=$(($previous_count + 1))
echo "$count" > "$STUB_DIR/calls"
# Stop the supervisor too: an exit of this lead alone lets a broken run retry.
if [ "$count" -gt 32 ]; then
  echo "test lead exceeded 32 sessions" >&2
  own_signal TERM "$PPID"
  exit 98
fi
git rev-parse HEAD > "$STUB_DIR/head.$count" 2> /dev/null || true
echo "session $count" >> "$STUB_DIR/events"
cat clock 2> /dev/null >> "$STUB_DIR/session.times" || echo 0 >> "$STUB_DIR/session.times"
echo "$prompt" >> "$STUB_DIR/prompts"
echo "${RUN_STATE-unset}" >> "$STUB_DIR/state-vars"
# Optional shell actions use the same fixture and run before this result.
before_file="$STUB_DIR/before.$count"
[ ! -f "$before_file" ] || run_fixture_hook "$before_file" action
line=$(sed -n "${count}p" "$STUB_DIR/script")
status=$?
[ "${RUN_TEST_EVIDENCE:-0}" != 1 ] || printf 'session %s: script lookup status=%s\n' "$count" "$status" >> "$STUB_DIR/lookup-status"
if [ "${REPEAT_CARDS:-0}" = 1 ]; then
  number=${prompt#*card #}; number=${number%% *}
  line=$(cat "$STUB_DIR/result.$number")
fi
while :; do
case "$line" in
  gatecode\ *:*)
    code=${line#gatecode }; echo "${code%%:*}" > gate.code
    line=${line#*:} ;;
  empty:*)
    git -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit --allow-empty -qm request
    line=${line#empty:} ;;
  commit:* | branch:* | twin:*)
    if [ "${line%%:*}" = branch ]; then git switch -qc other origin/main; fi
    if [ "${line%%:*}" = twin ]; then
      git switch -qc twin origin/main
      git -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit --allow-empty -qm twin
    else
      echo "$count" >> repair
      git add -f repair
      git -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit -qm repair
    fi
    line=${line#*:} ;;
  merge:*)
    git merge -q --no-ff --no-edit origin/main
    line=${line#merge:} ;;
  dirty:*) echo dirty >> step; line=${line#dirty:} ;;
  untracked:*) touch loose; line=${line#untracked:} ;;
  nohead:*) git update-ref -d refs/heads/main; git read-tree --empty; line=${line#nohead:} ;;
  badindex:*) echo bad > .git/index; line=${line#badindex:} ;;
  nomain:*) git remote remove origin; line=${line#nomain:} ;;
  ahead:* | aheadloose:*)
    echo "main $count" > "$STUB_DIR/origin/step"
    git -C "$STUB_DIR/origin" add -f step
    git -C "$STUB_DIR/origin" -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit -qm main
    git fetch -q origin main
    [ "${line%%:*}" != aheadloose ] || touch loose
    line=${line#*:} ;;
  conflict:*)
    echo main > "$STUB_DIR/origin/step"
    git -C "$STUB_DIR/origin" add -f step
    git -C "$STUB_DIR/origin" -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit -qm main
    echo branch > step
    git add -f step
    git -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit -qm step
    line=${line#conflict:} ;;
  other:*)
    jq '(.items[] | select(.content.number == 2) | .sequence) = 0' items.json > items.new
    mv items.new items.json
    line=${line#other:} ;;
  takes\ *) seconds=${line%%:*}; seconds=${seconds#takes }
    echo "$(($(cat "$STUB_DIR/clock" 2> /dev/null || echo 0) + seconds))" > "$STUB_DIR/clock"
    line=${line#*:} ;;
  *) break ;;
esac
done
case "$line" in
  full:*) echo 1 > "$STUB_DIR/free"; line=${line#full:} ;;
  low:*) echo 13 > "$STUB_DIR/free"; line=${line#low:} ;;
  nospace:*) echo "error: writing a file: No space left on device"; line=${line#nospace:} ;;
  nospace28:*) printf 'error: failed to write x\n\nCaused by:\n  No space left on device (os error 28)\n'; line=${line#nospace28:} ;;
  refused:*) echo "gate: not enough free disk space for the full gate"; line=${line#refused:} ;;
  nogh:*) mv "$STUB_DIR/items.json" "$STUB_DIR/items.away"; echo 1 > "$STUB_DIR/away"; line=${line#nogh:} ;;
  nogh2:*) mv "$STUB_DIR/items.json" "$STUB_DIR/items.away"; echo 2 > "$STUB_DIR/away"; line=${line#nogh2:} ;;
  reads:*) echo "  grep -qE \"No space left on device|x\" and echo \"gate: not enough free disk space\""; line=${line#reads:} ;;
  open:*) touch "$STUB_DIR/open"; line=${line#open:} ;;
  main:* | mainfull:* | mainbad:*)
    [ "${line%%:*}" != mainfull ] || echo 1 > "$STUB_DIR/free"
    source="$STUB_DIR/next.sh"
    [ "${line%%:*}" != mainbad ] || source="$STUB_DIR/invalid.sh"
    cp "$source" "$STUB_DIR/origin/architecture/run.sh.new"
    mv "$STUB_DIR/origin/architecture/run.sh.new" "$STUB_DIR/origin/architecture/run.sh"
    git -C "$STUB_DIR/origin" add architecture/run.sh
    git -C "$STUB_DIR/origin" -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit -qm supervisor
    line=${line#*:} ;;
  edit:*)
    cp "$STUB_DIR/next.sh" "$STUB_DIR/architecture/run.sh.new"
    mv "$STUB_DIR/architecture/run.sh.new" "$STUB_DIR/architecture/run.sh"
    line=${line#edit:} ;;
  outside:*)
    cp "$STUB_DIR/next.sh" "$OUTSIDE.new"
    mv "$OUTSIDE.new" "$OUTSIDE"
    line=${line#outside:} ;;
esac
case "$line" in
  gate*) git rev-parse HEAD > "$STUB_DIR/request.$count" 2> /dev/null || true ;;
esac
case "$line" in
  -|"") ;;
  sleep*) sleep "${line#sleep }" ;;
  *) echo "STEP $line" > "$out" ;;
esac
EOF

# The notification command, and four cleanups for the scripted disk: one that
# frees space at its second call, one at its eighth, one at every second call,
# and one that always does. A sleep that records and advances the fake clock.
# It also brings back a backlog that "nogh:" or "nogh2:" took away.
mkdir "$work/sleep"
cat > "$work/sleep/sleep" << 'STUB'
#!/bin/sh
. "$RUN_TEST_HOOKS"
. "$OWN_HELPER"; own_record "$PPID" >/dev/null
echo "$1" >> sleeps
# Bound faulty polling without a real wait, and retain its status at every pass.
lookup_failed() {
  [ "${RUN_TEST_EVIDENCE:-0}" != 1 ] ||
    printf '%s lookup status=%s\n' "$2" "$1" >> "$STUB_DIR/lookup-status"
}
polls=$(wc -l < sleeps) || lookup_failed "$?" 'sleep count'
[ "$polls" -le 200 ] || { own_signal TERM "$PPID"; exit 98; }
cat .cache/run.status >> sleep.status
# Per-pass actions share the same fixture as before.N session actions.
poll_file="$STUB_DIR/poll.$(wc -l < sleeps | tr -d ' ')" || lookup_failed "$?" 'poll action check'
[ ! -f "$poll_file" ] || run_fixture_hook "$poll_file" action
polls=$(wc -l < sleeps) || lookup_failed "$?" 'main poll count'
if [ "$polls" = "${POLL_MAIN_AT:-never}" ]; then
  cp next.sh origin/architecture/run.sh
  git -C origin add architecture/run.sh
  git -C origin -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit -qm supervisor
fi
polls=$(wc -l < sleeps) || lookup_failed "$?" 'ready poll count'
if [ "$polls" = "${POLL_READY_AT:-never}" ]; then cp ready.json items.json; fi
clock=$(cat clock 2> /dev/null || echo 0) || lookup_failed "$?" 'sleep clock'
echo "$(($clock + $1))" > clock
if [ -n "${DAY_END:-}" ] && [ "$(cat clock)" -ge "$DAY_END" ]; then own_signal TERM "$PPID"; exit 0; fi
[ -f away ] || exit 0
left=$(($(cat away) - 1))
if [ "$left" -le 0 ]; then mv items.away items.json; rm away; else echo "$left" > away; fi
STUB
chmod +x "$work/sleep/sleep"
echo 'echo x >> cleanups; [ $(($(wc -l < cleanups) % 2)) -ne 0 ] || echo 100 > free' > "$work/even.sh"
echo 'echo "$*" >> notified' > "$work/notify.sh"
echo 'echo x >> cleanups; [ "$(wc -l < cleanups)" -lt 2 ] || echo 100 > free' > "$work/second.sh"
echo 'echo x >> cleanups; [ "$(wc -l < cleanups)" -lt 8 ] || echo 100 > free' > "$work/eighth.sh"
echo 'echo 100 > free' > "$work/always.sh"
echo '[ -e open ]' > "$work/open.sh"
cat > "$work/now.sh" <<'STUB'
cat clock 2> /dev/null || {
  status=$?
  [ "${RUN_TEST_EVIDENCE:-0}" != 1 ] || printf 'clock read status=%s\n' "$status" >> "$STUB_DIR/lookup-status"
  echo 0
}
STUB
echo 'echo cleanup >> events; echo 100 > free' > "$work/gate-clean.sh"
# The gate records order and status while it runs, writes both output streams
# and advances the test clock. A real sleep checks the timeout command.
cat > "$work/gate.sh" << 'STUB'
. "$RUN_TEST_HOOKS"
echo "gate $(cat calls)" >> events
cat .cache/run.status >> gate.status
git rev-parse HEAD >> gate.commits
echo "stdout gate $(wc -l < gate.status)"
echo "stderr gate $(wc -l < gate.status)" >&2
echo "$(($(cat clock 2> /dev/null || echo 0) + ${GATE_SECONDS:-0}))" > clock
[ "${GATE_COMMIT:-0}" = 0 ] || git -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit --allow-empty -qm during-gate
[ ! -f gate.after ] || run_fixture_hook ./gate.after
[ -z "${GATE_SLEEP:-}" ] || sleep "$GATE_SLEEP"
exit "$(cat gate.code 2> /dev/null || echo "${GATE_CODE:-0}")"
STUB
# The scripted disk. A run that still asks after 60 times waits without end:
# the disk then reports space, and the case fails.
cat > "$work/disk.sh" <<'STUB'
echo x >> probes
probes=$(wc -l < probes) || {
  status=$?
  [ "${RUN_TEST_EVIDENCE:-0}" != 1 ] ||
    printf 'disk probe count lookup status=%s\n' "$status" >> "$STUB_DIR/lookup-status"
}
if [ "$probes" -gt 60 ]; then touch gave-up; echo 100; else cat free; fi
STUB

# Two ready cards of the Studio lane.
card() { printf '{"content":{"number":%s,"body":""},"title":"card %s","task ID":"T%s","kind":"Task","phase":"Phase 1","workstream":"Studio","status":"Blocked","sequence":%s,"assignees":["joeldsouzax"]}' "$1" "$1" "$1" "$1"; }
printf '{"items":[%s,%s]}' "$(card 1)" "$(card 2)" > "$work/items.json"

# Runs one case in a fresh directory. Arguments: name, wanted exit code, the
# script of the lead (lines divided by "|"), then VAR=value settings. LANE names
# another lane than studio.
# A run that loops without end is stopped after two minutes, and its case fails.
guard=()
if command -v timeout > /dev/null; then
  guard=(timeout 120)
  export RUN_TEST_TIMEOUT=$(command -v timeout)
  mkdir "$work/timeout"
  printf '#!%s\n' "$RUN_TEST_BASH" > "$work/timeout/timeout"
  cat >> "$work/timeout/timeout" << 'STUB'
echo "$*" >> timeout.calls
started=$EPOCHREALTIME
"$RUN_TEST_TIMEOUT" "$@"
code=$?
printf '%s %s %s %s\n' "$started" "$EPOCHREALTIME" "$code" "$*" >> timeout.times
exit "$code"
STUB
  chmod +x "$work/timeout/timeout"
fi
case_number=0
run_case() {
  local name=$1 want=$2 script=$3; shift 3
  case_number=$((case_number + 1))
  dir=$work/case$case_number; mkdir -p "$dir"; cd "$dir"
  printf '%s\n' "$script" | tr '|' '\n' > script
  echo "${FREE:-100}" > free
  cp "${ITEMS:-$work/items.json}" items.json
  [ -z "${REVIEWED:-}" ] || { mkdir -p .cache; echo "$REVIEWED" > .cache/design-review; }
  local got=0 started=$EPOCHREALTIME
  env RUN_TEST_NAME="$name" RUN_GH="bash $work/land.sh" STUB_DIR="$dir" STEP_LEAD="bash $work/lead.sh" RUN_ITEMS="$work/items.json" RUN_SYNC=0 \
    RUN_RETRY_WAIT=0 RUN_CLOCK=1 RUN_QUICK=0 RUN_CLEAN=true STEP_FREE=0 RUN_REVIEW=0 \
    RUN_GATE="bash $work/gate.sh" RUN_TEST_EVIDENCE="${checks_evidence:-0}" \
    RUN_NOTIFY="bash $work/notify.sh" RUN_FREE="bash $work/disk.sh" "$@" \
    ${guard[@]+"${guard[@]}"} bash "${SUPERVISOR:-$supervisor}" "${LANE:-studio}" ${ONCE:-} > out 2>&1 || got=$?
  run_started=$started run_ended=$EPOCHREALTIME run_status=$got
  # The supervisor can handle a stub's exit status. A broken fixture fails the case.
  [ ! -s hook.failure ] || fail "$(cat hook.failure)"
  [ ! -s "$RUN_TEST_GH_SENTINEL" ] || fail "$name: default gh called: $(cat "$RUN_TEST_GH_SENTINEL")"
  [ "$got" -ne 124 ] || fail "$name: the run did not end in two minutes"
  [ ! -e gave-up ] || fail "$name: the run waited for disk space without end"
  [ ! -e seam-leak ] || fail "$(cat seam-leak)"
  [ "$got" -eq "$want" ] || fail "$name: exit $got, wanted $want"
  calls=$(cat calls 2> /dev/null || echo 0) || {
    local status=$?
    [ "${checks_evidence:-0}" != 1 ] || fail "$name: call count lookup status=$status" "$status"
    return "$status"
  }
}
fail() {
  local file
  echo "run-test: FAILED: $1"
  if [ "${checks_evidence:-0}" = 1 ]; then
    awk -v start="$run_started" -v end="$run_ended" -v status="$run_status" \
      'BEGIN { printf "run-test: supervisor status=%s, elapsed %.3f s\n", status, end - start }' || true
    for file in "$dir/events" "$dir/.cache/run.history"; do
      [ -s "$file" ] || continue
      printf 'run-test: last %s entry: ' "${file##*/}"
      tail -n 1 "$file" || true
    done
  fi
  cat "$dir/out" 2> /dev/null || true
  for file in "$dir/prompts" "$dir/remote.calls" "$dir/land.calls" "$dir/land.reads" \
    "$dir/mutations" "$dir/merge.inputs" "$dir"/land.*.args \
    "$dir/timeout.calls" "$dir/timeout.times" "$dir/session.starts" "$dir/lookup-status" \
    "$dir/.cache/land.log" "$dir/.cache/step"; do
    [ -f "$file" ] || continue
    printf '\nrun-test: evidence: %s\n' "${file#"$dir/"}"
    tail -n 60 "$file" | cut -c 1-2000
  done
  exit "${2:-1}"
}
expect() { grep -qE "$2" "$dir/out" || fail "$1: no line matches: $2"; }
sessions() { [ "$calls" -eq "$2" ] || fail "$1: $calls sessions, wanted $2"; }
order() {
  local index=0 card_number
  for card_number in "$@"; do
    index=$((index + 1))
    sed -n "${index}p" "$dir/prompts" | grep -q "card #$card_number in" || fail "$name: session $index was not on #$card_number"
  done
}


# Prepare shell actions for one session in the next case, without new lead words.
before() {
  dir=$work/case$((case_number + 1)); mkdir -p "$dir"
  printf '%s\n' "$2" > "$dir/before.$1"
}

for action in before.1 poll.1; do
  name="a failing $action stops its fixture case"
  dir=$work/case$((case_number + 1)); mkdir -p "$dir"
  printf '%s\n' 'touch action-started' 'false | true' 'touch action-continued' > "$dir/$action"
  if ( ONCE=--once run_case "$name" 0 'waiting 1 setup' PATH="$work/sleep:$PATH" ) > "$dir/fixture-result" 2>&1; then
    fail "$name: the fixture accepted the failing action"
  fi
  grep -Fxq "run-test: FAILED: $name: fixture action $action failed (exit 1)" "$dir/fixture-result" ||
    fail "$name: wrong failure: $(head -n 1 "$dir/fixture-result")"
  [ -e "$dir/action-started" ] || fail "$name: the action did not start"
  [ ! -e "$dir/action-continued" ] || fail "$name: the action continued after failure"
  [ ! -e "$dir/clock" ] || fail "$name: the fake continued after failure"
  case_number=$((case_number + 1))
done

name="the lead stub stops a run that exceeds its session bound"
case_number=$((case_number + 1))
dir=$work/case$case_number; mkdir -p "$dir"; cd "$dir"
echo 32 > calls
got=0
# One last call models a looping supervisor at the bound. It must kill that
# parent, rather than let it continue after this lead exits.
env STUB_DIR="$dir" "$BASH" -c 'trap "exit 143" TERM; bash "$1" -o ignored probe; echo survived > survived' \
  _ "$work/lead.sh" > out 2>&1 || got=$?
[ "$got" -eq 143 ] || fail "$name: exit $got, wanted TERM (143)"
[ ! -e survived ] || fail "$name: the supervisor survived the stub bound"
grep -Fq 'test lead exceeded 32 sessions' out || fail "$name: no bound message"

name="a blocked card and a failed card are parked, and the run goes on"
run_case "$name" 1 'blocked needs a decision|merged pr 7|failed boom|failed boom'
sessions "$name" 4
expect "$name" 'parked #1: blocked needs a decision'
expect "$name" 'parked #2: failed boom'
expect "$name" 'steps merged or planned in this run: 1'
expect "$name" 'no other card is ready'
grep -q 'studio: parked #1: blocked needs a decision' "$dir/notified" || fail "$name: Joel was not told about the parked card"
grep -q 'studio: ended: no other card is ready' "$dir/notified" || fail "$name: Joel was not told about the end"

name="a lead that cannot start is tried again and is not parked"
run_case "$name" 2 '-|-|-' RUN_QUICK=999 RUN_RETRIES=3
sessions "$name" 3
expect "$name" 'the lead could not start 3 times in a row'
grep -q 'parked #' "$dir/out" && fail "$name: a card was parked"

name="a failure that the lead states is not taken for a lead that cannot start"
run_case "$name" 1 'failed real|failed real|blocked b' RUN_QUICK=999
sessions "$name" 3
expect "$name" 'parked #1: failed real'

name="a waiting step continues on the same card"
run_case "$name" 1 'waiting 0 host busy|waiting 0 reviewer busy|merged pr 12|blocked x|blocked y'
sessions "$name" 5
expect "$name" 'waiting 0 minutes: reviewer busy'
expect "$name" 'parked #1: blocked x'
grep -q 'Checks run of main failed' "$dir/prompts" && fail "$name: a lead was told about a failure that did not happen"

# A gh that answers `run list` from the file `runs` and applies the -q filter.
cat > "$work/gh.sh" << 'STUB'
echo "$*" >> gh.calls
[ ! -e checks-unreadable ] || { cat checks-unreadable; exit 1; }
while [ $# -gt 0 ]; do [ "$1" = -q ] && filter=$2; shift; done
status=0; jq -r "$filter" "$STUB_RUNS" || status=$?
if [ "${RUN_TEST_EVIDENCE:-0}" = 1 ] && [ "$status" != 0 ]; then
  printf 'Checks read status=%s\n' "$status" >> "$STUB_DIR/lookup-status"
fi
exit "$status"
STUB
echo '[{"databaseId":4242,"conclusion":"failure","event":"schedule"}]' > "$work/failed.json"
echo '[{"databaseId":4247,"conclusion":"failure","event":"push"},{"databaseId":4243,"conclusion":"success","event":"workflow_dispatch"},{"databaseId":4242,"conclusion":"failure","event":"schedule"}]' > "$work/green.json"

name="a failed Checks run of main is told to the lead of each session"
run_case "$name" 1 'merged pr 1|blocked a|blocked b' RUN_CHECKS="bash $work/gh.sh" STUB_RUNS="$work/failed.json"
sessions "$name" 3
[ "$(grep -c 'The last Checks run of main failed: run 4242\.' "$dir/prompts")" -eq 3 ] || fail "$name: not every lead was told"
grep -q -- '--branch main --status completed --limit 100' "$dir/gh.calls" || fail "$name: the query does not ask for the last finished runs of main"
grep -q -- '--event' "$dir/gh.calls" && fail "$name: the query looks at one kind of run only"

name="a cancelled run and a skipped run do not hide the failed run before them"
echo '[{"databaseId":4245,"conclusion":"cancelled","event":"workflow_dispatch"},{"databaseId":4248,"conclusion":"skipped","event":"schedule"},{"databaseId":4244,"conclusion":"timed_out","event":"schedule"},{"databaseId":4243,"conclusion":"success","event":"schedule"}]' > "$work/cancelled.json"
run_case "$name" 1 'merged pr 1|blocked a|blocked b' RUN_CHECKS="bash $work/gh.sh" STUB_RUNS="$work/cancelled.json"
sessions "$name" 3
[ "$(grep -c 'The last Checks run of main failed: run 4244\.' "$dir/prompts")" -eq 3 ] || fail "$name: the lead was not told about run 4244"

name="only cancelled runs tell the lead nothing"
echo '[{"databaseId":4246,"conclusion":"cancelled","event":"schedule"}]' > "$work/only-cancelled.json"
run_case "$name" 1 'merged pr 1|blocked a|blocked b' RUN_CHECKS="bash $work/gh.sh" STUB_RUNS="$work/only-cancelled.json"
sessions "$name" 3
grep -q 'Checks run of main failed' "$dir/prompts" && fail "$name: a lead was told about a failure"

name="a green manual run clears a failed night, and an earlier push check decides nothing"
run_case "$name" 1 'merged pr 1|blocked a|blocked b' RUN_CHECKS="bash $work/gh.sh" STUB_RUNS="$work/green.json"
sessions "$name" 3
grep -q 'Checks run of main failed' "$dir/prompts" && fail "$name: a lead was told about a failure"

name="a card that only waits is parked after six waits"
run_case "$name" 1 'waiting 0 a|waiting 0 a|waiting 0 a|waiting 0 a|waiting 0 a|waiting 0 a|waiting 0 a|blocked y'
sessions "$name" 8
expect "$name" 'parked #1: waited six times'

name="a card that is planned twice is parked"
run_case "$name" 1 'planned|planned|blocked z'
sessions "$name" 3
expect "$name" 'parked #1: planned twice without a step'

name="the run stops when GitHub cannot be read"
run_case "$name" 2 'merged' RUN_ITEMS=/nonexistent/items.json RUN_RETRIES=2
sessions "$name" 0
expect "$name" 'GitHub could not be read 2 times in a row'
grep -q 'studio: stopped: GitHub could not be read' "$dir/notified" || fail "$name: Joel was not told about the stop"

name="GitHub failures that are not in a row do not stop the run"
run_case "$name" 1 'nogh:merged pr 1|nogh:merged pr 2|nogh:merged pr 3|blocked a|blocked b' RUN_ITEMS=items.json RUN_RETRIES=2 PATH="$work/sleep:$PATH"
sessions "$name" 5
[ "$(grep -c 'GitHub could not be read; next try' "$dir/out")" -eq 3 ] || fail "$name: GitHub did not fail three times"

name="GitHub failures and lead failures are counted apart"
run_case "$name" 1 'nogh2:-|nogh2:-|blocked a|blocked b' RUN_ITEMS=items.json RUN_QUICK=999 RUN_RETRIES=3 PATH="$work/sleep:$PATH"
sessions "$name" 4
[ "$(grep -c 'GitHub could not be read; next try' "$dir/out")" -eq 2 ] || fail "$name: GitHub did not fail twice"
grep -q 'could not start 3 times' "$dir/out" && fail "$name: GitHub failures were counted as lead failures"

name="a session that only read the words of a full disk counts against its card"
run_case "$name" 1 'reads:failed tests|reads:failed tests|blocked b'
sessions "$name" 3
expect "$name" 'parked #1: failed tests'

name="a full disk makes the run wait, and it goes on when there is space"
FREE=1 run_case "$name" 1 'merged pr 1|blocked a|blocked b' STEP_FREE=15 RUN_FREE="bash $work/disk.sh" RUN_CLEAN="bash $work/second.sh"
sessions "$name" 3
expect "$name" 'waiting for disk space'
expect "$name" 'steps merged or planned in this run: 1'

name="a disk that stays full never stops the run; Joel hears at the first wait and at every sixth"
FREE=1 run_case "$name" 1 'merged pr 1|blocked a|blocked b' STEP_FREE=15 RUN_FREE="bash $work/disk.sh" RUN_CLEAN="bash $work/eighth.sh" RUN_RETRIES=3 \
  RUN_RETRY_WAIT=7 PATH="$work/sleep:$PATH"
sessions "$name" 3
[ "$(grep -c '^7$' "$dir/sleeps")" -eq 7 ] || fail "$name: the run did not sleep at each of the seven waits"
[ "$(grep -c 'waiting for disk space; next try' "$dir/out")" -eq 7 ] || fail "$name: the run did not wait seven times"
[ "$(grep -c 'waiting for disk space' "$dir/notified")" -eq 2 ] || fail "$name: Joel was not told exactly twice about the wait"

name="Joel hears again when the disk fills a second time"
FREE=1 run_case "$name" 1 'full:merged pr 1|blocked a|blocked b' STEP_FREE=15 RUN_FREE="bash $work/disk.sh" RUN_CLEAN="bash $work/even.sh"
sessions "$name" 3
[ "$(grep -c 'waiting for disk space' "$dir/notified")" -eq 2 ] || fail "$name: Joel was not told at both first waits"

name="RUN_CLEAN receives own-target before GiB, before a session starts"
cat > "$work/arguments.sh" << 'STUB'
printf '%s\n' "$@" > cleanup.args
[ ! -e calls ] || exit 88
echo 100 > free
STUB
FREE=1 run_case "$name" 1 'merged pr 1|blocked a|blocked b' STEP_FREE=20 RUN_CLEAN="bash $work/arguments.sh"
[ "$(cat "$dir/cleanup.args")" = "$(printf '%s\n' --own-target 20)" ] || fail "$name: arguments"
sessions "$name" 3

name="the default cleanup passes own-target to the space script, also from a path with a space"
# The copy is writable, also when the source is a read-only Nix store path.
mkdir -p "$work/a b" "$work/gc"; cp -R "$(dirname "$supervisor")" "$work/a b/architecture"; chmod -R u+w "$work/a b"
mv "$work/a b/architecture/space.sh" "$work/a b/architecture/real-space.sh"
cat > "$work/a b/architecture/space.sh" << 'STUB'
printf '%s\n' "$@" > cleanup.args
if [ "${BASH_SOURCE[0]}" = "$0" ]; then exec bash "$REAL_SPACE" "$@"; fi
. "$REAL_SPACE" "$@"
STUB
printf '#!/bin/sh\necho 100 > free\n' > "$work/gc/nix"; chmod +x "$work/gc/nix"
FREE=1 SUPERVISOR="$work/a b/architecture/run.sh" run_case "$name" 1 'merged pr 1|blocked a|blocked b' STEP_FREE=15 RUN_FREE="bash $work/disk.sh" \
  RUN_CLEAN= REAL_SPACE="$work/a b/architecture/real-space.sh" SPACE_FREE='cat free' SPACE_CACHE="$work/none" SPACE_BUILDS="$work/none" PATH="$work/gc:$PATH"
sessions "$name" 3
[ "$(cat "$dir/cleanup.args")" = "$(printf '%s\n' --own-target 15)" ] || fail "$name: default arguments"
expect "$name" 'space: 1 GiB free, 15 needed'
grep -q 'waiting for disk space' "$dir/out" && fail "$name: the space script freed nothing"

name="a session that fails on a full disk does not count against its card"
run_case "$name" 1 'full:failed no space|full:failed no space|merged pr 2|blocked a|blocked b' STEP_FREE=15 RUN_FREE="bash $work/disk.sh" RUN_CLEAN="bash $work/always.sh"
sessions "$name" 5
expect "$name" 'this failure does not count against the card'
expect "$name" 'parked #1: blocked a'
grep -q 'parked #1: failed' "$dir/out" && fail "$name: the card was parked for the disk"

name="a session whose log shows a full disk does not count either"
run_case "$name" 1 'nospace:failed build|nospace:failed build|merged pr 2|blocked a|blocked b' STEP_FREE=15 RUN_FREE="bash $work/disk.sh"
sessions "$name" 5
expect "$name" 'parked #1: blocked a'

name="the Rust form of the full-disk error does not count either"
run_case "$name" 1 'nospace28:failed build|nospace28:failed build|merged pr 2|blocked a|blocked b' STEP_FREE=15
sessions "$name" 5
expect "$name" 'parked #1: blocked a'

name="a session that the gate refused for disk space does not count either"
run_case "$name" 1 'refused:failed gate|refused:failed gate|merged pr 2|blocked a|blocked b' STEP_FREE=15 RUN_FREE="bash $work/disk.sh"
sessions "$name" 5
expect "$name" 'parked #1: blocked a'

name="a lead that ends at once on a full disk is not taken for a lead that cannot start"
run_case "$name" 1 'full:-|full:-|full:-|blocked b' STEP_FREE=15 RUN_FREE="bash $work/disk.sh" RUN_CLEAN="bash $work/always.sh" RUN_QUICK=999 RUN_RETRIES=3
sessions "$name" 4
expect "$name" 'parked #1: the disk filled during three sessions'

name="the count of full-disk sessions starts again for the next card and after a merge"
run_case "$name" 1 'full:failed x|full:failed x|merged pr 1|full:failed x|full:failed x|blocked a|full:failed y|full:failed y|blocked b' \
  STEP_FREE=15 RUN_FREE="bash $work/disk.sh" RUN_CLEAN="bash $work/always.sh"
sessions "$name" 9
expect "$name" 'parked #1: blocked a'
expect "$name" 'parked #2: blocked b'
grep -q 'the disk filled during three sessions' "$dir/out" && fail "$name: a card was parked for the disk"

name="a card is parked after three sessions with a full disk"
run_case "$name" 1 'full:failed x|full:failed x|full:failed x|blocked b' STEP_FREE=15 RUN_FREE="bash $work/disk.sh" RUN_CLEAN="bash $work/always.sh"
sessions "$name" 4
expect "$name" 'parked #1: the disk filled during three sessions: failed x'

name="a failure with some space left counts against the card"
run_case "$name" 1 'low:failed tests|low:failed tests|blocked b' STEP_FREE=15 RUN_FREE="bash $work/disk.sh" RUN_CLEAN="bash $work/always.sh"
sessions "$name" 3
expect "$name" 'parked #1: failed tests'

if command -v timeout > /dev/null; then
  name="a session that passes its limit fails, and its card is parked at the second time"
  run_case "$name" 1 'sleep 5|sleep 5|blocked w' STEP_LIMIT=1
  sessions "$name" 3
  expect "$name" 'parked #1: failed the session reached its limit'
else
  echo 'run-test: unavailable: a session that passes its limit fails, and its card is parked at the second time: timeout is not on PATH'
fi

reviews() { grep -c "weekly design review of the studio lane" "$dir/prompts" || true; }

# A review that is tried without end can only be stopped by the time limit of
# run_case, so these cases need `timeout`.
if command -v timeout > /dev/null; then

# This case leaves RUN_REVIEW empty: the review is on unless it is turned off.
name="the first session of a week is the design review, and only that one"
run_case "$name" 1 'reviewed 2|merged pr 1|blocked a|blocked b' RUN_REVIEW= RUN_WEEK=2026-46
sessions "$name" 4
[ "$(reviews)" -eq 1 ] || fail "$name: $(reviews) review sessions"
sed -n 1p "$dir/prompts" | grep -q "weekly design review" || fail "$name: the review was not the first session"
[ "$(cat "$dir/.cache/design-review")" = "2026-46 done" ] || fail "$name: the week is not recorded as done"

name="a design review that fails is tried once more after a card session, and then left for the week"
run_case "$name" 1 'failed x|merged pr 1|waiting 60 judge busy|blocked a|blocked b' RUN_REVIEW=1 RUN_WEEK=2026-46
sessions "$name" 5
[ "$(reviews)" -eq 2 ] || fail "$name: $(reviews) review sessions"
[ "$(grep -n "weekly design review" "$dir/prompts" | cut -d: -f1 | tr '\n' ' ')" = "1 3 " ] || fail "$name: the second try did not wait for a card session"
[ "$(cat "$dir/.cache/design-review")" = "2026-46 2" ] || fail "$name: the tries are not recorded"
[ "$(grep -c 'weekly design review failed twice' "$dir/notified")" -eq 1 ] || fail "$name: Joel was not told once that the review is left for the week"
grep -q 'parked the weekly' "$dir/out" && fail "$name: the review was parked as a card"
grep -q 'weekly design review of the studio lane for week 2026-46\.' "$dir/prompts" || fail "$name: the prompt does not name the week"

name="the wait after a failed review holds over a restart, and a card session ends it"
REVIEWED="2026-46 1 pause" ONCE=--once run_case "$name" 0 'merged pr 1' RUN_REVIEW=1 RUN_WEEK=2026-46
sessions "$name" 1
[ "$(reviews)" -eq 0 ] || fail "$name: the review was tried again at once"
[ "$(cat "$dir/.cache/design-review")" = "2026-46 1" ] || fail "$name: the card session did not end the wait"

name="a week with a finished review has no second review"
REVIEWED="2026-46 done" run_case "$name" 1 'merged pr 1|blocked a|blocked b' RUN_REVIEW=1 RUN_WEEK=2026-46
sessions "$name" 3
[ "$(reviews)" -eq 0 ] || fail "$name: $(reviews) review sessions"

name="a new week brings a new review"
REVIEWED="2026-45 done" run_case "$name" 1 'reviewed 0|merged pr 1|blocked a|blocked b' RUN_REVIEW=1 RUN_WEEK=2026-46
sessions "$name" 4
[ "$(reviews)" -eq 1 ] || fail "$name: $(reviews) review sessions"

name="the design review also runs when no card is ready"
echo '{"items":[]}' > "$work/empty.json"
run_case "$name" 0 'reviewed 0' RUN_REVIEW=1 RUN_WEEK=2026-46 RUN_ITEMS="$work/empty.json"
sessions "$name" 1
[ "$(reviews)" -eq 1 ] || fail "$name: $(reviews) review sessions"
expect "$name" 'no ready card in studio'

name="a lead that cannot start is no try of the design review"
run_case "$name" 1 '-|-|-|reviewed 0|merged pr 1|blocked a|blocked b' RUN_REVIEW=1 RUN_WEEK=2026-46 RUN_QUICK=999
sessions "$name" 7
[ "$(reviews)" -eq 4 ] || fail "$name: $(reviews) review sessions"
[ "$(cat "$dir/.cache/design-review")" = "2026-46 done" ] || fail "$name: the review did not happen after the lead came back"

name="--once ends after the design review"
ONCE=--once run_case "$name" 0 'reviewed 0|merged pr 1' RUN_REVIEW=1 RUN_WEEK=2026-46
sessions "$name" 1

name="a state file with odd content does not lose the review"
REVIEWED="2026-46" run_case "$name" 1 'reviewed 1 apps/studio/src/program|merged pr 1|blocked a|blocked b' RUN_REVIEW=1 RUN_WEEK=2026-46
sessions "$name" 4
[ "$(reviews)" -eq 1 ] || fail "$name: $(reviews) review sessions"

else
  printf 'run-test: unavailable: %s: timeout is not on PATH\n' \
    'the first session of a week is the design review, and only that one' \
    'a design review that fails is tried once more after a card session, and then left for the week' \
    'the wait after a failed review holds over a restart, and a card session ends it' \
    'a week with a finished review has no second review' \
    'a new week brings a new review' \
    'the design review also runs when no card is ready' \
    'a lead that cannot start is no try of the design review' \
    '--once ends after the design review' \
    'a state file with odd content does not lose the review'
fi

# A DevOps card beside the two Studio cards. The Nix lane takes it.
ops_card() { card "$1" | sed 's/"Studio"/"DevOps"/'; }
printf '{"items":[%s,%s,%s]}' "$(card 1)" "$(card 2)" "$(ops_card 9)" > "$work/ops.json"

name="the Nix lane takes the DevOps cards and has no weekly design review"
LANE=nix run_case "$name" 1 'blocked a decision' RUN_ITEMS="$work/ops.json" RUN_REVIEW=1 RUN_WEEK=2026-46
sessions "$name" 1
expect "$name" 'parked #9: blocked a decision'
grep -q 'card #9 in the nix lane' "$dir/prompts" || fail "$name: the lead did not get the DevOps card"
grep -q 'weekly design review' "$dir/prompts" && fail "$name: the Nix lane got a design review"

name="the Studio lane leaves the DevOps cards"
run_case "$name" 1 'blocked a|blocked b' RUN_ITEMS="$work/ops.json"
sessions "$name" 2
grep -q 'card #9' "$dir/prompts" && fail "$name: the Studio lane took a DevOps card"

name="an unknown lane is refused"
# Without the refusal this run finds no card and ends with 0. The settings
# keep such a run from a design review and from a wait.
dir=$work/nolane; mkdir -p "$dir"; cd "$dir"; got=0
env STUB_DIR="$dir" STEP_LEAD="bash $work/lead.sh" RUN_ITEMS="$work/items.json" RUN_REVIEW=0 RUN_RETRY_WAIT=0 \
  RUN_RETRIES=1 ${guard[@]+"${guard[@]}"} bash "$supervisor" mobile > out 2>&1 || got=$?
[ "$got" -eq 2 ] || fail "$name: exit $got, wanted 2"
expect "$name" 'unknown lane mobile'

# Card 3 of the Studio lane waits for card 9 of the DevOps workstream, for
# card 11, which has no kind and no assignee, and for card 77, which is not in
# the project. Card 4 of the Studio lane waits for card 3 of its own lane.
name="the status names each card outside the run of the lane that blocks a card"
dir=$work/status; mkdir -p "$dir"; cd "$dir"
waiting=$(card 3 | sed 's|"body":""|"body":"## Dependencies\\n\\n- [O9](https://github.com/labcast/labcast-server/issues/9)\\n- [O11](https://github.com/labcast/labcast-server/issues/11)\\n- [X](https://github.com/labcast/labcast-server/issues/77)\\n"|')
bare=$(ops_card 11 | sed 's/"kind":"Task",//; s/"assignees":\["joeldsouzax"\]/"assignees":[]/')
own=$(card 4 | sed 's|"body":""|"body":"## Dependencies\\n\\n- [T3](https://github.com/labcast/labcast-server/issues/3)\\n"|')
printf '{"items":[%s,%s,%s,%s]}' "$waiting" "$(ops_card 9)" "$own" "$bare" > items.json
jq -r --arg lane studio --arg mode status --arg skip "" -f "$backlog" items.json > out 2>&1 || fail "$name: the status did not run"
expect "$name" '^open 2, ready 0,'
expect "$name" '^  #9 Task DevOps Phase 1 joeldsouzax blocks 1: card 9$'
expect "$name" '^  #11 - DevOps Phase 1 nobody blocks 1: card 11$'
expect "$name" '^  #77 not in the project blocks 1$'
grep -q '^  #3 ' out && fail "$name: the lane names its own card as a card outside its run"

diff -u - out <<'EXPECTED' || fail "$name: legacy status changed"
open 2, ready 0, waiting for Joel's answer 0
ready now:
cards outside the run of this lane that block open cards:
  #9 Task DevOps Phase 1 joeldsouzax blocks 1: card 9
  #11 - DevOps Phase 1 nobody blocks 1: card 11
  #77 not in the project blocks 1
EXPECTED

name="status lists the lane's loop cards and their loop dependencies"
case_number=$((case_number + 1))
dir=$work/status-loops; mkdir -p "$dir"; cd "$dir"
printf '{"items":[%s,%s,%s,%s]}' "$(card 10)" "$(ops_card 2)" "$(card 1)" "$(card 20)" \
  | jq '.items[0].content.body="## Dependencies\n/issues/10\n/issues/2\n/issues/20\n/issues/2\n/issues/77"
      | .items[1].content.body="## Dependencies\n/issues/10"
      | .items[2].content.body="## Dependencies\n/issues/1"
      | .items[2].assignees=[]' > items.json
jq -r --arg lane studio --arg mode status --arg skip '' -f "$backlog" items.json > out 2>&1 || fail "$name: status failed"
diff -u - out <<'EXPECTED' || fail "$name: wrong loop cards or dependencies"
open 2, ready 1, waiting for Joel's answer 0
ready now:
  #20	T20	card 20
cards outside the run of this lane that block open cards:
  #2 Task DevOps Phase 1 joeldsouzax blocks 2: card 2
  #77 not in the project blocks 1
cards of this lane in a dependency loop:
  #1 waits for #1
  #10 waits for #2 #10
EXPECTED

# A Linux check owed by a dependency does not hold its dependent card.
for scenario in next status unlabelled mixed; do
  name="linux-owed: $scenario dependency readiness"
  case_number=$((case_number + 1))
  dir=$work/linux-owed-$scenario; mkdir -p "$dir"; cd "$dir"
  printf '{"items":[%s,%s]}' "$(card 3)" "$(ops_card 9)" \
    | jq '.items[0].content.body="## Dependencies\nhttps://github.com/labcast/labcast-server/issues/9"
      | .items[1].labels=["linux-owed", "needs-joel"]' > items.json
  mode=next
  case "$scenario" in
    status) mode=status ;;
    unlabelled) jq '.items[1].labels=["needs-joel"]' items.json > items.new; mv items.new items.json ;;
    mixed)
      jq --argjson dependency "$(ops_card 11)" '.items += [$dependency]
        | .items[0].content.body += "\nhttps://github.com/labcast/labcast-server/issues/11"' items.json > items.new
      mv items.new items.json ;;
  esac
  jq -r --arg lane studio --arg mode "$mode" --arg skip '' -f "$backlog" items.json > out 2>&1 || fail "$name: selection failed"
  case "$scenario" in
    next) [ "$(cat out)" = $'#3\tT3\tcard 3' ] || fail "$name: dependent not selected" ;;
    status)
      expect "$name" '^open 1, ready 1, waiting for Joel.s answer 0$'
      expect "$name" $'^  #3\tT3\tcard 3$'
      ! grep -q 'blocks ' out || fail "$name: dependency still listed as blocking" ;;
    *) [ ! -s out ] || fail "$name: unlabelled dependency did not hold the card" ;;
  esac
  # The owed card stays open and selectable, with its own Joel hold intact.
  jq -r --arg lane nix --arg mode joel --arg skip '' -f "$backlog" items.json > out
  [ "$(cat out)" = 1 ] || fail "$name: the owed card lost its Joel hold"
  jq '.items[1].labels=["linux-owed"]' items.json > items.new; mv items.new items.json
  jq -r --arg lane nix --arg mode next --arg skip '' -f "$backlog" items.json > out
  [ "$(cat out)" = $'#9\tT9\tcard 9' ] || fail "$name: the owed card is no longer selected"
done

name="without a test clock the run uses its own seconds"
run_case "$name" 1 'later 0 x|blocked a|blocked b' PATH="$work/sleep:$PATH" RUN_OPEN=false
sessions "$name" 3
expect "$name" 'set aside #1 for 0 minutes: x'
for prompt in '1 #1' '2 #1' '3 #2'; do
  read -r line card_number <<< "$prompt"
  sed -n "${line}p" "$dir/prompts" | grep -q "card $card_number in" || fail "$name: prompt $line does not name $card_number"
done
[ ! -e "$dir/sleeps" ] || fail "$name: the run called sleep"

name="a card that waits without an open change is set aside and the next card is worked"
run_case "$name" 1 'later 2 nightly evidence|blocked b|merged pr 9|blocked a' RUN_RETRY_WAIT=600 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false
sessions "$name" 4
expect "$name" 'set aside #1 for 2 minutes: nightly evidence'
grep -q 'set aside #1 for 2 minutes: nightly evidence' "$dir/.cache/run.history" || fail "$name: the wait was not noted"
for prompt in '1 #1' '2 #2' '3 #1' '4 #1'; do
  read -r line card_number <<< "$prompt"
  sed -n "${line}p" "$dir/prompts" | grep -q "card $card_number in" || fail "$name: prompt $line does not name $card_number"
done

name="a card that waits with an open change is not set aside"
run_case "$name" 1 'later 1 gate busy|merged pr 3|blocked a|blocked b' RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=true
sessions "$name" 4
expect "$name" 'waiting 1 minutes: gate busy'
grep -q 'set aside' "$dir/out" && fail "$name: a card with an open change was set aside"
sed -n 2p "$dir/prompts" | grep -q 'card #1 in' || fail "$name: prompt 2 does not name #1"

name="a waiting card is never set aside"
run_case "$name" 1 'waiting 1 x|merged pr 3|blocked a|blocked b' RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false
sessions "$name" 4
expect "$name" 'waiting 1 minutes: x'
grep -q 'set aside' "$dir/out" && fail "$name: a waiting card was set aside"
sed -n 2p "$dir/prompts" | grep -q 'card #1 in' || fail "$name: prompt 2 does not name #1"

name="minutes with a leading zero"
run_case "$name" 1 'waiting 08 x|blocked a|blocked b' RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
sessions "$name" 3
expect "$name" 'waiting 8 minutes: x'
[ "$(cat "$dir/sleeps")" = 8 ] || fail "$name: the run did not sleep for eight minutes"

name="minutes with too many digits"
run_case "$name" 1 'waiting 99999999999999999999 x|blocked a|blocked b' RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
sessions "$name" 3
expect "$name" 'waiting 120 minutes: x'
[ "$(cat "$dir/sleeps")" = 120 ] || fail "$name: the run did not cap the wait at 120 minutes"

name="minutes that would wrap below zero"
run_case "$name" 1 'waiting 9999999999999999999 x|blocked a|blocked b' RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
sessions "$name" 3
expect "$name" 'waiting 120 minutes: x'
[ "$(cat "$dir/sleeps")" = 120 ] || fail "$name: the run did not cap the wait at 120 minutes"

name="a card that comes back from its wait keeps its count of waits"
run_case "$name" 1 'later 1 n|blocked b|later 0 n|later 0 n|later 0 n|later 0 n|later 0 n|later 0 n|blocked never' RUN_RETRY_WAIT=600 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false
sessions "$name" 8
expect "$name" 'parked #1: waited six times: 0 n$'

name="a card that comes back keeps its count of failures"
run_case "$name" 1 'failed x|later 1 y|blocked b|failed x|blocked never' RUN_RETRY_WAIT=600 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false
sessions "$name" 4
expect "$name" 'parked #1: failed x'

name="a card does not take the failure count of another card"
run_case "$name" 1 'failed x|later 1 n|failed y|blocked b|blocked a' RUN_RETRY_WAIT=600 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false
sessions "$name" 5
grep -q 'parked #2: failed y' "$dir/out" && fail "$name: card 2 took the failure count of card 1"
expect "$name" 'parked #2: blocked b'
expect "$name" 'parked #1: blocked a'

name="a card that comes back keeps its count of full-disk sessions"
run_case "$name" 1 'full:failed x|later 1 y|blocked b|full:failed x|full:failed x|blocked never' RUN_RETRY_WAIT=600 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" \
  RUN_OPEN=false RUN_CLOCK=5 STEP_FREE=15 RUN_FREE="bash $work/disk.sh" RUN_CLEAN="bash $work/always.sh"
sessions "$name" 5
expect "$name" 'parked #1: the disk filled during three sessions: failed x'

name="a card that comes back keeps its planned result"
run_case "$name" 1 'planned|later 1 y|blocked b|planned|blocked never' RUN_RETRY_WAIT=600 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false
sessions "$name" 4
expect "$name" 'parked #1: planned twice without a step'

name="a card does not take the planned result of another card"
run_case "$name" 1 'planned|later 1 n|planned|blocked b|blocked a' RUN_RETRY_WAIT=600 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false
sessions "$name" 5
grep -q 'parked #2: planned twice' "$dir/out" && fail "$name: card 2 took the planned result of card 1"
expect "$name" 'parked #2: blocked b'
expect "$name" 'parked #1: blocked a'

name="a second waiting card keeps its count when the first card comes back"
run_case "$name" 1 'later 1 a|takes 1:waiting 0 b|planned|blocked a|waiting 0 b|waiting 0 b|waiting 0 b|waiting 0 b|waiting 0 b|waiting 0 b|blocked never' RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false
sessions "$name" 10
expect "$name" 'parked #2: waited six times: 0 b$'

name="a card keeps its planned result while a set-aside card comes back"
run_case "$name" 1 'later 1 n|takes 1:planned|later 1 n|takes 1:planned|blocked a|blocked never' RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false
sessions "$name" 5
expect "$name" 'parked #2: planned twice without a step'
grep -q 'blocked never' "$dir/out" && fail "$name: the last scripted line was used"

name="a card keeps its failures while a set-aside card comes back"
run_case "$name" 1 'later 1 n|takes 1:failed x|later 1 n|takes 1:failed x|blocked a|blocked never' RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false
sessions "$name" 5
expect "$name" 'parked #2: failed x'
grep -q 'blocked never' "$dir/out" && fail "$name: the last scripted line was used"

name="a merge clears the wait count of a card that came back"
run_case "$name" 1 'later 1 n|blocked b|merged pr 9|later 0 n|later 0 n|later 0 n|later 0 n|later 0 n|later 0 n|blocked a' RUN_RETRY_WAIT=600 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false
sessions "$name" 10
expect "$name" 'parked #1: blocked a'
grep -q 'waited six times' "$dir/out" && fail "$name: the old wait count was restored after the merge"

name="a second waiting card sleeps while the first one is set aside"
# The second card takes GitHub away for two sleeps. Its wait must sleep once;
# the next GitHub read then stops the run before the first card's long wait.
run_case "$name" 2 'later 120 first|nogh2:waiting 1 second' RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false RUN_ITEMS=items.json RUN_RETRIES=1
sessions "$name" 2
expect "$name" 'set aside #1 for 120 minutes: first'
expect "$name" 'waiting 1 minutes: second'
grep -q 'set aside #2' "$dir/out" && fail "$name: the second card was set aside too"
[ "$(cat "$dir/sleeps")" = 1 ] || fail "$name: the second card did not sleep for one minute"
sed -n 2p "$dir/prompts" | grep -q 'card #2 in' || fail "$name: prompt 2 does not name #2"

name="a second later card is set aside while the first one is set aside"
run_case "$name" 1 'later 120 first|later 1 second|blocked b|blocked a' RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" \
  RUN_OPEN=false RUN_RETRY_WAIT=600
sessions "$name" 4
expect "$name" 'set aside #1 for 120 minutes: first'
expect "$name" 'set aside #2 for 1 minutes: second'
sed -n 3p "$dir/prompts" | grep -q 'card #2 in' || fail "$name: the second card missed its earlier deadline"
[ "$(cat "$dir/session.times")" = "$(printf '0\n0\n1\n120')" ] || fail "$name: the deadlines changed"

name="after a sleep the card that slept is next, before the set-aside card"
run_case "$name" 1 'later 1 n|waiting 2 gate|merged pr 5|blocked a|blocked b' RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false
sessions "$name" 5
for prompt in '1 #1' '2 #2' '3 #2' '4 #1' '5 #2'; do
  read -r line card_number <<< "$prompt"
  sed -n "${line}p" "$dir/prompts" | grep -q "card $card_number in" || fail "$name: prompt $line does not name $card_number"
done

name="a card does not come back from its wait while another step is open"
before 3 'rm -f open'
run_case "$name" 1 'later 1 n|takes 1:open:planned|merged pr 5|blocked a|blocked b' RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN="bash $work/open.sh"
sessions "$name" 5
for prompt in '1 #1' '2 #2' '3 #2' '4 #1' '5 #2'; do
  read -r line card_number <<< "$prompt"
  sed -n "${line}p" "$dir/prompts" | grep -q "card $card_number in" || fail "$name: prompt $line does not name $card_number"
done

name="a card comes back when no other card is ready, also with a change left open"
run_case "$name" 1 'later 1 n|open:blocked b|blocked a' RUN_RETRY_WAIT=600 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN="bash $work/open.sh"
sessions "$name" 3
for prompt in '1 #1' '2 #2' '3 #1'; do
  read -r line card_number <<< "$prompt"
  sed -n "${line}p" "$dir/prompts" | grep -q "card $card_number in" || fail "$name: prompt $line does not name $card_number"
done

name="the run sleeps when no card is ready and a card is set aside"
run_case "$name" 1 'later 2 x|blocked b|blocked a' RUN_RETRY_WAIT=600 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false
sessions "$name" 3
[ "$(cat "$dir/sleeps")" = 2 ] || fail "$name: the run did not sleep for two seconds"
[ "$(wc -l < "$dir/sleeps")" -eq 1 ] || fail "$name: the run did not sleep exactly once"

if command -v git > /dev/null; then
  # Each run owns its script. The lead changes the local origin during a
  # session; the next fetch and merge must bring that version into the run.
  supervisor_repo() {
    dir=$work/case$((case_number + 1)); mkdir -p "$dir/architecture"; cd "$dir"
    git init -q -b main
    git config user.name Test
    git config user.email test@example.com
    git config commit.gpgsign false
    printf '*\n!loose\n' > .gitignore
    echo base > step
    cp "$supervisor" architecture/run.sh
    cp "$backlog" architecture/backlog.jq
    # Cases edit these copies even when the source is a read-only Nix store path.
    chmod u+w architecture/run.sh architecture/backlog.jq
    git add -f architecture/run.sh architecture/backlog.jq step
    git -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit -qm base
    git clone -q . origin
    # Cloning does not copy the local identity; never guess it from the host.
    git -C origin config user.useConfigOnly true
    git -C origin config user.name "$(git config user.name)"
    git -C origin config user.email "$(git config user.email)"
    git remote add origin "$dir/origin"
    git fetch -q origin main
    sed 's/say "session on /say "main session on /' architecture/run.sh > next.sh
  }
  no_restart() {
    grep -q 'started again with the supervisor of main' "$dir/out" && fail "$name: the supervisor restarted"
    grep -q 'main session on' "$dir/out" && fail "$name: the new supervisor ran"
    return 0
  }
  no_supervisor_line() {
    if grep -qi supervisor "$dir/out" "$dir/.cache/run.history" "$dir/notified"; then
      fail "$name: an unchanged or unmerged supervisor was reported"
    fi
  }
  restarted_once() {
    [ "$(grep -c 'started again with the supervisor of main' "$dir/out")" -eq 1 ] || fail "$name: not exactly one restart"
    [ "$(grep -c 'started again with the supervisor of main' "$dir/.cache/run.history")" -eq 1 ] || fail "$name: not exactly one restart note"
    [ "$(wc -l < "$dir/starts")" -eq 2 ] || fail "$name: not exactly two supervisor launches"
    [ "$(tail -n 1 "$dir/starts")" = architecture/run.sh ] || fail "$name: the restart did not use the worktree copy"
    [ "$(sort -u "$dir/pids" | wc -l)" -eq 1 ] || fail "$name: the restart changed the process id"
    [ ! -e "$dir/.cache/run.state" ] || fail "$name: the state file was not removed after the read"
    grep -qv '^unset$' "$dir/state-vars" && fail "$name: RUN_STATE reached the lead"
    return 0
  }
  outside_supervisor() {
    # The launcher's root needs the same backlog, but lives outside this repo.
    outside=$work/launcher$((case_number + 1))/architecture/run.sh
    mkdir -p "$(dirname "$outside")"
    cp architecture/run.sh "$outside"
    cp "$backlog" "$(dirname "$outside")/backlog.jq"
    chmod u+w "$outside" "$(dirname "$outside")/backlog.jq"
  }
  invalid_once() {
    local message='the supervisor of main does not parse; the run keeps its supervisor'
    [ "$(grep -c "$message" "$dir/out")" -eq 1 ] || fail "$name: the invalid change was not said exactly once"
    [ "$(grep -c "$message" "$dir/.cache/run.history")" -eq 1 ] || fail "$name: the invalid change was not noted exactly once"
    [ "$(grep -c "studio: $message" "$dir/notified")" -eq 1 ] || fail "$name: the invalid change was not notified exactly once"
  }

  # Landing cases use real local Git objects and a strict, stateful remote fake.
  landing_fixture() {
    supervisor_repo
    git switch -qc feat/step-1
    echo 'the reviewed step' > change
    git add -f change; git commit -qm step
    if git merge-base --is-ancestor HEAD origin/main; then fail "$name: fixture step is already on main"; fi
    cat > pr-text <<'TEXT'
A title with "quotes", `backticks` and $(touch title-executed)

The lead text: 'quotes', `backticks`, $(touch body-executed).
Second line.
TEXT
    before 1 'mkdir -p .cache
      reviewed_head=$(git rev-parse HEAD)
      printf "Reviewed-head: %s\n" "$reviewed_head" > .cache/pr.md
      cat pr-text >> .cache/pr.md
      cp .cache/pr.md prepared.md
      [ ! -f prepare.after ] || run_fixture_hook ./prepare.after'
  }
  landing_case() {
    run_case "$name" "${LAND_WANT:-1}" "$1" RUN_PUSH="bash $work/land.sh" "${@:2}"
  }
  landing_prompt() { sed -n "${1}p" "$dir/prompts"; }
  landing_spans() {
    read -r kill_span continue_span < <(
      awk -v call="bash $work/land.sh $1 " -v lead="$(sed -n '2p' "$dir/session.starts")" \
        'index($0, call) { printf "%.6f %.6f\n", $2 - $1, lead - $2 }' "$dir/timeout.times"
    )
  }
  no_landing() { [ ! -e "$dir/land.calls" ] || fail "$name: an ineligible step tried landing"; }
  pending_landing() {
    mkdir -p .cache
    request=$(git rev-parse HEAD)
    printf 'gate_requests[1]=%s\ngate_branches[1]=feat/step-1\n' "$request" > .cache/pending
    printf 'Reviewed-head: %s\n' "$request" > .cache/pr.md
    cat pr-text >> .cache/pr.md
  }

  for hook in prepare.after gate.after local.after land.after; do
    name="a failing $hook stops its fixture case"
    landing_fixture
    printf '%s\n' 'false | true' 'touch hook-continued' > "$hook"
    local_change=0; [ "$hook" != local.after ] || local_change=1
    if ( landing_case 'gate|blocked a|blocked b' LAND_LOCAL_CHANGE="$local_change" ) > fixture-result 2>&1; then
      fail "$name: the fixture accepted the failing hook"
    fi
    grep -Fxq "run-test: FAILED: $name: fixture hook $hook failed (exit 1)" fixture-result ||
      fail "$name: wrong failure: $(head -n 1 fixture-result)"
    [ ! -e hook-continued ] || fail "$name: the hook continued after failure"
    if [ "$hook" = local.after ]; then
      [ "$(cat mutations)" = "$(printf 'push\ncreate')" ] || fail "$name: the fake create did not stop"
    fi
    case_number=$((case_number + 1))
  done

  name="a failed review head lookup stops the manifest setup"
  landing_fixture
  {
    printf '%s\n' 'git() { if [ "$*" = "rev-parse HEAD" ]; then return 27; fi; command git "$@"; }'
    cat before.1
  } > before.new
  mv before.new before.1
  if ( landing_case 'gate|blocked a|blocked b' ) > fixture-result 2>&1; then
    fail "$name: the fixture accepted the failed lookup"
  fi
  grep -Fxq "run-test: FAILED: $name: fixture action before.1 failed (exit 27)" fixture-result ||
    fail "$name: wrong failure: $(head -n 1 fixture-result)"
  [ ! -e .cache/pr.md ] || fail "$name: the failed lookup wrote a manifest"
  case_number=$((case_number + 1))

  name="main did not move so the fresh gate lands a real squash and tells the next lead once"
  landing_fixture
  landing_case 'gate|planned|blocked a|blocked b' PATH="$work/timeout:$PATH"
  sessions "$name" 4
  head=$(cat "$dir/gate.commits"); squash=$(git -C "$dir/origin" rev-parse main)
  [ "$head" != "$squash" ] || fail "$name: remote used the step as its squash commit"
  [ "$(git -C "$dir" rev-parse "$head^{tree}")" = "$(git -C "$dir/origin" rev-parse "$squash^{tree}")" ] || fail "$name: squash lost the step tree"
  git -C "$dir" merge-base --is-ancestor "$squash" origin/main || fail "$name: squash was not fetched"
  [ "$(grep -Fc '#1 STEP merged 42 (the run)' "$dir/.cache/run.history")" -eq 1 ] || fail "$name: merged history is missing or duplicated"
  [ "$(cat "$dir/mutations")" = "$(printf 'push\ncreate\nmerge')" ] || fail "$name: mutations repeated or a session landed the step"
  [ ! -f "$dir/.cache/pr.md" ] && [ ! -f "$dir/.cache/landing" ] || fail "$name: completed landing files survived"
  landing_prompt 2 | grep -Fq "The run merged this step as pull request 42 after the full gate passed on $head." || fail "$name: next lead lost the merge note"
  [ "$(grep -c 'The run merged this step' "$dir/prompts")" -eq 1 ] || fail "$name: merge note repeated"
  ! grep -q 'full gate on commit' "$dir/prompts" || fail "$name: a gate result survived the merge"
  expect "$name" 'steps merged or planned in this run: 2$'
  [ "$(cat "$dir/land.push.args")" = "$(printf 'push\n-q\norigin\nHEAD:refs/heads/feat/step-1')" ] || fail "$name: wrong push or hooks disabled"
  [ "$(grep -c '^list$' "$dir/remote.calls")" -eq 1 ] || fail "$name: landing did not use exactly one PR list"
  ! grep -Fxq -- '--base' "$dir/land.list.args" || fail "$name: PR list hid another base"
  [ "$(cat "$dir/land.merge.args")" = "$(printf 'pr\nmerge\n42\n-R\nlabcast/labcast-server\n--squash\n--match-head-commit\n%s' "$head")" ] || fail "$name: wrong merge or gate head"
  jq -e '.[0].title == "A title with \"quotes\", `backticks` and $(touch title-executed)"' "$dir/remote-prs.json" > /dev/null || fail "$name: title changed"
  grep -Fq '$(touch body-executed)' "$dir/.cache/land-body.md" || fail "$name: body was evaluated or changed"
  grep -Fq "Full gate: passed on $head in the automatic run." "$dir/.cache/land-body.md" || fail "$name: gate evidence was lost"
  [ ! -e "$dir/title-executed" ] && [ ! -e "$dir/body-executed" ] || fail "$name: PR text ran as shell code"
  if command -v timeout > /dev/null; then
    [ "$(grep -c -- "^--signal=TERM --kill-after=10 300 bash $work/land.sh " "$dir/timeout.calls")" -eq 9 ] || fail "$name: a landing read or act lacks the default limit"
  fi

  name="landing still works when timeout is absent from PATH"
  landing_fixture
  mkdir -p "$work/land-no-timeout"
  for tool in bash sh git jq awk basename cat cksum cp date dirname env grep mkdir mv rm sed sort tail tr wc; do
    ln -sf "$(command -v "$tool")" "$work/land-no-timeout/$tool"
  done
  no_timeout_path="$work/bin:$work/land-no-timeout"
  if env PATH="$no_timeout_path" sh -c 'command -v timeout' > /dev/null; then fail "$name: fixture still has timeout"; fi
  saved_guard=( ${guard[@]+"${guard[@]}"} ); guard=()
  landing_case 'gate|blocked a|blocked b' PATH="$no_timeout_path"
  guard=( ${saved_guard[@]+"${saved_guard[@]}"} )
  [ "$(cat "$dir/mutations")" = "$(printf 'push\ncreate\nmerge')" ] || fail "$name: no-timeout landing failed"
  expect "$name" 'steps merged or planned in this run: 1$'

  name="a clean merge of main after the request lands the gated merge commit"
  landing_fixture
  landing_case 'ahead:gate|blocked a|blocked b' RUN_SYNC=1
  request=$(cat "$dir/request.1"); head=$(cat "$dir/gate.commits")
  [ "$head" != "$request" ] || fail "$name: main did not merge into the request"
  [ "$(head -n 1 "$dir/prepared.md")" = "Reviewed-head: $request" ] || fail "$name: the file did not name the reviewed request"
  [ "$(tail -n 1 "$dir/land.merge.args")" = "$head" ] || fail "$name: merge did not match the gated merge head"
  expect "$name" '#1 STEP merged 42 \(the run\)$'

  name="an existing pull request gets the prepared title and text before its merge"
  landing_fixture
  landing_case 'gate|blocked a|blocked b' LAND_OPEN=42
  [ "$(cat "$dir/mutations")" = "$(printf 'push\nedit\nmerge')" ] || fail "$name: existing PR was duplicated or not edited"
  [ "$(cat "$dir/land.edit.args")" = "$(printf 'pr\nedit\n42\n-R\nlabcast/labcast-server\n--title\n%s\n--body-file\n.cache/land-body.md' "$(head -n 1 "$dir/pr-text")")" ] || fail "$name: wrong edit arguments"
  grep -q '^Full gate: passed on ' "$dir/land.body" || fail "$name: edit lost gate evidence"
  expect "$name" '#1 STEP merged 42 \(the run\)$'

  name="an open pull request listed at the previous head merges once after a direct read"
  landing_fixture
  landing_case 'gate|blocked a|blocked b' LAND_OPEN=head
  [ "$(cat "$dir/remote.calls")" = "$(printf 'fetch\npush\nlist\nview\nedit\nview\nfetch\nmerge\nview\nfetch')" ] || fail "$name: the direct read did not confirm the head before editing and merging"
  [ "$(cat "$dir/land.merge.args")" = "$(printf 'pr\nmerge\n42\n-R\nlabcast/labcast-server\n--squash\n--match-head-commit\n%s' "$(cat "$dir/gate.commits")")" ] || fail "$name: merge used an unconfirmed head"
  expect "$name" '#1 STEP merged 42 \(the run\)$'

  name="an open pull request that stays stale for two direct reads merges on the third"
  landing_fixture
  landing_case 'gate|blocked a|blocked b' LAND_OPEN=head LAND_STALE_VIEWS=2 PATH="$work/sleep:$PATH"
  [ "$(cat "$dir/remote.calls")" = "$(printf 'fetch\npush\nlist\nview\nview\nview\nedit\nview\nfetch\nmerge\nview\nfetch')" ] || fail "$name: delayed head did not reach exactly one merge"
  [ "$(cat "$dir/sleeps")" = "$(printf '7\n55')" ] || fail "$name: the confirmations did not wait between stale reads"
  expect "$name" '#1 STEP merged 42 \(the run\)$'

  for invalid in missing stale title text separator main detached unreadable; do
    name="a $invalid landing file or branch stops all landing calls and saves its reason"
    landing_fixture
    case "$invalid" in
      missing) action='rm .cache/pr.md'; reason='.cache/pr.md is missing' ;;
      stale) action='sed "1s/.*/Reviewed-head: 1111111111111111111111111111111111111111/" .cache/pr.md > bad; mv bad .cache/pr.md'; reason='.cache/pr.md does not name a reviewed ancestor with only clean merges of main' ;;
      title) action='sed "2s/.*/   /" .cache/pr.md > bad; mv bad .cache/pr.md'; reason='.cache/pr.md has no title' ;;
      text) action='head -n 3 .cache/pr.md > bad; printf "  \n" >> bad; mv bad .cache/pr.md'; reason='.cache/pr.md has no text' ;;
      separator) action='sed "3s/^/text/" .cache/pr.md > bad; mv bad .cache/pr.md'; reason='.cache/pr.md needs an empty third line' ;;
      main) action='git branch -f main HEAD; git switch -q main'; reason='the branch is main' ;;
      detached) action='git checkout -q --detach'; reason='the branch has no readable name' ;;
      unreadable)
        mkdir branch-bin
        printf '#!/bin/sh\nif [ "$1 $2" = "branch --show-current" ]; then echo feat/step-1; exit 9; fi\nexec "$RUN_TEST_SAFE_GIT" "$@"\n' > branch-bin/git
        chmod +x branch-bin/git
        action=':'; reason='the branch has no readable name' ;;
    esac
    echo "$action" > prepare.after
    landing_case 'gate|-|planned|blocked a|blocked b' PATH="$dir/branch-bin:$PATH" RUN_QUICK=999
    no_landing
    [ ! -f "$dir/land.reads" ] || fail "$name: invalid preparation reached a landing read"
    for line in 2 3; do
      landing_prompt "$line" | grep -Fq "The run did not merge: $reason." || fail "$name: refusal was lost before a lead wrote a result"
      landing_prompt "$line" | grep -q 'it passed\.' || fail "$name: gate pass was lost"
    done
    ! landing_prompt 4 | grep -q 'The run did not merge' || fail "$name: acknowledged refusal survived"
  done

  name="main advancing during the gate keeps the request and gates again without a lead"
  landing_fixture
  cat > gate.after <<'ACTION'
if [ ! -f main.advanced ]; then
  touch main.advanced
  echo new > origin/new-main; git -C origin add -f new-main; git -C origin commit -qm main
fi
ACTION
  landing_case 'gate|blocked a|blocked b' RUN_SYNC=1
  [ "$(wc -l < "$dir/gate.status")" -eq 2 ] || fail "$name: stale gate result was reused or request was lost"
  [ "$(grep -c 'main advanced during the gate for #1; the gate runs again' "$dir/.cache/run.history")" -eq 1 ] || fail "$name: regate history is missing"
  [ "$(sed -n '1,/land push/p' "$dir/events" | grep -c '^gate ')" -eq 2 ] || fail "$name: a landing ran before the new main was gated"
  [ "$(sed -n '1,/land push/p' "$dir/events" | grep -c '^session ')" -eq 1 ] || fail "$name: stale gate started a lead"
  git -C "$dir" merge-base --is-ancestor "$(git -C "$dir" rev-parse origin/main^)" "$(tail -n 1 "$dir/gate.commits")" || fail "$name: next gate did not include main"
  expect "$name" '#1 STEP merged 42 \(the run\)$'

  name="six repeated main advances after the first stale gate park the card by the wait rule"
  landing_fixture
  cat > gate.after <<'ACTION'
printf '%s\n' "$(git -C origin rev-parse HEAD)" >> origin/new-main
git -C origin add -f new-main; git -C origin commit -qm main
ACTION
  landing_case 'gate|blocked b' RUN_SYNC=1
  [ "$(wc -l < "$dir/gate.status")" -eq 7 ] || fail "$name: regates did not use the existing six-wait bound"
  [ "$(grep -c 'main advanced during the gate for #1; the gate runs again' "$dir/.cache/run.history")" -eq 7 ] || fail "$name: stale gates were not counted"
  no_landing
  sessions "$name" 2
  expect "$name" 'parked #1: waited six times: main advanced during the gate'

  name="main advancing into a conflict regates no stale result and leaves the integration note"
  landing_fixture
  echo 'echo conflicting > origin/change; git -C origin add -f change; git -C origin commit -qm conflict' > gate.after
  landing_case 'gate|blocked a|blocked b' RUN_SYNC=1
  no_landing
  [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: a conflict was gated"
  landing_prompt 2 | grep -Fq 'main is not in the step' || fail "$name: conflict did not reach the existing integration refusal"

  name="an unreadable main after the gate keeps the pass until a lead writes a result"
  landing_fixture
  landing_case 'gate|-|planned|blocked a|blocked b' LAND_FETCH_FAIL=1 RUN_QUICK=999
  no_landing
  [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: failed fetch regated the request"
  for line in 2 3; do
    landing_prompt "$line" | grep -Fq 'main could not be read after the gate; end with STEP gate' || fail "$name: lost the fetch refusal"
    landing_prompt "$line" | grep -q 'it passed\.' || fail "$name: fetch failure discarded the pass"
  done
  ! landing_prompt 4 | grep -q 'it passed\.' || fail "$name: a result line did not consume the gate note"

  name="a review followed by a clean merge of main before STEP gate can land"
  landing_fixture
  cat > prepare.after <<'ACTION'
echo main > origin/new-main; git -C origin add -f new-main; git -C origin commit -qm main
git fetch -q origin main; git merge -q --no-edit origin/main
ACTION
  landing_case 'gate|blocked a|blocked b'
  reviewed=$(sed -n '1s/Reviewed-head: //p' "$dir/prepared.md")
  [ "$reviewed" != "$(cat "$dir/request.1")" ] || fail "$name: fixture did not separate review from request"
  expect "$name" '#1 STEP merged 42 \(the run\)$'

  for between in sibling future edit empty reverted merge-edit; do
    name="a $between between review and request refuses landing with its reason"
    landing_fixture
    case "$between" in
      future) action='other=$(printf future | git commit-tree "$(git rev-parse HEAD^{tree})" -p "$(git rev-parse HEAD)"); sed "1s/.*/Reviewed-head: $other/" .cache/pr.md > bad; mv bad .cache/pr.md' ;;
      sibling) action='other=$(printf sibling | git commit-tree "$(git rev-parse HEAD^{tree})" -p "$(git rev-parse origin/main)"); sed "1s/.*/Reviewed-head: $other/" .cache/pr.md > bad; mv bad .cache/pr.md' ;;
      edit) action='echo extra >> change; git add -f change; git commit -qm extra' ;;
      empty) action='git commit --allow-empty -qm empty' ;;
      reverted) action='echo extra >> change; git add -f change; git commit -qm extra; git revert --no-edit HEAD > /dev/null' ;;
      merge-edit) action='echo main > origin/new-main; git -C origin add -f new-main; git -C origin commit -qm main; git fetch -q origin main; git merge -q --no-ff --no-commit origin/main; echo extra >> change; git add -f change; git commit -qm merge-edit' ;;
    esac
    echo "$action" > prepare.after
    landing_case 'gate|blocked a|blocked b'
    no_landing
    [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: original gate rule changed"
    landing_prompt 2 | grep -Fq 'does not name a reviewed ancestor with only clean merges of main' || fail "$name: reviewed history refusal was lost"
  done

  for format in prefix trailing short; do
    name="a $format malformed Reviewed-head line refuses landing"
    landing_fixture
    case "$format" in
      prefix) action='sed "1s/Reviewed-head:/Other-head:/" .cache/pr.md > bad; mv bad .cache/pr.md' ;;
      trailing) action='sed "1s/$/ trailing/" .cache/pr.md > bad; mv bad .cache/pr.md' ;;
      short) action='sed "1s/.$//" .cache/pr.md > bad; mv bad .cache/pr.md' ;;
    esac
    echo "$action" > prepare.after
    landing_case 'gate|blocked a|blocked b'
    no_landing
    landing_prompt 2 | grep -Fq 'does not name a reviewed ancestor with only clean merges of main' || fail "$name: invalid review line was accepted"
  done

  for broken in history clean-tree parents; do
    name="a failed $broken read refuses landing even with plausible output"
    landing_fixture
    cat > prepare.after <<'ACTION'
echo main > origin/new-main; git -C origin add -f new-main; git -C origin commit -qm main
git fetch -q origin main; git merge -q --no-edit origin/main
ACTION
    mkdir read-bin
    cat > read-bin/git <<'STUB'
#!/bin/sh
if [ -f read.fail ]; then
  if [ "$BROKEN_READ:$1" = history:rev-list ]; then "$RUN_TEST_SAFE_GIT" "$@"; exit 9; fi
  if [ "$BROKEN_READ:$1" = parents:show ]; then "$RUN_TEST_SAFE_GIT" "$@"; exit 9; fi
  if [ "$BROKEN_READ:$1" = clean-tree:merge-tree ]; then "$RUN_TEST_SAFE_GIT" "$@"; exit 9; fi
fi
exec "$RUN_TEST_SAFE_GIT" "$@"
STUB
    chmod +x read-bin/git
    echo 'touch read.fail' > gate.after
    landing_case 'gate|blocked a|blocked b' PATH="$dir/read-bin:$PATH" BROKEN_READ="$broken"
    no_landing
    landing_prompt 2 | grep -Fq 'does not name a reviewed ancestor with only clean merges of main' || fail "$name: failed Git read was treated as proof"
  done

  for wrong in release two mixed-base two-fork head fork invalid numeric-text zero negative fraction object; do
    name="open pull request answer $wrong stops landing before any edit or merge"
    landing_fixture
    settings=("LAND_OPEN=$wrong")
    expected_calls=$(printf 'fetch\npush\nlist')
    if [ "$wrong" = head ]; then
      settings+=(LAND_VIEW=head PATH="$work/sleep:$PATH")
      expected_calls+=$'\nview\nview\nview'
    fi
    landing_case 'gate|blocked a|blocked b' "${settings[@]}"
    [ "$(cat "$dir/mutations")" = push ] || fail "$name: invalid open PR allowed create, edit or merge"
    [ "$(cat "$dir/remote.calls")" = "$expected_calls" ] || fail "$name: only a head mismatch permits the bounded direct reads"
    case "$wrong" in
      release) reason='does not have base main, this branch, this repository and the gated head' ;;
      two | mixed-base | two-fork) reason='several open pull requests of this branch: 42, 43' ;;
      head | fork) reason='does not have base main, this branch, this repository and the gated head' ;;
      invalid | numeric-text | zero | negative | fraction | object) reason='invalid number or answer' ;;
    esac
    landing_prompt 2 | grep -Fq "$reason" || fail "$name: rejection did not identify the wrong PR answer"
    ! grep -q 'STEP merged' "$dir/.cache/run.history" || fail "$name: invalid PR was counted as merged"
  done

  for wrong in closed-list invalid-head; do
    name="a list entry with $wrong stops without a direct read"
    landing_fixture
    state=OPEN; listed_head=$(git rev-parse origin/main)
    if [ "$wrong" = closed-list ]; then state=CLOSED; else listed_head=invalid; fi
    reply=$(jq -cn --arg state "$state" --arg head "$listed_head" \
      '[{number:42,baseRefName:"main",headRefName:"feat/step-1",headRefOid:$head,isCrossRepository:false,state:$state}]')
    landing_case 'gate|blocked a|blocked b' LAND_OPEN=42 LAND_LIST_RAW="$reply"
    [ "$(cat "$dir/remote.calls")" = "$(printf 'fetch\npush\nlist')" ] || fail "$name: invalid list entry reached another call"
    if [ "$wrong" = closed-list ]; then reason='pull request 42 is not OPEN'
    else reason='does not have base main, this branch, this repository and the gated head'; fi
    landing_prompt 2 | grep -Fq "$reason" || fail "$name: invalid list entry was not explained"
  done

  name="a direct read at the gated head stops before editing if the pull request closed"
  landing_fixture
  landing_case 'gate|blocked a|blocked b' LAND_OPEN=head LAND_VIEW=closed
  [ "$(cat "$dir/remote.calls")" = "$(printf 'fetch\npush\nlist\nview')" ] || fail "$name: closed pull request reached another call"
  landing_prompt 2 | grep -Fq 'pull request 42 is not OPEN' || fail "$name: closed state was not explained"

  for wrong in base fork branch; do
    name="a direct read at the gated head stops before editing with the wrong $wrong"
    landing_fixture
    landing_case 'gate|blocked a|blocked b' LAND_OPEN=head LAND_VIEW="$wrong"
    [ "$(cat "$dir/remote.calls")" = "$(printf 'fetch\npush\nlist\nview')" ] || fail "$name: wrong identity reached another call"
    landing_prompt 2 | grep -Fq 'does not have base main, this branch, this repository and the gated head' || fail "$name: wrong identity was not explained"
  done

  name="a failed direct read of a stale pull request stops without a retry"
  landing_fixture
  landing_case 'gate|blocked a|blocked b' LAND_OPEN=head LAND_FAIL=view
  [ "$(cat "$dir/remote.calls")" = "$(printf 'fetch\npush\nlist\nview')" ] || fail "$name: a failed read reached another call"
  landing_prompt 2 | grep -Fq 'the pull request state read failed (view final error)' || fail "$name: failed read was not explained"

  for wrong in closed head branch fork base number invalid numeric-text; do
    name="the final pull request read with $wrong refuses the merge"
    landing_fixture
    landing_case 'gate|blocked a|blocked b' LAND_VIEW="$wrong"
    [ "$(cat "$dir/mutations")" = "$(printf 'push\ncreate')" ] || fail "$name: final identity or state guard allowed a merge"
    ! grep -q 'STEP merged' "$dir/.cache/run.history" || fail "$name: refused merge was counted"
    if [ "$wrong" = closed ]; then reason='pull request 42 is not OPEN'
    elif [ "$wrong" = number ] || [ "$wrong" = invalid ] || [ "$wrong" = numeric-text ]; then reason='invalid number or answer'
    else reason='does not have base main, this branch, this repository and the gated head'; fi
    landing_prompt 2 | grep -Fq "$reason" || fail "$name: final read failure was not explained"
  done

  for wrong in base head branch fork open commit ref unreachable; do
    name="merge confirmation with $wrong cannot count a step merged on main"
    landing_fixture
    settings=("LAND_CONFIRM=$wrong"); [ "$wrong" != unreachable ] || settings=(LAND_UNREACHABLE=1)
    landing_case 'gate|blocked a|blocked b' ${settings[@]+"${settings[@]}"}
    [ "$(grep -c '^merge$' "$dir/mutations")" -eq 1 ] || fail "$name: merge call repeated"
    ! grep -q 'STEP merged' "$dir/.cache/run.history" || fail "$name: false merge confirmation was counted"
    ! grep -q 'The run merged this step' "$dir/prompts" || fail "$name: lead was told main holds the step"
    case "$wrong" in
      base | head | branch | fork) reason='does not have base main, this branch, this repository and the gated head' ;;
      open) reason='has no confirmed merge commit in origin/main' ;;
      *) reason='has no confirmed merge commit in origin/main' ;;
    esac
    landing_prompt 2 | grep -Fq "$reason" || fail "$name: confirmation failure was not explained"
  done

  # Every lost reply stops the sequence. Only merge has one confirmation read.
  for mode in fail limit; do
    for effect in 0 1; do
      for act in push list create edit merge; do
        name="the $act $mode with remote effect $effect stops calls or confirms a landed merge"
        if [ "$mode" = limit ] && ! command -v timeout > /dev/null; then
          echo "run-test: unavailable: $name: timeout is not on PATH"; continue
        fi
        landing_fixture
        settings=("LAND_FAIL=$act"); detail="$act final error"; outcome=failed
        if [ "$mode" = limit ]; then
          settings=("LAND_HANG=$act"); detail="$act still running"; outcome='reached its limit'
        fi
        open=''; [ "$act" != edit ] || open=42
        landing_case 'gate|-|planned|blocked a|blocked b' ${settings[@]+"${settings[@]}"} LAND_EFFECT="$effect" LAND_OPEN="$open" RUN_LAND_LIMIT=1 RUN_QUICK=999
        case "$act" in
          push) expected=push ;;
          list) expected=$(printf 'push\nlist') ;;
          create) expected=$(printf 'push\nlist\ncreate') ;;
          edit) expected=$(printf 'push\nlist\nedit') ;;
          merge) expected=$(printf 'push\nlist\ncreate\nview\nfetch\nmerge\nview'); [ "$effect" != 1 ] || expected=$(printf '%s\nfetch' "$expected") ;;
        esac
        [ "$(tail -n +2 "$dir/remote.calls")" = "$expected" ] || fail "$name: later call ran after failure or merge lacked its one confirmation"
        # Prove the fault happened before or after its remote effect as named.
        case "$act" in
          push)
            remote=$(git -C "$dir/origin" rev-parse --verify refs/heads/feat/step-1 2> /dev/null || true)
            if [ "$effect" = 1 ]; then [ "$remote" = "$(cat "$dir/gate.commits")" ] || fail "$name: push effect did not reach the branch"
            else [ -z "$remote" ] || fail "$name: refused push changed the branch"; fi ;;
          list) jq -e 'length == 0' "$dir/remote-prs.json" > /dev/null || fail "$name: list changed remote PRs" ;;
          create) [ "$(jq length "$dir/remote-prs.json")" -eq "$effect" ] || fail "$name: create effect does not match the named fault" ;;
          edit)
            if [ "$effect" = 1 ]; then
              jq -e --arg title "$(head -n 1 "$dir/pr-text")" '.[0].title == $title and (.[0].body | contains("Full gate: passed on "))' "$dir/remote-prs.json" > /dev/null || fail "$name: edit effect lost prepared text"
            else jq -e '.[0].title == "old" and .[0].body == "old"' "$dir/remote-prs.json" > /dev/null || fail "$name: refused edit changed text"; fi ;;
          merge)
            state=OPEN; [ "$effect" != 1 ] || state=MERGED
            [ "$(jq -r '.[0].state' "$dir/remote-prs.json")" = "$state" ] || fail "$name: merge effect does not match the named fault" ;;
        esac
        [ ! -e "$dir/.cache/landing" ] && [ ! -e "$dir/.cache/landing-notes" ] || fail "$name: a landing record was made"
        if [ "$act:$effect" = merge:1 ]; then
          [ "$(grep -c '#1 STEP merged 42 (the run)' "$dir/.cache/run.history")" -eq 1 ] || fail "$name: merged step was not counted once"
          landing_prompt 2 | grep -Fq 'The run merged this step as pull request 42' || fail "$name: failed merge hid its confirmed success"
          ! grep -qi 'push and merge it\|not merged\|not known' "$dir/prompts" || fail "$name: merged PR was offered for another landing"
          expect "$name" 'steps merged or planned in this run: 2$'
        else
          label="the pull request $act"; [ "$act" != push ] || label='the push'; [ "$act" != list ] || label='the pull request list'
          for line in 2 3; do
            landing_prompt "$line" | grep -Fq "$label $outcome ($detail)" || fail "$name: stopped call or its last output line was lost"
            landing_prompt "$line" | grep -Fq 'The landing by the run started;' || fail "$name: missing start notice"
            landing_prompt "$line" | grep -Fq 'not known from that call: read the branch and the pull request before any act' || fail "$name: uncertain state was guessed"
            ! landing_prompt "$line" | grep -qi 'push and merge it\|not merged' || fail "$name: uncertain landing told the lead to repeat an act"
            landing_prompt "$line" | grep -q 'it passed\.' || fail "$name: stopped landing lost the gate result"
          done
          ! landing_prompt 4 | grep -q 'The landing by the run started' || fail "$name: result line did not consume the note"
          ! grep -q 'STEP merged' "$dir/.cache/run.history" || fail "$name: unconfirmed landing was counted"
        fi
        [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: saved gate result ran again"
        [ ! -f "$dir/duplicate.create" ] && [ ! -f "$dir/duplicate.merge" ] || fail "$name: duplicate remote act"
      done
    done
  done

  name="a failed merge whose single confirmation read fails stays unknown without another call"
  landing_fixture
  landing_case 'gate|blocked a|blocked b' LAND_FAIL=merge LAND_EFFECT=1 LAND_VIEW_FAIL=merge
  [ "$(tail -n 2 "$dir/remote.calls")" = "$(printf 'merge\nview')" ] || fail "$name: failed confirmation read retried or fetched"
  landing_prompt 2 | grep -Fq 'the pull request merge failed (merge final error)' || fail "$name: confirmation hid the original failed call"
  landing_prompt 2 | grep -Fq 'not known from that call: read the branch and the pull request before any act' || fail "$name: failed read guessed the merge"
  ! landing_prompt 2 | grep -qi 'push and merge it\|not merged' || fail "$name: unknown merge asked the lead to repeat it"
  ! grep -q 'STEP merged' "$dir/.cache/run.history" || fail "$name: unreadable merge was counted"

  name="a merged but unread pull request keeps the read-before-act note across sync and a quick lead failure"
  landing_fixture
  landing_case 'gate|-|planned|blocked a|blocked b' LAND_FAIL=merge LAND_EFFECT=1 LAND_VIEW_FAIL=merge RUN_SYNC=1 RUN_QUICK=999
  for line in 2 3; do
    landing_prompt "$line" | grep -Fq 'the pull request merge failed (merge final error)' || fail "$name: sync lost the stopped call"
    landing_prompt "$line" | grep -Fq 'not known from that call: read the branch and the pull request before any act' || fail "$name: sync guessed the unread merge"
    ! landing_prompt "$line" | grep -qi 'push and merge it\|not merged' || fail "$name: sync told the lead to repeat a completed act"
  done
  ! landing_prompt 4 | grep -Fq 'not known from that call' || fail "$name: result line kept the note"
  [ "$(grep -c '^merge$' "$dir/mutations")" -eq 1 ] || fail "$name: unread merge repeated"
  ! grep -q '#1 STEP merged' "$dir/.cache/run.history" || fail "$name: failed confirmation counted the merge"

  for act in push list create edit merge; do
    name="TERM after $act leaves discovery to the next lead without a landing record"
    landing_fixture
    open=''; [ "$act" != edit ] || open=42
    LAND_WANT=143 landing_case 'gate|blocked a|blocked b' LAND_TERM="$act" LAND_OPEN="$open"
    [ ! -e "$dir/.cache/landing" ] && [ ! -e "$dir/.cache/landing-notes" ] || fail "$name: TERM left a landing record"
    ! grep -q 'STEP merged' "$dir/.cache/run.history" || fail "$name: TERM before confirmation recorded a merge"
    old_calls=$(wc -l < "$dir/remote.calls")
    case_number=$((case_number - 1))
    landing_case 'gate|blocked a|blocked b' LAND_OPEN="$open"
    [ "$(wc -l < "$dir/remote.calls")" -eq "$old_calls" ] || fail "$name: new run replayed landing or recovery calls"
    [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: new run replayed the interrupted gate request"
    landing_prompt 2 | grep -Fq 'Follow the automatic run in .agents/project.md' || fail "$name: next lead did not receive instruction 2's owner"
    [ ! -f "$dir/duplicate.create" ] && [ ! -f "$dir/duplicate.merge" ] || fail "$name: interrupted act repeated"
    if [ "$act" = merge ]; then
      jq -e '.[0].state == "MERGED"' "$dir/remote-prs.json" > /dev/null || fail "$name: fixture did not complete merge before TERM"
      expect "$name" 'steps merged or planned in this run: 0$'
    elif [ "$act" = create ] || [ "$act" = edit ]; then
      jq -e 'length == 1 and .[0].state == "OPEN"' "$dir/remote-prs.json" > /dev/null || fail "$name: next lead could not find the open PR"
    else
      [ "$(git -C "$dir/origin" rev-parse feat/step-1)" = "$(cat "$dir/gate.commits")" ] || fail "$name: next lead could not find the pushed branch"
    fi
  done

  name="a quick lead failure after landing keeps the merge sentence until a result line"
  landing_fixture
  landing_case 'gate|-|planned|blocked a|blocked b' RUN_QUICK=999
  for line in 2 3; do landing_prompt "$line" | grep -Fq 'The run merged this step' || fail "$name: unstarted lead consumed the merge sentence"; done
  ! landing_prompt 4 | grep -q 'The run merged this step' || fail "$name: acknowledged merge sentence survived"
  [ "$(grep -c '^merge$' "$dir/mutations")" -eq 1 ] || fail "$name: quick failure landed twice"

  name="a stopped landing note survives HEAD moving before a lead writes a result"
  landing_fixture
  before 2 'echo extra >> change; git add -f change; git commit -qm later'
  landing_case 'gate|-|planned|blocked a|blocked b' LAND_FAIL=create RUN_QUICK=999
  for line in 2 3; do
    landing_prompt "$line" | grep -Fq 'the pull request create failed (create final error)' || fail "$name: moved HEAD lost the unread call note"
    landing_prompt "$line" | grep -Fq 'read the branch and the pull request before any act' || fail "$name: moved HEAD lost the state check instruction"
  done
  landing_prompt 3 | grep -Fq 'HEAD changed after the request' || fail "$name: fixture did not move outside the gate tree"
  ! landing_prompt 4 | grep -Fq 'the pull request create failed' || fail "$name: result line kept the call note"
  [ "$(tail -n 1 "$dir/remote.calls")" = create ] || fail "$name: moving HEAD retried the landing"

  for read in view confirm; do
    for mode in fail limit; do
      name="the $read $mode leaves the stopped call note and runs no later call"
      if [ "$mode" = limit ] && ! command -v timeout > /dev/null; then
        echo "run-test: unavailable: $name: timeout is not on PATH"; continue
      fi
      landing_fixture
      settings=("LAND_FAIL=view"); label='the pull request state read'; detail='view final error'; outcome=failed
      if [ "$mode" = limit ]; then
        settings=(LAND_HANG=view); detail='view still running'; outcome='reached its limit'
      fi
      if [ "$read" = confirm ]; then
        settings=(LAND_VIEW_FAIL=merge); detail='view failed'
        if [ "$mode" = limit ]; then settings=(LAND_CONFIRM_HANG=1); detail='confirmation still running'; fi
      fi
      landing_case 'gate|-|planned|blocked a|blocked b' ${settings[@]+"${settings[@]}"} RUN_LAND_LIMIT=1 RUN_QUICK=999
      for line in 2 3; do
        landing_prompt "$line" | grep -Fq "$label $outcome ($detail)" || fail "$name: failed read diagnostic was lost"
        landing_prompt "$line" | grep -Fq 'not known from that call: read the branch and the pull request before any act' || fail "$name: unread state was guessed"
        ! landing_prompt "$line" | grep -qi 'push and merge it\|not merged' || fail "$name: note repeated an unknown act"
      done
      [ "$(tail -n 1 "$dir/remote.calls")" = view ] || fail "$name: failed read ran another call"
      ! grep -q 'STEP merged' "$dir/.cache/run.history" || fail "$name: failed confirmation counted a merge"
    done
  done

  for change in head dirty status; do
    name="a $change change during the full gate cannot start landing"
    landing_fixture
    if [ "$change" = head ]; then echo 'git commit --allow-empty -qm during-gate' > gate.after
    elif [ "$change" = dirty ]; then echo 'echo dirty >> change' > gate.after
    else
      mkdir status-bin
      cat > status-bin/git <<'STUB'
#!/bin/sh
if [ "$1" = status ] && [ -f status.fail ]; then exit 9; fi
exec "$RUN_TEST_SAFE_GIT" "$@"
STUB
      chmod +x status-bin/git
      echo 'touch status.fail' > gate.after
    fi
    landing_case 'gate|blocked a|blocked b' PATH="$dir/status-bin:$PATH"
    no_landing
    landing_prompt 2 | grep -Fq 'the head or worktree changed after the gate' || fail "$name: changed gate input reached landing"
  done

  for change in head branch; do
    name="a $change change after PR creation cannot reach the merge"
    landing_fixture
    if [ "$change" = head ]; then echo 'git commit --allow-empty -qm after-create' > local.after
    else echo 'git switch -qc another' > local.after; fi
    landing_case 'gate|blocked a|blocked b' LAND_LOCAL_CHANGE=1
    [ "$(cat "$dir/mutations")" = "$(printf 'push\ncreate')" ] || fail "$name: a later local change reached the merge"
    landing_prompt 2 | grep -Fq 'the head or branch changed after the gate' || fail "$name: later branch or head change was not reported"
  done

  name="a replacement supervisor keeps the branch with a pending gate request"
  landing_fixture
  SUPERVISOR=architecture/run.sh landing_case 'main:gate|blocked a|blocked b' RUN_SYNC=1
  restarted_once
  ( . "$dir/restart.state"; [ "${gate_branches[1]:-}" = feat/step-1 ] ) || fail "$name: pending request lost its branch on restart"
  expect "$name" '#1 STEP merged 42 \(the run\)$'

  name="a failed fetch of merged main leaves the merge result unknown"
  landing_fixture
  landing_case 'gate|blocked a|blocked b' LAND_MERGED_FETCH_FAIL=1
  [ "$(grep -c '^merge$' "$dir/mutations")" -eq 1 ] || fail "$name: merge call did not run exactly once"
  ! grep -q 'STEP merged' "$dir/.cache/run.history" || fail "$name: failed fetch confirmed a merge"
  landing_prompt 2 | grep -Fq 'the merged main fetch failed (fetch failed)' || fail "$name: failed fetch was hidden"
  landing_prompt 2 | grep -Fq 'not known from that call' || fail "$name: failed fetch guessed the merge"

  name="an unreadable branch at STEP gate is not filled from its error output"
  landing_fixture
  mkdir capture-bin
  cat > capture-bin/git <<'STUB'
#!/bin/sh
if [ "$1 $2" = 'branch --show-current' ] && [ -f .cache/step ] && [ ! -f capture.failed ] && grep -q '^STEP gate' .cache/step; then
  touch capture.failed; echo feat/step-1; exit 9
fi
exec "$RUN_TEST_SAFE_GIT" "$@"
STUB
  chmod +x capture-bin/git
  landing_case 'gate|blocked a|blocked b' PATH="$dir/capture-bin:$PATH"
  no_landing
  landing_prompt 2 | grep -Fq 'the branch changed after the request' || fail "$name: unreadable requested branch was guessed"

  name="a push error printed on stdout is retained without parsing it as state"
  landing_fixture
  landing_case 'gate|blocked a|blocked b' LAND_FAIL=push LAND_FAIL_STDOUT=1
  landing_prompt 2 | grep -Fq 'the push failed (push final error)' || fail "$name: stdout diagnostic was lost"
  landing_prompt 2 | grep -Fq 'not known from that call' || fail "$name: error text became branch state"
  [ "$(cat "$dir/mutations")" = push ] || fail "$name: stdout error reached a later mutation"

  for history in empty reverted; do
    name="$history commits after the reviewed head allow the gate but refuse landing"
    landing_fixture; pending_landing
    if [ "$history" = empty ]; then git commit --allow-empty -qm empty
    else
      echo extra >> change; git add -f change; git commit -qm extra
      git revert --no-edit HEAD > /dev/null
    fi
    landing_case 'blocked a|blocked b' RUN_STATE=.cache/pending
    [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: existing gate tree rule changed"
    no_landing
    landing_prompt 1 | grep -Fq 'commits follow the reviewed head' || fail "$name: landing did not enforce reviewed history"
  done

  name="a merge commit with another edit still refuses the full gate"
  landing_fixture; pending_landing
  echo main > origin/new-main; git -C origin add -f new-main; git -C origin commit -qm main
  git fetch -q origin main; git merge -q --no-ff --no-commit origin/main
  echo extra >> change; git add -f change; git commit -qm 'merge with another edit'
  landing_case 'blocked a|blocked b' RUN_STATE=.cache/pending
  [ ! -f "$dir/gate.status" ] || fail "$name: an unreviewed edit was gated"
  no_landing
  landing_prompt 1 | grep -Fq 'HEAD changed after the request' || fail "$name: existing tree refusal was lost"

  name="switching to another branch at the same commit after STEP gate refuses landing"
  landing_fixture
  echo 'git switch -qc another' > gate.after
  landing_case 'gate|blocked a|blocked b'
  no_landing
  landing_prompt 2 | grep -Fq 'the branch changed after the request' || fail "$name: a different branch landed at the same commit"

  name="a bad create stdout stops without using a number from stderr"
  landing_fixture
  landing_case 'gate|blocked a|blocked b' LAND_BAD_CREATE=1 LAND_STDERR=https://github.com/labcast/labcast-server/pull/99
  [ "$(cat "$dir/mutations")" = "$(printf 'push\ncreate')" ] || fail "$name: malformed create output reached a mutation"
  [ "$(tail -n 1 "$dir/remote.calls")" = create ] || fail "$name: invalid create output triggered another call"
  landing_prompt 2 | grep -Fq 'the pull request create returned an invalid number' || fail "$name: malformed create stdout was accepted"
  [ ! -f "$dir/land.view.args" ] || ! grep -q '^99$' "$dir/land.view.args" || fail "$name: stderr supplied a PR selector"

  name="a list diagnostic containing a number never changes the PR selector"
  landing_fixture
  landing_case 'gate|blocked a|blocked b' LAND_OPEN=42 LAND_STDERR=99
  [ "$(sed -n 3p "$dir/land.merge.args")" = 42 ] || fail "$name: stderr changed the selected PR"
  expect "$name" '#1 STEP merged 42 \(the run\)$'

  name="the fake refuses an option that the real merge command does not support"
  landing_fixture
  case_number=$((case_number + 1))
  got=0
  RUN_TEST_NAME="$name" bash "$work/land.sh" pr merge 42 -R labcast/labcast-server --squash --base main > out 2>&1 || got=$?
  [ "$got" = 98 ] || fail "$name: fake accepted --base on pr merge"
  grep -Fq 'unsupported pr merge option: --base' out || fail "$name: fake did not name the unsupported option"
  [ ! -f mutations ] || fail "$name: option validation happened after a remote act"
  grep -Fq "$name: unsupported pr merge option: --base" "$RUN_TEST_GH_SENTINEL" || fail "$name: unknown option did not retain its case name"
  rm "$RUN_TEST_GH_SENTINEL"

  name="the fake refuses list options outside the S4 list by command name"
  landing_fixture
  case_number=$((case_number + 1))
  got=0
  RUN_TEST_NAME="$name" bash "$work/land.sh" pr list -R labcast/labcast-server --head feat/step-1 --limit 100 > out 2>&1 || got=$?
  [ "$got" = 98 ] || fail "$name: fake accepted --limit outside S4"
  grep -Fq 'unsupported pr list option: --limit' out || fail "$name: unsupported list option lost its command name"
  [ ! -f mutations ] || fail "$name: invalid option reached a remote act"
  grep -Fq "$name: unsupported pr list option: --limit" "$RUN_TEST_GH_SENTINEL" || fail "$name: unknown list option did not retain its case name"
  rm "$RUN_TEST_GH_SENTINEL"

  name="a prepared manifest without STEP gate never starts a gate or landing"
  landing_fixture; pending_landing
  landing_case 'blocked a|blocked b'
  sessions "$name" 2
  no_landing
  [ ! -f "$dir/gate.status" ] || fail "$name: a manifest triggered an unrequested gate"

  for ineligible in failure changed dirty main-ahead stored once once-pending; do
    name="a prepared landing does nothing for $ineligible instead of a fresh gate pass"
    landing_fixture
    script='gate|blocked a|blocked b'; settings=()
    case "$ineligible" in
      failure) settings=(GATE_CODE=1) ;;
      changed) before 2 'echo repair >> step; git add step; git commit -qm changed'; script='other:gate|blocked b|blocked a'; settings=(RUN_ITEMS=items.json RUN_OPEN=false) ;;
      dirty) script='dirty:gate|blocked a|blocked b' ;;
      main-ahead) script='conflict:gate|blocked a|blocked b'; settings=(RUN_SYNC=1) ;;
      stored | once-pending)
        pending_landing
        [ "$ineligible" != stored ] || printf 'gate_results[1]="%s The stored gate passed."\n' "$request" >> .cache/pending
        settings=(RUN_STATE=.cache/pending)
        script='blocked a|blocked b' ;;
      once) script='gate' ;;
    esac
    if [[ "$ineligible" = once* ]]; then
      ONCE=--once LAND_WANT=0 landing_case "$script" ${settings[@]+"${settings[@]}"}
    else landing_case "$script" ${settings[@]+"${settings[@]}"}; fi
    no_landing
    if [ "$ineligible" != failure ]; then [ ! -e "$dir/gate.status" ] || fail "$name: an ineligible gate ran"; fi
  done

  name="landing card one releases the next card and resets all merged-step counts"
  landing_fixture
  mkdir -p .cache
  printf 'last=$\x27#1\tT1\tcard 1\x27\nwaits=5\nrepeats=1\ndisk_fails=2\nlast_result=planned\n' > .cache/counts
  cat > land.after <<'ACTION'
jq '(.items[] | select(.content.number == 2) | .sequence)=0' items.json > items.new
mv items.new items.json
ACTION
  SUPERVISOR=architecture/run.sh landing_case 'gate|main:blocked b|planned|blocked a' RUN_STATE=.cache/counts RUN_ITEMS=items.json RUN_SYNC=1
  sessions "$name" 4; order 1 2 1 1
  restarted_once
  ( . "$dir/restart.state"; [ "${saved_counts[1]}" = '0 0 0 ' ] ) || fail "$name: card one did not save reset counts"
  expect "$name" 'steps merged or planned in this run: 2$'
  ! landing_prompt 2 | grep -q 'The run merged this step' || fail "$name: another card received the merge note"
  landing_prompt 3 | grep -q 'The run merged this step' || fail "$name: owning card lost the merge note"

  name="a replacement supervisor preserves the unacknowledged merge sentence"
  landing_fixture
  cat > land.after <<'ACTION'
cp next.sh origin/architecture/run.sh
git -C origin add architecture/run.sh; git -C origin commit -qm supervisor
ACTION
  SUPERVISOR=architecture/run.sh landing_case 'gate|planned|blocked a|blocked b' RUN_SYNC=1
  sessions "$name" 4; restarted_once
  ( . "$dir/restart.state"; [ -n "${merged_notes[1]:-}" ] && [ -z "${gate_requests[1]:-}" ] && [ -z "${gate_results[1]:-}" ] ) || fail "$name: replacement lost the note or kept the gate"
  landing_prompt 2 | grep -q 'The run merged this step' || fail "$name: replacement lost the merge sentence"
  ! landing_prompt 3 | grep -q 'The run merged this step' || fail "$name: acknowledged sentence survived replacement"
  name="a held gate request drops its branch and the returning lead lands the new branch request"
  landing_fixture
  printf 'saved_counts[1]="6 0 0 "\n' > .cache-counts
  mkdir -p .cache; mv .cache-counts .cache/counts
  cp "$work/items.json" initial.json
  jq '(.items[] | select(.content.number == 2) | .status)="Merged"' initial.json > ready.json
  echo 'jq '\''(.items[] | select(.content.number == 1) | .labels)=["needs-joel"]'\'' items.json > items.new; mv items.new items.json' > prepare.after
  before 2 'jq '\''(.items[] | select(.content.number == 2) | .status)="Merged"'\'' items.json > items.new; mv items.new items.json'
  before 3 '[ -f .cache/pr.md ] || exit 97; git switch -qc feat/step-1-new'
  ITEMS="$dir/initial.json" SUPERVISOR=architecture/run.sh landing_case 'gate old|main:merged other|gate new|planned|blocked done' \
    RUN_ITEMS=items.json RUN_STATE=.cache/counts RUN_SYNC=1 LABEL_REPLY=items POLL_READY_AT=1 PATH="$work/sleep:$PATH"
  sessions "$name" 5; order 1 2 1 1 1
  restarted_once
  ( . "$dir/restart.state"; [ -z "${gate_requests[1]+set}" ] && [ -z "${gate_branches[1]+set}" ] ) || fail "$name: held request left its old branch in state"
  [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: held request ran a gate or new request was lost"
  [ "$(tail -n 1 "$dir/land.push.args")" = HEAD:refs/heads/feat/step-1-new ] || fail "$name: landing used the old request branch"
  jq -e '.[0].headRefName == "feat/step-1-new" and .[0].state == "MERGED"' "$dir/remote-prs.json" > /dev/null || fail "$name: new branch did not land"
  ! landing_prompt 3 | grep -q 'full gate' || fail "$name: returning lead read the old gate result"

  name="an unread merge sentence follows a held card until a lead writes a result"
  landing_fixture
  cp "$work/items.json" initial.json
  jq '(.items[] | select(.content.number == 2) | .status)="Merged"' initial.json > ready.json
  before 3 'jq '\''(.items[] | select(.content.number == 1) | .labels)=["needs-joel"]'\'' items.json > items.new; mv items.new items.json'
  before 4 'jq '\''(.items[] | select(.content.number == 2) | .status)="Merged"'\'' items.json > items.new; mv items.new items.json'
  ITEMS="$dir/initial.json" landing_case 'gate|-|-|merged other|planned|blocked done' \
    RUN_ITEMS=items.json LABEL_REPLY=items POLL_READY_AT=1 PATH="$work/sleep:$PATH"
  sessions "$name" 6; order 1 1 1 2 1 1
  for line in 2 3 5; do landing_prompt "$line" | grep -Fq 'The run merged this step as pull request 42' || fail "$name: held card lost an unread merge sentence"; done
  ! landing_prompt 4 | grep -q 'The run merged this step' || fail "$name: another card received the sentence"
  ! landing_prompt 6 | grep -q 'The run merged this step' || fail "$name: written result did not consume the sentence"
  [ "$(grep -c '^merge$' "$dir/mutations")" -eq 1 ] || fail "$name: held card repeated the merge"

  name="an earlier request file refuses a later commit on the same card and branch"
  landing_fixture
  landing_case 'gate|gatecode 0:commit:gate|blocked a|blocked b' GATE_CODE=1
  no_landing
  [ "$(wc -l < "$dir/gate.status")" -eq 2 ] || fail "$name: either request was not gated"
  [ "$(cat "$dir/request.1")" != "$(cat "$dir/request.2")" ] || fail "$name: fixture did not add a commit"
  [ "$(head -n 1 "$dir/.cache/pr.md")" = "Reviewed-head: $(cat "$dir/request.1")" ] || fail "$name: fixture replaced the earlier request file"
  landing_prompt 3 | grep -Fq 'does not name a reviewed ancestor with only clean merges of main' || fail "$name: stale reviewed head was accepted"

  name="an earlier step file refuses a new branch from the squash on the same card"
  landing_fixture
  before 2 'cp prepared.md .cache/pr.md; git switch -qc feat/step-1-next origin/main
    echo next > next-step; git add -f next-step; git commit -qm next'
  landing_case 'gate|gate|blocked a|blocked b'
  [ "$(wc -l < "$dir/gate.status")" -eq 2 ] || fail "$name: next request was not gated"
  [ "$(grep -c '^push$' "$dir/mutations")" -eq 1 ] && [ "$(grep -c '^merge$' "$dir/mutations")" -eq 1 ] || fail "$name: stale step file started a second landing"
  if git -C "$dir" merge-base --is-ancestor "$(cat "$dir/request.1")" "$(cat "$dir/request.2")"; then fail "$name: squash fixture retained the reviewed ancestor"; fi
  landing_prompt 3 | grep -Fq 'does not name a reviewed ancestor with only clean merges of main' || fail "$name: stale squash-step file was accepted"
  [ "$(head -n 1 "$dir/.cache/pr.md")" = "Reviewed-head: $(cat "$dir/request.1")" ] || fail "$name: fixture did not retain the earlier file"

  name="an earlier card file refuses another card on its own branch"
  landing_fixture
  before 2 'git switch -qc feat/step-2 origin/main
    echo other > other-step; git add -f other-step; git commit -qm other'
  landing_case 'other:gate|gatecode 0:gate|blocked b|blocked a' RUN_ITEMS=items.json RUN_OPEN=false
  sessions "$name" 4; order 1 2 2 1
  no_landing
  [ "$(git -C "$dir" branch --show-current)" = feat/step-2 ] || fail "$name: other card did not own its branch"
  [ "$(head -n 1 "$dir/.cache/pr.md")" = "Reviewed-head: $(cat "$dir/request.1")" ] || fail "$name: other card replaced the earlier file"
  landing_prompt 3 | grep -Fq 'does not name a reviewed ancestor with only clean merges of main' || fail "$name: another card's reviewed head was accepted"

  name="the shared gh sentinel names an ignored default call"
  landing_fixture
  case_number=$((case_number + 1))
  RUN_TEST_NAME="$name" gh ignored command > out 2>&1 || true
  [ "$(cat "$RUN_TEST_GH_SENTINEL")" = "$name: ignored command" ] || fail "$name: ignored gh failure lost its case name"
  rm "$RUN_TEST_GH_SENTINEL"

  for invalid_call in command issue-option issue-extra issue-number issue-query pr-command selector value repo head state field push fetch; do
    name="the shared fake names an ignored $invalid_call call"
    landing_fixture
    case_number=$((case_number + 1))
    case "$invalid_call" in
      command) args=(unknown command) ;;
      issue-option) args=(issue view 1 -R labcast/labcast-server --json labels --bad 'any(.labels[]; .name == "needs-joel")') ;;
      issue-extra) args=(issue view 1 -R labcast/labcast-server --json labels -q 'any(.labels[]; .name == "needs-joel")' --bad) ;;
      issue-number) args=(issue view invalid -R labcast/labcast-server --json labels -q 'any(.labels[]; .name == "needs-joel")') ;;
      issue-query) args=(issue view 1 -R labcast/labcast-server --json labels -q unknown) ;;
      pr-command) args=(pr unknown) ;;
      selector) args=(pr view invalid -R labcast/labcast-server --json number) ;;
      value) args=(pr list -R) ;;
      repo) args=(pr list -R other/repo) ;;
      head) args=(pr list -R labcast/labcast-server --head owner:branch) ;;
      state) args=(pr list -R labcast/labcast-server --state invalid) ;;
      field) args=(pr list -R labcast/labcast-server --json unknown) ;;
      push) args=(push -q origin invalid) ;;
      fetch) args=(fetch -q other main) ;;
    esac
    got=0
    RUN_TEST_NAME="$name" bash "$work/land.sh" "${args[@]}" > out 2>&1 || got=$?
    [ "$got" = 98 ] || fail "$name: unknown fake call was accepted"
    grep -Fq "$name:" "$RUN_TEST_GH_SENTINEL" || fail "$name: fake failure lost its case name"
    [ ! -f mutations ] || fail "$name: unknown call reached an act"
    rm "$RUN_TEST_GH_SENTINEL"
  done
  name="the gh sentinel fails a nested case whose caller ignores the exit code"
  landing_fixture
  if ( run_case "$name" 1 'blocked a|blocked b' RUN_GH=gh; echo survived ) > "$dir/meta.log" 2>&1; then
    fail "$name: an ignored gh error did not fail the caller's case"
  fi
  case_number=$((case_number + 1))
  grep -Fq "run-test: FAILED: $name: default gh called:" "$dir/meta.log" || fail "$name: nested failure did not name its caller"
  rm "$RUN_TEST_GH_SENTINEL"

  for during in no yes; do
    name="a request with older main lands after the run merges newer main with gate advance $during"
    landing_fixture
    cat > prepare.after <<'ACTION'
echo first > origin/main-one; git -C origin add -f main-one; git -C origin commit -qm main-one
git fetch -q origin main; git merge -q --no-edit origin/main
echo second > origin/main-two; git -C origin add -f main-two; git -C origin commit -qm main-two
ACTION
    if [ "$during" = yes ]; then
      cat > gate.after <<'ACTION'
if [ ! -f main.advanced ]; then
  touch main.advanced
  echo third > origin/main-three; git -C origin add -f main-three; git -C origin commit -qm main-three
fi
ACTION
    fi
    landing_case 'gate|planned|blocked a|blocked b' RUN_SYNC=1
    sessions "$name" 4
    count=1; [ "$during" != yes ] || count=2
    [ "$(wc -l < "$dir/gate.status")" -eq "$count" ] || fail "$name: older-main request was refused or a stale gate was reused"
    [ "$(cat "$dir/request.1")" != "$(tail -n 1 "$dir/gate.commits")" ] || fail "$name: run did not merge the newer main"
    [ "$(grep -c '^merge$' "$dir/mutations")" -eq 1 ] || fail "$name: valid reviewed history did not land once"
    [ "$(grep -c '^create$' "$dir/mutations")" -eq 1 ] || fail "$name: valid history created another PR"
    if [ "$during" = yes ]; then
      [ "$(grep -c 'main advanced during the gate for #1; the gate runs again' "$dir/.cache/run.history")" -eq 1 ] || fail "$name: M3 did not take the regate path"
      [ "$(sed -n '1,/land push/p' "$dir/events" | grep -c '^session ')" -eq 1 ] || fail "$name: main advance started another lead"
    fi
    expect "$name" '#1 STEP merged 42 \(the run\)$'
  done

  for distance in review request; do
    name="an edit hidden and reverted in two merge commits refuses the $distance distance"
    landing_fixture
    cat > prepare-history <<'ACTION'
review=$(git rev-parse HEAD)
printf '%s\n' "$review" > original-review
echo one > origin/main-one; git -C origin add -f main-one; git -C origin commit -qm main-one
git fetch -q origin main; git merge -q --no-ff --no-commit origin/main
echo unreviewed >> change; git add -f change; git commit -qm merge-with-edit
echo two > origin/main-two; git -C origin add -f main-two; git -C origin commit -qm main-two
git fetch -q origin main; git merge -q --no-ff --no-commit origin/main
git show "$review:change" > change; git add -f change; git commit -qm merge-removing-edit
ACTION
    if [ "$distance" = review ]; then mv prepare-history prepare.after; script='gate|blocked a|blocked b'; settings=(); line=2
    else pending_landing; . ./prepare-history; script='blocked a|blocked b'; settings=(RUN_STATE=.cache/pending); line=1; fi
    landing_case "$script" ${settings[@]+"${settings[@]}"}
    no_landing
    [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: full_gate endpoint behaviour changed"
    [ "$(git -C "$dir" rev-parse HEAD^{tree})" = "$(git -C "$dir" merge-tree --write-tree origin/main "$(cat "$dir/original-review")")" ] || fail "$name: fixture did not restore the clean final tree"
    if [ "$distance" = review ]; then reason='does not name a reviewed ancestor with only clean merges of main'
    else reason='commits follow the reviewed head'; fi
    landing_prompt "$line" | grep -Fq "$reason" || fail "$name: an intervening edited merge was accepted"
  done

  for forbidden in commit merge-edit topic second-parent sibling octopus; do
    name="older main in the request still refuses $forbidden history before the request"
    landing_fixture
    cat > prepare.after <<'ACTION'
review=$(git rev-parse HEAD); base=$(git rev-parse origin/main)
printf '%s\n' "$review" > original-review
if [ "$F1_KIND" = second-parent ]; then
  git -C origin fetch -q .. "$review"
  git -C origin merge -q --no-ff --no-edit "$review"
  git reset -q --hard "$base"
  git fetch -q origin main; git merge -q --no-ff --no-edit origin/main
else
  echo one > origin/main-one; git -C origin add -f main-one; git -C origin commit -qm main-one
  git fetch -q origin main
  if [ "$F1_KIND" = octopus ]; then
    main=$(git rev-parse origin/main)
    extra=$(printf extra | git commit-tree "$(git rev-parse "$main^{tree}")" -p "$main")
    tree=$(git merge-tree --write-tree "$review" "$main")
    head=$(printf octopus | git commit-tree "$tree" -p "$review" -p "$main" -p "$extra")
    git reset -q --hard "$head"
  else
    git merge -q --no-ff --no-commit origin/main
    if [ "$F1_KIND" = merge-edit ]; then echo unreviewed >> change; git add -f change; fi
    git commit -qm integrate-main
  fi
  case "$F1_KIND" in
    commit) echo unreviewed >> change; git add -f change; git commit -qm lead-edit ;;
    topic)
      topic=$(printf topic | git commit-tree "$(git rev-parse HEAD^{tree})" -p HEAD)
      git merge -q --no-ff --no-edit "$topic" ;;
    sibling)
      sibling=$(printf sibling | git commit-tree "$(git rev-parse "$review^{tree}")" -p "$base")
      sed "1s/.*/Reviewed-head: $sibling/" .cache/pr.md > old-review; mv old-review .cache/pr.md ;;
  esac
fi
echo two > origin/main-two; git -C origin add -f main-two; git -C origin commit -qm main-two
ACTION
    landing_case 'gate|blocked a|blocked b' RUN_SYNC=1 F1_KIND="$forbidden"
    no_landing
    [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: existing gate no longer ran on the request plus main"
    landing_prompt 2 | grep -Fq 'does not name a reviewed ancestor with only clean merges of main' || fail "$name: forbidden reviewed history was accepted"
    if [ "$forbidden" = second-parent ]; then
      git -C "$dir" merge-base --is-ancestor "$(cat "$dir/original-review")" "$(cat "$dir/request.1")" || fail "$name: review was not reachable through a parent"
      ! git -C "$dir" rev-list --first-parent "$(cat "$dir/request.1")" | grep -Fxq "$(cat "$dir/original-review")" || fail "$name: review unexpectedly lies on the first-parent chain"
    fi
  done

  for during in push list create edit view stale-view; do
    name="main advancing during $during stops before merge and regates the existing pull request"
    landing_fixture
    open=''
    case "$during" in
      edit) open=42 ;;
      stale-view) open=head ;;
    esac
    landing_case 'gate|planned|blocked a|blocked b' RUN_SYNC=1 LAND_ADVANCE="${during#stale-}" LAND_OPEN="$open"
    [ -f "$dir/merge.inputs" ] || fail "$name: retry never reached a confirmed merge"
    while read -r base head; do
      git -C "$dir" merge-base --is-ancestor "$base" "$head" || fail "$name: merge call ran on a moved main"
    done < "$dir/merge.inputs"
    [ "$(wc -l < "$dir/gate.status")" -eq 2 ] || fail "$name: main advance did not invalidate the first gate result"
    [ "$(grep -c 'main advanced during the gate for #1; the gate runs again' "$dir/.cache/run.history")" -eq 1 ] || fail "$name: late main advance lost its history row"
    [ "$(sed -n '1,/land merge/p' "$dir/events" | grep -c '^session ')" -eq 1 ] || fail "$name: late main advance started a lead"
    [ "$(grep -c '^push$' "$dir/mutations")" -eq 2 ] || fail "$name: retry did not push the new gated head once"
    [ "$(grep -c '^merge$' "$dir/mutations")" -eq 1 ] || fail "$name: merge ran before regating or ran twice"
    [ "$(jq length "$dir/remote-prs.json")" -eq 1 ] || fail "$name: retry opened another pull request"
    [ "$(grep -c '^edit$' "$dir/mutations")" -ge 1 ] || fail "$name: retry did not set the existing PR text again"
    [ ! -f "$dir/duplicate.create" ] || fail "$name: retry attempted another create"
    grep -Fq "Full gate: passed on $(tail -n 1 "$dir/gate.commits")" "$dir/land.body" || fail "$name: retry left the first gate's evidence"
    expect "$name" '#1 STEP merged 42 \(the run\)$'
  done

  name="a failed last main fetch stops the merge and keeps the gate result for the lead"
  landing_fixture
  landing_case 'gate|-|planned|blocked a|blocked b' LAND_LAST_FETCH_FAIL=1 RUN_QUICK=999
  [ "$(cat "$dir/remote.calls")" = "$(printf 'fetch\npush\nlist\ncreate\nview\nfetch')" ] || fail "$name: failed final fetch ran a merge or another call"
  for line in 2 3; do
    landing_prompt "$line" | grep -Fq 'The run did not merge: main could not be read after the gate; end with STEP gate' || fail "$name: final fetch did not use the first fetch's note"
    landing_prompt "$line" | grep -Fq 'it passed.' || fail "$name: final fetch failure discarded the gate result"
  done
  ! landing_prompt 4 | grep -q 'main could not be read' || fail "$name: acknowledged final-fetch note survived"
  [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: failed final fetch regated without a new request"

  name="the fake squash includes main that moves inside the remaining merge call window"
  landing_fixture
  landing_case 'gate|blocked a|blocked b' LAND_MERGE_ADVANCE=1
  [ "$(git -C "$dir/origin" show main:main-during-merge)" = 'main moved inside merge' ] || fail "$name: squash discarded the moved main"
  [ "$(git -C "$dir/origin" show main:change)" = 'the reviewed step' ] || fail "$name: squash discarded the step"
  [ "$(git -C "$dir/origin" rev-parse main^{tree})" != "$(git -C "$dir" rev-parse "$(cat "$dir/gate.commits")^{tree}")" ] || fail "$name: fake copied the gated head tree"
  expect "$name" '#1 STEP merged 42 \(the run\)$'

  name="an old unwritable pull request body stops every landing call and keeps its reason"
  landing_fixture
  cat > prepare.after <<'ACTION'
printf 'old body\n' > .cache/land-body.md
chmod a-w .cache/land-body.md
[ ! -w .cache/land-body.md ] || exit 97
ACTION
  landing_case 'gate|-|planned|blocked a|blocked b' RUN_QUICK=999
  no_landing
  [ ! -f "$dir/land.reads" ] || fail "$name: body write failure made a main fetch"
  [ "$(cat "$dir/.cache/land-body.md")" = 'old body' ] || fail "$name: failed redirection changed the old text"
  for line in 2 3; do landing_prompt "$line" | grep -Fq 'the text of the pull request could not be written' || fail "$name: failed body write lost its note"; done
  ! landing_prompt 4 | grep -q 'could not be written' || fail "$name: acknowledged body note survived"

  name="a failed body read cannot publish gate evidence as a truncated pull request text"
  landing_fixture
  mkdir sed-bin
  real_sed=$(command -v sed)
  cat > sed-bin/sed <<'STUB'
#!/bin/sh
if [ "$1 $2 $3" = '-n 4,$p .cache/pr.md' ]; then
  count=$(($(cat body.reads 2> /dev/null || echo 0) + 1)); echo "$count" > body.reads
  [ "$count" != 2 ] || exit 9
fi
exec "$REAL_BODY_SED" "$@"
STUB
  chmod +x sed-bin/sed
  landing_case 'gate|blocked a|blocked b' REAL_BODY_SED="$real_sed" PATH="$dir/sed-bin:$PATH"
  no_landing
  [ ! -f "$dir/land.reads" ] || fail "$name: truncated body reached a landing read"
  landing_prompt 2 | grep -Fq 'the text of the pull request could not be written' || fail "$name: failed body input was ignored"

  name="a push that ignores TERM is killed after its limit and no later call runs"
  if command -v timeout > /dev/null; then
    landing_fixture
    landing_case 'gate|blocked a|blocked b' LAND_IGNORE_TERM=1 RUN_LAND_LIMIT=3 RUN_LAND_KILL=1 PATH="$work/timeout:$PATH"
    [ ! -f "$dir/ignored-term-finished" ] || fail "$name: TERM-resistant push survived the kill interval"
    [ "$(cat "$dir/remote.calls")" = "$(printf 'fetch\npush')" ] || fail "$name: killed push reached a later call"
    landing_prompt 2 | grep -Fq 'the push reached its limit' || fail "$name: KILL was reported as an ordinary failure"
    grep -Fq 'push ignores TERM' "$dir/.cache/land.log" || fail "$name: push did not reach its TERM trap"
    grep -Fq -- "--signal=TERM --kill-after=1 3 bash $work/land.sh push " "$dir/timeout.calls" || fail "$name: landing did not use the configured kill interval"
    landing_spans push
    awk -v span="$kill_span" 'BEGIN { exit !(span < 7) }' || fail "$name: push kill span reached limit + kill + 3 seconds ($kill_span)"
    awk -v span="$continue_span" 'BEGIN { exit !(span < 3) }' || fail "$name: push continuation took 3 seconds or more ($continue_span)"
    printf 'run-test: TERM-resistant push ended in %s seconds (limit 3 + kill 1); continued in %s seconds\n' "$kill_span" "$continue_span"
  else
    echo "run-test: unavailable: $name: timeout is not on PATH"
  fi
  name="a failed remote merge-tree read with a valid tree cannot invent a merged pull request"
  landing_fixture
  mkdir merge-bin
  cat > merge-bin/git <<'STUB'
#!/bin/sh
if [ "$1 $2 $3" = '-C origin merge-tree' ]; then "$RUN_TEST_SAFE_GIT" "$@"; exit 9; fi
exec "$RUN_TEST_SAFE_GIT" "$@"
STUB
  chmod +x merge-bin/git
  landing_case 'gate|blocked a|blocked b' PATH="$dir/merge-bin:$PATH"
  jq -e '.[0].state == "OPEN" and .[0].mergeCommit == null' "$dir/remote-prs.json" > /dev/null || fail "$name: failed tree read changed remote state to MERGED"
  landing_prompt 2 | grep -Fq 'the pull request merge failed' || fail "$name: failed tree read was reported as a successful merge call"
  [ "$(tail -n 2 "$dir/remote.calls")" = "$(printf 'merge\nview')" ] || fail "$name: failed merge was retried instead of read once"
  ! grep -q 'STEP merged' "$dir/.cache/run.history" || fail "$name: failed tree read counted the step"

  name="a killed main fetch keeps the gate instruction and reports its limit"
  if command -v timeout > /dev/null; then
    landing_fixture
    landing_case 'gate|blocked a|blocked b' LAND_IGNORE_TERM=1 LAND_IGNORE_ACTION=fetch RUN_LAND_LIMIT=3 RUN_LAND_KILL=1 PATH="$work/timeout:$PATH"
    no_landing
    [ "$(cat "$dir/remote.calls")" = fetch ] || fail "$name: killed main fetch reached another call"
    landing_prompt 2 | grep -Fq 'main could not be read after the gate; end with STEP gate' || fail "$name: killed fetch lost the gate instruction"
    landing_prompt 2 | grep -Fq 'the main fetch reached its limit' || fail "$name: freshness helper hid the killed call status"
    [ ! -f "$dir/ignored-term-finished" ] || fail "$name: killed fetch survived its kill interval"
    grep -Fq 'fetch ignores TERM' "$dir/.cache/land.log" || fail "$name: fetch did not reach its TERM trap"
    grep -Fq -- "--signal=TERM --kill-after=1 3 bash $work/land.sh fetch " "$dir/timeout.calls" || fail "$name: landing did not use the configured kill interval"
    landing_spans fetch
    awk -v span="$kill_span" 'BEGIN { exit !(span < 7) }' || fail "$name: fetch kill span reached limit + kill + 3 seconds ($kill_span)"
    awk -v span="$continue_span" 'BEGIN { exit !(span < 3) }' || fail "$name: fetch continuation took 3 seconds or more ($continue_span)"
    printf 'run-test: TERM-resistant fetch ended in %s seconds (limit 3 + kill 1); continued in %s seconds\n' "$kill_span" "$continue_span"
  else
    echo "run-test: unavailable: $name: timeout is not on PATH"
  fi

  # End landing cases.

  # Gate cases use real commits and the same local origin as restart cases.
  for change in branch commit twin; do
    name="another card's $change refuses the original request"
    supervisor_repo
    SUPERVISOR=architecture/run.sh run_case "$name" 1 "empty:other:gate|$change:blocked b|blocked a" RUN_ITEMS=items.json
    sessions "$name" 3
    request=$(cat "$dir/request.1")
    [ ! -e "$dir/gate.status" ] || fail "$name: another card's HEAD was gated"
    sed -n 3p "$dir/prompts" | grep -Fq " The run did not run the full gate: HEAD changed after the request ($request) by more than a merge of main. Make sure that the step is checked out and committed, then end with STEP gate." || fail "$name: no request refusal"
  done

  name="the sync merge of main holds the request and gates the merge commit"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'commit:ahead:gate|blocked a|blocked b' RUN_SYNC=1
  sessions "$name" 3
  request=$(cat "$dir/request.1")
  commit=$(git -C "$dir" rev-parse HEAD)
  [ "$request" != "$commit" ] || fail "$name: HEAD did not move"
  [ "$(git -C "$dir" rev-list --parents -n 1 HEAD | wc -w)" -eq 3 ] || fail "$name: HEAD is not a merge"
  [ "$(cat "$dir/gate.commits")" = "$commit" ] || fail "$name: the merge commit was not gated"
  sed -n 2p "$dir/prompts" | grep -Fq "The run ran the full gate on commit $commit: it passed." || fail "$name: no merge note"

  name="an unread result is refused after a lead moves away from its request"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|commit:-|blocked a|blocked b' RUN_QUICK=999
  sessions "$name" 4
  request=$(cat "$dir/request.1")
  sed -n 3p "$dir/prompts" | grep -Fq '.cache/pr.md is missing' || fail "$name: moving HEAD lost the unacknowledged landing note"
  sed -n 3p "$dir/prompts" | grep -Fq "HEAD changed after the request ($request) by more than a merge of main. Make sure that the step is checked out and committed, then end with STEP gate." || fail "$name: a stale result escaped the request check"
  [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: the unrelated HEAD was gated"

  name="an unread pass is given again when HEAD has not moved"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|-|planned|blocked a|blocked b' RUN_QUICK=999
  sessions "$name" 5
  commit=$(cat "$dir/request.1")
  for line in 2 3; do
    refusal=" The run did not merge: .cache/pr.md is missing."
    [ "$(sed -n "${line}p" "$dir/prompts")" = "Lead the next step of card #1 in the studio lane. Follow the automatic run in .agents/project.md. The run ran the full gate on commit $commit: it passed.$refusal" ] || fail "$name: the exact pass note was lost"
  done
  grep -q 'Main came in after that gate' "$dir/prompts" && fail "$name: unchanged HEAD got a main-update note"
  [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: the gate repeated"

  name="failed gate, repairs, two main updates and reviewer waits do not park the card"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 \
    'gatecode 1:gate|gatecode 0:commit:gate|commit:gate|ahead:gate|ahead:gate|waiting 0 reviewer|gate|blocked a|blocked b' RUN_SYNC=1
  sessions "$name" 9
  expect "$name" 'parked #1: blocked a$'
  [ "$(wc -l < "$dir/gate.status")" -eq 6 ] || fail "$name: the productive gates did not all run"
  [ "$(sort -u "$dir/gate.commits" | wc -l)" -eq 5 ] || fail "$name: the repairs and main updates did not reach the gate"
  sed -n 2p "$dir/prompts" | grep -q 'it failed\.' || fail "$name: the first gate did not fail"
  for line in 3 4 5 6 8; do
    sed -n "${line}p" "$dir/prompts" | grep -q 'it passed\.' || fail "$name: session $line lost its pass"
  done

  for code in 1 124; do
    name="seven unsuccessful gates on new commits park the seventh request (exit $code)"
    supervisor_repo
    SUPERVISOR=architecture/run.sh run_case "$name" 1 \
      'commit:gate|commit:gate|commit:gate|commit:gate|commit:gate|commit:gate|commit:gate final|blocked b' GATE_CODE="$code"
    sessions "$name" 8
    expect "$name" 'parked #1: waited six times: gate final$'
    [ "$(wc -l < "$dir/gate.status")" -eq 6 ] || fail "$name: not exactly six gates"
    [ "$(sort -u "$dir/gate.commits" | wc -l)" -eq 6 ] || fail "$name: the gates did not use distinct commits"
    if [ "$code" = 1 ]; then outcome='it failed\.'; else outcome='it did not end in 360 minutes\.'; fi
    for line in 2 3 4 5 6 7; do
      sed -n "${line}p" "$dir/prompts" | grep -q "$outcome" || fail "$name: session $line lost its unsuccessful result"
    done
  done

  name="seven refused requests still park the card"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'dirty:gate|gate|gate|gate|gate|gate|gate final|blocked b'
  sessions "$name" 8
  [ ! -e "$dir/gate.status" ] || fail "$name: the dirty step was gated"
  expect "$name" 'parked #1: waited six times: gate final$'

  name="a pass on a new commit resets earlier waiting lines"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 \
    'waiting 0 a|waiting 0 b|waiting 0 c|waiting 0 d|waiting 0 e|commit:gate|waiting 0 f|waiting 0 g|blocked a|blocked b'
  sessions "$name" 10
  expect "$name" 'parked #1: blocked a$'

  name="a reused result does not reset waits"
  supervisor_repo
  mkdir -p .cache
  commit=$(git rev-parse HEAD)
  # Restore a pending result with six waits. An unread result keeps that count.
  declare -a gate_requests gate_results last_passed_requests
  gate_requests=() gate_results=() last_passed_requests=()
  gate_requests[1]=$commit gate_results[1]="$commit The saved gate note." last_passed_requests[1]=$commit
  declare -p gate_requests gate_results last_passed_requests > .cache/run.state
  printf 'last=$\x27#1\tT1\tcard 1\x27\nwaits=6\n' >> .cache/run.state
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate final|blocked b' RUN_STATE=.cache/run.state
  sessions "$name" 2
  [ ! -e "$dir/gate.status" ] || fail "$name: the saved result was gated again"
  sed -n 1p "$dir/prompts" | grep -Fq ' The saved gate note.' || fail "$name: the saved result was not given"
  expect "$name" 'parked #1: waited six times: gate final$'

  name="STEP gate runs once before the next session and reports the passed commit"
  supervisor_repo
  commit=$(git rev-parse HEAD)
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|planned|blocked a|blocked b' \
    RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 GATE_SECONDS=125 PATH="$work/sleep:$PATH"
  sessions "$name" 4
  [ "$(cat "$dir/events")" = "$(printf 'session 1\ngate 1\nsession 2\nsession 3\nsession 4')" ] || fail "$name: the gate ran out of order or more than once"
  sed -n 2p "$dir/prompts" | grep -Fq " The run ran the full gate on commit $commit: it passed. The run did not merge: .cache/pr.md is missing." || fail "$name: no pass note with the full commit"
  tail -n 2 "$dir/prompts" | grep -q 'The run ran the full gate' && fail "$name: the note repeated"
  grep -q 'full gate for #1 passed in 3 minutes$' "$dir/.cache/run.history" || fail "$name: no rounded pass history"
  expect "$name" 'full gate for #1 passed in 3 minutes$'
  [ "$(cat "$dir/gate.status")" = 'full gate for #1' ] || fail "$name: the status did not name the running gate"
  [ ! -e "$dir/sleeps" ] || fail "$name: STEP gate slept"

  name="a quick lead without a result keeps the gate note and does not repeat the gate"
  supervisor_repo
  commit=$(git rev-parse HEAD)
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|-|planned|blocked a|blocked b' RUN_QUICK=999 GATE_CODE=1
  sessions "$name" 5
  for line in 2 3; do
    sed -n "${line}p" "$dir/prompts" | grep -Fq "The run ran the full gate on commit $commit: it failed." || fail "$name: session $line lost the note"
  done
  [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: the gate repeated"

  name="a longer lead without a result keeps the gate note and does not repeat the gate"
  supervisor_repo
  commit=$(git rev-parse HEAD)
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|sleep 2|planned|blocked a|blocked b' RUN_QUICK=1 GATE_CODE=1
  sessions "$name" 5
  grep -q 'the lead ended within' "$dir/out" && fail "$name: the session ended within the quick time"
  for line in 2 3; do
    sed -n "${line}p" "$dir/prompts" | grep -Fq "The run ran the full gate on commit $commit: it failed." || fail "$name: session $line lost the note"
  done
  [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: the gate repeated"

  name="a full disk without a result keeps the gate note"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|full:-|planned|blocked a|blocked b' \
    STEP_FREE=15 RUN_CLEAN="bash $work/always.sh"
  sessions "$name" 5
  sed -n 3p "$dir/prompts" | grep -q 'The run ran the full gate.*it passed\.' || fail "$name: the full disk lost the note"
  [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: the gate repeated"

  name="a written failure consumes the gate note before the next session on the same card"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|failed tests|blocked a|blocked b' GATE_CODE=1
  sessions "$name" 4
  sed -n 2p "$dir/prompts" | grep -q 'The run ran the full gate.*it failed\.' || fail "$name: no failure note"
  sed -n 3p "$dir/prompts" | grep -q 'card #1 in' || fail "$name: the next session is not on the same card"
  sed -n 3p "$dir/prompts" | grep -q 'The run ran the full gate' && fail "$name: the consumed note repeated"
  [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: the gate repeated"

  for code in 0 1; do
    name="main advancing three times keeps one unread gate after restart (exit $code)"
    supervisor_repo
    SUPERVISOR=architecture/run.sh run_case "$name" 1 \
      'commit:ahead:gate|main:-|ahead:-|ahead:-|blocked a|blocked b' RUN_SYNC=1 RUN_QUICK=999 GATE_CODE="$code"
    sessions "$name" 6
    restarted_once
    request=$(cat "$dir/request.1")
    commit=$(cat "$dir/gate.commits")
    [ "$request" != "$commit" ] || fail "$name: the initial sync did not merge main"
    [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: the gate repeated"
    if [ "$code" = 0 ]; then
      note=" The run ran the full gate on commit $commit: it passed."
    else
      note=" The run ran the full gate on commit $commit: it failed. Read the end of .cache/gate.log first."
    fi
    prefix='Lead the next step of card #1 in the studio lane. Follow the automatic run in .agents/project.md.'
    refusal=""; [ "$code" != 0 ] || refusal=" The run did not merge: .cache/pr.md is missing."
    [ "$(sed -n 2p "$dir/prompts")" = "$prefix$note$refusal" ] || fail "$name: no exact original note"
    if [ "$code" = 0 ]; then
      note=" The run ran the full gate on commit $commit: it passed. The run did not merge: .cache/pr.md is missing."
    fi
    for line in 3 4 5; do
      head=$(cat "$dir/head.$line")
      [ "$head" != "$commit" ] || fail "$name: main did not move HEAD"
      [ "$(sed -n "${line}p" "$dir/prompts")" = "$prefix$note Main came in after that gate: HEAD is now $head, and the gate did not run for it. End with STEP gate." ] || fail "$name: session $line lost the stored note or new HEAD"
    done
    [ "$(cat "$dir/head.3" "$dir/head.4" "$dir/head.5" | sort -u | wc -l)" -eq 3 ] || fail "$name: main did not advance three times"
    ( . "$dir/restart.state"; [ "${gate_requests[1]:-}" = "$request" ] && [[ "${gate_results[1]:-}" == "$commit The run ran"* ]] ) || fail "$name: the saved state lost the request or result"
    if [ "$code" = 0 ]; then
      ( . "$dir/restart.state"; [ "${last_passed_requests[1]:-}" = "$request" ] ) || fail "$name: the saved state lost the last passed request"
    fi
  done

  name="restart keeps the last pass across a failed gate on a new commit"
  supervisor_repo
  commit=$(git rev-parse HEAD)
  SUPERVISOR=architecture/run.sh run_case "$name" 1 \
    'gate|gatecode 1:commit:gate|main:-|blocked a|blocked b' RUN_SYNC=1 RUN_QUICK=999
  sessions "$name" 5
  restarted_once
  failed_commit=$(cat "$dir/request.2")
  [ "$commit" != "$failed_commit" ] || fail "$name: the failed gate did not use a new commit"
  ( . "$dir/restart.state"; [ "${last_passed_requests[1]:-}" = "$commit" ] && [[ "${gate_results[1]:-}" == "$failed_commit "*'it failed.'* ]] ) || fail "$name: the failed gate replaced or lost the saved last pass"

  name="a failing gate reports failure and keeps the log for the lead"
  supervisor_repo
  commit=$(git rev-parse HEAD)
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate tests|blocked a|blocked b' GATE_CODE=1 RUN_NOW="bash $work/now.sh"
  sessions "$name" 3
  sed -n 2p "$dir/prompts" | grep -Fq " The run ran the full gate on commit $commit: it failed. Read the end of .cache/gate.log first." || fail "$name: no failure note"
  grep -q 'full gate for #1 failed in 1 minutes$' "$dir/.cache/run.history" || fail "$name: no failure history with at least one minute"
  expect "$name" 'full gate for #1 failed in 1 minutes$'
  grep -q '^stdout gate 1$' "$dir/.cache/gate.log" || fail "$name: stdout was lost"
  grep -q '^stderr gate 1$' "$dir/.cache/gate.log" || fail "$name: stderr was lost"

  mkdir "$work/gate-bin"
  cat > "$work/gate-bin/uname" <<'STUB'
#!/bin/sh
case "$GATE_FIXTURE" in held-flock) echo Linux ;; *) echo Darwin ;; esac
STUB
  cat > "$work/gate-bin/sleep" <<'STUB'
#!/bin/sh
cat .cache/run.status > waiting.status
clock=$(cat clock)
case "$GATE_FIXTURE" in
  short-directory) echo 1030 > clock; rm -r "$GATE_LOCK" ;;
  held-directory)
    echo "$((clock + 60))" > clock
    if [ "$clock" -ge 1060 ]; then rm -r "$GATE_LOCK"; fi ;;
  cleanup) echo 1120 > clock; rm "$GATE_LOCK.cleanup" ;;
  failed-wait) echo 1120 > clock; exit 7 ;;
  held-flock) echo 1120 > clock ;;
esac
STUB
  cat > "$work/gate-bin/flock" <<'STUB'
#!/bin/sh
if [ "$1" = -n ]; then exit 75; fi
exec sleep 1
STUB
  printf '#!%s\n' "$BASH" > "$work/gate-bin/lslocks"
  cat >> "$work/gate-bin/lslocks" <<'STUB'
read -r device inode < <(stat -Lc '%Hd:%Ld %i' "$GATE_LOCK") || exit 1
printf '1 FLOCK %s 0\n%s FLOCK %s %s\n' "$device" "$(cat holder.pid)" "$device" "$inode"
STUB
  cat > "$work/gate-bin/nix" <<'STUB'
#!/bin/sh
if [ "$1 $2" = 'flake check' ]; then
  cat .cache/run.status > checking.status
  echo "$(($(cat clock) + 60))" > clock
  if [ "$GATE_FIXTURE" = corrupt-timing ]; then
    echo 'held undefined_clock' >> "$GATE_RUN_TIMING"
  fi
fi
STUB
  chmod +x "$work/gate-bin/"*
  cat > "$work/real-gate.sh" <<'STUB'
set -eu
export PATH="$GATE_FIXTURE_BIN:$PATH" GATE_LOCK=$PWD/test-lock
export GATE_PASSED=$PWD/test-passed GATE_KEEP=$PWD/test-keep
echo 1000 > clock
case "$GATE_FIXTURE" in
  held-directory | short-directory | failed-wait)
    mkdir "$GATE_LOCK"; echo "$PPID" | tee "$GATE_LOCK/pid" > holder.pid ;;
  cleanup) echo fixture-owner > "$GATE_LOCK.cleanup" ;;
  held-flock) echo "$PPID" > holder.pid ;;
esac
exec bash architecture/gate.sh
STUB
  for lock_case in held-directory short-directory free-directory cleanup failed-wait corrupt-timing held-flock; do
    [ "$lock_case" != held-flock ] || [ "$(uname -s)" = Linux ] || continue
    name="a full gate reports build and lock wait time ($lock_case)"
    supervisor_repo
    cp "$gate" architecture/gate.sh
    printf 'if [ "$1" = --profile ]; then echo .cache/test-profile; fi\n' > architecture/space.sh
    SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|blocked a|blocked b' \
      RUN_GATE="bash $work/real-gate.sh" RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 \
      GATE_FIXTURE="$lock_case" GATE_FIXTURE_BIN="$work/gate-bin"
    sessions "$name" 3
    if [ -f holder.pid ] || [ "$lock_case" = cleanup ]; then
      blocker='cleanup of fixture-owner'
      if [ "$lock_case" != cleanup ]; then blocker="process $(cat holder.pid)"; fi
      grep -Fxq "full gate for #1 is waiting for the gate lock ($blocker)" waiting.status \
        || fail "$name: status did not name the holder of the gate lock"
    fi
    if [ "$lock_case" = free-directory ] || [ "$lock_case" = short-directory ] || [ "$lock_case" = corrupt-timing ]; then
      ! grep -q 'minutes for the gate lock$' .cache/run.history \
        || fail "$name: a wait of less than one minute was recorded"
    else
      [ "$(grep -c 'waited .* minutes for the gate lock$' .cache/run.history)" -eq 1 ] \
        && grep -q 'waited 2 minutes for the gate lock$' .cache/run.history \
        || fail "$name: the lock wait was not recorded once at two minutes"
    fi
    if [ "$lock_case" = failed-wait ]; then
      expect "$name" 'full gate for #1 failed in 0 minutes$'
    else
      grep -Fxq 'full gate for #1' checking.status || fail "$name: status did not return to the full gate"
      expect "$name" 'full gate for #1 passed in 1 minutes$'
    fi
    printf 'run-test: %s: pass\n' "$name"
  done

  if command -v timeout > /dev/null; then
    name="a gate timeout reports its limit to the lead and history"
    supervisor_repo
    commit=$(git rev-parse HEAD)
    SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|blocked a|blocked b' \
      RUN_GATE_LIMIT=1 RUN_CLOCK=2 GATE_SLEEP=5 PATH="$work/timeout:$PATH"
    sessions "$name" 3
    sed -n 2p "$dir/prompts" | grep -Fq " The run ran the full gate on commit $commit: it did not end in 1 minutes. Read the end of .cache/gate.log first." || fail "$name: no timeout note"
    grep -q 'full gate for #1 did not end in 1 minutes$' "$dir/.cache/run.history" || fail "$name: no timeout history"
    expect "$name" 'full gate for #1 did not end in 1 minutes$'
    grep -Fxq -- "--signal=TERM 2 bash $work/gate.sh" "$dir/timeout.calls" || fail "$name: timeout did not use TERM and limit times the clock unit"

    name="the gate timeout defaults to 360 minutes"
    supervisor_repo
    SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|blocked a|blocked b' RUN_CLOCK=60 PATH="$work/timeout:$PATH"
    sessions "$name" 3
    grep -Fxq -- "--signal=TERM 21600 bash $work/gate.sh" "$dir/timeout.calls" || fail "$name: the default limit is not 360 minutes"
  else
    echo 'run-test: unavailable: a gate timeout reports its limit to the lead and history: timeout is not on PATH'
    echo 'run-test: unavailable: the gate timeout defaults to 360 minutes: timeout is not on PATH'
  fi

  name="tracked changes refuse and clear a pending gate"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'dirty:gate|planned|blocked a|blocked b'
  sessions "$name" 4
  [ ! -e "$dir/gate.status" ] || fail "$name: the gate ran on tracked edits"
  sed -n 2p "$dir/prompts" | grep -Fq ' The run did not run the full gate: the worktree holds tracked changes that are not committed. Commit the step, then end with STEP gate.' || fail "$name: no tracked-edit note"
  sed -n 3p "$dir/prompts" | grep -q 'The run did not run the full gate' && fail "$name: the refusal repeated"

  name="an unreadable HEAD refuses the gate"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'nohead:gate|blocked a|blocked b'
  sessions "$name" 3
  [ ! -e "$dir/gate.status" ] || fail "$name: the gate ran without HEAD"
  sed -n 2p "$dir/prompts" | grep -Fq 'The run did not run the full gate: HEAD changed after the request (unknown) by more than a merge of main. Make sure that the step is checked out and committed, then end with STEP gate.' || fail "$name: no missing-HEAD note"

  name="a failed status read refuses the gate"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'badindex:gate|blocked a|blocked b'
  sessions "$name" 3
  [ ! -e "$dir/gate.status" ] || fail "$name: the gate ran with an unreadable index"
  sed -n 2p "$dir/prompts" | grep -Fq 'The run did not run the full gate: the worktree holds tracked changes that are not committed.' || fail "$name: no status-read note"

  name="untracked files do not refuse the gate"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'untracked:gate|blocked a|blocked b'
  sessions "$name" 3
  [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: the gate did not run once"
  sed -n 2p "$dir/prompts" | grep -q 'it passed\.' || fail "$name: no pass note"

  name="a failed merge of main refuses and clears a pending gate"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'conflict:gate|planned|blocked a|blocked b' RUN_SYNC=1
  sessions "$name" 4
  [ ! -e "$dir/gate.status" ] || fail "$name: the gate ran after a failed merge"
  sed -n 2p "$dir/prompts" | grep -Fq ' The run did not run the full gate: main is not in the step. It does not merge cleanly, or files that are not tracked stopped the run from merging it. Integrate main, remove or commit those files, then end with STEP gate.' || fail "$name: no merge note"
  sed -n 3p "$dir/prompts" | grep -q 'The run did not run the full gate' && fail "$name: the refusal repeated"

  name="untracked files with main ahead prevent the merge and refuse the gate"
  supervisor_repo
  commit=$(git rev-parse HEAD)
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'aheadloose:gate|dirty:gate|blocked a|blocked b' RUN_SYNC=1
  sessions "$name" 4
  [ -f "$dir/loose" ] && [ "$(git rev-parse HEAD)" = "$commit" ] || fail "$name: the sync block merged main"
  if git merge-base --is-ancestor origin/main HEAD; then fail "$name: main is not ahead"; fi
  [ ! -e "$dir/gate.status" ] || fail "$name: the gate ran without main"
  for line in 2 3; do
    sed -n "${line}p" "$dir/prompts" | grep -Fq ' The run did not run the full gate: main is not in the step. It does not merge cleanly, or files that are not tracked stopped the run from merging it. Integrate main, remove or commit those files, then end with STEP gate.' || fail "$name: session $line lost the main note"
  done

  name="sync off runs the gate with main ahead"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'ahead:gate|blocked a|blocked b' RUN_SYNC=0
  sessions "$name" 3
  if git merge-base --is-ancestor origin/main HEAD; then fail "$name: main is not ahead"; fi
  [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: the gate did not run once"
  sed -n 2p "$dir/prompts" | grep -q 'it passed\.' || fail "$name: no pass note"

  name="sync on with unreadable main still runs the gate"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'nomain:gate|blocked a|blocked b' RUN_SYNC=1
  sessions "$name" 3
  if git rev-parse --verify origin/main > /dev/null 2>&1; then fail "$name: main can still be read"; fi
  [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: the gate did not run once"
  sed -n 2p "$dir/prompts" | grep -q 'it passed\.' || fail "$name: no pass note"

  name="a fresh card parks on request eight after its first pass and seven unchanged requests"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|gate|gate|gate|gate|gate|gate|gate final|blocked b'
  sessions "$name" 9
  expect "$name" 'parked #1: waited six times: gate final$'
  [ "$(wc -l < "$dir/gate.status")" -eq 7 ] || fail "$name: not exactly seven gates"

  for action in ahead: ahead:merge:; do
    name="main updates without lead commits park repeated requests ($action)"
    supervisor_repo
    SUPERVISOR=architecture/run.sh run_case "$name" 1 \
      "commit:gate|${action}gate|${action}gate|${action}gate|${action}gate|${action}gate|${action}gate|${action}gate final|blocked b" RUN_SYNC=1
    sessions "$name" 9
    expect "$name" 'parked #1: waited six times: gate final$'
    [ "$(wc -l < "$dir/gate.status")" -eq 7 ] || fail "$name: not exactly seven gates"
    [ "$(sort -u "$dir/gate.commits" | wc -l)" -eq 7 ] || fail "$name: the gates did not use distinct commits"
    [ "$(git -C "$dir" rev-list --count --merges HEAD)" -eq 7 ] || fail "$name: main did not arrive through real merges"
    if [ "$action" = ahead:merge: ]; then
      for line in 2 3 4 5 6 7 8; do
        [ "$(git -C "$dir" rev-list --parents -n 1 "$(cat "$dir/request.$line")" | wc -w)" -eq 3 ] || fail "$name: the request is not a merge commit"
      done
    fi
  done

  name="lead repairs between main updates keep repeated requests moving"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 \
    'commit:gate|ahead:commit:gate|ahead:commit:gate|ahead:commit:gate|ahead:commit:gate|ahead:commit:gate|ahead:commit:gate|ahead:commit:gate|blocked a|blocked b' RUN_SYNC=1
  sessions "$name" 10
  expect "$name" 'parked #1: blocked a$'
  [ "$(wc -l < "$dir/gate.status")" -eq 8 ] || fail "$name: the repair gates did not all run"
  [ "$(git -C "$dir" rev-list --count --merges HEAD)" -eq 7 ] || fail "$name: main did not arrive through real merges"

  name="a missing last passed request counts as new progress"
  supervisor_repo
  mkdir -p .cache
  commit=$(git rev-parse HEAD)
  printf 'gate_requests[1]=%s\nlast_passed_requests[1]=%s\n' "$commit" 1111111111111111111111111111111111111111 > .cache/run.state
  printf 'last=$\x27#1\tT1\tcard 1\x27\nwaits=6\n' >> .cache/run.state
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|blocked a|blocked b' RUN_STATE=.cache/run.state
  sessions "$name" 3
  expect "$name" 'parked #1: blocked a$'
  [ "$(wc -l < "$dir/gate.status")" -eq 2 ] || fail "$name: the missing commit did not reset waits"

  name="a merge between gate requests resets their wait count"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|gate|gate|gate|gate|gate|merged 1|gate|blocked a|blocked b'
  sessions "$name" 10
  expect "$name" 'parked #1: blocked a'
  [ "$(wc -l < "$dir/gate.status")" -eq 7 ] || fail "$name: the merge did not reset waits"

  name="a gate result resets the count of leads that could not start"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 '-|gate|-|blocked a|blocked b' RUN_QUICK=999 RUN_RETRIES=2
  sessions "$name" 5
  expect "$name" 'parked #1: blocked a'

  name="--once ends at STEP gate without running a gate"
  supervisor_repo
  ONCE=--once SUPERVISOR=architecture/run.sh run_case "$name" 0 'gate|blocked never'
  sessions "$name" 1
  [ ! -e "$dir/gate.status" ] || fail "$name: --once ran the gate"
  [ "$(grep -Fc 'run: with --once the run runs no gate; run just gate' "$dir/out")" -eq 1 ] || fail "$name: no single --once explanation"
  [ ! -e "$dir/.cache/run.state" ] || fail "$name: --once saved state"

  name="another selected card runs first and the pending gate follows its own card"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'other:gate|blocked b|blocked a' RUN_ITEMS=items.json
  sessions "$name" 3
  [ "$(cat "$dir/events")" = "$(printf 'session 1\nsession 2\ngate 2\nsession 3')" ] || fail "$name: the pending gate did not wait for its card"
  sed -n 2p "$dir/prompts" | grep -q 'card #2 in' || fail "$name: the other card did not run first"
  sed -n 2p "$dir/prompts" | grep -q 'The run ran the full gate' && fail "$name: the other card got the gate note"
  sed -n 3p "$dir/prompts" | grep -q 'card #1 in.*The run ran the full gate.*it passed\.' || fail "$name: its card did not get the gate note"

  name="a gate request keeps the sleeping card next without sleeping"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'later 1 x|takes 2:gate|blocked b|blocked a' \
    RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false
  sessions "$name" 4
  sed -n 3p "$dir/prompts" | grep -q 'card #2 in.*it passed\.' || fail "$name: the set-aside card came back before the gate"
  [ ! -e "$dir/sleeps" ] || fail "$name: the gate request slept"

  name="a second card can request a gate without losing the first card's gate"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'other:gate|gate|blocked b|blocked a' RUN_ITEMS=items.json
  sessions "$name" 4
  [ "$(cat "$dir/events")" = "$(printf 'session 1\nsession 2\ngate 2\nsession 3\ngate 3\nsession 4')" ] || fail "$name: a pending gate was lost or ran on the wrong card"
  [ "$(cat "$dir/gate.status")" = "$(printf 'full gate for #2\nfull gate for #1')" ] || fail "$name: the gates ran on the wrong cards"

  name="the disk check makes space before the pending gate"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'full:gate|blocked a|blocked b' \
    STEP_FREE=15 RUN_CLEAN="bash $work/gate-clean.sh"
  sessions "$name" 3
  [ "$(cat "$dir/events")" = "$(printf 'session 1\ncleanup\ngate 1\nsession 2\nsession 3')" ] || fail "$name: the gate ran before the disk check"

  name="a pending gate survives a supervisor restart and runs once after sync"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'main:gate|blocked a|blocked b' RUN_SYNC=1
  sessions "$name" 3
  restarted_once
  ( . "$dir/restart.state"; [ "${gate_requests[1]:-}" = "$(cat "$dir/request.1")" ] && [ -z "${gate_results[1]+set}" ] ) || fail "$name: the gate ran before restart"
  [ "$(cat "$dir/events")" = "$(printf 'session 1\ngate 1\nsession 2\nsession 3')" ] || fail "$name: restart lost or repeated the gate"
  commit=$(git -C "$dir" rev-parse HEAD)
  sed -n 2p "$dir/prompts" | grep -Fq "The run ran the full gate on commit $commit: it passed." || fail "$name: the gate note did not survive restart or used the old commit"
  expect "$name" '^run: main session on #1'

  name="each gate replaces its log with stdout and stderr"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|gate|blocked a|blocked b'
  sessions "$name" 4
  [ "$(cat "$dir/.cache/gate.log")" = "$(printf 'stdout gate 2\nstderr gate 2')" ] || fail "$name: the log lost output or kept the previous gate"

  name="a supervisor restart before any gate keeps empty pending state"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'main:planned|gate|blocked a|blocked b' RUN_SYNC=1
  sessions "$name" 4
  restarted_once
  [ "$(grep -c '^run: session on ' "$dir/out")" -eq 1 ] || fail "$name: restart was delayed until after the gate request"
  [ "$(wc -l < "$dir/gate.status")" -eq 1 ] || fail "$name: the empty state did not accept a later gate"

  name="a failed Checks note and a gate note both reach the lead"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|blocked a|blocked b' RUN_CHECKS="bash $work/gh.sh" STUB_RUNS="$work/failed.json"
  sessions "$name" 3
  sed -n 2p "$dir/prompts" | grep -q ' The last Checks run of main failed: run 4242\. The run ran the full gate on commit .*it passed\.' || fail "$name: the lead did not get both notes"

  name="the default gate command runs in the worktree"
  supervisor_repo
  cp "$work/gate.sh" architecture/gate.sh
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|blocked a|blocked b' RUN_GATE=
  sessions "$name" 3
  [ "$(cat "$dir/gate.status")" = 'full gate for #1' ] || fail "$name: the default command did not run in the worktree"

  name="the gate note names HEAD before the gate"
  supervisor_repo
  commit=$(git rev-parse HEAD)
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|blocked a|blocked b' GATE_COMMIT=1
  sessions "$name" 3
  [ "$commit" != "$(git -C "$dir" rev-parse HEAD)" ] || fail "$name: the fixture did not change HEAD during the gate"
  sed -n 2p "$dir/prompts" | grep -Fq "The run ran the full gate on commit $commit: it passed." || fail "$name: the gate used HEAD after the gate"

  name="the gate runs without timeout when the command is absent"
  supervisor_repo
  mkdir -p "$work/no-timeout"
  for tool in bash git jq awk basename cat cksum cp date dirname env grep mkdir mv rm sed sort tail tr wc; do
    ln -s "$(command -v "$tool")" "$work/no-timeout/$tool"
  done
  saved_guard=( ${guard[@]+"${guard[@]}"} ); guard=()
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|blocked a|blocked b' PATH="$work/no-timeout"
  guard=( ${saved_guard[@]+"${saved_guard[@]}"} )
  sessions "$name" 3
  sed -n 2p "$dir/prompts" | grep -q 'it passed\.' || fail "$name: the gate failed without timeout"

  name="an unchanged supervisor stays quiet across several passes"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'planned|blocked a|blocked b' \
    RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 3
  no_restart
  no_supervisor_line

  name="a failed state save keeps the supervisor and the run goes on"
  supervisor_repo
  mkdir -p .cache/run.state
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'main:merged pr 7|blocked a|blocked b' \
    RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 3
  expect "$name" 'the run state could not be saved; the run keeps its supervisor'
  expect "$name" 'steps merged or planned in this run: 1'
  no_restart

  name="a supervisor removed on main is not reported"
  supervisor_repo; outside_supervisor
  git -C origin rm -q architecture/run.sh
  git -C origin -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit -qm removed
  SUPERVISOR="$outside" run_case "$name" 1 'blocked a|blocked b' RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 2; no_restart; no_supervisor_line
  grep -q 'No such file or directory' "$dir/out" && fail "$name: a missing-file error was printed"

  name="a clean update from main restarts once and keeps a parked card parked"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 \
    'blocked before restart|main:merged pr 7|blocked after restart' \
    RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 3
  expect "$name" 'main session on #2'
  expect "$name" 'no other card is ready in studio'
  expect "$name" '^  #1 T1 card 1: blocked before restart$'
  [ "$(grep -c 'studio: parked #1:' "$dir/notified")" -eq 1 ] || fail "$name: the parked card was notified twice"
  for prompt in '1 #1' '2 #2' '3 #2'; do
    read -r line card_number <<< "$prompt"
    sed -n "${line}p" "$dir/prompts" | grep -q "card $card_number in" || fail "$name: prompt $line does not name $card_number"
  done
  restarted_once
  grep -q 'parked #1: blocked before restart' "$dir/.cache/run.history" || fail "$name: the old history was lost"
  [ "$(cat "$dir/.cache/run.status")" = 'ended: no other card is ready' ] || fail "$name: the status was not kept"

  name="a failure before restart counts toward the next failure"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'main:failed x|failed x|blocked b' \
    RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 3
  expect "$name" 'parked #1: failed x'
  restarted_once

  name="restart keeps the sleeping card next after the set-aside deadline"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'later 5 x|main:waiting 10 y|blocked a|blocked b' \
    RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false
  sessions "$name" 4
  for prompt in '1 #1' '2 #2' '3 #2' '4 #1'; do
    read -r line card_number <<< "$prompt"
    sed -n "${line}p" "$dir/prompts" | grep -q "card $card_number in" || fail "$name: prompt $line does not name $card_number"
  done
  [ "$(cat "$dir/sleeps")" = 10 ] || fail "$name: the waiting card did not sleep for ten seconds"
  restarted_once

  name="lead starts before restart count toward the retry limit"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 2 '-|main:-|-|-|-' \
    RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_QUICK=999 RUN_RETRIES=3
  sessions "$name" 3
  expect "$name" 'the lead could not start 3 times in a row'
  grep -q 'parked #' "$dir/out" && fail "$name: a card was parked"
  restarted_once

  name="a planned result before restart counts toward the next plan"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'main:planned|planned|blocked b' \
    RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 3
  expect "$name" 'parked #1: planned twice without a step'
  restarted_once

  name="waits before restart count toward parking"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 \
    'waiting 0 x|waiting 0 x|waiting 0 x|waiting 0 x|waiting 0 x|main:waiting 0 x|waiting 0 x|blocked b' \
    RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 8
  expect "$name" 'parked #1: waited six times: 0 x'
  restarted_once

  name="full-disk failures before restart count toward parking"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 \
    'full:failed x|mainfull:failed x|full:failed x|blocked b' \
    RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" \
    STEP_FREE=15 RUN_CLEAN="bash $work/always.sh"
  sessions "$name" 4
  expect "$name" 'parked #1: the disk filled during three sessions: failed x'
  restarted_once

  name="restart keeps a set-aside deadline, sparse counts and the step total"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 \
    'failed x|later 10 n|main:planned|takes 3:merged pr 7|blocked b|failed x' \
    RUN_SYNC=1 RUN_RETRY_WAIT=600 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN=false
  sessions "$name" 6
  for prompt in '1 #1' '2 #1' '3 #2' '4 #2' '5 #2' '6 #1'; do
    read -r line card_number <<< "$prompt"
    sed -n "${line}p" "$dir/prompts" | grep -q "card $card_number in" || fail "$name: prompt $line does not name $card_number"
  done
  [ "$(cat "$dir/sleeps")" = 7 ] || fail "$name: the saved deadline did not leave seven seconds"
  [ "$(cat "$dir/clock")" = 10 ] || fail "$name: the card did not return at its deadline"
  expect "$name" 'parked #1: failed x'
  expect "$name" 'steps merged or planned in this run: 2'
  # Card 1 has a saved entry and index 0 is absent, on both Bash versions.
  ( . "$dir/restart.state"; [ -z "${saved_counts[0]+set}" ] && [ -n "${saved_counts[1]+set}" ] ) \
    || fail "$name: the fixture did not exercise an indexed array with holes"
  restarted_once

  name="the default clock keeps deadlines in system seconds"
  supervisor_repo
  before=$(date +%s)
  SUPERVISOR=architecture/run.sh run_case "$name" 2 'later 120 clock|main:merged pr 7|nogh2:blocked b' \
    RUN_SYNC=1 RUN_NOW= PATH="$work/sleep:$PATH" RUN_OPEN=false RUN_ITEMS=items.json RUN_RETRIES=1
  sessions "$name" 3
  ( . "$dir/restart.state"; [ "${aside_ends[1]}" -ge "$((before + 120))" ] ) \
    || fail "$name: the deadline uses seconds since startup instead of system seconds"
  [ ! -e "$dir/sleeps" ] || fail "$name: this case must not wait for real time"
  restarted_once

  name="state from a later version keeps missing defaults and ignores extra variables"
  supervisor_repo
  mkdir -p .cache
  printf 'declare -- parked_lines="  restored summary\n"\ndeclare -- future_state="unused"\n' > .cache/run.state
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'blocked a|blocked b' RUN_STATE=.cache/run.state
  sessions "$name" 2
  expect "$name" '^  restored summary$'
  expect "$name" 'steps merged or planned in this run: 0'
  [ ! -e "$dir/.cache/run.state" ] || fail "$name: the state file remains"
  grep -qv '^unset$' "$dir/state-vars" && fail "$name: RUN_STATE reached the lead"

  name="a leftover state file without the variable is ignored and removed"
  supervisor_repo
  mkdir -p .cache
  printf 'declare -- parked="1,2"\ndeclare -- done_steps="99"\n' > .cache/run.state
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'blocked a|blocked b'
  sessions "$name" 2
  expect "$name" 'steps merged or planned in this run: 0'
  [ ! -e "$dir/.cache/run.state" ] || fail "$name: the leftover state file remains"

  name="a state file that does not parse starts empty and says one line"
  supervisor_repo
  mkdir -p .cache
  printf 'declare -- parked="1,2"\ndeclare -- done_steps="99"\nif\n' > .cache/run.state
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'blocked a|blocked b' RUN_STATE=.cache/run.state
  sessions "$name" 2
  expect "$name" 'steps merged or planned in this run: 0'
  [ "$(grep -c 'saved run state could not be read; starting with empty state' "$dir/out")" -eq 1 ] || fail "$name: no single state warning"
  grep -qiE 'syntax error|unexpected (token|end)' "$dir/out" && fail "$name: bash printed a state syntax error"
  [ ! -e "$dir/.cache/run.state" ] || fail "$name: the bad state file remains"

  name="a state file that cannot be read starts empty and says one line"
  supervisor_repo
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'blocked a|blocked b' RUN_STATE=.cache/missing.state
  sessions "$name" 2
  expect "$name" 'steps merged or planned in this run: 0'
  [ "$(grep -c 'saved run state could not be read; starting with empty state' "$dir/out")" -eq 1 ] || fail "$name: no single state warning"
  grep -q 'No such file' "$dir/out" && fail "$name: bash printed a read error"

  name="a worktree without origin/main prints no fatal line"
  supervisor_repo
  outside_supervisor
  printf '\n# outside version\n' >> "$outside"
  git update-ref -d refs/remotes/origin/main
  SUPERVISOR="$outside" run_case "$name" 1 'blocked a|blocked b' \
    RUN_SYNC=0 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 2
  no_restart
  no_supervisor_line
  grep -q 'fatal:' "$dir/out" && fail "$name: git printed a fatal line"

  name="a physical worktree path does not restart through a directory link"
  supervisor_repo
  ln -s "$dir" "$work/worktree-link"
  SUPERVISOR="$work/worktree-link/architecture/run.sh" run_case "$name" 1 'blocked a|blocked b' \
    RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 2
  no_restart
  no_supervisor_line

  name="a main update restarts before a due weekly design review"
  supervisor_repo
  cp next.sh origin/architecture/run.sh.new
  mv origin/architecture/run.sh.new origin/architecture/run.sh
  git -C origin add architecture/run.sh
  git -C origin -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit -qm supervisor
  SUPERVISOR=architecture/run.sh run_case "$name" 1 'reviewed 0|blocked a|blocked b' \
    RUN_REVIEW=1 RUN_WEEK=2026-46 RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 3
  expect "$name" '^run: main session on the weekly design review$'
  sed -n 1p "$dir/prompts" | grep -q 'weekly design review' || fail "$name: the review was not the first session"
  [ "$(reviews)" -eq 1 ] || fail "$name: $(reviews) review sessions"
  [ "$(cat "$dir/.cache/design-review")" = '2026-46 done' ] || fail "$name: the week is not recorded as done"
  restarted_once

  name="a worktree edit keeps the old supervisor and stays quiet across several passes"
  supervisor_repo
  SUPERVISOR="$dir/architecture/run.sh" run_case "$name" 1 \
    'edit:planned|merged pr 1|planned|blocked a|blocked b' \
    RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 5
  expect "$name" 'session on #2'
  no_restart
  no_supervisor_line

  name="a main supervisor with invalid syntax keeps the old supervisor"
  supervisor_repo
  { cat next.sh; printf '\nif\n'; } > invalid.sh
  mv invalid.sh next.sh
  SUPERVISOR=architecture/run.sh run_case "$name" 1 \
    'main:planned|merged pr 1|planned|blocked a|blocked b' \
    RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 5
  expect "$name" 'session on #2'
  no_restart
  invalid_once
  grep -qiE 'syntax error|unexpected (token|end)' "$dir/out" && fail "$name: bash printed a syntax error"

  name="restart remembers an invalid supervisor already reported"
  supervisor_repo
  { cat next.sh; printf '\nif\n'; } > invalid.sh
  SUPERVISOR=architecture/run.sh run_case "$name" 1 \
    'mainbad:planned|main:merged pr 7|mainbad:planned|blocked a|blocked b' \
    RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 5
  invalid_once
  restarted_once

  name="an identical outside supervisor moves once and follows a later main update once"
  supervisor_repo
  outside_supervisor
  SUPERVISOR="$outside" run_case "$name" 1 \
    'planned|main:merged pr 7|blocked a|blocked b' \
    RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 4
  [ "$(grep -c '^run: session on ' "$dir/out")" -eq 2 ] || fail "$name: the unchanged main did not keep the old behaviour"
  [ "$(grep -c '^run: main session on ' "$dir/out")" -eq 2 ] || fail "$name: the main update did not change behaviour"
  [ "$(sed -n '1p' "$dir/starts")" = "$outside" ] || fail "$name: the launcher did not start the outside copy"
  [ "$(grep -c 'started again with the supervisor of main' "$dir/out")" -eq 2 ] || fail "$name: not exactly two restarts"
  [ "$(grep -c 'started again with the supervisor of main' "$dir/.cache/run.history")" -eq 2 ] || fail "$name: not exactly two restart notes"
  [ "$(wc -l < "$dir/starts")" -eq 3 ] || fail "$name: not exactly three launches"
  [ "$(sed -n '2p' "$dir/starts")" = architecture/run.sh ] || fail "$name: the first move did not use the worktree"
  [ "$(sed -n '3p' "$dir/starts")" = architecture/run.sh ] || fail "$name: the update did not use the worktree"
  [ "$(sort -u "$dir/pids" | wc -l)" -eq 1 ] || fail "$name: a restart changed the process id"
  expect "$name" 'no other card is ready in studio'

  name="a different outside supervisor restarts before the first session"
  supervisor_repo
  outside_supervisor
  sed 's/say "session on /say "outside session on /' "$outside" > "$outside.new"
  mv "$outside.new" "$outside"
  SUPERVISOR="$outside" run_case "$name" 1 'blocked a|blocked b' \
    RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 2
  expect "$name" '^run: session on #1'
  grep -q 'outside session on' "$dir/out" && fail "$name: the outside behaviour reached a session"
  [ "$(grep -c '^run: session on ' "$dir/out")" -eq 2 ] || fail "$name: the worktree did not own every session"
  restarted_once

  name="after the move a replacement outside file changes nothing"
  supervisor_repo
  outside_supervisor
  SUPERVISOR="$outside" run_case "$name" 1 \
    'outside:planned|merged pr 1|planned|blocked a|blocked b' \
    OUTSIDE="$outside" RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 5
  cmp -s "$outside" "$dir/next.sh" || fail "$name: the outside file was not replaced"
  [ "$(grep -c '^run: session on ' "$dir/out")" -eq 5 ] || fail "$name: the old behaviour did not last"
  restarted_once

  name="--once keeps the old supervisor when its first sync changes the file"
  supervisor_repo
  cp next.sh origin/architecture/run.sh.new
  mv origin/architecture/run.sh.new origin/architecture/run.sh
  git -C origin add architecture/run.sh
  git -C origin -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit -qm supervisor
  ONCE=--once SUPERVISOR=architecture/run.sh run_case "$name" 0 'blocked a|blocked never' \
    RUN_SYNC=1 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
  sessions "$name" 1
  expect "$name" 'session on #1'
  no_restart
  no_supervisor_line

  for state in main squash commit edit conflict broken nomain; do
    dir=$work/case$((case_number + 1)); mkdir -p "$dir"; cd "$dir"
    git init -q
    echo '*' > .gitignore
    echo base > tracked
    git add -f tracked
    git -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit -qm base
    git update-ref refs/remotes/origin/main HEAD
    if [ "$state" = nomain ]; then git update-ref -d refs/remotes/origin/main; fi
    if [ "$state" != main ] && [ "$state" != broken ] && [ "$state" != nomain ]; then echo changed > tracked; fi
    if [ "$state" = squash ] || [ "$state" = commit ] || [ "$state" = conflict ]; then
      git add -f tracked
      git -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit -qm change
    fi
    if [ "$state" = squash ]; then
      squash=$(git -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit-tree 'HEAD^{tree}' -p HEAD~1 -m squash)
      git update-ref refs/remotes/origin/main "$squash"
    fi
    if [ "$state" = conflict ]; then
      blob=$(echo 'changed on main' | git hash-object -w --stdin)
      tree=$(printf '100644 blob %s\ttracked\n' "$blob" | git mktree)
      main=$(git -c user.name=Test -c user.email=test@example.com -c commit.gpgsign=false commit-tree "$tree" -p HEAD~1 -m main)
      git update-ref refs/remotes/origin/main "$main"
    fi
    case "$state" in
      main) name="a clean worktree at main holds no open change" ;;
      squash) name="a branch that main took through a squash holds no open change" ;;
      commit) name="a commit that main lacks is an open change" ;;
      edit) name="an edit that is not committed is an open change" ;;
      conflict) name="a branch that conflicts with main is an open change" ;;
      broken) name="a worktree that git cannot read is an open change" ;;
      nomain) name="a clean worktree without origin/main is an open change" ;;
    esac
    if [ "$state" = broken ]; then
      echo bad > .git/index
      if git status --porcelain > /dev/null 2>&1; then
        fail "$name: git status did not fail"
      fi
      git merge-tree --write-tree origin/main HEAD > /dev/null 2>&1 || fail "$name: git merge-tree failed"
    fi
    if [ "$state" = conflict ]; then
      [ -z "$(git status --porcelain)" ] || fail "$name: the worktree is not clean"
      if git merge-tree --write-tree origin/main HEAD > /dev/null 2>&1; then
        fail "$name: the branches do not conflict"
      fi
    fi
    if [ "$state" = main ] || [ "$state" = squash ]; then
      run_case "$name" 1 'later 1 x|blocked b|blocked a' RUN_RETRY_WAIT=600 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN= GIT_CEILING_DIRECTORIES="$work"
      sessions "$name" 3
      expect "$name" 'set aside #1 for 1 minutes: x'
    else
      run_case "$name" 1 'later 1 x|blocked a|blocked b' RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN= GIT_CEILING_DIRECTORIES="$work"
      sessions "$name" 3
      expect "$name" 'waiting 1 minutes: x'
      grep -q 'set aside' "$dir/out" && fail "$name: an open change was set aside"
    fi
  done
fi

name="a folder that is not a repository counts as an open change"
run_case "$name" 1 'later 1 x|blocked a|blocked b' RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH" RUN_OPEN= GIT_CEILING_DIRECTORIES="$work"
sessions "$name" 3
expect "$name" 'waiting 1 minutes: x'
grep -q 'set aside' "$dir/out" && fail "$name: a folder without a repository was set aside"

name="one session with --once"
ONCE=--once run_case "$name" 0 'merged pr 3|merged pr 4'
sessions "$name" 1

# Any number of cards can wait. Ported stage D scenarios use the clock seam.
aside_fixture() {
  dir=$work/case$((case_number + 1)); mkdir -p "$dir/.cache"; cd "$dir"
  echo 0 > clock
  printf '{"items":[%s,%s,%s]}' "$(card 1)" "$(card 2)" "$(card 3)" > initial.json
}

name="observed later 120, repeated later 10, third card over one simulated day"
aside_fixture
printf 'later 120 shared step must merge\n' > result.1
printf 'later 10 dependency not ready\n' > result.2
printf 'later 720 host busy\n' > result.3
ITEMS="$dir/initial.json" RUN_OPEN=false run_case "$name" 143 '' REPEAT_CARDS=1 DAY_END=86400 RUN_ITEMS=items.json \
  RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 RUN_RETRY_WAIT=3600 PATH="$work/sleep:$PATH"
sessions "$name" 16; order 1 2 3 2 2 2 2 2 2 1
[ "$(sed -n 10p "$dir/session.times")" = 7200 ] || fail "$name: card 1 did not return after 120 minutes"
for row in '1 7' '2 7' '3 2'; do
  read -r n count <<< "$row"
  [ "$(grep -c "card #$n in" "$dir/prompts")" -eq "$count" ] || fail "$name: card $n session count changed"
done
[ "$(grep -c 'studio: parked #' "$dir/notified")" -eq 2 ] || fail "$name: repeated park notices"
[ "$(wc -l < "$dir/notified")" -eq 2 ] || fail "$name: unexpected notification kind"
[ "$(cat "$dir/clock")" = 86400 ] || fail "$name: did not simulate one day"
[ "$(wc -l < "$dir/sleeps")" -eq 29 ] || fail "$name: the day did not use bounded polling"
echo 'run-test: day: sessions 7/7/2; notifications park=2 end=0 idle=0 other=0; polls=29; clock=86400'

name="three later 120 cards use idle polls and return at their own deadlines"
aside_fixture
ITEMS="$dir/initial.json" RUN_OPEN=false run_case "$name" 1 \
  'takes 60:later 120 first|takes 60:later 120 second|takes 60:later 120 third|blocked a|blocked b|blocked c' \
  RUN_ITEMS=items.json RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 RUN_RETRY_WAIT=600 \
  PATH="$work/sleep:$PATH"
sessions "$name" 6; order 1 2 3 1 2 3
[ "$(cat "$dir/session.times")" = "$(printf '0\n60\n120\n7260\n7320\n7380')" ] || fail "$name: a card returned at the wrong time"
[ "$(grep -c 'set aside #[123] for 120 minutes:' "$dir/.cache/run.history")" -eq 3 ] || fail "$name: not all three were set aside"
[ "$(grep -c '^600$' "$dir/sleeps")" -eq 11 ] || fail "$name: long waits did not use retry_wait"
[ "$(tail -n 3 "$dir/sleeps")" = "$(printf '480\n60\n60')" ] || fail "$name: the earliest deadline was missed"
[ "$(grep -c '^3 cards set aside; next end at 7260$' "$dir/sleep.status")" -eq 12 ] || fail "$name: status lost the count or next deadline"
[ "$(tail -n 2 "$dir/sleep.status")" = "$(printf '2 cards set aside; next end at 7320\n1 cards set aside; next end at 7380')" ] || fail "$name: status did not follow each deadline"

name="several expired cards with an open change return together in backlog order"
aside_fixture
jq '(.items[] | select(.content.number == 3) | .sequence)=0 | (.items[] | select(.content.number == 2) | .sequence)=1 | (.items[] | select(.content.number == 1) | .sequence)=2' initial.json > ordered.json
printf 'declare -a aside_ends=([1]="600" [2]="600" [3]="600")\n' > .cache/asides
ITEMS="$dir/ordered.json" RUN_OPEN=false run_case "$name" 1 'blocked c|blocked b|blocked a' RUN_ITEMS=items.json RUN_STATE=.cache/asides RUN_OPEN=true \
  RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 3; order 3 2 1
[ "$(cat "$dir/session.times")" = "$(printf '600\n600\n600')" ] || fail "$name: a deadline was ignored"

name="two expired set-aside cards stay aside while an open change has another ready card"
aside_fixture
before 4 'rm -f open'
printf '{"items":[%s,%s,%s,%s]}' "$(card 1)" "$(card 2)" "$(card 3)" "$(card 4)" > initial.json
ITEMS="$dir/initial.json" RUN_OPEN=false run_case "$name" 1 \
  'later 1 first|later 2 second|takes 180:open:blocked c|blocked d|blocked a|blocked b' \
  RUN_ITEMS=items.json RUN_OPEN="bash $work/open.sh" RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 \
  RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 6; order 1 2 3 4 1 2
[ "$(cat "$dir/session.times")" = "$(printf '0\n0\n0\n180\n180\n180')" ] || fail "$name: an expired card displaced ready work"

name="a sleep keeps its card next when several set-aside deadlines end"
aside_fixture
ITEMS="$dir/initial.json" RUN_OPEN=false run_case "$name" 1 'later 1 first|later 2 second|waiting 3 host|blocked c|blocked a|blocked b' \
  RUN_ITEMS=items.json RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 6; order 1 2 3 3 1 2
[ "$(cat "$dir/session.times")" = "$(printf '0\n0\n0\n180\n180\n180')" ] || fail "$name: a set-aside card displaced the sleeping card"

name="the earliest end can belong to a higher-numbered card"
aside_fixture
printf 'declare -a aside_ends=([1]="900" [2]="600")\n' > .cache/asides
RUN_OPEN=false run_case "$name" 1 'blocked b|blocked a' RUN_STATE=.cache/asides RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=1000 \
  PATH="$work/sleep:$PATH"
sessions "$name" 2; order 2 1
[ "$(cat "$dir/session.times")" = "$(printf '600\n900')" ] || fail "$name: did not use the earliest end"
[ "$(head -n 1 "$dir/sleep.status")" = '2 cards set aside; next end at 600' ] || fail "$name: status did not name the earliest end"

name="an expired set-aside card returns in backlog order while another clean card is still ready"
RUN_OPEN=false run_case "$name" 1 'later 1 first|takes 60:planned|blocked a|blocked b' RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 \
  RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 4; order 1 2 1 2
[ "$(cat "$dir/session.times")" = "$(printf '0\n0\n60\n60')" ] || fail "$name: a ready card was held past its deadline"

for mode in aside waiting open_later; do
  for minutes in 720 721 99999 missing; do
    name="$mode minutes: $minutes uses its limit or the default"
    ending=later; opened=false; cap=720; action='set aside #1 for'
    case "$mode" in
      waiting) ending=waiting; cap=120; action=waiting ;;
      open_later) opened=true; cap=120; action=waiting ;;
    esac
    value=$minutes; expected=$cap
    if [ "$minutes" = missing ]; then value=; expected=30; fi
    ONCE=--once RUN_OPEN=false run_case "$name" 0 "$ending $value" RUN_OPEN="$opened" RUN_CLOCK=60 \
      RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
    sessions "$name" 1
    grep -Fq "$action $expected minutes:" "$dir/out" || fail "$name: the minutes changed"
    if [ "$mode" = aside ]; then
      [ ! -e "$dir/sleeps" ] || fail "$name: --once slept after setting a card aside"
    else
      [ "$(cat "$dir/sleeps")" = "$((expected * 60))" ] || fail "$name: the sleep changed"
      ! grep -q 'set aside' "$dir/out" || fail "$name: a sleep became a set-aside card"
    fi
  done
done

name="set-aside minutes that would wrap below zero use the 720 minute limit"
ONCE=--once RUN_OPEN=false run_case "$name" 0 'later 9999999999999999999 overflow' RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
sessions "$name" 1
expect "$name" 'set aside #1 for 720 minutes: overflow'

name="a polling restart with main keeps two set-aside cards and their exact end times"
supervisor_repo
SUPERVISOR=architecture/run.sh RUN_OPEN=false run_case "$name" 1 'later 10 first|takes 60:later 20 second|blocked a|blocked b' \
  RUN_ITEMS=items.json RUN_SYNC=1 RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 RUN_RETRY_WAIT=300 \
  POLL_MAIN_AT=1 PATH="$work/sleep:$PATH"
sessions "$name" 4; order 1 2 1 2
restarted_once
( . "$dir/restart.state"; [ "${#aside_ends[@]}" -eq 2 ] && [ "${aside_ends[1]}" = 600 ] && [ "${aside_ends[2]}" = 1260 ] ) || fail "$name: restart lost or changed a deadline"
[ "$(cat "$dir/session.times")" = "$(printf '0\n0\n600\n1260')" ] || fail "$name: restart changed the return times"
! grep -qE '^declare .* aside(=|_until=)' "$dir/restart.state" || fail "$name: restart retained the obsolete pair"

for mode in normal once; do
  name="legacy aside=7 aside_until=601 imports one card in $mode mode"
  aside_fixture
  printf '{"items":[%s]}' "$(card 7)" > initial.json
  printf 'aside=7\naside_until=601\n' > .cache/legacy
  once_flag=; code=1
  if [ "$mode" = once ]; then once_flag=--once; code=0; fi
  ONCE="$once_flag" ITEMS="$dir/initial.json" RUN_OPEN=false run_case "$name" "$code" 'blocked same' RUN_ITEMS=items.json RUN_STATE=.cache/legacy \
    RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=300 PATH="$work/sleep:$PATH"
  sessions "$name" 1; order 7
  [ "$(cat "$dir/session.times")" = 601 ] || fail "$name: legacy card lost its deadline"
  [ "$(cat "$dir/sleeps")" = "$(printf '300\n300\n1')" ] || fail "$name: legacy wait did not poll to its deadline"
done

name="no no-card notice until all set-aside deadlines end and the backlog still selects nothing"
aside_fixture
echo '{"items":[]}' > initial.json
printf 'declare -a aside_ends=([1]="5" [2]="9")\n' > .cache/asides
ITEMS="$dir/initial.json" RUN_OPEN=false run_case "$name" 0 'blocked never' RUN_ITEMS=items.json RUN_STATE=.cache/asides \
  RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=3 PATH="$work/sleep:$PATH"
sessions "$name" 0
[ "$(cat "$dir/clock")" = 9 ] || fail "$name: no-card notice came before the last deadline"
[ "$(cat "$dir/sleeps")" = "$(printf '3\n2\n3\n1')" ] || fail "$name: polling missed a deadline"
[ "$(cat "$dir/sleep.status")" = "$(printf '2 cards set aside; next end at 5\n2 cards set aside; next end at 5\n1 cards set aside; next end at 9\n1 cards set aside; next end at 9')" ] || fail "$name: a premature idle notice replaced set-aside status"
[ "$(cat "$dir/notified")" = 'studio: ended: no ready card' ] || fail "$name: wrong end notice"
[ "$(grep -c 'ended: no ready card' "$dir/.cache/run.history")" -eq 1 ] || fail "$name: wrong end history"

name="a deadline that passes during an idle pass causes selection instead of a negative sleep"
aside_fixture
echo '{"items":[]}' > initial.json
printf 'declare -a aside_ends=([1]="3")\n' > .cache/asides
# Each clock read takes two seconds, including the reads before the sleep.
printf 'value=$(cat clock); echo "$((value + 2))" > clock; echo "$value"\n' > advancing.sh
ITEMS="$dir/initial.json" RUN_OPEN=false run_case "$name" 0 'blocked never' RUN_ITEMS=items.json RUN_STATE=.cache/asides \
  RUN_NOW='bash advancing.sh' RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 0
[ ! -e "$dir/sleeps" ] || fail "$name: the run slept after the deadline"

name="a GitHub card becomes ready during set-aside polling and gets the next session"
aside_fixture
printf '{"items":[%s,%s]}' "$(card 1)" "$(card 2)" > initial.json
printf '{"items":[%s,%s,%s]}' "$(card 1)" "$(card 2)" "$(card 3)" | jq '(.items[] | select(.content.number == 3) | .sequence)=0' > ready.json
ITEMS="$dir/initial.json" RUN_OPEN=false run_case "$name" 1 \
  'later 120 first|later 120 second|blocked new|blocked a|blocked b' RUN_ITEMS=items.json \
  RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 RUN_RETRY_WAIT=600 POLL_READY_AT=1 PATH="$work/sleep:$PATH"
sessions "$name" 5; order 1 2 3 1 2
[ "$(cat "$dir/session.times")" = "$(printf '0\n0\n600\n7200\n7200')" ] || fail "$name: new work waited for a set-aside deadline"

name="every set-aside polling pass fetches main and an empty run still ends"
supervisor_repo
mkdir -p .cache poll-bin
printf 'declare -a aside_ends=([1]="9")\n' > .cache/asides
echo '{"items":[]}' > initial.json
real_git=$(command -v git)
printf '#!/bin/sh\necho "$*" >> git.calls\nexec "%s" "$@"\n' "$real_git" > poll-bin/git
chmod +x poll-bin/git
ITEMS="$dir/initial.json" SUPERVISOR=architecture/run.sh RUN_OPEN=false run_case "$name" 0 '' \
  RUN_ITEMS=items.json RUN_STATE=.cache/asides RUN_SYNC=1 RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=3 \
  PATH="$dir/poll-bin:$work/sleep:$PATH"
sessions "$name" 0
[ "$(grep -c '^fetch -q origin main$' "$dir/git.calls")" -eq 3 ] || fail "$name: main was not fetched on each polling pass"
[ "$(cat "$dir/clock")" = 9 ] || fail "$name: run did not end at the last deadline"
[ "$(cat "$dir/notified")" = 'studio: ended: no ready card' ] || fail "$name: wrong end notice"

name="--once with set-aside cards and no ready card waits to the last deadline then ends"
supervisor_repo
mkdir -p .cache poll-bin
printf 'declare -a aside_ends=([1]="5" [2]="9")\n' > .cache/asides
echo '{"items":[]}' > initial.json
printf '#!/bin/sh\necho "$*" >> git.calls\nexit 99\n' > poll-bin/git
chmod +x poll-bin/git
ONCE=--once ITEMS="$dir/initial.json" SUPERVISOR=architecture/run.sh RUN_OPEN=false run_case "$name" 0 '' \
  RUN_ITEMS=items.json RUN_STATE=.cache/asides RUN_SYNC=1 RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=3 \
  PATH="$dir/poll-bin:$work/sleep:$PATH"
sessions "$name" 0
[ ! -e "$dir/git.calls" ] || fail "$name: --once fetched main without a ready card"
[ "$(cat "$dir/clock")" = 9 ] || fail "$name: --once ended before the last deadline"
[ "$(cat "$dir/notified")" = 'studio: ended: no ready card' ] || fail "$name: wrong end notice"


name="legacy aside without an end time keeps main's default of zero"
aside_fixture
printf '{"items":[%s]}' "$(card 7)" > initial.json
printf 'aside=7\n' > .cache/legacy
ITEMS="$dir/initial.json" RUN_OPEN=false run_case "$name" 1 'blocked same' RUN_ITEMS=items.json RUN_STATE=.cache/legacy \
  RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=300 PATH="$work/sleep:$PATH"
sessions "$name" 1; order 7
[ "$(cat "$dir/session.times")" = 0 ] || fail "$name: missing legacy end time changed its default"
[ ! -e "$dir/sleeps" ] || fail "$name: a zero legacy deadline slept"

# Checks waits use the same fixtures and set-aside entries as timed waits.
checks_fixture() {
  aside_fixture
  cp "$work/failed.json" runs.json
  echo '[{"databaseId":5000,"conclusion":"success","event":"workflow_dispatch"}]' > next-runs.json
}

name="Checks wait notes its reason and works the next card until a new run"
checks_fixture
before 2 'cp next-runs.json runs.json'
RUN_OPEN=false run_case "$name" 1 'later checks qualification|blocked b|blocked a' \
  RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json
sessions "$name" 3; order 1 2 1
expect "$name" 'set aside #1 until the next Checks run: qualification'
grep -Fq 'set aside #1 until the next Checks run: qualification' "$dir/.cache/run.history" || fail "$name: missing history note"
[ "$(wc -l < "$dir/gh.calls")" -eq 3 ] || fail "$name: Checks was read twice in one pass"

name="a Checks run ending during the session returns the card in the next pass"
checks_fixture
before 1 'cp next-runs.json runs.json'
# Bound a missed run with a later one; the card must return before this poll.
jq '.[0].databaseId=6000' next-runs.json > later-runs.json
echo 'cp later-runs.json runs.json' > poll.1
RUN_OPEN=false run_case "$name" 1 'later checks qualification|blocked a|blocked b' \
  RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json RUN_NOW="bash $work/now.sh" \
  RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 3; order 1 1 2
[ ! -e "$dir/sleeps" ] || fail "$name: the run ending during the session was missed"
[ "$(wc -l < "$dir/gh.calls")" -eq 3 ] || fail "$name: the result line refreshed the pass snapshot"

for conclusion in success failure; do
  name="Checks wait ignores unchanged cancelled skipped and push runs then returns on $conclusion"
  checks_fixture
  jq --arg conclusion "$conclusion" '.[0].conclusion=$conclusion' next-runs.json > final.json
  for row in '1 cancelled workflow_dispatch' '2 skipped schedule' '3 failure push'; do
    read -r poll outcome event <<< "$row"
    jq --arg outcome "$outcome" --arg event "$event" \
      '[{databaseId:4999,conclusion:$outcome,event:$event}] + .' runs.json > "runs.$poll.json"
    printf 'cp runs.%s.json runs.json\n' "$poll" > "poll.$poll"
  done
  echo 'cp final.json runs.json' > poll.4
  RUN_OPEN=false run_case "$name" 1 'later checks qualification|blocked b|blocked a' \
    RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json RUN_NOW="bash $work/now.sh" \
    RUN_RETRY_WAIT=900 RUN_CLOCK=60 PATH="$work/sleep:$PATH"
  sessions "$name" 3; order 1 2 1
  [ "$(cat "$dir/session.times")" = "$(printf '0\n0\n2400')" ] || fail "$name: returned before the qualifying run or missed the next pass"
  [ "$(cat "$dir/sleeps")" = "$(printf '600\n600\n600\n600')" ] || fail "$name: Checks polling exceeded ten minutes"
  [ "$(sort -u "$dir/sleep.status")" = '1 cards set aside; next end at 129600; 1 of them until the next Checks run' ] || fail "$name: wrong Checks status"
  [ "$(wc -l < "$dir/gh.calls")" -eq 7 ] || fail "$name: not one Checks read per active or idle pass"
  if [ "$conclusion" = failure ]; then
    tail -n 1 "$dir/prompts" | grep -Fq 'The last Checks run of main failed: run 5000.' || fail "$name: failed run did not reach the returning lead"
  else
    tail -n 1 "$dir/prompts" | grep -q 'Checks run of main failed' && fail "$name: success still reported failure"
  fi
done

name="Checks wait with no previous run returns on the first run"
checks_fixture
echo '[]' > runs.json
echo 'cp next-runs.json runs.json' > poll.1
RUN_OPEN=false run_case "$name" 1 'later checks first|blocked b|blocked a' \
  RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 3; order 1 2 1
[ "$(cat "$dir/session.times")" = "$(printf '0\n0\n600')" ] || fail "$name: empty baseline did not wait for its first run"

name="Checks polling uses a retry shorter than ten minutes"
checks_fixture
echo 'cp next-runs.json runs.json' > poll.1
RUN_OPEN=false run_case "$name" 1 'later checks r|blocked b|blocked a' \
  RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=120 PATH="$work/sleep:$PATH"
sessions "$name" 3; order 1 2 1
[ "$(cat "$dir/sleeps")" = 120 ] || fail "$name: Checks polling replaced a shorter retry wait"

name="timed polling keeps retry waits longer than ten minutes"
checks_fixture
RUN_OPEN=false run_case "$name" 1 'later 20 r|blocked b|blocked a' \
  RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 RUN_RETRY_WAIT=900 PATH="$work/sleep:$PATH"
sessions "$name" 3; order 1 2 1
[ "$(cat "$dir/sleeps")" = "$(printf '900\n300')" ] || fail "$name: timed polling gained a Checks limit"

name="seven Checks waits do not add waits or park the card"
checks_fixture
script='later checks qualification|blocked b'
for poll in 1 2 3 4 5 6 7; do
  printf 'echo '\''[{"databaseId":%s,"conclusion":"success","event":"schedule"}]'\'' > runs.json\n' "$((5000 + poll))" > "poll.$poll"
  if [ "$poll" -lt 7 ]; then script="$script|later checks qualification"; fi
done
# Six ordinary waits after seven Checks waits prove that the latter left zero.
script="$script|waiting 0 counted|waiting 0 counted|waiting 0 counted|waiting 0 counted|waiting 0 counted|waiting 0 counted|blocked a"
RUN_OPEN=false run_case "$name" 1 "$script" RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json \
  RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 15
expect "$name" 'parked #1: blocked a$'
[ "$(grep -c 'set aside #1 until the next Checks run' "$dir/.cache/run.history")" -eq 7 ] || fail "$name: did not exercise seven Checks waits"
! grep -q 'waited six times' "$dir/out" || fail "$name: Checks waits raised the wait count"

name="seven timed later waits still park the card"
RUN_OPEN=false run_case "$name" 1 'later 1 r|blocked b|later 1 r|later 1 r|later 1 r|later 1 r|later 1 r|later 1 r' \
  RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 8
expect "$name" 'parked #1: waited six times: 1 r$'

name="an unreadable Checks result becomes later 120 and counts toward parking"
checks_fixture
printf '9999\tfailure\n' > checks-unreadable
RUN_OPEN=false run_case "$name" 1 'later checks r|blocked b|later checks r|later checks r|later checks r|later checks r|later checks r|later checks r' \
  RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 \
  RUN_RETRY_WAIT=7200 PATH="$work/sleep:$PATH"
sessions "$name" 8
expect "$name" 'the Checks runs could not be read; using STEP later 120 r'
expect "$name" 'set aside #1 for 120 minutes: r'
expect "$name" 'parked #1: waited six times: 120 r$'
[ "$(sort -u "$dir/sleeps")" = 7200 ] || fail "$name: fallback did not wait 120 minutes"
[ "$(wc -l < "$dir/gh.calls")" -eq 8 ] || fail "$name: failed Checks read was repeated in the pass"
! grep -q 'Checks run of main failed' "$dir/prompts" || fail "$name: partial failed read reached a lead"

name="an unreadable Checks poll keeps waiting until a later readable run"
checks_fixture
echo 'cp next-runs.json runs.json; touch checks-unreadable' > poll.1
echo 'rm checks-unreadable' > poll.3
RUN_OPEN=false run_case "$name" 1 'later checks r|blocked b|blocked a' \
  RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 3; order 1 2 1
[ "$(cat "$dir/session.times")" = "$(printf '0\n0\n1800')" ] || fail "$name: failed read ended the wait or stopped the run"
[ "$(wc -l < "$dir/gh.calls")" -eq 6 ] || fail "$name: failed polling read repeated in one pass"
[ "$(grep -c 'until the next Checks run' "$dir/sleep.status")" -eq 3 ] || fail "$name: failed poll lost its waiting status"

name="an unreadable Checks result under once uses timed fallback and ignores partial output"
checks_fixture
printf '9999\tfailure\n' > checks-unreadable
ONCE=--once RUN_OPEN=false run_case "$name" 0 'later checks r' \
  RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json PATH="$work/sleep:$PATH"
sessions "$name" 1
expect "$name" 'set aside #1 for 120 minutes: r'
! grep -q 'Checks run of main failed' "$dir/prompts" || fail "$name: partial failed read reached a lead"
[ ! -e "$dir/sleeps" ] || fail "$name: --once fallback slept"

name="timed and Checks cards each return by their own condition"
checks_fixture
echo 'cp next-runs.json runs.json' > poll.1
RUN_OPEN=false run_case "$name" 1 'later 20 clock|later checks r|blocked b|blocked a' \
  RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 4; order 1 2 2 1
[ "$(cat "$dir/session.times")" = "$(printf '0\n0\n600\n1200')" ] || fail "$name: mixed waits returned by the wrong condition"
[ "$(head -n 1 "$dir/sleep.status")" = '2 cards set aside; next end at 1200; 1 of them until the next Checks run' ] || fail "$name: mixed status is unreadable"
[ "$(tail -n 1 "$dir/sleep.status")" = '1 cards set aside; next end at 1200' ] || fail "$name: timed status changed"

for kind in later waiting; do
  name="checks remains a nonnumeric word on the $kind sleep path"
  checks_fixture
  ONCE=--once run_case "$name" 0 "$kind checks r" RUN_OPEN=true RUN_BRANCH='echo x/1-y' \
    RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 PATH="$work/sleep:$PATH"
  sessions "$name" 1
  expect "$name" 'waiting 30 minutes: r'
  [ "$(cat "$dir/sleeps")" = 1800 ] || fail "$name: nonnumeric sleep changed"
done

name="Checks later with another open owner sets aside without sleeping under once"
checks_fixture
echo x/1-y > branch
before 1 'echo x/2-y > branch'
ONCE=--once run_case "$name" 0 'later checks r' RUN_OPEN=true RUN_BRANCH='cat branch' \
  RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json PATH="$work/sleep:$PATH"
sessions "$name" 1
expect "$name" 'set aside #1 until the next Checks run: r'
[ ! -e "$dir/sleeps" ] || fail "$name: --once slept after Checks set-aside"

name="two Checks cards share one read per pass and return together"
checks_fixture
echo 'cp next-runs.json runs.json' > poll.1
RUN_OPEN=false run_case "$name" 1 'later checks first|later checks second|blocked a|blocked b' \
  RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 4; order 1 2 1 2
[ "$(cat "$dir/sleep.status")" = '2 cards set aside; next end at 2160; 2 of them until the next Checks run' ] || fail "$name: status lost a Checks card"
[ "$(cat "$dir/session.times")" = "$(printf '0\n0\n600\n600')" ] || fail "$name: Checks cards did not return together"
[ "$(wc -l < "$dir/gh.calls")" -eq 5 ] || fail "$name: cards did not share the pass read"

for mode in clean slept open idle; do
  name="Checks return follows the $mode set-aside selection path"
  checks_fixture
  before 2 'cp next-runs.json runs.json'
  script='later checks r|planned|blocked a|blocked b'; opened=false
  case "$mode" in
    slept) script='later checks r|waiting 0 r|blocked b|blocked a' ;;
    open)
      script='later checks r|open:planned|blocked b|blocked a'
      opened="bash $work/open.sh" ;;
    idle)
      script='later checks r|open:blocked b|blocked a'
      opened="bash $work/open.sh" ;;
  esac
  RUN_OPEN=false run_case "$name" 1 "$script" RUN_OPEN="$opened" RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json \
    PATH="$work/sleep:$PATH"
  if [ "$mode" = idle ]; then sessions "$name" 3; order 1 2 1
  elif [ "$mode" = clean ]; then sessions "$name" 4; order 1 2 1 2
  else sessions "$name" 4; order 1 2 2 1; fi
done

name="Checks wait survives a polling supervisor restart then returns on a new run"
supervisor_repo
cp "$work/failed.json" runs.json
echo '[{"databaseId":5000,"conclusion":"success","event":"schedule"}]' > next-runs.json
echo 'cp next-runs.json runs.json' > poll.3
SUPERVISOR=architecture/run.sh RUN_OPEN=false run_case "$name" 1 'later checks r|blocked b|blocked a' \
  RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json RUN_SYNC=1 POLL_MAIN_AT=1 \
  RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 3; order 1 2 1; restarted_once
( . "$dir/restart.state"; [ "${aside_ends[1]}" = 2160 ] && [ "${aside_runs[1]:-}" = 4242 ] ) || fail "$name: restart lost the Checks deadline or baseline"
[ "$(cat "$dir/session.times")" = "$(printf '0\n0\n1800')" ] || fail "$name: restart released Checks before a new run"

name="a person starting again clears the saved Checks wait"
checks_fixture
printf 'aside_ends[1]=2160\naside_runs[1]=4242\n' > .cache/run.state
RUN_OPEN=false run_case "$name" 1 'blocked a|blocked b' RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json
sessions "$name" 2; order 1 2
[ ! -e "$dir/.cache/run.state" ] || fail "$name: manual start left saved state"

# A Checks wait always has a deadline, even when no usable run ends.
for mode in unchanged cancelled unreadable; do
  name="Checks deadline returns once with $mode runs and no wait count"
  checks_fixture
  case "$mode" in
    cancelled) echo '[{"databaseId":5000,"conclusion":"cancelled","event":"schedule"}]' > runs.json ;;
    unreadable) before 2 'touch checks-unreadable' ;;
  esac
  before 9 'jq '\''(.items[0].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
  RUN_OPEN=false run_case "$name" 1 \
    'takes 7:later checks r|blocked b|waiting 0 counted|waiting 0 counted|waiting 0 counted|waiting 0 counted|waiting 0 counted|waiting 0 counted|merged done' \
    RUN_ITEMS=items.json RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json RUN_NOW="bash $work/now.sh" \
    RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
  sessions "$name" 9; order 1 2 1 1 1 1 1 1 1
  [ "$(sed -n 3p "$dir/session.times")" = 2167 ] || fail "$name: deadline did not use result time plus 2160 minutes"
  [ "$(grep -c '#1 waited 2160 minutes for a Checks run; none ended' "$dir/.cache/run.history")" -eq 1 ] || fail "$name: missing or repeated deadline history"
  expect "$name" '#1 waited 2160 minutes for a Checks run; none ended'
  ! grep -q 'parked #1' "$dir/out" || fail "$name: deadline raised the wait count"
done

name="seven Checks deadlines do not park or count as waits"
checks_fixture
script='later checks r|blocked b'
for _ in 1 2 3 4 5 6; do script="$script|later checks r"; done
script="$script|waiting 0 counted|waiting 0 counted|waiting 0 counted|waiting 0 counted|waiting 0 counted|waiting 0 counted|merged done"
before 15 'jq '\''(.items[0].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
checks_evidence=1
RUN_OPEN=false run_case "$name" 1 "$script" RUN_ITEMS=items.json \
  RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 15
deadline_status=0; deadlines=$(grep -c '#1 waited 2160 minutes for a Checks run; none ended' "$dir/.cache/run.history") || deadline_status=$?
time_status=0; ninth_session=$(sed -n 9p "$dir/session.times") || time_status=$?
printf 'deadline count lookup status=%s\nninth session lookup status=%s\n' "$deadline_status" "$time_status" >> "$dir/lookup-status"
[ "$deadlines" -eq 7 ] || fail "$name: not seven deadline returns"
[ "$ninth_session" = 15120 ] || fail "$name: seven limits did not pass"
! grep -q 'parked #1' "$dir/out" || fail "$name: deadline returns raised the wait count"
unset checks_evidence

for ending in run deadline; do
  name="a $ending return clears both entries before a plain timed wait"
  checks_fixture
  first=30; notes=0
  if [ "$ending" = run ]; then before 2 'cp next-runs.json runs.json'
  else first=129600; notes=1; fi
  printf '{"items":[%s,%s,%s]}' "$(card 1)" "$(card 2)" "$(card 3)" > initial.json
  before 4 'jq '\''.[0].databaseId=6000'\'' next-runs.json > runs.json'
  before 6 'jq '\''(.items[0].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
  ITEMS="$dir/initial.json" RUN_OPEN=false run_case "$name" 1 \
    "later checks r|takes $first:blocked b|later 1 clock|takes 1:planned|blocked c|merged done" \
    RUN_ITEMS=items.json RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json RUN_NOW="bash $work/now.sh" \
    RUN_CLOCK=60 RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
  sessions "$name" 6; order 1 2 1 3 3 1
  [ "$(tail -n 1 "$dir/session.times")" = "$((first + 60))" ] || fail "$name: plain wait ended on a new Checks run"
  [ "$(grep -c 'none ended' "$dir/.cache/run.history" || true)" -eq "$notes" ] || fail "$name: wrong deadline note count"
  ! grep -q 'parked #1' "$dir/out" || fail "$name: returned card was parked"
done

name="a new Checks run at the deadline returns without a deadline note"
checks_fixture
before 2 'cp next-runs.json runs.json'
RUN_OPEN=false run_case "$name" 1 'later checks r|takes 2160:blocked b|blocked a' \
  RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
sessions "$name" 3; order 1 2 1
! grep -q 'none ended' "$dir/.cache/run.history" || fail "$name: a new run got a deadline note"

name="a state before Checks waits loads a numeric deadline without a run id"
checks_fixture
printf 'declare -a aside_ends=([1]="60")\n' > .cache/state
RUN_OPEN=false run_case "$name" 1 'blocked b|blocked a' RUN_STATE=.cache/state \
  RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 2; order 2 1
[ "$(tail -n 1 "$dir/session.times")" = 60 ] || fail "$name: old deadline was lost"
[ "$(cat "$dir/sleep.status")" = '1 cards set aside; next end at 60' ] || fail "$name: timed status changed"
! grep -q 'none ended' "$dir/.cache/run.history" || fail "$name: timed wait got a Checks deadline note"

for mode in Checks mixed; do
  name="a $mode restart saves decimal deadlines and the Checks run id under its card"
  supervisor_repo
  cp "$work/failed.json" runs.json
  echo x/2-y > branch
  expected=1; script='open:later checks r|main:blocked b|blocked a'
  if [ "$mode" = mixed ]; then
    expected=2; script='open:later checks r|main:later 1 clock|blocked b|blocked a'
    before 2 'echo x/1-y > branch'
  fi
  SUPERVISOR=architecture/run.sh run_case "$name" 1 "$script" \
    RUN_OPEN="bash $work/open.sh" RUN_BRANCH='cat branch' \
    RUN_CHECKS="bash $work/gh.sh" STUB_RUNS=runs.json RUN_SYNC=1 \
    RUN_NOW="bash $work/now.sh" RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
  (
    . "$dir/restart.state"
    [ "${#aside_ends[@]}" -eq "$expected" ] || exit 1
    for number in "${!aside_ends[@]}"; do
      [[ "${aside_ends[number]}" =~ ^[0-9]+$ ]] || exit 1
    done
  ) || fail "$name: saved deadlines are not non-negative decimal numbers"
  ( . "$dir/restart.state"; [ "${aside_ends[1]:-}" = 2160 ] && [ "${aside_runs[1]:-}" = 4242 ] && [ "${#aside_runs[@]}" -eq 1 ] ) \
    || fail "$name: saved Checks run id is missing or paired with the wrong card"
  restarted_once
  if [ "$mode" = mixed ]; then sessions "$name" 4; order 1 2 2 1
  else sessions "$name" 3; order 1 2 1; fi
done

# End Checks wait cases.
# End stage D scenarios.

name="no lead command"
dir=$work/nolead; mkdir -p "$dir"; cd "$dir"; got=0
env -u STEP_LEAD RUN_ITEMS="$work/items.json" bash "$supervisor" studio > out 2>&1 || got=$?
[ "$got" -eq 2 ] || fail "$name: exit $got, wanted 2"
expect "$name" 'STEP_LEAD is not set'

# Open-change cases. Reuse the lead, clock, sleep, card and repository fixtures.
selection() { # branch, skip, owner, first ready, selected (0 means none)
  local branch=$1 skip=$2 owner=$3 first=$4 selected=$5 got_owner got_first picked
  case_number=$((case_number + 1))
  jq -r --arg lane studio --arg mode next --arg skip "$skip" --arg branch "$branch" \
    -f "$backlog" initial.json > out 2>&1 || fail "$name: selection did not run"
  [ "$(wc -l < out)" -eq 1 ] || fail "$name: next returned more than one selection"
  IFS=$'\t' read -r got_owner got_first picked < out
  [ "$got_owner" = "$owner" ] && [ "$got_first" = "$first" ] || fail "$name: owner/first $got_owner/$got_first, wanted $owner/$first"
  if [ "$selected" = 0 ]; then [ -z "$picked" ] || fail "$name: selected an unready card"
  else [ "${picked%%$'\t'*}" = "#$selected" ] || fail "$name: selected $picked, wanted #$selected"; fi
}

name="ready cards follow phase, sequence and card number"
aside_fixture
printf '{"items":[%s,%s,%s,%s,%s,%s]}' \
  "$(card 5 | jq 'del(.sequence)')" \
  "$(card 4 | jq '.phase = "Phase 2"')" \
  "$(card 3 | jq '.status = "Active" | .sequence = 900')" \
  "$(card 2 | jq '.sequence = 21')" \
  "$(card 1 | jq '.sequence = 21')" \
  "$(card 6 | jq '.sequence = 1000000000')" > initial.json
selection '' '' 0 1 1
selection '' '1' 0 2 2
selection '' '1,2' 0 3 3
selection '' '1,2,3' 0 6 6
selection '' '1,2,3,6' 0 5 5
selection 'x/3-y' '' 3 1 3

name="observed sequence: open card 2 precedes ready card 1; merging releases card 1"
supervisor_repo
git branch -m x/step-2-y
echo change >> step; git add step; git commit -qm step
before 1 'jq '\''(.items[] | select(.content.number == 2) | .status)="Merged"'\'' items.json > items.new; mv items.new items.json
 git update-ref refs/remotes/origin/main HEAD'
ITEMS="$work/items.json" run_case "$name" 1 'merged pr 2|blocked done' RUN_ITEMS=items.json
sessions "$name" 2; order 2 1
expect "$name" 'taking #2 with its open change before first ready card #1'
[ "$(grep -c 'taking #' "$dir/out")" -eq 1 ] || fail "$name: notice without a displaced card"
! grep -q 'taking #' "$dir/.cache/run.history" || fail "$name: notice added history"
[ ! -e "$dir/sleeps" ] || fail "$name: selection slept"

# Q1 examples all see the same lane cards, with card 2 first in backlog order.
for row in \
  'refactor/studio-331-8a-handles 331 331' \
  'studio-369-2-transition 369 369' \
  'feat/v2-api-12 12 12' \
  'feat/control-report-20 20 20' \
  'fix/2026-11-12-hotfix 0 2' \
  'studio-370-2-transition 0 2' \
  'feat/x-21 0 2' \
  'fix/requirements-baselines 0 2' \
  'main 0 2' \
  'detached 0 2'; do
  read -r branch owner picked <<< "$row"
  name="Q1: branch example $branch has owner $owner"
  aside_fixture
  printf '{"items":[%s,%s,%s,%s,%s,%s,%s]}' "$(card 2)" "$(card 11)" "$(card 12)" "$(card 20)" \
    "$(card 331)" "$(card 369)" "$(card 21 | jq '.workstream="Server"')" > initial.json
  [ "$branch" != detached ] || branch=''
  selection "$branch" '' "$owner" 2 "$picked"
done
for row in 'x/7 7' 'x/007 7' 'x/0007-2 7' 'x/007-2 0' 'x/000-2 0'; do
  read -r branch owner <<< "$row"
  name="Q1: leading zeros are decimal: $branch has owner $owner"
  aside_fixture
  printf '{"items":[%s,%s]}' "$(card 2)" "$(card 7)" > initial.json
  [ "$branch" != x/007-2 ] || jq 'del(.items[1])' initial.json > initial.new
  [ "$branch" != x/007-2 ] || mv initial.new initial.json
  picked=$owner; [ "$picked" != 0 ] || picked=2
  selection "$branch" '' "$owner" 2 "$picked"
done
for separator in / - _ .; do
  name="Q1: $separator separates a digits-only part"
  aside_fixture
  selection "feat${separator}2${separator}v11" '' 2 1 2
done
for invalid in other-lane merged missing kind unassigned phase; do
  name="Q1: $invalid first number is not an owner and never falls through to card 2"
  aside_fixture
  printf '{"items":[%s,%s,%s]}' "$(card 1)" "$(card 2)" "$(card 369)" > initial.json
  case "$invalid" in
    other-lane) filter='(.items[2].workstream)="Server"' ;;
    merged) filter='(.items[2].status)="Merged"' ;;
    missing) filter='del(.items[2])' ;;
    kind) filter='(.items[2].kind)="Milestone"' ;;
    unassigned) filter='(.items[2].assignees)=[]' ;;
    phase) filter='(.items[2].phase)="Phase 3"' ;;
  esac
  jq "$filter" initial.json > initial.new; mv initial.new initial.json
  selection studio-369-2-transition '' 0 1 1
done
name="Q1: a part with letters is not a number (2a and v2)"
aside_fixture
selection feat/v2-2a '' 0 1 1
for reason in needs-joel dependency parked set-aside; do
  name="P2: $reason owner keeps today's selection"
  aside_fixture; skip=''
  case "$reason" in
    needs-joel) jq '(.items[1].labels)=["needs-joel"]' initial.json > initial.new; mv initial.new initial.json ;;
    dependency) jq '(.items[1].content.body)="## Dependencies\nhttps://github.com/labcast/labcast-server/issues/9"' initial.json > initial.new; mv initial.new initial.json ;;
    parked|set-aside) skip=2 ;;
  esac
  selection x/2-y "$skip" 2 1 1
done
name="P2: the owner is the only ready card"
aside_fixture
printf '{"items":[%s]}' "$(card 2)" > initial.json
selection x/2-y '' 2 2 2
name="P2: no ready card produces no selection"
aside_fixture
jq '.items[].labels=["needs-joel"]' initial.json > initial.new; mv initial.new initial.json
selection x/2-y '' 2 0 0
name="P1/P2: an unready first owner never tries a later ready number"
aside_fixture
jq '(.items[1].labels)=["needs-joel"]' initial.json > initial.new; mv initial.new initial.json
selection x/2-1 '' 2 1 1

name="status output is identical with and without the branch argument"
aside_fixture; case_number=$((case_number + 1))
jq -r --arg lane studio --arg mode status --arg skip '' -f "$backlog" initial.json > out 2>&1 || fail "$name: status failed"
jq -r --arg lane studio --arg mode status --arg skip '' --arg branch x/2 -f "$backlog" initial.json > with-branch 2>&1 || fail "$name: branch status failed"
cmp out with-branch || fail "$name: status changed"
name="next without the new argument preserves the existing card line"
case_number=$((case_number + 1))
jq -r --arg lane studio --arg mode next --arg skip '' -f "$backlog" initial.json > out 2>&1 || fail "$name: legacy next failed"
[ "$(cat out)" = $'#1\tT1\tcard 1' ] || fail "$name: legacy next changed"

name="P3: another owner sets card 1 aside at its own end; 3 sessions and 11 sleeps in 130 minutes"
aside_fixture
printf 'open:later 120 r\n' > result.1
printf 'takes 600:merged pr 2\n' > result.2
before 2 'rm -f open
 jq '\''(.items[] | select(.content.number == 2) | .status)="Merged"'\'' items.json > items.new; mv items.new items.json'
before 3 'echo "takes 600:blocked done" > result.1'
ITEMS="$work/items.json" run_case "$name" 1 '' REPEAT_CARDS=1 RUN_ITEMS=items.json RUN_BRANCH='echo x/step-2-y' \
  RUN_OPEN="bash $work/open.sh" RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 3; order 1 2 1
[ "$(cat "$dir/session.times")" = "$(printf '0\n0\n7200')" ] || fail "$name: session times changed"
[ "$(cat "$dir/clock")" = 7800 ] || fail "$name: did not cover 130 minutes"
[ "$(wc -l < "$dir/sleeps")" -eq 11 ] && [ "$(sort -u "$dir/sleeps")" = 600 ] || fail "$name: expected 11 polling sleeps"
[ "$(grep -c 'set aside #1 for 120 minutes: r' "$dir/.cache/run.history")" -eq 1 ] || fail "$name: missing set-aside history"
[ "$(grep -c '^1 cards set aside; next end at 7200$' "$dir/sleep.status")" -eq 11 ] || fail "$name: lost its own end"

for branch in x/1-y fix/requirements-baselines detached x/21 x/22 x/999; do
  name="P3: own or unknown owner sleeps as today: $branch"
  aside_fixture
  printf '{"items":[%s,%s,%s,%s]}' "$(card 1)" "$(card 2)" "$(card 21 | jq '.workstream="Server"')" "$(card 22 | jq '.status="Merged"')" > initial.json
  branch_command="echo $branch"
  if [ "$branch" = detached ]; then supervisor_repo; git checkout -q --detach; branch_command=''; fi
  ONCE=--once ITEMS="$dir/initial.json" run_case "$name" 0 'later 721 r' RUN_ITEMS=items.json RUN_OPEN=true RUN_BRANCH="$branch_command" \
    RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 PATH="$work/sleep:$PATH"
  sessions "$name" 1; order 1
  [ "$(cat "$dir/sleeps")" = 7200 ] || fail "$name: did not sleep with the 120 minute cap"
  ! grep -q 'set aside' "$dir/.cache/run.history" || fail "$name: own or unknown change was set aside"
done
name="P3: a changed branch is read at the result line, with the 720 minute cap"
aside_fixture; echo x/1-y > branch
before 1 'echo x/2-y > branch'
ONCE=--once run_case "$name" 0 'later 721 r' RUN_OPEN=true RUN_BRANCH='cat branch' RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 PATH="$work/sleep:$PATH"
sessions "$name" 1; order 1
expect "$name" 'set aside #1 for 720 minutes: r'
[ ! -e "$dir/sleeps" ] || fail "$name: another owner slept"
name="P3: STEP waiting still sleeps with another owner at the result line"
ONCE=--once run_case "$name" 0 'open:waiting 1 r' RUN_OPEN="bash $work/open.sh" RUN_BRANCH='echo x/2-y' \
  RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 PATH="$work/sleep:$PATH"
[ "$(cat "$dir/sleeps")" = 60 ] || fail "$name: waiting became later"
name="P3: no open change still sets the card aside"
ONCE=--once run_case "$name" 0 'later 1 r' RUN_OPEN=false RUN_BRANCH='echo x/2-y' PATH="$work/sleep:$PATH"
sessions "$name" 1; order 1
expect "$name" 'set aside #1 for 1 minutes: r'
[ ! -e "$dir/sleeps" ] || fail "$name: a clean later slept"
for state in parked set-aside; do
  name="P2: run passes its $state owner to the existing skip list"
  aside_fixture; mkdir -p .cache
  if [ "$state" = parked ]; then echo 'parked=2' > .cache/state
  else echo 'aside_ends[2]=7200' > .cache/state; fi
  ONCE=--once run_case "$name" 0 'blocked done' RUN_OPEN=true RUN_BRANCH='echo x/2-y' RUN_STATE=.cache/state RUN_NOW="bash $work/now.sh"
  sessions "$name" 1; order 1
  ! grep -q 'taking #' "$dir/out" || fail "$name: an excluded owner got a notice"
done
name="P1: no open change ignores a branch that names card 2"
ONCE=--once run_case "$name" 0 'blocked done' RUN_OPEN=false RUN_BRANCH='echo x/2-y'
sessions "$name" 1; order 1
name="P1: an unreadable branch command is unknown even if it prints a number"
aside_fixture; printf 'echo x/2; exit 1\n' > unreadable
ONCE=--once run_case "$name" 0 'later 1 r' RUN_OPEN=true RUN_BRANCH='bash unreadable' \
  RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 PATH="$work/sleep:$PATH"
sessions "$name" 1; order 1
[ "$(cat "$dir/sleeps")" = 60 ] || fail "$name: an unreadable branch provided an owner"

# Failure injection wraps the existing jq only for a named selection read.
name="P3: a failed owner read falls back to unknown and sleeps"
aside_fixture; mkdir jq-bin
real_jq=$(command -v jq)
printf '#!/bin/sh\nn=$(($(cat jq.next 2>/dev/null || echo 0) + 1)); echo "$n" > jq.next\n[ "$n" != "$FAIL_CHOICE_AT" ] || exit 99\nexec "%s" "$@"\n' "$real_jq" > jq-bin/jq
chmod +x jq-bin/jq
ONCE=--once run_case "$name" 0 'open:later 1 r' RUN_OPEN="bash $work/open.sh" RUN_BRANCH='echo x/2-y' FAIL_CHOICE_AT=2 \
  RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 PATH="$dir/jq-bin:$work/sleep:$PATH"
sessions "$name" 1; order 1
[ "$(cat "$dir/sleeps")" = 60 ] || fail "$name: a failed owner read set the card aside"
name="a failed selection still follows the GitHub retry path"
wrapper=$dir/jq-bin/jq
aside_fixture; mkdir jq-bin; cp "$wrapper" jq-bin/jq
ONCE=--once run_case "$name" 2 'blocked never' RUN_OPEN=false FAIL_CHOICE_AT=1 RUN_RETRIES=1 PATH="$dir/jq-bin:$PATH"
sessions "$name" 0
expect "$name" 'GitHub could not be read 1 times in a row'
name="--once uses the owner of the open change"
ONCE=--once run_case "$name" 0 'blocked done' RUN_OPEN=true RUN_BRANCH='echo x/step-2-y'
sessions "$name" 1; order 2

name="P6: one GitHub board read per selection, including the result owner check"
aside_fixture
before 2 'jq '\''(.items[] | select(.content.number == 2) | .status)="Merged"'\'' items.json > items.new; mv items.new items.json; rm -f open'
run_case "$name" 1 'open:later 0 r|merged pr 2|blocked done' RUN_ITEMS= RUN_OPEN="bash $work/open.sh" RUN_BRANCH='echo x/2-y' \
  RUN_NOW="bash $work/now.sh" STUB_PROJECT=1 PATH="$work/sleep:$PATH"
sessions "$name" 3; order 1 2 1
[ "$(grep -c '^api graphql --paginate ' "$dir/gh.calls")" -eq 4 ] || fail "$name: expected one board read for each of 4 selections"
name="gate request and next session stay with open card 2 before ready card 1"
supervisor_repo; git branch -m x/step-2-y
echo change >> step; git add step; git commit -qm step
before 2 'jq '\''(.items[] | select(.content.number == 2) | .status)="Merged"'\'' items.json > items.new; mv items.new items.json
 git update-ref refs/remotes/origin/main HEAD'
run_case "$name" 1 'gate reviewed head|merged pr 2|blocked done' RUN_ITEMS=items.json
sessions "$name" 3; order 2 2 1
[ "$(cat "$dir/events")" = "$(printf 'session 1\ngate 1\nsession 2\nsession 3')" ] || fail "$name: gate/session order changed"
[ "$(cat "$dir/gate.status")" = 'full gate for #2' ] || fail "$name: gate was not card 2's"
sed -n 2p "$dir/prompts" | grep -q 'card #2 in.*full gate.*it passed' || fail "$name: card 2 did not receive the gate result"
! sed -n 3p "$dir/prompts" | grep -q 'full gate' || fail "$name: card 1 received card 2's gate result"

name="Q1: review replay feat/v2-api-12 gives card 12 its session and sleeps on its own change"
aside_fixture
printf '{"items":[%s,%s]}' "$(card 12 | jq '.sequence=0')" "$(card 2)" > initial.json
ONCE=--once ITEMS="$dir/initial.json" run_case "$name" 0 'later 60 r' RUN_ITEMS=items.json RUN_OPEN=true RUN_BRANCH='echo feat/v2-api-12' \
  RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 PATH="$work/sleep:$PATH"
sessions "$name" 1; order 12
[ "$(cat "$dir/sleeps")" = 3600 ] || fail "$name: its own later did not sleep 60 minutes"
! grep -q 'set aside' "$dir/.cache/run.history" || fail "$name: its own change was set aside"
name="Q4: weekly review takes its pass without a notice; notice precedes the owner's session"
run_case "$name" 1 'reviewed 0|blocked owner|blocked first' RUN_REVIEW=1 RUN_WEEK=2026-46 RUN_OPEN=true RUN_BRANCH='echo x/2-y'
sessions "$name" 3
sed -n 1p "$dir/prompts" | grep -q 'weekly design review' || fail "$name: review did not run first"
sed -n 2p "$dir/prompts" | grep -q 'card #2 in' || fail "$name: owner did not follow review"
[ "$(grep -c 'taking #' "$dir/out")" -eq 1 ] || fail "$name: notice repeated"
review_line=$(grep -n '^run: STEP reviewed' "$dir/out" | cut -d: -f1)
notice_line=$(grep -n '^run: taking #' "$dir/out" | cut -d: -f1)
card_line=$(grep -n '^run: session on #2' "$dir/out" | cut -d: -f1)
[ "$review_line" -lt "$notice_line" ] && [ "$notice_line" -lt "$card_line" ] || fail "$name: notice was not in the owner's pass"
name="Q4: --once weekly review does not announce a card session"
ONCE=--once run_case "$name" 0 'reviewed 0' RUN_REVIEW=1 RUN_WEEK=2026-46 RUN_OPEN=true RUN_BRANCH='echo x/2-y'
sessions "$name" 1
! grep -q 'taking #' "$dir/out" || fail "$name: announced an unstarted card"
name="Q6: waiting sleeps before a newly ready owner takes the next session"
aside_fixture
printf '{"items":[%s,%s]}' "$(card 1)" "$(card 2)" > ready.json
jq '(.items[1].labels)=["needs-joel"]' ready.json > initial.json
ITEMS="$dir/initial.json" run_case "$name" 1 'waiting 60 r|blocked owner|blocked first' RUN_ITEMS=items.json \
  RUN_OPEN=true RUN_BRANCH='echo x/2-y' RUN_NOW="bash $work/now.sh" RUN_CLOCK=60 POLL_READY_AT=1 PATH="$work/sleep:$PATH"
sessions "$name" 3; order 1 2 1
[ "$(cat "$dir/sleeps")" = 3600 ] || fail "$name: waiting did not sleep 60 minutes"
[ "$(cat "$dir/session.times")" = "$(printf '0\n3600\n3600')" ] || fail "$name: owner did not follow the sleep"
! grep -q 'set aside' "$dir/.cache/run.history" || fail "$name: waiting was set aside"
# End open-change cases.

# Label-held cards and backlog polling. All changes use the existing clock,
# items, lead and per-session fixtures; polling actions run in the sleep stub.
name="a labelled blocked card returns after another card merges, with fresh counts"
aside_fixture
cp "$work/items.json" initial.json
jq '(.items[] | select(.content.number == 2) | .status)="Merged"' initial.json > ready.json
printf 'saved_counts[1]="6 1 2 planned"\n' > .cache/counts
before 1 'jq '\''(.items[] | select(.content.number == 1) | .labels)=["needs-joel"]'\'' items.json > items.new; mv items.new items.json'
before 2 'jq '\''(.items[] | select(.content.number == 2) | .status)="Merged"'\'' items.json > items.new; mv items.new items.json'
ITEMS="$dir/initial.json" run_case "$name" 1 \
  'blocked needs-joel question|merged card2|planned|failed once|nospace:failed once|waiting 0 once|blocked done' \
  RUN_ITEMS=items.json RUN_STATE=.cache/counts LABEL_REPLY=items RUN_RETRY_WAIT=7 \
  RUN_NOW="bash $work/now.sh" POLL_READY_AT=3 PATH="$work/sleep:$PATH"
sessions "$name" 7; order 1 2 1 1 1 1 1
expect "$name" 'parked #1: blocked needs-joel question'
[ "$(grep -c 'waiting: no ready card; for Joel 1, parked in this run 0' "$dir/.cache/run.history")" -eq 1 ] || fail "$name: waiting history repeated or has wrong counts"
[ "$(grep -c 'studio: waiting: no ready card; for Joel 1, parked in this run 0' "$dir/notified")" -eq 1 ] || fail "$name: waiting notification repeated or has wrong counts"
[ "$(grep -c '^waiting: no ready card; for Joel 1, parked in this run 0$' "$dir/sleep.status")" -eq 3 ] || fail "$name: status missing during the wait"
[ "$(head -n 3 "$dir/sleeps")" = "$(printf '7\n7\n7')" ] || fail "$name: polling did not use retry_wait"
[ "$(wc -l < "$dir/labels.calls")" -eq 2 ] || fail "$name: labels were not read exactly once at each park"
! grep -q '^  #1 .*needs-joel question' "$dir/out" || fail "$name: labelled card stayed in the summary list"
[ "$(wc -l < "$dir/starts")" -eq 1 ] || fail "$name: the supervisor restarted"

for reply in fail empty false other; do
  name="a label read of $reply keeps the card on the run list and work continues"
  run_case "$name" 1 'blocked question|merged step|blocked done' LABEL_REPLY="$reply"
  sessions "$name" 3; order 1 2 2
  expect "$name" 'no other card is ready'
  grep -q '^  #1 T1 card 1: blocked question$' "$dir/out" || fail "$name: card missing from park summary"
  [ "$(grep -c '^issue view 1 -R labcast/labcast-server --json labels -q ' "$dir/labels.calls")" -eq 1 ] || fail "$name: label query changed or repeated"
  [ "$(cat "$dir/notified")" = "$(printf 'studio: parked #1: blocked question\nstudio: parked #2: blocked done\nstudio: ended: no other card is ready')" ] || fail "$name: existing notices changed"
done

name="two failures park a labelled card by its label and it can fail once after returning"
aside_fixture
cp "$work/items.json" initial.json
jq '(.items[] | select(.content.number == 2) | .status)="Merged"' initial.json > ready.json
before 2 'jq '\''(.items[] | select(.content.number == 1) | .labels)=["needs-joel"]'\'' items.json > items.new; mv items.new items.json'
before 3 'jq '\''(.items[] | select(.content.number == 2) | .status)="Merged"'\'' items.json > items.new; mv items.new items.json'
ITEMS="$dir/initial.json" run_case "$name" 1 'failed first|failed second|merged card2|failed once|blocked done' \
  RUN_ITEMS=items.json LABEL_REPLY=items POLL_READY_AT=1 PATH="$work/sleep:$PATH"
sessions "$name" 5; order 1 1 2 1 1
expect "$name" 'parked #1: failed second'

name="a dependency in another lane does not keep an idle run alive"
aside_fixture
printf '{"items":[%s,%s]}' "$(card 1)" "$(ops_card 9)" \
 | jq '.items[0].content.body="## Dependencies\nhttps://github.com/labcast/labcast-server/issues/9"' > initial.json
ITEMS="$dir/initial.json" run_case "$name" 0 '' RUN_ITEMS=items.json RUN_RETRY_WAIT=600 DAY_END=12000 PATH="$work/sleep:$PATH"
sessions "$name" 0
[ "$(cat "$dir/notified")" = 'studio: ended: no ready card' ] || fail "$name: no-ready notice changed"
[ ! -e "$dir/sleeps" ] || fail "$name: dependency caused a wait"

name="a Joel-only idle wait also fetches main on each polling pass"
supervisor_repo
card 1 | jq '{items: [. + {labels:["needs-joel"]}]}' > initial.json
jq '(.items[].labels)=[]' initial.json > ready.json
mkdir poll-bin
real_git=$(command -v git)
printf '#!/bin/sh\necho "$*" >> git.calls\nexec "%s" "$@"\n' "$real_git" > poll-bin/git
chmod +x poll-bin/git
before 1 'jq '\''(.items[].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
ITEMS="$dir/initial.json" SUPERVISOR=architecture/run.sh run_case "$name" 0 'merged ready' RUN_ITEMS=items.json \
 RUN_SYNC=1 RUN_RETRY_WAIT=7 POLL_READY_AT=2 PATH="$dir/poll-bin:$work/sleep:$PATH"
sessions "$name" 1
[ "$(grep -c '^fetch -q origin main$' "$dir/git.calls")" -eq 3 ] || fail "$name: main was not fetched during the Joel wait"

name="a supervisor update during an idle wait restarts without saving a new wait field"
supervisor_repo
card 1 | jq '{items: [. + {labels:["needs-joel"]}]}' > initial.json
jq '(.items[].labels)=[]' initial.json > ready.json
before 1 'jq '\''(.items[].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
ITEMS="$dir/initial.json" SUPERVISOR=architecture/run.sh run_case "$name" 0 'merged ready' RUN_ITEMS=items.json \
 RUN_SYNC=1 RUN_RETRY_WAIT=7 POLL_MAIN_AT=1 POLL_READY_AT=2 PATH="$work/sleep:$PATH"
sessions "$name" 1
[ "$(wc -l < "$dir/starts")" -eq 2 ] && [ "$(sort -u "$dir/pids" | wc -l)" -eq 1 ] || fail "$name: update did not use one exec in the same process"
! grep -qE '^declare .* (waiting|for_joel)=' "$dir/restart.state" || fail "$name: new wait state was saved"
[ "$(grep -c 'studio: waiting: no ready card;' "$dir/notified")" -eq 2 ] || fail "$name: restarted wait was not announced once again"

name="a session between idle waits gives one new history line and notice"
aside_fixture
card 1 | jq '{items: [. + {labels:["needs-joel"]}]}' > initial.json
jq '(.items[].labels)=[]' initial.json > ready.json
printf 'cp ready.json items.json\n' > poll.2
printf 'cp ready.json items.json\n' > poll.4
before 1 'jq '\''(.items[].labels)=["needs-joel"]'\'' items.json > items.new; mv items.new items.json'
before 2 'jq '\''(.items[].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
ITEMS="$dir/initial.json" run_case "$name" 0 'blocked question|merged ready' RUN_ITEMS=items.json LABEL_REPLY=items \
  RUN_RETRY_WAIT=7 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
sessions "$name" 2; order 1 1
[ "$(grep -c 'waiting: no ready card;' "$dir/.cache/run.history")" -eq 2 ] || fail "$name: not one history line per wait"
[ "$(grep -c 'studio: waiting: no ready card;' "$dir/notified")" -eq 2 ] || fail "$name: not one notification per wait"
[ "$(wc -l < "$dir/sleep.status")" -eq 4 ] || fail "$name: status not retained on all passes"

name="a card for Joel keeps the run waiting while another card waits for a dependency"
aside_fixture
printf '{"items":[%s,%s,%s,%s]}' "$(card 1)" "$(card 2)" "$(card 3)" "$(ops_card 9)" \
 | jq '(.items[0].labels)=["needs-joel"] | (.items[1].content.body)="## Dependencies\nhttps://github.com/labcast/labcast-server/issues/9"' > initial.json
before 1 'jq '\''(.items[] | select(.content.number != 3) | .status)="Merged"'\'' items.json > ready.json'
ITEMS="$dir/initial.json" run_case "$name" 1 'blocked run-list' RUN_ITEMS=items.json LABEL_REPLY=items POLL_READY_AT=2 PATH="$work/sleep:$PATH"
sessions "$name" 1; order 3
[ "$(grep -c 'studio: waiting: no ready card; for Joel 1, parked in this run 1' "$dir/notified")" -eq 1 ] || fail "$name: Joel or parked count wrong"
expect "$name" 'no other card is ready'

name="idle status follows the Joel count without repeating the notice"
aside_fixture
printf '{"items":[%s,%s,%s]}' "$(card 1)" "$(card 2)" "$(ops_card 9)" \
 | jq '(.items[] | select(.workstream == "Studio") | .labels)=["needs-joel"]' > initial.json
printf '%s\n' 'jq '\''(.items[0].labels)=[] | (.items[0].content.body)="## Dependencies\nhttps://github.com/labcast/labcast-server/issues/9"'\'' items.json > items.new; mv items.new items.json' > poll.1
printf '%s\n' 'jq '\''(.items[0].status)="Merged"'\'' items.json > items.new; mv items.new items.json' > poll.2
printf '%s\n' 'jq '\''(.items[].status)="Merged"'\'' items.json > items.new; mv items.new items.json' > poll.3
ITEMS="$dir/initial.json" run_case "$name" 0 '' RUN_ITEMS=items.json RUN_RETRY_WAIT=7 PATH="$work/sleep:$PATH"
sessions "$name" 0
[ "$(cat "$dir/sleep.status")" = "$(printf 'waiting: no ready card; for Joel 2, parked in this run 0\nwaiting: no ready card; for Joel 1, parked in this run 0\nwaiting: no ready card; for Joel 1, parked in this run 0')" ] || fail "$name: idle status kept old counts"
[ "$(grep -c 'waiting: no ready card;' "$dir/.cache/run.history")" -eq 1 ] && [ "$(grep -c 'studio: waiting: no ready card;' "$dir/notified")" -eq 1 ] || fail "$name: a count change repeated the notice"

name="a weekly design review becomes due during an idle wait"
aside_fixture
echo '2026-46 done' > .cache/design-review
card 1 | jq '{items: [. + {labels:["needs-joel"]}]}' > initial.json
jq '(.items[].labels)=[]' initial.json > ready.json
echo 'rm .cache/design-review' > poll.1
before 2 'jq '\''(.items[].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
ITEMS="$dir/initial.json" run_case "$name" 0 'reviewed 0 studio|merged ready' RUN_ITEMS=items.json \
 RUN_REVIEW=1 RUN_WEEK=2026-46 POLL_READY_AT=3 PATH="$work/sleep:$PATH"
sessions "$name" 2
sed -n 1p "$dir/prompts" | grep -q 'weekly design review' || fail "$name: review did not run during the wait"
sed -n 2p "$dir/prompts" | grep -q 'card #1 in' || fail "$name: card did not follow the review"

name="unreadable GitHub during an idle wait retries then stops"
aside_fixture
card 1 | jq '{items: [. + {labels:["needs-joel"]}]}' > initial.json
echo 'mv items.json items.away' > poll.1
ITEMS="$dir/initial.json" run_case "$name" 2 '' RUN_ITEMS=items.json RUN_RETRIES=3 RUN_RETRY_WAIT=7 PATH="$work/sleep:$PATH"
sessions "$name" 0
expect "$name" 'GitHub could not be read 3 times in a row'
[ "$(cat "$dir/sleeps")" = "$(printf '7\n7\n7')" ] || fail "$name: GitHub retries changed"

name="an unreadable Joel count retries through the GitHub failure rule"
aside_fixture
mkdir jq-bin
real_jq=$(command -v jq)
printf '#!/bin/sh\ncase " $* " in *" --arg mode joel "*) exit 99 ;; esac\nexec "%s" "$@"\n' "$real_jq" > jq-bin/jq
chmod +x jq-bin/jq
card 1 | jq '{items: [. + {labels:["needs-joel"]}]}' > initial.json
ITEMS="$dir/initial.json" run_case "$name" 2 '' RUN_ITEMS=items.json RUN_RETRIES=2 RUN_RETRY_WAIT=7 PATH="$dir/jq-bin:$work/sleep:$PATH"
sessions "$name" 0
expect "$name" 'GitHub could not be read 2 times in a row'
[ "$(cat "$dir/sleeps")" = 7 ] || fail "$name: count failures did not use bounded retries"

name="a seventh gate request held by Joel is discarded before the card returns"
supervisor_repo
cp "$work/items.json" initial.json
jq '(.items[] | select(.content.number == 2) | .status)="Merged"' initial.json > ready.json
# Failed gates do not reset the wait count; the seventh request parks the card.
before 7 'jq '\''(.items[] | select(.content.number == 1) | .labels)=["needs-joel"]'\'' items.json > items.new; mv items.new items.json'
before 8 'jq '\''(.items[] | select(.content.number == 2) | .status)="Merged"'\'' items.json > items.new; mv items.new items.json'
ITEMS="$dir/initial.json" SUPERVISOR=architecture/run.sh run_case "$name" 1 \
 'gate|gate|gate|gate|gate|gate|gate|merged card2|gate fresh|blocked done' \
 RUN_ITEMS=items.json GATE_CODE=1 LABEL_REPLY=items POLL_READY_AT=1 PATH="$work/sleep:$PATH"
sessions "$name" 10; order 1 1 1 1 1 1 1 2 1 1
[ "$(cat "$dir/events")" = "$(printf 'session 1\ngate 1\nsession 2\ngate 2\nsession 3\ngate 3\nsession 4\ngate 4\nsession 5\ngate 5\nsession 6\ngate 6\nsession 7\nsession 8\nsession 9\ngate 9\nsession 10')" ] || fail "$name: old gate ran or fresh request was lost"
sed -n 9p "$dir/prompts" | grep -q 'full gate' && fail "$name: returned card got an old gate note"

name="once with a blocked labelled card reads no labels"
aside_fixture
before 1 'jq '\''(.items[] | select(.content.number == 1) | .labels)=["needs-joel"]'\'' items.json > items.new; mv items.new items.json'
ONCE=--once run_case "$name" 0 'blocked needs-joel question' RUN_ITEMS=items.json LABEL_REPLY=items
sessions "$name" 1
[ ! -e "$dir/labels.calls" ] || fail "$name: --once read labels"

name="once with only a labelled card ends without idle polling"
supervisor_repo
mkdir poll-bin
printf '#!/bin/sh\necho "$*" >> git.calls\nexit 99\n' > poll-bin/git
chmod +x poll-bin/git
card 1 | jq '{items: [. + {labels:["needs-joel"]}]}' > initial.json
ITEMS="$dir/initial.json" ONCE=--once run_case "$name" 0 '' RUN_ITEMS=items.json RUN_SYNC=1 RUN_OPEN=false PATH="$dir/poll-bin:$work/sleep:$PATH"
sessions "$name" 0
[ ! -e "$dir/labels.calls" ] && [ ! -e "$dir/sleeps" ] && [ ! -e "$dir/git.calls" ] || fail "$name: --once polled, fetched main or read labels"

name="Joel count includes labelled cards and excludes skipped cards and cards outside the lane"
aside_fixture
printf '{"items":[%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s]}' \
 "$(card 1)" "$(card 2)" "$(card 3)" "$(card 4)" "$(card 5)" "$(card 6)" \
 "$(card 7)" "$(card 8)" "$(ops_card 9)" "$(card 10)" "$(card 11)" "$(card 12)" \
 | jq '(.items[] | select(.content.number == 2 or .content.number == 4 or .content.number == 5) | .labels)=["needs-joel"]
 | (.items[] | select(.content.number == 3 or .content.number == 4 or .content.number == 6) | .content.body)="## Dependencies\nhttps://github.com/labcast/labcast-server/issues/9"
 | .items[6].phase="Phase 3" | .items[7].assignees=[] | .items[9].status="Merged"
 | .items[10].kind="Idea" | .items[11].workstream="Server"
 | (.items[] | select(.content.number >= 7) | .labels)=["needs-joel"]' > initial.json
case_number=$((case_number + 1))
jq -r --arg lane studio --arg mode joel --arg skip '5,6' -f "$backlog" initial.json > out
[ "$(cat out)" = 2 ] || fail "$name: wrong Joel count"
jq -r --arg lane studio --arg mode next --arg skip '5,6' -f "$backlog" initial.json > out
[ "$(cat out)" = $'#1\tT1\tcard 1' ] || fail "$name: legacy next changed"
jq -r --arg lane studio --arg mode status --arg skip '5,6' -f "$backlog" initial.json > out
[ "$(cat out)" = "$(printf "open 6, ready 1, waiting for Joel's answer 3\nready now:\n  #1\tT1\tcard 1\ncards outside the run of this lane that block open cards:\n  #9 Task DevOps Phase 1 joeldsouzax blocks 3: card 9")" ] || fail "$name: legacy status changed"
[ ! -s "$RUN_TEST_GH_SENTINEL" ] || fail "$name: default gh was reached"
# Dependencies do not keep an idle run alive. Set-aside deadlines keep their
# existing priority.
name="a twice-failed dependency ends the run with its parked-card notice"
aside_fixture
jq '(.items[] | select(.content.number == 2) | .content.body)="## Dependencies\nhttps://github.com/labcast/labcast-server/issues/1"' "$work/items.json" > initial.json
ITEMS="$dir/initial.json" run_case "$name" 1 'failed first|failed second' RUN_ITEMS=items.json \
 RUN_RETRY_WAIT=600 RUN_NOW="bash $work/now.sh" DAY_END=12000 PATH="$work/sleep:$PATH"
sessions "$name" 2; order 1 1
expect "$name" 'parked #1: failed second'
[ "$(tail -n 1 "$dir/notified")" = 'studio: ended: no other card is ready' ] || fail "$name: old end notice changed"
grep -q $'\tended: no other card is ready$' "$dir/.cache/run.history" || fail "$name: missing end history"
[ ! -e "$dir/sleeps" ] || fail "$name: a parked dependency kept polling"

for reason in 'not in the project' 'that is not a Task' 'without an assignee' 'assigned to someone else' 'outside the selected phases'; do
  name="a dependency $reason does not keep an idle run alive"
  aside_fixture
  printf '{"items":[%s,%s]}' "$(ops_card 1)" "$(card 2)" \
    | jq '(.items[1].content.body)="## Dependencies\nhttps://github.com/labcast/labcast-server/issues/1"' > initial.json
  case "$reason" in
    'not in the project') change='del(.items[0])' ;;
    'that is not a Task') change='.items[0].kind="Idea"' ;;
    'without an assignee') change='del(.items[0].assignees)' ;;
    'assigned to someone else') change='.items[0].assignees=["someone-else"]' ;;
    'outside the selected phases') change='.items[0].phase="Phase 3"' ;;
  esac
  jq "$change" initial.json > initial.new; mv initial.new initial.json
  ITEMS="$dir/initial.json" run_case "$name" 0 '' RUN_ITEMS=items.json \
    RUN_RETRY_WAIT=600 DAY_END=12000 PATH="$work/sleep:$PATH"
  sessions "$name" 0
  [ "$(cat "$dir/notified")" = 'studio: ended: no ready card' ] || fail "$name: old no-ready notice changed"
  [ ! -e "$dir/sleeps" ] || fail "$name: an ineligible dependency kept polling"
  jq '(.items[] | select(.content.number == 2) | .labels)=["needs-joel"]' initial.json > labelled.json
  jq -r --arg lane studio --arg mode joel --arg skip '' -f "$backlog" labelled.json > counts
  [ "$(cat counts)" = 1 ] || fail "$name: missing dependency changed the Joel count"
done

name="two unmet dependencies without Joel end the idle run"
aside_fixture
printf '{"items":[%s,%s,%s]}' "$(ops_card 1)" "$(ops_card 2)" "$(card 3)" \
 | jq '.items[1].phase="Phase 3" | .items[2].content.body="## Dependencies\nhttps://github.com/labcast/labcast-server/issues/1\nhttps://github.com/labcast/labcast-server/issues/2"' > initial.json
ITEMS="$dir/initial.json" run_case "$name" 0 '' RUN_ITEMS=items.json DAY_END=12000 RUN_RETRY_WAIT=600 PATH="$work/sleep:$PATH"
sessions "$name" 0
[ "$(cat "$dir/notified")" = 'studio: ended: no ready card' ] || fail "$name: dependency-only cards kept the run alive"
[ ! -e "$dir/sleeps" ] || fail "$name: the run polled an ineligible dependency"

name="a merged dependency and an unmet dependency without Joel end the idle run"
aside_fixture
printf '{"items":[%s,%s,%s]}' "$(ops_card 1)" "$(ops_card 2)" "$(card 3)" \
 | jq '.items[1].status="Merged" | .items[2].content.body="## Dependencies\nhttps://github.com/labcast/labcast-server/issues/1\nhttps://github.com/labcast/labcast-server/issues/2"' > initial.json
ITEMS="$dir/initial.json" run_case "$name" 0 '' RUN_ITEMS=items.json RUN_RETRY_WAIT=600 DAY_END=12000 PATH="$work/sleep:$PATH"
sessions "$name" 0
[ "$(cat "$dir/notified")" = 'studio: ended: no ready card' ] || fail "$name: no-ready notice changed"
[ ! -e "$dir/sleeps" ] || fail "$name: dependency caused a wait"

name="a set-aside dependency waits to its deadline before its dependent card runs"
aside_fixture
jq '(.items[] | select(.content.number == 2) | .content.body)="## Dependencies\nhttps://github.com/labcast/labcast-server/issues/1"' "$work/items.json" > initial.json
before 2 'jq '\''(.items[] | select(.content.number == 1) | .status)="Merged"'\'' items.json > items.new; mv items.new items.json'
before 3 'jq '\''(.items[] | select(.content.number == 2) | .status)="Merged"'\'' items.json > items.new; mv items.new items.json'
ITEMS="$dir/initial.json" run_case "$name" 0 'later 1 dependency pending|merged dependency|merged dependent' \
 RUN_ITEMS=items.json RUN_OPEN=false RUN_CLOCK=60 RUN_RETRY_WAIT=30 RUN_NOW="bash $work/now.sh" PATH="$work/sleep:$PATH"
sessions "$name" 3; order 1 1 2
[ "$(cat "$dir/session.times")" = "$(printf '0\n60\n60')" ] || fail "$name: dependent ran before the deadline or merge"
[ "$(cat "$dir/sleeps")" = "$(printf '30\n30')" ] || fail "$name: set-aside polling changed"
[ "$(cat "$dir/sleep.status")" = "$(printf '1 cards set aside; next end at 60\n1 cards set aside; next end at 60')" ] || fail "$name: blocker wait displaced set-aside status"
[ "$(cat "$dir/notified")" = 'studio: ended: no ready card' ] || fail "$name: a dependency wait was announced during a set-aside wait"
name="a two-link dependency chain through a parked card ends as main"
aside_fixture
jq '.items[1].content.body="## Dependencies\nhttps://github.com/labcast/labcast-server/issues/1"
 | .items[2].content.body="## Dependencies\nhttps://github.com/labcast/labcast-server/issues/2"' initial.json > initial.new; mv initial.new initial.json
ITEMS="$dir/initial.json" run_case "$name" 1 'failed first|failed second' RUN_ITEMS=items.json \
 RUN_RETRY_WAIT=600 RUN_NOW="bash $work/now.sh" DAY_END=12000 PATH="$work/sleep:$PATH"
sessions "$name" 2; order 1 1
[ "$(tail -n 1 "$dir/notified")" = 'studio: ended: no other card is ready' ] || fail "$name: main's parked-card end notice changed"
[ ! -e "$dir/sleeps" ] || fail "$name: dependency chain kept polling"

name="two cards that depend on each other end with main's no-ready notice"
aside_fixture
jq '.items[0].content.body="## Dependencies\nhttps://github.com/labcast/labcast-server/issues/2"
 | .items[1].content.body="## Dependencies\nhttps://github.com/labcast/labcast-server/issues/1"' "$work/items.json" > initial.json
ITEMS="$dir/initial.json" run_case "$name" 0 '' RUN_ITEMS=items.json RUN_RETRY_WAIT=600 DAY_END=12000 PATH="$work/sleep:$PATH"
sessions "$name" 0
[ "$(cat "$dir/notified")" = 'studio: ended: no ready card' ] || fail "$name: main's no-ready notice changed"
[ ! -e "$dir/sleeps" ] || fail "$name: dependency cycle kept polling"

name="an unread gate result is discarded before a held card requests another gate"
supervisor_repo
cp "$work/items.json" initial.json
jq '(.items[] | select(.content.number == 2) | .status)="Merged"' initial.json > ready.json
before 3 'jq '\''(.items[] | select(.content.number == 1) | .labels)=["needs-joel"]'\'' items.json > items.new; mv items.new items.json'
before 4 'jq '\''(.items[] | select(.content.number == 2) | .status)="Merged"'\'' items.json > items.new; mv items.new items.json'
ITEMS="$dir/initial.json" SUPERVISOR=architecture/run.sh run_case "$name" 1 'gate|-|-|merged other|gatecode 0:gate|blocked done' \
 RUN_ITEMS=items.json GATE_CODE=1 LABEL_REPLY=items POLL_READY_AT=1 PATH="$work/sleep:$PATH"
sessions "$name" 6; order 1 1 1 2 1 1
[ "$(cat "$dir/events")" = "$(printf 'session 1\ngate 1\nsession 2\nsession 3\nsession 4\nsession 5\ngate 5\nsession 6')" ] || fail "$name: fresh gate missing or old gate ran"
for line in 2 3; do
 sed -n "${line}p" "$dir/prompts" | grep -q 'The run ran the full gate.*it failed\.' || fail "$name: unread result was not retained"
done
sed -n 5p "$dir/prompts" | grep -q 'full gate' && fail "$name: returning lead got an old gate note"
sed -n 6p "$dir/prompts" | grep -q 'The run ran the full gate.*it passed\.' || fail "$name: fresh result was not passed to the lead"
[ "$(wc -l < "$dir/starts")" -eq 1 ] || fail "$name: supervisor restarted"

name="a held card's fresh pass resets waits even on its last passed commit"
supervisor_repo
cp "$work/items.json" initial.json
jq '(.items[] | select(.content.number == 2) | .status)="Merged"' initial.json > ready.json
before 3 'jq '\''(.items[] | select(.content.number == 1) | .labels)=["needs-joel"]'\'' items.json > items.new; mv items.new items.json'
before 4 'jq '\''(.items[] | select(.content.number == 2) | .status)="Merged"'\'' items.json > items.new; mv items.new items.json'
ITEMS="$dir/initial.json" SUPERVISOR=architecture/run.sh run_case "$name" 1 \
 'gate|-|-|merged other|gate|waiting 0 first|waiting 0 second|waiting 0 third|waiting 0 fourth|waiting 0 fifth|waiting 0 sixth|blocked done' \
 RUN_ITEMS=items.json LABEL_REPLY=items POLL_READY_AT=1 PATH="$work/sleep:$PATH"
sessions "$name" 12; order 1 1 1 2 1 1 1 1 1 1 1 1
[ "$(wc -l < "$dir/gate.status")" -eq 2 ] || fail "$name: old request ran or fresh gate was lost"
[ "$(sort -u "$dir/gate.commits" | wc -l)" -eq 1 ] || fail "$name: the returning pass was on another commit"
expect "$name" 'parked #1: blocked done$'
! grep -q 'parked #1: waited six times:' "$dir/out" || fail "$name: fresh pass failed to reset waits"
sed -n 5p "$dir/prompts" | grep -q 'full gate' && fail "$name: returning lead got an old result"
sed -n 6p "$dir/prompts" | grep -q 'The run ran the full gate.*it passed\.' || fail "$name: returning request was not gated"

# End dependency-only end cases.
# End label-held cards and backlog polling.

# Parked cards return only after their named blocker becomes done.
name="returning #1 and #79 keeps #11 and #179 parked"
dir=$work/case$((case_number + 1)); mkdir -p "$dir"
printf '{"items":[%s,%s,%s,%s,%s,%s,%s]}' \
  "$(card 11)" "$(card 1)" "$(card 179)" "$(card 79)" "$(card 9)" "$(card 501)" "$(card 502)" \
  | jq '.items |= (to_entries | map(.value + {sequence:.key}))
        | (.items[] | select(.content.number >= 501) | .kind)="Milestone"' > "$dir/initial.json"
before 5 'jq '\''(.items[] | select(.content.number == 501) | .status)="Merged"'\'' items.json > items.new; mv items.new items.json'
ITEMS="$dir/initial.json" run_case "$name" 1 \
  'blocked #502 pending|blocked #501 old|blocked #502 pending|blocked #501 old|blocked other|blocked fresh|blocked fresh' RUN_ITEMS=items.json
sessions "$name" 7; order 11 1 179 79 9 1 79
[ "$(sed -n '/^run: parked in this run:$/,$ { /^run:/d; p; }' "$dir/out")" = "$(printf '%s\n' \
  '  #11 T11 card 11: blocked #502 pending' \
  '  #179 T179 card 179: blocked #502 pending' '  #9 T9 card 9: blocked other' \
  '  #1 T1 card 1: blocked fresh' '  #79 T79 card 79: blocked fresh')" ] || fail "$name: summary removed another card or kept an old park"

name="a return removes the whole literal park text"
before 2 'jq '\''(.items[1].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
before 3 'jq '\''(.items[0].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
run_case "$name" 1 'blocked #2 * ? [x] \ tail \n z|blocked other|merged returned' RUN_ITEMS=items.json
sessions "$name" 3; order 1 2 1
[ "$(sed -n '/^run: parked in this run:$/,$ { /^run:/d; p; }' "$dir/out")" = "$(printf '%s\n' \
  '  #2 T2 card 2: blocked other')" ] || fail "$name: summary kept part of the returned park"

for done in merged linux-owed; do
  name="a parked card returns when its blocker becomes $done"
  if [ "$done" = merged ]; then
    before 2 'jq '\''(.items[1].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
  else
    before 2 'jq '\''(.items[1].labels)=["linux-owed"]'\'' items.json > items.new; mv items.new items.json'
  fi
  before 4 'jq '\''(.items[] | select(.content.number == 1) | .status)="Merged"'\'' items.json > items.new; mv items.new items.json'
  run_case "$name" 1 'blocked #2 x|blocked other|planned|merged returned' RUN_ITEMS=items.json
  sessions "$name" 4; order 1 2 1 1
  message='returned #1: #2 is merged'
  [ "$done" != linux-owed ] || message='returned #1: #2 only owes its Linux check'
  [ "$(grep -Fc "$message" "$dir/out")" -eq 1 ] || fail "$name: return was not printed once"
  [ "$(grep -Fc "$message" "$dir/.cache/run.history")" -eq 1 ] || fail "$name: return was not noted once"
  ! grep -q '^  #1 ' "$dir/out" || fail "$name: returned card stayed in the summary"
  grep -q '^  #2 T2 card 2: blocked other$' "$dir/out" || fail "$name: another park left the summary"
  [ "$(grep -c 'studio: parked #1:' "$dir/notified")" -eq 1 ] || fail "$name: park notice changed"
done

name="a parked card stays parked while its blocker is pending"
run_case "$name" 1 'blocked #2 x|blocked other' RUN_ITEMS=items.json
sessions "$name" 2; order 1 2
! grep -q 'returned #' "$dir/out" || fail "$name: pending blocker returned a card"

name="a parked card stays parked when its blocker leaves the board"
before 2 'jq '\''.items |= map(select(.content.number != 2))'\'' items.json > items.new; mv items.new items.json'
run_case "$name" 1 'blocked #2 x|blocked other|blocked again' RUN_ITEMS=items.json
sessions "$name" 2; order 1 2
! grep -q 'returned #' "$dir/out" || fail "$name: absent blocker returned a card"

for done in merged linux-owed; do
  name="a blocker already $done at the park cannot return the card"
  dir=$work/case$((case_number + 1)); mkdir -p "$dir"
  jq --arg done "$done" '(.items[1]) |= (if $done == "merged" then .status="Merged" else .labels=["linux-owed"] end)' "$work/items.json" > "$dir/initial.json"
  ITEMS="$dir/initial.json" run_case "$name" 1 'blocked #2 x|blocked other' RUN_ITEMS=items.json
  if [ "$done" = merged ]; then sessions "$name" 1; else sessions "$name" 2; fi
  ! grep -q 'returned #' "$dir/out" || fail "$name: already done blocker returned a card"
done

name="a returned card parked again behind its done blocker stays parked"
before 2 'jq '\''(.items[1].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
run_case "$name" 1 'blocked #2 x|blocked other|blocked #2 again' RUN_ITEMS=items.json
sessions "$name" 3; order 1 2 1
[ "$(grep -c 'returned #1:' "$dir/out")" -eq 1 ] || fail "$name: card returned more than once"
grep -q '^  #1 T1 card 1: blocked #2 again$' "$dir/out" || fail "$name: new park missing from summary"

for reason in '#2: waits for the API' '#2, waits' '#2.'; do
  name="a park with reason $reason returns the card"
  before 2 'jq '\''(.items[1].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
  run_case "$name" 1 "blocked $reason|blocked other|blocked done" RUN_ITEMS=items.json
  sessions "$name" 3; order 1 2 1
  expect "$name" 'returned #1: #2 is merged'
  expect "$name" 'parked #1: blocked done$'
done

for reason in 'no card' 'no #2 x' '#2x' '2 x' '#1 x' '#001 x' '#9 x'; do
  name="a park with reason $reason cannot return the card"
  before 2 'jq '\''(.items[].status)="Merged" | .items += [{content:{number:9},status:"Merged"}]'\'' items.json > items.new; mv items.new items.json'
  run_case "$name" 1 "blocked $reason|blocked other" RUN_ITEMS=items.json
  sessions "$name" 2; order 1 2
  ! grep -q 'returned #' "$dir/out" || fail "$name: ineligible reason returned a card"
  grep -q '^  #1 ' "$dir/out" || fail "$name: ineligible park left the summary"
done

name="a blocker merged during the lead still uses the last board read at the park"
before 1 'jq '\''(.items[1].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
before 2 'jq '\''(.items[0].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
run_case "$name" 0 'blocked #2 x|merged returned' RUN_ITEMS=items.json
sessions "$name" 2; order 1 1
expect "$name" 'returned #1: #2 is merged'
! grep -q '^  #1 ' "$dir/out" || fail "$name: returned card stayed in the summary"

name="an unreadable board returns no parked card"
before 2 'echo invalid > items.json'
run_case "$name" 2 'blocked #2 x|blocked other' RUN_ITEMS=items.json RUN_RETRIES=1
sessions "$name" 2
! grep -q 'returned #' "$dir/out" || fail "$name: unreadable board returned a card"
grep -q '^  #1 ' "$dir/out" || fail "$name: unreadable board changed the summary"
expect "$name" 'GitHub could not be read 1 times in a row'

name="a parked blocker survives the supervisor restart"
supervisor_repo
printf '{"items":[%s,%s,%s]}' "$(card 1)" "$(card 2)" "$(card 3)" > initial.json
# The replacement sees the merge only after it has read the saved memory.
printf '#!%s\n' "$BASH" > replacement
echo 'jq '\''(.items[1].status)="Merged"'\'' items.json > items.new; mv items.new items.json' >> replacement
cat next.sh >> replacement; mv replacement next.sh
before 3 'jq '\''(.items[0].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
ITEMS="$dir/initial.json" SUPERVISOR=architecture/run.sh run_case "$name" 1 \
  'blocked #2 x|main:blocked other|merged returned|blocked third' RUN_ITEMS=items.json RUN_SYNC=1
sessions "$name" 4; order 1 2 1 3
restarted_once
expect "$name" 'returned #1: #2 is merged'
! grep -q '^  #1 ' "$dir/out" || fail "$name: returned card stayed in the summary after restart"

name="older supervisor state without parked blockers still loads"
supervisor_repo
mkdir -p .cache
printf 'parked=2\nparked_lines="  restored summary\\n"\n' > "$dir/.cache/old"
SUPERVISOR=architecture/run.sh run_case "$name" 1 'main:planned|blocked same' RUN_ITEMS=items.json RUN_STATE=.cache/old RUN_SYNC=1
sessions "$name" 2; order 1 1
restarted_once
! grep -q 'unbound variable' "$dir/out" || fail "$name: old state lost a default"
expect "$name" '^  restored summary$'

name="a returned card can fail once with fresh counts"
before 3 'jq '\''(.items[1].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
run_case "$name" 1 'failed one|blocked #2 x|blocked other|failed two|blocked done' RUN_ITEMS=items.json
sessions "$name" 5; order 1 1 2 1 1
! grep -q 'parked #1: failed two' "$dir/out" || fail "$name: pre-park failure counted after return"
expect "$name" 'parked #1: blocked done$'

name="a returned card can plan once with fresh counts"
before 3 'jq '\''(.items[1].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
run_case "$name" 1 'planned|blocked #2 x|blocked other|planned|blocked done' RUN_ITEMS=items.json
sessions "$name" 5; order 1 1 2 1 1
! grep -q 'parked #1: planned twice' "$dir/out" || fail "$name: pre-park plan counted after return"
expect "$name" 'parked #1: blocked done$'

name="a returned last card starts with no session counts"
dir=$work/case$((case_number + 1)); mkdir -p "$dir/.cache"
printf 'saved_counts[1]="6 1 2 planned"\nlast=$\x27#1\tT1\tcard 1\x27\nwaits=6\nrepeats=1\ndisk_fails=2\nlast_result=planned\n' > "$dir/.cache/counts"
before 1 'jq '\''(.items[1].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
run_case "$name" 1 'blocked #2 x|planned|failed once|nospace:failed once|waiting 0 once|blocked done' \
  RUN_ITEMS=items.json RUN_STATE=.cache/counts
sessions "$name" 6; order 1 1 1 1 1 1
[ "$(grep -c 'studio: parked #1:' "$dir/notified")" -eq 2 ] || fail "$name: old counts parked the returned card"
expect "$name" 'parked #1: blocked done$'

name="a returned card's fresh pass resets waits after only merges of main"
supervisor_repo
git switch -qc feat/step-1; git commit --allow-empty -qm step
before 3 'jq '\''(.items[1].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
SUPERVISOR=architecture/run.sh run_case "$name" 1 \
  'gate|blocked #2 x|blocked other|waiting 0 before|ahead:merge:gate|waiting 0 first|waiting 0 second|waiting 0 third|waiting 0 fourth|waiting 0 fifth|waiting 0 sixth|blocked done' RUN_ITEMS=items.json
sessions "$name" 12; order 1 1 2 1 1 1 1 1 1 1 1 1
[ "$(wc -l < "$dir/gate.status")" -eq 2 ] || fail "$name: fresh gate was lost"
git merge-base --is-ancestor "$(cat "$dir/request.1")" "$(cat "$dir/request.5")" || fail "$name: fresh request lost the passed ancestor"
[ -z "$(git rev-list --no-merges "$(cat "$dir/request.1")..$(cat "$dir/request.5")" --not origin/main)" ] || fail "$name: fresh request added a lead commit"
[ "$(git rev-list --count --merges "$(cat "$dir/request.1")..$(cat "$dir/request.5")")" -eq 1 ] || fail "$name: fresh request added no merge of main"
sed -n 4p "$dir/prompts" | grep -q 'full gate' && fail "$name: returning lead got an old gate note"
sed -n 6p "$dir/prompts" | grep -q 'The run ran the full gate.*it passed\.' || fail "$name: fresh request did not pass"
expect "$name" 'parked #1: blocked done$'
! grep -q 'parked #1: waited six times:' "$dir/out" || fail "$name: pre-park pass prevented the reset of waits"

name="a card that was never parked still parks at its second failure"
run_case "$name" 1 'failed one|failed two|blocked other' RUN_ITEMS=items.json
sessions "$name" 3; order 1 1 2
expect "$name" 'parked #1: failed two$'

name="a return keeps the failure count of another card"
dir=$work/case$((case_number + 1)); mkdir -p "$dir"
printf '{"items":[%s,%s,%s]}' "$(card 1)" "$(card 2)" "$(card 9)" \
  | jq '(.items[2].kind)="Milestone"' > "$dir/initial.json"
before 2 'jq '\''(.items[2].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
ITEMS="$dir/initial.json" run_case "$name" 1 'blocked #9 x|failed one|blocked done|failed two' RUN_ITEMS=items.json
sessions "$name" 4; order 1 2 1 2
expect "$name" 'parked #2: failed two$'
# End parked blocker cases.

SET_LABEL='jq '\''(.items[] | select(.content.number == 1) | .labels)=["ready-again"]'\'' items.json > items.new; mv items.new items.json'
EDIT_CALL='issue edit 1 -R labcast/labcast-server --remove-label ready-again'

name="the label ready-again returns a parked card with fresh counts"
before 3 "$SET_LABEL"
run_case "$name" 1 'failed one|failed two|blocked other|blocked done' RUN_ITEMS=items.json
sessions "$name" 4; order 1 1 2 1
[ "$(grep -Fxc 'run: returned #1: the label ready-again' "$dir/out")" -eq 1 ] || fail "$name: return was not printed once"
[ "$(grep -Fc 'returned #1: the label ready-again' "$dir/.cache/run.history")" -eq 1 ] || fail "$name: return was not noted once"
[ "$(cat "$dir/edit.calls")" = "$EDIT_CALL" ] || fail "$name: wrong label calls: $(cat "$dir/edit.calls")"
! grep -q '^  #1 T1 card 1: failed two$' "$dir/out" || fail "$name: returned card stayed in the summary"
expect "$name" '^run: parked #1: blocked done$'

name="a failed label removal keeps the card parked"
before 3 "$SET_LABEL"
run_case "$name" 1 'failed one|failed two|blocked other' RUN_ITEMS=items.json EDIT_FAIL=1
sessions "$name" 3; order 1 1 2
! grep -q 'returned #' "$dir/out" || fail "$name: card returned without the removal"
[ "$(grep -Fxc 'run: no return of #1 by the label ready-again: the label could not be removed' "$dir/out")" -ge 1 ] || fail "$name: no kept line"
! grep -q 'no return of #' "$dir/.cache/run.history" || fail "$name: the log line reached the history"
[ "$(sort -u "$dir/edit.calls")" = "$EDIT_CALL" ] || fail "$name: wrong label calls: $(sort -u "$dir/edit.calls")"
grep -Fxq '  #1 T1 card 1: failed two' "$dir/out" || fail "$name: parked card left the summary"

name="the label ready-again on a card that is not parked is not touched"
before 2 'jq '\''.items[1].labels=["ready-again"] | .items[1].status="Merged"'\'' items.json > items.new; mv items.new items.json'
run_case "$name" 1 'blocked one|merged pr 2' RUN_ITEMS=items.json
sessions "$name" 2; order 1 2
[ ! -e "$dir/edit.calls" ] || fail "$name: label removed on a card that is not parked: $(cat "$dir/edit.calls")"
! grep -q 'returned #' "$dir/out" || fail "$name: unparked card returned"

name="the label ready-again returns a card parked behind a blocker once"
before 2 "$SET_LABEL"
before 3 'jq '\''(.items[1].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
run_case "$name" 1 'blocked #2 x|blocked other|blocked done' RUN_ITEMS=items.json
sessions "$name" 3; order 1 2 1
[ "$(grep -c 'returned #1:' "$dir/out")" -eq 1 ] || fail "$name: card returned more than once"
expect "$name" '^run: returned #1: the label ready-again$'
[ "$(cat "$dir/edit.calls")" = "$EDIT_CALL" ] || fail "$name: wrong label calls: $(cat "$dir/edit.calls")"

name="a failed label removal does not hold back the return behind a done blocker"
before 2 'jq '\''(.items[0].labels)=["ready-again"] | (.items[1].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
run_case "$name" 1 'blocked #2 x|blocked other|blocked done' RUN_ITEMS=items.json EDIT_FAIL=1
sessions "$name" 3; order 1 2 1
expect "$name" '^run: no return of #1 by the label ready-again: the label could not be removed$'
expect "$name" '^run: returned #1: #2 is merged$'
[ "$(grep -Fc 'returned #1: #2 is merged' "$dir/.cache/run.history")" -eq 1 ] || fail "$name: the blocker return was not noted once"
! grep -q 'returned #1: the label' "$dir/out" || fail "$name: card returned by the label although the removal failed"

name="a card returned by the label fails once with fresh counts"
before 3 "$SET_LABEL"
run_case "$name" 1 'failed one|failed two|blocked other|failed three|blocked done' RUN_ITEMS=items.json
sessions "$name" 5; order 1 1 2 1 1
! grep -q 'parked #1: failed three' "$dir/out" || fail "$name: old failures counted after the return"
expect "$name" '^run: parked #1: blocked done$'

name="a label return removes the whole literal park text of a plain park"
before 3 "$SET_LABEL"
run_case "$name" 1 'failed one|failed two \n z|blocked other|blocked done' RUN_ITEMS=items.json
sessions "$name" 4; order 1 1 2 1
[ "$(sed -n '/^run: parked in this run:$/,$ { /^run:/d; p; }' "$dir/out")" = "$(printf '%s\n' \
  '  #2 T2 card 2: blocked other' '  #1 T1 card 1: blocked done')" ] || fail "$name: summary kept part of the returned park"

name="a label return clears the park line that an older supervisor saved"
dir=$work/case$((case_number + 1)); mkdir -p "$dir/.cache"
printf 'parked=1,2\nparked_lines="  #1 T1 card 1: failed two\\n  #2 T2 card 2: failed two\\n"\n' > "$dir/.cache/old"
jq '(.items[0].labels)=["ready-again"]' "$work/items.json" > "$dir/initial.json"
ITEMS="$dir/initial.json" run_case "$name" 1 'blocked done' RUN_ITEMS=items.json RUN_STATE=.cache/old
sessions "$name" 1; order 1
expect "$name" '^run: returned #1: the label ready-again$'
[ "$(sed -n '/^run: parked in this run:$/,$ { /^run:/d; p; }' "$dir/out")" = "$(printf '%s\n' \
  '  #2 T2 card 2: failed two' '  #1 T1 card 1: blocked done')" ] || fail "$name: summary kept the old park of #1 or lost #2"

name="a label return survives a failed selection read in the same pass"
dir=$work/case$((case_number + 1)); mkdir -p "$dir/.cache" "$dir/jq-bin"
real_jq=$(command -v jq)
printf '#!/bin/sh\nn=$(($(cat jq.next 2>/dev/null || echo 0) + 1)); echo "$n" > jq.next\n[ "$n" != "$FAIL_CHOICE_AT" ] || exit 99\nexec "%s" "$@"\n' "$real_jq" > "$dir/jq-bin/jq"
chmod +x "$dir/jq-bin/jq"
printf 'parked=1\nparked_lines="  #1 T1 card 1: failed two\\n"\n' > "$dir/.cache/old"
jq '(.items[0].labels)=["ready-again"]' "$work/items.json" > "$dir/initial.json"
ITEMS="$dir/initial.json" run_case "$name" 1 'blocked done|blocked other' RUN_ITEMS=items.json RUN_STATE=.cache/old FAIL_CHOICE_AT=3 RUN_RETRIES=5 \
  PATH="$dir/jq-bin:$work/sleep:$PATH"
sessions "$name" 2; order 1 2
expect "$name" '^run: GitHub could not be read; next try in 0 seconds$'
[ "$(grep -Fc 'returned #1: the label ready-again' "$dir/out")" -eq 1 ] || fail "$name: card not returned once"
[ "$(cat "$dir/edit.calls")" = "$EDIT_CALL" ] || fail "$name: wrong label calls: $(cat "$dir/edit.calls")"

summary() { sed -n '/^run: parked in this run:$/,$ { /^run:/d; p; }' "$dir/out"; }
three_cards() { printf '{"items":[%s,%s,%s]}' "$(card 1)" "$(card 2)" "$(card 3)" | jq "$1" > "$dir/initial.json"; }

name="a label return keeps an older park whose reason names the card"
dir=$work/case$((case_number + 1)); mkdir -p "$dir/.cache"
printf 'parked=2,1\nparked_lines="  #2 T2 card 2: failed waiting on  #1 to finish\\n  #1 T1 card 1: failed build\\n"\n' > "$dir/.cache/old"
jq '(.items[0].labels)=["ready-again"]' "$work/items.json" > "$dir/initial.json"
ITEMS="$dir/initial.json" run_case "$name" 1 'blocked done' RUN_ITEMS=items.json RUN_STATE=.cache/old
sessions "$name" 1; order 1
expect "$name" '^run: returned #1: the label ready-again$'
[ "$(summary)" = "$(printf '%s\n' '  #2 T2 card 2: failed waiting on  #1 to finish' '  #1 T1 card 1: blocked done')" ] \
  || fail "$name: summary changed the park of #2: $(summary)"

name="a label return keeps a park that quotes the park line of the card"
dir=$work/case$((case_number + 1)); mkdir -p "$dir"
three_cards '.items[0].sequence=3 | .items[1].sequence=1 | .items[2].sequence=2'
before 5 "$SET_LABEL"
ITEMS="$dir/initial.json" run_case "$name" 1 'failed one|failed two  #1 T1 card 1: failed two|blocked three|failed one|failed two|blocked done' RUN_ITEMS=items.json
sessions "$name" 6; order 2 2 3 1 1 1
expect "$name" '^run: returned #1: the label ready-again$'
[ "$(summary)" = "$(printf '%s\n' '  #2 T2 card 2: failed two  #1 T1 card 1: failed two' '  #3 T3 card 3: blocked three' '  #1 T1 card 1: blocked done')" ] \
  || fail "$name: summary changed the park of #2 or #3: $(summary)"

name="a return behind a blocker keeps a park that quotes the park line of the card"
dir=$work/case$((case_number + 1)); mkdir -p "$dir"
three_cards '.items[0].sequence=2 | .items[1].sequence=1 | .items[2].sequence=3'
before 3 'jq '\''(.items[2].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
ITEMS="$dir/initial.json" run_case "$name" 1 'blocked x  #1 T1 card 1: blocked #3 x|blocked #3 x|merged returned|blocked done' RUN_ITEMS=items.json
sessions "$name" 4; order 2 1 3 1
expect "$name" '^run: returned #1: #3 is merged$'
[ "$(summary)" = "$(printf '%s\n' '  #2 T2 card 2: blocked x  #1 T1 card 1: blocked #3 x' '  #1 T1 card 1: blocked done')" ] \
  || fail "$name: summary changed the park of #2: $(summary)"

name="a failed label removal is tried again in the next pass"
dir=$work/case$((case_number + 1)); mkdir -p "$dir"
three_cards .
before 3 "$SET_LABEL"
ITEMS="$dir/initial.json" run_case "$name" 1 'failed one|failed two|blocked other|blocked three|blocked done' RUN_ITEMS=items.json EDIT_FAIL=first
sessions "$name" 5; order 1 1 2 3 1
[ "$(cat "$dir/edit.calls")" = "$(printf '%s\n%s' "$EDIT_CALL" "$EDIT_CALL")" ] || fail "$name: wrong label calls: $(cat "$dir/edit.calls")"
[ "$(grep -Fxc 'run: no return of #1 by the label ready-again: the label could not be removed' "$dir/out")" -eq 1 ] || fail "$name: failure not logged once"
[ "$(grep -Fxc 'run: returned #1: the label ready-again' "$dir/out")" -eq 1 ] || fail "$name: card not returned once"

label_cards() { local n; for n; do printf 'jq '\''(.items[] | select(.content.number == %s) | .labels)=["ready-again"]'\'' items.json > items.new; mv items.new items.json\n' "$n"; done; }
edit_calls() { printf 'issue edit %s -R labcast/labcast-server --remove-label ready-again\n' "$@"; }

name="a label on card 12 does not return the parked cards 1 and 112"
dir=$work/case$((case_number + 1)); mkdir -p "$dir"
printf '{"items":[%s,%s,%s]}' "$(card 1)" "$(card 12)" "$(card 112)" > "$dir/initial.json"
before 3 "$(label_cards 12)"
ITEMS="$dir/initial.json" run_case "$name" 1 'blocked a|blocked a|blocked a|blocked again|blocked x|blocked y' RUN_ITEMS=items.json
sessions "$name" 4; order 1 12 112 12
[ "$(grep '^run: returned' "$dir/out")" = 'run: returned #12: the label ready-again' ] || fail "$name: wrong returns: $(grep '^run: returned' "$dir/out")"
[ "$(cat "$dir/edit.calls")" = "$(edit_calls 12)" ] || fail "$name: wrong label calls: $(cat "$dir/edit.calls")"
! grep -Eq 'github\.com|could not edit issue' "$dir/out" || fail "$name: the output of the label call reached the run output"
[ "$(summary)" = "$(printf '%s\n' '  #1 T1 card 1: blocked a' '  #112 T112 card 112: blocked a' '  #12 T12 card 12: blocked again')" ] || fail "$name: wrong summary: $(summary)"

name="a label return of card 1 keeps the saved park of card 12 that has no saved text"
dir=$work/case$((case_number + 1)); mkdir -p "$dir/.cache"
printf 'parked=12,1\nparked_lines="  #12 T12 card 12: failed two\\n  #1 T1 card 1: failed two\\n"\n' > "$dir/.cache/old"
printf '{"items":[%s,%s]}' "$(card 1)" "$(card 12)" | jq '(.items[0].labels)=["ready-again"]' > "$dir/initial.json"
ITEMS="$dir/initial.json" run_case "$name" 1 'blocked done' RUN_ITEMS=items.json RUN_STATE=.cache/old
sessions "$name" 1; order 1
expect "$name" '^run: returned #1: the label ready-again$'
[ "$(cat "$dir/edit.calls")" = "$(edit_calls 1)" ] || fail "$name: wrong label calls: $(cat "$dir/edit.calls")"
[ "$(summary)" = "$(printf '%s\n' '  #12 T12 card 12: failed two' '  #1 T1 card 1: blocked done')" ] || fail "$name: wrong summary: $(summary)"

name="two labelled parked cards are both returned in one pass"
dir=$work/case$((case_number + 1)); mkdir -p "$dir"
three_cards .
before 3 "$(label_cards 1 2)"
ITEMS="$dir/initial.json" run_case "$name" 1 'blocked a|blocked b|blocked c|blocked a2|blocked b2' RUN_ITEMS=items.json
sessions "$name" 5; order 1 2 3 1 2
[ "$(grep '^run: returned' "$dir/out")" = "$(printf '%s\n' 'run: returned #1: the label ready-again' 'run: returned #2: the label ready-again')" ] || fail "$name: wrong returns: $(grep '^run: returned' "$dir/out")"
[ "$(grep -E '^run: (returned|session on|parked #)' "$dir/out" | sed -n '7,8p')" = "$(printf '%s\n' 'run: returned #1: the label ready-again' 'run: returned #2: the label ready-again')" ] || fail "$name: the second return waited for a session"
[ "$(cat "$dir/edit.calls")" = "$(edit_calls 1 2)" ] || fail "$name: wrong label calls: $(cat "$dir/edit.calls")"
[ "$(summary)" = "$(printf '%s\n' '  #3 T3 card 3: blocked c' '  #1 T1 card 1: blocked a2' '  #2 T2 card 2: blocked b2')" ] || fail "$name: wrong summary: $(summary)"

name="a failed removal of one label does not stop the return of the next labelled card"
dir=$work/case$((case_number + 1)); mkdir -p "$dir"
three_cards .
before 3 "$(label_cards 1 2)"
ITEMS="$dir/initial.json" run_case "$name" 1 'blocked a|blocked b|blocked c|blocked b2|blocked a2' RUN_ITEMS=items.json EDIT_FAIL=first
sessions "$name" 5; order 1 2 3 2 1
[ "$(grep -E '^run: (returned|no return)' "$dir/out")" = "$(printf '%s\n' \
  'run: no return of #1 by the label ready-again: the label could not be removed' \
  'run: returned #2: the label ready-again' 'run: returned #1: the label ready-again')" ] || fail "$name: wrong returns: $(grep -E '^run: (returned|no return)' "$dir/out")"
[ "$(cat "$dir/edit.calls")" = "$(edit_calls 1 2 1)" ] || fail "$name: wrong label calls: $(cat "$dir/edit.calls")"
! grep -Eq 'github\.com|could not edit issue' "$dir/out" || fail "$name: the output of the label call reached the run output"

name="a card returned by the label is not the last session of its old park"
before 2 "$(label_cards 1)"
run_case "$name" 1 'failed one|failed two|failed three|blocked done|blocked b' RUN_ITEMS=items.json
sessions "$name" 5; order 1 1 1 1 2
! grep -q 'parked #1: failed three' "$dir/out" || fail "$name: the old failure counted with the new one"
expect "$name" '^run: parked #1: blocked done$'
[ "$(cat "$dir/edit.calls")" = "$(edit_calls 1)" ] || fail "$name: wrong label calls: $(cat "$dir/edit.calls")"

name="a card returned by the label has no passed gate request of its old park"
supervisor_repo
git switch -qc feat/step-1; git commit --allow-empty -qm step
before 3 "$(label_cards 1)"
SUPERVISOR=architecture/run.sh run_case "$name" 1 \
  'gate|blocked x|blocked other|waiting 0 before|ahead:merge:gate|waiting 0 first|waiting 0 second|waiting 0 third|waiting 0 fourth|waiting 0 fifth|waiting 0 sixth|blocked done' RUN_ITEMS=items.json
sessions "$name" 12; order 1 1 2 1 1 1 1 1 1 1 1 1
[ "$(wc -l < "$dir/gate.status" | tr -d ' ')" -eq 2 ] || fail "$name: wrong number of gates: $(cat "$dir/gate.status")"
! grep -q 'waited six times' "$dir/out" || fail "$name: the waits ran on from the old gate request"
expect "$name" '^run: parked #1: blocked done$'

name="a label on card 112 returns neither the parked card 12 nor 2 nor 1"
dir=$work/case$((case_number + 1)); mkdir -p "$dir"
printf '{"items":[%s,%s,%s,%s]}' "$(card 1)" "$(card 2)" "$(card 12)" "$(card 112)" > "$dir/initial.json"
before 4 "$(label_cards 112)"
ITEMS="$dir/initial.json" run_case "$name" 1 'blocked a|blocked a|blocked a|blocked a|blocked again|blocked x|blocked y' RUN_ITEMS=items.json
sessions "$name" 5; order 1 2 12 112 112
[ "$(grep '^run: returned' "$dir/out")" = 'run: returned #112: the label ready-again' ] || fail "$name: wrong returns: $(grep '^run: returned' "$dir/out")"
[ "$(cat "$dir/edit.calls")" = "$(edit_calls 112)" ] || fail "$name: wrong label calls: $(cat "$dir/edit.calls")"

name="only the label ready-again returns a parked card"
dir=$work/case$((case_number + 1)); mkdir -p "$dir"
three_cards '.items[0].labels=["not-ready-again-yet"] | .items[1].labels=["bug"] | .items[2].labels=["bug","ready-again"]'
ITEMS="$dir/initial.json" run_case "$name" 1 'blocked a|blocked b|blocked c|blocked c2|blocked x' RUN_ITEMS=items.json
sessions "$name" 4; order 1 2 3 3
[ "$(grep '^run: returned' "$dir/out")" = 'run: returned #3: the label ready-again' ] || fail "$name: wrong returns: $(grep '^run: returned' "$dir/out")"
[ "$(cat "$dir/edit.calls")" = "$(edit_calls 3)" ] || fail "$name: wrong label calls: $(cat "$dir/edit.calls")"

name="a label return keeps the counts of the card that was in its session"
dir=$work/case$((case_number + 1)); mkdir -p "$dir"
before 2 "$(label_cards 1)"
run_case "$name" 1 'blocked a|failed one|blocked b|failed two|blocked c' RUN_ITEMS=items.json
sessions "$name" 4; order 1 2 1 2
expect "$name" '^run: parked #2: failed two$'

name="a label return removes only the first entry of the card"
dir=$work/case$((case_number + 1)); mkdir -p "$dir"
before 3 "$(label_cards 1)"
run_case "$name" 1 'blocked done|failed a|failed x \n  #1 T1 card 1: blocked done|blocked again' RUN_ITEMS=items.json
sessions "$name" 4; order 1 2 2 1
[ "$(summary)" = "$(printf '%s\n' '  #2 T2 card 2: failed x ' '  #1 T1 card 1: blocked done' '  #1 T1 card 1: blocked again')" ] || fail "$name: wrong summary: $(summary)"

name="no label is read while no card is parked"
dir=$work/case$((case_number + 1)); mkdir -p "$dir/jq-bin"
real_jq=$(command -v jq)
printf '#!/bin/sh\necho "$*" >> jq.args\nexec "%s" "$@"\n' "$real_jq" > "$dir/jq-bin/jq"
chmod +x "$dir/jq-bin/jq"
before 1 'jq '\''(.items[].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
run_case "$name" 0 'merged pr 1' RUN_ITEMS=items.json PATH="$dir/jq-bin:$PATH"
sessions "$name" 1
[ -s "$dir/jq.args" ] || fail "$name: the jq stub saw no call"
! grep -q -- '--arg label' "$dir/jq.args" || fail "$name: the label was read with no parked card"

name="a failed read of the labels is tried again before any return"
dir=$work/case$((case_number + 1)); mkdir -p "$dir/.cache" "$dir/jq-bin"
real_jq=$(command -v jq)
printf '#!/bin/sh\nn=$(($(cat jq.next 2>/dev/null || echo 0) + 1)); echo "$n" > jq.next\n[ "$n" != "$FAIL_CHOICE_AT" ] || exit 99\nexec "%s" "$@"\n' "$real_jq" > "$dir/jq-bin/jq"
chmod +x "$dir/jq-bin/jq"
printf 'parked=1\nparked_lines="  #1 T1 card 1: failed two\\n"\n' > "$dir/.cache/old"
jq '(.items[0].labels)=["ready-again"]' "$work/items.json" > "$dir/initial.json"
ITEMS="$dir/initial.json" run_case "$name" 1 'blocked done|blocked other' RUN_ITEMS=items.json RUN_STATE=.cache/old FAIL_CHOICE_AT=1 RUN_RETRIES=5 \
  PATH="$dir/jq-bin:$work/sleep:$PATH"
sessions "$name" 2; order 1 2
[ "$(grep -Fxc 'run: GitHub could not be read; next try in 0 seconds' "$dir/out")" -eq 1 ] || fail "$name: the failed read was not reported once"
[ "$(grep -Fxc 'run: returned #1: the label ready-again' "$dir/out")" -eq 1 ] || fail "$name: card not returned once"
[ "$(cat "$dir/edit.calls")" = "$(edit_calls 1)" ] || fail "$name: wrong label calls: $(cat "$dir/edit.calls")"

name="the run ends with no parked card after the last parked card is returned by the label"
dir=$work/case$((case_number + 1)); mkdir -p "$dir"
before 1 "$(label_cards 1)
jq '(.items[1].status)=\"Merged\"' items.json > items.new; mv items.new items.json"
before 2 'jq '\''(.items[].status)="Merged"'\'' items.json > items.new; mv items.new items.json'
run_case "$name" 0 'blocked a|merged pr 1' RUN_ITEMS=items.json
sessions "$name" 2; order 1 1
expect "$name" '^run: returned #1: the label ready-again$'
expect "$name" '^run: no ready card in studio$'
! grep -q 'no other card is ready' "$dir/out" || fail "$name: the returned card still counted as parked"

echo "run-test: $((case_number + 3)) cases pass"

cd "$(dirname "$supervisor")/.."
"$BASH" "$turn_tests"
