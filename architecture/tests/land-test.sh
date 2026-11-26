#!/usr/bin/env bash
# Run from the repository root. An optional script path supports guard tests.
set -euo pipefail

architecture=$PWD/architecture
script=${1:-$architecture/land.sh}
supervisor=$architecture/run.sh
work=$(mktemp -d)
work=$(cd -- "$work" && pwd -P)
export HOME=$work/home XDG_STATE_HOME=$work/xdg
mkdir -p "$HOME" "$XDG_STATE_HOME"
holder='' landing=''
cleanup() {
  local pid
  if [ -n "${CALLS:-}" ] && [ -f "$CALLS.merge.ready" ]; then touch "$CALLS.merge.release"; fi
  for pid in "$landing" "$holder"; do
    [ -n "$pid" ] || continue
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  rm -rf -- "$work"
}
trap cleanup EXIT
mkdir "$work/bin"
export REAL_GIT FIXTURE_REPO TITLE='Harness fixture' BRANCH=feat/fixture
REAL_GIT=$(command -v git)
export GIT_ALLOW_PROTOCOL=file GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TERMINAL_PROMPT=0
export GIT_AUTHOR_NAME=Fixture GIT_AUTHOR_EMAIL=fixture@example.invalid
export GIT_COMMITTER_NAME=$GIT_AUTHOR_NAME GIT_COMMITTER_EMAIL=$GIT_AUTHOR_EMAIL
export RUN_TURN_POLL=0.05
unset GH_TOKEN LAND_TURN HARNESS_REPO HARNESS_MAIN RUN_TURN_OWNER RUN_TURN_RESUME RUN_TURN_PS
unset PS_COMMAND PS_DENIED
tree=$work/tools/architecture
mkdir -p "$tree/owner"
"$REAL_GIT" -C "${tree%/*}" init -q
cp "$architecture/"{project.env,start-common.sh,landing-record.sh} "$tree/"
cp "$architecture/owner/"where.sh "$tree/owner/"
cp "$script" "$tree/land.sh"
script=$tree/land.sh
# shellcheck source=architecture/landing-record.sh
. "$tree/landing-record.sh"
export LAND_TEST_PS=''
if [ "${LAND_TEST_NATIVE:-0}" = 1 ]; then
  LAND_TEST_PS=$(command -v ps)
  export RUN_TURN_PS=$LAND_TEST_PS
else
  export RUN_TURN_PS=false
fi

printf '#!%s\n' "$BASH" > "$work/bin/ps"
cat >> "$work/bin/ps" <<'STUB'
set -euo pipefail
args=("$@")
if [ "$1" = -ww ]; then shift; fi
[ "$#" = 4 ] && [ "$1" = -p ] && [ "$3" = -o ] || exit 1
pid=$2
[ "$pid" != "${PS_DENIED:-}" ] || exit 1
if [ -n "$LAND_TEST_PS" ]; then exec "$LAND_TEST_PS" "${args[@]}"; fi
kill -0 "$pid" 2>/dev/null || exit 1
case "$4" in
  stat=) printf '%s\n' S ;;
  lstart=) printf '%s\n' 'Fri Nov 20 10:00:00 2026' ;;
  command=) printf '%s\n' "${PS_COMMAND:-bash architecture/land.sh}" ;;
  *) exit 1 ;;
esac
STUB

printf '#!%s\n' "$BASH" > "$work/bin/land-pause"
cat >> "$work/bin/land-pause" <<'STUB'
set -euo pipefail
touch "$1.ready"
end=$((SECONDS + 20))
while [ ! -e "$1.release" ]; do
  if [ "$SECONDS" -ge "$end" ]; then
    printf 'land-test: timed out at %s\n' "$1" >&2
    exit 1
  fi
  sleep 0.05
done
STUB

printf '#!%s\n' "$BASH" > "$work/bin/git"
cat >> "$work/bin/git" <<'STUB'
set -euo pipefail
case " $* " in
  *' fetch '*)
    refspec="+refs/heads/$FIXTURE_MAIN:refs/remotes/origin/$FIXTURE_MAIN"
    [ "$*" = "-c credential.helper=!gh auth git-credential fetch -q --no-write-fetch-head https://github.com/$FIXTURE_REPO.git $refspec" ]
    printf 'fetch\n' >> "$CALLS"
    if [ "$CASE" = fetch-fail ]; then exit 7; fi
    if [[ $CASE == stale-* ]]; then
      records=("$HARNESS_STATE"/landings/*.record)
      record=${records[0]}
      index=0
      while IFS= read -r -d '' field; do
        index=$((index + 1))
        case $CASE:$index in
          stale-pid:8) field=$$ ;;
          stale-start:9) field='Fri Nov 20 10:00:01 2026' ;;
        esac
        printf '%s\0' "$field"
      done < "$record" > "$CALLS.replacement"
      cp "$CALLS.replacement" "$record"
      exit 7
    fi
    if [ "$CASE" = live ] && [ ! -e "$CALLS.fetch.ready" ]; then land-pause "$CALLS.fetch"; fi
    "$REAL_GIT" fetch -q --no-write-fetch-head "$REMOTE" "$refspec"
    if [ "$CASE" = off-main ] && [ -e "$CALLS.pr-merged" ]; then
      "$REAL_GIT" fetch -q "$REMOTE" "$BRANCH"
    fi ;;
  *' push '*)
    [ "$*" = "-c core.hooksPath=.githooks -c credential.helper=!gh auth git-credential push -q https://github.com/$FIXTURE_REPO.git HEAD:refs/heads/$BRANCH" ]
    [ "${RUN_TURN_OWNER:-}" = "$(cat "$RUN_TURN/pid")" ]
    [ "$STEP_BASE" = "origin/$FIXTURE_MAIN" ]
    printf 'push\n' >> "$CALLS"
    if [ "$CASE" = push-fail ]; then exit 8; fi
    exec "$REAL_GIT" push -q "$REMOTE" "HEAD:refs/heads/$BRANCH" ;;
  *) exec "$REAL_GIT" "$@" ;;
esac
STUB
printf '#!%s\n' "$BASH" > "$work/bin/nix"
cat >> "$work/bin/nix" <<'STUB'
set -eu
[ "$1 $2 $3" = 'develop --no-update-lock-file -c' ]
shift 3
case "$1" in
  bash) [ "$*" = 'bash architecture/gate.sh' ] ;;
  git) [ "$#" -eq 9 ] ;;
  *) exit 98 ;;
esac
exec "$@"
STUB
printf '#!%s\n' "$BASH" > "$work/bin/advance"
cat >> "$work/bin/advance" <<'STUB'
set -eu
"$REAL_GIT" -C "$REMOTE" commit -qm 'Moved main' --allow-empty
STUB
printf '#!%s\n' "$BASH" > "$work/bin/gh"
cat >> "$work/bin/gh" <<'STUB'
set -euo pipefail
[ "${GH_TOKEN:-unset}" = "$FIXTURE_TOKEN" ]
pr=https://github.com/$FIXTURE_REPO/pull/42
fields=baseRefName,headRefName,headRefOid,isCrossRepository,state,mergeCommit
[ "${RUN_TURN_OWNER:-}" = "$(cat "$RUN_TURN/pid")" ]
pull() {
  local status=OPEN merge='' head base=$FIXTURE_MAIN branch=$BRANCH cross=false
  head=$("$REAL_GIT" -C "$REMOTE" rev-parse "$BRANCH")
  if [ -e "$CALLS.pr-merged" ]; then
    status=MERGED
    merge=$(cat "$CALLS.pr-merged")
  fi
  case $CASE in
    wrong-base) base=release ;;
    wrong-branch) branch=feat/another ;;
    wrong-head) head=$("$REAL_GIT" -C "$REMOTE" rev-parse "$FIXTURE_MAIN") ;;
    wrong-fork) cross=true ;;
    wrong-state) status=CLOSED ;;
    open-after-merge) status=OPEN ;;
  esac
  jq -cn --arg base "$base" --arg branch "$branch" --arg head "$head" --argjson cross "$cross" \
    --arg status "$status" --arg merge "$merge" '{baseRefName:$base, headRefName:$branch,
      headRefOid:$head, isCrossRepository:$cross, state:$status, mergeCommit:{oid:$merge}}'
}
case "$1 $2" in
  'pr create')
    [ "$#" -eq 12 ]
    [ "$*" = "pr create -R $FIXTURE_REPO --head $BRANCH --base $FIXTURE_MAIN --title $TITLE --body-file $BODY" ]
    [ "$(cat "$BODY")" = 'Fixture body' ]
    [ ! -e "$CALLS.pr-open" ]
    printf 'create\n' >> "$CALLS"
    if [ "$CASE" = create-fail ]; then exit 8; fi
    touch "$CALLS.pr-open"
    if [ "$CASE" = after-push ]; then advance; fi
    case $CASE in
      invalid-pr-host) echo '42' ;;
      invalid-pr-url) echo "$pr/details" ;;
      *) echo "$pr" ;;
    esac ;;
  'pr merge')
    [ "$*" = "pr merge $pr -R $FIXTURE_REPO --squash --match-head-commit $("$REAL_GIT" rev-parse HEAD)" ]
    [ "$8" = "$("$REAL_GIT" -C "$REMOTE" rev-parse "$BRANCH")" ]
    printf 'merge\n' >> "$CALLS"
    if [ "$CASE" = merge-fail ]; then exit 8; fi
    if [ "$CASE" = interrupted-merge ]; then land-pause "$CALLS.merge"; fi
    if [ "$CASE" != queued ]; then
      if [ "$CASE" != off-main ]; then
        "$REAL_GIT" -C "$REMOTE" merge -q --squash "$BRANCH" > /dev/null
        "$REAL_GIT" -C "$REMOTE" commit -qm 'Squashed fixture'
        "$REAL_GIT" -C "$REMOTE" rev-parse "$FIXTURE_MAIN" > "$CALLS.pr-merged"
      else
        "$REAL_GIT" -C "$REMOTE" rev-parse "$BRANCH" > "$CALLS.pr-merged"
      fi
    fi
    if [ "$CASE" = merged-after-error ]; then exit 8; fi ;;
  'pr view')
    [ "$*" = "pr view $pr -R $FIXTURE_REPO --json $fields" ]
    [ -e "$CALLS.pr-open" ]
    if [ "$CASE" = view-open-fail ]; then exit 8; fi
    if [ "$CASE" = view-merged-fail ] && [ -e "$CALLS.pr-merged" ]; then exit 8; fi
    pull ;;
  *) printf 'unexpected gh: %s\n' "$*" >> "$CALLS"; exit 99 ;;
esac
STUB
cat > "$work/gate.sh" <<'STUB'
set -euo pipefail
[ "${RUN_TURN_OWNER:-}" = "$(cat "$RUN_TURN/pid")" ]
[ "$(git rev-parse "origin/$FIXTURE_MAIN")" = "$("$REAL_GIT" -C "$REMOTE" rev-parse "$FIXTURE_MAIN")" ]
git merge-base --is-ancestor "origin/$FIXTURE_MAIN" HEAD
printf 'gate\n' >> "$CALLS"
git rev-parse "origin/$FIXTURE_MAIN" >> "$BASES"
if [ "$CASE" = live ]; then land-pause "$CALLS.gate"; fi
if [ "$CASE" = gate-fail ]; then echo 'fixture gate failed'; exit 5; fi
echo 'fixture gate passed'
# The remote moves on each of the first three gates in the busy fixture.
tries=$(wc -l < "$BASES")
if { [ "$CASE" = moves-three ] && [ "$tries" -le 3 ]; } ||
   { [ "$CASE" = moves ] && [ "$tries" -eq 1 ]; }; then advance; fi
STUB
chmod +x "$work/bin/"*
export PATH="$work/bin:$PATH"

setup() {
  export CASE=$1 FIXTURE_TOKEN=unset FIXTURE_REPO=devrandom-labs/bombay
  export FIXTURE_MAIN=${2:-main}
  local dir=$work/$CASE
  export REMOTE="$dir/remote" BODY="$dir/PR body" CALLS="$dir/calls" BASES="$dir/bases" RUN_TURN="$dir/turn.lock"
  export HARNESS_STATE="$dir/state"
  clone="$dir/step clone"
  out="$dir/out"
  mkdir -p "$REMOTE/architecture"
  cp "$supervisor" "$REMOTE/architecture/run.sh"
  chmod -R u+w "$REMOTE/architecture/run.sh"
  if [ "$CASE" = live ] && [ -z "$LAND_TEST_PS" ]; then
    mv "$REMOTE/architecture/run.sh" "$REMOTE/architecture/supervisor.sh"
    cat > "$REMOTE/architecture/run.sh" <<'STUB'
# run.sh turn <command>
printf '%s\n' "bash $0 $*" > "$CALLS.turn-command"
exec bash architecture/supervisor.sh "$@"
STUB
  fi
  if [ "$CASE" = old-run ]; then : > "$REMOTE/architecture/run.sh"; fi
  cp "$work/gate.sh" "$REMOTE/architecture/gate.sh"
  if [ "$CASE" = conflict ]; then echo base > "$REMOTE/overlap"; fi
  "$REAL_GIT" -C "$REMOTE" init -q -b "$FIXTURE_MAIN"
  "$REAL_GIT" -C "$REMOTE" add .
  "$REAL_GIT" -C "$REMOTE" commit -qm 'Base'
  "$REAL_GIT" clone -q "$REMOTE" "$clone"
  "$REAL_GIT" -C "$clone" switch -qc "$BRANCH"
  echo step > "$clone/step"
  if [ "$CASE" = conflict ]; then echo step > "$clone/overlap"; fi
  "$REAL_GIT" -C "$clone" add .
  "$REAL_GIT" -C "$clone" commit -qm 'Step'
  if [ "$CASE" = conflict ]; then
    echo main > "$REMOTE/overlap"
    "$REAL_GIT" -C "$REMOTE" commit -qam 'Overlapping main'
  fi
  echo 'Fixture body' > "$BODY"
  : > "$CALLS"
  : > "$BASES"
  start_head=$("$REAL_GIT" -C "$clone" rev-parse HEAD)
  start_short=$("$REAL_GIT" -C "$clone" rev-parse --short HEAD)
}

land() {
  local expected=$1 result=0
  shift
  bash "$script" "$@" </dev/null > "$out" 2>&1 || result=$?
  if [ "$result" != "$expected" ]; then
    printf 'land-test: %s exited %s, expected %s\n' "$CASE" "$result" "$expected" >&2
    cat "$out" "$CALLS" >&2
    return 1
  fi
  [ ! -e "$RUN_TURN" ]
}
run_landing() {
  land "$1" "$clone" "$BRANCH" "$TITLE" "$BODY"
}
no_push() {
  if "$REAL_GIT" -C "$REMOTE" show-ref --verify --quiet "refs/heads/$BRANCH" ||
     grep -Eq '^(push|create|merge)$' "$CALLS"; then
    echo "land-test: $CASE wrote to the remote" >&2
    return 1
  fi
}
calls() { printf '%s\n' "$@" | diff -u - "$CALLS"; }
gate_log() { printf '%s\n' "$2" | diff -u - "${clone%/*}/gate-feat-fixture-$1.log"; }
result_line() { "$REAL_GIT" -C "$REMOTE" log --oneline -1 "$FIXTURE_MAIN" | cut -c1-90; }

setup clean
run_landing 0
diff -u - "$out" <<EXPECTED
land: taking the merge turn
land: full gate for $start_short, try 1
land: pushed $start_head to $BRANCH
land: https://github.com/$FIXTURE_REPO/pull/42
land: merged; main is $(result_line)
EXPECTED
calls fetch fetch gate fetch push create fetch merge fetch
gate_log 1 'fixture gate passed'
[ "$("$REAL_GIT" -C "$REMOTE" show main:step)" = step ]
[ "$("$REAL_GIT" -C "$REMOTE" rev-list --count main)" -eq 2 ]

setup moves trunk
export HARNESS_MAIN=trunk HARNESS_REPO=example/other FIXTURE_REPO=example/other FIXTURE_TOKEN=fixture-token
GH_TOKEN=fixture-token run_landing 0
head=$("$REAL_GIT" -C "$clone" rev-parse HEAD)
short=$("$REAL_GIT" -C "$clone" rev-parse --short HEAD)
base=$(sed -n '2p' "$BASES")
diff -u - "$out" <<EXPECTED
land: taking the merge turn
land: full gate for $start_short, try 1
land: main moved during the gate although this script holds the turn
land: brought main $base in; head is $short
land: full gate for $short, try 2
land: pushed $head to $BRANCH
land: https://github.com/$FIXTURE_REPO/pull/42
land: merged; main is $(result_line)
EXPECTED
calls fetch fetch gate fetch fetch gate fetch push create fetch merge fetch
gate_log 1 'fixture gate passed'
gate_log 2 'fixture gate passed'
"$REAL_GIT" -C "$REMOTE" merge-base --is-ancestor "$base" "$FIXTURE_MAIN"
unset HARNESS_REPO HARNESS_MAIN

setup conflict
run_landing 1
diff -u - "$out" <<'EXPECTED'
Auto-merging overlap
CONFLICT (content): Merge conflict in overlap
Automatic merge failed; fix conflicts and then commit the result.
land: main does not merge cleanly; resolve, prove and review the merge first
EXPECTED
calls fetch
no_push
[ "$("$REAL_GIT" -C "$clone" rev-parse HEAD)" = "$start_head" ]
[ -z "$("$REAL_GIT" -C "$clone" status --porcelain)" ]

setup gate-fail
run_landing 1
diff -u - "$out" <<EXPECTED
land: taking the merge turn
land: full gate for $start_short, try 1
land: the full gate failed; see ${clone%/*}/gate-feat-fixture-1.log
fixture gate failed
EXPECTED
calls fetch fetch gate
gate_log 1 'fixture gate failed'
no_push
records=("$HARNESS_STATE"/landings/*.record)
read_landing "${records[0]}"
diff -u <(printf 'the full gate failed; see %s/gate-feat-fixture-1.log\n' "${clone%/*}") \
  <(printf '%s\n' "$landing_reason")

setup moves-three
run_landing 1
base=$(sed -n '2p' "$BASES")
short=$("$REAL_GIT" -C "$clone" rev-parse --short HEAD)
diff -u - "$out" <<EXPECTED
land: taking the merge turn
land: full gate for $start_short, try 1
land: main moved during the gate although this script holds the turn
land: brought main $base in; head is $short
land: full gate for $short, try 2
land: main moved during the gate although this script holds the turn
land: main moved twice; nothing is pushed
EXPECTED
calls fetch fetch gate fetch fetch gate fetch
no_push

setup after-push
run_landing 2
diff -u - "$out" <<EXPECTED
land: taking the merge turn
land: full gate for $start_short, try 1
land: pushed $start_head to $BRANCH
land: https://github.com/$FIXTURE_REPO/pull/42
land: main moved after the push; the pull request stays open and is not merged
EXPECTED
calls fetch fetch gate fetch push create fetch
[ "$("$REAL_GIT" -C "$REMOTE" rev-parse "$BRANCH")" = "$start_head" ]
[ "$("$REAL_GIT" -C "$REMOTE" log -1 --format=%s main)" = 'Moved main' ]

setup old-run
run_landing 1
diff -u <(echo 'land: architecture/run.sh of the clone has no turn command') "$out"
calls fetch
no_push

setup input
land 2 "$clone" main "$TITLE" "$BODY"
diff -u <(echo 'land: main is the main branch; land a branch') "$out"
HARNESS_MAIN=trunk land 2 --dry-run "$clone" trunk "$TITLE" "$BODY"
diff -u <(echo 'land: trunk is the main branch; land a branch') "$out"
land 0 --dry-run "$clone" "$BRANCH" "$TITLE" "$BODY"
diff -u <(printf 'land: would land %s from %s in %s as "%s" using %s\n' "$BRANCH" "$(cd "$clone" && pwd)" "$FIXTURE_REPO" "$TITLE" "$(cd "${BODY%/*}" && pwd)/${BODY##*/}") "$out"
[ ! -e "$HARNESS_STATE" ]
land 1 "$clone" feat/other "$TITLE" "$BODY"
diff -u <(echo 'land: the clone is not on feat/other') "$out"
echo dirty >> "$clone/step"
run_landing 1
diff -u <(echo 'land: the clone holds changes that are not committed') "$out"
bad_input() {
  land 2 "$@"
  diff -u <(echo 'usage: land.sh [--dry-run] <clone> <branch> <title> <body file>') "$out"
}
bad_input
bad_input "$clone" "$BRANCH" "$TITLE"
bad_input "$clone" "$BRANCH" "$TITLE" "$BODY" extra
bad_input "$work/missing" "$BRANCH" "$TITLE" "$BODY"
bad_input "$clone" '' "$TITLE" "$BODY"
bad_input "$clone" "$BRANCH" '' "$BODY"
bad_input "$clone" "$BRANCH" "$TITLE" "$work/missing"
bad_input "$clone" "$BRANCH" "$TITLE" "$work/input"
cp "$BODY" "$work/unreadable"
chmod u-r "$work/unreadable"
if [ ! -r "$work/unreadable" ]; then
  bad_input "$clone" "$BRANCH" "$TITLE" "$work/unreadable"
fi
land 2 "$clone" $'feat/bad\nbranch' "$TITLE" "$BODY"
diff -u <(echo 'land: use a valid branch name') "$out"
mv -- "$clone" "$work/input/-"
(cd "$work/input" && land 0 --dry-run - "$BRANCH" "$TITLE" "$BODY")
diff -u <(printf 'land: would land %s from %s in %s as "%s" using %s\n' \
  "$BRANCH" "$work/input/-" "$FIXTURE_REPO" "$TITLE" "$BODY") "$out"
[ ! -s "$CALLS" ]
echo 'land-test: landing, main movement, conflict, gate failure, dry run and bad input pass'

wait_for() {
  local pattern=$1 pid=$2 log=$3 until=$((SECONDS + 15)) path
  until [ "$SECONDS" -ge "$until" ]; do
    for path in "${pattern%/*}"/${pattern##*/}; do
      if [ -e "$path" ]; then return 0; fi
    done
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.05
  done
  printf 'land-test: did not see %s\n' "$pattern" >&2
  cat "$log" >&2
  return 1
}

duplicate() {
  local expected=$1 status=0
  shift
  env CASE=fetch-fail RUN_TURN="$work/duplicate.lock" "$@" \
    bash "$script" "$clone" "$BRANCH" "$TITLE" "$BODY" </dev/null > "$work/duplicate" 2>&1 || status=$?
  diff -u <(printf '%s\n' "$expected") "$work/duplicate"
  [ "$status" = 1 ]
  cmp "$work/original-record" "$record"
}

setup live
TITLE=$'An owner\'s title\twith a tab'
bash "$supervisor" turn land-pause "$CALLS.turn" </dev/null > "$work/holder" 2>&1 &
holder=$!
wait_for "$CALLS.turn.ready" "$holder" "$work/holder"
bash "$script" "$clone" "$BRANCH" "$TITLE" "$BODY" </dev/null > "$out" 2>&1 &
landing=$!
wait_for "$CALLS.fetch.ready" "$landing" "$out"
records=("$HARNESS_STATE"/landings/*.record)
[ "${#records[@]}" = 1 ]
record=${records[0]}
read_landing "$record"
diff -u <(printf '%s\n' "$clone" "$BRANCH" "$TITLE" "$BODY" "$FIXTURE_REPO" "$FIXTURE_MAIN" "$landing" '') \
  <(printf '%s\n' "$landing_clone" "$landing_branch" "$landing_title" "$landing_body" "$landing_repo" "$landing_main" "$landing_pid" "$landing_reason")
[ -n "$landing_started" ]
[ "$landing_begin" = "$landing_started" ]
[ -n "$landing_command" ]
[ -n "$landing_turn_token" ]
began=$landing_begin
token=$landing_turn_token
cp "$record" "$work/original-record"
duplicate "land: $BRANCH already runs (process $landing)"
touch "$CALLS.fetch.release"
wait_for "$RUN_TURN.wait/*.$landing.*" "$landing" "$out"
if [ -z "$LAND_TEST_PS" ]; then
  PS_COMMAND=$(cat "$CALLS.turn-command")
  export PS_COMMAND
fi
duplicate "land: $BRANCH already runs (process $landing)"
duplicate "land: could not check the process of $BRANCH" PS_DENIED="$landing"
duplicate 'land: could not save the landing record' LAND_TURN=wrong RUN_TURN_OWNER="$landing"
duplicate 'land: could not save the landing record' LAND_TURN="$token" RUN_TURN_OWNER="$$"
unset PS_COMMAND
calls fetch

touch "$CALLS.turn.release"
wait "$holder"
holder=''
wait_for "$CALLS.gate.ready" "$landing" "$out"
read_landing "$record"
[ "$landing_pid" != "$landing" ]
kill -0 "$landing_pid"
[ "$landing_begin" = "$began" ]
[ -n "$landing_started" ]
[ -n "$landing_command" ]
[ -z "$landing_turn_token" ]
[ -z "$landing_reason" ]
cp "$record" "$work/original-record"
duplicate "land: $BRANCH already runs (process $landing_pid)"
touch "$CALLS.gate.release"
wait "$landing"
landing=''
[ ! -e "$record" ]
[ "$("$REAL_GIT" -C "$REMOTE" show main:step)" = step ]
TITLE='Harness fixture'

for failed in fetch-fail push-fail create-fail view-open-fail merge-fail view-merged-fail off-main queued open-after-merge; do
  setup "$failed"
  case "$failed" in
    fetch-fail) result=7; reason='could not fetch main' ;;
    push-fail) result=8; reason="could not push $BRANCH" ;;
    create-fail) result=8; reason='could not open the pull request' ;;
    view-open-fail) result=8; reason='could not read the pull request' ;;
    merge-fail) result=8; reason='could not merge the pull request' ;;
    view-merged-fail) result=8; reason='could not read the merged pull request' ;;
    off-main|queued|open-after-merge) result=1; reason='could not find the merge commit on main' ;;
  esac
  run_landing "$result"
  records=("$HARNESS_STATE"/landings/*.record)
  read_landing "${records[0]}"
  diff -u <(printf '%s\n' "$reason") <(printf '%s\n' "$landing_reason")
  if [ "$failed" = off-main ]; then
    # The reported commit exists locally and in FETCH_HEAD, off the configured main.
    [ "$("$REAL_GIT" -C "$clone" rev-parse FETCH_HEAD)" = "$start_head" ]
    ancestry=0
    "$REAL_GIT" -C "$clone" merge-base --is-ancestor "$start_head" "origin/$FIXTURE_MAIN" || ancestry=$?
    [ "$ancestry" = 1 ]
  fi
done

setup merged-after-error
run_landing 0
[ -z "$(find "$HARNESS_STATE/landings" -name '*.record' -print)" ]
[ "$("$REAL_GIT" -C "$REMOTE" show main:step)" = step ]

setup interrupted-merge
bash "$script" "$clone" "$BRANCH" "$TITLE" "$BODY" </dev/null > "$out" 2>&1 &
landing=$!
wait_for "$CALLS.merge.ready" "$landing" "$out"
records=("$HARNESS_STATE"/landings/*.record)
read_landing "${records[0]}"
[ "$landing_pid" != "$landing" ]
kill -TERM "$landing_pid"
sleep 0.5
if [ ! -d "$RUN_TURN" ] || ! kill -0 "$landing_pid"; then
  echo 'land-test: the turn ended while the merge command was still running' >&2
  exit 1
fi
touch "$CALLS.merge.release"
result=0
wait "$landing" || result=$?
landing=''
[ "$result" = 143 ]
[ ! -e "$RUN_TURN" ]
read_landing "${records[0]}"
[ -z "$landing_reason" ]

for stale in stale-pid stale-start; do
  setup "$stale"
  run_landing 7
  diff -u "$out" - <<'EXPECTED'
land: could not fetch main
land: could not save the landing result
EXPECTED
  for record in "$HARNESS_STATE"/landings/*.record; do cmp "$CALLS.replacement" "$record"; done
done

for invalid in invalid-pr-host invalid-pr-url wrong-base wrong-branch wrong-head wrong-fork wrong-state; do
  setup "$invalid"
  run_landing 1
  case "$invalid" in
    invalid-pr-*) reason='got an invalid pull request URL' ;;
    *) reason='the open pull request does not match the branch, base, repository and gated head' ;;
  esac
  records=("$HARNESS_STATE"/landings/*.record)
  read_landing "${records[0]}"
  diff -u <(printf '%s\n' "$reason") <(printf '%s\n' "$landing_reason")
  calls fetch fetch gate fetch push create
done

setup owner-clone
unset HARNESS_STATE
architecture=$tree
# shellcheck source=architecture/owner/where.sh
. "$tree/owner/where.sh"
owner_state=$harness_state
mkdir -p "$owner_state/work/fixture"
mv "$clone" "$owner_state/work/fixture/clone"
clone=$owner_state/work/fixture/clone
cp "$tree/"{land.sh,landing-record.sh,project.env,start-common.sh} "$clone/architecture/"
mkdir "$clone/architecture/owner"
cp "$tree/owner/where.sh" "$clone/architecture/owner/"
"$REAL_GIT" -C "$clone" add architecture
"$REAL_GIT" -C "$clone" commit -qm 'Install the landing tools in the clone'
script=$clone/architecture/land.sh
HARNESS_STATE="$work/custom-state" CASE=fetch-fail run_landing 7
[ ! -d "$owner_state/landings" ]
records=("$work/custom-state"/landings/*.record)
read_landing "${records[0]}"
[ "$landing_clone" = "$clone" ]
diff -u <(echo 'could not fetch main') <(printf '%s\n' "$landing_reason")
CASE=fetch-fail run_landing 7
records=("$owner_state"/landings/*.record)
read_landing "${records[0]}"
[ "$landing_clone" = "$clone" ]
diff -u <(echo 'could not fetch main') <(printf '%s\n' "$landing_reason")

echo 'land-test: record before the turn, process handoff, duplicate refusal, saved failures and verified removal pass'
