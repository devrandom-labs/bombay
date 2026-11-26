#!/usr/bin/env bash
# Land an accepted harness step under the merge turn from the given clone.
# Usage: land.sh [--dry-run] <clone> <branch> <title> <body file>
# Redirect all streams when landing. Dry run prints the target and takes no turn.
# Saves a record in the harness state before it waits for the turn.
# Run from the owner checkout or a clone under its state/work/. For an
# independent checkout and a custom state folder, pass HARNESS_STATE.
# Integrates main in the clone, writes gate logs beside it, pushes the branch,
# opens the pull request and squash-merges it. A conflict stops the landing.
# Exit 1 if main moves during both gate attempts, before any push.
# Close an open PR and delete its remote branch to undo an unmerged landing.
# Revert a merged landing with a reviewed PR. Gate logs can be removed locally.
# GH_TOKEN, when set, supplies credentials. Otherwise gh uses its active login.
set -euo pipefail
unset CDPATH

absolute_directory() (
  local directory=$1
  [[ $directory == /* ]] || directory=$PWD/$directory
  cd -P -- "$directory" && printf '%s.' "$PWD"
)

dry_run=0
if [ "${1:-}" = --dry-run ]; then dry_run=1; shift; fi
usage() {
  echo 'usage: land.sh [--dry-run] <clone> <branch> <title> <body file>' >&2
  exit 2
}
if [ "$#" -ne 4 ] || [ ! -d "$1" ] || [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ]; then
  usage
fi

# The script starts itself again from inside the clone: keep its own path absolute.
case $0 in
  */*) script_dir=${0%/*}; script_dir=${script_dir:-/} ;;
  *) script_dir=. ;;
esac
here=$(absolute_directory "$script_dir")
here=${here%.}
self=$here/${0##*/}
clone=$(absolute_directory "$1") branch=$2 title=$3
clone=${clone%.}
case $4 in
  /*) body=$4 ;;
  *) body=$PWD/$4 ;;
esac
log=${clone%/*}/gate-${branch//\//-}
# shellcheck source=architecture/project.env
. "$here/project.env"
repo=$HARNESS_REPO
url=https://github.com/$repo.git
if [ "$branch" = "$HARNESS_MAIN" ]; then
  printf 'land: %s is the main branch; land a branch\n' "$branch" >&2
  exit 2
fi
if [ "$(git check-ref-format --branch "$branch" 2>/dev/null)" != "$branch" ]; then
  echo 'land: use a valid branch name' >&2
  exit 2
fi
if [ -z "${LAND_TURN:-}" ] && { [ ! -f "$body" ] || [ ! -r "$body" ]; }; then usage; fi
if [ "$dry_run" = 1 ]; then
  printf 'land: would land %s from %s in %s as "%s" using %s\n' "$branch" "$clone" "$repo" "$title" "$body"
  exit 0
fi

architecture=$here
# shellcheck source=architecture/start-common.sh
. "$here/start-common.sh"
# shellcheck source=architecture/landing-record.sh
. "$here/landing-record.sh"
harness_state=$(landing_state "$harness_state" "$harness_here" "$harness_main")
records=$harness_state/landings
(umask 077; mkdir -p -- "$records")
record=$records/$(harness_path_hash "$branch").record
control=$records/control

write_landing() (
  local pending
  pending=$(mktemp "$records/.pending.XXXXXXXX") || return 1
  trap 'rm -f -- "$pending"' EXIT
  printf '%s\0' "$clone" "$branch" "$title" "$body" "$repo" "$HARNESS_MAIN" "$landing_begin" "$$" \
    "$started" "$command" "$turn_token" "$1" > "$pending" &&
    mv -f -- "$pending" "$record"
)

# Hold the lock only while checking and replacing the record.
claim_landing() {
  local live=0
  control_lock 8 || return 1
  if [ -z "${LAND_TURN:-}" ]; then
    if [ -e "$record" ]; then
      read_landing "$record" || return 1
      landing_live || live=$?
      case $live in
        0) printf 'land: %s already runs (process %s)\n' "$branch" "$landing_pid"; return 2 ;;
        2) printf 'land: could not check the process of %s\n' "$branch"; return 2 ;;
      esac
    fi
    landing_begin=$started
    turn_token=$(harness_path_hash "$$.$started.$RANDOM.$RANDOM") || return 1
  else
    # The turn runner passes the record to the child that does the merge.
    read_landing "$record" &&
      [ "$LAND_TURN" = "$landing_turn_token" ] &&
      [ "${RUN_TURN_OWNER:-}" = "$landing_pid" ] || return 1
    turn_token=''
  fi
  write_landing ''
}

update_owned_record() (
  control_lock 8 && read_landing "$record" &&
    [ "$landing_pid" = "$$" ] && [ "$landing_started" = "$started" ] &&
    "$@"
) 8>> "$control"

fail() {
  local status=$1 reason=$2
  reason=${reason//$'\n'/ }
  reason=${reason//$'\r'/ }
  printf 'land: %s\n' "$reason"
  if ! update_owned_record write_landing "$reason"; then
    echo 'land: could not save the landing result' >&2
  fi
  if [ "$#" = 3 ]; then tail -n 15 "$3" | cut -c1-200 || true; fi
  exit "$status"
}

started=$(process_started "$$") && [ -n "$started" ] &&
  command=$(process_command "$$") && [ -n "$command" ] || {
  echo 'land: could not read the landing process' >&2
  exit 1
}
# A trapped signal waits for a foreground Git, Nix or GitHub command to end.
# The turn runner can then release the turn without leaving that command alive.
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 131' QUIT
trap 'exit 143' TERM
claim_status=0
claim_landing 8>> "$control" || claim_status=$?
case "$claim_status" in
  0) ;;
  2) exit 1 ;;
  *) echo 'land: could not save the landing record' >&2; exit 1 ;;
esac
[ -f "$body" ] && [ -r "$body" ] || fail 2 'the pull request body is missing or unreadable'
cd -- "$clone" || fail "$?" 'could not enter the clone'
main_ref=refs/remotes/origin/$HARNESS_MAIN
pull_url=https://github.com/$repo/pull/
fields=baseRefName,headRefName,headRefOid,isCrossRepository,state,mergeCommit

valid_pull() {
  jq -e --arg base "$HARNESS_MAIN" --arg branch "$branch" --arg head "$head" --arg state "$2" \
    '.baseRefName == $base and .headRefName == $branch and .state == $state and
     .isCrossRepository == false and .headRefOid == $head' >/dev/null <<< "$1"
}

fetch() {
  git -c credential.helper='!gh auth git-credential' fetch -q --no-write-fetch-head "$url" \
    "+refs/heads/$HARNESS_MAIN:$main_ref" \
    || fail "$?" "could not fetch $HARNESS_MAIN"
  fetched=$(git rev-parse --verify "$main_ref^{commit}") || fail "$?" 'could not read the fetched commit'
}
bring_main() {
  local ancestry=0
  fetch
  main=$fetched
  git merge-base --is-ancestor "$main" HEAD || ancestry=$?
  if [ "$ancestry" = 1 ]; then
    if ! git -c commit.gpgsign=false merge -q --no-edit "$main"; then
      git merge --abort || true
      fail 1 'main does not merge cleanly; resolve, prove and review the merge first'
    fi
    short=$(git rev-parse --short HEAD) || fail "$?" 'could not read the branch head'
    echo "land: brought main $main in; head is $short"
  elif [ "$ancestry" != 0 ]; then
    fail "$ancestry" 'could not compare main and the branch head'
  fi
}

finish_landing() {
  local merged result
  merged=$(jq -r '.mergeCommit.oid // empty' <<< "$1")
  fetch
  if ! [[ $merged =~ ^[[:xdigit:]]{40}$ ]] || ! git merge-base --is-ancestor "$merged" "$fetched"; then
    fail 1 "could not find the merge commit on $HARNESS_MAIN"
  fi
  update_owned_record rm -f -- "$record" || fail "$?" 'could not remove the landing record'
  if result=$(git log --oneline -1 "$fetched" | cut -c1-90); then
    echo "land: merged; main is $result"
  else
    echo "land: merged; main is $fetched"
  fi
  exit 0
}

changes=$(git status --porcelain) || fail "$?" 'could not read the clone status'
[ -z "$changes" ] || fail 1 'the clone holds changes that are not committed'
current=$(git branch --show-current) || fail "$?" 'could not read the branch of the clone'
[ "$current" = "$branch" ] || fail 1 "the clone is not on $branch"

if [ -z "${LAND_TURN:-}" ]; then
  # Main first, so that the clone holds the turn command and the wait for the turn is short.
  bring_main
  grep -q 'run.sh turn <command>' architecture/run.sh \
    || fail 1 'architecture/run.sh of the clone has no turn command'
  echo "land: taking the merge turn"
  shopt -s execfail
  HARNESS_STATE=$harness_state \
    exec bash architecture/run.sh turn env "LAND_TURN=$turn_token" \
    bash "$self" "$clone" "$branch" "$title" "$body" < /dev/null \
    || fail "$?" 'could not start the merge turn'
fi

# From here the script holds the merge turn.
stable=false
for try in 1 2; do
  bring_main
  short=$(git rev-parse --short HEAD) || fail "$?" 'could not read the branch head'
  echo "land: full gate for $short, try $try"
  nix develop --no-update-lock-file -c bash architecture/gate.sh > "$log-$try.log" 2>&1 \
    || fail 1 "the full gate failed; see $log-$try.log" "$log-$try.log"
  fetch
  if [ "$fetched" = "$main" ]; then
    stable=true
    break
  fi
  echo "land: main moved during the gate although this script holds the turn"
done
"$stable" || fail 1 'main moved twice; nothing is pushed'
head=$(git rev-parse HEAD) || fail "$?" 'could not read the branch head'
STEP_BASE="origin/$HARNESS_MAIN" nix develop --no-update-lock-file -c git -c core.hooksPath=.githooks -c credential.helper='!gh auth git-credential' \
  push -q "$url" "HEAD:refs/heads/$branch" || fail "$?" "could not push $branch"
echo "land: pushed $head to $branch"
pr=$(gh pr create -R "$repo" --head "$branch" --base "$HARNESS_MAIN" --title "$title" --body-file "$body") \
  || fail "$?" 'could not open the pull request'
if [[ $pr != "$pull_url"* || ! ${pr#"$pull_url"} =~ ^[1-9][0-9]*$ ]]; then
  fail 1 'got an invalid pull request URL'
fi
echo "land: $pr"
pull=$(gh pr view "$pr" -R "$repo" --json "$fields") || fail "$?" 'could not read the pull request'
if ! valid_pull "$pull" OPEN; then
  fail 1 'the open pull request does not match the branch, base, repository and gated head'
fi
fetch
[ "$fetched" = "$main" ] || fail 2 'main moved after the push; the pull request stays open and is not merged'
merge_status=0
gh pr merge "$pr" -R "$repo" --squash --match-head-commit "$head" || merge_status=$?
pull=$(gh pr view "$pr" -R "$repo" --json "$fields") || fail "$?" 'could not read the merged pull request'
if ! valid_pull "$pull" MERGED; then
  [ "$merge_status" = 0 ] || fail "$merge_status" 'could not merge the pull request'
  fail 1 "could not find the merge commit on $HARNESS_MAIN"
fi
finish_landing "$pull"
