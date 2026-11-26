#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=architecture/tests/start-fixture.sh
. "$(dirname -- "$0")/start-fixture.sh"
fixture_init
trap fixture_cleanup EXIT
fixture_copy architecture/owner/check.sh

cat > "$checkout/architecture/measure.sh" <<'STUB'
printf 'measure\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$(git -C "$(dirname "$0")" rev-parse HEAD)" >> "$FIXTURE/calls"
echo 'measure output'
exit "${MEASURE_CODE:-0}"
STUB
cat > "$checkout/architecture/owner/tools.sh" <<'STUB'
printf 'tools\t%s\t%s\t%s\n' "$1" "$2" "$3" >> "$FIXTURE/calls"
echo 'tool output'
echo "tools result ${TOOLS_CODE:-0}"
exit "${TOOLS_CODE:-0}"
STUB
fixture_commit
fixture_remote
check=$checkout/architecture/owner/check.sh

expected_where=$(where_lines "$checkout" "$checkout" "$worktrees" "$state" owner)
expect "$expected_where" "$("$BASH" "$check" --where)"
[ ! -e "$state" ]
[ ! -e "$fixture/calls" ]

check_case() {
  local code=$1 sync=$2 measure=$3 tools=$4 hours=$5 sha=$6
  shift 6
  : > "$fixture/calls"
  invoke "$code" "$BASH" "$check" "$@"
  expect "$(printf '%s\n' '== main ==' "$sync" '== measure ==' 'measure output' \
    '== tools ==' 'tool output' "tools result $tools" "check: measure $measure; tools tools result $tools")" "$actual"
  expect "$(printf 'measure\t%s\tfixture\t%s\t%s\ntools\tcheck\t%s\t%s' \
    "$worktrees" "$hours" "$sha" "$state" "$worktrees")" "$(cat "$fixture/calls")"
}

head=$(fixture_advance first)
check_case 0 "check: main is at $head" ok 0 2 "$head"
[ -n "$(find "$state" -prune -perm 700 -print)" ]
[ "$(git -C "$checkout" rev-parse HEAD)" = "$head" ]
export MEASURE_CODE=7
check_case 1 "check: main is at $head" failed 0 4 "$head" 4
unset MEASURE_CODE
export TOOLS_CODE=9
check_case 1 "check: main is at $head" ok 9 2 "$head"
unset TOOLS_CODE

git -C "$checkout" switch -qc work
new=$(fixture_advance second)
check_case 0 'check: the checkout is on work; the tools are of that tree' ok 0 2 "$head"
[ "$(git -C "$checkout" rev-parse origin/trunk)" = "$new" ]
[ "$(git -C "$checkout" branch --show-current)" = work ]
git -C "$checkout" switch -q trunk
mkdir "$checkout/untracked"
printf 'local change\n' > "$checkout/untracked/file"
printf 'tracked change\n' >> "$checkout/tracked"
check_case 0 'check: the checkout has changes; the tools are of that tree' ok 0 2 "$head"
git -C "$checkout" restore tracked
check_case 0 "check: main is at $new" ok 0 2 "$new"
expect 'local change' "$(cat "$checkout/untracked/file")"

git -C "$checkout" remote set-url origin "$fixture/missing-remote"
invoke 0 "$BASH" "$check"
expect 'check: measure ok; tools tools result 0' "$(tail -n 1 <<< "$actual")"
grep -Fxq 'check: could not fetch trunk' <<< "$actual"
echo 'owner-check-test: fetch, update, both reports and failures pass'
