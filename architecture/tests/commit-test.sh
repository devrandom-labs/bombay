#!/usr/bin/env bash
# Real Git and Just, with SSH's remote shell and the media tools replaced locally.
set -euo pipefail
root=$PWD
work=$(mktemp -d)
. "$root/architecture/tests/own-processes.sh"
real_sleep=$(command -v sleep)
cleanup() {
  local child deadline=$((SECONDS + 2))
  trap - EXIT
  touch "$work/release" "$work/release-test" "$work/release-generic"
  own_stop "$deadline" "$real_sleep"
  for child in $(jobs -p); do
    wait "$child" 2>/dev/null || true
  done
  # Only the directories that this test's SSH fixture allocated.
  if [ -f "$work/directories" ]; then
    while IFS= read -r directory; do rm -rf -- "$directory"; done < "$work/directories"
  fi
  rm -rf "$work"
}
trap cleanup EXIT
fail() { echo "commit-test: FAILED: $*" >&2; cat "$work"/*.log >&2; exit 1; }
wait_for() {
  local deadline=$((SECONDS + 30))
  while ! grep -q "$1" "$2" 2>/dev/null; do
    kill -0 "$3" 2>/dev/null || fail "process exited before $1"
    [ "$SECONDS" -lt "$deadline" ] || fail "timed out before $1"
    sleep 0.01
  done
}
mkdir -p "$work/bin" "$work/source with spaces/architecture" "$work/home" "$work/archive"
# The Linux sandbox has no /usr/bin/env; keep the existing recipes' Bash interpreter.
sed "s|#!/usr/bin/env bash|#!$BASH|g" "$root/justfile" > "$work/source with spaces/justfile"
cp "$root/architecture/"{gate,space}.sh "$work/source with spaces/architecture/"
chmod u+w "$work/source with spaces/architecture/"*.sh
export work GATE_LOCK=$work/lock GATE_POLL=0.01 GATE_FREE=0
export HOME=$work/home SPACE_FREE='echo 100' SPACE_CACHE=$work/none SPACE_BUILDS=$work/none
export SMELTER_IMAGE=test LABCAST_LOCAL_STUDIO_IMAGE=test
export COMMIT_TEST_GIT
COMMIT_TEST_GIT=$(command -v git)
export COMMIT_TEST_RM
COMMIT_TEST_RM=$(command -v rm)
printf '#!%s\n' "$BASH" > "$work/bin/git"
cat >> "$work/bin/git" <<'SH'
set -euo pipefail
case "$PWD" in
  /tmp/labcast-linux.*/checkout*)
    test -z "${GATE_OWNER+x}"
    GATE_OWNER="$STUDIO_TEST_OWNER" locked
    ;;
  /tmp/labcast-studio.*|/tmp/labcast-linux.*) locked ;;
esac
exec "$COMMIT_TEST_GIT" "$@"
SH
printf '#!%s\n' "$BASH" > "$work/bin/rm"
cat >> "$work/bin/rm" <<'SH'
set -euo pipefail
if [[ "$PWD" = /tmp/labcast-studio.* || "$PWD" = /tmp/labcast-linux.* ]]; then
  for argument in "$@"; do
    if [ "$argument" = checkout ]; then
      locked
      if [ "${FAIL_CHECKOUT_REMOVAL:-0}" = 1 ]; then
        echo 'fixture: checkout removal failed' >&2
        exit 28
      fi
    fi
  done
fi
if [ "${FAIL_LOCAL_REMOVAL:-0}" = 1 ]; then
  for argument in "$@"; do
    if [[ "$argument" = "$work/local/"* ]] && [ -f "$argument/source.bundle" ]; then
      printf '%s\n' "$argument" >> "$work/directories"
      echo 'fixture: local bundle removal failed' >&2
      exit 29
    fi
  done
fi
exec "$COMMIT_TEST_RM" "$@"
SH
printf '#!%s\n' "$BASH" > "$work/bin/ssh"
cat >> "$work/bin/ssh" <<'SH'
set -euo pipefail
[ "$1" = -T ] && [ "$2" = labcast-desktop ] && [ "$#" = 3 ]
if [[ "$3" = 'mktemp -d /tmp/labcast-studio.XXXXXXXXXXXX' || "$3" = 'mktemp -d /tmp/labcast-linux.XXXXXXXXXXXX' ]]; then
  if [ "${FAIL_ALLOCATION:-0}" = 1 ]; then exit 24; fi
  directory=$(bash -c "$3")
  printf '%s\n' "$directory" >> "$work/directories"
  printf '%s\n' "$directory"
else
  if [ "${FAIL_TRANSFER:-0}" = 1 ] && [[ "$3" = tar\ -xf* ]]; then cat >/dev/null; exit 26; fi
  if [ "${DROP_COMMAND:-0}" = 1 ] && [[ "$3" = *STUDIO_ARTIFACTS* ]]; then exit 255; fi
  if { [ "${OTHER_OWNER:-0}" = 1 ] || [ "${EMPTY_OWNER:-0}" = 1 ]; } && [[ "$3" = *record=* ]]; then
    if [ "${EMPTY_OWNER:-0}" = 1 ]; then : > "$GATE_LOCK.cleanup"; else printf 'another-owner\n' > "$GATE_LOCK.cleanup"; fi
    trap 'rm -f "$GATE_LOCK.cleanup"' EXIT
    bash -c "$3"
    exit "$?"
  fi
  exec bash -c "$3"
fi
SH
printf '#!%s\n' "$BASH" > "$work/bin/locked"
cat >> "$work/bin/locked" <<'SH'
set -euo pipefail
test -n "${GATE_OWNER:-}"
test "$(cat "$GATE_LOCK.cleanup")" = "$GATE_OWNER"
if [ "$(uname -s)" = Linux ]; then
  if flock -n "$GATE_LOCK" true; then echo 'command bypassed host lock' >&2; exit 90; fi
else
  test -d "$GATE_LOCK"
  kill -0 "$(cat "$GATE_LOCK/pid")"
fi
SH
printf '#!%s\n' "$BASH" > "$work/bin/nix"
cat >> "$work/bin/nix" <<'SH'
set -euo pipefail
locked
if [ "$1" = develop ]; then
  test "$(git rev-parse HEAD)" = "$EXPECTED_COMMIT"
  test "$(cat value)" = "$EXPECTED_VALUE"
  test ! -e untracked
  test -z "$(git status --porcelain)"
  [ "$STUDIO_REQUIRE_GPU" = true ]
  printf 'tested %s\n' "$EXPECTED_COMMIT"
  while [ "$1" != --command ]; do shift; done
  shift
  exec "$@"
fi
case "$2" in
  .\#smelter-image) printf '%s\n' "$work/bin/image" ;;
  .\#studio) mkdir -p .cache/studio/package ;;
  .\#studio-tests) printf '%s\n' "$work/archive" ;;
  *) echo "unexpected nix command: $*" >&2; exit 91 ;;
esac
SH
printf '#!%s\n' "$BASH" > "$work/bin/cargo"
cat >> "$work/bin/cargo" <<'SH'
set -euo pipefail
locked
test "$STUDIO_ENGINE_IMAGE" = test
printf '%s\n' "$*" >> "$STUDIO_ARTIFACTS/arguments"
printf 'retained media\n' > "$STUDIO_ARTIFACTS/picture"
if [ "${TEST_FAILURE:-0}" != 0 ]; then
  echo 'FAIL fixture::native_failure'
  exit "$TEST_FAILURE"
fi
echo 'PASS fixture::native_success'
if [ "${HOLD_TEST:-0}" = 1 ]; then
  echo ready > "$work/test-ready"
  while [ ! -e "$work/release-test" ]; do sleep 0.01; done
fi
SH
printf '#!%s\n' "$BASH" > "$work/bin/docker"
cat >> "$work/bin/docker" <<'SH'
set -euo pipefail
locked
if [ "$1" = load ]; then
  cat >/dev/null
  if [ -n "${LABCAST_TEST_IMAGE_TOOL:-}" ]; then printf 'Loaded image: %s\n' "$SMELTER_IMAGE"; fi
fi
if [ "$1" = ps ]; then
  if [ "${FAIL_CLEANUP:-0}" = 1 ]; then exit 27; fi
fi
SH
printf '#!/bin/sh\nexit 0\n' > "$work/bin/image"
chmod +x "$work/bin/"*
export PATH="$work/bin:$PATH"
cd "$work/source with spaces"
git init -q -b main
git config user.name Fixture
git config user.email fixture@example.invalid
printf 'old\n' > value
git add .
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -qm old
old=$(git rev-parse HEAD)
# The selected older commit deliberately has a different gate source.
printf '\n# newer control\n' >> architecture/gate.sh
printf 'new\n' > value
git add .
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -qm new
export EXPECTED_COMMIT
EXPECTED_COMMIT=$(git rev-parse HEAD)
export EXPECTED_VALUE=new
printf 'dirty\n' > value
touch untracked

# Failed transfer must leave no remote folder, even before the locked job starts.
status=0
FAIL_TRANSFER=1 just studio-linux > "$work/transfer.log" 2>&1 || status=$?
test "$status" = 26 || fail "transfer failure status became $status"
directory=$(tail -n 1 "$work/directories")
test ! -e "$directory" || fail 'transfer failure leaked its host folder'

# The unpublished HEAD must wait and then run from a clean tree, with full evidence.
bash architecture/gate.sh bash -c 'echo ready > "$work/ready"; while [ ! -e "$work/release" ]; do sleep 0.01; done' > "$work/holder.log" 2>&1 &
holder=$!
wait_for ready "$work/ready" "$holder"
just studio-linux > "$work/default.log" 2>&1 &
runner=$!
wait_for 'gate: waiting' "$work/default.log" "$runner"
test ! -d "$HOME/.cache/labcast/studio" || fail 'test ran before its turn'
touch "$work/release"
wait "$holder"
wait "$runner" || fail 'unpublished HEAD failed'
evidence=$(sed -n 's/^studio-linux: evidence //p' "$work/default.log")
test "$(cat "$evidence/commit")" = "$EXPECTED_COMMIT" || fail 'wrong commit recorded'
test "$(cat "$evidence/status")" = 0 || fail 'success not recorded'
grep -q 'PASS fixture::native_success' "$evidence/run.log" || fail 'missing test log'
test "$(cat "$evidence/media/picture")" = 'retained media' || fail 'media lost with checkout'
test "$(wc -l < "$evidence/media/arguments")" -eq 2 || fail 'full run omitted package calibration'
grep -q 'test(complete_calibration)' "$evidence/media/arguments" || fail 'package calibration missing'

# An explicit detached commit, quoted filter and repeats reach the real archive recipe.
export EXPECTED_COMMIT=$old EXPECTED_VALUE=old
filter='binary_id(labcast-studio::program) & test("a $quoted REPEATS FILTER RUN_COMMAND name")'
just studio-linux "$filter" 3 "$old" > "$work/filter.log" 2>&1 || fail 'selected older commit failed'
evidence=$(sed -n 's/^studio-linux: evidence //p' "$work/filter.log")
grep -Fq -- "--ignore-default-filter -E $filter --stress-count 3" "$evidence/media/arguments" || fail 'filter or repeats changed'
test "$(wc -l < "$evidence/media/arguments")" -eq 1 || fail 'filtered run added calibration'

# Two calls of this recipe serialize too, including the previous owner's teardown.
HOLD_TEST=1 just studio-linux 'test(native_success)' 1 "$old" > "$work/first.log" 2>&1 &
first=$!
wait_for ready "$work/test-ready" "$first"
just studio-linux '' 2 "$old" > "$work/second.log" 2>&1 &
second=$!
wait_for 'gate: waiting' "$work/second.log" "$second"
grep -q 'tested ' "$work/second.log" && fail 'second call ran before first finished'
touch "$work/release-test"
wait "$first" || fail 'first call failed'
wait "$second" || fail 'waiting call failed'
evidence=$(sed -n 's/^studio-linux: evidence //p' "$work/second.log")
grep -q -- '--stress-count 2' "$evidence/media/arguments" || fail 'full-suite repeats lost'
test "$(wc -l < "$evidence/media/arguments")" -eq 2 || fail 'repeated full run omitted calibration'

export TEST_FAILURE=23
status=0
just studio-linux 'test(native_failure)' 1 "$old" > "$work/failure.log" 2>&1 || status=$?
test "$status" = 23 || fail "test failure status became $status"
evidence=$(sed -n 's/^studio-linux: evidence //p' "$work/failure.log")
test "$(cat "$evidence/status")" = 23 || fail 'failure status not retained'
grep -q 'FAIL fixture::native_failure' "$evidence/run.log" || fail 'failure not named'
while IFS= read -r directory; do test ! -e "$directory" || fail 'checkout not removed'; done < "$work/directories"
test "$(cat value)" = dirty && test -e untracked || fail 'source worktree changed'
test ! -e "$GATE_LOCK.cleanup" || fail 'cleanup owner record left behind'

# A failed resource teardown must retain the source of its printed recovery command.
unset TEST_FAILURE
OTHER_OWNER=1 just studio-linux 'test(native_success)' 1 "$old" > "$work/other-owner.log" 2>&1 || fail 'later owner prevented finished checkout cleanup'
status=0
EMPTY_OWNER=1 just studio-linux 'test(native_success)' 1 "$old" > "$work/unknown-owner.log" 2>&1 || status=$?
test "$status" != 0 || fail 'unknown cleanup owner accepted'
directory=$(tail -n 1 "$work/directories")
test -f "$directory/architecture/gate.sh" || fail 'unknown cleanup owner lost recovery source'
rm -rf "$directory"
status=0
FAIL_CLEANUP=1 just studio-linux 'test(native_success)' 1 "$old" > "$work/cleanup.log" 2>&1 || status=$?
test "$status" = 27 || fail "cleanup failure status became $status"
directory=$(tail -n 1 "$work/directories")
test -f "$directory/architecture/gate.sh" && test -f "$GATE_LOCK.cleanup" || fail 'pending cleanup lost recovery source'
test ! -d "$directory/checkout" || fail 'large checkout removed outside lock'
owner=$(cat "$GATE_LOCK.cleanup")
GATE_OWNER=$owner GATE_CLEANUP=1 bash "$directory/architecture/gate.sh" true > "$work/recovery.log" 2>&1 || fail 'cleanup recovery failed'
test ! -e "$GATE_LOCK.cleanup" || fail 'recovery left its owner record'
rm -rf "$directory"

# A checkout removal failure must keep recovery available and report the right status.
for outcome in '23 23' '0 28'; do
  read -r test_status expected_status <<< "$outcome"
  log="$work/checkout-removal-$test_status.log"
  status=0
  FAIL_CHECKOUT_REMOVAL=1 TEST_FAILURE="$test_status" just studio-linux 'test(checkout_cleanup)' 1 "$old" > "$log" 2>&1 || status=$?
  grep -Fxq 'fixture: checkout removal failed' "$log" || fail 'checkout removal failure was not exercised'
  test "$status" = "$expected_status" || fail "command status $test_status with failed removal returned $status, expected $expected_status"
  directory=$(tail -n 1 "$work/directories")
  grep -Fxq "studio-linux: checkout retained for recovery: $directory" "$log" || fail 'missing checkout recovery path'
  test -f "$directory/checkout/value" && test ! -e "$directory/finished" || fail 'unfinished checkout not retained'
  test ! -e "$GATE_LOCK.cleanup" || fail 'checkout removal prevented resource teardown'
done

# A disconnected SSH call has no completion proof; keep its checkout for inspection.
status=0
DROP_COMMAND=1 just studio-linux 'test(native_success)' 1 "$old" > "$work/disconnect.log" 2>&1 || status=$?
test "$status" = 255 || fail "disconnect status became $status"
directory=$(tail -n 1 "$work/directories")
test -f "$directory/architecture/gate.sh" && test ! -e "$directory/finished" || fail 'lost session removed recovery source'
echo 'commit-test: unpublished and older commits, lock, options, failure, evidence and checkout cleanup pass'

mkdir -p nix
cat > nix/test-images.sh <<'SH'
set -euo pipefail
locked
printf '%s\n' "$*" >> "$work/image-calls"
case "$1" in
  record) mkdir -p "$LABCAST_TEST_IMAGES" ;;
  clean) rmdir "$LABCAST_TEST_IMAGES" ;;
  *) exit 91 ;;
esac
SH
git add nix/test-images.sh
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -qm image-tool
gate_head=$(git rev-parse HEAD)
export EXPECTED_GATE_HASH COMMIT_TEST_NIX_CONFIG
EXPECTED_GATE_HASH=$(sha256sum architecture/gate.sh | cut -d ' ' -f 1)
COMMIT_TEST_NIX_CONFIG=${NIX_CONFIG-}
unset STUDIO_ARTIFACTS STUDIO_REQUIRE_GPU CARGO_BUILD_JOBS COMMIT_TEST_UNSET

# A standalone clone uses its committed gate and waits on the host lock.
bash architecture/gate.sh bash -c 'echo ready > "$work/generic-ready"; while [ ! -e "$work/release-generic" ]; do sleep 0.01; done' > "$work/generic-holder.log" 2>&1 &
holder=$!
wait_for ready "$work/generic-ready" "$holder"
printf '%s\n' 'echo wrong-gate; exit 97' > "$work/bad-gate"
mv "$work/bad-gate" architecture/gate.sh
caller=$(cat <<'SH'
test "$#" = 0
test -z "${GATE_OWNER+x}${STUDIO_ARTIFACTS+x}${STUDIO_REQUIRE_GPU+x}${CARGO_BUILD_JOBS+x}"
test -z "${LABCAST_TEST_IMAGE_TOOL+x}${LABCAST_TEST_IMAGES+x}"
test "${NIX_CONFIG-}" = "$COMMIT_TEST_NIX_CONFIG"
test ! -e ../test-images.sh
test ! -e ../.cache/test-images
test "$(sha256sum ../architecture/gate.sh | cut -d ' ' -f 1)" = "$EXPECTED_GATE_HASH"
test -z "$(git status --porcelain)"
git rev-parse HEAD
cat value
printf '%s\n' "literal \$HOME, quote ', and \"double quotes\""
SH
)
just linux-commit "$old" "$caller" > "$work/generic.log" 2>&1 &
runner=$!
wait_for 'gate: waiting' "$work/generic.log" "$runner"
if grep -Fxq old "$work/generic.log"; then fail 'generic caller ran before its turn'; fi
touch "$work/release-generic"
wait "$holder"
wait "$runner" || fail 'generic caller failed from the standalone clone'
directory=$(tail -n 1 "$work/directories")
grep -Fxq "linux-commit: gate HEAD $gate_head" "$work/generic.log" || fail 'missing HEAD gate source'
grep -Fxq "linux-commit: commit $old" "$work/generic.log" || fail 'missing selected commit'
grep -Fxq "linux-commit: temporary checkout $directory/checkout" "$work/generic.log" || fail 'missing generic checkout path'
grep -Fxq "$old" "$work/generic.log" && grep -Fxq old "$work/generic.log" || fail 'generic caller saw the wrong tree'
grep -Fxq $'literal $HOME, quote \', and "double quotes"' "$work/generic.log" || fail 'generic command quoting changed'
test ! -e "$directory" || fail 'successful generic caller left its checkout'
test ! -e "$HOME/.cache/labcast/studio/${directory##*/}" || fail 'generic caller wrote Studio evidence'

# With origin/main recorded locally, a broken candidate gate remains ordinary input.
git update-ref refs/remotes/origin/main "$gate_head"
git add architecture/gate.sh
git rm -q nix/test-images.sh
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -qm broken-gate
broken=$(git rev-parse HEAD)
just linux-commit "$broken" 'git rev-parse HEAD; cat architecture/gate.sh' > "$work/main-gate.log" 2>&1 || fail 'generic caller used the broken gate'
grep -Fxq "linux-commit: gate refs/remotes/origin/main $gate_head" "$work/main-gate.log" || fail 'missing origin/main gate source'
grep -Fxq "$broken" "$work/main-gate.log" || fail 'generic caller did not see the candidate commit'
grep -Fxq 'echo wrong-gate; exit 97' "$work/main-gate.log" || fail 'generic caller did not read the candidate gate'
test ! -e "$work/image-calls" || fail 'generic caller used the Studio image tool'

status=0
just linux-commit "$old" 'exit 23' > "$work/generic-exit.log" 2>&1 || status=$?
test "$status" = 23 || fail "generic exit status became $status"
directory=$(tail -n 1 "$work/directories")
grep -Fxq "linux-commit: temporary checkout $directory/checkout" "$work/generic-exit.log" || fail 'missing failed checkout path'
test ! -e "$directory" && test ! -e "$GATE_LOCK.cleanup" || fail 'generic failure left checkout or cleanup record'

for caller in 'false; true' 'false | true; true' ': "$COMMIT_TEST_UNSET"; true'; do
  status=0
  just linux-commit "$old" "$caller" > "$work/generic-options.log" 2>&1 || status=$?
  test "$status" = 1 || fail "generic Bash options returned $status for $caller"
done

# A local cleanup error must keep the command or allocation error and name the folder.
mkdir "$work/local"
for outcome in '23 0 23' '0 0 29' '0 1 24'; do
  read -r caller_status allocation_failure expected_status <<< "$outcome"
  log="$work/local-removal-$expected_status.log"
  status=0
  TMPDIR="$work/local" FAIL_LOCAL_REMOVAL=1 FAIL_ALLOCATION="$allocation_failure" \
    just linux-commit "$old" "exit $caller_status" > "$log" 2>&1 || status=$?
  test "$status" = "$expected_status" || fail "local bundle removal returned $status, expected $expected_status"
  grep -Fxq 'fixture: local bundle removal failed' "$log" || fail 'local removal failure was not exercised'
  directory=$(tail -n 1 "$work/directories")
  test -f "$directory/source.bundle" || fail 'local bundle missing after failed removal'
  grep -Fxq "linux-commit: could not remove local bundle folder: $directory" "$log" || fail 'local bundle folder missing from output'
done

# Studio still takes the working gate and image tool when both commit choices have a broken gate.
git update-ref refs/remotes/origin/main "$broken"
git show "$gate_head:architecture/gate.sh" > architecture/gate.sh
mkdir -p nix
git show "$gate_head:nix/test-images.sh" > nix/test-images.sh
EXPECTED_COMMIT=$broken EXPECTED_VALUE=new \
  just studio-linux 'test(native_success)' 1 "$broken" > "$work/studio-working-gate.log" 2>&1 || fail 'Studio did not use its working gate and tool'
grep -Fxq 'record test' "$work/image-calls" || fail 'Studio did not record its image'
test "$(tail -n 1 "$work/image-calls")" = clean || fail 'Studio did not clean its image'
directory=$(tail -n 1 "$work/directories")
test ! -e "$directory" || fail 'Studio image cleanup left the checkout'
echo 'commit-test: generic checkout, gate source, shell, status and cleanup pass'
