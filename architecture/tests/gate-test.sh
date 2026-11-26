#!/usr/bin/env bash
# Self-test of the gate lock, pass record and kept results, with a fake full gate.
# Run from the repository root. No Nix commands run; waits check process liveness.
set -euo pipefail
bash architecture/tests/own-processes-test.sh
unset GATE_OWNER GATE_CLEANUP

gate_path=$PATH
work=$(mktemp -d)
. "$PWD/architecture/tests/own-processes.sh"
own_init "$work"
trap 'rm -rf "$work"' EXIT
# Use the two scripts shipped by the Linux recipe to isolate the machine lock.
# turn-test.sh runs the checkout entry point with both locks.
cp architecture/{gate,space}.sh "$work/"
gate=$work/gate.sh
export GATE_LOCK=$work/lock GATE_POLL=1 GATE_FREE=0 GATE_PASSED=$work/passed
export GATE_AGAIN=0
export GATE_KEEP="$work/kept results"
export GATE_STORE="$work/store"
# The space script of the gate sees a scripted disk and no real cache.
export SPACE_FREE='echo 100' SPACE_CACHE=$work/none SPACE_BUILDS=$work/none
log=$work/log
fail() { echo "gate-test: FAILED: $1"; [ -f "$log" ] && cat "$log"; exit 1; }
cleanup() {
  own_cleanup
  for child in $(jobs -pr; jobs -ps); do
    kill -KILL -- "-$child" 2>/dev/null || true
    kill -KILL "$child" 2>/dev/null || true
    wait -f "$child" 2>/dev/null || true
  done
}
set -m
own_test_stopped_cleanup cleanup || fail 'stopped direct child cleanup'
set +m
echo 'gate-test: stopped direct child cleanup: pass'

mkdir "$work/bin"
real_awk=$(command -v awk)
real_sleep=$(command -v sleep)
real_rm=$(command -v rm)
export real_awk real_sleep real_rm record_read=$work/record-read
cat > "$work/bin/wait-for" <<'SH'
#!/bin/sh
while [ ! -e "$1" ]; do
  if ! kill -0 "$2" 2> /dev/null; then
    echo "gate-test: FAILED: process $2 exited before $1" >&2
    exit 1
  fi
  "$real_sleep" 0.01
done
SH
cat > "$work/bin/awk" <<'SH'
#!/bin/sh
echo read >> "$record_read"
exec "$real_awk" "$@"
SH
cat > "$work/bin/nix" <<'SH'
#!/bin/sh
if [ "$1 $2" = 'profile wipe-history' ]; then exit 0; fi
printf '%s\t' "$@" >> "$calls"
printf '\n' >> "$calls"
case "$1 $2" in
  'flake check')
    printf 'run\n' >> "$counter"
    printf '%s\n' "$@" > "$arguments"
    exec sh -c "${GATE_TEST_COMMAND:-:}"
    ;;
  'eval --impure')
    [ "${eval_fail:-}" != system ] || exit 1
    echo test-system
    ;;
  eval*)
    [ "${eval_fail:-}" != outputs ] || exit 1
    cat "$answers"
    ;;
  build*)
    case "$*" in
      *' --profile '*)
        for arg do profile=$arg; done
        [ ! -e /dev/fd/9 ] || echo "$profile" >> "$fd9_leaks"
        case "$profile" in
          *.pending)
            if [ "${profile_fail:-}" = pending ]; then
              echo "$dependency_output"
              exit 1
            fi
            ;;
          *) [ "${profile_fail:-}" != current ] || exit 1 ;;
        esac
        ln -sfn "$dependency_output" "$profile-1-link"
        ln -sfn "$profile-1-link" "$profile"
        case "$*" in *' --print-out-paths '*) echo "$dependency_output" ;; esac
        exit 0
        ;;
    esac
    [ "${root_fail:-}" != "$2" ] || exit 1
    [ -e "$2" ] && [ "$3" = --out-link ] || exit 1
    ln -sfn "$2" "$4"
    ;;
  *) exit 98 ;;
esac
SH
cat > "$work/bin/rm" <<'SH'
#!/bin/sh
if [ "${pending_remove_fail:-0}" = 1 ]; then
  case "$*" in *profile.pending*) exit 1 ;; esac
fi
exec "$real_rm" "$@"
SH
cat > "$work/bin/sleep" <<'SH'
#!/bin/sh
# A wait must either release the first gate or fail at once.
if [ -n "${GATE_TEST_WAIT_COMMAND:-}" ]; then
  sh -c "$GATE_TEST_WAIT_COMMAND"
  rm -rf "$GATE_LOCK"
  exit 0
fi
[ -n "${release:-}" ] || exit 97
touch "$release"
wait-for "$finished" "$first"
SH
cat > "$work/bin/free" <<'SH'
#!/bin/sh
sh -c "${GATE_TEST_SPACE_COMMAND:-:}"
echo 100
SH
printf '#!/bin/sh\necho 2026-11-13T12:00:00Z\n' > "$work/bin/date"
chmod +x "$work/bin/"*
export PATH=$work/bin:$PATH
export counter=$work/counter arguments=$work/arguments fd9_leaks=$work/fd9-leaks
export calls=$work/calls answers=$work/outputs
mkdir "$work/store"
touch "$work/store/server" "$work/store/studio"
export dependency_output=$work/store/dependencies
touch "$dependency_output"
printf 'server-check\t%s\nstudio.check\t%s\n' "$work/store/server" "$work/store/studio" > "$answers"

# Linux uses a permanent flock file. Isolate its default path in a copy so this
# test also runs inside a full gate that already holds the machine's real lock.
if [ "$(uname -s)" = Linux ]; then
  mkdir "$work/linux"
  sed "s|/tmp/\${cache_ns}-gate.lock|$work/linux/shared.lock|g" "$gate" > "$work/linux/gate.sh"
  grep -Fq "lock=\${GATE_LOCK:-$work/linux/shared.lock}" "$work/linux/gate.sh" || fail "the Linux test did not isolate the default lock path"
  cp "$(dirname "$gate")/space.sh" "$work/linux/space.sh"
  linux_gate=$work/linux/gate.sh
  (
    unset GATE_LOCK
    export HOME=$work/home-one XDG_CACHE_HOME=$work/cache-one TMPDIR=$work/tmp-one
    "$BASH" "$linux_gate" sh -ec 'touch "$1"; wait-for "$2" "$4"; echo first >> "$3"' \
      sh "$work/linux/ready" "$work/linux/release" "$work/linux/order" $$
  ) > "$work/linux/one.out" 2>&1 &
  first=$!
  wait-for "$work/linux/ready" "$first"
  (
    unset GATE_LOCK
    export HOME=$work/home-two XDG_CACHE_HOME=$work/cache-two TMPDIR=$work/tmp-two
    "$BASH" "$linux_gate" sh -c 'echo second >> "$1"' sh "$work/linux/order"
  ) > "$work/linux/two.out" 2>&1 &
  second=$!
  while ! grep -q 'gate: waiting' "$work/linux/two.out"; do
    kill -0 "$second" 2> /dev/null || fail "case 42: Linux gates with different homes did not wait"
    "$real_sleep" 0.01
  done
  [ ! -e "$work/linux/order" ] || fail "case 42: the second Linux gate ran while the first held its lock"
  touch "$work/linux/release"
  wait "$first"
  wait "$second"
  [ "$(tr '\n' ' ' < "$work/linux/order")" = 'first second ' ] || fail "case 42: Linux gates with different homes ran out of order"
  [ -f "$work/linux/shared.lock" ] || fail "case 42: Linux did not use the shared default lock file"
  [ "$(find "$work/linux/shared.lock" -perm 666)" = "$work/linux/shared.lock" ] || fail "case 42: the Linux lock is not readable by both users"

  mkdir "$work/linux/full"
  printf '#!/bin/sh\nexit 1\n' > "$work/linux/full/mktemp"
  chmod +x "$work/linux/full/mktemp"
  (unset GATE_LOCK; PATH="$work/linux/full:$PATH" "$BASH" "$linux_gate" true) || fail 'an existing Linux lock needed a new file'

  # A file made by another user may be read-only to us, including under Linux's
  # protected_regular policy. Acquiring it must need no write or chmod operation.
  chmod 444 "$work/linux/shared.lock"
  status=0
  (unset GATE_LOCK; "$BASH" "$linux_gate" sh -c 'exit 7') > "$work/linux/fail.out" 2>&1 || status=$?
  [ "$status" -eq 7 ] || fail "case 43: a read-only Linux lock hid the command status ($status)"
  (unset GATE_LOCK; "$BASH" "$linux_gate" true) > "$work/linux/retry.out" 2>&1 || fail "case 43: a failed Linux gate kept the lock"
  flock -n "$work/linux/shared.lock" true || fail "case 43: an exited Linux gate kept the lock"

  # An acquisition error is not contention and must never run the command.
  mkdir "$work/linux/bin"
  printf '#!/bin/sh\nexit 76\n' > "$work/linux/bin/flock"
  chmod +x "$work/linux/bin/flock"
  status=0
  GATE_LOCK=$work/linux/error.lock PATH="$work/linux/bin:$PATH" "$BASH" "$linux_gate" touch "$work/linux/unsafe" \
    > "$work/linux/error.out" 2>&1 || status=$?
  [ "$status" -eq 76 ] || fail "case 44: a Linux lock error was hidden ($status)"
  [ ! -e "$work/linux/unsafe" ] || fail "case 44: a Linux lock error ran the command"
  if grep -q 'gate: waiting' "$work/linux/error.out"; then
    fail "case 44: a Linux acquisition error was reported as contention"
  fi

  # Invalid shared paths fail with their name and leave existing content alone.
  echo keep > "$work/linux/target"
  ln -s "$work/linux/target" "$work/linux/link"
  ln -s "$work/linux/missing" "$work/linux/dangling"
  mkdir "$work/linux/directory"
  mkfifo "$work/linux/fifo"
  for kind in link dangling directory fifo; do
    status=0
    GATE_LOCK=$work/linux/$kind "$BASH" "$linux_gate" touch "$work/linux/unsafe" \
      > "$work/linux/invalid.out" 2>&1 || status=$?
    [ "$status" -ne 0 ] || fail "case 45: a $kind was accepted as a Linux lock"
    [ ! -e "$work/linux/unsafe" ] || fail "case 45: an invalid Linux lock ran the command"
    grep -Fq "$work/linux/$kind" "$work/linux/invalid.out" || fail "case 45: an invalid Linux lock did not name its path"
    grep -q 'regular file' "$work/linux/invalid.out" || fail "case 45: an invalid Linux lock did not explain the error"
  done
  [ "$(cat "$work/linux/target")" = keep ] || fail "case 45: an invalid Linux lock changed its target"
  [ -z "$(find "$work/linux/directory" -mindepth 1 -print)" ] || fail "case 45: an invalid Linux lock directory was changed"

  # A command's background child can remain alive after the command. It must
  # not keep the gate lock. Check both the replacement and full-gate commands.
  export child_ready=$work/linux/child-ready child_release=$work/linux/child-release
  export child_finished=$work/linux/child-finished child_pid=$work/linux/child-pid
  export test_pid=$$ child_script=$work/linux/child
  cat > "$child_script" <<'SH'
#!/bin/sh
. "$OWN_HELPER"; own_publish $$ "$child_pid"
touch "$child_ready"
wait-for "$child_release" "$test_pid"
touch "$child_finished"
SH
  chmod +x "$child_script"
  git -C "$work/linux" init -q --initial-branch=main
  touch "$work/linux/tracked"
  git -C "$work/linux" add tracked
  git -C "$work/linux" -c user.name=test -c user.email=test@example.test -c core.hooksPath=/dev/null commit -qm fixture
  for mode in replacement full; do
    ln "$work/linux/shared.lock" "$work/linux/lock-inode"
    (
      cd "$work/linux"
      unset GATE_LOCK
      if [ "$mode" = replacement ]; then
        "$BASH" "$linux_gate" sh -c '"$child_script" & wait-for "$child_ready" "$!"'
      else
        GATE_TEST_COMMAND='"$child_script" & wait-for "$child_ready" "$!"' "$BASH" "$linux_gate"
      fi
    ) > "$work/linux/child.out" 2>&1
    [ "$work/linux/shared.lock" -ef "$work/linux/lock-inode" ] || fail "case 46: a $mode gate removed the Linux lock file"
    [ ! -e "$fd9_leaks" ] || fail 'case 46: a dependency build inherited the Linux lock descriptor'
    rm "$work/linux/lock-inode"
    flock -n "$work/linux/shared.lock" true || fail "case 46: a $mode command's child kept the Linux lock"
    touch "$child_release"
    wait-for "$child_finished" "$(cat "$child_pid")"
    rm "$child_ready" "$child_release" "$child_finished"
  done
  # Replace the path after open but before flock. The command must hold the
  # replacement inode, rather than run with a lock on the retired file.
  real_flock=$(command -v flock)
  export real_flock swap_done=$work/linux/swapped
  export shared_lock=$work/linux/shared.lock
  cat > "$work/linux/bin/flock" <<'SH'
#!/bin/sh
if [ ! -e "$swap_done" ]; then
  mv "$shared_lock" "$shared_lock.retired"
  : > "$shared_lock"
  touch "$swap_done"
fi
exec "$real_flock" "$@"
SH
  (
    unset GATE_LOCK
    PATH="$work/linux/bin:$PATH" "$BASH" "$linux_gate" sh -c '
      if "$real_flock" -n "$shared_lock" true; then exit 1; fi
    '
  ) > "$work/linux/swap.out" 2>&1 || fail "case 47: the Linux gate locked a retired inode"
  grep -q 'lock path changed' "$work/linux/swap.out" || fail "case 47: the Linux gate did not report its retry"
  echo 'gate-test: 6 Linux lock cases pass'
else
  echo 'gate-test: Linux lock cases need Linux'
fi

# Keep the existing directory-lock and pass-record cases on the macOS branch.
rm -f "$GATE_PASSED"
printf '#!/bin/sh\necho Darwin\n' > "$work/bin/uname"
chmod +x "$work/bin/uname"

# A second gate waits for the first and then runs.
("$BASH" "$gate" sh -ec 'touch "$1"; wait-for "$2" "$4"; echo first >> "$3"' \
  sh "$work/ready" "$work/release" "$log" $$ > "$work/one.out" 2>&1
  touch "$work/finished") &
first=$!
wait-for "$work/ready" "$first"
[ -f "$GATE_LOCK/pid" ] || fail "the first gate took no lock"
release=$work/release finished=$work/finished first=$first \
  "$BASH" "$gate" sh -c 'echo second >> "$1"' sh "$log" > "$work/two.out" 2>&1
wait "$first"
[ "$(tr '\n' ' ' < "$log")" = "first second " ] || fail "the second gate did not wait for the first"
grep -q 'gate: waiting' "$work/two.out" || fail "the second gate did not report its wait"
[ ! -e "$GATE_LOCK" ] || fail "the lock remains after both gates"

# A lock of a dead process does not block.
sh -c 'exit 0' & dead=$!
wait "$dead"
mkdir "$GATE_LOCK" && own_publish "$dead" "$GATE_LOCK/pid"
"$BASH" "$gate" sh -c 'echo third >> "$1"' sh "$log" > "$work/three.out" 2>&1
grep -q third "$log" || fail "a stale lock blocked the gate"
[ ! -e "$GATE_LOCK" ] || fail "the lock remains after a stale takeover"

# The gate returns the status of its command and frees the lock.
status=0
"$BASH" "$gate" false > /dev/null 2>&1 || status=$?
[ "$status" -eq 1 ] || fail "the gate hid a failing command (status $status)"
[ ! -e "$GATE_LOCK" ] || fail "the lock remains after a failed gate"

# A gate without disk space fails before its command and frees the lock.
status=0
# The space script removes old result links of the folder that it runs in.
mkdir "$work/tree"
(cd "$work/tree" && GATE_FREE=101 \
  "$BASH" "$gate" sh -c 'echo fourth >> "$1"' sh "$log") > "$work/four.out" 2>&1 || status=$?
[ "$status" -eq 1 ] || fail "a gate without disk space returned status $status"
grep -q fourth "$log" && fail "a gate without disk space ran its command"
grep -q 'gate: not enough free disk space' "$work/four.out" || fail "a gate without disk space did not say so"
[ ! -e "$GATE_LOCK" ] || fail "the lock remains after a gate without disk space"

# All record cases use a disposable repository, never the caller's tree.
rm -f "$counter"
cd "$work/tree"
git init -q --initial-branch=main
git config user.name Gate
git config user.email gate@example.invalid
git config commit.gpgsign false
echo first > tracked
git add tracked
git -c core.hooksPath=/dev/null commit -qm first
tree=$(git rev-parse 'HEAD^{tree}')
commit=$(git rev-parse HEAD)

# 5. A clean pass is recorded; a hit runs no command, lock or disk cleanup.
"$BASH" "$gate" > "$work/pass.out" 2>&1 || fail "case 5: the first clean gate failed"
[ "$(cat "$GATE_PASSED")" = "$tree 2026-11-13T12:00:00Z" ] || fail "case 5: a clean pass was not recorded"
[ "$(wc -l < "$counter")" -eq 1 ] || fail "case 5: the first gate ran no command"
printf '%s\n' flake check "git+file://$PWD?rev=$commit" --no-update-lock-file --max-jobs 1 --cores 4 -L > "$work/expected"
git diff --no-index --quiet -- "$work/expected" "$arguments" || fail "case 5: a clean gate did not check the pinned HEAD with the gate flags"
# This test process is a live holder. Any lock wait fails via the fake sleep.
mkdir "$GATE_LOCK" && own_publish $$ "$GATE_LOCK/pid"
GATE_FREE=101 "$BASH" "$gate" > "$work/hit.out" 2>&1 || fail "case 5: a hit took the lock or made space"
[ "$(wc -l < "$counter")" -eq 1 ] || fail "case 5: a hit ran the command"
[ "$(cat "$GATE_LOCK/pid")" = $$ ] || fail "case 5: a hit changed the held lock"
grep -Fxq 'gate: this tree passed the full gate (2026-11-13T12:00:00Z); it does not run again' \
  "$work/hit.out" || fail "case 5: a hit did not report its date"
rm -rf "$GATE_LOCK"

# 6. A failed gate records nothing and the next gate runs.
rm "$GATE_PASSED" "$counter"
status=0
GATE_TEST_COMMAND='exit 7' "$BASH" "$gate" > "$work/fail.out" 2>&1 || status=$?
[ "$status" -eq 7 ] || fail "case 6: the full gate hid a failure"
[ ! -e "$GATE_PASSED" ] || fail "case 6: a failed gate was recorded"
"$BASH" "$gate" > "$work/retry.out" 2>&1
[ "$(wc -l < "$counter")" -eq 2 ] || fail "case 6: the gate did not retry after failure"

# 7. Tracked working-tree and staged changes neither hit nor write the record.
for staged in no yes; do
  echo changed > tracked
  [ "$staged" = no ] || git add tracked
  cp "$GATE_PASSED" "$work/before"
  rm "$counter" "$record_read"
  "$BASH" "$gate" > "$work/dirty.out" 2>&1 || fail "case 7: a tracked change failed the gate ($staged)"
  [ -f "$counter" ] || fail "case 7: a tracked change used the record ($staged)"
  [ ! -e "$record_read" ] || fail "case 7: a tracked change read the record ($staged)"
  printf '%s\n' flake check . --no-update-lock-file --max-jobs 1 --cores 4 -L > "$work/expected"
  git diff --no-index --quiet -- "$work/expected" "$arguments" || fail "case 7: a tracked change did not check the working tree ($staged)"
  git diff --no-index --quiet -- "$work/before" "$GATE_PASSED" || fail "case 7: a tracked change was recorded ($staged)"
  rm "$GATE_PASSED" "$counter"
  "$BASH" "$gate" > "$work/dirty-new.out" 2>&1
  [ ! -e "$GATE_PASSED" ] || fail "case 7: a tracked change created a record ($staged)"
  git restore --staged --worktree tracked
  "$BASH" "$gate" > "$work/clean.out" 2>&1
done

# 8. Another tree runs; an empty commit with that same tree skips.
echo second > tracked
git add tracked
git -c core.hooksPath=/dev/null commit -qm second
rm "$counter"
"$BASH" "$gate" > "$work/new.out" 2>&1
[ "$(wc -l < "$counter")" -eq 1 ] || fail "case 8: a new tree did not run"
git -c core.hooksPath=/dev/null commit --allow-empty -qm empty
"$BASH" "$gate" > "$work/empty.out" 2>&1
[ "$(wc -l < "$counter")" -eq 1 ] || fail "case 8: an empty commit ran again"

# 9. A commit or a tracked edit during a pinned gate preserves its source tree.
rm "$GATE_PASSED" "$calls"
commit=$(git rev-parse HEAD)
tree=$(git rev-parse "$commit^{tree}")
GATE_TEST_COMMAND='echo third > tracked; git add tracked; git -c core.hooksPath=/dev/null commit -qm third' \
  "$BASH" "$gate" > "$work/commit.out" 2>&1
[ "$(cat "$GATE_PASSED")" = "$tree 2026-11-13T12:00:00Z" ] || fail "case 9: the gate did not record the checked commit's tree"
printf '%s\n' flake check "git+file://$PWD?rev=$commit" --no-update-lock-file --max-jobs 1 --cores 4 -L > "$work/expected"
git diff --no-index --quiet -- "$work/expected" "$arguments" || fail "case 9: a concurrent commit changed the checked source"
printf '%s\t' eval "git+file://$PWD?rev=$commit#checks.test-system" --no-update-lock-file --raw \
  --apply 'checks: builtins.concatStringsSep "\n" (map (name: name + "\t" + checks.${name}.outPath) (builtins.attrNames checks))' > "$work/evaluation"
grep -Fq "$(cat "$work/evaluation")" "$calls" || fail "case 9: keeping evaluated a later commit"
[ -L "$GATE_KEEP/$tree/server-check" ] || fail "case 9: the checked tree lost its roots after HEAD moved"
rm "$GATE_PASSED"
tree=$(git rev-parse 'HEAD^{tree}')
GATE_TEST_COMMAND='echo fourth > tracked' "$BASH" "$gate" > "$work/edit.out" 2>&1
[ "$(cat "$GATE_PASSED")" = "$tree 2026-11-13T12:00:00Z" ] || fail "case 9: a concurrent edit lost the checked commit's tree"
git restore tracked

# 10. GATE_AGAIN=1 runs even when the tree is recorded.
"$BASH" "$gate" > "$work/record.out" 2>&1
rm "$counter"
GATE_AGAIN=1 "$BASH" "$gate" > "$work/again.out" 2>&1
[ -f "$counter" ] || fail "case 10: GATE_AGAIN did not force a run"

# 11. Outside Git the command runs and writes no record.
mkdir "$work/outside"
rm "$GATE_PASSED" "$counter" "$record_read"
(cd "$work/outside" && "$BASH" "$gate") > "$work/outside.out" 2>&1 || fail "case 11: the gate outside Git failed"
[ -f "$counter" ] || fail "case 11: the gate outside Git did not run"
[ ! -e "$record_read" ] || fail "case 11: the gate outside Git read the record"
[ ! -e "$GATE_PASSED" ] || fail "case 11: the gate outside Git wrote a record"

# 12. Only the last 50 lines remain, including the latest pass.
for n in $(seq 55); do echo "old-$n old-date"; done > "$GATE_PASSED"
"$BASH" "$gate" > "$work/limit.out" 2>&1
[ "$(wc -l < "$GATE_PASSED")" -eq 50 ] || fail "case 12: the record did not keep 50 lines"
[ "$(head -n 1 "$GATE_PASSED")" = 'old-7 old-date' ] || fail "case 12: the record lost the wrong lines"
[ "$(tail -n 1 "$GATE_PASSED")" = "$(git rev-parse 'HEAD^{tree}') 2026-11-13T12:00:00Z" ] || fail "case 12: the latest pass was lost"

# 13. An untracked file does not change the identity.
echo untracked > untracked
rm "$counter"
"$BASH" "$gate" > "$work/untracked.out" 2>&1
[ ! -e "$counter" ] || fail "case 13: an untracked file changed the identity"

# 14. Replacement commands never read or write the pass record.
rm "$record_read"
"$BASH" "$gate" sh -c 'echo custom >> "$counter"' > "$work/args.out" 2>&1
[ "$(cat "$counter")" = custom ] || fail "case 14: a replacement command read the record"
[ ! -e "$record_read" ] || fail "case 14: a replacement command looked up the record"
rm "$GATE_PASSED"
"$BASH" "$gate" true > "$work/args-new.out" 2>&1
[ ! -e "$GATE_PASSED" ] || fail "case 14: a replacement command wrote a record"

# 15. Git errors, even after partial output, do not supply an identity.
real_git=$(command -v git)
mkdir "$work/broken"
cat > "$work/broken/git" <<'SH'
#!/bin/sh
call=$1
if [ "$1" = rev-parse ]; then
  case "$2" in
    HEAD) call=commit ;;
    *'^{tree}') call=tree ;;
  esac
fi
if [ "$call" = "$broken" ] && { [ -z "${break_after:-}" ] || [ -e "$break_after" ]; }; then
  [ "$1" != rev-parse ] || "$real_git" "$@"
  exit 1
fi
exec "$real_git" "$@"
SH
chmod +x "$work/broken/git"
export real_git
for broken in status commit tree; do
  rm "$counter"
  PATH="$work/broken:$PATH" broken=$broken "$BASH" "$gate" > "$work/git-error.out" 2>&1 || fail "case 15: a Git error failed the gate ($broken)"
  [ -f "$counter" ] || fail "case 15: a Git error stopped the gate ($broken)"
  [ ! -e "$GATE_PASSED" ] || fail "case 15: a Git error wrote a record ($broken)"
  PATH="$work/broken:$PATH" broken=$broken break_after=$work/break SPACE_FREE=free \
    GATE_TEST_SPACE_COMMAND='touch "$break_after"' "$BASH" "$gate" > "$work/git-after.out" 2>&1
  [ ! -e "$GATE_PASSED" ] || fail "case 15: a Git error before the command wrote a record ($broken)"
  printf '%s\n' flake check . --no-update-lock-file --max-jobs 1 --cores 4 -L > "$work/expected"
  git diff --no-index --quiet -- "$work/expected" "$arguments" || fail "case 15: a Git error before the command did not use the working tree ($broken)"
  rm "$work/break"
done

# 16. An edit during the lock wait, restored by Nix, cannot record HEAD.
echo bad > tracked
git add tracked
git -c core.hooksPath=/dev/null commit -qm bad
mkdir "$GATE_LOCK" && own_publish $$ "$GATE_LOCK/pid"
GATE_TEST_WAIT_COMMAND='echo good > tracked' GATE_TEST_COMMAND='git checkout -- tracked' \
  "$BASH" "$gate" > "$work/restore.out" 2>&1 || fail "case 16: the edit and restore gate failed"
printf '%s\n' flake check . --no-update-lock-file --max-jobs 1 --cores 4 -L > "$work/expected"
git diff --no-index --quiet -- "$work/expected" "$arguments" || fail "case 16: the lock-wait edit did not check the working tree"
[ "$(cat tracked)" = bad ] || fail "case 16: the fake Nix did not restore HEAD"
[ ! -e "$GATE_PASSED" ] || fail "case 16: the unchecked HEAD tree was recorded"

# 17. A clean commit made during the lock wait supplies the source and record.
mkdir "$GATE_LOCK" && own_publish $$ "$GATE_LOCK/pid"
GATE_TEST_WAIT_COMMAND='echo next > tracked; git add tracked; git -c core.hooksPath=/dev/null commit -qm next' \
  "$BASH" "$gate" > "$work/move.out" 2>&1 || fail "case 17: a commit during the wait failed the gate"
commit=$(git rev-parse HEAD)
tree=$(git rev-parse "$commit^{tree}")
printf '%s\n' flake check "git+file://$PWD?rev=$commit" --no-update-lock-file --max-jobs 1 --cores 4 -L > "$work/expected"
git diff --no-index --quiet -- "$work/expected" "$arguments" || fail "case 17: the gate did not pin the commit made during the wait"
[ "$(cat "$GATE_PASSED")" = "$tree 2026-11-13T12:00:00Z" ] || fail "case 17: the gate recorded the tree from before the wait"

# 18. Read the state after disk cleanup, which can itself change tracked files.
rm "$GATE_PASSED"
SPACE_FREE=free GATE_TEST_SPACE_COMMAND='echo cleanup > tracked' \
  "$BASH" "$gate" > "$work/space-edit.out" 2>&1 || fail "case 18: a cleanup edit failed the gate"
printf '%s\n' flake check . --no-update-lock-file --max-jobs 1 --cores 4 -L > "$work/expected"
git diff --no-index --quiet -- "$work/expected" "$arguments" || fail "case 18: the gate read the state before disk cleanup"
[ ! -e "$GATE_PASSED" ] || fail "case 18: a cleanup edit was recorded"
git restore tracked

# 19. The record can live in a folder that does not exist yet.
GATE_PASSED="$work/new/folder/passed" "$BASH" "$gate" > "$work/folder.out" 2>&1 || fail "case 19: a missing record folder failed the gate"
[ "$(cat "$work/new/folder/passed")" = "$tree 2026-11-13T12:00:00Z" ] || fail "case 19: the record in a new folder was not written"

# 20. URL-special paths use the working tree and write no record.
for path in 'with space' 'with#fragment' 'with?query' 'with%escape'; do
  git clone -q . "$work/$path"
  (cd "$work/$path" && "$BASH" "$gate") > "$work/path.out" 2>&1 || fail "case 20: a URL-special path failed the gate ($path)"
  printf '%s\n' flake check . --no-update-lock-file --max-jobs 1 --cores 4 -L > "$work/expected"
  git diff --no-index --quiet -- "$work/expected" "$arguments" || fail "case 20: a URL-special path was passed as a URL ($path)"
  [ ! -e "$GATE_PASSED" ] || fail "case 20: a URL-special path wrote a record ($path)"
done

# 21. Even if HEAD moves during selection, the record names the pinned commit.
mkdir "$work/moving"
cat > "$work/moving/git" <<'SH'
#!/bin/sh
if [ "$1" = rev-parse ] && [ "$2" = HEAD ]; then
  "$real_git" "$@" || exit 1
  echo later > tracked
  "$real_git" add tracked
  "$real_git" -c core.hooksPath=/dev/null commit -qm later > /dev/null
  exit
fi
exec "$real_git" "$@"
SH
chmod +x "$work/moving/git"
commit=$(git rev-parse HEAD)
tree=$(git rev-parse "$commit^{tree}")
PATH="$work/moving:$PATH" "$BASH" "$gate" > "$work/select.out" 2>&1 || fail "case 21: a commit during selection failed the gate"
printf '%s\n' flake check "git+file://$PWD?rev=$commit" --no-update-lock-file --max-jobs 1 --cores 4 -L > "$work/expected"
git diff --no-index --quiet -- "$work/expected" "$arguments" || fail "case 21: the selected commit was not pinned"
[ "$(cat "$GATE_PASSED")" = "$tree 2026-11-13T12:00:00Z" ] || fail "case 21: the recorded tree did not belong to the selected commit"

# 22. Every signal wait fails when its producer exits, even before readiness.
sh -c 'exit 0' & dead=$!
wait "$dead"
for signal in ready release finished; do
  status=0
  wait-for "$work/missing-$signal" "$dead" > "$work/wait.out" 2>&1 || status=$?
  [ "$status" -eq 1 ] || fail "case 22: a dead producer did not fail the $signal wait"
  grep -q 'exited before' "$work/wait.out" || fail "case 22: the $signal wait did not explain its failure"
done
# A delivered signal is valid even if the producer has since exited.
touch "$work/delivered"
wait-for "$work/delivered" "$dead" || fail "case 22: a delivered signal was lost"

# 23. A pass stays passed if creating, appending, trimming or replacing the record fails.
mkdir "$work/write-failure" "$work/recording"
real_mkdir=$(command -v mkdir)
real_mv=$(command -v mv)
export real_mkdir real_mv write_parent=$work/recording write_calls=$work/write-calls
cat > "$work/write-failure/mkdir" <<'SH'
#!/bin/sh
if [ "$2" = "$write_parent" ] && [ "$write_fail" = mkdir ]; then exit 1; fi
exec "$real_mkdir" "$@"
SH
cat > "$work/write-failure/mv" <<'SH'
#!/bin/sh
echo mv >> "$write_calls"
[ "$write_fail" != mv ] || exit 1
exec "$real_mv" "$@"
SH
chmod +x "$work/write-failure/"*
for write_fail in mkdir append trim mv; do
  record=$write_parent/passed
  rm -rf "$record" "$record.tmp" "$write_calls"
  case "$write_fail" in
    append) mkdir "$record" ;;
    trim) mkdir "$record.tmp" ;;
  esac
  rm -f "$counter"
  PATH="$work/write-failure:$PATH" write_fail=$write_fail GATE_PASSED="$record" \
    "$BASH" "$gate" > "$work/write.out" 2>&1 || fail "case 23: a record write failure failed the gate ($write_fail)"
  [ -f "$counter" ] || fail "case 23: a record write failure prevented the gate ($write_fail)"
  grep -Fxq 'gate: passed; the pass record could not be written' "$work/write.out" || fail "case 23: a record write failure was not explained ($write_fail)"
  case "$write_fail" in
    mkdir) [ ! -e "$record" ] || fail "case 23: recording continued after mkdir failed" ;;
    append) [ ! -e "$record.tmp" ] || fail "case 23: recording continued after an append failure" ;;
    trim) [ ! -e "$write_calls" ] || fail "case 23: recording continued after trimming failed" ;;
    mv) [ -f "$write_calls" ] || fail "case 23: the replacement was not attempted" ;;
  esac
  [ ! -e "$GATE_LOCK" ] || fail "case 23: a record write failure left the lock"
done

# 24. Each output gets a registered root, including dash/dot names and spaces.
export GATE_KEEP="$work/pass results"
rm -f "$GATE_PASSED" "$calls"
tree=$(git rev-parse 'HEAD^{tree}')
commit=$(git rev-parse HEAD)
"$BASH" "$gate" > "$work/keep.out" 2>&1 || fail "case 24: keeping results failed the gate"
for check in server-check studio.check; do
  [ -L "$GATE_KEEP/$tree/$check" ] || fail "case 24: no root for $check"
done
[ "$(readlink "$GATE_KEEP/$tree/server-check")" = "$work/store/server" ] || fail "case 24: wrong server output"
[ "$(readlink "$GATE_KEEP/$tree/studio.check")" = "$work/store/studio" ] || fail "case 24: wrong Studio output"
{
  printf '%s\t' build "git+file://$PWD?rev=$commit#dependencies" --no-update-lock-file --no-link --print-out-paths --max-jobs 1 --cores 4 -L --profile "$SPACE_CACHE/dependencies/profile.pending"
  printf '\n'
  printf '%s\t' flake check "git+file://$PWD?rev=$commit" --no-update-lock-file --max-jobs 1 --cores 4 -L
  printf '\n'
  printf '%s\t' build "$dependency_output" --no-link --profile "$SPACE_CACHE/dependencies/profile"
  printf '\n'
  printf '%s\t' eval --impure --raw --expr builtins.currentSystem
  printf '\n'
  printf '%s\t' eval "git+file://$PWD?rev=$commit#checks.test-system" --no-update-lock-file --raw \
    --apply 'checks: builtins.concatStringsSep "\n" (map (name: name + "\t" + checks.${name}.outPath) (builtins.attrNames checks))'
  printf '\n'
  printf '%s\t' build "$work/store/server" --out-link "$GATE_KEEP/$tree/server-check"
  printf '\n'
  printf '%s\t' build "$work/store/studio" --out-link "$GATE_KEEP/$tree/studio.check"
  printf '\n'
} > "$work/expected"
git diff --no-index --quiet -- "$work/expected" "$calls" || fail "case 24: wrong evaluations or root commands"

# 25. A failed full gate makes no evaluations or roots.
export GATE_KEEP="$work/failed results"
rm "$GATE_PASSED" "$calls"
status=0
GATE_TEST_COMMAND='exit 7' "$BASH" "$gate" > "$work/keep-fail.out" 2>&1 || status=$?
[ "$status" -eq 7 ] || fail "case 25: keeping hid the gate failure"
[ ! -e "$GATE_KEEP" ] || fail "case 25: a failed gate kept results"
grep -q '^eval' "$calls" && fail "case 25: a failed gate evaluated outputs"
[ ! -e "$GATE_PASSED" ] || fail "case 25: a failed gate wrote a record"

# 26. A recorded tree does not evaluate or recreate removed roots.
export GATE_KEEP="$work/skipped results"
"$BASH" "$gate" > "$work/keep-first.out" 2>&1
rm -rf "$GATE_KEEP"
rm "$calls"
"$BASH" "$gate" > "$work/keep-hit.out" 2>&1 || fail "case 26: a recorded tree failed"
[ ! -e "$calls" ] || fail "case 26: a skipped gate called Nix"
[ ! -e "$GATE_KEEP" ] || fail "case 26: a skipped gate recreated roots"

# 27. Arguments, tracked changes and unsafe URLs keep nothing.
for mode in args dirty staged unsafe; do
  export GATE_KEEP="$work/$mode results"
  rm -f "$calls"
  case "$mode" in
    args) "$BASH" "$gate" true > "$work/no-keep.out" 2>&1 ;;
    dirty|staged)
      echo dirty > tracked
      [ "$mode" = dirty ] || git add tracked
      "$BASH" "$gate" > "$work/no-keep.out" 2>&1
      git restore --staged --worktree tracked
      ;;
    unsafe) (cd "$work/with space" && "$BASH" "$gate") > "$work/no-keep.out" 2>&1 ;;
  esac
  [ ! -e "$GATE_KEEP" ] || fail "case 27: $mode kept results"
  if [ -f "$calls" ]; then
    grep -q '^eval' "$calls" && fail "case 27: $mode evaluated outputs"
  fi
done

# 28. Either evaluation can fail without losing the pass or its record.
for eval_fail in system outputs; do
  export GATE_KEEP="$work/$eval_fail results"
  rm "$GATE_PASSED"
  eval_fail=$eval_fail "$BASH" "$gate" > "$work/eval.out" 2>&1 || fail "case 28: $eval_fail evaluation failed the gate"
  grep -Fxq 'gate: passed; the results could not be kept' "$work/eval.out" || fail "case 28: no $eval_fail failure line"
  [ "$(cat "$GATE_PASSED")" = "$tree 2026-11-13T12:00:00Z" ] || fail "case 28: $eval_fail failure lost the record"
  [ ! -e "$GATE_KEEP" ] || fail "case 28: $eval_fail failure continued keeping"
done

# 29. An unwritable keep folder cannot fail the gate or its record.
# A file as the parent fails even for root; chmod alone does not.
touch "$work/not-a-folder"
rm "$GATE_PASSED" "$calls"
GATE_KEEP="$work/not-a-folder/results" "$BASH" "$gate" > "$work/keep-write.out" 2>&1 || fail "case 29: an unwritable folder failed the gate"
grep -Fxq 'gate: passed; the results could not be kept' "$work/keep-write.out" || fail "case 29: no keep failure line"
[ "$(cat "$GATE_PASSED")" = "$tree 2026-11-13T12:00:00Z" ] || fail "case 29: an unwritable folder lost the record"
grep -q '^build.*--out-link' "$calls" && fail "case 29: an unwritable folder still attempted roots"

# 30. A missing output is named and skipped; later outputs are still rooted.
export GATE_KEEP="$work/missing results"
rm "$GATE_PASSED" "$work/store/server" "$calls"
"$BASH" "$gate" > "$work/missing.out" 2>&1 || fail "case 30: a missing output failed the gate"
grep -Fxq 'gate: passed; check server-check is missing from the store; skipped' "$work/missing.out" || fail "case 30: the missing check was not named"
[ ! -L "$GATE_KEEP/$tree/server-check" ] || fail "case 30: the missing check got a root"
[ "$(readlink "$GATE_KEEP/$tree/studio.check")" = "$work/store/studio" ] || fail "case 30: the remaining check was not kept"
[ "$(grep -c '^build.*--out-link' "$calls")" -eq 1 ] || fail "case 30: the missing output was built"
grep -q 'the results could not be kept' "$work/missing.out" && fail "case 30: a missing output stopped keeping"
touch "$work/store/server"

# 31. Expired 40/64-digit trees lose links and folders; recorded trees stay.
export GATE_KEEP="$work/pruned results"
for width in 40 64; do
  expired=$(printf '%0*d' "$width" 1)
  retained=$(printf '%0*d' "$width" 7)
  mkdir -p "$GATE_KEEP/$expired" "$GATE_KEEP/$retained"
  ln -s "$work/store/server" "$GATE_KEEP/$expired/root"
  ln -s "$work/store/missing" "$GATE_KEEP/$expired/dangling"
  ln -s "$work/store/studio" "$GATE_KEEP/$retained/root"
done
touch "$GATE_KEEP/note"
for n in $(seq 55); do
  case "$n" in
    2) printf '%064d old-date\n' 1 ;;
    8) printf '%064d old-date\n' 7 ;;
    *) printf '%040d old-date\n' "$n" ;;
  esac
done > "$GATE_PASSED"
"$BASH" "$gate" > "$work/prune.out" 2>&1 || fail "case 31: pruning failed the gate"
for width in 40 64; do
  expired=$(printf '%0*d' "$width" 1)
  retained=$(printf '%0*d' "$width" 7)
  [ ! -e "$GATE_KEEP/$expired" ] || fail "case 31: an expired $width-digit tree folder remains"
  [ -L "$GATE_KEEP/$retained/root" ] || fail "case 31: a recorded $width-digit tree folder was removed"
done
[ -f "$GATE_KEEP/note" ] || fail "case 31: pruning removed a non-folder"
[ -L "$GATE_KEEP/$tree/server-check" ] || fail "case 31: pruning lost the new roots"

# 32. A regular file with an expired tree name stays in the keep folder.
export GATE_KEEP="$work/tree file"
file_tree=$(printf '%040d' 1)
mkdir "$GATE_KEEP"
printf 'keep tree file\n' > "$GATE_KEEP/$file_tree"
rm "$GATE_PASSED"
"$BASH" "$gate" > "$work/tree-file.out" 2>&1 || fail "case 32: a tree-named file failed the gate"
[ "$(cat "$GATE_KEEP/$file_tree")" = 'keep tree file' ] || fail "case 32: pruning lost a tree-named regular file"
grep -q 'rmdir:\|find:' "$work/tree-file.out" && fail "case 32: a tree-named file reported a removal error"

# 33. A root command failure cannot fail the gate or its record.
export GATE_KEEP="$work/root failure"
rm "$GATE_PASSED" "$calls"
root_fail="$work/store/server" "$BASH" "$gate" > "$work/root.out" 2>&1 || fail "case 33: a root failure failed the gate"
grep -Fxq 'gate: passed; the results could not be kept' "$work/root.out" || fail "case 33: no root failure line"
[ "$(cat "$GATE_PASSED")" = "$tree 2026-11-13T12:00:00Z" ] || fail "case 33: a root failure lost the record"
[ "$(grep -c '^build.*--out-link' "$calls")" -eq 1 ] || fail "case 33: keeping continued after a root failure"

# 34. A record failure does not stop keeping, or prune against a partial record.
export GATE_KEEP="$work/record failure"
expired=$(printf '%040d' 1)
mkdir -p "$GATE_KEEP/$expired"
ln -s "$work/store/server" "$GATE_KEEP/$expired/root"
PATH="$work/write-failure:$PATH" write_fail=mkdir GATE_AGAIN=1 GATE_PASSED="$write_parent/passed" \
  "$BASH" "$gate" > "$work/record-keep.out" 2>&1 || fail "case 34: record failure failed the gate"
[ -L "$GATE_KEEP/$tree/server-check" ] || fail "case 34: record failure stopped keeping"
[ -L "$GATE_KEEP/$expired/root" ] || fail "case 34: record failure pruned an unrecorded tree"

# 35. With no override, XDG_CACHE_HOME and then HOME own the roots.
for cache in xdg home; do
  rm "$GATE_PASSED"
  (
    unset GATE_KEEP
    export HOME="$work/home"
    if [ "$cache" = xdg ]; then
      export XDG_CACHE_HOME="$work/xdg"
    else
      unset XDG_CACHE_HOME
    fi
    "$BASH" "$gate"
  ) > "$work/default.out" 2>&1 || fail "case 35: the $cache default failed the gate"
  case "$cache" in
    xdg) root=$work/xdg/bombay/nix/gate ;;
    home) root=$work/home/.cache/bombay/nix/gate ;;
  esac
  [ -L "$root/$tree/server-check" ] || fail "case 35: the $cache default kept results elsewhere"
done

# 36. An empty evaluation answer creates no root or keeping failure.
export GATE_KEEP="$work/empty results"
: > "$answers"
rm "$GATE_PASSED" "$calls"
"$BASH" "$gate" > "$work/empty.out" 2>&1 || fail "case 36: empty checks failed the gate"
grep -q '^build.*--out-link' "$calls" && fail "case 36: empty checks built an output"
grep -q 'gate: passed;' "$work/empty.out" && fail "case 36: empty checks reported a keeping failure"

# 37. Either cleanup command can fail without losing the pass, record or roots.
expired=$(printf '%040d' 1)
mkdir "$work/remove-failure"
real_find=$(command -v find)
real_rmdir=$(command -v rmdir)
export real_find real_rmdir
cat > "$work/remove-failure/find" <<'SH'
#!/bin/sh
[ "$1" != "$remove_folder" ] || [ "$remove_fail" != find ] || exit 1
exec "$real_find" "$@"
SH
cat > "$work/remove-failure/rmdir" <<'SH'
#!/bin/sh
[ "$1" != "$remove_folder" ] || [ "$remove_fail" != rmdir ] || exit 1
exec "$real_rmdir" "$@"
SH
chmod +x "$work/remove-failure/"*
printf 'server-check\t%s\n' "$work/store/server" > "$answers"
for remove_fail in find rmdir; do
  export GATE_KEEP="$work/$remove_fail failure"
  export remove_folder="$GATE_KEEP/$expired"
  mkdir -p "$remove_folder"
  ln -s "$work/store/server" "$remove_folder/root"
  rm "$GATE_PASSED"
  PATH="$work/remove-failure:$PATH" remove_fail=$remove_fail "$BASH" "$gate" > "$work/remove.out" 2>&1 || fail "case 37: a $remove_fail failure failed the gate"
  [ "$(cat "$GATE_PASSED")" = "$tree 2026-11-13T12:00:00Z" ] || fail "case 37: a $remove_fail failure lost the record"
  [ -L "$GATE_KEEP/$tree/server-check" ] || fail "case 37: a $remove_fail failure stopped keeping"
done

# 38. A wrong keep folder leaves non-tree folder names and their links alone.
export GATE_KEEP="$work/unrelated folders"
unrelated=(Documents "$(printf '%039dA' 0)" "$(printf '%039dg' 0)" \
  "$(printf '%039d' 0)" "$(printf '%041d' 0)" \
  "$(printf '%063d' 0)" "$(printf '%065d' 0)")
for name in "${unrelated[@]}"; do
  mkdir -p "$GATE_KEEP/$name"
  ln -s "$work/store/server" "$GATE_KEEP/$name/root"
done
rm "$GATE_PASSED"
"$BASH" "$gate" > "$work/unrelated.out" 2>&1 || fail "case 38: unrelated folders failed the gate"
for name in "${unrelated[@]}"; do
  [ -L "$GATE_KEEP/$name/root" ] || fail "case 38: pruning changed unrelated folder $name"
done

# 39. Tree folders with regular content stay; only their direct store links go.
export GATE_KEEP="$work/regular content"
file_tree=$(printf '%040d' 1)
folder_tree=$(printf '%064d' 1)
mkdir -p "$GATE_KEEP/$file_tree" "$GATE_KEEP/$folder_tree/subfolder"
printf 'keep file\n' > "$GATE_KEEP/$file_tree/file"
printf 'keep nested file\n' > "$GATE_KEEP/$folder_tree/subfolder/file"
ln -s "$work/store/server" "$GATE_KEEP/$folder_tree/subfolder/root"
for name in "$file_tree" "$folder_tree"; do
  ln -s "$work/store/server" "$GATE_KEEP/$name/root"
done
rm "$GATE_PASSED"
"$BASH" "$gate" > "$work/content.out" 2>&1 || fail "case 39: regular content failed the gate"
[ "$(cat "$GATE_KEEP/$file_tree/file")" = 'keep file' ] || fail "case 39: pruning lost a regular file"
[ "$(cat "$GATE_KEEP/$folder_tree/subfolder/file")" = 'keep nested file' ] || fail "case 39: pruning lost a subfolder's file"
[ -L "$GATE_KEEP/$folder_tree/subfolder/root" ] || fail "case 39: pruning entered a subfolder"
for name in "$file_tree" "$folder_tree"; do
  [ ! -L "$GATE_KEEP/$name/root" ] || fail "case 39: pruning left a direct link"
done
grep -q 'rmdir:' "$work/content.out" && fail "case 39: retained content reported a removal error"

# 40. A tree-named link to a folder is not a folder to prune.
export GATE_KEEP="$work/linked folders"
mkdir -p "$GATE_KEEP" "$work/store/elsewhere"
ln -s "$work/store/server" "$work/store/elsewhere/root"
ln -s "$work/store/elsewhere" "$GATE_KEEP/$file_tree"
rm "$GATE_PASSED"
"$BASH" "$gate" > "$work/linked.out" 2>&1 || fail "case 40: a linked folder failed the gate"
[ -L "$GATE_KEEP/$file_tree" ] || fail "case 40: pruning removed a linked folder"
[ -L "$work/store/elsewhere/root" ] || fail "case 40: pruning changed a linked folder's content"

# 41. Expired tree folders keep foreign links but lose store links.
export GATE_KEEP="$work/foreign links"
mkdir -p "$GATE_KEEP/$file_tree"
ln -s "$work/tree" "$GATE_KEEP/$file_tree/foreign"
ln -s "$work/store/server" "$GATE_KEEP/$file_tree/root"
rm "$GATE_PASSED"
"$BASH" "$gate" > "$work/foreign.out" 2>&1 || fail "case 41: foreign links failed the gate"
[ -d "$GATE_KEEP/$file_tree" ] || fail "case 41: pruning lost a folder with a foreign link"
[ "$(readlink "$GATE_KEEP/$file_tree/foreign")" = "$work/tree" ] || fail "case 41: pruning lost a foreign link"
[ ! -L "$GATE_KEEP/$file_tree/root" ] || fail "case 41: pruning left a store link"

# A full gate holds dependencies before checking, then promotes them on success.
rm -f "$GATE_PASSED" "$calls"
mkdir -p "$SPACE_CACHE/dependencies"
ln -s "$dependency_output" "$SPACE_CACHE/dependencies/profile.pending-stale-link"
"$BASH" "$gate" > "$work/dependencies.out" 2>&1 || fail 'dependency gate failed'
profile=$SPACE_CACHE/dependencies/profile
head -1 "$calls" | grep -Fq -- "--profile$(printf '\t')$profile.pending" || fail 'dependencies were not held before the checks'
sed -n 2p "$calls" | grep -q '^flake' || fail 'the dependency profile was updated before the checks'
sed -n 3p "$calls" | grep -Fq -- "--profile$(printf '\t')$profile" || fail 'a successful gate did not update the dependency profile'
[ "$(readlink "$profile-1-link")" = "$dependency_output" ] || fail 'a successful gate did not keep its dependencies'
[ ! -L "$profile.pending-1-link" ] && [ ! -L "$profile.pending-stale-link" ] || fail 'a successful gate left a temporary root'
rm -f "$GATE_PASSED" "$calls" "$profile" "$profile-1-link"
status=0
GATE_TEST_COMMAND='exit 7' "$BASH" "$gate" > "$work/dependencies-fail.out" 2>&1 || status=$?
[ "$status" -eq 7 ] && [ "$(wc -l < "$calls")" -eq 2 ] || fail 'a failed gate updated the dependency profile'
[ ! -L "$profile" ] || fail 'a failed gate kept its unchecked dependencies'
[ ! -L "$profile.pending-1-link" ] || fail 'a failed gate left its temporary root'
[ ! -e "$GATE_LOCK" ] || fail 'the lock remains after a failed dependency gate'

# Dead pending roots must go before disk cleanup can refuse the gate.
ln -s "$dependency_output" "$profile.pending-stale-link"
printf '#!/bin/sh\n[ -L "$SPACE_CACHE/dependencies/profile.pending-stale-link" ] && echo 0 || echo 100\n' > "$work/bin/pending-free"
chmod +x "$work/bin/pending-free"
SPACE_FREE=pending-free GATE_FREE=1 "$BASH" "$gate" > "$work/stale-pending.out" 2>&1 || fail 'a stale pending root blocked disk cleanup'
[ ! -L "$profile.pending-stale-link" ] || fail 'disk cleanup left a stale pending root'
rm "$GATE_PASSED"

# A dirty tree cannot replace the checked dependency profile.
touch "$work/store/checked-dependencies"
ln -sfn "$work/store/checked-dependencies" "$profile-1-link"
echo dirty > tracked
rm "$calls"
"$BASH" "$gate" > "$work/unpinned-dependencies.out" 2>&1 || fail 'an unpinned gate failed'
[ "$(readlink "$profile-1-link")" = "$work/store/checked-dependencies" ] || fail 'an unpinned gate replaced checked dependencies'
grep -Fq -- "$(printf -- '--profile\t%s\t' "$profile")" "$calls" && fail 'an unpinned gate promoted dependencies'
[ ! -e "$GATE_PASSED" ] || fail 'an unpinned gate wrote a pass record'
git restore tracked

# Retention is best effort: each write failure preserves a successful gate's
# pass record and check roots. A failed check still returns its own status.
for failure in directory pending current; do
  rm -f "$GATE_PASSED" "$calls"
  cache=$work/dependencies-$failure
  mkdir -p "$cache"
  [ "$failure" != directory ] || touch "$cache/dependencies"
  SPACE_CACHE=$cache profile_fail=$failure "$BASH" "$gate" > "$work/retention-$failure.out" 2>&1 || fail "retention $failure failed a successful gate"
  grep -Fq "$cache/dependencies/profile" "$work/retention-$failure.out" || fail "retention $failure did not name the profile"
  grep -q 'dependencies could not be' "$work/retention-$failure.out" || fail "retention $failure did not warn"
  [ "$(cat "$GATE_PASSED")" = "$tree 2026-11-13T12:00:00Z" ] || fail "retention $failure lost the pass record"
  [ "$(readlink "$GATE_KEEP/$tree/server-check")" = "$work/store/server" ] || fail "retention $failure lost the check root"
  if [ "$failure" != current ]; then
    grep -Fq -- "$(printf -- '--profile\t%s\t' "$cache/dependencies/profile")" "$calls" && fail "retention $failure promoted an unchecked result"
  fi
  [ ! -L "$cache/dependencies/profile.pending-1-link" ] || fail "retention $failure left a temporary root"
  [ ! -e "$GATE_LOCK" ] || fail "retention $failure kept the directory lock"
  rm "$GATE_PASSED"
  status=0
  SPACE_CACHE=$cache profile_fail=$failure GATE_TEST_COMMAND='exit 7' "$BASH" "$gate" > "$work/retention-failed-$failure.out" 2>&1 || status=$?
  [ "$status" -eq 7 ] || fail "retention $failure hid a failed check"
  [ ! -e "$GATE_PASSED" ] || fail "retention $failure recorded a failed check"
done

# A failed temporary-root removal reports its path but keeps the check status.
for check_status in 0 7; do
  rm -f "$GATE_PASSED"
  status=0
  pending_remove_fail=1 GATE_TEST_COMMAND="exit $check_status" "$BASH" "$gate" > "$work/remove-$check_status.out" 2>&1 || status=$?
  [ "$status" -eq "$check_status" ] || fail 'temporary removal changed the check status'
  grep -Fq "stale dependencies could not be removed before cleanup: $profile.pending" "$work/remove-$check_status.out" || fail 'stale removal did not warn with its path'
  grep -Fq "temporary dependencies could not be removed: $profile.pending" "$work/remove-$check_status.out" || fail 'temporary removal did not warn with its path'
  if [ "$check_status" -eq 0 ]; then
    [ "$(cat "$GATE_PASSED")" = "$tree 2026-11-13T12:00:00Z" ] || fail 'temporary removal lost the pass record'
    [ "$(readlink "$GATE_KEEP/$tree/server-check")" = "$work/store/server" ] || fail 'temporary removal lost the check root'
  fi
  [ ! -e "$GATE_LOCK" ] || fail 'temporary removal kept the directory lock'
done
rm -f "$profile" "$profile"-*-link "$profile".pending*

echo "gate-test: 50 cases pass"

# Cleanup uses the native lock and real polling, apart from the disk probe.
{
  mkdir "$work/native"
  printf '#!%s\n' "$BASH" > "$work/native/nix"
  cat >> "$work/native/nix" <<'SH'
if [ "${SPACE_TEST_GC:-0}" = 1 ] && [ "$*" = 'store gc' ]; then
  if [ "$(uname -s)" = Linux ]; then
    status=0
    flock -n -E 75 "$GATE_LOCK" true || status=$?
    [ "$status" = 75 ] || echo unlocked >> collected
  else
    kill -0 "$(cat "$GATE_LOCK/pid")" 2>/dev/null || echo unlocked >> collected
  fi
  echo gc >> collected
  if [ "${SPACE_TEST_PAUSE:-0}" = 1 ]; then
    touch collecting
    while [ ! -e release-collection ]; do sleep 0.01; done
  fi
  echo 100 > gc-free
  exit 0
fi
echo 'gate-test: unexpected Nix call' >&2
exit 99
SH
  chmod +x "$work/native/nix"
  export PATH=$work/native:$gate_path GATE_POLL=0.01
  unset GATE_OWNER GATE_CLEANUP
  # Run in the suite process so a signal reaches its EXIT cleanup.
  trap 'cleanup; rm -rf "$work"' EXIT
  # Include polling children in each background holder's cleanup group.
  set -m
  fail() {
    echo "gate-test: FAILED: $*" >&2
    tail -n 6 "$work/cleanup/"*.log >&2 || true
    if [ -s "$work/cleanup/signal.trace" ]; then
      cat "$work/cleanup/signal.trace" >&2
      if [ -f "$work/cleanup/signal-pid" ] && IFS= read -r child < "$work/cleanup/signal-pid"; then
        printf 'command pid=%s\n' "$child" >&2
      fi
      if [ -e "$work/cleanup/signalled" ]; then echo 'command observed the signal' >&2; fi
    fi
    exit 1
  }
  wait_log() {
    local deadline=$((SECONDS + 10))
    while ! grep -q "$1" "$2"; do
      kill -0 "$3" 2>/dev/null || fail "cleanup process exited before $1"
      [ "$SECONDS" -lt "$deadline" ] || fail "cleanup timed out before $1"
      sleep 0.01
    done
  }
  wait_for() {
    local deadline=$((SECONDS + 10))
    while [ ! -e "$1" ]; do
      kill -0 "$2" 2>/dev/null || fail "cleanup process exited before $1"
      [ "$SECONDS" -lt "$deadline" ] || fail "cleanup timed out before $1"
      sleep 0.01
    done
  }
  mkdir "$work/cleanup"
  cd "$work/cleanup"
  export GATE_LOCK=$PWD/lock

  if [ "$(uname -s)" = Darwin ]; then
    # Model another waiter replacing a dead lock while cleanup waits to reap it.
    stale_lock=$PWD/stale-lock
    mkdir "$stale_lock" "$work/stale-bin"
    sh -c ':' & dead=$!
    wait "$dead"
    printf '%s\n' "$dead" > "$stale_lock/pid"
    real_cat=$(command -v cat)
    cat > "$work/stale-bin/cat" <<'SH'
#!/bin/sh
"$real_cat" "$@" || exit
if [ "$*" = "$GATE_LOCK/pid" ]; then touch saw-holder; fi
SH
    chmod +x "$work/stale-bin/cat"
    (
      /usr/bin/lockf -s 9
      touch reaping
      while [ ! -e release-reap ]; do sleep 0.01; done
    ) 9>> "$stale_lock.reap" &
    reaper=$!
    wait_for reaping "$reaper"
    real_cat=$real_cat GATE_LOCK=$stale_lock PATH="$work/stale-bin:$PATH" "$BASH" "$gate" touch stale-checked > stale.log 2>&1 &
    contender=$!
    wait_for saw-holder "$contender"
    printf '%s\n' "$$" > replacement-pid
    mv replacement-pid "$stale_lock/pid"
    touch release-reap
    wait "$reaper"
    wait_log 'gate: waiting' stale.log "$contender"
    [ ! -e stale-checked ] && [ "$(cat "$stale_lock/pid")" = "$$" ] || fail 'stale recovery removed a live gate lock'
    rm -rf "$stale_lock"
    wait "$contender" || fail 'stale contender did not start after the gate'
    echo 'gate-test: stale directory recovery rechecks the holder under a lock'

    mkdir "$work/full-bin"
    real_mkdir=$(command -v mkdir)
    cat > "$work/full-bin/mkdir" <<'SH'
#!/bin/sh
if [ "$#" = 1 ] && [ "$1" = "$GATE_LOCK" ]; then
  echo "mkdir: $1: No space left on device" >&2
  exit 1
fi
exec "$real_mkdir" "$@"
SH
    chmod +x "$work/full-bin/mkdir"
    echo 1 > gc-free
    status=0
    real_mkdir=$real_mkdir PATH="$work/full-bin:$PATH" SPACE_TEST_GC=1 SPACE_FREE='cat gc-free' \
      timeout 10 "$BASH" "$work/space.sh" --own-target 15 > space-full.log 2>&1 || status=$?
    [ "$status" -eq 1 ] || fail "macOS cleanup did not return the lock creation error ($status)"
    grep -q 'No space left on device' space-full.log || fail 'macOS cleanup hid the lock creation error'
    [ ! -e collected ] || fail 'macOS cleanup collected without a lock'
    echo 'gate-test: macOS disk cleanup reports a lock creation failure'
  fi

  # The run's between-session cleanup must wait on the same lock as a gate.
  "$BASH" "$gate" "$BASH" -ec 'touch building; while [ ! -e release-build ]; do sleep 0.01; done' > build.log 2>&1 &
  holder=$!
  wait_for building "$holder"
  echo 1 > gc-free
  SPACE_TEST_GC=1 SPACE_FREE='cat gc-free' "$BASH" "$work/space.sh" --own-target 15 > space.log 2>&1 &
  collector=$!
  wait_log 'gate: waiting' space.log "$collector"
  [ ! -e collected ] || fail 'store collection started while the gate held the lock'
  touch release-build
  wait "$holder"
  wait "$collector" || fail 'disk cleanup did not finish after the gate'
  [ "$(cat collected)" = gc ] || fail 'the waiting cleanup did not collect exactly once'

  echo 1 > gc-free
  SPACE_TEST_GC=1 SPACE_FREE='cat gc-free' timeout 10 "$BASH" "$work/space.sh" --own-target 15 > space-idle.log 2>&1 || fail 'cleanup with no gate failed'
  [ "$(cat collected)" = $'gc\ngc' ] || fail 'cleanup with no gate did not collect exactly once'

  echo 1 > gc-free
  SPACE_TEST_GC=1 SPACE_FREE='cat gc-free' GATE_FREE=15 timeout 10 "$BASH" "$gate" touch checked > space-startup.log 2>&1 || fail 'startup cleanup did not make space for the gate'
  [ "$(cat collected)" = $'gc\ngc\ngc' ] && [ -e checked ] || fail 'the gate did not collect under its own lock before its command'
  echo 'gate-test: disk cleanup holds the native gate lock with or without contention and at gate startup'

  echo 1 > gc-free
  SPACE_TEST_GC=1 SPACE_TEST_PAUSE=1 SPACE_FREE='cat gc-free' "$BASH" "$work/space.sh" --own-target 15 > space-term.log 2>&1 &
  collector=$!
  wait_for collecting "$collector"
  kill -TERM "$collector"
  "$BASH" "$gate" touch entered-after-collection > next-gate.log 2>&1 &
  contender=$!
  wait_log 'gate: waiting' next-gate.log "$contender"
  [ ! -e entered-after-collection ] || fail 'terminating cleanup let a gate build during collection'
  touch release-collection
  status=0; wait "$collector" || status=$?
  [ "$status" -eq 143 ] || fail "cleanup did not report TERM ($status)"
  wait "$contender" || fail 'gate did not start after interrupted cleanup ended'
  [ -e entered-after-collection ] || fail 'gate did not run its command after interrupted cleanup ended'
  echo 'gate-test: an interrupted cleanup holds the lock through collection'

  rm collected
  echo 1 > gc-free
  printf 'aborted\n' > "$GATE_LOCK.cleanup"
  SPACE_TEST_GC=1 SPACE_FREE='cat gc-free' "$BASH" "$work/space.sh" --own-target 15 > space-recovery.log 2>&1 &
  collector=$!
  wait_log 'gate: waiting for cleanup of aborted' space-recovery.log "$collector"
  [ ! -e collected ] || fail 'disk cleanup bypassed a pending gate recovery'
  GATE_OWNER=aborted GATE_CLEANUP=1 "$BASH" "$gate" true
  wait "$collector" || fail 'disk cleanup did not finish after recovery'
  [ "$(cat collected)" = gc ] || fail 'disk cleanup did not collect after recovery'
  echo 'gate-test: disk cleanup waits for unfinished gate recovery'

  for mode in no-command no-owner newline; do
    status=0
    case "$mode" in
      no-command) GATE_OWNER=owner "$BASH" "$gate" > invalid.log 2>&1 || status=$? ;;
      no-owner) GATE_CLEANUP=1 "$BASH" "$gate" true > invalid.log 2>&1 || status=$? ;;
      newline) GATE_OWNER=$'one\ntwo' "$BASH" "$gate" true > invalid.log 2>&1 || status=$? ;;
    esac
    [ "$status" = 2 ] || fail "invalid $mode ownership was accepted"
    grep -q 'single-line GATE_OWNER' invalid.log || fail 'invalid ownership had no explanation'
  done
  GATE_OWNER=success "$BASH" "$gate" true
  [ "$(cat "$GATE_LOCK.cleanup")" = success ] || fail 'successful owned command lost pending cleanup'
  GATE_OWNER=success GATE_CLEANUP=1 "$BASH" "$gate" true
  for owner in partial unknown; do
    if [ "$owner" = partial ]; then
      printf partial > "$GATE_LOCK.cleanup"
    else
      : > "$GATE_LOCK.cleanup"
    fi
    GATE_OWNER=$owner GATE_CLEANUP=1 GATE_POLL=invalid timeout 10 "$BASH" "$gate" true || fail 'incomplete record could not recover'
    [ ! -e "$GATE_LOCK.cleanup" ] || fail 'incomplete record remained after recovery'
  done
  # An ordinary command must keep its input, including a real Linux terminal.
  printf 'hello\n' > input
  "$BASH" "$gate" "$BASH" -ec 'read -r value; [ "$value" = hello ]; ! kill -0 -- "-$$" 2>/dev/null' < input || fail 'ordinary command lost its input or process group'
  if [ "$(uname -s)" = Linux ]; then
    for owner in '' terminal; do
      printf -v terminal_command '%q ' "$BASH" "$gate" "$BASH" -ec '. "$OWN_HELPER"; own_write $$ terminal-pid; read -r value; [ "$value" = hello ]; touch terminal-read'
      status=0
      GATE_OWNER=$owner timeout 10 script -qec "$terminal_command" /dev/null < input > terminal.log 2>&1 || status=$?
      if [ "$status" != 0 ]; then
        [ ! -f terminal-pid ] || own_signal KILL "$(cat terminal-pid)" 2>/dev/null || true
        fail 'terminal command stopped or lost its input'
      fi
      [ -e terminal-read ] || fail 'terminal command did not read its input'
      rm terminal-read
      if [ -n "$owner" ]; then
        [ "$(cat "$GATE_LOCK.cleanup")" = "$owner" ] || fail 'terminal command lost pending cleanup'
        GATE_OWNER=$owner GATE_CLEANUP=1 "$BASH" "$gate" true
      fi
    done
    # Each terminal stream keeps the command in the foreground, even with a redirected stdin.
    for stream in 0 1 2; do
      printf -v terminal_command '%q ' "$BASH" "$gate" "$BASH" -ec '. "$OWN_HELPER"; own_write $$ terminal-pid; read -r value < /dev/tty; [ "$value" = hello ]; touch terminal-read'
      case "$stream" in
        0) terminal_command+=' > /dev/null 2>&1' ;;
        1) terminal_command+=' < /dev/null 2>/dev/null' ;;
        2) terminal_command+=' < /dev/null > /dev/null' ;;
      esac
      status=0
      GATE_OWNER=terminal timeout 10 script -qec "$terminal_command" /dev/null < input > terminal.log 2>&1 || status=$?
      if [ "$status" != 0 ]; then
        [ ! -f terminal-pid ] || own_signal KILL "$(cat terminal-pid)" 2>/dev/null || true
        fail "command stopped at its terminal stream $stream"
      fi
      [ -e terminal-read ] || fail 'redirected command did not read its terminal'
      GATE_OWNER=terminal GATE_CLEANUP=1 "$BASH" "$gate" true
      rm terminal-read
    done
  fi
  # Hold one waiter's poll open so recovery must acquire the released lock.
  mkdir "$work/poll"
  printf '#!%s\n' "$BASH" > "$work/poll/sleep"
  cat >> "$work/poll/sleep" <<'SH'
if ! grep -q 'gate: waiting for cleanup' contender.log; then
  exec "$real_sleep" "$@"
fi
touch polling
while [ ! -e poll-release ]; do "$real_sleep" 0.01; done
[ ! -e poll-fail ] || exit 97
SH
  chmod +x "$work/poll/sleep"
  # Reject links and non-files without opening them or changing their targets.
  echo foreign > target
  for kind in link dangling directory fifo; do
    case "$kind" in
      link) ln -s target "$GATE_LOCK.cleanup" ;;
      dangling) ln -s missing "$GATE_LOCK.cleanup" ;;
      directory) mkdir "$GATE_LOCK.cleanup" ;;
      fifo) mkfifo "$GATE_LOCK.cleanup" ;;
    esac
    status=0
    GATE_POLL=invalid "$BASH" "$gate" touch unsafe > invalid.log 2>&1 || status=$?
    [ "$status" != 0 ] && [ ! -e unsafe ] || fail "accepted a $kind cleanup record"
    grep -q 'must be a regular file' invalid.log || fail "$kind record had no type error"
    rm -rf "$GATE_LOCK.cleanup"
  done
  [ "$(cat target)" = foreign ] || fail 'changed a foreign cleanup target'
  # A record that arrives after admission must not be overwritten at publication.
  printf '#!%s\nprintf "foreign\\n" > "$GATE_LOCK.cleanup"\necho 100\n' "$BASH" > "$work/native/free"
  chmod +x "$work/native/free"
  status=0
  GATE_OWNER=race GATE_FREE=1 SPACE_FREE="$work/native/free" "$BASH" "$gate" touch unsafe > race.log 2>&1 || status=$?
  [ "$status" != 0 ] && [ ! -e unsafe ] || fail 'owner publication overwrote a late record'
  [ "$(cat "$GATE_LOCK.cleanup")" = foreign ] || fail 'owner publication lost the late owner'
  GATE_OWNER=foreign GATE_CLEANUP=1 "$BASH" "$gate" true
  # Failed teardown and a holder that never returns both need recoverable records.
  for mode in failed lost; do
    export GATE_OWNER=$mode
    "$BASH" "$gate" "$BASH" -ec '
      test ! -e /dev/fd/9
      . "$OWN_HELPER"; own_record $$ group >/dev/null; own_write $$ command-pid
      touch service ready
      while [ ! -e finish ]; do sleep 0.01; done
      exit 23
    ' "$GATE_LOCK" > holder.log 2>&1 &
    holder=$!
    wait_for ready "$holder"
    command_pid=$(cat command-pid)
    # An ordinary command cannot enter while the holder or its cleanup is active.
    GATE_OWNER= PATH="$work/poll:$PATH" "$BASH" "$gate" "$BASH" -ec '[ ! -e service ]; touch entered' > contender.log 2>&1 &
    contender=$!
    wait_log 'gate: waiting' contender.log "$contender"
    [ ! -e entered ] || fail "$mode contender bypassed its holder"
    if [ "$mode" = lost ]; then
      kill -KILL "$holder"
      own_signal KILL "-$command_pid"
      status=0; wait "$holder" || status=$?
      [ "$status" = 137 ] || fail "lost holder status was $status"
    else
      touch finish
      status=0; wait "$holder" || status=$?
      [ "$status" = 23 ] || fail "command failure was hidden: $status"
    fi
    wait_log 'gate: waiting for cleanup' contender.log "$contender"
    wait_for polling "$contender"
    [ "$(cat "$GATE_LOCK.cleanup")" = "$mode" ] || fail 'unfinished cleanup lost its owner'
    grep -Fq "$GATE_LOCK.cleanup" contender.log || fail 'waiting message omitted the record path'
    grep -q 'GATE_CLEANUP=1' contender.log || fail 'waiting message omitted recovery'
    [ ! -e entered ] || fail 'contender entered before cleanup'

    # Cleanup for another owner and an ordinary retry by this owner must wait too.
    status=0
    GATE_OWNER=other GATE_CLEANUP=1 GATE_POLL=invalid timeout 10 "$BASH" "$gate" touch wrong > wrong.log 2>&1 || status=$?
    [ "$status" != 0 ] || fail 'another owner entered pending cleanup'
    grep -q 'gate: waiting for cleanup' wrong.log || fail 'another owner did not wait for cleanup'
    status=0
    GATE_POLL=invalid timeout 10 "$BASH" "$gate" touch retry > retry.log 2>&1 || status=$?
    [ "$status" != 0 ] || fail 'ordinary retry entered pending cleanup'
    grep -q 'gate: waiting for cleanup' retry.log || fail 'ordinary retry did not wait for cleanup'
    [ ! -e wrong ] && [ ! -e retry ] || fail 'a non-cleanup command bypassed the record'

    status=0
    GATE_CLEANUP=1 timeout 10 "$BASH" "$gate" "$BASH" -c 'exit 27' > failed.log 2>&1 || status=$?
    [ "$status" = 27 ] || fail 'cleanup failure was hidden'
    [ -e "$GATE_LOCK.cleanup" ] && [ ! -e entered ] || fail 'failed cleanup released the host'

    # A new process performs the documented recovery as the record owner.
    # Teardown must work even with no free disk and keep contenders out until done.
    GATE_CLEANUP=1 GATE_FREE=15 SPACE_FREE='echo 0' "$BASH" "$gate" "$BASH" -ec '
      . "$OWN_HELPER"; own_record $$ group >/dev/null; own_write $$ recovery-pid
      touch cleaning
      while [ ! -e release ]; do sleep 0.01; done
      rm service
    ' "$GATE_LOCK" > recovery.log 2>&1 &
    recovery=$!
    wait_for cleaning "$recovery"
    [ -e "$GATE_LOCK.cleanup" ] && [ ! -e entered ] || fail 'recovery released the host before teardown'
    if [ "$(uname -s)" != Linux ] && [ "$mode" = failed ]; then
      # An exiting waiter must not remove a recovery's new directory lock.
      touch poll-fail poll-release
      status=0; wait "$contender" || status=$?
      [ "$status" = 97 ] || fail 'poll failure was hidden'
      [ -d "$GATE_LOCK" ] && [ "$(cat "$GATE_LOCK/pid")" = "$recovery" ] || fail 'exiting waiter removed the recovery lock'
      GATE_OWNER= "$BASH" "$gate" "$BASH" -ec '[ ! -e service ]; touch entered' > next.log 2>&1 &
      contender=$!
      wait_log 'gate: waiting' next.log "$contender"
      [ ! -e entered ] || fail 'next contender bypassed recovery'
    fi
    touch release
    wait "$recovery" || fail 'recovery failed'
    touch poll-release
    wait "$contender" || fail 'contender did not run after recovery'
    [ ! -e "$GATE_LOCK.cleanup" ] && [ -e entered ] || fail 'successful recovery kept the record'
    GATE_CLEANUP=1 "$BASH" "$gate" true || fail 'repeated cleanup failed'
    own_forget "$(cat command-pid)" "$(cat recovery-pid)"
    rm command-pid recovery-pid ready finish cleaning release entered 2>/dev/null || true
    rm polling poll-release
    rm -f poll-fail
  done
  echo 'gate-test: 2 cleanup recovery scenarios pass'
  unset GATE_OWNER

  # Signals to an owned batch command reach its child and keep teardown locked.
  # Simulate a detached caller; Bash cannot trap signals ignored on entry.
  export SIGNAL_TRACE=$PWD/signal.trace
  cat > "$work/signal-env" <<'SH'
kill() {
  local result=0
  builtin kill "$@" || result=$?
  if [ "$1" != -0 ]; then
    printf 'pid=%s kill %s status=%s\n' "$BASHPID" "$*" "$result" >> "$SIGNAL_TRACE" || true
  fi
  return "$result"
}
SH
  for signal in INT TERM HUP QUIT; do
    printf 'owned command: signal=%s\n' "$signal" > "$SIGNAL_TRACE"
    GATE_OWNER=signal BASH_ENV="$work/signal-env" env --ignore-signal=HUP,TERM env --default-signal=INT,TERM,HUP,QUIT "$BASH" "$gate" "$BASH" -c '
      trap '\''touch signalled; while [ ! -e release ]; do sleep 0.01; done; exit 31'\'' "$1"
      . "$OWN_HELPER"; own_record $$ group >/dev/null; own_write $$ signal-pid
      touch ready
      while :; do sleep 0.01; done
    ' sh "$signal" "$GATE_LOCK" > signal.log 2>&1 &
    holder=$!
    wait_for ready "$holder"
    "$BASH" "$gate" touch entered > contender.log 2>&1 &
    contender=$!
    wait_log 'gate: waiting' contender.log "$contender"
    send_status=0; kill -"$signal" "$holder" || send_status=$?
    printf 'send %s to holder=%s status=%s\n' "$signal" "$holder" "$send_status" >> "$SIGNAL_TRACE"
    [ "$send_status" = 0 ] || fail 'could not signal the holder'
    wait_for signalled "$holder"
    [ ! -e entered ] || fail 'signal released the lock before command teardown'
    touch release
    status=0; wait "$holder" || status=$?
    [ "$status" = 31 ] || fail "signal lost the command status: $status"
    wait_log 'gate: waiting for cleanup' contender.log "$contender"
    GATE_OWNER=signal GATE_CLEANUP=1 "$BASH" "$gate" true
    wait "$contender" || fail 'signal contender failed'
    pid_status=0; signal_pid=$(cat signal-pid) || pid_status=$?
    printf 'signal PID lookup status=%s\n' "$pid_status" >> "$SIGNAL_TRACE"
    own_forget "$signal_pid"
    rm ready signalled release entered signal-pid
  done
  echo 'gate-test: cleanup record types and command signal pass'

  # Inject each signal at launch and immediately before wait, without a timing race.
  for probe_command in 'command_pid=$!' 'wait "$command_pid"'; do
    for probe_signal in INT TERM HUP QUIT; do
      export probe_command probe_signal
      printf 'owned command: signal=%s at %s\n' "$probe_signal" "$probe_command" > "$SIGNAL_TRACE"
      GATE_OWNER=window BASH_ENV="$work/signal-env" env --ignore-signal=HUP,TERM env --default-signal=INT,TERM,HUP,QUIT "$BASH" -c '
        set -T
        trap '\''
          if [ "$BASH_COMMAND" = "$probe_command" ] && [ ! -e injected ]; then
            deadline=$((SECONDS + 10))
            while [ ! -e ready ]; do [ "$SECONDS" -lt "$deadline" ] || exit 98; sleep 0.01; done
            touch injected
            kill -"$probe_signal" "$$"
          fi
        '\'' DEBUG
        source "$0" "$BASH" -c '\''
          trap "touch signalled; while [ ! -e release ]; do sleep 0.01; done; exit 31" "$probe_signal"
          . "$OWN_HELPER"; own_record $$ group >/dev/null; own_write $$ signal-pid
          touch ready
          while :; do sleep 0.01; done
        '\'' "$GATE_LOCK"
      ' "$gate" > window.log 2>&1 &
      holder=$!
      wait_for ready "$holder"
      wait_for injected "$holder"
      wait_for signalled "$holder"
      "$BASH" "$gate" touch entered > contender.log 2>&1 &
      contender=$!
      wait_log 'gate: waiting' contender.log "$contender"
      [ ! -e entered ] || fail 'window signal released the lock during teardown'
      touch release
      status=0; wait "$holder" || status=$?
      [ "$status" = 31 ] || fail "window signal lost the command status: $status"
      wait_log 'gate: waiting for cleanup' contender.log "$contender"
      GATE_OWNER=window GATE_CLEANUP=1 "$BASH" "$gate" true
      wait "$contender" || fail 'window signal contender failed'
      pid_status=0; signal_pid=$(cat signal-pid) || pid_status=$?
      printf 'signal PID lookup status=%s\n' "$pid_status" >> "$SIGNAL_TRACE"
      own_forget "$signal_pid"
      rm ready injected signalled release entered signal-pid
    done
  done
  rm -f "$SIGNAL_TRACE"
  unset SIGNAL_TRACE
  echo 'gate-test: 8 signal windows pass'

  if [ "$(uname -s)" != Linux ]; then
    # A signal during directory release must not let EXIT remove the next lock.
    mkdir "$work/release-bin"
    export real_rm gate
    real_rm=$(command -v rm)
    printf '#!%s\n' "$BASH" > "$work/release-bin/rm"
    cat >> "$work/release-bin/rm" <<'SH'
set -euo pipefail
. "$OWN_HELPER"; own_record "$PPID" >/dev/null
"$real_rm" "$@"
if [ "$*" = "-rf $GATE_LOCK" ] && [ "$PPID" = "${releaser_pid:-}" ] && [ ! -e injected ]; then
  touch injected
  env -u releaser_pid GATE_OWNER=release GATE_CLEANUP=1 "$BASH" "$gate" "$BASH" -ec '
    . "$OWN_HELPER"; own_record $$ group >/dev/null; own_write $$ recovery-pid
    touch ready
    while [ ! -e release ]; do sleep 0.01; done
  ' "$GATE_LOCK" > recovery.log 2>&1 &
  . "$OWN_HELPER"; own_write $! recovery-holder
  deadline=$((SECONDS + 10))
  while [ ! -e ready ]; do [ "$SECONDS" -lt "$deadline" ] || exit 98; sleep 0.01; done
  own_signal TERM "$PPID"
fi
SH
    chmod +x "$work/release-bin/rm"
    printf 'release\n' > "$GATE_LOCK.cleanup"
    PATH="$work/release-bin:$PATH" env --ignore-signal=TERM env --default-signal=TERM "$BASH" -c 'export releaser_pid=$$; source "$0" true' "$gate" > release.log 2>&1 &
    holder=$!
    wait_for ready "$holder"
    wait_log '^[0-9][0-9]*$' recovery-holder "$holder"
    recovery=$(cat recovery-holder)
    status=0; wait "$holder" || status=$?
    [ "$status" = 143 ] || fail "release signal status was $status"
    [ -d "$GATE_LOCK" ] && [ "$(cat "$GATE_LOCK/pid")" = "$recovery" ] || fail 'release signal removed the live recovery lock'
    "$BASH" "$gate" touch entered > contender.log 2>&1 &
    contender=$!
    wait_log 'gate: waiting' contender.log "$contender"
    [ ! -e entered ] || fail 'release signal let a contender bypass recovery'
    touch release
    wait "$contender" || fail 'release contender failed after recovery'
    [ ! -e "$GATE_LOCK.cleanup" ] || fail 'release recovery left its record'
    own_forget "$(cat recovery-pid)"
    rm ready injected release entered recovery-pid recovery-holder
    echo 'gate-test: signal during directory release passes'
  fi
}
