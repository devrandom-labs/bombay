#!/usr/bin/env bash
# Self-test of the space script, with scripted Nix, Docker and disk space.
# Run from the repository root.
set -euo pipefail
space=$PWD/architecture/space.sh
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
fail() { echo "space-test: FAILED: $1"; cat "$work/out" 2> /dev/null; exit 1; }
# A Nix whose garbage collection frees the GiB in `gc`, once for each line.
mkdir "$work/bin" "$work/nops" "$work/tools" "$work/docker"
cp "${space%/*}/gate.sh" "$space" "$work/"
# Gate invocations use only these tools, so they cannot reach the host Docker.
for tool in bash cat chmod cut dirname du env grep ln mkdir mktemp readlink sed test touch tr uname wc; do
  ln -s "$(type -P "$tool")" "$work/tools/$tool"
done
if [ "$(uname -s)" = Linux ]; then
  ln -s "$(command -v flock)" "$work/tools/flock"
fi
printf '#!%s\n' "$BASH" > "$work/bin/nix"
cat >> "$work/bin/nix" << 'STUB'
echo "$*" >> "$STUB_DIR/nix.calls"
if [ "$1 $2" = 'profile wipe-history' ]; then
  echo profile >> "$STUB_DIR/events"
  [ "${PROFILE_STATUS:-0}" -eq 0 ] || exit "$PROFILE_STATUS"
  profile=$4
  current=$(readlink "$profile")
  for generation in "$profile"-*-link; do
    [ "${generation##*/}" = "$current" ] || "$REAL_RM" -f "$generation"
  done
  exit 0
fi
echo gc >> "$STUB_DIR/events"
gain=$(sed -n "$(grep -c '^store gc$' "$STUB_DIR/nix.calls")p" "$STUB_DIR/gc" 2> /dev/null)
echo $(($(cat "$STUB_DIR/free") + ${gain:-0})) > "$STUB_DIR/free"
STUB
# Two process lists that do not work: one fails, one leaves out the asker.
mkdir "$work/partial" "$work/process"
printf '#!/bin/sh\nexit 1\n' > "$work/nops/ps"
printf '#!/bin/sh\necho "    1 /sbin/launchd"\n' > "$work/partial/ps"
chmod +x "$work/bin/nix" "$work/nops/ps" "$work/partial/ps"
printf '#!%s\n' "$BASH" > "$work/docker/docker"
cat >> "$work/docker/docker" << 'STUB'
printf '%s\n' "$*" >> "$STUB_DIR/docker.calls"
if [ "$*" = 'context inspect --format {{.Endpoints.docker.Host}}' ]; then
  echo inspect >> "$STUB_DIR/events"
  printf '%s\n' "$DOCKER_ENDPOINT"
  exit 0
fi
echo prune >> "$STUB_DIR/events"
[ "$*" = "--host $DOCKER_ENDPOINT image prune --all --force" ] || exit 1
echo $(($(cat "$STUB_DIR/free") + DOCKER_GAIN)) > "$STUB_DIR/free"
exit "$DOCKER_STATUS"
STUB
chmod +x "$work/docker/docker"
# A complete command list, including the asker; model truncation without -ww.
printf '#!%s\n' "$BASH" > "$work/process/ps"
cat >> "$work/process/ps" << 'STUB'
echo "$*" >> "$STUB_DIR/ps.calls"
echo "$SPACE_TEST_PID bash"
echo "$SPACE_TEST_PARENT bash"
case " $* " in
  *' -ww -o pid=,args= '*)
    printf '%s\n' "${PROCESS_ARGS:-}"
    if grep -qx target "$STUB_DIR/events"; then printf '%s\n' "${PROCESS_ARGS_LATER:-}"; fi ;;
esac
STUB
chmod +x "$work/process/ps"
# Record removals and their disk gain; failures work even for root.
real_rm=$(command -v rm); real_find=$(command -v find)
printf '#!%s\n' "$BASH" > "$work/bin/rm"
cat >> "$work/bin/rm" << 'STUB'
case "${2:-$1}" in
  "$GATE_LOCK" | "$GATE_LOCK".*) exec "$REAL_RM" "$@" ;;
esac
if [ "$1" = -rf ]; then
  printf '%s\0' "$2" >> "$STUB_DIR/rm.calls"
  [ "$2" != "${RM_FAIL:-}" ] || exit 1
  "$REAL_RM" "$@" || exit 1
  case "$2" in
    "$PWD/target/"*) echo own >> "$STUB_DIR/events" ;;
    *) echo target >> "$STUB_DIR/events" ;;
  esac
  echo $(($(cat "$STUB_DIR/free") + $(cat "$STUB_DIR/rm.gain"))) > "$STUB_DIR/free"
else
  echo link >> "$STUB_DIR/events"
  exec "$REAL_RM" "$@"
fi
STUB
printf '#!%s\n' "$BASH" > "$work/bin/find"
cat >> "$work/bin/find" << 'STUB'
activity=0 previous= discovery=0
for arg in "$@"; do
  if [ "$previous" = -mmin ] && [[ "$arg" = -* ]]; then activity=1; fi
  previous=$arg; [ "$arg" != '{}/.rustc_info.json' ] || discovery=1
done
if [ "$activity" -eq 1 ]; then
  echo scan >> "$STUB_DIR/scans"
  # A build starts after the list was made, before the removal check.
  scans=$(wc -l < "$STUB_DIR/scans" | tr -d ' ')
  if [ -n "${WRITE_LATER:-}" ] && [ "$scans" -eq 2 ]; then touch "$WRITE_LATER"; fi
  case "${SCAN_FAIL:-0}:$scans" in
    1:*|first:1|second:2) exit 1 ;;
  esac
fi
found=$STUB_DIR/found.$$
"$REAL_FIND" "$@" > "$found"
status=$?
[ "$discovery" -eq 0 ] || cat "$found" >> "$STUB_DIR/discovered"
cat "$found"
exit "$status"
STUB
chmod +x "$work/bin/rm" "$work/bin/find"
# Script disk space and old/new result links in an isolated worktree.
case_number=0
fixture() {
  case_number=$((case_number + 1))
  dir=$work/case$case_number
  mkdir -p "$dir/tree/.cache/roots" "$dir/tree/architecture" "$dir/user" "$dir/builds"; cd "$dir/tree"
  : > architecture/space.sh
  echo "$1" > "$dir/free"; printf '%s\n' "$2" | tr '|' '\n' > "$dir/gc"; : > "$dir/nix.calls"
  echo 0 > "$dir/rm.gain"; : > "$dir/rm.calls"; : > "$dir/discovered"; : > "$dir/events"
  ln -s /nix/store/old-result .cache/roots/old; touch -h -t 202601010000 .cache/roots/old
  ln -s /nix/store/new-result .cache/roots/new
  ln -s /nix/store/old-user-result "$dir/user/old"; touch -h -t 202601010000 "$dir/user/old"
  ln -s "$dir/free" .cache/roots/other; touch -h -t 202601010000 .cache/roots/other
  ln -s /nix/store/old-result outside; touch -h -t 202601010000 outside
}
run() {
  got=0
  local docker_path=
  local invocation=("$space" "$@")
  if [ -n "${DOCKER_GAIN:-}" ]; then docker_path=$work/docker:; fi
  if [ "${AT_GATE:-0}" -eq 1 ]; then
    invocation=("$work/gate.sh" bash -c 'echo gate >> "$STUB_DIR/events"')
  fi
  env PATH="${docker_path}$work/${NOPS:-process}:$work/bin:$work/tools" STUB_DIR="$dir" \
    DOCKER_GAIN="${DOCKER_GAIN:-0}" DOCKER_STATUS="${DOCKER_STATUS:-0}" \
    DOCKER_ENDPOINT="${DOCKER_ENDPOINT:-unix:///var/run/docker.sock}" \
    SPACE_FREE="cat $dir/free" SPACE_CACHE="$dir/user" \
    GATE_LOCK="$dir/gate.lock" GATE_FREE="${1:-15}" \
    PROFILE_STATUS="${PROFILE_STATUS:-0}" \
    SPACE_TEST_PARENT=$$ PROCESS_ARGS="${PROCESS_ARGS:-}" PROCESS_ARGS_LATER="${PROCESS_ARGS_LATER:-}" \
    REAL_RM="$real_rm" REAL_FIND="$real_find" RM_FAIL="${RM_FAIL:-}" SCAN_FAIL="${SCAN_FAIL:-0}" WRITE_LATER="${WRITE_LATER:-}" \
    ${AGE:+SPACE_AGE=$AGE} SPACE_BUILDS="$dir/builds" SPACE_BUILDER=bash bash -c 'export SPACE_TEST_PID=$$; exec bash "$@"' space-test "${invocation[@]}" > "$work/out" 2>&1 || got=$?
  collections=$(grep -c '^store gc$' "$dir/nix.calls" || true)
}
links() {
  local name
  for name in .cache/roots/old .cache/roots/new "$dir/user/old" .cache/roots/other outside; do
    [ -L "$name" ] && printf '%s ' "${name##*/}"
  done
  echo
}
all="old new old other outside "
# Cargo-shaped trees, with old timestamps unless a case overwrites them.
aged() {
  local minutes=$1 path=$2 stamp
  stamp=$(date -u -v-"${minutes}"M +%Y%m%d%H%M.%S 2> /dev/null) || stamp=$(date -u -d "$minutes minutes ago" +%Y%m%d%H%M.%S)
  TZ=UTC touch -t "$stamp" "$path"
}
age_tree() {
  : > "$work/age"; aged "$1" "$work/age"
  find "$2" -exec touch -r "$work/age" {} +
}
target() {
  mkdir -p "$1/debug/.fingerprint/unit" "$1/debug/build/unit/out" "$1/debug/deps" "$1/debug/incremental"
  echo '{}' > "$1/.rustc_info.json"; : > "$1/CACHEDIR.TAG"
  touch "$1/debug/deps/unit.o" "$1/debug/.cargo-lock" "$1/debug/.fingerprint/unit/invoked.timestamp" "$1/debug/build/unit/invoked.timestamp"
  age_tree "${2:-240}" "$1"
}
name="old mutant target goes; source and step logs stay; Cargo cleanup precedes collection"
fixture 5 '12'
target .cache/331-8c/mutant/target
mkdir .cache/331-8c/mutant/source; echo log > .cache/331-8c/step.log
echo 12 > "$dir/rm.gain"; run 15
[ "$got" -eq 0 ] && [ "$collections" -eq 0 ] && [ ! -e .cache/331-8c/mutant/target/debug ] || fail "$name"
[ -d .cache/331-8c/mutant/source ] && [ -f .cache/331-8c/step.log ] && [ "$(links)" = "$all" ] || fail "$name: neighbors"
grep -q '^space: removed 1 old Cargo profile folders; 17 GiB are free now$' "$work/out" || fail "$name: report"
for minutes in 10 170 190; do
  name="target with a fingerprint write $minutes minutes ago"
  fixture 5 '12'; target .cache/target
  aged "$minutes" .cache/target/debug/.fingerprint/unit/invoked.timestamp; run 15
  if [ "$minutes" -le 180 ]; then
    [ -d .cache/target/debug ] || fail "$name: active target removed"
    [ "$(cat "$dir/scans")" = scan ] && [ ! -s "$dir/ps.calls" ] || fail "$name: active target was checked again"
  else
    [ ! -e .cache/target/debug ] || fail "$name: old target kept"
  fi
done
for folder in debug/deps debug debug/.fingerprint; do
  name="a recent $folder folder over old files keeps the profile"
  fixture 5 '12'; target .cache/target
  touch ".cache/target/$folder"
  [ "$folder" != debug ] || touch .cache/target/debug/.fingerprint
  run 15
  [ -d .cache/target/debug ] || fail "$name"
done
name="only marked profiles go; evidence, root files and unmarked folders stay; repeat costs nothing"
fixture 5 '12'; target .cache/target
cp -R .cache/target/debug .cache/target/release
: > .cache/target/.cargo-lock
mkdir .cache/target/debug/nested; : > .cache/target/debug/nested/.cargo-lock
mkdir -p .cache/target/studio-evidence .cache/target/nextest .cache/target/doc .cache/target/package .cache/target/tmp
printf 'evidence\n' > .cache/target/studio-evidence/log; echo note > .cache/target/notes.txt
age_tree 240 .cache/target; run 15
[ ! -d .cache/target/debug ] && [ ! -d .cache/target/release ] || fail "$name: profiles"
[ "$(cat .cache/target/studio-evidence/log)" = evidence ] && [ "$(cat .cache/target/notes.txt)" = note ] || fail "$name: evidence"
for entry in .rustc_info.json CACHEDIR.TAG .cargo-lock nextest doc package tmp; do
  [ -e ".cache/target/$entry" ] || fail "$name: $entry"
done
[ "$(tr -cd '\000' < "$dir/rm.calls" | wc -c | tr -d ' ')" -eq 2 ] || fail "$name: removal count"
grep -q 'removed 2 old Cargo profile folders' "$work/out" || fail "$name: reported count"
: > "$dir/rm.calls"; : > "$dir/discovered"; : > "$dir/nix.calls"; echo 5 > "$dir/free"
age_tree 240 .cache/target; run 15
[ -s "$dir/discovered" ] && [ ! -s "$dir/rm.calls" ] || fail "$name: repeat"
grep -q 'old Cargo profile folders' "$work/out" && fail "$name: repeat reported a removal"
name="cross-target marked profile goes; unmarked debug and linked profiles stay"
fixture 5 '12'; target .cache/target; target "$dir/elsewhere"
mkdir -p .cache/target/extra/deep/profile; : > .cache/target/extra/deep/profile/.cargo-lock
mkdir .cache/target/triple; mv .cache/target/debug .cache/target/triple/debug
mkdir .cache/target/debug; echo keep > .cache/target/debug/note
ln -s "$dir/elsewhere/debug" .cache/target/release
mkdir .cache/target/linked; ln -s "$dir/elsewhere/debug" .cache/target/linked/custom
age_tree 240 .cache/target; run 15
[ ! -e .cache/target/triple/debug ] && [ -d .cache/target/triple ] && [ -f .cache/target/extra/deep/profile/.cargo-lock ] || fail "$name: triple"
[ "$(cat .cache/target/debug/note)" = keep ] && [ -L .cache/target/release ] && [ -L .cache/target/linked/custom ] && [ -f "$dir/elsewhere/debug/.cargo-lock" ] || fail "$name: preserved"
name="cache root is never a target and does not hide deeper targets"
fixture 5 '12'; rm -r .cache/roots; target .cache; target .cache/deeper/target
age_tree 240 .cache; run 15
[ -d .cache/debug ] && [ ! -e .cache/deeper/target/debug ] || fail "$name"
name="pre-existing target without CACHEDIR.TAG goes; a target name or cache tag alone stays"
fixture 5 '12'; target .cache/pre-existing
rm .cache/pre-existing/CACHEDIR.TAG; aged 240 .cache/pre-existing
mkdir .cache/target .cache/tag-only; : > .cache/tag-only/CACHEDIR.TAG
aged 240 .cache/target; aged 240 .cache/tag-only; run 15
[ ! -e .cache/pre-existing/debug ] && [ -d .cache/target ] && [ -d .cache/tag-only ] || fail "$name"
name="both old targets go when the first frees enough space"
fixture 5 '12'; target .cache/a; target .cache/z
echo 12 > "$dir/rm.gain"; run 15
[ "$got" -eq 0 ] && [ ! -s "$dir/nix.calls" ] && [ ! -e .cache/a/debug ] && [ ! -e .cache/z/debug ] || fail "$name"
[ "$(cat "$work/out")" = 'space: removed 2 old Cargo profile folders; 29 GiB are free now' ] || fail "$name: report"
name="enough space removes idle profiles and keeps recent, named and ordinary folders"
fixture 20 ''; target .cache/idle; target .cache/recent 170
target '.cache/running test'; target .cache/named; target .cache/relative
cp -R .cache/idle/debug .cache/idle/release; age_tree 240 .cache/idle
mkdir -p .cache/ordinary/debug; touch .cache/ordinary/debug/.cargo-lock; age_tree 240 .cache/ordinary
PROCESS_ARGS=$(printf '123 %s\n456 cargo --target-dir %s\n789 cargo --target-dir=./.cache/relative' \
  '.cache/running test/debug/deps/unit' "$PWD/.cache/named") run 15
[ "$got" -eq 0 ] && [ ! -s "$dir/nix.calls" ] && [ "$(links)" = "$all" ] || fail "$name: later cleanup"
[ ! -e .cache/idle/debug ] && [ ! -e .cache/idle/release ] || fail "$name: idle profiles kept"
for kept in recent 'running test' named relative ordinary; do
  [ -d ".cache/$kept/debug" ] || fail "$name: $kept removed"
done
[ "$(cat "$work/out")" = 'space: removed 2 old Cargo profile folders; 20 GiB are free now' ] || fail "$name: report"
name="a process that starts during cleanup protects the next target"
fixture 20 ''; target .cache/a; target .cache/b
PROCESS_ARGS_LATER=$(printf '123 %s\n456 %s' "$PWD/.cache/a/debug/deps/unit" "$PWD/.cache/b/debug/deps/unit") run 15
[ "$got" -eq 0 ] && [ "$(cat "$dir/events")" = target ] || fail "$name: removal count"
[[ -d .cache/a/debug || -d .cache/b/debug ]] || fail "$name: active profile removed"
for list in nops partial; do
  name="enough space keeps old cached targets when the process list fails: $list"
  fixture 20 ''; target .cache/target; NOPS=$list run 15
  [ "$got" -eq 0 ] && [ -d .cache/target/debug ] && [ ! -s "$dir/rm.calls" ] && [ ! -s "$work/out" ] || fail "$name"
done
name="outside a Labcast worktree the cache and its old targets stay"
fixture 5 '2|9'; target .cache/target; rm -r architecture; run 15
[ "$got" -eq 0 ] && [ -d .cache/target/debug ] && [ -L .cache/roots/old ] || fail "$name"
name="user cache, worktree target and outside folders stay"
fixture 5 '12'; target "$dir/user/target"; target target; target "$dir/elsewhere"; run 15
[ -d "$dir/user/target/debug" ] && [ -d target/debug ] && [ -d "$dir/elsewhere/debug" ] || fail "$name"
name="a cache symlink to an outside target and its target both stay"
fixture 5 '12'; target "$dir/elsewhere"; ln -s "$dir/elsewhere" .cache/target
touch -h -t 202601010000 .cache/target; run 15
[ -L .cache/target ] && [ -d "$dir/elsewhere" ] && [ ! -s "$dir/rm.calls" ] && [ ! -s "$dir/discovered" ] || fail "$name"
name="a symlink used as .cache and its outside target both stay"
fixture 5 '12'; rm -r .cache; target "$dir/elsewhere"; ln -s "$dir/elsewhere" .cache
touch -h -t 202601010000 .cache; run 15
[ -L .cache ] && [ -d "$dir/elsewhere" ] && [ ! -s "$dir/rm.calls" ] || fail "$name"
name="a symlink to a Cargo marker does not make a folder a target"
fixture 5 '12'; target "$dir/elsewhere"; mkdir -p .cache/target/debug
: > .cache/target/debug/.cargo-lock
ln -s "$dir/elsewhere/.rustc_info.json" .cache/target/.rustc_info.json
age_tree 240 .cache/target; run 15
[ -d .cache/target/debug ] && [ -f "$dir/elsewhere/.rustc_info.json" ] || fail "$name"
name="nested Cargo target is not discovered; each removed profile counts"
fixture 5 '12'; target .cache/outer/inner; target .cache/outer
aged 240 .cache/outer/inner; run 15
[ ! -e .cache/outer/debug ] && [ ! -e .cache/outer/inner/debug ] && [ -f .cache/outer/inner/.rustc_info.json ] || fail "$name"
[ "$(tr -cd '\000' < "$dir/discovered" | wc -c | tr -d ' ')" -eq 1 ] || fail "$name: nested target discovered"
[ "$(tr -cd '\000' < "$dir/rm.calls" | wc -c | tr -d ' ')" -eq 2 ] || fail "$name: nested target counted"
name="folder names with a space and a line break"
fixture 5 '12'; target '.cache/with space'; target $'.cache/with\nbreak\n'; run 15
[ ! -e '.cache/with space/debug' ] && [ ! -e $'.cache/with\nbreak\n/debug' ] || fail "$name"
grep -q 'removed 2 old Cargo profile folders' "$work/out" || fail "$name: count"
name="an unremovable target continues to the next target and next action"
fixture 5 '12'; target .cache/a 300; target .cache/b
RM_FAIL=$PWD/.cache/a/debug run 15
[ "$got" -eq 0 ] && [ "$collections" -eq 1 ] && [ -d .cache/a/debug ] && [ ! -e .cache/b/debug ] || fail "$name"
grep -q 'removed 1 old Cargo profile folders; 5 GiB' "$work/out" || fail "$name: failed removal counted"
name="the later order stays: nix store gc, then result links, then gc again"
fixture 5 '2|9'; target .cache/target; run 15
[ "$got" -eq 0 ] && [ "$(tr '\n' ' ' < "$dir/events")" = 'target gc link link gc ' ] || fail "$name"
name="SPACE_AGE changes the idle limit"
fixture 5 '12'; target .cache/target 181; AGE=300 run 15
[ -d .cache/target/debug ] || fail "$name"
name="cross-target profile fingerprint write keeps an old target"
fixture 5 '12'; target .cache/target
mkdir -p .cache/target/triple/custom/.fingerprint/unit
: > .cache/target/triple/custom/.fingerprint/unit/invoked.timestamp
age_tree 240 .cache/target
aged 10 .cache/target/triple/custom/.fingerprint/unit/invoked.timestamp; run 15
[ -d .cache/target/debug ] || fail "$name"
for folder in deps incremental out; do
  name="a target named $folder is still checked for recent writes"
  fixture 5 '12'; target ".cache/$folder"
  aged 10 ".cache/$folder/debug/.fingerprint/unit/invoked.timestamp"; run 15
  [ -d ".cache/$folder/debug" ] || fail "$name"
done
name="a custom profile named out is still checked for recent writes"
fixture 5 '12'; target .cache/target
mv .cache/target/debug .cache/target/out; aged 240 .cache/target
aged 10 .cache/target/out/.fingerprint/unit/invoked.timestamp; run 15
[ -d .cache/target/out ] || fail "$name"
name="activity search is bounded and prunes deps, incremental and build-script out"
fixture 5 '12'; target .cache/target
for leaf in debug/deps debug/incremental debug/build/unit/out extra/a/b/c/d; do
  mkdir -p ".cache/target/$leaf"; : > ".cache/target/$leaf/not-visited"
done
age_tree 240 .cache/target
for leaf in debug/deps debug/incremental debug/build/unit/out extra/a/b/c/d; do
  touch ".cache/target/$leaf/not-visited"
done
run 15
[ ! -d .cache/target/debug ] || fail "$name: walked artifact payloads"
name="an internal symlink does not supply activity from outside the target"
fixture 5 '12'; target .cache/target; : > "$dir/recent"
ln -s "$dir/recent" .cache/target/recent; aged 240 .cache/target; run 15
[ ! -e .cache/target/debug ] && [ -f "$dir/recent" ] || fail "$name"
for scan in first second; do
  name="a failed $scan activity scan keeps the target and continues cleanup"
  fixture 5 '12'; target .cache/target; SCAN_FAIL=$scan run 15
  [ "$got" -eq 0 ] && [ "$collections" -eq 1 ] && [ -d .cache/target/debug ] || fail "$name"
done
name="a build that starts after discovery keeps its target"
fixture 5 '12'; target .cache/target
WRITE_LATER=$PWD/.cache/target/debug/.fingerprint/unit/invoked.timestamp run 15
[ "$got" -eq 0 ] && [ -d .cache/target/debug ] || fail "$name"
dependency_generations() {
  mkdir "$dir/user/dependencies"
  ln -s /nix/store/old-dependencies "$dir/user/dependencies/profile-1-link"
  ln -s /nix/store/current-dependencies "$dir/user/dependencies/profile-2-link"
  ln -s profile-2-link "$dir/user/dependencies/profile"
  touch -h -t 202601010000 "$dir/user/dependencies/"*
}
name="enough space with no idle target changes and prints nothing"
fixture 20 ''; target .cache/recent 170; dependency_generations; run 15
[ "$got" -eq 0 ] && [ -d .cache/recent/debug ] && [ "$(links)" = "$all" ] || fail "$name"
[ -L "$dir/user/dependencies/profile-1-link" ] || fail "$name: old generation removed"
[ ! -s "$dir/nix.calls" ] && [ ! -s "$dir/rm.calls" ] && [ ! -s "$work/out" ] || fail "$name: unexpected cleanup or report"
name="a collection that frees enough keeps the result links"
fixture 5 '12'; run 15
[ "$got" -eq 0 ] && [ "$collections" -eq 1 ] && [ "$(links)" = "$all" ] || fail "$name"
name="old result links of the caches go when the first collection is not enough"
fixture 5 '2|9'; run 15
[ "$got" -eq 0 ] && [ "$collections" -eq 2 ] || fail "$name: status $got, $collections collections"
[ "$(links)" = "new other outside " ] || fail "$name: links left: $(links)"
name="a folder that is no Labcast worktree keeps the links of its .cache"
fixture 5 '2|9'; rm -r architecture; run 15
[ "$got" -eq 0 ] && [ "$collections" -eq 2 ] || fail "$name: status $got, $collections collections"
[ "$(links)" = "old new other outside " ] || fail "$name: links left: $(links)"
name="cleanup keeps the current dependency profile even after a day and removes older generations"
fixture 5 '2|9'; dependency_generations
run 15
[ "$got" -eq 0 ] && [ "$collections" -eq 2 ] || fail "$name: status $got, $collections collections"
[ -L "$dir/user/dependencies/profile" ] && [ -L "$dir/user/dependencies/profile-2-link" ] || fail "$name: current root lost"
[ ! -L "$dir/user/dependencies/profile-1-link" ] || fail "$name: old root kept"
[ "$(links)" = "new other outside " ] || fail "$name: ordinary old results kept"

name="cleanup removes old dependency generations when their profile link is absent"
fixture 5 '2|9'
mkdir "$dir/user/dependencies"
ln -s /nix/store/old-dependencies "$dir/user/dependencies/profile-1-link"
touch -h -t 202601010000 "$dir/user/dependencies/profile-1-link"
run 15
[ "$got" -eq 0 ] && [ "$collections" -eq 2 ] || fail "$name: status $got, $collections collections"
[ ! -L "$dir/user/dependencies/profile-1-link" ] || fail "$name: orphan root kept"
[ "$(links)" = "new other outside " ] || fail "$name: ordinary old results kept"

name="a failed profile cleanup warns, keeps the generations and lets collection run"
fixture 5 '12'; dependency_generations
PROFILE_STATUS=1 run 15
[ "$got" -eq 0 ] && [ "$collections" -eq 1 ] || fail "$name: status $got, $collections collections"
grep -q 'space: could not remove old dependency generations; keeping them' "$work/out" || fail "$name: no warning"
grep -qF "$dir/user/dependencies/profile" "$work/out" || fail "$name: no profile path in warning"
[ -L "$dir/user/dependencies/profile-1-link" ] || fail "$name: old root lost"
[ -L "$dir/user/dependencies/profile-2-link" ] || fail "$name: current root lost"

name="the gate and cleanup share the dependency profile path; --profile leaves idle targets"
fixture 20 ''; target .cache/target; run --profile
[ "$got" -eq 0 ] && [ "$(cat "$work/out")" = "$dir/user/dependencies/profile" ] || fail "$name"
[ -d .cache/target/debug ] && [ ! -s "$dir/rm.calls" ] || fail "$name: profile removed"

name="a disk that stays full is reported"
fixture 5 '1|1'; run 15
[ "$got" -eq 1 ] || fail "$name: status $got, wanted 1"
[ "$collections" -eq 2 ] || fail "$name: $collections collections, wanted 2"
grep -q '7 GiB free after the cleanup, 15 needed' "$work/out" || fail "$name: no report"
grep -q 'Docker' "$work/out" && fail "$name: Docker was reported with no client available"

if [ "$(uname -s)" = Linux ]; then
  name="Docker image prune follows Nix cleanup when the disk is still short"
  fixture 5 '1|1'; AT_GATE=1 DOCKER_GAIN=8 DOCKER_ENDPOINT=unix:///run/user/1000/docker.sock run 15
  [ "$got" -eq 0 ] || fail "$name: status $got, wanted 0"
  [ "$(cat "$dir/events")" = $'gc\nlink\nlink\ngc\ninspect\nprune\ngate' ] || fail "$name: cleanup order"
  grep -Fxq -- '--host unix:///run/user/1000/docker.sock image prune --all --force' "$dir/docker.calls" || fail "$name: Docker arguments"

  name="the full gate preserves its refusal without Docker"
  fixture 5 '1|1'; AT_GATE=1 run 15
  [ "$got" -eq 1 ] || fail "$name"
  grep -Fxq 'gate: not enough free disk space for the full gate' "$work/out" || fail "$name: no refusal"
  grep -q 'Docker' "$work/out" && fail "$name: Docker was reported with no client available"

  name="Docker image prune is skipped when Nix frees enough space"
  fixture 5 '1|9'; AT_GATE=1 DOCKER_GAIN=8 run 15
  [ "$got" -eq 0 ] && [ ! -e "$dir/docker.calls" ] || fail "$name"

  name="Docker image prune still refuses when the disk stays short"
  fixture 5 '1|1'; AT_GATE=1 DOCKER_GAIN=1 run 15
  [ "$got" -eq 1 ] && [ -s "$dir/docker.calls" ] || fail "$name"
  grep -q '^space: 8 GiB free after the cleanup, 15 needed$' "$work/out" || fail "$name: no report"

  name="Docker image prune can free enough space before reporting an error"
  fixture 5 '1|1'; AT_GATE=1 DOCKER_GAIN=8 DOCKER_STATUS=1 run 15
  [ "$got" -eq 0 ] && [ -s "$dir/docker.calls" ] || fail "$name"

  name="the gate leaves a remote Docker endpoint alone"
  fixture 5 '1|1'; AT_GATE=1 DOCKER_GAIN=8 DOCKER_ENDPOINT=ssh://docker.example run 15
  [ "$got" -eq 1 ] || fail "$name"
  [ "$(cat "$dir/events")" = $'gc\nlink\nlink\ngc\ninspect' ] || fail "$name: cleanup order"

  name="cleanup-only calls leave Docker alone"
  fixture 5 '1|1'; DOCKER_GAIN=8 run 15
  [ "$got" -eq 1 ] && [ ! -e "$dir/docker.calls" ] || fail "$name"
else
  name="Docker image prune is skipped outside Linux"
  fixture 5 '1|1'; AT_GATE=1 DOCKER_GAIN=8 run 15
  [ "$got" -eq 1 ] && [ ! -e "$dir/docker.calls" ] || fail "$name"
fi
name="a need that is not a number changes nothing"
fixture 5 '12'; run many
[ "$got" -eq 2 ] && [ "$collections" -eq 0 ] && [ "$(links)" = "$all" ] || fail "$name: status $got"
name="--builds removes nothing when the process list does not work"
fixture 5 ''
sh -c 'exit 0' & dead=$!; wait "$dead"
mkdir "$dir/builds/nix-$dead-1"
for list in nops partial; do
  NOPS=$list run --builds
  [ "$got" -eq 1 ] || fail "$name ($list): status $got, wanted 1"
  [ -d "$dir/builds/nix-$dead-1" ] || fail "$name ($list): a folder was removed"
  NOPS=$list run 15
  grep -q 'leftover Nix build folders' "$work/out" && fail "$name ($list): a folder was called leftover"
done
# The scripted process list includes this test and omits the ended build.
name="a full disk names the leftover build folders and their command"
fixture 5 ''
rm .cache/roots/old "$dir/user/old"
mkdir "$dir/builds/nix-$dead-1" "$dir/builds/nix-$$-1" "$dir/builds/other" "$dir/builds/nix-shell-1" "$dir/builds/nix-$dead-x"
run 15
[ "$got" -eq 1 ] || fail "$name: status $got, wanted 1"
[ "$collections" -eq 1 ] || fail "$name: a second collection ran although no link was removed"
grep -q '^space: 1 leftover Nix build folders' "$work/out" || fail "$name: wrong report"
grep -q 'space.sh --builds' "$work/out" || fail "$name: no command"
name="--builds removes only the folders of ended builds"
target .cache/target; run --builds
[ "$got" -eq 0 ] || fail "$name: status $got"
[ ! -e "$dir/builds/nix-$dead-1" ] || fail "$name: the folder of an ended build remains"
[ -d "$dir/builds/nix-$$-1" ] || fail "$name: the folder of a running build was removed"
[ -d "$dir/builds/other" ] && [ -d "$dir/builds/nix-shell-1" ] && [ -d "$dir/builds/nix-$dead-x" ] \
  || fail "$name: a folder that is not a build folder was removed"
grep -q 'removed 1 leftover' "$work/out" || fail "$name: wrong count"
[ "$(links)" = "new other outside " ] || fail "$name: --builds removed a link"
[ -d .cache/target/debug ] || fail "$name: idle Cargo profile removed"
name="own target removes debug, release and triple/debug; root files and evidence stay"
fixture 5 ''; target target
cp -R target/debug target/release; mkdir target/triple; cp -R target/debug target/triple/debug
mkdir target/tmp target/studio-evidence; echo proof > target/studio-evidence/log
# A recent file is no reason to retain an unused own target.
touch target/debug/deps/unit.o; sleep 1
echo 4 > "$dir/rm.gain"; run --own-target
[ "$got" -eq 0 ] && [ "$collections" -eq 0 ] || fail "$name: status or gc"
for profile in debug release triple/debug; do [ ! -e "target/$profile" ] || fail "$name: $profile"; done
for entry in tmp CACHEDIR.TAG .rustc_info.json studio-evidence/log; do [ -e "target/$entry" ] || fail "$name: $entry"; done
grep -q '^space: removed 3 own Cargo profile folders; 17 GiB are free now$' "$work/out" || fail "$name: count"
[ "$(cat "$dir/ps.calls")" = '-A -ww -o pid=,args=' ] || fail "$name: process list must be read once with full arguments"

name="without own-target the same own tree stays and exits 1"
fixture 5 ''; target target; cp -R target/debug target/release; mkdir target/triple; cp -R target/debug target/triple/debug
run 15
[ "$got" -eq 1 ] && [ -d target/debug ] && [ -d target/release ] && [ -d target/triple/debug ] || fail "$name"
grep -q 'own Cargo profile folders' "$work/out" && fail "$name: own action reported"

name="enough after cache cleanup leaves own target and skips Nix"
fixture 5 ''; target .cache/target; target target; dependency_generations
echo 12 > "$dir/rm.gain"; run --own-target 15
[ "$got" -eq 0 ] && [ -d target/debug ] && [ ! -s "$dir/nix.calls" ] && [ "$(cat "$dir/events")" = target ] || fail "$name"
[ -L "$dir/user/dependencies/profile-1-link" ] || fail "$name: old generation removed"
grep -q 'own Cargo profile folders' "$work/out" && fail "$name: own action ran"

name="own target cleanup follows cache cleanup and skips Nix when enough"
fixture 5 ''; target .cache/target; target target
# Each profile frees 5 GiB: both actions must run to reach 15.
echo 5 > "$dir/rm.gain"; run --own-target 15
[ "$got" -eq 0 ] && [ "$(tr '\n' ' ' < "$dir/events")" = 'target own ' ] || fail "$name"
grep -q '^space: removed 1 own Cargo profile folders; 15 GiB are free now$' "$work/out" || fail "$name: own count excludes cache profiles"
name="short space cleans cache, dependency generations, own target, Nix and links in order"
fixture 1 '1|1'; target .cache/target; target target; dependency_generations
echo 6 > "$dir/rm.gain"; run --own-target 15
[ "$got" -eq 0 ] && [ "$(tr '\n' ' ' < "$dir/events")" = 'target profile own gc link link gc ' ] || fail "$name"

for process in compiler build-script test other prefix; do
  name="own target process guard: $process"
  fixture 5 ''; target target
  case "$process" in
    compiler) command="rustc --out-dir $PWD/target/debug/deps" ;;
    build-script) command="$PWD/target/debug/build/unit/build-script-build" ;;
    test) command="$PWD/target/debug/deps/unit --nocapture" ;;
    other) command='/other/target/debug/deps/unit' ;;
    prefix) command="$PWD/target-old/debug/deps/unit" ;;
  esac
  # Put the path beyond the usual short ps output width.
  PROCESS_ARGS="123 rustc $(printf '%150s' '') $command" run --own-target 15
  case "$process" in
    other|prefix)
      [ ! -e target/debug ] || fail "$name: unrelated process protected own target" ;;
    *) [ -d target/debug ] || fail "$name: active target removed"
       grep -q '^space: left own target folder because a process uses it$' "$work/out" || fail "$name: no message"
       grep -q 'own Cargo profile folders' "$work/out" && fail "$name: removal reported" ;;
  esac
done
for list in nops partial; do
  name="own target stays when process list fails: $list"
  fixture 5 ''; target target; NOPS=$list run --own-target 15
  [ "$got" -eq 1 ] && [ -d target/debug ] && [ ! -s "$dir/rm.calls" ] || fail "$name"
  grep -q 'process list (ps) does not work; own target left in place' "$work/out" || fail "$name: no message"
done

for shape in unmarked marker-link target-link profile-link ordinary; do
  name="own target boundary: $shape"
  fixture 5 ''; target "$dir/elsewhere"; target target
  case "$shape" in
    unmarked) rm target/.rustc_info.json ;;
    marker-link) rm target/.rustc_info.json; ln -s "$dir/elsewhere/.rustc_info.json" target/.rustc_info.json ;;
    target-link) rm -r target; ln -s "$dir/elsewhere" target ;;
    profile-link) rm -r target/debug; ln -s "$dir/elsewhere/debug" target/debug ;;
    ordinary) rm -r target; mkdir -p target/debug; echo keep > target/debug/note ;;
  esac
  run --own-target 15
  [ "$got" -eq 1 ] && [ -d target/debug ] && [ -f "$dir/elsewhere/debug/.cargo-lock" ] && [ ! -s "$dir/rm.calls" ] || fail "$name"
  if [ "$shape" = profile-link ]; then
    grep -q 'removed 0 own Cargo profile folders' "$work/out" || fail "$name: zero count"
  else
    grep -q 'own Cargo profile folders' "$work/out" && fail "$name: invalid target reported"
  fi
done
name="own target in a worktree path with spaces removes only its profiles"
fixture 5 ''; mv "$dir/tree" "$dir/tree with space"; cd "$dir/tree with space"
target target; target "$dir/elsewhere"; ln -s "$dir/elsewhere/debug" target/release
mkdir target/not-cargo; echo keep > target/not-cargo/note
run --own-target 15
[ ! -e target/debug ] && [ -L target/release ] && [ -f "$dir/elsewhere/debug/.cargo-lock" ] && [ -f target/not-cargo/note ] || fail "$name"
PROCESS_ARGS="123 $PWD/target/release/unit" run --own-target 15
grep -q 'because a process uses it' "$work/out" || fail "$name: spaced process path"

name="outside Labcast own-target removes nothing and emits no Cargo search line"
fixture 5 ''; target target; target .cache/target; rm -r architecture; run --own-target 15
[ "$got" -eq 1 ] && [ -d target/debug ] && [ -d .cache/target/debug ] && [ ! -s "$dir/rm.calls" ] || fail "$name"
grep -q 'Cargo profile folders' "$work/out" && fail "$name: search did not run"

for where in .cache/target target; do
  name="cargo-build-lock alone marks a profile: $where"
  fixture 5 ''; target "$where"; mv "$where/debug/.cargo-lock" "$where/debug/.cargo-build-lock"
  age_tree 240 "$where"; run --own-target 15
  [ ! -e "$where/debug" ] || fail "$name"
done
name="own target failed removal is not counted"
fixture 5 ''; target target; RM_FAIL=$PWD/target/debug run --own-target 15
[ -d target/debug ] || fail "$name: profile removed"
grep -q 'removed 0 own Cargo profile folders' "$work/out" || fail "$name: count"

for args in '--own-target' '--own-target 20' '20 --own-target' '--own' '--own-target --builds' '--builds --own-target' '--profile 15' '--profile x'; do
  name="own-target argument order: $args"
  fixture 17 ''; target target; run $args
  case "$args" in
    --own-target) [ "$got" -eq 0 ] && [ -d target/debug ] || fail "$name: default is 15" ;;
    '--own-target 20')
      [ "$got" -eq 1 ] && [ ! -e target/debug ] || fail "$name: need 20"
      grep -q '17 GiB free after the cleanup, 20 needed' "$work/out" || fail "$name: final text" ;;
    *) [ "$got" -eq 2 ] && [ -d target/debug ] && [ "$collections" -eq 0 ] || fail "$name: invalid arguments changed files"
       grep -q '^usage: space.sh \[--own-target\] \[GiB\] | --builds | --profile$' "$work/out" || fail "$name: usage" ;;
  esac
done
echo "space-test: $case_number cases pass"
