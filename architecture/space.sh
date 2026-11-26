#!/usr/bin/env bash
# Makes free disk space for a session or for the full gate. The run supervisor
# and the gate call this; a person can call it too.
# Space requests take the gate lock. For a command inside the gate, set
# GATE_FREE at gate startup so cleanup runs under the lock it already holds.
#
#   bash architecture/space.sh [--own-target] [GiB]   free at least that much (15)
#   bash architecture/space.sh --profile      print the dependency profile path
#   sudo bash architecture/space.sh --builds   remove leftover Nix build folders
#
# For a space request it first removes Cargo profile folders in unused targets
# idle for 180 minutes in this worktree's .cache. The harness keeps this cache
# small because a build can make its contents again.
# While space is short, it then removes these in order:
#   1. Old dependency profile generations.
#   2. With --own-target, this worktree's Cargo profile folders, unless a
#      process names a path under target/; use only between sessions.
#   3. Nix store paths that nothing refers to.
#   4. Labcast result links older than a day, in the .cache of the Labcast
#      worktree that it runs in and in the Nix folder of the Labcast cache of
#      the user (~/.cache/labcast/nix), and the store paths that only they
#      kept. It keeps the dependency profile. In another folder it leaves
#      .cache alone.
#   5. At Linux gate startup, unused images on the configured local Docker
#      daemon.
# It returns 1 when that is not enough, and it says what else holds the disk.
#
# A Nix build that was kept (--keep-failed) or killed leaves its folder in the
# build directory. The folder belongs to a build user, so only root can remove
# it; --builds removes each folder whose build process has ended. It removes
# nothing when the process list does not work.
# SPACE_FREE, SPACE_BUILDS, SPACE_CACHE, SPACE_BUILDER and SPACE_AGE (minutes)
# serve the test.
set -uo pipefail

builds=${SPACE_BUILDS:-/nix/var/nix/builds}
_space_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
if [ -f "$_space_dir/project.env" ]; then . "$_space_dir/project.env"; fi
cache=${SPACE_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/${HARNESS_CACHE_NAME:-bombay}/nix}
dependency_profile=$cache/dependencies/profile
usage() { echo "usage: space.sh [--own-target] [GiB] | --builds | --profile"; exit 2; }
if [ "${1:-}" = --profile ]; then
  [ "$#" -eq 1 ] || usage
  echo "$dependency_profile"
  exit 0
fi

free_gib() {
  if [ -n "${SPACE_FREE:-}" ]; then $SPACE_FREE; else df -Pk . | awk 'NR == 2 { print int($4 / 1048576) }'; fi
}

# The list of all processes, as "pid command" lines. It counts only when it
# holds this process: without a list that works nobody can tell which builds
# run, and every folder counts as in use.
processes() {
  local table
  table=$(ps -A -ww -o "pid=,${1:-comm}=" 2> /dev/null) || return 1
  grep -qE "^ *$$ " <<< "$table" || return 1
  printf '%s\n' "$table"
}

# The folders of builds that have ended: the name holds the process of the
# build. The process list is taken after the folders, so the process of every
# listed folder whose build still runs is in it. A folder counts as ended only
# when its process is not in the list or is not a Nix process.
leftover() {
  local dir pid table line
  local dirs=("$builds"/nix-*)
  table=$(processes) || return 0
  for dir in "${dirs[@]}"; do
    [ -d "$dir" ] || continue
    [[ "${dir##*/}" =~ ^nix-([0-9]+)-[0-9]+$ ]] || continue
    pid=${BASH_REMATCH[1]}
    line=$(grep -E "^ *$pid " <<< "$table" || true)
    [[ "$line" == *"${SPACE_BUILDER:-nix}"* ]] || echo "$dir"
  done
}

own_target=0
if [ "${1:-}" = --own-target ]; then own_target=1; shift; fi
[ "$#" -le 1 ] || usage
if [ "${1:-}" = --builds ]; then
  [ "$own_target" -eq 0 ] || usage
  processes > /dev/null || { echo "space: the process list (ps) does not work; nothing removed"; exit 1; }
  count=0
  while IFS= read -r dir; do
    rm -rf "$dir" && count=$((count + 1))
  done < <(leftover)
  echo "space: removed $count leftover Nix build folders; $(free_gib) GiB are free now"
  exit 0
fi

need=${1:-15}
[[ "$need" =~ ^[0-9]+$ ]] || usage

# The gate sources this script in a subshell after taking the machine lock.
# A direct cleanup call takes the same lock first.
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  [ "$own_target" -eq 0 ] || set -- --own-target "$@"
  exec "$BASH" "$(dirname "$0")/gate.sh" --space "$@"
fi

enough() { [ "$(free_gib)" -ge "$need" ]; }

# A program can start after its target's process and idle checks, while
# removal runs.
# Bound metadata scans; do not walk artifact, incremental or script outputs.
target_idle() {
  local recent
  recent=$(find "$1" -maxdepth 5 ! -type l \
    \( -mmin "-${SPACE_AGE:-180}" -print -quit -o \
      \( \( \( -name deps -o -name incremental \) -exec test -f '{}/../.cargo-lock' \; \) \
        -o \( -name out -exec test -f '{}/../invoked.timestamp' \; \) \) -prune \) 2> /dev/null) || return 1
  [ -z "$recent" ]
}

# Remove only Cargo profiles, leaving root files and evidence in the target.
remove_profiles() {
  local profile
  while IFS= read -r -d '' profile; do
    rm -rf "$profile" && removed=$((removed + 1))
  done < <(find "$1" -mindepth 1 -maxdepth 2 -type d \
    \( -exec test -f '{}/.cargo-lock' \; -o -exec test -f '{}/.cargo-build-lock' \; \) \
    -print0 -prune 2> /dev/null)
}

remove_idle_profiles() {
  [ -f "$PWD/architecture/space.sh" ] || return 0
  local dir relative i table removed=0
  local targets=()
  while IFS= read -r -d '' dir; do
    target_idle "$dir" && targets+=("$dir")
  done < <(find "$PWD/.cache" -mindepth 1 -type d -exec test -f '{}/.rustc_info.json' \; \
    -exec test ! -L '{}/.rustc_info.json' \; -print0 -prune 2> /dev/null)
  for ((i=0; i<${#targets[@]}; i++)); do
    dir=${targets[i]}
    table=$(processes args) || break
    table+=$'\n'
    relative=${dir#"$PWD"/}
    # A process can name the target itself or a path inside it.
    if [[ "$table" == *"$dir"[/[:space:]]* ||
          "$table" == *[[:space:]=]"$relative"[/[:space:]]* ||
          "$table" == *[[:space:]=]"./$relative"[/[:space:]]* ]]; then
      continue
    fi
    # Check again just before removal: another build may have started meanwhile.
    target_idle "$dir" || continue
    remove_profiles "$dir"
  done
  if [ "$removed" -gt 0 ]; then
    echo "space: removed $removed old Cargo profile folders; $(free_gib) GiB are free now"
  fi
}

remove_idle_profiles
enough && exit 0

# Nix profiles keep their current generation; only the old dependency builds go.
if [ -L "$dependency_profile" ]; then
  if ! nix profile wipe-history --profile "$dependency_profile"; then
    echo "space: could not remove old dependency generations; keeping them at $dependency_profile"
  fi
  enough && exit 0
fi

if [ "$own_target" -eq 1 ] && [ -f "$PWD/architecture/space.sh" ] \
  && [ ! -L "$PWD/target" ] \
  && [ -f "$PWD/target/.rustc_info.json" ] && [ ! -L "$PWD/target/.rustc_info.json" ]; then
  if ! table=$(processes args); then
    echo "space: the process list (ps) does not work; own target left in place"
  elif [[ "$table" == *"$PWD/target/"* ]]; then
    echo "space: left own target folder because a process uses it"
  else
    removed=0
    remove_profiles "$PWD/target"
    echo "space: removed $removed own Cargo profile folders; $(free_gib) GiB are free now"
  fi
fi
enough && exit 0

echo "space: $(free_gib) GiB free, $need needed; removing unused Nix store paths"
nix store gc > /dev/null 2>&1 || true
enough && exit 0

# The .cache of the current folder counts only in a Labcast worktree: from
# another folder, such as the home folder, it holds the roots of other tools.
roots=("$cache")
[ ! -f "$PWD/architecture/space.sh" ] || roots+=("$PWD/.cache")
removed=0
while IFS= read -r -d '' link; do
  rm -f "$link" && removed=$((removed + 1))
done < <(for dir in "${roots[@]}"; do
  [ -d "$dir" ] && find "$dir" -path "${dependency_profile%/*}" -exec test -L "$dependency_profile" \; -prune -o -type l -lname '/nix/store/*' -mmin +1440 -print0 2> /dev/null
done)
if [ "$removed" -gt 0 ]; then
  echo "space: removed $removed Labcast result links older than a day; removing what they kept"
  nix store gc > /dev/null 2>&1 || true
fi
enough && exit 0

if [ "$space_only" -eq 0 ] && [ "$kernel" = Linux ] &&
  docker_host=$(docker context inspect --format '{{.Endpoints.docker.Host}}' 2> /dev/null) &&
  [[ "$docker_host" = unix:///?* ]]; then
  echo "space: removing unused Docker images"
  docker --host "$docker_host" image prune --all --force || true
  enough && exit 0
fi

echo "space: $(free_gib) GiB free after the cleanup, $need needed"
count=0 size=0
while IFS= read -r dir; do
  [ -n "$dir" ] || continue
  count=$((count + 1))
  used=$(du -sk "$dir" 2> /dev/null | cut -f1)
  size=$((size + ${used:-0}))
done < <(leftover)
if [ "$count" -gt 0 ]; then
  echo "space: $count leftover Nix build folders hold at least $((size / 1048576)) GiB. Only root can remove them:"
  echo "space:   sudo bash $(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/space.sh --builds"
fi
exit 1
