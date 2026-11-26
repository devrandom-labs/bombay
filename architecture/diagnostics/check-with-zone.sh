#!/usr/bin/env bash
# Run the harness suites in a checkout and report timing and kernel zone changes.
# Run from the checkout's Nix shell on macOS. Give each run its own log folder.
# The folder retains suite logs and a sample of the zone and log size every two seconds.
set -euo pipefail

usage() {
  echo 'usage: check-with-zone.sh <clone> <log folder>' >&2
  echo 'run inside the development shell of the clone' >&2
  exit 2
}

[ "$#" -eq 2 ] && [ -n "$1" ] && [ -n "$2" ] || usage
clone=$1
out=$2
mkdir -p -- "$out"
out=$(cd -- "$out" && pwd)
cd -- "$clone" || exit 2
head=$(git rev-parse --short HEAD)
source architecture/tests/suites.sh
scratch=$(mktemp -d)
export TMPDIR="$scratch"

stop_sampler() {
  # Use this shell's own job even if the sampler has already exited.
  kill -TERM %1 2> /dev/null || true
  wait %1 2> /dev/null || true
}

trap 'stop_sampler; rm -rf -- "$scratch"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# The settings of architecture/check.sh for its self-tests.
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null
export GIT_CONFIG_COUNT=3
export GIT_CONFIG_KEY_0=commit.gpgsign GIT_CONFIG_VALUE_0=false
export GIT_CONFIG_KEY_1=init.defaultBranch GIT_CONFIG_VALUE_1=main
export GIT_CONFIG_KEY_2=core.hooksPath GIT_CONFIG_VALUE_2=/dev/null
inuse() {
  zprint -L data.kalloc.1024 | awk '
    $1 == "data.kalloc.1024" && $7 ~ /^[0-9]+$/ { print $7; found = 1 }
    END { if (!found) exit 1 }
  '
}
# The suites create executable fixtures. Record CPU time used by the macOS scanner.
scan() {
  ps -Ao cputime=,comm= | awk '/XprotectService$/ { n = split($1, t, ":"); s = 0; for (i = 1; i <= n; i++) s = s * 60 + t[i]; total += s } END { printf "%d\n", total }'
}
printf 'start %s\thead %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$head"
: > "$scratch/current"
: >> "$out/zone-fine.tsv"
( while :; do
    name=$(cat "$scratch/current")
    lines=0; [ -z "$name" ] || [ ! -f "$out/$name.log" ] || lines=$(wc -l < "$out/$name.log" | tr -d ' ')
    zone=$(inuse) || zone='?'
    printf '%s\t%s\t%s\t%s\n' "$(date '+%H:%M:%S')" "$zone" "${name:-idle}" "$lines" >> "$out/zone-fine.tsv"
    sleep 2
  done ) &
a=$(inuse); sleep 20; b=$(inuse)
printf 'idle 20 s\tzone %+d\n' "$((b - a))"
status=0
export GITHUB_OUTPUT="$out/github-output"
# host-test (it runs commit-test first) belongs to checks.workflow, not to check.sh.
for name in "${suites[@]}" host; do
  echo "$name" > "$scratch/current"
  a=$(inuse); t0=$(date +%s); x0=$(scan)
  # host-test needs yq (checks.workflow supplies pkgs.yq-go; the development shell has none).
  with=(bash)
  [ "$name" != host ] || command -v yq > /dev/null \
    || with=(nix shell --no-update-lock-file --inputs-from . 'nixpkgs#yq-go' --command bash)
  code=0
  "${with[@]}" "architecture/tests/$name-test.sh" > "$out/$name.log" 2>&1 || code=$?
  b=$(inuse); t1=$(date +%s); x1=$(scan)
  [ "$code" = 0 ] || status=1
  printf '%s-test\texit %s\tseconds %s\tscan CPU s %s\tzone %+d\t%s\n' "$name" "$code" "$((t1 - t0))" "$((x1 - x0))" "$((b - a))" "$(tail -n 1 "$out/$name.log" | cut -c1-90)"
done
: > "$scratch/current"
a=$(inuse); sleep 20; b=$(inuse)
printf 'idle 20 s\tzone %+d\n' "$((b - a))"
stop_sampler
printf 'end %s\tstatus %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$status"
exit "$status"
