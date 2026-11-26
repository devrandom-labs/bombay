#!/usr/bin/env bash
# Compare program starts on macOS using the data.kalloc.1024 kernel zone.
# Warm the files before timing so that the first scan is outside each phase.
# Run from the Nix shell. The count is the number of starts per phase.
set -euo pipefail

if [ "$#" -ne 1 ] || ! [[ $1 =~ ^[0-9]+$ ]] || ! [ "$1" -ge 0 ] 2>/dev/null; then
  echo 'usage: kalloc-fast-probe.sh <count>' >&2
  exit 2
fi

count=$((10#$1))
work=$(mktemp -d) || exit 2
trap 'rm -rf "$work"' EXIT
inuse() {
  zprint -L data.kalloc.1024 | awk '
    $1 == "data.kalloc.1024" && $7 ~ /^[0-9]+$/ { print $7; found = 1 }
    END { if (!found) exit 1 }
  '
}
nix_bash=$(command -v bash)
printf '#!/bin/sh\nexit 0\n' > "$work/apple-sh"
printf '#!/bin/bash\nexit 0\n' > "$work/apple-bash"
printf '#!%s\nexit 0\n' "$nix_bash" > "$work/nix-bash"
printf '#!/usr/bin/env bash\nexit 0\n' > "$work/env-bash"
printf 'exit 0\n' > "$work/plain"
chmod +x "$work/apple-sh" "$work/apple-bash" "$work/nix-bash" "$work/env-bash"
# One start of each file first, so that the scan of the new files is not in a phase.
"$work/apple-sh"
"$work/apple-bash"
"$work/nix-bash"
"$work/env-bash"
loop() {
  local i
  for ((i = 0; i < count; i++)); do "$@"; done
}
phase() {
  local name=$1 a b t0 t1
  shift
  a=$(inuse)
  t0=$(date +%s)
  "$@" > /dev/null
  b=$(inuse)
  t1=$(date +%s)
  printf '%-34s seconds %3s  zone %+7d\n' "$name" "$((t1 - t0))" "$((b - a))"
}
printf 'start %s  count %s  nix bash %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$count" "$nix_bash"
for ((round = 0; round < 3; round++)); do
  phase "idle 10 s" sleep 10
  phase "binary /usr/bin/true" loop /usr/bin/true
  phase "script #!/bin/sh" loop "$work/apple-sh"
  phase "script #!/bin/bash" loop "$work/apple-bash"
  phase "script #!<nix bash>" loop "$work/nix-bash"
  phase "script #!/usr/bin/env bash" loop "$work/env-bash"
  phase "/bin/sh file (no script start)" loop /bin/sh "$work/plain"
  phase "/bin/bash file" loop /bin/bash "$work/plain"
  phase "<nix bash> file" loop "$nix_bash" "$work/plain"
  phase "idle 10 s" sleep 10
done
printf 'end %s\n' "$(date '+%Y-%m-%d %H:%M:%S')"
