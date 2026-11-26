#!/usr/bin/env bash
set -euo pipefail
architecture=${1:-architecture}
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
export HOME=$work/home XDG_STATE_HOME=$work/xdg
mkdir -p "$HOME" "$XDG_STATE_HOME" "$work/lanes"

registered=$("$BASH" "$architecture/owner/tools.sh" list "$work/state" "$work/lanes" |
  awk '{
    sub(/^([^|]*\|){3}/, "")
    while (match($0, /(^|[[:space:];])(tool |architecture\/)[[:alnum:]_./-]+([[:space:];|]|$)/)) {
      path = substr($0, RSTART, RLENGTH)
      sub(/^[[:space:];]?(tool |architecture\/)/, "", path)
      sub(/[[:space:];|]$/, "", path)
      print path
      $0 = substr($0, RSTART + RLENGTH)
    }
  }')

coverage() {
  local tree=$1 file path missing=0
  while IFS= read -r -d '' file; do
    grep -Iq '^# Usage:' "$file" || continue
    path=${file#"$tree"/}
    case "$path" in
      check.sh) continue ;; # Architecture checks run in the build.
      gate.sh) continue ;; # The repository gate runs before a push.
      run.sh) continue ;; # lane.sh starts the lane supervisor.
      step.sh) continue ;; # The push hook checks the step.
    esac
    if ! grep -Fxq -- "$path" <<< "$registered"; then
      printf 'owner registry: architecture/%s has a Usage header and no registry line\n' "$path"
      missing=1
    fi
  done < <(find "$tree" -type f -print0)
  return "$missing"
}

coverage "$architecture"
mkdir "$work/probe"
printf '# %s\n' 'Usage: forgotten.sh' > "$work/probe/forgotten.sh"
if output=$(coverage "$work/probe"); then
  echo 'owner-registry-test: a tool without a registry line passed' >&2
  exit 1
fi
diff -u <(echo 'owner registry: architecture/forgotten.sh has a Usage header and no registry line') <(printf '%s\n' "$output")
echo 'owner-registry-test: every Usage header has a registry line or a reason'
