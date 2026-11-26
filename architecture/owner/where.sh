# shellcheck shell=bash
# Source with `architecture` set to the directory that holds the caller.
# shellcheck disable=SC2034 # These values are read by the caller.

: "${architecture:?set the architecture directory before sourcing}"
if command -v git >/dev/null; then
  harness_here=$(git -C "$architecture" rev-parse --show-toplevel) || return 1
  harness_main=$(git -C "$architecture" worktree list --porcelain) || return 1
  harness_main=${harness_main%%$'\n'*}
  harness_main=${harness_main#worktree }
else
  # Let doctor report a missing Git in a fresh clone. Without Git, a linked
  # checkout cannot establish that it is the owner.
  harness_here=${architecture%/*}
  harness_main=
  if [ -d "$harness_here/.git" ]; then harness_main=$harness_here; fi
fi

_harness_path() (
  path=$1 suffix=''
  [[ $path == /* ]] || path=$PWD/$path
  # Resolve the existing parent even before the directory is created.
  while [ ! -d "$path" ]; do
    name=${path##*/}
    if [ -n "$name" ]; then suffix=/$name$suffix; fi
    path=${path%/*}
    if [ -z "$path" ]; then path=/; fi
  done
  cd -P -- "$path" || exit 1
  if [ -n "$suffix" ]; then printf '%s\n' "${PWD%/}$suffix"; else pwd -P; fi
)
harness_worktrees=$(_harness_path "${HARNESS_WORKTREES:-${harness_main:-$harness_here}-worktrees}") || return 1
harness_path_hash() {
  local digest
  if command -v sha256sum >/dev/null; then
    digest=$(printf '%s' "$1" | sha256sum) || return 1
  else
    digest=$(printf '%s' "$1" | shasum -a 256) || return 1
  fi
  printf '%s\n' "${digest:0:64}"
}
_harness_default_state() (
  checkout=${harness_main:-$harness_here}
  digest=$(harness_path_hash "$checkout") || exit 1
  printf '%s/harness/%s-%s\n' "${XDG_STATE_HOME:-$HOME/.local/state}" "${checkout##*/}" "${digest:0:8}"
)
harness_state=${HARNESS_STATE:-$(_harness_default_state)} || return 1
harness_state=$(_harness_path "$harness_state") || return 1
unset -f _harness_path
unset -f _harness_default_state

# shellcheck disable=SC2034 # Used by callers.
harness_role=$(
  if [ "$harness_here" = "$harness_main" ]; then
    echo owner
  else
    IFS=' ' read -r -a names <<< "$HARNESS_LANES"
    for name in "${names[@]}"; do
      if [ "$harness_here" = "$harness_worktrees/$name" ]; then
        printf 'lane %s\n' "$name"
        exit
      fi
    done
    echo other
  fi
)
