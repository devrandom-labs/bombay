#!/usr/bin/env bash
# Shared local Git fixture for the four harness start tests.
# Call fixture_init, install the needed tools, then call fixture_commit.

fixture_init() {
  set -E
  trap 'if [ "$BASH_SUBSHELL" -eq 0 ]; then
    printf "%s: failed at line %s: %s\n" "${0##*/}" "$LINENO" "$BASH_COMMAND" >&2
  fi' ERR
  unset CDPATH HARNESS_STATE HARNESS_WORKTREES HARNESS_LANES HARNESS_MAIN HARNESS_ASSIGNEE
  unset STEP_LEAD STEP_FREE HARNESS_LEAD OWNER_CHECK_MINUTES OWNER_CHECK_HOURS OWNER_CHECK_SECONDS
  unset HARNESS_OWNER_CLI HARNESS_OWNER_MODEL
  unset OWNER_POLL_SECONDS OWNER_CHECK_LOG_LIMIT OWNER_PERMISSION_MODE GH_TOKEN
  unset HARNESS_PROJECT_OWNER HARNESS_PROJECT_NUMBER HARNESS_MIN_FREE_GIB
  source_root=$(cd -- "$(dirname -- "$0")/../.." && pwd -P)
  fixture=$(mktemp -d)
  trap 'fixture_cleanup' EXIT
  fixture=$(cd -- "$fixture" && pwd -P)
  checkout=$fixture/'main checkout'
  worktrees=$checkout-worktrees
  export HOME=$fixture/home XDG_STATE_HOME=$fixture/xdg
  mkdir -p "$HOME" "$XDG_STATE_HOME"
  export FIXTURE_STATE
  FIXTURE_STATE=$(fixture_state "$XDG_STATE_HOME" "$checkout")
  # shellcheck disable=SC2034 # Used by the tests.
  state=$FIXTURE_STATE
  export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 GIT_ALLOW_PROTOCOL=file
  export GIT_AUTHOR_NAME=Fixture GIT_AUTHOR_EMAIL=fixture@example.invalid
  export GIT_COMMITTER_NAME=Fixture GIT_COMMITTER_EMAIL=fixture@example.invalid
  export FIXTURE=$fixture TZ=UTC LC_ALL=C
  mkdir -p "$checkout/architecture/owner" "$fixture/bin" "$worktrees"
  git init -q -b trunk "$checkout"
  printf '.cache/\n' > "$checkout/.gitignore"
  printf 'fixture\n' > "$checkout/tracked"
  fixture_copy architecture/start-common.sh
  fixture_copy architecture/owner/where.sh
  fixture_copy architecture/project.env
  export HARNESS_MAIN=trunk HARNESS_ASSIGNEE=fixture HARNESS_REPO=example/harness
  export HARNESS_LANES='server studio nix'
  export HARNESS_OWNER_CLI=claude
  export HARNESS_PROJECT_OWNER=fixture-org HARNESS_PROJECT_NUMBER=12
  printf '#!%s\n' "$BASH" > "$fixture/bin/unexpected"
  cat >> "$fixture/bin/unexpected" <<'STUB'
printf 'unexpected call to %s\n' "${0##*/}" >> "$FIXTURE/unexpected"
exit 99
STUB
  chmod +x "$fixture/bin/unexpected"
  local name
  for name in claude agy gh sudo caffeinate direnv lsof pgrep nix; do
    ln -s unexpected "$fixture/bin/$name"
  done
  mkdir "$fixture/commands"
  export FIXTURE_REAL_PS
  FIXTURE_REAL_PS=$(command -v ps || true)
  fixture_stub ps <<'STUB'
case "$*" in
  '-A -o pid=,comm=') cat "$FIXTURE/process-table" ;;
  '-p '*)
    kill -0 "$2" 2>/dev/null || exit 1
    case "$4" in
      stat=)
        if [ -n "$FIXTURE_REAL_PS" ]; then "$FIXTURE_REAL_PS" "$@" 2>/dev/null || echo S
        else echo S; fi
        ;;
      lstart=) echo 'Fri Nov 20 10:00:00 2026' ;;
      *) exit 2 ;;
    esac
    ;;
  '-ww -p '*)
    kill -0 "$3" 2>/dev/null && cat "$FIXTURE/commands/$3"
    ;;
  '-ww -o etime=,command= -p '*) echo "00:01 ${FIXTURE_AGENT_NAME:-claude} fixture" ;;
  *) exit 2 ;;
esac
STUB
  export PATH="$fixture/bin:$PATH"
}

fixture_hash() {
  local digest
  if command -v sha256sum >/dev/null; then
    digest=$(printf '%s' "$1" | sha256sum) || return 1
  else
    digest=$(printf '%s' "$1" | shasum -a 256) || return 1
  fi
  printf '%s\n' "${digest:0:64}"
}

fixture_state() {
  local digest
  digest=$(fixture_hash "$2") || return 1
  printf '%s/harness/%s-%s\n' "$1" "${2##*/}" "${digest:0:8}"
}

fixture_session_commands() {
  export BASH_ENV=$fixture/session-env
  cat > "$BASH_ENV" <<'ENV'
# Bash 3.2 assigns the script arguments after reading BASH_ENV.
fixture_record_session() {
  case "$0:${1:-}" in
    */architecture/owner/session.sh:run)
      printf '%s %s %s\n' "$BASH" "$0" "$*" > "$FIXTURE/commands/$$" ;;
  esac
  if [ "${1:-}:${2:-}" = run:check ]; then export FIXTURE_NO_FLOCK=1; fi
}
trap 'trap - DEBUG; fixture_record_session "$@"' DEBUG
ENV
}

fixture_copy() {
  cp "$source_root/$1" "$checkout/$1"
  chmod -R u+w "$checkout/$1"
}

fixture_stub() {
  local path=$fixture/bin/$1
  rm -f "$path"
  printf '#!%s\n' "$BASH" > "$path"
  cat >> "$path"
  chmod +x "$path"
}

fixture_commit() {
  git -C "$checkout" add .
  git -C "$checkout" commit -qm base
}

fixture_remote() {
  git init -q --bare "$fixture/remote"
  git -C "$checkout" remote add origin "$fixture/remote"
  git -C "$checkout" push -q origin trunk
  git clone -q -b trunk "$fixture/remote" "$fixture/upstream"
}

fixture_advance() {
  printf '%s\n' "$1" >> "$fixture/upstream/tracked"
  git -C "$fixture/upstream" commit -qam "$1"
  git -C "$fixture/upstream" push -q
  git -C "$fixture/upstream" rev-parse HEAD
}

expect() {
  diff -u <(printf '%s\n' "$1") <(printf '%s\n' "$2")
}

where_lines() {
  printf 'main: %s\nhere: %s\nworktrees: %s\nstate: %s\nrole: %s\n' "$@"
}

invoke() {
  local expected=$1 code=0
  shift
  actual=$("$@" 2>&1) || code=$?
  if [ "$code" != "$expected" ]; then
    printf 'Expected exit %s, got %s. Output:\n%s\n' "$expected" "$code" "$actual" >&2
    return 1
  fi
}

await() {
  local attempt
  for ((attempt = 0; attempt < 100; attempt++)); do
    if "$@"; then return 0; fi
    sleep 0.05
  done
  printf 'Timed out waiting for %s\n' "$*" >&2
  return 1
}

fixture_alive() {
  local status
  status=$(ps -p "$1" -o stat= 2>/dev/null) && [[ $status =~ ^[[:space:]]*[^Z[:space:]] ]]
}

fixture_dead() {
  ! fixture_alive "$1"
}

fixture_cleanup() {
  local code=${1:-$?}
  if [ -s "$fixture/unexpected" ]; then cat "$fixture/unexpected" >&2; code=1; fi
  rm -rf -- "$fixture"
  if [ "$code" != 0 ]; then printf '%s failed\n' "${0##*/}" >&2; fi
  exit "$code"
}
