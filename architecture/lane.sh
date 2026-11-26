#!/usr/bin/env bash
# Usage: lane.sh start|stop <lane>, lane.sh status|sweep|--where
# Start or stop a lane's background supervisor. Start can fetch main, fast-forward
# clean checkouts and create a missing worktree. Writes .cache/run.{pid,log} in
# the lane. Stop ends the run, removes its PID and marks it stopped. A manual
# start clears the mark after the supervisor starts. Sweep deletes ended Nix builds
# only with a terminal on stdin. Worktrees and run logs stay available after stop.
set -euo pipefail
unset CDPATH
architecture=$(cd -- "$(dirname -- "$0")" && pwd -P)
# shellcheck source=architecture/start-common.sh
. "$architecture/start-common.sh"

builds=${HARNESS_BUILDS:-/nix/var/nix/builds}

leftover() {
  local dir table line
  local dirs=("$builds"/nix-*)
  # A missing or incomplete process list cannot tell us which builds ended.
  table=$(ps -A -o pid=,comm= 2>/dev/null) || return 0
  grep -qE "^ *$$ " <<< "$table" || return 0
  for dir in "${dirs[@]}"; do
    [ -d "$dir" ] && [ ! -L "$dir" ] || continue
    [[ ${dir##*/} =~ ^nix-([0-9]+)-[0-9]+$ ]] || continue
    line=$(grep -E "^ *${BASH_REMATCH[1]} " <<< "$table" || true)
    [[ $line == *nix* ]] || printf '%s\n' "$dir"
  done
}

sweep() {
  local list before dir
  local dirs=()
  list=$(leftover)
  [ -n "$list" ] || { echo 'lane: no leftover Nix build folders'; return 0; }
  while IFS= read -r dir; do dirs+=("$dir"); done <<< "$list"
  if [ ! -t 0 ]; then
    printf 'lane: %s leftover Nix build folders; run in a terminal: bash %q sweep\n' "${#dirs[@]}" "$architecture/lane.sh"
    return 0
  fi
  before=$(free_gib "$harness_main")
  printf 'lane: removing %s leftover Nix build folders; this needs your password\n' "${#dirs[@]}"
  if sudo rm -rf -- "${dirs[@]}"; then
    printf 'lane: %s GiB are free now (before: %s GiB)\n' "$(free_gib "$harness_main")" "$before"
  else
    echo 'lane: the leftover folders stay; the run starts anyway'
  fi
}

status() {
  local name cache list stopped
  for name in "${lanes[@]}"; do
    cache=$harness_worktrees/$name/.cache
    if stopped=$(stopped_on "$name"); then
      printf '%s: stopped by a person on %s\n' "$name" "$stopped"
    elif supervisor_pid "$name" >/dev/null; then echo "$name: RUNNING"
    else echo "$name: not running"; fi
    [ -d "$harness_worktrees/$name" ] || echo '  no worktree yet'
    if [ -f "$cache/run.status" ]; then printf '  now: %s\n' "$(cat "$cache/run.status")"; fi
    if [ -f "$cache/run.history" ]; then
      echo '  last sessions:'
      tail -n 8 "$cache/run.history" | sed 's/^/    /'
    fi
  done
  printf 'disk: %s GiB free; a session needs %s\n' "$(free_gib "$harness_main")" "${STEP_FREE:-15}"
  list=$(leftover)
  if [ -n "$list" ]; then
    printf 'disk: %s leftover Nix build folders; remove them with: bash %q sweep\n' \
      "$(awk 'END { print NR }' <<< "$list")" "$architecture/lane.sh"
  fi
}

start() {
  local pid branch runner attempt stopped name
  local -a launcher
  local -a environment=(
    PATH HOME USER LOGNAME SHELL TMPDIR LANG LC_ALL LC_CTYPE TZ
    XDG_CACHE_HOME XDG_CONFIG_HOME XDG_DATA_HOME XDG_STATE_HOME
    GH_TOKEN GH_CONFIG_DIR SSH_AUTH_SOCK CODEX_HOME
    NIX_PATH NIX_CONFIG NIX_REMOTE NIX_SSL_CERT_FILE SSL_CERT_FILE
    STEP_LEAD STEP_LIMIT STEP_FREE
    RUN_GATE_LIMIT RUN_LAND_LIMIT RUN_LAND_KILL RUN_TURN RUN_TURN_POLL
    RUN_TURN_TELL RUN_TURN_FETCH RUN_FULL RUN_RETRY_WAIT RUN_RETRIES RUN_REVIEW RUN_NOTIFY
    GATE_KEEP GATE_PASSED GATE_FREE GATE_AGAIN
  )
  if [ "${3:-}" = --unless-stopped ] && stopped=$(stopped_on "$lane"); then
    printf '%s: stopped by a person on %s; start it with: bash architecture/lane.sh start %s\n' \
      "$lane" "$stopped" "$lane"
    return 0
  fi
  if supervisor_pid "$lane" >/dev/null; then
    rm -f -- "$dir/.cache/run.stopped"
    echo "$lane already runs."
    status
    return 0
  fi
  export STEP_LEAD=${STEP_LEAD-$HARNESS_LEAD}
  if pid=$(agent_in_checkout "$dir" '^([^[:space:]]*/)?(codex|claude|agy|antigravity)([[:space:]]|$)'); then
    echo "$lane: process $pid already works in $dir:"
    ps -ww -o etime=,command= -p "$pid" | cut -c1-110 || true
    echo 'Let it finish and close it, then run this again.'
    return 1
  fi

  sweep
  # Pass the active login explicitly so an older supervisor does not choose
  # its own account. A supplied token takes precedence.
  if [ -z "${GH_TOKEN:-}" ]; then
    GH_TOKEN=$(gh auth token) && [ -n "$GH_TOKEN" ] || {
      echo 'lane: sign in with gh auth login or set GH_TOKEN' >&2
      return 1
    }
  fi
  export GH_TOKEN
  if fetch_main; then
    branch=$(checkout_branch)
    if [ "$branch" = "$HARNESS_MAIN" ] && clean_checkout "$harness_main"; then
      fast_forward "$harness_main" || echo 'lane: the main checkout did not update'
    fi
  else
    echo "lane: could not fetch $HARNESS_MAIN; using the local copy"
  fi

  if [ ! -d "$dir" ]; then
    echo "lane: making the worktree of the $lane lane at $dir"
    git -C "$harness_main" worktree add -q --detach "$dir" "origin/$HARNESS_MAIN"
  fi
  cd -- "$dir"
  mkdir -p .cache
  if clean_checkout "$dir"; then fast_forward "$dir" 2>/dev/null || true; fi
  runner=$dir/architecture/run.sh
  # A dirty or diverged worktree can still predate the self-updating supervisor.
  grep -q 'started again with the supervisor of main' "$runner" 2>/dev/null ||
    runner=$harness_main/architecture/run.sh
  launcher=("$BASH" "$runner" "$lane")
  if command -v caffeinate >/dev/null; then launcher=(caffeinate -i "${launcher[@]}"); fi
  if [ -f .envrc ] && command -v direnv >/dev/null; then
    direnv allow . >/dev/null 2>&1 || true
    launcher=(direnv exec . "${launcher[@]}")
  fi
  # Open the PID file first so a failed write cannot leave an untracked run.
  exec 3> .cache/run.pid
  printf '%s\n' "--- started $(date '+%Y-%m-%d %H:%M') ---" >> .cache/run.log
  # Pass values on a pipe to keep the token off the command line.
  # shellcheck disable=SC2016 # The clean shell reads and expands these values.
  nohup env -i "$BASH" -c '
    while IFS= read -r -d "" setting; do
      export "$setting"
    done <&3
    exec 3<&-
    exec "$@"
  ' lane "${launcher[@]}" >> .cache/run.log 2>&1 </dev/null 8>&- 3< <(
    for name in "${environment[@]}"; do
      if [ -n "${!name+x}" ]; then printf '%s=%s\0' "$name" "${!name}"; fi
    done
  ) &
  pid=$!
  if ! printf '%s\n' "$pid" >&3; then
    kill_tree "$pid"
    return 1
  fi
  exec 3>&-
  for ((attempt = 0; attempt < 100; attempt++)); do
    if supervisor_pid "$lane" >/dev/null; then
      rm -f -- .cache/run.stopped
      printf '%s: started in the background.\n' "$lane"
      printf '  status:  bash %q status\n' "$architecture/lane.sh"
      printf '  follow:  tail -f %q\n' "$dir/.cache/run.log"
      printf '  stop:    bash %q stop %s\n' "$architecture/lane.sh" "$lane"
      return 0
    fi
    process_alive "$pid" || break
    sleep 0.05
  done
  kill_tree "$pid"
  rm -f .cache/run.pid
  printf '%s: supervisor did not start; see %s\n' "$lane" "$dir/.cache/run.log" >&2
  return 1
}

stop() {
  local pid stamp
  [ -d "$dir" ] || { printf '%s: no worktree yet\n' "$lane" >&2; return 1; }
  mkdir -p -- "$dir/.cache"
  stamp=$(date '+%Y-%m-%d %H:%M')
  printf '%s\n' "$stamp" > "$dir/.cache/run.stopped"
  if pid=$(supervisor_pid "$lane"); then
    kill_tree "$pid"
    echo "$lane: stopped"
  else
    echo "$lane: was not running"
  fi
  rm -f "$dir/.cache/run.pid"
}

usage() {
  echo 'usage: lane.sh start|stop <lane>, lane.sh status|sweep|--where' >&2
  exit 2
}

case "${1:-}:$#" in
  --where:1) show_paths; exit ;;
  sweep:1) sweep; exit ;;
  status:1|start:2|stop:2) ;;
  start:3) [ "$3" = --unless-stopped ] || usage ;;
  *) usage ;;
esac
IFS=' ' read -r -a lanes <<< "$HARNESS_LANES"
if [ "$1" = status ]; then status; exit; fi
lane=$2
case "$lane" in
  ''|.|..|*[!a-zA-Z0-9_-]*) usage ;;
esac
case " ${lanes[*]} " in
  *" $lane "*) ;;
  *) echo "lane: unknown lane $lane (choose from $HARNESS_LANES)" >&2; exit 2 ;;
esac
dir=$harness_worktrees/$lane
create_state
exec 8>> "$harness_state/lane-$lane.control"
control_lock 8
"$1" "$@"
