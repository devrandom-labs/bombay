#!/usr/bin/env bash
# Record times for new build, Cargo and test lines in each lane's gate log.
# Usage: gate-times.sh start|status|stop <state folder> <worktree parent>
# Run in the Nix shell. The recorder stops after 24 hours.
# Writes gate-times.tsv, gate-times.pid and two locks in the state folder.
# Stop the recorder before removing its state folder or recorded output.
set -euo pipefail
unset CDPATH
# shellcheck source=architecture/project.env
source "$(dirname -- "$0")/project.env"
export HARNESS_LANES

usage() {
  echo 'usage: gate-times.sh start|status|stop <state folder> <worktree parent>' >&2
  exit 2
}

lock() {
  if [[ $OSTYPE == darwin* ]]; then /usr/bin/lockf -s -t "$1" "$2"
  else flock -E 75 -w "$1" "$2"; fi
}

record() {
  local lane lanes logs=()
  read -ra lanes <<< "$HARNESS_LANES"
  for lane in "${lanes[@]}"; do logs+=("$1/$lane/.cache/gate.log"); done
  # GNU tail follows the logs across file creation, truncation and replacement.
  # The PID check also stops it if the timeout process disappears.
  tail -v -n 0 -F --pid="$PPID" -- "${logs[@]}" 2>/dev/null |
    awk '
      /^==> .*\/\.cache\/gate\.log <==$/ {
        lane = $0
        sub(/\/\.cache\/gate\.log <==$/, "", lane)
        sub(/^.*\//, "", lane)
        next
      }
      {
        text = $0
        if (text ~ /^building .\/nix\/store\/[a-z0-9]+-/) {
          sub(/^building .\/nix\/store\//, "", text)
          if (index(text, "-") != 33) next
          text = substr(text, 34)
          if (!match(text, /^.+\.drv/)) next
          text = "build " substr(text, 1, RLENGTH - 4)
        } else if (match(text, /^these [0-9]+ derivations will be built/)) {
          text = substr(text, 1, RLENGTH)
        } else if (match(text, /Finished .* target\(s\) in .*/)) {
          text = substr(text, RSTART)
        } else if (match(text, /^labcast-architecture-tests> (Running [0-9]+ tests|test result: [[:alnum:]_]+)/)) {
          text = substr(text, 1, RLENGTH)
          sub(/^labcast-architecture-tests> /, "", text)
          text = "self-tests: " text
        } else if (text ~ /^labcast-architecture-tests> [a-z-]+-test: /) {
          split(text, fields, " ")
          script = fields[2]
          sub(/:$/, "", script)
          if (fields[3] ~ /^[0-9]+$/ && fields[4] == "cases" && fields[5] ~ /^pass/) {
            text = "self-tests: " script " " fields[3] " cases pass"
          } else if (script != seen[lane]) {
            text = "self-tests: first line of " script
          } else next
          seen[lane] = script
        } else if (match(text, /Summary \[[[:space:]]*[0-9.]+s\] [0-9]+ tests run.*/)) {
          text = substr(text, RSTART)
          sub(/^Summary \[[[:space:]]*/, "", text)
          seconds = text
          sub(/s\].*/, "", seconds)
          sub(/^[^]]+\] /, "", text)
          text = "nextest " text " in " seconds " s"
        } else next
        clock = "date \"+%Y-%m-%d %H:%M:%S\""
        clock | getline now
        close(clock)
        print now "\t" lane "\t" text
        fflush()
      }
    '
}

control() {
  local result=0 pid
  lock 15 8
  lock 0 9 || result=$?
  case "$result" in
    0|75) ;;
    *) return "$result" ;;
  esac
  case "$action:$result" in
    start:0)
      command -v timeout >/dev/null || { echo 'gate-times requires GNU timeout from the Nix shell' >&2; return 1; }
      exec 4>> "$out"
      [ -s "$out" ] || printf 'time\tlane\ttext\n' >&4
      exec 3> "$pidfile"
      export -f record
      trap '' HUP
      # shellcheck disable=SC2016 # The child receives the worktree path as $1.
      timeout 86400 "$BASH" -c 'set -euo pipefail; record "$1"' gate-times "$worktrees" \
        3>&- 8>&- </dev/null >&4 4>&- 2>/dev/null &
      pid=$!
      if ! printf '%s\n' "$pid" >&3; then
        kill -TERM "$pid" 2>/dev/null || true
        wait "$pid" || true
        rm -f -- "$pidfile"
        return 1
      fi
      echo "gate-times started (PID $pid), output $out"
      ;;
    start:75|status:75) echo 'gate-times is running' ;;
    status:0) echo 'gate-times is not running' ;;
    stop:75)
      read -r pid < "$pidfile"
      [[ $pid =~ ^[1-9][0-9]*$ ]] || { echo 'gate-times PID is invalid' >&2; return 1; }
      kill -TERM "$pid" 2>/dev/null || true
      lock 15 9 || { echo 'gate-times did not stop within 15 seconds' >&2; return 1; }
      ;;
    stop:0) ;;
  esac
  if [ "$action" = stop ]; then
    rm -f -- "$pidfile"
    echo 'gate-times stopped'
  fi
}

[ "$#" -eq 3 ] && [ -n "$2" ] && [ -d "$3" ] || usage
action=$1 directory=$2
worktrees=$(cd -- "$3" && pwd)
umask 077
case "$action" in
  start) mkdir -p -- "$directory" ;;
  status|stop)
    if [ ! -d "$directory" ]; then
      if [ "$action" = status ]; then echo 'gate-times is not running'; else echo 'gate-times stopped'; fi
      exit 0
    fi
    ;;
  *) usage ;;
esac
out=$directory/gate-times.tsv
pidfile=$directory/gate-times.pid
control 8>> "$directory/gate-times-control.lock" 9>> "$directory/gate-times-recorder.lock"
