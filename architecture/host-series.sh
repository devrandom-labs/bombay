#!/usr/bin/env bash
# Record up to 24 hours of macOS host state without root.
# Give each recorder a private local directory for its logs and control files.
# The disk path selects the filesystem whose free space appears in host-top.tsv.
# Run in the Nix shell for GNU timeout.
set -euo pipefail

usage() {
  echo 'usage: host-series.sh start <directory> <disk path>' >&2
  echo '       host-series.sh {stop|status} <directory>' >&2
  exit 2
}

lock() {
  if [[ $OSTYPE == darwin* ]]; then /usr/bin/lockf -s -t "$1" "$2"
  else flock -E 75 -w "$1" "$2"; fi
}

headers() {
  if [ ! -s "$series" ]; then
    printf 'time\tkalloc1024_elements\tfree_pages\twired_pages\tcompressor_pages\tswap_used_M\tswap_free_M\tpressure\tload1\tprocesses\n' > "$series"
  fi
  if [ ! -s "$top" ]; then
    printf 'time\tkalloc1024_elements\tdisk_free_M\tlast_pid\tcpu_percent_by_program_top12\n' > "$top"
  fi
}

sample() {
  local now zone pages swap pressure load count disk lastpid busy
  now=$(date '+%Y-%m-%d %H:%M:%S')
  zone=$(zprint 2>/dev/null | awk '$1 == "data.kalloc.1024" { print $7 }') || zone=
  pages=$(vm_stat | awk -F: '
    /Pages free/ { free = $2 }
    /Pages wired down/ { wired = $2 }
    /Pages occupied by compressor/ { compressed = $2 }
    END { gsub(/[ .]/, "", free); gsub(/[ .]/, "", wired); gsub(/[ .]/, "", compressed);
          printf "%s\t%s\t%s", free, wired, compressed }
  ') || pages=$'?\t?\t?'
  swap=$(sysctl -n vm.swapusage | awk '{ gsub(/M/, ""); printf "%s\t%s", $6, $9 }') || swap=$'?\t?'
  pressure=$(sysctl -n kern.memorystatus_vm_pressure_level) || pressure='?'
  load=$(sysctl -n vm.loadavg | awk '{ print $2 }') || load='?'
  count=$(ps -axo pid= | awk 'END { print NR }') || count='?'
  disk=$(timeout -k 1 2 df -m -- "$disk_path" 2>/dev/null | awk 'NR == 2 { print $4 }') || disk=
  lastpid=$(sh -c 'echo $$')
  busy=$(ps -axo pcpu=,comm= | awk '
    { cpu = $1; sub(/^[ \t]*[^ \t]+[ \t]+/, ""); sub(/.*\//, ""); total[$0] += cpu }
    END {
      for (name in total) if (total[name] >= 1) {
        label = name; gsub(/[ \t;]/, "_", label);
        printf "%f\t%s\n", total[name], label
      }
    }
  ' | sort -rn | awk -F '\t' 'NR <= 12 { printf "%s%s:%.0f", (NR > 1 ? ";" : ""), $2, $1 }') || busy='?'
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$now" "${zone:-?}" "$pages" "$swap" "$pressure" "$load" "$count" >> "$series"
  printf '%s\t%s\t%s\t%s\t%s\n' "$now" "${zone:-?}" "${disk:-?}" "$lastpid" "$busy" >> "$top"
}

kernel_stream() {
  # Retain kernel events as they arrive. macOS may evict them before a later query.
  log stream --style compact --timeout 24h --predicate 'process == "kernel" AND (eventMessage CONTAINS[c] "memorystatus" OR eventMessage CONTAINS[c] "jetsam" OR eventMessage CONTAINS "CODE SIGNING" OR eventMessage CONTAINS "SIGKILL" OR eventMessage CONTAINS[c] "zone_map" OR eventMessage CONTAINS[c] "low swap") AND NOT (eventMessage CONTAINS "jetsam_snapshot" OR eventMessage CONTAINS "ANE_")' 9>&- </dev/null >&4 2>&1 &
  # The job spec identifies this shell's own stream. The stop command sends no signals.
  trap 'kill -TERM %1 2>/dev/null || true; wait 2>/dev/null || true' EXIT
  IFS= read -r _ || true
}

record() {
  local minute delay message
  # The stream guard holds the recorder lock until its child ends. The pipe
  # closes when the recorder exits, even if it cannot run a signal trap.
  exec 6> >( ( kernel_stream ) 7>&-)
  trap 'exit 143' TERM
  for ((minute = 1; minute <= 1440; minute++)); do
    delay=$(( started + minute * 60 - SECONDS ))
    (( delay > 0 )) || delay=1
    if IFS= read -r -t "$delay" message <&7; then
      [ "$message" != stop ] || return 0
    fi
    (( minute < 1440 )) || break
    sample 6>&- 7>&- 9>&-
  done
}

control() {
  local result=0
  lock 15 8
  lock 0 9 || result=$?
  case "$result" in
    0|75) ;;
    *) return "$result" ;;
  esac
  case "$action:$result" in
    start:0)
      [ "$(uname -s)" = Darwin ] || { echo 'host-series requires macOS' >&2; return 1; }
      command -v timeout >/dev/null || { echo 'host-series requires GNU timeout from the Nix shell' >&2; return 1; }
      exec 4>> "$directory/host-kernel.log" 5>> "$directory/host-recorder.log"
      rm -f -- "$channel"
      mkfifo -- "$channel"
      exec 7<> "$channel"
      headers
      started=$SECONDS
      sample 7>&- 8>&- 9>&-
      # The recorder inherits its lock, while the control lock follows this command.
      trap '' HUP
      ( record ) 8>&- </dev/null >&5 2>&1 &
      echo "host-series started as process $!"
      ;;
    start:75) echo 'host-series already runs' ;;
    status:0) echo 'host-series is stopped' ;;
    status:75) echo 'host-series is running' ;;
    stop:0) echo 'host-series is stopped' ;;
    stop:75)
      [ -p "$channel" ] || { echo 'host-series control pipe is missing' >&2; return 1; }
      printf 'stop\n' 7<> "$channel" >&7
      if ! lock 15 9; then
        echo 'host-series did not stop within 15 seconds' >&2
        return 1
      fi
      echo 'host-series stopped'
      ;;
  esac
}

[ "$#" -ge 2 ] && [ -n "$2" ] || usage
action=$1 directory=$2
umask 077
case "$action" in
  start)
    [ "$#" -eq 3 ] && [ -n "$3" ] || usage
    disk_path=$3
    mkdir -p -- "$directory"
    ;;
  stop|status)
    [ "$#" -eq 2 ] || usage
    if [ ! -d "$directory" ]; then
      echo 'host-series is stopped'
      exit 0
    fi
    ;;
  *) usage ;;
esac
series=$directory/host-series.tsv
top=$directory/host-top.tsv
channel=$directory/stop.pipe
control 8>> "$directory/control.lock" 9>> "$directory/recorder.lock"
