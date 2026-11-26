#!/usr/bin/env bash
set -euo pipefail

script=architecture/host-series.sh
fixture=$(mktemp -d)
first="$fixture/first run"
second="$fixture/second run"

cleanup() {
  local result=$?
  bash "$script" stop "$first" >/dev/null 2>&1 || true
  bash "$script" stop "$second" >/dev/null 2>&1 || true
  rm -rf "$fixture"
  if [ "$result" != 0 ]; then echo 'host-series: fixture failed' >&2; fi
}
trap cleanup EXIT

mkdir "$fixture/bin" "$fixture/first-log" "$fixture/second-log"
mkfifo "$fixture/first-log/wait" "$fixture/second-log/wait"
printf '#!%s\n' "$BASH" > "$fixture/bin/probe"
cat >> "$fixture/bin/probe" <<'PROBE'
set -eu
case "${0##*/}:${FAILED_PROBES:-0}" in
  vm_stat:1|sysctl:1|ps:1) exit 1 ;;
esac
case "${0##*/}" in
  uname) echo Darwin ;;
  zprint) echo 'data.kalloc.1024 a b c d e 42' ;;
  vm_stat)
    printf 'Pages free: 11.\nPages wired down: 22.\nPages occupied by compressor: 33.\n'
    ;;
  sysctl)
    case "$2" in
      vm.swapusage) echo 'total = 1024.00M used = 256.00M free = 768.00M' ;;
      kern.memorystatus_vm_pressure_level) echo 1 ;;
      vm.loadavg) echo '{ 2.50 2.10 1.90 }' ;;
      *) exit 1 ;;
    esac
    ;;
  ps)
    case "$2" in
      pid=) printf ' 10\n 11\n' ;;
      pcpu=,comm=) printf ' 2.15 /a/beta\n 2.49 /b/alpha\n 1.00 /c/extra cpu\n' ;;
      *) exit 1 ;;
    esac
    ;;
  df)
    if [ "$3" = unavailable ]; then
      trap 'echo timed-out > "$TRACK/disk"; exit 1' TERM
      exec 3<> "$TRACK/wait"
      read -r -t 8 ignored <&3 || true
      exit 1
    fi
    printf 'Filesystem 1M-blocks Used Available\nfixture 1000 100 900\n'
    ;;
  date) echo '2026-11-19 08:00:00' ;;
  log)
    [ "$1" = stream ] && [ "$6" = --predicate ]
    trap 'sleep 0.1; echo stopped >> "$TRACK/events"; exit' TERM
    exec 3<> "$TRACK/wait"
    echo 'fixture kernel event'
    echo started >> "$TRACK/events"
    read -r -t 30 ignored <&3 || true
    ;;
esac
PROBE
chmod +x "$fixture/bin/probe"
for probe in uname zprint vm_stat sysctl ps df date log; do
  ln -s probe "$fixture/bin/$probe"
done
export PATH="$fixture/bin:$PATH"

wait_for_stream() {
  local track=$1 attempt
  for ((attempt = 0; attempt < 200; attempt++)); do
    [ ! -s "$track/events" ] || return 0
    sleep 0.05
  done
  echo 'fixture kernel stream did not start' >&2
  return 1
}

[ "$(bash "$script" status "$first")" = 'host-series is stopped' ]
[ ! -e "$first" ]
TRACK="$fixture/first-log" bash "$script" start "$first" "$fixture" > "$fixture/start1" &
start1=$!
TRACK="$fixture/first-log" bash "$script" start "$first" "$fixture" > "$fixture/start2" &
start2=$!
wait "$start1"
wait "$start2"
sort "$fixture/start1" "$fixture/start2" > "$fixture/starts"
sed -E 's/ as process [1-9][0-9]*$//' "$fixture/starts" |
  diff -u <(printf 'host-series already runs\nhost-series started\n') -
[ "$(bash "$script" status "$first")" = 'host-series is running' ]
wait_for_stream "$fixture/first-log"

{
  printf 'time\tkalloc1024_elements\tfree_pages\twired_pages\tcompressor_pages\tswap_used_M\tswap_free_M\tpressure\tload1\tprocesses\n'
  printf '2026-11-19 08:00:00\t42\t11\t22\t33\t256.00\t768.00\t1\t2.50\t2\n'
} | diff -u - "$first/host-series.tsv"
awk -F '\t' 'NR == 2 && $4 ~ /^[1-9][0-9]*$/ { print $1, $2, $3, $5 }' "$first/host-top.tsv" |
  diff -u <(echo '2026-11-19 08:00:00 42 900 alpha:2;beta:2;extra_cpu:1') -
diff -u <(echo 'fixture kernel event') "$first/host-kernel.log"

FAILED_PROBES=1 TRACK="$fixture/second-log" bash "$script" start "$second" "$fixture"
wait_for_stream "$fixture/second-log"
[ "$(awk -F '\t' 'NR == 2 { print $2, $3, $4, $5, $6, $7, $8, $9, $10 }' "$second/host-series.tsv")" = '42 ? ? ? ? ? ? ? ?' ]
[ "$(awk -F '\t' 'NR == 2 { print $3, $5 }' "$second/host-top.tsv")" = '900 ?' ]
[ "$(bash "$script" stop "$first")" = 'host-series stopped' ]
[ "$(bash "$script" status "$first")" = 'host-series is stopped' ]
[ "$(bash "$script" stop "$first")" = 'host-series is stopped' ]
[ "$(bash "$script" status "$second")" = 'host-series is running' ]
diff -u <(printf 'started\nstopped\n') "$fixture/first-log/events"
diff -u <(echo started) "$fixture/second-log/events"
bash "$script" stop "$second"
diff -u <(printf 'started\nstopped\n') "$fixture/second-log/events"

series_lines=$(awk 'END { print NR }' "$first/host-series.tsv")
top_lines=$(awk 'END { print NR }' "$first/host-top.tsv")
: > "$fixture/first-log/events"
start=$(TRACK="$fixture/first-log" bash "$script" start "$first" unavailable)
[ "$(cat "$fixture/first-log/disk")" = timed-out ]
recorder=${start##* }
[[ $recorder =~ ^[1-9][0-9]+$ ]]
wait_for_stream "$fixture/first-log"
# This PID comes from the successful launch above. The recorder owns no other job.
kill -KILL "$recorder"
bash "$script" stop "$first"
diff -u <(printf 'started\nstopped\n') "$fixture/first-log/events"
[ "$(bash "$script" status "$first")" = 'host-series is stopped' ]
[ ! -s "$first/host-recorder.log" ]
[ ! -s "$second/host-recorder.log" ]
[ "$(awk 'END { print NR }' "$first/host-series.tsv")" = "$((series_lines + 1))" ]
[ "$(awk -F '\t' 'END { print NR, $3 }' "$first/host-top.tsv")" = "$((top_lines + 1)) ?" ]

mkdir -p "$fixture/unwritable/host-recorder.log"
if bash "$script" start "$fixture/unwritable" "$fixture" > "$fixture/start-error" 2>&1; then
  echo 'start succeeded with an unusable log path' >&2
  exit 1
fi
[ ! -e "$fixture/unwritable/host-series.tsv" ]
echo 'host-series: sample, concurrent start, isolated stop, restart and failed probes pass'
