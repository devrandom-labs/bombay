#!/usr/bin/env bash
set -Eeuo pipefail
helper=$(realpath -- "${OWN_TEST_HELPER:-$PWD/architecture/tests/own-processes.sh}")
. "$helper"
work=$(mktemp -d); own_init "$work"; export OWN_HELPER=$helper; set -m
cleanup() {
  if [ -n "${member:-}" ]; then touch "$work/release-member"; local end=$((SECONDS + 10)); while builtin kill -0 "$member" 2>/dev/null && [ ! -e "$work/member-done" ] && [ "$SECONDS" -lt "$end" ]; do sleep 0.01; done; fi
  for child in $(jobs -pr); do builtin kill -KILL -- "-$child" 2>/dev/null || true; builtin kill -KILL "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true; done
  [ -z "${member:-}" ] || [ -e "$work/member-done" ] || ! builtin kill -0 "$member" 2>/dev/null || { echo "own-processes-test: retained $work for member cleanup" >&2; return; }
  rm -rf "$work"
}
trap cleanup EXIT
fail() {
  local file
  echo "own-processes-test: FAILED: $*" >&2
  if [ -f "$work/identity.trace" ]; then
    for file in identity.trace report calls; do
      [ -f "$work/$file" ] || continue
      printf 'own-processes-test: %s\n' "$file" >&2
      cat "$work/$file" >&2
    done
  fi
  exit 1
}
trap 'fail "unexpected failure at line $LINENO"' ERR

sed '/^---$/,$d' architecture/locale.yml > "$work/temporary-locale.yml"
locale_scan=(ast-grep scan --report-style short --rule "$work/temporary-locale.yml")
"${locale_scan[@]}" --globs '*.sh' architecture || fail 'temporary locale assignment check'
case "$helper" in
  "$(realpath architecture)"/*) ;;
  *) "${locale_scan[@]}" --stdin < "$helper" || fail 'temporary locale assignment check' ;;
esac
echo 'own-processes-test: temporary locale assignments use env: pass'

wait_dead() { local end=$((SECONDS + 10)); while builtin kill -0 "$1" 2>/dev/null; do [ "$SECONDS" -lt "$end" ] || fail 'process survived cleanup'; sleep 0.01; done; wait "$1" 2>/dev/null || true; }
wait_file() { local end=$((SECONDS + 10)); while [ ! -e "$1" ]; do [ "$SECONDS" -lt "$end" ] || fail 'fixture timed out'; sleep 0.01; done; }
# Prove refusal before checking survival; a mutant's PID 0 is never forwarded.
kill() { printf '%s\n' "$*" >> "$work/calls"; [ ! -e "$work/kill-fails" ] || return 7; case "${2:-}/${3:-}" in --/[0-9]*) return 2 ;; esac; [ "${@: -1}" = 0 ] || builtin kill "$@"; }
sleep 60 & decoy=$!
if own_identity "$decoy" >/dev/null; then own_record "$decoy" >/dev/null; own_cleanup; wait_dead "$decoy"; echo 'own-processes-test: native identity: pass'
else builtin kill -KILL "$decoy"; wait "$decoy" 2>/dev/null || true; echo 'own-processes-test: native identity unavailable'; fi
rm -f "$OWN_RECORDS/"*; export OWN_TEST_PID=$$
cat > "$work/ps" <<'PS'
#!/bin/sh
echo "$4" >> "$OWN_RECORDS/../queries"
if [ "$4" = "$OWN_TEST_PID" ]; then echo parent; exit 0; fi
if [ -e "$OWN_RECORDS/block-$4" ]; then touch "$OWN_RECORDS/../record-blocked"; while [ -d "$OWN_RECORDS" ] && [ ! -e "$OWN_RECORDS/../release-record" ]; do sleep 0.01; done; fi
[ ! -e "$OWN_RECORDS/ps-denied" ] || exit 126
if [ -e "$OWN_RECORDS/locale-$4" ]; then printf '%s\n' "${LC_ALL:-unset}"; exit 0; fi
[ -f "$OWN_RECORDS/source-$4" ] || exit 1
cat "$OWN_RECORDS/source-$4"
[ ! -e "$OWN_RECORDS/ps-fails" ]
PS
printf '#!/bin/sh\ncat "$OWN_RECORDS/../user"\n[ ! -e "$OWN_RECORDS/../id-fails" ]\n' > "$work/id"
chmod +x "$work/ps" "$work/id"; OWN_PS=$work/ps OWN_PROC='' OWN_ID=$work/id; echo ordinary > "$work/user"
# The stub makes locale differences observable even in a build without installed locales.
sleep 60 & locale_child=$!
touch "$OWN_RECORDS/locale-$locale_child"
(export LC_ALL=C; own_record "$locale_child" >/dev/null)
(export LC_ALL=POSIX; own_cleanup > "$work/report")
[ ! -s "$work/report" ] || fail 'locale change prevented cleanup'
wait_dead "$locale_child"
rm -f "$OWN_RECORDS/$locale_child" "$OWN_RECORDS/locale-$locale_child"
echo 'own-processes-test: ps identity survives a caller locale change: pass'
cat > "$work/group" <<'GROUP'
set +m; "$BASH" -c 'while [ ! -e "$1/release-member" ]; do sleep 0.01; done; touch "$1/member-done"' _ "$1" &
. "$OWN_HELPER"; own_publish $! "$1/member"; while [ ! -e "$1/end-leader" ]; do sleep 0.01; done
GROUP
"$BASH" "$work/group" "$work" & leader=$!; wait_file "$work/member"; member=$(cat "$work/member") || fail "member lookup status=$?"
echo previous > "$OWN_RECORDS/source-$leader"; own_record "$leader" group >/dev/null; [ -f "$OWN_RECORDS/-$leader" ] || fail 'group record missing'; touch "$work/end-leader"; wait "$leader"; rm "$OWN_RECORDS/source-$leader"
: > "$work/calls"
# Keep the actual Bash substitution status, before own_identity maps it to unknown.
saved_ps4=$PS4
PS4='+ status=$? '
exec {trace_fd}> "$work/identity.trace"
BASH_XTRACEFD=$trace_fd
set -x
own_cleanup > "$work/report"
set +x
unset BASH_XTRACEFD
PS4=$saved_ps4
[ ! -s "$work/calls" ] || fail 'foreign leaderless group was signalled'
builtin kill -0 "$member" || fail 'foreign member died'
[ ! -s "$work/report" ] || fail 'absent leader counted'
rm "$work/identity.trace"
own_forget "$leader"; [ ! -e "$OWN_RECORDS/-$leader" ] || fail 'group was not forgotten'
: > "$OWN_RECORDS/-123"; : > "$OWN_RECORDS/-456"; own_forget 123 456; [ ! -e "$OWN_RECORDS/-123" ] && [ ! -e "$OWN_RECORDS/-456" ] || fail 'group batch was not forgotten'
echo 'own-processes-test: foreign leaderless group survives and record is forgotten: pass'
echo current > "$OWN_RECORDS/source-$member"; own_record "$member" >/dev/null; own_signal KILL "$member" || fail 'delivered signal status'; wait_dead "$member"; member=''; rm -f "$OWN_RECORDS/"*
for user in ordinary _nixbld3 nixbld; do echo "$user" > "$work/user"; for kind in process group; do
  sleep 60 & child=$!; echo original > "$OWN_RECORDS/source-$child"; own_record "$child" "$kind" >/dev/null; : > "$work/queries"
  sleep 60 & peer=$!; echo original > "$OWN_RECORDS/source-$peer"; own_record "$peer" >/dev/null
  own_cleanup > "$work/report"; wait_dead "$child"; wait_dead "$peer"; [ ! -s "$work/report" ] || fail 'own cleanup reported skips'; [ "$(grep -cx "$$" "$work/queries")" = 1 ] || fail 'source checked more than once'
  rm -f "$OWN_RECORDS/"*; : > "$work/calls"; sleep 60 & decoy=$!; echo original > "$OWN_RECORDS/source-$decoy"; own_record "$decoy" "$kind" >/dev/null; echo replacement > "$OWN_RECORDS/source-$decoy"
  target=$decoy; [ "$kind" != group ] || target=-$decoy; if own_signal TERM "$target"; then fail 'changed identity returned success'; fi
  own_cleanup > "$work/report"; [ ! -s "$work/calls" ] || fail "foreign $kind was signalled"; builtin kill -0 "$decoy" || fail 'decoy died'; grep -qx 'own-processes: left 1 changed and 0 unchecked recorded numbers alone' "$work/report" || fail 'changed count'
  builtin kill -KILL "$decoy"; wait "$decoy" 2>/dev/null || true; rm -f "$OWN_RECORDS/"*
done; done
echo 'own-processes-test: own leftovers end; foreign processes/groups survive for all users: pass'
echo ordinary > "$work/user"; sleep 60 & decoy=$!
for fault in failed empty unrecorded denied absent nosource; do echo original > "$OWN_RECORDS/source-$decoy"; own_record "$decoy" >/dev/null; own_record "$decoy" group >/dev/null; : > "$work/calls"
  case "$fault" in failed) touch "$OWN_RECORDS/ps-fails" ;; empty|unrecorded) : > "$OWN_RECORDS/source-$decoy"; own_record "$decoy" >/dev/null; own_record "$decoy" group >/dev/null; [ "$fault" = empty ] || echo new > "$OWN_RECORDS/source-$decoy" ;; denied) touch "$OWN_RECORDS/ps-denied" ;; absent) rm "$OWN_RECORDS/source-$decoy" ;; nosource) OWN_PS='' ;; esac
  own_cleanup > "$work/report"; [ ! -s "$work/calls" ] || fail "$fault query signalled decoy"; builtin kill -0 "$decoy" || fail 'decoy died'
  if own_signal TERM "$decoy"; then fail "$fault refusal returned success"; fi
  if [ "$fault" = absent ]; then [ ! -s "$work/report" ] || fail 'ended records counted'; else grep -qx 'own-processes: left 0 changed and 2 unchecked recorded numbers alone' "$work/report" || fail "$fault unchecked count"; fi
  rm -f "$OWN_RECORDS/"*; OWN_PS=$work/ps
done
echo 'own-processes-test: failed, empty, denied, absent and unavailable sources: pass'
OWN_PS=''; for user in _nixbld3 nixbld17 nixbld; do echo "$user" > "$work/user"; for kind in process group; do
  sleep 60 & child=$!; own_record "$child" "$kind" >/dev/null; own_cleanup; wait_dead "$child"; rm -f "$OWN_RECORDS/"*
done; done
: > "$work/calls"; if own_signal KILL "$decoy"; then fail 'missing record returned success'; fi; echo 0 > "$OWN_RECORDS/0"; if own_signal KILL 0; then fail 'invalid record returned success'; fi; [ ! -s "$work/calls" ] || fail 'missing or invalid record was signalled'; builtin kill -0 "$decoy" || fail 'decoy died'
touch "$work/id-fails"; ! own_build_user || fail 'failed id accepted'; rm "$work/id-fails"
for user in ordinary _nixbld nixbld2suffix _nixbld-1; do echo "$user" > "$work/user"; ! own_build_user || fail "accepted user $user"; done
OWN_PS=$work/ps; echo original > "$OWN_RECORDS/source-0"; ! own_identity 0 >/dev/null || fail 'accepted identity zero'; rm -f "$OWN_RECORDS/"*; echo ordinary > "$work/user"
echo 'own-processes-test: only build users permit unchecked cleanup; missing/invalid records refused: pass'
echo original > "$OWN_RECORDS/source-$decoy"; own_record "$decoy" >/dev/null; touch "$work/kill-fails"; : > "$work/calls"; own_result=0; own_signal TERM "$decoy" || own_result=$?; [ "$own_result" = 7 ] || fail 'kill failure status was lost'
[ -s "$work/calls" ] || fail 'failed kill was not attempted'; builtin kill -0 "$decoy" || fail 'failed-kill decoy died'; rm "$work/kill-fails"
echo previous > "$work/published"; rm "$OWN_RECORDS/$decoy"; mkdir "$OWN_RECORDS/$decoy"; if own_write "$decoy" "$work/published" 2>/dev/null; then fail 'failed record write published PID'; fi
[ "$(cat "$work/published")" = previous ] || fail 'failed record changed PID file'; rmdir "$OWN_RECORDS/$decoy"
touch "$OWN_RECORDS/block-$decoy"; own_write "$decoy" "$work/published" & writer=$!; wait_file "$work/record-blocked"
[ "$(cat "$work/published")" = previous ] || fail 'PID file truncated during identity read'; touch "$work/release-record"; wait "$writer"; [ "$(cat "$work/published")" = "$decoy" ] || fail 'PID file publication'; rm -f "$OWN_RECORDS/"*
for signal_shell in "$BASH" /bin/sh $(command -v dash || true); do "$signal_shell" -ec '. "$OWN_HELPER"; export OWN_TEST_PID=$$; sleep 60 & p=$!; trap '\''for pending in $(jobs -p); do kill -KILL "$pending" 2>/dev/null || true; wait "$pending" 2>/dev/null || true; done'\'' EXIT; echo original > "$OWN_RECORDS/source-$p"; own_write "$p" "$OWN_RECORDS/../shell-pid"; [ "$(cat "$OWN_RECORDS/../shell-pid")" = "$p" ]; own_signal TERM "$p"; wait "$p" || [ "$?" -gt 128 ]' || fail "positive signal in $signal_shell"; done; rm -f "$OWN_RECORDS/"*
for previous in absent previous; do
  rm -f "$work/published" "$work/write-blocked" "$work/release-write"
  [ "$previous" = absent ] || echo previous > "$work/published"
  ( printf() { if [ "${2:-}" = "$decoy" ]; then
      [ "$previous" = absent ] || builtin printf '%s' "${decoy:0:1}"
      touch "$work/write-blocked"; wait_file "$work/release-write"
      if [ "$previous" != absent ]; then builtin printf '%s\n' "${decoy:1}"; return; fi
    fi; builtin printf "$@"; }; own_publish "$decoy" "$work/published" ) & writer=$!
  ( wait_file "$work/published"; value=$(cat "$work/published"); [ "$value" = "$decoy" ] || [ "$value" = "$previous" ] || fail 'reader saw incomplete PID' ) & reader=$!
  wait_file "$work/write-blocked"
  if [ "$previous" = absent ]; then [ ! -e "$work/published" ] || fail 'PID visible before complete write'; else [ "$(cat "$work/published")" = previous ] || fail 'PID replaced before complete write'; fi
  touch "$work/release-write"; wait "$writer"; wait "$reader"; [ "$(cat "$work/published")" = "$decoy" ] || fail 'renamed PID content'
done
echo 'own-processes-test: stopped writer publishes only a complete PID by rename: pass'
echo 'own-processes-test: delivery status, atomic PID publication and sh signals: pass'
OWN_PROC=$work/proc; mkdir -p "$OWN_PROC/$$" "$OWN_PROC/$decoy"; stat="$decoy (a12345) name) S"; for field in {4..21}; do stat="$stat 0"; done; stat="$stat 12345"; printf '%s\n' "$stat" > "$OWN_PROC/$$/stat"
for fault in changed partial empty short; do printf '%s\n' "$stat" > "$OWN_PROC/$decoy/stat"; [ "$(own_identity "$decoy")" = proc:12345 ] || fail 'proc start field'; own_record "$decoy" >/dev/null; : > "$work/calls"
  case "$fault" in changed) printf '%s\n' "${stat% 12345} 67890" ;; partial) printf '%s' "$stat" ;; empty) : ;; short) printf '%s\n' "$decoy (short) S" ;; esac > "$OWN_PROC/$decoy/stat"; [ "$fault" = changed ] || own_record "$decoy" >/dev/null
  own_cleanup > "$work/report"; [ ! -s "$work/calls" ] || fail "$fault proc record signalled decoy"; builtin kill -0 "$decoy" || fail 'decoy died'; rm -f "$OWN_RECORDS/"*
done
echo 'own-processes-test: proc parser and changed, partial, empty, short records: pass'
printf '%s\n' "$stat" > "$OWN_PROC/$decoy/stat"; own_record "$decoy" >/dev/null; rm -rf "$OWN_PROC/$decoy"; OWN_PS=''; : > "$work/calls"; own_cleanup > "$work/report"; [ ! -s "$work/report" ] || fail 'absent proc counted unchecked'; [ ! -s "$work/calls" ] || fail 'absent proc was signalled'; OWN_PS=$work/ps
OWN_PROC=''; "$BASH" -c 'trap '\''touch "$1/term"; exit 0'\'' TERM; touch "$1/term-ready"; while :; do sleep 0.01; done' _ "$work" & child=$!
echo original > "$OWN_RECORDS/source-$child"; own_record "$child" >/dev/null; wait_file "$work/term-ready"; : > "$work/calls"; own_cleanup TERM; own_stop "$((SECONDS + 2))" "$(command -v sleep)"; wait_dead "$child"; [ -e "$work/term" ] || fail 'TERM trap did not run before wait'; ! grep -q KILL "$work/calls" || fail 'KILL preceded willing TERM trap'
rm "$OWN_RECORDS/source-$child"; own_cleanup > "$work/report"; [ ! -s "$work/report" ] || fail 'ended process counted'
echo 'own-processes-test: TERM trap and wait precede final KILL: pass'
(
  set +m
  "$BASH" -c 'trap "" TERM; touch "$1/stubborn-ready"; while :; do sleep 0.01; done' _ "$work" & stubborn=$!
  own_publish "$stubborn" "$work/stubborn-pid"; wait_file "$work/stubborn-ready"
  own_stop "$((SECONDS + 2))" "$(command -v sleep)"
  for pending in $(jobs -p); do wait "$pending" 2>/dev/null || true; done
  touch "$work/cleanup-done"
) & cleaner=$!
end=$((SECONDS + 8)); while [ ! -e "$work/cleanup-done" ]; do [ "$SECONDS" -lt "$end" ] || fail 'TERM-ignoring child held cleanup past deadline'; sleep 0.01; done
wait "$cleaner"; ! builtin kill -0 "$(cat "$work/stubborn-pid")" 2>/dev/null || fail 'TERM-ignoring child survived cleanup'
echo 'own-processes-test: TERM-ignoring direct child ends within cleanup deadline: pass'
