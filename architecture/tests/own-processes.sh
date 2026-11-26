#!/usr/bin/env bash
# Sourceable by the harness shells and their sh fixtures.
own_init() {
  export OWN_HELPER="$PWD/architecture/tests/own-processes.sh"
  export OWN_RECORDS="$1/owners" OWN_PS="$(command -v ps || true)" OWN_PROC=/proc
  export OWN_ID="$(command -v id || true)"
  mkdir -p "$OWN_RECORDS"
}
# Status 1 is absent, 2 unknown, 3 invalid. Absence never authorizes a signal.
own_identity() {
  local stat value status=0
  case "$1" in ''|*[!0-9]*|0|1) return 3 ;; esac
  if [ -n "$OWN_PROC" ] && [ -d "$OWN_PROC/$1" ]; then
    IFS= read -r stat < "$OWN_PROC/$1/stat" 2>/dev/null || return 2
    # comm may contain spaces and parentheses; starttime is field 22.
    value=$(printf '%s\n' "${stat##*) }" | awk '{print $20}')
    [ -n "$value" ] || return 2
    printf 'proc:%s\n' "$value"
  elif [ -n "$OWN_PROC" ] && [ -r "$OWN_PROC/$$/stat" ]; then return 1
  else
    [ -n "$OWN_PS" ] || return 2
    value=$(env LC_ALL=C "$OWN_PS" -o lstart= -p "$1" 2>&1) || status=$?
    if [ "$status" != 0 ]; then [ "$status" = 1 ] && [ -z "$value" ] && return 1; return 2; fi
    [ -n "$value" ] || return 2
    printf 'ps:%s\n' "$value"
  fi
}
own_build_user() {
  local user
  user=$("$OWN_ID" -un 2>/dev/null) || return 1
  case "$user" in
    nixbld|_nixbld[0-9]*|nixbld[0-9]*) case "${user#*nixbld}" in *[!0-9]*) return 1 ;; esac ;;
    *) return 1 ;;
  esac
}
own_source() { if own_identity "$$" >/dev/null; then echo checked; elif own_build_user; then echo build; else echo unknown; fi; }
own_record() {
  local identity target=$1
  [ "${2:-}" != group ] || target=-$1
  identity=$(own_identity "$1") || identity=''
  printf '%s\n' "$identity" > "$OWN_RECORDS/$target" || return
  printf '%s\n' "$1"
}
own_publish() {
  local pending directory=${2%/*}
  [ "$directory" != "$2" ] || directory=.
  pending=$(mktemp "$directory/.own-pid.XXXXXXXX") || return
  printf '%s\n' "$1" > "$pending" && mv -f "$pending" "$2"
}
own_write() { local pid; pid=$(own_record "$1" "${3:-}") || return; own_publish "$pid" "$2"; }
# Bash drivers pass one cleanup deadline and their real sleep (fixtures replace it).
own_stop() {
  local child
  for child in $(jobs -pr); do kill -TERM "$child" 2>/dev/null || true; done
  while [ -n "$(jobs -pr)" ] && [ "$SECONDS" -lt "$1" ]; do "$2" 0.01; done
  for child in $(jobs -pr) $(jobs -ps); do kill -KILL "$child" 2>/dev/null || true; done
}
# Call with job control enabled and before the suite starts other jobs.
own_test_stopped_cleanup() {
  local stopped pending
  "$BASH" -c 'kill -STOP "$$"; exec sleep 60' &
  stopped=$!
  wait "$stopped" 2>/dev/null || true
  if [ "$(jobs -ps)" = "$stopped" ]; then
    "$1"
    [ -z "$(jobs -p)" ] && return 0
    echo 'stopped direct child survived cleanup' >&2
  else
    echo 'direct child did not stop' >&2
  fi
  for pending in $(jobs -p); do
    kill -KILL "$pending" 2>/dev/null || true
    wait -f "$pending" 2>/dev/null || true
  done
  return 1
}
own_forget() { local pid; for pid in "$@"; do rm -f "$OWN_RECORDS/-$pid"; done; }
own_signal() {
  local expected='' current='' pid=${2#-} status=0 source=${3:-$(own_source)}
  OWN_RESULT=''
  case "$pid" in ''|*[!0-9]*|0|1) return 1 ;; esac
  [ -f "$OWN_RECORDS/$2" ] || return 1
  IFS= read -r expected < "$OWN_RECORDS/$2" || true
  if [ "$source" = checked ]; then
    current=$(own_identity "$pid") || status=$?
    if [ -z "$expected" ] || [ "$expected" != "$current" ]; then
      if [ "$status" != 1 ]; then OWN_RESULT=unchecked; [ "$status" != 0 ] || [ -z "$expected" ] || OWN_RESULT=changed; fi
      return 1
    fi
  elif [ "$source" != build ]; then OWN_RESULT=unchecked; return 1; fi
  case "$2" in -*) kill -"$1" -- "$2" ;; *) kill -"$1" "$2" ;; esac
}
own_cleanup() {
  local record signal=${1:-KILL} source changed=0 unchecked=0
  source=$(own_source)
  for record in "$OWN_RECORDS"/-* "$OWN_RECORDS"/[0-9]*; do
    [ -f "$record" ] || continue
    case "${record##*/}" in -*) own_signal KILL "${record##*/}" "$source" 2>/dev/null || true ;; *) own_signal "$signal" "${record##*/}" "$source" 2>/dev/null || true ;; esac
    case "$OWN_RESULT" in changed) changed=$((changed + 1)) ;; unchecked) unchecked=$((unchecked + 1)) ;; esac
  done
  [ "$signal" = TERM ] || { [ "$((changed + unchecked))" = 0 ] || printf 'own-processes: left %s changed and %s unchecked recorded numbers alone\n' "$changed" "$unchecked"; }
}
