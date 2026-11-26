#!/usr/bin/env bash
# Supervisor of the automatic run of one lane (.agents/project.md, Automatic run).
#
#   bash architecture/run.sh <lane>          work through the ready cards
#   bash architecture/run.sh <lane> --once   one session, then stop
#
# It selects a ready open-change owner before the first ready card of the backlog.
# It brings main in, starts a fresh lead with a time limit and acts on the lead's
# last line. A card that cannot move is parked for this run, or held by its
# needs-joel label; the next ready card starts.
# A park with `blocked #N` returns when card N is done. The number follows
# `blocked` and a space, with a space, the end or punctuation after the number.
# A full disk never stops it: it makes space (architecture/space.sh), waits
# for space and tells Joel. It waits while a card is set aside or waits for
# Joel. It stops when no card is ready, none is set aside and none waits for
# Joel, or when the lead or GitHub stays unusable for RUN_RETRIES tries.
# STEP gate [reason] asks the run to run the full gate and, after a pass, land the step.
# A restart with main keeps parked cards, card counts and the set-aside cards.
# History, status and the weekly design review file stay in place.
# The run follows the supervisor copy in its worktree.
# A merge turn covers bringing main in, the requested gate and the landing.
# No lead session holds the turn.
# bash architecture/run.sh turn <command> [arguments...] holds the same turn
# for a command, without starting a lane.
# Redirect stdin, stdout and stderr; a terminal on any stream is refused.
# A first command word containing = is refused rather than passed to env.
# Use bash [<path>/]architecture/run.sh turn; another known command line is refused.
# Children moved to another group or session are the command's own to stop.
# KILL of this process leaves its command running; the next taker removes its turn folder.
# Calling run.sh turn inside a process holding the same turn waits for that holder.
# A command that reads /dev/tty can stop its group.
# A stopped command that traps TERM keeps the turn until it is continued or killed.
# Untested limit: changing user so kill -0 gets EPERM can release the turn before the command ends.
# Started with & without job control, ignored INT and QUIT cannot be trapped or forwarded, as in gate.sh.
#
# STEP_LEAD     lead command; you set it, this script names no access level
# STEP_LIMIT    minutes that one session may run            (120)
# RUN_GATE_LIMIT minutes that the full gate may run         (360)
# RUN_LAND_LIMIT seconds for each landing call or held notice (300)
# RUN_LAND_KILL seconds after TERM before a landing, fetch or notice gets KILL (10)
# RUN_TURN     merge turn folder (per-user Labcast cache/turn.lock)
# RUN_TURN_POLL seconds between tries for the merge turn    (20)
# RUN_TURN_TELL seconds before one notice of a turn wait    (7200)
# RUN_TURN_FETCH seconds allowed for fetch while holding the turn (300)
# RUN_TURN_PS  command that reads holder and waiter process details (ps)
# RUN_TURN_RESUME passes a queue entry to a replacement supervisor.
# STEP_FREE     free GiB that a session needs               (15)
# RUN_FULL      free GiB below which the disk counts as full after a session (2)
# RUN_RETRY_WAIT seconds between tries when the lead, GitHub or the disk is unusable (600)
# RUN_RETRIES   such tries in a row before the run stops for the lead or GitHub (36)
# RUN_REVIEW    0 turns the weekly design review off; the Nix lane has none (1)
# RUN_NOTIFY    command that tells Joel one line; a macOS notification by default
# The test also sets RUN_ITEMS, RUN_SYNC, RUN_QUICK, RUN_CLOCK, RUN_CLEAN,
# RUN_FREE, RUN_OPEN, RUN_BRANCH, RUN_NOW, RUN_CHECKS, RUN_WEEK, RUN_GATE,
# RUN_PUSH (git) and RUN_GH (gh).
set -uo pipefail

lane=${1:?usage: run.sh <lane> [--once]}
once=${2:-}
gate_turn=0
if [ "$lane" = gate ]; then
  # Private entry from gate.sh. Keep its PID, streams and foreground group.
  gate_turn=1 lane=turn
fi
if [ "$lane" = turn ] && [ "$#" -lt 2 ]; then
  echo 'usage: run.sh turn <command> [arguments...]'; exit 2
fi
supervisor_sum=$(cksum < "$0") rejected_sums="|"
# The lanes that the run works (.agents/project.md, Automatic run). The weekly
# design review is for the Server lane and the Studio lane (AGENTS.md, Design
# review): its procedure reads Rust code.
root=$(cd "$(dirname "$0")/.." && pwd)
supervisor_path=$(cd "$(dirname "$0")" && pwd -P)/$(basename "$0")
if [ -f "$root/architecture/project.env" ]; then . "$root/architecture/project.env"; fi
case "$lane" in
  runtime | domain | adapters | store | identity | network | server | studio) review=${RUN_REVIEW:-1} ;;
  nix) review=0 ;;
  turn) ;;
  *) echo "run: unknown lane $lane; choose from ${HARNESS_LANES:-runtime domain adapters nix}"; exit 2 ;;
esac
repo=${HARNESS_REPO:-devrandom-labs/bombay}
ready_label=ready-again
limit=${STEP_LIMIT:-120} gate_limit=${RUN_GATE_LIMIT:-360} free=${STEP_FREE:-15}
checks_limit=2160
retry_wait=${RUN_RETRY_WAIT:-600} retries=${RUN_RETRIES:-36} full_at=${RUN_FULL:-2}
quick=${RUN_QUICK:-120} sync=${RUN_SYNC:-1} unit=${RUN_CLOCK:-60}
turn=${RUN_TURN:-${XDG_CACHE_HOME:-$HOME/.cache}/${HARNESS_CACHE_NAME:-bombay}/turn.lock}
while [[ $turn == */ && $turn != / ]]; do turn=${turn%/}; done
turn_queue=$turn.wait turn_waiter="" turn_pass_reported=""
turn_poll=${RUN_TURN_POLL:-20} turn_tell=${RUN_TURN_TELL:-7200} turn_fetch=${RUN_TURN_FETCH:-300}
if [ "$gate_turn" = 1 ]; then turn_enabled=1
else turn_enabled=$(command -v timeout) || echo 'run: timeout is not on PATH; taking no merge turn'; fi
if [ "$lane" != turn ]; then
  mkdir -p .cache
  history=.cache/run.history status=.cache/run.status
fi

say() { echo "run: $*"; }
note() { printf '%s\t%s\n' "$(date '+%Y-%m-%d %H:%M')" "$*" >> "$history"; echo "$*" > "$status"; }
# One line for Joel when the run needs him or ends.
notify() {
  local cap=()
  if [ "${turn_held:-0}" = 1 ]; then
    cap=(timeout --signal=TERM --kill-after="${RUN_LAND_KILL:-10}" "${RUN_LAND_LIMIT:-300}")
  fi
  if [ -n "${RUN_NOTIFY:-}" ]; then ${cap[@]+"${cap[@]}"} $RUN_NOTIFY "$lane: $1" || true
  elif command -v osascript > /dev/null; then
    ${cap[@]+"${cap[@]}"} osascript -e 'on run a' -e 'display notification (item 1 of a) with title (item 2 of a)' -e 'end run' \
      "$1" "${HARNESS_CACHE_NAME:-Bombay} run: $lane" > /dev/null 2>&1 || true
  fi
}
stop() { say "$1"; note "stopped: $1"; notify "stopped: $1"; summary; exit "${2:-1}"; }

if [ "$lane" != turn ]; then
  if [ -z "${STEP_LEAD:-}" ]; then
    say "STEP_LEAD is not set. Set the lead command first, for example:"
    say '  export STEP_LEAD="codex exec -m gpt-6.2-sol -s <your access level>"'
    exit 2
  fi
  if [ -z "${RUN_ITEMS:-}" ]; then
    export GH_TOKEN=${GH_TOKEN:-$(gh auth token --user joeldsouzax)}
    export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=credential.https://github.com.helper GIT_CONFIG_VALUE_0='!gh auth git-credential'
  fi
fi

parked="" parked_lines="" done_steps=0
saved_counts=() aside_ends=() aside_runs=() parked_behind=() parked_text=()
summary() {
  say "steps merged or planned in this run: $done_steps"
  [ -z "$parked_lines" ] || { say "parked in this run:"; printf '%b' "$parked_lines"; }
}

# Use the same GitHub items for selection and the owner check at STEP later.
# The branch is read again: a lead may switch it during its session.
choice() {
  local branch=""
  if open_change; then branch=$(${RUN_BRANCH:-git branch --show-current} 2> /dev/null) || branch=""; fi
  jq -r --arg lane "$lane" --arg mode next --arg skip "$1" --arg branch "$branch" \
    -f "$root/architecture/backlog.jq" <<< "$items"
}

card_state() {
  jq -r --arg lane "$lane" --arg mode state --arg skip "" --arg number "$1" \
    -f "$root/architecture/backlog.jq" <<< "$items"
}

# Ends the park of a card in this run and starts it with fresh counts.
# An entry of parked_lines starts at the start or after a literal backslash-n; a title or a
# reason that holds a literal backslash-n makes a false entry start (accepted limit).
return_card() { # number, message
  local number=$1 newline='\n' text prefix kept="" rest=$parked_lines
  text=${parked_text[number]:-}
  parked=,$parked,; parked=${parked/,$number,/,}; parked=${parked#,}; parked=${parked%,}
  # A state saved by an older supervisor has no text for a park that was not behind a blocker.
  prefix="  #$number "; [ -z "$text" ] || prefix=$text
  while [ -n "$rest" ]; do
    if [[ $rest == "$prefix"* ]]; then
      if [ -n "$text" ]; then rest=${rest#"$prefix"}
      elif [[ $rest == *"$newline"* ]]; then rest=${rest#*"$newline"}
      else rest=""; fi
      break
    fi
    if [[ $rest == *"$newline"* ]]; then
      kept=$kept${rest%%"$newline"*}$newline; rest=${rest#*"$newline"}
    else kept=$kept$rest; rest=""; fi
  done
  parked_lines=$kept$rest
  unset 'parked_behind[number]' 'parked_text[number]'
  unset 'saved_counts[number]' 'last_passed_requests[number]'
  # Do not save or reuse the old counts when this was the last session.
  if [ "${last%%$'\t'*}" = "#$number" ]; then last=""; fi
  say "$2"; note "$2"
}

# Sets card to "#number<TAB>task id<TAB>title", empty when none is ready.
# A ready open-change owner comes first. Also sets owner and first for its notice.
# Returns 1 when GitHub cannot be read.
select_card() {
  local number selection state message skip returned=() labelled=""
  if [ -n "${RUN_ITEMS:-}" ]; then items=$(cat "$RUN_ITEMS" 2> /dev/null) || return 1
  else items=$(bash "$root/architecture/board.sh" 2> /dev/null) || return 1; fi
  # The label is removed before any later read, so a failure after it keeps the return.
  if [ -n "$parked" ]; then
    labelled=$(jq -r --arg label "$ready_label" '.items[] | select((.labels // []) | index($label)) | .content.number' <<< "$items") || return 1
  fi
  for number in ${parked//,/ }; do
    [[ $'\n'$labelled$'\n' == *$'\n'"$number"$'\n'* ]] || continue
    # shellcheck disable=SC2086
    if ${RUN_GH:-gh} issue edit "$number" -R "$repo" --remove-label "$ready_label" > /dev/null 2>&1; then
      return_card "$number" "returned #$number: the label $ready_label"
    else
      say "no return of #$number by the label $ready_label: the label could not be removed"
    fi
  done
  skip=,$parked,
  for number in "${!parked_behind[@]}"; do
    state=$(card_state "${parked_behind[number]}") || return 1
    if [ -n "$state" ] && [ "$state" != pending ]; then
      returned[number]=$state
      skip=${skip/,$number,/,}
    fi
  done
  skip=${skip#,}; skip=${skip%,}
  for number in "${!aside_ends[@]}"; do skip="${skip:+$skip,}$number"; done
  selection=$(choice "$skip") || return 1
  IFS=$'\t' read -r owner first card <<< "$selection"
  if [ -z "$card" ]; then
    for_joel=$(jq -r --arg lane "$lane" --arg mode joel --arg skip "$skip" \
      -f "$root/architecture/backlog.jq" <<< "$items") || return 1
  fi
  # Commit returns only after every board read for this selection succeeded.
  for number in "${!returned[@]}"; do
    message="returned #$number: #${parked_behind[number]} is ${returned[number]}"
    if [ "${returned[number]}" = linux-owed ]; then
      message="returned #$number: #${parked_behind[number]} only owes its Linux check"
    fi
    return_card "$number" "$message"
  done
}

# The last Checks run of main that ended with a result. A nightly run and a
# manual run both count, so a green manual run clears a failed night. A
# cancelled or skipped run decides nothing, and neither does a run of the
# earlier push and pull request checks.
# It reads the last 100 finished runs, which is one page of the GitHub API and
# more than three months of nightly runs. RUN_CHECKS replaces `gh` in the test.
checks_run() {
  # Keep both the answer and a failed read for this pass, including an empty answer.
  if [ "$checks_read" = 0 ]; then
    checks_read=1 checks_ok=0 checks_latest=""
    if [ -z "${RUN_ITEMS:-}" ] || [ -n "${RUN_CHECKS:-}" ]; then
      # shellcheck disable=SC2086
      if checks_latest=$(${RUN_CHECKS:-gh} run list -R "$repo" --workflow checks.yml --branch main --status completed --limit 100 \
        --json databaseId,conclusion,event \
        -q 'map(select((.event == "schedule" or .event == "workflow_dispatch") and .conclusion != "cancelled" and .conclusion != "skipped"))[0] // empty | [.databaseId, .conclusion] | @tsv' \
        2> /dev/null); then checks_ok=1; fi
    fi
  fi
  [ "$checks_ok" = 1 ]
}

checks_failed() {
  red=""
  checks_run || return 0
  if [[ "$checks_latest" != *$'\t'success ]]; then red=${checks_latest%%$'\t'*}; fi
}

# Keep deadlines numeric so an older supervisor can read them as timed waits.
# End an eligible wait once, including its run id and any deadline note.
aside_over() {
  local number=$1 message
  if [ "${aside_runs[number]+set}" = set ] && checks_run \
    && [ "${checks_latest%%$'\t'*}" != "${aside_runs[number]}" ]; then
    : # A new run ends the wait without a deadline note.
  elif [ "$(now)" -lt "${aside_ends[number]}" ]; then
    return 1
  elif [ "${aside_runs[number]+set}" = set ]; then
    message="#$number waited $checks_limit minutes for a Checks run; none ended"
    say "$message"; note "$message"
  fi
  unset 'aside_ends[number]' 'aside_runs[number]'
}

free_gib() {
  if [ -n "${RUN_FREE:-}" ]; then $RUN_FREE; else df -Pk . | awk 'NR == 2 { print int($4 / 1048576) }'; fi
}

now() {
  if [ -n "${RUN_NOW:-}" ]; then $RUN_NOW; else date +%s; fi
}

remove_turn_entry() {
  rm -f "$1/held" && rmdir "$1" 2> /dev/null || true
}

leave_turn_queue() {
  [ -z "$turn_waiter" ] || remove_turn_entry "$turn_waiter"
  turn_waiter=""
}

release_turn_lock() {
  [ "$(cat "$turn/pid" 2> /dev/null)" != "$$" ] || rm -rf "$turn"
  turn_held=0
}

release_turn() {
  leave_turn_queue
  release_turn_lock
}
trap release_turn EXIT

is_run() {
  [[ $1 =~ ^([^[:space:]]*/)?bash[[:space:]]+([^[:space:]]*/)?architecture/run[.]sh([[:space:]]|$) ||
     $1 =~ ^([^[:space:]]*/)?bash[[:space:]]+/.*/architecture/run[.]sh[[:space:]]+gate[[:space:]] ]]
}

# A missing lookup is unknown. A zombie still has a PID but has stopped waiting.
turn_birth() {
  local info checksum
  info=$(env LC_ALL=C TZ=UTC ${RUN_TURN_PS:-ps} -ww -p "$1" -o state= -o lstart= 2> /dev/null) || return 1
  [[ $info =~ ^[[:space:]]*([^[:space:]]+)[[:space:]]+(.+)$ ]] || return 1
  [[ ${BASH_REMATCH[1]} != Z* ]] || return 2
  checksum=$(cksum <<< "${BASH_REMATCH[2]}") || return 1
  printf '%s\n' "${checksum%% *}"
}

lock_turn_queue() {
  if [[ $OSTYPE == darwin* ]]; then /usr/bin/lockf -s 9
  else flock -x 9; fi
}

# Serialize ticket publication with the final claim of a turn. A process
# exit closes its descriptor and releases the queue lock.
record_turn_waiter() (
  local entry ticket last=0 birth suffix=XXXXXXXX
  lock_turn_queue || return 1
  birth=$(turn_birth "$$") || birth=unknown
  for entry in "$turn_queue"/*; do
    ticket=${entry##*/}; ticket=${ticket%%.*}
    if [[ $ticket =~ ^[1-9][0-9]*$ ]] && [ "$ticket" -gt "$last" ]; then last=$ticket; fi
  done
  # Card tickets use ticket.pid.birth.p<count>c<card>xXXXXXXXX. A FIFO
  # supervisor still recognizes the alphanumeric suffix.
  if [ -n "$2" ]; then suffix=p${1}c${2}x$suffix; fi
  mktemp -d "$turn_queue/$((last + 1)).$$.$birth.$suffix"
) 9>> "$turn_queue/lock"

parse_turn_card() {
  [[ ${1##*.} =~ ^p(0|[1-9][0-9]*)c([1-9][0-9]*)x[[:alnum:]]{8}$ ]]
}

parse_turn_entry() {
  [[ ${1##*/} =~ ^([1-9][0-9]*)\.([1-9][0-9]*)\.([0-9]+|unknown)\.[[:alnum:]]+$ ]]
}

read_turn_passes() {
  turn_passes=""
  [ ! -e "$turn_queue/passed" ] || turn_passes=$'\n'$(cat "$turn_queue/passed")
}

find_turn_pass() {
  local prefix=$'\n'${1##*/}$'\t'
  [[ $turn_passes == *"$prefix"* ]] || return 1
  turn_pass=${turn_passes#*"$prefix"}
  turn_pass=${turn_pass%%$'\n'*}
}

turn_waiter_live() {
  local pid=$1 birth=$2 current="" read_status=0 command
  kill -0 "$pid" 2> /dev/null || return 1
  current=$(turn_birth "$pid") || read_status=$?
  case "$read_status" in
    2) return 1 ;;
    1) return 0 ;; # A failed process read does not show that it exited.
  esac
  if [ "$birth" != unknown ]; then [ "$birth" = "$current" ]; return; fi
  command=$(${RUN_TURN_PS:-ps} -ww -p "$pid" -o args= 2> /dev/null) || command=""
  [ -z "$command" ] || is_run "$command"
}

first_turn_waiter() {
  local entry ticket pid birth other_count passed=0
  read_turn_passes || return 1
  if find_turn_pass "$turn_waiter"; then passed=1; fi
  for entry in "$turn_queue"/*; do
    [ "$entry" != "$turn_waiter" ] || continue
    parse_turn_entry "$entry" || continue
    ticket=${BASH_REMATCH[1]} pid=${BASH_REMATCH[2]} birth=${BASH_REMATCH[3]}
    # An unreadable resume can leave another ticket for this process.
    if [ "$pid" = "$$" ]; then remove_turn_entry "$entry"; continue; fi
    if find_turn_pass "$entry"; then
      if (( passed && ticket >= turn_ticket )); then continue; fi
    else
      other_count=0
      if parse_turn_card "$entry"; then other_count=${BASH_REMATCH[1]}; fi
      if (( passed || other_count < count || (other_count == count && ticket >= turn_ticket) )); then continue; fi
    fi
    if turn_waiter_live "$pid" "$birth"; then
      # A holder reserves its ticket for a restart but is not a waiter.
      [ -f "$entry/held" ] && continue
      ahead=$pid; return 1
    fi
    remove_turn_entry "$entry"
  done
  return 0
}

# Store passes outside the ticket folders so an older run can still remove
# its ticket. Replace the whole record before releasing the queue lock.
write_turn_passes() {
  local entry ticket pid birth passed_at=""
  for entry in "$turn_queue"/*; do
    parse_turn_entry "$entry" || continue
    ticket=${BASH_REMATCH[1]} pid=${BASH_REMATCH[2]} birth=${BASH_REMATCH[3]}
    if ! find_turn_pass "$entry"; then
      if (( ticket >= turn_ticket )) || [ -f "$entry/held" ] || ! turn_waiter_live "$pid" "$birth"; then continue; fi
      if [ -z "$passed_at" ]; then passed_at=$(date '+%Y-%m-%d %H:%M:%S %z') || return 1; fi
      turn_pass="$passed_at by $passer"
    fi
    printf '%s\t%s\n' "${entry##*/}" "$turn_pass" || return 1
  done
}

claim_turn() {
  # A higher-ranked waiter can join or resume while mkdir is in progress.
  if ! lock_turn_queue || ! first_turn_waiter; then rmdir "$turn"; return; fi
  if echo $$ > "$turn/pid"; then
    if : > "$turn_waiter/held" && write_turn_passes > "$turn_queue/passed.next" &&
      mv -f "$turn_queue/passed.next" "$turn_queue/passed"; then
      turn_held=1
    else
      rm -f "$turn_waiter/held" "$turn_queue/passed.next"
      release_turn_lock
    fi
  fi
}

report_turn_pass() {
  [ "$lane" != turn ] && [ "$turn_pass_reported" != "$turn_waiter" ] || return 0
  if read_turn_passes && find_turn_pass "$turn_waiter"; then
    note "merge turn request for #$1 was passed at $turn_pass"
    turn_pass_reported=$turn_waiter
  fi
}

turn_unavailable() {
  local message="the folder of the merge turn cannot be made; the run takes no merge turn"
  turn_enabled=""
  leave_turn_queue
  # A standalone CI gate can have no writable home or cache. It still has the
  # machine lock, just as a lane does when it cannot create the turn folder.
  [ "$gate_turn" = 0 ] || { say "$message"; return 0; }
  [ "$lane" != turn ] || { say "$message"; return 1; }
  note "$message"; notify "$message"
}

clear_stale_turn() {
  local holder=$1 command
  if [ -n "$holder" ] && { [ "$holder" = "$$" ] || ! kill -0 "$holder" 2> /dev/null || {
    command=$(${RUN_TURN_PS:-ps} -ww -p "$holder" -o args= 2> /dev/null) || command=""
    # just run, a launcher and the restart all use bash <path>/architecture/run.sh.
    [ -n "$command" ] && ! is_run "$command"
  }; }; then
    [ "$(cat "$turn/pid" 2> /dev/null)" != "$holder" ] || rm -rf "$turn"
  # As in gate.sh, allow a new holder a minute to write its process id.
  elif [ -z "$holder" ] && [ -n "$(find "$turn" -maxdepth 0 -mmin +1 2> /dev/null)" ]; then
    [ "$(cat "$turn/pid" 2> /dev/null)" != "$holder" ] || rm -rf "$turn"
  fi
}

wait_for_turn_queue() {
  local mkdir_error message told=0
  while [ -z "$turn_waiter" ]; do
    if mkdir -p "$turn_queue" 2> /dev/null; then
      if turn_waiter=$(record_turn_waiter "$1" "$2" 2> /dev/null); then return 0; fi
      message="waiting to join the merge turn queue"
    else
      # Keep the old fallback only when the turn folder itself cannot be made.
      if mkdir_error=$(env LC_ALL=C mkdir "$turn" 2>&1); then
        rmdir "$turn"
      elif [[ $mkdir_error != *": File exists" ]]; then
        turn_unavailable; return $?
      fi
      message="waiting for the merge turn queue to open"
    fi
    [ "$lane" != turn ] || { say "$message"; return 1; }
    if [ "$told" = 0 ]; then say "$message"; note "$message"; notify "$message"; told=1; fi
    sleep "$turn_poll"
  done
}

take_turn() {
  local count=${1:-0} holder started minutes mkdir_error turn_ticket ahead="" told=0 previous='?'
  local turn_passes turn_pass passer
  if [ "$lane" = turn ]; then printf -v passer 'command %q (PID %s)' "$3" "$$"
  else passer="#$2"; fi
  started=$(now)
  wait_for_turn_queue "$count" "${2:-}" || return $?
  [ -n "$turn_enabled" ] || return 0
  turn_ticket=${turn_waiter##*/}; turn_ticket=${turn_ticket%%.*}
  while [ "$turn_held" = 0 ]; do
    if first_turn_waiter; then
      # Use mkdir's error because a holder may remove the folder after it fails.
      if mkdir_error=$(env LC_ALL=C mkdir "$turn" 2>&1); then
        claim_turn 9>> "$turn_queue/lock"
      elif [[ $mkdir_error != *": File exists" ]]; then
        turn_unavailable; return $?
      fi
      [ "$turn_held" = 0 ] || break
      holder=$(cat "$turn/pid" 2> /dev/null)
      clear_stale_turn "$holder"
    else
      holder=$(cat "$turn/pid" 2> /dev/null)
      holder=${holder:-$ahead}
    fi
    report_turn_pass "${2:-}"
    if [ "$lane" = turn ]; then
      [ "$holder" = "$previous" ] || say "waiting for the merge turn of process ${holder:-that starts now}"
      previous=$holder
    else
      echo "waiting for the merge turn of process ${holder:-that starts now}" > "$status"
      if [ "$told" = 0 ] && [ "$(($(now) - started))" -ge "$turn_tell" ]; then
        notify "waiting for the merge turn of process ${holder:-that starts now}"
        told=1
      fi
    fi
    sleep "$turn_poll"
  done
  report_turn_pass "${2:-}"
  minutes=$((($(now) - started) / 60))
  if [ "$lane" != turn ] && [ "$minutes" -ge 1 ]; then note "waited $minutes minutes for the merge turn"; fi
}

take_card_turn() {
  local number=$1 count
  if parse_turn_card "$turn_waiter" && [ "${BASH_REMATCH[2]}" = "$number" ]; then
    count=${BASH_REMATCH[1]}
  else
    # A compatible restart can select a different card from the fresh board.
    leave_turn_queue
    if ! count=$(jq -r --arg lane "$lane" --arg mode dependents --arg skip "" --arg number "$number" \
      -f "$root/architecture/backlog.jq" <<< "$items" 2> /dev/null) \
      || ! [[ $count =~ ^(0|[1-9][0-9]*)$ ]]; then
      note "merge turn for #$number: dependency count unavailable; using zero"
      count=0
    fi
  fi
  take_turn "$count" "$number"
}

if [ "$lane" = turn ]; then
  if [ "$gate_turn" = 0 ] && [[ -t 0 || -t 1 || -t 2 ]]; then
    say 'turn refuses a terminal; redirect all three streams: run.sh turn <command> [arguments...] < /dev/null > output.log 2>&1'
    exit 2
  fi
  if [ "$gate_turn" = 0 ] && [[ $2 = *=* ]]; then
    printf 'run: turn refuses command word containing =: %q\n' "$2"
    exit 2
  fi
  [ -n "$turn_enabled" ] || exit 1
  command_line=$(${RUN_TURN_PS:-ps} -ww -p $$ -o args= 2> /dev/null) || command_line=""
  # Keep failed or empty reads, as a waiter does for its holder.
  if [ -n "$command_line" ] && ! is_run "$command_line"; then
    say 'turn requires bash [<path>/]architecture/run.sh turn <command> ...; another form would not keep the turn'
    exit 2
  fi
  turn_held=0
  trap 'exit 130' INT; trap 'exit 143' TERM
  trap 'exit 129' HUP; trap 'exit 131' QUIT
  take_turn 0 "" "$2" || exit 1
  shift
  if [ "$gate_turn" = 1 ]; then
    trap - INT TERM HUP QUIT
    . "$root/architecture/gate.sh"
    exit $?
  fi
  command_pid='' pending_signal=''
  forward_signal() {
    if [ -n "$command_pid" ]; then
      kill -"$1" -- "-$command_pid" 2> /dev/null || true
    else pending_signal=$1; fi
  }
  trap 'forward_signal INT' INT; trap 'forward_signal TERM' TERM
  trap 'forward_signal HUP' HUP; trap 'forward_signal QUIT' QUIT
  set -m
  RUN_TURN_OWNER=$$ env -- "$@" & command_pid=$!
  set +m
  [ -z "$pending_signal" ] || forward_signal "$pending_signal"
  while :; do
    wait "$command_pid" || true
    # A signal interrupts wait before the command has finished its cleanup.
    kill -0 "$command_pid" 2> /dev/null || break
  done
  trap - INT TERM HUP QUIT
  trap 'exit 131' QUIT
  status=0
  wait "$command_pid" || status=$?
  exit "$status"
fi

open_change() {
  if [ -n "${RUN_OPEN:-}" ]; then $RUN_OPEN; return $?; fi
  local changes merged main
  changes=$(git status --porcelain 2> /dev/null) || return 0
  [ -z "$changes" ] || return 0
  # A squash-merged branch holds nothing open.
  merged=$(git merge-tree --write-tree origin/main HEAD 2> /dev/null)
  main=$(git rev-parse --verify 'origin/main^{tree}' 2> /dev/null)
  [ -z "$main" ] || [ "$merged" != "$main" ]
}

park() { # card line, reason
  release_turn
  local number=${1%%$'\t'*}; number=${number#\#}
  local held dependency line
  if [ "$once" != --once ] && held=$(${RUN_GH:-gh} issue view "$number" -R "$repo" --json labels \
    -q 'any(.labels[]; .name == "needs-joel")' 2> /dev/null) && [ "$held" = true ]; then
    unset 'saved_counts[number]' 'gate_requests[number]' 'gate_branches[number]' 'last_passed_requests[number]'
    # Do not save the counts of the session that just ended on the next card.
    last=""
  else
    parked="${parked:+$parked,}$number"
    line="  ${1//$'\t'/ }: $2\n"
    parked_lines="$parked_lines$line"
    parked_text[number]=$line
    if [ "$once" != --once ] && [[ $2 =~ ^blocked[[:space:]]+#([0-9]+)([[:space:][:punct:]]|$) ]]; then
      dependency=${BASH_REMATCH[1]}
      if [ "$dependency" != "$number" ] && [ "$(card_state "$dependency")" = pending ]; then
        parked_behind[number]=$dependency
      fi
    fi
  fi
  say "parked ${1%%$'\t'*}: $2"
  note "parked ${1%%$'\t'*}: $2"
  notify "parked ${1%%$'\t'*}: $2"
}

# One session of the lead on a card. Sets `result` and `seconds`.
# The prompt is the next step of the card, unless a third argument gives another.
session() { # card line or a label, note for the lead, prompt
  local number=${1%%$'\t'*} started=$SECONDS cap=()
  local prompt=${3:-"Lead the next step of card $number in the $lane lane. Follow the automatic run in .agents/project.md.$2"}
  rm -f .cache/step
  if command -v timeout > /dev/null; then cap=(timeout --signal=TERM "$((limit * unit))"); fi
  say "session on ${1//$'\t'/ }"
  note "session on $number"
  # shellcheck disable=SC2086
  ${cap[@]+"${cap[@]}"} $STEP_LEAD -C . -o .cache/step "$prompt" < /dev/null > .cache/step.log 2>&1
  local code=$?
  seconds=$((SECONDS - started))
  result=$(tail -n 15 .cache/step 2> /dev/null | grep -E '^STEP (merged|planned|waiting|later|blocked|failed|reviewed|gate)' | tail -n 1)
  wrote=1
  if [ -z "$result" ]; then
    wrote=0
    if [ "$code" -eq 124 ]; then result="STEP failed the session reached its limit of $limit minutes"
    else result="STEP failed the lead wrote no result line"; fi
  fi
  say "$result"
  note "$number $result"
}

# The gate belongs to the run, with no lead alive. Sets `gate_note`.
full_gate() {
  local request=${gate_requests[number]} commit changes started minutes code outcome progress saved=${gate_results[number]:-} cap=()
  local timing=.cache/gate.timing label="full gate for #$number" ended event timestamp waiting= held= wait_minutes
  commit=$(git rev-parse --verify HEAD 2> /dev/null)
  if [ -z "$request" ] || { [ "$commit" != "$request" ] && {
    ! git merge-base --is-ancestor "$request" "$commit" 2> /dev/null \
      || [ "$(git rev-parse "$commit^{tree}" 2> /dev/null)" != "$(git merge-tree --write-tree origin/main "$request" 2> /dev/null)" ]
  }; }; then
    gate_note=" The run did not run the full gate: HEAD changed after the request (${request:-unknown}) by more than a merge of main. Make sure that the step is checked out and committed, then end with STEP gate."
    gate_note="$gate_note ${saved#* }"
    return
  fi
  if [ -n "${gate_results[number]:-}" ]; then
    gate_note=" ${gate_results[number]#* }"
    if [[ ${gate_results[number]} != "$commit "* ]]; then
      gate_note="$gate_note Main came in after that gate: HEAD is now $commit, and the gate did not run for it. End with STEP gate."
    fi
    return
  fi
  if [ "$sync" = 1 ] && git rev-parse --verify origin/main > /dev/null 2>&1 \
    && ! git merge-base --is-ancestor origin/main HEAD 2> /dev/null; then
    gate_note=" The run did not run the full gate: main is not in the step. It does not merge cleanly, or files that are not tracked stopped the run from merging it. Integrate main, remove or commit those files, then end with STEP gate."
    return
  fi
  if ! changes=$(git status --porcelain --untracked-files=no 2> /dev/null) || [ -n "$changes" ]; then
    gate_note=" The run did not run the full gate: the worktree holds tracked changes that are not committed. Commit the step, then end with STEP gate."
    return
  fi
  if command -v timeout > /dev/null; then cap=(timeout --signal=TERM "$((gate_limit * unit))"); fi
  echo "$label" > "$status"
  : > "$timing"
  started=$(now)
  # shellcheck disable=SC2086
  GATE_RUN_LABEL=$label GATE_RUN_STATUS=$status GATE_RUN_TIMING=$timing GATE_RUN_NOW=${RUN_NOW:-} \
    ${cap[@]+"${cap[@]}"} ${RUN_GATE:-bash architecture/gate.sh} > .cache/gate.log 2>&1
  code=$?
  ended=$(now)
  # An old gate, a cached pass, or a failure before the lock may report no time.
  while read -r event timestamp; do
    [[ $timestamp =~ ^(0|[1-9][0-9]{0,11})$ ]] || continue
    case "$event" in
      waiting) waiting=$timestamp ;;
      held) held=$timestamp ;;
    esac
  done < "$timing"
  minutes=$(((ended - ${held:-started} + unit - 1) / unit))
  [ "$minutes" -ge 1 ] || minutes=1
  if [ -n "$waiting" ]; then
    [ -n "$held" ] || minutes=0
    wait_minutes=$(((${held:-ended} - waiting) / unit))
    if [ "$wait_minutes" -ge 1 ]; then note "waited $wait_minutes minutes for the gate lock"; fi
  fi
  if [ "$code" -eq 0 ]; then
    land_head=$commit
    if [ -z "${last_passed_requests[number]:-}" ] \
      || ! progress=$(git rev-list --no-merges "${last_passed_requests[number]:-}..$request" --not origin/main 2> /dev/null) \
      || [ -n "$progress" ]; then
      waits=0
    fi
    last_passed_requests[number]=$request
    outcome="passed in $minutes minutes"
    gate_note=" The run ran the full gate on commit $commit: it passed."
  else
    if [ "$code" -eq 124 ]; then outcome="did not end in $gate_limit minutes"
    else outcome="failed in $minutes minutes"; fi
    gate_note=" The run ran the full gate on commit $commit: it failed. Read the end of .cache/gate.log first."
    [ "$code" -ne 124 ] || gate_note=" The run ran the full gate on commit $commit: it did not end in $gate_limit minutes. Read the end of .cache/gate.log first."
  fi
  gate_results[number]="$commit$gate_note"
  say "full gate for #$number $outcome"
  note "full gate for #$number $outcome"
}

# Keep stdout separate: a diagnostic must never become a pull request number.
land_call() {
  local call=$1 code detail cap=(); shift
  if command -v timeout > /dev/null; then cap=(timeout --signal=TERM --kill-after="${RUN_LAND_KILL:-10}" "${RUN_LAND_LIMIT:-300}"); fi
  ${cap[@]+"${cap[@]}"} "$@" > .cache/land.out 2> .cache/land.log
  code=$?
  if [ "$code" -ne 0 ]; then
    detail=$(tail -n 1 .cache/land.log)
    [ -n "$detail" ] || detail=$(tail -n 1 .cache/land.out)
    land_reason="$call failed ($detail)"
    case "$code" in 124 | 137) land_reason="$call reached its limit ($detail)" ;; esac
    return 1
  fi
}
merged_step() { down=0 done_steps=$((done_steps + 1)) last_result="" repeats=0 waits=0 disk_fails=0; }
pr_number() { [[ "$1" =~ ^[1-9][0-9]*$ ]]; }
# Both distances allow only clean merges of main after the reviewed commit.
main_merges() {
  local commits commit parents first second tree last=$1
  [ "$1" = "$2" ] && return 0
  commits=$(git rev-list --first-parent --reverse "$1..$2" 2> /dev/null) || return 1
  [ -n "$commits" ] || return 1
  for commit in $commits; do
    parents=$(git show -s --format=%P "$commit" 2> /dev/null) || return 1
    set -- $parents
    [ "$#" -eq 2 ] || return 1
    first=$1 second=$2
    [ "$first" = "$last" ] || return 1
    git merge-base --is-ancestor "$second" origin/main 2> /dev/null || return 1
    tree=$(git merge-tree --write-tree "$first" "$second" 2> /dev/null) || return 1
    [ "$(git rev-parse "$commit^{tree}" 2> /dev/null)" = "$tree" ] || return 1
    last=$commit
  done
}
list_prs() {
  # shellcheck disable=SC2086
  land_call "the pull request list" ${RUN_GH:-gh} pr list -R "$repo" --head "$land_branch" --state open \
    --json number,baseRefName,headRefName,headRefOid,isCrossRepository,state,mergeCommit || return 1
  pr_list=$(cat .cache/land.out)
  land_reason="the pull request list has an invalid number or answer"
  jq -e 'type == "array" and all(.[]; (.number | type == "number") and (.number | tostring | test("^[1-9][0-9]*$")))' <<< "$pr_list" > /dev/null 2>&1
}
read_pr() {
  # shellcheck disable=SC2086
  land_call "the pull request state read" ${RUN_GH:-gh} pr view "$land_pr" -R "$repo" \
    --json number,baseRefName,headRefName,headRefOid,isCrossRepository,state,mergeCommit || return 1
  pr_json=$(cat .cache/land.out)
  land_reason="the pull request state read has an invalid number or answer"
  jq -e --arg pr "$land_pr" '(.number | type == "number") and (.number | tostring) == $pr' <<< "$pr_json" > /dev/null 2>&1
}
pr_target() {
  land_reason="pull request $land_pr does not have base main, this branch, this repository and the gated head"
  jq -e --arg branch "$land_branch" \
    '.baseRefName == "main" and .headRefName == $branch and .isCrossRepository == false
     and (.headRefOid | test("^[0-9a-f]{40}$"))' <<< "$pr_json" > /dev/null 2>&1
}
pr_head() { jq -e --arg head "$land_head" '.headRefOid == $head' <<< "$pr_json" > /dev/null 2>&1; }
pr_identity() { pr_target && pr_head; }
pr_open() {
  jq -e '.state == "OPEN"' <<< "$pr_json" > /dev/null 2>&1 && return 0
  land_reason="pull request $land_pr is not OPEN"
  return 1
}
confirm_listed_head() {
  local delay
  pr_target && pr_open || return 1
  # The saved reads in #1159 had caught up within seconds and by the next minute.
  for delay in 0 7 55; do
    pr_head && return 0
    if [ "$delay" -gt 0 ]; then sleep "$delay" || return 1; fi
    read_pr && pr_target && pr_open || return 1
  done
  pr_head
}
confirm_merge() {
  local merge_commit
  read_pr && pr_identity || return 1
  land_reason="pull request $land_pr has no confirmed merge commit in origin/main"
  [ "$(jq -r .state <<< "$pr_json")" = MERGED ] || return 1
  merge_commit=$(jq -r '.mergeCommit.oid // empty' <<< "$pr_json")
  # shellcheck disable=SC2086
  [[ "$merge_commit" =~ ^[0-9a-f]{40}$ ]] && land_call "the merged main fetch" ${RUN_PUSH:-git} fetch -q origin main \
    && git merge-base --is-ancestor "$merge_commit" origin/main 2> /dev/null
}
fresh_main() {
  # shellcheck disable=SC2086
  if ! land_call "the main fetch" ${RUN_PUSH:-git} fetch -q origin main; then
    land_reason="main could not be read after the gate; end with STEP gate. $land_reason"; return 1
  elif ! git merge-base --is-ancestor origin/main HEAD 2> /dev/null; then
    unset 'gate_results[number]'
    note "main advanced during the gate for #$number; the gate runs again"
    gate_wait 'main advanced during the gate'; gate_again=1; return 1
  fi
}
land_sequence() {
  local count issue=""
  # shellcheck disable=SC2086
  land_call "the push" ${RUN_PUSH:-git} push -q origin "HEAD:refs/heads/$land_branch" || return 1
  list_prs || return 1
  count=$(jq length <<< "$pr_list")
  if [ "$count" -gt 1 ]; then land_reason="several open pull requests of this branch: $(jq -r 'map(.number | tostring) | join(", ")' <<< "$pr_list")"; return 1; fi
  pr_json=$(jq '.[0] // null' <<< "$pr_list"); land_pr=$(jq -r '.number // empty' <<< "$pr_json")
  if [ -z "$land_pr" ]; then
    # shellcheck disable=SC2086
    land_call "the pull request create" ${RUN_GH:-gh} pr create -R "$repo" --base main --head "$land_branch" --title "$title" --body-file .cache/land-body.md || return 1
    land_pr=$(cat .cache/land.out); land_pr=${land_pr#https://github.com/$repo/pull/}
    if ! pr_number "$land_pr"; then land_reason="the pull request create returned an invalid number"; return 1; fi
  else
    confirm_listed_head || return 1
    # shellcheck disable=SC2086
    land_call "the pull request edit" ${RUN_GH:-gh} pr edit "$land_pr" -R "$repo" --title "$title" --body-file .cache/land-body.md || return 1
  fi
  read_pr && pr_identity && pr_open || return 1
  if [ "$(git rev-parse HEAD)" != "$land_head" ] || [ "$(git branch --show-current)" != "$land_branch" ]; then land_reason="the head or branch changed after the gate"; return 1; fi
  fresh_main || return 2
  # shellcheck disable=SC2086
  land_call "the pull request merge" ${RUN_GH:-gh} pr merge "$land_pr" -R "$repo" --squash --match-head-commit "$land_head" || issue=$land_reason
  confirm_merge && return 0
  [ -z "$issue" ] || land_reason=$issue
  return 1
}
gate_wait() {
  down=0 waits=$((waits + 1))
  if [ "$waits" -gt 6 ]; then park "$card" "waited six times: $1"
  else slept=1; fi
}
# Only a fresh gate pass starts a landing; a failed act is left for the lead.
land_step() {
  local reviewed title body land_branch land_pr="" pr_json pr_list changes code
  land_reason=""
  land_branch=$(git branch --show-current 2> /dev/null) || land_branch=""
  if [ ! -f .cache/pr.md ]; then land_reason=".cache/pr.md is missing"
  else
    reviewed=$(sed -n '1p' .cache/pr.md); title=$(sed -n '2p' .cache/pr.md); body=$(sed -n '4,$p' .cache/pr.md)
    if ! [[ "$reviewed" =~ ^Reviewed-head:\ ([0-9a-f]{40})$ ]] || ! main_merges "${BASH_REMATCH[1]:-}" "${gate_requests[number]}"; then land_reason=".cache/pr.md does not name a reviewed ancestor with only clean merges of main"
    elif ! [[ "$title" =~ [^[:space:]] ]]; then land_reason=".cache/pr.md has no title"
    elif [ -n "$(sed -n '3p' .cache/pr.md)" ]; then land_reason=".cache/pr.md needs an empty third line"
    elif ! [[ "$body" =~ [^[:space:]] ]]; then land_reason=".cache/pr.md has no text"
    elif [ -z "$land_branch" ]; then land_reason="the branch has no readable name"
    elif [ "$land_branch" = main ]; then land_reason="the branch is main"
    elif [ "$land_branch" != "${gate_branches[number]:-}" ]; then land_reason="the branch changed after the request"
    elif ! main_merges "${gate_requests[number]}" "$land_head"; then land_reason="commits follow the reviewed head"
    elif [ "$(git rev-parse HEAD)" != "$land_head" ] || ! changes=$(git status --porcelain --untracked-files=no 2> /dev/null) || [ -n "$changes" ]; then land_reason="the head or worktree changed after the gate"
    else { sed -n '4,$p' .cache/pr.md && printf '\nFull gate: passed on %s in the automatic run.\n' "$land_head"; } > .cache/land-body.md || land_reason="the text of the pull request could not be written"
    fi
  fi
  if [ -z "$land_reason" ]; then
    fresh_main
    [ "$gate_again" = 0 ] || return 1
  fi
  if [ -z "$land_reason" ]; then
    if land_sequence; then
      merged_notes[number]=" The run merged this step as pull request $land_pr after the full gate passed on $land_head. Tick the step and go on."
      unset 'gate_requests[number]' 'gate_branches[number]' 'gate_results[number]'
      say "#$number STEP merged $land_pr (the run)"; note "#$number STEP merged $land_pr (the run)"
      merged_step; rm -f .cache/pr.md; return 0
    else code=$?; fi
    [ "$gate_again" = 0 ] || return 1
    if [ "$code" = 2 ]; then gate_note="$gate_note The run did not merge: $land_reason."
    else gate_note="$gate_note The landing by the run started; $land_reason. The state on GitHub is not known from that call: read the branch and the pull request before any act."; fi
  else gate_note="$gate_note The run did not merge: $land_reason."; fi
  gate_results[number]="$land_head$gate_note"
  return 1
}
# The lead ended at once without a result: a usage limit, a login or the
# network. The run tries again and stops after RUN_RETRIES such tries in a row.
lead_down() {
  down=$((down + 1))
  if [ "$down" -ge "$retries" ]; then
    say "last lines of the lead:"; tail -n 5 .cache/step.log 2> /dev/null
    stop "the lead could not start $retries times in a row" 2
  fi
  say "the lead ended within $seconds seconds; next try in $retry_wait seconds"; sleep "$retry_wait"
}

# A set-aside polling pass also follows main, without starting a lead.
sync_main() {
  if [ "$sync" = 1 ]; then
    local cap=()
    if [ "$turn_held" = 1 ]; then cap=(timeout --signal=TERM --kill-after="${RUN_LAND_KILL:-10}" "$turn_fetch"); fi
    ${cap[@]+"${cap[@]}"} git fetch -q origin main || say "main could not be fetched"
    if [ -z "$(git status --porcelain)" ] && ! git merge-base --is-ancestor origin/main HEAD 2> /dev/null; then
      git merge -q --no-edit origin/main || { git merge --abort; say "main does not merge cleanly; the lead integrates it"; }
    fi
  fi

  # A merged supervisor must take over before a lead reads main's instructions.
  if [ "$once" != --once ] && [ -f architecture/run.sh ]; then
    checksum=$(cksum < architecture/run.sh)
    if { [ "$checksum" != "$supervisor_sum" ] || [ "$supervisor_path" != "$(pwd -P)/architecture/run.sh" ]; } \
      && git diff --quiet origin/main -- architecture/run.sh 2> /dev/null; then
      if bash -n architecture/run.sh 2> /dev/null; then
        # github_down and full are 0 here after their successful checks.
        # Configuration and script identity are rebuilt; pass values are
        # recomputed, pending gates follow their cards, and weekly review
        # state stays in .cache/design-review.
        if declare -p parked parked_lines parked_behind parked_text done_steps aside_ends aside_runs saved_counts last last_result repeats waits disk_fails slept down rejected_sums gate_requests gate_results last_passed_requests gate_branches merged_notes \
          > .cache/run.state; then
          say "started again with the supervisor of main"
          note "started again with the supervisor of main"
          # A compatible restart releases the turn but keeps its place in
          # the queue. An older supervisor would leave that entry in place.
          if [ -n "$turn_waiter" ] && grep -q '^# RUN_TURN_RESUME ' architecture/run.sh && rm -f "$turn_waiter/held"; then
            release_turn_lock
          else
            release_turn
          fi
          RUN_TURN_RESUME=$turn_waiter RUN_TURN_REPORTED=$turn_pass_reported \
            RUN_STATE=.cache/run.state exec bash architecture/run.sh "$lane"
        fi
        say "the run state could not be saved; the run keeps its supervisor"
      else
        case "$rejected_sums" in
          *"|$checksum|"*) ;;
          *)
            message="the supervisor of main does not parse; the run keeps its supervisor"
            say "$message"; note "$message"; notify "$message"
            rejected_sums="$rejected_sums$checksum|" ;;
        esac
      fi
    fi
  fi
}

down=0 github_down=0 full=0 last="" last_result="" repeats=0 waits=0 disk_fails=0 slept=0
waiting=0
# A result is its commit followed by the note. Only the last request that passed
# survives a lead's result line; an empty request still needs a refusal.
gate_requests=() gate_results=() last_passed_requests=() gate_branches=() merged_notes=()
# Read at top level: declare inside a function would restore local variables.
# Missing variables keep their defaults; extra variables from a later version
# are harmless. Only the exec hands state to the next run.
if [ -n "${RUN_STATE:-}" ]; then
  resumed=${RUN_TURN_RESUME:-}
  if [[ $resumed == "$turn_queue"/[0-9]*."$$".* && ${resumed#"$turn_queue"/} != */* && -d $resumed ]]; then
    turn_waiter=$resumed
    if [ "${RUN_TURN_REPORTED:-}" = "$resumed" ]; then turn_pass_reported=$resumed; fi
  fi
  if bash -n "$RUN_STATE" 2> /dev/null; then
    . "$RUN_STATE"
  else
    leave_turn_queue
    say "saved run state could not be read; starting with empty state"
  fi
  rm -f "$RUN_STATE"
  unset RUN_STATE
else
  rm -f .cache/run.state
fi
unset RUN_TURN_RESUME RUN_TURN_REPORTED
# Import the single set-aside card saved by supervisors before aside_ends.
if [ -n "${aside:-}" ]; then aside_ends[aside]=${aside_until:-0}; fi
unset aside aside_until
# An older state can name a turn; no turn survives a restart.
turn_held=0
while true; do
  checks_read=0
  for number in "${!aside_ends[@]}"; do
    if [ "$slept" = 0 ] && ! open_change; then aside_over "$number"; fi
  done
  card=""
  if ! select_card; then
    leave_turn_queue
    github_down=$((github_down + 1))
    [ "$github_down" -lt "$retries" ] || stop "GitHub could not be read $retries times in a row" 2
    say "GitHub could not be read; next try in $retry_wait seconds"; sleep "$retry_wait"; continue
  fi
  github_down=0
  # Once in a calendar week the lane gets its design review before the next
  # card, also when no card is ready. The file holds the week and the tries,
  # or "done". A review that does not end with "STEP reviewed" is tried once
  # more that week, after the next session on a card: a judge that did not
  # answer needs time, not a second try at once. The word "pause" in the file
  # holds that wait, also over a restart of the run.
  due=0
  if [ "$review" = 1 ]; then
    week=${RUN_WEEK:-$(date +%G-%V)}
    state_week="" tries="" pause=""
    [ ! -f .cache/design-review ] || read -r state_week tries pause < .cache/design-review || true
    [ "$state_week" = "$week" ] || { tries=0 pause=""; }
    [ "$tries" = done ] || [[ "$tries" =~ ^[0-9]+$ ]] || tries=0
    if [ "$tries" != done ] && [ "$tries" -lt 2 ] && [ "$pause" != pause ]; then due=1; fi
  fi
  if [ -z "$card" ] && [ "$due" = 0 ]; then
    leave_turn_queue
    if [ "$once" != --once ] && { [ "${#aside_ends[@]}" -gt 0 ] \
      || [ "$for_joel" -gt 0 ]; }; then sync_main; fi
    next_end="" expired=0 checks_count=0
    for number in "${!aside_ends[@]}"; do
      if aside_over "$number"; then
        expired=1
      else
        if [ "${aside_runs[number]+set}" = set ]; then checks_count=$((checks_count + 1)); fi
        if [ -z "$next_end" ] || [ "${aside_ends[number]}" -lt "$next_end" ]; then next_end=${aside_ends[number]}; fi
      fi
    done
    [ "$expired" = 0 ] || continue
    if [ "${#aside_ends[@]}" -gt 0 ]; then
      remaining=$((next_end - $(now)))
      [ "$remaining" -gt 0 ] || continue
      [ "$remaining" -le "$retry_wait" ] || remaining=$retry_wait
      # Checks waits poll at most every ten minutes, even with a longer retry setting.
      if [ "$checks_count" -gt 0 ] && [ "$remaining" -gt 600 ]; then remaining=600; fi
      message="${#aside_ends[@]} cards set aside; next end at $next_end"
      if [ "$checks_count" -gt 0 ]; then message="$message; $checks_count of them until the next Checks run"; fi
      echo "$message" > "$status"
      sleep "$remaining"; continue
    fi
    if [ "$once" != --once ] && { [ "$for_joel" -gt 0 ]; }; then
      parked_count=0
      for number in ${parked//,/ }; do parked_count=$((parked_count + 1)); done
      message="waiting: no ready card; for Joel $for_joel, parked in this run $parked_count"
      if [ "$waiting" = 0 ]; then
        say "$message"; note "$message"; notify "$message"
        waiting=1
      fi
      echo "$message" > "$status"
      sleep "$retry_wait"; continue
    fi
    summary
    if [ -n "$parked" ]; then
      note "ended: no other card is ready"; notify "ended: no other card is ready"; say "no other card is ready in $lane"; exit 1
    fi
    note "ended: no ready card"; notify "ended: no ready card"; say "no ready card in $lane"; exit 0
  fi
  if [ -n "$card" ] && [ "$card" != "$last" ]; then
    if [ -n "$last" ]; then
      number=${last%%$'\t'*}; number=${number#\#}
      saved_counts[number]="$waits $repeats $disk_fails $last_result"
    fi
    last=$card
    number=${card%%$'\t'*}; number=${number#\#}
    read -r waits repeats disk_fails last_result <<< "${saved_counts[$number]:-0 0 0}"
  fi

  if [ "$(free_gib)" -lt "$free" ]; then
    leave_turn_queue
    say "less than $free GiB free; making space"
    if [ -n "${RUN_CLEAN:-}" ]; then $RUN_CLEAN --own-target "$free" || true
    else bash "$root/architecture/space.sh" --own-target "$free" || true; fi
    if [ "$(free_gib)" -lt "$free" ]; then
      # The run does not stop here: a wait costs no session. It tells Joel at
      # the first wait and then at every sixth, which is once an hour.
      full=$((full + 1))
      [ $(((full - 1) % 6)) -ne 0 ] || notify "waiting for disk space: $(free_gib) GiB free, $free needed"
      echo "waiting for disk space: $(free_gib) GiB free, $free needed" > "$status"
      say "waiting for disk space; next try in $retry_wait seconds"; sleep "$retry_wait"; continue
    fi
  fi
  full=0
  if [ "$due" = 0 ]; then checks_failed; fi
  number=${card%%$'\t'*}; number=${number#\#}
  if [ -n "$turn_enabled" ] && [ "$once" != --once ] && [ "$due" = 0 ] \
    && [ "${gate_requests[number]+set}" = set ] && [ -z "${gate_results[number]:-}" ]; then
    take_card_turn "$number"
  else
    leave_turn_queue
  fi
  sync_main

  waiting=0
  if [ "$due" = 1 ]; then
    session "the weekly design review" "" \
      "Run the weekly design review of the $lane lane for week $week. Follow the weekly review in .agents/skills/design-review/SKILL.md."
    if [[ "$result" == "STEP reviewed"* ]]; then down=0; echo "$week done" > .cache/design-review
    elif [ "$wrote" = 0 ] && [ "$seconds" -lt "$quick" ]; then lead_down # no try of the review
    else
      down=0
      echo "$week $((tries + 1)) pause" > .cache/design-review
      [ "$tries" -lt 1 ] || notify "the weekly design review failed twice (${result#STEP }); it is left for this week"
    fi
    [ "$once" != --once ] || { summary; exit 0; }
    continue
  fi

  number=${card%%$'\t'*}; number=${number#\#}
  slept=0
  gate_note=""
  land_head="" gate_again=0
  if [ "$once" != --once ] && [ "${gate_requests[number]+set}" = set ]; then full_gate; fi
  if [ -n "$land_head" ] && land_step; then
    release_turn
    continue
  fi
  release_turn
  [ "$gate_again" = 0 ] || continue
  if [ "${card%%$'\t'*}" != "#$first" ]; then
    say "taking #$owner with its open change before first ready card #$first"
  fi
  session "$card" "${red:+ The last Checks run of main failed: run $red.}$gate_note${merged_notes[number]:-}"
  request_commit=$(git rev-parse HEAD 2> /dev/null) || request_commit=""
  request_branch=$(git branch --show-current 2> /dev/null) || request_branch=""
  if [ "$wrote" = 1 ]; then
    unset 'gate_requests[number]' 'gate_branches[number]' 'gate_results[number]' 'merged_notes[number]'
  fi
  # A session on a card ends the pause of a failed design review.
  if [ -f .cache/design-review ] && read -r state_week tries pause < .cache/design-review && [ "$pause" = pause ]; then
    echo "$state_week $tries" > .cache/design-review
  fi
  case "$result" in
    "STEP merged"*)
      merged_step ;;
    "STEP planned"*)
      down=0
      if [ "$last_result" = planned ]; then park "$card" "planned twice without a step"
      else done_steps=$((done_steps + 1)); last_result=planned; fi ;;
    "STEP gate"*)
      if [ "$once" = --once ]; then
        say "with --once the run runs no gate; run just gate"
      else
        gate_requests[number]=$request_commit
        gate_branches[number]=$request_branch
        gate_wait "${result#STEP }"
      fi ;;
    "STEP waiting"* | "STEP later"*)
      down=0
      wait_limit=120
      if [[ "$result" == "STEP later"* ]]; then
        change_owner=$(choice "") || change_owner=0
        change_owner=${change_owner%%$'\t'*}
        if ! open_change || { [ "$change_owner" != 0 ] && [ "$change_owner" != "$number" ]; }; then wait_limit=720; fi
      fi
      read -r _ _ kind rest <<< "$result"
      # Reuse this pass's read: a Checks run ending during the session returns the card next pass.
      if [ "$wait_limit" = 720 ] && [ "$kind" = checks ] && checks_run; then
        aside_ends[number]=$(($(now) + checks_limit * unit))
        aside_runs[number]=${checks_latest%%$'\t'*}
        say "set aside #$number until the next Checks run: $rest"
        note "set aside #$number until the next Checks run: $rest"
      else
        if [ "$wait_limit" = 720 ] && [ "$kind" = checks ]; then
          say "the Checks runs could not be read; using STEP later 120 $rest"
          kind=120
          result="STEP later 120 $rest"
        fi
        waits=$((waits + 1))
        [[ "$kind" =~ ^[0-9]+$ ]] || kind=30
        [ "${#kind}" -le 4 ] || kind=$wait_limit
        kind=$((10#$kind))
        [ "$kind" -le "$wait_limit" ] || kind=$wait_limit
        if [ "$waits" -gt 6 ]; then park "$card" "waited six times: ${result#STEP * }"
        elif [ "$wait_limit" = 720 ]; then
          aside_ends[number]=$(($(now) + kind * unit))
          say "set aside #$number for $kind minutes: $rest"
          note "set aside #$number for $kind minutes: $rest"
        else
          say "waiting $kind minutes: $rest"
          # A sleep holds expired set-aside cards back for the next selection;
          # a ready owner of the open change still comes first.
          slept=1
          sleep "$((kind * unit))"
        fi
      fi ;;
    "STEP blocked"*)
      down=0; park "$card" "${result#STEP }" ;;
    *)
      if [ "$(free_gib)" -lt "$full_at" ] \
        || grep -qE "^gate: not enough free disk space|No space left on device( \(os error 28\))?\$" .cache/step.log 2> /dev/null; then
        # The disk ran out during the session: that is not the card's failure,
        # and it is not a lead that cannot start.
        down=0 disk_fails=$((disk_fails + 1))
        say "the disk filled during the session; this failure does not count against the card"
        [ "$disk_fails" -lt 3 ] || park "$card" "the disk filled during three sessions: ${result#STEP }"
      elif [ "$wrote" = 0 ] && [ "$seconds" -lt "$quick" ]; then lead_down
      else
        down=0 repeats=$((repeats + 1))
        [ "$repeats" -lt 2 ] || park "$card" "${result#STEP }"
      fi ;;
  esac
  [ "$once" != --once ] || { summary; exit 0; }
done
