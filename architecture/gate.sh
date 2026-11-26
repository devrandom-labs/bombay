#!/usr/bin/env bash
# Full gate behind a machine-wide lock. Labcast lanes run one full gate at a
# time; a second gate waits for its turn and does not fail. Builds of other
# projects do not hold this lock. AGENTS.md, Workflow, Lanes owns the rule.
# `just gate` and `.githooks/pre-push` run this from the repository root.
# A clean HEAD tree that passed on this machine skips the lock and the gate.
# GATE_PASSED sets the record file (the last 50 passes in the Labcast cache);
# GATE_AGAIN=1 skips only the pass record; Nix still reuses every output in the
# store. Tracked changes and Git failures have no identity.
# A pinned pass keeps each check's output as a garbage-collector root under
# ${XDG_CACHE_HOME:-$HOME/.cache}/labcast/nix/gate/<tree>/<check>.
# Measured outputs: most 0 KiB; openapi 0.2 MiB; documents 10 MiB (55 MiB closure).
# GATE_KEEP sets that folder. The gate removes store links of a tree that left
# the pass record (the last 50), then removes its folder only if it is empty.
# space.sh removes links older than a day only when `nix store gc` alone did
# not free enough space, and only below the Labcast cache folder
# ${XDG_CACHE_HOME:-$HOME/.cache}/labcast/nix. A GATE_KEEP outside that folder
# is not cleaned by space.sh.
# The gate needs about 12 GiB of disk. It makes GATE_FREE GiB (15) of space
# first with space.sh, and it fails at once when it cannot: a build that runs
# out of disk fails late.
# A temporary Nix profile holds dependencies during the checks. A successful
# full gate keeps that same result in the current profile. Retention failures
# warn without losing the check result, its pass record or its output roots.
# Arguments replace the gate command and never use the pass record. A checkout
# with run.sh gives ordinary commands a place in the merge turn before the
# machine lock. The Linux recipe can ship just gate.sh and space.sh to its host.
# --space runs disk cleanup under this lock without joining the merge turn.
# Linux uses flock on /tmp/labcast-gate.lock for all users; macOS keeps its
# per-user directory lock. GATE_LOCK and GATE_POLL serve the test.
# GATE_OWNER records a command's unfinished cleanup beside that lock. Other
# commands wait, releasing the lock so its GATE_CLEANUP=1 backstop can run.
# Only successful cleanup by the recorded owner removes that record.
# The Linux record is /tmp/labcast-gate.lock.cleanup ($GATE_LOCK.cleanup in tests).
# If its owner never returns or teardown fails, inspect that owner's processes
# and resources first. For CI, find containers labelled labcast.studio.test=<owner>
# and its Compose project. Stop those resources with the owner's teardown command.
# Run that idempotent teardown as the file's user (the runner for CI) or root:
# GATE_OWNER=<recorded-owner> GATE_CLEANUP=1 GATE_FREE=0 bash architecture/gate.sh <teardown-command>
# This holds the lock through recovery and clears the record only on success.
# Do not remove a record just because it is old: its resources may still run.
# An empty record reports owner unknown; inspect its resources and recover with
# GATE_OWNER=unknown. A partial nonempty line retains the owner that it contains.
# GATE_STORE (default /nix/store) sets
# the store prefix only for the link-removal filter in the test.
set -euo pipefail

space_only=0
if [ "${1:-}" = --space ]; then space_only=1; shift; fi

scripts_cfg=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
if [ -f "$scripts_cfg/project.env" ]; then . "$scripts_cfg/project.env"; fi
cache_ns=${HARNESS_CACHE_NAME:-bombay}
kernel=$(uname -s)
if [ "$kernel" = Linux ]; then
  lock=${GATE_LOCK:-/tmp/${cache_ns}-gate.lock}
else
  lock=${GATE_LOCK:-${XDG_CACHE_HOME:-$HOME/.cache}/${cache_ns}/gate.lock}
fi
poll=${GATE_POLL:-20}
cleanup_record=$lock.cleanup
passed=${GATE_PASSED:-${XDG_CACHE_HOME:-$HOME/.cache}/${cache_ns}/gate.passed}
keep=${GATE_KEEP:-${XDG_CACHE_HOME:-$HOME/.cache}/${cache_ns}/nix/gate}

if [ "$space_only" = 0 ] && { [ -n "${GATE_OWNER:-}" ] || [ "${GATE_CLEANUP:-0}" = 1 ]; }; then
  if [ $# -eq 0 ] || [ -z "${GATE_OWNER:-}" ] || [[ "$GATE_OWNER" = *$'\n'* ]]; then
    echo 'gate: cleanup ownership needs a command and a nonempty, single-line GATE_OWNER' >&2
    exit 2
  fi
fi

scripts=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
gate_in_run=0
if [ "${BASH_SOURCE[1]:-}" = "$scripts/run.sh" ]; then gate_in_run=1; fi
if [ "$space_only" = 0 ] && [ "$#" -gt 0 ] && [ "${GATE_CLEANUP:-0}" != 1 ] &&
  [ "$gate_in_run" = 0 ] && [ -f "$scripts/run.sh" ]; then
  turn_path=${RUN_TURN:-${XDG_CACHE_HOME:-$HOME/.cache}/${cache_ns}/turn.lock}
  turn_owner=$(cat "$turn_path/pid" 2>/dev/null || true)
  # run.sh turn passes its PID through intervening commands. The direct parent
  # also covers an older run.sh turn launching this gate itself.
  if [ -z "$turn_owner" ] || ! { [ "$turn_owner" = "${RUN_TURN_OWNER:-}" ] || [ "$turn_owner" = "$PPID" ]; }; then
    exec "$BASH" "$scripts/run.sh" gate "$@"
  fi
fi

space_signals() {
  trap 'exit 130' INT
  trap 'exit 143' TERM
  trap 'exit 129' HUP
  trap 'exit 131' QUIT
}

make_space() {
  local status=0
  # A trapped signal waits for the foreground child. Keep the directory lock
  # until both the cleanup shell and its current command have stopped.
  space_signals
  (
    space_signals
    set +e
    . "$scripts/space.sh" "$@"
  ) || status=$?
  trap - INT TERM HUP QUIT
  return "$status"
}

identity() {
  local changes tree
  changes=$(git status --porcelain --untracked-files=no 2> /dev/null) || return 1
  [ -z "$changes" ] || return 1
  tree=$(git rev-parse "${1:-HEAD}^{tree}" 2> /dev/null) || return 1
  printf '%s\n' "$tree"
}

keep_results() {
  local system outputs name output
  system=$(nix eval --impure --raw --expr builtins.currentSystem) || return 1
  outputs=$(nix eval "$source#checks.$system" --no-update-lock-file --raw \
    --apply 'checks: builtins.concatStringsSep "\n" (map (name: name + "\t" + checks.${name}.outPath) (builtins.attrNames checks))') || return 1
  mkdir -p "$keep/$tree" || return 1
  while IFS=$'\t' read -r name output; do
    [ -n "$name" ] || continue
    if [ ! -e "$output" ]; then
      echo "gate: passed; check $name is missing from the store; skipped"
      continue
    fi
    # build --out-link registers a GC root, using the same Nix CLI as the gate.
    nix build "$output" --out-link "$keep/$tree/$name" > /dev/null || return 1
  done <<< "$outputs"
}

tree=
if [ "$space_only" = 0 ] && [ $# -eq 0 ]; then
  tree=$(identity) || tree=
fi
if [ -n "$tree" ] && [ "${GATE_AGAIN:-0}" != 1 ]; then
  date=$(awk -v tree="$tree" '$1 == tree { print substr($0, index($0, " ") + 1); exit }' "$passed" 2> /dev/null || true)
  if [ -n "$date" ]; then
    echo "gate: this tree passed the full gate ($date); it does not run again"
    exit 0
  fi
fi

# The supervisor owns these files. A direct gate leaves them alone.
gate_time() {
  local timestamp
  if [ -n "${GATE_RUN_NOW:-}" ]; then timestamp=$($GATE_RUN_NOW); else timestamp=$(date +%s); fi
  printf '%s %s\n' "$1" "$timestamp" >> "$GATE_RUN_TIMING"
}

reported_wait=0
report_wait() {
  [ -n "${GATE_RUN_LABEL:-}" ] || return 0
  if [ "$reported_wait" = 0 ]; then
    gate_time waiting || true
    reported_wait=1
  fi
  printf '%s is waiting for the gate lock (%s)\n' "$GATE_RUN_LABEL" "$1" > "$GATE_RUN_STATUS" || true
}

linux_lock_holder() {
  local device inode
  read -r device inode < <(stat -Lc '%Hd:%Ld %i' /dev/fd/9) || return 1
  # Match the open file, even if the lock path is replaced while we wait.
  lslocks -n -o PID,TYPE,MAJ:MIN,INODE | awk -v device="$device" -v inode="$inode" \
    '$1 > 0 && $2 == "FLOCK" && $3 == device && $4 == inode { print $1; exit }'
}

stale_directory() {
  if [ -n "$1" ]; then
    ! kill -0 "$1" 2> /dev/null
  else
    # Allow a new holder a minute to publish its PID.
    [ -n "$(find "$lock" -maxdepth 0 -mmin +1 2> /dev/null)" ]
  fi
}

clear_stale_directory() (
  # Two waiters can observe the same dead holder. Serialize the recheck and
  # removal so the second cannot remove the directory of a new holder.
  # The Linux tests also exercise the directory path with a stubbed uname.
  if [[ $OSTYPE == darwin* ]]; then /usr/bin/lockf -s 9
  else flock -x 9; fi
  holder=$(cat "$lock/pid" 2> /dev/null || true)
  if stale_directory "$holder"; then
    if [ -n "$holder" ]; then echo "gate: process $holder left a lock; removing it"
    else echo "gate: removing a lock without an owner"; fi
    rm -rf "$lock"
  fi
) 9>> "$lock.reap"

mkdir -p "$(dirname "$lock")"

recovering=0
gate_locked=0
while :; do
  if [ "$kernel" = Linux ]; then
    while :; do
      # Publish a regular file without opening an existing path (even a FIFO).
      if [ ! -e "$lock" ] && [ ! -L "$lock" ]; then
        temporary=$(mktemp "$lock.XXXXXX")
        chmod 666 "$temporary"
        ln -T "$temporary" "$lock" 2> /dev/null || true
        rm "$temporary"
      fi
      [ -f "$lock" ] && [ ! -L "$lock" ] || {
        echo "gate: $lock must be a regular file, not a link" >&2
        exit 1
      }
      # Linux can forbid another user's writable open in /tmp, even at mode 0666.
      exec 9< "$lock"
      if flock -n -E 75 9; then
        :
      else
        status=$?
        [ "$status" -eq 75 ] || exit "$status"
        echo 'gate: waiting for the machine lock'
        if [ -n "${GATE_RUN_LABEL:-}" ]; then
          holder=$(linux_lock_holder 2> /dev/null) || holder=
          report_wait "process ${holder:-unknown}"
        fi
        flock 9
      fi
      # A cleaner can replace an idle file between open and lock acquisition.
      [ /dev/fd/9 -ef "$lock" ] && break
      exec 9<&-
      echo 'gate: the lock path changed; acquiring it again'
    done
    # Keep the file: unlinking it could let a new holder bypass an existing waiter.
  else
    until mkdir_error=$(env LC_ALL=C mkdir "$lock" 2>&1); do
      if [[ "$mkdir_error" != *": File exists" ]]; then
        echo "gate: could not create the machine lock: $mkdir_error" >&2
        exit 1
      fi
      holder=$(cat "$lock/pid" 2> /dev/null || true)
      if stale_directory "$holder"; then
        clear_stale_directory
        continue
      fi
      echo "gate: waiting for the full gate of process ${holder:-that starts now}"
      report_wait "process ${holder:-unknown}"
      sleep "$poll"
    done
    echo $$ > "$lock/pid"
    gate_locked=1
    trap 'if [ "$gate_locked" = 1 ]; then rm -rf "$lock"; fi; if [ "$gate_in_run" = 1 ]; then release_turn; fi' EXIT
  fi

  cleanup_owner=
  if [ -e "$cleanup_record" ] || [ -L "$cleanup_record" ]; then
    [ -f "$cleanup_record" ] && [ ! -L "$cleanup_record" ] || {
      echo "gate: $cleanup_record must be a regular file, not a link" >&2
      exit 1
    }
    IFS= read -r cleanup_owner < "$cleanup_record" || [ -n "$cleanup_owner" ] || cleanup_owner=unknown
    if [ "$space_only" = 0 ] && [ "${GATE_CLEANUP:-0}" = 1 ] && [ "$GATE_OWNER" = "$cleanup_owner" ]; then
      recovering=1
      break
    fi
    echo "gate: waiting for cleanup of $cleanup_owner; record: $cleanup_record"
    report_wait "cleanup of $cleanup_owner"
    echo 'gate: recovery: as the record user or root, inspect its resources, then run its teardown with GATE_OWNER=<recorded-owner> GATE_CLEANUP=1 GATE_FREE=0 bash architecture/gate.sh <teardown-command>'
    # Let the owning job's backstop take the lock before another heavy command.
    if [ "$kernel" = Linux ]; then
      exec 9<&-
    else
      gate_locked=0
      rm -rf "$lock"
    fi
    sleep "$poll"
  else
    break
  fi
done

if [ "$space_only" = 1 ]; then
  make_space "$@"
  exit
fi

if [ -n "${GATE_RUN_LABEL:-}" ]; then
  gate_time held || true
  printf '%s\n' "$GATE_RUN_LABEL" > "$GATE_RUN_STATUS" || true
fi

if [ $# -eq 0 ]; then
  profile=$(bash "$scripts/space.sh" --profile)
  # The machine lock owns stale temporary generations left by a killed gate.
  trap 'rm -f "$profile".pending* || echo "gate: temporary dependencies could not be removed: $profile.pending"; if [ "$kernel" != Linux ]; then rm -rf "$lock"; fi' EXIT
  if ! rm -f "$profile".pending*; then
    echo "gate: stale dependencies could not be removed before cleanup: $profile.pending"
  fi
fi

if [ "${GATE_CLEANUP:-0}" != 1 ] && ! make_space "${GATE_FREE:-15}"; then
  echo "gate: not enough free disk space for the full gate"
  exit 1
fi

tree=
if [ $# -gt 0 ]; then
  if [ -n "${GATE_OWNER:-}" ] && [ "${GATE_CLEANUP:-0}" != 1 ]; then
    # A killed holder leaves this fact behind. Only its successful cleanup clears it.
    (umask 022; set -C; printf '%s\n' "$GATE_OWNER" > "$cleanup_record")
  fi
  status=0
  command_prefix=()
  # A gate entered by the supervisor must not resolve its private functions
  # as commands. Bash's command builtin still allows normal shell builtins.
  if [ "$gate_in_run" = 1 ]; then command_prefix=(command --); fi
  if [ -z "${GATE_OWNER:-}" ] || [[ -t 0 || -t 1 || -t 2 ]]; then
    # Keep terminal input and ordinary commands in the caller's foreground group.
    ${command_prefix[@]+"${command_prefix[@]}"} "$@" 9<&- || status=$?
  else
    # Owned batch commands keep cleanup pending even if their gate is killed.
    # Forward signals to their group and hold the lock through EXIT cleanup.
    command_pid=
    pending_signal=
    forward_signal() {
      if [ -n "$command_pid" ]; then
        kill -"$1" -- "-$command_pid" 2> /dev/null || true
      else
        pending_signal=$1
      fi
    }
    trap 'forward_signal INT' INT
    trap 'forward_signal TERM' TERM
    trap 'forward_signal HUP' HUP
    trap 'forward_signal QUIT' QUIT
    set -m
    ${command_prefix[@]+"${command_prefix[@]}"} "$@" 9<&- <&0 &
    command_pid=$!
    set +m
    if [ -n "$pending_signal" ]; then
      forward_signal "$pending_signal"
    fi
    while :; do
      wait "$command_pid" || true
      # A signal interrupts wait before the child has finished its cleanup.
      kill -0 "$command_pid" 2> /dev/null || break
    done
    wait "$command_pid" || status=$?
    trap - INT TERM HUP QUIT
  fi
  if [ "$status" -eq 0 ] && [ "$recovering" -eq 1 ]; then
    rm "$cleanup_record"
  fi
  exit "$status"
else
  source=.
  # Pin only URL-safe paths; other paths use the working tree without a record.
  if [[ "$PWD" != *[!a-zA-Z0-9/._~-]* ]] &&
    commit=$(git rev-parse HEAD 2> /dev/null) && tree=$(identity "$commit"); then
    source="git+file://$PWD?rev=$commit"
  fi
  dependencies=
  if mkdir -p "$(dirname "$profile")" &&
    dependencies=$(nix build "$source#dependencies" --no-update-lock-file --no-link --print-out-paths --max-jobs 1 --cores 4 -L --profile "$profile.pending" 9<&-); then
    :
  else
    echo "gate: dependencies could not be held during checks: $profile.pending"
    dependencies=
  fi
  nix flake check "$source" --no-update-lock-file --max-jobs 1 --cores 4 -L 9<&-
  if [ -n "$tree" ] && [ -n "$dependencies" ] && ! nix build "$dependencies" --no-link --profile "$profile" 9<&-; then
    echo "gate: passed; the dependencies could not be kept: $profile"
  fi
fi

if [ -n "$tree" ]; then
  if ! {
    mkdir -p "$(dirname "$passed")" &&
    printf '%s %s\n' "$tree" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$passed" &&
    tail -n 50 "$passed" > "$passed.tmp" &&
    mv "$passed.tmp" "$passed"
  }; then
    echo "gate: passed; the pass record could not be written"
  else
    for folder in "$keep"/*; do
      [ ! -L "$folder" ] || continue
      name=${folder##*/}
      [[ "$name" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ ]] || continue
      if ! awk -v tree="$name" '$1 == tree { found=1; exit } END { exit !found }' "$passed"; then
        find "$folder" -maxdepth 1 -type l -lname "${GATE_STORE:-/nix/store}/*" -delete 2> /dev/null || true
        rmdir "$folder" 2> /dev/null || true
      fi
    done
  fi
  if ! keep_results; then
    echo "gate: passed; the results could not be kept"
  fi
fi
