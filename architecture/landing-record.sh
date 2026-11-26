# shellcheck shell=bash
# Read a landing record with the process helpers from start-common.sh.
# Use a NUL between fields so paths and titles keep their spaces and newlines.

# An independent clone in the owner's work folder lands into that owner's
# state. Keep its ordinary harness state separate so it cannot start the
# owner's check loop or lanes using the candidate checkout.
landing_state() {
  local state=$1 checkout=$2 owner_checkout=$3 parent=${1%/*} relative
  if [ -z "${HARNESS_STATE:-}" ] && [ "$checkout" = "$owner_checkout" ]; then
    case $checkout in
      "$parent"/*/work/*)
        relative=${checkout#"$parent"/}
        printf '%s/%s\n' "$parent" "${relative%%/*}"
        return
        ;;
    esac
  fi
  printf '%s\n' "$state"
}

# shellcheck disable=SC2034 # The caller reads the record fields.
read_landing() {
  {
    IFS= read -r -d '' landing_clone &&
    IFS= read -r -d '' landing_branch &&
    IFS= read -r -d '' landing_title &&
    IFS= read -r -d '' landing_body &&
    IFS= read -r -d '' landing_repo &&
    IFS= read -r -d '' landing_main &&
    IFS= read -r -d '' landing_begin &&
    IFS= read -r -d '' landing_pid &&
    IFS= read -r -d '' landing_started &&
    IFS= read -r -d '' landing_command &&
    IFS= read -r -d '' landing_turn_token &&
    IFS= read -r -d '' landing_reason
  } < "$1"
}

landing_live() {
  local command started status
  [[ $landing_pid =~ ^[1-9][0-9]*$ && -n $landing_started ]] || return 2
  if ! status=$(ps -p "$landing_pid" -o stat= 2>/dev/null) ||
     ! started=$(process_started "$landing_pid") ||
     ! command=$(process_command "$landing_pid"); then
    if kill -0 "$landing_pid" 2>/dev/null; then return 2; fi
    return 1
  fi
  [[ $status =~ ^[[:space:]]*[^Z[:space:]] && $started == "$landing_started" ]] || return 1
  # The one-time turn token comes before arguments that ps can escape.
  [ "$command" = "$landing_command" ] ||
    [[ -n $landing_turn_token && $command == "bash architecture/run.sh turn env LAND_TURN=$landing_turn_token "* ]]
}
