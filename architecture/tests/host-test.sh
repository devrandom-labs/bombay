#!/usr/bin/env bash
# Exercise the actual Just recipe and workflow shells with disposable commands.
set -euo pipefail
bash architecture/tests/commit-test.sh
root=$PWD
work=$(mktemp -d)
. "$PWD/architecture/tests/own-processes.sh"
own_init "$work"
# Fixture launchers get their own groups, so failure also ends their wait loops.
set -m
cleanup() {
  own_cleanup
  for child in $(jobs -pr; jobs -ps); do
    kill -KILL -- "-$child" 2>/dev/null || true
    kill -KILL "$child" 2>/dev/null || true
    wait -f "$child" 2>/dev/null || true
  done
}
trap 'trap - EXIT; cleanup; rm -rf "$work"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
trap 'exit 131' QUIT
fail() { echo "host-test: FAILED: $*" >&2; cat "$work"/*.log >&2; exit 1; }
wait_for() {
  local deadline=$((SECONDS + 10))
  while ! grep -q "$1" "$2"; do
    kill -0 "$3" 2>/dev/null || fail "process exited before $1"
    [ "$SECONDS" -lt "$deadline" ] || fail "timed out before $1"
    sleep 0.01
  done
}
wait_file() {
  local deadline=$((SECONDS + 10))
  while [ ! -e "$1" ]; do
    kill -0 "$2" 2>/dev/null || fail "process exited before $1"
    [ "$SECONDS" -lt "$deadline" ] || fail "timed out before $1"
    sleep 0.01
  done
}
mkdir -p "$work/bin" "$work/checkout with spaces/architecture"
cp architecture/{gate,space}.sh "$work/checkout with spaces/architecture/"
chmod u+w "$work/checkout with spaces/architecture/"*.sh
export GATE_LOCK=$work/lock GATE_POLL=0.01 GATE_FREE=15
export SPACE_FREE='echo 100' SPACE_CACHE=$work/none SPACE_BUILDS=$work/none
export work STUDIO_TEST_OWNER=test
printf '#!%s\n' "$BASH" > "$work/bin/ssh"
cat >> "$work/bin/ssh" <<'EOF'
set -euo pipefail
[ "$1" = -T ] && [ "$2" = labcast-desktop ] && [ "$#" = 3 ]
. "$OWN_HELPER"; own_write $$ "$work/ssh-pid"
exec bash -c "$3"
EOF
printf '#!%s\n' "$BASH" > "$work/bin/locked"
cat >> "$work/bin/locked" <<'EOF'
set -euo pipefail
if [ "$(uname -s)" = Linux ]; then
  if flock -n "$GATE_LOCK" true; then
    echo 'command ran without the host lock' >&2
    exit 90
  fi
else
  test -d "$GATE_LOCK"
  kill -0 "$(cat "$GATE_LOCK/pid")"
fi
EOF
printf '#!%s\n' "$BASH" > "$work/bin/nix"
cat >> "$work/bin/nix" <<'EOF'
set -euo pipefail
# Tool provisioning precedes the gate; wrapped builds still require its lock.
if [[ "$*" = 'shell --no-update-lock-file --inputs-from . '* ]]; then
  shift 4
  while [ "$1" != --command ]; do shift; done
  shift
  exec "$@"
fi
# Real Nix installs signal handlers, even when its parent ignores the signal.
if [ "${RESET_SIGNALS:-0}" = 1 ] && [ "${HOST_TEST_SIGNALS_RESET:-0}" != 1 ]; then
  exec env --default-signal=INT,TERM,HUP,QUIT HOST_TEST_SIGNALS_RESET=1 "$BASH" "$0" "$@"
fi
locked
printf 'nix %s\n' "$*" >> "$work/commands.log"
signal_entry() {
  trap 'exit 130' INT
  trap 'exit 143' TERM
  trap 'exit 129' HUP
  trap 'exit 131' QUIT
  . "$OWN_HELPER"; own_signal "$1" "$GATE_ENTRY"
  # Stay live until forwarding arrives; exiting here races the shell's trap.
  while :; do sleep 0.01; done
}
case "$*" in
  'flake check '*) exit "${BUILD_CHECK_STATUS:-0}" ;;
  *'docker-compose --profile server logs'*|*'--command docker compose --profile studio logs'*)
    touch "$work/cleanup-started"
    if [ "${FAIL_LOGS:-0}" = 1 ]; then exit 29; fi
    ;;
  *'docker-compose ps -aq server') echo 'server-id' ;;
  *'docker inspect --format'* ) echo 'false 0' ;;
  *'--print-out-paths'*)
    if [ "$2" = .#smelter-image ] && [ "${FAIL_BUILD_IMAGE:-0}" = 1 ]; then exit 43; fi
    echo "$work/bin/image"
    ;;
  *'--command just studio-down'|*'docker-compose --profile server down --volumes')
    . "$OWN_HELPER"; own_record "$PPID" group >/dev/null; own_write "$PPID" "$work/cleanup-pid"
    if [ "${RESET_SIGNALS:-0}" = 1 ]; then trap 'touch "$work/cleanup-interrupted"; exit 97' INT TERM; fi
    if [ "${HOLD_CLEANUP:-0}" = 1 ] && [ -e "$work/cleanup-started" ]; then
      touch "$work/cleanup-ready"
      while [ ! -e "$work/release-cleanup" ]; do sleep 0.01; done
    fi
    if [ "${FAIL_CLEANUP:-0}" = 1 ] && [ -e "$work/cleanup-started" ]; then exit 27; fi
    rm -f "$work/active-service"
    ;;
  *'just studio-linux-test'*|*'just telemetry'|*'just studio-tests'|*'just fuzz-run'*)
    touch "$work/active-service"
    if [ "${INT_ENTRY:-0}" = 1 ]; then signal_entry INT; fi
    if [ "${TERM_ENTRY:-0}" = 1 ]; then signal_entry TERM; fi
    if [ "${HUP_ENTRY:-0}" = 1 ]; then signal_entry HUP; fi
    if [ "${QUIT_ENTRY:-0}" = 1 ]; then signal_entry QUIT; fi
    if [ "${TERM_TEST:-0}" = 1 ]; then . "$OWN_HELPER"; own_signal TERM "$PPID"; exit 0; fi
    if [ "${FAIL_TEST:-0}" = 1 ]; then exit 23; fi
    ;;
esac
EOF
printf '#!%s\n' "$BASH" > "$work/bin/docker"
cat >> "$work/bin/docker" <<'EOF'
set -euo pipefail
if [ "${RECIPE_HOLD_CLEANUP:-0}" = 1 ] && [ "${RECIPE_DOCKER_RESET:-0}" != 1 ]; then
  exec env --default-signal=INT,TERM,HUP,QUIT RECIPE_DOCKER_RESET=1 "$BASH" "$0" "$@"
fi
locked
printf 'docker %s\n' "$*" >> "$work/commands.log"
case "$1" in
  ps)
    if [ "${RECIPE_HOLD_CLEANUP:-0}" = 1 ]; then . "$OWN_HELPER"; own_record "$PPID" group >/dev/null; own_write "$PPID" "$work/recipe-cleanup-pid"; fi
    echo test-engine
    ;;
  rm)
    if [ "${RECIPE_HOLD_CLEANUP:-0}" = 1 ]; then
      trap 'touch "$work/recipe-cleanup-interrupted"; exit 97' INT TERM HUP QUIT
      touch "$work/recipe-cleanup-ready"
      while [ ! -e "$work/recipe-release-cleanup" ]; do sleep 0.01; done
    fi
    rm -f "$work/active-service"
    ;;
esac
EOF
printf '#!/bin/sh\nlocked && echo "test image"\n' > "$work/bin/image"
chmod +x "$work/bin/"*
export PATH="$work/bin:$PATH"
touch "$work/commands.log"
own_test_stopped_cleanup cleanup || fail 'stopped direct child cleanup'
echo 'host-test: stopped direct child cleanup: pass'

# The Nix fixture must dispatch a tool shell before the real gate takes its lock.
nix shell --no-update-lock-file --inputs-from . nixpkgs#util-linux --command \
  bash architecture/gate.sh locked > "$work/bootstrap.log" 2>&1 || fail 'tool bootstrap failed'
nix shell --no-update-lock-file --inputs-from . nixpkgs#util-linux nixpkgs#gawk --command \
  bash architecture/gate.sh locked > "$work/bootstrap-multiple.log" 2>&1 || fail 'multiple tool bootstrap failed'

# The recipe must wait for a different holder, preserve quoting and return failure.
bash architecture/gate.sh bash -c 'touch "$work/holder-ready"; while [ ! -e "$work/release-holder" ]; do sleep 0.01; done' > "$work/holder.log" 2>&1 &
holder=$!
wait_file "$work/holder-ready" "$holder"
just --justfile "$root/justfile" linux "$work/checkout with spaces" 'locked; printf "%s\n" "literal $ and quote '\''" > result; exit 17' > "$work/recipe.log" 2>&1 &
recipe=$!
wait_for 'gate: waiting' "$work/recipe.log" "$recipe"
[ ! -e "$work/checkout with spaces/result" ] || fail 'recipe bypassed holder'
touch "$work/release-holder"
wait "$holder"
status=0
wait "$recipe" || status=$?
[ "$status" = 17 ] || fail "recipe hid command status: $status"
[ "$(cat "$work/checkout with spaces/result")" = "literal \$ and quote '" ] || fail 'recipe changed command quoting'
[ ! -e "$GATE_LOCK.cleanup" ] || fail 'recipe kept its cleanup record after teardown'
status=0
just --justfile "$root/justfile" linux "$work/checkout with spaces" 'locked; touch "$work/active-service"; exit 23' > "$work/recipe-failure.log" 2>&1 || status=$?
[ "$status" = 23 ] || fail 'recipe hid failure during teardown'
[ ! -e "$work/active-service" ] && [ ! -e "$GATE_LOCK.cleanup" ] || fail 'recipe failed to tear down its resource'

# An older or edited remote gate must not choose a different locking contract.
for script in gate space; do
  echo '# different remote source' >> "$work/checkout with spaces/architecture/$script.sh"
  status=0
  just --justfile "$root/justfile" linux "$work/checkout with spaces" 'touch result-stale' > "$work/stale.log" 2>&1 || status=$?
  [ "$status" = 2 ] || fail 'recipe accepted a different remote gate source'
  [ ! -e "$work/checkout with spaces/result-stale" ] || fail 'stale recipe ran its command'
  cp "$root/architecture/$script.sh" "$work/checkout with spaces/architecture/$script.sh"
done

# A lost SSH session and killed test leave their owner until labelled teardown.
just --justfile "$root/justfile" linux "$work/checkout with spaces" '
  echo "$STUDIO_TEST_OWNER" > "$work/recipe-owner"
  . "$OWN_HELPER"; own_record $$ group >/dev/null; own_write $$ "$work/recipe-command-pid"
  . "$OWN_HELPER"; own_write $PPID "$work/recipe-gate-pid"
  touch "$work/active-service" "$work/recipe-ready"
  while :; do sleep 0.01; done
' > "$work/lost-recipe.log" 2>&1 &
recipe=$!
wait_file "$work/recipe-ready" "$recipe"
owner=$(cat "$work/recipe-owner")
[ "$(cat "$GATE_LOCK.cleanup")" = "$owner" ] || fail 'recipe has no cleanup owner'
own_signal KILL "$(cat "$work/ssh-pid")"
own_signal KILL "-$(cat "$work/recipe-command-pid")"
own_signal KILL "$(cat "$work/recipe-gate-pid")" 2>/dev/null || true
status=0; wait "$recipe" || status=$?
[ "$status" != 0 ] || fail 'lost recipe reported success'
bash architecture/gate.sh bash -ec '[ ! -e "$work/active-service" ]; touch "$work/recipe-resumed"' > "$work/lost-contender.log" 2>&1 &
contender=$!
wait_for 'gate: waiting for cleanup' "$work/lost-contender.log" "$contender"
[ ! -e "$work/recipe-resumed" ] || fail 'lost recipe released a running resource'
GATE_OWNER=$owner STUDIO_TEST_OWNER=$owner GATE_CLEANUP=1 GATE_FREE=0 bash architecture/gate.sh bash -euo pipefail -c 'docker ps -aq --filter "label=labcast.studio.test=$STUDIO_TEST_OWNER" | xargs -r docker rm --force' > "$work/recipe-recovery.log" 2>&1 || fail 'recipe recovery failed'
wait "$contender" || fail 'recipe contender failed after cleanup'
[ ! -e "$GATE_LOCK.cleanup" ] && [ -e "$work/recipe-resumed" ] || fail 'recipe recovery did not release the host'
own_forget "$(cat "$work/recipe-command-pid")"
rm "$work/recipe-command-pid" "$work/recipe-gate-pid" "$work/ssh-pid"

# Signal the remote shell while its command is live, then let that command end.
# A second signal must not change status or release the host during teardown.
for signal in INT TERM HUP QUIT; do
  for phase in command cleanup; do
    rm -f "$work/recipe-ready" "$work/recipe-release-command" "$work/recipe-cleanup-ready" "$work/recipe-release-cleanup" "$work/recipe-resumed"
    export RECIPE_HOLD_CLEANUP=1 RECIPE_PHASE=$phase
    expected=23
    if [ "$phase" = command ]; then
      case "$signal" in INT) expected=130 ;; TERM) expected=143 ;; HUP) expected=129 ;; QUIT) expected=131 ;; esac
    fi
    env --default-signal=INT,TERM,HUP,QUIT just --justfile "$root/justfile" linux "$work/checkout with spaces" '
      trap "exit 130" INT
      trap "exit 143" TERM
      trap "exit 129" HUP
      trap "exit 131" QUIT
      . "$OWN_HELPER"; own_record $$ group >/dev/null; own_write $$ "$work/recipe-command-pid"
      . "$OWN_HELPER"; own_write $PPID "$work/recipe-gate-pid"
      touch "$work/active-service" "$work/recipe-ready"
      if [ "$RECIPE_PHASE" = cleanup ]; then exit 23; fi
      while [ ! -e "$work/recipe-release-command" ]; do sleep 0.01; done
    ' > "$work/recipe-$signal-$phase.log" 2>&1 &
    recipe=$!
    wait_file "$work/recipe-ready" "$recipe"
    if [ "$phase" = command ]; then
      own_signal "$signal" "$(cat "$work/ssh-pid")"
      touch "$work/recipe-release-command"
    fi
    wait_file "$work/recipe-cleanup-ready" "$recipe"
    bash architecture/gate.sh bash -ec '[ ! -e "$work/active-service" ]; touch "$work/recipe-resumed"' > "$work/recipe-contender.log" 2>&1 &
    contender=$!
    wait_for 'gate: waiting' "$work/recipe-contender.log" "$contender"
    if [ "$phase" = cleanup ]; then
      own_signal "$signal" "$(cat "$work/ssh-pid")"
    else
      own_signal TERM "$(cat "$work/ssh-pid")"
    fi
    [ ! -e "$work/recipe-resumed" ] || fail 'signalled recipe released its resource early'
    touch "$work/recipe-release-cleanup"
    status=0; wait "$recipe" || status=$?
    [ "$status" = "$expected" ] || fail "recipe $signal $phase status was $status, expected $expected"
    [ ! -e "$work/recipe-cleanup-interrupted" ] || fail 'recipe signal interrupted teardown'
    wait "$contender" || fail 'recipe contender entered before teardown'
    [ ! -e "$GATE_LOCK.cleanup" ] && [ -e "$work/recipe-resumed" ] || fail 'signalled recipe did not finish cleanup'
    own_forget "$(cat "$work/recipe-command-pid")" "$(cat "$work/recipe-cleanup-pid")"
    rm "$work/recipe-command-pid" "$work/recipe-gate-pid" "$work/recipe-cleanup-pid" "$work/ssh-pid"
    printf 'host-test: recipe %s during %s passes\n' "$signal" "$phase"
  done
done
unset RECIPE_HOLD_CLEANUP RECIPE_PHASE

# Read the workflow itself so a bypass in its shell or cleanup is observable.
yq -o=json .github/workflows/checks.yml > "$work/workflow.json"
workflow_env() {
  jq -r --arg job "$1" --argjson index "$2" '.jobs[$job].steps | map(select(has("run"))) | .[$index].env // {} | to_entries[] | [.key, .value] | @tsv' "$work/workflow.json" |
    while IFS=$'\t' read -r key value; do
      value=${value//'${{ env.STUDIO_TEST_OWNER }}'/$STUDIO_TEST_OWNER}
      printf 'export %s=%q\n' "$key" "$value"
    done
}
for job in build check; do
  jq -r --arg job "$job" '.jobs[$job].steps | map(select(has("run"))) | .[0].run' "$work/workflow.json" > "$work/$job.sh"
  [ -s "$work/$job.sh" ] || fail "$job has no command span"
  shell=$(jq -r --arg job "$job" '.jobs[$job].steps | map(select(has("run"))) | .[0].shell // "bash -e {0}"' "$work/workflow.json")
  # Replace trusted Actions expressions with their manual-run values.
  sed -e "s/\${{ github.event_name == 'schedule' \&\& 2 || 1 }}/1/g" \
      -e "s/\${{ github.event_name == 'schedule' \&\& 300 || 15 }}/15/g" "$work/$job.sh" > "$work/$job-run.sh"
  if [ "$job" = check ]; then
    { printf '. "$OWN_HELPER"; own_record $$ group >/dev/null; own_write $$ "$work/command-pid"; . "$OWN_HELPER"; own_write $PPID "$work/gate-pid"; export GATE_ENTRY=$PPID\n'; cat "$work/$job-run.sh"; } > "$work/instrumented"
    mv "$work/instrumented" "$work/$job-run.sh"
  fi
  printf -v run_path '%q' "$work/$job-run.sh"
  { workflow_env "$job" 0; printf '%s\n' "${shell//\{0\}/$run_path}"; } > "$work/$job-launch.sh"
done
jq -er '.jobs.check.steps[] | select(.name == "Finish cleanup after cancellation or failure") | .if | contains("always()")' "$work/workflow.json" > /dev/null || fail 'missing always-run cleanup'
jq -r '.jobs.check.steps | map(select(has("run"))) | .[1].run' "$work/workflow.json" > "$work/backstop.sh"
{ printf '. "$OWN_HELPER"; own_write $PPID "$work/gate-pid"\n'; cat "$work/backstop.sh"; } > "$work/instrumented"
mv "$work/instrumented" "$work/backstop.sh"
shell=$(jq -r '.jobs.check.steps | map(select(has("run"))) | .[1].shell' "$work/workflow.json")
printf -v run_path '%q' "$work/backstop.sh"
{ workflow_env check 1; printf '%s\n' "${shell//\{0\}/$run_path}"; } > "$work/backstop-launch.sh"
mkdir -p "$work/ci/architecture" "$work/ci/.github" "$work/ci/platform/local" "$work/ci/apps/server/tests/fixtures" "$work/ci/target/studio-evidence/calibration-test"
cp architecture/{gate,space}.sh "$work/ci/architecture/"
cp .github/cleanup.sh "$work/ci/.github/"
touch "$work/ci/platform/local/"{jaeger.yaml,prometheus.yaml,postgres.sql} "$work/ci/apps/server/tests/fixtures/readiness.sql"
# Fake only the unchanged source-preservation command; all lock behavior is real.
printf '#!/bin/sh\nexit 0\n' > "$work/bin/git"
chmod +x "$work/bin/git"
export RUNNER_TEMP=$work/tmp RUNNER_TOOL_CACHE=$work/tools
export STUDIO_PROFILE='' CHECK_EVENT=workflow_dispatch
export SMELTER_IMAGE=test LABCAST_LOCAL_SERVER_IMAGE=test
cd "$work/ci"
bash "$work/build-launch.sh" > "$work/build.log" 2>&1 || fail 'build span failed'
status=0
BUILD_CHECK_STATUS=42 RUNNER_TEMP="$work/check-failure" GITHUB_OUTPUT="$work/check-failure.output" \
  bash "$work/build-launch.sh" > "$work/check-failure.log" 2>&1 || status=$?
[ "$status" = 42 ] || fail "build span returned $status after the check failed, expected 42"
grep -qx 'saved=true' "$work/check-failure.output" || fail 'failed check prevented marking a complete bundle saved'
for name in server studio smelter; do
  [ -s "$work/check-failure/tested-images/$name-image.tar" ] || fail "failed check prevented saving the $name image"
done
echo 'host-test: failed check retains its status and saves the images'

# The second run starts with the complete image set saved by the failed check.
for build_temp in fresh check-failure; do
  output="$work/producer-$build_temp.output"
  : > "$output"
  status=0
  BUILD_CHECK_STATUS=42 FAIL_BUILD_IMAGE=1 RUNNER_TEMP="$work/$build_temp" GITHUB_OUTPUT="$output" \
    bash "$work/build-launch.sh" > "$work/producer-$build_temp.log" 2>&1 || status=$?
  if grep -qx 'saved=true' "$output"; then fail "failed image producer marked the $build_temp bundle saved"; fi
  [ "$status" = 43 ] || fail "failed image producer returned $status with $build_temp artifacts, expected 43"
  echo "host-test: failed image producer leaves no marker ($build_temp)"
done

for kind in telemetry fuzz; do
  CHECK_KIND=$kind bash "$work/check-launch.sh" > "$work/$kind.log" 2>&1 || fail "$kind success span failed"
  CHECK_KIND=$kind timeout 10 bash "$work/backstop-launch.sh" > "$work/backstop.log" 2>&1 || fail "$kind success cleanup failed"
  [ ! -e "$GATE_LOCK.cleanup" ] || fail 'successful cleanup kept the handoff record'
  CHECK_KIND=$kind timeout 10 bash "$work/backstop-launch.sh" > "$work/backstop.log" 2>&1 || fail "$kind repeated cleanup failed"
done
# Isolate the driver's presence probe; commands still use the real workflow shell.
sed "s|/run/opengl-driver/share/vulkan/icd.d/nvidia_icd.json|$work/driver|g" "$work/check-run.sh" > "$work/check-driver.sh"
mv "$work/check-driver.sh" "$work/check-run.sh"
touch "$work/driver"
CHECK_KIND=Linux-release STUDIO_PROFILE=release bash "$work/check-launch.sh" > "$work/profile.log" 2>&1 || fail 'profile span failed'
CHECK_KIND=Linux-release STUDIO_PROFILE=release timeout 10 bash "$work/backstop-launch.sh" > "$work/backstop.log" 2>&1 || fail 'profile cleanup failed'
CHECK_KIND=GPU bash "$work/check-launch.sh" > "$work/GPU.log" 2>&1 || fail 'GPU span failed'
CHECK_KIND=GPU timeout 10 bash "$work/backstop-launch.sh" > "$work/backstop.log" 2>&1 || fail 'GPU cleanup failed'
grep -q 'docker rm --force test-engine' "$work/commands.log" || fail 'profile did not clean its engines'
light=$(jq -r '.jobs.changes.steps[] | select(.name == "Check documents, workflows and Nix formatting") | .run' "$work/workflow.json")
light_free=$(jq -r '.jobs.changes.env.GATE_FREE // "15"' "$work/workflow.json")
GATE_FREE=$light_free SPACE_FREE='echo 0' bash -c "$light" > "$work/light.log" 2>&1 || fail 'light job required full-gate disk space'
# Failure and TERM both retain the lock until teardown finishes.
for mode in failure term entry-term entry-int entry-hup entry-quit cleanup-cancel cleanup-failure logs-failure hard-kill; do
  for pid_file in command-pid cleanup-pid; do [ ! -f "$work/$pid_file" ] || own_forget "$(cat "$work/$pid_file")"; done
  rm -f "$work/cleanup-ready" "$work/release-cleanup" "$work/cleanup-started" "$work/command-pid" "$work/cleanup-pid"
  export FAIL_TEST=0 TERM_TEST=0 TERM_ENTRY=0 INT_ENTRY=0 HUP_ENTRY=0 QUIT_ENTRY=0 RESET_SIGNALS=0 FAIL_CLEANUP=0 FAIL_LOGS=0 HOLD_CLEANUP=1
  case "$mode" in
    failure) export FAIL_TEST=1; expected=23 ;;
    term) export TERM_TEST=1; expected=143 ;;
    entry-term) export TERM_ENTRY=1; expected=143 ;;
    entry-int) export INT_ENTRY=1 RESET_SIGNALS=1; expected=130 ;;
    entry-hup) export HUP_ENTRY=1 RESET_SIGNALS=1; expected=129 ;;
    entry-quit) export QUIT_ENTRY=1 RESET_SIGNALS=1; expected=131 ;;
    cleanup-cancel) export RESET_SIGNALS=1; expected=0 ;;
    cleanup-failure) export FAIL_CLEANUP=1; expected=27 ;;
    logs-failure) export FAIL_LOGS=1; expected=29 ;;
    hard-kill) export FAIL_TEST=1; expected=137 ;;
  esac
  # The first telemetry down is setup; block only the final teardown.
  # Instrument its final diagnostic call to arm the cleanup wait.
  CHECK_KIND=telemetry env --default-signal=INT,TERM,HUP,QUIT bash "$work/check-launch.sh" > "$work/$mode.log" 2>&1 &
  runner=$!
  wait_file "$work/command-pid" "$runner"
  wait_file "$work/cleanup-ready" "$runner"
  cleanup_group=$(cat "$work/cleanup-pid")
  bash architecture/gate.sh bash -ec '[ ! -e "$work/active-service" ]; touch "$work/unsafe"' > "$work/contender.log" 2>&1 &
  contender=$!
  wait_for 'gate: waiting' "$work/contender.log" "$contender"
  [ ! -e "$work/unsafe" ] || fail "$mode released the lock before cleanup"
  if [ "$mode" = entry-int ]; then
    own_signal TERM "$(cat "$work/gate-pid")"
    [ ! -e "$work/unsafe" ] || fail 'second signal released the cleanup lock'
  elif [ "$mode" = cleanup-cancel ]; then
    own_signal INT "$(cat "$work/gate-pid")"
  elif [ "$mode" = hard-kill ]; then
    [ -e "$work/active-service" ] || fail 'hard-kill fixture had no running service'
    own_signal KILL "-$(cat "$work/command-pid")"
    own_signal KILL "-$(cat "$work/cleanup-pid")"
  fi
  touch "$work/release-cleanup"
  status=0
  wait "$runner" || status=$?
  [ "$status" = "$expected" ] || fail "$mode status was $status, expected $expected"
  [ ! -e "$work/cleanup-interrupted" ] || fail 'second signal interrupted a cleanup command'
  wait_for 'gate: waiting for cleanup' "$work/contender.log" "$contender"
  [ ! -e "$work/unsafe" ] || fail "$mode contender ran before the cleanup handoff"
  if [ "$mode" = cleanup-failure ]; then
    status=0
    HOLD_CLEANUP=0 CHECK_KIND=telemetry timeout 10 bash "$work/backstop-launch.sh" > "$work/failed-backstop.log" 2>&1 || status=$?
    [ "$status" = 27 ] || fail 'backstop hid its cleanup failure'
    [ -e "$GATE_LOCK.cleanup" ] && [ ! -e "$work/unsafe" ] || fail 'failed cleanup ended the handoff'
  elif [ "$mode" = hard-kill ]; then
    GATE_OWNER=other GATE_CLEANUP=1 bash architecture/gate.sh touch "$work/wrong-owner" > "$work/wrong-owner.log" 2>&1 &
    wrong_owner=$!
    wait_for 'gate: waiting for cleanup' "$work/wrong-owner.log" "$wrong_owner"
    [ ! -e "$work/wrong-owner" ] || fail 'another owner bypassed the cleanup handoff'
  fi
  rm "$work/cleanup-ready" "$work/release-cleanup"
  export FAIL_CLEANUP=0 FAIL_LOGS=0 RESET_SIGNALS=1
  # A diagnostic outage must not prevent resource recovery or clear job failure.
  if [ "$mode" = logs-failure ]; then export FAIL_LOGS=1; fi
  CHECK_KIND=telemetry SPACE_FREE='echo 0' env --default-signal=INT,TERM,HUP,QUIT timeout 10 bash "$work/backstop-launch.sh" > "$work/backstop.log" 2>&1 &
  backstop=$!
  wait_file "$work/cleanup-ready" "$backstop"
  own_signal INT "$(cat "$work/gate-pid")"
  [ ! -e "$work/unsafe" ] || fail 'cancelled backstop released its cleanup handoff'
  touch "$work/release-cleanup"
  wait "$backstop" || fail 'backstop failed'
  [ ! -e "$GATE_LOCK.cleanup" ] || fail 'successful cleanup kept the handoff record'
  wait "$contender" || fail "$mode contender ran while a service was active"
  if [ "$mode" = hard-kill ]; then wait "$wrong_owner" || fail 'other owner did not resume after cleanup'; fi
  [ ! -e "$work/active-service" ] || fail 'backstop left a service running'
  rm "$work/unsafe"
  own_forget "$(cat "$work/command-pid")" "$cleanup_group" "$(cat "$work/cleanup-pid")"
  rm "$work/command-pid"
done
echo 'host-test: recipe, CI spans, signal cleanup and hard-kill backstop pass'
