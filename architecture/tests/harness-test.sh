#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=architecture/tests/start-fixture.sh
. "$(dirname -- "$0")/start-fixture.sh"
fixture_init
fixture_session_commands
cleanup() {
  local code=$? file pid
  for file in "$fixture/holder.parent" "$fixture"/fresh-*.pid; do
    if [ -r "$file" ] && IFS= read -r pid < "$file"; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done
  fixture_cleanup "$code"
}
trap cleanup EXIT
fixture_copy architecture/harness.sh
fixture_copy architecture/lane.sh
mv "$checkout/architecture/lane.sh" "$checkout/architecture/lane-real.sh"

mkdir -p "$checkout/.claude/agents"
touch "$checkout/.claude/agents/harness-owner.md" "$checkout/architecture/owner/settings.json"
cat > "$checkout/architecture/run.sh" <<'STUB'
# started again with the supervisor of main
FIXTURE=${HOME%/home}
printf '%s %s %s\n' "$BASH" "$0" "$1" > "$FIXTURE/commands/$$"
printf '%s\n' "$$" > "$FIXTURE/fresh-$1.pid"
while [ ! -e "$FIXTURE/no-ready-cards" ]; do sleep 0.05; done
printf 'no ready card in %s\n' "$1"
STUB

for tool in lane.sh host-series.sh gate-times.sh owner/session.sh; do
  cat > "$checkout/architecture/$tool" <<'STUB'
set -eu
{
  printf '%s' "${0##*/}"
  printf '\t%s' "$@"
  printf '\n'
} >> "$FIXTURE/calls"
if [ "${FRESH:-0}" = 1 ] && [ "${0##*/}:$1" = lane.sh:start ]; then
  exec "$BASH" "${0%/*}/lane-real.sh" "$@"
fi
case "${0##*/}:$*" in
  'lane.sh:start studio --unless-stopped')
    printf 'studio already runs.\nserver: RUNNING\n' ;;
  'lane.sh:start server'*) exec "$BASH" "${0%/*}/lane-real.sh" "$@" ;;
  'lane.sh:start idle --unless-stopped')
    echo 'lane: 1 leftover Nix build folders; run in a terminal: bash lane.sh sweep'
    echo 'idle: started in the background.' ;;
  'host-series.sh:start '*)
    if [ "${FAILED_PART:-}" = host ]; then
      printf 'host probe error\nhost requires macOS\n\n' >&2
      exit 7
    fi
    echo 'host-series started as process 13'
    ;;
  'gate-times.sh:start '*) echo 'gate-times is running' ;;
  'session.sh:loop start') echo 'owner loop started (process 14)' ;;
  *) printf '%s says %s\n' "${0##*/}" "$1" ;;
esac
STUB
done
fixture_commit
fixture_remote
harness=$checkout/architecture/harness.sh

# A fresh clone is its own main checkout. A linked checkout outside the
# configured lanes has the other role, even when its directory is a lane name.
clone=$fixture/clone/${checkout##*/}
git clone -q -b trunk "$fixture/remote" "$clone"
git -C "$checkout" worktree add -q --detach "$worktrees/studio" HEAD
git -C "$checkout" worktree add -q --detach "$fixture/server" HEAD
expect "$(where_lines "$checkout" "$checkout" "$worktrees" "$state" owner)" "$("$BASH" "$harness" --where)"
expect "$(where_lines "$checkout" "$worktrees/studio" "$worktrees" "$state" 'lane studio')" \
  "$("$BASH" "$worktrees/studio/architecture/harness.sh" --where)"
expect "$(where_lines "$checkout" "$fixture/server" "$worktrees" "$state" other)" \
  "$("$BASH" "$fixture/server/architecture/harness.sh" --where)"
clone_state=$(fixture_state "$XDG_STATE_HOME" "$clone")
[ "$state" != "$clone_state" ]
expect "$(where_lines "$clone" "$clone" "$clone-worktrees" "$clone_state" owner)" \
  "$("$BASH" "$clone/architecture/harness.sh" --where)"
expect "$(where_lines "$checkout" "$checkout" "$worktrees" "$(fixture_state "$HOME/.local/state" "$checkout")" owner)" \
  "$(XDG_STATE_HOME='' "$BASH" "$harness" --where)"
[ ! -e "$state" ]
[ ! -e "$HOME/.local" ]
[ ! -e "$fixture/calls" ]

# Test discovery and missing commands without falling through to host tools.
mkdir "$fixture/probe"
for name in git bash mkdir dirname awk cat; do
  ln -s "$(command -v "$name")" "$fixture/probe/$name"
done
for name in codex claude oxigraph nix just ast-grep; do
  fixture_stub "$name" <<'STUB'
exit 98 # Doctor checks availability without starting the program.
STUB
done
fixture_stub jq <<'STUB'
[ "$#" = 1 ] && [ "$1" = --version ] || exit 98
printf '%s\n' "${JQ_VERSION:-jq-1.8.2}"
STUB
fixture_stub gh <<'STUB'
case "$*" in
  'api repos/example/harness -q .full_name')
    echo repo >> "$FIXTURE/api"
    [ "${FAILED_PART:-}" != github ] || exit 1
    echo example/harness ;;
  'api graphql -f query=query($login: String!, $number: Int!)'*)
    [ "$5 $6 $7 $8 $9 ${10}" = '-f login=fixture-org -F number=12 -q .data.organization.projectV2.title' ] &&
      [[ $4 == *'organization(login: $login) { projectV2(number: $number) { title } }'* ]] || exit 98
    echo board >> "$FIXTURE/api"
    if [ "${FAILED_PART:-}" = board ]; then echo null; else echo 'Fixture Board'; fi ;;
  *) exit 98 ;;
esac
STUB
fixture_stub df <<'STUB'
[ "$*" = "-Pk $HARNESS_STATE" ] || exit 98
[ "${DF_UNAVAILABLE:-0}" = 0 ] || exit 98
printf 'Filesystem 1024-blocks Used Available\nfixture 99999999 100 %s\n' "$((${DF_GIB:-15} * 1048576))"
STUB
for name in gh jq ast-grep nix just oxigraph codex claude df ps; do
  ln -s "$fixture/bin/$name" "$fixture/probe/$name"
done

export HARNESS_WORKTREES=$fixture/'lanes override' HARNESS_STATE=$fixture/'state override'
export HARNESS_LANES='studio server idle missing' STEP_LEAD='bash fixture lead'
mkdir -p "$HARNESS_WORKTREES"/{studio,server,idle}/.cache
printf '%s\n' "$$" > "$HARNESS_WORKTREES/studio/.cache/run.pid"
printf '%s\n' "bash $HARNESS_WORKTREES/studio/architecture/run.sh studio" > "$fixture/commands/$$"
printf '%s\n' '2026-11-20 10:00' > "$HARNESS_WORKTREES/server/.cache/run.stopped"
ln -s "$fixture" "$fixture/alias"
expect "$(where_lines "$checkout" "$checkout" "$HARNESS_WORKTREES" "$HARNESS_STATE" owner)" \
  "$(cd -- "$fixture" && HARNESS_STATE='alias/state override/' HARNESS_WORKTREES='alias/lanes override' "$BASH" "$harness" --where)"
[ ! -e "$HARNESS_STATE" ]
[ ! -e "$fixture/api" ]
expect "$(where_lines "$checkout" "$worktrees/studio" "$HARNESS_WORKTREES" "$HARNESS_STATE" other)" \
  "$("$BASH" "$worktrees/studio/architecture/harness.sh" --where)"
expect "$(where_lines "$checkout" "$worktrees/studio" "$worktrees" "$HARNESS_STATE" other)" \
  "$(HARNESS_WORKTREES="$worktrees" HARNESS_LANES=server "$BASH" "$worktrees/studio/architecture/harness.sh" --where)"

probe() {
  PATH="$fixture/probe" "$BASH" "$harness" "$@"
}

ready=$(cat <<EXPECTED
ok       role: owner (the main checkout $checkout)
ok       tools git: $fixture/probe/git
ok       tools gh: $fixture/probe/gh
ok       tools jq: jq-1.8.2 ($fixture/probe/jq)
ok       tools ast-grep: $fixture/probe/ast-grep
ok       tools nix: $fixture/probe/nix
ok       tools just: $fixture/probe/just
ok       tools oxigraph: $fixture/probe/oxigraph
ok       tools codex: $fixture/probe/codex
ok       tools claude: $fixture/probe/claude
ok       github: example/harness
ok       board: fixture-org/12 (Fixture Board)
ok       lead: bash fixture lead (from STEP_LEAD)
ok       agent: harness-owner and owner settings exist
ok       lane studio: running
note     lane server: stopped by a person on 2026-11-20 10:00
note     lane idle: not running
note     lane missing: no worktree yet; up makes it
ok       state: $HARNESS_STATE (15 GiB free)
doctor: ready
EXPECTED
)
invoke 0 probe doctor
expect "$ready" "$actual"
expect $'repo\nboard' "$(cat "$fixture/api")"
[ -n "$(find "$HARNESS_STATE" -prune -perm 700 -print)" ]
[ -z "$(ls -A "$HARNESS_STATE")" ]
[ ! -e "$fixture/calls" ]
JQ_VERSION=jq-2.0 invoke 0 probe doctor
expect "${ready/jq-1.8.2/jq-2.0}" "$actual"

nix_hint='run this from the Nix shell of the repository (nix develop, or direnv allow)'
github_missing='MISSING  github: cannot read example/harness -> gh auth login, or set GH_TOKEN for an account with access'
board_missing='MISSING  board: cannot read fixture-org/12 -> gh auth refresh -s project'
github_ok='ok       github: example/harness'
board_ok='ok       board: fixture-org/12 (Fixture Board)'
missing() {
  local count=$1 expected=$2
  shift 2
  invoke 1 "$@"
  expect "${expected/doctor: ready/doctor: $count missing}" "$actual"
}

# Assign the substitutions first so Bash 3.2 also respects the replacement quotes.
for name in git gh jq ast-grep nix just oxigraph codex claude; do
  mv "$fixture/probe/$name" "$fixture/removed"
  hint=$nix_hint
  case "$name" in codex|claude) hint='install the program and sign in' ;; esac
  line=$(grep -F "ok       tools $name: " <<< "$ready")
  replacement="MISSING  tools $name: not on PATH -> $hint"
  expected=${ready/$line/"$replacement"}
  count=1
  if [ "$name" = gh ]; then
    count=3
    expected=${expected/$github_ok/"$github_missing"}
    expected=${expected/$board_ok/"$board_missing"}
  fi
  missing "$count" "$expected" probe doctor
  mv "$fixture/removed" "$fixture/probe/$name"
done
line="ok       tools jq: jq-1.8.2 ($fixture/probe/jq)"
replacement="MISSING  tools jq: jq-1.7.1; need jq 1.8 or newer -> $nix_hint"
expected=${ready/$line/"$replacement"}
JQ_VERSION=jq-1.7.1 missing 1 "$expected" probe doctor
expected=${ready/$github_ok/"$github_missing"}
FAILED_PART=github missing 1 "$expected" probe doctor
expected=${ready/$board_ok/"$board_missing"}
FAILED_PART=board missing 1 "$expected" probe doctor
lead_ok='ok       lead: bash fixture lead (from STEP_LEAD)'
lead_default='ok       lead: codex exec -m gpt-6.2-sol -s danger-full-access (from architecture/project.env)'
unset STEP_LEAD
invoke 0 probe doctor
expected=${ready/$lead_ok/"$lead_default"}
expect "$expected" "$actual"
mv "$fixture/probe/codex" "$fixture/removed"
lead_missing='MISSING  lead: codex not on PATH (from architecture/project.env) -> install the program and sign in'
codex_ok="ok       tools codex: $fixture/probe/codex"
codex_missing='MISSING  tools codex: not on PATH -> install the program and sign in'
without_codex=${ready/$codex_ok/"$codex_missing"}
expected=${without_codex/$lead_ok/"$lead_missing"}
missing 2 "$expected" probe doctor
mv "$fixture/removed" "$fixture/probe/codex"
lead_missing='MISSING  lead: unknown-lead not on PATH (from STEP_LEAD) -> install the program and sign in'
expected=${ready/$lead_ok/"$lead_missing"}
STEP_LEAD='unknown-lead --option' missing 1 "$expected" probe doctor
export STEP_LEAD='bash fixture lead'
agent_ok='ok       agent: harness-owner and owner settings exist'
agent_missing='MISSING  agent: harness-owner or owner settings missing -> git restore -- .claude/agents/harness-owner.md architecture/owner/settings.json'
for file in .claude/agents/harness-owner.md architecture/owner/settings.json; do
  mv "$checkout/$file" "$fixture/removed"
  expected=${ready/$agent_ok/"$agent_missing"}
  missing 1 "$expected" probe doctor
  mv "$fixture/removed" "$checkout/$file"
done

mkdir -p "$checkout/.agents/agents"
touch "$checkout/.agents/agents/harness-owner.md" "$checkout/.agents/hooks.json" "$checkout/architecture/owner/agy-settings.json"
claude_ok="ok       tools claude: $fixture/probe/claude"
agy_ok="ok       tools agy: $fixture/probe/agy"
agy_tools=$claude_ok$'\n'$agy_ok
agy_ready=${ready/$claude_ok/"$agy_tools"}
agy_missing='MISSING  tools agy: not on PATH -> install the program and sign in'
expected=${agy_ready/$agy_ok/"$agy_missing"}
HARNESS_OWNER_CLI=agy missing 1 "$expected" probe doctor
ln -s "$fixture/bin/agy" "$fixture/probe/agy"
HARNESS_OWNER_CLI=agy invoke 0 probe doctor
expect "$agy_ready" "$actual"
agy_agent_missing='MISSING  agent: harness-owner or owner settings missing -> git restore -- .claude/agents/harness-owner.md .agents/agents/harness-owner.md .agents/hooks.json architecture/owner/settings.json architecture/owner/agy-settings.json'
for file in .claude/agents/harness-owner.md .agents/agents/harness-owner.md .agents/hooks.json \
  architecture/owner/settings.json architecture/owner/agy-settings.json; do
  mv "$checkout/$file" "$fixture/removed"
  expected=${agy_ready/$agent_ok/"$agy_agent_missing"}
  HARNESS_OWNER_CLI=agy missing 1 "$expected" probe doctor
  mv "$fixture/removed" "$checkout/$file"
done
bad_owner='MISSING  owner: HARNESS_OWNER_CLI must be agy or claude (got unknown) -> export HARNESS_OWNER_CLI=agy'
bad_tools=$claude_ok$'\n'$bad_owner
expected=${ready/$claude_ok/"$bad_tools"}
HARNESS_OWNER_CLI=unknown missing 1 "$expected" probe doctor

state_ok="ok       state: $HARNESS_STATE (15 GiB free)"
state_missing="MISSING  state: $HARNESS_STATE (14 GiB free) -> make space; a session needs 15 GiB"
expected=${ready/$state_ok/"$state_missing"}
DF_GIB=14 missing 1 "$expected" probe doctor
state_missing="MISSING  state: $HARNESS_STATE (15 GiB free) -> make space; a session needs 16 GiB"
expected=${ready/$state_ok/"$state_missing"}
HARNESS_MIN_FREE_GIB=16 missing 1 "$expected" probe up
[ ! -e "$fixture/calls" ]
state_missing='MISSING  state: HARNESS_MIN_FREE_GIB must be a nonnegative integer -> export HARNESS_MIN_FREE_GIB=15'
expected=${ready/$state_ok/"$state_missing"}
HARNESS_MIN_FREE_GIB=bad missing 1 "$expected" probe doctor
state_missing="MISSING  state: cannot read free disk space at $HARNESS_STATE -> check the filesystem or set HARNESS_STATE to a writable folder outside the checkout"
expected=${ready/$state_ok/"$state_missing"}
DF_UNAVAILABLE=1 missing 1 "$expected" probe doctor
touch "$fixture/state-file"
state_missing="MISSING  state: cannot create $fixture/state-file -> set HARNESS_STATE to a writable folder outside the checkout"
expected=${ready/$state_ok/"$state_missing"}
HARNESS_STATE="$fixture/state-file" missing 1 "$expected" probe doctor

# A non-owner exits after the role line, without API calls or state creation.
: > "$fixture/api"
for action in doctor up; do
  HARNESS_WORKTREES="$worktrees" HARNESS_STATE="$fixture/unused" \
    invoke 0 "$BASH" "$worktrees/studio/architecture/harness.sh" "$action"
  expect 'note     role: not the owner here (this is the worktree of the lane studio)' "$actual"
  HARNESS_WORKTREES="$worktrees" HARNESS_STATE="$fixture/unused" \
    invoke 0 "$BASH" "$fixture/server/architecture/harness.sh" "$action"
  expect "note     role: not the owner here (this checkout is not the main checkout $checkout)" "$actual"
done
[ ! -s "$fixture/api" ]
[ ! -e "$fixture/unused" ]
[ ! -e "$fixture/calls" ]

expect_up() {
  expect "$(printf 'lane.sh\tstart\t%s\t--unless-stopped\n' studio server idle missing)
$(printf 'host-series.sh\tstart\t%s\t%s\ngate-times.sh\tstart\t%s\t%s\nsession.sh\tloop\tstart' \
    "$HARNESS_STATE" "$checkout" "$HARNESS_STATE" "$HARNESS_WORKTREES")" "$(cat "$fixture/calls")"
}

started=$'studio: already runs\nserver: stopped by a person on 2026-11-20 10:00; start it with: bash architecture/lane.sh start server\nlane: 1 leftover Nix build folders; run in a terminal: bash lane.sh sweep\nidle: started\nmissing: started\nhost-series: started\ngate-times: already runs\nowner-loop: started'
# Start needs ordinary commands in addition to the tools doctor checks.
invoke 0 env PATH="$fixture/probe:$PATH" "$BASH" "$harness" up
expect "$ready"$'\n'"$started" "$actual"
expect_up
: > "$fixture/calls"
FAILED_PART=host invoke 1 env PATH="$fixture/probe:$PATH" "$BASH" "$harness" up
expect "$ready"$'\n'"${started/host-series: started/host-series: FAILED: host requires macOS}" "$actual"
expect_up

# Use the real lane launcher in a clone with no lane worktrees. The supervisor
# stub reports it has no ready cards and exits on its own after the assertions.
for name in pgrep caffeinate; do
  fixture_stub "$name" <<'STUB'
if [ "${0##*/}" = caffeinate ]; then shift; exec "$@"; fi
exit 1
STUB
done
: > "$fixture/process-table"
fixture_stub df <<'STUB'
[ "$1" = -Pk ]
printf 'Filesystem 1024-blocks Used Available\nfixture 99999999 100 33554432\n'
STUB
[ ! -e "$clone-worktrees" ]
fresh_state=$fixture/fresh-state
FRESH=1 HARNESS_STATE="$fresh_state" HARNESS_WORKTREES="$clone-worktrees" \
  HARNESS_LANES='server studio nix' GH_TOKEN=fixture-token \
  invoke 0 "$BASH" "$clone/architecture/harness.sh" up </dev/null
for name in server studio nix; do
  grep -Fxq "note     lane $name: no worktree yet; up makes it" <<< "$actual"
  grep -Fxq "$name: started" <<< "$actual"
  [ "$(git -C "$clone-worktrees/$name" rev-parse HEAD)" = "$(git -C "$clone" rev-parse HEAD)" ]
  expect "$(cat "$fixture/fresh-$name.pid")" "$(cat "$clone-worktrees/$name/.cache/run.pid")"
done
touch "$fixture/no-ready-cards"
for name in server studio nix; do
  await fixture_dead "$(cat "$fixture/fresh-$name.pid")"
  expect "no ready card in $name" "$(tail -n 1 "$clone-worktrees/$name/.cache/run.log")"
done

: > "$fixture/calls"
# The same command line that the real session uses, held by a fixture process.
mkfifo "$fixture/hold"
mkdir -p "$fixture/holder/architecture/owner"
cat > "$fixture/holder/architecture/owner/session.sh" <<'STUB'
exec 6<> "$FIXTURE/hold"
read -r unused <&6
STUB
state_arg=$(fixture_hash "$HARNESS_STATE")
"$BASH" "$fixture/holder/architecture/owner/session.sh" run talk "$state_arg" &
holder=$!
echo "$holder" > "$fixture/holder.parent"
await test -s "$fixture/commands/$holder"
mkdir -p "$HARNESS_STATE/owner.lock"
printf '%s\n' "$holder" > "$HARNESS_STATE/owner.lock/pid"
echo talk > "$HARNESS_STATE/owner.lock/kind"
echo 'Fri Nov 20 10:00:00 2026' > "$HARNESS_STATE/owner.lock/started"
status=$'lane.sh says status\nhost-series.sh says status\ngate-times.sh says status\nsession.sh says loop'
invoke 0 "$BASH" "$harness" status
expect "$status"$'\n'"owner: talk holds the lock (process $holder)" "$actual"
expect "$(printf 'lane.sh\tstatus\nhost-series.sh\tstatus\t%s\ngate-times.sh\tstatus\t%s\t%s\nsession.sh\tloop\tstatus' \
  "$HARNESS_STATE" "$HARNESS_STATE" "$HARNESS_WORKTREES")" "$(cat "$fixture/calls")"
echo 999999999 > "$HARNESS_STATE/owner.lock/pid"
expect "$status"$'\nowner: no lock holder' "$("$BASH" "$harness" status)"
printf 'close\n' > "$fixture/hold"
wait "$holder"
rm "$fixture/holder.parent"
invoke 2 "$BASH" "$harness" stop
expect 'usage: harness.sh up|status|doctor|--where' "$actual"
echo 'harness-test: checkout paths, preflight, start order and status pass'
