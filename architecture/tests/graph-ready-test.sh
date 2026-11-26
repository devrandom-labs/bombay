#!/usr/bin/env bash
# Compare named graph queries with the lane selector on the same fixture board.
set -euo pipefail
architecture=${1:-architecture}
work=$(mktemp -d)
export HOME=$work/home XDG_STATE_HOME=$work/xdg
mkdir -p "$HOME" "$XDG_STATE_HOME"
trap 'rm -rf -- "$work"' EXIT
tree=$work/architecture
data=$work/data
mkdir -p "$tree" "$work/bin"
cp -R "$architecture/graph" "$architecture/backlog.jq" "$architecture/project.env" "$tree/"
chmod -R u+w "$tree/"
unset HARNESS_ASSIGNEE GRAPH_PULLS
# shellcheck source=architecture/project.env
source "$tree/project.env"
export BOARD=$work/board.json
printf '#!%s\n' "$BASH" > "$tree/board.sh"
cat >> "$tree/board.sh" <<'BOARD'
cat "$BOARD"
BOARD
cat > "$work/bin/gh" <<'GH'
#!/bin/sh
[ "$1 $2" = 'pr list' ] || exit 98
printf '[]\n'
GH
chmod +x "$work/bin/gh"
export PATH="$work/bin:$PATH"

jq -n --arg assignee "$HARNESS_ASSIGNEE" '
  def card($n; $stream): {
    content: {number: $n}, title: "Card \($n)", workstream: $stream,
    assignees: [$assignee], phase: "Phase 1", sequence: 1, kind: "Task", status: "Active"
  };
  def dep($n): .content.body = "## Dependencies\n/issues/\($n)";
  {items: [
    (card(1; "Server") | dep(99)),
    (card(2; "Server") | del(.phase)),
    (card(3; "Server") | .assignees = ["someone-else"]),
    (card(4; "Server") | .labels = ["needs-joel"]),
    (card(5; "Server") | .labels = ["linux-owed"] | dep(2)),
    (card(6; "Shared") | .status = "Merged"),
    (card(8; "Server") | dep(6)),
    (card(7; "Server") | dep(5)),
    (card(9; "Shared") | .phase = "Phase 2" | .sequence = 0),
    (card(10; "Server") | del(.sequence)),
    (card(11; "Studio") | .labels = ["linux-owed"]),
    (card(12; "Studio") | .phase = "Phase 3"),
    (card(13; "Studio") | dep(2)),
    (card(14; "Studio") | .sequence = -1),
    (card(15; "DevOps") | dep(6))
  ]}' > "$BOARD"

bash "$tree/graph/build.sh" "$data" > /dev/null
while read -r lane stream; do
  jq -r --arg lane "$lane" --arg mode status --arg skip '' --arg assignee "$HARNESS_ASSIGNEE" \
    -f "$tree/backlog.jq" "$BOARD" |
    awk '/^ready now:$/ {ready=1; next} ready && /^  #/ {sub(/^  /, ""); print; next} {ready=0}' |
    jq -Rr --slurpfile board "$BOARD" --arg stream "$stream" '
      split("\t") as [$card, $task, $title]
      | select(any($board[0].items[]; "#\(.content.number)" == $card and .workstream == $stream))
      | "\($card)\t\($title)"' > "$work/backlog"
  [ -s "$work/backlog" ]
  bash "$tree/graph/q.sh" "$data" ready "$stream" | awk -F '\t' 'NR > 1 {print $1 "\t" $4}' |
    diff -u "$work/backlog" -
done <<'LANES'
server Server
server Shared
studio Studio
nix DevOps
LANES
echo 'graph-ready-test: ready matches backlog status for every fixture workstream'
