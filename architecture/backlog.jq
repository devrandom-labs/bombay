# Card selection for the automatic run (.agents/project.md, Automatic run).
# Input: `bash architecture/board.sh`.
# Arguments: lane (runtime | domain | adapters | nix), mode (next | status | joel | state | dependents | measure) and skip.

def streams: {
  runtime: ["Runtime", "Shared"],
  domain: ["Domain"],
  adapters: ["Adapters"],
  nix: ["DevOps"],
  store: ["Store", "Shared"],
  identity: ["Identity", "Shared"],
  network: ["Network"],
  server: ["Server", "Shared"],
  studio: ["Studio"]
};
def phases: ["Phase 1", "Phase 2", "Phase 3"];
def target_assignee: ($ARGS.named.assignee // "joeldsouzax");
def done:
  if .status == "Merged" then "merged"
  elif (.labels // []) | index("linux-owed") then "linux-owed"
  else null end;
def dependencies:
  (.content.body // "") | (split("## Dependencies")[1] // "") | (split("\n## ")[0] // "")
  | [scan("/issues/([0-9]+)") | .[0] | tonumber];
def line: "#\(.content.number)\t\(.["task ID"] // "-")\t\(.title)";

def card_numbers: map("#\(.)") | join(" ");
def merged: map(select(.status == "Merged")) | length;

def loops($cards):
  def prune:
    INDEX(.[]; .number) as $left
    | INDEX(.[].waits[]; .) as $incoming
    | map(select($incoming[.number | tostring] != null)
          | .waits |= map(select($left[tostring] != null))
          | select(.waits != [])) as $kept
    | if length == ($kept | length) then $kept else $kept | prune end;
  [$cards[] | select(done == null) | {number: .content.number, waits: (dependencies | unique)}]
  | sort_by(.number) | prune;

def measure($cards; $by; $assignee):
  ($cards | map(select(.kind == "Task" and .phase == "Phase 1"))) as $phase
  | ($phase | map(select(.["task ID"] != null and .["task ID"] != ""))) as $plan
  | "Phase 1 task cards Merged: \($phase | merged) of \($phase | length)",
    ($phase | group_by(.workstream)[] | "  \(.[0].workstream // "no workstream"): \(merged) of \(length) Merged, Active \(map(select(.status == "Active")) | length)"),
    "Phase 1 plan cards (with a Task ID) Merged: \($plan | merged) of \($plan | length)",
    ($plan | group_by(.workstream)[] | "  \(.[0].workstream // "no workstream"): \(merged) of \(length) Merged"),
    "Phase 1 cards made during the work (no Task ID) Merged: \(($phase | merged) - ($plan | merged)) of \(($phase | length) - ($plan | length))",
    "open cards of \($assignee) with missing board fields:",
    ($cards[] | select(.status != "Merged" and .kind != "Track" and .kind != "Decision")
      | select((.assignees // []) | index($assignee))
      | select(.kind == null or .phase == null or .workstream == null)
      | select((.status == "Candidate" and .phase == null) | not)
      | "  #\(.content.number) kind=\(.kind) phase=\(.phase) workstream=\(.workstream) status=\(.status) \((.title // "")[0:60])"),
    "cards with needs-joel:",
    ($cards[] | select(.status != "Merged") | select((.labels // []) | index("needs-joel"))
      | "  #\(.content.number) \(.workstream // "-") \((.title // "")[0:70])"),
    "cards that block the most open Phase 1 tasks:",
    ([$phase[] | select(.status != "Merged") | dependencies | unique | .[]]
      | group_by(.) | map({number: .[0], blocks: length})
      | map(select($by[.number | tostring] | done | not))
      | sort_by(-.blocks, .number) | .[0:6][]
      | $by[.number | tostring] as $blocker
      | "  #\(.number) (\(if $blocker == null then "not on the board" else $blocker.status // "no status" end)) blocks \(.blocks)"),
    "open cards in a dependency loop:",
    (loops($cards) | map(.number) | if length == 0 then "  none" else "  " + card_numbers end);

def backlog($cards; $by):
($skip | split(",") | map(select(length > 0) | tonumber)) as $skipped
| [ $cards[]
    | select(.kind == "Task" and .status != "Merged" and .status != "Candidate")
    | select((.assignees // []) | index(target_assignee))
    | select(.workstream as $stream | (streams[$lane] // []) | index($stream))
    | select(.phase as $phase | phases | index($phase))
    | . + { waits: [dependencies[] | select($by[tostring] | done | not)],
            joel: ((.labels // []) | index("needs-joel") != null) } ]
| sort_by((.phase as $phase | phases | index($phase)),
          (.sequence == null), .sequence, .content.number) as $open
| [$open[] | select(.joel | not) | select(.waits == [])
           | select(.content.number as $n | $skipped | index($n) | not)] as $ready
| if $mode == "next" then
    ([($ARGS.named.branch // "") | splits("[/_.-]")
      | select(test("^[0-9]+$")) | tonumber][0]) as $n
    | ([$open[] | select(.content.number == $n)][0]) as $owner
    | ([$ready[] | select(.content.number == $owner.content.number)][0] // $ready[0]) as $next
    | if $ARGS.named | has("branch") then
        [($owner.content.number // 0), ($ready[0].content.number // 0),
         ($next | if . == null then "" else line end)] | join("\t")
      else ($next // empty | line) end
  elif $mode == "state" then
    $by[$ARGS.named.number // ""] // empty | done // "pending"
  elif $mode == "joel" then
    [$open[] | select(.joel)
             | select(.content.number as $n | $skipped | index($n) | not)] | length
  else
    "open \($open | length), ready \($ready | length), waiting for Joel's answer \([$open[] | select(.joel)] | length)",
    "ready now:",
    ($ready[0:5][] | "  " + line),
    "cards outside the run of this lane that block open cards:",
    ( [$open[].content.number] as $mine
      | [$open[].waits[]] | group_by(.) | map({number: .[0], card: $by[.[0] | tostring], blocks: length})
      | map(select(.number as $n | $mine | index($n) | not))
      | sort_by(-.blocks)[]
      | if .card == null then "  #\(.number) not in the project blocks \(.blocks)"
        else "  #\(.number) \(.card.kind // "-") \(.card.workstream // "-") \(.card.phase // "-") \(if (.card.assignees // []) == [] then "nobody" else (.card.assignees | join(",")) end) blocks \(.blocks): \(.card.title // "")"
        end ),
    (loops($cards)
      | map(select($by[.number | tostring].workstream as $stream | (streams[$lane] // []) | index($stream)))
      | select(length > 0)
      | "cards of this lane in a dependency loop:",
        (.[] | "  #\(.number) waits for \(.waits | card_numbers)"))
  end;

[.items[] | select(.content.number != null)] as $cards
| ($cards | map({key: (.content.number | tostring), value: .}) | from_entries) as $by
| if $mode == "measure" then measure($cards; $by; target_assignee)
  elif $mode == "dependents" then
    ($by[$ARGS.named.number] // empty | .content.number) as $number
    | [$cards[] | select(.status != "Merged" and (dependencies | index($number) != null))] | length
  else backlog($cards; $by) end
