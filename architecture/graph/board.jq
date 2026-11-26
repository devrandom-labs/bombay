# Board (architecture/board.sh JSON) -> N-Quads for the harness graph, version 0.
# Use: jq -r --arg project labcast-server -f board.jq board.json > board.nq
# One named graph holds what the board says (source:board). A second one holds what the
# harness derives from it (derived:board): the state of a card and the edge "waitsFor",
# which exists only while the card is open and the dependency is on the board and not done.
# The rules for "dependency" and "done" are those of architecture/backlog.jq.
def xsd($t): "^^<http://www.w3.org/2001/XMLSchema#\($t)>";
def v($n): "<urn:harness:v0:\($n)>";
def card($n): "<urn:harness:\($project):card:\($n)>";
def src: "<urn:harness:\($project):source:board>";
def der: "<urn:harness:\($project):derived:board>";
def lit: tojson;
def dependencies:
  (.content.body // "") | (split("## Dependencies")[1] // "") | (split("\n## ")[0] // "")
  | [scan("/issues/([0-9]+)") | .[0] | tonumber] | unique;
def done: .status == "Merged" or (((.labels // []) | index("linux-owed")) != null);
# RDF decimals have no exponent. Expand it without rounding the digits.
def decimal:
  tostring | if test("[eE]") then
    capture("^(?<sign>-?)(?<whole>[0-9]+)(?:\\.(?<fraction>[0-9]+))?[eE](?<power>[+-]?[0-9]+)$")
    | (.whole + (.fraction // "")) as $digits
    | ((.whole | length) + (.power | tonumber)) as $point
    | .sign + (if $point <= 0 then "0." + ("0" * -$point // "") + $digits
      elif $point >= ($digits | length) then $digits + ("0" * ($point - ($digits | length)) // "")
      else $digits[:$point] + "." + $digits[$point:] end)
  else . end;

[.items[] | select(.content.number != null)] as $cards
| ($cards | map({key: (.content.number | tostring), value: .}) | from_entries) as $by
| $cards[]
| card(.content.number) as $s
| ( "\($s) <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> \(v("WorkItem")) \(src) .",
    "\($s) \(v("number")) \"\(.content.number)\"\(xsd("integer")) \(src) .",
    "\($s) \(v("title")) \((.title // .content.title // "") | lit) \(src) .",
    (select(.status != null) | "\($s) \(v("status")) \(.status | lit) \(src) ."),
    (select(.kind != null) | "\($s) \(v("kind")) \(.kind | lit) \(src) ."),
    (select(.phase != null) | "\($s) \(v("phase")) \(.phase | lit) \(src) ."),
    (select(.workstream != null) | "\($s) \(v("workstream")) \(.workstream | lit) \(src) ."),
    (select((.["task ID"] // "") != "") | "\($s) \(v("taskId")) \(.["task ID"] | lit) \(src) ."),
    (select(.sequence != null)
      | if (.sequence | type) == "number" then "\($s) \(v("sequence")) \"\(.sequence | decimal)\"\(xsd("decimal")) \(src) ."
        else "graph: card #\(.content.number) has a nonnumeric Sequence\n" | stderr | empty end),
    ((.labels // [])[] | "\($s) \(v("label")) \(. | lit) \(src) ."),
    ((.assignees // [])[] | "\($s) \(v("assignee")) \(. | lit) \(src) ."),
    (dependencies[] | "\($s) \(v("dependsOn")) \(card(.)) \(src) ."),
    "\($s) \(v("state")) \(if .status == "Merged" then "merged" elif done then "linux-owed" else "open" end | lit) \(der) .",
    (dependencies[] | select($by[tostring] == null)
      | "\($s) \(v("missingDependency")) \(card(.)) \(der) ."),
    (select(done | not) | dependencies[]
      | select($by[tostring] != null and ($by[tostring] | done | not))
      | "\($s) \(v("waitsFor")) \(card(.)) \(der) .") )
