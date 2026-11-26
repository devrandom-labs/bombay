# Merged pull requests (gh pr list --json number,title,mergedAt,mergeCommit,headRefName,files)
# -> N-Quads for the harness graph, version 0.
# Use: jq -r --arg project labcast-server -f changes.jq pulls.json > changes.nq
# A change implements the card whose number is the first digits-only part of its branch
# name (the rule of the bindings and of architecture/backlog.jq). A change touches files;
# a file belongs to a lane by its path (AGENTS.md, Lanes).
def xsd($t): "^^<http://www.w3.org/2001/XMLSchema#\($t)>";
def v($n): "<urn:harness:v0:\($n)>";
def card($n): "<urn:harness:\($project):card:\($n)>";
def change($n): "<urn:harness:\($project):change:\($n)>";
def file($p): "<urn:harness:\($project):file:\($p | @uri)>";
def src: "<urn:harness:\($project):source:pulls>";
def lit: tojson;
def lane($p):
  if ($p | test("^(apps/server/|docs/arch/server/)")) then "server"
  elif ($p | test("^(apps/studio/|crates/editorial/|docs/arch/studio/)")) then "studio"
  elif ($p | test("^(architecture/|\\.agents/|AGENTS\\.md$|CLAUDE\\.md$)")) then "harness"
  else "shared" end;
def owner: [(.headRefName // "") | splits("[/_.-]") | select(test("^[0-9]+$")) | tonumber][0];

.[] | change(.number) as $s
| ( "\($s) <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> \(v("Change")) \(src) .",
    "\($s) \(v("number")) \"\(.number)\"\(xsd("integer")) \(src) .",
    "\($s) \(v("title")) \(.title | lit) \(src) .",
    "\($s) \(v("mergedAt")) \"\(.mergedAt)\"\(xsd("dateTime")) \(src) .",
    (select(.mergeCommit.oid != null) | "\($s) \(v("commit")) \(.mergeCommit.oid | lit) \(src) .", "\($s) \(v("commitShort")) \(.mergeCommit.oid[0:12] | lit) \(src) ."),
    (owner as $n | select($n != null) | "\($s) \(v("implements")) \(card($n)) \(src) ."),
    ((.files // [])[] | .path as $p
      | "\($s) \(v("touches")) \(file($p)) \(src) .",
        "\(file($p)) \(v("path")) \($p | lit) \(src) .",
        "\(file($p)) \(v("lane")) \(lane($p) | lit) \(src) .") )
