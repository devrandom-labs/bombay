#!/usr/bin/env bash
# Run from the repository root. Pass a graph directory to test a modified copy.
set -euo pipefail
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
export HOME=$work/home XDG_STATE_HOME=$work/xdg
mkdir -p "$HOME" "$XDG_STATE_HOME"
graph=${1:-architecture/graph}

jq -r --arg project fixture -f "$graph/board.jq" <<'INPUT' | diff -u /dev/fd/3 - 3<<'EXPECTED'
{"items":[
  {"content":{"number":1,"title":"A \"title\" with \\","body":"## Dependencies\n/issues/2 /issues/2 /issues/3 /issues/4 /issues/99\n## Notes\n/issues/5"},
   "status":"Active","kind":"Task","phase":"Phase 1","workstream":"Server","task ID":"T1","sequence":0.5,
   "labels":["tag","other"],"assignees":["one","two"]},
  {"content":{"number":2},"title":"Fallback","task ID":""},
  {"content":{"number":3,"body":"## Dependencies\n/issues/2"},"status":"Merged","labels":["linux-owed"]},
  {"content":{"number":4,"body":"## Dependencies\n/issues/2"},"labels":["linux-owed"]},
  {"content":{"number":5,"body":"/issues/2"}},
  {"content":{"title":"Draft without a number"}}
]}
INPUT
<urn:harness:fixture:card:1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:harness:v0:WorkItem> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:number> "1"^^<http://www.w3.org/2001/XMLSchema#integer> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:title> "A \"title\" with \\" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:status> "Active" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:kind> "Task" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:phase> "Phase 1" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:workstream> "Server" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:taskId> "T1" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:sequence> "0.5"^^<http://www.w3.org/2001/XMLSchema#decimal> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:label> "tag" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:label> "other" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:assignee> "one" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:assignee> "two" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:dependsOn> <urn:harness:fixture:card:2> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:dependsOn> <urn:harness:fixture:card:3> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:dependsOn> <urn:harness:fixture:card:4> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:dependsOn> <urn:harness:fixture:card:99> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:state> "open" <urn:harness:fixture:derived:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:missingDependency> <urn:harness:fixture:card:99> <urn:harness:fixture:derived:board> .
<urn:harness:fixture:card:1> <urn:harness:v0:waitsFor> <urn:harness:fixture:card:2> <urn:harness:fixture:derived:board> .
<urn:harness:fixture:card:2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:harness:v0:WorkItem> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:2> <urn:harness:v0:number> "2"^^<http://www.w3.org/2001/XMLSchema#integer> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:2> <urn:harness:v0:title> "Fallback" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:2> <urn:harness:v0:state> "open" <urn:harness:fixture:derived:board> .
<urn:harness:fixture:card:3> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:harness:v0:WorkItem> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:3> <urn:harness:v0:number> "3"^^<http://www.w3.org/2001/XMLSchema#integer> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:3> <urn:harness:v0:title> "" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:3> <urn:harness:v0:status> "Merged" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:3> <urn:harness:v0:label> "linux-owed" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:3> <urn:harness:v0:dependsOn> <urn:harness:fixture:card:2> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:3> <urn:harness:v0:state> "merged" <urn:harness:fixture:derived:board> .
<urn:harness:fixture:card:4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:harness:v0:WorkItem> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:4> <urn:harness:v0:number> "4"^^<http://www.w3.org/2001/XMLSchema#integer> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:4> <urn:harness:v0:title> "" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:4> <urn:harness:v0:label> "linux-owed" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:4> <urn:harness:v0:dependsOn> <urn:harness:fixture:card:2> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:4> <urn:harness:v0:state> "linux-owed" <urn:harness:fixture:derived:board> .
<urn:harness:fixture:card:5> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:harness:v0:WorkItem> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:5> <urn:harness:v0:number> "5"^^<http://www.w3.org/2001/XMLSchema#integer> <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:5> <urn:harness:v0:title> "" <urn:harness:fixture:source:board> .
<urn:harness:fixture:card:5> <urn:harness:v0:state> "open" <urn:harness:fixture:derived:board> .
EXPECTED

jq -r --arg project fixture -f "$graph/board.jq" 2> "$work/warning" <<'INPUT' > "$work/board"
{"items":[{"content":{"number":31},"sequence":"bad"},{"content":{"number":32},"sequence":1e-7}]}
INPUT
awk '/<urn:harness:v0:sequence>/' "$work/board" | diff -u - 3<<'EXPECTED' /dev/fd/3
<urn:harness:fixture:card:32> <urn:harness:v0:sequence> "0.0000001"^^<http://www.w3.org/2001/XMLSchema#decimal> <urn:harness:fixture:source:board> .
EXPECTED
diff -u <(printf 'graph: card #31 has a nonnumeric Sequence\n') "$work/warning"

jq -r --arg project fixture -f "$graph/changes.jq" <<'INPUT' | diff -u /dev/fd/3 - 3<<'EXPECTED'
[
  {"number":15,"title":"A \"change\" with \\","mergedAt":"2026-11-20T09:00:00Z",
   "headRefName":"feat/card-007_work.12-done","mergeCommit":{"oid":"0123456789abcdef"},
   "files":[{"path":"apps/server/a b.rs"},{"path":"crates/editorial/b.rs"},{"path":"architecture/c.sh"},{"path":"Cargo.toml"}]},
  {"number":16,"title":"Unowned","mergedAt":"2026-11-20T10:00:00Z","headRefName":"fix/version2"}
]
INPUT
<urn:harness:fixture:change:15> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:harness:v0:Change> <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:change:15> <urn:harness:v0:number> "15"^^<http://www.w3.org/2001/XMLSchema#integer> <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:change:15> <urn:harness:v0:title> "A \"change\" with \\" <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:change:15> <urn:harness:v0:mergedAt> "2026-11-20T09:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:change:15> <urn:harness:v0:commit> "0123456789abcdef" <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:change:15> <urn:harness:v0:commitShort> "0123456789ab" <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:change:15> <urn:harness:v0:implements> <urn:harness:fixture:card:7> <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:change:15> <urn:harness:v0:touches> <urn:harness:fixture:file:apps%2Fserver%2Fa%20b.rs> <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:file:apps%2Fserver%2Fa%20b.rs> <urn:harness:v0:path> "apps/server/a b.rs" <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:file:apps%2Fserver%2Fa%20b.rs> <urn:harness:v0:lane> "server" <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:change:15> <urn:harness:v0:touches> <urn:harness:fixture:file:crates%2Feditorial%2Fb.rs> <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:file:crates%2Feditorial%2Fb.rs> <urn:harness:v0:path> "crates/editorial/b.rs" <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:file:crates%2Feditorial%2Fb.rs> <urn:harness:v0:lane> "studio" <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:change:15> <urn:harness:v0:touches> <urn:harness:fixture:file:architecture%2Fc.sh> <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:file:architecture%2Fc.sh> <urn:harness:v0:path> "architecture/c.sh" <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:file:architecture%2Fc.sh> <urn:harness:v0:lane> "harness" <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:change:15> <urn:harness:v0:touches> <urn:harness:fixture:file:Cargo.toml> <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:file:Cargo.toml> <urn:harness:v0:path> "Cargo.toml" <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:file:Cargo.toml> <urn:harness:v0:lane> "shared" <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:change:16> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:harness:v0:Change> <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:change:16> <urn:harness:v0:number> "16"^^<http://www.w3.org/2001/XMLSchema#integer> <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:change:16> <urn:harness:v0:title> "Unowned" <urn:harness:fixture:source:pulls> .
<urn:harness:fixture:change:16> <urn:harness:v0:mergedAt> "2026-11-20T10:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> <urn:harness:fixture:source:pulls> .
EXPECTED

printf '%s\n' $'# comment\tignored\t99' '' $'incomplete\trow' $'invalid\tcount\t-1' \
  $'primitive\tString\t5\tx.rs:7 folder/with space.rs:9 +2' \
  $'oracle-edit\tabc Fix tests\t17\tremoved=10 changed=0 authorized=0 added=7 folder/other%20file.rs' |
  jq -Rr --arg project fixture --arg source quality-facts --arg commit head -f "$graph/facts.jq" |
  diff -u /dev/fd/3 - 3<<'EXPECTED'
<urn:harness:fixture:fact:quality-facts:primitive:String> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:harness:v0:Fact> <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:primitive:String> <urn:harness:v0:signal> "primitive" <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:primitive:String> <urn:harness:v0:key> "String" <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:primitive:String> <urn:harness:v0:count> "5"^^<http://www.w3.org/2001/XMLSchema#integer> <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:primitive:String> <urn:harness:v0:atCommit> "head" <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:primitive:String> <urn:harness:v0:inFile> <urn:harness:fixture:file:x.rs> <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:primitive:String> <urn:harness:v0:place> "x.rs:7" <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:primitive:String> <urn:harness:v0:inFile> <urn:harness:fixture:file:folder%2Fwith%20space.rs> <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:primitive:String> <urn:harness:v0:place> "folder/with space.rs:9" <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:oracle-edit:abc%20Fix%20tests> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:harness:v0:Fact> <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:oracle-edit:abc%20Fix%20tests> <urn:harness:v0:signal> "oracle-edit" <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:oracle-edit:abc%20Fix%20tests> <urn:harness:v0:key> "abc Fix tests" <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:oracle-edit:abc%20Fix%20tests> <urn:harness:v0:count> "17"^^<http://www.w3.org/2001/XMLSchema#integer> <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:oracle-edit:abc%20Fix%20tests> <urn:harness:v0:atCommit> "head" <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:oracle-edit:abc%20Fix%20tests> <urn:harness:v0:commitShort> "abc" <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:oracle-edit:abc%20Fix%20tests> <urn:harness:v0:removed> "10"^^<http://www.w3.org/2001/XMLSchema#integer> <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:oracle-edit:abc%20Fix%20tests> <urn:harness:v0:added> "7"^^<http://www.w3.org/2001/XMLSchema#integer> <urn:harness:fixture:source:quality-facts> .
<urn:harness:fixture:fact:quality-facts:oracle-edit:abc%20Fix%20tests> <urn:harness:v0:inFile> <urn:harness:fixture:file:folder%2Fother%20file.rs> <urn:harness:fixture:source:quality-facts> .
EXPECTED

echo 'graph-converters-test: board, changes and facts pass'
