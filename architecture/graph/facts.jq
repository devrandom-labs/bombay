# Output of architecture/design/facts.sh or architecture/quality/facts.sh (tab-separated:
# signal, key, count, places) -> N-Quads for the harness graph, version 0.
# Use: jq -R -r --arg project labcast-server --arg source design-facts --arg commit <id> \
#        -f facts.jq facts.tsv > facts.nq
# A row becomes a fact node with its signal, key and count. A place `path:line` links the
# fact to the file node that the pull requests also use, so a query can join "what the
# tools found" with "which work touched it". A row of `oracle-edit` starts with a short
# commit id; it gets that id and its removed and added counts, so it joins the change.
def xsd($t): "^^<http://www.w3.org/2001/XMLSchema#\($t)>";
def v($n): "<urn:harness:v0:\($n)>";
def file($p): "<urn:harness:\($project):file:\($p | @uri)>";
def src: "<urn:harness:\($project):source:\($source)>";
def lit: tojson;
def path:
  if $source == "quality-facts" then
    gsub("(?<escape>%25|%20|%09|%0D|%0A)";
      {"%25":"%","%20":" ","%09":"\t","%0D":"\r","%0A":"\n"}[.escape])
  else . end;

select(startswith("#") | not)
| split("\t") as $r
| select($r[2] // "" | test("^[0-9]+$"))
| $r[0] as $signal | $r[1] as $key | ($r[3] // "") as $places
| "<urn:harness:\($project):fact:\($source):\($signal):\($key | @uri)>" as $s
| ( "\($s) <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> \(v("Fact")) \(src) .",
    "\($s) \(v("signal")) \($signal | lit) \(src) .",
    "\($s) \(v("key")) \($key | lit) \(src) .",
    "\($s) \(v("count")) \"\($r[2])\"\(xsd("integer")) \(src) .",
    "\($s) \(v("atCommit")) \($commit | lit) \(src) .",
    ( $places | scan("(?:^| )([^ ].*?):([0-9]+)(?= |$)")
      | (.[0] | path) as $path
      | "\($s) \(v("inFile")) \(file($path)) \(src) .",
        "\($s) \(v("place")) \("\($path):\(.[1])" | lit) \(src) ." ),
    ( select($signal == "oracle-edit")
      | ($key | split(" ")[0]) as $id
      | "\($s) \(v("commitShort")) \($id | lit) \(src) .",
        ( $places | split(" ")[] | select(test("^(removed|added)=[0-9]+$")) | split("=") as $kv
          | "\($s) \(v($kv[0])) \"\($kv[1])\"\(xsd("integer")) \(src) ." ),
        ( $places | sub("^(?:(?:removed|changed|authorized|added)=[0-9]+ +)*"; "")
          | scan("(?:^| )([^ ].*?\\.rs)(?= |$)") | .[0] | path
          | "\($s) \(v("inFile")) \(file(.)) \(src) ." ) ) )
