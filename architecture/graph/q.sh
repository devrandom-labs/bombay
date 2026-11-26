#!/usr/bin/env bash
# Usage: q.sh <data folder> <name> [arguments]
# Query the store built by build.sh. A temporary marker protects open readers.
#   loops                      open cards in a dependency loop
#   releases <card>            open cards that wait for the card, directly or through others
#   blockers [n]               open cards by how many open cards wait for them
#   ready <workstream>         selectable tasks in the run's order (all cards)
#   touched <path> <since>     merged changes by path prefix and ISO date and time
#   overlap <card>             other cards with merged changes that touch the same files
#   oracle [n]                 removed and added assertions by card
#   hot-design [n]             design signals by how many cards changed the file
#   sparql <file>              a SPARQL query, with raw TSV results
# Named queries print literal text. Tabs, carriage returns and newlines within
# values appear as \t, \r and \n so each row remains one line of TSV.
set -euo pipefail
export CDPATH=

usage() {
  echo 'Usage: q.sh <data folder> {loops|releases|blockers|ready|touched|overlap|oracle|hot-design|sparql} [arguments]' >&2
  exit 2
}
[ "$#" -ge 2 ] && [ -n "$1" ] || usage
data=$1
name=$2
shift 2
if ! command -v oxigraph > /dev/null; then
  echo 'graph: oxigraph is not on PATH (the Nix shell of the repository provides it)' >&2
  exit 3
fi

case "$name" in
  loops) [ "$#" = 0 ] || usage ;;
  releases|overlap) [ "$#" = 1 ] && [[ $1 =~ ^[1-9][0-9]*$ ]] || usage ;;
  blockers|oracle|hot-design)
    [ "$#" = 0 ] || { [ "$#" = 1 ] && [[ $1 =~ ^[0-9]+$ ]]; } || usage ;;
  ready) [ "$#" = 1 ] && [ -n "$1" ] || usage ;;
  touched)
    [ "$#" = 2 ] && [[ $2 =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})?$ ]] || usage ;;
  sparql) [ "$#" = 1 ] && [ -f "$1" ] && [ -r "$1" ] || usage ;;
  *) usage ;;
esac
no_store() { echo 'q: no store; run graph/build.sh' >&2; exit 1; }
store=$data/store
lock=$data/.build.lock
[ -d "$lock" ] || [ -d "$store" ] || no_store
reader=$data/.query.$$
deadline=$((SECONDS + 300))
publish_deadline=
builder_active() {
  local owner
  for owner in "$lock"/*; do
    if [ -d "$owner" ] && kill -0 "${owner##*/}" 2>/dev/null; then return 0; fi
  done
  return 1
}
while :; do
  mkdir -- "$reader"
  publishing=false
  for marker in "$data"/.publish.*; do
    if [ -f "$marker" ] && kill -0 "${marker##*.}" 2>/dev/null; then publishing=true; fi
  done
  if ! $publishing && [ -d "$store" ]; then break; fi
  rmdir -- "$reader"
  if $publishing; then
    publish_deadline=${publish_deadline:-$((SECONDS + 30))}
    if (( SECONDS >= publish_deadline )); then
      echo 'graph: a build is publishing; try again' >&2
      exit 4
    fi
  else
    publish_deadline=
    # A first build can take longer than the wait for an abandoned build lock.
    if { [ ! -d "$lock" ] || { (( SECONDS >= deadline )) && ! builder_active; }; } &&
        [ ! -d "$store" ]; then no_store; fi
  fi
  sleep 0.1
done
trap 'rmdir -- "$reader"' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

here=$(cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=architecture/project.env
source "$here/../project.env"
project=${HARNESS_REPO##*/}
prefixes='PREFIX h: <urn:harness:v0:>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>'
query=(query --location "$store" --union-default-graph --results-format tsv)
card() { printf '<urn:harness:%s:card:%s>' "$project" "$1"; }
literal() { jq -nr --arg value "$1" '$value | tojson'; }
run() {
  oxigraph query --location "$store" --union-default-graph --results-format srj --query "$prefixes
$1" | jq -r --arg project "$project" '
    def urn($kind): "urn:harness:\($project):\($kind):";
    def cell:
      if .type == "uri" then
        .value | if startswith(urn("card")) then "#" + ltrimstr(urn("card"))
        elif startswith(urn("change")) then "PR " + ltrimstr(urn("change"))
        else "<" + . + ">" end
      else .value // "" | gsub("\t"; "\\t") | gsub("\r"; "\\r") | gsub("\n"; "\\n") end;
    .head.vars as $columns
    | ($columns | map("?" + .) | join("\t")),
      (.results.bindings[] | . as $row | [$columns[] | $row[.] | cell] | join("\t"))'
}

case "$name" in
  loops)
    run 'SELECT DISTINCT ?card WHERE { ?card h:waitsFor+ ?card } ORDER BY ?card' ;;
  releases)
    run "SELECT ?card ?workstream ?title WHERE {
      ?card h:waitsFor+ $(card "$1") ; h:title ?title .
      OPTIONAL { ?card h:workstream ?workstream }
    } ORDER BY ?card" ;;
  blockers)
    run "SELECT ?card (COUNT(DISTINCT ?d) AS ?waiting) (SAMPLE(?t) AS ?title) WHERE {
      ?d h:waitsFor+ ?card . ?card h:title ?t
      FILTER(?d != ?card)
    } GROUP BY ?card ORDER BY DESC(?waiting) ?card LIMIT ${1:-10}" ;;
  ready)
    # Read the workstreams and phase order from the run's selector.
    selection=$(sed -n '/^def streams: /p; /^def phases: /p' "$here/../backlog.jq")
    phases=$(jq -ner --arg stream "$1" "$selection"'
      if (streams | [.[][]] | index($stream)) == null then ""
      else phases | to_entries | map("(\(.value | tojson) \(.key))") | join(" ") end')
    run "SELECT ?card ?phase ?sequence ?title WHERE {
      VALUES (?phase ?rank) { $phases }
      ?card h:state ?state ; h:workstream $(literal "$1") ; h:kind \"Task\" ;
        h:title ?title ; h:number ?number ; h:phase ?phase ; h:assignee $(literal "$HARNESS_ASSIGNEE") .
      FILTER(?state != \"merged\")
      OPTIONAL { ?card h:sequence ?sequence }
      FILTER NOT EXISTS { ?card h:dependsOn ?x . ?x h:state \"open\" }
      FILTER NOT EXISTS { ?card h:missingDependency ?x }
      FILTER NOT EXISTS { ?card h:label \"needs-joel\" }
    } ORDER BY ?rank (!BOUND(?sequence)) ?sequence ?number" ;;
  touched)
    run "SELECT ?change ?mergedAt ?title (COUNT(?f) AS ?files) WHERE {
      ?change h:touches ?f ; h:mergedAt ?mergedAt ; h:title ?title . ?f h:path ?p .
      FILTER(STRSTARTS(?p, $(literal "$1"))) FILTER(?mergedAt >= \"$2\"^^xsd:dateTime)
    } GROUP BY ?change ?mergedAt ?title ORDER BY ?mergedAt ?change" ;;
  overlap)
    run "SELECT ?other (COUNT(DISTINCT ?f) AS ?sharedFiles) (SAMPLE(?t) AS ?title) WHERE {
      ?c1 h:implements $(card "$1") ; h:touches ?f . ?c2 h:touches ?f ; h:implements ?other .
      OPTIONAL { ?other h:title ?boardTitle }
      BIND(COALESCE(?boardTitle, \"(not on the board)\") AS ?t)
      FILTER(?other != $(card "$1"))
    } GROUP BY ?other ORDER BY DESC(?sharedFiles) ?other LIMIT 12" ;;
  oracle)
    run "SELECT ?card (COUNT(DISTINCT ?c) AS ?changes) (SUM(?r) AS ?removed) (SUM(?a) AS ?added) (SAMPLE(?t) AS ?title) WHERE {
      ?f h:signal \"oracle-edit\" ; h:commitShort ?id ; h:removed ?r ; h:added ?a .
      ?c h:commitShort ?id ; h:implements ?card .
      OPTIONAL { ?card h:title ?boardTitle }
      BIND(COALESCE(?boardTitle, \"(not on the board)\") AS ?t)
    } GROUP BY ?card ORDER BY DESC(?removed) ?card LIMIT ${1:-8}" ;;
  hot-design)
    run "SELECT ?path (COUNT(DISTINCT ?card) AS ?cards) (COUNT(DISTINCT ?c) AS ?changes)
      (GROUP_CONCAT(DISTINCT ?sig; separator=\",\") AS ?signals) WHERE {
      ?f h:signal ?sig ; h:inFile ?file . FILTER(?sig IN (\"primitive\",\"condition\",\"mirror-enum\",\"state-literal\",\"same-name\"))
      ?file h:path ?path . ?c h:touches ?file ; h:implements ?card
    } GROUP BY ?path ORDER BY DESC(?cards) ?path LIMIT ${1:-8}" ;;
  sparql)
    oxigraph "${query[@]}" --query-file "$1" ;;
esac
