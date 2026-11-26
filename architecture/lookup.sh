#!/usr/bin/env bash
# Read code and gate rules from the local origin/main ref.
# Common words such as "new" can give many rows with like.
set -euo pipefail
export CDPATH='' LC_ALL=C

usage() {
  echo 'Usage: lookup.sh {exists|like} <name> | rules <repository-relative .rs file>' >&2
  exit 2
}
[ "$#" = 2 ] && [ -n "$2" ] || usage
lookup=$1 target=$2
case "$lookup" in
  exists|like) paths=() ;;
  rules)
    target=${target#./}
    [[ $target == *.rs ]] || usage
    case "/$target/" in *//*|*/../*|*/./*) usage ;; esac
    paths=(architecture/sgconfig.yml architecture/rules) ;;
  *) usage ;;
esac

root=$(cd -- "$(dirname -- "$0")/.." && pwd)
if ! commit=$(git -C "$root" rev-parse --verify -q 'refs/remotes/origin/main^{commit}'); then
  echo 'lookup: origin/main is missing; fetch it' >&2
  exit 1
fi
if [ "$lookup" = rules ] &&
    [ "$(git -C "$root" cat-file -t "$commit:$target" 2>/dev/null || true)" = tree ]; then
  usage
fi
time=$(git -C "$root" log -1 --format='%cI' "$commit")
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
git -C "$root" archive "$commit" -- "${paths[@]}" | tar -x -C "$work"
cd -- "$work"
printf 'main %s %s\n' "$commit" "$time"
ignore=(--no-ignore hidden --no-ignore dot --no-ignore vcs --no-ignore global --no-ignore parent)

symbols() {
  local kind node
  while read -r kind node; do
    cat <<RULE
---
id: $kind
language: Rust
severity: hint
rule:
  kind: $node
  has:
    field: name
    pattern: \$NAME
RULE
  done <<'KINDS'
struct struct_item
enum enum_item
trait trait_item
type type_item
fn function_item
fn-signature function_signature_item
mod mod_item
KINDS
}

if [ "$lookup" != rules ]; then
  ast-grep scan --inline-rules="$(symbols)" --json=stream "${ignore[@]}" |
    jq -rs --arg lookup "$lookup" --arg target "${target#r#}" '
      def words:
        gsub("(?<capital>[A-Z])"; " \(.capital)") | gsub("_"; " ")
        | ascii_downcase | split(" ") | map(select(length > 0));
      ($target | words) as $wanted
      | sort_by(.file, .range.start.line, .range.start.column)[]
      | (.metaVariables.single.NAME.text | ltrimstr("r#")) as $name
      | select(if $lookup == "exists" then $name == $target
               else any($name | words[]; . as $word | $wanted | index($word) != null) end)
      | [(.ruleId | sub("-signature$"; "")),
         "\(.file | ltrimstr("./")):\(.range.start.line + 1)", (.text | split("\n")[0])]
      | @tsv'
  exit
fi

# The gate rules are Rust rules. Match the root of one synthetic Rust file
# so ast-grep itself decides which files/ignores globs apply, even to new files.
# shellcheck disable=SC2016 # ast-grep expands the pattern variables.
rewrite='
id: match-probe
language: Yaml
severity: hint
rule:
  pattern: {context: "rule: $VALUE", selector: block_mapping_pair}
  inside:
    kind: block_mapping
    inside:
      kind: block_node
      inside: {kind: document}
fix: |
  rule: {kind: source_file}
---
id: drop-match-options
language: Yaml
severity: hint
rule:
  pattern: {context: "$KEY: $VALUE", selector: block_mapping_pair}
  inside:
    kind: block_mapping
    inside:
      kind: block_node
      inside: {kind: document}
constraints:
  KEY: {regex: "^(constraints|transform|fix)$"}
fix: ""'
if ! ast-grep scan --inline-rules "$rewrite" --update-all "${ignore[@]}" architecture/rules > "$work/rewrite.log" 2>&1; then
  cat "$work/rewrite.log" >&2
  exit 1
fi
mkdir -p -- "$(dirname -- "$target")"
printf 'fn probe() {}\n' > "$target"
ast-grep scan --config architecture/sgconfig.yml --hint --json=stream "${ignore[@]}" -- "./$target" |
  jq -rs 'sort_by(.ruleId)[] | [.ruleId, (.message | split("\n")[0])] | @tsv'
