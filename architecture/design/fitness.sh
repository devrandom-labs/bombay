#!/usr/bin/env bash
# Book-grounded AST fitness functions (#1300) for a git revision across crates/.
# Usage: bash architecture/design/fitness.sh <rev> [scope...]
# Outputs sorted `<signal>\t<count>` lines for non-test Rust production files.
set -euo pipefail
export LC_ALL=C CDPATH=

rev=${1:?usage: fitness.sh <rev> [scope...]}
shift
scopes=("$@")
if (( ${#scopes[@]} == 0 )); then scopes=(crates); fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
export GIT_INDEX_FILE="$work/index"
git read-tree "$rev"
mkdir "$work/tree"

excluded='(^|/)(tests|fuzz|benches|_entities)/'
rust_test='(^|/)(test|tests)[.]rs$'
git --literal-pathspecs ls-files --stage -z -- "${scopes[@]}" |
  awk -v RS='\0' -v ORS='\0' -v excluded="$excluded" -v rust_test="$rust_test" '
    /^100[0-7]{3} / {
      path = substr($0, index($0, "\t") + 1)
      if (path ~ /\.rs$/ && path !~ excluded && path !~ rust_test) print path
    }
  ' > "$work/files"

if [ ! -s "$work/files" ]; then exit 0; fi
git -c core.autocrlf=false checkout-index --prefix="$work/tree/" --stdin -z < "$work/files"

# Strip #[cfg(test)] mod bodies so test-only helpers inside src/ do not skew production counts.
if command -v ast-grep >/dev/null 2>&1; then
  cat > "$work/strip.yml" <<'YAML'
id: test-module
language: Rust
rule:
  kind: declaration_list
  pattern: $BODY
  inside:
    kind: mod_item
    field: body
    follows:
      kind: attribute_item
      pattern: {context: '#[cfg(test)] mod m {}', selector: attribute_item}
      stopBy:
        not: {any: [{kind: attribute_item}, {kind: line_comment}, {kind: block_comment}]}
fix: '{}'
YAML
  ast-grep scan --rule "$work/strip.yml" --update-all "$work/tree" >/dev/null 2>&1 || true
fi

files=()
while IFS= read -r -d '' f; do files+=("$work/tree/$f"); done < "$work/files"

count_re() {
  local pat=$1 n=0
  n=$(grep -Eh -- "$pat" "${files[@]}" 2>/dev/null | wc -l | tr -d ' ') || true
  printf '%s\n' "${n:-0}"
}

printf 'discarded-error-source\t%s\n' "$(count_re 'map_err\(\|[[:space:]]*_[[:space:]]*\|')"
printf 'seqcst-ordering\t%s\n' "$(count_re 'Ordering::SeqCst')"
printf 'slice-aversion\t%s\n' "$(count_re '&(mut[[:space:]]+)?(Vec<|String\b|Box<)')"
printf 'deref-polymorphism\t%s\n' "$(count_re 'impl(<[^>]*>)?[[:space:]]+(std::ops::|core::ops::)?Deref[[:space:]]+for\b')"
printf 'temp-lock-guard\t%s\n' "$(count_re '(if[[:space:]]+let|while[[:space:]]+let|match)[[:space:]].*\.lock\(')"
printf 'unbounded-channel\t%s\n' "$(count_re 'mpsc::unbounded_channel\(')"
printf 'inline-use\t%s\n' "$(count_re '^[[:space:]]{4,}use [a-zA-Z0-9_:]+::')"

# Ball-of-mud error enums (>= 6 #[from] attributes inside a single file).
bom=$(awk '
  FNR == 1 { if (c >= 6) total++; c = 0 }
  /#\[from\]/ { c++ }
  END { if (c >= 6) total++; print total + 0 }
' "${files[@]}")
printf 'ball-of-mud-error\t%s\n' "$bom"
