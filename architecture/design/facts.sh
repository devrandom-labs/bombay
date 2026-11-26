#!/usr/bin/env bash
# Facts for the discovery review of the design-review skill.
#
# What an absent line means and the known limits
# Only tracked Rust and SQL files in HEAD count. The working tree is ignored.
# Paths under tests/, fuzz/, benches/ and _entities/, Rust files named test.rs
# or tests.rs, and Rust module bodies with #[cfg(test)] are left out.
# A key can be absent when it is below the threshold for its signal.
# primitive (5): named function parameters and named struct fields of the
# listed numeric, bool, String, &str, Uuid, Instant and Duration types, or
# Option of one of them. Types use the last path segment.
# condition (3): Column::Name calls to eq, ne, is_null, is_not_null, is_in,
# is_not_in, lt, lte, gt and gte, and .len() compared with a number or tested
# with a literal range's .contains(&value.len()).
# mirror-enum (2 files): identical sets of at least two named enum variants.
# same-name (2 files): struct, enum or trait declarations with plain pub.
# state-literal (1): variables, fields and Column variants ending in state or
# status, with optional dereference or .as_str(). Looks for ==, !=, <, <=, >,
# >=, the methods eq, ne, lt, le, lte, gt, ge and gte, and string patterns
# (including alternatives) in match arms. Guard and body literals only count
# when used in a comparison.
# sql-rule (0): named CHECK constraints, CREATE [OR REPLACE] FUNCTION and
# CREATE [OR REPLACE | CONSTRAINT] TRIGGER outside SQL comments and strings.
# Counts files with the rule name as a word, including Rust comments and strings.
# unused-module (0): top-level pub mod name; in a library scope with no
# external Rust path, grouped import or glob under apps/ or crates/. The
# scan uses source syntax and does not resolve aliases or macro expansions.
# churn (10): file changes in the last 400 commits, including both paths of a
# rename, after the tree facts. A shallow clone can give different counts.
set -euo pipefail
export LC_ALL=C CDPATH=

here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
root=$(git rev-parse --show-toplevel)
cd -P -- "$root"
root=$PWD

if (( $# == 0 )); then
  echo 'Usage: architecture/design/facts.sh <scope>...' >&2
  exit 2
fi

scopes=()
for scope in "$@"; do
  if ! dir=$(cd -P -- "$scope" 2>/dev/null && pwd); then
    printf 'facts: scope is not a directory: %s\n' "$scope" >&2
    exit 2
  fi
  case "$dir" in
    "$root") scopes+=(".") ;;
    "$root"/*) scopes+=("${dir#"$root"/}") ;;
    *) printf 'facts: scope is outside the repository: %s\n' "$scope" >&2; exit 2 ;;
  esac
done

# Read committed files through a temporary index. Local edits and staged files
# cannot change the report for this HEAD.
head=$(git rev-parse --verify HEAD)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
export GIT_INDEX_FILE="$work/index"
git read-tree "$head"
mkdir "$work/tree"

excluded='(^|/)(tests|fuzz|benches|_entities)/'
rust_test='(^|/)(test|tests)[.]rs$'
git --literal-pathspecs ls-files --stage -z -- "${scopes[@]}" |
  awk -v RS='\0' -v ORS='\0' -v excluded="$excluded" -v rust_test="$rust_test" '
    /^100[0-7]{3} / {
      path = substr($0, index($0, "\t") + 1)
      if (path ~ /\.(rs|sql)$/ && path !~ excluded && path !~ rust_test) print path
    }
  ' > "$work/files"
git -c core.autocrlf=false checkout-index --prefix="$work/tree/" --stdin -z < "$work/files"

rust=()
sql=()
while IFS= read -r -d '' file; do
  case "$file" in
    *.rs) rust+=("$file") ;;
    *.sql) sql+=("./$file") ;;
  esac
done < "$work/files"

# A manifest can give the library a name different from the directory.
crate_name() {
  local crate=$1 manifest name
  manifest="${crate#./}/Cargo.toml"
  if git cat-file -e "$head:$manifest" 2>/dev/null; then
    name=$(git show "$head:$manifest" | awk '
      /^[[:space:]]*\[/ {
        section = $0
        sub(/^[[:space:]]*\[/, "", section)
        sub(/\].*$/, "", section)
      }
      /^[[:space:]]*name[[:space:]]*=/ && (section == "package" || section == "lib") {
        if (match($0, /["\047][[:alnum:]_-]+["\047]/))
          names[section] = substr($0, RSTART + 1, RLENGTH - 2)
      }
      END { print names["lib"] ? names["lib"] : names["package"] }
    ')
  fi
  if [[ "$crate" == . ]]; then crate=$root; fi
  name=${name:-${crate##*/}}
  printf '%s\n' "${name//-/_}"
}

scan() {
  (cd "$work/tree" && ast-grep scan --config "$work/sgconfig.yml" --no-ignore hidden "$@")
}

scan_rust() {
  local crate file status=0
  local external=() sources=()
  : > "$work/crates"
  for crate in "${scopes[@]}"; do
    if [[ -f "$work/tree/$crate/src/lib.rs" ]]; then
      crate_name "$crate" >> "$work/crates"
    fi
  done
  : > "$work/imports"
  : > "$work/rust"
  if [[ -s "$work/crates" ]]; then
    # Preselect files by crate name, then use Rust syntax to check their paths.
    (git grep --cached --no-color --no-textconv -IlzFw -f "$work/crates" -- \
      ':(glob)apps/**/*.rs' ':(glob)crates/**/*.rs' || {
      status=$?
      if (( status != 1 )); then exit "$status"; fi
    }) | awk -v RS='\0' -v ORS='\0' -v excluded="$excluded" -v rust_test="$rust_test" '
      $0 !~ excluded && $0 !~ rust_test
    ' > "$work/external"
    git -c core.autocrlf=false checkout-index --force --prefix="$work/tree/" --stdin -z < "$work/external"
    while IFS= read -r -d '' file; do external+=("$file"); done < "$work/external"
  fi
  jq -n --arg dir "$here" '{ruleDirs: [$dir]}' > "$work/sgconfig.yml"
  if (( ${#rust[@]} )); then
    while IFS= read -r -d '' file; do sources+=("$file"); done < <(
      printf '%s\0' "${rust[@]}" ${external[@]+"${external[@]}"} | sort -zu
    )
    # Mask test bodies in the temporary copy while keeping line numbers.
    if ! scan --filter '^test-module$' --update-all -- "${sources[@]}" > /dev/null 2>"$work/rewrite"; then
      cat "$work/rewrite" >&2
      return 1
    fi
    scan --off=test-module,module-path,module-group --json=stream -- "${rust[@]}" |
      jq -rs --arg mode facts -f "$here/rust.jq" > "$work/rust"
  fi
  if (( ${#external[@]} )); then
    scan --filter '^module-(path|group)$' --json=stream -- "${external[@]}" |
      jq -rs --arg mode imports -f "$here/rust.jq" > "$work/imports"
  fi
}

scan_rust

modules() {
  local crate lib name prefix
  for crate in "${scopes[@]}"; do
    lib="$crate/src/lib.rs"
    lib=${lib#./}
    [[ -f "$work/tree/$lib" ]] || continue
    name=$(crate_name "$crate")
    prefix="$crate/"
    prefix=${prefix#./}
    awk -F '\t' -v lib="$lib" -v name="$name" -v prefix="$prefix" '
      FILENAME == ARGV[1] {
        if ($1 == name && prefix != "" && index($3, prefix) != 1) used[$2]
        next
      }
      $1 == "module" && $3 == lib {
        module = $2
        sub(/^r#/, "", module)
        if (!("*" in used) && !(module in used))
          printf "unused-module\t%s::%s\t%s\t%s\n", name, $2, $3, $4
      }
    ' "$work/imports" "$work/rust"
  done | sort -u
}

facts() {
  awk -F '\t' '$1 != "module"' "$work/rust"
  if (( ${#sql[@]} )); then
    (cd "$work/tree" && awk -f "$here/sql.awk" -- "${sql[@]}")
  fi
  modules
}

# Group sorted locations. Track file count separately for cross-file signals.
facts | sort -t $'\t' -k1,1 -k2,2 -k3,3 -k4,4n -k5,5 | awk -F '\t' '
  BEGIN { OFS = "\t" }
  function report() {
    if (!count || (signal == "primitive" && count < 5) ||
        (signal == "condition" && count < 3) ||
        ((signal == "mirror-enum" || signal == "same-name") && files < 2)) return
    if (count > 12) places = places " +" (count - 12)
    print signal, key, count, places
  }
  $1 != signal || $2 != key {
    report()
    signal = $1; key = $2; count = files = 0; file = places = ""
  }
  {
    if ($3 != file) { files++; file = $3 }
    if (++count <= 12) {
      place = $3 ":" $4
      if ($5 != "") place = place " " $5
      places = places (count > 1 ? " " : "") place
    }
  }
  END { report() }
' > "$work/groups"

while IFS=$'\t' read -r signal key count places; do
  case "$signal" in
    sql-rule)
      count=0
      if (( ${#rust[@]} )); then
        count=$(
          (cd "$work/tree"
           status=0
           git --literal-pathspecs grep --no-index --no-color -IlzFw -e "$key" -- "${rust[@]}" || status=$?
           if (( status > 1 )); then exit "$status"; fi) |
            awk -v RS='\0' 'END { print NR }'
        )
      fi
      ;;
    unused-module) count=0 ;;
  esac
  printf '%s\t%s\t%s\t%s\n' "$signal" "$key" "$count" "$places"
done < "$work/groups" |
  awk -F '\t' '{ printf "%d\t%s\n", $1 == "sql-rule" ? -$3 : $3, $0 }' |
  sort -t $'\t' -k2,2 -k1,1nr -k3,3 |
  cut -f2-

printf '# history\n'
# Limit the repository history before filtering to the requested file paths.
git log "$head" -n 400 --format='%x00' --name-only -z --no-renames --no-notes \
  --no-show-signature --no-color --no-relative |
  awk -v RS='\0' '
    FILENAME == ARGV[1] { files[$0]; next }
    { sub(/^\n/, "") }
    $0 in files { count[$0]++ }
    END {
      for (path in count)
        if (count[path] >= 10) printf "churn\t%s\t%d\t%s:1\n", path, count[path], path
    }
  ' "$work/files" - |
  sort -t $'\t' -k3,3nr -k2,2
