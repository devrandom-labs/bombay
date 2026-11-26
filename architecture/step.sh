#!/usr/bin/env bash
# Step gate: keep rule changes separate from code, stay in one lane, check
# changed signatures and test unreachable!() counts, enforce the AST fitness
# non-regression ratchet, and require reasons for large additions and removed
# or changed test assertions.
set -euo pipefail

base=${STEP_BASE:-origin/main}
note=150
here=$(cd -- "$(dirname -- "$0")" && pwd -P)

if ! git rev-parse --verify --quiet "$base^{commit}" > /dev/null; then
  echo "step: base $base not found; skipped"
  exit 0
fi

# -M counts only the edited lines of a moved file. A binary file counts 0.
count() { git diff --numstat -M "$base...HEAD" -- "$@" | awk -v column="$column" '
  { total += $1 * (column != "deleted") + $2 * (column != "added") } END { print total + 0 }'; }
column=both changed=$(count ':(glob)**/*.rs')
column=added added=$(count ':(glob)**/src/**/*.rs')

lanes=$(git diff --name-only "$base...HEAD" | awk -F/ '
  $1 == "apps" && ($2 == "server" || $2 == "studio") && !seen[$2]++ { count++ }
  $1 == "crates" && ($2 ~ /^bombay-(observe|domain-)/) && !seen["domain"]++ { count++ }
  $1 == "crates" && ($2 ~ /^bombay-(zenoh|axum)$/) && !seen["adapters"]++ { count++ }
  $1 == "crates" && ($2 ~ /^bombay-(runtime|engine|macros)$/) && !seen["runtime"]++ { count++ }
  ($1 == "examples" && $2 == "remote-counter") && !seen["adapters"]++ { count++ }
  ($1 == "crates" && $2 == "bombay" && $3 == "src" && $4 == "remote") && !seen["adapters"]++ { count++ }
  ($1 == "crates" && $2 == "bombay" && $3 == "src" && ($4 == "local" || $4 == "entity" || $4 == "actors")) && !seen["runtime"]++ { count++ }
  END { print count + 0 }')

# Rule files and code change in separate steps, so no feature step rewrites the rules.
mixed=$(git diff --name-only "$base...HEAD" | awk '
  /^(AGENTS\.md|CLAUDE\.md|\.agents\/|\.claude\/|\.githooks\/|architecture\/)/ { rules = 1 }
  /^(apps|crates)\// { code = 1 } END { print (rules && code) + 0 }')
trailers=$(git log --format=%B "$base..HEAD")
failed=0

if [ "$mixed" -eq 1 ] && ! grep -qE '^Rules: .+' <<< "$trailers"; then
  echo "error[rules-alone]: this branch changes rule files and code together."
  echo "note: Source: AGENTS.md, Workflow, Lanes: change a shared file in a separate step."
  echo "      Rule files: AGENTS.md, CLAUDE.md, .agents/, .claude/, .githooks/, architecture/."
  echo "      Fix: move the rule change to its own step and merge it first."
  echo "      If a rule and its code must change together, add a commit trailer: Rules: <reason>"
  failed=1
fi

if [ "$lanes" -gt 1 ] && ! grep -qE '^Lanes: .+' <<< "$trailers"; then
  echo "error[one-lane]: this branch changes multiple lanes; a step stays in one lane."
  echo "note: Source: AGENTS.md, Workflow, Lanes."
  echo "      Fix: make the other lane's change a card for that lane."
  echo "      If one shared change needs multiple lanes, add a commit trailer: Lanes: <reason>"
  failed=1
fi

if [ "$added" -gt "$note" ] && ! grep -qE '^Reuse: .+' <<< "$trailers"; then
  echo "error[reuse-note]: this branch adds $added production lines without a reuse note."
  echo "note: Source: AGENTS.md, Workflow, Steps."
  echo "      Fix: search the standard library, the accepted stack and maintained crates."
  echo "      Then add a commit trailer: Reuse: <crate or tool used, or the gap that none fills>"
  failed=1
fi

hook_source() {
  # The step rules are mandatory, including on lines with inline suppressions.
  git cat-file blob "$1" | sed 's/ast-grep-ignore/ast-grep-review/g'
}

test_path() {
  [[ $1 =~ ^(apps|crates)/(.*/)?tests/.*[.]rs$ ]]
}

production_path() {
  [[ $1 =~ ^(apps|crates)/(.*/)?src/.*[.]rs$ ]] &&
    [[ ! $1 =~ (^|/)(tests|fuzz|benches|_entities)/ && ! $1 =~ (^|/)(test|tests)[.]rs$ ]]
}

unreachable_count() {
  hook_source "$1" |
    ast-grep scan --stdin --rule "$here/step-rules/unreachable.yml" --json=stream |
    awk 'END { print NR + 0 }'
}

added_ranges() {
  # Compare the blobs paired by rename detection. A new file is all new lines.
  if [[ $1 != 100* ]]; then
    git cat-file blob "$3" | awk 'END { if (NR) print 1, NR }'
    return
  fi
  git diff --no-color --no-ext-diff --no-textconv -U0 --inter-hunk-context=0 "$2" "$3" |
    awk '/^@@ -[0-9]/ {
      fields = split(substr($3, 2), range, ",")
      count = fields == 1 ? 1 : range[2]
      if (count > 0) print range[1], range[1] + count - 1
    }'
}

signature_findings() {
  local file=$1 ranges=$2
  if ! ast-grep scan --config "$work/mask.yml" --filter '^test-module$' --update-all -- \
    "$work/source.rs" > /dev/null 2>"$work/mask.log"; then
    cat "$work/mask.log" >&2
    return 1
  fi
  ast-grep scan --rule "$here/step-rules/signatures.yml" --json=stream -- "$work/source.rs" |
    jq -rs --arg file "$file" --arg ranges "$ranges" --argjson callbacks 3 \
      -f "$here/step-rules/findings.jq"
}

design_findings() {
  # Read committed blobs and both paths of a rename. Deleted test files still
  # contribute to the base count. A move within tests keeps the same count.
  git diff --raw --no-abbrev --no-relative -z -M "$base...HEAD" | (
    set -e
    work=
    trap 'if [[ -n $work ]]; then rm -rf -- "$work"; fi' EXIT
    base_count=0 head_count=0
    while IFS= read -rd '' entry && IFS= read -rd '' old_path; do
      read -r old_mode new_mode old_blob new_blob status <<< "${entry#:}"
      path=$old_path
      if [[ $status == R* ]]; then IFS= read -rd '' path; fi
      if [[ $old_blob == "$new_blob" && $old_path == "$path" ]]; then continue; fi
      if [[ $old_mode == 100* ]] && test_path "$old_path"; then
        count=$(unreachable_count "$old_blob")
        base_count=$((base_count + count))
      fi
      if [[ $new_mode == 100* ]] && test_path "$path"; then
        count=$(unreachable_count "$new_blob")
        head_count=$((head_count + count))
      fi
      if [[ $new_mode == 100* && $old_blob != "$new_blob" ]] && production_path "$path"; then
        ranges=$(added_ranges "$old_mode" "$old_blob" "$new_blob")
        [[ -n $ranges ]] || continue
        if [[ -z $work ]]; then
          work=$(mktemp -d)
          jq -n --arg dir "$here/design" '{ruleDirs: [$dir]}' > "$work/mask.yml"
        fi
        hook_source "$new_blob" > "$work/source.rs"
        signature_findings "$path" "$ranges"
      fi
    done
    if (( head_count > base_count )); then
      printf 'error[test-unreachable]: test unreachable!() calls rose from %s to %s in changed test files\n' "$base_count" "$head_count"
      echo 'note: See bombay.md, Tests. Use a stateful fake or assert the expected variant.'
    fi
  )
}

design=$(design_findings)
if [[ -n $design ]]; then
  printf '%s\n' "$design"
  failed=1
fi

oracle=$(bash "$here/quality/facts.sh" --oracle "$base" HEAD)
IFS=$'\t' read -r removed changed_assertions _ _ files <<< "${oracle%$'\t'*}"
unseen=${oracle##*$'\t'}
if [ -n "$unseen" ]; then
  echo "note: $unseen hold proptest or macro bodies that this check does not read"
fi
oracle_edits=$((removed + changed_assertions))
if [ "$oracle_edits" -gt 0 ]; then
  reasons=$(git log --format='%(trailers:key=Oracle,valueonly,unfold)' "$base..HEAD")
  reason=$(awk '/[^[:space:]]/ { print; exit }' <<< "$reasons")
  if [ -z "$reason" ]; then
    echo "error[oracle-stated]: test assertions in this branch: removed=$removed changed=$changed_assertions: $files"
    echo "note: Source: AGENTS.md, Roles and Rules about rules: a test oracle is not weakened to accept a change."
    echo "      Fix: keep the assertions. If they moved, or the expectation is wrong, say so in a commit trailer:"
    echo "      Oracle: moved to <file>   or   Oracle: <why the old expectation was wrong>"
    echo "      A change of what a test expects is Joel's decision: put it on the card under needs-joel first."
    failed=1
  fi
fi

# Book-grounded AST fitness non-regression ratchet (#1338) when crates/ production Rust changed.
if [ -f "$here/design/fitness.sh" ] && git diff --name-only "$base...HEAD" -- ':(glob)crates/**/src/**/*.rs' | grep -q .; then
  base_fit=$(bash "$here/design/fitness.sh" "$base" crates)
  head_fit=$(bash "$here/design/fitness.sh" HEAD crates)
  regressions=$(awk -F '\t' '
    NR == FNR { before[$1] = $2 + 0; next }
    ($2 + 0) > (before[$1] + 0) { printf "  %s: %d -> %d (+%d)\n", $1, before[$1] + 0, $2 + 0, ($2 - before[$1]) }
  ' <(printf '%s\n' "$base_fit") <(printf '%s\n' "$head_fit"))
  if [ -n "$regressions" ]; then
    design_reasons=$(git log --format='%(trailers:key=Design,valueonly,unfold)' "$base..HEAD")
    design_reason=$(awk '/[^[:space:]]/ { print; exit }' <<< "$design_reasons")
    if [ -z "$design_reason" ]; then
      echo "error[design-ratchet]: book-grounded AST fitness regressions detected in crates/:"
      echo "$regressions"
      echo "note: Source: AGENTS.md & bombay.md (Ford & Richards, Software Architecture: The Hard Parts, ch. 1)."
      echo "      Fix: eliminate the structural anti-pattern, or justify an intentional exception with:"
      echo "      Design: <reason>"
      failed=1
    fi
  fi
fi

[ "$failed" -eq 0 ] || exit 1
echo "step: $changed changed Rust lines, $added added production lines"
if [ "$oracle_edits" -gt 0 ]; then
  echo "step: test assertions removed=$removed changed=$changed_assertions (Oracle: $reason)"
fi
