#!/usr/bin/env bash
# Report these facts from Git, with no quality threshold:
# test-count counts test functions; ignored-test counts tests with #[ignore].
# no-assertion lists tests with no recognized assertion, error check or helper.
# A test that checks only with .unwrap(), ?, or a SQL file it runs is listed
# here even though it can fail.
# helper-assertion counts tests with assertions only through helpers in the scope.
# assertion-count counts assertion macros in test files and inline test functions.
# weak-assertion counts specific whole conditions in those macros per scope.
# unseen-block names files with proptest! blocks. Their tests and assertions
# are not counted because the Rust parser exposes the blocks as tokens.
# exemption counts allow, expect and ast-grep-ignore markers in Rust files.
# oracle-edit counts whole assertions removed or changed after matching moves
# and pairing integer edits. The default range is the last 300 commits, with
# merges excluded. Places start with removed, changed, authorized and added
# counts. oracle-edit-total sums these over all compared commits, including
# commits that only add assertions or change authorized expectations.
# oracle-unseen lists changed Rust test files that hold proptest! or macro_rules!
# bodies before or after the change. The history scan cannot read assertions
# inside these token trees.
# --oracle compares a tip with its merge base and prints one TSV row with the
# four counts in that order, then the edited and unseen file lists.
# survival measures the percent of added non-test Rust lines still credited by
# Git blame, rounded down.
#
# Blank places mean no locations were requested or found. An absent variable
# row means zero findings; absent survival means no window was requested.
# Scope rows can overlap. A test listed by no-assertion has no match in any
# supplied scope that includes it.
# Locations escape percent signs and whitespace for the space-separated list.
# Helpers match by the last name in a call, a function passed directly as an
# argument, or the name of an invoked macro. Names can collide.
# The Rust scan does not expand macros or cfg_attr or resolve calls. It cannot
# prove that a test ran real code, reached an assertion, or asserted the right
# result. Test files follow the path rules below. Weak conditions are
# recognized in assert!, debug_assert! and prop_assert! only.
# Identical assertion text removed from one test and added to another reads
# as a move. History does not expand helpers or macros that hide assertions.
# Whitespace is ignored even inside strings. Other text changes, including
# comments, can appear as a removal and an addition.
# Integer pairing can match changes to any integer argument in the assertion.
# Authorization checks the patterns in authorized.tsv and compares the path
# keys of the before and after OpenAPI snapshots. Review checks the operation
# named on the card and the rest of the snapshot change.
# Test files in history are recognized by path, including /tests/, tests.rs
# and test.rs. Inline tests in other Rust files are part of survival.
# A shallow clone gives different numbers. Use YYYY-MM-DD or timestamps with
# a time for a stable window. Bare YYYY-MM-DD dates cover the full day in UTC.

set -euo pipefail
export LC_ALL=C TZ=UTC0 GIT_NO_REPLACE_OBJECTS=1 GIT_ATTR_NOSYSTEM=1
unset CDPATH

die() { printf 'facts: %s\n' "$*" >&2; exit 2; }
usage() { die 'usage: facts.sh [--commits <rev range>] [--window <since> <until>] <scope>... | facts.sh --oracle <base> <tip>'; }

code=$(cd -- "$(dirname -- "$0")" && pwd)
test_path='(^|/)tests/|(^|/)(tests|test)[.]rs$'
snapshot=apps/server/tests/requests/snapshots/application__requests__health__openapi.snap

git_read() {
  git -c core.quotePath=false -c core.attributesFile=/dev/null --attr-source="$head" "$@"
}

new_work() {
  work=$(mktemp -d)
  trap 'find "$work" -depth -delete' EXIT
}

# No rename detection: a rename can move into or out of the test paths.
# Read both blobs only for test paths, including additions and deletions.
oracle_paths() {
  local commit=$1 command=$2 entry path old_mode new_mode old new
  shift 2
  git_read "$command" --raw -z --no-abbrev --no-renames --no-relative -O/dev/null "$@" |
    while IFS= read -r -d '' entry && IFS= read -r -d '' path; do
      [[ $path =~ $test_path ]] || continue
      read -r old_mode new_mode old new _ <<< "${entry#:}"
      [[ $old_mode == 100* || $new_mode == 100* ]] || continue
      if [[ $old_mode != 100* ]]; then old=-; fi
      if [[ $new_mode != 100* ]]; then new=-; fi
      printf '%s\0%s\0%s\0%s\0' "$commit" "$old" "$new" "$path"
    done
}

oracle_report() {
  local branch=$1 blob
  # A move can cross scope boundaries. Retain all test paths in commits
  # that touch the requested scope, and parse each distinct blob once.
  jq -Rs --slurpfile scopes "$work/scopes.json" '
    split("\u0000") | [range(0; length - 1; 4) as $i | .[$i:$i + 4]] |
    group_by(.[0]) | map(select(any(.[][3]; . as $path |
      any($scopes[0][]; . as $scope | $scope == "." or ($path | startswith($scope + "/")))))) |
    add // []
  ' "$work/paths" > "$work/paths.json"
  jq -r --arg snapshot "$snapshot" '
    [.[] | select(.[3] == $snapshot) | .[1:3][] | select(. != "-")] | unique[]
  ' "$work/paths.json" |
    while IFS= read -r blob; do
      git_read cat-file blob "$blob" | jq -Rs --arg blob "$blob" '
        {key: $blob, value: (
          try (sub("\\A---\\r?\\n.*?\\r?\\n---\\r?\\n"; ""; "m") |
            fromjson | .paths | objects | keys | map(select(startswith("/")))) catch null
        )} | select(.value != null)
      '
    done | jq -s 'from_entries' > "$work/snapshots"
  jq -r --arg snapshot "$snapshot" '
    [.[] | select(.[3] != $snapshot) | .[1:3][] | select(. != "-")] | unique[]
  ' "$work/paths.json" > "$work/blobs"
  mkdir "$work/blobs-rust"
  while IFS= read -r blob; do
    git_read cat-file blob "$blob" > "$work/blobs-rust/$blob.rs"
  done < "$work/blobs"
  if [[ -s $work/blobs ]]; then
    (
      cd "$work/blobs-rust"
      ast-grep run -l rust \
        -k 'macro_invocation, integer_literal, macro_definition, macro_invocation > :is(identifier, scoped_identifier)' \
        --json=stream \
        --no-ignore hidden --no-ignore dot --no-ignore global --no-ignore vcs . || [[ $? == 1 ]]
    )
  fi | jq -s -f "$code/assertions.jq" > "$work/oracle-assertions"
  jq -L "$code" -nr --argjson branch "$branch" --rawfile commits "$work/commits" \
    --slurpfile paths "$work/paths.json" --slurpfile assertions "$work/oracle-assertions" \
    --slurpfile scope_list "$work/scopes.json" --rawfile authorized "$code/authorized.tsv" \
    --arg snapshot "$snapshot" --slurpfile snapshots "$work/snapshots" \
    -f "$code/oracle.jq"
}

if [[ ${1:-} == --oracle ]]; then
  [[ $# == 3 ]] || usage
  base=$(git rev-parse --verify --end-of-options "$2^{commit}") || die "cannot read base: $2"
  head=$(git rev-parse --verify --end-of-options "$3^{commit}") || die "cannot read tip: $3"
  new_work
  oracle_paths "$head" diff "$base...$head" -- > "$work/paths"
  if [[ ! -s $work/paths ]]; then
    printf '0\t0\t0\t0\t\t\n'
    exit 0
  fi
  printf '%s\t\n' "$head" > "$work/commits"
  printf '["."]\n' > "$work/scopes.json"
  oracle_report true
  exit
fi

range=
since=
until=
options_done=0
while [[ $# -gt 0 ]]; do
  case $1 in
    --commits)
      [[ $# -ge 2 && -n $2 && -z $range ]] || usage
      range=$2
      shift 2
      ;;
    --window)
      [[ $# -ge 3 && -n $2 && -n $3 && -z $since ]] || usage
      since=$2 until=$3
      shift 3
      ;;
    --) options_done=1; shift; break ;;
    -*) usage ;;
    *) break ;;
  esac
done
[[ $# -gt 0 ]] || usage

root=$(git rev-parse --show-toplevel) || die 'cannot read the repository'
cd -- "$root"
head=$(git rev-parse --verify 'HEAD^{commit}') || die 'cannot read HEAD'
new_work

scopes=()
pathspecs=()
all_paths=0
for scope in "$@"; do
  given=$scope
  if [[ $given == -* && $options_done == 0 ]]; then die "option must precede scopes: $given"; fi
  while [[ $scope == ./* ]]; do scope=${scope#./}; done
  while [[ $scope == */ ]]; do scope=${scope%/}; done
  [[ -n $scope || $given == ./ ]] || die "absent scope: $given"
  scope=${scope:-.}
  [[ $scope != *$'\t'* && $scope != *$'\n'* && $scope != *$'\r'* ]] ||
    die "scope contains a tab or newline: $given"
  if [[ $scope != . ]]; then
    [[ $(git cat-file -t "$head:$scope" 2>/dev/null) == tree ]] ||
      die "absent scope: $given"
  else
    all_paths=1
  fi
  scopes+=("$scope")
  pathspecs+=(":(literal)$scope")
done
printf '%s\n' "${scopes[@]}" | sort -u > "$work/scopes"
jq -Rsc 'split("\n")[:-1]' "$work/scopes" > "$work/scopes.json"
# ls-tree reads the pinned commit even if the index has staged changes.
if [[ $all_paths == 1 ]]; then
  git ls-tree -rz "$head" > "$work/files"
else
  git ls-tree -rz "$head" -- "${pathspecs[@]}" > "$work/files"
fi

if [[ -n $range ]]; then
  git rev-list --end-of-options "$range" -- > "$work/revisions" ||
    die "cannot read commit range: $range"
else
  git rev-list -300 "$head" > "$work/revisions"
fi

mkdir "$work/rust"
while IFS= read -r -d '' entry; do
  path=${entry#*$'\t'}
  if [[ $entry == 100* && $path == *.rs ]]; then
    if [[ $path == */* ]]; then mkdir -p -- "$work/rust/${path%/*}"; fi
    git show "$head:$path" > "$work/rust/$path"
  fi
done < "$work/files"

(
  cd "$work/rust"
  # ast-grep scan honors suppressions. Read real comments with the matcher,
  # then neutralize the marker in the private copy used by scan.
  ast-grep run -l rust -k 'line_comment, block_comment' --json=stream \
    --no-ignore hidden --no-ignore dot --no-ignore global --no-ignore vcs . \
    > "$work/comments" || [[ $? -eq 1 ]]
  grep -rIlZF -- 'ast-grep-ignore' . > "$work/markers" || [[ $? -eq 1 ]]
  while IFS= read -r -d '' path; do
    awk '{ gsub(/ast-grep-ignore/, "ast-grep-IGNORE"); print }' "$path" > "$work/rust-copy"
    mv -- "$work/rust-copy" "$path"
  done < "$work/markers"
  ast-grep scan --rule "$code/tree.yml" --json=stream \
    --no-ignore hidden --no-ignore dot --no-ignore global --no-ignore vcs . > "$work/nodes"
)
jq -sc '[.[] | select(.ruleId == "assertion" and
  (.metaVariables.single.MACRO.text | test("^(r#)?(assert|debug_assert|prop_assert)$")))]' \
  "$work/nodes" > "$work/assertions"
jq -r 'to_entries[] |
  .value.metaVariables.single.ARGS.text as $args |
  "fn quality_\(.key)() { quality_condition(\($args[1:-1])); }"' \
  "$work/assertions" > "$work/conditions.rs"
ast-grep scan --rule "$code/conditions.yml" --json=stream \
  "$work/conditions.rs" > "$work/conditions"
jq -L "$code" -nr --rawfile scopes "$work/scopes" --slurpfile nodes "$work/nodes" \
  --slurpfile comments "$work/comments" --slurpfile assertions "$work/assertions" \
  --slurpfile conditions "$work/conditions" --arg test_path "$test_path" \
  -f "$code/tree.jq" > "$work/tree"

if [[ -s $work/revisions ]]; then
  git_read log --no-walk=unsorted --no-merges --stdin --no-decorate --no-notes \
    --no-show-signature --format='%H%x09%s' --encoding=UTF-8 < "$work/revisions"
fi > "$work/commits"
while IFS=$'\t' read -r commit _; do
  oracle_paths "$commit" diff-tree -r --root --no-commit-id "$commit" --
done < "$work/commits" > "$work/paths"
oracle_report false > "$work/oracle"

if [[ -n $since ]]; then
  start=$since end=$until
  day='^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
  if [[ $start =~ $day ]]; then start+=" 00:00:00 +0000"; fi
  if [[ $end =~ $day ]]; then end+=" 23:59:59 +0000"; fi
  git_read log --no-merges --since-as-filter="$start" --until="$end" \
    --format='%H' --no-decorate --no-notes --no-show-signature --numstat -z \
    --no-ext-diff --no-textconv --diff-algorithm=myers --no-indent-heuristic \
    -M50% -l0 "$head" > "$work/window"

  # Numstat's NUL format gives a rename an empty path followed by two paths.
  # Use its destination to count additions. Emit only Rust paths.
  : > "$work/window-commits"
  while IFS= read -r -d '' record; do
    record=${record#$'\n'}
    if [[ $record != *$'\t'* ]]; then
      if [[ -n $record ]]; then printf '%s\n' "$record" >> "$work/window-commits"; fi
      continue
    fi
    added=${record%%$'\t'*}
    path=${record#*$'\t'}
    path=${path#*$'\t'}
    if [[ -z $path ]]; then
      IFS= read -r -d '' _
      IFS= read -r -d '' path
    fi
    if [[ $added != - && $path == *.rs && ! $path =~ $test_path ]]; then
      printf 'added\0%s\0%s\0' "$added" "$path"
    fi
  done < "$work/window" > "$work/lines"

  if [[ -s $work/window-commits ]]; then
    # Blame once per path even when several scopes include it.
    while IFS= read -r -d '' entry; do
      path=${entry#*$'\t'}
      if [[ $entry == 100* && $path == *.rs && ! $path =~ $test_path ]]; then
        survived=$(git_read blame --incremental --ignore-revs-file "" \
          --diff-algorithm=myers --no-indent-heuristic -M -C -w "$head" -- "$path" |
          awk 'NR == FNR { commits[$1] = 1; next }
            NF == 4 && $1 in commits && $2 ~ /^[0-9]+$/ &&
              $3 ~ /^[0-9]+$/ && $4 ~ /^[0-9]+$/ { lines += $4 }
            END { print lines + 0 }' "$work/window-commits" -)
        printf 'survived\0%s\0%s\0' "$survived" "$path"
      fi
    done < "$work/files" >> "$work/lines"
  fi

  # Read paths as NUL records through Bash, whose read also preserves spaces.
  while IFS= read -r -d '' kind && IFS= read -r -d '' lines && IFS= read -r -d '' path; do
    while IFS= read -r scope; do
      if [[ $scope == . || $path == "$scope/"* ]]; then
        printf '%s\t%s\t%s\n' "$scope" "$kind" "$lines"
      fi
    done < "$work/scopes"
  done < "$work/lines" |
    awk -F '\t' -v window="$since to $until" '
      NR == FNR { scope[FNR] = $0; next }
      $2 == "added" { added[$1] += $3 }
      $2 == "survived" { survived[$1] += $3 }
      END {
        for (i = 1; i in scope; i++) {
          name = scope[i]
          a = added[name] + 0
          s = survived[name] + 0
          printf "survival\t%s %s\t%d\tadded=%d", name, window, a ? s * 100 / a : 0, a
          if (a) printf " survived=%d", s
          printf "\n"
        }
      }' "$work/scopes" - > "$work/survival"
fi

cat "$work/tree"
printf '# history\n'
cat "$work/oracle"
if [[ -n $since ]]; then cat "$work/survival"; fi
