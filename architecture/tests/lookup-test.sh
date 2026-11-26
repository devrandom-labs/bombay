#!/usr/bin/env bash
# Test the local main lookups in a repository whose worktree has diverged.
set -euo pipefail
architecture=${1:-architecture}
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
mkdir -p "$work/architecture/rules" "$work"/{first,second}/src "$work/first/src/folder.rs"
cp "$architecture/lookup.sh" "$work/architecture/"
cd -- "$work"
git init -q
git config user.name Fixture
git config user.email fixture@example.test
git config commit.gpgsign false
git config core.hooksPath /dev/null
cat > Cargo.toml <<'TOML'
[workspace]
members = ["first", "second"]
TOML
for crate in first second; do
  printf '[package]\nname = "%s"\nversion = "0.0.0"\n' "$crate" > "$crate/Cargo.toml"
done
cat > first/src/lib.rs <<'RUST'
pub enum Error { Bad }
pub(crate) fn retry_policy() {}
struct RetryPolicy;
trait RecoveryPolicy {
    fn recover();
}
type DefaultPolicy = RetryPolicy;
mod policy {}
struct Policyholder;
// struct Error;
fn new() {}
RUST
cat > second/src/lib.rs <<'RUST'
enum Error { Missing }
pub(super) struct BackoffPolicy;
struct Retry;
type ErrorCode = u8;
pub fn new() {}
RUST
printf 'fn nested() {}\n' > first/src/folder.rs/lib.rs
printf 'ruleDirs: [rules]\n' > architecture/sgconfig.yml
cat > architecture/rules/scope.yml <<'YAML'
id: fixture-scope
language: Rust
severity: error
message: |
  Rule for the first crate.
  This second line does not appear in TSV.
files: ["first/src/**/*.rs"]
ignores: ["first/src/generated/**"]
rule:
  pattern: forbidden($VALUE)
constraints:
  VALUE:
    kind: string_literal
YAML
git add .
GIT_AUTHOR_DATE=2026-11-23T10:00:00+00:00 GIT_COMMITTER_DATE=2026-11-23T10:00:00+00:00 git commit -qm main
main=$(git rev-parse HEAD)
git update-ref refs/remotes/origin/main "$main"
printf 'struct Error;\n' > second/src/lib.rs
git commit -qam branch
rm -- architecture/rules/scope.yml first/src/folder.rs/lib.rs
rmdir -- first/src/folder.rs
printf '\nstruct WorktreePolicy;\n' >> first/src/lib.rs

header="main $main 2026-11-23T10:00:00Z"
lookup() { bash architecture/lookup.sh "$@"; }
lookup exists Error | diff -u <(printf '%s\n' "$header" \
  $'enum\tfirst/src/lib.rs:1\tpub enum Error { Bad }' \
  $'enum\tsecond/src/lib.rs:1\tenum Error { Missing }') -
lookup exists retry_policy | diff -u <(printf '%s\n' "$header" \
  $'fn\tfirst/src/lib.rs:2\tpub(crate) fn retry_policy() {}') -
lookup exists recover | diff -u <(printf '%s\n' "$header" \
  $'fn\tfirst/src/lib.rs:5\tfn recover();') -
lookup exists Missing | diff -u <(printf '%s\n' "$header") -
lookup like RetryPolicy | diff -u <(printf '%s\n' "$header" \
  $'fn\tfirst/src/lib.rs:2\tpub(crate) fn retry_policy() {}' \
  $'struct\tfirst/src/lib.rs:3\tstruct RetryPolicy;' \
  $'trait\tfirst/src/lib.rs:4\ttrait RecoveryPolicy {' \
  $'type\tfirst/src/lib.rs:7\ttype DefaultPolicy = RetryPolicy;' \
  $'mod\tfirst/src/lib.rs:8\tmod policy {}' \
  $'struct\tsecond/src/lib.rs:2\tpub(super) struct BackoffPolicy;' \
  $'struct\tsecond/src/lib.rs:3\tstruct Retry;') -
lookup like new_policy | diff -u <(printf '%s\n' "$header" \
  $'fn\tfirst/src/lib.rs:2\tpub(crate) fn retry_policy() {}' \
  $'struct\tfirst/src/lib.rs:3\tstruct RetryPolicy;' \
  $'trait\tfirst/src/lib.rs:4\ttrait RecoveryPolicy {' \
  $'type\tfirst/src/lib.rs:7\ttype DefaultPolicy = RetryPolicy;' \
  $'mod\tfirst/src/lib.rs:8\tmod policy {}' \
  $'fn\tfirst/src/lib.rs:11\tfn new() {}' \
  $'struct\tsecond/src/lib.rs:2\tpub(super) struct BackoffPolicy;' \
  $'fn\tsecond/src/lib.rs:5\tpub fn new() {}') -
lookup like new | diff -u <(printf '%s\n' "$header" \
  $'fn\tfirst/src/lib.rs:11\tfn new() {}' \
  $'fn\tsecond/src/lib.rs:5\tpub fn new() {}') -
lookup rules first/src/new/nested.rs | diff -u <(printf '%s\n' "$header" \
  $'fixture-scope\tRule for the first crate.') -
lookup rules first/src/generated/lib.rs | diff -u <(printf '%s\n' "$header") -
lookup rules second/src/lib.rs | diff -u <(printf '%s\n' "$header") -
expect_error() {
  local expected=$1 message=$2 status=0
  shift 2
  lookup "$@" > "$work/out" 2> "$work/err" || status=$?
  if [ "$status" != "$expected" ] || [ -s "$work/out" ]; then
    printf 'lookup-test: expected exit %s, got %s from %s\n' "$expected" "$status" "$*" >&2
    cat "$work/out" "$work/err" >&2
    return 1
  fi
  diff -u <(printf '%s\n' "$message") "$work/err"
}
usage='Usage: lookup.sh {exists|like} <name> | rules <repository-relative .rs file>'
expect_error 2 "$usage" rules first/src
expect_error 2 "$usage" rules first/src/folder.rs
expect_error 2 "$usage" rules first/src/readme.md
expect_error 2 "$usage" rules architecture/sgconfig.yml
expect_error 2 "$usage" rules ../outside.rs
git update-ref -d refs/remotes/origin/main
expect_error 1 'lookup: origin/main is missing; fetch it' exists Error
echo 'lookup-test: symbols, similar names and rules are read from origin/main'
