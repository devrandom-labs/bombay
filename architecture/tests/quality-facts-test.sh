#!/usr/bin/env bash
# Run from the repository root. The optional script path permits guard tests.
set -euo pipefail

facts=${1:-$PWD/architecture/quality/facts.sh}
work=$(mktemp -d)
trap 'find "$work" -depth -delete' EXIT
fail() { printf 'quality-facts-test: %s\n' "$1" >&2; exit 1; }

export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
export GIT_AUTHOR_NAME=Fixture GIT_AUTHOR_EMAIL=fixture@example.invalid
export GIT_COMMITTER_NAME=Fixture GIT_COMMITTER_EMAIL=fixture@example.invalid
export LC_ALL=C TZ=UTC0
unset CDPATH

commit() {
  git add -A
  GIT_AUTHOR_DATE="2026-10-30T12:00:00+00:00" GIT_COMMITTER_DATE="$1" git commit -qm "$2"
}

git init -q --template= --object-format=sha1 --initial-branch=main "$work/repo"
cd "$work/repo"
git config diff.renames true
mkdir -p alpha/{src,tests} beta/{src,tests} empty outside/tests
touch empty/.keep

cat > alpha/tests/checks.rs <<'RUST'
#![allow(dead_code, reason /* comment */ = "fixture")]
#![allow(unused)]
#![expect(unused_variables)]

// A comment about the exception.
// ast-grep-ignore: particular-rule
#[test]
#[ignore /* x=y is a comment, with no reason value */]
fn loud() {
    println!("assert!(value.is_ok()) ast-grep-ignore");
}

// ast-grep-ignore
#[tokio::test]
/// An exact check.
#[ignore = r#"measurement"#]
async fn exact() {
    assert_eq!(value, Ok(3));
    assert_eq!(value.is_ok(), true);
}

#[rstest]
#[case(3)]
fn helper() { assert_state(); }

#[module::test]
fn error() { value.unwrap_err(); }

#[test]
fn expect_error() { value.expect_err("an error"); }

#[rstest::rstest]
fn question() { tests::check_state::<u8>()?; }

#[test]
fn other_helpers() { expect_value(); }
#[test]
fn verify() { state.verify_value(); }

#[test]
fn debug() { debug_assert_eq!(value, Ok(3)); }
#[test]
fn property() { prop_assert_eq!(value, Ok(3)); }
#[test]
fn snapshot() { ::insta::assert_snapshot!(value); }
#[test]
fn panics() { panic!("failed"); }
#[test]
fn impossible() { unreachable!(); }
#[test]
fn matches() { matches!(value, Ok(3)); }

#[test]
fn weak() {
    assert!(lookup(1, 2).is_ok());
    debug_assert!(value.is_ok(), "context: {:?}", value);
    prop_assert! { value.is_err() }
    assert![
        value.is_some()
    ];
    assert!(value.is_none());
    assert!((!value.is_empty()));
    assert!(value.contains(argument(1, 2)));
    assert!(value.len() > 0);
    assert!(value.len() >= 1);
    assert!(true);
    assert!((flag || value.is_ok()));
    assert!(value.is_ok() && other.is_ok());
    assert!(!value.is_ok());
    assert!(value.is_empty());
    assert!(value.len() > 1);
    assert!(value.len() <= 1);
    assert!(quality_condition(true).passes());
    assert!(other, "is_ok: {:?}", value.is_ok());
}

#[ignore]
fn ordinary() {}
#[test_support::test_case]
fn similar_name() {}

fn non_test() {
    assert!(value.is_ok());
    // assert!(true);
    let text = "assert!(true)";
}

/* Block reason. */
// ast-grep-ignore: another-rule
#[allow(unused_variables)]
const VALUE: u32 = 3;

#[allow(reason)] // ast-grep-ignore
#[test]
fn not_error_check() { custom.unwrap_err("same name"); }
RUST
cat > alpha/tests/loud.rs <<'RUST'
#[test]
fn prints() { println!("only prints"); }
#[test]
fn text() { println!("verify_state()?"); }
#[test]
fn similar_call() { verify(); }
RUST
cat > beta/tests/quiet.rs <<'RUST'
const VALUE: u32 = 4; // The previous line of an exemption must be a comment.
// ast-grep-ignore: beta-rule
#[test]
fn passes() { ::std::assert_eq!(VALUE, 4); }
#[test]
fn weak() { assert!(value.is_err()); }
#[test]
fn other_scope() { scenarios::leaf(); }
/* Code follows this comment. */ const OTHER: u32 = 5;
// ast-grep-ignore: trailing-code
const UNUSED: u32 = 6;
RUST
cat > alpha/tests/scenarios.rs <<'RUST'
fn middle() { scenarios::leaf::<u8>(); }
fn top() { middle(); }
fn plain() { println!("only prints"); }

#[test]
fn direct_helper() { scenarios::leaf::<u8>(); }
#[test]
fn indirect_helper() { scenarios::top(); }
#[test]
fn no_check() { plain(); }
#[test]
fn direct_and_helper() { leaf(); assert_eq!(value, 1); }
#[test]
fn calls_test() { direct_and_helper(); }
#[test]
fn calls_production() { production(); }
#[test]
fn method_helper() { state.leaf(); }
RUST
cat > alpha/tests/support.rs <<'RUST'
fn leaf() { assert_ne!(value, 0); }
RUST
cat > alpha/tests/helper_kinds.rs <<'RUST'
macro_rules! oracle { () => {{ ::insta::assert_snapshot!(state); }}; }
macro_rules! only_prints { () => { println!("assert!(true)"); }; }
macro_rules! identifier_only { () => { let assert_eq = true; }; }
#[test]
fn passed_helper() { tokio::task::spawn_blocking(leaf); }
#[test]
fn called_macro_helper() { oracle!(); }
#[test]
fn printed_names() { let oracle = "leaf"; only_prints!(); identifier_only!(); }

proptest::proptest! {
    #[test]
    fn property(value in 0..10) { prop_assert!(value < 10); }
}
proptest! {
    #[test]
    fn second(value in 10..20) { prop_assert!(value < 20); }
}

fn probe() { assert_eq!(value, 3); }
fn boot() { assert!(booted); }
#[test]
fn names_without_calls() {
    let probe = 3;
    let data = Data { probe };
    let typed: probe = data.probe;
    probe!();
}
#[test]
fn sql_only() {
    let boot = boot_test::<App>().await.unwrap();
    boot.app_context.db.execute_unprepared(include_str!("check.sql")).await.unwrap();
    boot.app_context.db.close().await.unwrap();
}
RUST
cat > alpha/src/inline.rs <<'RUST'
#[test] fn inline_test() { assert_eq!(1, 1); }
fn production() { assert!(value.is_none()); }
#[test]
fn inline_macro() {
    macro_rules! inline_oracle { () => { assert_eq!(1, 1) }; }
    inline_oracle!();
}
RUST
cat > alpha/src/reordered.rs <<'RUST'
fn first_anchor() -> u16 { 1111 }
fn second_anchor() -> u32 { 2222 }
fn third_anchor() -> u64 { 3333 }
fn fourth_anchor() -> u128 { 4444 }
RUST
printf '%s\n' 'fn original_destination_function() -> u64 { 6666 }' > alpha/src/reused.rs
printf '%s\n' 'fn original_function_with_a_long_and_stable_name() -> u64 { 6666 }' > beta/src/original.rs
cat > alpha/tests/oracle.txt <<'TEXT'
assert_eq!(score(), 1);
keep this line between assertions
assert!(moved());
keep this line too
assert_eq!(
    value,
    Ok(2)
);
keep this line after the expected value
debug_assert!(debug_only());
println!("no assertion");
assert!(twice());
keep this line between duplicates
assert!(twice());
TEXT
printf '%s\n' 'prop_assert_eq!(score(), 1);' > alpha/test.rs
printf '%s\n' '::insta::assert_snapshot!(state);' > alpha/src/tests.rs
printf '%s\n' 'fn contest() {' '    assert!(non_test_path());' '}' > alpha/src/contest.rs
printf '%s\n' 'assert!(in_a_path_with_spaces());' > 'alpha/tests/with space.txt'
printf '%s\n' 'keep the original text here' > outside/tests/moved.txt
printf '%s\n' 'assert!(out_of_scope());' > outside/tests/unrelated.txt
printf '%s\n' 'assert!(first());' 'assert!(second());' 'assert!(third());' > alpha/tests/edits.txt
cat > alpha/tests/numbers.txt <<'RUST'
assert_eq!(score(), 13);
assert_eq!(score(), 14);
assert_eq!(label, "v1");
assert_eq!(lengths["paths"][0].len(), 7);
RUST
printf '%s\n' 'assert_eq!(value("é"), 0x2_u8);' > alpha/tests/number_formats.txt
printf '%s\n' 'assert_eq!(document["paths"].as_object().unwrap().len(), 7);' > alpha/tests/api.txt
printf '%s\n' 'assert!(leaves_test_paths());' > alpha/tests/rename.txt
git config core.hooksPath /dev/null
commit '2026-10-31T12:00:00+00:00' 'Baseline outside the window'

cat > alpha/src/live.rs <<'RUST'
fn inside_rewrite() -> u8 { 1 }
fn outside_rewrite() -> u32 { 2 }
fn whitespace() -> u16 { 3 }
fn stays() -> u64 { 4 }
fn line_reassigned_to_a_different_file_after_the_reporting_window() -> u128 { 111 }
RUST
printf '%s\n' 'fn a_long_line_that_will_move_within_the_same_file() -> u8 { 88 }' > "$work/edit"
cat alpha/src/reordered.rs >> "$work/edit"
mv "$work/edit" alpha/src/reordered.rs
printf '%s\n' 'fn removed_after_window() -> u8 { 5 }' > beta/src/offer.rs
printf '%s\n' '#[test]' 'fn new_test() {' '    assert_eq!(new_value(), 4);' '}' > alpha/tests/new.rs
printf '%s\n' 'This is outside the Rust measure.' > alpha/src/history.md
commit '2026-11-01T01:01:00+00:00' 'Add production lines and a new test file'

awk '!/moved\(\)/' alpha/tests/oracle.txt > "$work/edit"
mv "$work/edit" alpha/tests/oracle.txt
printf '%s\n' '    assert! ( moved() );' >> outside/tests/moved.txt
git mv beta/src/original.rs beta/src/renamed.rs
printf '%s\n' 'fn extra() {}' >> beta/src/renamed.rs
commit '2026-11-02T08:00:00+00:00' 'Move an assertion outside the scope with different spacing'

awk '!/score|debug_only|println/' alpha/tests/oracle.txt > "$work/edit"
mv "$work/edit" alpha/tests/oracle.txt
: > alpha/test.rs
git rm -q alpha/src/tests.rs 'alpha/tests/with space.txt'
: > outside/tests/unrelated.txt
commit '2026-11-03T09:00:01+00:00' 'Remove test assertions and leave a debug assertion out of the count'
removed=$(git rev-parse HEAD)

awk '{ sub(/Ok\(2\)/, "Ok(3)") } !/twice\(\)/' alpha/tests/oracle.txt > "$work/edit"
mv "$work/edit" alpha/tests/oracle.txt
printf '%s\n' '  assert!( twice() );' >> outside/tests/moved.txt
awk '{ sub(/u8 \{ 1 \}/, "u8 { 9 }"); print }' alpha/src/live.rs > "$work/edit"
mv "$work/edit" alpha/src/live.rs
commit '2026-11-10T22:00:03+00:00' 'Change an expected value and move one of two identical assertions'

awk '!/reassigned/ { sub(/u32 \{ 2 \}/, "u32 { 7 }"); sub(/u16 \{ 3 \}/, "u16 {   3 }"); print }' alpha/src/live.rs > "$work/edit"
awk '/reassigned/' alpha/src/live.rs >> alpha/src/reused.rs
mv "$work/edit" alpha/src/live.rs
awk 'NR == 1 { moved = $0; next } { print } END { print moved }' alpha/src/reordered.rs > "$work/edit"
mv "$work/edit" alpha/src/reordered.rs
git rm -q beta/src/offer.rs
printf '%s\n' 'assert_eq!(score(), 1);' >> outside/tests/moved.txt
: > alpha/src/contest.rs
commit '2026-11-11T00:00:00+00:00' 'Rewrite outside the window and reintroduce the deleted assertion'

printf '%s\n' 'assert!(third());' > alpha/tests/edits.txt
cat > beta/tests/added.rs <<'RUST'
fn scenario() {
    assert_eq!(first(), 1);
    assert_eq!(second(), 2);
    assert_eq!(third(), 3);
    assert_eq!(value, Ok(3));
}
RUST
# The previous move outside the scope must still cancel. A new move within
# the scope must cancel on both sides, even when the assertion moves crates.
awk '/^assert_eq!\($/ { skip = 1 } skip && /^\);$/ { skip = 0; next } !skip' \
  alpha/tests/oracle.txt > "$work/edit"
mv "$work/edit" alpha/tests/oracle.txt
printf '%s\n' 'assert!(outside());' >> outside/tests/moved.txt
printf '%s\n' 'assert!(outside_a_test_file());' >> alpha/src/history.md
commit '2026-11-11T01:00:00+00:00' 'Replace two assertions with three in a new test file'

: > alpha/tests/edits.txt
commit '2026-11-11T02:00:00+00:00' 'Delete one assertion without additions'
sed -i 's/len(), 7/len(), 8/' alpha/tests/api.txt
commit '2026-11-11T03:00:00+00:00' 'Raise the OpenAPI path count'
cat > alpha/tests/numbers.txt <<'RUST'
assert_eq!(score(), 15);
assert_eq!(label, "v2");
assert_eq!(lengths["paths"][0].len(), 8);
RUST
sed -i 's/len(), 8/len(), 9/' alpha/tests/api.txt
commit '2026-11-11T04:00:00+00:00' 'Change numeric, OpenAPI and string expectations'
printf '%s\n' 'assert_eq!(value("é"), 0b11u8);' > alpha/tests/number_formats.txt
commit '2026-11-11T04:30:00+00:00' 'Change an integer written with a base and suffix'
git mv alpha/tests/rename.txt alpha/src/rename.txt
commit '2026-11-11T05:00:00+00:00' 'Move an assertion out of the test paths'
renamed=$(git rev-parse HEAD)
printf '%s\n' 'The commit date is earlier than the date of its parent.' > outside/dated-note
commit '2026-10-30T12:00:00+00:00' 'Add a note with an earlier commit date'

git switch -qc side
printf '%s\n' 'A side commit outside the date window.' > outside/note
commit '2026-11-12T12:00:00+00:00' 'Add a note on a side branch'
git switch -q main
git merge -q --no-ff --no-commit side > "$work/merge" 2>&1 || { cat "$work/merge"; fail 'fixture merge failed'; }
printf '%s\n' 'fn merge_only() -> u8 { 8 }' >> alpha/src/live.rs
commit '2026-11-09T12:00:00+00:00' 'Merge the side branch with a line the measure must omit'

# Add the final TSV separator on rows with empty places.
awk -F '\t' 'NF == 3 { $0 = $0 FS } { print }' > "$work/expected" <<'EXPECTED'
assertion-count	alpha	30
assertion-count	alpha/tests	29
assertion-count	beta	6
assertion-count	empty	0
exemption	allow with reason	1
exemption	allow without reason	3	alpha/tests/checks.rs:2 alpha/tests/checks.rs:90 alpha/tests/checks.rs:93
exemption	ast-grep-ignore with comment	2
exemption	ast-grep-ignore without comment	4	alpha/tests/checks.rs:13 alpha/tests/checks.rs:93 beta/tests/quiet.rs:10 beta/tests/quiet.rs:2
exemption	expect attribute	1
helper-assertion	alpha	6	alpha/src/inline.rs:4 alpha/tests/helper_kinds.rs:5 alpha/tests/helper_kinds.rs:7 alpha/tests/scenarios.rs:18 alpha/tests/scenarios.rs:6 alpha/tests/scenarios.rs:8
helper-assertion	alpha/tests	5	alpha/tests/helper_kinds.rs:5 alpha/tests/helper_kinds.rs:7 alpha/tests/scenarios.rs:18 alpha/tests/scenarios.rs:6 alpha/tests/scenarios.rs:8
helper-assertion	beta	0
helper-assertion	empty	0
ignored-test	with reason	1
ignored-test	without reason	1	alpha/tests/checks.rs:9
no-assertion	alpha/tests/helper_kinds.rs	3	alpha/tests/helper_kinds.rs:23 alpha/tests/helper_kinds.rs:30 alpha/tests/helper_kinds.rs:9
no-assertion	alpha/tests/loud.rs	3	alpha/tests/loud.rs:2 alpha/tests/loud.rs:4 alpha/tests/loud.rs:6
no-assertion	alpha/tests/scenarios.rs	3	alpha/tests/scenarios.rs:10 alpha/tests/scenarios.rs:14 alpha/tests/scenarios.rs:16
no-assertion	alpha/tests/checks.rs	2	alpha/tests/checks.rs:9 alpha/tests/checks.rs:95
no-assertion	beta/tests/quiet.rs	1	beta/tests/quiet.rs:8
test-count	alpha	34	alpha/src/inline.rs:1 alpha/src/inline.rs:4 alpha/tests/checks.rs:17 alpha/tests/checks.rs:24 alpha/tests/checks.rs:27 alpha/tests/checks.rs:30 alpha/tests/checks.rs:33 alpha/tests/checks.rs:36 alpha/tests/checks.rs:38 alpha/tests/checks.rs:41 alpha/tests/checks.rs:43 alpha/tests/checks.rs:45 +22
test-count	alpha/tests	32	alpha/tests/checks.rs:17 alpha/tests/checks.rs:24 alpha/tests/checks.rs:27 alpha/tests/checks.rs:30 alpha/tests/checks.rs:33 alpha/tests/checks.rs:36 alpha/tests/checks.rs:38 alpha/tests/checks.rs:41 alpha/tests/checks.rs:43 alpha/tests/checks.rs:45 alpha/tests/checks.rs:47 alpha/tests/checks.rs:49 +20
test-count	beta	3	beta/tests/quiet.rs:4 beta/tests/quiet.rs:6 beta/tests/quiet.rs:8
test-count	empty	0
unseen-block	alpha/tests/helper_kinds.rs	2	alpha/tests/helper_kinds.rs:11 alpha/tests/helper_kinds.rs:15
weak-assertion	alpha is_ok	3	alpha/tests/checks.rs:55 alpha/tests/checks.rs:56 alpha/tests/checks.rs:83
weak-assertion	alpha/tests is_ok	3	alpha/tests/checks.rs:55 alpha/tests/checks.rs:56 alpha/tests/checks.rs:83
weak-assertion	alpha !is_empty	1	alpha/tests/checks.rs:62
weak-assertion	alpha contains	1	alpha/tests/checks.rs:63
weak-assertion	alpha is_err	1	alpha/tests/checks.rs:57
weak-assertion	alpha is_none	1	alpha/tests/checks.rs:61
weak-assertion	alpha is_some	1	alpha/tests/checks.rs:58
weak-assertion	alpha len > 0	1	alpha/tests/checks.rs:64
weak-assertion	alpha len >= 1	1	alpha/tests/checks.rs:65
weak-assertion	alpha true	1	alpha/tests/checks.rs:66
weak-assertion	alpha/tests !is_empty	1	alpha/tests/checks.rs:62
weak-assertion	alpha/tests contains	1	alpha/tests/checks.rs:63
weak-assertion	alpha/tests is_err	1	alpha/tests/checks.rs:57
weak-assertion	alpha/tests is_none	1	alpha/tests/checks.rs:61
weak-assertion	alpha/tests is_some	1	alpha/tests/checks.rs:58
weak-assertion	alpha/tests len > 0	1	alpha/tests/checks.rs:64
weak-assertion	alpha/tests len >= 1	1	alpha/tests/checks.rs:65
weak-assertion	alpha/tests true	1	alpha/tests/checks.rs:66
weak-assertion	beta is_err	1	beta/tests/quiet.rs:6
# history
oracle-edit	49be0beee971 Change numeric, OpenAPI and string expectations	5	removed=2 changed=3 authorized=0 added=1 alpha/tests/api.txt alpha/tests/numbers.txt
oracle-edit	28274829f36e Remove test assertions and leave a debug assertion out of th	4	removed=4 changed=0 authorized=0 added=0 alpha/src/tests.rs alpha/test.rs alpha/tests/oracle.txt alpha/tests/with%20space.txt
oracle-edit	df1ca9d68eff Change an expected value and move one of two identical asser	2	removed=1 changed=1 authorized=0 added=0 alpha/tests/oracle.txt
oracle-edit	f7be4d2b957c Replace two assertions with three in a new test file	2	removed=2 changed=0 authorized=0 added=3 alpha/tests/edits.txt
oracle-edit	5e2748c0c6bb Raise the OpenAPI path count	1	removed=0 changed=1 authorized=0 added=0 alpha/tests/api.txt
oracle-edit	849a3166a679 Change an integer written with a base and suffix	1	removed=0 changed=1 authorized=0 added=0 alpha/tests/number_formats.txt
oracle-edit	940671116951 Move an assertion out of the test paths	1	removed=1 changed=0 authorized=0 added=0 alpha/tests/rename.txt
oracle-edit	f2e5c57ffba8 Delete one assertion without additions	1	removed=1 changed=0 authorized=0 added=0 alpha/tests/edits.txt
oracle-edit-total	8 of 14	17	removed=11 changed=6 authorized=0 added=50
oracle-unseen	93ba07938c61 Baseline outside the window	1	alpha/tests/helper_kinds.rs
survival	alpha 2026-11-01 to 2026-11-10	71	added=7 survived=5
survival	alpha/tests 2026-11-01 to 2026-11-10	0	added=0
survival	beta 2026-11-01 to 2026-11-10	50	added=2 survived=1
survival	empty 2026-11-01 to 2026-11-10	0	added=0
EXPECTED

# -C can recover within-file moves too. Check that blame explicitly requests
# -M while still running the real Git command and comparing the survival rows.
mkdir "$work/bin"
export QUALITY_TEST_GIT
QUALITY_TEST_GIT=$(command -v git)
printf '#!%s\n' "$BASH" > "$work/bin/git"
cat >> "$work/bin/git" <<'BASH'
blame=0 move=0
for arg; do
  case $arg in
    blame) blame=1 ;;
    -M) move=1 ;;
    --) break ;;
  esac
done
if [[ $blame == 1 && $move == 0 ]]; then
  echo 'quality-facts-test: blame must request -M' >&2
  exit 1
fi
exec "$QUALITY_TEST_GIT" "$@"
BASH
chmod +x "$work/bin/git"
run() { PATH="$work/bin:$PATH" bash "$facts" --window 2026-11-01 2026-11-10 beta empty alpha/tests alpha; }
run > "$work/first"
diff -u "$work/expected" "$work/first" || fail 'fixture output differs'

# The working copy, including the index, has different Rust and a new scope.
git rm -q alpha/tests/checks.rs
printf '%s\n' '#[test] fn staged() { assert!(true); }' > alpha/tests/staged.rs
git add alpha/tests/staged.rs
printf '%s\n' '#[test] fn unstaged() {}' >> beta/tests/quiet.rs
mkdir missing
printf '%s\n' '#[test] fn untracked() {}' > missing/new.rs
TZ=Europe/Oslo run > "$work/second"
cmp "$work/first" "$work/second" || fail 'output changed without a new commit'

bash "$facts" --commits "$removed^..$removed" alpha > "$work/one"
awk '/^# history$/ { history = 1 } history { print }' "$work/one" > "$work/history"
awk '/^# history$/ { print } /^oracle-edit\t.*Remove test assertions/ { print }' \
  "$work/expected" > "$work/one-expected"
printf 'oracle-edit-total\t1 of 1\t4\tremoved=4 changed=0 authorized=0 added=0\n' >> "$work/one-expected"
diff -u "$work/one-expected" "$work/history" || fail 'explicit range differs'

printf '5\t0\t0\t0\talpha/src/tests.rs alpha/test.rs alpha/tests/oracle.txt alpha/tests/with%%20space.txt outside/tests/unrelated.txt\t\n' \
  > "$work/branch-expected"
bash "$facts" --oracle "$removed^" "$removed" > "$work/branch"
diff -u "$work/branch-expected" "$work/branch" || fail 'branch oracle count or file paths differ'

printf '1\t0\t0\t0\talpha/tests/rename.txt\t\n' > "$work/rename-expected"
bash "$facts" --oracle "$renamed^" "$renamed" > "$work/rename"
diff -u "$work/rename-expected" "$work/rename" || fail 'rename must compare the old and new paths separately'

awk -F '\t' -v OFS='\t' '
  /^(exemption|ignored-test)\t/ { $3 = 0; $4 = ""; print }
  $2 == "empty" || /^# history$/ { print }
  $1 == "oracle-edit-total" { $2 = "0 of 0"; $3 = 0; $4 = "removed=0 changed=0 authorized=0 added=0"; print }
' "$work/expected" > "$work/none-expected"
expect_input_error() {
  code=0
  bash "$facts" "$@" > "$work/bad-out" 2> "$work/bad-err" || code=$?
  [[ $code == 2 && ! -s $work/bad-out ]] || fail "expected status 2 and no output for $*; got $code"
  grep -Fq 'facts:' "$work/bad-err" || fail "expected a diagnostic for $*"
}
expect_input_error missing
expect_input_error alpha/test.rs
expect_input_error --commits bad-start..bad-end alpha
expect_input_error alpha --window 2026-11-01 2026-11-10
printf 'facts: option must precede scopes: --window\n' > "$work/option-expected"
diff -u "$work/option-expected" "$work/bad-err" || fail 'misplaced option diagnostic differs'

# Place the fixture merge exactly 300 commits from the tip. It uses one
# position in the default range, then drops out of the non-merge count.
awk -v head="$(git rev-parse HEAD)" 'BEGIN {
  for (i = 1; i <= 299; i++) {
    print "commit refs/heads/range-limit"
    print "committer Fixture <fixture@example.invalid> 1795262400 +0000"
    print "data 5\nLimit"
    if (i == 1) print "from " head
    print ""
  }
}' | git fast-import --quiet
git symbolic-ref HEAD refs/heads/range-limit
bash "$facts" --commits HEAD..HEAD empty > "$work/none"
diff -u "$work/none-expected" "$work/none" || fail 'empty range differs'
bash "$facts" empty > "$work/limit"
awk -F '\t' '$1 == "oracle-edit-total" { $2 = "0 of 299" } { print }' OFS='\t' \
  "$work/none-expected" > "$work/limit-expected"
diff -u "$work/limit-expected" "$work/limit" || fail 'default range limit differs'

git init -q --template= --object-format=sha1 --initial-branch=main "$work/openapi"
cd "$work/openapi"
health=apps/server/tests/requests/health.rs
snapshot=apps/server/tests/requests/snapshots/application__requests__health__openapi.snap
other=apps/server/tests/requests/other.rs
similar=$health$'\n'
mkdir -p apps/server/tests/requests/snapshots
write_count() {
  printf '%s\n' "assert_eq!(document[\"paths\"].as_object().unwrap().len(), $2);" \
    "assert_eq!(document[\"servers\"].as_array().unwrap().len(), ${3:-7});" > "$1"
}
write_snapshot() {
  printf '%s\n' '---' "source: $health" 'expression: document' '---' "$1" > "$snapshot"
}
write_count "$health" 7
write_count "$other" 7
write_count "$similar" 7
write_snapshot '{"paths": {"/old": {"get": {}}, "/kept": {"get": {}}}}'
cat > apps/server/tests/properties.rs <<'RUST'
proptest::proptest /* comment */ ! {
    fn bounded(value in 0..10) { prop_assert!(value < 10); }
}
RUST
cat > apps/server/tests/macros.rs <<'RUST'
macro_rules! checked { () => { assert_eq!(value, 1); }; }
RUST
commit '2026-11-13T00:00:00+00:00' 'OpenAPI fixture base'

openapi_case() {
  local name=$1 file=$2 count=$3 document=$4 changed=$5 authorized=$6 files=
  git checkout -q -B step main
  write_count "$file" "$count" "${7:-7}"
  if [[ -n $document ]]; then write_snapshot "$document"; fi
  commit '2026-11-13T01:00:00+00:00' "$name"
  if [[ $changed == 1 ]]; then files=${file//$'\n'/%0A}; fi
  printf '0\t%s\t%s\t0\t%s\t\n' "$changed" "$authorized" "$files" > "$work/case-expected"
  bash "$facts" --oracle main HEAD > "$work/case"
  diff -u "$work/case-expected" "$work/case" || fail "$name in branch scan"
  printf 'oracle-edit-total\t%s of 1\t%s\tremoved=0 changed=%s authorized=%s added=0\n' \
    "$changed" "$changed" "$changed" "$authorized" > "$work/case-expected"
  bash "$facts" --commits main..HEAD apps/server > "$work/report"
  awk '/^oracle-edit-total\t/' "$work/report" > "$work/case"
  diff -u "$work/case-expected" "$work/case" || fail "$name in history scan"
}
added_path='{"paths": {"/old": {"get": {}}, "/kept": {"get": {}}, "/new": {"post": {}}}}'
openapi_case 'one added path authorizes the rise' "$health" 8 "$added_path" 0 1
openapi_case 'a rise with an untouched snapshot needs a trailer' "$health" 8 '' 1 0
openapi_case 'a rise greater than the path addition needs a trailer' "$health" 9 "$added_path" 1 0
openapi_case 'a fall despite one addition needs a trailer' "$health" 6 "$added_path" 1 0
openapi_case 'the same assertion in another file needs a trailer' "$other" 8 "$added_path" 1 0
openapi_case 'an almost identical file name needs a trailer' "$similar" 8 "$added_path" 1 0
openapi_case 'another assertion in the OpenAPI test needs a trailer' "$health" 7 "$added_path" 1 0 8
openapi_case 'an added and a removed path need a trailer' "$health" 8 \
  '{"paths": {"/kept": {"get": {}}, "/new": {"post": {}}}}' 1 0
openapi_case 'an unreadable snapshot needs a trailer' "$health" 8 '{"paths":' 1 0
openapi_case 'an extension is not an added path' "$health" 8 \
  '{"paths": {"/old": {"get": {}}, "/kept": {"get": {}}, "x-meta": {}}}' 1 0
openapi_case 'a rewritten number must still rise' "$health" 7_ \
  '{"paths": {"/old": {"post": {}}, "/kept": {"get": {}}}}' 1 0

git checkout -q -B step main
write_count "$health" 9007199254740991
commit '2026-11-13T01:01:00+00:00' 'Save a large expected count'
write_count "$health" 9007199254740993
write_snapshot "$added_path"
commit '2026-11-13T01:02:00+00:00' 'Raise by two where subtraction can round to one'
bash "$facts" --oracle HEAD^ HEAD > "$work/case"
printf '0\t1\t0\t0\t%s\t\n' "$health" > "$work/case-expected"
diff -u "$work/case-expected" "$work/case" || fail 'rounded count needs a trailer'

git checkout -q -B step main
sed -i 's/value < 10/value < 9/' apps/server/tests/properties.rs
commit '2026-11-13T02:00:00+00:00' 'Change a property assertion'
property=$(git rev-parse --short=12 HEAD)
sed -i 's/value, 1/value, 2/' apps/server/tests/macros.rs
cp apps/server/tests/properties.rs apps/server/tests/fixture.txt
commit '2026-11-13T03:00:00+00:00' 'Change a macro assertion'
macro=$(git rev-parse --short=12 HEAD)
printf '0\t0\t0\t0\t\tapps/server/tests/macros.rs apps/server/tests/properties.rs\n' > "$work/unseen-expected"
bash "$facts" --oracle main HEAD > "$work/unseen"
diff -u "$work/unseen-expected" "$work/unseen" || fail 'branch oracle unseen files differ'
printf 'oracle-unseen\t%s\t1\t%s\n' \
  "$macro Change a macro assertion" apps/server/tests/macros.rs \
  "$property Change a property assertion" apps/server/tests/properties.rs | sort > "$work/unseen-expected"
bash "$facts" --commits main..HEAD apps/server > "$work/report"
awk '/^oracle-unseen\t/' "$work/report" > "$work/unseen"
diff -u "$work/unseen-expected" "$work/unseen" || fail 'history oracle unseen files differ'

git rm -q apps/server/tests/properties.rs
commit '2026-11-13T04:00:00+00:00' 'Delete a property test'
printf '0\t0\t0\t0\t\tapps/server/tests/properties.rs\n' > "$work/unseen-expected"
bash "$facts" --oracle HEAD^ HEAD > "$work/unseen"
diff -u "$work/unseen-expected" "$work/unseen" || fail 'a deleted property test is still unseen'

echo 'quality-facts-test: all signals, dates, reproducibility, range and input errors pass'
