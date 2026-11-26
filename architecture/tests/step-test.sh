#!/usr/bin/env bash
# Self-test of the step gate, in a temporary repository. Run from the repository root.
set -euo pipefail

gate=${1:-$PWD/architecture/step.sh}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
mkdir "$work/repo"
cd "$work/repo"
git init --quiet --initial-branch main
git config user.name step-test
git config user.email step-test@example.invalid
mkdir -p app/src app/tests
printf '%s\n' 'fn main() {' '    assert!(true);' '}' > app/src/main.rs
printf '%s\n' '// The score check.' '    assert_eq!(score(), 1);' > app/tests/score.rs
cat > app/tests/multiline.rs <<'RUST'
#[test]
fn score_is_exact() {
    assert_eq!(
        score(),
        1
    );
}
RUST
health=apps/server/tests/requests/health.rs
snapshot=apps/server/tests/requests/snapshots/application__requests__health__openapi.snap
other=apps/server/tests/requests/other.rs
mkdir -p apps/server/tests/requests/snapshots
write_count() {
  printf 'assert_eq!(document["paths"].as_object().unwrap().len(), %s);\n' "$2" > "$1"
}
write_snapshot() {
  printf '%s\n' '---' "source: $health" 'expression: document' '---' "$1" > "$snapshot"
}
write_count "$health" 7
write_count "$other" 7
write_snapshot '{"paths": {"/old": {"get": {}}, "/kept": {"get": {}}}}'
printf '%s\n' 'proptest! { fn check() { prop_assert!(value < 10); } }' > app/tests/properties.rs
printf '%s\n' 'macro_rules! check { () => { assert_eq!(value, 1); }; }' > app/tests/macros.rs
git add . && git commit --quiet --message base

lines() { for _ in $(seq "$1"); do echo "// $2 $RANDOM"; done; }
branch() { git checkout --quiet -B step main && mkdir -p app/src app/tests; }
compare_base=main
expect() {
  local want=$1 name=$2 got=0
  local output
  output=$(STEP_BASE="$compare_base" bash "$gate" 2>&1) || got=$?
  if [ "$got" -ne "$want" ] ||
    { [ -n "${3:-}" ] && ! grep -q "$3" <<< "$output"; } ||
    { [ -n "${4:-}" ] && grep -q "$4" <<< "$output"; }; then
    echo "step-test: FAILED: $name (exit $got, wanted $want)"; echo "$output"; exit 1
  fi
}

expect_exact() {
  local want=$1 name=$2 expected=$3 got=0
  STEP_BASE="$compare_base" bash "$gate" > "$work/result" 2>&1 || got=$?
  printf '%s\n' "$expected" > "$work/expected"
  if [ "$got" -ne "$want" ] || ! cmp -s "$work/expected" "$work/result"; then
    echo "step-test: FAILED: $name (exit $got, wanted $want)"
    diff -u "$work/expected" "$work/result" || true
    exit 1
  fi
}

oracle_error() {
  printf 'error[oracle-stated]: test assertions in this branch: removed=%s changed=%s: %s\n' "$1" "$2" "$3"
  cat <<'EXPECTED'
note: Source: AGENTS.md, Roles and Rules about rules: a test oracle is not weakened to accept a change.
      Fix: keep the assertions. If they moved, or the expectation is wrong, say so in a commit trailer:
      Oracle: moved to <file>   or   Oracle: <why the old expectation was wrong>
      A change of what a test expects is Joel's decision: put it on the card under needs-joel first.
EXPECTED
}

branch; lines 100 test >> app/tests/small.rs
git add . && git commit --quiet --message small
expect 0 "a small step passes"

branch; lines 2000 test >> app/tests/large.rs
git add . && git commit --quiet --message large
expect 0 "a large step passes" '^step: 2000 changed Rust lines, 0 added production lines$'

branch; git mv app/src/main.rs app/src/moved.rs; lines 500 doc >> notes.md
git add . && git commit --quiet --message move
expect 0 "a moved file and a document do not count" '^step: 0 changed Rust lines, 0 added production lines$'

branch; lines 151 production >> app/src/new.rs
git add . && git commit --quiet --message new
expect 1 "new production code without a reuse note fails" 'error\[reuse-note\]'
git commit --quiet --allow-empty --message $'note\n\nReuse: no crate parses this format; checked lib.rs'
expect 0 "a Reuse trailer admits new production code"

branch; lines 150 production >> app/src/edge.rs
git add . && git commit --quiet --message edge
expect 0 "production code at the note threshold passes"

branch; mkdir -p crates/bombay-domain-wire/src crates/bombay-zenoh/src
lines 5 wire >> crates/bombay-domain-wire/src/lib.rs; lines 5 zenoh >> crates/bombay-zenoh/src/lib.rs
git add . && git commit --quiet --message both
expect 1 "a step in two lanes fails" 'error\[one-lane\]'
git commit --quiet --allow-empty --message $'shared\n\nLanes: one contract rename needs both consumers'
expect 0 "a Lanes trailer admits a shared change"

branch; mkdir -p crates/bombay-runtime/src .agents
lines 5 runtime >> crates/bombay-runtime/src/lib.rs; echo "changed rule" >> AGENTS.md
git add . && git commit --quiet --message rules
expect 1 "a step that changes rule files and code fails" 'error\[rules-alone\]'
git commit --quiet --allow-empty --message $'pair\n\nRules: the complexity rule and its last offender change together'
expect 0 "a Rules trailer admits a paired change"

branch; echo "changed rule" >> AGENTS.md; lines 5 doc >> notes.md
git add . && git commit --quiet --message rules-only
expect 0 "a step that changes only rule files passes"

branch; git rm --quiet app/tests/score.rs
git commit --quiet --message 'Delete an assertion'
expect_exact 1 "a deleted assertion needs an Oracle trailer" "$(oracle_error 1 0 app/tests/score.rs)"
git commit --quiet --allow-empty --message $'Mention an oracle in prose\n\nOracle: this line is in the body\n\nEnd of the body.'
expect 1 "a line in the commit body cannot admit the deletion" '^error\[oracle-stated\]'
git commit --quiet --allow-empty --message $'Explain the change\n\nOracle: the old expectation was wrong'
git commit --quiet --allow-empty --message 'Finish the step'
expect_exact 0 "an Oracle trailer admits the deletion" \
  $'step: 2 changed Rust lines, 0 added production lines\nstep: test assertions removed=1 changed=0 (Oracle: the old expectation was wrong)'

branch; git rm --quiet app/tests/score.rs
git commit --quiet --message 'Remove the original test'
printf '%s\n' 'const DESTINATION: bool = true;' '    assert_eq!(score(), 1);' > app/test.rs
git add . && git commit --quiet --message 'Move the assertion into a new test'
expect 0 "an assertion moved across files and commits passes" '^step: ' '[Oo]racle'

branch; printf '%s\n' '// The score check.' '    assert_eq!(score(), 2);' > app/tests/score.rs
git add . && git commit --quiet --message 'Change the expected value'
expect_exact 1 "changing an expected value needs an Oracle trailer" "$(oracle_error 0 1 app/tests/score.rs)"

branch; sed -i 's/^        1$/        2/' app/tests/multiline.rs
git add . && git commit --quiet --message 'Change the expected value on a continuation line'
expect_exact 1 "changing a continuation line needs an Oracle trailer" \
  "$(oracle_error 0 1 app/tests/multiline.rs)"

added_path='{"paths": {"/old": {"get": {}}, "/kept": {"get": {}}, "/new": {"post": {}}}}'
branch; write_count "$health" 8
git add . && git commit --quiet --message 'Raise the OpenAPI path count by one'
expect_exact 1 "a rise without a snapshot change needs a trailer" "$(oracle_error 0 1 "$health")"
write_snapshot "$added_path"
git add . && git commit --quiet --message 'Add one path to the snapshot'
expect_exact 0 "one added path in a later commit authorizes the rise on the branch" \
  'step: 2 changed Rust lines, 0 added production lines'

branch; write_count "$health" 9; write_snapshot "$added_path"
git add . && git commit --quiet --message 'Raise the count by two and add one path'
expect_exact 1 "a rise greater than the path addition needs a trailer" "$(oracle_error 0 1 "$health")"

branch; write_count "$health" 6; write_snapshot "$added_path"
git add . && git commit --quiet --message 'Lower the count despite adding a path'
expect_exact 1 "a fall needs a trailer" "$(oracle_error 0 1 "$health")"

branch; write_count "$other" 8; write_snapshot "$added_path"
git add . && git commit --quiet --message 'Raise the same count in another test'
expect_exact 1 "the same assertion in another file needs a trailer" "$(oracle_error 0 1 "$other")"

branch; write_count "$health" 8; write_snapshot '{"paths": {"/kept": {"get": {}}, "/new": {"post": {}}}}'
git add . && git commit --quiet --message 'Raise the count and replace a path'
expect_exact 1 "an added and a removed path need a trailer" "$(oracle_error 0 1 "$health")"

branch; write_count "$health" 8; write_snapshot '{"paths":'
git add . && git commit --quiet --message 'Raise the count with an unreadable snapshot'
expect_exact 1 "an unreadable snapshot needs a trailer" "$(oracle_error 0 1 "$health")"

branch; sed -i 's/value < 10/value < 9/' app/tests/properties.rs
sed -i 's/value, 1/value, 2/' app/tests/macros.rs
git add . && git commit --quiet --message 'Change assertions inside macro bodies'
expect_exact 0 "unseen bodies print one note and do not require a trailer" \
  $'note: app/tests/macros.rs app/tests/properties.rs hold proptest or macro bodies that this check does not read\nstep: 4 changed Rust lines, 0 added production lines'

branch; printf '%s\n' '    assert_eq!(score(), 2);' > app/tests/new.rs
git add . && git commit --quiet --message 'Add an assertion in a new test file'
expect_exact 0 "a new test passes silently" 'step: 1 changed Rust lines, 0 added production lines'

branch; printf '%s\n' 'fn main() {}' > app/src/main.rs
git add . && git commit --quiet --message 'Change production code only'
mkdir "$work/bin"
printf '#!%s\n' "$BASH" > "$work/bin/jq"
cat >> "$work/bin/jq" <<'BASH'
echo 'step-test: the oracle scan ran' >&2
exit 99
BASH
chmod +x "$work/bin/jq"
PATH="$work/bin:$PATH" \
  expect_exact 0 "production-only changes skip the oracle scan" \
    'step: 4 changed Rust lines, 1 added production lines'

branch; printf '%s\n' 'fn main() {}' > app/src/main.rs
printf '%s\n' '// Check the initial score.' '    assert_eq!(score(), 1);' > app/tests/score.rs
git add . && git commit --quiet --message 'Edit a test comment and remove a production assertion'
expect 0 "only assertions in test files count" '^step: ' '[Oo]racle'

branch; printf '%s\n' '    assert!(another_check());' >> app/tests/score.rs
git add . && git commit --quiet --message 'Advance the base with a new assertion'
git branch advanced
branch; echo "step notes" > notes.md
git add . && git commit --quiet --message 'Change a document on the branch'
compare_base=advanced expect 0 "the branch uses the merge base" '^step: ' '[Oo]racle'

session=crates/bombay-runtime/src/session.rs
admission=crates/bombay-runtime/tests/session.rs

branch; mkdir -p "$(dirname "$session")"
cat > "$session" <<'RUST'
trait Session {
    fn admit((a, b): (u8, u8));
}
// ast-grep-ignore
fn from((a, b): (u8, u8)) {}
RUST
git add . && git commit --quiet --message 'Pack parameters in a trait and a free function'
expect 1 "a trait signature cannot pack tuple parameters" "^error\[signature-design\]: tuple-parameter-packing at $session:2 (admit)$"
expect 1 "a free function named from is still checked" "^error\[signature-design\]: tuple-parameter-packing at $session:5 (from)$"

cat > "$session" <<'RUST'
impl From<(u8, u8)> for Pair {
    fn from(#[allow(unused)] (a, b): (u8, u8)) -> Self { Self { a, b } }
}
impl ::core::convert::/* standard */ TryFrom<(u8, u8)> for OtherPair {
    type Error = Error;
    fn try_from((a, b): (u8, u8)) -> Result<Self, Error> { Ok(Self { a, b }) }
}
impl std::prelude::v1::From::<(u8, u8)> for model::ThirdPair {
    fn from((a, b): (u8, u8)) -> Self { Self { a, b } }
}
impl<Item> ::std::prelude::rust_2021::TryFrom::<(Item, Item)> for FourthPair<Item> {
    type Error = Error;
    fn try_from((a, b): (Item, Item)) -> Result<Self, Error> { Ok(Self { a, b }) }
}
RUST
git add . && git commit --quiet --message 'Destructure tuple input in standard conversions'
expect_exact 0 "From and TryFrom can destructure their single argument" 'step: 14 changed Rust lines, 14 added production lines'

cat > "$session" <<'RUST'
fn queued(a: impl FnOnce(), b: impl AsyncFnOnce(), c: &dyn FnMut()) {}
fn bounds<F1, F2, F3>(a: F1, b: &F2, c: Box<F3>)
where F1: FnOnce(), F2: FnMut(), F3: Fn() {}
trait Session {
    fn inline<Open: FnOnce(), Close>(a: Open, b: Close, c: impl Fn())
    where Close: for<'a> Fn(&'a u8);
}
fn packed((a, (b, c)): (impl FnOnce(), (impl FnMut(), impl Fn()))) {}
fn associated<Port: Factory>(a: Port::Callback, b: Port::Callback, c: Port::Callback)
where for<'a> Port::Callback: Fn(&'a str) {}
fn optional(a: Option<(u8, impl Fn())>, b: impl Fn(), c: impl Fn()) {}
fn grouped<Callback>(a: Callback, b: Callback, c: Callback) where (Callback): (Fn()) {}
RUST
git add . && git commit --quiet --message 'Pass three callbacks by several routes'
expect 1 "three impl and dyn callbacks fail" "^error\[signature-design\]: callback-soup at $session:1 (queued)$"
expect 1 "three where-bounded callbacks fail" "^error\[signature-design\]: callback-soup at $session:2 (bounds)$"
expect 1 "inline and higher-ranked bounds count in trait signatures" "^error\[signature-design\]: callback-soup at $session:5 (inline)$"
expect 1 "callbacks packed into a nested tuple count separately" "^error\[signature-design\]: callback-soup at $session:8 (packed)$"
expect 1 "a where bound on a named associated type counts" "^error\[signature-design\]: callback-soup at $session:9 (associated)$"
expect 1 "an optional tuple still carries a callback" "^error\[signature-design\]: callback-soup at $session:11 (optional)$"
expect 1 "parentheses do not hide a closure bound" "^error\[signature-design\]: callback-soup at $session:12 (grouped)$"

cat > "$session" <<'RUST'
fn two<F1, F2, Unused>(a: F1, b: F2)
where F1: FnOnce() + Send, F2: FnMut(), Unused: Fn() {}
fn nested(a: impl Fn() -> Box<dyn Fn()>, b: &dyn Fn(), c: fn() -> Box<dyn Fn()>) {}
fn repeated<Callback: Fn()>(a: Callback, b: Callback) {}
fn marker<Callback: Fn()>(a: Callback, b: Callback, c: &std::marker::PhantomData<Callback>) {}
fn reader<Callback: Fn()>(a: Callback, b: Callback, c: impl AsRef<Callback>) {}
fn grouped_impl<Callback: Fn()>(a: Callback, b: Callback, c: impl (AsRef<Callback>)) {}
fn grouped_dyn<Callback: Fn()>(a: Callback, b: Callback, c: &dyn (AsRef<Callback>)) {}
fn ranked_dyn<Callback: Fn()>(a: Callback, b: Callback, c: &dyn for<'a> AsRef<&'a Callback>) {}
RUST
git add . && git commit --quiet --message 'Pass two callable values'
expect_exact 0 "two callbacks pass and unused, nested and fn-pointer types do not count" \
  'step: 9 changed Rust lines, 9 added production lines'

cat > "$session" <<'RUST'
pub fn admit_controls<H, T, S, R, E>() {}
pub async fn start_inputs<'a, S, H, B, P, D, E>() {}
struct Packed<A, B, C>(A, B, C);
enum Reply<D, E, F> { Data(D, E, F) }
trait Request<r#G, r#H, r#I> {}
RUST
git add . && git commit --quiet --message 'Declare single-letter type parameters'
expect 1 "admit_controls has too many unnamed types" "^error\[signature-design\]: single-letter-generics at $session:1 (admit_controls)$"
expect 1 "start_inputs has too many unnamed types" "^error\[signature-design\]: single-letter-generics at $session:2 (start_inputs)$"
expect 1 "struct type parameters are checked" "^error\[signature-design\]: single-letter-generics at $session:3 (Packed)$"
expect 1 "enum type parameters are checked" "^error\[signature-design\]: single-letter-generics at $session:4 (Reply)$"
expect 1 "trait type parameters are checked" "^error\[signature-design\]: single-letter-generics at $session:5 (Request)$"

cat > "$session" <<'RUST'
pub fn ordinary<'a, T, E, const X: usize>() {}
pub fn named<HoldProfile, Transition, RestrictionsError>() {}
RUST
git add . && git commit --quiet --message 'Give types domain names'
expect_exact 0 "two letters, lifetimes, consts and domain names pass" \
  'step: 2 changed Rust lines, 2 added production lines'

cat > "$session" <<'RUST'
pub fn plain() -> Result<(Option<u32>, u32), Err> { todo!() }
pub async fn qualified() -> std::result::Result</* ok */ (u8, u8), Err> { todo!() }
pub fn parenthesized() -> (Result<((u8, u8) /* group */), Err>) { todo!() }
pub fn turbofish() -> Result::<(u8, u8), Err> { todo!() }
pub fn qualified_turbofish() -> ::core::result::Result::<(u8, u8), Err> { todo!() }
pub fn grouped_turbofish() -> (Result::<(u8, u8), Err>) { todo!() }
RUST
git add . && git commit --quiet --message 'Return public result tuples'
expect 1 "an Ok tuple fails in a public function" "^error\[signature-design\]: pub-result-tuple at $session:1 (plain)$"
expect 1 "a qualified Result tuple fails in a public async function" "^error\[signature-design\]: pub-result-tuple at $session:2 (qualified)$"
expect 1 "extra parentheses leave the public tuple unchanged" "^error\[signature-design\]: pub-result-tuple at $session:3 (parenthesized)$"
expect 1 "a turbofish leaves the public tuple unchanged" "^error\[signature-design\]: pub-result-tuple at $session:4 (turbofish)$"
expect 1 "a qualified turbofish leaves the public tuple unchanged" "^error\[signature-design\]: pub-result-tuple at $session:5 (qualified_turbofish)$"
expect 1 "parentheses around a turbofish leave the public tuple unchanged" "^error\[signature-design\]: pub-result-tuple at $session:6 (grouped_turbofish)$"

cat > "$session" <<'RUST'
pub fn ok_unit() -> Result<(), Err> { todo!() }
pub fn one_field() -> Result<(u8, /* trailing comment */), Err> { todo!() }
pub fn one_tuple_field() -> Result<((u8, u8),), Err> { todo!() }
fn private_tuple() -> Result<(u8, u8), Err> { todo!() }
pub(crate) fn crate_tuple() -> Result<(u8, u8), Err> { todo!() }
pub fn named() -> Result<Admitted, Err> { todo!() }
pub fn error_tuple() -> Result<u8, (u8, u8)> { todo!() }
pub fn turbofish_unit() -> Result::<(), Err> { todo!() }
RUST
git add . && git commit --quiet --message 'Keep named and local results'
expect_exact 0 "only plain pub functions with two or more Ok fields fail" \
  'step: 8 changed Rust lines, 8 added production lines'

git checkout --quiet main
mkdir -p "$(dirname "$session")" "$(dirname "$admission")"
cat > "$session" <<'RUST'
pub fn legacy<A, B, C>() {
    keep();
    remove();
}
fn clean() {}
RUST
cat > "$admission" <<'RUST'
fn legacy() { unreachable!("unused"); }
RUST
git add . && git commit --quiet --message 'Keep legacy code and tests on main'

branch; sed -i 's/fn clean() {}/fn clean() { keep(); }/' "$session"
cat >> "$session" <<'RUST'
#[cfg(test)]
#[allow(dead_code)]
mod tests {
    fn helper((a, b): (u8, u8)) {}
}
RUST
git add . && git commit --quiet --message 'Edit a clean function and add an inline test module'
expect_exact 0 "untouched legacy functions and inline test modules are skipped" \
  'step: 7 changed Rust lines, 6 added production lines'
{ printf '%s\n' '// Session entry points.'; cat "$session"; } > "$work/preamble"
mv "$work/preamble" "$session"
git add . && git commit --quiet --message 'Add a comment above the legacy function'
git config diff.interHunkContext 50
expect_exact 0 "Git context between hunks does not touch legacy code" \
  'step: 8 changed Rust lines, 7 added production lines'
git config --unset diff.interHunkContext
sed -i 's/^    keep();/    changed();/' "$session"
git add . && git commit --quiet --message 'Edit the body of the legacy function'
expect_exact 1 "a body hunk intersects the whole legacy function" \
  "error[signature-design]: single-letter-generics at $session:2 (legacy)
note: See bombay.md, Signature & Module Integrity. Use concrete domain types or name each type parameter for the role it serves."

branch; sed -i '/^    remove();$/d' "$session"
git add . && git commit --quiet --message 'Delete one line without adding any'
expect_exact 0 "pure deletion does not touch a legacy signature" 'step: 1 changed Rust lines, 0 added production lines'

branch; renamed=${session%.rs}-renamed.rs
git mv "$session" "$renamed"
git commit --quiet --message 'Move a legacy source file'
expect_exact 0 "pure rename does not touch a legacy signature" 'step: 0 changed Rust lines, 0 added production lines'
sed -i 's/fn clean() {}/fn clean() { keep(); }/' "$renamed"
git add . && git commit --quiet --message 'Edit a clean function in the renamed file'
expect_exact 0 "an edited rename compares the old and new source" 'step: 2 changed Rust lines, 1 added production lines'

branch; cat >> "$admission" <<'RUST'
// ast-grep-ignore
fn new() { std::unreachable!("no check"); }
RUST
git add . && git commit --quiet --message 'Add a dummy callback to tests'
expect_exact 1 "unreachable calls cannot rise even with a suppression" \
  $'error[test-unreachable]: test unreachable!() calls rose from 1 to 2 in changed test files\nnote: See bombay.md, Tests. Use a stateful fake or assert the expected variant.'

branch; cat >> "$admission" <<'RUST'
fn nested() {
    assert_eq!(Some(1u8).unwrap_or_else(|| unreachable /* name */ !()), 1);
}
RUST
git add . && git commit --quiet --message 'Add a dummy callback inside an assertion'
expect 1 "an outer macro does not hide a nested call" \
  '^error\[test-unreachable\]: test unreachable!() calls rose from 1 to 2 in changed test files$'

branch; cat >> "$admission" <<'RUST'
macro_rules! on_close {
    () => { || unreachable!() };
    (unreachable!()) => { || () };
    ($($name:ident),+) => { $(let $name = || unreachable!();)* };
}
fn example() { let _callback = on_close!(); }
RUST
git add . && git commit --quiet --message 'Add a dummy callback to a macro expansion body'
expect 1 "macro expansion calls and repetitions count and a macro pattern does not" \
  '^error\[test-unreachable\]: test unreachable!() calls rose from 1 to 3 in changed test files$'

branch; cat >> "$admission" <<'RUST'
// unreachable!("comment");
/* unreachable!("block comment"); */
const MESSAGE: &str = "unreachable!(\"string\")";
fn nested_text() { assert_eq!(r#"unreachable!()"#, /* unreachable!() */ MESSAGE); }
RUST
git add . && git commit --quiet --message 'Add only text that resembles a macro call'
expect_exact 0 "comments and strings do not raise the count" 'step: 4 changed Rust lines, 0 added production lines'
sed -i '/^fn legacy()/d' "$admission"
git add . && git commit --quiet --message 'Remove a dummy callback'
expect_exact 0 "a lower count passes" 'step: 5 changed Rust lines, 0 added production lines'

branch; mkdir -p crates/contracts/tests
git mv "$admission" crates/contracts/tests/moved.rs
git commit --quiet --message 'Move a test between scanned scopes'
expect_exact 0 "moving a test preserves the count" 'step: 0 changed Rust lines, 0 added production lines'

branch; cat >> "$session" <<'RUST'
fn bad_atomic(flag: &std::sync::atomic::AtomicUsize) -> usize {
    flag.load(Ordering::SeqCst)
}
RUST
git add . && git commit --quiet --message 'Add SeqCst without justification'
expect 1 "fitness ratchet blocks new Ordering::SeqCst in crates/" 'error\[design-ratchet\]'
git commit --quiet --allow-empty --message $'justify\n\nDesign: linearizable hardware fence proof in ADR'
expect 0 "Design trailer admits an explicit fitness exception"

echo "step-test: 67 cases pass"
