#!/usr/bin/env bash
# Run from the repository root. The optional tool path permits guard tests.
set -euo pipefail

facts=${1:-$PWD/architecture/design/facts.sh}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null
export GIT_AUTHOR_DATE=2026-11-20T00:00:00Z GIT_COMMITTER_DATE=2026-11-20T00:00:00Z
mkdir "$work/repo"
cd "$work/repo"
git init -q -b main
git config user.name Fixture
git config user.email fixture@example.test
git config commit.gpgsign false
git config core.hooksPath /dev/null
git config diff.renames true
git config log.diffMerges first-parent

library=crates/design-fixture
peer=crates/peer/src
mkdir -p "$library/"{src,tests,fuzz,benches,_entities,migrations} "$peer" \
  crates/{star,group-star}/src apps/consumer/{src,tests} tools/{consumer,large}
cat > "$library/Cargo.toml" <<'TOML'
[package]
name = "review-fixture"
TOML
cat > "$library/src/lib.rs" <<'RUST'
pub mod used;
pub mod unused;
pub(crate) mod restricted;
pub mod inline { pub mod nested; }
// review_fixture::unused
pub mod a;
pub mod b;
pub mod c;
pub mod deep;
pub mod item;
pub mod renamed;
pub mod typed;
pub mod inner;
pub mod false_path;
use review_fixture::unused;
pub mod branch;
RUST
cat > "$library/src/a.rs" <<'RUST'
pub struct Shared {
    id: u64,
    short: u32,
    wrong: Vec<u64>,
}
pub struct Only;
struct Hidden;
pub(crate) struct Restricted;
pub enum LocalNamed { Flag }
mod nested { pub enum LocalNamed { Other } }
enum First { Cold, Hot(u32) }
enum LoneA { Single }
enum LocalA { Left, Right }
enum LocalB { Right, Left }
fn f(id: u64, short: u32, wrong: Vec<u64>) {
    Column::State.eq("live");
    things.len() > 64;
    (1..=64).contains(&things.len());
    Column::Only.eq(3);
    Other::State.eq("not-column");
    Column::State.contains("unsupported");
    things.len() + 64;
    things.len() > maximum;
    (1..=maximum).contains(&things.len());
    status == other;
    state_code == "suffix";
    status + "operator";
    Column::State.eq(other);
    job.STATUS != "blocked";
    // program_rule task_limit task_limit unnamed_rule_extra
    job.status.eq(r#"new"#);
    job.status.replace("ignored");
    identifier.eq("unrelated");
    status == "has\\nescape";
    (minimum..=64).contains(&things.len());
    things.capacity() > 64; things.capacity() > 64; things.capacity() > 64;
}
enum Fields { Record { short: u32 } }
RUST
cat > "$library/src/b.rs" <<'RUST'
pub trait Shared {}
struct Hidden;
pub(crate) struct Restricted;
enum Second<T> { Hot { item: T }, Cold }
enum LoneB { Single }
enum Different { Cold, Warm }
fn f(id: u64, short: u32, wrong: Vec<u64>) {
    entities::ThingColumn::State.eq("live");
    things.len() > 64;
    (1..=64).contains(&things.len());
    Column::Only.eq(3);
    Other::State.eq("not-column");
    Column::State.contains("unsupported");
    things.len() + 64;
    things.len() > maximum;
    (1..=maximum).contains(&things.len());
    64 >= things.len(); 64 >= things.len(); 64 >= things.len();
    "blocked" == device_state;
    // task_limit and program_rule_extra
    (minimum..=64).contains(&things.len());
    let _ = |short: u32| short;
}
RUST
cat > "$peer/c.rs" <<'RUST'
struct Data { id: u64, short: u32, wrong: Vec<u64> }
fn f(id: u64, wrong: Vec<u64>) {
    AnotherColumn::State.eq("live");
    things.len()
        > 64;
    (1..=64).contains(&things.len());
    Other::State.eq("not-column");
    Column::State.contains("unsupported");
    things.len() + 64;
    things.len() > maximum;
    (1..=maximum).contains(&things.len());
}
fn n1(nick: Option<&str>) {}
fn n2(nick: Option < &str >) {}
fn n3(nick: Option<&'static str>) {}
fn n4(nick: std::option::Option<&::core::primitive::str>) {}
fn n5(nick: ::std::option::Option<&str>) {}
RUST
for n in {1..13}; do
  printf 'fn p%s(port: u16) {}\n' "$n" >> "$peer/c.rs"
done
echo 'fn g() { (minimum..=64).contains(&things.len()); }' >> "$peer/c.rs"
echo 'pub mod stray;' >> "$library/src/a.rs"

cat > "$peer/conditions.rs" <<'RUST'
fn conditions() {
    Column::Rank.ne(1); Column::Rank.ne(1); Column::Rank.ne(1);
    Column::Rank.is_null(); Column::Rank.is_null(); Column::Rank.is_null();
    Column::Rank.is_not_null(); Column::Rank.is_not_null(); Column::Rank.is_not_null();
    Column::Rank.is_in([1]); Column::Rank.is_in([1]); Column::Rank.is_in([1]);
    Column::Rank.is_not_in([1]); Column::Rank.is_not_in([1]); Column::Rank.is_not_in([1]);
    Column::Rank.lt(1); Column::Rank.lt(1); Column::Rank.lt(1);
    Column::Rank.lte(1); Column::Rank.lte(1); Column::Rank.lte(1);
    Column::Rank.gt(1); Column::Rank.gt(1); Column::Rank.gt(1);
    Column::Rank.gte(1); Column::Rank.gte(1); Column::Rank.gte(1);
    status == "eq-op";
    status != "ne-op";
    status < "lt-op";
    status <= "le-op";
    status > "gt-op";
    status >= "ge-op";
    status.eq("eq-method");
    status.ne("ne-method");
    status.lt("lt-method");
    status.le("le-method");
    status.lte("lte-method");
    status.gt("gt-method");
    status.ge("ge-method");
    status.gte("gte-method");
    row.status.as_str() == "as-str";
    *status == "dereferenced";
    !status == "not-a-deref";
    row.status.as_str(3) == "not-as-str";
    match row.status.as_str() {
        "arm" | r#"alternative"# => {},
        "guarded" if flag == "guard" => { let _ = "body"; },
        _ => {},
    }
    match other { "other-arm" => {}, _ => {} }
}
fn t1(started: tokio::time::Instant) {}
fn t2(started: std::time::Instant) {}
fn t3(started: Instant) {}
fn t4(started: another::Instant) {}
fn t5(started: Instant) {}
#[cfg(/* test only */ test,)]
// A second attribute must not hide cfg(test).
#[allow(dead_code)]
mod test_support {
    fn f(short: u32) { let _ = status == "test-module"; }
    // constraint_trigger
    #[cfg(test)]
    mod nested { fn f() { status == "nested-test"; } }
} fn production() { status == "after-test-module"; }
#[cfg(test)]
fn test_helper() {}
mod checked { fn f() { status == "outside-test-module"; } }
RUST
echo 'fn f(short: u32) { status == "test-file"; } // constraint_trigger' > "$peer/test.rs"
echo 'fn f(short: u32) { status == "tests-file"; } // constraint_trigger' > "$peer/tests.rs"

for folder in tests fuzz benches _entities; do
  echo 'fn f(short: u32) { let _ = status == "excluded"; }' > "$library/$folder/skip.rs"
done
cat > apps/consumer/src/main.rs <<'RUST'
use review_fixture::used;
use review_fixture::{a, b};
use review_fixture::c as cc;
use { ::review_fixture::{
    {deep},
    item::Item,
    renamed as another,
    branch::{inner},
}, other::{inner}};
use review_fixture::unused_extra;
use other::review_fixture::false_path;
use other::{review_fixture::inner};
fn typed(_: review_fixture::typed::Item) {}
#[cfg(test)] mod test_support { use review_fixture::unused; }
use star::*;
use group_star::{*};
RUST
echo 'pub mod imported;' > crates/star/src/lib.rs
echo 'pub mod imported;' > crates/group-star/src/lib.rs
echo 'use review_fixture::unused;' > apps/consumer/src/test.rs
echo 'use review_fixture::unused;' > apps/consumer/src/tests.rs
echo 'use review_fixture::unused;' > apps/consumer/tests/import.rs
echo 'use review_fixture::unused;' > tools/consumer/main.rs
echo 'not source' > "$peer/notes.txt"
echo '// rename this file' > "$peer/moves.rs"
cat > "$library/migrations/rules.sql" <<'SQL'
ALTER TABLE jobs ADD CONSTRAINT task_limit
    CHECK (amount > 0);
CREATE TRIGGER unnamed_rule AFTER INSERT ON jobs EXECUTE FUNCTION public.program_rule();
CREATE OR REPLACE
FUNCTION public.program_rule() RETURNS trigger AS $body$
    -- CONSTRAINT not_a_rule CHECK (false)
    CREATE FUNCTION inside_body();
$body$ LANGUAGE plpgsql;
/* nested /* CREATE TRIGGER comment_rule */ CONSTRAINT other_rule CHECK (false) */
SELECT 'CREATE TRIGGER string_rule';
ALTER TABLE jobs DROP CONSTRAINT dropped_rule;
-- CREATE TRIGGER line_comment_rule
CREATE TABLE new_jobs (amount integer);
CREATE OR REPLACE TRIGGER replace_trigger
    AFTER INSERT ON jobs EXECUTE FUNCTION public.program_rule();
CREATE CONSTRAINT TRIGGER constraint_trigger
    AFTER INSERT ON jobs EXECUTE FUNCTION public.program_rule();
SELECT E'escaped \' CREATE TRIGGER escape_rule';
SELECT $$CREATE TRIGGER dollar_rule$$;
SELECT $outer$ $inner$ CREATE TRIGGER inner_rule $inner$ $outer$;
SQL
# Keep the last declaration after enough tokens to catch a quadratic scan.
awk 'BEGIN {
  for (i = 0; i < 72000; i++) print "CREATE TABLE jobs (amount integer);"
  print "CREATE TRIGGER last_trigger AFTER INSERT ON jobs EXECUTE FUNCTION public.program_rule();"
}' > tools/large/rules.sql
git add .
git commit -qm fixture

# Put the first change just outside the 400-commit window.
{
  printf 'commit refs/heads/main\ncommitter Fixture <fixture@example.test> 1763596800 +0000\ndata 0\nfrom %s\n\n' "$(git rev-parse HEAD)"
  for ((n = 1; n < 390; n++)); do
    printf 'commit refs/heads/main\ncommitter Fixture <fixture@example.test> 1763596800 +0000\ndata 0\n\n'
  done
} | git fast-import --quiet
for n in {1..10}; do
  printf '// change %d\n' "$n" >> "$library/src/a.rs"
  printf '// change %d\n' "$n" >> tools/consumer/main.rs
  printf 'change %d\n' "$n" >> "$peer/notes.txt"
  if (( n < 10 )); then printf '// change %d\n' "$n" >> "$library/src/b.rs"; fi
  case "$n" in
    9) git mv "$peer/moves.rs" "$peer/moved.rs" ;;
    10) git mv "$peer/moved.rs" "$peer/moves.rs" ;;
    *) printf '// change %d\n' "$n" >> "$peer/moves.rs" ;;
  esac
  git commit -qam "change $n"
done
git switch -qc side
echo '// side change' >> "$library/src/a.rs"
git commit -qam side
git switch -q main
git merge -q --no-ff --no-edit side

cat > "$work/expected" <<'TSV'
condition	(1..=64).contains(&things.len())	3	crates/design-fixture/src/a.rs:18 crates/design-fixture/src/b.rs:10 crates/peer/src/c.rs:6
condition	64 >= things.len()	3	crates/design-fixture/src/b.rs:17 crates/design-fixture/src/b.rs:17 crates/design-fixture/src/b.rs:17
condition	Column::Rank.gt(1)	3	crates/peer/src/conditions.rs:9 crates/peer/src/conditions.rs:9 crates/peer/src/conditions.rs:9
condition	Column::Rank.gte(1)	3	crates/peer/src/conditions.rs:10 crates/peer/src/conditions.rs:10 crates/peer/src/conditions.rs:10
condition	Column::Rank.is_in([1])	3	crates/peer/src/conditions.rs:5 crates/peer/src/conditions.rs:5 crates/peer/src/conditions.rs:5
condition	Column::Rank.is_not_in([1])	3	crates/peer/src/conditions.rs:6 crates/peer/src/conditions.rs:6 crates/peer/src/conditions.rs:6
condition	Column::Rank.is_not_null()	3	crates/peer/src/conditions.rs:4 crates/peer/src/conditions.rs:4 crates/peer/src/conditions.rs:4
condition	Column::Rank.is_null()	3	crates/peer/src/conditions.rs:3 crates/peer/src/conditions.rs:3 crates/peer/src/conditions.rs:3
condition	Column::Rank.lt(1)	3	crates/peer/src/conditions.rs:7 crates/peer/src/conditions.rs:7 crates/peer/src/conditions.rs:7
condition	Column::Rank.lte(1)	3	crates/peer/src/conditions.rs:8 crates/peer/src/conditions.rs:8 crates/peer/src/conditions.rs:8
condition	Column::Rank.ne(1)	3	crates/peer/src/conditions.rs:2 crates/peer/src/conditions.rs:2 crates/peer/src/conditions.rs:2
condition	Column::State.eq("live")	3	crates/design-fixture/src/a.rs:16 crates/design-fixture/src/b.rs:8 crates/peer/src/c.rs:3
condition	things.len() > 64	3	crates/design-fixture/src/a.rs:17 crates/design-fixture/src/b.rs:9 crates/peer/src/c.rs:4
mirror-enum	Cold|Hot	2	crates/design-fixture/src/a.rs:11 First crates/design-fixture/src/b.rs:4 Second
primitive	port: u16	13	crates/peer/src/c.rs:18 crates/peer/src/c.rs:19 crates/peer/src/c.rs:20 crates/peer/src/c.rs:21 crates/peer/src/c.rs:22 crates/peer/src/c.rs:23 crates/peer/src/c.rs:24 crates/peer/src/c.rs:25 crates/peer/src/c.rs:26 crates/peer/src/c.rs:27 crates/peer/src/c.rs:28 crates/peer/src/c.rs:29 +1
primitive	id: u64	5	crates/design-fixture/src/a.rs:2 crates/design-fixture/src/a.rs:15 crates/design-fixture/src/b.rs:7 crates/peer/src/c.rs:1 crates/peer/src/c.rs:2
primitive	nick: Option<&str>	5	crates/peer/src/c.rs:13 crates/peer/src/c.rs:14 crates/peer/src/c.rs:15 crates/peer/src/c.rs:16 crates/peer/src/c.rs:17
primitive	started: Instant	5	crates/peer/src/conditions.rs:36 crates/peer/src/conditions.rs:37 crates/peer/src/conditions.rs:38 crates/peer/src/conditions.rs:39 crates/peer/src/conditions.rs:40
same-name	Shared	2	crates/design-fixture/src/a.rs:1 crates/design-fixture/src/b.rs:1
sql-rule	constraint_trigger	0	crates/design-fixture/migrations/rules.sql:16
sql-rule	replace_trigger	0	crates/design-fixture/migrations/rules.sql:14
sql-rule	unnamed_rule	0	crates/design-fixture/migrations/rules.sql:3
sql-rule	program_rule	1	crates/design-fixture/migrations/rules.sql:4
sql-rule	task_limit	2	crates/design-fixture/migrations/rules.sql:1
state-literal	"live"	3	crates/design-fixture/src/a.rs:16 crates/design-fixture/src/b.rs:8 crates/peer/src/c.rs:3
state-literal	"blocked"	2	crates/design-fixture/src/a.rs:29 crates/design-fixture/src/b.rs:18
state-literal	"after-test-module"	1	crates/peer/src/conditions.rs:49
state-literal	"arm"	1	crates/peer/src/conditions.rs:30
state-literal	"as-str"	1	crates/peer/src/conditions.rs:25
state-literal	"dereferenced"	1	crates/peer/src/conditions.rs:26
state-literal	"eq-method"	1	crates/peer/src/conditions.rs:17
state-literal	"eq-op"	1	crates/peer/src/conditions.rs:11
state-literal	"ge-method"	1	crates/peer/src/conditions.rs:23
state-literal	"ge-op"	1	crates/peer/src/conditions.rs:16
state-literal	"gt-method"	1	crates/peer/src/conditions.rs:22
state-literal	"gt-op"	1	crates/peer/src/conditions.rs:15
state-literal	"gte-method"	1	crates/peer/src/conditions.rs:24
state-literal	"guarded"	1	crates/peer/src/conditions.rs:31
state-literal	"has\\nescape"	1	crates/design-fixture/src/a.rs:34
state-literal	"le-method"	1	crates/peer/src/conditions.rs:20
state-literal	"le-op"	1	crates/peer/src/conditions.rs:14
state-literal	"lt-method"	1	crates/peer/src/conditions.rs:19
state-literal	"lt-op"	1	crates/peer/src/conditions.rs:13
state-literal	"lte-method"	1	crates/peer/src/conditions.rs:21
state-literal	"ne-method"	1	crates/peer/src/conditions.rs:18
state-literal	"ne-op"	1	crates/peer/src/conditions.rs:12
state-literal	"outside-test-module"	1	crates/peer/src/conditions.rs:52
state-literal	r#"alternative"#	1	crates/peer/src/conditions.rs:30
state-literal	r#"new"#	1	crates/design-fixture/src/a.rs:31
unused-module	review_fixture::false_path	0	crates/design-fixture/src/lib.rs:14
unused-module	review_fixture::inner	0	crates/design-fixture/src/lib.rs:13
unused-module	review_fixture::unused	0	crates/design-fixture/src/lib.rs:2
# history
churn	crates/design-fixture/src/a.rs	11	crates/design-fixture/src/a.rs:1
churn	crates/peer/src/moves.rs	10	crates/peer/src/moves.rs:1
TSV

bash "$facts" "$library" "$peer" crates/star crates/group-star > "$work/out"
diff -u "$work/expected" "$work/out"

# Untracked and staged sources, scope order and overlapping scopes must leave
# the report for the same commit byte-identical.
echo 'fn f() { status == "untracked"; }' > "$library/src/untracked.rs"
echo 'fn f() { status == "staged"; }' >> "$peer/c.rs"
git add "$peer/c.rs"
bash "$facts" "$peer" "$library" "$library/src" "$library" crates/group-star crates/star > "$work/again"
cmp "$work/out" "$work/again"

status=0
bash "$facts" absent > "$work/out" 2> "$work/error" || status=$?
if [[ "$status" != 2 || -s "$work/out" || ! -s "$work/error" ]]; then
  echo "design-facts-test: absent scope returned $status or gave an invalid message" >&2
  exit 1
fi
if ! grep -Fxq '# What an absent line means and the known limits' "$facts"; then
  echo 'design-facts-test: missing signal limits in the tool header' >&2
  exit 1
fi

printf 'sql-rule\tlast_trigger\t0\ttools/large/rules.sql:72001\n# history\n' > "$work/expected"
timeout 15 bash "$facts" tools/large > "$work/out"
diff -u "$work/expected" "$work/out"
echo 'design-facts-test: exact report, repeatability, absent scope, header and large SQL pass'
