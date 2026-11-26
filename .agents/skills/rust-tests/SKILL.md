---
name: rust-tests
description: Select and run Rust unit, snapshot and integration tests, including Insta review, Loco request/model tests and PostgreSQL tests. Route property and fuzz work to their installed skills.
metadata:
  origin: labcast-local
---

# Rust tests

Read [ADR 0004](../../../docs/adr/0004-test-server-behavior.md), which remains
proposed, and the owning operation's acceptance criteria. Check current tools
before adding a dependency. Skills do not approve a test-stack change.

Use ordinary Rust tests for pure domain behavior; Tokio tests for async work;
Tower router tests for HTTP behavior; and a real listener when network lifecycle
is part of the contract. Use disposable PostgreSQL owned by Nix for SeaORM model tests.
Keep fixtures and fakes in the owning tests directory.

For an exact count, test below, at and above the required value for each counted
item independently, with other counts valid. For exactly one, use 0, 1 and 2.
Testing only one side can miss a guard that enforces only the other bound.

Apply ecosystem reuse and design review to tests too. Use suitable maintained
crates to simplify fixtures and repeated cases. Improve one owning area at a time,
preserving every oracle, expected value and required check's coverage.

Run tests through Cargo, or use the exact executable Cargo reports for that
checkout. Do not select test binaries by modification time from a shared Cargo
target directory. It can contain newer artifacts from another checkout or a mutant.
When a mutation checkout shares a target, restoring source bytes with old
modification times can leave compiled mutant artifacts. Refresh the restored
inputs or use a separate target, and confirm Cargo rebuilt the affected crate.
A matching source hash alone does not prove that its executable is clean.

## Database and application boundaries

Choose the test owner from the invariant, not from the application's language.

- Test database constraints, foreign keys, uniqueness and database-role privileges
  in SQL against the migrated schema. Check maintained PostgreSQL tools such as
  [pgTAP](https://pgtap.org/documentation.html) and
  [pg_prove](https://pgtap.org/pg_prove.html) in the locked Nix packages. If none
  fits, record the gap and ask Joel; do not write a Rust harness for database-only
  assertions. Nix owns the tools, disposable database and check; the test files
  stay with the owning application.
- Test business authorization, validation and transitions through the owning
  Loco model operations. Server has no idempotency receipts, version
  preconditions or uncertain-commit outcomes to test. Use real PostgreSQL for transaction, concurrency
  and durable-state claims. SeaORM access in these tests is valid for setup and
  persisted outcomes; do not reproduce the operation under test with test SQL.
- Test API parsing, status codes and response contracts through requests. Use
  focused integration tests where a contract crosses these boundaries; separating
  test responsibilities must not remove end-to-end proof or required failures.

Loco models own business rules and persistence here. Do not introduce a service,
repository or duplicate model layer to organize tests. A table-existence check
proves only existence, not saved ownership, authorization or constraint behavior.
For database constraints, run valid and invalid writes as a role that may write,
and expect the constraint by name (the column for NOT NULL), not only its
SQLSTATE. A privilege refusal or another constraint with the same SQLSTATE can
hide a missing constraint. Each invalid write differs from a valid control only
in the tested value; give other unique keys fresh valid values. Test each foreign
key from both sides against its owner's retention rule: a child that names an
absent parent, and deletion and key change of a parent that only that child
references. Where the owner keeps the child, expect that key by name. Where the
owner selected a cascade or null action, check exactly which rows changed. A
child-only test passes any parent action. Run privilege
cases as the application's runtime login role, not as a superuser. Perform each
permitted operation, which can be only a read, and attempt refused writes with
the expected failure. Check the saved result of each permitted write: an UPDATE
that matches no visible row also succeeds. Do not add a grant to create a
positive control. Also compare that role's effective table and column privileges
in each owned schema, including the migrator's history schema, with an exact
allowlist of every privilege that the running PostgreSQL defines. Name each
privilege by schema and relation; set comparisons ignore duplicates. Include
non-table relations, database and schema `CREATE`, and every role the login is a
member of. Sampled writes miss untried privileges, table-level checks miss column
grants, and pgTAP 1.3.4 `table_privs_are` omits PostgreSQL 17 `MAINTAIN`.
Keep an existing Rust test of a database-only invariant in its required check
until a SQL test in a required check rejects the same faults; then remove the
duplicate in that change.

## Snapshot review

Use Insta for stable externally meaningful output. Redact only unstable values
whose exact value is not under test. Keep authorization, identities, ordering and
error distinctions visible where they matter. Do not snapshot debug output of
private implementation details in place of behavioral assertions.

Generate pending snapshots with the smallest relevant test. Inspect the old and
new content and approve only intended changes with the installed cargo-insta
interface. Do not bulk-accept snapshots merely to turn a failing check green.
Run the affected tests again after acceptance; CI must not auto-update baselines.

If unavailable dependency metadata prevents cargo-insta from running, generate
with `INSTA_UPDATE=new` and review the pending `.snap.new` file. Accept only that
reviewed output with `INSTA_UPDATE=always` on the same test, then rerun it with
`INSTA_UPDATE=no`.

## Other test modes

- Use [property-based-testing](../property-based-testing/SKILL.md) for properties
  over input domains and state transitions.
- Use [cargo-fuzz](../cargo-fuzz/SKILL.md) for parser/codec boundaries and fuzz
  crash reproduction. Nix supplies the required toolchain.
- Use [Loco model tests](../loco/recipes/testing.md) with real PostgreSQL for rollback, concurrent updates and durable work.
- Follow [editorial evaluation](../../../docs/arch/studio/editorial.md#detachable-evaluation)
  for model adapter tests and editorial quality comparisons.

## Layer and schedule evidence

A model test cannot show what an outer layer adds. When the contract states a
status, error body, request deadline or log exclusion, test it through the real
router. For a deadline contract, stall the last slow stage, such as COMMIT, and
assert the response against the outer deadline. For a log exclusion, capture the
log with a test subscriber and assert the rejected value is absent. A rule that
only the model owns keeps its model test.

A branch reached only after a lock wait or race needs a forced schedule. For a
PostgreSQL row wait, hold the row, prove each contender waits with
`pg_blocking_pids` and check that the blocked statement is the intended one.
Release it, then assert the responses and committed state. Include the actual
returned result in a forced-schedule assertion's failure message.
Prove the test can fail: remove that branch in a
throwaway copy, not a shared worktree. This does not apply to other branches.

A green repeat does not close a failed timing or concurrency test. Keep its input
and log. Split the failed interval by owner on one clock. Then reproduce the named
mechanism with a forced fault, or record which segment lacks evidence and leave the
failure open. A later pass is a measurement, not a repair. Do not widen its allowance.

A fault test must prove that it reached its boundary before it acts: a filler is
still blocked, or a contender appears in `pg_blocking_pids`. A run with no rows, a
harness timeout or a failed guard is not a result. Report it and fix the fixture.

A server process test starts through the existing process fixture, even when it
needs another stream or deadline. Extend that fixture. A second start path can lose
child identity, status checks, the bind-only retry, log capture or cleanup.

Choose tests that can reject the original defect or a plausible contract error.
Do not add all test types to every change. Record actual runs and unrun media,
GPU, provider or platform prerequisites in the linked PR.

## Sources

- [Insta](https://insta.rs/docs/)
- [Tokio tests](https://docs.rs/tokio/latest/tokio/attr.test.html)
- [Axum test example](https://github.com/tokio-rs/axum/blob/main/examples/testing/src/main.rs)
- [Loco testing](../loco/recipes/testing.md)
