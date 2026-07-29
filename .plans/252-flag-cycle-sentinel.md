# 252 — flag_cycle's awaiting count: unwrap_or(u32::MAX) → expect (banned sentinel)

## Context

Card #252. `crates/core/src/actor/supervision.rs:402` in `Children::flag_cycle`:

```rust
let awaiting = u32::try_from(stops.len()).unwrap_or(u32::MAX);
```

`unwrap_or(sentinel)` on a failed conversion is banned by the arithmetic-safety
rules: a `usize → u32` overflow silently caps `awaiting` at `u32::MAX` and the
set-cycle then waits on MAX deaths that never land — a silent wedge.

Decision (already made, do not revisit): surface overflow as a **panic via
`expect`**, NOT a `Result`. The bound argument holds — `stops` collects at most
one edge per member of the suffix `self.entries[from..]`, so
`stops.len() ≤ entries.len()`, and a child table longer than `u32::MAX` is an
unreachable programmer bug (memory exhausts far earlier). The sole production
caller, `start_or_widen_cycle` (`crates/core/src/actor/kind.rs:713-745`),
already treats the same bound the same way: `checked_add(added).expect(...)`
under `#[expect(clippy::expect_used, reason = ...)]` with a comment arguing
panic-over-silent-wedge. Mirror that site exactly in tone and structure.

Invariants that must hold:
- `awaiting` returned by `flag_cycle` equals `stops.len()` exactly (the count
  of newly flagged AND live members of the suffix) — never capped, never
  saturated.
- No lint relaxation beyond the one item-scoped
  `#[expect(clippy::expect_used, reason = ...)]`.

## Steps

### Step 1 — replace the sentinel conversion (SEQUENTIAL, file: `crates/core/src/actor/supervision.rs`)

In `Children::flag_cycle`, replace line 402:

```rust
let awaiting = u32::try_from(stops.len()).unwrap_or(u32::MAX);
```

with a comment + item-scoped expect mirroring `kind.rs:732-745`:

```rust
// `stops` holds at most one edge per member of the suffix `[from..]`, so its
// length is bounded by the child table length and cannot overflow u32: a
// silent `unwrap_or(MAX)` here would wedge the cycle forever (waiting on MAX
// deaths that never land), so an overflow is surfaced as a panic rather than
// absorbed.
#[expect(
    clippy::expect_used,
    reason = "the awaiting count is bounded by the child table length and \
              cannot overflow u32; an overflow would be an unreachable bug, \
              surfaced as a panic rather than a silent cycle wedge"
)]
let awaiting = u32::try_from(stops.len())
    .expect("awaiting is bounded by the child table length; cannot overflow u32");
```

Expected outcome: no `unwrap_or` remains in `flag_cycle`; `cargo clippy` clean.

### Step 2 — proptest pinning `awaiting == stops.len()` across table sizes (SEQUENTIAL — same file as step 1)

In the existing `#[cfg(test)] mod tests` of `supervision.rs` (helpers
`child_entry`/`handle` at lines ~468-496), add a `proptest!` block styled after
`crates/core/src/restart.rs:878-885`. Repo contract: the test name MUST start
with `prop_` (MIRI sweep skips by that prefix — say so in the doc comment like
restart.rs does).

Add imports to the test module: `use proptest::{prop_assert_eq, proptest};`
(proptest is already a dev-dependency of this crate — do NOT touch Cargo.toml).

Test spec — exercises the table-length boundary the expect's reasoning relies
on, across sizes including the boundaries 0 and 1 and mixed member states:

```rust
proptest! {
    /// `awaiting` is exactly the number of newly flagged live members of the
    /// suffix — never capped or saturated — across table sizes including the
    /// empty-table and empty-suffix boundaries. MIRI-skipped by prefix (the
    /// repo's `prop_` naming contract).
    #[test]
    fn prop_flag_cycle_awaiting_equals_newly_flagged_live_count(
        members in proptest::collection::vec((proptest::bool::ANY, proptest::bool::ANY), 0..16),
        from_seed: usize,
    ) {
        let mut children = Children::new();
        for (i, (live, pre_cycling)) in members.iter().enumerate() {
            let id = ActorId::from_raw_for_test(u64::try_from(i).expect("i < 16") + 1);
            let mut child = child_entry(id);
            if !live {
                child.handle = None;
            }
            child.cycling = *pre_cycling;
            children.insert(id, child);
        }
        // `from` covers the full boundary range [0, len] — len = empty suffix.
        let from = if members.is_empty() { 0 } else { from_seed % (members.len() + 1) };

        let (stops, awaiting) = children.flag_cycle(from);

        let expected = members[from..]
            .iter()
            .filter(|(live, pre_cycling)| *live && !*pre_cycling)
            .count();
        prop_assert_eq!(stops.len(), expected);
        prop_assert_eq!(awaiting as usize, expected);
    }
}
```

Adjust mechanically if needed to satisfy clippy (e.g. cast style — prefer
`usize::try_from(awaiting)` over `as` if the pedantic cast lints fire), but the
asserted invariant must stay exact equality on BOTH `stops.len()` and
`awaiting`.

## Verification

Sandbox rule: do NOT run any test binary (`cargo test`/`cargo nextest`) — they
hang in this sandbox. Verify with:

```
cargo check -p bombay --all-targets
cargo clippy -p bombay --all-targets -- -D warnings
cargo fmt --check
```

Tests run later via `nix flake check` (driven by the controller, not you).

## Out of scope

- `start_or_widen_cycle` / anything in `kind.rs` (already correct).
- Cargo.toml, clippy.toml, lint tables, mutants baseline.
- The other `flag_cycle` unit tests — leave them untouched.
