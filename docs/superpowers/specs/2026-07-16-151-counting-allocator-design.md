# Card #151 — exact-memory counting allocator (the in-gate half)

**Status:** Accepted (2026-07-16)
**Card:** [#151](https://github.com/devrandom-labs/bombay/issues/151) · sub-task of #117

## Summary

#151 has two halves. The **nightly MIRI leak/UB half is already delivered by #150**
(`miri.yml`: MIRI's leak checker is active in the isolation-on sweep, the seeds leg
explores schedules, and `dropping_receiver_mid_backlog_frees_the_queued_message` runs in
both legs). This spec covers the remaining half: a **stable, in-gate, test-only counting
`#[global_allocator]`** proving the self-referential
`Shared → queue → Signal → Sender → Arc<Shared>` cycle reclaims with **exact byte count,
zero leak** after a receiver drops mid-backlog.

## The one load-bearing constraint — process isolation

A `#[global_allocator]` counts **every** allocation in its process. The flake gate runs
**cargo-nextest** (process per test), but plain `cargo test` (rustup users) shares one
process across all tests in a binary — concurrent tests would pollute the counter and
make an exact-baseline assertion flaky.

**Decision (maintainer-approved):** a **dedicated one-test binary**,
`bombay-core/tests/alloc_exact.rs`, holding the `#[global_allocator]` declaration and
exactly one `#[test]`, no tokio. Own binary = own process under *both* harnesses, by
construction. The allocator **type** lives in `test_support` (the reusable seam the card
names); only the declaration and the test live in the binary.

## Design

### `CountingAlloc` (in `bombay-core/src/test_support.rs`)

- Wraps `std::alloc::System`. Implements **only** `alloc` and `dealloc` — the default
  `alloc_zeroed`/`realloc` provided methods route through `self.alloc`/`self.dealloc`,
  so they are counted without extra unsafe surface.
- Counters: `live_bytes: AtomicIsize`, `live_allocs: AtomicIsize` — **signed**, so a
  `dealloc` of memory allocated before a baseline snapshot cannot underflow. The
  `usize → isize` cast carries
  `#[expect(clippy::as_conversions, reason = ...)]` citing the std contract:
  `Layout::from_size_align` rejects sizes that overflow `isize::MAX` when rounded up to
  alignment, so `Layout::size()` always fits `isize`.
- Only **successful** allocations (non-null return) are counted.
- `Relaxed` ordering with a structural proof in the doc: the sole consumer is a
  single-threaded one-test binary, so every counter op is program-ordered on one thread;
  atomicity exists only because `GlobalAlloc` must be `Sync`. No cross-thread invariant
  is claimed.
- `snapshot()` returns a `Snapshot { bytes: isize, allocs: isize }` (`Debug + PartialEq`
  for `assert_eq!` with exact values). `snapshot()` itself does not allocate.
- This module is behind the `test-support` feature ⇒ it gets the **full production
  clippy bar** (deny all/pedantic/nursery) — the #150 lesson, priced in up front.

### The test (`bombay-core/tests/alloc_exact.rs`)

1. **Warm-up round first**: build and destroy a mailbox once *before* taking the
   baseline, so any one-time lazy initialization (harness, flume internals) is excluded
   from the measurement. This keeps the assertion exact without whitelisting.
2. Baseline snapshot.
3. In a scope: `Mailbox::bounded(cap)` → `try_send_message` × N (each embeds the strong
   `self_sender`, building the exact cycle the card names) → **drop the receiver
   mid-backlog** (messages still queued) → drop the sender.
4. After the scope: `assert_eq!(COUNTER.snapshot(), baseline)` — exact bytes **and**
   exact allocation count.

TDD: the test lands first (red — `CountingAlloc` does not exist), then the allocator.
Falsifiability before the PR: a temporary `std::mem::forget` of a queued-signal carrier
must make the assertion fail; then reverted.

## Non-goals

- Running `alloc_exact` under MIRI — the sweep is `--lib` only; MIRI's own leak checker
  already covers this path in the lane. (Extending the sweep to this binary is a
  possible later tweak, not this card.)
- Any new CI: the binary rides the existing `bombay-nextest` flake check automatically
  (the `test-support` feature arrives via the dev-dep, so no `required-features` or
  `Cargo.toml` change is needed).
- Counting-allocator use in other tests (future cards reuse the type via the seam).

## Done =

- `CountingAlloc` + `Snapshot` in `test_support`, full-bar clean.
- `alloc_exact.rs` green under `nix flake check` (nextest) **and** plain `cargo test`.
- Falsifiability verified and reverted.
- Coverage-baseline note; PR closes #151, documenting the MIRI-half-delivered-by-#150
  split.
