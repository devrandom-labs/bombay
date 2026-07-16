# Card #151 — counting allocator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the `Shared → queue → Signal → Sender → Arc<Shared>` cycle reclaims to an exact byte baseline after a mid-backlog receiver drop, via a stable in-gate counting `#[global_allocator]`.

**Architecture:** `CountingAlloc` type in the `test-support` seam; declaration + single test in a dedicated one-test binary (`tests/alloc_exact.rs`) so the counter is process-isolated under both nextest and plain `cargo test`. Warm-up-then-baseline makes the assertion exact without whitelisting lazy init. TDD, falsifiability probe before PR.

**Tech Stack:** stable Rust 1.96, `std::alloc::{GlobalAlloc, System, Layout}`, `AtomicIsize`, flume-backed `Mailbox` sync surface (no tokio).

**Spec:** `docs/superpowers/specs/2026-07-16-151-counting-allocator-design.md`

---

### Task 1: TDD — the test, then `CountingAlloc`

**Files:**
- Create: `bombay-core/tests/alloc_exact.rs`
- Modify: `bombay-core/src/test_support.rs`

- [ ] **Step 1: Write the failing test**

Create `bombay-core/tests/alloc_exact.rs`:

```rust
//! Exact-memory reclamation of the self-pinning signal cycle (card #151).
//!
//! ONE test, in its OWN binary, on purpose: a `#[global_allocator]` counts every
//! allocation in its process, and only a single-test binary is process-isolated
//! under BOTH harnesses (nextest runs per-process anyway; plain `cargo test`
//! shares a process per binary). Adding a second test here would silently break
//! the exactness guarantee — don't.

use std::alloc::System;

use bombay_core::{
    mailbox::{Capacity, Mailbox, Mailboxed},
    test_support::CountingAlloc,
};

#[global_allocator]
static COUNTER: CountingAlloc = CountingAlloc::new(System);

struct Probe;

impl Mailboxed for Probe {
    type Msg = Vec<u8>;
}

/// One round of the cycle the card names: bounded mailbox, N sends (each
/// `Signal::Message` embeds a strong `self_sender` clone — ADR-0003), then the
/// receiver drops MID-BACKLOG (messages still queued), then the sender drops.
fn cycle_round(messages: usize, payload_len: usize) {
    let capacity = Capacity::try_from(messages).expect("valid test capacity");
    let (tx, rx) = Mailbox::<Probe>::bounded(capacity);
    for _ in 0..messages {
        tx.try_send_message(vec![0_u8; payload_len])
            .expect("capacity holds all test messages");
    }
    drop(rx); // mid-backlog: every queued signal still holds a self_sender
    drop(tx);
}

#[test]
fn cycle_reclaims_to_exact_baseline() {
    // Warm-up: one full round BEFORE the baseline, so one-time lazy
    // initialization (harness, flume internals) never pollutes the measurement.
    cycle_round(8, 64);

    let baseline = COUNTER.snapshot();
    cycle_round(8, 64);
    let after = COUNTER.snapshot();

    assert_eq!(
        after, baseline,
        "the queue->Signal->Sender->Arc cycle must reclaim exactly (ADR-0003)"
    );
}
```

Adjust API names to the real mailbox surface if they differ (check `bombay-core/src/mailbox.rs`: `Mailbox::<A>::bounded`, `try_send_message`, `Capacity::try_from`) — but do NOT change the test's shape.

- [ ] **Step 2: Watch it fail to compile**

```bash
nix develop --command cargo test -p bombay-core --test alloc_exact
```
Expected: compile error — `CountingAlloc` not found in `test_support`.

- [ ] **Step 3: Implement `CountingAlloc` in `bombay-core/src/test_support.rs`**

Append to the existing module (keep `terminate_bound()` untouched):

```rust
use core::sync::atomic::{AtomicIsize, Ordering};
use std::alloc::{GlobalAlloc, Layout, System};

/// A live-memory snapshot from a [`CountingAlloc`]: compare two with
/// `assert_eq!` for an exact-reclamation check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    /// Live heap bytes at snapshot time, relative to process start.
    pub bytes: isize,
    /// Live allocation count at snapshot time, relative to process start.
    pub allocs: isize,
}

/// A counting `#[global_allocator]` wrapper (card #151): tracks live bytes and
/// allocation count so a test can assert **exact** reclamation.
///
/// Only `alloc`/`dealloc` are overridden — `GlobalAlloc`'s provided
/// `alloc_zeroed`/`realloc` route through them, so everything is counted with
/// the minimum unsafe surface. Only successful (non-null) allocations count.
///
/// Counters are **signed** so freeing memory allocated before a baseline
/// snapshot cannot underflow. `Relaxed` suffices structurally: the sole
/// consumer is a single-threaded one-test binary (see `tests/alloc_exact.rs`),
/// so every counter op is program-ordered on one thread — the atomics exist
/// only because `GlobalAlloc` must be `Sync`, not for any cross-thread
/// invariant.
pub struct CountingAlloc {
    inner: System,
    live_bytes: AtomicIsize,
    live_allocs: AtomicIsize,
}

impl CountingAlloc {
    /// Wraps the system allocator with zeroed counters.
    #[must_use]
    pub const fn new(inner: System) -> Self {
        Self {
            inner,
            live_bytes: AtomicIsize::new(0),
            live_allocs: AtomicIsize::new(0),
        }
    }

    /// The current live-memory counters. Does not allocate.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            bytes: self.live_bytes.load(Ordering::Relaxed),
            allocs: self.live_allocs.load(Ordering::Relaxed),
        }
    }
}

#[expect(
    clippy::as_conversions,
    reason = "std contract: Layout::from_size_align rejects sizes that overflow \
              isize::MAX rounded up to alignment, so Layout::size() fits isize"
)]
const fn layout_bytes(layout: Layout) -> isize {
    layout.size() as isize
}

// SAFETY: delegates allocation to `System` unchanged; the wrapper only adjusts
// counters and never fabricates, retains, or re-derives pointers.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: caller upholds `GlobalAlloc::alloc`'s contract; forwarded as-is.
        let ptr = unsafe { self.inner.alloc(layout) };
        if !ptr.is_null() {
            self.live_bytes
                .fetch_add(layout_bytes(layout), Ordering::Relaxed);
            self.live_allocs.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: caller upholds `GlobalAlloc::dealloc`'s contract; forwarded as-is.
        unsafe { self.inner.dealloc(ptr, layout) };
        self.live_bytes
            .fetch_sub(layout_bytes(layout), Ordering::Relaxed);
        self.live_allocs.fetch_sub(1, Ordering::Relaxed);
    }
}
```

If clippy demands different `#[expect]` placement or flags something else at the full bar, fix it properly — never relax a lint.

- [ ] **Step 4: Green**

```bash
nix develop --command cargo test -p bombay-core --test alloc_exact
nix develop --command bash -c 'cargo clippy -p bombay-core --all-features && cargo fmt --all'
```
Expected: 1 test passes; clippy clean at the full bar.

- [ ] **Step 5: Falsifiability probe (then revert)**

In `cycle_round`, temporarily replace `drop(rx)` with `std::mem::forget(rx)` — the
receiver (and every queued signal it owns) leaks. Run the test: it MUST fail (live
bytes above baseline). Revert the probe, confirm green again. Report both outcomes.

- [ ] **Step 6: Commit**

```bash
git add bombay-core/tests/alloc_exact.rs bombay-core/src/test_support.rs
git commit -m "test(151): exact-memory counting allocator + mid-backlog cycle test

CountingAlloc on the test-support seam (signed counters, Relaxed with a
structural single-thread proof, alloc/dealloc only); one-test binary so
the counter is process-isolated under both nextest and plain cargo
test. Warm-up round excludes one-time lazy init, so the baseline
assertion is exact. Falsifiability verified via mem::forget probe,
reverted.

Refs #151"
```

---

### Task 2: Docs + gate + PR

**Files:**
- Modify: `docs/testing/coverage-baseline.md`

- [ ] **Step 1: Coverage note**

Append a short section: exact-memory reclamation now asserted in-gate
(`tests/alloc_exact.rs`, dedicated one-test binary, warm-up-then-baseline); #151's MIRI
half was delivered by #150's lane (leak checker in the sweep; the mid-backlog Drop test
runs in both legs).

- [ ] **Step 2: fmt + gate (commit first — the gate is slow)**

```bash
nix develop --command cargo fmt --all
git add -A && git commit -m "docs(testing): coverage note — #151 exact-memory in-gate

Refs #151"
nix flake check
```

- [ ] **Step 3: PR**

```bash
git push -u origin test/151-counting-allocator
gh pr create --repo devrandom-labs/bombay --title "test(actor_ref): exact-memory counting allocator for the self-pin cycle" --body "Closes #151

..."
```

Body must say **Closes #151**, describe both halves (allocator landed here; MIRI half
delivered by #150's miri.yml — cite the sweep's leak checker), and carry NO `#117`
reference (GitHub's closing parser links issue numbers regardless of negation).
Merge on green (`Nix Flake Check` + `miri-gate` both run on PR).
