//! Test-only helpers shared by the unit and integration suites: the MIRI-aware
//! fail-fast bound (card #150) and the exact-memory counting allocator (#151).
//!
//! Behind the `test-support` feature: `tests/*.rs` link the lib externally and
//! cannot reach `pub(crate)`, and `#[doc(hidden)]` is not access control.

use core::sync::atomic::{AtomicIsize, Ordering};
use core::time::Duration;
use std::alloc::{GlobalAlloc, Layout, System};

use futures::stream::AbortHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    actor::{Actor, ActorRef},
    mailbox::{ActorId, MailboxReceiver, MailboxSender, Mailboxed, Signal},
    watch::{LinkReceiver, WatchReg},
};

/// Assembles an [`ActorRef`] over a raw mailbox pair **without spawning a
/// run-loop** (card #118's allocation tests drive the receiver by hand).
///
/// The external-test sibling of the in-crate `ActorRef::new` scaffold:
/// `tests/*.rs` link the lib externally and cannot reach `pub(crate)`.
#[must_use]
pub fn unstarted_actor<A: Actor>(
    (tx, rx): (MailboxSender<A>, MailboxReceiver<A>),
) -> (ActorRef<A>, MailboxReceiver<A>) {
    let (abort, _registration) = AbortHandle::new_pair();
    let actor_ref = ActorRef::new(ActorId::new(0), tx, CancellationToken::new(), abort, None);
    (actor_ref, rx)
}

/// Mints a `Signal::Watch` for an external test/fuzz crate (card #195).
///
/// `WatchReg` and its `LinkSender` live in bombay-core's private `watch` module
/// and are not part of the public API, so a raw `Signal::Watch` cannot be built
/// from an external crate. This builds the watcher's UNBOUNDED link channel
/// internally and hands back the enqueue-able signal plus the [`LinkReceiver`]
/// half, so the caller can optionally observe the `LinkDied` notice delivered
/// when the watched actor stops.
///
/// `watcher` is the watcher's identity (also the unwatch key); `linked` picks a
/// `link` edge (propagating, `true`) over a `watch` edge (notify-only, `false`).
#[must_use]
pub fn watch_signal<A: Mailboxed>(watcher: ActorId, linked: bool) -> (Signal<A>, LinkReceiver) {
    let (link_tx, link_rx) = flume::unbounded();
    let reg = WatchReg {
        watcher,
        link_tx,
        linked,
    };
    (Signal::Watch(Box::new(reg)), link_rx)
}

/// The fail-fast bound for a "this must terminate" await (card #148): a
/// regression that hangs the loop FAILS here instead of stalling the suite.
///
/// Scaled under MIRI. MIRI's virtual clock advances **5 µs per basic block**
/// (`miri/src/clock.rs`: `NANOSECONDS_PER_BASIC_BLOCK = 5000`) — roughly 5000×
/// faster than the work it times — so a natively-calibrated bound fires
/// spuriously under the interpreter, on a test that is making fine progress.
/// Measured (#150): the 8×50-sender race needs ~20 s real under MIRI and passes
/// comfortably inside this bound, while the native 5 s fail-fast is unchanged.
#[must_use]
pub const fn terminate_bound() -> Duration {
    if cfg!(miri) {
        Duration::from_mins(10)
    } else {
        Duration::from_secs(5)
    }
}

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
    gross_allocs: AtomicIsize,
}

impl CountingAlloc {
    /// Wraps the system allocator with zeroed counters.
    #[must_use]
    pub const fn new(inner: System) -> Self {
        Self {
            inner,
            live_bytes: AtomicIsize::new(0),
            live_allocs: AtomicIsize::new(0),
            gross_allocs: AtomicIsize::new(0),
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

    /// Monotonic count of every successful allocation ever (never decremented
    /// by frees). Delta two readings around an operation to assert its **gross**
    /// allocation count — a transient alloc-then-free that live counters cancel
    /// out still shows here (card #118's `tell`-zero-alloc / `ask`-one-alloc
    /// claims are gross claims).
    #[must_use]
    pub fn gross_allocs(&self) -> isize {
        self.gross_allocs.load(Ordering::Relaxed)
    }
}

/// Converts a layout's byte size to a signed count for the live-byte counter.
///
/// `Layout::from_size_align`'s std contract rejects any size that, rounded up
/// to alignment, would overflow `isize::MAX` — so `layout.size()` always fits
/// `isize` and `cast_signed()` never wraps.
const fn layout_bytes(layout: Layout) -> isize {
    layout.size().cast_signed()
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
            self.gross_allocs.fetch_add(1, Ordering::Relaxed);
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

#[cfg(test)]
mod tests {
    use super::*;

    const SIZE: usize = 4096;
    const SIZE_SIGNED: isize = 4096;

    /// `live_bytes` must move by the layout's REAL size, not by any constant.
    ///
    /// This is the only test that can tell the difference. `tests/alloc_exact.rs`
    /// compares two snapshots (`after == baseline`), which a **constant**
    /// `layout_bytes` satisfies trivially: each `alloc` adds the constant and each
    /// `dealloc` subtracts it, so the pair always balances — and with `-> 0`,
    /// `bytes` is always `0` and `0 == 0` passes. That left `Snapshot::bytes`
    /// vacuous and #151's exact-reclamation claim resting on the `allocs` half
    /// alone (card #179; the `-> 0/1/-1` mutants survived before this test).
    ///
    /// `CountingAlloc` is driven **directly** rather than installed as
    /// `#[global_allocator]`: `alloc_exact.rs` must stay a single-test binary for
    /// its exactness guarantee, so the byte-accounting probe cannot live there.
    #[test]
    fn live_bytes_tracks_the_layouts_real_size() {
        let counter = CountingAlloc::new(System);
        let layout = Layout::from_size_align(SIZE, 8).expect("4096/8 is a valid layout");

        let before = counter.snapshot();
        // SAFETY: `layout` has non-zero size; the pointer is freed below with the
        // exact same layout, and is never read or written.
        let ptr = unsafe { counter.alloc(layout) };
        assert!(!ptr.is_null(), "System returned null for a 4 KiB layout");
        let after = counter.snapshot();

        assert_eq!(
            after.bytes - before.bytes,
            SIZE_SIGNED,
            "live_bytes must move by the layout's real size, not a constant"
        );
        assert_eq!(after.allocs - before.allocs, 1, "one live allocation");

        // SAFETY: `ptr` came from this allocator with this exact layout and has
        // not been freed.
        unsafe { counter.dealloc(ptr, layout) };
        assert_eq!(
            counter.snapshot(),
            before,
            "dealloc reverses the exact byte count it added"
        );
    }
}
