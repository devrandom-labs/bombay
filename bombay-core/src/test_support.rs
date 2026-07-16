//! Test-only helpers shared by the unit and integration suites (card #150).
//!
//! Behind the `test-support` feature: `tests/*.rs` link the lib externally and
//! cannot reach `pub(crate)`, and `#[doc(hidden)]` is not access control.

use core::sync::atomic::{AtomicIsize, Ordering};
use core::time::Duration;
use std::alloc::{GlobalAlloc, Layout, System};

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
