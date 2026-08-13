//! Process-isolated allocation-retention gate for bombay composition.
//!
//! This is intentionally one test binary with one test: a global allocator is
//! process-wide. Primitive hot-path allocation laws remain in their Bombay
//! owners; this oracle proves repeated bombay spawn/send/stop/retirement
//! composition returns its owned allocations to an isolated warm baseline.

use std::alloc::{GlobalAlloc, Layout, System as Allocator};
use std::sync::atomic::{AtomicIsize, Ordering};

use bombay::behavior::{Actions, Delivery, Exit, Handler, MailAddr, Never, NoBirths, Pure};
use bombay::{AddressRouter, MailboxConfig, RunExit, System, TaskOutcome};

struct Counting;

static LIVE_ALLOCATIONS: AtomicIsize = AtomicIsize::new(0);

// SAFETY: this delegates every operation unchanged to the system allocator and
// only maintains an atomic count after successful allocation. The counter is
// diagnostic state and does not participate in allocation or deallocation.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { Allocator.alloc(layout) };
        if !pointer.is_null() {
            LIVE_ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE_ALLOCATIONS.fetch_sub(1, Ordering::Relaxed);
        unsafe { Allocator.dealloc(pointer, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { Allocator.alloc_zeroed(layout) };
        if !pointer.is_null() {
            LIVE_ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        unsafe { Allocator.realloc(pointer, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

struct Stop;

impl Handler<u8> for Stop {
    type Addr = MailAddr;
    type Msg = u8;

    fn receive(
        &mut self,
        _from: MailAddr,
        _message: u8,
    ) -> bombay::behavior::Acted<MailAddr, Never, Vec<Delivery<MailAddr, u8>>, NoBirths, Never>
    {
        Ok(Actions::stop(Exit::Normal))
    }
}

async fn cycle(address: u64) {
    let system = System::new(MailboxConfig::bounded(1), AddressRouter::default());
    let actor = system
        .spawn(MailAddr(address), Pure::new(Stop))
        .expect("vacant allocation-oracle address");
    actor
        .actor_ref()
        .send(MailAddr(0), 1)
        .await
        .expect("allocation-oracle delivery");
    assert!(matches!(
        actor.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
}

const fn returns_to_baseline(growth: isize) -> bool {
    growth <= 0
}

#[test]
fn repeated_runtime_composition_returns_allocations_to_warm_baseline() {
    assert!(
        !returns_to_baseline(1),
        "positive growth must fail the gate"
    );
    assert!(returns_to_baseline(0));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("allocation-oracle runtime");

    for address in 0..64 {
        runtime.block_on(cycle(address));
    }
    let baseline = LIVE_ALLOCATIONS.load(Ordering::Relaxed);
    for address in 64..128 {
        runtime.block_on(cycle(address));
    }
    let growth = LIVE_ALLOCATIONS.load(Ordering::Relaxed) - baseline;

    assert!(
        returns_to_baseline(growth),
        "64 completed bombay compositions retained {growth} allocations above the warm baseline"
    );
}
