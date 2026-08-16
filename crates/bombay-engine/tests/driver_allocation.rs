use std::alloc::{GlobalAlloc, Layout, System};
use std::convert::Infallible;
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use behavior::{Actions, Behavior, BehaviorActed, MailAddr, Never, NoBirths, User};
use bombay_engine::{ActionsOf, ActiveEnvironment, Completion};

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every operation delegates unchanged to the system allocator. The
// diagnostic counter is atomic and never participates in memory management.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(pointer, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct StopOnOne;

impl Behavior for StopOnOne {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = User<MailAddr, u8>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        assert_eq!(event.message, 1);
        Ok(Actions::stop())
    }
}

struct ImmediateEnvironment(bool);

impl ActiveEnvironment<StopOnOne> for ImmediateEnvironment {
    type Error = Infallible;

    async fn next(&mut self) -> Option<<StopOnOne as Behavior>::Event> {
        (!std::mem::replace(&mut self.0, true)).then(|| User::new(MailAddr(1), 1))
    }

    async fn apply(&mut self, _: ActionsOf<StopOnOne>) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn retire(self) {}
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("immediate environment unexpectedly pended"),
    }
}

#[test]
fn one_complete_driver_execution_allocates_nothing() {
    let driver = direct(StopOnOne, ImmediateEnvironment(false));
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    assert_eq!(block_on(driver.run()), Ok(Completion::Stopped));
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
    assert_eq!(allocations, 0, "Driver allocated {allocations} times");
}
mod support;

use support::direct;
