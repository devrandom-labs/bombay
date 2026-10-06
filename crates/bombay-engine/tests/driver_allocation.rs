use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::convert::Infallible;
use std::future::Future;
use std::hint::{black_box, spin_loop};
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::thread;

use behavior::{Actions, Behavior, BehaviorActed, MailAddr, Never, NoBirths, User};
use bombay_engine::{ActionsOf, Completion};

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy)]
enum AllocationMeasurement {
    Idle,
    Active(usize),
    Overflowed,
}

thread_local! {
    static MEASURED_ALLOCATIONS: Cell<AllocationMeasurement> = const {
        Cell::new(AllocationMeasurement::Idle)
    };
}

fn record_measured_allocation() {
    match MEASURED_ALLOCATIONS.try_with(|measurement| match measurement.get() {
        AllocationMeasurement::Idle | AllocationMeasurement::Overflowed => {}
        AllocationMeasurement::Active(count) => {
            measurement.set(match count.checked_add(1) {
                Some(count) => AllocationMeasurement::Active(count),
                None => AllocationMeasurement::Overflowed,
            });
        }
    }) {
        Ok(()) | Err(_) => {}
    }
}

// SAFETY: every operation delegates unchanged to the system allocator. The
// atomic and const thread-local counters never participate in memory management.
// Const Cell storage requires no allocation. Inaccessible TLS during thread
// teardown is outside a live measurement and must not prevent allocation.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            record_measured_allocation();
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
            record_measured_allocation();
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        record_measured_allocation();
        unsafe { System.realloc(pointer, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct StopOnOne;

impl Behavior for StopOnOne {
    type Protocol = behavior::MessageProtocol<MailAddr, u8>;
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

struct ImmediateEnvironment(Option<User<MailAddr, u8>>);

impl TestActions<StopOnOne> for ImmediateEnvironment {
    type Error = Infallible;
    type Residual = ();

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> Option<<StopOnOne as Behavior>::Event> {
        self.0.take()
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(&mut self, _: ActionsOf<StopOnOne>) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn retire(&mut self) {}
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
fn one_complete_driver_execution_allocates_one_settlement_queue() {
    let driver = direct(
        StopOnOne,
        ImmediateEnvironment(Some(User::new(MailAddr(1), 1))),
    );
    // Count future construction and all polling/drops through return, then end
    // the measurement before assertions or test-harness activity.
    MEASURED_ALLOCATIONS.set(AllocationMeasurement::Active(0));
    let retirement = block_on(driver.run());
    let allocations = match MEASURED_ALLOCATIONS.replace(AllocationMeasurement::Idle) {
        AllocationMeasurement::Active(count) => count,
        AllocationMeasurement::Overflowed => panic!("Driver allocation count overflowed"),
        AllocationMeasurement::Idle => {
            panic!("Driver measurement must retain its allocation count")
        }
    };
    let retirement = match retirement {
        Ok(retirement) => retirement,
        Err(driver) => {
            drop(driver);
            panic!("the immediate allocation host completes original retirement");
        }
    };
    assert!(retirement.additional_failures.is_empty());
    assert!(matches!(retirement.disposition, Ok(Completion::Stopped)));
    assert_eq!(allocations, 1, "Driver allocated {allocations} times");
}

#[derive(Clone, Copy)]
#[repr(usize)]
enum AllocationWork {
    Waiting,
    Ready,
    Requested,
    Retained,
    Released,
}

fn allocation_work(phase: &AtomicUsize) -> AllocationWork {
    match phase.load(Ordering::Acquire) {
        0 => AllocationWork::Waiting,
        1 => AllocationWork::Ready,
        2 => AllocationWork::Requested,
        3 => AllocationWork::Retained,
        4 => AllocationWork::Released,
        _ => unreachable!("only closed allocation-work phases are stored"),
    }
}

#[test]
fn allocations_on_another_thread_do_not_belong_to_driver_execution() {
    let phase = AtomicUsize::new(AllocationWork::Waiting as usize);
    let other_thread_allocations = AtomicUsize::new(0);
    thread::scope(|scope| {
        let worker = scope.spawn(|| {
            phase.store(AllocationWork::Ready as usize, Ordering::Release);
            while !matches!(allocation_work(&phase), AllocationWork::Requested) {
                spin_loop();
            }
            MEASURED_ALLOCATIONS.set(AllocationMeasurement::Active(0));
            let retained = std::array::from_fn::<_, 5, _>(|_| Box::new(black_box([7_u8; 8])));
            black_box(&retained);
            let count = match MEASURED_ALLOCATIONS.replace(AllocationMeasurement::Idle) {
                AllocationMeasurement::Active(count) => count,
                AllocationMeasurement::Overflowed => panic!("other allocation count overflowed"),
                AllocationMeasurement::Idle => {
                    panic!("other thread measurement must retain its allocation count")
                }
            };
            other_thread_allocations.store(count, Ordering::Release);
            phase.store(AllocationWork::Retained as usize, Ordering::Release);
            while !matches!(allocation_work(&phase), AllocationWork::Released) {
                spin_loop();
            }
            drop(retained);
        });
        while !matches!(allocation_work(&phase), AllocationWork::Ready) {
            spin_loop();
        }
        let driver = direct(
            StopOnOne,
            ImmediateEnvironment(Some(User::new(MailAddr(1), 1))),
        );
        let before = ALLOCATIONS.load(Ordering::Relaxed);
        MEASURED_ALLOCATIONS.set(AllocationMeasurement::Active(0));
        phase.store(AllocationWork::Requested as usize, Ordering::Release);
        while !matches!(allocation_work(&phase), AllocationWork::Retained) {
            spin_loop();
        }
        let retirement = block_on(driver.run());
        let allocations = match MEASURED_ALLOCATIONS.replace(AllocationMeasurement::Idle) {
            AllocationMeasurement::Active(count) => count,
            AllocationMeasurement::Overflowed => panic!("Driver allocation count overflowed"),
            AllocationMeasurement::Idle => {
                panic!("Driver measurement must retain its allocation count")
            }
        };
        let global_allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
        phase.store(AllocationWork::Released as usize, Ordering::Release);
        worker.join().expect("other allocation owner completes");
        let retirement = match retirement {
            Ok(retirement) => retirement,
            Err(driver) => {
                drop(driver);
                panic!("the immediate allocation host completes original retirement");
            }
        };
        assert!(retirement.additional_failures.is_empty());
        assert!(matches!(retirement.disposition, Ok(Completion::Stopped)));
        assert_eq!(other_thread_allocations.load(Ordering::Acquire), 5);
        assert!(global_allocations >= 6);
        assert_eq!(allocations, 1, "Driver allocated {allocations} times");
    });
}

#[test]
fn overflowing_allocation_measurement_cannot_report_a_small_count() {
    MEASURED_ALLOCATIONS.set(AllocationMeasurement::Active(usize::MAX));
    record_measured_allocation();
    record_measured_allocation();
    let measurement = MEASURED_ALLOCATIONS.replace(AllocationMeasurement::Idle);
    assert!(matches!(measurement, AllocationMeasurement::Overflowed));
}

mod support;

use support::{TestActions, direct};
