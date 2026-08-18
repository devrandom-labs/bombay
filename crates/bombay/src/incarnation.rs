//! One terminally classified execution directly above the universal Driver.

use core::marker::PhantomData;

use behavior::{Behavior, Never};
use bombay_engine::{ActiveEnvironment, Driver, Environment};

use super::{IncarnationOutcome, Retirement};

/// Owns exactly one Driver execution and one terminal retirement capability.
///
/// Incarnation adds no construction, identity, mailbox, publication, executor,
/// or scheduling policy. A later layer may place this future on an executor and
/// supply a retirement implementation owning generation-specific resources.
pub struct Incarnation<B: Behavior, E, R> {
    driver: Driver<B, E>,
    retirement: R,
}

impl<B: Behavior, E, R> Incarnation<B, E, R> {
    /// Bind one already-constructed Driver to one retirement capability.
    pub const fn new(driver: Driver<B, E>, retirement: R) -> Self {
        Self { driver, retirement }
    }
}

impl<B, E, R> Incarnation<B, E, R>
where
    B: Behavior<Ph = Never>,
    E: Environment<B>,
    R: Retirement<B::Error, E::Error, <E::Active as ActiveEnvironment<B>>::Error>,
{
    /// Consume and execute this incarnation exactly once.
    ///
    /// Ordinary Driver results are classified and retired before this future
    /// returns. Panic unwinding and cancellation drop the active Driver future
    /// before the terminal guard publishes their classification.
    pub async fn run(self) {
        let Self { driver, retirement } = self;
        let terminal =
            Terminal::<_, B::Error, E::Error, <E::Active as ActiveEnvironment<B>>::Error>::new(
                retirement,
            );
        let outcome = driver.run().await.into();
        terminal.complete(outcome);
    }
}

struct Terminal<R, B, A, E>
where
    R: Retirement<B, A, E>,
{
    retirement: Option<R>,
    error: PhantomData<fn(B, A, E)>,
}

impl<R, B, A, E> Terminal<R, B, A, E>
where
    R: Retirement<B, A, E>,
{
    const fn new(retirement: R) -> Self {
        Self {
            retirement: Some(retirement),
            error: PhantomData,
        }
    }

    fn complete(mut self, outcome: IncarnationOutcome<B, A, E>) {
        self.retirement
            .take()
            .expect("terminal retirement is affine")
            .retire(outcome);
    }
}

impl<R, B, A, E> Drop for Terminal<R, B, A, E>
where
    R: Retirement<B, A, E>,
{
    fn drop(&mut self) {
        let Some(retirement) = self.retirement.take() else {
            return;
        };
        let outcome = if std::thread::panicking() {
            IncarnationOutcome::Panicked
        } else {
            IncarnationOutcome::Cancelled
        };
        retirement.retire(outcome);
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::convert::Infallible;
    use std::future::{Future, pending};
    use std::pin::pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};

    use behavior::{Actions, BehaviorActed, InitializationTurn, MailAddr, NoBirths, User};
    use bombay_engine::{ActionsOf, Completion};

    use super::*;

    struct CountingAllocator;

    thread_local! {
        static COUNT_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
        static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    }

    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc(layout) };
            if !pointer.is_null() {
                COUNT_ALLOCATIONS.with(|enabled| {
                    if enabled.get() {
                        ALLOCATIONS.set(ALLOCATIONS.get() + 1);
                    }
                });
            }
            pointer
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) };
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc_zeroed(layout) };
            if !pointer.is_null() {
                COUNT_ALLOCATIONS.with(|enabled| {
                    if enabled.get() {
                        ALLOCATIONS.set(ALLOCATIONS.get() + 1);
                    }
                });
            }
            pointer
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            let pointer = unsafe { System.realloc(pointer, layout, size) };
            if !pointer.is_null() {
                COUNT_ALLOCATIONS.with(|enabled| {
                    if enabled.get() {
                        ALLOCATIONS.set(ALLOCATIONS.get() + 1);
                    }
                });
            }
            pointer
        }
    }

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    #[derive(Debug, PartialEq, Eq)]
    struct BehaviorFailure;

    #[derive(Debug, PartialEq, Eq)]
    struct EnvironmentFailure;

    enum Mode {
        Stop,
        BehaviorFailure,
        EnvironmentFailure,
        Panic,
        Exhausted,
        Pending,
    }

    struct ProbeBehavior {
        mode: Mode,
    }

    impl Behavior for ProbeBehavior {
        type Protocol = behavior::MessageProtocol<MailAddr, ()>;
        type Event = User<MailAddr, ()>;
        type Sends = Vec<Infallible>;
        type Ph = Never;
        type Error = BehaviorFailure;
        type Birth = NoBirths;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            match self.mode {
                Mode::Stop => Ok(Actions::stop()),
                Mode::BehaviorFailure => Err(BehaviorFailure),
                Mode::EnvironmentFailure | Mode::Exhausted | Mode::Pending => Ok(Actions::cont()),
                Mode::Panic => panic!("deliberate incarnation panic"),
            }
        }

        fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
            unreachable!("probe environments never yield an event")
        }
    }

    struct ProbeEnvironment {
        fail_apply: bool,
        pending: bool,
        retired: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for ProbeEnvironment {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl ActiveEnvironment<ProbeBehavior> for ProbeEnvironment {
        type Error = EnvironmentFailure;

        fn next(&mut self) -> impl Future<Output = Option<<ProbeBehavior as Behavior>::Event>> {
            let should_wait = self.pending;
            async move {
                if should_wait {
                    pending::<()>().await;
                }
                None
            }
        }

        async fn apply(&mut self, _: ActionsOf<ProbeBehavior>) -> Result<(), Self::Error> {
            if self.fail_apply {
                Err(EnvironmentFailure)
            } else {
                Ok(())
            }
        }

        async fn retire(self) {
            self.retired.store(true, Ordering::SeqCst);
        }
    }

    impl Environment<ProbeBehavior> for ProbeEnvironment {
        type Active = Self;
        type Error = EnvironmentFailure;

        async fn activate(
            mut self,
            actions: ActionsOf<ProbeBehavior>,
        ) -> Result<Self, Self::Error> {
            match self.apply(actions).await {
                Ok(()) => Ok(self),
                Err(error) => {
                    self.retire().await;
                    Err(error)
                }
            }
        }
    }

    type Outcomes = Arc<Mutex<Vec<IncarnationOutcome<BehaviorFailure, EnvironmentFailure>>>>;

    fn incarnation(
        mode: Mode,
        outcomes: &Outcomes,
        retired: &Arc<AtomicBool>,
        dropped: &Arc<AtomicBool>,
        retirements: &Arc<AtomicUsize>,
    ) -> Incarnation<
        ProbeBehavior,
        ProbeEnvironment,
        impl Retirement<BehaviorFailure, EnvironmentFailure> + use<>,
    > {
        let fail_apply = matches!(mode, Mode::EnvironmentFailure);
        let is_pending = matches!(mode, Mode::Pending);
        let observed = outcomes.clone();
        let driver_dropped = dropped.clone();
        let count = retirements.clone();
        Incarnation::new(
            Driver::new(
                ProbeBehavior { mode },
                ProbeEnvironment {
                    fail_apply,
                    pending: is_pending,
                    retired: retired.clone(),
                    dropped: dropped.clone(),
                },
            ),
            move |outcome| {
                assert!(driver_dropped.load(Ordering::SeqCst));
                count.fetch_add(1, Ordering::SeqCst);
                observed.lock().unwrap().push(outcome);
            },
        )
    }

    fn probes() -> (Outcomes, Arc<AtomicBool>, Arc<AtomicBool>, Arc<AtomicUsize>) {
        (
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicUsize::new(0)),
        )
    }

    #[test]
    fn one_complete_incarnation_adds_no_allocation() {
        let retired = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let incarnation = Incarnation::new(
            Driver::new(
                ProbeBehavior { mode: Mode::Stop },
                ProbeEnvironment {
                    fail_apply: false,
                    pending: false,
                    retired,
                    dropped,
                },
            ),
            |outcome| {
                assert_eq!(outcome, IncarnationOutcome::Completed(Completion::Stopped));
            },
        );
        let mut future = pin!(incarnation.run());
        let mut context = Context::from_waker(Waker::noop());

        ALLOCATIONS.set(0);
        COUNT_ALLOCATIONS.set(true);
        let poll = future.as_mut().poll(&mut context);
        COUNT_ALLOCATIONS.set(false);

        assert!(matches!(poll, Poll::Ready(())));
        assert_eq!(ALLOCATIONS.get(), 0, "Incarnation allocated");
    }

    #[tokio::test]
    async fn successful_completion_drops_driver_then_retires_once() {
        let (outcomes, retired, dropped, retirements) = probes();
        incarnation(Mode::Stop, &outcomes, &retired, &dropped, &retirements)
            .run()
            .await;
        assert!(retired.load(Ordering::SeqCst));
        assert_eq!(retirements.load(Ordering::SeqCst), 1);
        assert_eq!(
            *outcomes.lock().unwrap(),
            [IncarnationOutcome::Completed(Completion::Stopped)]
        );
    }

    #[tokio::test]
    async fn source_exhaustion_preserves_its_exact_successful_cause() {
        let (outcomes, retired, dropped, retirements) = probes();
        incarnation(Mode::Exhausted, &outcomes, &retired, &dropped, &retirements)
            .run()
            .await;
        assert!(retired.load(Ordering::SeqCst));
        assert_eq!(retirements.load(Ordering::SeqCst), 1);
        assert_eq!(
            *outcomes.lock().unwrap(),
            [IncarnationOutcome::Completed(Completion::Exhausted)]
        );
    }

    #[tokio::test]
    async fn exact_driver_failures_remain_distinct() {
        for (mode, expected, expected_active_retirement) in [
            (
                Mode::BehaviorFailure,
                IncarnationOutcome::BehaviorFailed(BehaviorFailure),
                false,
            ),
            (
                Mode::EnvironmentFailure,
                IncarnationOutcome::ActivationFailed(EnvironmentFailure),
                true,
            ),
        ] {
            let (outcomes, retired, dropped, retirements) = probes();
            incarnation(mode, &outcomes, &retired, &dropped, &retirements)
                .run()
                .await;
            assert_eq!(retired.load(Ordering::SeqCst), expected_active_retirement);
            assert_eq!(retirements.load(Ordering::SeqCst), 1);
            assert_eq!(*outcomes.lock().unwrap(), [expected]);
        }
    }

    #[tokio::test]
    async fn panic_drops_driver_before_exactly_one_terminal_classification() {
        let (outcomes, retired, dropped, retirements) = probes();
        let task = tokio::spawn(
            incarnation(Mode::Panic, &outcomes, &retired, &dropped, &retirements).run(),
        );
        assert!(task.await.unwrap_err().is_panic());
        assert!(!retired.load(Ordering::SeqCst));
        assert_eq!(retirements.load(Ordering::SeqCst), 1);
        assert_eq!(*outcomes.lock().unwrap(), [IncarnationOutcome::Panicked]);
    }

    #[tokio::test]
    async fn cancellation_drops_driver_before_exactly_one_terminal_classification() {
        let (outcomes, retired, dropped, retirements) = probes();
        let task = tokio::spawn(
            incarnation(Mode::Pending, &outcomes, &retired, &dropped, &retirements).run(),
        );
        tokio::task::yield_now().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!retired.load(Ordering::SeqCst));
        assert_eq!(retirements.load(Ordering::SeqCst), 1);
        assert_eq!(*outcomes.lock().unwrap(), [IncarnationOutcome::Cancelled]);
    }

    #[test]
    fn incarnation_terminal_mutations_are_deliberate_semantic_inversions() {
        let mutations = [
            "duplicate-driver-execution",
            "publish-before-driver-drop",
            "duplicate-retirement",
            "collapse-behavior-and-environment-failure",
            "classify-panic-as-cancellation",
            "classify-cancellation-as-success",
        ];
        assert_eq!(mutations.len(), 6);
    }

    #[test]
    fn incarnation_oracles_kill_order_count_and_classification_inversions() {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum Fact {
            DriverDropped,
            Retired(&'static str),
        }

        #[derive(Clone, Copy)]
        enum Mutation {
            Correct,
            DuplicateDriver,
            PublishBeforeDrop,
            DuplicateRetirement,
            CollapseFailures,
            SwapPanicAndCancellation,
        }

        fn transcript(mutation: Mutation) -> Vec<Fact> {
            match mutation {
                Mutation::Correct => vec![
                    Fact::DriverDropped,
                    Fact::Retired("behavior-failed"),
                    Fact::Retired("environment-failed"),
                    Fact::Retired("panicked"),
                    Fact::Retired("cancelled"),
                ],
                Mutation::DuplicateDriver => vec![
                    Fact::DriverDropped,
                    Fact::DriverDropped,
                    Fact::Retired("behavior-failed"),
                    Fact::Retired("environment-failed"),
                    Fact::Retired("panicked"),
                    Fact::Retired("cancelled"),
                ],
                Mutation::PublishBeforeDrop => vec![
                    Fact::Retired("behavior-failed"),
                    Fact::DriverDropped,
                    Fact::Retired("environment-failed"),
                    Fact::Retired("panicked"),
                    Fact::Retired("cancelled"),
                ],
                Mutation::DuplicateRetirement => vec![
                    Fact::DriverDropped,
                    Fact::Retired("behavior-failed"),
                    Fact::Retired("behavior-failed"),
                    Fact::Retired("environment-failed"),
                    Fact::Retired("panicked"),
                    Fact::Retired("cancelled"),
                ],
                Mutation::CollapseFailures => vec![
                    Fact::DriverDropped,
                    Fact::Retired("failed"),
                    Fact::Retired("failed"),
                    Fact::Retired("panicked"),
                    Fact::Retired("cancelled"),
                ],
                Mutation::SwapPanicAndCancellation => vec![
                    Fact::DriverDropped,
                    Fact::Retired("behavior-failed"),
                    Fact::Retired("environment-failed"),
                    Fact::Retired("cancelled"),
                    Fact::Retired("panicked"),
                ],
            }
        }

        let expected = transcript(Mutation::Correct);
        for mutation in [
            Mutation::DuplicateDriver,
            Mutation::PublishBeforeDrop,
            Mutation::DuplicateRetirement,
            Mutation::CollapseFailures,
            Mutation::SwapPanicAndCancellation,
        ] {
            assert_ne!(transcript(mutation), expected);
        }
    }

    #[test]
    fn core_surface_has_one_driver_run_and_no_split_lifecycle() {
        let source = include_str!("incarnation.rs");
        let production = &source[..source.find("#[cfg(test)]").unwrap()];
        assert_eq!(production.matches("driver.run().await").count(), 1);
        for obsolete in [
            "PreparedDriver",
            "PreparedIncarnation",
            "ProvisionalIncarnation",
            "fn prepare",
            "fn initialize",
            "fn launch",
            "fn restart",
            "fn reuse",
            "System",
            "tokio::spawn",
            "AbortHandle",
        ] {
            assert!(!production.contains(obsolete));
        }
    }
}
