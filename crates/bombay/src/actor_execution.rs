//! One terminally classified execution directly above the universal Driver.

use core::marker::PhantomData;
use std::thread;

use behavior::{Behavior, Never};
use bombay_engine::{Driver, Environment};

use super::{ActorExecutionOutcome, Retirement};

/// Owns exactly one Driver execution and one terminal retirement capability.
///
/// `ActorExecution` adds no construction, identity, mailbox, publication, executor,
/// or scheduling policy. A later layer may place this future on an executor and
/// supply a retirement implementation owning generation-specific resources.
pub struct ActorExecution<B: Behavior, E, R> {
    driver: Driver<B, E>,
    retirement: R,
}

impl<B: Behavior, E, R> ActorExecution<B, E, R> {
    /// Bind one already-constructed Driver to one retirement capability.
    pub const fn new(driver: Driver<B, E>, retirement: R) -> Self {
        Self { driver, retirement }
    }
}

impl<B, E, R> ActorExecution<B, E, R>
where
    B: Behavior<Ph = Never>,
    E: Environment<B>,
    R: Retirement<B, E::Residual, B::Error, E::Error>,
{
    /// Consume and execute this incarnation exactly once.
    ///
    /// Ordinary Driver results are classified and retired before this future
    /// returns. Panic unwinding and cancellation drop the active Driver future
    /// before the terminal guard publishes their classification.
    pub async fn run(self) -> R::Output {
        let Self { driver, retirement } = self;
        let terminal = Terminal::<_, B, E::Residual, B::Error, E::Error>::new(retirement);
        let outcome = driver.run().await.into();
        terminal.complete(outcome)
    }
}

struct Terminal<R, B, Residual, BehaviorError, ActivationError>
where
    R: Retirement<B, Residual, BehaviorError, ActivationError>,
{
    retirement: Option<R>,
    driver_state: PhantomData<fn(B, Residual)>,
    failure_types: PhantomData<fn(BehaviorError, ActivationError)>,
}

impl<R, B, Residual, BehaviorError, ActivationError>
    Terminal<R, B, Residual, BehaviorError, ActivationError>
where
    R: Retirement<B, Residual, BehaviorError, ActivationError>,
{
    const fn new(retirement: R) -> Self {
        Self {
            retirement: Some(retirement),
            driver_state: PhantomData,
            failure_types: PhantomData,
        }
    }

    fn complete(
        mut self,
        outcome: ActorExecutionOutcome<B, Residual, BehaviorError, ActivationError>,
    ) -> R::Output {
        self.retirement
            .take()
            .expect("terminal retirement is affine")
            .retire(outcome)
    }
}

impl<R, B, Residual, BehaviorError, ActivationError> Drop
    for Terminal<R, B, Residual, BehaviorError, ActivationError>
where
    R: Retirement<B, Residual, BehaviorError, ActivationError>,
{
    fn drop(&mut self) {
        let Some(retirement) = self.retirement.take() else {
            return;
        };
        let outcome = if thread::panicking() {
            ActorExecutionOutcome::Panicked
        } else {
            ActorExecutionOutcome::Cancelled
        };
        drop(retirement.retire(outcome));
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::fs;
    use std::future::{Future, pending};
    use std::hint::black_box;
    use std::panic::catch_unwind;
    use std::path::PathBuf;
    use std::pin::pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};

    use behavior::{
        Actions, BehaviorActed, BehaviorSettlements, Here, InitializationTurn, Interpretation,
        NoBirths, NoSends, SourceCustody, SourceSettlementCustody, User,
    };
    use bombay_engine::{ActionsOf, ActiveEnvironment, Completion};

    use super::*;
    use crate::MailAddr;
    use crate::interpret::ActionSettlementOf;

    struct CountingAllocator;

    #[derive(Clone, Copy)]
    enum AllocationMeasurement {
        Disabled,
        Counting(usize),
        Overflow,
    }

    thread_local! {
        static ALLOCATION_MEASUREMENT: Cell<AllocationMeasurement> = const { Cell::new(AllocationMeasurement::Disabled) };
    }

    fn record_allocation() {
        ALLOCATION_MEASUREMENT.with(|measurement| {
            let next = match measurement.get() {
                AllocationMeasurement::Disabled => AllocationMeasurement::Disabled,
                AllocationMeasurement::Counting(count) => match count.checked_add(1) {
                    Some(count) => AllocationMeasurement::Counting(count),
                    None => AllocationMeasurement::Overflow,
                },
                AllocationMeasurement::Overflow => AllocationMeasurement::Overflow,
            };
            measurement.set(next);
        });
    }

    pub(crate) fn begin_allocations() {
        ALLOCATION_MEASUREMENT.with(|measurement| match measurement.get() {
            AllocationMeasurement::Disabled => measurement.set(AllocationMeasurement::Counting(0)),
            AllocationMeasurement::Counting(_) | AllocationMeasurement::Overflow => {
                panic!("allocation measurement already owns this thread")
            }
        });
    }

    /// Err means the successful-allocation count overflowed.
    pub(crate) fn finish_allocations() -> Result<usize, ()> {
        ALLOCATION_MEASUREMENT.with(|measurement| {
            match measurement.replace(AllocationMeasurement::Disabled) {
                AllocationMeasurement::Counting(count) => Ok(count),
                AllocationMeasurement::Overflow => Err(()),
                AllocationMeasurement::Disabled => {
                    panic!("allocation measurement does not own this thread")
                }
            }
        })
    }

    #[cfg(tokio_unstable)]
    pub(crate) fn current_allocations() -> usize {
        ALLOCATION_MEASUREMENT.with(|measurement| match measurement.get() {
            AllocationMeasurement::Counting(count) => count,
            AllocationMeasurement::Disabled | AllocationMeasurement::Overflow => {
                panic!("current poll has no finite allocation count")
            }
        })
    }

    struct AllocationRelease;

    impl Drop for AllocationRelease {
        fn drop(&mut self) {
            ALLOCATION_MEASUREMENT.set(AllocationMeasurement::Disabled);
        }
    }

    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc(layout) };
            if !pointer.is_null() {
                record_allocation();
            }
            pointer
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) };
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc_zeroed(layout) };
            if !pointer.is_null() {
                record_allocation();
            }
            pointer
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            let pointer = unsafe { System.realloc(pointer, layout, size) };
            if !pointer.is_null() {
                record_allocation();
            }
            pointer
        }
    }

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    pub(crate) fn allocations_during(operation: impl FnOnce()) -> usize {
        begin_allocations();
        let release = AllocationRelease;
        operation();
        let count = finish_allocations().expect("successful-allocation count overflowed");
        drop(release);
        count
    }

    #[test]
    fn allocation_measurement_retains_overflow_and_releases_panicked_scope() {
        ALLOCATION_MEASUREMENT.set(AllocationMeasurement::Counting(usize::MAX));
        let original = Box::new(black_box(17_u64));
        black_box(&original);
        let count = finish_allocations();
        assert_eq!(count, Err(()));
        assert_eq!(*original, 17);
        drop(original);
        let failed = catch_unwind(|| allocations_during(|| panic!("measured source panic")));
        assert!(failed.is_err());
        let mut original = None;
        let count = allocations_during(|| original = Some(Box::new(black_box(23_u64))));
        assert_eq!(count, 1);
        assert_eq!(original.as_deref(), Some(&23));
        begin_allocations();
        let nested = catch_unwind(begin_allocations);
        assert!(nested.is_err());
        let count = finish_allocations();
        assert!(count.is_ok());
    }

    #[derive(Debug, PartialEq, Eq)]
    struct BehaviorFailure;

    #[derive(Debug, PartialEq, Eq)]
    struct EnvironmentFailure;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Mode {
        Stop,
        BehaviorFailure,
        EnvironmentFailure,
        Panic,
        ActivePanic,
        Exhausted,
        Pending,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct ProbeBehavior {
        mode: Mode,
    }

    impl Behavior for ProbeBehavior {
        type Protocol = behavior::MessageProtocol<MailAddr, ()>;
        type Event = User<MailAddr, ()>;
        type Sends = NoSends;
        type Ph = Never;
        type Error = BehaviorFailure;
        type Birth = NoBirths;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            match self.mode {
                Mode::Stop => Ok(Actions::stop()),
                Mode::BehaviorFailure => Err(BehaviorFailure),
                Mode::EnvironmentFailure | Mode::ActivePanic | Mode::Exhausted | Mode::Pending => {
                    Ok(Actions::cont())
                }
                Mode::Panic => panic!("deliberate incarnation panic"),
            }
        }

        fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
            match self.mode {
                Mode::ActivePanic => panic!("deliberate active incarnation panic"),
                _ => unreachable!("only the active panic probe receives an event"),
            }
        }
    }

    #[derive(Clone, Copy)]
    enum EnvironmentResponse {
        Exhaust,
        RejectActivation,
        OneEvent,
        Wait,
    }

    struct ProbeEnvironment {
        response: EnvironmentResponse,
        active_retirements: Arc<AtomicUsize>,
        environment_drops: Arc<AtomicUsize>,
    }

    impl Drop for ProbeEnvironment {
        fn drop(&mut self) {
            self.environment_drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl ActiveEnvironment<ProbeBehavior> for ProbeEnvironment {
        type Settlement = ActionSettlementOf<ProbeBehavior>;
        type Residual = ();

        fn next(&mut self) -> impl Future<Output = Option<<ProbeBehavior as Behavior>::Event>> {
            let response = self.response;
            async move {
                match response {
                    EnvironmentResponse::Wait => pending::<Option<User<MailAddr, ()>>>().await,
                    EnvironmentResponse::OneEvent => Some(User::new(MailAddr(1), ())),
                    EnvironmentResponse::Exhaust | EnvironmentResponse::RejectActivation => None,
                }
            }
        }

        #[expect(
            clippy::unused_async_trait_impl,
            reason = "Defer trait-port work and owned inputs until the future is polled."
        )]
        async fn next_source(&mut self) -> Option<<ProbeBehavior as Behavior>::Event> {
            None
        }

        async fn apply(
            &mut self,
            actions: ActionsOf<ProbeBehavior>,
        ) -> Interpretation<Self::Settlement> {
            actions
                .interpret::<_, <ProbeBehavior as Behavior>::Event, Here>(&mut ())
                .await
        }

        async fn offer_next(
            &mut self,
            settlement: Self::Settlement,
        ) -> SourceCustody<Self::Settlement> {
            <Self::Settlement as SourceSettlementCustody<
                (),
                <ProbeBehavior as Behavior>::Event,
            >>::offer_next_to_source(settlement, &mut ())
            .await
        }

        fn publish(&mut self) {}

        async fn retire(self, settlements: Vec<Self::Settlement>) -> Self::Residual {
            drop(settlements);
            self.active_retirements.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Environment<ProbeBehavior> for ProbeEnvironment {
        type Active = Self;
        type Settlement = <ProbeBehavior as BehaviorSettlements>::Settlements;
        type Error = EnvironmentFailure;
        type Residual = ();

        async fn activate(
            self,
            actions: ActionsOf<ProbeBehavior>,
        ) -> Result<(Self::Active, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)>
        {
            if matches!(self.response, EnvironmentResponse::RejectActivation) {
                self.active_retirements.fetch_add(1, Ordering::SeqCst);
                return Err((EnvironmentFailure, ()));
            }
            let interpretation = actions
                .interpret::<_, <ProbeBehavior as Behavior>::Event, Here>(&mut ())
                .await;
            Ok((self, interpretation))
        }

        async fn retire(self) -> Self::Residual {}
    }

    type ProbeOutcome =
        ActorExecutionOutcome<ProbeBehavior, (), BehaviorFailure, EnvironmentFailure>;
    type Outcomes = Arc<Mutex<Vec<ProbeOutcome>>>;

    fn actor_execution(
        mode: Mode,
        outcomes: &Outcomes,
        active_retirements: &Arc<AtomicUsize>,
        environment_drops: &Arc<AtomicUsize>,
        retirements: &Arc<AtomicUsize>,
    ) -> ActorExecution<
        ProbeBehavior,
        ProbeEnvironment,
        impl Retirement<ProbeBehavior, (), BehaviorFailure, EnvironmentFailure, Output = ()> + use<>,
    > {
        let response = match mode {
            Mode::EnvironmentFailure => EnvironmentResponse::RejectActivation,
            Mode::Pending => EnvironmentResponse::Wait,
            Mode::ActivePanic => EnvironmentResponse::OneEvent,
            Mode::Stop | Mode::BehaviorFailure | Mode::Panic | Mode::Exhausted => {
                EnvironmentResponse::Exhaust
            }
        };
        let observed = outcomes.clone();
        let observed_environment_drops = environment_drops.clone();
        let count = retirements.clone();
        ActorExecution::new(
            Driver::new(
                ProbeBehavior { mode },
                ProbeEnvironment {
                    response,
                    active_retirements: active_retirements.clone(),
                    environment_drops: environment_drops.clone(),
                },
            ),
            move |outcome| {
                let retirement = count.fetch_add(1, Ordering::SeqCst) + 1;
                assert_eq!(
                    observed_environment_drops.load(Ordering::SeqCst),
                    retirement
                );
                observed.lock().unwrap().push(outcome);
            },
        )
    }

    fn probes() -> (
        Outcomes,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        (
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        )
    }

    #[test]
    fn one_complete_actor_execution_allocates_only_its_exact_settlement_custody() {
        let active_retirements = Arc::new(AtomicUsize::new(0));
        let environment_drops = Arc::new(AtomicUsize::new(0));
        let actor_execution = ActorExecution::new(
            Driver::new(
                ProbeBehavior { mode: Mode::Stop },
                ProbeEnvironment {
                    response: EnvironmentResponse::Exhaust,
                    active_retirements,
                    environment_drops,
                },
            ),
            |outcome| {
                assert_eq!(
                    outcome,
                    ActorExecutionOutcome::Completed {
                        behavior: ProbeBehavior { mode: Mode::Stop },
                        residual: (),
                        completion: Completion::Stopped,
                    }
                );
            },
        );
        let mut future = pin!(actor_execution.run());
        let mut context = Context::from_waker(Waker::noop());

        let mut poll = None;
        let allocations = allocations_during(|| {
            poll = Some(future.as_mut().poll(&mut context));
        });

        assert!(matches!(poll, Some(Poll::Ready(()))));
        assert_eq!(
            allocations, 1,
            "ActorExecution allocated {allocations} times"
        );
    }

    #[tokio::test]
    async fn successful_completion_drops_driver_then_retires_once() {
        let (outcomes, active_retirements, environment_drops, retirements) = probes();
        actor_execution(
            Mode::Stop,
            &outcomes,
            &active_retirements,
            &environment_drops,
            &retirements,
        )
        .run()
        .await;
        assert_eq!(active_retirements.load(Ordering::SeqCst), 1);
        assert_eq!(retirements.load(Ordering::SeqCst), 1);
        assert_eq!(
            *outcomes.lock().unwrap(),
            [ActorExecutionOutcome::Completed {
                behavior: ProbeBehavior { mode: Mode::Stop },
                residual: (),
                completion: Completion::Stopped,
            }]
        );
    }

    #[tokio::test]
    async fn source_exhaustion_preserves_its_exact_successful_cause() {
        let (outcomes, active_retirements, environment_drops, retirements) = probes();
        actor_execution(
            Mode::Exhausted,
            &outcomes,
            &active_retirements,
            &environment_drops,
            &retirements,
        )
        .run()
        .await;
        assert_eq!(active_retirements.load(Ordering::SeqCst), 1);
        assert_eq!(retirements.load(Ordering::SeqCst), 1);
        assert_eq!(
            *outcomes.lock().unwrap(),
            [ActorExecutionOutcome::Completed {
                behavior: ProbeBehavior {
                    mode: Mode::Exhausted,
                },
                residual: (),
                completion: Completion::Exhausted,
            }]
        );
    }

    #[tokio::test]
    async fn exact_driver_failures_remain_distinct() {
        for (mode, expected, expected_active_retirements) in [
            (
                Mode::BehaviorFailure,
                ActorExecutionOutcome::BehaviorFailed {
                    behavior: ProbeBehavior {
                        mode: Mode::BehaviorFailure,
                    },
                    residual: (),
                    error: BehaviorFailure,
                },
                0,
            ),
            (
                Mode::EnvironmentFailure,
                ActorExecutionOutcome::ActivationFailed {
                    behavior: ProbeBehavior {
                        mode: Mode::EnvironmentFailure,
                    },
                    residual: (),
                    error: EnvironmentFailure,
                },
                1,
            ),
        ] {
            let (outcomes, active_retirements, environment_drops, retirements) = probes();
            actor_execution(
                mode,
                &outcomes,
                &active_retirements,
                &environment_drops,
                &retirements,
            )
            .run()
            .await;
            assert_eq!(
                active_retirements.load(Ordering::SeqCst),
                expected_active_retirements
            );
            assert_eq!(retirements.load(Ordering::SeqCst), 1);
            assert_eq!(*outcomes.lock().unwrap(), [expected]);
        }
    }

    #[tokio::test]
    async fn panic_drops_driver_before_exactly_one_terminal_classification() {
        let (outcomes, active_retirements, environment_drops, retirements) = probes();
        let task = tokio::spawn(
            actor_execution(
                Mode::ActivePanic,
                &outcomes,
                &active_retirements,
                &environment_drops,
                &retirements,
            )
            .run(),
        );
        let joined = task.await;
        let failure = joined.expect_err("the deliberate Behavior panic must unwind the task");
        assert!(failure.is_panic());
        assert_eq!(active_retirements.load(Ordering::SeqCst), 0);
        assert_eq!(retirements.load(Ordering::SeqCst), 1);
        assert_eq!(*outcomes.lock().unwrap(), [ActorExecutionOutcome::Panicked]);
    }

    #[tokio::test]
    async fn pure_initialization_panic_retires_prepared_environment_once() {
        let (outcomes, active_retirements, environment_drops, retirements) = probes();
        actor_execution(
            Mode::Panic,
            &outcomes,
            &active_retirements,
            &environment_drops,
            &retirements,
        )
        .run()
        .await;
        assert_eq!(active_retirements.load(Ordering::SeqCst), 0);
        assert_eq!(environment_drops.load(Ordering::SeqCst), 1);
        assert_eq!(retirements.load(Ordering::SeqCst), 1);
        assert_eq!(
            *outcomes.lock().unwrap(),
            [ActorExecutionOutcome::InitializationPanicked {
                behavior: ProbeBehavior { mode: Mode::Panic },
                residual: (),
            }]
        );
    }

    #[tokio::test]
    async fn cancellation_drops_driver_before_exactly_one_terminal_classification() {
        let (outcomes, active_retirements, environment_drops, retirements) = probes();
        let task = tokio::spawn(
            actor_execution(
                Mode::Pending,
                &outcomes,
                &active_retirements,
                &environment_drops,
                &retirements,
            )
            .run(),
        );
        tokio::task::yield_now().await;
        task.abort();
        let joined = task.await;
        let failure = joined.expect_err("aborting the pending Driver must cancel its task");
        assert!(failure.is_cancelled());
        assert_eq!(active_retirements.load(Ordering::SeqCst), 0);
        assert_eq!(retirements.load(Ordering::SeqCst), 1);
        assert_eq!(
            *outcomes.lock().unwrap(),
            [ActorExecutionOutcome::Cancelled]
        );
    }

    #[test]
    fn actor_execution_mutation_evidence_owns_production_source() {
        let source = include_str!("actor_execution.rs");
        for disconnected in [
            concat!(
                "fn actor_execution_terminal_",
                "mutations_are_deliberate_semantic_inversions"
            ),
            concat!(
                "fn actor_execution_oracles_",
                "kill_order_count_and_classification_inversions"
            ),
        ] {
            assert!(!source.contains(disconnected));
        }

        let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let evidence = fs::read_to_string(crate_root.join("tests/actor-execution-law-evidence.sh"))
            .expect("ActorExecution mutation evidence must be executable repository source");
        for production_source in [
            "crates/bombay/src/actor_execution.rs",
            "crates/bombay/src/actor_outcome.rs",
        ] {
            assert!(evidence.contains(production_source));
        }
        for inversion in [
            "terminal-after-driver",
            "retirement-before-driver-drop",
            "discard-abnormal-retirement",
            "duplicate-driver",
            "duplicate-retirement",
            "panic-as-cancellation",
            "cancellation-as-panic",
            "erase-behavior-failure",
            "erase-activation-failure",
        ] {
            assert!(evidence.contains(inversion));
        }

        let flake = fs::read_to_string(crate_root.join("../../flake.nix"))
            .expect("workspace flake must remain readable");
        assert!(flake.contains("bombay-actor-execution-law-evidence"));
    }

    #[tokio::test]
    async fn replayed_abnormal_terminations_are_each_accepted_once() {
        let (outcomes, active_retirements, environment_drops, retirements) = probes();

        for _ in 0..2 {
            let task = tokio::spawn(
                actor_execution(
                    Mode::ActivePanic,
                    &outcomes,
                    &active_retirements,
                    &environment_drops,
                    &retirements,
                )
                .run(),
            );
            let joined = task.await;
            let failure = joined.expect_err("the replayed Behavior panic must unwind its task");
            assert!(failure.is_panic());
        }

        for _ in 0..2 {
            let task = tokio::spawn(
                actor_execution(
                    Mode::Pending,
                    &outcomes,
                    &active_retirements,
                    &environment_drops,
                    &retirements,
                )
                .run(),
            );
            tokio::task::yield_now().await;
            task.abort();
            let joined = task.await;
            let failure = joined.expect_err("the replayed pending Driver must be cancelled");
            assert!(failure.is_cancelled());
        }

        assert_eq!(active_retirements.load(Ordering::SeqCst), 0);
        assert_eq!(environment_drops.load(Ordering::SeqCst), 4);
        assert_eq!(retirements.load(Ordering::SeqCst), 4);
        assert_eq!(
            *outcomes.lock().unwrap(),
            [
                ActorExecutionOutcome::Panicked,
                ActorExecutionOutcome::Panicked,
                ActorExecutionOutcome::Cancelled,
                ActorExecutionOutcome::Cancelled,
            ]
        );
    }

    #[test]
    fn core_surface_has_one_driver_run_and_no_split_lifecycle() {
        let source = include_str!("actor_execution.rs");
        let production = &source[..source.find("#[cfg(test)]").unwrap()];
        assert_eq!(production.matches("driver.run().await").count(), 1);
        for obsolete in [
            "PreparedDriver",
            "PreparedActorExecution",
            "ProvisionalActorExecution",
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
