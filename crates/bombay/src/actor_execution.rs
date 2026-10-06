//! One terminally classified execution directly above the universal Driver.

use core::future::pending;
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
pub struct ActorExecution<B, E, R>
where
    B: Behavior<Ph = Never>,
    E: Environment<B>,
{
    driver: Driver<B, E>,
    retirement: R,
}

impl<B, E, R> ActorExecution<B, E, R>
where
    B: Behavior<Ph = Never>,
    E: Environment<B>,
{
    /// Bind one already-constructed Driver to one retirement capability.
    pub const fn new(driver: Driver<B, E>, retirement: R) -> Self {
        Self { driver, retirement }
    }
}

impl<B, E, R> ActorExecution<B, E, R>
where
    B: Behavior<Ph = Never>,
    E: Environment<B>,
    R: Retirement<B, E::Residual, B::Error, E::Error, E::RetirementRequest>,
{
    /// Consume and execute this incarnation exactly once.
    ///
    /// Ordinary Driver results are classified and retired before this future
    /// returns. Panic unwinding and cancellation drop the active Driver future
    /// before the terminal guard publishes their classification.
    pub async fn run(self) -> R::Output {
        let Self { driver, retirement } = self;
        let terminal = Terminal::<_, B, E::Residual, B::Error, E::Error, E::RetirementRequest>::new(
            retirement,
        );
        // The original Driver and terminal capability stay owned until the
        // completion barrier yields a genuine residual. An incomplete advanced
        // host cannot trigger a replay of its arbitrary retirement callback.
        let mut driver = Some(driver);
        let mut received = None;
        Driver::receive_run(&mut driver, &mut received).await;
        match received.take() {
            Some(Ok(retirement)) => terminal.complete(retirement.into()),
            Some(Err(surviving)) => {
                driver = Some(surviving);
                let never = pending::<R::Output>().await;
                drop(driver);
                never
            }
            None => {
                let never = pending::<R::Output>().await;
                drop(driver);
                never
            }
        }
    }
}

struct Terminal<R, B, Residual, BehaviorError, ActivationError, Request>
where
    R: Retirement<B, Residual, BehaviorError, ActivationError, Request>,
{
    retirement: Option<R>,
    driver_state: PhantomData<fn(B, Residual, Request)>,
    failure_types: PhantomData<fn(BehaviorError, ActivationError)>,
}

impl<R, B, Residual, BehaviorError, ActivationError, Request>
    Terminal<R, B, Residual, BehaviorError, ActivationError, Request>
where
    R: Retirement<B, Residual, BehaviorError, ActivationError, Request>,
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
        outcome: ActorExecutionOutcome<B, Residual, BehaviorError, ActivationError, Request>,
    ) -> R::Output {
        self.retirement
            .take()
            .expect("terminal retirement is affine")
            .retire(outcome)
    }
}

impl<R, B, Residual, BehaviorError, ActivationError, Request> Drop
    for Terminal<R, B, Residual, BehaviorError, ActivationError, Request>
where
    R: Retirement<B, Residual, BehaviorError, ActivationError, Request>,
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
    use core::ops::ControlFlow;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::fs;
    use std::future::{Future, pending};
    use std::hint::black_box;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::path::PathBuf;
    use std::pin::pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};

    use behavior::{
        Actions, BehaviorActed, BehaviorSettlements, Here, InitializationTurn, Interpretation,
        InterpretationProgress, NoBirths, NoSends, SourceCustody, SourceProgress,
        SourceSettlementCustody, User,
    };
    use bombay_engine::{ActionsOf, ActiveEnvironment, Completion, DriverError};

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
        RetirementBeforeReceiptPanic,
        RetirementAfterReceiptPanic,
    }

    struct ProbeEnvironment {
        response: EnvironmentResponse,
        active_retirements: Arc<AtomicUsize>,
        environment_drops: Arc<AtomicUsize>,
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<ProbeBehavior>,
                <ProbeBehavior as BehaviorSettlements>::InterpretationCustody,
                ActionSettlementOf<ProbeBehavior>,
            >,
        >,
        source: Option<
            SourceProgress<
                ActionSettlementOf<ProbeBehavior>,
                <ProbeBehavior as BehaviorSettlements>::SourceCustody,
            >,
        >,
    }

    impl Drop for ProbeEnvironment {
        fn drop(&mut self) {
            self.environment_drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl ActiveEnvironment<ProbeBehavior> for ProbeEnvironment {
        type RetirementRequest = Never;
        type Settlement = ActionSettlementOf<ProbeBehavior>;
        type Residual = ();

        fn next(
            &mut self,
        ) -> impl Future<Output = ControlFlow<Never, Option<<ProbeBehavior as Behavior>::Event>>>
        {
            let response = self.response;
            async move {
                ControlFlow::Continue(match response {
                    EnvironmentResponse::Wait => pending::<Option<User<MailAddr, ()>>>().await,
                    EnvironmentResponse::OneEvent => Some(User::new(MailAddr(1), ())),
                    EnvironmentResponse::Exhaust
                    | EnvironmentResponse::RejectActivation
                    | EnvironmentResponse::RetirementBeforeReceiptPanic
                    | EnvironmentResponse::RetirementAfterReceiptPanic => None,
                })
            }
        }

        #[expect(
            clippy::unused_async_trait_impl,
            reason = "Defer trait-port work and owned inputs until the future is polled."
        )]
        async fn next_source(
            &mut self,
        ) -> ControlFlow<Never, Option<<ProbeBehavior as Behavior>::Event>> {
            ControlFlow::Continue(None)
        }

        async fn apply(
            &mut self,
            actions: &mut Option<ActionsOf<ProbeBehavior>>,
            received: &mut Option<Interpretation<Self::Settlement>>,
        ) {
            if received.is_some() {
                return;
            }
            match (&self.interpretation, actions.as_ref()) {
                (None, Some(_)) => {
                    self.interpretation = actions.take().map(InterpretationProgress::Original);
                }
                (Some(_), None) => {}
                (Some(_), Some(_)) | (None, None) => return,
            }
            ActionsOf::<ProbeBehavior>::interpret::<_, <ProbeBehavior as Behavior>::Event, Here>(
                &mut self.interpretation,
                &mut (),
            )
            .await;
            match self.interpretation.take() {
                Some(InterpretationProgress::Completed(interpretation)) => {
                    *received = Some(interpretation);
                }
                retained => self.interpretation = retained,
            }
        }

        async fn offer_next(
            &mut self,
            settlement: &mut Option<Self::Settlement>,
            received: &mut Option<SourceCustody<Self::Settlement>>,
        ) {
            if received.is_some() {
                return;
            }
            match (&self.source, settlement.as_ref()) {
                (None, Some(_)) => self.source = settlement.take().map(SourceProgress::Original),
                (Some(_), None) => {}
                (Some(_), Some(_)) | (None, None) => return,
            }
            <Self::Settlement as SourceSettlementCustody<(), <ProbeBehavior as Behavior>::Event>>::prepare_source(&mut self.source);
            if let Some(SourceProgress::Offering(custody)) = self.source.as_mut() {
                <Self::Settlement as SourceSettlementCustody<
                    (),
                    <ProbeBehavior as Behavior>::Event,
                >>::offer_next_to_source(custody, &mut ())
                .await;
            }
            <Self::Settlement as SourceSettlementCustody<(), <ProbeBehavior as Behavior>::Event>>::finish_source(&mut self.source);
            match self.source.take() {
                Some(SourceProgress::Completed(source)) => *received = Some(source),
                retained => self.source = retained,
            }
        }

        fn publish(&mut self) -> ControlFlow<Self::RetirementRequest, ()> {
            ControlFlow::Continue(())
        }

        #[expect(
            clippy::unused_async_trait_impl,
            reason = "Retirement effects and native fault occur on poll while original inputs remain outside."
        )]
        async fn retire(
            environment: &mut Option<Self>,
            actions: &mut Option<ActionsOf<ProbeBehavior>>,
            interpretation: &mut Option<Interpretation<Self::Settlement>>,
            source: &mut Option<SourceCustody<Self::Settlement>>,
            source_index: &mut Option<usize>,
            ingress: &mut Option<ControlFlow<Never, Option<<ProbeBehavior as Behavior>::Event>>>,
            settlements: &mut Option<Vec<Self::Settlement>>,
            received: &mut Option<Self::Residual>,
        ) {
            if received.is_some()
                || actions.is_some()
                || interpretation.is_some()
                || source.is_some()
                || source_index.is_some()
                || ingress.is_some()
            {
                return;
            }
            let Some(owner) = environment.as_ref() else {
                return;
            };
            if owner.interpretation.is_some() || owner.source.is_some() || settlements.is_none() {
                return;
            }
            owner.active_retirements.fetch_add(1, Ordering::SeqCst);
            if matches!(
                owner.response,
                EnvironmentResponse::RetirementBeforeReceiptPanic
            ) {
                panic!("deliberate retirement panic before original residual acquisition");
            }
            let response = owner.response;
            drop(settlements.take());
            drop(environment.take());
            *received = Some(());
            if matches!(response, EnvironmentResponse::RetirementAfterReceiptPanic) {
                panic!("deliberate retirement panic after original residual acquisition");
            }
        }
    }

    impl Environment<ProbeBehavior> for ProbeEnvironment {
        type Active = Self;
        type RetirementRequest = Never;
        type Settlement = <ProbeBehavior as BehaviorSettlements>::Settlements;
        type Error = EnvironmentFailure;
        type Residual = ();

        async fn activate(
            environment: &mut Option<Self>,
            actions: &mut Option<ActionsOf<ProbeBehavior>>,
            received: &mut Option<
                Result<
                    (Self::Active, Interpretation<Self::Settlement>),
                    (Self::Error, Self::Residual),
                >,
            >,
        ) {
            if received.is_some() {
                return;
            }
            let Some(owner) = environment.as_mut() else {
                return;
            };
            if matches!(owner.response, EnvironmentResponse::RejectActivation) {
                owner.active_retirements.fetch_add(1, Ordering::SeqCst);
                // This concrete host has no sends/births; the Driver already owns the initialization verdict.
                drop(actions.take());
                drop(environment.take());
                *received = Some(Err((EnvironmentFailure, ())));
                return;
            }
            let mut interpretation = None;
            <Self as ActiveEnvironment<ProbeBehavior>>::apply(owner, actions, &mut interpretation)
                .await;
            if let Some(interpretation) = interpretation {
                let owner = environment
                    .take()
                    .expect("the complete interpretation still has its original environment");
                *received = Some(Ok((owner, interpretation)));
            }
        }

        async fn retire(
            environment: &mut Option<Self>,
            actions: &mut Option<ActionsOf<ProbeBehavior>>,
            received: &mut Option<Self::Residual>,
        ) {
            if received.is_some() {
                return;
            }
            let Some(owner) = environment.as_ref() else {
                return;
            };
            if owner.interpretation.is_some() || owner.source.is_some() {
                return;
            }
            drop(actions.take());
            drop(environment.take());
            *received = Some(());
        }
    }

    type ProbeOutcome =
        ActorExecutionOutcome<ProbeBehavior, (), BehaviorFailure, EnvironmentFailure>;

    fn assert_probe_outcome(actual: &ProbeOutcome, expected: &ProbeOutcome) {
        match (actual, expected) {
            (
                ActorExecutionOutcome::Completed {
                    behavior: actual_behavior,
                    residual: actual_residual,
                    additional_failures: actual_additional_failures,
                    completion: actual_completion,
                },
                ActorExecutionOutcome::Completed {
                    behavior: expected_behavior,
                    residual: expected_residual,
                    additional_failures: expected_additional_failures,
                    completion: expected_completion,
                },
            ) => {
                assert_eq!(actual_behavior, expected_behavior);
                assert_eq!(actual_residual, expected_residual);
                assert!(actual_additional_failures.is_empty());
                assert!(expected_additional_failures.is_empty());
                assert_eq!(actual_completion, expected_completion);
            }
            (
                ActorExecutionOutcome::BehaviorFailed {
                    behavior: actual_behavior,
                    residual: actual_residual,
                    additional_failures: actual_additional_failures,
                    error: actual_error,
                },
                ActorExecutionOutcome::BehaviorFailed {
                    behavior: expected_behavior,
                    residual: expected_residual,
                    additional_failures: expected_additional_failures,
                    error: expected_error,
                },
            ) => {
                assert_eq!(actual_behavior, expected_behavior);
                assert_eq!(actual_residual, expected_residual);
                assert!(actual_additional_failures.is_empty());
                assert!(expected_additional_failures.is_empty());
                assert_eq!(actual_error, expected_error);
            }
            (
                ActorExecutionOutcome::ActivationFailed {
                    behavior: actual_behavior,
                    residual: actual_residual,
                    additional_failures: actual_additional_failures,
                    error: actual_error,
                },
                ActorExecutionOutcome::ActivationFailed {
                    behavior: expected_behavior,
                    residual: expected_residual,
                    additional_failures: expected_additional_failures,
                    error: expected_error,
                },
            ) => {
                assert_eq!(actual_behavior, expected_behavior);
                assert_eq!(actual_residual, expected_residual);
                assert!(actual_additional_failures.is_empty());
                assert!(expected_additional_failures.is_empty());
                assert_eq!(actual_error, expected_error);
            }
            (
                ActorExecutionOutcome::InitializationPanicked {
                    payload: _,
                    behavior: actual_behavior,
                    residual: actual_residual,
                    additional_failures: actual_additional_failures,
                },
                ActorExecutionOutcome::InitializationPanicked {
                    payload: _,
                    behavior: expected_behavior,
                    residual: expected_residual,
                    additional_failures: expected_additional_failures,
                },
            ) => {
                assert_eq!(actual_behavior, expected_behavior);
                assert_eq!(actual_residual, expected_residual);
                assert!(actual_additional_failures.is_empty());
                assert!(expected_additional_failures.is_empty());
            }
            (ActorExecutionOutcome::Panicked, ActorExecutionOutcome::Panicked)
            | (ActorExecutionOutcome::Cancelled, ActorExecutionOutcome::Cancelled) => {}
            (actual, expected) => panic!(
                "unexpected complete probe outcome: actual={actual:?}, expected={expected:?}"
            ),
        }
    }
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
                    interpretation: None,
                    source: None,
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
    fn actor_execution_adds_no_allocation_to_the_same_driver_receiving_work() {
        let (outcomes, active_retirements, environment_drops, retirements) = probes();
        let direct = Driver::new(
            ProbeBehavior { mode: Mode::Stop },
            ProbeEnvironment {
                response: EnvironmentResponse::Exhaust,
                active_retirements: active_retirements.clone(),
                environment_drops: environment_drops.clone(),
                interpretation: None,
                source: None,
            },
        );
        let mut direct_future = pin!(direct.run());
        let mut context = Context::from_waker(Waker::noop());
        let mut direct_poll = None;
        let direct_allocations = allocations_during(|| {
            direct_poll = Some(direct_future.as_mut().poll(&mut context));
        });
        let Some(Poll::Ready(Ok(direct_retirement))) = direct_poll else {
            panic!("the concrete direct Driver must complete its actual retirement");
        };
        let direct_outcome: ProbeOutcome = direct_retirement.into();
        let mut received = None;
        let (execution_allocations, execution_poll) = {
            let execution = ActorExecution::new(
                Driver::new(
                    ProbeBehavior { mode: Mode::Stop },
                    ProbeEnvironment {
                        response: EnvironmentResponse::Exhaust,
                        active_retirements,
                        environment_drops,
                        interpretation: None,
                        source: None,
                    },
                ),
                |outcome| received = Some(outcome),
            );
            let mut future = pin!(execution.run());
            let mut execution_poll = None;
            let execution_allocations = allocations_during(|| {
                execution_poll = Some(future.as_mut().poll(&mut context));
            });
            (execution_allocations, execution_poll)
        };
        assert!(matches!(execution_poll, Some(Poll::Ready(()))));
        let received = received.expect("the complete execution transfers its exact outcome");
        assert_probe_outcome(&received, &direct_outcome);
        assert_eq!(execution_allocations, direct_allocations);
        assert_eq!(execution_allocations, 1);
        println!(
            "historical direct Driver=1; current direct Driver={direct_allocations}; current ActorExecution={execution_allocations}; outer allocation overhead=0"
        );
        drop(outcomes);
        drop(retirements);
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
        {
            let observed = outcomes.lock().unwrap();
            let expected = [ActorExecutionOutcome::Completed {
                behavior: ProbeBehavior { mode: Mode::Stop },
                residual: (),
                additional_failures: Vec::new(),
                completion: Completion::Stopped,
            }];
            assert_eq!(observed.len(), expected.len());
            for (actual, expected) in observed.iter().zip(&expected) {
                assert_probe_outcome(actual, expected);
            }
        };
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
        {
            let observed = outcomes.lock().unwrap();
            let expected = [ActorExecutionOutcome::Completed {
                behavior: ProbeBehavior {
                    mode: Mode::Exhausted,
                },
                residual: (),
                additional_failures: Vec::new(),
                completion: Completion::Exhausted,
            }];
            assert_eq!(observed.len(), expected.len());
            for (actual, expected) in observed.iter().zip(&expected) {
                assert_probe_outcome(actual, expected);
            }
        };
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
                    additional_failures: Vec::new(),
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
                    additional_failures: Vec::new(),
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
            {
                let observed = outcomes.lock().unwrap();
                let expected = [expected];
                assert_eq!(observed.len(), expected.len());
                for (actual, expected) in observed.iter().zip(&expected) {
                    assert_probe_outcome(actual, expected);
                }
            };
        }
    }

    #[test]
    fn panic_drops_driver_before_exactly_one_terminal_classification() {
        let (outcomes, active_retirements, environment_drops, retirements) = probes();
        let driver_drops_at_retirement = Arc::new(Mutex::new(Vec::new()));
        let observed = outcomes.clone();
        let observed_drops = environment_drops.clone();
        let observed_retirement_drops = driver_drops_at_retirement.clone();
        let count = retirements.clone();
        let execution = ActorExecution::new(
            Driver::new(
                ProbeBehavior {
                    mode: Mode::Pending,
                },
                ProbeEnvironment {
                    response: EnvironmentResponse::Wait,
                    active_retirements: active_retirements.clone(),
                    environment_drops: environment_drops.clone(),
                    interpretation: None,
                    source: None,
                },
            ),
            move |outcome| {
                let _retirement = count.fetch_add(1, Ordering::SeqCst) + 1;
                observed_retirement_drops
                    .lock()
                    .unwrap()
                    .push(observed_drops.load(Ordering::SeqCst));
                observed.lock().unwrap().push(outcome);
            },
        );
        let mut first_poll = None;
        let unwound: Result<(), _> = catch_unwind(AssertUnwindSafe(|| {
            let mut future = pin!(execution.run());
            let mut context = Context::from_waker(Waker::noop());
            first_poll = Some(future.as_mut().poll(&mut context));
            panic!("outside the pending incarnation poll");
        }));
        let payload = unwound.expect_err("the surrounding native unwind remains with its caller");
        drop(payload);
        assert_eq!(first_poll, Some(Poll::Pending));
        assert_eq!(active_retirements.load(Ordering::SeqCst), 0);
        assert_eq!(environment_drops.load(Ordering::SeqCst), 1);
        assert_eq!(retirements.load(Ordering::SeqCst), 1);
        let observed_drops = driver_drops_at_retirement.lock().unwrap();
        assert_eq!(observed_drops.as_slice(), &[1]);
        let observed = outcomes.lock().unwrap();
        let [ActorExecutionOutcome::Panicked] = observed.as_slice() else {
            panic!("the pending incarnation retires exactly once as an uncaught unwind");
        };
    }

    #[tokio::test]
    async fn pure_transition_panic_retires_surviving_behavior_once() {
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
        joined.expect("the caught pure event fold returns after retirement");
        assert_eq!(active_retirements.load(Ordering::SeqCst), 1);
        assert_eq!(environment_drops.load(Ordering::SeqCst), 1);
        assert_eq!(retirements.load(Ordering::SeqCst), 1);
        let observed = outcomes.lock().unwrap();
        let [
            ActorExecutionOutcome::TransitionPanicked {
                behavior,
                residual,
                additional_failures,
                payload: _,
            },
        ] = observed.as_slice()
        else {
            panic!("pure event-fold panic retains its exact provenance");
        };
        assert_eq!(
            behavior,
            &ProbeBehavior {
                mode: Mode::ActivePanic
            }
        );
        assert_eq!(*residual, ());
        assert!(additional_failures.is_empty());
    }

    #[test]
    fn incomplete_retirement_retains_driver_without_replaying_work() {
        let (outcomes, active_retirements, environment_drops, retirements) = probes();
        let observed = outcomes.clone();
        let observed_drops = environment_drops.clone();
        let count = retirements.clone();
        let execution = ActorExecution::new(
            Driver::new(
                ProbeBehavior { mode: Mode::Stop },
                ProbeEnvironment {
                    response: EnvironmentResponse::RetirementBeforeReceiptPanic,
                    active_retirements: active_retirements.clone(),
                    environment_drops: environment_drops.clone(),
                    interpretation: None,
                    source: None,
                },
            ),
            move |outcome| {
                let retirement = count.fetch_add(1, Ordering::SeqCst) + 1;
                assert_eq!(observed_drops.load(Ordering::SeqCst), retirement);
                observed.lock().unwrap().push(outcome);
            },
        );
        let mut future = Box::pin(execution.run());
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..2 {
            let poll = future.as_mut().poll(&mut context);
            assert!(poll.is_pending());
            assert_eq!(active_retirements.load(Ordering::SeqCst), 1);
            assert_eq!(environment_drops.load(Ordering::SeqCst), 0);
            assert_eq!(retirements.load(Ordering::SeqCst), 0);
            assert!(outcomes.lock().unwrap().is_empty());
        }
        drop(future);
        assert_eq!(active_retirements.load(Ordering::SeqCst), 1);
        assert_eq!(environment_drops.load(Ordering::SeqCst), 1);
        assert_eq!(retirements.load(Ordering::SeqCst), 1);
        let observed = outcomes.lock().unwrap();
        assert!(matches!(
            observed.as_slice(),
            [ActorExecutionOutcome::Cancelled]
        ));
    }

    #[tokio::test]
    async fn completed_actor_retains_later_retirement_failure() {
        let (outcomes, active_retirements, environment_drops, retirements) = probes();
        let observed = outcomes.clone();
        let observed_drops = environment_drops.clone();
        let count = retirements.clone();
        ActorExecution::new(
            Driver::new(
                ProbeBehavior { mode: Mode::Stop },
                ProbeEnvironment {
                    response: EnvironmentResponse::RetirementAfterReceiptPanic,
                    active_retirements: active_retirements.clone(),
                    environment_drops: environment_drops.clone(),
                    interpretation: None,
                    source: None,
                },
            ),
            move |outcome| {
                let retirement = count.fetch_add(1, Ordering::SeqCst) + 1;
                assert_eq!(observed_drops.load(Ordering::SeqCst), retirement);
                observed.lock().unwrap().push(outcome);
            },
        )
        .run()
        .await;
        assert_eq!(active_retirements.load(Ordering::SeqCst), 1);
        assert_eq!(environment_drops.load(Ordering::SeqCst), 1);
        assert_eq!(retirements.load(Ordering::SeqCst), 1);
        let observed = outcomes.lock().unwrap();
        let [
            ActorExecutionOutcome::Completed {
                behavior,
                residual,
                completion: Completion::Stopped,
                additional_failures,
            },
        ] = observed.as_slice()
        else {
            panic!("the completed actor and acquired residual survive later retirement failure");
        };
        assert_eq!(behavior, &ProbeBehavior { mode: Mode::Stop });
        assert_eq!(*residual, ());
        assert!(matches!(
            additional_failures.as_slice(),
            [DriverError::RetirementPanicked(_)]
        ));
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
        {
            let observed = outcomes.lock().unwrap();
            let expected = [ActorExecutionOutcome::InitializationPanicked {
                payload: Box::new(()),
                behavior: ProbeBehavior { mode: Mode::Panic },
                residual: (),
                additional_failures: Vec::new(),
            }];
            assert_eq!(observed.len(), expected.len());
            for (actual, expected) in observed.iter().zip(&expected) {
                assert_probe_outcome(actual, expected);
            }
        };
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
        {
            let observed = outcomes.lock().unwrap();
            let expected = [ActorExecutionOutcome::Cancelled];
            assert_eq!(observed.len(), expected.len());
            for (actual, expected) in observed.iter().zip(&expected) {
                assert_probe_outcome(actual, expected);
            }
        };
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
            joined.expect("the replayed pure transition panic returns after retirement");
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

        assert_eq!(active_retirements.load(Ordering::SeqCst), 2);
        assert_eq!(environment_drops.load(Ordering::SeqCst), 4);
        assert_eq!(retirements.load(Ordering::SeqCst), 4);
        {
            let observed = outcomes.lock().unwrap();
            let [
                ActorExecutionOutcome::TransitionPanicked {
                    behavior: first_behavior,
                    residual: first_residual,
                    additional_failures: first_additional_failures,
                    payload: _,
                },
                ActorExecutionOutcome::TransitionPanicked {
                    behavior: second_behavior,
                    residual: second_residual,
                    additional_failures: second_additional_failures,
                    payload: _,
                },
                ActorExecutionOutcome::Cancelled,
                ActorExecutionOutcome::Cancelled,
            ] = observed.as_slice()
            else {
                panic!("each transition panic and cancellation must retire exactly once");
            };
            let expected_behavior = ProbeBehavior {
                mode: Mode::ActivePanic,
            };
            assert_eq!([first_behavior, second_behavior], [&expected_behavior; 2]);
            assert_eq!([first_residual, second_residual], [&(); 2]);
            assert!(first_additional_failures.is_empty());
            assert!(second_additional_failures.is_empty());
        };
    }

    #[test]
    fn core_surface_has_one_driver_receipt_and_no_split_lifecycle() {
        let source = include_str!("actor_execution.rs");
        let production = &source[..source.find("#[cfg(test)]").unwrap()];
        assert_eq!(
            production
                .matches("Driver::receive_run(&mut driver, &mut received).await")
                .count(),
            1
        );
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
