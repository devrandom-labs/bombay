use std::any::Any;
use std::cell::Cell;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::{Future, pending};
use std::panic::resume_unwind;
use std::pin::Pin;
use std::ptr::{self, from_ref};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use behavior::{
    Actions, ActiveTurn, Become, Behavior, BehaviorActed, Births, CreateChild, CreationId,
    CreationKind, CreationSequence, Creations, InitializationTurn, MailAddr, MessageProtocol,
    Never, NoBirths, Step, Stopped, User, UserEvent,
};
use core::fmt::Debug;

use bombay_engine::{
    ActionsOf, Completion, Driver, DriverError, DriverRetirement, SettlementFailure,
};
use tokio::runtime::Builder;
use tokio::time::timeout;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExecutionCustody {
    Retained,
    Released,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetirementObservation {
    NotAttempted,
    Attempted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExecutionEvent {
    Commit(Vec<u64>, Become<Never>),
    Created(CreationId, CreationKind, u64, usize),
    CommittedOriginals(Vec<(u64, usize)>, Become<Never>),
    Next,
    DeliveredInput(u64),
    Retired,
    EnvironmentReleased,
    RetirementReceived,
}

#[derive(Default)]
struct SettlementActor {
    initialized: usize,
    received_inputs: Vec<u64>,
}

impl Behavior for SettlementActor {
    type Protocol = MessageProtocol<MailAddr, u64>;
    type Event = User<MailAddr, u64>;
    type Sends = Vec<u64>;
    type Ph = Never;
    type Error = &'static str;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        self.initialized += 1;
        Ok(Actions::send(vec![0]))
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        let value = event.message;
        self.received_inputs.push(value);
        match value {
            7 => Err("controlled"),
            6 => Ok(Actions::send(vec![60, 61])),
            9 => Ok(Actions::new(
                vec![90, 91],
                Creations::empty(),
                Step::Stop(Stopped),
            )),
            value => Ok(Actions::send(vec![value * 10])),
        }
    }
}

struct SettlementEnvironment {
    events: VecDeque<u64>,
    execution_trace: Arc<Mutex<Vec<ExecutionEvent>>>,
    fail_on: Option<u64>,
}

type SettlementDriver = Driver<SettlementActor, support::TestEnvironment<SettlementEnvironment>>;
type ExecutionTrace = Arc<Mutex<Vec<ExecutionEvent>>>;

impl TestActions<SettlementActor> for SettlementEnvironment {
    type Error = u64;
    type Residual = ();

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> Option<<SettlementActor as Behavior>::Event> {
        self.execution_trace
            .lock()
            .unwrap()
            .push(ExecutionEvent::Next);
        let value = self.events.pop_front();
        if let Some(value) = value {
            self.execution_trace
                .lock()
                .unwrap()
                .push(ExecutionEvent::DeliveredInput(value));
        }
        value.map(|value| User::new(MailAddr(1), value))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(&mut self, effect: ActionsOf<SettlementActor>) -> Result<(), Self::Error> {
        let Actions {
            sends,
            creates,
            become_,
        } = effect;
        assert_eq!(creates, Creations::empty());
        let mut committed = Vec::new();
        for value in sends {
            if self.fail_on == Some(value) {
                self.execution_trace
                    .lock()
                    .unwrap()
                    .push(ExecutionEvent::Commit(committed, become_));
                return Err(value);
            }
            committed.push(value);
        }
        self.execution_trace
            .lock()
            .unwrap()
            .push(ExecutionEvent::Commit(committed, become_));
        Ok(())
    }

    async fn retire(&mut self) {
        self.execution_trace
            .lock()
            .unwrap()
            .push(ExecutionEvent::Retired);
    }
}

fn execution(
    events: impl IntoIterator<Item = u64>,
    fail_on: Option<u64>,
) -> (SettlementDriver, ExecutionTrace) {
    let execution_trace = Arc::new(Mutex::new(Vec::new()));
    let driver = direct(
        SettlementActor::default(),
        SettlementEnvironment {
            events: events.into_iter().collect(),
            execution_trace: execution_trace.clone(),
            fail_on,
        },
    );
    (driver, execution_trace)
}

#[tokio::test]
async fn universal_causal_transcript_has_no_prefetch_or_reentrancy() {
    let (driver, execution_trace) = execution([1, 2, 9, 100], None);
    let retirement = driver.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&retirement.disposition, &Ok(Completion::Stopped));
    assert_eq!(retirement.behavior.initialized, 1);
    assert_eq!(retirement.behavior.received_inputs, [1, 2, 9]);
    assert_eq!(
        *execution_trace.lock().unwrap(),
        [
            ExecutionEvent::Commit(vec![0], Step::Continue),
            ExecutionEvent::Next,
            ExecutionEvent::DeliveredInput(1),
            ExecutionEvent::Commit(vec![10], Step::Continue),
            ExecutionEvent::Next,
            ExecutionEvent::DeliveredInput(2),
            ExecutionEvent::Commit(vec![20], Step::Continue),
            ExecutionEvent::Next,
            ExecutionEvent::DeliveredInput(9),
            ExecutionEvent::Commit(vec![90, 91], Step::Stop(Stopped)),
            ExecutionEvent::Retired,
        ]
    );
}

#[tokio::test]
async fn initialization_has_exact_disposition_and_trace_across_terminal_boundaries() {
    for (events, fail_on, disposition, received_inputs, transcript) in [
        (
            vec![9],
            None,
            Ok(Completion::Stopped),
            vec![9],
            vec![
                ExecutionEvent::Commit(vec![0], Step::Continue),
                ExecutionEvent::Next,
                ExecutionEvent::DeliveredInput(9),
                ExecutionEvent::Commit(vec![90, 91], Step::Stop(Stopped)),
                ExecutionEvent::Retired,
            ],
        ),
        (
            vec![],
            None,
            Ok(Completion::Exhausted),
            vec![],
            vec![
                ExecutionEvent::Commit(vec![0], Step::Continue),
                ExecutionEvent::Next,
                ExecutionEvent::Retired,
            ],
        ),
        (
            vec![7],
            None,
            Err(DriverError::Behavior("controlled")),
            vec![7],
            vec![
                ExecutionEvent::Commit(vec![0], Step::Continue),
                ExecutionEvent::Next,
                ExecutionEvent::DeliveredInput(7),
                ExecutionEvent::Retired,
            ],
        ),
        (
            vec![1],
            Some(0),
            Err(DriverError::Settlement(SettlementFailure::Corrupt)),
            vec![],
            vec![
                ExecutionEvent::Commit(vec![], Step::Continue),
                ExecutionEvent::Retired,
            ],
        ),
        (
            vec![9],
            Some(91),
            Err(DriverError::Settlement(SettlementFailure::Corrupt)),
            vec![9],
            vec![
                ExecutionEvent::Commit(vec![0], Step::Continue),
                ExecutionEvent::Next,
                ExecutionEvent::DeliveredInput(9),
                ExecutionEvent::Commit(vec![90], Step::Stop(Stopped)),
                ExecutionEvent::Retired,
            ],
        ),
    ] {
        let (driver, execution_trace) = execution(events, fail_on);
        let retirement = driver.run().await.unwrap_or_else(|driver| {
            drop(driver);
            panic!("the selected test host completes retirement")
        });
        assert_driver_disposition(&retirement.disposition, &disposition);
        assert_eq!(retirement.behavior.initialized, 1);
        assert_eq!(retirement.behavior.received_inputs, received_inputs);
        assert_eq!(retirement.residual, ());
        assert_eq!(*execution_trace.lock().unwrap(), transcript);
    }
}

struct LedgerActor {
    value: usize,
    initial_values: Arc<Vec<usize>>,
}

impl Behavior for LedgerActor {
    type Protocol = MessageProtocol<MailAddr, usize>;
    type Event = User<MailAddr, usize>;
    type Sends = Vec<usize>;
    type Ph = Never;
    type Error = &'static str;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        self.value = self.initial_values.iter().sum();
        Ok(Actions::send(vec![self.value]))
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        if event.message == usize::MAX {
            return Err("stateful failure");
        }
        if event.message == 0 {
            return Ok(Actions::new(
                vec![self.value],
                Creations::empty(),
                Step::Stop(Stopped),
            ));
        }
        self.value += event.message;
        Ok(Actions::send(vec![self.value]))
    }
}

struct LedgerEnvironment {
    events: VecDeque<usize>,
    committed: Arc<Mutex<Vec<usize>>>,
}

impl TestActions<LedgerActor> for LedgerEnvironment {
    type Error = Infallible;
    type Residual = ();

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> Option<<LedgerActor as Behavior>::Event> {
        self.events
            .pop_front()
            .map(|value| User::new(MailAddr(1), value))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(&mut self, actions: ActionsOf<LedgerActor>) -> Result<(), Self::Error> {
        self.committed.lock().unwrap().extend(actions.sends);
        Ok(())
    }

    async fn retire(&mut self) {}
}

#[tokio::test]
async fn successor_state_and_complete_actions_come_from_the_same_decision() {
    let initial_values = Arc::new(vec![0]);
    let retained_values = Arc::downgrade(&initial_values);
    let original_allocation = initial_values.as_ptr();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let driver = direct(
        LedgerActor {
            value: 0,
            initial_values,
        },
        LedgerEnvironment {
            events: VecDeque::from([2, 3, 0, 100]),
            committed: committed.clone(),
        },
    );

    let retirement = driver.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&retirement.disposition, &Ok(Completion::Stopped));
    assert_eq!(*committed.lock().unwrap(), [0, 2, 5, 5]);
    assert_eq!(retirement.behavior.value, 5);
    assert_eq!(retirement.behavior.initial_values.as_slice(), [0]);
    assert_eq!(
        retirement.behavior.initial_values.as_ptr(),
        original_allocation
    );
    assert_eq!(retained_values.strong_count(), 1);
    drop(retirement);
    assert_eq!(retained_values.strong_count(), 0);
}

struct RejectedLedgerEnvironment {
    event: Option<usize>,
    commits: usize,
}

impl TestActions<LedgerActor> for RejectedLedgerEnvironment {
    type Error = &'static str;
    type Residual = ();

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> Option<<LedgerActor as Behavior>::Event> {
        self.event.take().map(|value| User::new(MailAddr(1), value))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(&mut self, _actions: ActionsOf<LedgerActor>) -> Result<(), Self::Error> {
        self.commits += 1;
        if self.commits == 2 {
            Err("commit")
        } else {
            Ok(())
        }
    }

    async fn retire(&mut self) {}
}

#[tokio::test]
async fn commitment_failure_does_not_roll_back_the_successful_fold() {
    let initial_values = Arc::new(vec![0]);
    let retained_values = Arc::downgrade(&initial_values);
    let original_allocation = initial_values.as_ptr();
    let driver = direct(
        LedgerActor {
            value: 0,
            initial_values,
        },
        RejectedLedgerEnvironment {
            event: Some(3),
            commits: 0,
        },
    );

    let retirement = driver.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(
        &retirement.disposition,
        &Err(DriverError::Settlement(SettlementFailure::Corrupt)),
    );
    assert_eq!(retirement.behavior.value, 3);
    assert_eq!(retirement.behavior.initial_values.as_slice(), [0]);
    assert_eq!(
        retirement.behavior.initial_values.as_ptr(),
        original_allocation
    );
    assert_eq!(retained_values.strong_count(), 1);
    drop(retirement);
    assert_eq!(retained_values.strong_count(), 0);
}

#[tokio::test]
async fn unrelated_custom_behavior_shapes_use_the_same_driver_algorithm() {
    let (settlement_execution, _) = execution([9], None);
    let settlement_retirement = settlement_execution.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&settlement_retirement.disposition, &Ok(Completion::Stopped));

    let move_retirement = direct(
        CellActor(Cell::new(0)),
        MoveInputEnvironment(Some(Box::new(42))),
    )
    .run()
    .await
    .unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&move_retirement.disposition, &Ok(Completion::Stopped));

    let complete = Arc::new(Mutex::new(Vec::new()));
    let complete_retirement = direct(
        CompleteActionActor {
            sends: Some([Box::new(10), Box::new(11)]),
            children: Some([Box::new(20), Box::new(21)]),
            issued_creations: Vec::new(),
        },
        CompleteEnvironment(complete),
    )
    .run()
    .await
    .unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&complete_retirement.disposition, &Ok(Completion::Stopped));
}

enum ClosedEvent {
    User(User<MailAddr, Box<usize>>),
    Capability(usize),
}

impl UserEvent for ClosedEvent {
    type Addr = MailAddr;
    type Message = Box<usize>;

    fn user(from: Self::Addr, message: Self::Message) -> Self {
        Self::User(User::new(from, message))
    }

    fn into_user(self) -> Result<User<Self::Addr, Self::Message>, Self> {
        match self {
            Self::User(user) => Ok(user),
            capability @ Self::Capability(_) => Err(capability),
        }
    }
}

struct ClosedInputActor;

impl Behavior for ClosedInputActor {
    type Protocol = MessageProtocol<MailAddr, Box<usize>>;
    type Event = ClosedEvent;
    type Sends = Vec<usize>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event {
            ClosedEvent::User(user) => Ok(Actions::send(vec![*user.message])),
            ClosedEvent::Capability(value) => Ok(Actions::new(
                vec![value],
                Creations::empty(),
                Step::Stop(Stopped),
            )),
        }
    }
}

struct ClosedInputEnvironment {
    events: VecDeque<ClosedEvent>,
    committed: Arc<Mutex<Vec<usize>>>,
}

impl TestActions<ClosedInputActor> for ClosedInputEnvironment {
    type Error = Infallible;
    type Residual = ();

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> Option<ClosedEvent> {
        self.events.pop_front()
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(&mut self, actions: ActionsOf<ClosedInputActor>) -> Result<(), Self::Error> {
        self.committed.lock().unwrap().extend(actions.sends);
        Ok(())
    }

    async fn retire(&mut self) {}
}

#[tokio::test]
async fn driver_accepts_only_the_final_closed_behavior_event_type() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let driver = direct(
        ClosedInputActor,
        ClosedInputEnvironment {
            events: VecDeque::from([
                ClosedEvent::user(MailAddr(1), Box::new(7)),
                ClosedEvent::Capability(8),
            ]),
            committed: committed.clone(),
        },
    );

    let retirement = driver.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&retirement.disposition, &Ok(Completion::Stopped));
    assert_eq!(*committed.lock().unwrap(), [7, 8]);
}

#[tokio::test]
async fn at_most_one_behavior_fold_is_active() {
    let (driver, execution_trace) = execution([1, 2, 9], None);
    let retirement = driver.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&retirement.disposition, &Ok(Completion::Stopped));
    assert_eq!(retirement.behavior.initialized, 1);
    assert_eq!(retirement.behavior.received_inputs, [1, 2, 9]);
    assert_eq!(
        *execution_trace.lock().unwrap(),
        [
            ExecutionEvent::Commit(vec![0], Step::Continue),
            ExecutionEvent::Next,
            ExecutionEvent::DeliveredInput(1),
            ExecutionEvent::Commit(vec![10], Step::Continue),
            ExecutionEvent::Next,
            ExecutionEvent::DeliveredInput(2),
            ExecutionEvent::Commit(vec![20], Step::Continue),
            ExecutionEvent::Next,
            ExecutionEvent::DeliveredInput(9),
            ExecutionEvent::Commit(vec![90, 91], Step::Stop(Stopped)),
            ExecutionEvent::Retired,
        ]
    );
}

#[test]
fn pending_commit_prevents_reentrant_or_later_fold() {
    let commits_started = Arc::new(AtomicUsize::new(0));
    let retirement_started = Arc::new(Mutex::new(RetirementObservation::NotAttempted));
    let dropped = Arc::new(Mutex::new(ExecutionCustody::Retained));
    let execution_trace = Arc::new(Mutex::new(Vec::new()));
    let mut execution = Box::pin(
        direct(
            SettlementActor::default(),
            StallingEnvironment {
                stall_at: StallAt::TurnCommit,
                events: VecDeque::from([1, 2]),
                execution_trace: execution_trace.clone(),
                commits_started,
                retirement_started,
                dropped,
            },
        )
        .run(),
    );
    let mut context = Context::from_waker(Waker::noop());

    let pending = execution.as_mut().poll(&mut context);
    assert!(pending.is_pending());
    assert_eq!(
        *execution_trace.lock().unwrap(),
        [
            ExecutionEvent::Commit(vec![0], Step::Continue),
            ExecutionEvent::Next,
            ExecutionEvent::DeliveredInput(1),
            ExecutionEvent::Commit(vec![10], Step::Continue)
        ]
    );
}

#[tokio::test]
async fn local_commitment_advances_only_through_a_later_capability_event() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let driver = direct(
        ClosedInputActor,
        ClosedInputEnvironment {
            events: VecDeque::from([
                ClosedEvent::user(MailAddr(1), Box::new(7)),
                ClosedEvent::Capability(8),
            ]),
            committed: committed.clone(),
        },
    );

    let retirement = driver.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&retirement.disposition, &Ok(Completion::Stopped));
    assert_eq!(*committed.lock().unwrap(), [7, 8]);
}

struct AlternateSettlementEnvironment(VecDeque<u64>);

impl TestActions<SettlementActor> for AlternateSettlementEnvironment {
    type Error = Infallible;
    type Residual = ();

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> Option<<SettlementActor as Behavior>::Event> {
        self.0
            .pop_front()
            .map(|value| User::new(MailAddr(2), value))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(&mut self, _actions: ActionsOf<SettlementActor>) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn retire(&mut self) {}
}

#[tokio::test]
async fn one_behavior_is_substitutable_across_distinct_static_environments() {
    let (recording, _) = execution([9], None);
    let recording_retirement = recording.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&recording_retirement.disposition, &Ok(Completion::Stopped));

    let alternate = direct(
        SettlementActor::default(),
        AlternateSettlementEnvironment(VecDeque::from([9])),
    );
    let alternate_retirement = alternate.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&alternate_retirement.disposition, &Ok(Completion::Stopped));
}

#[tokio::test]
async fn exact_behavior_and_environment_errors_remain_distinct() {
    let (behavior_failure, _) = execution([7], None);
    let behavior_retirement = behavior_failure.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(
        &behavior_retirement.disposition,
        &Err(DriverError::Behavior("controlled")),
    );

    let (environment_failure, _) = execution([6], Some(61));
    let environment_retirement = environment_failure.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(
        &environment_retirement.disposition,
        &Err(DriverError::Settlement(SettlementFailure::Corrupt)),
    );
}

#[tokio::test]
async fn controlled_failure_is_terminal_and_commits_no_nonexistent_actions() {
    let (driver, execution_trace) = execution([7, 8], None);
    let retirement = driver.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(
        &retirement.disposition,
        &Err(DriverError::Behavior("controlled")),
    );
    assert_eq!(retirement.behavior.initialized, 1);
    assert_eq!(retirement.behavior.received_inputs, [7]);
    assert_eq!(
        *execution_trace.lock().unwrap(),
        [
            ExecutionEvent::Commit(vec![0], Step::Continue),
            ExecutionEvent::Next,
            ExecutionEvent::DeliveredInput(7),
            ExecutionEvent::Retired,
        ]
    );
}

#[tokio::test]
async fn commit_failure_preserves_the_factual_committed_prefix() {
    let (driver, execution_trace) = execution([6, 10], Some(61));
    let retirement = driver.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(
        &retirement.disposition,
        &Err(DriverError::Settlement(SettlementFailure::Corrupt)),
    );
    assert_eq!(retirement.behavior.initialized, 1);
    assert_eq!(retirement.behavior.received_inputs, [6]);
    assert_eq!(
        *execution_trace.lock().unwrap(),
        [
            ExecutionEvent::Commit(vec![0], Step::Continue),
            ExecutionEvent::Next,
            ExecutionEvent::DeliveredInput(6),
            ExecutionEvent::Commit(vec![60], Step::Continue),
            ExecutionEvent::Retired,
        ]
    );
}

#[tokio::test]
async fn initialization_commit_failure_is_exact_and_terminal() {
    let (driver, execution_trace) = execution([1], Some(0));
    let retirement = driver.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(
        &retirement.disposition,
        &Err(DriverError::Settlement(SettlementFailure::Corrupt)),
    );
    assert_eq!(retirement.behavior.initialized, 1);
    assert_eq!(retirement.behavior.received_inputs, []);
    assert_eq!(
        *execution_trace.lock().unwrap(),
        [
            ExecutionEvent::Commit(Vec::new(), Step::Continue),
            ExecutionEvent::Retired
        ]
    );
}

#[tokio::test]
async fn source_closure_folds_no_synthetic_event_and_retires_once() {
    let (driver, execution_trace) = execution([], None);
    let retirement = driver.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&retirement.disposition, &Ok(Completion::Exhausted));
    assert_eq!(retirement.behavior.initialized, 1);
    assert_eq!(retirement.behavior.received_inputs, []);
    assert_eq!(
        *execution_trace.lock().unwrap(),
        [
            ExecutionEvent::Commit(vec![0], Step::Continue),
            ExecutionEvent::Next,
            ExecutionEvent::Retired
        ]
    );
}

#[tokio::test]
async fn completion_preserves_stop_and_input_exhaustion_as_success() {
    let (stopping, _) = execution([9], None);
    let (closing, _) = execution([], None);

    let stopping_retirement = stopping.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&stopping_retirement.disposition, &Ok(Completion::Stopped));
    let closing_retirement = closing.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&closing_retirement.disposition, &Ok(Completion::Exhausted));
}

struct RejectedInitialization;

impl Behavior for RejectedInitialization {
    type Protocol = MessageProtocol<MailAddr, Never>;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = &'static str;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Err("init")
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

struct RejectedInitializationEnvironment(Arc<AtomicUsize>);

impl TestActions<RejectedInitialization> for RejectedInitializationEnvironment {
    type Error = Infallible;
    type Residual = ();

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> Option<<RejectedInitialization as Behavior>::Event> {
        None
    }
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(
        &mut self,
        _actions: ActionsOf<RejectedInitialization>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn retire(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn initialization_failure_returns_definition_and_retires_prepared_environment() {
    let retirements = Arc::new(AtomicUsize::new(0));
    let retirement = direct(
        RejectedInitialization,
        RejectedInitializationEnvironment(retirements.clone()),
    )
    .run()
    .await
    .unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&retirement.disposition, &Err(DriverError::Behavior("init")));
    assert_eq!(retirements.load(Ordering::SeqCst), 1);
}

struct PanickingInitialization {
    initialized: usize,
    #[expect(
        clippy::redundant_allocation,
        reason = "Preserve native panic object identity separately from its shared payload lifetime."
    )]
    payload: Option<Box<Arc<Vec<u64>>>>,
}

impl Behavior for PanickingInitialization {
    type Protocol = MessageProtocol<MailAddr, Never>;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        self.initialized += 1;
        let payload = self
            .payload
            .take()
            .expect("one original initialization payload");
        resume_unwind(payload)
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

struct InitializationPanicEnvironment {
    retirements: Arc<AtomicUsize>,
    execution_trace: ExecutionTrace,
}

impl Drop for InitializationPanicEnvironment {
    fn drop(&mut self) {
        self.execution_trace
            .lock()
            .unwrap()
            .push(ExecutionEvent::EnvironmentReleased);
    }
}

impl TestActions<PanickingInitialization> for InitializationPanicEnvironment {
    type Error = Infallible;
    type Residual = ();

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> Option<<PanickingInitialization as Behavior>::Event> {
        self.execution_trace
            .lock()
            .unwrap()
            .push(ExecutionEvent::Next);
        None
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(
        &mut self,
        actions: ActionsOf<PanickingInitialization>,
    ) -> Result<(), Self::Error> {
        assert_eq!(actions.sends, Vec::<Never>::new());
        assert_eq!(actions.creates, Creations::empty());
        self.execution_trace
            .lock()
            .unwrap()
            .push(ExecutionEvent::Commit(Vec::new(), actions.become_));
        Ok(())
    }

    async fn retire(&mut self) {
        self.retirements.fetch_add(1, Ordering::SeqCst);
        self.execution_trace
            .lock()
            .unwrap()
            .push(ExecutionEvent::Retired);
    }
}

#[tokio::test]
async fn pure_initialization_panic_returns_surviving_behavior_and_retires_once() {
    let retirements = Arc::new(AtomicUsize::new(0));
    let execution_trace = Arc::new(Mutex::new(Vec::new()));
    let payload = Box::new(Arc::new(vec![103_u64, 107, 109]));
    let payload_identity: *const (dyn Any + Send) = payload.as_ref();
    let retained = Arc::downgrade(payload.as_ref());
    let retirement = direct(
        PanickingInitialization {
            initialized: 0,
            payload: Some(payload),
        },
        InitializationPanicEnvironment {
            retirements: retirements.clone(),
            execution_trace: execution_trace.clone(),
        },
    )
    .run()
    .await
    .unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    execution_trace
        .lock()
        .unwrap()
        .push(ExecutionEvent::RetirementReceived);
    let DriverRetirement {
        behavior:
            PanickingInitialization {
                initialized,
                payload: None,
            },
        residual: (),
        disposition: Err(DriverError::InitializationPanicked(cause)),
        additional_failures,
    } = retirement
    else {
        panic!("initialization panic returns the complete surviving Behavior and native cause");
    };
    assert_eq!(initialized, 1);
    assert!(additional_failures.is_empty());
    assert_eq!(retirements.load(Ordering::SeqCst), 1);
    println!(
        "native fold cleanup: {:?}",
        *execution_trace.lock().unwrap()
    );
    assert_eq!(
        *execution_trace.lock().unwrap(),
        [
            ExecutionEvent::Retired,
            ExecutionEvent::EnvironmentReleased,
            ExecutionEvent::RetirementReceived,
        ]
    );
    assert_eq!(retained.strong_count(), 1);
    let received_payload_identity: *const (dyn Any + Send) = cause.as_ref();
    assert!(ptr::eq(
        received_payload_identity.cast::<()>(),
        payload_identity.cast::<()>(),
    ));
    drop(cause);
    assert_eq!(retained.strong_count(), 0);
}

#[tokio::test]
async fn every_ordinary_return_attempts_retirement_exactly_once() {
    let init_retirements = Arc::new(AtomicUsize::new(0));
    let initialization = direct(
        RejectedInitialization,
        RejectedInitializationEnvironment(init_retirements.clone()),
    )
    .run()
    .await
    .unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(
        &initialization.disposition,
        &Err(DriverError::Behavior("init")),
    );
    assert_eq!(init_retirements.load(Ordering::SeqCst), 1);

    for (events, fail_on) in [
        (Vec::from([1]), Some(0)),
        (Vec::from([7]), None),
        (Vec::from([9]), Some(91)),
        (Vec::from([9]), None),
        (Vec::new(), None),
    ] {
        let (driver, execution_trace) = execution(events, fail_on);
        let _result = driver
            .run()
            .await
            .unwrap_or_else(|driver| {
                drop(driver);
                panic!("the selected test host completes retirement")
            })
            .disposition;
        assert_eq!(
            execution_trace
                .lock()
                .unwrap()
                .iter()
                .filter(|event| **event == ExecutionEvent::Retired)
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn every_ordinary_terminal_edge_is_fused_against_later_work() {
    for (events, fail_on, expected) in [
        (Vec::from([9, 100]), None, Ok(Completion::Stopped)),
        (
            Vec::from([7, 100]),
            None,
            Err(DriverError::Behavior("controlled")),
        ),
        (
            Vec::from([6, 100]),
            Some(61),
            Err(DriverError::Settlement(SettlementFailure::Corrupt)),
        ),
        (Vec::new(), None, Ok(Completion::Exhausted)),
        (
            Vec::from([1]),
            Some(0),
            Err(DriverError::Settlement(SettlementFailure::Corrupt)),
        ),
    ] {
        let (driver, execution_trace) = execution(events, fail_on);
        let retirement = driver.run().await.unwrap_or_else(|driver| {
            drop(driver);
            panic!("the selected test host completes retirement")
        });
        assert_driver_disposition(&retirement.disposition, &expected);
        let execution_trace = execution_trace.lock().unwrap();
        assert_eq!(execution_trace.last(), Some(&ExecutionEvent::Retired));
        assert_eq!(
            execution_trace
                .iter()
                .filter(|event| **event == ExecutionEvent::Retired)
                .count(),
            1
        );
        assert!(
            !execution_trace
                .windows(2)
                .any(|pair| pair[0] == ExecutionEvent::Retired)
        );
    }
}

struct CellActor(Cell<u64>);

impl Behavior for CellActor {
    type Protocol = MessageProtocol<MailAddr, Box<u64>>;
    type Event = User<MailAddr, Box<u64>>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        self.0.set(*event.message);
        Ok(Actions::stop())
    }
}

struct MoveInputEnvironment(Option<Box<u64>>);

impl TestActions<CellActor> for MoveInputEnvironment {
    type Error = Infallible;
    type Residual = ();

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> Option<<CellActor as Behavior>::Event> {
        self.0.take().map(|value| User::new(MailAddr(1), value))
    }
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(&mut self, _actions: ActionsOf<CellActor>) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn retire(&mut self) {}
}

#[tokio::test]
async fn driver_adds_no_sync_clone_or_static_payload_bound() {
    let driver = direct(
        CellActor(Cell::new(0)),
        MoveInputEnvironment(Some(Box::new(42))),
    );
    let retirement = driver.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&retirement.disposition, &Ok(Completion::Stopped));
}

struct PendingEnvironment {
    dropped: Arc<Mutex<ExecutionCustody>>,
    retired: Arc<Mutex<RetirementObservation>>,
}

impl Drop for PendingEnvironment {
    fn drop(&mut self) {
        *self.dropped.lock().unwrap() = ExecutionCustody::Released;
    }
}

impl TestActions<SettlementActor> for PendingEnvironment {
    type Error = Infallible;
    type Residual = ();

    async fn next(&mut self) -> Option<<SettlementActor as Behavior>::Event> {
        pending().await
    }
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(&mut self, _actions: ActionsOf<SettlementActor>) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn retire(&mut self) {
        *self.retired.lock().unwrap() = RetirementObservation::Attempted;
    }
}

#[tokio::test]
async fn cancellation_drops_ownership_without_claiming_async_retirement() {
    let dropped = Arc::new(Mutex::new(ExecutionCustody::Retained));
    let retired = Arc::new(Mutex::new(RetirementObservation::NotAttempted));
    let future = direct(
        SettlementActor::default(),
        PendingEnvironment {
            dropped: dropped.clone(),
            retired: retired.clone(),
        },
    )
    .run();
    let timed_out = timeout(Duration::ZERO, future).await;
    assert!(timed_out.is_err());
    assert_eq!(*dropped.lock().unwrap(), ExecutionCustody::Released);
    assert_eq!(
        *retired.lock().unwrap(),
        RetirementObservation::NotAttempted
    );
}

struct PendingInput {
    polls: Arc<AtomicUsize>,
}

impl Future for PendingInput {
    type Output = Option<<SettlementActor as Behavior>::Event>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}

struct PendingInputEnvironment {
    polls: Arc<AtomicUsize>,
}

impl TestActions<SettlementActor> for PendingInputEnvironment {
    type Error = Infallible;
    type Residual = ();

    fn next(&mut self) -> impl Future<Output = Option<<SettlementActor as Behavior>::Event>> {
        PendingInput {
            polls: self.polls.clone(),
        }
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(&mut self, _actions: ActionsOf<SettlementActor>) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn retire(&mut self) {}
}

struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn pending_input_is_polled_once_without_busy_wait_or_self_wake() {
    let polls = Arc::new(AtomicUsize::new(0));
    let wake_counter = Arc::new(WakeCount(AtomicUsize::new(0)));
    let mut execution = Box::pin(
        direct(
            SettlementActor::default(),
            PendingInputEnvironment {
                polls: polls.clone(),
            },
        )
        .run(),
    );
    let test_waker = Waker::from(wake_counter.clone());
    let mut context = Context::from_waker(&test_waker);

    let pending = execution.as_mut().poll(&mut context);
    assert!(pending.is_pending());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(wake_counter.0.load(Ordering::SeqCst), 0);
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StallAt {
    InitializationCommit,
    Input,
    TurnCommit,
    Retirement,
}

struct StallingEnvironment {
    stall_at: StallAt,
    events: VecDeque<u64>,
    execution_trace: ExecutionTrace,
    commits_started: Arc<AtomicUsize>,
    retirement_started: Arc<Mutex<RetirementObservation>>,
    dropped: Arc<Mutex<ExecutionCustody>>,
}

impl Drop for StallingEnvironment {
    fn drop(&mut self) {
        *self.dropped.lock().unwrap() = ExecutionCustody::Released;
    }
}

impl TestActions<SettlementActor> for StallingEnvironment {
    type Error = Infallible;
    type Residual = ();

    async fn next(&mut self) -> Option<<SettlementActor as Behavior>::Event> {
        if self.stall_at == StallAt::Input {
            pending::<()>().await;
        }
        self.execution_trace
            .lock()
            .unwrap()
            .push(ExecutionEvent::Next);
        let value = self.events.pop_front();
        if let Some(value) = value {
            self.execution_trace
                .lock()
                .unwrap()
                .push(ExecutionEvent::DeliveredInput(value));
        }
        value.map(|value| User::new(MailAddr(1), value))
    }

    async fn apply(&mut self, actions: ActionsOf<SettlementActor>) -> Result<(), Self::Error> {
        assert_eq!(actions.creates, Creations::empty());
        self.execution_trace
            .lock()
            .unwrap()
            .push(ExecutionEvent::Commit(actions.sends, actions.become_));
        let index = self.commits_started.fetch_add(1, Ordering::SeqCst);
        if (self.stall_at == StallAt::InitializationCommit && index == 0)
            || (self.stall_at == StallAt::TurnCommit && index == 1)
        {
            pending::<()>().await;
        }
        Ok(())
    }

    async fn retire(&mut self) {
        *self.retirement_started.lock().unwrap() = RetirementObservation::Attempted;
        if self.stall_at == StallAt::Retirement {
            pending::<()>().await;
        }
    }
}

fn cancellation_at(stall_at: StallAt) -> (usize, RetirementObservation, ExecutionCustody) {
    let commits_started = Arc::new(AtomicUsize::new(0));
    let retirement_started = Arc::new(Mutex::new(RetirementObservation::NotAttempted));
    let dropped = Arc::new(Mutex::new(ExecutionCustody::Retained));
    let events = if matches!(stall_at, StallAt::InitializationCommit | StallAt::Input) {
        VecDeque::new()
    } else {
        VecDeque::from([9])
    };
    let execution_trace = Arc::new(Mutex::new(Vec::new()));
    let mut execution = Box::pin(
        direct(
            SettlementActor::default(),
            StallingEnvironment {
                stall_at,
                events,
                execution_trace: execution_trace.clone(),
                commits_started: commits_started.clone(),
                retirement_started: retirement_started.clone(),
                dropped: dropped.clone(),
            },
        )
        .run(),
    );
    let test_waker = Waker::noop();
    let mut context = Context::from_waker(test_waker);
    let pending = execution.as_mut().poll(&mut context);
    assert!(pending.is_pending());
    drop(execution);
    (
        commits_started.load(Ordering::SeqCst),
        *retirement_started.lock().unwrap(),
        *dropped.lock().unwrap(),
    )
}

#[test]
fn cancellation_at_every_await_drops_ownership_without_false_completion_or_retirement() {
    let initialization_commit = cancellation_at(StallAt::InitializationCommit);
    let input = cancellation_at(StallAt::Input);
    let turn_commit = cancellation_at(StallAt::TurnCommit);
    let retirement = cancellation_at(StallAt::Retirement);
    assert_eq!(
        initialization_commit,
        (
            1,
            RetirementObservation::NotAttempted,
            ExecutionCustody::Released
        )
    );
    assert_eq!(
        input,
        (
            1,
            RetirementObservation::NotAttempted,
            ExecutionCustody::Released
        )
    );
    assert_eq!(
        turn_commit,
        (
            2,
            RetirementObservation::NotAttempted,
            ExecutionCustody::Released
        )
    );
    assert_eq!(
        retirement,
        (
            2,
            RetirementObservation::Attempted,
            ExecutionCustody::Released
        )
    );
}

enum PanicStage {
    Initialization,
    Turn,
}

struct PanickingActor {
    panic_stage: PanicStage,
    initialized: usize,
    received_inputs: Vec<u64>,
    #[expect(
        clippy::redundant_allocation,
        reason = "Preserve native panic object identity separately from its shared payload lifetime."
    )]
    payload: Option<Box<Arc<Vec<u64>>>>,
}

impl Behavior for PanickingActor {
    type Protocol = MessageProtocol<MailAddr, u64>;
    type Event = User<MailAddr, u64>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        self.initialized += 1;
        match &self.panic_stage {
            PanicStage::Initialization => {
                let payload = self
                    .payload
                    .take()
                    .expect("one original initialization cause");
                resume_unwind(payload)
            }
            PanicStage::Turn => Ok(Actions::cont()),
        }
    }
    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        self.received_inputs.push(event.message);
        let payload = self.payload.take().expect("one original event-fold cause");
        resume_unwind(payload)
    }
}

struct PanicEnvironment {
    next: Arc<AtomicUsize>,
    dropped: Arc<Mutex<ExecutionCustody>>,
    execution_trace: ExecutionTrace,
}

impl Drop for PanicEnvironment {
    fn drop(&mut self) {
        *self.dropped.lock().unwrap() = ExecutionCustody::Released;
        self.execution_trace
            .lock()
            .unwrap()
            .push(ExecutionEvent::EnvironmentReleased);
    }
}

impl TestActions<PanickingActor> for PanicEnvironment {
    type Error = Infallible;
    type Residual = ();

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> Option<<PanickingActor as Behavior>::Event> {
        self.next.fetch_add(1, Ordering::SeqCst);
        self.execution_trace
            .lock()
            .unwrap()
            .push(ExecutionEvent::Next);
        self.execution_trace
            .lock()
            .unwrap()
            .push(ExecutionEvent::DeliveredInput(1));
        Some(User::new(MailAddr(1), 1))
    }
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(&mut self, actions: ActionsOf<PanickingActor>) -> Result<(), Self::Error> {
        assert_eq!(actions.sends, Vec::<Never>::new());
        assert_eq!(actions.creates, Creations::empty());
        self.execution_trace
            .lock()
            .unwrap()
            .push(ExecutionEvent::Commit(Vec::new(), actions.become_));
        Ok(())
    }
    async fn retire(&mut self) {
        self.execution_trace
            .lock()
            .unwrap()
            .push(ExecutionEvent::Retired);
    }
}

#[expect(
    clippy::redundant_allocation,
    reason = "Preserve native panic object identity separately from its shared payload lifetime."
)]
fn panic_case(
    panic_stage: PanicStage,
    payload: Box<Arc<Vec<u64>>>,
    execution_trace: &ExecutionTrace,
) -> (
    DriverRetirement<PanickingActor, (), DriverError<Infallible, Infallible>>,
    usize,
    ExecutionCustody,
) {
    let next = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(Mutex::new(ExecutionCustody::Retained));
    let driver = direct(
        PanickingActor {
            panic_stage,
            initialized: 0,
            received_inputs: Vec::new(),
            payload: Some(payload),
        },
        PanicEnvironment {
            next: next.clone(),
            dropped: dropped.clone(),
            execution_trace: execution_trace.clone(),
        },
    );
    let runtime = Builder::new_current_thread().build().unwrap();
    let retirement = runtime.block_on(driver.run()).unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    execution_trace
        .lock()
        .unwrap()
        .push(ExecutionEvent::RetirementReceived);
    (
        retirement,
        next.load(Ordering::SeqCst),
        *dropped.lock().unwrap(),
    )
}

#[test]
fn pure_initialization_panic_retires_before_acquiring_input() {
    let execution_trace = Arc::new(Mutex::new(Vec::new()));
    let payload = Box::new(Arc::new(vec![103_u64, 107, 109]));
    let (retirement, next, custody) =
        panic_case(PanicStage::Initialization, payload, &execution_trace);
    let DriverRetirement {
        behavior:
            PanickingActor {
                panic_stage: PanicStage::Initialization,
                initialized,
                received_inputs,
                payload: None,
            },
        residual: (),
        disposition: Err(DriverError::InitializationPanicked(_)),
        additional_failures,
    } = retirement
    else {
        panic!("initialization panic returns its surviving definition after retirement");
    };
    assert_eq!(initialized, 1);
    assert_eq!(received_inputs, Vec::<u64>::new());
    assert!(additional_failures.is_empty());
    assert_eq!((next, custody), (0, ExecutionCustody::Released));
    assert_eq!(
        *execution_trace.lock().unwrap(),
        [
            ExecutionEvent::Retired,
            ExecutionEvent::EnvironmentReleased,
            ExecutionEvent::RetirementReceived,
        ]
    );
}

#[test]
fn pure_turn_panic_returns_native_cause_after_retirement() {
    let execution_trace = Arc::new(Mutex::new(Vec::new()));
    let payload = Box::new(Arc::new(vec![127_u64, 131, 137]));
    let payload_identity: *const (dyn Any + Send) = payload.as_ref();
    let retained = Arc::downgrade(payload.as_ref());
    let (retirement, next, custody) = panic_case(PanicStage::Turn, payload, &execution_trace);
    let DriverRetirement {
        behavior:
            PanickingActor {
                panic_stage: PanicStage::Turn,
                initialized,
                received_inputs,
                payload: None,
            },
        residual: (),
        disposition: Err(DriverError::TransitionPanicked(cause)),
        additional_failures,
    } = retirement
    else {
        panic!("turn panic returns its surviving actor and original cause after retirement");
    };
    assert_eq!(initialized, 1);
    assert_eq!(received_inputs, [1]);
    assert!(additional_failures.is_empty());
    assert_eq!((next, custody), (1, ExecutionCustody::Released));
    println!(
        "native fold cleanup: {:?}",
        *execution_trace.lock().unwrap()
    );
    assert_eq!(
        *execution_trace.lock().unwrap(),
        [
            ExecutionEvent::Commit(Vec::new(), Step::Continue),
            ExecutionEvent::Next,
            ExecutionEvent::DeliveredInput(1),
            ExecutionEvent::Retired,
            ExecutionEvent::EnvironmentReleased,
            ExecutionEvent::RetirementReceived,
        ]
    );
    assert_eq!(retained.strong_count(), 1);
    let received_payload_identity: *const (dyn Any + Send) = cause.as_ref();
    assert!(ptr::eq(
        received_payload_identity.cast::<()>(),
        payload_identity.cast::<()>(),
    ));
    drop(cause);
    assert_eq!(retained.strong_count(), 0);
}

struct SelfSendEnvironment {
    events: VecDeque<u64>,
    transcript: ExecutionTrace,
}

impl TestActions<SelfSender> for SelfSendEnvironment {
    type Error = Infallible;
    type Residual = ();

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> Option<<SelfSender as Behavior>::Event> {
        self.transcript.lock().unwrap().push(ExecutionEvent::Next);
        let value = self.events.pop_front();
        if let Some(value) = value {
            self.transcript
                .lock()
                .unwrap()
                .push(ExecutionEvent::DeliveredInput(value));
        }
        value.map(|value| User::new(MailAddr(1), value))
    }
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(&mut self, effect: ActionsOf<SelfSender>) -> Result<(), Self::Error> {
        let Actions {
            sends,
            creates,
            become_,
        } = effect;
        assert_eq!(creates, Creations::empty());
        self.transcript
            .lock()
            .unwrap()
            .push(ExecutionEvent::Commit(sends.clone(), become_));
        self.events.extend(sends);
        Ok(())
    }
    async fn retire(&mut self) {}
}

struct SelfSender {
    received_inputs: Vec<u64>,
}

impl Behavior for SelfSender {
    type Protocol = MessageProtocol<MailAddr, u64>;
    type Event = User<MailAddr, u64>;
    type Sends = Vec<u64>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        self.received_inputs.push(event.message);
        if event.message == 1 {
            Ok(Actions::send(vec![2]))
        } else {
            Ok(Actions::stop())
        }
    }
}

#[tokio::test]
async fn self_send_reenters_only_as_a_later_ordinary_event() {
    let transcript = Arc::new(Mutex::new(Vec::new()));
    let driver = direct(
        SelfSender {
            received_inputs: Vec::new(),
        },
        SelfSendEnvironment {
            events: [1].into(),
            transcript: transcript.clone(),
        },
    );
    let retirement = driver.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&retirement.disposition, &Ok(Completion::Stopped));
    assert_eq!(retirement.behavior.received_inputs, [1, 2]);
    assert_eq!(
        *transcript.lock().unwrap(),
        [
            ExecutionEvent::Commit(vec![], Step::Continue),
            ExecutionEvent::Next,
            ExecutionEvent::DeliveredInput(1),
            ExecutionEvent::Commit(vec![2], Step::Continue),
            ExecutionEvent::Next,
            ExecutionEvent::DeliveredInput(2),
            ExecutionEvent::Commit(vec![], Step::Stop(Stopped))
        ]
    );
}

struct CompleteActionActor {
    sends: Option<[Box<u64>; 2]>,
    children: Option<[Box<u64>; 2]>,
    issued_creations: Vec<(CreationId, CreationKind)>,
}

impl Behavior for CompleteActionActor {
    type Protocol = MessageProtocol<MailAddr, Never>;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Box<u64>>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = Births<Box<u64>>;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        let mut creations = CreationSequence::new();
        let first = creations.issue().expect("the first creation ID exists");
        let second = creations.issue().expect("the second creation ID exists");
        self.issued_creations = vec![
            (first, CreationKind::Birth),
            (second, CreationKind::Replacement { previous: first }),
        ];
        let sends = self.sends.take().expect("original sends are owned");
        let [child, replacement] = self.children.take().expect("original children are owned");
        Ok(Actions::new(
            sends.into_iter().collect(),
            [
                CreateChild::birth(first, child),
                CreateChild::replacement(second, first, replacement),
            ]
            .into_iter()
            .collect(),
            Step::Stop(Stopped),
        ))
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

struct CompleteEnvironment(ExecutionTrace);

impl TestActions<CompleteActionActor> for CompleteEnvironment {
    type Error = Infallible;
    type Residual = ();

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> Option<<CompleteActionActor as Behavior>::Event> {
        panic!("initial stop must prevent ingress")
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(&mut self, actions: ActionsOf<CompleteActionActor>) -> Result<(), Self::Error> {
        let Actions {
            sends,
            creates,
            become_,
        } = actions;
        let mut observed = self.0.lock().unwrap();
        for creation in creates {
            let (id, child, kind) = creation.into_parts();
            let allocation = from_ref(child.as_ref()) as usize;
            observed.push(ExecutionEvent::Created(id, kind, *child, allocation));
        }
        let original_sends = sends
            .into_iter()
            .map(|value| {
                let allocation = from_ref(value.as_ref()) as usize;
                (*value, allocation)
            })
            .collect();
        observed.push(ExecutionEvent::CommittedOriginals(original_sends, become_));
        Ok(())
    }

    async fn retire(&mut self) {
        self.0.lock().unwrap().push(ExecutionEvent::Retired);
    }
}

#[tokio::test]
async fn complete_move_only_stop_actions_cross_once_before_completion() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let sends = [Box::new(10), Box::new(11)];
    let children = [Box::new(20), Box::new(21)];
    let send_allocations = sends
        .each_ref()
        .map(|value| from_ref(value.as_ref()) as usize);
    let child_allocations = children
        .each_ref()
        .map(|value| from_ref(value.as_ref()) as usize);
    let retirement = direct(
        CompleteActionActor {
            sends: Some(sends),
            children: Some(children),
            issued_creations: Vec::new(),
        },
        CompleteEnvironment(observed.clone()),
    )
    .run()
    .await
    .unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&retirement.disposition, &Ok(Completion::Stopped));
    assert!(retirement.behavior.sends.is_none());
    assert!(retirement.behavior.children.is_none());
    let [(birth_id, birth_kind), (replacement_id, replacement_kind)] =
        retirement.behavior.issued_creations.as_slice()
    else {
        panic!("both original creation requests must remain identified");
    };
    assert_ne!(birth_id, replacement_id);
    assert_eq!(*birth_kind, CreationKind::Birth);
    assert_eq!(
        *replacement_kind,
        CreationKind::Replacement {
            previous: *birth_id
        }
    );
    assert_eq!(
        *observed.lock().unwrap(),
        [
            ExecutionEvent::Created(*birth_id, *birth_kind, 20, child_allocations[0]),
            ExecutionEvent::Created(*replacement_id, *replacement_kind, 21, child_allocations[1]),
            ExecutionEvent::CommittedOriginals(
                vec![(10, send_allocations[0]), (11, send_allocations[1])],
                Step::Stop(Stopped)
            ),
            ExecutionEvent::Retired,
        ]
    );
}

enum CreationResultEvent {
    Results(Vec<u64>),
}

impl UserEvent for CreationResultEvent {
    type Addr = MailAddr;
    type Message = Never;

    fn user(_: Self::Addr, message: Self::Message) -> Self {
        match message {}
    }

    fn into_user(self) -> Result<User<Self::Addr, Self::Message>, Self> {
        Err(self)
    }
}

struct CreationScopeActor {
    results: Vec<u64>,
}

impl Behavior for CreationScopeActor {
    type Protocol = MessageProtocol<MailAddr, Never>;
    type Event = CreationResultEvent;
    type Sends = Vec<usize>;
    type Ph = Never;
    type Error = &'static str;
    type Birth = Births<usize>;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        let mut creations = CreationSequence::new();
        let first = creations.issue().expect("the first creation ID exists");
        let second = creations.issue().expect("the second creation ID exists");
        Ok(Actions::new(
            vec![90],
            [
                CreateChild::birth(first, 20),
                CreateChild::replacement(second, first, 21),
            ]
            .into_iter()
            .collect(),
            Step::Continue,
        ))
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        let CreationResultEvent::Results(nonces) = event;
        self.results = nonces;
        Ok(Actions::new(
            vec![99],
            Creations::empty(),
            Step::Stop(Stopped),
        ))
    }
}

#[derive(Debug, PartialEq, Eq)]
enum CreationEvent {
    Created(u64, CreationKind, usize),
    Sent(usize),
    Results(Vec<u64>),
    Retired,
}

struct CreationScopeEnvironment {
    result: Option<CreationResultEvent>,
    execution_trace: Arc<Mutex<Vec<CreationEvent>>>,
}

impl TestActions<CreationScopeActor> for CreationScopeEnvironment {
    type Error = Infallible;
    type Residual = ();

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> Option<CreationResultEvent> {
        let result = self.result.take();
        if let Some(CreationResultEvent::Results(nonces)) = &result {
            self.execution_trace
                .lock()
                .unwrap()
                .push(CreationEvent::Results(nonces.clone()));
        }
        result
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(&mut self, actions: ActionsOf<CreationScopeActor>) -> Result<(), Self::Error> {
        let mut nonces = Vec::new();
        let mut execution_trace = self.execution_trace.lock().unwrap();
        for creation in actions.creates {
            let (id, child, kind) = creation.into_parts();
            nonces.push(id.get());
            execution_trace.push(CreationEvent::Created(id.get(), kind, child));
        }
        for send in actions.sends {
            execution_trace.push(CreationEvent::Sent(send));
        }
        drop(execution_trace);
        if !nonces.is_empty() {
            self.result = Some(CreationResultEvent::Results(nonces));
        }
        Ok(())
    }

    async fn retire(&mut self) {
        self.execution_trace
            .lock()
            .unwrap()
            .push(CreationEvent::Retired);
    }
}

#[tokio::test]
async fn environment_preserves_creation_precedence_and_same_action_result_scope() {
    let execution_trace = Arc::new(Mutex::new(Vec::new()));
    let driver = direct(
        CreationScopeActor {
            results: Vec::new(),
        },
        CreationScopeEnvironment {
            result: None,
            execution_trace: execution_trace.clone(),
        },
    );

    let retirement = driver.run().await.unwrap_or_else(|driver| {
        drop(driver);
        panic!("the selected test host completes retirement")
    });
    assert_driver_disposition(&retirement.disposition, &Ok(Completion::Stopped));
    assert_eq!(retirement.residual, ());
    let observed = execution_trace.lock().unwrap();
    let [
        CreationEvent::Created(first, CreationKind::Birth, 20),
        CreationEvent::Created(second, CreationKind::Replacement { previous }, 21),
        CreationEvent::Sent(90),
        CreationEvent::Results(results),
        CreationEvent::Sent(99),
        CreationEvent::Retired,
    ] = observed.as_slice()
    else {
        panic!("creation, send, result, and retirement order changed: {observed:?}");
    };
    assert_ne!(first, second);
    assert_eq!(previous.get(), *first);
    assert_eq!(results.as_slice(), [*first, *second]);
    assert_eq!(retirement.behavior.results, *results);
}
mod support;

use support::{TestActions, direct};

fn assert_driver_disposition<
    Request: PartialEq + Debug,
    BehaviorError: PartialEq + Debug,
    ActivationError: PartialEq + Debug,
>(
    actual: &Result<Completion<Request>, DriverError<BehaviorError, ActivationError>>,
    expected: &Result<Completion<Request>, DriverError<BehaviorError, ActivationError>>,
) {
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => assert_eq!(actual, expected),
        (Err(DriverError::Behavior(actual)), Err(DriverError::Behavior(expected))) => {
            assert_eq!(actual, expected);
        }
        (Err(DriverError::Activation(actual)), Err(DriverError::Activation(expected))) => {
            assert_eq!(actual, expected);
        }
        (Err(DriverError::Settlement(actual)), Err(DriverError::Settlement(expected))) => {
            assert_eq!(actual, expected);
        }
        (
            Err(DriverError::InitializationPanicked(_)),
            Err(DriverError::InitializationPanicked(_)),
        ) => {}
        (actual, expected) => panic!(
            "unexpected disposition in this non-native-panic law: actual={actual:?}, expected={expected:?}"
        ),
    }
}
