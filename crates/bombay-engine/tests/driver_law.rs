use std::cell::Cell;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use behavior::{
    Actions, Behavior, BehaviorActed, Births, Create, CreationKind, MailAddr, Never, NoBirths,
    Step, User, UserEvent,
};
use bombay_engine::{ActiveEnvironment, Completion, Driver, DriverError};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Fact {
    Initialized,
    Commit(Vec<u64>),
    Next,
    Fold(u64),
    Retired,
}

struct Probe {
    facts: Arc<Mutex<Vec<Fact>>>,
}

impl Behavior for Probe {
    type Protocol = behavior::MessageProtocol<MailAddr, u64>;
    type Event = User<MailAddr, u64>;
    type Sends = Vec<u64>;
    type Ph = Never;
    type Error = &'static str;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        self.facts.lock().unwrap().push(Fact::Initialized);
        Ok(Actions::send(vec![0]))
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        let value = event.message;
        self.facts.lock().unwrap().push(Fact::Fold(value));
        match value {
            7 => Err("controlled"),
            9 => Ok(Actions::new(
                vec![90, 91],
                Vec::new(),
                Step::Stop(behavior::Stopped),
            )),
            value => Ok(Actions::send(vec![value * 10])),
        }
    }
}

struct Env {
    events: VecDeque<u64>,
    facts: Arc<Mutex<Vec<Fact>>>,
    fail_on: Option<u64>,
}

type ProbeDriver = Driver<Probe, support::TestEnvironment<Env>>;
type Facts = Arc<Mutex<Vec<Fact>>>;

impl ActiveEnvironment<Probe> for Env {
    type Error = u64;

    async fn next(&mut self) -> Option<<Probe as Behavior>::Event> {
        self.facts.lock().unwrap().push(Fact::Next);
        self.events
            .pop_front()
            .map(|value| User::new(MailAddr(1), value))
    }

    async fn apply(&mut self, effect: bombay_engine::ActionsOf<Probe>) -> Result<(), Self::Error> {
        let Actions { sends, .. } = effect;
        let mut committed = Vec::new();
        for value in sends {
            if self.fail_on == Some(value) {
                self.facts.lock().unwrap().push(Fact::Commit(committed));
                return Err(value);
            }
            committed.push(value);
        }
        self.facts.lock().unwrap().push(Fact::Commit(committed));
        Ok(())
    }

    async fn retire(self) {
        self.facts.lock().unwrap().push(Fact::Retired);
    }
}

fn execution(events: impl IntoIterator<Item = u64>, fail_on: Option<u64>) -> (ProbeDriver, Facts) {
    let facts = Arc::new(Mutex::new(Vec::new()));
    let driver = direct(
        Probe {
            facts: facts.clone(),
        },
        Env {
            events: events.into_iter().collect(),
            facts: facts.clone(),
            fail_on,
        },
    );
    (driver, facts)
}

#[tokio::test]
async fn universal_causal_transcript_has_no_prefetch_or_reentrancy() {
    let (driver, facts) = execution([1, 2, 9, 100], None);
    assert_eq!(driver.run().await, Ok(Completion::Stopped));
    assert_eq!(
        *facts.lock().unwrap(),
        [
            Fact::Initialized,
            Fact::Commit(vec![0]),
            Fact::Next,
            Fact::Fold(1),
            Fact::Commit(vec![10]),
            Fact::Next,
            Fact::Fold(2),
            Fact::Commit(vec![20]),
            Fact::Next,
            Fact::Fold(9),
            Fact::Commit(vec![90, 91]),
            Fact::Retired,
        ]
    );
}

#[tokio::test]
async fn every_successful_decision_is_committed_exactly_once() {
    let (driver, facts) = execution([1, 2, 9, 100], None);
    assert_eq!(driver.run().await, Ok(Completion::Stopped));
    let facts = facts.lock().unwrap();
    assert_eq!(
        facts
            .iter()
            .filter(|fact| matches!(fact, Fact::Commit(_)))
            .count(),
        4
    );
    assert_eq!(
        facts
            .iter()
            .filter(|fact| matches!(fact, Fact::Fold(_)))
            .count(),
        3
    );
}

#[tokio::test]
async fn initialization_occurs_once_across_stop_closure_and_failure_boundaries() {
    for (events, fail_on) in [
        (Vec::from([9]), None),
        (Vec::new(), None),
        (Vec::from([7]), None),
        (Vec::from([1]), Some(0)),
        (Vec::from([9]), Some(91)),
    ] {
        let (driver, facts) = execution(events, fail_on);
        let _result = driver.run().await;
        assert_eq!(
            facts
                .lock()
                .unwrap()
                .iter()
                .filter(|fact| **fact == Fact::Initialized)
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn every_accepted_event_is_folded_exactly_once() {
    let (driver, facts) = execution([1, 2, 9, 100], None);
    assert_eq!(driver.run().await, Ok(Completion::Stopped));
    let folds: Vec<_> = facts
        .lock()
        .unwrap()
        .iter()
        .filter_map(|fact| match fact {
            Fact::Fold(value) => Some(*value),
            _ => None,
        })
        .collect();
    assert_eq!(folds, [1, 2, 9]);
}

struct StatefulDecision {
    value: usize,
    dropped_value: Arc<AtomicUsize>,
}

impl Drop for StatefulDecision {
    fn drop(&mut self) {
        self.dropped_value.store(self.value, Ordering::SeqCst);
    }
}

impl Behavior for StatefulDecision {
    type Protocol = behavior::MessageProtocol<MailAddr, usize>;
    type Event = User<MailAddr, usize>;
    type Sends = Vec<usize>;
    type Ph = Never;
    type Error = &'static str;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::send(vec![self.value]))
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        if event.message == usize::MAX {
            return Err("stateful failure");
        }
        if event.message == 0 {
            return Ok(Actions::new(
                vec![self.value],
                Vec::new(),
                Step::Stop(behavior::Stopped),
            ));
        }
        self.value += event.message;
        Ok(Actions::send(vec![self.value]))
    }
}

struct StatefulDecisionEnv {
    events: VecDeque<usize>,
    committed: Arc<Mutex<Vec<usize>>>,
}

impl ActiveEnvironment<StatefulDecision> for StatefulDecisionEnv {
    type Error = Infallible;

    async fn next(&mut self) -> Option<<StatefulDecision as Behavior>::Event> {
        self.events
            .pop_front()
            .map(|value| User::new(MailAddr(1), value))
    }

    async fn apply(
        &mut self,
        actions: bombay_engine::ActionsOf<StatefulDecision>,
    ) -> Result<(), Self::Error> {
        self.committed.lock().unwrap().extend(actions.sends);
        Ok(())
    }

    async fn retire(self) {}
}

#[tokio::test]
async fn successor_state_and_complete_actions_come_from_the_same_decision() {
    let dropped_value = Arc::new(AtomicUsize::new(usize::MAX));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let driver = direct(
        StatefulDecision {
            value: 0,
            dropped_value: dropped_value.clone(),
        },
        StatefulDecisionEnv {
            events: VecDeque::from([2, 3, 0, 100]),
            committed: committed.clone(),
        },
    );

    assert_eq!(driver.run().await, Ok(Completion::Stopped));
    assert_eq!(*committed.lock().unwrap(), [0, 2, 5, 5]);
    assert_eq!(dropped_value.load(Ordering::SeqCst), 5);
}

struct FailingStateEnv {
    event: Option<usize>,
    commits: usize,
}

impl ActiveEnvironment<StatefulDecision> for FailingStateEnv {
    type Error = &'static str;

    async fn next(&mut self) -> Option<<StatefulDecision as Behavior>::Event> {
        self.event.take().map(|value| User::new(MailAddr(1), value))
    }

    async fn apply(
        &mut self,
        _actions: bombay_engine::ActionsOf<StatefulDecision>,
    ) -> Result<(), Self::Error> {
        self.commits += 1;
        if self.commits == 2 {
            Err("commit")
        } else {
            Ok(())
        }
    }

    async fn retire(self) {}
}

#[tokio::test]
async fn commitment_failure_does_not_roll_back_the_successful_fold() {
    let dropped_value = Arc::new(AtomicUsize::new(usize::MAX));
    let driver = direct(
        StatefulDecision {
            value: 0,
            dropped_value: dropped_value.clone(),
        },
        FailingStateEnv {
            event: Some(3),
            commits: 0,
        },
    );

    assert_eq!(driver.run().await, Err(DriverError::Environment("commit")));
    assert_eq!(dropped_value.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn unrelated_custom_behavior_shapes_use_the_same_driver_algorithm() {
    let (probe, _) = execution([9], None);
    assert_eq!(probe.run().await, Ok(Completion::Stopped));

    assert_eq!(
        direct(SendNotSync(Cell::new(0)), MoveEnv(Some(Box::new(42))))
            .run()
            .await,
        Ok(Completion::Stopped)
    );

    let complete = Arc::new(Mutex::new(Vec::new()));
    assert_eq!(
        direct(CompleteActions, CompleteEnvironment(complete))
            .run()
            .await,
        Ok(Completion::Stopped)
    );
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

struct ClosedInputBehavior;

impl Behavior for ClosedInputBehavior {
    type Protocol = behavior::MessageProtocol<MailAddr, Box<usize>>;
    type Event = ClosedEvent;
    type Sends = Vec<usize>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event {
            ClosedEvent::User(user) => Ok(Actions::send(vec![*user.message])),
            ClosedEvent::Capability(value) => Ok(Actions::new(
                vec![value],
                Vec::new(),
                Step::Stop(behavior::Stopped),
            )),
        }
    }
}

struct ClosedInputEnv {
    events: VecDeque<ClosedEvent>,
    committed: Arc<Mutex<Vec<usize>>>,
}

impl ActiveEnvironment<ClosedInputBehavior> for ClosedInputEnv {
    type Error = Infallible;

    async fn next(&mut self) -> Option<ClosedEvent> {
        self.events.pop_front()
    }

    async fn apply(
        &mut self,
        actions: bombay_engine::ActionsOf<ClosedInputBehavior>,
    ) -> Result<(), Self::Error> {
        self.committed.lock().unwrap().extend(actions.sends);
        Ok(())
    }

    async fn retire(self) {}
}

#[tokio::test]
async fn driver_accepts_only_the_final_closed_behavior_event_type() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let driver = direct(
        ClosedInputBehavior,
        ClosedInputEnv {
            events: VecDeque::from([
                ClosedEvent::user(MailAddr(1), Box::new(7)),
                ClosedEvent::Capability(8),
            ]),
            committed: committed.clone(),
        },
    );

    assert_eq!(driver.run().await, Ok(Completion::Stopped));
    assert_eq!(*committed.lock().unwrap(), [7, 8]);
}

struct ExclusiveFold {
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
}

impl Behavior for ExclusiveFold {
    type Protocol = behavior::MessageProtocol<MailAddr, usize>;
    type Event = User<MailAddr, usize>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        let previous = self.active.fetch_add(1, Ordering::SeqCst);
        self.maximum.fetch_max(previous + 1, Ordering::SeqCst);
        self.active.fetch_sub(1, Ordering::SeqCst);
        if event.message == 0 {
            Ok(Actions::stop())
        } else {
            Ok(Actions::cont())
        }
    }
}

struct ExclusiveFoldEnv(VecDeque<usize>);

impl ActiveEnvironment<ExclusiveFold> for ExclusiveFoldEnv {
    type Error = Infallible;

    async fn next(&mut self) -> Option<<ExclusiveFold as Behavior>::Event> {
        self.0
            .pop_front()
            .map(|value| User::new(MailAddr(1), value))
    }

    async fn apply(
        &mut self,
        _actions: bombay_engine::ActionsOf<ExclusiveFold>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn retire(self) {}
}

#[tokio::test]
async fn at_most_one_behavior_fold_is_active() {
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let driver = direct(
        ExclusiveFold {
            active: active.clone(),
            maximum: maximum.clone(),
        },
        ExclusiveFoldEnv(VecDeque::from([1, 1, 0])),
    );

    assert_eq!(driver.run().await, Ok(Completion::Stopped));
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(maximum.load(Ordering::SeqCst), 1);
}

#[test]
fn pending_commit_prevents_reentrant_or_later_fold() {
    let commits_started = Arc::new(AtomicUsize::new(0));
    let retirement_started = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let facts = Arc::new(Mutex::new(Vec::new()));
    let mut execution = Box::pin(
        direct(
            Probe {
                facts: facts.clone(),
            },
            StallingEnv {
                stall_at: StallAt::TurnCommit,
                events: VecDeque::from([1, 2]),
                commits_started,
                retirement_started,
                dropped,
            },
        )
        .run(),
    );
    let mut context = Context::from_waker(Waker::noop());

    assert!(execution.as_mut().poll(&mut context).is_pending());
    assert_eq!(*facts.lock().unwrap(), [Fact::Initialized, Fact::Fold(1)]);
}

#[tokio::test]
async fn local_commitment_advances_only_through_a_later_capability_event() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let driver = direct(
        ClosedInputBehavior,
        ClosedInputEnv {
            events: VecDeque::from([
                ClosedEvent::user(MailAddr(1), Box::new(7)),
                ClosedEvent::Capability(8),
            ]),
            committed: committed.clone(),
        },
    );

    assert_eq!(driver.run().await, Ok(Completion::Stopped));
    assert_eq!(*committed.lock().unwrap(), [7, 8]);
}

struct AlternateProbeEnv(VecDeque<u64>);

impl ActiveEnvironment<Probe> for AlternateProbeEnv {
    type Error = Infallible;

    async fn next(&mut self) -> Option<<Probe as Behavior>::Event> {
        self.0
            .pop_front()
            .map(|value| User::new(MailAddr(2), value))
    }

    async fn apply(
        &mut self,
        _actions: bombay_engine::ActionsOf<Probe>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn retire(self) {}
}

#[tokio::test]
async fn one_behavior_is_substitutable_across_distinct_static_environments() {
    let (recording, _) = execution([9], None);
    assert_eq!(recording.run().await, Ok(Completion::Stopped));

    let facts = Arc::new(Mutex::new(Vec::new()));
    let alternate = direct(Probe { facts }, AlternateProbeEnv(VecDeque::from([9])));
    assert_eq!(alternate.run().await, Ok(Completion::Stopped));
}

#[tokio::test]
async fn exact_behavior_and_environment_errors_remain_distinct() {
    let (behavior_failure, _) = execution([7], None);
    assert_eq!(
        behavior_failure.run().await,
        Err(DriverError::Behavior("controlled"))
    );

    let (environment_failure, _) = execution([9], Some(91));
    assert_eq!(
        environment_failure.run().await,
        Err(DriverError::Environment(91))
    );
}

#[tokio::test]
async fn controlled_failure_is_terminal_and_commits_no_nonexistent_actions() {
    let (driver, facts) = execution([7, 8], None);
    assert_eq!(driver.run().await, Err(DriverError::Behavior("controlled")));
    assert_eq!(
        *facts.lock().unwrap(),
        [
            Fact::Initialized,
            Fact::Commit(vec![0]),
            Fact::Next,
            Fact::Fold(7),
            Fact::Retired,
        ]
    );
}

#[tokio::test]
async fn commit_failure_preserves_the_factual_committed_prefix() {
    let (driver, facts) = execution([9, 10], Some(91));
    assert_eq!(driver.run().await, Err(DriverError::Environment(91)));
    assert_eq!(
        *facts.lock().unwrap(),
        [
            Fact::Initialized,
            Fact::Commit(vec![0]),
            Fact::Next,
            Fact::Fold(9),
            Fact::Commit(vec![90]),
            Fact::Retired,
        ]
    );
}

#[tokio::test]
async fn initialization_commit_failure_is_exact_and_terminal() {
    let (driver, facts) = execution([1], Some(0));
    assert_eq!(driver.run().await, Err(DriverError::Activation(0)));
    assert_eq!(
        *facts.lock().unwrap(),
        [Fact::Initialized, Fact::Commit(Vec::new()), Fact::Retired]
    );
}

#[tokio::test]
async fn source_closure_folds_no_synthetic_event_and_retires_once() {
    let (driver, facts) = execution([], None);
    assert_eq!(driver.run().await, Ok(Completion::Exhausted));
    assert_eq!(
        *facts.lock().unwrap(),
        [
            Fact::Initialized,
            Fact::Commit(vec![0]),
            Fact::Next,
            Fact::Retired
        ]
    );
}

#[tokio::test]
async fn completion_preserves_stop_and_input_exhaustion_as_success() {
    let (stopping, _) = execution([9], None);
    let (closing, _) = execution([], None);

    assert_eq!(stopping.run().await, Ok(Completion::Stopped));
    assert_eq!(closing.run().await, Ok(Completion::Exhausted));
}

struct InitFailure;

impl Behavior for InitFailure {
    type Protocol = behavior::MessageProtocol<MailAddr, Never>;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = &'static str;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Err("init")
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

struct EmptyEnv(Arc<AtomicUsize>);

impl ActiveEnvironment<InitFailure> for EmptyEnv {
    type Error = Infallible;

    async fn next(&mut self) -> Option<<InitFailure as Behavior>::Event> {
        None
    }
    async fn apply(
        &mut self,
        _actions: bombay_engine::ActionsOf<InitFailure>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn retire(self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn initialization_failure_consumes_definition_and_drops_prepared_environment() {
    let retirements = Arc::new(AtomicUsize::new(0));
    assert_eq!(
        direct(InitFailure, EmptyEnv(retirements.clone()))
            .run()
            .await,
        Err(DriverError::Behavior("init"))
    );
    assert_eq!(retirements.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn every_ordinary_return_attempts_retirement_exactly_once() {
    let init_retirements = Arc::new(AtomicUsize::new(0));
    assert_eq!(
        direct(InitFailure, EmptyEnv(init_retirements.clone()))
            .run()
            .await,
        Err(DriverError::Behavior("init"))
    );
    assert_eq!(init_retirements.load(Ordering::SeqCst), 0);

    for (events, fail_on) in [
        (Vec::from([1]), Some(0)),
        (Vec::from([7]), None),
        (Vec::from([9]), Some(91)),
        (Vec::from([9]), None),
        (Vec::new(), None),
    ] {
        let (driver, facts) = execution(events, fail_on);
        let _result = driver.run().await;
        assert_eq!(
            facts
                .lock()
                .unwrap()
                .iter()
                .filter(|fact| **fact == Fact::Retired)
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
            Vec::from([9, 100]),
            Some(91),
            Err(DriverError::Environment(91)),
        ),
        (Vec::new(), None, Ok(Completion::Exhausted)),
        (Vec::from([1]), Some(0), Err(DriverError::Activation(0))),
    ] {
        let (driver, facts) = execution(events, fail_on);
        assert_eq!(driver.run().await, expected);
        let facts = facts.lock().unwrap();
        assert_eq!(facts.last(), Some(&Fact::Retired));
        assert_eq!(
            facts.iter().filter(|fact| **fact == Fact::Retired).count(),
            1
        );
        assert!(!facts.windows(2).any(|pair| pair[0] == Fact::Retired));
    }
}

struct SendNotSync(Cell<u64>);

impl Behavior for SendNotSync {
    type Protocol = behavior::MessageProtocol<MailAddr, Box<u64>>;
    type Event = User<MailAddr, Box<u64>>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        self.0.set(*event.message);
        Ok(Actions::stop())
    }
}

struct MoveEnv(Option<Box<u64>>);

impl ActiveEnvironment<SendNotSync> for MoveEnv {
    type Error = Infallible;

    async fn next(&mut self) -> Option<<SendNotSync as Behavior>::Event> {
        self.0.take().map(|value| User::new(MailAddr(1), value))
    }
    async fn apply(
        &mut self,
        _actions: bombay_engine::ActionsOf<SendNotSync>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn retire(self) {}
}

#[tokio::test]
async fn driver_adds_no_sync_clone_or_static_payload_bound() {
    let driver = direct(SendNotSync(Cell::new(0)), MoveEnv(Some(Box::new(42))));
    assert_eq!(driver.run().await, Ok(Completion::Stopped));
}

struct PendingEnv {
    dropped: Arc<AtomicBool>,
    retired: Arc<AtomicBool>,
}

impl Drop for PendingEnv {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

impl ActiveEnvironment<Probe> for PendingEnv {
    type Error = Infallible;

    async fn next(&mut self) -> Option<<Probe as Behavior>::Event> {
        std::future::pending().await
    }
    async fn apply(
        &mut self,
        _actions: bombay_engine::ActionsOf<Probe>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn retire(self) {
        self.retired.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancellation_drops_ownership_without_claiming_async_retirement() {
    let dropped = Arc::new(AtomicBool::new(false));
    let retired = Arc::new(AtomicBool::new(false));
    let facts = Arc::new(Mutex::new(Vec::new()));
    let future = direct(
        Probe { facts },
        PendingEnv {
            dropped: dropped.clone(),
            retired: retired.clone(),
        },
    )
    .run();
    assert!(
        tokio::time::timeout(std::time::Duration::ZERO, future)
            .await
            .is_err()
    );
    assert!(dropped.load(Ordering::SeqCst));
    assert!(!retired.load(Ordering::SeqCst));
}

struct PendingInputProbe {
    polls: Arc<AtomicUsize>,
}

impl Future for PendingInputProbe {
    type Output = Option<<Probe as Behavior>::Event>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}

struct PendingInputEnv {
    polls: Arc<AtomicUsize>,
}

impl ActiveEnvironment<Probe> for PendingInputEnv {
    type Error = Infallible;

    fn next(&mut self) -> impl Future<Output = Option<<Probe as Behavior>::Event>> {
        PendingInputProbe {
            polls: self.polls.clone(),
        }
    }

    async fn apply(
        &mut self,
        _actions: bombay_engine::ActionsOf<Probe>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn retire(self) {}
}

struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
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
    let wake_counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let facts = Arc::new(Mutex::new(Vec::new()));
    let mut execution = Box::pin(
        direct(
            Probe { facts },
            PendingInputEnv {
                polls: polls.clone(),
            },
        )
        .run(),
    );
    let test_waker = Waker::from(wake_counter.clone());
    let mut context = Context::from_waker(&test_waker);

    assert!(execution.as_mut().poll(&mut context).is_pending());
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

struct StallingEnv {
    stall_at: StallAt,
    events: VecDeque<u64>,
    commits_started: Arc<AtomicUsize>,
    retirement_started: Arc<AtomicBool>,
    dropped: Arc<AtomicBool>,
}

impl Drop for StallingEnv {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

impl ActiveEnvironment<Probe> for StallingEnv {
    type Error = Infallible;

    async fn next(&mut self) -> Option<<Probe as Behavior>::Event> {
        if self.stall_at == StallAt::Input {
            std::future::pending::<()>().await;
        }
        self.events
            .pop_front()
            .map(|value| User::new(MailAddr(1), value))
    }

    async fn apply(
        &mut self,
        _actions: bombay_engine::ActionsOf<Probe>,
    ) -> Result<(), Self::Error> {
        let index = self.commits_started.fetch_add(1, Ordering::SeqCst);
        if (self.stall_at == StallAt::InitializationCommit && index == 0)
            || (self.stall_at == StallAt::TurnCommit && index == 1)
        {
            std::future::pending::<()>().await;
        }
        Ok(())
    }

    async fn retire(self) {
        self.retirement_started.store(true, Ordering::SeqCst);
        if self.stall_at == StallAt::Retirement {
            std::future::pending::<()>().await;
        }
    }
}

fn cancellation_at(stall_at: StallAt) -> (usize, bool, bool) {
    let commits_started = Arc::new(AtomicUsize::new(0));
    let retirement_started = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let facts = Arc::new(Mutex::new(Vec::new()));
    let events = if matches!(stall_at, StallAt::InitializationCommit | StallAt::Input) {
        VecDeque::new()
    } else {
        VecDeque::from([9])
    };
    let mut execution = Box::pin(
        direct(
            Probe { facts },
            StallingEnv {
                stall_at,
                events,
                commits_started: commits_started.clone(),
                retirement_started: retirement_started.clone(),
                dropped: dropped.clone(),
            },
        )
        .run(),
    );
    let test_waker = Waker::noop();
    let mut context = Context::from_waker(test_waker);
    assert!(execution.as_mut().poll(&mut context).is_pending());
    drop(execution);
    (
        commits_started.load(Ordering::SeqCst),
        retirement_started.load(Ordering::SeqCst),
        dropped.load(Ordering::SeqCst),
    )
}

#[test]
fn cancellation_at_every_await_drops_ownership_without_false_completion_or_retirement() {
    assert_eq!(
        cancellation_at(StallAt::InitializationCommit),
        (1, false, true)
    );
    assert_eq!(cancellation_at(StallAt::Input), (1, false, true));
    assert_eq!(cancellation_at(StallAt::TurnCommit), (2, false, true));
    assert_eq!(cancellation_at(StallAt::Retirement), (2, true, true));
}

struct PanicBehavior {
    panic_in_init: bool,
}

impl Behavior for PanicBehavior {
    type Protocol = behavior::MessageProtocol<MailAddr, u64>;
    type Event = User<MailAddr, u64>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        assert!(!self.panic_in_init, "injected initialization panic");
        Ok(Actions::cont())
    }
    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        panic!("injected fold panic")
    }
}

struct PanicEnv {
    next: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
}

impl Drop for PanicEnv {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

impl ActiveEnvironment<PanicBehavior> for PanicEnv {
    type Error = Infallible;

    async fn next(&mut self) -> Option<<PanicBehavior as Behavior>::Event> {
        self.next.fetch_add(1, Ordering::SeqCst);
        Some(User::new(MailAddr(1), 1))
    }
    async fn apply(
        &mut self,
        _actions: bombay_engine::ActionsOf<PanicBehavior>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn retire(self) {}
}

fn panic_case(panic_in_init: bool) -> (bool, usize, bool) {
    let next = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let driver = direct(
        PanicBehavior { panic_in_init },
        PanicEnv {
            next: next.clone(),
            dropped: dropped.clone(),
        },
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime.block_on(driver.run())
    }));
    (
        panic.is_err(),
        next.load(Ordering::SeqCst),
        dropped.load(Ordering::SeqCst),
    )
}

#[test]
fn initialization_and_turn_panics_consume_the_only_execution_and_cannot_poll_again() {
    assert_eq!(panic_case(true), (true, 0, true));
    assert_eq!(panic_case(false), (true, 1, true));
}

#[test]
fn panic_consumes_the_only_execution_and_cannot_poll_again() {
    assert_eq!(panic_case(false), (true, 1, true));
}

struct SelfSendEnv {
    events: VecDeque<u64>,
    transcript: Arc<Mutex<Vec<&'static str>>>,
}

impl ActiveEnvironment<SelfSender> for SelfSendEnv {
    type Error = Infallible;

    async fn next(&mut self) -> Option<<SelfSender as Behavior>::Event> {
        self.transcript.lock().unwrap().push("next");
        self.events
            .pop_front()
            .map(|value| User::new(MailAddr(1), value))
    }
    async fn apply(
        &mut self,
        effect: bombay_engine::ActionsOf<SelfSender>,
    ) -> Result<(), Self::Error> {
        let Actions { sends, .. } = effect;
        self.transcript.lock().unwrap().push("commit");
        self.events.extend(sends);
        Ok(())
    }
    async fn retire(self) {}
}

struct SelfSender(Arc<Mutex<Vec<&'static str>>>);

impl Behavior for SelfSender {
    type Protocol = behavior::MessageProtocol<MailAddr, u64>;
    type Event = User<MailAddr, u64>;
    type Sends = Vec<u64>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        self.0.lock().unwrap().push(if event.message == 1 {
            "fold-1"
        } else {
            "fold-2"
        });
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
        SelfSender(transcript.clone()),
        SelfSendEnv {
            events: [1].into(),
            transcript: transcript.clone(),
        },
    );
    assert_eq!(driver.run().await, Ok(Completion::Stopped));
    assert_eq!(
        *transcript.lock().unwrap(),
        [
            "commit", "next", "fold-1", "commit", "next", "fold-2", "commit"
        ]
    );
}

struct CompleteActions;

impl Behavior for CompleteActions {
    type Protocol = behavior::MessageProtocol<MailAddr, Never>;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Box<u64>>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = Births<Box<u64>>;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::new(
            vec![Box::new(10), Box::new(11)],
            vec![
                Create::new(7, Box::new(20), CreationKind::Birth),
                Create::new(8, Box::new(21), CreationKind::replacement_of(7)),
            ],
            Step::Stop(behavior::Stopped),
        ))
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

struct CompleteEnvironment(Arc<Mutex<Vec<u64>>>);

impl ActiveEnvironment<CompleteActions> for CompleteEnvironment {
    type Error = Infallible;

    async fn next(&mut self) -> Option<<CompleteActions as Behavior>::Event> {
        panic!("initial stop must prevent ingress")
    }

    async fn apply(
        &mut self,
        actions: bombay_engine::ActionsOf<CompleteActions>,
    ) -> Result<(), Self::Error> {
        let Actions {
            sends,
            creates,
            become_,
        } = actions;
        let mut observed = self.0.lock().unwrap();
        observed.extend(sends.into_iter().map(|value| *value));
        for creation in creates {
            observed.push(creation.nonce);
            observed.push(*creation.child);
        }
        assert!(matches!(become_, Step::Stop(behavior::Stopped)));
        Ok(())
    }

    async fn retire(self) {
        self.0.lock().unwrap().push(99);
    }
}

#[tokio::test]
async fn complete_move_only_stop_actions_cross_once_before_completion() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    assert_eq!(
        direct(CompleteActions, CompleteEnvironment(observed.clone()))
            .run()
            .await,
        Ok(Completion::Stopped)
    );
    assert_eq!(*observed.lock().unwrap(), [10, 11, 7, 20, 8, 21, 99]);
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

struct CreationScopeBehavior;

impl Behavior for CreationScopeBehavior {
    type Protocol = behavior::MessageProtocol<MailAddr, Never>;
    type Event = CreationResultEvent;
    type Sends = Vec<usize>;
    type Ph = Never;
    type Error = &'static str;
    type Birth = Births<usize>;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::new(
            vec![90],
            vec![
                Create::new(7, 20, CreationKind::Birth),
                Create::new(8, 21, CreationKind::replacement_of(7)),
            ],
            Step::Continue,
        ))
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        let CreationResultEvent::Results(nonces) = event;
        if nonces != [7, 8] {
            return Err("mis-scoped creation results");
        }
        Ok(Actions::new(
            vec![99],
            Vec::new(),
            Step::Stop(behavior::Stopped),
        ))
    }
}

#[derive(Debug, PartialEq, Eq)]
enum CreationFact {
    Created(u64, usize),
    Sent(usize),
    Results(Vec<u64>),
    Retired,
}

struct CreationScopeEnv {
    result: Option<CreationResultEvent>,
    facts: Arc<Mutex<Vec<CreationFact>>>,
}

impl ActiveEnvironment<CreationScopeBehavior> for CreationScopeEnv {
    type Error = Infallible;

    async fn next(&mut self) -> Option<CreationResultEvent> {
        let result = self.result.take();
        if let Some(CreationResultEvent::Results(nonces)) = &result {
            self.facts
                .lock()
                .unwrap()
                .push(CreationFact::Results(nonces.clone()));
        }
        result
    }

    async fn apply(
        &mut self,
        actions: bombay_engine::ActionsOf<CreationScopeBehavior>,
    ) -> Result<(), Self::Error> {
        let mut nonces = Vec::new();
        let mut facts = self.facts.lock().unwrap();
        for creation in actions.creates {
            nonces.push(creation.nonce);
            facts.push(CreationFact::Created(creation.nonce, creation.child));
        }
        for send in actions.sends {
            facts.push(CreationFact::Sent(send));
        }
        drop(facts);
        if !nonces.is_empty() {
            self.result = Some(CreationResultEvent::Results(nonces));
        }
        Ok(())
    }

    async fn retire(self) {
        self.facts.lock().unwrap().push(CreationFact::Retired);
    }
}

#[tokio::test]
async fn environment_preserves_creation_precedence_and_same_action_result_scope() {
    let facts = Arc::new(Mutex::new(Vec::new()));
    let driver = direct(
        CreationScopeBehavior,
        CreationScopeEnv {
            result: None,
            facts: facts.clone(),
        },
    );

    assert_eq!(driver.run().await, Ok(Completion::Stopped));
    assert_eq!(
        *facts.lock().unwrap(),
        [
            CreationFact::Created(7, 20),
            CreationFact::Created(8, 21),
            CreationFact::Sent(90),
            CreationFact::Results(vec![7, 8]),
            CreationFact::Sent(99),
            CreationFact::Retired,
        ]
    );
}
mod support;

use support::direct;
