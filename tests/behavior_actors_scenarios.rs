use std::collections::VecDeque;
use std::convert::Infallible;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use behavior::{
    AcknowledgementError, AcknowledgementMessage, AcknowledgementOutcome, Acknowledgements,
    Actions, Barrier, BarrierConfigError, BarrierError, BarrierGeneration, BarrierMessage,
    BarrierReleased, Behavior, BehaviorActed, Births, BreakerAttempt, BreakerConfigError,
    BreakerMessage, BreakerOutcome, BreakerRejection, Broadcast, Buffer, BufferConfigError,
    BufferMessage, BufferOutcome, BufferRejection, Cache, CacheConfigError, CacheEntry,
    CacheMessage, CacheResult, ChildStopped, ChildTopology, CircuitBreaker, Compose, Configuration,
    ConfigurationError, ConfigurationMessage, ConfigurationState, ConfigurationVersion,
    ConsistentHash, CorrelationResult, Correlator, CorrelatorError, CorrelatorMessage, Crash,
    CreationKind, CreationRejection, CreationResolved, DeadlineEvent, Deduplicator,
    DeduplicatorConfigError, DeduplicatorMessage, DeduplicatorOutcome, Delivery, EventInput, Exit,
    Feature, FeatureSet, FeatureStatus, Features, HashPolicyError, Health, HealthError,
    HealthMessage, HealthReport, HealthStatus, InterruptionPolicy, KeyedWorkerPool, Latch,
    LatchMessage, LatchReleased, Lease, LeaseMessage, LeaseOutcome, LeaseRejection, LeaseSends,
    LeastLoaded, LeastLoadedError, Load, LoadObservation, LoadVersion, Machine, MailAddr,
    MemberToken, MemberTokenObservation, MemberTokenVersion, Move, Never, NoBirths,
    ObservationVersion, ObservePeer, OrderGate, OrderGateMessage, OrderGateOutcome, OverflowPolicy,
    PeerStopped, PoolAssignment, PoolConfigError, PoolConfiguration, PoolResponse, Presence,
    PresenceError, PresenceMessage, PresenceOutcome, PresencePhase, PresenceReply, PresenceVersion,
    PriorityQueue, PriorityQueueConfigError, PriorityQueueMessage, PriorityQueueOutcome,
    PriorityQueueRejection, Proxy, ProxyCommand, ProxyEvent, PubSub, PubSubError, PubSubMessage,
    RateLimitRejection, RateLimiter, RateLimiterConfigError, RateLimiterMessage,
    RateLimiterOutcome, Readiness, ReadinessError, ReadinessMessage, ReadinessReport,
    ReadinessStatus, Recipient, Registry, RegistryError, RegistryMessage, RegistryResult,
    RendezvousHash, Resolution, Resolver, ResolverConfigError, ResolverMessage,
    RestartConfiguration, RestartPolicy, RoundRobin, RouteInput, RouteKey, Router, RouterError,
    RouterMessage, ScheduleAfter, Sequence, Sequencer, SequencerMessage, SequencerOutcome,
    ShutdownRequested, StashRoute, Step, Stopped, Strategy, SupervisionEvent, Supervisor, Task,
    TaskMessage, TaskResult, TimerElapsed, TimerGeneration, TimerId, TokenCount, Topic, TopicError,
    TopicMessage, User, UserEvent, WatchSends, WorkQueue, WorkQueueMessage, WorkQueueOutcome,
    WorkQueueRejection, WorkerCreationResolved, WorkerPool, WorkerStopped, Workflow,
    WorkflowConfigError, WorkflowDefinition, WorkflowMessage, WorkflowOutcome, WorkflowRejection,
};
use bombay_engine::{ActionsOf, ActiveEnvironment, Completion, DriverError};

struct RecordingEnvironment<B: Behavior<Ph = Never>> {
    events: VecDeque<B::Event>,
    applications: Arc<AtomicUsize>,
    retirements: Arc<AtomicUsize>,
}

impl<B: Behavior<Ph = Never>> ActiveEnvironment<B> for RecordingEnvironment<B> {
    type Error = Infallible;

    async fn next(&mut self) -> Option<B::Event> {
        self.events.pop_front()
    }

    async fn apply(&mut self, _: ActionsOf<B>) -> Result<(), Self::Error> {
        self.applications.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn retire(self) {
        self.retirements.fetch_add(1, Ordering::Relaxed);
    }
}

struct CaptureEnvironment<B: Behavior<Ph = Never>> {
    events: VecDeque<B::Event>,
    actions: Arc<Mutex<Vec<ActionsOf<B>>>>,
    retirements: Arc<AtomicUsize>,
}

impl<B: Behavior<Ph = Never>> ActiveEnvironment<B> for CaptureEnvironment<B> {
    type Error = Infallible;

    async fn next(&mut self) -> Option<B::Event> {
        self.events.pop_front()
    }

    async fn apply(&mut self, actions: ActionsOf<B>) -> Result<(), Self::Error> {
        self.actions.lock().unwrap().push(actions);
        Ok(())
    }

    async fn retire(self) {
        self.retirements.fetch_add(1, Ordering::Relaxed);
    }
}

async fn drive<B>(
    behavior: B,
    events: impl IntoIterator<Item = B::Event>,
) -> (
    Result<Completion, DriverError<B::Error, Infallible>>,
    usize,
    usize,
)
where
    B: Behavior<Addr = MailAddr, Ph = Never>,
{
    let applications = Arc::new(AtomicUsize::new(0));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        behavior,
        RecordingEnvironment {
            events: events.into_iter().collect(),
            applications: Arc::clone(&applications),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;
    (
        result,
        applications.load(Ordering::Relaxed),
        retirements.load(Ordering::Relaxed),
    )
}

fn inject<B, I>(behavior: &B, input: I) -> B::Event
where
    B: Behavior,
    B::Event: EventInput<I>,
{
    let _ = behavior;
    B::Event::inject(input)
}

fn user_event<B>(behavior: &B, from: B::Addr, message: B::Msg) -> B::Event
where
    B: Behavior,
    B::Event: UserEvent<Addr = B::Addr, Message = B::Msg>,
{
    let _ = behavior;
    B::Event::user(from, message)
}

struct SendRecordingEnvironment<B: Behavior<Ph = Never, Sends = Vec<u64>>> {
    events: VecDeque<B::Event>,
    sends: Arc<Mutex<Vec<Vec<u64>>>>,
    retirements: Arc<AtomicUsize>,
}

impl<B: Behavior<Ph = Never, Sends = Vec<u64>>> ActiveEnvironment<B>
    for SendRecordingEnvironment<B>
{
    type Error = Infallible;

    async fn next(&mut self) -> Option<B::Event> {
        self.events.pop_front()
    }

    async fn apply(&mut self, actions: ActionsOf<B>) -> Result<(), Self::Error> {
        self.sends.lock().unwrap().push(actions.sends);
        Ok(())
    }

    async fn retire(self) {
        self.retirements.fetch_add(1, Ordering::Relaxed);
    }
}

struct Finalizing {
    folds: Arc<Mutex<Vec<u64>>>,
}

impl Behavior for Finalizing {
    type Addr = MailAddr;
    type Msg = u64;
    type Event = User<MailAddr, u64>;
    type Sends = Vec<u64>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::send(vec![1]))
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        self.folds.lock().unwrap().push(event.message);
        Ok(Actions::send(vec![event.message]))
    }
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "FinalizeOnShutdown's authored reaction contract preserves Behavior errors"
)]
fn final_shutdown_actions(
    behavior: &mut Finalizing,
    _: ShutdownRequested,
) -> BehaviorActed<Finalizing> {
    behavior.folds.lock().unwrap().push(99);
    Ok(Actions::send(vec![99]))
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "ReceiveTimeout's authored reaction contract preserves Behavior errors"
)]
fn timeout_stop(behavior: &mut Finalizing) -> BehaviorActed<Finalizing> {
    behavior.folds.lock().unwrap().push(20);
    Ok(Actions::new(vec![20], Vec::new(), Step::Stop(Stopped)))
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "OneShot's authored reaction contract preserves Behavior errors"
)]
fn one_shot_stop(behavior: &mut Finalizing) -> BehaviorActed<Finalizing> {
    behavior.folds.lock().unwrap().push(10);
    Ok(Actions::new(vec![10], Vec::new(), Step::Stop(Stopped)))
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "Periodic's authored reaction contract preserves Behavior errors"
)]
fn periodic_tick(behavior: &mut Finalizing) -> BehaviorActed<Finalizing> {
    let mut folds = behavior.folds.lock().unwrap();
    folds.push(30);
    let stop = folds.iter().filter(|value| **value == 30).count() == 2;
    Ok(Actions::new(
        vec![30],
        Vec::new(),
        if stop {
            Step::Stop(Stopped)
        } else {
            Step::Continue
        },
    ))
}

struct LeaseReply;

impl Behavior for LeaseReply {
    type Addr = MailAddr;
    type Msg = LeaseOutcome<u8>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct LeaseEnvironment<B: Behavior<Ph = Never, Sends = LeaseSends<LeaseReply>>> {
    events: VecDeque<B::Event>,
    outcomes: Arc<Mutex<Vec<LeaseOutcome<u8>>>>,
    schedules: Arc<Mutex<Vec<ScheduleAfter>>>,
    retirements: Arc<AtomicUsize>,
}

impl<B: Behavior<Ph = Never, Sends = LeaseSends<LeaseReply>>> ActiveEnvironment<B>
    for LeaseEnvironment<B>
{
    type Error = Infallible;

    async fn next(&mut self) -> Option<B::Event> {
        self.events.pop_front()
    }

    async fn apply(&mut self, actions: ActionsOf<B>) -> Result<(), Self::Error> {
        self.outcomes.lock().unwrap().extend(
            actions
                .sends
                .outcomes
                .into_iter()
                .map(|delivery| delivery.message),
        );
        self.schedules
            .lock()
            .unwrap()
            .extend(actions.sends.schedules);
        Ok(())
    }

    async fn retire(self) {
        self.retirements.fetch_add(1, Ordering::Relaxed);
    }
}

struct WatchEnvironment<B: Behavior<Ph = Never, Sends = WatchSends<MailAddr, Vec<u64>>>> {
    events: VecDeque<B::Event>,
    behavior_sends: Arc<Mutex<Vec<Vec<u64>>>>,
    observations: Arc<Mutex<Vec<ObservePeer<MailAddr>>>>,
    retirements: Arc<AtomicUsize>,
}

impl<B: Behavior<Ph = Never, Sends = WatchSends<MailAddr, Vec<u64>>>> ActiveEnvironment<B>
    for WatchEnvironment<B>
{
    type Error = Infallible;

    async fn next(&mut self) -> Option<B::Event> {
        self.events.pop_front()
    }

    async fn apply(&mut self, actions: ActionsOf<B>) -> Result<(), Self::Error> {
        self.behavior_sends
            .lock()
            .unwrap()
            .push(actions.sends.behavior);
        self.observations
            .lock()
            .unwrap()
            .extend(actions.sends.observations);
        Ok(())
    }

    async fn retire(self) {
        self.retirements.fetch_add(1, Ordering::Relaxed);
    }
}

struct TaskReply;

impl Behavior for TaskReply {
    type Addr = MailAddr;
    type Msg = TaskResult<Box<u64>>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct LatchParticipant;

impl Behavior for LatchParticipant {
    type Addr = MailAddr;
    type Msg = LatchReleased;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct ConfigurationReply;

impl Behavior for ConfigurationReply {
    type Addr = MailAddr;
    type Msg = ConfigurationState<String>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct FeaturesReply;

impl Behavior for FeaturesReply {
    type Addr = MailAddr;
    type Msg = ConfigurationState<FeatureSet<u8>>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct BarrierParticipant;

impl Behavior for BarrierParticipant {
    type Addr = MailAddr;
    type Msg = BarrierReleased;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct CacheReply;

impl Behavior for CacheReply {
    type Addr = MailAddr;
    type Msg = CacheResult<u8, String>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct RegistryDestination;

impl Behavior for RegistryDestination {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct RegistryReply;

impl Behavior for RegistryReply {
    type Addr = MailAddr;
    type Msg = RegistryResult<String, RegistryDestination>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct ResolverReply;

impl Behavior for ResolverReply {
    type Addr = MailAddr;
    type Msg = Resolution<String, RegistryDestination>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct PublicationSubscriber;

impl Behavior for PublicationSubscriber {
    type Addr = MailAddr;
    type Msg = String;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct PresenceReplyBehavior;

impl Behavior for PresenceReplyBehavior {
    type Addr = MailAddr;
    type Msg = PresenceReply<u8>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct AcknowledgementReply;

impl Behavior for AcknowledgementReply {
    type Addr = MailAddr;
    type Msg = AcknowledgementOutcome<String, String>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct SequencerTarget;

impl Behavior for SequencerTarget {
    type Addr = MailAddr;
    type Msg = Box<u64>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct SequencerReply;

impl Behavior for SequencerReply {
    type Addr = MailAddr;
    type Msg = SequencerOutcome<Box<u64>>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type SequencerSubject = Sequencer<MailAddr, Box<u64>, SequencerTarget, SequencerReply>;

fn sequencer_event(sequence: u64, value: u64) -> <SequencerSubject as Behavior>::Event {
    User::new(
        MailAddr(7),
        SequencerMessage::Offer {
            sequence: Sequence(sequence),
            value: Box::new(value),
            to: Recipient::global(MailAddr(1)),
            reply_to: Recipient::global(MailAddr(2)),
        },
    )
}

struct OrderGateTarget;

impl Behavior for OrderGateTarget {
    type Addr = MailAddr;
    type Msg = Box<u64>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct OrderGateReply;

impl Behavior for OrderGateReply {
    type Addr = MailAddr;
    type Msg = OrderGateOutcome<u8, Box<u64>>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type OrderGateSubject = OrderGate<MailAddr, u8, Box<u64>, OrderGateTarget, OrderGateReply>;

fn hold_event(key: u8, value: u64) -> <OrderGateSubject as Behavior>::Event {
    User::new(
        MailAddr(7),
        OrderGateMessage::Hold {
            key,
            value: Box::new(value),
            to: Recipient::global(MailAddr(1)),
            reply_to: Recipient::global(MailAddr(2)),
        },
    )
}

fn open_event(through: u8) -> <OrderGateSubject as Behavior>::Event {
    User::new(
        MailAddr(7),
        OrderGateMessage::OpenThrough {
            through,
            reply_to: Recipient::global(MailAddr(2)),
        },
    )
}

struct DeduplicatorTarget;

impl Behavior for DeduplicatorTarget {
    type Addr = MailAddr;
    type Msg = Box<u64>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct DeduplicatorReply;

impl Behavior for DeduplicatorReply {
    type Addr = MailAddr;
    type Msg = DeduplicatorOutcome<u8, Box<u64>>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type DeduplicatorSubject =
    Deduplicator<MailAddr, u8, Box<u64>, DeduplicatorTarget, DeduplicatorReply>;

fn deduplicator_event(key: u8, value: u64) -> <DeduplicatorSubject as Behavior>::Event {
    User::new(
        MailAddr(7),
        DeduplicatorMessage::Deliver {
            key,
            value: Box::new(value),
            to: Recipient::global(MailAddr(1)),
            reply_to: Recipient::global(MailAddr(2)),
        },
    )
}

struct CorrelatorReply;

impl Behavior for CorrelatorReply {
    type Addr = MailAddr;
    type Msg = CorrelationResult<u8, Box<u64>>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type CorrelatorSubject = Correlator<MailAddr, u8, Box<u64>, CorrelatorReply>;

fn begin_correlation(key: u8) -> <CorrelatorSubject as Behavior>::Event {
    User::new(
        MailAddr(7),
        CorrelatorMessage::Begin {
            key,
            reply_to: Recipient::global(MailAddr(2)),
        },
    )
}

fn resolve_correlation(key: u8, value: u64) -> <CorrelatorSubject as Behavior>::Event {
    User::new(
        MailAddr(7),
        CorrelatorMessage::Resolve {
            key,
            value: Box::new(value),
        },
    )
}

fn cancel_correlation(key: u8) -> <CorrelatorSubject as Behavior>::Event {
    User::new(MailAddr(7), CorrelatorMessage::Cancel { key })
}

type CorrelatorRun = (
    Result<Completion, DriverError<CorrelatorError<u8, Box<u64>>, Infallible>>,
    Arc<Mutex<Vec<ActionsOf<CorrelatorSubject>>>>,
    usize,
);

async fn drive_correlator(
    events: impl IntoIterator<Item = <CorrelatorSubject as Behavior>::Event>,
) -> CorrelatorRun {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        CorrelatorSubject::new(),
        CaptureEnvironment {
            events: events.into_iter().collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;
    (result, actions, retirements.load(Ordering::Relaxed))
}

struct BufferReply;

impl Behavior for BufferReply {
    type Addr = MailAddr;
    type Msg = BufferOutcome<Box<u64>>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type BufferSubject = Buffer<MailAddr, Box<u64>, DeduplicatorTarget, BufferReply>;

fn offer_to_buffer(value: u64) -> <BufferSubject as Behavior>::Event {
    User::new(
        MailAddr(7),
        BufferMessage::Offer {
            value: Box::new(value),
            reply_to: Recipient::global(MailAddr(2)),
        },
    )
}

fn release_from_buffer() -> <BufferSubject as Behavior>::Event {
    User::new(
        MailAddr(7),
        BufferMessage::Release {
            to: Recipient::global(MailAddr(1)),
            reply_to: Recipient::global(MailAddr(2)),
        },
    )
}

async fn drive_buffer(
    policy: OverflowPolicy,
) -> (
    Result<Completion, DriverError<Never, Infallible>>,
    Arc<Mutex<Vec<ActionsOf<BufferSubject>>>>,
    usize,
) {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        BufferSubject::new(2, policy).unwrap(),
        CaptureEnvironment {
            events: [
                offer_to_buffer(10),
                offer_to_buffer(11),
                offer_to_buffer(12),
                release_from_buffer(),
                release_from_buffer(),
                release_from_buffer(),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;
    (result, actions, retirements.load(Ordering::Relaxed))
}

struct WorkQueueReply;

impl Behavior for WorkQueueReply {
    type Addr = MailAddr;
    type Msg = WorkQueueOutcome<Box<u64>>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type WorkQueueSubject = WorkQueue<MailAddr, Box<u64>, DeduplicatorTarget, WorkQueueReply>;

fn work_queue_event(
    message: WorkQueueMessage<Box<u64>, DeduplicatorTarget, WorkQueueReply>,
) -> <WorkQueueSubject as Behavior>::Event {
    User::new(MailAddr(7), message)
}

fn submit_work(value: u64) -> <WorkQueueSubject as Behavior>::Event {
    work_queue_event(WorkQueueMessage::Submit {
        value: Box::new(value),
        reply_to: Recipient::global(MailAddr(9)),
    })
}

fn worker_available(address: u64) -> <WorkQueueSubject as Behavior>::Event {
    work_queue_event(WorkQueueMessage::Available {
        worker: Recipient::global(MailAddr(address)),
    })
}

fn withdraw_worker(address: u64) -> <WorkQueueSubject as Behavior>::Event {
    work_queue_event(WorkQueueMessage::Withdraw {
        worker: Recipient::global(MailAddr(address)),
    })
}

struct PriorityQueueReply;

impl Behavior for PriorityQueueReply {
    type Addr = MailAddr;
    type Msg = PriorityQueueOutcome<Box<u64>>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type PriorityQueueSubject =
    PriorityQueue<MailAddr, Box<u64>, u8, DeduplicatorTarget, PriorityQueueReply>;

fn offer_priority(value: u64, priority: u8) -> <PriorityQueueSubject as Behavior>::Event {
    User::new(
        MailAddr(7),
        PriorityQueueMessage::Offer {
            value: Box::new(value),
            priority,
            reply_to: Recipient::global(MailAddr(2)),
        },
    )
}

fn release_priority() -> <PriorityQueueSubject as Behavior>::Event {
    User::new(
        MailAddr(7),
        PriorityQueueMessage::Release {
            to: Recipient::global(MailAddr(1)),
            reply_to: Recipient::global(MailAddr(2)),
        },
    )
}

struct BreakerReply;

impl Behavior for BreakerReply {
    type Addr = MailAddr;
    type Msg = BreakerOutcome;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type BreakerSubject = CircuitBreaker<MailAddr, BreakerReply>;

fn breaker_event(message: BreakerMessage<BreakerReply>) -> <BreakerSubject as Behavior>::Event {
    User::new(MailAddr(7), message)
}

fn breaker_admit() -> <BreakerSubject as Behavior>::Event {
    breaker_event(BreakerMessage::Admit {
        reply_to: Recipient::global(MailAddr(9)),
    })
}

fn breaker_elapsed(id: u64, generation: u64) -> <BreakerSubject as Behavior>::Event {
    breaker_event(BreakerMessage::Elapsed(TimerElapsed::new(
        TimerId(id),
        TimerGeneration(generation),
    )))
}

struct RateLimiterReply;

impl Behavior for RateLimiterReply {
    type Addr = MailAddr;
    type Msg = RateLimiterOutcome<Box<u64>>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type RateLimiterSubject = RateLimiter<MailAddr, Box<u64>, DeduplicatorTarget, RateLimiterReply>;

fn token_count(value: u64) -> TokenCount {
    TokenCount::new(NonZeroU64::new(value).unwrap())
}

fn acquire_tokens(cost: u64, value: u64) -> <RateLimiterSubject as Behavior>::Event {
    User::new(
        MailAddr(7),
        RateLimiterMessage::Acquire {
            cost: token_count(cost),
            value: Box::new(value),
            to: Recipient::global(MailAddr(1)),
            reply_to: Recipient::global(MailAddr(2)),
        },
    )
}

fn refill_tokens(tokens: u64) -> <RateLimiterSubject as Behavior>::Event {
    User::new(
        MailAddr(7),
        RateLimiterMessage::Refill {
            tokens: token_count(tokens),
        },
    )
}

struct HealthReply;

impl Behavior for HealthReply {
    type Addr = MailAddr;
    type Msg = HealthReport<u8>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type HealthSubject = Health<MailAddr, u8, HealthReply>;
type HealthRun = (
    Result<Completion, DriverError<HealthError<u8>, Infallible>>,
    Arc<Mutex<Vec<ActionsOf<HealthSubject>>>>,
    usize,
);

fn health_event(message: HealthMessage<u8, HealthReply>) -> <HealthSubject as Behavior>::Event {
    User::new(MailAddr(7), message)
}

fn observe_health(
    component: u8,
    version: u64,
    status: HealthStatus,
) -> <HealthSubject as Behavior>::Event {
    health_event(HealthMessage::Observe {
        component,
        version: ObservationVersion(version),
        status,
    })
}

fn query_health() -> <HealthSubject as Behavior>::Event {
    health_event(HealthMessage::Query {
        reply_to: Recipient::global(MailAddr(9)),
    })
}

async fn drive_health(
    events: impl IntoIterator<Item = <HealthSubject as Behavior>::Event>,
) -> HealthRun {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        HealthSubject::new(),
        CaptureEnvironment {
            events: events.into_iter().collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;
    (result, actions, retirements.load(Ordering::Relaxed))
}

struct ReadinessReply;

impl Behavior for ReadinessReply {
    type Addr = MailAddr;
    type Msg = ReadinessReport<u8>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type ReadinessSubject = Readiness<MailAddr, u8, ReadinessReply>;
type ReadinessRun = (
    Result<Completion, DriverError<ReadinessError<u8>, Infallible>>,
    Arc<Mutex<Vec<ActionsOf<ReadinessSubject>>>>,
    usize,
);

fn readiness_event(
    message: ReadinessMessage<u8, ReadinessReply>,
) -> <ReadinessSubject as Behavior>::Event {
    User::new(MailAddr(7), message)
}

fn observe_readiness(
    dependency: u8,
    version: u64,
    status: ReadinessStatus,
) -> <ReadinessSubject as Behavior>::Event {
    readiness_event(ReadinessMessage::Observe {
        dependency,
        version: ObservationVersion(version),
        status,
    })
}

fn query_readiness() -> <ReadinessSubject as Behavior>::Event {
    readiness_event(ReadinessMessage::Query {
        reply_to: Recipient::global(MailAddr(9)),
    })
}

async fn drive_readiness(
    dependencies: impl IntoIterator<Item = u8>,
    events: impl IntoIterator<Item = <ReadinessSubject as Behavior>::Event>,
) -> ReadinessRun {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        ReadinessSubject::new(dependencies),
        CaptureEnvironment {
            events: events.into_iter().collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;
    (result, actions, retirements.load(Ordering::Relaxed))
}

struct WorkflowReply;

impl Behavior for WorkflowReply {
    type Addr = MailAddr;
    type Msg = WorkflowOutcome<&'static str>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type WorkflowSubject = Workflow<MailAddr, &'static str, WorkflowReply>;

fn workflow_definition() -> WorkflowDefinition<&'static str> {
    WorkflowDefinition {
        steps: vec!["root", "left", "right", "join"],
        dependencies: vec![
            ("root", "left"),
            ("root", "right"),
            ("left", "join"),
            ("right", "join"),
        ],
    }
}

fn workflow_event(
    message: WorkflowMessage<&'static str, WorkflowReply>,
) -> <WorkflowSubject as Behavior>::Event {
    User::new(MailAddr(7), message)
}

fn start_workflow() -> <WorkflowSubject as Behavior>::Event {
    workflow_event(WorkflowMessage::Start {
        reply_to: Recipient::global(MailAddr(9)),
    })
}

fn cancel_workflow() -> <WorkflowSubject as Behavior>::Event {
    workflow_event(WorkflowMessage::Cancel {
        reply_to: Recipient::global(MailAddr(9)),
    })
}

async fn drive_workflow(
    events: impl IntoIterator<Item = <WorkflowSubject as Behavior>::Event>,
) -> (
    Result<Completion, DriverError<Never, Infallible>>,
    Arc<Mutex<Vec<ActionsOf<WorkflowSubject>>>>,
    usize,
) {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        WorkflowSubject::new(workflow_definition()).unwrap(),
        CaptureEnvironment {
            events: events.into_iter().collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;
    (result, actions, retirements.load(Ordering::Relaxed))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RoutedValue(u64);

struct RouterDestination;

impl Behavior for RouterDestination {
    type Addr = MailAddr;
    type Msg = RoutedValue;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

fn router_recipient(address: u64) -> Recipient<RouterDestination> {
    Recipient::global(MailAddr(address))
}

type RoundRobinSubject = Router<MailAddr, RouterDestination, RoundRobin>;
type BroadcastSubject = Router<MailAddr, RouterDestination, Broadcast>;

fn round_robin_event(
    message: RouterMessage<RouterDestination, RoundRobin>,
) -> <RoundRobinSubject as Behavior>::Event {
    User::new(MailAddr(7), message)
}

fn broadcast_event(
    message: RouterMessage<RouterDestination, Broadcast>,
) -> <BroadcastSubject as Behavior>::Event {
    User::new(MailAddr(7), message)
}

type LeastLoadedSubject = Router<MailAddr, RouterDestination, LeastLoaded<RouterDestination>>;

fn least_loaded_event(
    message: RouterMessage<RouterDestination, LeastLoaded<RouterDestination>>,
) -> <LeastLoadedSubject as Behavior>::Event {
    User::new(MailAddr(7), message)
}

fn observe_load(
    recipient: u64,
    version: u64,
    load: u64,
) -> <LeastLoadedSubject as Behavior>::Event {
    least_loaded_event(RouterMessage::Observe(LoadObservation {
        recipient: router_recipient(recipient),
        version: LoadVersion(version),
        load: Load(load),
    }))
}

async fn drive_least_loaded(
    events: impl IntoIterator<Item = <LeastLoadedSubject as Behavior>::Event>,
) -> (
    Result<
        Completion,
        DriverError<RouterError<RoutedValue, LeastLoadedError<RouterDestination>>, Infallible>,
    >,
    Arc<Mutex<Vec<ActionsOf<LeastLoadedSubject>>>>,
    usize,
) {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        LeastLoadedSubject::new(
            vec![router_recipient(1), router_recipient(2)],
            LeastLoaded::new(),
        ),
        CaptureEnvironment {
            events: events.into_iter().collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;
    (result, actions, retirements.load(Ordering::Relaxed))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct KeyedRoutedValue {
    key: u64,
    value: u64,
}

impl RouteKey<u64> for KeyedRoutedValue {
    fn route_key(&self) -> &u64 {
        &self.key
    }
}

struct KeyedRouterDestination;

impl Behavior for KeyedRouterDestination {
    type Addr = MailAddr;
    type Msg = KeyedRoutedValue;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

fn keyed_recipient(address: u64) -> Recipient<KeyedRouterDestination> {
    Recipient::global(MailAddr(address))
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "hash strategy's authored key function contract borrows K"
)]
fn identity_hash(key: &u64) -> u64 {
    *key
}

type ConsistentHashSubject =
    Router<MailAddr, KeyedRouterDestination, ConsistentHash<KeyedRouterDestination, u64>>;
type RendezvousHashSubject =
    Router<MailAddr, KeyedRouterDestination, RendezvousHash<KeyedRouterDestination, u64>>;

fn consistent_event(
    message: RouterMessage<KeyedRouterDestination, ConsistentHash<KeyedRouterDestination, u64>>,
) -> <ConsistentHashSubject as Behavior>::Event {
    User::new(MailAddr(7), message)
}

fn rendezvous_event(
    message: RouterMessage<KeyedRouterDestination, RendezvousHash<KeyedRouterDestination, u64>>,
) -> <RendezvousHashSubject as Behavior>::Event {
    User::new(MailAddr(7), message)
}

fn member_token(
    recipient: u64,
    version: u64,
    token: u64,
) -> MemberTokenObservation<KeyedRouterDestination> {
    MemberTokenObservation {
        recipient: keyed_recipient(recipient),
        version: MemberTokenVersion(version),
        token: MemberToken(token),
    }
}

struct ProxyChild {
    marker: Box<u64>,
}

impl ProxyChild {
    fn new(marker: u64) -> Self {
        Self {
            marker: Box::new(marker),
        }
    }
}

impl Behavior for ProxyChild {
    type Addr = MailAddr;
    type Msg = Box<u64>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type ProxySubject = Proxy<ProxyChild>;

fn proxy_command(command: ProxyCommand<ProxyChild>) -> <ProxySubject as Behavior>::Event {
    ProxyEvent::Command(User::new(MailAddr(7), command))
}

fn proxy_stopped(nonce: u64) -> <ProxySubject as Behavior>::Event {
    ProxyEvent::ChildStopped(ChildStopped {
        nonce,
        outcome: Err(Crash::Failed),
        at: Instant::now(),
    })
}

#[allow(
    dead_code,
    reason = "the conformance event sum proves routability; its inner behavior intentionally ignores service facts"
)]
enum SupervisorInnerEvent {
    User(User<MailAddr, u64>),
    ChildStopped(ChildStopped<MailAddr>),
    CreationResolved(CreationResolved<u64>),
    WorkerCreationResolved(WorkerCreationResolved<u64>),
}

impl UserEvent for SupervisorInnerEvent {
    type Addr = MailAddr;
    type Message = u64;

    fn user(from: MailAddr, message: u64) -> Self {
        Self::User(User::new(from, message))
    }

    fn into_user(self) -> Result<User<MailAddr, u64>, Self> {
        match self {
            Self::User(event) => Ok(event),
            event @ (Self::ChildStopped(_)
            | Self::CreationResolved(_)
            | Self::WorkerCreationResolved(_)) => Err(event),
        }
    }
}

impl RouteInput<ChildStopped<MailAddr>> for SupervisorInnerEvent {
    fn route(event: ChildStopped<MailAddr>) -> Result<Self, ChildStopped<MailAddr>> {
        Ok(Self::ChildStopped(event))
    }
}

impl RouteInput<CreationResolved<u64>> for SupervisorInnerEvent {
    fn route(event: CreationResolved<u64>) -> Result<Self, CreationResolved<u64>> {
        Ok(Self::CreationResolved(event))
    }
}

impl RouteInput<WorkerCreationResolved<u64>> for SupervisorInnerEvent {
    fn route(event: WorkerCreationResolved<u64>) -> Result<Self, WorkerCreationResolved<u64>> {
        Ok(Self::WorkerCreationResolved(event))
    }
}

struct SupervisorInner;

impl Behavior for SupervisorInner {
    type Addr = MailAddr;
    type Msg = u64;
    type Event = SupervisorInnerEvent;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<ProxyChild>;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "ChildTopology's authored factory contract permits rejecting an index"
)]
fn supervisor_child(index: usize) -> Option<ProxyChild> {
    Some(ProxyChild::new(u64::try_from(index).unwrap()))
}

fn supervisor_nonce(index: usize) -> u64 {
    u64::try_from(index).unwrap()
}

type SupervisorSubject = Supervisor<SupervisorInner, ProxyChild>;

struct PoolReply;

impl Behavior for PoolReply {
    type Addr = MailAddr;
    type Msg = PoolResponse<String, u64, MailAddr>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

#[allow(
    dead_code,
    reason = "the non-Clone marker proves child ownership inside the opaque Proxy birth"
)]
struct PoolWorker {
    marker: Box<u64>,
}

impl Behavior for PoolWorker {
    type Addr = MailAddr;
    type Msg = PoolAssignment<String>;
    type Event = User<MailAddr, Self::Msg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "ChildTopology's authored factory contract permits rejecting an index"
)]
fn pool_worker(index: usize) -> Option<PoolWorker> {
    Some(PoolWorker {
        marker: Box::new(u64::try_from(index).unwrap()),
    })
}

fn pool_configuration() -> PoolConfiguration {
    PoolConfiguration::new(
        2,
        InterruptionPolicy::Fail,
        RestartPolicy::Permanent,
        2,
        Duration::MAX,
    )
}

type WorkerPoolSubject = WorkerPool<MailAddr, PoolReply, String, u64, PoolWorker>;
type KeyedWorkerPoolSubject =
    KeyedWorkerPool<MailAddr, PoolReply, u8, String, u64, PoolWorker, fn(&u8) -> u64>;

#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "AffinitySelector's authored contract borrows the application key"
)]
fn select_pool_worker(key: &u8) -> u64 {
    u64::from(*key % 2)
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "Presence's pure timer-mapping contract requires a borrowed participant"
)]
fn presence_timer(participant: &u8) -> TimerId {
    TimerId(u64::from(*participant % 2))
}

type TopicSubject = Topic<MailAddr, String, PublicationSubscriber>;
type TopicRun = (
    Result<Completion, DriverError<TopicError<String>, Infallible>>,
    Arc<Mutex<Vec<ActionsOf<TopicSubject>>>>,
    usize,
);

async fn drive_topic(
    events: impl IntoIterator<Item = <TopicSubject as Behavior>::Event>,
) -> TopicRun {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        TopicSubject::new(),
        CaptureEnvironment {
            events: events.into_iter().collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;
    (result, actions, retirements.load(Ordering::Relaxed))
}

fn topic_event(
    message: TopicMessage<String, PublicationSubscriber>,
) -> User<MailAddr, TopicMessage<String, PublicationSubscriber>> {
    User::new(MailAddr(7), message)
}

type PubSubSubject = PubSub<MailAddr, String, String, PublicationSubscriber>;
type PubSubRun = (
    Result<Completion, DriverError<PubSubError<String, String>, Infallible>>,
    Arc<Mutex<Vec<ActionsOf<PubSubSubject>>>>,
    usize,
);

async fn drive_pub_sub(
    events: impl IntoIterator<Item = <PubSubSubject as Behavior>::Event>,
) -> PubSubRun {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        PubSubSubject::new(),
        CaptureEnvironment {
            events: events.into_iter().collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;
    (result, actions, retirements.load(Ordering::Relaxed))
}

fn pub_sub_event(
    message: PubSubMessage<String, String, PublicationSubscriber>,
) -> <PubSubSubject as Behavior>::Event {
    User::new(MailAddr(7), message)
}

type RegistrySubject = Registry<MailAddr, String, RegistryDestination, RegistryReply>;
type RegistryRun = (
    Result<Completion, DriverError<RegistryError<String>, Infallible>>,
    Arc<Mutex<Vec<ActionsOf<RegistrySubject>>>>,
    usize,
);

async fn drive_registry(
    events: impl IntoIterator<Item = <RegistrySubject as Behavior>::Event>,
) -> RegistryRun {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        RegistrySubject::new(),
        CaptureEnvironment {
            events: events.into_iter().collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;
    (result, actions, retirements.load(Ordering::Relaxed))
}

fn registry_event(
    message: RegistryMessage<String, RegistryDestination, RegistryReply>,
) -> <RegistrySubject as Behavior>::Event {
    User::new(MailAddr(7), message)
}

type BarrierSubject = Barrier<MailAddr, u8, BarrierParticipant>;
type BarrierRun = (
    Result<Completion, DriverError<BarrierError<u8>, Infallible>>,
    Arc<Mutex<Vec<ActionsOf<BarrierSubject>>>>,
    usize,
);

async fn drive_barrier(
    events: impl IntoIterator<Item = <BarrierSubject as Behavior>::Event>,
) -> BarrierRun {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        BarrierSubject::new(vec![1, 2]).unwrap(),
        CaptureEnvironment {
            events: events.into_iter().collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;
    (result, actions, retirements.load(Ordering::Relaxed))
}

fn barrier_event(
    generation: u64,
    participant: u8,
    reply_to: Recipient<BarrierParticipant>,
) -> <BarrierSubject as Behavior>::Event {
    User::new(
        MailAddr(7),
        BarrierMessage {
            generation: BarrierGeneration(generation),
            participant,
            reply_to,
        },
    )
}

struct TaskEnvironment<B: Behavior<Ph = Never, Sends = Vec<Delivery<TaskReply>>>> {
    events: VecDeque<B::Event>,
    results: Arc<Mutex<Vec<TaskResult<Box<u64>>>>>,
    applications: Arc<AtomicUsize>,
    retirements: Arc<AtomicUsize>,
}

impl<B: Behavior<Ph = Never, Sends = Vec<Delivery<TaskReply>>>> ActiveEnvironment<B>
    for TaskEnvironment<B>
{
    type Error = Infallible;

    async fn next(&mut self) -> Option<B::Event> {
        self.events.pop_front()
    }

    async fn apply(&mut self, actions: ActionsOf<B>) -> Result<(), Self::Error> {
        self.applications.fetch_add(1, Ordering::Relaxed);
        self.results
            .lock()
            .unwrap()
            .extend(actions.sends.into_iter().map(|delivery| delivery.message));
        Ok(())
    }

    async fn retire(self) {
        self.retirements.fetch_add(1, Ordering::Relaxed);
    }
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    clippy::unnecessary_wraps,
    reason = "Machine's authored transition contract requires a borrowed message and Result"
)]
fn record_and_stop_on_three(
    _: (),
    seen: &mut Arc<Mutex<Vec<u8>>>,
    message: &u8,
) -> Result<Move<()>, Infallible> {
    seen.lock().unwrap().push(*message);
    Ok(if *message == 3 {
        Move::Stop
    } else {
        Move::Stay
    })
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "Stash's semantic routing contract requires a borrowed message"
)]
fn stash_one_and_release_on_two(message: &u8) -> StashRoute {
    match message {
        1 => StashRoute::Stash,
        2 => StashRoute::Release,
        _ => StashRoute::Deliver,
    }
}

#[tokio::test]
async fn inferred_machine_runs_directly_through_the_universal_driver() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let behavior =
        Machine::<MailAddr, _, _, _, _>::new(Arc::clone(&seen), (), record_and_stop_on_three);

    let (result, applications, retirements) = drive(
        behavior,
        [User::new(MailAddr(7), 1), User::new(MailAddr(7), 3)],
    )
    .await;

    assert!(matches!(result, Ok(Completion::Stopped)));
    assert_eq!(*seen.lock().unwrap(), [1, 3]);
    assert_eq!(applications, 3, "initialization plus two event actions");
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn inferred_stash_of_machine_uses_the_same_driver_without_a_named_stack() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let behavior =
        Machine::<MailAddr, _, _, _, _>::new(Arc::clone(&seen), (), record_and_stop_on_three)
            .stash(stash_one_and_release_on_two);

    let (result, applications, retirements) = drive(
        behavior,
        [
            User::new(MailAddr(7), 1),
            User::new(MailAddr(7), 2),
            User::new(MailAddr(7), 3),
        ],
    )
    .await;

    assert!(matches!(result, Ok(Completion::Stopped)));
    assert_eq!(
        *seen.lock().unwrap(),
        [2, 3],
        "the wrapper retains message 1 because its semantic route remains Stash"
    );
    assert_eq!(applications, 4, "initialization plus three outer events");
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn inferred_machine_stash_deadline_stack_changes_protocol_without_changing_driver() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let behavior =
        Machine::<MailAddr, _, _, _, _>::new(Arc::clone(&seen), (), record_and_stop_on_three)
            .stash(stash_one_and_release_on_two)
            .deadline(TimerId(11), Some(Instant::now()), |_| {
                Ok(Step::Stop(Stopped))
            });

    let (result, applications, retirements) = drive(
        behavior,
        [
            DeadlineEvent::Behavior(User::new(MailAddr(7), 1)),
            DeadlineEvent::Behavior(User::new(MailAddr(7), 2)),
            DeadlineEvent::Elapsed(TimerElapsed::new(TimerId(11), TimerGeneration(9))),
            DeadlineEvent::Elapsed(TimerElapsed::new(TimerId(11), TimerGeneration(0))),
            DeadlineEvent::Behavior(User::new(MailAddr(7), 3)),
        ],
    )
    .await;

    assert!(matches!(result, Ok(Completion::Stopped)));
    assert_eq!(
        *seen.lock().unwrap(),
        [2],
        "user events delegate through both wrappers; the deadline stops outside Machine"
    );
    assert_eq!(
        applications, 5,
        "initialization, two user decisions, stale elapsed, and matching deadline stop"
    );
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn deadline_then_shutdown_stops_through_inferred_typed_input() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let behavior =
        Machine::<MailAddr, _, _, _, _>::new(Arc::clone(&seen), (), record_and_stop_on_three)
            .deadline(TimerId(12), Some(Instant::now()), |_| Ok(Step::Continue))
            .stop_on_shutdown();
    let shutdown = inject(&behavior, ShutdownRequested);
    let forbidden_later_user = user_event(&behavior, MailAddr(7), 3);

    let (result, applications, retirements) =
        drive(behavior, [shutdown, forbidden_later_user]).await;

    assert!(matches!(result, Ok(Completion::Stopped)));
    assert!(seen.lock().unwrap().is_empty());
    assert_eq!(applications, 2, "initialization then shutdown stop");
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn shutdown_then_deadline_stops_through_inferred_typed_input() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let behavior =
        Machine::<MailAddr, _, _, _, _>::new(Arc::clone(&seen), (), record_and_stop_on_three)
            .stop_on_shutdown()
            .deadline(TimerId(13), Some(Instant::now()), |_| Ok(Step::Continue));
    let shutdown = inject(&behavior, ShutdownRequested);
    let forbidden_later_user = user_event(&behavior, MailAddr(7), 3);

    let (result, applications, retirements) =
        drive(behavior, [shutdown, forbidden_later_user]).await;

    assert!(matches!(result, Ok(Completion::Stopped)));
    assert!(seen.lock().unwrap().is_empty());
    assert_eq!(applications, 2, "initialization then shutdown stop");
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn inferred_finalize_on_shutdown_preserves_final_typed_actions_before_stop() {
    let folds = Arc::new(Mutex::new(Vec::new()));
    let behavior = Finalizing {
        folds: Arc::clone(&folds),
    }
    .finalize_on_shutdown(final_shutdown_actions);
    let shutdown = inject(&behavior, ShutdownRequested);
    let forbidden_later_user = user_event(&behavior, MailAddr(7), 7);
    let sends = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));

    let result = direct(
        behavior,
        SendRecordingEnvironment {
            events: [shutdown, forbidden_later_user].into_iter().collect(),
            sends: Arc::clone(&sends),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Stopped)));
    assert_eq!(*folds.lock().unwrap(), [99]);
    assert_eq!(
        *sends.lock().unwrap(),
        [vec![1], vec![99]],
        "initialization and final wrapper actions each cross exactly once"
    );
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_receive_timeout_accepts_only_the_live_generation_then_stops() {
    let folds = Arc::new(Mutex::new(Vec::new()));
    let behavior = Finalizing {
        folds: Arc::clone(&folds),
    }
    .receive_timeout(TimerId(20), Duration::from_secs(1), timeout_stop);
    let stale = inject(
        &behavior,
        TimerElapsed::new(TimerId(20), TimerGeneration(9)),
    );
    let elapsed = inject(
        &behavior,
        TimerElapsed::new(TimerId(20), TimerGeneration(0)),
    );
    let forbidden_later_user = user_event(&behavior, MailAddr(7), 7);

    let (result, applications, retirements) =
        drive(behavior, [stale, elapsed, forbidden_later_user]).await;

    assert!(matches!(result, Ok(Completion::Stopped)));
    assert_eq!(*folds.lock().unwrap(), [20]);
    assert_eq!(applications, 3, "initialization, stale event, live timeout");
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn inferred_one_shot_accepts_once_then_stops() {
    let folds = Arc::new(Mutex::new(Vec::new()));
    let behavior = Finalizing {
        folds: Arc::clone(&folds),
    }
    .one_shot(TimerId(10), Duration::from_secs(1), one_shot_stop);
    let stale = inject(
        &behavior,
        TimerElapsed::new(TimerId(10), TimerGeneration(9)),
    );
    let elapsed = inject(
        &behavior,
        TimerElapsed::new(TimerId(10), TimerGeneration(0)),
    );
    let forbidden_duplicate = inject(
        &behavior,
        TimerElapsed::new(TimerId(10), TimerGeneration(0)),
    );

    let (result, applications, retirements) =
        drive(behavior, [stale, elapsed, forbidden_duplicate]).await;

    assert!(matches!(result, Ok(Completion::Stopped)));
    assert_eq!(*folds.lock().unwrap(), [10]);
    assert_eq!(
        applications, 3,
        "initialization, stale event, live one-shot"
    );
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn inferred_periodic_accepts_successive_generations_until_reaction_stops() {
    let folds = Arc::new(Mutex::new(Vec::new()));
    let behavior = Finalizing {
        folds: Arc::clone(&folds),
    }
    .periodic(TimerId(30), Duration::from_secs(1), periodic_tick);
    let first = inject(
        &behavior,
        TimerElapsed::new(TimerId(30), TimerGeneration(0)),
    );
    let stale_duplicate = inject(
        &behavior,
        TimerElapsed::new(TimerId(30), TimerGeneration(0)),
    );
    let second = inject(
        &behavior,
        TimerElapsed::new(TimerId(30), TimerGeneration(1)),
    );
    let forbidden_later_user = user_event(&behavior, MailAddr(7), 7);

    let (result, applications, retirements) = drive(
        behavior,
        [first, stale_duplicate, second, forbidden_later_user],
    )
    .await;

    assert!(matches!(result, Ok(Completion::Stopped)));
    assert_eq!(*folds.lock().unwrap(), [30, 30]);
    assert_eq!(
        applications, 4,
        "initialization, first generation, stale duplicate, second generation"
    );
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn inferred_watch_interprets_one_typed_request_and_reacts_only_to_its_peer() {
    let folds = Arc::new(Mutex::new(Vec::new()));
    let watched_peer = MailAddr(8);
    let behavior = Finalizing {
        folds: Arc::clone(&folds),
    }
    .watch(watched_peer, behavior::stop_on_abnormal_death);
    let normal = inject(&behavior, PeerStopped::new(watched_peer, Ok(Exit::Normal)));
    let unrelated = inject(&behavior, PeerStopped::new(MailAddr(9), Err(Crash::Failed)));
    let abnormal = inject(
        &behavior,
        PeerStopped::new(watched_peer, Err(Crash::Failed)),
    );
    let forbidden_later_user = user_event(&behavior, MailAddr(7), 77);
    let behavior_sends = Arc::new(Mutex::new(Vec::new()));
    let observations = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));

    let result = direct(
        behavior,
        WatchEnvironment {
            events: [normal, unrelated, abnormal, forbidden_later_user]
                .into_iter()
                .collect(),
            behavior_sends: Arc::clone(&behavior_sends),
            observations: Arc::clone(&observations),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Stopped)));
    assert!(folds.lock().unwrap().is_empty());
    assert_eq!(
        *observations.lock().unwrap(),
        [ObservePeer::new(watched_peer)],
        "initialization registers the exact peer once"
    );
    assert_eq!(
        *behavior_sends.lock().unwrap(),
        [vec![1], vec![], vec![], vec![]],
        "every admitted decision crosses once; no event follows the terminal action"
    );
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

fn composable_base() -> impl Behavior<
    Addr = MailAddr,
    Msg = u64,
    Event = User<MailAddr, u64>,
    Sends = Vec<u64>,
    Ph = Never,
    Error = Infallible,
    Birth = NoBirths,
> {
    Finalizing {
        folds: Arc::new(Mutex::new(Vec::new())),
    }
}

async fn drive_empty_composition<B>(behavior: B)
where
    B: Behavior<Addr = MailAddr, Ph = Never>,
{
    let (result, applications, retirements) = drive(behavior, std::iter::empty()).await;
    assert!(matches!(result, Ok(Completion::Exhausted)));
    assert_eq!(applications, 1);
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn every_wrapper_edge_in_both_orders_and_the_maximal_stack_use_one_driver() {
    macro_rules! every_outer {
        ($inner:expr) => {{
            drive_empty_composition(($inner).stop_on_shutdown()).await;
            drive_empty_composition(($inner).finalize_on_shutdown(|_, _| Ok(Actions::cont())))
                .await;
            drive_empty_composition(($inner).watch(MailAddr(91), |_, _, _| Ok(Step::Continue)))
                .await;
            drive_empty_composition(($inner).deadline(TimerId(92), None, |_| Ok(Step::Continue)))
                .await;
            drive_empty_composition(($inner).receive_timeout(
                TimerId(93),
                Duration::from_nanos(1),
                |_| Ok(Actions::cont()),
            ))
            .await;
            drive_empty_composition(($inner).one_shot(
                TimerId(94),
                Duration::from_nanos(1),
                |_| Ok(Actions::cont()),
            ))
            .await;
            drive_empty_composition(($inner).periodic(
                TimerId(95),
                Duration::from_nanos(1),
                |_| Ok(Actions::cont()),
            ))
            .await;
            drive_empty_composition(($inner).stash(|_| StashRoute::Deliver)).await;
        }};
    }

    every_outer!(composable_base().stop_on_shutdown());
    every_outer!(composable_base().finalize_on_shutdown(|_, _| Ok(Actions::cont())));
    every_outer!(composable_base().watch(MailAddr(81), |_, _, _| Ok(Step::Continue)));
    every_outer!(composable_base().deadline(TimerId(82), None, |_| Ok(Step::Continue)));
    every_outer!(
        composable_base().receive_timeout(TimerId(83), Duration::from_nanos(1), |_| Ok(
            Actions::cont()
        ),)
    );
    every_outer!(
        composable_base().one_shot(
            TimerId(84),
            Duration::from_nanos(1),
            |_| Ok(Actions::cont()),
        )
    );
    every_outer!(
        composable_base().periodic(
            TimerId(85),
            Duration::from_nanos(1),
            |_| Ok(Actions::cont()),
        )
    );
    every_outer!(composable_base().stash(|_| StashRoute::Deliver));

    let maximal = composable_base()
        .stash(|_| StashRoute::Deliver)
        .watch(MailAddr(71), |_, _, _| Ok(Step::Continue))
        .receive_timeout(
            TimerId(72),
            Duration::from_nanos(1),
            |_| Ok(Actions::cont()),
        )
        .one_shot(
            TimerId(73),
            Duration::from_nanos(1),
            |_| Ok(Actions::cont()),
        )
        .periodic(
            TimerId(74),
            Duration::from_nanos(1),
            |_| Ok(Actions::cont()),
        )
        .deadline(TimerId(75), None, |_| Ok(Step::Continue))
        .finalize_on_shutdown(|_, _| Ok(Actions::cont()))
        .stop_on_shutdown();
    drive_empty_composition(maximal).await;
}

#[tokio::test]
async fn inferred_task_preserves_owned_completion_and_distinct_cancellation_until_stop() {
    let reply_to = Recipient::<TaskReply>::global(MailAddr(8));

    let completed_task = Task::<MailAddr, Box<u64>, TaskReply>::new();
    let completed_results = Arc::new(Mutex::new(Vec::new()));
    let completed_applications = Arc::new(AtomicUsize::new(0));
    let completed_retirements = Arc::new(AtomicUsize::new(0));
    let completed = direct(
        completed_task,
        TaskEnvironment {
            events: [
                User::new(
                    MailAddr(7),
                    TaskMessage::Complete {
                        result: Box::new(41),
                        reply_to,
                    },
                ),
                User::new(MailAddr(7), TaskMessage::Cancel { reply_to }),
            ]
            .into_iter()
            .collect(),
            results: Arc::clone(&completed_results),
            applications: Arc::clone(&completed_applications),
            retirements: Arc::clone(&completed_retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(completed, Ok(Completion::Stopped)));
    assert!(matches!(
        completed_results.lock().unwrap().as_slice(),
        [TaskResult::Completed(result)] if **result == 41
    ));
    assert_eq!(completed_applications.load(Ordering::Relaxed), 2);
    assert_eq!(completed_retirements.load(Ordering::Relaxed), 1);

    let cancelled_task = Task::<MailAddr, Box<u64>, TaskReply>::new();
    let cancelled_results = Arc::new(Mutex::new(Vec::new()));
    let cancelled_applications = Arc::new(AtomicUsize::new(0));
    let cancelled_retirements = Arc::new(AtomicUsize::new(0));
    let cancelled = direct(
        cancelled_task,
        TaskEnvironment {
            events: [
                User::new(MailAddr(7), TaskMessage::Cancel { reply_to }),
                User::new(
                    MailAddr(7),
                    TaskMessage::Complete {
                        result: Box::new(99),
                        reply_to,
                    },
                ),
            ]
            .into_iter()
            .collect(),
            results: Arc::clone(&cancelled_results),
            applications: Arc::clone(&cancelled_applications),
            retirements: Arc::clone(&cancelled_retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(cancelled, Ok(Completion::Stopped)));
    assert!(matches!(
        cancelled_results.lock().unwrap().as_slice(),
        [TaskResult::Cancelled]
    ));
    assert_eq!(cancelled_applications.load(Ordering::Relaxed), 2);
    assert_eq!(cancelled_retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_latch_preserves_threshold_order_late_arrival_and_zero_boundary() {
    let one = Recipient::<LatchParticipant>::global(MailAddr(1));
    let two = Recipient::<LatchParticipant>::global(MailAddr(2));
    let late = Recipient::<LatchParticipant>::global(MailAddr(3));
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));

    let result = direct(
        Latch::<MailAddr, LatchParticipant>::new(2),
        CaptureEnvironment {
            events: [
                User::new(MailAddr(7), LatchMessage::arrive(one)),
                User::new(MailAddr(7), LatchMessage::arrive(two)),
                User::new(MailAddr(7), LatchMessage::arrive(late)),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    {
        let actions = actions.lock().unwrap();
        assert_eq!(actions.len(), 4, "initialization plus three arrivals");
        assert!(actions[0].sends.is_empty());
        assert!(actions[1].sends.is_empty());
        assert_eq!(
            actions[2]
                .sends
                .iter()
                .map(|delivery| delivery.to.resolve(MailAddr(0)))
                .collect::<Vec<_>>(),
            [MailAddr(1), MailAddr(2)],
            "the release action preserves accepted arrival order"
        );
        assert_eq!(
            actions[3].sends[0].to.resolve(MailAddr(0)),
            MailAddr(3),
            "an arrival after release receives its own ordinary action"
        );
    }
    assert_eq!(retirements.load(Ordering::Relaxed), 1);

    let zero_actions = Arc::new(Mutex::new(Vec::new()));
    let zero_retirements = Arc::new(AtomicUsize::new(0));
    let zero = direct(
        Latch::<MailAddr, LatchParticipant>::new(0),
        CaptureEnvironment {
            events: [User::new(MailAddr(7), LatchMessage::arrive(one))]
                .into_iter()
                .collect(),
            actions: Arc::clone(&zero_actions),
            retirements: Arc::clone(&zero_retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(zero, Ok(Completion::Exhausted)));
    assert_eq!(
        zero_actions.lock().unwrap()[1].sends[0]
            .to
            .resolve(MailAddr(0)),
        MailAddr(1)
    );
    assert_eq!(zero_retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the successful, stale, and conflicting configuration transcripts stay adjacent"
)]
async fn inferred_configuration_preserves_queries_versions_owned_rejections_and_fusion() {
    type Subject = Configuration<MailAddr, String, ConfigurationReply>;

    let reply_to = Recipient::<ConfigurationReply>::global(MailAddr(8));
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let successful = direct(
        Subject::new(),
        CaptureEnvironment {
            events: [
                User::new(MailAddr(7), ConfigurationMessage::Query { reply_to }),
                User::new(
                    MailAddr(7),
                    ConfigurationMessage::Apply {
                        version: ConfigurationVersion(2),
                        value: "blue".to_owned(),
                    },
                ),
                User::new(
                    MailAddr(7),
                    ConfigurationMessage::Apply {
                        version: ConfigurationVersion(2),
                        value: "blue".to_owned(),
                    },
                ),
                User::new(MailAddr(7), ConfigurationMessage::Query { reply_to }),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(successful, Ok(Completion::Exhausted)));
    {
        let actions = actions.lock().unwrap();
        assert!(matches!(
            actions[1].sends[0].message,
            ConfigurationState::Unconfigured
        ));
        assert!(actions[2].sends.is_empty());
        assert!(actions[3].sends.is_empty());
        assert!(matches!(
            &actions[4].sends[0].message,
            ConfigurationState::Configured { version: ConfigurationVersion(2), value }
                if value == "blue"
        ));
    }
    assert_eq!(retirements.load(Ordering::Relaxed), 1);

    let stale_actions = Arc::new(Mutex::new(Vec::new()));
    let stale_retirements = Arc::new(AtomicUsize::new(0));
    let stale = direct(
        Subject::new(),
        CaptureEnvironment {
            events: [
                User::new(
                    MailAddr(7),
                    ConfigurationMessage::Apply {
                        version: ConfigurationVersion(2),
                        value: "kept".to_owned(),
                    },
                ),
                User::new(
                    MailAddr(7),
                    ConfigurationMessage::Apply {
                        version: ConfigurationVersion(1),
                        value: "returned".to_owned(),
                    },
                ),
                User::new(MailAddr(7), ConfigurationMessage::Query { reply_to }),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&stale_actions),
            retirements: Arc::clone(&stale_retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(
        stale,
        Err(DriverError::Behavior(ConfigurationError::Stale {
            proposed: ConfigurationVersion(1),
            current: ConfigurationVersion(2),
            value,
        })) if value == "returned"
    ));
    assert_eq!(stale_actions.lock().unwrap().len(), 2);
    assert_eq!(stale_retirements.load(Ordering::Relaxed), 1);

    let conflict_actions = Arc::new(Mutex::new(Vec::new()));
    let conflict_retirements = Arc::new(AtomicUsize::new(0));
    let conflict = direct(
        Subject::new(),
        CaptureEnvironment {
            events: [
                User::new(
                    MailAddr(7),
                    ConfigurationMessage::Apply {
                        version: ConfigurationVersion(2),
                        value: "kept".to_owned(),
                    },
                ),
                User::new(
                    MailAddr(7),
                    ConfigurationMessage::Apply {
                        version: ConfigurationVersion(2),
                        value: "returned".to_owned(),
                    },
                ),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&conflict_actions),
            retirements: Arc::clone(&conflict_retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(
        conflict,
        Err(DriverError::Behavior(
            ConfigurationError::ConflictingVersion {
                version: ConfigurationVersion(2),
                value,
            }
        )) if value == "returned"
    ));
    assert_eq!(conflict_actions.lock().unwrap().len(), 2);
    assert_eq!(conflict_retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_features_specialization_preserves_normalized_typed_state() {
    let reply_to = Recipient::<FeaturesReply>::global(MailAddr(8));
    let features = FeatureSet::new([
        Feature {
            feature: 1,
            status: FeatureStatus::Disabled,
        },
        Feature {
            feature: 2,
            status: FeatureStatus::Enabled,
        },
        Feature {
            feature: 1,
            status: FeatureStatus::Enabled,
        },
    ]);
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));

    let result = direct(
        Features::<MailAddr, u8, FeaturesReply>::new(),
        CaptureEnvironment {
            events: [
                User::new(
                    MailAddr(7),
                    ConfigurationMessage::Apply {
                        version: ConfigurationVersion(1),
                        value: features,
                    },
                ),
                User::new(MailAddr(7), ConfigurationMessage::Query { reply_to }),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    let ConfigurationState::Configured { version, value } = &actions[2].sends[0].message else {
        panic!("query must return the configured feature state");
    };
    assert_eq!(*version, ConfigurationVersion(1));
    assert_eq!(
        value.features(),
        [
            Feature {
                feature: 1,
                status: FeatureStatus::Enabled,
            },
            Feature {
                feature: 2,
                status: FeatureStatus::Enabled,
            },
        ]
    );
    drop(actions);
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_barrier_preserves_constructor_and_ordered_generation_boundaries() {
    assert!(matches!(
        BarrierSubject::new(Vec::new()),
        Err(BarrierConfigError::EmptyMembership)
    ));
    assert!(matches!(
        BarrierSubject::new(vec![1, 2, 1]),
        Err(BarrierConfigError::DuplicateParticipant(1))
    ));

    let one = Recipient::<BarrierParticipant>::global(MailAddr(1));
    let two = Recipient::<BarrierParticipant>::global(MailAddr(2));
    let (result, actions, retirements) = drive_barrier([
        barrier_event(0, 2, two),
        barrier_event(0, 1, one),
        barrier_event(1, 1, one),
        barrier_event(1, 2, two),
    ])
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 5, "initialization plus four arrivals");
    assert!(actions[1].sends.is_empty());
    assert_eq!(
        actions[2]
            .sends
            .iter()
            .map(|delivery| (
                delivery.to.resolve(MailAddr(0)),
                delivery.message.generation
            ))
            .collect::<Vec<_>>(),
        [
            (MailAddr(2), BarrierGeneration(0)),
            (MailAddr(1), BarrierGeneration(0)),
        ]
    );
    assert!(actions[3].sends.is_empty());
    assert_eq!(
        actions[4]
            .sends
            .iter()
            .map(|delivery| (
                delivery.to.resolve(MailAddr(0)),
                delivery.message.generation
            ))
            .collect::<Vec<_>>(),
        [
            (MailAddr(1), BarrierGeneration(1)),
            (MailAddr(2), BarrierGeneration(1)),
        ]
    );
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn inferred_barrier_returns_exact_rejections_without_later_work() {
    let one = Recipient::<BarrierParticipant>::global(MailAddr(1));
    let two = Recipient::<BarrierParticipant>::global(MailAddr(2));

    let (unknown, actions, retirements) =
        drive_barrier([barrier_event(0, 9, one), barrier_event(0, 1, one)]).await;
    assert!(matches!(
        unknown,
        Err(DriverError::Behavior(BarrierError::UnknownParticipant(9)))
    ));
    assert_eq!(actions.lock().unwrap().len(), 1);
    assert_eq!(retirements, 1);

    let (future, actions, retirements) =
        drive_barrier([barrier_event(1, 1, one), barrier_event(0, 1, one)]).await;
    assert!(matches!(
        future,
        Err(DriverError::Behavior(BarrierError::FutureGeneration {
            participant: 1,
            observed: BarrierGeneration(1),
            current: BarrierGeneration(0),
        }))
    ));
    assert_eq!(actions.lock().unwrap().len(), 1);
    assert_eq!(retirements, 1);

    let (duplicate, actions, retirements) = drive_barrier([
        barrier_event(0, 1, one),
        barrier_event(0, 1, one),
        barrier_event(0, 2, two),
    ])
    .await;
    assert!(matches!(
        duplicate,
        Err(DriverError::Behavior(BarrierError::DuplicateArrival {
            participant: 1,
            generation: BarrierGeneration(0),
        }))
    ));
    assert_eq!(actions.lock().unwrap().len(), 2);
    assert_eq!(retirements, 1);

    let (stale, actions, retirements) = drive_barrier([
        barrier_event(0, 1, one),
        barrier_event(0, 2, two),
        barrier_event(0, 1, one),
        barrier_event(1, 1, one),
    ])
    .await;
    assert!(matches!(
        stale,
        Err(DriverError::Behavior(BarrierError::StaleGeneration {
            participant: 1,
            observed: BarrierGeneration(0),
            current: BarrierGeneration(1),
        }))
    ));
    assert_eq!(actions.lock().unwrap().len(), 3);
    assert_eq!(retirements, 1);
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "every cache command result and ownership transition forms one transcript"
)]
async fn inferred_cache_preserves_recency_displaced_values_and_factual_absence() {
    type Subject = Cache<MailAddr, u8, String, CacheReply>;

    assert!(matches!(
        Subject::new(0),
        Err(CacheConfigError::ZeroCapacity)
    ));
    let reply_to = Recipient::<CacheReply>::global(MailAddr(8));
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        Subject::new(2).unwrap(),
        CaptureEnvironment {
            events: [
                CacheMessage::Put {
                    key: 1,
                    value: "one".to_owned(),
                    reply_to,
                },
                CacheMessage::Put {
                    key: 2,
                    value: "two".to_owned(),
                    reply_to,
                },
                CacheMessage::Get { key: 1, reply_to },
                CacheMessage::Put {
                    key: 3,
                    value: "three".to_owned(),
                    reply_to,
                },
                CacheMessage::Put {
                    key: 1,
                    value: "one-new".to_owned(),
                    reply_to,
                },
                CacheMessage::Get { key: 2, reply_to },
                CacheMessage::Remove { key: 1, reply_to },
                CacheMessage::Remove { key: 1, reply_to },
            ]
            .map(|message| User::new(MailAddr(7), message))
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let results = actions
        .lock()
        .unwrap()
        .iter()
        .skip(1)
        .map(|actions| actions.sends[0].message.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        results,
        [
            CacheResult::Stored {
                key: 1,
                replaced: None,
                evicted: None,
            },
            CacheResult::Stored {
                key: 2,
                replaced: None,
                evicted: None,
            },
            CacheResult::Hit {
                key: 1,
                value: "one".to_owned(),
            },
            CacheResult::Stored {
                key: 3,
                replaced: None,
                evicted: Some(CacheEntry {
                    key: 2,
                    value: "two".to_owned(),
                }),
            },
            CacheResult::Stored {
                key: 1,
                replaced: Some("one".to_owned()),
                evicted: None,
            },
            CacheResult::Miss { key: 2 },
            CacheResult::Removed {
                key: 1,
                value: "one-new".to_owned(),
            },
            CacheResult::Absent { key: 1 },
        ]
    );
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_registry_preserves_typed_found_missing_bind_and_unbind_facts() {
    let destination = Recipient::<RegistryDestination>::global(MailAddr(1));
    let reply_to = Recipient::<RegistryReply>::global(MailAddr(8));
    let key = || "worker".to_owned();
    let (result, actions, retirements) = drive_registry([
        registry_event(RegistryMessage::Lookup {
            key: key(),
            reply_to,
        }),
        registry_event(RegistryMessage::Bind {
            key: key(),
            recipient: destination,
        }),
        registry_event(RegistryMessage::Lookup {
            key: key(),
            reply_to,
        }),
        registry_event(RegistryMessage::Unbind {
            key: key(),
            recipient: destination,
        }),
        registry_event(RegistryMessage::Lookup {
            key: key(),
            reply_to,
        }),
    ])
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 6, "initialization plus five commands");
    assert!(matches!(
        &actions[1].sends[0].message,
        RegistryResult::Missing { key } if key == "worker"
    ));
    assert!(actions[2].sends.is_empty());
    assert!(matches!(
        &actions[3].sends[0].message,
        RegistryResult::Found { key, recipient }
            if key == "worker" && *recipient == destination
    ));
    assert!(actions[4].sends.is_empty());
    assert!(matches!(
        &actions[5].sends[0].message,
        RegistryResult::Missing { key } if key == "worker"
    ));
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn inferred_registry_preserves_exact_mutation_rejections_and_fuses() {
    let one = Recipient::<RegistryDestination>::global(MailAddr(1));
    let two = Recipient::<RegistryDestination>::global(MailAddr(2));
    let key = || "worker".to_owned();

    let (already_bound, actions, retirements) = drive_registry([
        registry_event(RegistryMessage::Bind {
            key: key(),
            recipient: one,
        }),
        registry_event(RegistryMessage::Bind {
            key: key(),
            recipient: two,
        }),
        registry_event(RegistryMessage::Unbind {
            key: key(),
            recipient: one,
        }),
    ])
    .await;
    assert!(matches!(
        already_bound,
        Err(DriverError::Behavior(RegistryError::AlreadyBound(key)))
            if key == "worker"
    ));
    assert_eq!(actions.lock().unwrap().len(), 2);
    assert_eq!(retirements, 1);

    let (stale, actions, retirements) = drive_registry([
        registry_event(RegistryMessage::Bind {
            key: key(),
            recipient: one,
        }),
        registry_event(RegistryMessage::Unbind {
            key: key(),
            recipient: two,
        }),
        registry_event(RegistryMessage::Unbind {
            key: key(),
            recipient: one,
        }),
    ])
    .await;
    assert!(matches!(
        stale,
        Err(DriverError::Behavior(RegistryError::StaleBinding(key)))
            if key == "worker"
    ));
    assert_eq!(actions.lock().unwrap().len(), 2);
    assert_eq!(retirements, 1);

    let (not_bound, actions, retirements) = drive_registry([
        registry_event(RegistryMessage::Unbind {
            key: key(),
            recipient: one,
        }),
        registry_event(RegistryMessage::Bind {
            key: key(),
            recipient: one,
        }),
    ])
    .await;
    assert!(matches!(
        not_bound,
        Err(DriverError::Behavior(RegistryError::NotBound(key)))
            if key == "worker"
    ));
    assert_eq!(actions.lock().unwrap().len(), 1);
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn inferred_resolver_preserves_borrowed_definition_and_typed_lookup_facts() {
    type Subject = Resolver<MailAddr, String, RegistryDestination, ResolverReply>;

    let destination = Recipient::<RegistryDestination>::global(MailAddr(1));
    let duplicate_source = vec![
        ("worker".to_owned(), destination),
        ("worker".to_owned(), destination),
    ];
    assert!(matches!(
        Subject::from_bindings(&duplicate_source),
        Err(ResolverConfigError::DuplicateKey { key }) if key == "worker"
    ));
    assert_eq!(
        duplicate_source.len(),
        2,
        "rejection retains borrowed source"
    );

    let reply_to = Recipient::<ResolverReply>::global(MailAddr(8));
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        Subject::from_bindings(&[("worker".to_owned(), destination)]).unwrap(),
        CaptureEnvironment {
            events: ["worker", "missing"]
                .map(|key| {
                    User::new(
                        MailAddr(7),
                        ResolverMessage::Resolve {
                            key: key.to_owned(),
                            reply_to,
                        },
                    )
                })
                .into_iter()
                .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 3, "initialization plus two lookups");
    assert!(matches!(
        &actions[1].sends[0].message,
        Resolution::Found { key, recipient }
            if key == "worker" && *recipient == destination
    ));
    assert!(matches!(
        &actions[2].sends[0].message,
        Resolution::Missing { key } if key == "missing"
    ));
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_topic_preserves_idempotent_membership_and_publication_order() {
    let one = Recipient::<PublicationSubscriber>::global(MailAddr(1));
    let two = Recipient::<PublicationSubscriber>::global(MailAddr(2));
    let (result, actions, retirements) = drive_topic([
        topic_event(TopicMessage::Subscribe(one)),
        topic_event(TopicMessage::Subscribe(two)),
        topic_event(TopicMessage::Subscribe(one)),
        topic_event(TopicMessage::Publish("first".to_owned())),
        topic_event(TopicMessage::Unsubscribe(one)),
        topic_event(TopicMessage::Publish("second".to_owned())),
    ])
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 7, "initialization plus six topic commands");
    assert_eq!(
        actions[4]
            .sends
            .iter()
            .map(|delivery| (delivery.to.resolve(MailAddr(0)), delivery.message.as_str()))
            .collect::<Vec<_>>(),
        [(MailAddr(1), "first"), (MailAddr(2), "first")]
    );
    assert_eq!(actions[6].sends.len(), 1);
    assert_eq!(actions[6].sends[0].to.resolve(MailAddr(0)), MailAddr(2));
    assert_eq!(actions[6].sends[0].message, "second");
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn inferred_topic_returns_empty_publication_ownership_and_fuses() {
    let one = Recipient::<PublicationSubscriber>::global(MailAddr(1));
    let (result, actions, retirements) = drive_topic([
        topic_event(TopicMessage::Publish("owned".to_owned())),
        topic_event(TopicMessage::Subscribe(one)),
    ])
    .await;

    assert!(matches!(
        result,
        Err(DriverError::Behavior(TopicError::NoSubscribers(value)))
            if value == "owned"
    ));
    assert_eq!(actions.lock().unwrap().len(), 1);
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn inferred_pub_sub_preserves_keyed_membership_and_snapshot_order() {
    let one = Recipient::<PublicationSubscriber>::global(MailAddr(1));
    let two = Recipient::<PublicationSubscriber>::global(MailAddr(2));
    let topic = || "orders".to_owned();
    let (result, actions, retirements) = drive_pub_sub([
        pub_sub_event(PubSubMessage::Subscribe {
            topic: topic(),
            subscriber: one,
        }),
        pub_sub_event(PubSubMessage::Subscribe {
            topic: topic(),
            subscriber: two,
        }),
        pub_sub_event(PubSubMessage::Subscribe {
            topic: topic(),
            subscriber: one,
        }),
        pub_sub_event(PubSubMessage::Publish {
            topic: topic(),
            value: "first".to_owned(),
        }),
        pub_sub_event(PubSubMessage::Unsubscribe {
            topic: topic(),
            subscriber: one,
        }),
        pub_sub_event(PubSubMessage::Publish {
            topic: topic(),
            value: "second".to_owned(),
        }),
    ])
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 7);
    assert_eq!(
        actions[4]
            .sends
            .iter()
            .map(|delivery| (delivery.to.resolve(MailAddr(0)), delivery.message.as_str()))
            .collect::<Vec<_>>(),
        [(MailAddr(1), "first"), (MailAddr(2), "first")]
    );
    assert_eq!(actions[6].sends.len(), 1);
    assert_eq!(actions[6].sends[0].to.resolve(MailAddr(0)), MailAddr(2));
    assert_eq!(actions[6].sends[0].message, "second");
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn inferred_pub_sub_returns_exact_unsubscribe_and_publication_rejections() {
    let one = Recipient::<PublicationSubscriber>::global(MailAddr(1));
    let two = Recipient::<PublicationSubscriber>::global(MailAddr(2));
    let topic = || "orders".to_owned();

    let (unknown, actions, retirements) = drive_pub_sub([
        pub_sub_event(PubSubMessage::Unsubscribe {
            topic: topic(),
            subscriber: one,
        }),
        pub_sub_event(PubSubMessage::Subscribe {
            topic: topic(),
            subscriber: one,
        }),
    ])
    .await;
    assert!(matches!(
        unknown,
        Err(DriverError::Behavior(PubSubError::UnknownTopic { topic }))
            if topic == "orders"
    ));
    assert_eq!(actions.lock().unwrap().len(), 1);
    assert_eq!(retirements, 1);

    let (not_subscribed, actions, retirements) = drive_pub_sub([
        pub_sub_event(PubSubMessage::Subscribe {
            topic: topic(),
            subscriber: one,
        }),
        pub_sub_event(PubSubMessage::Unsubscribe {
            topic: topic(),
            subscriber: two,
        }),
        pub_sub_event(PubSubMessage::Publish {
            topic: topic(),
            value: "later".to_owned(),
        }),
    ])
    .await;
    assert!(matches!(
        not_subscribed,
        Err(DriverError::Behavior(PubSubError::NotSubscribed { topic }))
            if topic == "orders"
    ));
    assert_eq!(actions.lock().unwrap().len(), 2);
    assert_eq!(retirements, 1);

    let (empty, actions, retirements) = drive_pub_sub([
        pub_sub_event(PubSubMessage::Subscribe {
            topic: topic(),
            subscriber: one,
        }),
        pub_sub_event(PubSubMessage::Unsubscribe {
            topic: topic(),
            subscriber: one,
        }),
        pub_sub_event(PubSubMessage::Publish {
            topic: topic(),
            value: "owned".to_owned(),
        }),
    ])
    .await;
    assert!(matches!(
        empty,
        Err(DriverError::Behavior(PubSubError::NoSubscribers { topic, value }))
            if topic == "orders" && value == "owned"
    ));
    assert_eq!(actions.lock().unwrap().len(), 3);
    assert_eq!(retirements, 1);

    let (unknown_publish, actions, retirements) = drive_pub_sub([
        pub_sub_event(PubSubMessage::Publish {
            topic: "missing".to_owned(),
            value: "returned".to_owned(),
        }),
        pub_sub_event(PubSubMessage::Subscribe {
            topic: topic(),
            subscriber: one,
        }),
    ])
    .await;
    assert!(matches!(
        unknown_publish,
        Err(DriverError::Behavior(PubSubError::NoSubscribers { topic, value }))
            if topic == "missing" && value == "returned"
    ));
    assert_eq!(actions.lock().unwrap().len(), 1);
    assert_eq!(retirements, 1);
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "all presence outcomes, named lanes, and timer generations form one causal transcript"
)]
async fn inferred_presence_preserves_versions_named_lanes_expiry_and_tombstones() {
    type Subject = Presence<MailAddr, u8, PresenceReplyBehavior>;

    let reply_to = Recipient::<PresenceReplyBehavior>::global(MailAddr(8));
    let lifetime = Duration::from_secs(1);
    let changed_lifetime = Duration::from_secs(2);
    let announce = |participant, version, lifetime| {
        User::new(
            MailAddr(7),
            PresenceMessage::Announce {
                participant,
                version: PresenceVersion(version),
                lifetime,
                reply_to,
            },
        )
    };
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        Subject::new(presence_timer),
        CaptureEnvironment {
            events: [
                announce(1, 1, lifetime),
                announce(1, 1, lifetime),
                announce(1, 1, changed_lifetime),
                announce(1, 0, lifetime),
                announce(3, 1, lifetime),
                announce(1, 2, lifetime),
                User::new(
                    MailAddr(7),
                    PresenceMessage::Elapsed(TimerElapsed::new(TimerId(0), TimerGeneration(0))),
                ),
                User::new(
                    MailAddr(7),
                    PresenceMessage::Elapsed(TimerElapsed::new(TimerId(1), TimerGeneration(0))),
                ),
                User::new(MailAddr(7), PresenceMessage::Query { reply_to }),
                User::new(
                    MailAddr(7),
                    PresenceMessage::Elapsed(TimerElapsed::new(TimerId(1), TimerGeneration(1))),
                ),
                User::new(MailAddr(7), PresenceMessage::Query { reply_to }),
                announce(1, 3, lifetime),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 13, "initialization plus twelve facts");
    assert!(matches!(
        actions[1].sends.replies[0].message,
        PresenceReply::Outcome(PresenceOutcome::Announced {
            participant: 1,
            version: PresenceVersion(1),
            generation: TimerGeneration(0),
        })
    ));
    assert_eq!(actions[1].sends.schedules[0].id, TimerId(1));
    assert_eq!(actions[1].sends.schedules[0].generation, TimerGeneration(0));
    assert!(matches!(
        actions[2].sends.replies[0].message,
        PresenceReply::Outcome(PresenceOutcome::Unchanged { participant: 1, .. })
    ));
    assert!(actions[2].sends.schedules.is_empty());
    assert!(matches!(
        actions[3].sends.replies[0].message,
        PresenceReply::Outcome(PresenceOutcome::Rejected(
            PresenceError::ConflictingVersion { participant: 1, .. }
        ))
    ));
    assert!(matches!(
        actions[4].sends.replies[0].message,
        PresenceReply::Outcome(PresenceOutcome::Rejected(PresenceError::Stale {
            participant: 1,
            ..
        }))
    ));
    assert!(matches!(
        actions[5].sends.replies[0].message,
        PresenceReply::Outcome(PresenceOutcome::Rejected(PresenceError::TimerCollision {
            participant: 3,
            existing: 1,
            timer_id: TimerId(1),
        }))
    ));
    assert!(matches!(
        actions[6].sends.replies[0].message,
        PresenceReply::Outcome(PresenceOutcome::Refreshed {
            participant: 1,
            version: PresenceVersion(2),
            generation: TimerGeneration(1),
        })
    ));
    assert_eq!(actions[6].sends.schedules[0].generation, TimerGeneration(1));
    assert!(actions[7].sends.replies.is_empty());
    assert!(actions[7].sends.schedules.is_empty());
    assert!(actions[8].sends.replies.is_empty());
    assert!(actions[8].sends.schedules.is_empty());
    assert!(matches!(
        &actions[9].sends.replies[0].message,
        PresenceReply::Report(report)
            if matches!(report.entries[0].phase, PresencePhase::Present {
                version: PresenceVersion(2),
                generation: TimerGeneration(1),
                ..
            })
    ));
    assert!(matches!(
        actions[10].sends.replies[0].message,
        PresenceReply::Outcome(PresenceOutcome::Expired {
            participant: 1,
            version: PresenceVersion(2),
            generation: TimerGeneration(1),
        })
    ));
    assert!(matches!(
        &actions[11].sends.replies[0].message,
        PresenceReply::Report(report)
            if matches!(report.entries[0].phase, PresencePhase::Expired {
                version: PresenceVersion(2),
                generation: TimerGeneration(1),
            })
    ));
    assert!(matches!(
        actions[12].sends.replies[0].message,
        PresenceReply::Outcome(PresenceOutcome::Refreshed {
            participant: 1,
            version: PresenceVersion(3),
            generation: TimerGeneration(2),
        })
    ));
    assert_eq!(
        actions[12].sends.schedules[0].generation,
        TimerGeneration(2)
    );
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the exhaustive acknowledgement lifecycle and rejection sum form one transcript"
)]
async fn inferred_acknowledgements_preserves_every_phase_outcome_and_rejection() {
    type Subject = Acknowledgements<MailAddr, String, String, AcknowledgementReply>;

    let reply_to = Recipient::<AcknowledgementReply>::global(MailAddr(8));
    let begin = |key: &str, participants: &[&str]| {
        User::new(
            MailAddr(7),
            AcknowledgementMessage::Begin {
                key: key.to_owned(),
                participants: participants
                    .iter()
                    .map(|participant| (*participant).to_owned())
                    .collect(),
                reply_to,
            },
        )
    };
    let acknowledge = |key: &str, participant: &str| {
        User::new(
            MailAddr(7),
            AcknowledgementMessage::Acknowledge {
                key: key.to_owned(),
                participant: participant.to_owned(),
                reply_to,
            },
        )
    };
    let cancel = |key: &str| {
        User::new(
            MailAddr(7),
            AcknowledgementMessage::Cancel {
                key: key.to_owned(),
                reply_to,
            },
        )
    };
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        Subject::new(),
        CaptureEnvironment {
            events: [
                begin("empty", &[]),
                begin("empty", &["ignored"]),
                acknowledge("unknown", "one"),
                cancel("unknown"),
                begin("work", &["one", "one", "two"]),
                acknowledge("work", "unexpected"),
                acknowledge("work", "one"),
                acknowledge("work", "one"),
                acknowledge("work", "two"),
                acknowledge("work", "two"),
                cancel("work"),
                begin("cancelled", &["three"]),
                cancel("cancelled"),
                acknowledge("cancelled", "three"),
                cancel("cancelled"),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let outcomes = actions
        .lock()
        .unwrap()
        .iter()
        .skip(1)
        .map(|actions| actions.sends[0].message.clone())
        .collect::<Vec<_>>();
    assert_eq!(outcomes.len(), 15);
    assert!(matches!(
        &outcomes[0],
        AcknowledgementOutcome::Completed { key } if key == "empty"
    ));
    assert!(matches!(
        &outcomes[1],
        AcknowledgementOutcome::Rejected(AcknowledgementError::Existing { key })
            if key == "empty"
    ));
    assert!(matches!(
        &outcomes[2],
        AcknowledgementOutcome::Rejected(AcknowledgementError::Unknown { key })
            if key == "unknown"
    ));
    assert!(matches!(
        &outcomes[3],
        AcknowledgementOutcome::Rejected(AcknowledgementError::Unknown { key })
            if key == "unknown"
    ));
    assert!(matches!(
        &outcomes[4],
        AcknowledgementOutcome::Started { key, remaining: 2 } if key == "work"
    ));
    assert!(matches!(
        &outcomes[5],
        AcknowledgementOutcome::Rejected(AcknowledgementError::UnexpectedParticipant {
            key,
            participant,
        }) if key == "work" && participant == "unexpected"
    ));
    assert!(matches!(
        &outcomes[6],
        AcknowledgementOutcome::Acknowledged {
            key,
            participant,
            remaining: 1,
        } if key == "work" && participant == "one"
    ));
    assert!(matches!(
        &outcomes[7],
        AcknowledgementOutcome::Rejected(AcknowledgementError::DuplicateParticipant {
            key,
            participant,
        }) if key == "work" && participant == "one"
    ));
    assert!(matches!(
        &outcomes[8],
        AcknowledgementOutcome::Completed { key } if key == "work"
    ));
    assert!(matches!(
        &outcomes[9],
        AcknowledgementOutcome::Rejected(AcknowledgementError::Completed { key })
            if key == "work"
    ));
    assert!(matches!(
        &outcomes[10],
        AcknowledgementOutcome::Rejected(AcknowledgementError::Completed { key })
            if key == "work"
    ));
    assert!(matches!(
        &outcomes[11],
        AcknowledgementOutcome::Started { key, remaining: 1 } if key == "cancelled"
    ));
    assert!(matches!(
        &outcomes[12],
        AcknowledgementOutcome::Cancelled { key } if key == "cancelled"
    ));
    assert!(matches!(
        &outcomes[13],
        AcknowledgementOutcome::Rejected(AcknowledgementError::Cancelled { key })
            if key == "cancelled"
    ));
    assert!(matches!(
        &outcomes[14],
        AcknowledgementOutcome::Rejected(AcknowledgementError::Cancelled { key })
            if key == "cancelled"
    ));
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_sequencer_preserves_move_only_gap_release_and_rejections() {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        SequencerSubject::new(Sequence(3)),
        CaptureEnvironment {
            events: [
                sequencer_event(4, 40),
                sequencer_event(4, 41),
                sequencer_event(3, 30),
                sequencer_event(3, 31),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 5);
    assert!(actions[1].sends.deliveries.is_empty());
    assert!(matches!(
        actions[1].sends.outcomes[0].message,
        SequencerOutcome::Accepted {
            released: 0,
            buffered: 1,
        }
    ));
    assert!(actions[2].sends.deliveries.is_empty());
    assert!(matches!(
        &actions[2].sends.outcomes[0].message,
        SequencerOutcome::Duplicate {
            value,
            sequence: Sequence(4),
        } if **value == 41
    ));
    assert_eq!(
        actions[3]
            .sends
            .deliveries
            .iter()
            .map(|delivery| *delivery.message.as_ref())
            .collect::<Vec<_>>(),
        [30, 40]
    );
    assert!(matches!(
        actions[3].sends.outcomes[0].message,
        SequencerOutcome::Accepted {
            released: 2,
            buffered: 0,
        }
    ));
    assert!(matches!(
        &actions[4].sends.outcomes[0].message,
        SequencerOutcome::Stale {
            value,
            expected: Sequence(5),
        } if **value == 31
    ));
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_sequencer_exhausts_maximum_without_wrapping_or_stopping_driver() {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        SequencerSubject::new(Sequence(u64::MAX)),
        CaptureEnvironment {
            events: [sequencer_event(u64::MAX, 1), sequencer_event(u64::MAX, 2)]
                .into_iter()
                .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(*actions[1].sends.deliveries[0].message, 1);
    assert!(matches!(
        actions[1].sends.outcomes[0].message,
        SequencerOutcome::Accepted {
            released: 1,
            buffered: 0,
        }
    ));
    assert!(actions[2].sends.deliveries.is_empty());
    assert!(matches!(
        &actions[2].sends.outcomes[0].message,
        SequencerOutcome::Exhausted { value } if **value == 2
    ));
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_order_gate_preserves_move_only_release_order_and_watermark_facts() {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        OrderGateSubject::new(),
        CaptureEnvironment {
            events: [
                hold_event(3, 30),
                hold_event(1, 10),
                hold_event(3, 31),
                open_event(2),
                hold_event(2, 20),
                open_event(2),
                open_event(4),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 8);
    assert!(matches!(
        actions[1].sends.outcomes[0].message,
        OrderGateOutcome::Held { key: 3, held: 1 }
    ));
    assert!(matches!(
        actions[2].sends.outcomes[0].message,
        OrderGateOutcome::Held { key: 1, held: 2 }
    ));
    assert!(matches!(
        &actions[3].sends.outcomes[0].message,
        OrderGateOutcome::Duplicate { key: 3, value } if **value == 31
    ));
    assert_eq!(*actions[4].sends.deliveries[0].message, 10);
    assert!(matches!(
        actions[4].sends.outcomes[0].message,
        OrderGateOutcome::Opened {
            through: 2,
            released: 1,
            held: 1,
        }
    ));
    assert_eq!(*actions[5].sends.deliveries[0].message, 20);
    assert!(matches!(
        actions[5].sends.outcomes[0].message,
        OrderGateOutcome::Delivered { key: 2 }
    ));
    assert!(actions[6].sends.deliveries.is_empty());
    assert!(matches!(
        actions[6].sends.outcomes[0].message,
        OrderGateOutcome::StaleOpening {
            requested: 2,
            current: 2,
        }
    ));
    assert_eq!(*actions[7].sends.deliveries[0].message, 30);
    assert!(matches!(
        actions[7].sends.outcomes[0].message,
        OrderGateOutcome::Opened {
            through: 4,
            released: 1,
            held: 0,
        }
    ));
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_deduplicator_preserves_move_only_ownership_and_fifo_retention_facts() {
    assert!(matches!(
        DeduplicatorSubject::new(0),
        Err(DeduplicatorConfigError::ZeroCapacity)
    ));

    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        DeduplicatorSubject::new(2).unwrap(),
        CaptureEnvironment {
            events: [
                deduplicator_event(1, 10),
                deduplicator_event(1, 11),
                deduplicator_event(2, 20),
                deduplicator_event(3, 30),
                deduplicator_event(1, 12),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 6, "initialization plus five exact turns");
    assert_eq!(*actions[1].sends.deliveries[0].message, 10);
    assert!(matches!(
        actions[1].sends.outcomes[0].message,
        DeduplicatorOutcome::Delivered {
            key: 1,
            evicted: None,
        }
    ));
    assert!(actions[2].sends.deliveries.is_empty());
    assert!(matches!(
        &actions[2].sends.outcomes[0].message,
        DeduplicatorOutcome::Duplicate { key: 1, value } if **value == 11
    ));
    assert_eq!(*actions[3].sends.deliveries[0].message, 20);
    assert!(matches!(
        actions[3].sends.outcomes[0].message,
        DeduplicatorOutcome::Delivered {
            key: 2,
            evicted: None,
        }
    ));
    assert_eq!(*actions[4].sends.deliveries[0].message, 30);
    assert!(matches!(
        actions[4].sends.outcomes[0].message,
        DeduplicatorOutcome::Delivered {
            key: 3,
            evicted: Some(1),
        }
    ));
    assert_eq!(*actions[5].sends.deliveries[0].message, 12);
    assert!(matches!(
        actions[5].sends.outcomes[0].message,
        DeduplicatorOutcome::Delivered {
            key: 1,
            evicted: Some(2),
        }
    ));
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_correlator_preserves_terminal_results_and_owned_rejection_facts() {
    let (resolved, resolved_actions, resolved_retirements) = drive_correlator([
        begin_correlation(1),
        resolve_correlation(1, 10),
        begin_correlation(2),
        cancel_correlation(2),
    ])
    .await;
    assert!(matches!(resolved, Ok(Completion::Exhausted)));
    {
        let resolved_actions = resolved_actions.lock().unwrap();
        assert_eq!(resolved_actions.len(), 5);
        assert!(matches!(
            &resolved_actions[2].sends[0].message,
            CorrelationResult::Resolved { key: 1, value } if **value == 10
        ));
        assert!(matches!(
            resolved_actions[4].sends[0].message,
            CorrelationResult::Cancelled { key: 2 }
        ));
    }
    assert_eq!(resolved_retirements, 1);

    let (unknown_reply, actions, retirements) =
        drive_correlator([resolve_correlation(3, 30)]).await;
    assert!(matches!(
        unknown_reply,
        Err(DriverError::Behavior(CorrelatorError::UnknownReply { key: 3, value }))
            if *value == 30
    ));
    assert_eq!(
        actions.lock().unwrap().len(),
        1,
        "only initialization committed"
    );
    assert_eq!(retirements, 1);

    let (duplicate, actions, retirements) =
        drive_correlator([begin_correlation(4), begin_correlation(4)]).await;
    assert!(matches!(
        duplicate,
        Err(DriverError::Behavior(CorrelatorError::AlreadyPending(4)))
    ));
    assert_eq!(actions.lock().unwrap().len(), 2);
    assert_eq!(retirements, 1);

    let (stale_completed, actions, retirements) = drive_correlator([
        begin_correlation(5),
        resolve_correlation(5, 50),
        resolve_correlation(5, 51),
    ])
    .await;
    assert!(matches!(
        stale_completed,
        Err(DriverError::Behavior(CorrelatorError::StaleCompleted { key: 5, value }))
            if *value == 51
    ));
    assert_eq!(
        actions.lock().unwrap().len(),
        3,
        "committed prefix is factual"
    );
    assert_eq!(retirements, 1);

    let (stale_cancelled, actions, retirements) = drive_correlator([
        begin_correlation(6),
        cancel_correlation(6),
        resolve_correlation(6, 60),
    ])
    .await;
    assert!(matches!(
        stale_cancelled,
        Err(DriverError::Behavior(CorrelatorError::StaleCancelled { key: 6, value }))
            if *value == 60
    ));
    assert_eq!(
        actions.lock().unwrap().len(),
        3,
        "committed prefix is factual"
    );
    assert_eq!(retirements, 1);

    let (unknown_cancel, actions, retirements) = drive_correlator([cancel_correlation(7)]).await;
    assert!(matches!(
        unknown_cancel,
        Err(DriverError::Behavior(CorrelatorError::Unknown(7)))
    ));
    assert_eq!(actions.lock().unwrap().len(), 1);
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn inferred_buffer_preserves_every_move_only_value_under_all_overflow_policies() {
    assert!(matches!(
        BufferSubject::new(0, OverflowPolicy::Reject),
        Err(BufferConfigError::ZeroCapacity)
    ));

    for policy in [
        OverflowPolicy::Reject,
        OverflowPolicy::DropNewest,
        OverflowPolicy::DropOldest,
    ] {
        let (result, actions, retirements) = drive_buffer(policy).await;
        assert!(matches!(result, Ok(Completion::Exhausted)));
        let actions = actions.lock().unwrap();
        assert_eq!(actions.len(), 7, "initialization plus six exact turns");
        assert!(matches!(
            actions[1].sends.outcomes[0].message,
            BufferOutcome::Accepted { depth: 1 }
        ));
        assert!(matches!(
            actions[2].sends.outcomes[0].message,
            BufferOutcome::Accepted { depth: 2 }
        ));

        match policy {
            OverflowPolicy::Reject | OverflowPolicy::DropNewest => {
                let expected = if policy == OverflowPolicy::Reject {
                    BufferRejection::Full
                } else {
                    BufferRejection::DroppedNewest
                };
                assert!(matches!(
                    &actions[3].sends.outcomes[0].message,
                    BufferOutcome::Rejected { value, reason }
                        if **value == 12 && *reason == expected
                ));
                assert_eq!(*actions[4].sends.deliveries[0].message, 10);
                assert_eq!(*actions[5].sends.deliveries[0].message, 11);
            }
            OverflowPolicy::DropOldest => {
                assert!(matches!(
                    &actions[3].sends.outcomes[0].message,
                    BufferOutcome::Evicted { value } if **value == 10
                ));
                assert!(matches!(
                    actions[3].sends.outcomes[1].message,
                    BufferOutcome::Accepted { depth: 2 }
                ));
                assert_eq!(*actions[4].sends.deliveries[0].message, 11);
                assert_eq!(*actions[5].sends.deliveries[0].message, 12);
            }
        }
        assert!(matches!(
            actions[4].sends.outcomes[0].message,
            BufferOutcome::Released { remaining: 1 }
        ));
        assert!(matches!(
            actions[5].sends.outcomes[0].message,
            BufferOutcome::Released { remaining: 0 }
        ));
        assert!(actions[6].sends.deliveries.is_empty());
        assert!(matches!(
            actions[6].sends.outcomes[0].message,
            BufferOutcome::Empty
        ));
        assert_eq!(retirements, 1);
    }
}

#[tokio::test]
async fn inferred_work_queue_preserves_fifo_availability_admission_and_owned_rejection() {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        WorkQueueSubject::new(2),
        CaptureEnvironment {
            events: [
                worker_available(1),
                worker_available(1),
                worker_available(2),
                withdraw_worker(1),
                withdraw_worker(1),
                submit_work(10),
                submit_work(20),
                submit_work(30),
                submit_work(40),
                worker_available(3),
                worker_available(4),
                worker_available(5),
                submit_work(50),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(
        actions.len(),
        14,
        "initialization plus thirteen exact turns"
    );
    assert!(actions[6].sends.assignments[0].to == Recipient::global(MailAddr(2)));
    assert_eq!(*actions[6].sends.assignments[0].message, 10);
    assert!(matches!(
        actions[6].sends.outcomes[0].message,
        WorkQueueOutcome::Dispatched { queued: 0 }
    ));
    assert!(matches!(
        actions[7].sends.outcomes[0].message,
        WorkQueueOutcome::Queued { depth: 1 }
    ));
    assert!(matches!(
        actions[8].sends.outcomes[0].message,
        WorkQueueOutcome::Queued { depth: 2 }
    ));
    assert!(matches!(
        &actions[9].sends.outcomes[0].message,
        WorkQueueOutcome::Rejected {
            value,
            reason: WorkQueueRejection::Full,
        } if **value == 40
    ));
    assert!(actions[10].sends.assignments[0].to == Recipient::global(MailAddr(3)));
    assert_eq!(*actions[10].sends.assignments[0].message, 20);
    assert!(matches!(
        actions[10].sends.outcomes[0].message,
        WorkQueueOutcome::Dispatched { queued: 1 }
    ));
    assert!(actions[11].sends.assignments[0].to == Recipient::global(MailAddr(4)));
    assert_eq!(*actions[11].sends.assignments[0].message, 30);
    assert!(matches!(
        actions[11].sends.outcomes[0].message,
        WorkQueueOutcome::Dispatched { queued: 0 }
    ));
    assert!(actions[13].sends.assignments[0].to == Recipient::global(MailAddr(5)));
    assert_eq!(*actions[13].sends.assignments[0].message, 50);
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_priority_queue_preserves_stable_order_and_move_only_boundaries() {
    assert!(matches!(
        PriorityQueueSubject::new(0),
        Err(PriorityQueueConfigError::ZeroCapacity)
    ));

    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        PriorityQueueSubject::new(4).unwrap(),
        CaptureEnvironment {
            events: [
                offer_priority(10, 2),
                offer_priority(20, 3),
                offer_priority(30, 3),
                offer_priority(40, 1),
                offer_priority(50, 9),
                release_priority(),
                release_priority(),
                release_priority(),
                release_priority(),
                release_priority(),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 11, "initialization plus ten exact turns");
    for (index, depth) in (1..=4).zip(1..=4) {
        assert!(matches!(
            actions[index].sends.outcomes[0].message,
            PriorityQueueOutcome::Accepted { depth: actual } if actual == depth
        ));
    }
    assert!(matches!(
        &actions[5].sends.outcomes[0].message,
        PriorityQueueOutcome::Rejected {
            value,
            reason: PriorityQueueRejection::Full,
        } if **value == 50
    ));
    assert_eq!(
        actions[6..10]
            .iter()
            .map(|action| *action.sends.deliveries[0].message.as_ref())
            .collect::<Vec<_>>(),
        [20, 30, 10, 40],
        "greater priority wins and equal-priority insertion order is stable"
    );
    assert!(matches!(
        actions[6].sends.outcomes[0].message,
        PriorityQueueOutcome::Released { remaining: 3 }
    ));
    assert!(matches!(
        actions[9].sends.outcomes[0].message,
        PriorityQueueOutcome::Released { remaining: 0 }
    ));
    assert!(actions[10].sends.deliveries.is_empty());
    assert!(matches!(
        actions[10].sends.outcomes[0].message,
        PriorityQueueOutcome::Empty
    ));
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the complete typed breaker phase transcript stays readable as one causal run"
)]
async fn inferred_circuit_breaker_preserves_phase_attempt_and_timer_generation_facts() {
    assert!(matches!(
        BreakerSubject::new(NonZeroU32::new(2).unwrap(), Duration::ZERO, TimerId(8)),
        Err(BreakerConfigError::ZeroResetDelay)
    ));

    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        BreakerSubject::new(
            NonZeroU32::new(2).unwrap(),
            Duration::from_secs(1),
            TimerId(8),
        )
        .unwrap(),
        CaptureEnvironment {
            events: [
                breaker_event(BreakerMessage::Succeeded {
                    attempt: BreakerAttempt(99),
                }),
                breaker_elapsed(8, 99),
                breaker_admit(),
                breaker_admit(),
                breaker_event(BreakerMessage::Failed {
                    attempt: BreakerAttempt(99),
                }),
                breaker_event(BreakerMessage::Failed {
                    attempt: BreakerAttempt(0),
                }),
                breaker_admit(),
                breaker_event(BreakerMessage::Failed {
                    attempt: BreakerAttempt(1),
                }),
                breaker_admit(),
                breaker_elapsed(9, 0),
                breaker_elapsed(8, 9),
                breaker_elapsed(8, 0),
                breaker_admit(),
                breaker_admit(),
                breaker_event(BreakerMessage::Failed {
                    attempt: BreakerAttempt(2),
                }),
                breaker_elapsed(8, 0),
                breaker_elapsed(8, 1),
                breaker_admit(),
                breaker_event(BreakerMessage::Succeeded {
                    attempt: BreakerAttempt(3),
                }),
                breaker_admit(),
                breaker_event(BreakerMessage::Succeeded {
                    attempt: BreakerAttempt(4),
                }),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(
        actions.len(),
        22,
        "initialization plus twenty-one exact turns"
    );
    for index in [1, 2, 5, 10, 11, 12, 16, 17] {
        assert!(actions[index].sends.replies.is_empty());
        assert!(actions[index].sends.schedules.as_slice().is_empty());
    }
    assert!(matches!(
        actions[3].sends.replies[0].message,
        BreakerOutcome::Admitted {
            attempt: BreakerAttempt(0)
        }
    ));
    assert!(matches!(
        actions[4].sends.replies[0].message,
        BreakerOutcome::Rejected(BreakerRejection::Busy)
    ));
    assert!(matches!(
        actions[6].sends.replies[0].message,
        BreakerOutcome::FailureRecorded {
            attempt: BreakerAttempt(0),
            consecutive_failures: 1,
        }
    ));
    assert!(matches!(
        actions[7].sends.replies[0].message,
        BreakerOutcome::Admitted {
            attempt: BreakerAttempt(1)
        }
    ));
    assert!(matches!(
        actions[8].sends.replies[0].message,
        BreakerOutcome::Opened {
            attempt: BreakerAttempt(1),
            generation: TimerGeneration(0),
        }
    ));
    assert_eq!(
        actions[8].sends.schedules.as_slice()[0].generation,
        TimerGeneration(0)
    );
    assert!(matches!(
        actions[9].sends.replies[0].message,
        BreakerOutcome::Rejected(BreakerRejection::Open {
            generation: TimerGeneration(0)
        })
    ));
    assert!(matches!(
        actions[13].sends.replies[0].message,
        BreakerOutcome::ProbeAdmitted {
            attempt: BreakerAttempt(2)
        }
    ));
    assert!(matches!(
        actions[14].sends.replies[0].message,
        BreakerOutcome::Rejected(BreakerRejection::Busy)
    ));
    assert!(matches!(
        actions[15].sends.replies[0].message,
        BreakerOutcome::Opened {
            attempt: BreakerAttempt(2),
            generation: TimerGeneration(1),
        }
    ));
    assert_eq!(
        actions[15].sends.schedules.as_slice()[0].generation,
        TimerGeneration(1)
    );
    assert!(matches!(
        actions[18].sends.replies[0].message,
        BreakerOutcome::ProbeAdmitted {
            attempt: BreakerAttempt(3)
        }
    ));
    assert!(matches!(
        actions[19].sends.replies[0].message,
        BreakerOutcome::Succeeded {
            attempt: BreakerAttempt(3)
        }
    ));
    assert!(matches!(
        actions[20].sends.replies[0].message,
        BreakerOutcome::Admitted {
            attempt: BreakerAttempt(4)
        }
    ));
    assert!(matches!(
        actions[21].sends.replies[0].message,
        BreakerOutcome::Succeeded {
            attempt: BreakerAttempt(4)
        }
    ));
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_rate_limiter_preserves_move_only_admission_rejection_and_saturation() {
    assert!(matches!(
        RateLimiterSubject::new(token_count(5), 6),
        Err(RateLimiterConfigError::InitialExceedsCapacity {
            capacity,
            initial: 6,
        }) if capacity == token_count(5)
    ));

    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        RateLimiterSubject::new(token_count(5), 3).unwrap(),
        CaptureEnvironment {
            events: [
                acquire_tokens(2, 10),
                acquire_tokens(2, 20),
                acquire_tokens(6, 30),
                refill_tokens(u64::MAX),
                acquire_tokens(5, 40),
                refill_tokens(2),
                acquire_tokens(2, 50),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 8, "initialization plus seven exact turns");
    assert_eq!(*actions[1].sends.deliveries[0].message, 10);
    assert!(matches!(
        actions[1].sends.outcomes[0].message,
        RateLimiterOutcome::Admitted { remaining: 1 }
    ));
    assert!(matches!(
        &actions[2].sends.outcomes[0].message,
        RateLimiterOutcome::Rejected {
            value,
            reason: RateLimitRejection::InsufficientTokens,
        } if **value == 20
    ));
    assert!(matches!(
        &actions[3].sends.outcomes[0].message,
        RateLimiterOutcome::Rejected {
            value,
            reason: RateLimitRejection::ExceedsCapacity,
        } if **value == 30
    ));
    assert!(actions[4].sends.deliveries.is_empty());
    assert!(actions[4].sends.outcomes.is_empty());
    assert_eq!(*actions[5].sends.deliveries[0].message, 40);
    assert!(matches!(
        actions[5].sends.outcomes[0].message,
        RateLimiterOutcome::Admitted { remaining: 0 }
    ));
    assert!(actions[6].sends.deliveries.is_empty());
    assert!(actions[6].sends.outcomes.is_empty());
    assert_eq!(*actions[7].sends.deliveries[0].message, 50);
    assert!(matches!(
        actions[7].sends.outcomes[0].message,
        RateLimiterOutcome::Admitted { remaining: 0 }
    ));
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the complete health report and controlled-failure evidence stays auditable together"
)]
async fn inferred_health_preserves_reports_tombstones_versions_and_controlled_failures() {
    let (result, actions, retirements) = drive_health([
        query_health(),
        observe_health(1, 1, HealthStatus::Healthy),
        observe_health(1, 1, HealthStatus::Healthy),
        observe_health(2, 1, HealthStatus::Unhealthy),
        query_health(),
        health_event(HealthMessage::Remove {
            component: 2,
            version: ObservationVersion(2),
        }),
        query_health(),
        observe_health(2, 3, HealthStatus::Degraded),
        query_health(),
    ])
    .await;
    assert!(matches!(result, Ok(Completion::Exhausted)));
    {
        let actions = actions.lock().unwrap();
        assert_eq!(actions.len(), 10);
        assert_eq!(actions[1].sends[0].message.overall(), HealthStatus::Healthy);
        assert!(actions[1].sends[0].message.components.is_empty());
        assert_eq!(
            actions[5].sends[0]
                .message
                .components
                .iter()
                .map(|component| (component.component, component.status))
                .collect::<Vec<_>>(),
            [(1, HealthStatus::Healthy), (2, HealthStatus::Unhealthy)]
        );
        assert_eq!(
            actions[5].sends[0].message.overall(),
            HealthStatus::Unhealthy
        );
        assert_eq!(
            actions[7].sends[0]
                .message
                .components
                .iter()
                .map(|component| component.component)
                .collect::<Vec<_>>(),
            [1]
        );
        assert_eq!(
            actions[9].sends[0]
                .message
                .components
                .iter()
                .map(|component| (component.component, component.version, component.status))
                .collect::<Vec<_>>(),
            [
                (1, ObservationVersion(1), HealthStatus::Healthy),
                (2, ObservationVersion(3), HealthStatus::Degraded),
            ]
        );
        assert_eq!(
            actions[9].sends[0].message.overall(),
            HealthStatus::Degraded
        );
    }
    assert_eq!(retirements, 1);

    let (stale, actions, retirements) = drive_health([
        observe_health(1, 3, HealthStatus::Degraded),
        observe_health(1, 2, HealthStatus::Healthy),
    ])
    .await;
    assert!(matches!(
        stale,
        Err(DriverError::Behavior(HealthError::Stale {
            component: 1,
            observed: ObservationVersion(2),
            current: ObservationVersion(3),
        }))
    ));
    assert_eq!(
        actions.lock().unwrap().len(),
        2,
        "only the factual prefix committed"
    );
    assert_eq!(retirements, 1);

    let (conflict, actions, retirements) = drive_health([
        health_event(HealthMessage::Remove {
            component: 2,
            version: ObservationVersion(4),
        }),
        observe_health(2, 4, HealthStatus::Healthy),
    ])
    .await;
    assert!(matches!(
        conflict,
        Err(DriverError::Behavior(HealthError::ConflictingVersion {
            component: 2,
            version: ObservationVersion(4),
        }))
    ));
    assert_eq!(
        actions.lock().unwrap().len(),
        2,
        "the tombstone prefix committed"
    );
    assert_eq!(retirements, 1);
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the complete readiness reports and controlled failures stay auditable together"
)]
async fn inferred_readiness_preserves_fixed_membership_versions_and_complete_reports() {
    let (result, actions, retirements) = drive_readiness(
        [1, 1, 2],
        [
            query_readiness(),
            observe_readiness(1, 1, ReadinessStatus::Ready),
            observe_readiness(1, 1, ReadinessStatus::Ready),
            observe_readiness(2, 1, ReadinessStatus::NotReady),
            query_readiness(),
            observe_readiness(2, 2, ReadinessStatus::Ready),
            query_readiness(),
        ],
    )
    .await;
    assert!(matches!(result, Ok(Completion::Exhausted)));
    {
        let actions = actions.lock().unwrap();
        assert_eq!(actions.len(), 8);
        assert!(!actions[1].sends[0].message.ready());
        assert_eq!(actions[1].sends[0].message.dependencies.len(), 2);
        assert!(!actions[5].sends[0].message.ready());
        assert_eq!(
            actions[5].sends[0]
                .message
                .dependencies
                .iter()
                .map(|state| state.dependency)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert!(actions[7].sends[0].message.ready());
        assert_eq!(
            actions[7].sends[0]
                .message
                .dependencies
                .iter()
                .map(|state| state.dependency)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }
    assert_eq!(retirements, 1);

    let (empty, actions, retirements) = drive_readiness([], [query_readiness()]).await;
    assert!(matches!(empty, Ok(Completion::Exhausted)));
    assert!(actions.lock().unwrap()[1].sends[0].message.ready());
    assert_eq!(retirements, 1);

    let (stale, actions, retirements) = drive_readiness(
        [1],
        [
            observe_readiness(1, 2, ReadinessStatus::Ready),
            observe_readiness(1, 1, ReadinessStatus::NotReady),
        ],
    )
    .await;
    assert!(matches!(
        stale,
        Err(DriverError::Behavior(ReadinessError::Stale {
            dependency: 1,
            observed: ObservationVersion(1),
            current: ObservationVersion(2),
        }))
    ));
    assert_eq!(actions.lock().unwrap().len(), 2);
    assert_eq!(retirements, 1);

    let (conflict, actions, retirements) = drive_readiness(
        [1],
        [
            observe_readiness(1, 2, ReadinessStatus::Ready),
            observe_readiness(1, 2, ReadinessStatus::NotReady),
        ],
    )
    .await;
    assert!(matches!(
        conflict,
        Err(DriverError::Behavior(ReadinessError::ConflictingVersion {
            dependency: 1,
            version: ObservationVersion(2),
        }))
    ));
    assert_eq!(actions.lock().unwrap().len(), 2);
    assert_eq!(retirements, 1);

    let (unknown, actions, retirements) =
        drive_readiness([1], [observe_readiness(9, 1, ReadinessStatus::Ready)]).await;
    assert!(matches!(
        unknown,
        Err(DriverError::Behavior(ReadinessError::UnknownDependency {
            dependency: 9
        }))
    ));
    assert_eq!(actions.lock().unwrap().len(), 1);
    assert_eq!(retirements, 1);
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "workflow validation and every public lifecycle branch stay in one conformance oracle"
)]
async fn inferred_workflow_preserves_validation_activation_order_and_terminal_facts() {
    assert!(matches!(
        WorkflowSubject::new(WorkflowDefinition {
            steps: vec![],
            dependencies: vec![],
        }),
        Err(WorkflowConfigError::Empty)
    ));
    assert!(matches!(
        WorkflowSubject::new(WorkflowDefinition {
            steps: vec!["a", "a"],
            dependencies: vec![],
        }),
        Err(WorkflowConfigError::DuplicateStep { step: "a" })
    ));
    assert!(matches!(
        WorkflowSubject::new(WorkflowDefinition {
            steps: vec!["a"],
            dependencies: vec![("missing", "a")],
        }),
        Err(WorkflowConfigError::UnknownPrerequisite { step: "missing" })
    ));
    assert!(matches!(
        WorkflowSubject::new(WorkflowDefinition {
            steps: vec!["a"],
            dependencies: vec![("a", "missing")],
        }),
        Err(WorkflowConfigError::UnknownDependent { step: "missing" })
    ));
    assert!(matches!(
        WorkflowSubject::new(WorkflowDefinition {
            steps: vec!["a"],
            dependencies: vec![("a", "a")],
        }),
        Err(WorkflowConfigError::SelfDependency { step: "a" })
    ));
    assert!(matches!(
        WorkflowSubject::new(WorkflowDefinition {
            steps: vec!["a", "b"],
            dependencies: vec![("a", "b"), ("b", "a")],
        }),
        Err(WorkflowConfigError::Cycle)
    ));

    let (success, actions, retirements) = drive_workflow([
        start_workflow(),
        workflow_event(WorkflowMessage::Complete { step: "root" }),
        workflow_event(WorkflowMessage::Complete { step: "left" }),
        workflow_event(WorkflowMessage::Complete { step: "right" }),
        workflow_event(WorkflowMessage::Complete { step: "join" }),
        workflow_event(WorkflowMessage::Complete { step: "join" }),
        cancel_workflow(),
    ])
    .await;
    assert!(matches!(success, Ok(Completion::Exhausted)));
    {
        let actions = actions.lock().unwrap();
        assert_eq!(actions.len(), 8);
        assert!(matches!(
            &actions[1].sends[0].message,
            WorkflowOutcome::Started { activated } if activated == &["root"]
        ));
        assert!(matches!(
            &actions[2].sends[0].message,
            WorkflowOutcome::Advanced { completed: "root", activated }
                if activated == &["left", "right"]
        ));
        assert!(matches!(
            &actions[3].sends[0].message,
            WorkflowOutcome::Advanced { completed: "left", activated }
                if activated.is_empty()
        ));
        assert!(matches!(
            &actions[4].sends[0].message,
            WorkflowOutcome::Advanced { completed: "right", activated }
                if activated == &["join"]
        ));
        assert!(matches!(
            actions[5].sends[0].message,
            WorkflowOutcome::Succeeded { completed: "join" }
        ));
        assert!(actions[6].sends.is_empty());
        assert!(matches!(
            actions[7].sends[0].message,
            WorkflowOutcome::Rejected(WorkflowRejection::Terminal { step: None })
        ));
    }
    assert_eq!(retirements, 1);

    let (failed, actions, retirements) = drive_workflow([
        start_workflow(),
        start_workflow(),
        workflow_event(WorkflowMessage::Complete { step: "join" }),
        workflow_event(WorkflowMessage::Fail { step: "missing" }),
        workflow_event(WorkflowMessage::Complete { step: "root" }),
        workflow_event(WorkflowMessage::Complete { step: "root" }),
        workflow_event(WorkflowMessage::Fail { step: "left" }),
        cancel_workflow(),
    ])
    .await;
    assert!(matches!(failed, Ok(Completion::Exhausted)));
    {
        let actions = actions.lock().unwrap();
        assert!(matches!(
            actions[2].sends[0].message,
            WorkflowOutcome::Rejected(WorkflowRejection::AlreadyStarted)
        ));
        assert!(matches!(
            actions[3].sends[0].message,
            WorkflowOutcome::Rejected(WorkflowRejection::Blocked { step: "join" })
        ));
        assert!(matches!(
            actions[4].sends[0].message,
            WorkflowOutcome::Rejected(WorkflowRejection::UnknownStep { step: "missing" })
        ));
        assert!(matches!(
            actions[6].sends[0].message,
            WorkflowOutcome::Rejected(WorkflowRejection::AlreadyCompleted { step: "root" })
        ));
        assert!(matches!(
            actions[7].sends[0].message,
            WorkflowOutcome::Failed { step: "left" }
        ));
        assert!(matches!(
            actions[8].sends[0].message,
            WorkflowOutcome::Rejected(WorkflowRejection::Terminal { step: None })
        ));
    }
    assert_eq!(retirements, 1);

    let (cancelled, actions, retirements) =
        drive_workflow([cancel_workflow(), start_workflow(), cancel_workflow()]).await;
    assert!(matches!(cancelled, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert!(matches!(
        actions[1].sends[0].message,
        WorkflowOutcome::Cancelled
    ));
    assert!(matches!(
        actions[2].sends[0].message,
        WorkflowOutcome::Rejected(WorkflowRejection::AlreadyStarted)
    ));
    assert!(matches!(
        actions[3].sends[0].message,
        WorkflowOutcome::Rejected(WorkflowRejection::Terminal { step: None })
    ));
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn inferred_round_robin_router_preserves_membership_cursor_and_owned_empty_rejection() {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        RoundRobinSubject::new(
            vec![
                router_recipient(1),
                router_recipient(2),
                router_recipient(1),
            ],
            RoundRobin::default(),
        ),
        CaptureEnvironment {
            events: [
                round_robin_event(RouterMessage::Route(RoutedValue(10))),
                round_robin_event(RouterMessage::Route(RoutedValue(20))),
                round_robin_event(RouterMessage::Remove(router_recipient(1))),
                round_robin_event(RouterMessage::Route(RoutedValue(30))),
                round_robin_event(RouterMessage::Add(router_recipient(3))),
                round_robin_event(RouterMessage::Add(router_recipient(3))),
                round_robin_event(RouterMessage::Route(RoutedValue(40))),
                round_robin_event(RouterMessage::Route(RoutedValue(50))),
                round_robin_event(RouterMessage::Remove(router_recipient(99))),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;
    assert!(matches!(result, Ok(Completion::Exhausted)));
    {
        let actions = actions.lock().unwrap();
        assert_eq!(actions.len(), 10);
        for (index, recipient, value) in [
            (1, router_recipient(1), 10),
            (2, router_recipient(2), 20),
            (4, router_recipient(2), 30),
            (7, router_recipient(2), 40),
            (8, router_recipient(3), 50),
        ] {
            assert!(actions[index].sends[0].to == recipient);
            assert_eq!(actions[index].sends[0].message, RoutedValue(value));
        }
    }
    assert_eq!(retirements.load(Ordering::Relaxed), 1);

    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let rejected = direct(
        RoundRobinSubject::new(Vec::new(), RoundRobin::default()),
        CaptureEnvironment {
            events: [round_robin_event(RouterMessage::Route(RoutedValue(99)))]
                .into_iter()
                .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;
    assert!(matches!(
        rejected,
        Err(DriverError::Behavior(RouterError::NoEligibleRecipients(
            RoutedValue(99)
        )))
    ));
    assert_eq!(actions.lock().unwrap().len(), 1);
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_broadcast_router_preserves_deduplicated_membership_order() {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        BroadcastSubject::new(
            vec![
                router_recipient(1),
                router_recipient(2),
                router_recipient(1),
            ],
            Broadcast,
        ),
        CaptureEnvironment {
            events: [
                broadcast_event(RouterMessage::Route(RoutedValue(10))),
                broadcast_event(RouterMessage::Remove(router_recipient(1))),
                broadcast_event(RouterMessage::Add(router_recipient(3))),
                broadcast_event(RouterMessage::Add(router_recipient(3))),
                broadcast_event(RouterMessage::Route(RoutedValue(20))),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 6);
    assert_eq!(actions[1].sends.len(), 2);
    assert!(actions[1].sends[0].to == router_recipient(1));
    assert!(actions[1].sends[1].to == router_recipient(2));
    assert_eq!(actions[1].sends[0].message, RoutedValue(10));
    assert_eq!(actions[1].sends[1].message, RoutedValue(10));
    assert_eq!(actions[5].sends.len(), 2);
    assert!(actions[5].sends[0].to == router_recipient(2));
    assert!(actions[5].sends[1].to == router_recipient(3));
    assert_eq!(actions[5].sends[0].message, RoutedValue(20));
    assert_eq!(actions[5].sends[1].message, RoutedValue(20));
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "least-loaded selection and every typed evidence rejection stay in one oracle"
)]
async fn inferred_least_loaded_router_preserves_versioned_evidence_and_selection() {
    let (result, actions, retirements) = drive_least_loaded([
        observe_load(1, 0, 3),
        observe_load(1, 0, 3),
        observe_load(2, 0, 3),
        least_loaded_event(RouterMessage::Route(RoutedValue(10))),
        observe_load(2, 1, 1),
        least_loaded_event(RouterMessage::Route(RoutedValue(20))),
        least_loaded_event(RouterMessage::Remove(router_recipient(2))),
        least_loaded_event(RouterMessage::Route(RoutedValue(30))),
        least_loaded_event(RouterMessage::Add(router_recipient(3))),
        observe_load(3, 0, 0),
        least_loaded_event(RouterMessage::Route(RoutedValue(40))),
    ])
    .await;
    assert!(matches!(result, Ok(Completion::Exhausted)));
    {
        let actions = actions.lock().unwrap();
        assert_eq!(actions.len(), 12);
        for index in [1, 2, 3, 5, 7, 9, 10] {
            assert!(actions[index].sends.is_empty());
        }
        for (index, recipient, value) in [
            (4, router_recipient(1), 10),
            (6, router_recipient(2), 20),
            (8, router_recipient(1), 30),
            (11, router_recipient(3), 40),
        ] {
            assert!(actions[index].sends[0].to == recipient);
            assert_eq!(actions[index].sends[0].message, RoutedValue(value));
        }
    }
    assert_eq!(retirements, 1);

    let (no_evidence, actions, retirements) =
        drive_least_loaded([least_loaded_event(RouterMessage::Route(RoutedValue(50)))]).await;
    assert!(matches!(
        no_evidence,
        Err(DriverError::Behavior(RouterError::NoEligibleRecipients(
            RoutedValue(50)
        )))
    ));
    assert_eq!(actions.lock().unwrap().len(), 1);
    assert_eq!(retirements, 1);

    let (stale, actions, retirements) =
        drive_least_loaded([observe_load(1, 2, 4), observe_load(1, 1, 0)]).await;
    assert!(matches!(
        stale,
        Err(DriverError::Behavior(RouterError::Policy(
            LeastLoadedError::Stale(_)
        )))
    ));
    assert_eq!(actions.lock().unwrap().len(), 2);
    assert_eq!(retirements, 1);

    let (conflict, actions, retirements) =
        drive_least_loaded([observe_load(1, 2, 4), observe_load(1, 2, 5)]).await;
    assert!(matches!(
        conflict,
        Err(DriverError::Behavior(RouterError::Policy(
            LeastLoadedError::ConflictingVersion(_)
        )))
    ));
    assert_eq!(actions.lock().unwrap().len(), 2);
    assert_eq!(retirements, 1);

    let (unknown, actions, retirements) = drive_least_loaded([observe_load(9, 0, 0)]).await;
    assert!(matches!(
        unknown,
        Err(DriverError::Behavior(RouterError::Policy(
            LeastLoadedError::UnknownRecipient(_)
        )))
    ));
    assert_eq!(actions.lock().unwrap().len(), 1);
    assert_eq!(retirements, 1);
}

#[tokio::test]
async fn inferred_consistent_hash_router_preserves_determinism_and_minimal_remapping() {
    let mut events = vec![
        consistent_event(RouterMessage::Observe(member_token(1, 0, 11))),
        consistent_event(RouterMessage::Observe(member_token(2, 0, 22))),
        consistent_event(RouterMessage::Observe(member_token(3, 0, 33))),
    ];
    events.extend(
        (0..32).map(|key| {
            consistent_event(RouterMessage::Route(KeyedRoutedValue { key, value: key }))
        }),
    );
    events.push(consistent_event(RouterMessage::Remove(keyed_recipient(2))));
    events.extend((0..32).map(|key| {
        consistent_event(RouterMessage::Route(KeyedRoutedValue {
            key,
            value: key + 100,
        }))
    }));

    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        ConsistentHashSubject::new(
            vec![keyed_recipient(1), keyed_recipient(2), keyed_recipient(3)],
            ConsistentHash::new(NonZeroU16::new(8).unwrap(), identity_hash),
        ),
        CaptureEnvironment {
            events: events.into_iter().collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 69);
    for key in 0..32_usize {
        let before = &actions[4 + key].sends[0];
        let after = &actions[37 + key].sends[0];
        assert_eq!(before.message.key, u64::try_from(key).unwrap());
        assert_eq!(after.message.value, u64::try_from(key).unwrap() + 100);
        if before.to != keyed_recipient(2) {
            assert!(after.to == before.to, "unaffected key {key} moved");
        }
    }
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_rendezvous_hash_router_preserves_determinism_remapping_and_typed_conflict() {
    let mut events = vec![
        rendezvous_event(RouterMessage::Observe(member_token(1, 0, 11))),
        rendezvous_event(RouterMessage::Observe(member_token(2, 0, 22))),
        rendezvous_event(RouterMessage::Observe(member_token(3, 0, 33))),
    ];
    events.extend(
        (0..32).map(|key| {
            rendezvous_event(RouterMessage::Route(KeyedRoutedValue { key, value: key }))
        }),
    );
    events.push(rendezvous_event(RouterMessage::Remove(keyed_recipient(2))));
    events.extend((0..32).map(|key| {
        rendezvous_event(RouterMessage::Route(KeyedRoutedValue {
            key,
            value: key + 100,
        }))
    }));

    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        RendezvousHashSubject::new(
            vec![keyed_recipient(1), keyed_recipient(2), keyed_recipient(3)],
            RendezvousHash::new(identity_hash),
        ),
        CaptureEnvironment {
            events: events.into_iter().collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;
    assert!(matches!(result, Ok(Completion::Exhausted)));
    {
        let actions = actions.lock().unwrap();
        assert_eq!(actions.len(), 69);
        for key in 0..32_usize {
            let before = &actions[4 + key].sends[0];
            let after = &actions[37 + key].sends[0];
            assert_eq!(before.message.key, u64::try_from(key).unwrap());
            assert_eq!(after.message.value, u64::try_from(key).unwrap() + 100);
            if before.to != keyed_recipient(2) {
                assert!(after.to == before.to, "unaffected key {key} moved");
            }
        }
    }
    assert_eq!(retirements.load(Ordering::Relaxed), 1);

    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let conflict = direct(
        RendezvousHashSubject::new(vec![keyed_recipient(1)], RendezvousHash::new(identity_hash)),
        CaptureEnvironment {
            events: [
                rendezvous_event(RouterMessage::Observe(member_token(1, 0, 11))),
                rendezvous_event(RouterMessage::Observe(member_token(1, 0, 99))),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;
    assert!(matches!(
        conflict,
        Err(DriverError::Behavior(RouterError::Policy(
            HashPolicyError::ConflictingVersion(_)
        )))
    ));
    assert_eq!(actions.lock().unwrap().len(), 2);
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the proxy installation, replacement, rejection, and retry transcript is one lifecycle"
)]
async fn inferred_proxy_forwards_complete_creation_and_incarnation_actions_without_interpretation()
{
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        ProxySubject::new(ProxyChild::new(0)),
        CaptureEnvironment {
            events: [
                proxy_command(ProxyCommand::Forward(Box::new(1))),
                ProxyEvent::CreationResolved(CreationResolved::birth(1)),
                ProxyEvent::CreationResolved(CreationResolved::birth(0)),
                proxy_command(ProxyCommand::Forward(Box::new(10))),
                proxy_command(ProxyCommand::Replace(ProxyChild::new(1))),
                proxy_stopped(0),
                proxy_command(ProxyCommand::Forward(Box::new(11))),
                ProxyEvent::CreationResolved(CreationResolved::replacement_incarnation(1, 0)),
                proxy_command(ProxyCommand::Forward(Box::new(20))),
                proxy_stopped(0),
                proxy_command(ProxyCommand::Replace(ProxyChild::new(2))),
                proxy_stopped(1),
                ProxyEvent::CreationResolved(CreationResolved::rejected(
                    2,
                    CreationKind::ReplacementIncarnation { replaces: 1 },
                    CreationRejection::EnvironmentFailed,
                )),
                proxy_command(ProxyCommand::Forward(Box::new(30))),
                proxy_command(ProxyCommand::Replace(ProxyChild::new(3))),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 16);
    assert_eq!(actions[0].creates[0].nonce, 0);
    assert_eq!(actions[0].creates[0].kind, CreationKind::Birth);
    assert_eq!(*actions[0].creates[0].child.marker, 0);
    assert_eq!(actions[0].sends.child_observations.as_slice()[0].nonce, 0);
    assert_eq!(
        actions[0].sends.creation_observations.as_slice()[0].nonce,
        0
    );
    assert!(actions[1].sends.deliveries.is_empty());
    assert!(actions[2].sends.creation_reports.as_slice().is_empty());
    assert_eq!(
        actions[3].sends.creation_reports.as_slice()[0].result,
        Ok(())
    );
    assert!(actions[4].sends.deliveries[0].to == Recipient::child(0));
    assert_eq!(*actions[4].sends.deliveries[0].message, 10);
    assert!(actions[5].creates.is_empty());
    assert_eq!(actions[6].creates[0].nonce, 1);
    assert_eq!(
        actions[6].creates[0].kind,
        CreationKind::ReplacementIncarnation { replaces: 0 }
    );
    assert_eq!(*actions[6].creates[0].child.marker, 1);
    assert_eq!(actions[6].sends.stopped_reports.as_slice()[0].worker, 0);
    assert!(actions[7].sends.deliveries.is_empty());
    assert_eq!(
        actions[8].sends.creation_reports.as_slice()[0].result,
        Ok(())
    );
    assert!(actions[9].sends.deliveries[0].to == Recipient::child(1));
    assert_eq!(*actions[9].sends.deliveries[0].message, 20);
    assert!(actions[10].sends.stopped_reports.as_slice().is_empty());
    assert!(actions[11].creates.is_empty());
    assert_eq!(actions[12].creates[0].nonce, 2);
    assert_eq!(*actions[12].creates[0].child.marker, 2);
    assert_eq!(
        actions[13].sends.creation_reports.as_slice()[0].result,
        Err(CreationRejection::EnvironmentFailed)
    );
    assert!(actions[14].sends.deliveries.is_empty());
    assert_eq!(actions[15].creates[0].nonce, 3);
    assert_eq!(
        actions[15].creates[0].kind,
        CreationKind::ReplacementIncarnation { replaces: 1 }
    );
    assert_eq!(*actions[15].creates[0].child.marker, 3);
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_supervisor_forwards_proxy_births_and_typed_replacement_commands() {
    let behavior = SupervisorSubject::new(
        SupervisorInner,
        ChildTopology::indexed(supervisor_nonce, 2, supervisor_child),
        RestartConfiguration::new(
            Strategy::OneForOne,
            RestartPolicy::Permanent,
            2,
            Duration::MAX,
        ),
    )
    .unwrap();
    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        behavior,
        CaptureEnvironment {
            events: [
                SupervisionEvent::WorkerStopped(WorkerStopped {
                    proxy: 0,
                    worker: 0,
                    outcome: Err(Crash::Failed),
                    at: Instant::now(),
                }),
                SupervisionEvent::Behavior(SupervisorInnerEvent::User(User::new(MailAddr(7), 99))),
            ]
            .into_iter()
            .collect(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 3);
    assert_eq!(actions[0].creates.len(), 2);
    assert_eq!(actions[0].creates[0].nonce, 0);
    assert_eq!(actions[0].creates[1].nonce, 1);
    assert!(
        actions[0]
            .creates
            .iter()
            .all(|create| create.kind == CreationKind::Birth)
    );
    assert_eq!(actions[0].sends.child_observations.as_slice().len(), 2);
    assert_eq!(actions[0].sends.child_observations.as_slice()[0].nonce, 0);
    assert_eq!(actions[0].sends.child_observations.as_slice()[1].nonce, 1);
    assert!(actions[1].creates.is_empty());
    assert_eq!(actions[1].sends.replacement_commands.len(), 1);
    assert!(actions[1].sends.replacement_commands[0].to == Recipient::child(0));
    let ProxyCommand::Replace(child) = &actions[1].sends.replacement_commands[0].message else {
        panic!("one-for-one restart must emit a replacement command");
    };
    assert_eq!(*child.marker, 0);
    assert!(actions[2].sends.behavior.is_empty());
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_worker_pool_forwards_validated_supervised_worker_births() {
    assert!(matches!(
        WorkerPoolSubject::new(
            ChildTopology::new(Vec::<u64>::new(), pool_worker),
            pool_configuration(),
        ),
        Err(PoolConfigError::NoWorkers)
    ));
    assert!(matches!(
        WorkerPoolSubject::new(
            ChildTopology::new([0, 0], pool_worker),
            pool_configuration(),
        ),
        Err(PoolConfigError::DuplicateWorker(0))
    ));

    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        WorkerPoolSubject::new(
            ChildTopology::indexed(supervisor_nonce, 2, pool_worker),
            pool_configuration(),
        )
        .unwrap(),
        CaptureEnvironment {
            events: VecDeque::new(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].creates.len(), 2);
    assert_eq!(actions[0].creates[0].nonce, 0);
    assert_eq!(actions[0].creates[1].nonce, 1);
    assert!(
        actions[0]
            .creates
            .iter()
            .all(|create| create.kind == CreationKind::Birth)
    );
    assert_eq!(actions[0].sends.child_observations.as_slice().len(), 2);
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn inferred_keyed_worker_pool_forwards_validated_supervised_worker_births() {
    assert!(matches!(
        KeyedWorkerPoolSubject::new(
            ChildTopology::new(Vec::<u64>::new(), pool_worker),
            pool_configuration(),
            select_pool_worker as fn(&u8) -> u64,
        ),
        Err(PoolConfigError::NoWorkers)
    ));

    let actions = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));
    let result = direct(
        KeyedWorkerPoolSubject::new(
            ChildTopology::indexed(supervisor_nonce, 2, pool_worker),
            pool_configuration(),
            select_pool_worker as fn(&u8) -> u64,
        )
        .unwrap(),
        CaptureEnvironment {
            events: VecDeque::new(),
            actions: Arc::clone(&actions),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    let actions = actions.lock().unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].creates.len(), 2);
    assert_eq!(actions[0].creates[0].nonce, 0);
    assert_eq!(actions[0].creates[1].nonce, 1);
    assert_eq!(actions[0].sends.child_observations.as_slice().len(), 2);
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the typed lease lifecycle transcript and every factual outcome stay together"
)]
async fn inferred_lease_preserves_typed_outcomes_and_generation_order_until_source_exhaustion() {
    let behavior = Lease::<MailAddr, u8, LeaseReply>::new(TimerId(40));
    let reply = Recipient::<LeaseReply>::global(MailAddr(9));
    let duration = Duration::from_secs(1);
    let events = [
        LeaseMessage::Acquire {
            holder: 1,
            duration,
            reply_to: reply,
        },
        LeaseMessage::Acquire {
            holder: 2,
            duration,
            reply_to: reply,
        },
        LeaseMessage::Renew {
            holder: 2,
            generation: TimerGeneration(0),
            duration,
            reply_to: reply,
        },
        LeaseMessage::Renew {
            holder: 1,
            generation: TimerGeneration(9),
            duration,
            reply_to: reply,
        },
        LeaseMessage::Renew {
            holder: 1,
            generation: TimerGeneration(0),
            duration,
            reply_to: reply,
        },
        LeaseMessage::Elapsed(TimerElapsed::new(TimerId(40), TimerGeneration(0))),
        LeaseMessage::Release {
            holder: 1,
            generation: TimerGeneration(1),
            reply_to: reply,
        },
        LeaseMessage::Elapsed(TimerElapsed::new(TimerId(40), TimerGeneration(1))),
        LeaseMessage::Acquire {
            holder: 2,
            duration,
            reply_to: reply,
        },
        LeaseMessage::Elapsed(TimerElapsed::new(TimerId(40), TimerGeneration(2))),
    ]
    .map(|message| User::new(MailAddr(7), message));
    let outcomes = Arc::new(Mutex::new(Vec::new()));
    let schedules = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(AtomicUsize::new(0));

    let result = direct(
        behavior,
        LeaseEnvironment {
            events: events.into_iter().collect(),
            outcomes: Arc::clone(&outcomes),
            schedules: Arc::clone(&schedules),
            retirements: Arc::clone(&retirements),
        },
    )
    .run()
    .await;

    assert!(matches!(result, Ok(Completion::Exhausted)));
    assert_eq!(
        *outcomes.lock().unwrap(),
        [
            LeaseOutcome::Acquired {
                holder: 1,
                generation: TimerGeneration(0),
            },
            LeaseOutcome::Rejected(LeaseRejection::Occupied { requested: 2 }),
            LeaseOutcome::Rejected(LeaseRejection::WrongHolder { requested: 2 }),
            LeaseOutcome::Rejected(LeaseRejection::StaleGeneration {
                observed: TimerGeneration(9),
                current: TimerGeneration(0),
            }),
            LeaseOutcome::Renewed {
                holder: 1,
                generation: TimerGeneration(1),
            },
            LeaseOutcome::Released {
                holder: 1,
                generation: TimerGeneration(1),
            },
            LeaseOutcome::Acquired {
                holder: 2,
                generation: TimerGeneration(2),
            },
            LeaseOutcome::Expired {
                holder: 2,
                generation: TimerGeneration(2),
            },
        ]
    );
    assert_eq!(
        schedules
            .lock()
            .unwrap()
            .iter()
            .map(|schedule| schedule.generation)
            .collect::<Vec<_>>(),
        [TimerGeneration(0), TimerGeneration(1), TimerGeneration(2)]
    );
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
}
