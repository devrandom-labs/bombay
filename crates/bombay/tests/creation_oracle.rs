//! Creation-law oracles for the transactional child-birth contract.
//!
//! These tests pin the interpreter laws that were previously ignored
//! placeholders: installation happens only after a successful synchronous
//! init and every initialization effect, rollback leaves address and nonce
//! unbound, same-action observation converts creation failure into a typed
//! recoverable result, and unobserved failure keeps its exact runtime error.
//! Every oracle here was proven to fail under a deliberate production
//! inversion (recorded in evidence.json).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bombay::behavior::{
    Actions, Address, Behavior, Births, CreationRejection, CreationResolved, Delivery, Inner,
    MailAddr, Never, NoBirths, ObserveChild, ObserveCreation, Own, Proxy, Recipient, RestartPolicy,
    SendAlgebra, SendProduct, ServiceSends, Strategy, SupervisionEvent, Supervisor, User,
    stop_on_supervision_failure,
};
use bombay::{
    Actor, ActorRef, AddressRouter, DeliveryRouter, EndpointRegistry, IncarnationEndpoint,
    LifecycleEvent, LifecycleSink, LifecycleTransition, MailboxAnchor, MailboxConfig, RunError,
    RuntimeEffectError, System, SystemBirthError, TaskOutcome,
};
use bombay_address::RegistrationId;
use tokio::task::yield_now;
use tokio::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InitFailure;

/// A child whose initialization fold always fails.
struct Fragile;

impl Behavior for Fragile {
    type Addr = MailAddr;
    type Msg = Never;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = InitFailure;
    type Birth = NoBirths;

    fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
        Err(InitFailure)
    }

    fn transition(&mut self, event: Self::Event) -> bombay::behavior::BehaviorActed<Self> {
        match event.message {}
    }
}

/// A message for the worker protocol, distinct from the parent protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChildMsg(u8);

/// A child whose first initialization attempt fails and later attempts run.
struct Flaky {
    attempts: Arc<AtomicUsize>,
    received: Arc<Mutex<Vec<u8>>>,
}

impl Behavior for Flaky {
    type Addr = MailAddr;
    type Msg = ChildMsg;
    type Event = User<MailAddr, ChildMsg>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = InitFailure;
    type Birth = NoBirths;

    fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(InitFailure);
        }
        Ok(Actions::cont())
    }

    fn transition(&mut self, event: Self::Event) -> bombay::behavior::BehaviorActed<Self> {
        self.received
            .lock()
            .expect("child record lock")
            .push(event.message.0);
        Ok(Actions::cont())
    }
}

/// A behavior that records every user message it receives.
struct Recorder {
    received: Arc<Mutex<Vec<u8>>>,
}

impl Behavior for Recorder {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = User<MailAddr, u8>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, event: Self::Event) -> bombay::behavior::BehaviorActed<Self> {
        self.received
            .lock()
            .expect("recorder lock")
            .push(event.message);
        Ok(Actions::cont())
    }
}

type ParentEvent = SupervisionEvent<User<MailAddr, u8>>;
type ParentSends = SendProduct<Vec<Delivery<Recorder>>, ServiceSends<ObserveCreation<u64>>>;
type ParentDeliveryPath = Inner<Own>;
type RetrySends = SendProduct<
    SendProduct<Vec<Delivery<Recorder>>, Vec<Delivery<Flaky>>>,
    ServiceSends<ObserveCreation<u64>>,
>;
type RetryReplyPath = Inner<Inner<Own>>;
type RetryChildPath = Inner<Own>;

/// A parent that stages one fragile child and observes its creation.
struct ObservedParent {
    resolutions: Arc<Mutex<Vec<CreationResolved<u64>>>>,
}

impl Behavior for ObservedParent {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = ParentEvent;
    type Sends = ParentSends;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<Fragile>;

    fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
        let mut sends = ParentSends::empty();
        sends.send::<_, Own>(ObserveCreation::new(7));
        Ok(Actions {
            sends,
            creates: vec![bombay::behavior::Create::birth(7, Fragile)],
            become_: bombay::behavior::Step::Continue,
        })
    }

    fn transition(&mut self, event: Self::Event) -> bombay::behavior::BehaviorActed<Self> {
        match event {
            SupervisionEvent::CreationResolved(resolved) => {
                self.resolutions
                    .lock()
                    .expect("resolution lock")
                    .push(resolved);
            }
            SupervisionEvent::Inner(User { from, message }) => {
                let mut sends = ParentSends::empty();
                sends.send::<_, ParentDeliveryPath>(Delivery::new(
                    Recipient::global(from),
                    message + 1,
                ));
                return Ok(Actions {
                    sends,
                    creates: Vec::new(),
                    become_: bombay::behavior::Step::Continue,
                });
            }
            _ => {}
        }
        Ok(Actions::cont())
    }
}

/// A parent that stages one fragile child without observing the creation.
struct UnobservedParent;

impl Behavior for UnobservedParent {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = User<MailAddr, u8>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<Fragile>;

    fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
        Ok(Actions {
            sends: Vec::new(),
            creates: vec![bombay::behavior::Create::birth(7, Fragile)],
            become_: bombay::behavior::Step::Continue,
        })
    }

    fn transition(&mut self, _event: Self::Event) -> bombay::behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

/// A parent that retries the same nonce after an observed rejection.
struct RetryParent {
    flaky: Flaky,
    resolutions: Arc<Mutex<Vec<CreationResolved<u64>>>>,
}

impl Behavior for RetryParent {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = ParentEvent;
    type Sends = RetrySends;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<Flaky>;

    fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
        Ok(Self::attempt(Flaky {
            attempts: self.flaky.attempts.clone(),
            received: self.flaky.received.clone(),
        }))
    }

    fn transition(&mut self, event: Self::Event) -> bombay::behavior::BehaviorActed<Self> {
        match event {
            SupervisionEvent::CreationResolved(resolved) => {
                self.resolutions
                    .lock()
                    .expect("resolution lock")
                    .push(resolved);
                if resolved.result.is_err() {
                    return Ok(Self::attempt(Flaky {
                        attempts: self.flaky.attempts.clone(),
                        received: self.flaky.received.clone(),
                    }));
                }
                // The rebound child must be parent-routable.
                let mut sends = RetrySends::empty();
                sends.send::<_, RetryChildPath>(Delivery::new(Recipient::child(7), ChildMsg(42)));
                return Ok(Actions {
                    sends,
                    creates: Vec::new(),
                    become_: bombay::behavior::Step::Continue,
                });
            }
            SupervisionEvent::Inner(User { from, message }) => {
                let mut sends = RetrySends::empty();
                sends
                    .send::<_, RetryReplyPath>(Delivery::new(Recipient::global(from), message + 1));
                return Ok(Actions {
                    sends,
                    creates: Vec::new(),
                    become_: bombay::behavior::Step::Continue,
                });
            }
            _ => {}
        }
        Ok(Actions::cont())
    }
}

impl RetryParent {
    fn attempt(child: Flaky) -> Actions<MailAddr, Never, RetrySends, Births<Flaky>> {
        let mut sends = RetrySends::empty();
        sends.send::<_, Own>(ObserveCreation::new(7));
        Actions {
            sends,
            creates: vec![bombay::behavior::Create::birth(7, child)],
            become_: bombay::behavior::Step::Continue,
        }
    }
}

/// A child whose init stages a fragile grandchild without observation.
struct ParentOfFragile;

impl Behavior for ParentOfFragile {
    type Addr = MailAddr;
    type Msg = Never;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<Fragile>;

    fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
        Ok(Actions {
            sends: Vec::new(),
            creates: vec![bombay::behavior::Create::birth(3, Fragile)],
            become_: bombay::behavior::Step::Continue,
        })
    }

    fn transition(&mut self, event: Self::Event) -> bombay::behavior::BehaviorActed<Self> {
        match event.message {}
    }
}

/// A parent observing one child whose own init effects must fail.
struct ObservedGrandparent {
    resolutions: Arc<Mutex<Vec<CreationResolved<u64>>>>,
}

impl Behavior for ObservedGrandparent {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = ParentEvent;
    type Sends = ParentSends;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<ParentOfFragile>;

    fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
        let mut sends = ParentSends::empty();
        sends.send::<_, Own>(ObserveCreation::new(7));
        Ok(Actions {
            sends,
            creates: vec![bombay::behavior::Create::birth(7, ParentOfFragile)],
            become_: bombay::behavior::Step::Continue,
        })
    }

    fn transition(&mut self, event: Self::Event) -> bombay::behavior::BehaviorActed<Self> {
        if let SupervisionEvent::CreationResolved(resolved) = event {
            self.resolutions
                .lock()
                .expect("resolution lock")
                .push(resolved);
        }
        Ok(Actions::cont())
    }
}

/// A parent that pairs `ObserveChild` with the fragile observed creation.
struct PairingParent {
    resolutions: Arc<Mutex<Vec<CreationResolved<u64>>>>,
}

type PairingSends = SendProduct<
    SendProduct<Vec<Delivery<Recorder>>, ServiceSends<ObserveChild<u64>>>,
    ServiceSends<ObserveCreation<u64>>,
>;
type PairingDeliveryPath = Inner<Inner<Own>>;
type PairingChildObservationPath = Inner<Own>;

impl Behavior for PairingParent {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = ParentEvent;
    type Sends = PairingSends;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<Fragile>;

    fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
        let mut sends = PairingSends::empty();
        sends.send::<_, PairingChildObservationPath>(ObserveChild::new(7));
        sends.send::<_, Own>(ObserveCreation::new(7));
        Ok(Actions {
            sends,
            creates: vec![bombay::behavior::Create::birth(7, Fragile)],
            become_: bombay::behavior::Step::Continue,
        })
    }

    fn transition(&mut self, event: Self::Event) -> bombay::behavior::BehaviorActed<Self> {
        match event {
            SupervisionEvent::CreationResolved(resolved) => {
                self.resolutions
                    .lock()
                    .expect("resolution lock")
                    .push(resolved);
            }
            SupervisionEvent::Inner(User { from, message }) => {
                let mut sends = PairingSends::empty();
                sends.send::<_, PairingDeliveryPath>(Delivery::new(
                    Recipient::global(from),
                    message + 1,
                ));
                return Ok(Actions {
                    sends,
                    creates: Vec::new(),
                    become_: bombay::behavior::Step::Continue,
                });
            }
            _ => {}
        }
        Ok(Actions::cont())
    }
}

#[derive(Clone, Default)]
struct RecordingLifecycles(Arc<Mutex<Vec<LifecycleEvent<MailAddr, RegistrationId>>>>);

impl LifecycleSink<MailAddr, RegistrationId> for RecordingLifecycles {
    fn record(&self, event: LifecycleEvent<MailAddr, RegistrationId>) {
        self.0.lock().expect("lifecycle lock").push(event);
    }
}

type SupervisorEventEndpoint = ActorRef<MailAddr, MailboxAnchor<ParentEvent>>;
type UserEndpoint = ActorRef<MailAddr, MailboxAnchor<User<MailAddr, u8>>>;
type WorkerEndpoint = ActorRef<MailAddr, MailboxAnchor<User<MailAddr, ChildMsg>>>;
type NeverEndpoint = ActorRef<MailAddr, MailboxAnchor<User<MailAddr, Never>>>;

#[derive(Clone, Default)]
struct CreationRouter {
    supers: AddressRouter<MailAddr, IncarnationEndpoint<MailAddr, SupervisorEventEndpoint>>,
    users: AddressRouter<MailAddr, IncarnationEndpoint<MailAddr, UserEndpoint>>,
    children: AddressRouter<MailAddr, IncarnationEndpoint<MailAddr, WorkerEndpoint>>,
    nevers: AddressRouter<MailAddr, IncarnationEndpoint<MailAddr, NeverEndpoint>>,
}

macro_rules! registry {
    ($router:ident, $behavior:ty, $endpoint:ty) => {
        impl EndpointRegistry<$behavior, IncarnationEndpoint<MailAddr, $endpoint>>
            for CreationRouter
        {
            type Error = bombay::AddressInUse<MailAddr>;
            type Registration = <AddressRouter<
                MailAddr,
                IncarnationEndpoint<MailAddr, $endpoint>,
            > as EndpointRegistry<
                $behavior,
                IncarnationEndpoint<MailAddr, $endpoint>,
            >>::Registration;

            fn register(
                &self,
                address: MailAddr,
                endpoint: IncarnationEndpoint<MailAddr, $endpoint>,
            ) -> Result<Self::Registration, Self::Error> {
                <AddressRouter<MailAddr, IncarnationEndpoint<MailAddr, $endpoint>> as EndpointRegistry<
                    $behavior,
                    IncarnationEndpoint<MailAddr, $endpoint>,
                >>::register(&self.$router, address, endpoint)
            }
        }
    };
}

type UserDeliveryError =
    <AddressRouter<MailAddr, IncarnationEndpoint<MailAddr, UserEndpoint>> as DeliveryRouter<
        Recorder,
    >>::Error;

impl DeliveryRouter<Recorder> for CreationRouter {
    type Error = UserDeliveryError;

    async fn deliver(
        &self,
        from: MailAddr,
        delivery: Delivery<Recorder>,
    ) -> Result<(), Self::Error> {
        self.users.deliver(from, delivery).await
    }
}

type ChildDeliveryError =
    <AddressRouter<MailAddr, IncarnationEndpoint<MailAddr, WorkerEndpoint>> as DeliveryRouter<
        Flaky,
    >>::Error;

impl DeliveryRouter<Flaky> for CreationRouter {
    type Error = ChildDeliveryError;

    async fn deliver(&self, from: MailAddr, delivery: Delivery<Flaky>) -> Result<(), Self::Error> {
        self.children.deliver(from, delivery).await
    }
}

registry!(supers, ObservedParent, SupervisorEventEndpoint);
registry!(supers, RetryParent, SupervisorEventEndpoint);
registry!(supers, ObservedGrandparent, SupervisorEventEndpoint);
registry!(supers, PairingParent, SupervisorEventEndpoint);
registry!(users, Recorder, UserEndpoint);
registry!(users, UnobservedParent, UserEndpoint);
registry!(children, Flaky, WorkerEndpoint);
registry!(nevers, Fragile, NeverEndpoint);
registry!(nevers, ParentOfFragile, NeverEndpoint);

fn creation_system() -> System<CreationRouter> {
    System::new(MailboxConfig::bounded(8), CreationRouter::default())
}

#[tokio::test]
async fn observed_creation_rejection_is_recoverable_behavior_input() {
    let resolutions = Arc::new(Mutex::new(Vec::new()));
    let pongs = Arc::new(Mutex::new(Vec::new()));
    let system = creation_system();
    let recorder = system
        .spawn(Actor::new(
            MailAddr(99),
            Recorder {
                received: pongs.clone(),
            },
        ))
        .unwrap();
    let parent = system
        .spawn(Actor::new(
            MailAddr(1),
            ObservedParent {
                resolutions: resolutions.clone(),
            },
        ))
        .unwrap();

    wait_for(|| resolutions.lock().expect("resolution lock").len() == 1).await;
    assert_eq!(
        resolutions.lock().expect("resolution lock").as_slice(),
        [CreationResolved::rejected(
            7,
            bombay::behavior::CreationKind::Birth,
            CreationRejection::InitializationFailed,
        )],
        "the observed rejection reaches the behavior exactly once"
    );

    assert!(parent.actor_ref().send(MailAddr(99), 41).await.is_ok());
    wait_for(|| pongs.lock().expect("pong lock").contains(&42)).await;
    assert_eq!(
        pongs.lock().expect("pong lock").as_slice(),
        [42],
        "the parent keeps processing ordinary events after the rejection"
    );
    recorder.abort();
    parent.abort();
}

#[tokio::test]
async fn unobserved_creation_failure_keeps_the_exact_runtime_error() {
    let system = creation_system();
    let parent = system
        .spawn(Actor::new(MailAddr(1), UnobservedParent))
        .unwrap();

    let outcome = parent.outcome().await;
    assert!(
        matches!(
            outcome,
            TaskOutcome::Returned(Err(RunError::Environment(RuntimeEffectError::Birth(
                SystemBirthError::Initialization(InitFailure)
            ))))
        ),
        "an unobserved initialization failure stays a fatal exact error"
    );
}

#[tokio::test]
async fn rejected_nonce_is_reused_after_rollback_and_becomes_routable() {
    let resolutions = Arc::new(Mutex::new(Vec::new()));
    let received = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let system = creation_system();
    let parent = system
        .spawn(Actor::new(
            MailAddr(1),
            RetryParent {
                flaky: Flaky {
                    attempts: attempts.clone(),
                    received: received.clone(),
                },
                resolutions: resolutions.clone(),
            },
        ))
        .unwrap();

    wait_for(|| received.lock().expect("child record lock").contains(&42)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(
        resolutions.lock().expect("resolution lock").as_slice(),
        [
            CreationResolved::rejected(
                7,
                bombay::behavior::CreationKind::Birth,
                CreationRejection::InitializationFailed,
            ),
            CreationResolved::birth(7),
        ],
        "the same nonce is rebound and installed after the rollback"
    );
    parent.abort();
}

#[tokio::test]
async fn child_binding_commits_only_after_all_initialization_effects_succeed() {
    let resolutions = Arc::new(Mutex::new(Vec::new()));
    let lifecycles = RecordingLifecycles::default();
    let system = System::with_lifecycle(
        MailboxConfig::bounded(8),
        CreationRouter::default(),
        lifecycles.clone(),
    );
    let parent = system
        .spawn(Actor::new(
            MailAddr(1),
            ObservedGrandparent {
                resolutions: resolutions.clone(),
            },
        ))
        .unwrap();

    wait_for(|| resolutions.lock().expect("resolution lock").len() == 1).await;
    assert_eq!(
        resolutions.lock().expect("resolution lock").as_slice(),
        [CreationResolved::rejected(
            7,
            bombay::behavior::CreationKind::Birth,
            CreationRejection::EnvironmentFailed,
        )],
        "the grandchild's unobserved failure rejects the child's installation"
    );

    let child_address = MailAddr(1).birth(7);
    let grandchild_address = child_address.birth(3);
    let recorded = lifecycles.0.lock().expect("lifecycle lock");
    assert!(
        recorded
            .iter()
            .filter(|event| event.address == child_address
                && matches!(
                    event.transition,
                    LifecycleTransition::Prepared
                        | LifecycleTransition::Started
                        | LifecycleTransition::Restarted
                ))
            .count()
            == 0,
        "no install lifecycle event exists before initialization effects complete: {recorded:?}"
    );
    assert!(
        recorded
            .iter()
            .all(|event| event.address != grandchild_address),
        "a failed grandchild leaves no lifecycle trace: {recorded:?}"
    );
    drop(recorded);
    parent.abort();
}

#[tokio::test]
async fn paired_observe_child_does_not_turn_rejection_fatal() {
    let resolutions = Arc::new(Mutex::new(Vec::new()));
    let pongs = Arc::new(Mutex::new(Vec::new()));
    let system = creation_system();
    let recorder = system
        .spawn(Actor::new(
            MailAddr(99),
            Recorder {
                received: pongs.clone(),
            },
        ))
        .unwrap();
    let parent = system
        .spawn(Actor::new(
            MailAddr(1),
            PairingParent {
                resolutions: resolutions.clone(),
            },
        ))
        .unwrap();

    wait_for(|| resolutions.lock().expect("resolution lock").len() == 1).await;
    assert!(parent.actor_ref().send(MailAddr(99), 10).await.is_ok());
    wait_for(|| pongs.lock().expect("pong lock").contains(&11)).await;
    assert_eq!(
        resolutions.lock().expect("resolution lock").as_slice(),
        [CreationResolved::rejected(
            7,
            bombay::behavior::CreationKind::Birth,
            CreationRejection::InitializationFailed,
        )],
        "the paired ObserveChild stayed inert and the parent survived"
    );
    recorder.abort();
    parent.abort();
}

enum TreeMsg {}

type TreeSupervisor = Supervisor<TreeParent, TreeWorker>;
type TreeProxy = Proxy<TreeWorker>;
type TreeParentEvent = SupervisionEvent<User<MailAddr, TreeMsg>>;
type TreeSupervisorEndpoint =
    ActorRef<MailAddr, MailboxAnchor<<TreeSupervisor as Behavior>::Event>>;
type TreeProxyEndpoint = ActorRef<MailAddr, MailboxAnchor<<TreeProxy as Behavior>::Event>>;
type TreeWorkerEndpoint = ActorRef<MailAddr, MailboxAnchor<User<MailAddr, Never>>>;

struct TreeParent;

impl Behavior for TreeParent {
    type Addr = MailAddr;
    type Msg = TreeMsg;
    type Event = TreeParentEvent;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<TreeWorker>;

    fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, event: Self::Event) -> bombay::behavior::BehaviorActed<Self> {
        match event {
            SupervisionEvent::Inner(user) => match user.message {},
            _ => Ok(Actions::cont()),
        }
    }
}

struct TreeWorker {
    attempts: Arc<AtomicUsize>,
}

impl Behavior for TreeWorker {
    type Addr = MailAddr;
    type Msg = Never;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = InitFailure;
    type Birth = NoBirths;

    fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(InitFailure)
    }

    fn transition(&mut self, event: Self::Event) -> bombay::behavior::BehaviorActed<Self> {
        match event.message {}
    }
}

#[derive(Clone, Default)]
struct TreeRouter {
    supervisors: AddressRouter<MailAddr, IncarnationEndpoint<MailAddr, TreeSupervisorEndpoint>>,
    proxies: AddressRouter<MailAddr, IncarnationEndpoint<MailAddr, TreeProxyEndpoint>>,
    workers: AddressRouter<MailAddr, IncarnationEndpoint<MailAddr, TreeWorkerEndpoint>>,
}

macro_rules! tree_registry_and_delivery {
    ($router:ident, $behavior:ty, $endpoint:ty) => {
        impl EndpointRegistry<$behavior, IncarnationEndpoint<MailAddr, $endpoint>>
            for TreeRouter
        {
            type Error = bombay::AddressInUse<MailAddr>;
            type Registration = <AddressRouter<
                MailAddr,
                IncarnationEndpoint<MailAddr, $endpoint>,
            > as EndpointRegistry<
                $behavior,
                IncarnationEndpoint<MailAddr, $endpoint>,
            >>::Registration;

            fn register(
                &self,
                address: MailAddr,
                endpoint: IncarnationEndpoint<MailAddr, $endpoint>,
            ) -> Result<Self::Registration, Self::Error> {
                <AddressRouter<MailAddr, IncarnationEndpoint<MailAddr, $endpoint>> as EndpointRegistry<
                    $behavior,
                    IncarnationEndpoint<MailAddr, $endpoint>,
                >>::register(&self.$router, address, endpoint)
            }
        }

        impl DeliveryRouter<$behavior> for TreeRouter {
            type Error = <AddressRouter<
                MailAddr,
                IncarnationEndpoint<MailAddr, $endpoint>,
            > as DeliveryRouter<$behavior>>::Error;

            async fn deliver(
                &self,
                from: MailAddr,
                delivery: Delivery<$behavior>,
            ) -> Result<(), Self::Error> {
                self.$router.deliver(from, delivery).await
            }
        }
    };
}

tree_registry_and_delivery!(proxies, TreeProxy, TreeProxyEndpoint);
tree_registry_and_delivery!(workers, TreeWorker, TreeWorkerEndpoint);

impl EndpointRegistry<TreeSupervisor, IncarnationEndpoint<MailAddr, TreeSupervisorEndpoint>>
    for TreeRouter
{
    type Error = bombay::AddressInUse<MailAddr>;
    type Registration = <AddressRouter<
        MailAddr,
        IncarnationEndpoint<MailAddr, TreeSupervisorEndpoint>,
    > as EndpointRegistry<
        TreeSupervisor,
        IncarnationEndpoint<MailAddr, TreeSupervisorEndpoint>,
    >>::Registration;

    fn register(
        &self,
        address: MailAddr,
        endpoint: IncarnationEndpoint<MailAddr, TreeSupervisorEndpoint>,
    ) -> Result<Self::Registration, Self::Error> {
        <AddressRouter<MailAddr, IncarnationEndpoint<MailAddr, TreeSupervisorEndpoint>> as EndpointRegistry<
            TreeSupervisor,
            IncarnationEndpoint<MailAddr, TreeSupervisorEndpoint>,
        >>::register(&self.supervisors, address, endpoint)
    }
}

static TREE_ATTEMPTS: std::sync::OnceLock<Arc<AtomicUsize>> = std::sync::OnceLock::new();

fn tree_worker(_index: usize) -> TreeWorker {
    TreeWorker {
        attempts: TREE_ATTEMPTS.get().expect("tree counter").clone(),
    }
}

#[tokio::test(start_paused = true)]
async fn nested_rejection_terminates_neither_proxy_nor_parent() {
    let attempts = Arc::new(AtomicUsize::new(0));
    TREE_ATTEMPTS
        .set(attempts.clone())
        .expect("single tree test");
    let supervisor = Supervisor::new(
        TreeParent,
        |_| 7,
        1,
        tree_worker,
        Strategy::OneForOne,
        RestartPolicy::Permanent,
        1,
        Duration::MAX,
    )
    .with_failure_reaction(stop_on_supervision_failure);
    let system = System::new(MailboxConfig::bounded(8), TreeRouter::default());
    let root = system.spawn(Actor::new(MailAddr(5), supervisor)).unwrap();

    for _ in 0..32 {
        yield_now().await;
    }
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "a rejected initial install is attempted exactly once"
    );

    root.abort();
    assert!(
        matches!(root.outcome().await, TaskOutcome::Cancelled),
        "the supervisor and its stable proxy were still alive to be cancelled"
    );
}

async fn wait_for(condition: impl Fn() -> bool) {
    for _ in 0..10_000 {
        if condition() {
            return;
        }
        yield_now().await;
    }
    panic!("condition was not met in bounded yields");
}
