//! End-to-end lifecycle contracts for the current composition.
//!
//! These tests deliberately use barriers, virtual time, and small reference
//! models. They contain no wall-clock sleeps.

use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bombay::behavior::{
    Actions, Address, Behavior, Births, ChildStopped, Compose, Create, Delivery, Exit, Handler,
    Inner, MailAddr, Never, NoBirths, ObserveChild, ObservePeer, Own, Proxy, Pure, Recipient,
    RestartDenial, RestartPolicy, SendAlgebra, SendProduct, ServiceSends, ShutdownRequested, Step,
    StopOnShutdown, Strategy, SupervisionEvent, SupervisionFailureReason, Supervisor, UnwatchPeer,
    User, Watch, WatchEvent, stop_on_supervision_failure,
};
use bombay::{
    Actor, ActorRef, AddressRouter, DeliveryEndpoint, DeliveryRouter, EndpointRegistry,
    IncarnationEndpoint, LifecycleEvent, LifecycleSink, LifecycleTransition, MailboxAnchor,
    MailboxConfig, RunExit, System, TaskOutcome,
};
use bombay_address::RegistrationId;
use tokio::sync::Notify;
use tokio::task::yield_now;
use tokio::time::{Duration, Instant, advance};

type CompletionEvent = SupervisionEvent<User<MailAddr, Never>>;
type RecordedChildOutcome = Result<Exit<MailAddr>, bombay::behavior::Crash>;

struct ImmediateChild;

impl Behavior for ImmediateChild {
    type Addr = MailAddr;
    type Msg = Never;
    type Event = CompletionEvent;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        Ok(Actions::stop(Exit::Normal))
    }

    fn transition(&mut self, _event: Self::Event) -> behavior::BehaviorActed<Self> {
        Ok(Actions::stop(Exit::Normal))
    }
}

struct ObserveImmediateChild {
    observed: Arc<Mutex<Option<RecordedChildOutcome>>>,
}

impl Behavior for ObserveImmediateChild {
    type Addr = MailAddr;
    type Msg = Never;
    type Event = CompletionEvent;
    type Sends = SendProduct<Vec<Never>, ServiceSends<ObserveChild<u64>>>;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<ImmediateChild>;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        let mut sends = Self::Sends::empty();
        sends.send::<_, Own>(ObserveChild { nonce: 7 });
        Ok(Actions {
            sends,
            creates: vec![Create::birth(7, ImmediateChild)],
            become_: Step::Continue,
        })
    }

    fn transition(&mut self, event: Self::Event) -> behavior::BehaviorActed<Self> {
        let SupervisionEvent::ChildStopped(ChildStopped { outcome, .. }) = event else {
            panic!("expected child termination");
        };
        *self.observed.lock().expect("completion record lock") = Some(outcome);
        Ok(Actions::stop(Exit::Normal))
    }
}

#[tokio::test]
async fn child_observation_reports_the_exact_spawned_generation() {
    let observed = Arc::new(Mutex::new(None));
    let router = AddressRouter::default();
    let system = System::new(MailboxConfig::bounded(1), router);
    let parent = system
        .spawn(Actor::new(
            MailAddr(100),
            ObserveImmediateChild {
                observed: observed.clone(),
            },
        ))
        .unwrap();

    assert!(matches!(
        parent.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
    assert_eq!(
        *observed.lock().expect("completion record lock"),
        Some(Ok(Exit::Normal))
    );
}

type PeerWatchEvent = WatchEvent<User<MailAddr, u8>>;

struct WatchedPeer;

impl Behavior for WatchedPeer {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = PeerWatchEvent;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, event: Self::Event) -> behavior::BehaviorActed<Self> {
        match event {
            WatchEvent::Inner(User { .. }) => Ok(Actions::stop(Exit::Normal)),
            WatchEvent::PeerStopped(_) => Ok(Actions::cont()),
        }
    }
}

struct WatchState {
    initialized: Arc<AtomicBool>,
    observed: Arc<Mutex<Option<(MailAddr, RecordedChildOutcome)>>>,
}

impl Handler for WatchState {
    type Addr = MailAddr;
    type Msg = u8;

    fn receive(
        &mut self,
        _from: MailAddr,
        _message: u8,
    ) -> behavior::Acted<MailAddr, Never, Vec<Never>, NoBirths, Never> {
        self.initialized.store(true, Ordering::SeqCst);
        Ok(Actions::cont())
    }
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "Bombay Behavior's link-reaction function pointer returns the behavior error domain"
)]
fn record_peer_stop(
    behavior: &mut Pure<WatchState>,
    peer: MailAddr,
    outcome: &RecordedChildOutcome,
) -> Result<behavior::Become<MailAddr>, Never> {
    *behavior.state().observed.lock().expect("peer outcome lock") = Some((peer, *outcome));
    Ok(Step::Stop(Exit::Normal))
}

#[tokio::test]
async fn watching_receives_the_exact_peers_normalized_outcome() {
    let initialized = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(Mutex::new(None));
    let router = AddressRouter::default();
    let system = System::new(MailboxConfig::bounded(2), router);
    let peer = system.spawn(Actor::new(MailAddr(20), WatchedPeer)).unwrap();
    let watcher = system
        .spawn(Actor::new(
            MailAddr(21),
            Watch::new(
                Pure::new(WatchState {
                    initialized: initialized.clone(),
                    observed: observed.clone(),
                }),
                MailAddr(20),
                record_peer_stop,
            ),
        ))
        .unwrap();

    watcher.actor_ref().send(MailAddr(0), 1).await.unwrap();
    while !initialized.load(Ordering::SeqCst) {
        yield_now().await;
    }
    peer.actor_ref().send(MailAddr(0), 1).await.unwrap();

    assert!(matches!(
        peer.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
    assert!(matches!(
        watcher.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
    assert_eq!(
        *observed.lock().expect("peer outcome lock"),
        Some((MailAddr(20), Ok(Exit::Normal)))
    );
}

type UnwatchSends =
    SendProduct<ServiceSends<ObservePeer<MailAddr>>, ServiceSends<UnwatchPeer<MailAddr>>>;
type ObservePeerPath = Inner<Own>;

struct UnwatchProbe {
    watch: Option<MailAddr>,
    cancel_folded: Arc<Notify>,
    peer_stopped: Arc<AtomicBool>,
}

impl Behavior for UnwatchProbe {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = WatchEvent<User<MailAddr, u8>>;
    type Sends = UnwatchSends;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        let mut sends = Self::Sends::empty();
        if let Some(peer) = self.watch {
            sends.send::<_, ObservePeerPath>(ObservePeer::new(peer));
        }
        Ok(Actions {
            sends,
            creates: Vec::new(),
            become_: Step::Continue,
        })
    }

    fn transition(&mut self, event: Self::Event) -> behavior::BehaviorActed<Self> {
        match event {
            WatchEvent::Inner(User { message: 0, .. }) => {
                let mut sends = Self::Sends::empty();
                sends.send::<_, Own>(UnwatchPeer::new(
                    self.watch.expect("watching probe has one peer"),
                ));
                self.cancel_folded.notify_one();
                Ok(Actions {
                    sends,
                    creates: Vec::new(),
                    become_: Step::Continue,
                })
            }
            WatchEvent::Inner(User { message: 1, .. }) => Ok(Actions::stop(Exit::Normal)),
            WatchEvent::Inner(User { .. }) => Ok(Actions::cont()),
            WatchEvent::PeerStopped(_) => {
                self.peer_stopped.store(true, Ordering::SeqCst);
                Ok(Actions::cont())
            }
        }
    }
}

#[tokio::test]
async fn typed_unwatch_cancels_the_matching_production_monitor() {
    let system = System::new(MailboxConfig::bounded(1), AddressRouter::default());
    let cancel_folded = Arc::new(Notify::new());
    let peer_stopped = Arc::new(AtomicBool::new(false));
    let peer = system
        .spawn(Actor::new(
            MailAddr(9),
            UnwatchProbe {
                watch: None,
                cancel_folded: cancel_folded.clone(),
                peer_stopped: peer_stopped.clone(),
            },
        ))
        .unwrap();
    let watcher = system
        .spawn(Actor::new(
            MailAddr(8),
            UnwatchProbe {
                watch: Some(MailAddr(9)),
                cancel_folded: cancel_folded.clone(),
                peer_stopped: peer_stopped.clone(),
            },
        ))
        .unwrap();

    watcher.actor_ref().send(MailAddr(1), 0).await.unwrap();
    cancel_folded.notified().await;
    peer.actor_ref().send(MailAddr(1), 1).await.unwrap();
    assert!(matches!(
        peer.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
    yield_now().await;
    assert!(!peer_stopped.load(Ordering::SeqCst));

    watcher.actor_ref().send(MailAddr(1), 1).await.unwrap();
    assert!(matches!(
        watcher.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TreeAddr(u64);

impl Address for TreeAddr {
    type Nonce = u64;

    fn birth(self, nonce: Self::Nonce) -> Self {
        Self(
            self.0
                .checked_mul(257)
                .and_then(|prefix| prefix.checked_add(nonce + 1))
                .expect("test address space exhausted"),
        )
    }
}

#[derive(Debug)]
struct WorkerFailure;

struct RestartWorker {
    starts: Arc<AtomicUsize>,
}

static RESTART_STARTS: std::sync::OnceLock<Arc<AtomicUsize>> = std::sync::OnceLock::new();

fn restart_worker(_index: usize) -> RestartWorker {
    RestartWorker {
        starts: RESTART_STARTS.get().expect("test worker counter").clone(),
    }
}

fn restart_proxy_nonce(_index: usize) -> u64 {
    7
}

impl Behavior for RestartWorker {
    type Addr = TreeAddr;
    type Msg = Never;
    type Event = User<TreeAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = WorkerFailure;
    type Birth = NoBirths;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        // The incarnation installs successfully and immediately stops, so
        // escalation flows through the running-worker WorkerStopped lane.
        Ok(Actions::stop(Exit::Normal))
    }

    fn transition(&mut self, event: Self::Event) -> behavior::BehaviorActed<Self> {
        match event.message {}
    }
}

enum ParentMsg {}

type RestartParentEvent = SupervisionEvent<User<TreeAddr, ParentMsg>>;

struct RestartParent;

impl Behavior for RestartParent {
    type Addr = TreeAddr;
    type Msg = ParentMsg;
    type Event = RestartParentEvent;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<RestartWorker>;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _event: Self::Event) -> behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type RestartSupervisor = Supervisor<RestartParent, RestartWorker>;
type RestartProxy = Proxy<RestartWorker>;
type SupervisorEndpoint = ActorRef<TreeAddr, MailboxAnchor<<RestartSupervisor as Behavior>::Event>>;
type ProxyEndpoint = ActorRef<TreeAddr, MailboxAnchor<<RestartProxy as Behavior>::Event>>;
type WorkerEndpoint = ActorRef<TreeAddr, MailboxAnchor<<RestartWorker as Behavior>::Event>>;
type SupervisorIncarnation = IncarnationEndpoint<TreeAddr, SupervisorEndpoint>;
type ProxyIncarnation = IncarnationEndpoint<TreeAddr, ProxyEndpoint>;
type WorkerIncarnation = IncarnationEndpoint<TreeAddr, WorkerEndpoint>;

#[derive(Clone, Default)]
struct RestartRouter {
    supervisors: AddressRouter<TreeAddr, SupervisorIncarnation>,
    proxies: AddressRouter<TreeAddr, ProxyIncarnation>,
    workers: AddressRouter<TreeAddr, WorkerIncarnation>,
}

impl EndpointRegistry<RestartSupervisor, SupervisorIncarnation> for RestartRouter {
    type Error = bombay::AddressInUse<TreeAddr>;
    type Registration = <AddressRouter<TreeAddr, SupervisorIncarnation> as EndpointRegistry<
        RestartSupervisor,
        SupervisorIncarnation,
    >>::Registration;

    fn register(
        &self,
        address: TreeAddr,
        endpoint: SupervisorIncarnation,
    ) -> Result<Self::Registration, Self::Error> {
        <AddressRouter<TreeAddr, SupervisorIncarnation> as EndpointRegistry<
            RestartSupervisor,
            SupervisorIncarnation,
        >>::register(&self.supervisors, address, endpoint)
    }
}

impl EndpointRegistry<RestartProxy, ProxyIncarnation> for RestartRouter {
    type Error = bombay::AddressInUse<TreeAddr>;
    type Registration = <AddressRouter<TreeAddr, ProxyIncarnation> as EndpointRegistry<
        RestartProxy,
        ProxyIncarnation,
    >>::Registration;

    fn register(
        &self,
        address: TreeAddr,
        endpoint: ProxyIncarnation,
    ) -> Result<Self::Registration, Self::Error> {
        <AddressRouter<TreeAddr, ProxyIncarnation> as EndpointRegistry<
            RestartProxy,
            ProxyIncarnation,
        >>::register(&self.proxies, address, endpoint)
    }
}

impl EndpointRegistry<RestartWorker, WorkerIncarnation> for RestartRouter {
    type Error = bombay::AddressInUse<TreeAddr>;
    type Registration = <AddressRouter<TreeAddr, WorkerIncarnation> as EndpointRegistry<
        RestartWorker,
        WorkerIncarnation,
    >>::Registration;

    fn register(
        &self,
        address: TreeAddr,
        endpoint: WorkerIncarnation,
    ) -> Result<Self::Registration, Self::Error> {
        <AddressRouter<TreeAddr, WorkerIncarnation> as EndpointRegistry<
            RestartWorker,
            WorkerIncarnation,
        >>::register(&self.workers, address, endpoint)
    }
}

impl DeliveryRouter<RestartProxy> for RestartRouter {
    type Error = <AddressRouter<TreeAddr, ProxyIncarnation> as DeliveryRouter<RestartProxy>>::Error;

    async fn deliver(
        &self,
        from: TreeAddr,
        delivery: Delivery<RestartProxy>,
    ) -> Result<(), Self::Error> {
        self.proxies.deliver(from, delivery).await
    }
}

impl DeliveryRouter<RestartWorker> for RestartRouter {
    type Error =
        <AddressRouter<TreeAddr, WorkerIncarnation> as DeliveryRouter<RestartWorker>>::Error;

    async fn deliver(
        &self,
        from: TreeAddr,
        delivery: Delivery<RestartWorker>,
    ) -> Result<(), Self::Error> {
        self.workers.deliver(from, delivery).await
    }
}

#[tokio::test]
async fn supervision_escalation_retires_and_releases_the_complete_tree() {
    let starts = Arc::new(AtomicUsize::new(0));
    // The Behavior algebra intentionally accepts a function pointer so the
    // topology is deterministic and contains no captured runtime capability.
    RESTART_STARTS
        .set(starts.clone())
        .expect("single lifecycle test");
    let supervisor = Supervisor::new(
        RestartParent,
        restart_proxy_nonce,
        1,
        restart_worker,
        Strategy::OneForOne,
        RestartPolicy::Permanent,
        1,
        Duration::MAX,
    )
    .with_failure_reaction(stop_on_supervision_failure);
    let system = System::new(MailboxConfig::bounded(4), RestartRouter::default());
    let parent = system.spawn(Actor::new(TreeAddr(1), supervisor)).unwrap();

    assert!(matches!(
        parent.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::SupervisionFailed(
            SupervisionFailureReason::RestartDenied(RestartDenial::BudgetExceeded {
                restarts_in_window: 1,
                replacements_requested: 1,
                maximum_restarts: 1,
            })
        ))))
    ));
    assert_eq!(starts.load(Ordering::SeqCst), 2, "W0 must become fresh W1");

    let replacement = Supervisor::new(
        RestartParent,
        restart_proxy_nonce,
        1,
        restart_worker,
        Strategy::OneForOne,
        RestartPolicy::Permanent,
        0,
        Duration::MAX,
    )
    .with_failure_reaction(stop_on_supervision_failure);
    let replacement = system
        .spawn(Actor::new(TreeAddr(1), replacement))
        .expect("supervisor completion must release its proxy and worker tree");
    assert!(matches!(
        replacement.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::SupervisionFailed(
            SupervisionFailureReason::RestartDenied(RestartDenial::BudgetExceeded {
                restarts_in_window: 0,
                replacements_requested: 1,
                maximum_restarts: 0,
            })
        ))))
    ));
}

struct Stop;

struct BirthChildAndSignal {
    signal: MailAddr,
}

impl Handler for Stop {
    type Addr = MailAddr;
    type Msg = u8;

    fn receive(
        &mut self,
        _from: MailAddr,
        _message: u8,
    ) -> bombay::behavior::Acted<MailAddr, Never, Vec<Never>, NoBirths, Never> {
        Ok(Actions::stop(bombay::behavior::Exit::Normal))
    }
}

impl Handler<Vec<Delivery<Pure<Stop>>>, Births<Pure<Stop>>> for BirthChildAndSignal {
    type Addr = MailAddr;
    type Msg = u8;

    fn receive(
        &mut self,
        _from: MailAddr,
        message: u8,
    ) -> bombay::behavior::Acted<
        MailAddr,
        Never,
        Vec<Delivery<Pure<Stop>>>,
        Births<Pure<Stop>>,
        Never,
    > {
        Ok(Actions {
            sends: vec![Delivery::new(Recipient::global(self.signal), message)],
            creates: vec![Create::birth(7, Pure::new(Stop))],
            become_: bombay::behavior::Step::Continue,
        })
    }
}

#[tokio::test]
async fn parent_retains_created_child_handle_while_parent_is_live() {
    let router = AddressRouter::default();
    let system = System::new(MailboxConfig::bounded(1), router.clone());
    let signal = system
        .spawn(Actor::new(MailAddr(9), Pure::new(Stop)))
        .unwrap();
    let parent = system
        .spawn(Actor::new(
            MailAddr(1),
            Pure::new(BirthChildAndSignal {
                signal: MailAddr(9),
            }),
        ))
        .unwrap();

    parent.actor_ref().send(MailAddr(0), 42).await.unwrap();
    assert!(matches!(
        signal.outcome().await,
        TaskOutcome::Returned(Ok(_))
    ));

    let child = MailAddr(1).birth(7);
    router
        .deliver(
            MailAddr(1),
            Delivery::new(Recipient::<Pure<Stop>>::global(child), 99),
        )
        .await
        .expect("a live parent must retain its created child's counting handle");

    assert!(matches!(
        parent.close().await,
        TaskOutcome::Returned(Ok(RunExit::EnvironmentClosed))
    ));
}

struct SameTurnChild {
    received: Arc<Notify>,
}

impl Handler for SameTurnChild {
    type Addr = MailAddr;
    type Msg = u8;

    fn receive(
        &mut self,
        _from: MailAddr,
        message: u8,
    ) -> bombay::behavior::Acted<MailAddr, Never, Vec<Never>, NoBirths, Never> {
        assert_eq!(message, 73);
        self.received.notify_one();
        Ok(Actions::cont())
    }
}

struct CreateAndSendSameTurn {
    child: Option<Pure<SameTurnChild>>,
}

impl Behavior for CreateAndSendSameTurn {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = User<MailAddr, u8>;
    type Sends = Vec<Delivery<Pure<SameTurnChild>>>;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<Pure<SameTurnChild>>;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        Ok(Actions {
            sends: vec![Delivery::new(Recipient::child(7), 73)],
            creates: vec![Create::birth(
                7,
                self.child.take().expect("parent initializes once"),
            )],
            become_: Step::Continue,
        })
    }

    fn transition(&mut self, event: Self::Event) -> behavior::BehaviorActed<Self> {
        let _ = event;
        Ok(Actions::cont())
    }
}

#[tokio::test]
async fn typed_creator_spawns_child_before_routing_same_transition_send() {
    let received = Arc::new(Notify::new());
    let system = System::new(MailboxConfig::bounded(1), AddressRouter::default());
    let parent = system
        .spawn(Actor::new(
            MailAddr(1),
            CreateAndSendSameTurn {
                child: Some(Pure::new(SameTurnChild {
                    received: received.clone(),
                })),
            },
        ))
        .unwrap();

    received.notified().await;
    assert!(matches!(
        parent.close().await,
        TaskOutcome::Returned(Ok(RunExit::EnvironmentClosed))
    ));
}

#[tokio::test]
async fn address_collision_rolls_back_without_disturbing_the_live_generation() {
    let router = AddressRouter::default();
    let system = System::new(MailboxConfig::bounded(1), router);
    let first = system
        .spawn(Actor::new(MailAddr(7), Pure::new(Stop)))
        .unwrap();

    assert!(
        system
            .spawn(Actor::new(MailAddr(7), Pure::new(Stop)))
            .is_err()
    );
    first.actor_ref().send(MailAddr(1), 0).await.unwrap();
    assert!(matches!(
        first.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(bombay::behavior::Exit::Normal)))
    ));

    let replacement = system
        .spawn(Actor::new(MailAddr(7), Pure::new(Stop)))
        .unwrap();
    replacement.actor_ref().send(MailAddr(1), 0).await.unwrap();
    assert!(matches!(
        replacement.outcome().await,
        TaskOutcome::Returned(Ok(_))
    ));
}

#[tokio::test]
async fn dropping_handle_detaches_terminal_observation_without_cancelling_actor() {
    let router = AddressRouter::default();
    let system = System::new(MailboxConfig::bounded(1), router);
    let handle = system
        .spawn(Actor::new(MailAddr(7), Pure::new(Stop)))
        .unwrap();
    let edge = handle.actor_ref().clone();
    drop(handle);

    edge.send(MailAddr(1), 0).await.unwrap();
    drop(edge);

    let replacement = loop {
        match system.spawn(Actor::new(MailAddr(7), Pure::new(Stop))) {
            Ok(replacement) => break replacement,
            Err(_) => yield_now().await,
        }
    };
    replacement.actor_ref().send(MailAddr(1), 0).await.unwrap();
    assert!(matches!(
        replacement.outcome().await,
        TaskOutcome::Returned(Ok(_))
    ));
}

#[tokio::test]
async fn last_edge_closure_is_not_pinned_by_the_registered_anchor() {
    let router = AddressRouter::default();
    let system = System::new(MailboxConfig::bounded(1), router);
    let spawned = system
        .spawn(Actor::new(MailAddr(7), Pure::new(Stop)))
        .unwrap();

    assert!(matches!(
        spawned.close().await,
        TaskOutcome::Returned(Ok(RunExit::EnvironmentClosed))
    ));
}

struct BlockingShutdown {
    entered: Arc<tokio::sync::Notify>,
    release: std::sync::mpsc::Receiver<()>,
    user_folds: Arc<AtomicUsize>,
    finalized: Arc<AtomicBool>,
    signal: MailAddr,
}

impl Behavior for BlockingShutdown {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = User<MailAddr, u8>;
    type Sends = Vec<Delivery<Pure<Stop>>>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        self.entered.notify_one();
        self.release.recv().expect("release signal");
        Ok(Actions::cont())
    }

    fn transition(&mut self, _event: Self::Event) -> behavior::BehaviorActed<Self> {
        self.user_folds.fetch_add(1, Ordering::SeqCst);
        Ok(Actions::cont())
    }
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "Bombay Behavior's shutdown-reaction function pointer returns the behavior error domain"
)]
fn finalize_shutdown(
    behavior: &mut BlockingShutdown,
    _request: ShutdownRequested,
) -> bombay::behavior::Acted<MailAddr, Never, Vec<Delivery<Pure<Stop>>>, NoBirths, Never> {
    behavior.finalized.store(true, Ordering::SeqCst);
    Ok(Actions {
        sends: vec![Delivery::new(Recipient::global(behavior.signal), 42)],
        creates: Vec::new(),
        become_: Step::Continue,
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn graceful_shutdown_preempts_user_backlog_and_interprets_final_effects() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release_tx, release) = std::sync::mpsc::channel();
    let user_folds = Arc::new(AtomicUsize::new(0));
    let finalized = Arc::new(AtomicBool::new(false));
    let router = AddressRouter::default();
    let system = System::new(MailboxConfig::bounded(2), router);
    let signal = system
        .spawn(Actor::new(
            MailAddr(9),
            Compose::new(Stop).stop_on_shutdown().build(),
        ))
        .unwrap();
    let actor = system
        .spawn(Actor::new(
            MailAddr(7),
            Compose::from_behavior(BlockingShutdown {
                entered: entered.clone(),
                release,
                user_folds: user_folds.clone(),
                finalized: finalized.clone(),
                signal: MailAddr(9),
            })
            .finalize_on_shutdown(finalize_shutdown)
            .build(),
        ))
        .unwrap();
    let retired = actor.actor_ref().clone();

    entered.notified().await;
    actor.actor_ref().send(MailAddr(0), 1).await.unwrap();
    actor.actor_ref().request_shutdown().unwrap();
    actor.actor_ref().send(MailAddr(0), 2).await.unwrap();
    release_tx.send(()).expect("actor init is waiting");

    assert!(matches!(
        actor.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
    assert!(finalized.load(Ordering::SeqCst));
    assert_eq!(
        user_folds.load(Ordering::SeqCst),
        0,
        "priority shutdown must overtake the complete user backlog"
    );
    assert!(matches!(
        signal.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
    assert!(retired.send(MailAddr(0), 3).await.is_err());
    assert_eq!(
        retired.request_shutdown(),
        Err(bombay::ShutdownRequestError::Closed)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn blocked_public_send_recovers_exact_payload_after_incarnation_retirement() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release_tx, release) = std::sync::mpsc::channel();
    let system = System::new(MailboxConfig::bounded(1), AddressRouter::default());
    let actor = system
        .spawn(Actor::new(
            MailAddr(7),
            BlockingShutdown {
                entered: entered.clone(),
                release,
                user_folds: Arc::new(AtomicUsize::new(0)),
                finalized: Arc::new(AtomicBool::new(false)),
                signal: MailAddr(9),
            },
        ))
        .unwrap();
    let actor_ref = actor.actor_ref().clone();

    entered.notified().await;
    actor_ref.send(MailAddr(0), 1).await.unwrap();
    actor_ref.send(MailAddr(0), 2).await.unwrap();
    let blocked = tokio::spawn({
        let actor_ref = actor_ref.clone();
        async move { actor_ref.send(MailAddr(0), 3).await }
    });
    yield_now().await;
    assert!(
        !blocked.is_finished(),
        "the bounded mailbox must apply backpressure"
    );

    actor.abort();
    release_tx.send(()).expect("release the actor's init fold");
    let rejected = blocked
        .await
        .unwrap()
        .expect_err("retirement must reject the blocked send");
    assert_eq!(
        rejected.0.message, 3,
        "closure must return the exact payload"
    );
    assert!(matches!(actor.outcome().await, TaskOutcome::Cancelled));
}

struct ScopedChild {
    retired: Arc<AtomicBool>,
}

impl Handler for ScopedChild {
    type Addr = MailAddr;
    type Msg = Never;

    fn receive(
        &mut self,
        _from: MailAddr,
        message: Never,
    ) -> behavior::Acted<MailAddr, Never, Vec<Never>, NoBirths, Never> {
        match message {}
    }
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "Bombay Behavior's shutdown-reaction function pointer returns the behavior error domain"
)]
fn retire_scoped_child(
    child: &mut Pure<ScopedChild>,
    _request: ShutdownRequested,
) -> behavior::Acted<MailAddr, Never, Vec<Never>, NoBirths, Never> {
    child.state().retired.store(true, Ordering::SeqCst);
    Ok(Actions::cont())
}

type ScopedChildBehavior = behavior::FinalizeOnShutdown<Pure<ScopedChild>>;

struct ScopedBranch {
    child: Option<ScopedChildBehavior>,
}

impl Behavior for ScopedBranch {
    type Addr = MailAddr;
    type Msg = Never;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<ScopedChildBehavior>;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        Ok(Actions {
            sends: Vec::new(),
            creates: vec![Create::birth(
                9,
                self.child.take().expect("branch initializes once"),
            )],
            become_: Step::Continue,
        })
    }

    fn transition(&mut self, event: Self::Event) -> behavior::BehaviorActed<Self> {
        match event.message {}
    }
}

type ScopedBranchBehavior = StopOnShutdown<ScopedBranch>;

struct ScopedRoot {
    child: Option<ScopedBranchBehavior>,
}

impl Behavior for ScopedRoot {
    type Addr = MailAddr;
    type Msg = Never;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<ScopedBranchBehavior>;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        Ok(Actions {
            sends: Vec::new(),
            creates: vec![Create::birth(
                7,
                self.child.take().expect("root initializes once"),
            )],
            become_: Step::Continue,
        })
    }

    fn transition(&mut self, event: Self::Event) -> behavior::BehaviorActed<Self> {
        match event.message {}
    }
}

#[tokio::test]
async fn root_shutdown_awaits_transitive_child_retirement() {
    let child_retired = Arc::new(AtomicBool::new(false));
    let router = AddressRouter::default();
    let system = System::new(MailboxConfig::bounded(1), router);
    let root = system
        .spawn(Actor::new(
            MailAddr(30),
            StopOnShutdown::new(ScopedRoot {
                child: Some(StopOnShutdown::new(ScopedBranch {
                    child: Some(behavior::FinalizeOnShutdown::new(
                        Pure::new(ScopedChild {
                            retired: child_retired.clone(),
                        }),
                        retire_scoped_child,
                    )),
                })),
            }),
        ))
        .unwrap();

    root.actor_ref().request_shutdown().unwrap();
    assert!(matches!(
        root.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
    assert!(child_retired.load(Ordering::SeqCst));

    let replacement = system
        .spawn(Actor::new(
            MailAddr(30).birth(7).birth(9),
            StopOnShutdown::new(Pure::new(ScopedChild {
                retired: Arc::new(AtomicBool::new(false)),
            })),
        ))
        .expect("root completion must imply descendant address reuse");
    replacement.actor_ref().request_shutdown().unwrap();
    let _ = replacement.outcome().await;
}

#[tokio::test(start_paused = true)]
async fn typed_behavior_timer_fires_through_the_incarnation() {
    let router = AddressRouter::default();
    let system = System::new(MailboxConfig::bounded(1), router);
    let due = Instant::now() + Duration::from_secs(1);
    let behavior = Compose::new(Stop)
        .deadline(Some(due), |_| Ok(Step::Stop(Exit::Normal)))
        .build();
    let handle = system.spawn(Actor::new(MailAddr(7), behavior)).unwrap();
    let outcome = tokio::spawn(async move { handle.outcome().await });

    yield_now().await;
    advance(Duration::from_secs(1)).await;
    yield_now().await;

    assert!(matches!(
        outcome.await.unwrap(),
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
}

struct ReceiveTimeoutProbe;

impl Handler for ReceiveTimeoutProbe {
    type Addr = MailAddr;
    type Msg = u8;

    fn receive(
        &mut self,
        _from: MailAddr,
        _message: u8,
    ) -> behavior::Acted<MailAddr, Never, Vec<Never>, NoBirths, Never> {
        Ok(Actions::cont())
    }
}

type ReceiveTimeoutInner = Pure<ReceiveTimeoutProbe>;

#[allow(
    clippy::unnecessary_wraps,
    reason = "Bombay Behavior's timeout-reaction function pointer returns the behavior error domain"
)]
fn stop_after_inactivity(
    _inner: &mut ReceiveTimeoutInner,
) -> behavior::Acted<MailAddr, Never, Vec<Never>, NoBirths, Never> {
    Ok(Actions::stop(Exit::Normal))
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "Bombay Behavior's deadline-reaction function pointer returns the behavior error domain"
)]
fn continue_after_service_deadline(
    _inner: &mut ReceiveTimeoutInner,
) -> Result<behavior::Become<MailAddr>, Never> {
    Ok(Step::Continue)
}

type DeadlineReceiveTimeoutInner = behavior::Deadline<ReceiveTimeoutInner>;

#[allow(
    clippy::unnecessary_wraps,
    reason = "Bombay Behavior's timeout-reaction function pointer returns the behavior error domain"
)]
fn stop_after_deadline_service_inactivity(
    _inner: &mut DeadlineReceiveTimeoutInner,
) -> behavior::BehaviorActed<DeadlineReceiveTimeoutInner> {
    Ok(Actions::stop(Exit::Normal))
}

#[tokio::test(start_paused = true)]
async fn successful_user_fold_replaces_the_live_receive_timeout_generation() {
    let system = System::new(MailboxConfig::bounded(1), AddressRouter::default());
    let behavior = Compose::new(ReceiveTimeoutProbe)
        .receive_timeout(Duration::from_secs(5), stop_after_inactivity)
        .build();
    let handle = system.spawn(Actor::new(MailAddr(8), behavior)).unwrap();
    let actor = handle.actor_ref().clone();
    let outcome = tokio::spawn(async move { handle.outcome().await });

    yield_now().await;
    advance(Duration::from_secs(4)).await;
    actor.send(MailAddr(1), 1).await.unwrap();
    yield_now().await;

    advance(Duration::from_secs(4)).await;
    yield_now().await;
    assert!(
        !outcome.is_finished(),
        "the stale initial generation must not end the new idle period"
    );

    advance(Duration::from_secs(1)).await;
    yield_now().await;
    assert!(matches!(
        outcome.await.unwrap(),
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
}

#[tokio::test(start_paused = true)]
async fn deadline_service_traffic_does_not_rearm_receive_timeout() {
    let system = System::new(MailboxConfig::bounded(1), AddressRouter::default());
    let service_at = Instant::now() + Duration::from_secs(4);
    let behavior = Compose::new(ReceiveTimeoutProbe)
        .deadline(Some(service_at), continue_after_service_deadline)
        .receive_timeout(
            Duration::from_secs(5),
            stop_after_deadline_service_inactivity,
        )
        .build();
    let watcher = system.spawn(Actor::new(MailAddr(8), behavior)).unwrap();
    let outcome = tokio::spawn(async move { watcher.outcome().await });

    yield_now().await;
    advance(Duration::from_secs(4)).await;
    yield_now().await;

    advance(Duration::from_secs(1)).await;
    yield_now().await;
    assert!(matches!(
        outcome.await.unwrap(),
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
}

#[tokio::test(start_paused = true)]
async fn nested_timers_at_the_same_deadline_keep_distinct_identities() {
    let router = AddressRouter::default();
    let system = System::new(MailboxConfig::bounded(1), router);
    let now = Instant::now();
    let behavior = Compose::new(Stop)
        .deadline(Some(now + Duration::from_secs(1)), |_| {
            Ok(Step::Stop(Exit::Normal))
        })
        .deadline(Some(now + Duration::from_secs(2)), |_| {
            Ok(Step::Stop(Exit::Normal))
        })
        .build();
    let handle = system.spawn(Actor::new(MailAddr(7), behavior)).unwrap();
    let outcome = tokio::spawn(async move { handle.outcome().await });

    yield_now().await;
    advance(Duration::from_secs(1)).await;
    yield_now().await;

    assert!(
        outcome.is_finished(),
        "the inner deadline must not be replaced"
    );
    assert!(matches!(
        outcome.await.unwrap(),
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
}

enum InitBehavior {
    Immediate,
    Distinct(Exit<MailAddr>),
    Panic,
    Pending,
}

type RollbackEndpoint = ActorRef<MailAddr, MailboxAnchor<User<MailAddr, u8>>>;
type RollbackIncarnation = IncarnationEndpoint<MailAddr, RollbackEndpoint>;

#[derive(Debug)]
enum RejectOnceError {
    Injected,
    AddressInUse,
}

#[derive(Clone)]
struct RejectOnceRouter {
    reject: Arc<AtomicBool>,
    rejected_endpoint: Arc<Mutex<Option<RollbackIncarnation>>>,
    inner: AddressRouter<MailAddr, RollbackIncarnation>,
}

impl RejectOnceRouter {
    fn new() -> Self {
        Self {
            reject: Arc::new(AtomicBool::new(true)),
            rejected_endpoint: Arc::new(Mutex::new(None)),
            inner: AddressRouter::default(),
        }
    }
}

impl EndpointRegistry<RollbackBehavior, RollbackIncarnation> for RejectOnceRouter {
    type Error = RejectOnceError;
    type Registration = <AddressRouter<MailAddr, RollbackIncarnation> as EndpointRegistry<
        RollbackBehavior,
        RollbackIncarnation,
    >>::Registration;

    fn register(
        &self,
        address: MailAddr,
        endpoint: RollbackIncarnation,
    ) -> Result<Self::Registration, Self::Error> {
        if self.reject.swap(false, Ordering::SeqCst) {
            *self.rejected_endpoint.lock().expect("endpoint probe lock") = Some(endpoint);
            Err(RejectOnceError::Injected)
        } else {
            <AddressRouter<MailAddr, RollbackIncarnation> as EndpointRegistry<
                RollbackBehavior,
                RollbackIncarnation,
            >>::register(&self.inner, address, endpoint)
            .map_err(|_| RejectOnceError::AddressInUse)
        }
    }
}

impl EndpointRegistry<InitBehavior, RollbackIncarnation> for RejectOnceRouter {
    type Error = RejectOnceError;
    type Registration = <AddressRouter<MailAddr, RollbackIncarnation> as EndpointRegistry<
        InitBehavior,
        RollbackIncarnation,
    >>::Registration;

    fn register(
        &self,
        address: MailAddr,
        endpoint: RollbackIncarnation,
    ) -> Result<Self::Registration, Self::Error> {
        <AddressRouter<MailAddr, RollbackIncarnation> as EndpointRegistry<
            InitBehavior,
            RollbackIncarnation,
        >>::register(&self.inner, address, endpoint)
        .map_err(|_| RejectOnceError::AddressInUse)
    }
}

impl DeliveryRouter<RollbackBehavior> for RejectOnceRouter {
    type Error =
        <AddressRouter<MailAddr, RollbackIncarnation> as DeliveryRouter<RollbackBehavior>>::Error;

    async fn deliver(
        &self,
        from: MailAddr,
        delivery: Delivery<RollbackBehavior>,
    ) -> Result<(), Self::Error> {
        self.inner.deliver(from, delivery).await
    }
}

struct RollbackBehavior {
    started: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl Drop for RollbackBehavior {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl Behavior for RollbackBehavior {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = User<MailAddr, u8>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(Actions::stop(Exit::Normal))
    }

    fn transition(&mut self, _event: Self::Event) -> behavior::BehaviorActed<Self> {
        unreachable!()
    }
}

#[tokio::test]
async fn registration_failure_rolls_back_real_preparation_before_task_start() {
    let router = RejectOnceRouter::new();
    let started = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let system = System::new(MailboxConfig::bounded(1), router.clone());

    assert!(
        system
            .spawn(Actor::new(
                MailAddr(7),
                RollbackBehavior {
                    started: started.clone(),
                    dropped: dropped.clone(),
                },
            ))
            .is_err()
    );
    yield_now().await;
    assert_eq!(started.load(Ordering::SeqCst), 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);

    let rejected = router
        .rejected_endpoint
        .lock()
        .expect("endpoint probe lock")
        .take()
        .expect("the failing registry retains its non-owning endpoint probe");
    assert!(rejected.deliver(MailAddr(0), 1).await.is_err());

    let replacement = system
        .spawn(Actor::new(MailAddr(7), InitBehavior::Immediate))
        .expect("rollback must leave the address available");
    assert!(matches!(
        replacement.outcome().await,
        TaskOutcome::Returned(Ok(_))
    ));
}

impl Behavior for InitBehavior {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = User<MailAddr, u8>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        match self {
            Self::Immediate => Ok(Actions::stop(Exit::Normal)),
            Self::Distinct(exit) => Ok(Actions::stop(*exit)),
            Self::Panic => panic!("oracle panic"),
            Self::Pending => Ok(Actions::cont()),
        }
    }

    fn transition(&mut self, _event: Self::Event) -> behavior::BehaviorActed<Self> {
        Ok(Actions {
            sends: Vec::new(),
            creates: Vec::new(),
            become_: Step::Continue,
        })
    }
}

#[tokio::test]
async fn immediate_completion_is_published_once_before_it_can_be_missed() {
    let router = AddressRouter::default();
    let system = System::new(MailboxConfig::bounded(1), router);
    let task = system
        .spawn(Actor::new(MailAddr(1), InitBehavior::Immediate))
        .unwrap();
    let retired = task.actor_ref().clone();
    assert!(matches!(
        task.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
    assert!(retired.send(MailAddr(0), 1).await.is_err());
    let replacement = system
        .spawn(Actor::new(MailAddr(1), InitBehavior::Immediate))
        .unwrap();
    assert!(matches!(
        replacement.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
}

#[tokio::test]
async fn panic_and_cancellation_are_distinct_terminal_publications() {
    let router = AddressRouter::default();
    let system = System::new(MailboxConfig::bounded(1), router);
    let panicked = system
        .spawn(Actor::new(MailAddr(1), InitBehavior::Panic))
        .unwrap();
    let panicked_ref = panicked.actor_ref().clone();
    assert!(matches!(panicked.outcome().await, TaskOutcome::Panicked));
    assert!(panicked_ref.send(MailAddr(0), 1).await.is_err());
    let after_panic = system
        .spawn(Actor::new(MailAddr(1), InitBehavior::Immediate))
        .unwrap();
    assert!(matches!(
        after_panic.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));

    let cancelled = system
        .spawn(Actor::new(MailAddr(2), InitBehavior::Pending))
        .unwrap();
    let cancelled_ref = cancelled.actor_ref().clone();
    cancelled.abort();
    assert!(matches!(cancelled.outcome().await, TaskOutcome::Cancelled));
    assert!(cancelled_ref.send(MailAddr(0), 1).await.is_err());
    let after_cancel = system
        .spawn(Actor::new(MailAddr(2), InitBehavior::Immediate))
        .unwrap();
    assert!(matches!(
        after_cancel.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
}

#[tokio::test]
async fn retained_completion_cannot_alias_a_replacement_incarnation() {
    let router = AddressRouter::default();
    let system = System::new(MailboxConfig::bounded(1), router);
    let old_exit = Exit::LinkDied(MailAddr(10));
    let replacement_exit = Exit::LinkDied(MailAddr(20));
    let old = system
        .spawn(Actor::new(MailAddr(7), InitBehavior::Distinct(old_exit)))
        .unwrap();

    let replacement = loop {
        match system.spawn(Actor::new(
            MailAddr(7),
            InitBehavior::Distinct(replacement_exit),
        )) {
            Ok(replacement) => break replacement,
            Err(_) => yield_now().await,
        }
    };

    assert!(matches!(
        replacement.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(exit))) if exit == replacement_exit
    ));
    assert!(matches!(
        old.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(exit))) if exit == old_exit
    ));
}

#[derive(Clone, Default)]
struct LifecycleRecorder(Arc<Mutex<Vec<LifecycleEvent<MailAddr>>>>);

impl LifecycleSink<MailAddr, RegistrationId> for LifecycleRecorder {
    fn record(&self, event: LifecycleEvent<MailAddr>) {
        self.0.lock().expect("lifecycle record lock").push(event);
    }
}

struct DuplicateReplacementParent;

struct ImmediateReplacementChild;

impl Behavior for ImmediateReplacementChild {
    type Addr = MailAddr;
    type Msg = Never;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        Ok(Actions::stop(Exit::Normal))
    }

    fn transition(&mut self, event: Self::Event) -> behavior::BehaviorActed<Self> {
        match event.message {}
    }
}

impl Behavior for DuplicateReplacementParent {
    type Addr = MailAddr;
    type Msg = Never;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<ImmediateReplacementChild>;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        Ok(Actions {
            sends: Vec::new(),
            creates: vec![
                Create::replacement_incarnation(7, 7, ImmediateReplacementChild),
                Create::replacement_incarnation(7, 7, ImmediateReplacementChild),
            ],
            become_: Step::Continue,
        })
    }

    fn transition(&mut self, event: Self::Event) -> behavior::BehaviorActed<Self> {
        match event.message {}
    }
}

#[tokio::test]
async fn lifecycle_facts_follow_the_exact_incarnation_edges() {
    let recorder = LifecycleRecorder::default();
    let system = System::with_lifecycle(
        MailboxConfig::bounded(1),
        AddressRouter::default(),
        recorder.clone(),
    );
    let handle = system
        .spawn(Actor::new(
            MailAddr(7),
            Compose::new(Stop).stop_on_shutdown().build(),
        ))
        .unwrap();

    yield_now().await;
    handle.actor_ref().request_shutdown().unwrap();
    assert!(matches!(
        handle.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));

    let events = recorder.0.lock().expect("lifecycle record lock");
    assert_eq!(events.len(), 5);
    assert!(events.iter().all(|event| event.address == MailAddr(7)));
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].incarnation == pair[1].incarnation)
    );
    assert_eq!(
        events
            .iter()
            .map(|event| event.transition)
            .collect::<Vec<_>>(),
        [
            LifecycleTransition::Prepared,
            LifecycleTransition::Started,
            LifecycleTransition::ShutdownRequested,
            LifecycleTransition::Retired,
            LifecycleTransition::Completed,
        ]
    );
}

#[tokio::test]
async fn reused_logical_address_gets_a_distinct_lifecycle_identity() {
    let recorder = LifecycleRecorder::default();
    let system = System::with_lifecycle(
        MailboxConfig::bounded(1),
        AddressRouter::default(),
        recorder.clone(),
    );

    for _ in 0..2 {
        let handle = system
            .spawn(Actor::new(MailAddr(7), InitBehavior::Immediate))
            .unwrap();
        assert!(matches!(
            handle.outcome().await,
            TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
        ));
    }

    let events = recorder.0.lock().expect("lifecycle record lock");
    let prepared = events
        .iter()
        .filter(|event| event.transition == LifecycleTransition::Prepared)
        .map(|event| event.incarnation.clone())
        .collect::<Vec<_>>();
    assert_eq!(prepared.len(), 2);
    assert_ne!(prepared[0], prepared[1]);
}

#[derive(Clone, Copy)]
struct PanickingLifecycleSink;

impl LifecycleSink<MailAddr, RegistrationId> for PanickingLifecycleSink {
    fn record(&self, _event: LifecycleEvent<MailAddr>) {
        panic!("injected lifecycle sink panic");
    }
}

#[tokio::test]
async fn panicking_lifecycle_sink_cannot_disrupt_actor_retirement() {
    let system = System::with_lifecycle(
        MailboxConfig::bounded(1),
        AddressRouter::default(),
        PanickingLifecycleSink,
    );
    let handle = system
        .spawn(Actor::new(MailAddr(7), InitBehavior::Immediate))
        .expect("instrumentation failure must not fail preparation");

    assert!(matches!(
        handle.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
    let replacement = system
        .spawn(Actor::new(MailAddr(7), InitBehavior::Immediate))
        .expect("instrumentation failure must not strand the registration");
    assert!(matches!(
        replacement.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
}

#[tokio::test]
async fn failed_marked_creation_emits_no_restart_for_the_rejected_installation() {
    let recorder = LifecycleRecorder::default();
    let system = System::with_lifecycle(
        MailboxConfig::bounded(1),
        AddressRouter::default(),
        recorder.clone(),
    );
    let root = system
        .spawn(Actor::new(MailAddr(1), DuplicateReplacementParent))
        .unwrap();

    assert!(matches!(
        root.outcome().await,
        TaskOutcome::Returned(Err(bombay::RunError::Environment(
            bombay::RuntimeEffectError::DuplicateChild
        )))
    ));

    let child = MailAddr(1).birth(7);
    let events = recorder.0.lock().expect("lifecycle record lock");
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.address == child && event.transition == LifecycleTransition::Restarted
            })
            .count(),
        1,
        "only the successfully installed marked creation may report restart"
    );
}
