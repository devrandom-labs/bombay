//! Frozen bombay-owned runtime-composition workloads.
//!
//! Primitive-only costs stay in their owning Bombay crates. These cases cross
//! the public bombay boundary and deliberately include registration,
//! mailbox composition, task execution, terminal publication, and retirement.

use std::hint::black_box;
use std::time::Duration;

use bombay::behavior::{
    Actions, Address, Become, Behavior, Births, Crash, Delivery, Exit, Handler, MailAddr, Never,
    NoBirths, Proxy, Pure, RestartDenial, RestartPolicy, ScheduleAfter, ServiceSends, Step,
    StopOnShutdown, Strategy, SupervisionEvent, SupervisionFailureReason, Supervisor, TimedEvent,
    TimerGeneration, TimerId, User, Watch, stop_on_supervision_failure,
};
use bombay::{
    Actor, ActorRef, AddressRouter, DeliveryRouter, EndpointRegistry, IncarnationEndpoint,
    MailboxAnchor, MailboxConfig, RunExit, System, TaskOutcome,
};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

const SENDS: usize = 1_024;

struct StopOn(u64);

impl Handler for StopOn {
    type Addr = MailAddr;
    type Msg = u64;

    fn receive(
        &mut self,
        _from: MailAddr,
        message: u64,
    ) -> bombay::behavior::Acted<MailAddr, Never, Vec<Never>, NoBirths, Never> {
        if message == self.0 {
            Ok(Actions::stop(Exit::Normal))
        } else {
            Ok(Actions::cont())
        }
    }
}

struct Waiting;

impl Handler for Waiting {
    type Addr = MailAddr;
    type Msg = Never;

    fn receive(
        &mut self,
        _from: MailAddr,
        message: Never,
    ) -> bombay::behavior::Acted<MailAddr, Never, Vec<Never>, NoBirths, Never> {
        match message {}
    }
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "Bombay Behavior's peer reaction requires the behavior error domain"
)]
fn peer_stop(
    _behavior: &mut Pure<StopOn>,
    _peer: MailAddr,
    _outcome: &Result<Exit<MailAddr>, Crash>,
) -> Result<Become<MailAddr>, Never> {
    Ok(Step::Stop(Exit::Normal))
}

#[derive(Debug)]
struct ArmTimer;

struct ArmedTimer;

impl Behavior for ArmedTimer {
    type Addr = MailAddr;
    type Msg = ArmTimer;
    type Event = TimedEvent<User<MailAddr, ArmTimer>>;
    type Sends = ServiceSends<ScheduleAfter>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, event: Self::Event) -> bombay::behavior::BehaviorActed<Self> {
        match event {
            TimedEvent::Inner(_) => Ok(Actions {
                sends: ServiceSends::one(ScheduleAfter {
                    id: TimerId(0),
                    generation: TimerGeneration(0),
                    after: Duration::ZERO,
                }),
                creates: Vec::new(),
                become_: Step::Continue,
            }),
            TimedEvent::Elapsed(elapsed) => {
                black_box(elapsed);
                Ok(Actions::stop(Exit::Normal))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TreeAddr(u64);

impl Address for TreeAddr {
    type Nonce = u64;

    fn birth(self, nonce: u64) -> Self {
        Self(self.0 * 257 + nonce + 1)
    }
}

#[derive(Debug)]
struct WorkerFailure;

struct RestartWorker;

fn restart_worker(_index: usize) -> RestartWorker {
    RestartWorker
}

fn restart_proxy_nonce(_index: usize) -> u64 {
    7
}

impl bombay::behavior::Behavior for RestartWorker {
    type Addr = TreeAddr;
    type Msg = Never;
    type Event = TimedEvent<User<TreeAddr, Never>>;
    type Sends = ServiceSends<ScheduleAfter>;
    type Ph = Never;
    type Error = WorkerFailure;
    type Birth = NoBirths;

    fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
        Ok(Actions::new(
            ServiceSends::one(ScheduleAfter {
                id: TimerId(0),
                generation: TimerGeneration(0),
                after: Duration::ZERO,
            }),
            Vec::new(),
            Step::Continue,
        ))
    }

    fn transition(&mut self, event: Self::Event) -> bombay::behavior::BehaviorActed<Self> {
        match event {
            TimedEvent::Inner(event) => match event.message {},
            TimedEvent::Elapsed(_) => Err(WorkerFailure),
        }
    }
}

enum ParentMsg {}

struct RestartParent;

impl bombay::behavior::Behavior for RestartParent {
    type Addr = TreeAddr;
    type Msg = ParentMsg;
    type Event = SupervisionEvent<User<TreeAddr, ParentMsg>>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<RestartWorker>;

    fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _event: Self::Event) -> bombay::behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type RestartSupervisor = Supervisor<RestartParent, RestartWorker>;
type RestartProxy = Proxy<RestartWorker>;
type SupervisorEndpoint =
    ActorRef<TreeAddr, MailboxAnchor<<RestartSupervisor as bombay::behavior::Behavior>::Event>>;
type ProxyEndpoint =
    ActorRef<TreeAddr, MailboxAnchor<<RestartProxy as bombay::behavior::Behavior>::Event>>;
type WorkerEndpoint =
    ActorRef<TreeAddr, MailboxAnchor<<RestartWorker as bombay::behavior::Behavior>::Event>>;
type SupervisorIncarnation = IncarnationEndpoint<TreeAddr, SupervisorEndpoint>;
type ProxyIncarnation = IncarnationEndpoint<TreeAddr, ProxyEndpoint>;
type WorkerIncarnation = IncarnationEndpoint<TreeAddr, WorkerEndpoint>;

#[derive(Clone, Default)]
struct RestartRouter {
    supervisors: AddressRouter<TreeAddr, SupervisorIncarnation>,
    proxies: AddressRouter<TreeAddr, ProxyIncarnation>,
    workers: AddressRouter<TreeAddr, WorkerIncarnation>,
}

macro_rules! register_restart {
    ($behavior:ty, $endpoint:ty, $field:ident) => {
        impl EndpointRegistry<$behavior, $endpoint> for RestartRouter {
            type Error = bombay::AddressInUse<TreeAddr>;
            type Registration = <AddressRouter<TreeAddr, $endpoint> as EndpointRegistry<
                $behavior,
                $endpoint,
            >>::Registration;

            fn register(
                &self,
                address: TreeAddr,
                endpoint: $endpoint,
            ) -> Result<Self::Registration, Self::Error> {
                EndpointRegistry::<$behavior, $endpoint>::register(&self.$field, address, endpoint)
            }
        }
    };
}

register_restart!(RestartSupervisor, SupervisorIncarnation, supervisors);
register_restart!(RestartProxy, ProxyIncarnation, proxies);
register_restart!(RestartWorker, WorkerIncarnation, workers);

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

fn restart_supervisor() -> RestartSupervisor {
    Supervisor::new(
        RestartParent,
        restart_proxy_nonce,
        1,
        restart_worker,
        Strategy::OneForOne,
        RestartPolicy::Permanent,
        1,
        Duration::MAX,
    )
    .with_failure_reaction(stop_on_supervision_failure)
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime")
}

#[allow(
    clippy::too_many_lines,
    reason = "one function keeps the seven frozen workloads in a single Criterion group"
)]
fn runtime_composition(c: &mut Criterion) {
    let runtime = runtime();
    let mut address = 0_u64;

    c.bench_function("bombay/spawn_abort_retire", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                address = address.checked_add(1).expect("address counter");
                address
            },
            |address| async move {
                let system = System::new(MailboxConfig::bounded(1), AddressRouter::default());
                let actor = system
                    .spawn(Actor::new(MailAddr(address), Pure::new(Waiting)))
                    .expect("vacant benchmark address");
                actor.abort();
                black_box(actor.outcome().await);
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("bombay/send_1024_then_stop", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                address = address.checked_add(1).expect("address counter");
                let system = System::new(MailboxConfig::bounded(SENDS), AddressRouter::default());
                system
                    .spawn(Actor::new(
                        MailAddr(address),
                        Pure::new(StopOn(
                            u64::try_from(SENDS - 1).expect("send count fits u64"),
                        )),
                    ))
                    .expect("vacant benchmark address")
            },
            |actor| async {
                for message in 0..SENDS {
                    actor
                        .actor_ref()
                        .send(
                            MailAddr(0),
                            u64::try_from(message).expect("send index fits u64"),
                        )
                        .await
                        .expect("benchmark delivery");
                }
                assert!(matches!(
                    actor.outcome().await,
                    TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
                ));
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("bombay/stop_and_retire", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                address = address.checked_add(1).expect("address counter");
                let system = System::new(MailboxConfig::bounded(1), AddressRouter::default());
                system
                    .spawn(Actor::new(MailAddr(address), Pure::new(StopOn(0))))
                    .expect("vacant benchmark address")
            },
            |actor| async {
                actor
                    .actor_ref()
                    .send(MailAddr(0), 0)
                    .await
                    .expect("benchmark stop delivery");
                assert!(matches!(
                    actor.outcome().await,
                    TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
                ));
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("bombay/arm_due_timer_and_retire", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                address = address.checked_add(1).expect("address counter");
                let system = System::new(MailboxConfig::bounded(1), AddressRouter::default());
                system
                    .spawn(Actor::new(MailAddr(address), ArmedTimer))
                    .expect("vacant benchmark address")
            },
            |actor| async move {
                actor
                    .actor_ref()
                    .send(MailAddr(0), ArmTimer)
                    .await
                    .expect("timer arm delivery");
                assert!(matches!(
                    actor.outcome().await,
                    TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
                ));
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("boundary/tokio_sleep_until_now", |b| {
        b.to_async(&runtime).iter(|| async {
            tokio::time::sleep_until(tokio::time::Instant::now()).await;
        });
    });

    c.bench_function("bombay/watch_peer_and_retire", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                address = address.checked_add(2).expect("address counter");
                let system = System::new(MailboxConfig::bounded(1), AddressRouter::default());
                let peer_address = MailAddr(address - 1);
                let watcher_address = MailAddr(address);
                let peer = system
                    .spawn(Actor::new(
                        peer_address,
                        Watch::new(Pure::new(StopOn(0)), peer_address, peer_stop),
                    ))
                    .expect("vacant peer address");
                let watcher = system
                    .spawn(Actor::new(
                        watcher_address,
                        Watch::new(Pure::new(StopOn(u64::MAX)), peer_address, peer_stop),
                    ))
                    .expect("vacant watcher address");
                (peer, watcher)
            },
            |(peer, watcher)| async move {
                tokio::task::yield_now().await;
                peer.actor_ref()
                    .send(MailAddr(0), 0)
                    .await
                    .expect("peer stop delivery");
                assert!(matches!(
                    peer.outcome().await,
                    TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
                ));
                assert!(matches!(
                    watcher.outcome().await,
                    TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
                ));
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("bombay/restart_once_and_retire_tree", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let system = System::new(MailboxConfig::bounded(4), RestartRouter::default());
                system
                    .spawn(Actor::new(TreeAddr(1), restart_supervisor()))
                    .expect("vacant supervisor address")
            },
            |supervisor| async move {
                let outcome = supervisor.outcome().await;
                assert!(matches!(
                    &outcome,
                    TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::SupervisionFailed(
                        SupervisionFailureReason::RestartDenied(RestartDenial::BudgetExceeded {
                            restarts_in_window: 1,
                            replacements_requested: 1,
                            maximum_restarts: 1,
                        })
                    ))))
                ));
                black_box(outcome);
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("bombay/coordinated_shutdown", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                address = address.checked_add(1).expect("address counter");
                let system = System::new(MailboxConfig::bounded(1), AddressRouter::default());
                system
                    .spawn(Actor::new(
                        MailAddr(address),
                        StopOnShutdown::new(Pure::new(Waiting)),
                    ))
                    .expect("vacant benchmark address")
            },
            |actor| async {
                actor
                    .actor_ref()
                    .request_shutdown()
                    .expect("benchmark shutdown request");
                assert!(matches!(
                    actor.outcome().await,
                    TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
                ));
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, runtime_composition);
criterion_main!(benches);
