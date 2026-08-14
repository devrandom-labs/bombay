//! One complete local-runtime composition using only the facade prelude.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bombay::{AddressInUse, DeliveryRouter, EndpointRegistry, IncarnationEndpoint};
use bombay_framework::prelude::*;
use tokio::task::yield_now;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct AppAddr(u64);

impl Address for AppAddr {
    type Nonce = u64;

    fn birth(self, nonce: u64) -> Self {
        Self(self.0 * 257 + nonce + 1)
    }
}

struct Ping;
struct WorkerFailure;

static DELIVERED: OnceLock<Arc<AtomicBool>> = OnceLock::new();
static TIMER_FIRED: OnceLock<Arc<AtomicBool>> = OnceLock::new();
static IDLE_FIRED: OnceLock<Arc<AtomicBool>> = OnceLock::new();
static WORKER_STARTS: OnceLock<Arc<AtomicUsize>> = OnceLock::new();

struct Root;

#[allow(clippy::unused_self, clippy::unnecessary_wraps)]
#[behavior::behavior(
    addr = AppAddr,
    message = Ping,
    sends = Vec<Never>,
    births = Births<Worker>,
    error = Never,
)]
impl Root {
    fn init(&mut self) -> behavior::Acted<AppAddr, Never, Vec<Never>, Births<Worker>, Never> {
        Ok(Actions::cont())
    }

    fn receive(
        &mut self,
        _from: AppAddr,
        _message: Ping,
    ) -> behavior::Acted<AppAddr, Never, Vec<Never>, Births<Worker>, Never> {
        DELIVERED
            .get()
            .expect("delivery probe")
            .store(true, Ordering::SeqCst);
        Ok(Actions::cont())
    }
}

struct Worker;

impl Behavior for Worker {
    type Addr = AppAddr;
    type Msg = Never;
    type Event = behavior::TimedEvent<User<AppAddr, Never>>;
    type Sends = ServiceSends<behavior::ScheduleAfter>;
    type Ph = Never;
    type Error = WorkerFailure;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> behavior::BehaviorActed<Self> {
        let start = WORKER_STARTS
            .get()
            .expect("worker probe")
            .fetch_add(1, Ordering::SeqCst);
        if start == 0 {
            Ok(Actions::new(
                ServiceSends::one(behavior::ScheduleAfter {
                    id: behavior::TimerId(0),
                    generation: behavior::TimerGeneration(0),
                    after: Duration::ZERO,
                }),
                Vec::new(),
                Step::Continue,
            ))
        } else {
            Ok(Actions::cont())
        }
    }

    fn transition(
        &mut self,
        _: behavior::ActiveTurn,
        event: Self::Event,
    ) -> behavior::BehaviorActed<Self> {
        match event {
            behavior::TimedEvent::Behavior(event) => match event.message {},
            behavior::TimedEvent::Elapsed(_) => Err(WorkerFailure),
        }
    }
}

fn worker(_index: usize) -> Option<Worker> {
    Some(Worker)
}

fn proxy_nonce(_index: usize) -> u64 {
    7
}

type RootSupervisor = Supervisor<Root, Worker>;

#[allow(
    clippy::unnecessary_wraps,
    reason = "Bombay Behavior's timer-reaction function pointer returns the behavior error domain"
)]
fn timer_fired(
    _supervisor: &mut RootSupervisor,
) -> Result<behavior::Become<AppAddr>, <RootSupervisor as Behavior>::Error> {
    TIMER_FIRED
        .get()
        .expect("timer probe")
        .store(true, Ordering::SeqCst);
    Ok(Step::Continue)
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "Bombay Behavior's receive-timeout reaction shares the inner error domain"
)]
fn idle_fired(
    _application: &mut Deadline<RootSupervisor>,
) -> behavior::BehaviorActed<Deadline<RootSupervisor>> {
    IDLE_FIRED
        .get()
        .expect("idle-timeout probe")
        .store(true, Ordering::SeqCst);
    Ok(Actions::cont())
}

type Application = StopOnShutdown<ReceiveTimeout<Deadline<RootSupervisor>>>;
type WorkerProxy = Proxy<Worker>;

type RootEndpoint = ActorRef<AppAddr, MailboxAnchor<<Application as Behavior>::Event>>;
type ProxyEndpoint = ActorRef<AppAddr, MailboxAnchor<<WorkerProxy as Behavior>::Event>>;
type WorkerEndpoint = ActorRef<AppAddr, MailboxAnchor<<Worker as Behavior>::Event>>;

type RootIncarnation = IncarnationEndpoint<AppAddr, RootEndpoint>;
type ProxyIncarnation = IncarnationEndpoint<AppAddr, ProxyEndpoint>;
type WorkerIncarnation = IncarnationEndpoint<AppAddr, WorkerEndpoint>;

#[derive(Clone, Default)]
struct ApplicationRoutes {
    roots: AddressRouter<AppAddr, RootIncarnation>,
    proxies: AddressRouter<AppAddr, ProxyIncarnation>,
    workers: AddressRouter<AppAddr, WorkerIncarnation>,
}

macro_rules! register_with {
    ($behavior:ty, $endpoint:ty, $field:ident) => {
        impl EndpointRegistry<$behavior, $endpoint> for ApplicationRoutes {
            type Error = AddressInUse<AppAddr>;
            type Registration = <AddressRouter<AppAddr, $endpoint> as EndpointRegistry<
                $behavior,
                $endpoint,
            >>::Registration;

            fn register(
                &self,
                address: AppAddr,
                endpoint: $endpoint,
            ) -> Result<Self::Registration, Self::Error> {
                EndpointRegistry::<$behavior, $endpoint>::register(&self.$field, address, endpoint)
            }
        }
    };
}

register_with!(Application, RootIncarnation, roots);
register_with!(WorkerProxy, ProxyIncarnation, proxies);
register_with!(Worker, WorkerIncarnation, workers);

impl DeliveryRouter<WorkerProxy> for ApplicationRoutes {
    type Error = <AddressRouter<AppAddr, ProxyIncarnation> as DeliveryRouter<WorkerProxy>>::Error;

    async fn deliver(
        &self,
        from: AppAddr,
        delivery: Delivery<WorkerProxy>,
    ) -> Result<(), Self::Error> {
        self.proxies.deliver(from, delivery).await
    }
}

impl DeliveryRouter<Worker> for ApplicationRoutes {
    type Error = <AddressRouter<AppAddr, WorkerIncarnation> as DeliveryRouter<Worker>>::Error;

    async fn deliver(&self, from: AppAddr, delivery: Delivery<Worker>) -> Result<(), Self::Error> {
        self.workers.deliver(from, delivery).await
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    run().await;
}

fn application() -> Compose<Application> {
    Compose::new(Root)
        .children(proxy_nonce, 1, worker)
        .expect("application supervisor topology")
        .restart(Strategy::OneForOne)
        .when(RestartPolicy::Permanent)
        .within(1, Duration::MAX)
        .deadline(
            behavior::TimerId(0),
            Some(Instant::now().into_std()),
            timer_fired,
        )
        .receive_timeout(behavior::TimerId(1), Duration::from_millis(10), idle_fired)
        .stop_on_shutdown()
}

async fn run() {
    let delivered = DELIVERED.get_or_init(|| Arc::new(AtomicBool::new(false)));
    let timer = TIMER_FIRED.get_or_init(|| Arc::new(AtomicBool::new(false)));
    let idle = IDLE_FIRED.get_or_init(|| Arc::new(AtomicBool::new(false)));
    let starts = WORKER_STARTS.get_or_init(|| Arc::new(AtomicUsize::new(0)));

    let system = local_system!(
        mailbox = MailboxConfig::bounded(8),
        routes = ApplicationRoutes::default(),
    );
    let handle = system
        .spawn(Actor::from_definition(AppAddr(1), application()))
        .expect("the root address is vacant");

    assert!(
        handle.actor_ref().send(AppAddr(0), Ping).await.is_ok(),
        "typed root delivery succeeds"
    );
    while !delivered.load(Ordering::SeqCst)
        || !timer.load(Ordering::SeqCst)
        || starts.load(Ordering::SeqCst) < 2
    {
        yield_now().await;
    }
    assert!(
        !idle.load(Ordering::SeqCst),
        "accepted user traffic resets the idle period"
    );
    while !idle.load(Ordering::SeqCst) {
        yield_now().await;
    }

    handle
        .actor_ref()
        .request_shutdown()
        .expect("the root accepts coordinated shutdown");
    assert!(matches!(
        handle.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));

    let replacement = system
        .spawn(Actor::from_definition(AppAddr(1), application()))
        .expect("root completion implies complete tree retirement");
    replacement
        .actor_ref()
        .request_shutdown()
        .expect("the replacement root accepts shutdown");
    assert!(matches!(
        replacement.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));

    println!(
        "delivery, absolute timer, receive timeout, observation, restart, and transitive shutdown completed"
    );
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn reference_application_composes_every_required_runtime_leg() {
        tokio::time::timeout(std::time::Duration::from_secs(2), super::run())
            .await
            .expect("the complete reference composition must terminate");
    }
}
