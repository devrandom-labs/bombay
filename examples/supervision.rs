use bombay::prelude::*;

struct Worker;

impl Behavior for Worker {
    type Protocol = behavior::MessageProtocol<MailAddr, Never>;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

type ManagedWorker = StopOnShutdown<Worker>;
type ManagedReply = StopOnShutdown<SupervisorReply>;
type Workers = DynamicSupervisor<MailAddr, ManagedWorker, SupervisorReply>;
type SystemProtocol = MessageProtocol<MailAddr, SystemMessage>;
type SupervisorReply =
    MessageAdapter<DynamicSupervisorOutcome<MailAddr, ManagedWorker>, SystemProtocol>;

type QueryReply = Machine<MailAddr, (), PoolResponse<u8, u16, MailAddr>, (), Never>;
type QueryPoolProtocol = WorkerPoolProtocol<MailAddr, QueryReply, u8, u16>;
type QueryWorker = MessageAdapter<PoolAssignment<QueryPoolProtocol>, QueryPoolProtocol>;
type ManagedQueryWorker = StopOnShutdown<QueryWorker>;
type QueryPool = WorkerPool<MailAddr, QueryReply, u8, u16, ManagedQueryWorker>;
type ManagedQueryReply = StopOnShutdown<QueryReply>;

#[allow(
    clippy::needless_pass_by_value,
    reason = "MessageAdapter consumes each owned input through a function pointer"
)]
fn complete_query(
    assignment: PoolAssignment<QueryPoolProtocol>,
) -> PoolMessage<MailAddr, QueryReply, u8, u16> {
    PoolMessage::Completed {
        worker: assignment.worker,
        assignment: assignment.assignment,
        result: u16::from(assignment.payload) * 2,
    }
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "ChildTopology factories may reject individual configured slots"
)]
fn query_worker(_: usize) -> Option<ManagedQueryWorker> {
    Some(StopOnShutdown::new(MessageAdapter::new(
        Recipient::global(MailAddr(0).birth(3)),
        complete_query,
    )))
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    clippy::unnecessary_wraps,
    reason = "Machine transitions use the template's exact fallible borrowed-message signature"
)]
fn retain_query_response(
    _: (),
    _: &mut (),
    _: &PoolResponse<u8, u16, MailAddr>,
) -> Result<Move<()>, Never> {
    Ok(Move::Stay)
}

struct QueryStarter;

impl Behavior for QueryStarter {
    type Protocol = MessageProtocol<MailAddr, Never>;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Delivery<QueryPoolProtocol>>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::send(vec![Delivery::new(
            Recipient::global(MailAddr(0).birth(3)),
            PoolMessage::Submit {
                job: JobId(1),
                payload: 21,
                reply_to: Recipient::global(MailAddr(0).birth(4)),
            },
        )]))
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

type ManagedQueryStarter = StopOnShutdown<QueryStarter>;

enum SystemMessage {
    SupervisorChanged,
}

fn supervisor_outcome(_: DynamicSupervisorOutcome<MailAddr, ManagedWorker>) -> SystemMessage {
    SystemMessage::SupervisorChanged
}

type SystemChildren = ChildChoice<
    ManagedQueryStarter,
    ChildChoice<
        ManagedQueryReply,
        ChildChoice<QueryPool, ChildChoice<ManagedReply, ChildChoice<Workers, Never>>>,
    >,
>;

#[derive(Debug, thiserror::Error)]
enum SystemError {
    #[error(transparent)]
    Pool(PoolConfigError<u64>),
    #[error(transparent)]
    Children(ChildrenError<u64>),
}

struct System;

impl Behavior for System {
    type Protocol = behavior::MessageProtocol<MailAddr, SystemMessage>;
    type Event = User<MailAddr, SystemMessage>;
    type Sends = Vec<Delivery<Workers>>;
    type Ph = Never;
    type Error = SystemError;
    type Birth = Births<SystemChildren>;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        let pool = WorkerPool::new(
            ChildTopology::new([10, 11], query_worker),
            PoolConfiguration::new(
                8,
                InterruptionPolicy::Retry,
                RestartPolicy::Transient,
                3,
                std::time::Duration::from_secs(1),
            ),
            Recipient::global(MailAddr(0).birth(3)),
        )
        .map_err(SystemError::Pool)?;
        let children = Children::<MailAddr>::new()
            .child(1, Workers::new())
            .child(
                2,
                StopOnShutdown::new(MessageAdapter::new(
                    Recipient::<SystemProtocol>::global(MailAddr(0)),
                    supervisor_outcome,
                )),
            )
            .child(3, pool)
            .child(
                4,
                StopOnShutdown::new(Machine::new((), (), retain_query_response)),
            )
            .child(5, StopOnShutdown::new(QueryStarter))
            .into_creates()
            .map_err(SystemError::Children)?;
        let start = DynamicSupervisorMessage::Start {
            nonce: 7,
            child: StopOnShutdown::new(Worker),
            reply_to: Recipient::global(MailAddr(0).birth(2)),
        };
        Ok(Actions::new(
            vec![Delivery::local_child(ChildRecipient::new(1), start)],
            children,
            Step::Continue,
        ))
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {
            SystemMessage::SupervisorChanged => Ok(Actions::cont()),
        }
    }
}

type ShutdownTargets = ShutdownChoice<
    Workers,
    ShutdownChoice<
        ManagedReply,
        ShutdownChoice<
            QueryPool,
            ShutdownChoice<
                ManagedQueryReply,
                ShutdownChoice<ManagedQueryStarter, NoShutdownTargets<MailAddr>>,
            >,
        >,
    >,
>;
type CoordinatedSystem = HeterogeneousShutdownCoordinator<System, ShutdownTargets>;
type Application = OneShot<CoordinatedSystem>;

#[allow(
    clippy::unnecessary_wraps,
    reason = "OneShot reactions use the exact fallible Behavior result signature"
)]
fn shutdown_system(system: &mut CoordinatedSystem) -> BehaviorActed<CoordinatedSystem> {
    behavior::delegate_transition(
        system,
        ShutdownCoordinatorEvent::Requested(ShutdownRequested),
    )
}

fn application() -> Result<Application, ShutdownPlanError<u64>> {
    let shutdown = HeterogeneousShutdownPlan::new([
        vec![ShutdownTargets::other(ShutdownChoice::other(
            ShutdownChoice::other(ShutdownChoice::other(ShutdownChoice::child(5))),
        ))],
        vec![
            ShutdownTargets::child(1),
            ShutdownTargets::other(ShutdownChoice::other(ShutdownChoice::child(3))),
        ],
        vec![
            ShutdownTargets::other(ShutdownChoice::child(2)),
            ShutdownTargets::other(ShutdownChoice::other(ShutdownChoice::other(
                ShutdownChoice::child(4),
            ))),
        ],
    ])?;
    Ok(OneShot::new(
        HeterogeneousShutdownCoordinator::new(System, shutdown),
        TimerId(1),
        std::time::Duration::from_millis(10),
        shutdown_system,
    ))
}

#[derive(Default)]
struct AppActors {
    system: ActorSpace<SystemProtocol>,
    workers: ActorSpace<Workers>,
    supervisor_reply: ActorSpace<SupervisorReply>,
    worker_proxy: ActorSpace<DynamicProxy<ManagedWorker>>,
    query_pool: ActorSpace<QueryPoolProtocol>,
    query_reply: ActorSpace<QueryReply>,
    query_proxy: ActorSpace<Proxy<ManagedQueryWorker>>,
    query_worker: ActorSpace<QueryWorker>,
    quiet: ActorSpace<MessageProtocol<MailAddr, Never>>,
}

impl Hosts<SystemProtocol> for AppActors {
    fn space(&self) -> &ActorSpace<SystemProtocol> {
        &self.system
    }
}

impl Hosts<Workers> for AppActors {
    fn space(&self) -> &ActorSpace<Workers> {
        &self.workers
    }
}

impl Hosts<SupervisorReply> for AppActors {
    fn space(&self) -> &ActorSpace<SupervisorReply> {
        &self.supervisor_reply
    }
}

impl Hosts<DynamicProxy<ManagedWorker>> for AppActors {
    fn space(&self) -> &ActorSpace<DynamicProxy<ManagedWorker>> {
        &self.worker_proxy
    }
}

impl Hosts<QueryPoolProtocol> for AppActors {
    fn space(&self) -> &ActorSpace<QueryPoolProtocol> {
        &self.query_pool
    }
}

impl Hosts<QueryReply> for AppActors {
    fn space(&self) -> &ActorSpace<QueryReply> {
        &self.query_reply
    }
}

impl Hosts<Proxy<ManagedQueryWorker>> for AppActors {
    fn space(&self) -> &ActorSpace<Proxy<ManagedQueryWorker>> {
        &self.query_proxy
    }
}

impl Hosts<QueryWorker> for AppActors {
    fn space(&self) -> &ActorSpace<QueryWorker> {
        &self.query_worker
    }
}

impl Hosts<MessageProtocol<MailAddr, Never>> for AppActors {
    fn space(&self) -> &ActorSpace<MessageProtocol<MailAddr, Never>> {
        &self.quiet
    }
}

fn main() -> Result<(), RunError> {
    App::new(
        application().expect("the static shutdown topology is valid"),
        AppActors::default(),
    )
    .run()
}
