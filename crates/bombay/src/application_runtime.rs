//! Manifest-backed creation and delivery capabilities.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use behavior::{
    Address, Behavior, BehaviorAddr, BehaviorMessage, BirthMode, ChildShutdownRejected,
    ChildShutdownRejection, ChildStopped, Create, CreationResolved, Delivery, DispatchBirth, Exit,
    Guardian, InjectEvent, MailAddr, Never, ObserveCreation, ObservePeer, Protocol,
    ReportSupervisionFailure, ReportWorkerCreationResolved, ReportWorkerStopped, ScheduleAfter,
    ScheduleAt, ShutdownChild, ShutdownRequested, TimerElapsed, UnwatchPeer,
};
use communication::ControlSender;

use crate::generation::TerminalOverride;
use crate::interpret::{
    ActionInterpreter, BirthInstaller, CreationResults, CreationTransaction, RetireCapabilities,
    SpawnChild,
};
use crate::observation::LocalPeerObservations;
use crate::reports::{LocalParentReports, LocalSupervisionReports};
use crate::time::LocalTimers;
use crate::topology::Hosts;

const DEFAULT_USER_CAPACITY: usize = 1_024;

/// Failure at the complete local application boundary.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// Tokio could not construct the application executor.
    #[error("Bombay could not construct its Tokio runtime")]
    Runtime(#[source] std::io::Error),
    /// The Guardian root failed before becoming live.
    #[error("the Bombay application root could not start: {0:?}")]
    Start(behavior::CreationRejection),
    /// The live root terminated abnormally.
    #[error("the Bombay application root crashed: {0:?}")]
    Crash(behavior::Crash),
    /// The live root stopped because a linked peer or supervised topology failed.
    #[error("the Bombay application root exited abnormally: {0:?}")]
    AbnormalExit(Exit<MailAddr>),
}

/// Private actor-system launch capability.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "the local actor spaces cannot execute root behavior `{A}`",
    label = "incomplete local actor system",
    note = "inspect the required `Hosts<Protocol>` bound and add that protocol's `ActorSpace`"
)]
pub(crate) trait LaunchSystem<A: Behavior<Protocol: Protocol<Addr = MailAddr>>> {
    fn launch(self, root: A) -> impl Future<Output = Result<(), RunError>> + Send;
}

impl<A, N> LaunchSystem<A> for N
where
    A: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    N: Hosts<A::Protocol> + Send + Sync + 'static,
    Guardian<A>: Behavior<Protocol = A::Protocol, Ph = Never> + Send + 'static,
    <Guardian<A> as Behavior>::Event: behavior::InjectEvent<ShutdownRequested, behavior::Here> + Send + 'static,
    BehaviorMessage<A>: Send + 'static,
    <Guardian<A> as Behavior>::Sends: Send + 'static,
    <Guardian<A> as Behavior>::Error: Send + 'static,
    <<Guardian<A> as Behavior>::Birth as BirthMode>::Child: Send + 'static,
    ActionInterpreter<MailAddr, ApplicationCapabilities<Guardian<A>, N>>:
        crate::local::CommitActions<Guardian<A>> + Send + 'static,
    <ActionInterpreter<MailAddr, ApplicationCapabilities<Guardian<A>, N>> as crate::local::CommitActions<
        Guardian<A>,
    >>::Error: Send + 'static,
{
    async fn launch(self, root: A) -> Result<(), RunError> {
        let actor_spaces = Arc::new(self);
        let roots = <N as Hosts<A::Protocol>>::space(&actor_spaces).clone();
        let address = MailAddr(0);
        let actor = crate::launch::spawn_with(
            roots,
            communication::Config::new(DEFAULT_USER_CAPACITY),
            address,
            Guardian::new(root),
            {
                let actor_spaces = actor_spaces.clone();
                move |control, termination, timers, facts| {
                    ActionInterpreter::new(
                        address,
                        ApplicationCapabilities::<Guardian<A>, N>::new(
                            address,
                            actor_spaces,
                            control,
                            termination,
                            timers,
                            facts,
                        ),
                    )
                }
            },
        )
            .await
            .map_err(|error| RunError::Start(error.rejection()))?;

        match actor.termination().await {
            Ok(Exit::Normal | Exit::Collected) => Ok(()),
            Ok(exit) => Err(RunError::AbnormalExit(exit)),
            Err(crash) => Err(RunError::Crash(crash)),
        }
    }
}

/// One complete actor application.
///
/// `R` is the pure root Behavior. `A` is the named product of runtime-owned
/// local actor spaces. Bombay separates them before execution begins.
pub struct App<R, A> {
    root: R,
    actors: A,
}

impl<R, A> App<R, A> {
    #[must_use]
    pub const fn new(root: R, actors: A) -> Self {
        Self { root, actors }
    }
}

#[allow(
    private_bounds,
    reason = "LaunchSystem is Bombay's sealed implementation proof, not application API"
)]
impl<R, A> App<R, A>
where
    R: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    A: LaunchSystem<R>,
{
    /// Run this application to root termination on Bombay's owned executor.
    ///
    /// # Errors
    ///
    /// Returns [`RunError`] when executor construction, root activation, or
    /// root termination fails.
    pub fn run(self) -> Result<(), RunError> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(RunError::Runtime)?
            .block_on(<A as LaunchSystem<R>>::launch(self.actors, self.root))
    }
}

pub(crate) struct ApplicationCapabilities<C, N, P = NoParent>
where
    C: Behavior,
    BehaviorAddr<C>: core::hash::Hash,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash,
{
    actor_spaces: Arc<N>,
    address: BehaviorAddr<C>,
    control: ControlSender<C::Event>,
    creations: CreationResults<BehaviorAddr<C>>,
    timers: LocalTimers<C::Event>,
    facts: crate::observation::FactQueue<C::Event>,
    peers: Option<LocalPeerObservations<C::Protocol, C::Event>>,
    supervision_reports: LocalSupervisionReports<BehaviorAddr<C>>,
    owned_children: HashSet<<BehaviorAddr<C> as Address>::Nonce>,
    child_tasks: HashMap<<BehaviorAddr<C> as Address>::Nonce, crate::launch::OwnedTask>,
    child_terminations: HashMap<
        <BehaviorAddr<C> as Address>::Nonce,
        observe::Observation<crate::local::Termination<BehaviorAddr<C>>>,
    >,
    child_observations: HashMap<<BehaviorAddr<C> as Address>::Nonce, u64>,
    stopping_children: HashSet<<BehaviorAddr<C> as Address>::Nonce>,
    parent_reports: P,
}

pub(crate) struct NoParent;

#[derive(Debug, thiserror::Error)]
pub(crate) enum InterpretationFailure {
    #[error(transparent)]
    Timer(#[from] crate::time::TimerError),
    #[error("no live actor exists at {0:?}")]
    Unknown(MailAddr),
    #[error("the actor mailbox at {0:?} is closed")]
    Closed(MailAddr),
}

impl<C, N, P> behavior::SendInterpreter for ApplicationCapabilities<C, N, P>
where
    C: Behavior,
    BehaviorAddr<C>: core::hash::Hash + Send,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash + Send,
    Self: Send,
{
    type Error = InterpretationFailure;
}

impl<C, N, P, Path> behavior::InterpretRequest<ScheduleAt, C::Event, Path>
    for ApplicationCapabilities<C, N, P>
where
    C: Behavior,
    BehaviorAddr<C>: core::hash::Hash + Send,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash + Send,
    C::Event: behavior::InjectEvent<TimerElapsed, Path> + Send,
    Self: Send,
{
    async fn interpret_request(&mut self, request: ScheduleAt) -> Result<(), Self::Error> {
        self.timers.schedule_at::<Path>(request);
        Ok(())
    }
}

impl<C, N, P, Path> behavior::InterpretRequest<ScheduleAfter, C::Event, Path>
    for ApplicationCapabilities<C, N, P>
where
    C: Behavior,
    BehaviorAddr<C>: core::hash::Hash + Send,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash + Send,
    C::Event: behavior::InjectEvent<TimerElapsed, Path> + Send,
    Self: Send,
{
    async fn interpret_request(&mut self, request: ScheduleAfter) -> Result<(), Self::Error> {
        self.timers.schedule_after::<Path>(request)?;
        Ok(())
    }
}

impl<C, N, P, Target> behavior::InterpretDelivery<Target> for ApplicationCapabilities<C, N, P>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send,
    N: Hosts<Target> + Send + Sync,
    Self: Send,
{
    async fn interpret_delivery(&mut self, delivery: Delivery<Target>) -> Result<(), Self::Error> {
        let address = delivery.to.resolve(self.address);
        let actor = self
            .actor_spaces
            .space()
            .resolve(&address)
            .ok_or(InterpretationFailure::Unknown(address))?;
        actor
            .send(self.address, delivery.message)
            .await
            .map_err(|_| InterpretationFailure::Closed(address))
    }
}

impl<C, N, P, Path> behavior::InterpretRequest<ObserveCreation<BehaviorAddr<C>>, C::Event, Path>
    for ApplicationCapabilities<C, N, P>
where
    C: Behavior,
    BehaviorAddr<C>: core::hash::Hash + Send,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash + Send,
    C::Event: behavior::InjectEvent<CreationResolved<BehaviorAddr<C>>, Path> + Send,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ObserveCreation<BehaviorAddr<C>>,
    ) -> Result<(), Self::Error> {
        if let Some(result) = self.creations.resolve(&request.nonce) {
            let _ = self.control.send(C::Event::inject_at(result));
        }
        Ok(())
    }
}

impl<C, N, P, Path> behavior::InterpretRequest<ObservePeer<BehaviorAddr<C>>, C::Event, Path>
    for ApplicationCapabilities<C, N, P>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    BehaviorAddr<C>: core::hash::Hash + Send + Sync + 'static,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash + Send,
    C::Event: InjectEvent<behavior::PeerStopped<BehaviorAddr<C>>, Path> + Send + 'static,
    BehaviorMessage<C>: Send,
    N: Hosts<C::Protocol>,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ObservePeer<BehaviorAddr<C>>,
    ) -> Result<(), Self::Error> {
        self.peers
            .get_or_insert_with(|| {
                LocalPeerObservations::new(self.actor_spaces.space().clone(), self.facts.clone())
            })
            .observe::<Path>(request)
            .map_err(|crate::observation::ObservationError::Unknown(address)| {
                InterpretationFailure::Unknown(address)
            })
    }
}

impl<C, N, P, Path> behavior::InterpretRequest<UnwatchPeer<BehaviorAddr<C>>, C::Event, Path>
    for ApplicationCapabilities<C, N, P>
where
    C: Behavior,
    BehaviorAddr<C>: core::hash::Hash + Send + Sync,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash + Send,
    C::Event: Send,
    BehaviorMessage<C>: Send,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: UnwatchPeer<BehaviorAddr<C>>,
    ) -> Result<(), Self::Error> {
        if let Some(peers) = &mut self.peers {
            peers.unwatch(request);
        }
        Ok(())
    }
}

impl<C, N, P, Path>
    behavior::InterpretRequest<behavior::ObserveChild<BehaviorAddr<C>>, C::Event, Path>
    for ApplicationCapabilities<C, N, P>
where
    C: Behavior,
    BehaviorAddr<C>: core::hash::Hash + Send + Sync + 'static,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash + Send + Sync + 'static,
    C::Event: InjectEvent<ChildStopped<BehaviorAddr<C>>, Path> + Send + 'static,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: behavior::ObserveChild<BehaviorAddr<C>>,
    ) -> Result<(), Self::Error> {
        if !self.owned_children.contains(&request.nonce) {
            return Ok(());
        }
        let Some(observation) = self.child_terminations.get(&request.nonce).cloned() else {
            return Ok(());
        };
        let nonce = request.nonce;
        let task = self.facts.insert(async move {
            C::Event::inject_at(ChildStopped::new(nonce, observation.await, Instant::now()))
        });
        if let Some(previous) = self.child_observations.insert(nonce, task) {
            self.facts.remove(previous);
        }
        Ok(())
    }
}

impl<Owner, N, P, Child, Path> behavior::InterpretRequest<ShutdownChild<Child>, Owner::Event, Path>
    for ApplicationCapabilities<Owner, N, P>
where
    Owner: Behavior,
    BehaviorAddr<Owner>: core::hash::Hash + Send,
    <BehaviorAddr<Owner> as Address>::Nonce: core::hash::Hash + Send,
    Owner::Event:
        InjectEvent<ChildShutdownRejected<<BehaviorAddr<Owner> as Address>::Nonce>, Path> + Send,
    Child: Behavior<Protocol: behavior::Protocol<Addr = BehaviorAddr<Owner>>>,
    Child::Event: InjectEvent<ShutdownRequested, behavior::Here> + Send,
    N: Hosts<Child::Protocol>,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ShutdownChild<Child>,
    ) -> Result<(), Self::Error> {
        let rejection = if self.stopping_children.contains(&request.nonce) {
            Some(ChildShutdownRejection::AlreadyStopping)
        } else if self.owned_children.contains(&request.nonce) {
            let child = self
                .actor_spaces
                .space()
                .resolve(&self.address.birth(request.nonce));
            if child.is_some_and(|child| child.request_shutdown()) {
                self.stopping_children.insert(request.nonce);
                None
            } else {
                Some(ChildShutdownRejection::NotEstablished)
            }
        } else {
            Some(ChildShutdownRejection::NotEstablished)
        };

        if let Some(reason) = rejection {
            let _ = self
                .control
                .send(Owner::Event::inject_at(ChildShutdownRejected::new(
                    request.nonce,
                    reason,
                )));
        }
        Ok(())
    }
}

impl<C, N, P, Path>
    behavior::InterpretRequest<ReportSupervisionFailure<BehaviorAddr<C>>, C::Event, Path>
    for ApplicationCapabilities<C, N, P>
where
    C: Behavior,
    BehaviorAddr<C>: core::hash::Hash + Send + Sync,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash + Send,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ReportSupervisionFailure<BehaviorAddr<C>>,
    ) -> Result<(), Self::Error> {
        self.supervision_reports.report(request);
        Ok(())
    }
}

impl<C, N, P, ParentPath, Path>
    behavior::InterpretRequest<ReportWorkerStopped<BehaviorAddr<C>, ParentPath>, C::Event, Path>
    for ApplicationCapabilities<C, N, P>
where
    C: Behavior,
    BehaviorAddr<C>: core::hash::Hash + Send,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash + Send,
    P: crate::reports::ParentReporting<BehaviorAddr<C>, ParentPath> + Send,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ReportWorkerStopped<BehaviorAddr<C>, ParentPath>,
    ) -> Result<(), Self::Error> {
        self.parent_reports.stopped(request);
        Ok(())
    }
}

impl<C, N, P, ParentPath, Path>
    behavior::InterpretRequest<
        ReportWorkerCreationResolved<<BehaviorAddr<C> as Address>::Nonce, ParentPath>,
        C::Event,
        Path,
    > for ApplicationCapabilities<C, N, P>
where
    C: Behavior,
    BehaviorAddr<C>: core::hash::Hash + Send,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash + Send,
    P: crate::reports::ParentReporting<BehaviorAddr<C>, ParentPath> + Send,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ReportWorkerCreationResolved<<BehaviorAddr<C> as Address>::Nonce, ParentPath>,
    ) -> Result<(), Self::Error> {
        self.parent_reports.created(request);
        Ok(())
    }
}

impl<C, N> ApplicationCapabilities<C, N, NoParent>
where
    C: Behavior,
    BehaviorAddr<C>: core::hash::Hash,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash + Send,
{
    pub(crate) fn new(
        address: BehaviorAddr<C>,
        actor_spaces: Arc<N>,
        control: ControlSender<C::Event>,
        termination: Arc<TerminalOverride<BehaviorAddr<C>>>,
        timers: LocalTimers<C::Event>,
        facts: crate::observation::FactQueue<C::Event>,
    ) -> Self {
        let creations = CreationResults::new();
        Self {
            address,
            timers,
            facts,
            peers: None,
            supervision_reports: LocalSupervisionReports::new(termination),
            owned_children: HashSet::new(),
            child_tasks: HashMap::new(),
            child_terminations: HashMap::new(),
            child_observations: HashMap::new(),
            stopping_children: HashSet::new(),
            parent_reports: NoParent,
            control,
            creations,
            actor_spaces,
        }
    }
}

impl<C, N, P> ApplicationCapabilities<C, N, P>
where
    C: Behavior,
    BehaviorAddr<C>: core::hash::Hash,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash + Send,
{
    fn with_parent(
        address: BehaviorAddr<C>,
        actor_spaces: Arc<N>,
        control: ControlSender<C::Event>,
        termination: Arc<TerminalOverride<BehaviorAddr<C>>>,
        timers: LocalTimers<C::Event>,
        facts: crate::observation::FactQueue<C::Event>,
        parent_reports: P,
    ) -> Self {
        let creations = CreationResults::new();
        Self {
            address,
            timers,
            facts,
            peers: None,
            supervision_reports: LocalSupervisionReports::new(termination),
            owned_children: HashSet::new(),
            child_tasks: HashMap::new(),
            child_terminations: HashMap::new(),
            child_observations: HashMap::new(),
            stopping_children: HashSet::new(),
            parent_reports,
            control,
            creations,
            actor_spaces,
        }
    }
}

impl<C, N, P> BirthInstaller<BehaviorAddr<C>> for ApplicationCapabilities<C, N, P>
where
    C: Behavior,
    BehaviorAddr<C>: core::hash::Hash,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash,
{
    type Error = Never;
}

impl<C, N, P> CreationTransaction<<BehaviorAddr<C> as Address>::Nonce>
    for ApplicationCapabilities<C, N, P>
where
    C: Behavior,
    BehaviorAddr<C>: core::hash::Hash,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash,
{
    fn begin_creations(&mut self) {
        self.creations.begin();
    }
}

impl<C, N, P> RetireCapabilities for ApplicationCapabilities<C, N, P>
where
    C: Behavior,
    BehaviorAddr<C>: core::hash::Hash + Send + Sync,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash + Send,
    C::Event: Send,
    BehaviorMessage<C>: Send,
    LocalPeerObservations<C::Protocol, C::Event>: Send,
    Self: Send,
{
    async fn retire(self) {
        for (_, child) in self.child_tasks {
            child.retire().await;
        }
        if let Some(peers) = self.peers {
            peers.retire().await;
        }
    }
}

impl<C, N, P, Child> SpawnChild<BehaviorAddr<C>, Child, Never>
    for ApplicationCapabilities<C, N, P>
where
    C: Behavior,
    BehaviorAddr<C>: core::hash::Hash + Clone + Send + Sync + 'static,
    <BehaviorAddr<C> as Address>::Nonce: core::hash::Hash + Send + 'static,
    C::Event: Send,
    BehaviorMessage<C>: Send,
    P: Send,
    Child: Behavior<Protocol: behavior::Protocol<Addr = BehaviorAddr<C>>, Ph = Never> + Send + 'static,
    Child::Event: behavior::InjectEvent<ShutdownRequested, behavior::Here> + Send + 'static,
    BehaviorMessage<Child>: Send + 'static,
    Child::Sends: Send + 'static,
    Child::Error: Send + 'static,
    <Child::Birth as BirthMode>::Child: Send + 'static,
    N: Hosts<Child::Protocol> + Send + Sync + 'static,
    ApplicationCapabilities<Child, N, LocalParentReports<BehaviorAddr<C>, C::Event>>:
        behavior::SendInterpreter + Send + 'static,
    <ApplicationCapabilities<Child, N, LocalParentReports<BehaviorAddr<C>, C::Event>> as behavior::SendInterpreter>::Error:
        Send + 'static,
    Child::Sends: behavior::InterpretSends<
            ApplicationCapabilities<Child, N, LocalParentReports<BehaviorAddr<C>, C::Event>>,
            Child::Event,
            behavior::Here,
        > + Send,
    <Child::Birth as BirthMode>::Child: DispatchBirth<
            BehaviorAddr<C>,
            ActionInterpreter<
                BehaviorAddr<C>,
                ApplicationCapabilities<Child, N, LocalParentReports<BehaviorAddr<C>, C::Event>>,
            >,
            (),
            Never,
        >,
{
    async fn spawn_child(
        &mut self,
        address: BehaviorAddr<C>,
        creation: Create<BehaviorAddr<C>, Child>,
    ) -> Result<(), Never> {
        let nonce = creation.nonce;
        let kind = creation.kind;
        let actor_spaces = self.actor_spaces.clone();
        let actors = self.actor_spaces.space().clone();
        let parent = self.control.clone();
        let result = crate::launch::spawn_owned_with(
            actors,
            communication::Config::new(DEFAULT_USER_CAPACITY),
            address,
            creation.child,
            move |control, termination, timers, facts| {
                ActionInterpreter::new(
                    address,
                    ApplicationCapabilities::<
                        Child,
                        N,
                        LocalParentReports<BehaviorAddr<C>, C::Event>,
                    >::with_parent(
                        address,
                        actor_spaces,
                        control,
                        termination,
                        timers,
                        facts,
                        LocalParentReports::new(nonce, parent),
                    ),
                )
            },
        )
        .await;

        self.creations.record(match result {
            Ok(owned) => {
                self.owned_children.insert(nonce);
                self.child_terminations
                    .insert(nonce, owned.actor.termination());
                self.child_tasks.insert(nonce, owned.task);
                CreationResolved::installed(nonce, kind, address)
            }
            Err(error) => CreationResolved::rejected(nonce, kind, error.rejection()),
        });
        Ok(())
    }
}
