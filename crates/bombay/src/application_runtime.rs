//! Static application assembly and local capability interpretation.

use core::convert::Infallible;
use core::fmt;
use core::future::Future;
use core::marker::PhantomData;
use std::collections::HashMap;
use std::io;
#[cfg(feature = "axum")]
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Instant;

use behavior::{
    Actions, Behavior, BehaviorBase, BehaviorMessage, BehaviorSettlements, BirthMode,
    BirthNodeAppend, Births, ChildCons, ChildCreationOutcome, ChildDelivery, ChildDeliveryReason,
    ChildHead, ChildInput, ChildInputIngress, ChildInputReason, ChildNamespaceExhausted,
    ChildOccurrenceProduct, ChildOccurrenceShape, ChildOccurrences, ChildProduct, ChildTail,
    Children, ClassifySettlement, CommittedChild, CreateChild, CreationCorrelation, CreationId,
    CreationKind, CreationRejection, CreationSequence, Creations, Delivery, EstablishChild,
    EstablishedActor, EstablishedCreation, EstablishedDelivery, EstablishedRecipient, EventIngress,
    ExactDeliveryReason, Here, Ingress, InjectEvent, InterpretItem, InterpreterFault,
    ItemSettlement, LogicalDeliveryReason, Never, NoBirths, NoChildren, ParentReportReason,
    Protocol, Recipient, RecoverEvent, ReportToParent, ResolveChildOccurrence, ResolvedChild,
    ResolvedChildPosition, RetirementBirths, RoutedCreation, SourceAdmission,
};
use behavior_actors::atomic::{
    ActivationPlan, ActivationStartRejection, AssignWorker, Assignment, AssignmentReceipt,
    BeginActivation, CustomerDelivery, DiagnosticAccepted, DiagnosticAction, InitializeWorker,
    PrepareWorkers, ProxyControl, ProxyControlAdmission, ProxyInputReceipt, ProxyOperation,
    StableProxy, WorkerActivation, WorkerInitializationOutcome, WorkerInitializationReport,
    WorkerPreparation, WorkerPreparationStarted, WorkerSource,
};
use behavior_actors::{
    CancelObservation, ChildShutdownRejection, ChildStopped, CreationResolved,
    EstablishedObservation, EstablishedShutdownResolved, InstallShutdownPlan,
    InterpretEstablishedObservation, InterpretEstablishedShutdown, ObservationId,
    ObservationOperation, ObservationRejection, ObserveChild, ObserveCreation, ObserveEstablished,
    ObserveEstablishedCreation, ObservePeer, PeerObservationRejection, PeerStopped, ReplyDelivery,
    ReportShutdownPlan, ReportTerminalOutcome, ScheduleAfter, ScheduleAfterRejection, ScheduleAt,
    ScheduleAtRejection, ShutdownChild, ShutdownEstablished, ShutdownId, ShutdownRejection,
    ShutdownRequested, TimerElapsed, TimerScheduled,
};
use bombay_address::ClaimError;
use communication::{ControlClosed, ControlSender};
use tokio::sync::oneshot;

use crate::actor_interface::{ActorInterface, ExtractLocalEndpoint};
use crate::address::{ApplicationAddresses, MailAddr};
use crate::application::Application;
use crate::child_bindings::{
    ChildBindingAt, ChildBindings, CreationBinding, NestedBindings, NestedChildBindings,
    NoChildBindings, RetireChildTasks, RuntimeChildBindings,
};
use crate::entity::{
    EntityAdmission, EntityApplicationFamilies, EntityDefinition, EntityFamilyAt,
    InstallEntityFamilies, InstalledEntityFamilies, NativeEntityHost, Passivation,
};
use crate::interpret::{ActionInterpreter, RetireCapabilities};
use crate::launch::{
    ActorSpace, OwnedTask, ProjectedTask, RootActor, SpawnError, spawn_owned_with, spawn_root_with,
};
use crate::local::{
    ActivationTasks, ActorRef, CapabilityRetirement, CommitActions, InstalledActor, Termination,
    request_actor_shutdown,
};
use crate::observation::{TerminationObservations, observe_peer};
use crate::reports::{
    LocalParentReports, LocalTerminalReports, ParentReporting, TerminalReportTransaction,
};
use crate::terminal::{ActorRetirement, ChildOrigin, LocalOutcome, ProjectTerminal, RootOrigin};
use crate::termination::TerminalReportDisposition;
use crate::time::LocalTimers;
use crate::topology::{HostedActorSpaces, Hosts, ResolveLogical};
use crate::worker_preparation::{WorkerPreparationSource, settle_worker_preparation};

const DEFAULT_USER_CAPACITY: usize = 1_024;

fn routed_creation<Child>(
    id: CreationId,
    kind: CreationKind,
    route: u64,
    child: Child,
) -> RoutedCreation<MailAddr, Child> {
    let creation = match kind {
        CreationKind::Birth => CreateChild::birth(id, child),
        CreationKind::Replacement { previous } => CreateChild::replacement(id, previous, child),
    };
    RoutedCreation::new(creation, route)
}

type RootBirthNode<Root> = <<Root as Behavior>::Birth as BirthMode>::Child;
type ApplicationProduct<Members> = <Members as StageApplicationChildren>::Product;
type ApplicationBirthNode<Members> =
    <ApplicationProduct<Members> as ChildProduct<MailAddr>>::Choice;
type BindingTerminal<Bindings> = <Bindings as RetireChildTasks>::Root;
type RootOriginProduct<Root> = ChildOccurrences<RootBirthNode<Root>, RootTerminalOriginMapper>;

pub trait AppendApplicationBirths<Tail>: BirthMode
where
    Self::Child: BirthNodeAppend<Tail>,
    Tail: BirthNodeAppend<Never>,
{
    type Output: BirthMode<Child = <Self::Child as BirthNodeAppend<Tail>>::Output>;
}

impl<Tail> AppendApplicationBirths<Tail> for NoBirths
where
    Never: BirthNodeAppend<Tail>,
    Tail: BirthNodeAppend<Never>,
{
    type Output = RetirementBirths<<Never as BirthNodeAppend<Tail>>::Output>;
}

impl<Head, Tail> AppendApplicationBirths<Tail> for Births<Head>
where
    Head: BirthNodeAppend<Tail>,
    Tail: BirthNodeAppend<Never>,
{
    type Output = Births<<Head as BirthNodeAppend<Tail>>::Output>;
}

impl<Head, Tail> AppendApplicationBirths<Tail> for RetirementBirths<Head>
where
    Head: BirthNodeAppend<Tail>,
    Tail: BirthNodeAppend<Never>,
{
    type Output = RetirementBirths<<Head as BirthNodeAppend<Tail>>::Output>;
}

type RootCapabilities<Actor, Spaces, Terminal, Origins> = ApplicationCapabilities<
    Actor,
    HostedActorSpaces<Spaces>,
    NoParent,
    ChildBindings<Actor, Terminal>,
    Origins,
>;
type RootInterpreter<Actor, Spaces, Terminal, Origins> =
    ActionInterpreter<RootCapabilities<Actor, Spaces, Terminal, Origins>>;

pub(crate) struct ApplicationCapabilityInputs<C, N>
where
    C: Behavior,
{
    pub(crate) address: MailAddr,
    pub(crate) actor_spaces: Arc<N>,
    pub(crate) allocations: ApplicationAddresses,
    pub(crate) control: ControlSender<C::Event>,
    pub(crate) timers: LocalTimers<C::Event>,
    pub(crate) observations: TerminationObservations<MailAddr, C::Event>,
    pub(crate) terminal_reports: LocalTerminalReports,
}

/// Failure at the complete local application boundary.
pub enum RunError<RootError = Never, RootTerminal = Never> {
    /// Tokio could not construct the application executor.
    Runtime(io::Error),
    /// The root address source rejected allocation.
    AllocationRejected(behavior::AllocationRejection),
    /// The root's controlled initialization fold failed.
    InitializationRejected(RootError),
    /// The runtime could not commit the root's address claim.
    HostRejected(ClaimError<MailAddr>),
    /// The root task panicked before activation.
    Panicked,
    /// The executor cancelled the root task before activation.
    Cancelled,
    /// The committed root retired before publication; its exact terminal is retained.
    Unpublished(RootTerminal),
    /// The private running application was initialized more than once.
    InitializedTwice,
    /// No collision-free creator-local nonce remained for an application actor.
    NonceExhausted,
}

impl<RootError, RootTerminal> fmt::Debug for RunError<RootError, RootTerminal> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Runtime(_) => "Runtime",
            Self::AllocationRejected(_) => "AllocationRejected",
            Self::InitializationRejected(_) => "InitializationRejected",
            Self::HostRejected(_) => "HostRejected",
            Self::Panicked => "Panicked",
            Self::Cancelled => "Cancelled",
            Self::Unpublished(_) => "Unpublished",
            Self::InitializedTwice => "InitializedTwice",
            Self::NonceExhausted => "NonceExhausted",
        })
    }
}

impl<RootError, RootTerminal> fmt::Display for RunError<RootError, RootTerminal> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Runtime(_) => "Bombay could not construct its Tokio runtime",
            Self::AllocationRejected(_) => "the root address source rejected allocation",
            Self::InitializationRejected(_) => "the root rejected initialization",
            Self::HostRejected(_) => "the actor host rejected root initialization",
            Self::Panicked => "root initialization panicked",
            Self::Cancelled => "root initialization was cancelled",
            Self::Unpublished(_) => "the committed root retired before publication",
            Self::InitializedTwice => "the running application was initialized more than once",
            Self::NonceExhausted => "application actor nonce allocation was exhausted",
        })
    }
}

impl<RootError, RootTerminal> std::error::Error for RunError<RootError, RootTerminal> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            _ => None,
        }
    }
}

/// A live application's typed root and capability projections.
pub struct ApplicationHandle<P: Protocol, Event, Families = ()> {
    root: ActorRef<P>,
    shutdown_control: Weak<ControlSender<Event>>,
    allocations: ApplicationAddresses,
    families: Families,
}

impl<P, Event, Families> fmt::Debug for ApplicationHandle<P, Event, Families>
where
    P: Protocol,
    P::Addr: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationHandle")
            .field("root", &self.root)
            .field("families_type", &core::any::type_name::<Families>())
            .finish_non_exhaustive()
    }
}

impl<P: Protocol, Event, Families: Clone> Clone for ApplicationHandle<P, Event, Families> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            shutdown_control: self.shutdown_control.clone(),
            allocations: self.allocations.clone(),
            families: self.families.clone(),
        }
    }
}

impl<P: Protocol, Event, Families> ApplicationHandle<P, Event, Families> {
    fn new(
        root: ActorRef<P>,
        shutdown_control: Weak<ControlSender<Event>>,
        allocations: ApplicationAddresses,
        families: Families,
    ) -> Self {
        Self {
            root,
            shutdown_control,
            allocations,
            families,
        }
    }

    /// Borrow the exact live root's delivery reference.
    #[must_use]
    pub const fn root(&self) -> &ActorRef<P> {
        &self.root
    }

    /// Form a transport-neutral interface from an explicit receptionist product.
    pub fn interface<Api>(&self, api: Api) -> ActorInterface<Api> {
        ActorInterface::new(api, self.allocations.clone())
    }

    /// Project lifecycle authority without exposing topology ownership.
    #[must_use]
    pub fn lifecycle(&self) -> ApplicationLifecycle<P, Event> {
        ApplicationLifecycle {
            root: self.root.clone(),
            control: self.shutdown_control.clone(),
        }
    }

    /// Select one statically declared native Entity family by semantic role.
    #[must_use]
    pub fn entities<Role, Position>(
        &self,
        _: Role,
    ) -> crate::entity::Entities<<Families as EntityFamilyAt<Role, Position>>::Definition>
    where
        Families: EntityFamilyAt<Role, Position>,
    {
        self.families.select_family()
    }

    /// Begin passivation of one identity in a statically declared family.
    #[must_use]
    #[allow(
        private_bounds,
        reason = "native execution remains a sealed application composition proof"
    )]
    pub fn passivate_entity<Role, Position>(
        &self,
        role: Role,
        id: &<<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::Id,
    ) -> Passivation
    where
        Families: EntityFamilyAt<Role, Position>,
        <<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::Hosts:
            NativeEntityHost<
                <<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::Behavior,
                <<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::Terminal,
            >,
{
        self.entities(role).passivate(id)
    }
}

/// Explicit application lifecycle authority, separate from actor messaging.
pub struct ApplicationLifecycle<P: Protocol, Event> {
    root: ActorRef<P>,
    control: Weak<ControlSender<Event>>,
}

impl<P, Event> fmt::Debug for ApplicationLifecycle<P, Event>
where
    P: Protocol,
    P::Addr: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationLifecycle")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl<P: Protocol, Event> Clone for ApplicationLifecycle<P, Event> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            control: self.control.clone(),
        }
    }
}

impl<P: Protocol, Event> ApplicationLifecycle<P, Event> {
    /// Request shutdown through the root lifecycle lane.
    ///
    /// # Errors
    ///
    /// Returns the exact rejection when the root is already stopping or has
    /// already stopped.
    pub fn request_shutdown(&self) -> Result<(), ShutdownRejection>
    where
        Event: InjectEvent<ShutdownRequested, Here>,
    {
        if self.root.termination_observation().try_get().is_some() {
            return Err(ShutdownRejection::AlreadyStopped);
        }
        let Some(control) = self.control.upgrade() else {
            return Err(self.root.shutdown_rejection());
        };
        request_actor_shutdown(&self.root, &control, Ingress::new())
    }

    /// Observe termination of the exact root incarnation.
    pub fn termination(&self) -> impl Future<Output = Termination<P::Addr>> + use<P, Event> {
        self.root.termination()
    }
}

/// Failure at the opt-in Axum HTTP boundary.
#[cfg(feature = "axum")]
#[derive(Debug, thiserror::Error)]
pub enum AxumRunError<RootTerminal, RootError = Never> {
    #[error(transparent)]
    Application(#[from] RunError<RootError, RootTerminal>),
    #[error("Bombay could not bind the Axum listener at {address}")]
    Bind {
        address: SocketAddr,
        #[source]
        source: io::Error,
    },
    #[error("the Axum server failed before the application root retired")]
    Serve {
        #[source]
        source: io::Error,
        terminal: RootTerminal,
    },
}

/// Explicit advanced composition of one root and its logical protocol spaces.
#[derive(Debug)]
pub struct App<Root, Spaces, Families = ()> {
    root: Root,
    spaces: Spaces,
    families: Families,
}

impl<Root, Spaces> App<Root, Spaces> {
    #[must_use]
    pub const fn new(root: Root, spaces: Spaces) -> Self {
        Self {
            root,
            spaces,
            families: (),
        }
    }
}

impl<Root, Spaces, Families> App<Root, Spaces, Families> {
    /// Declare one application-owned native Entity family under a semantic role.
    ///
    /// # Errors
    ///
    /// Returns the existing directory configuration error without starting the
    /// application or consuming the definition.
    #[allow(
        clippy::type_complexity,
        reason = "the inferred role-indexed family product is the public static composition"
    )]
    pub fn entity_family<Role, D>(
        self,
        role: Role,
        definition: D,
        directory: crate::entity::DirectoryConfig,
        capacity: crate::entity::EntityCapacity,
    ) -> Result<
        App<
            Root,
            Spaces,
            (
                Role,
                D,
                crate::entity::DirectoryConfig,
                crate::entity::EntityCapacity,
                Families,
            ),
        >,
        crate::entity::DirectoryError<BehaviorMessage<D::Behavior>>,
    >
    where
        D: EntityDefinition<Hosts = Spaces>,
    {
        if !directory.shards.get().is_power_of_two() {
            return Err(crate::entity::DirectoryError::InvalidShardCount);
        }
        Ok(App {
            root: self.root,
            spaces: self.spaces,
            families: (role, definition, directory, capacity, self.families),
        })
    }
}

struct DirectRoot;
struct DeclaredRoot;
pub(crate) struct StructuralOrigins<Owner>(PhantomData<fn() -> Owner>);
struct ApplicationOrigins<Root, Members>(PhantomData<fn() -> (Root, Members)>);

trait RootProjection<Actor, Terminal>
where
    Actor: behavior::BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    type Error;

    fn project_retirement(
        address: MailAddr,
        retirement: ActorRetirement<Actor, Terminal>,
    ) -> Terminal;

    fn project(address: MailAddr, outcome: LocalOutcome<Actor, Vec<Terminal>>) -> Terminal {
        Self::project_retirement(address, ActorRetirement::from_local(outcome))
    }

    fn startup_error(
        address: MailAddr,
        error: SpawnError<Actor, Vec<Terminal>>,
    ) -> RunError<Self::Error, Terminal>;
}

impl<Actor, Terminal> RootProjection<Actor, Terminal> for DirectRoot
where
    Actor: behavior::BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    Terminal: ProjectTerminal<RootOrigin<Actor>, ActorRetirement<Actor, Terminal>>,
{
    type Error = Actor::Error;

    fn project_retirement(
        address: MailAddr,
        retirement: ActorRetirement<Actor, Terminal>,
    ) -> Terminal {
        Terminal::project(RootOrigin::<Actor>::new(address), retirement)
    }

    fn startup_error(
        address: MailAddr,
        error: SpawnError<Actor, Vec<Terminal>>,
    ) -> RunError<Self::Error, Terminal> {
        match error {
            SpawnError::AllocationRejected { reason, .. } => RunError::AllocationRejected(reason),
            SpawnError::InitializationRejected { error, .. } => {
                RunError::InitializationRejected(error)
            }
            panicked @ SpawnError::InitializationPanicked { .. } => RunError::Unpublished(
                Self::project_retirement(address, panicked.into_retirement()),
            ),
            SpawnError::HostRejected { error, .. } => RunError::HostRejected(error),
            unpublished @ (SpawnError::BindingAbandoned { .. } | SpawnError::Unpublished(_)) => {
                RunError::Unpublished(Self::project_retirement(
                    address,
                    unpublished.into_retirement(),
                ))
            }
            SpawnError::Panicked => RunError::Panicked,
            SpawnError::Cancelled => RunError::Cancelled,
        }
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Return exact rejected actor inputs and retirement values without an extra allocation."
)]
async fn launch_application_root<Actor, Spaces, Terminal, Origins>(
    spaces: Spaces,
    root: Actor,
    allocations: ApplicationAddresses,
) -> Result<RootActor<Actor, Vec<Terminal>>, SpawnError<Actor, Vec<Terminal>>>
where
    Actor: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Actor::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    BehaviorMessage<Actor>: Send + 'static,
    Actor::Sends: Send + 'static,
    Actor::Error: Send + 'static,
    <Actor::Birth as BirthMode>::Child:
        ChildOccurrenceProduct<RuntimeChildBindings<Terminal>> + Send + 'static,
    Spaces: Hosts<Actor::Protocol> + Send + Sync + 'static,
    ChildBindings<Actor, Terminal>: Default + RetireChildTasks<Root = Terminal> + Send + 'static,
    RootInterpreter<Actor, Spaces, Terminal, Origins>:
        CommitActions<Actor, Retired = Vec<Terminal>> + Send + 'static,
    <Actor as BehaviorSettlements>::Settlements: ClassifySettlement + Send,
    Terminal: Send + 'static,
{
    let roots = <Spaces as Hosts<Actor::Protocol>>::space(&spaces).clone();
    let actor_spaces = Arc::new(HostedActorSpaces(spaces));
    let address = MailAddr::APPLICATION_ROOT;
    spawn_root_with(
        roots,
        communication::Config::new(DEFAULT_USER_CAPACITY),
        address,
        root,
        move |control, terminal_reports, timers, observations| {
            ActionInterpreter::new(ApplicationCapabilities::<
                Actor,
                HostedActorSpaces<Spaces>,
                NoParent,
                ChildBindings<Actor, Terminal>,
                Origins,
            >::new_with_bindings(
                ApplicationCapabilityInputs {
                    address,
                    actor_spaces,
                    allocations,
                    control,
                    timers,
                    observations,
                    terminal_reports,
                },
                ChildBindings::<Actor, Terminal>::default(),
            ))
        },
    )
    .await
}

trait LaunchSystem<Actor, Terminal, Origins, Projection>
where
    Actor: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    type RootError;

    fn launch(
        self,
        root: Actor,
    ) -> impl Future<Output = Result<Terminal, RunError<Self::RootError, Terminal>>> + Send;

    fn launch_with<Families, Boundary, BoundaryFuture, Output>(
        self,
        root: Actor,
        allocations: ApplicationAddresses,
        families: Families,
        boundary: Boundary,
    ) -> impl Future<Output = Result<(Output, Terminal), RunError<Self::RootError, Terminal>>> + Send
    where
        Families: Clone + Send + 'static,
        Boundary: FnOnce(ApplicationHandle<Actor::Protocol, Actor::Event, Families>) -> BoundaryFuture
            + Send,
        BoundaryFuture: Future<Output = Output> + Send,
        Output: Send;

    #[cfg(feature = "axum")]
    fn launch_axum<Router>(
        self,
        root: Actor,
        address: SocketAddr,
        router: Router,
    ) -> impl Future<Output = Result<Terminal, AxumRunError<Terminal, Self::RootError>>> + Send
    where
        Router: FnOnce(ApplicationHandle<Actor::Protocol, Actor::Event>) -> axum::Router + Send;
}

impl<Actor, Spaces, Terminal, Origins, Projection>
    LaunchSystem<Actor, Terminal, Origins, Projection> for Spaces
where
    Actor: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Actor::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    BehaviorMessage<Actor>: Send + 'static,
    Actor::Sends: Send + 'static,
    Actor::Error: Send + 'static,
    <Actor::Birth as BirthMode>::Child:
        ChildOccurrenceProduct<RuntimeChildBindings<Terminal>> + Send + 'static,
    Spaces: Hosts<Actor::Protocol> + Send + Sync + 'static,
    ChildBindings<Actor, Terminal>: Default + RetireChildTasks<Root = Terminal> + Send + 'static,
    RootInterpreter<Actor, Spaces, Terminal, Origins>:
        CommitActions<Actor, Retired = Vec<Terminal>> + Send + 'static,
    <Actor as BehaviorSettlements>::Settlements: ClassifySettlement + Send,
    Terminal: Send + 'static,
    Projection: RootProjection<Actor, Terminal>,
{
    type RootError = <Projection as RootProjection<Actor, Terminal>>::Error;

    async fn launch(self, root: Actor) -> Result<Terminal, RunError<Self::RootError, Terminal>> {
        <Spaces as LaunchSystem<Actor, Terminal, Origins, Projection>>::launch_with(
            self,
            root,
            ApplicationAddresses::new(),
            (),
            |_: ApplicationHandle<Actor::Protocol, Actor::Event>| async {},
        )
        .await
        .map(|((), terminal)| terminal)
    }

    async fn launch_with<Families, Boundary, BoundaryFuture, Output>(
        self,
        root: Actor,
        allocations: ApplicationAddresses,
        families: Families,
        boundary: Boundary,
    ) -> Result<(Output, Terminal), RunError<Self::RootError, Terminal>>
    where
        Families: Clone + Send + 'static,
        Boundary: FnOnce(ApplicationHandle<Actor::Protocol, Actor::Event, Families>) -> BoundaryFuture
            + Send,
        BoundaryFuture: Future<Output = Output> + Send,
        Output: Send,
    {
        let interface_allocations = allocations.clone();
        let address = MailAddr::APPLICATION_ROOT;
        let root =
            launch_application_root::<Actor, Spaces, Terminal, Origins>(self, root, allocations)
                .await
                .map_err(|error| Projection::startup_error(address, error))?;

        let application = ApplicationHandle::new(
            root.actor,
            root.shutdown_control,
            interface_allocations,
            families,
        );
        let output = boundary(application).await;
        let terminal = Projection::project(address, root.task.finish().await);
        Ok((output, terminal))
    }

    #[cfg(feature = "axum")]
    async fn launch_axum<Router>(
        self,
        root: Actor,
        address: SocketAddr,
        router: Router,
    ) -> Result<Terminal, AxumRunError<Terminal, Self::RootError>>
    where
        Router: FnOnce(ApplicationHandle<Actor::Protocol, Actor::Event>) -> axum::Router + Send,
    {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .map_err(|source| AxumRunError::Bind { address, source })?;
        let allocations = ApplicationAddresses::new();
        let interface_allocations = allocations.clone();
        let root_address = MailAddr::APPLICATION_ROOT;
        let root =
            launch_application_root::<Actor, Spaces, Terminal, Origins>(self, root, allocations)
                .await
                .map_err(|error| Projection::startup_error(root_address, error))?;

        let application =
            ApplicationHandle::new(root.actor, root.shutdown_control, interface_allocations, ());
        let lifecycle = application.lifecycle();
        let server_shutdown = lifecycle.termination();
        let serve = axum::serve(listener, router(application.clone()))
            .with_graceful_shutdown(async move {
                match server_shutdown.await {
                    Ok(_) | Err(_) => {}
                }
            })
            .await;
        if serve.is_err() {
            match lifecycle.request_shutdown() {
                Ok(())
                | Err(ShutdownRejection::AlreadyStopping | ShutdownRejection::AlreadyStopped) => {}
            }
        }
        let terminal = Projection::project(root_address, root.task.finish().await);
        match serve {
            Ok(()) => Ok(terminal),
            Err(source) => Err(AxumRunError::Serve { source, terminal }),
        }
    }
}

#[allow(
    private_bounds,
    reason = "the launch proof is private static runtime composition"
)]
impl<Root, Spaces> App<Root, Spaces>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
{
    /// Run the application to exact root termination.
    ///
    /// # Errors
    ///
    /// Returns the exact runtime-construction or root-startup failure.
    pub fn run<Terminal>(self) -> Result<Terminal, RunError<Root::Error, Terminal>>
    where
        Spaces: LaunchSystem<
                Root,
                Terminal,
                StructuralOrigins<Root::Base>,
                DirectRoot,
                RootError = Root::Error,
            >,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(RunError::Runtime)?
            .block_on(self.spaces.launch(self.root))
    }

    /// Run the application with one live external boundary.
    ///
    /// # Errors
    ///
    /// Returns the exact runtime-construction or root-startup failure.
    pub fn run_with<Terminal, Boundary, BoundaryFuture, Output>(
        self,
        boundary: Boundary,
    ) -> Result<(Output, Terminal), RunError<Root::Error, Terminal>>
    where
        Spaces: LaunchSystem<
                Root,
                Terminal,
                StructuralOrigins<Root::Base>,
                DirectRoot,
                RootError = Root::Error,
            >,
        Boundary: FnOnce(ApplicationHandle<Root::Protocol, Root::Event>) -> BoundaryFuture + Send,
        BoundaryFuture: Future<Output = Output> + Send,
        Output: Send,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(RunError::Runtime)?
            .block_on(
                self.spaces
                    .launch_with(self.root, ApplicationAddresses::new(), (), boundary),
            )
    }

    #[cfg(feature = "axum")]
    /// Run the application behind an Axum HTTP boundary.
    ///
    /// # Errors
    ///
    /// Returns the exact application startup, listener bind, or server error.
    pub fn run_axum<Terminal>(
        self,
        address: SocketAddr,
        router: impl FnOnce(ApplicationHandle<Root::Protocol, Root::Event>) -> axum::Router + Send,
    ) -> Result<Terminal, AxumRunError<Terminal, Root::Error>>
    where
        Spaces: LaunchSystem<
                Root,
                Terminal,
                StructuralOrigins<Root::Base>,
                DirectRoot,
                RootError = Root::Error,
            >,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(RunError::Runtime)?
            .block_on(self.spaces.launch_axum(self.root, address, router))
    }
}

#[allow(
    private_bounds,
    reason = "native installation and launch proofs remain private static composition"
)]
impl<Root, Spaces, Families> App<Root, Spaces, Families>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Families: InstallEntityFamilies<Spaces> + Send,
{
    /// Run the application with its native Entity receptionists at the boundary.
    ///
    /// The root settles before family admission closes, represented incarnations
    /// retire, and Bombay joins every family-owned lifecycle task.
    ///
    /// # Errors
    ///
    /// Returns the exact runtime-construction or root-startup failure after the
    /// installed families have shut down.
    #[allow(
        clippy::type_complexity,
        reason = "the exact root and role-indexed shutdown products remain caller visible"
    )]
    pub fn run_with_entities<Terminal, Boundary, BoundaryFuture, Output>(
        self,
        boundary: Boundary,
    ) -> Result<
        (
            Output,
            Terminal,
            <Families as EntityApplicationFamilies<Spaces>>::Shutdowns,
        ),
        RunError<Root::Error, Terminal>,
    >
    where
        Arc<Spaces>: LaunchSystem<
                Root,
                Terminal,
                StructuralOrigins<Root::Base>,
                DirectRoot,
                RootError = Root::Error,
            >,
        Boundary: FnOnce(
                ApplicationHandle<
                    Root::Protocol,
                    Root::Event,
                    <Families as EntityApplicationFamilies<Spaces>>::Receptionists,
                >,
            ) -> BoundaryFuture
            + Send,
        BoundaryFuture: Future<Output = Output> + Send,
        Output: Send,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(RunError::Runtime)?
            .block_on(async move {
                let Self {
                    root,
                    spaces,
                    families,
                } = self;
                let spaces = Arc::new(spaces);
                let allocations = ApplicationAddresses::new();
                let installed = families.install(Arc::clone(&spaces), allocations.clone());
                let receptionists = installed.receptionists();
                let outcome = spaces
                    .launch_with(root, allocations, receptionists, boundary)
                    .await;
                let shutdowns = installed.shutdown().await;
                outcome.map(|(output, terminal)| (output, terminal, shutdowns))
            })
    }
}

trait StageApplicationChildren: Sized {
    type Product: ChildProduct<MailAddr>;

    fn stage(
        self,
        creations: &mut CreationSequence,
    ) -> Result<Children<MailAddr, Self::Product>, ApplicationStagingError>;
}

trait DeclaredApplicationChildren: StageApplicationChildren {}

impl<Role, Actor, Tail> DeclaredApplicationChildren for (Role, Actor, Tail)
where
    Actor: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Tail: StageApplicationChildren,
{
}

impl StageApplicationChildren for () {
    type Product = NoChildren;

    fn stage(
        self,
        _: &mut CreationSequence,
    ) -> Result<Children<MailAddr, Self::Product>, ApplicationStagingError> {
        Ok(Children::new())
    }
}

impl<Role, Actor, Tail> StageApplicationChildren for (Role, Actor, Tail)
where
    Actor: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Tail: StageApplicationChildren,
{
    type Product = ChildCons<MailAddr, Actor, Tail::Product>;

    fn stage(
        self,
        creations: &mut CreationSequence,
    ) -> Result<Children<MailAddr, Self::Product>, ApplicationStagingError> {
        let (role, actor, tail) = self;
        let children = tail.stage(creations)?;
        let id = creations
            .issue()
            .ok_or(ApplicationStagingError::NonceExhausted)?;
        drop(role);
        Ok(children.child(id, actor))
    }
}

/// Failure while initializing the exact composed application behavior.
pub enum ApplicationDefinitionError<RootError> {
    Root(RootError),
    InitializedTwice,
}

enum ApplicationStagingError {
    NonceExhausted,
}

fn stage_application<Root, Members, Terminal>(
    root: Root,
    members: Members,
) -> Result<ApplicationBehavior<Root, ApplicationProduct<Members>>, RunError<Root::Error, Terminal>>
where
    Root: Behavior,
    Members: StageApplicationChildren,
{
    let mut creations = CreationSequence::new();
    let application = members
        .stage(&mut creations)
        .map_err(|ApplicationStagingError::NonceExhausted| RunError::NonceExhausted)?;
    Ok(ApplicationBehavior::new(root, application))
}

/// Exact behavior produced by composing an application root with its declared actors.
///
/// This type is public because terminal custody retains the final concrete
/// behavior and its complete action settlements. Applications normally create
/// it through [`Application`] and only name it in terminal projections.
pub struct ApplicationBehavior<Root, Product>
where
    Product: ChildProduct<MailAddr>,
{
    root: Root,
    application: Option<Children<MailAddr, Product>>,
}

impl<Root, Product> ApplicationBehavior<Root, Product>
where
    Product: ChildProduct<MailAddr>,
{
    const fn new(root: Root, application: Children<MailAddr, Product>) -> Self {
        Self {
            root,
            application: Some(application),
        }
    }
}

impl<Root, Product> Behavior for ApplicationBehavior<Root, Product>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    Product: ChildProduct<MailAddr>,
    RootBirthNode<Root>: BirthNodeAppend<Product::Choice>,
    Root::Birth: AppendApplicationBirths<Product::Choice>,
{
    type Protocol = Root::Protocol;
    type Event = Root::Event;
    type Sends = Root::Sends;
    type Ph = Never;
    type Error = ApplicationDefinitionError<Root::Error>;
    type Birth = <Root::Birth as AppendApplicationBirths<Product::Choice>>::Output;

    fn init(&mut self, _: behavior::InitializationTurn) -> behavior::BehaviorActed<Self> {
        let actions =
            behavior::initialize(&mut self.root).map_err(ApplicationDefinitionError::Root)?;
        let application = self
            .application
            .take()
            .ok_or(ApplicationDefinitionError::InitializedTwice)?;
        let application = application.into_creates();
        Ok(Actions {
            sends: actions.sends,
            creates: RootBirthNode::<Root>::append_creations(actions.creates, application),
            become_: actions.become_,
        })
    }

    fn transition(
        &mut self,
        _: behavior::ActiveTurn,
        event: Self::Event,
    ) -> behavior::BehaviorActed<Self> {
        let actions = behavior::delegate_transition(&mut self.root, event)
            .map_err(ApplicationDefinitionError::Root)?;
        Ok(Actions {
            sends: actions.sends,
            creates: RootBirthNode::<Root>::append_creations(actions.creates, Creations::empty()),
            become_: actions.become_,
        })
    }
}

impl<Root, Product> BehaviorBase for ApplicationBehavior<Root, Product>
where
    Root: BehaviorBase,
    Product: ChildProduct<MailAddr>,
{
    type Base = Root::Base;

    fn base(&self) -> &Self::Base {
        self.root.base()
    }
}

impl<Root, Product, Terminal> RootProjection<ApplicationBehavior<Root, Product>, Terminal>
    for DeclaredRoot
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    Product: ChildProduct<MailAddr>,
    RootBirthNode<Root>: BirthNodeAppend<Product::Choice>,
    Root::Birth: AppendApplicationBirths<Product::Choice>,
    ApplicationBehavior<Root, Product>: BehaviorSettlements<
            Protocol = Root::Protocol,
            Event = Root::Event,
            Sends = Root::Sends,
            Ph = Never,
            Error = ApplicationDefinitionError<Root::Error>,
            Birth = <Root::Birth as AppendApplicationBirths<Product::Choice>>::Output,
            Settlements: ClassifySettlement + Send + 'static,
        >,
    Terminal: ProjectTerminal<
            RootOrigin<Root>,
            ActorRetirement<ApplicationBehavior<Root, Product>, Terminal>,
        >,
{
    type Error = Root::Error;

    fn project_retirement(
        address: MailAddr,
        retirement: ActorRetirement<ApplicationBehavior<Root, Product>, Terminal>,
    ) -> Terminal {
        Terminal::project(RootOrigin::<Root>::new(address), retirement)
    }

    fn startup_error(
        address: MailAddr,
        error: SpawnError<ApplicationBehavior<Root, Product>, Vec<Terminal>>,
    ) -> RunError<Self::Error, Terminal> {
        match error {
            SpawnError::AllocationRejected { reason, .. } => RunError::AllocationRejected(reason),
            SpawnError::InitializationRejected { error, .. } => match error {
                ApplicationDefinitionError::Root(error) => RunError::InitializationRejected(error),
                ApplicationDefinitionError::InitializedTwice => RunError::InitializedTwice,
            },
            panicked @ SpawnError::InitializationPanicked { .. } => RunError::Unpublished(
                Self::project_retirement(address, panicked.into_retirement()),
            ),
            SpawnError::HostRejected { error, .. } => RunError::HostRejected(error),
            unpublished @ (SpawnError::BindingAbandoned { .. } | SpawnError::Unpublished(_)) => {
                RunError::Unpublished(Self::project_retirement(
                    address,
                    unpublished.into_retirement(),
                ))
            }
            SpawnError::Panicked => RunError::Panicked,
            SpawnError::Cancelled => RunError::Cancelled,
        }
    }
}

trait ComposeApplication<Root>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
{
    type Actor: Behavior<Protocol = Root::Protocol, Ph = Never>;
    type Origins;
    type Projection;

    fn compose<Terminal>(self, root: Root) -> Result<Self::Actor, RunError<Root::Error, Terminal>>;
}

impl<Root> ComposeApplication<Root> for ()
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
{
    type Actor = Root;
    type Origins = StructuralOrigins<Root::Base>;
    type Projection = DirectRoot;

    fn compose<Terminal>(self, root: Root) -> Result<Self::Actor, RunError<Root::Error, Terminal>> {
        Ok(root)
    }
}

impl<Root, Role, Actor, Tail> ComposeApplication<Root> for (Role, Actor, Tail)
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    (Role, Actor, Tail): DeclaredApplicationChildren,
    RootBirthNode<Root>: BirthNodeAppend<ApplicationBirthNode<(Role, Actor, Tail)>>,
    Root::Birth: AppendApplicationBirths<ApplicationBirthNode<(Role, Actor, Tail)>>,
{
    type Actor = ApplicationBehavior<Root, ApplicationProduct<(Role, Actor, Tail)>>;
    type Origins = ApplicationOrigins<Root, (Role, Actor, Tail)>;
    type Projection = DeclaredRoot;

    fn compose<Terminal>(self, root: Root) -> Result<Self::Actor, RunError<Root::Error, Terminal>> {
        stage_application(root, self)
    }
}

#[allow(
    private_bounds,
    reason = "the running application and its launch proof remain private"
)]
impl<Root, Members> Application<Root, Members>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Members: ComposeApplication<Root>,
{
    /// Run the application to exact root termination.
    ///
    /// # Errors
    ///
    /// Returns the exact root initialization or local runtime failure that
    /// prevented the application from reaching its retirement boundary.
    pub fn run<Terminal>(self) -> Result<Terminal, RunError<Root::Error, Terminal>>
    where
        ActorSpace<Root::Protocol>: LaunchSystem<
                Members::Actor,
                Terminal,
                Members::Origins,
                Members::Projection,
                RootError = Root::Error,
            >,
    {
        let (root, members) = self.into_parts();
        let actor = members.compose(root)?;
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(RunError::Runtime)?
            .block_on(<ActorSpace<Root::Protocol> as LaunchSystem<
                Members::Actor,
                Terminal,
                Members::Origins,
                Members::Projection,
            >>::launch(ActorSpace::new(), actor))
    }

    /// Run the application with one live external boundary.
    ///
    /// # Errors
    ///
    /// Returns the exact root initialization or local runtime failure that
    /// prevented the application from reaching its retirement boundary.
    pub fn run_with<Terminal, Boundary, BoundaryFuture, Output>(
        self,
        boundary: Boundary,
    ) -> Result<(Output, Terminal), RunError<Root::Error, Terminal>>
    where
        ActorSpace<Root::Protocol>: LaunchSystem<
                Members::Actor,
                Terminal,
                Members::Origins,
                Members::Projection,
                RootError = Root::Error,
            >,
        Boundary: FnOnce(
                ApplicationHandle<Root::Protocol, <Members::Actor as Behavior>::Event>,
            ) -> BoundaryFuture
            + Send,
        BoundaryFuture: Future<Output = Output> + Send,
        Output: Send,
    {
        let (root, members) = self.into_parts();
        let actor = members.compose(root)?;
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(RunError::Runtime)?
            .block_on(<ActorSpace<Root::Protocol> as LaunchSystem<
                Members::Actor,
                Terminal,
                Members::Origins,
                Members::Projection,
            >>::launch_with(
                ActorSpace::new(),
                actor,
                ApplicationAddresses::new(),
                (),
                boundary,
            ))
    }

    #[cfg(feature = "axum")]
    /// Run the application behind an Axum HTTP boundary.
    ///
    /// # Errors
    ///
    /// Returns the exact application construction, runtime, server, or terminal
    /// failure while preserving terminal custody when it is available.
    pub fn run_axum<Terminal>(
        self,
        address: SocketAddr,
        router: impl FnOnce(
            ApplicationHandle<Root::Protocol, <Members::Actor as Behavior>::Event>,
        ) -> axum::Router
        + Send,
    ) -> Result<Terminal, AxumRunError<Terminal, Root::Error>>
    where
        ActorSpace<Root::Protocol>: LaunchSystem<
                Members::Actor,
                Terminal,
                Members::Origins,
                Members::Projection,
                RootError = Root::Error,
            >,
    {
        let (root, members) = self.into_parts();
        let actor = members.compose(root).map_err(AxumRunError::Application)?;
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(RunError::Runtime)?
            .block_on(<ActorSpace<Root::Protocol> as LaunchSystem<
                Members::Actor,
                Terminal,
                Members::Origins,
                Members::Projection,
            >>::launch_axum(
                ActorSpace::new(), actor, address, router
            ))
    }
}

trait ProjectChildTerminal<Position, Child, Terminal>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal>;
}

impl<Owner, Position, Child, Terminal> ProjectChildTerminal<Position, Child, Terminal>
    for StructuralOrigins<Owner>
where
    Owner: 'static,
    Position: 'static,
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Child::Event: Send + 'static,
    BehaviorMessage<Child>: Send + 'static,
    Child::Sends: Send + 'static,
    Child::Error: Send + 'static,
    <Child::Birth as BirthMode>::Child: Send + 'static,
    <Child as BehaviorSettlements>::Settlements: Send + 'static,
    Terminal: ProjectTerminal<ChildOrigin<Owner, Position>, ActorRetirement<Child, Terminal>>
        + Send
        + 'static,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal> {
        ProjectedTask::project(task, ChildOrigin::<Owner, Position>::new(address, nonce))
    }
}

struct RootTerminalOriginMapper;
struct NoRootTerminalOrigins;
struct RootTerminalOrigin<Child, Tail>(PhantomData<fn() -> (Child, Tail)>);
struct AppendedOrigins<RootOrigins, Members>(PhantomData<fn() -> (RootOrigins, Members)>);

impl ChildOccurrenceShape for RootTerminalOriginMapper {
    type Empty = NoRootTerminalOrigins;
    type Member<Position, Child: Behavior, Tail> = RootTerminalOrigin<Child, Tail>;
}

trait ProjectAppendedChild<Target, Absolute, Root, Child, Terminal>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal>;
}

trait ProjectDeclaredChild<Target, Root, Child, Terminal>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal>;
}

impl<Members, Target, Absolute, Root, Child, Terminal>
    ProjectAppendedChild<Target, Absolute, Root, Child, Terminal>
    for AppendedOrigins<NoRootTerminalOrigins, Members>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    Members: ProjectDeclaredChild<Target, Root, Child, Terminal>,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal> {
        Members::task(task, address, nonce)
    }
}

impl<RootChild, Rest, Members, Absolute, Root, Terminal>
    ProjectAppendedChild<ChildHead, Absolute, Root, RootChild, Terminal>
    for AppendedOrigins<RootTerminalOrigin<RootChild, Rest>, Members>
where
    Root: BehaviorBase,
    Root::Base: 'static,
    Absolute: 'static,
    RootChild:
        BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    RootChild::Event: Send + 'static,
    BehaviorMessage<RootChild>: Send + 'static,
    RootChild::Sends: Send + 'static,
    RootChild::Error: Send + 'static,
    <RootChild::Birth as BirthMode>::Child: Send + 'static,
    <RootChild as BehaviorSettlements>::Settlements: Send + 'static,
    Terminal: ProjectTerminal<ChildOrigin<Root::Base, Absolute>, ActorRetirement<RootChild, Terminal>>
        + Send
        + 'static,
{
    fn task(
        task: OwnedTask<RootChild, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<RootChild, Terminal> {
        ProjectedTask::project(
            task,
            ChildOrigin::<Root::Base, Absolute>::new(address, nonce),
        )
    }
}

impl<RootChild, Rest, Members, Relative, Absolute, Root, Child, Terminal>
    ProjectAppendedChild<ChildTail<Relative>, Absolute, Root, Child, Terminal>
    for AppendedOrigins<RootTerminalOrigin<RootChild, Rest>, Members>
where
    RootChild: Behavior,
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    AppendedOrigins<Rest, Members>: ProjectAppendedChild<Relative, Absolute, Root, Child, Terminal>,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal> {
        <AppendedOrigins<Rest, Members> as ProjectAppendedChild<
            Relative,
            Absolute,
            Root,
            Child,
            Terminal,
        >>::task(task, address, nonce)
    }
}

impl<Role, Child, Tail, Root, Terminal> ProjectDeclaredChild<ChildHead, Root, Child, Terminal>
    for (Role, Child, Tail)
where
    Root: 'static,
    Role: 'static,
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Child::Event: Send + 'static,
    BehaviorMessage<Child>: Send + 'static,
    Child::Sends: Send + 'static,
    Child::Error: Send + 'static,
    <Child::Birth as BirthMode>::Child: Send + 'static,
    <Child as BehaviorSettlements>::Settlements: Send + 'static,
    Terminal:
        ProjectTerminal<ChildOrigin<Root, Role>, ActorRetirement<Child, Terminal>> + Send + 'static,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal> {
        ProjectedTask::project(task, ChildOrigin::<Root, Role>::new(address, nonce))
    }
}

impl<Role, Declared, Tail, Relative, Root, Child, Terminal>
    ProjectDeclaredChild<ChildTail<Relative>, Root, Child, Terminal> for (Role, Declared, Tail)
where
    Declared: Behavior,
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    Tail: ProjectDeclaredChild<Relative, Root, Child, Terminal>,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal> {
        Tail::task(task, address, nonce)
    }
}

impl<Root, Members, Position, Child, Terminal> ProjectChildTerminal<Position, Child, Terminal>
    for ApplicationOrigins<Root, Members>
where
    Root: Behavior + BehaviorBase,
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    RootBirthNode<Root>: ChildOccurrenceProduct<RootTerminalOriginMapper>,
    AppendedOrigins<RootOriginProduct<Root>, Members>:
        ProjectAppendedChild<Position, Position, Root, Child, Terminal>,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal> {
        <AppendedOrigins<RootOriginProduct<Root>, Members> as ProjectAppendedChild<
            Position,
            Position,
            Root,
            Child,
            Terminal,
        >>::task(task, address, nonce)
    }
}

pub(crate) struct ApplicationCapabilities<
    C,
    N,
    P = NoParent,
    Bindings = NoChildBindings,
    Origins = StructuralOrigins<C>,
> where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    actor_spaces: Arc<N>,
    allocations: ApplicationAddresses,
    address: MailAddr,
    control: ControlSender<C::Event>,
    timers: LocalTimers<C::Event>,
    observations: TerminationObservations<MailAddr, C::Event>,
    next_child_route: u64,
    child_bindings: Bindings,
    activation_tasks: ActivationTasks<C::Event>,
    exact_observations: Arc<Mutex<HashMap<ObservationId, oneshot::Sender<()>>>>,
    terminal_reports: LocalTerminalReports,
    parent_reports: P,
    origins: PhantomData<fn() -> Origins>,
}

pub(crate) struct NoParent;

impl<C, N, Bindings, Origins> ApplicationCapabilities<C, N, NoParent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    pub(crate) fn new_with_bindings(
        inputs: ApplicationCapabilityInputs<C, N>,
        child_bindings: Bindings,
    ) -> Self {
        Self {
            actor_spaces: inputs.actor_spaces,
            allocations: inputs.allocations,
            address: inputs.address,
            control: inputs.control,
            timers: inputs.timers,
            observations: inputs.observations,
            next_child_route: 0,
            child_bindings,
            activation_tasks: ActivationTasks::new(),
            exact_observations: Arc::new(Mutex::new(HashMap::new())),
            terminal_reports: inputs.terminal_reports,
            parent_reports: NoParent,
            origins: PhantomData,
        }
    }
}

impl<C, N, P, Bindings, Origins> ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn with_parent<Parent>(
        self,
        parent_reports: Parent,
    ) -> ApplicationCapabilities<C, N, Parent, Bindings, Origins> {
        ApplicationCapabilities {
            actor_spaces: self.actor_spaces,
            allocations: self.allocations,
            address: self.address,
            control: self.control,
            timers: self.timers,
            observations: self.observations,
            next_child_route: self.next_child_route,
            child_bindings: self.child_bindings,
            activation_tasks: self.activation_tasks,
            exact_observations: self.exact_observations,
            terminal_reports: self.terminal_reports,
            parent_reports,
            origins: PhantomData,
        }
    }

    fn inject_control_event<Control, Path>(&self, control: Control)
    where
        C::Event: InjectEvent<Control, Path>,
    {
        let event = C::Event::inject_at(control);
        match self.control.send(event) {
            Ok(()) => {}
            Err(ControlClosed(_)) => {
                unreachable!("the actor control lane remains live while its effects commit")
            }
        }
    }
}

impl<C, N, P, Bindings, Origins> TerminalReportTransaction
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn finish_terminal_reports(&mut self, disposition: TerminalReportDisposition) {
        self.terminal_reports.finish(disposition);
    }
}

impl<C, N, P, Bindings, Origins, New, RootEvent, Path>
    InterpretItem<Creations<CreateChild<MailAddr, New>>, RootEvent, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    New: Send,
    Bindings: Send,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        creations: Creations<CreateChild<MailAddr, New>>,
    ) -> ItemSettlement<
        Creations<CreateChild<MailAddr, New>>,
        Creations<RoutedCreation<MailAddr, New>>,
        ChildNamespaceExhausted,
        Never,
    > {
        let Ok(count) = u64::try_from(creations.len()) else {
            return ItemSettlement::Rejected {
                item: creations,
                reason: ChildNamespaceExhausted,
            };
        };
        let Some(next) = self.next_child_route.checked_add(count) else {
            return ItemSettlement::Rejected {
                item: creations,
                reason: ChildNamespaceExhausted,
            };
        };
        let mut route = self.next_child_route;
        self.next_child_route = next;
        ItemSettlement::Accepted(creations.map(|creation| {
            let routed = RoutedCreation::new(creation, route);
            route += 1;
            routed
        }))
    }
}

impl<C, N, P, Bindings, Origins, Source, Input> SourceAdmission<C::Event, Source, Input>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: EventIngress<Source, Input>,
    Input: Send,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn admit_source(&mut self, input: Input) -> Result<(), Input> {
        let event = C::Event::ingress(input);
        match self.control.send(event) {
            Ok(()) => Ok(()),
            Err(ControlClosed(_)) => {
                unreachable!("the actor control lane remains live while its effects commit")
            }
        }
    }
}

impl<C, N, P, Bindings, Origins, Position, Child> EstablishChild<Position, Child>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>> + BehaviorBase,
    C::Event: Send + 'static,
    BehaviorMessage<C>: Send,
    N: Send + Sync + 'static,
    P: Send,
    Position: 'static,
    Bindings: ChildBindingAt<Position, Child = Child, Root = BindingTerminal<Bindings>>
        + NestedChildBindings<Child>
        + RetireChildTasks
        + Send,
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>
        + BehaviorBase
        + Send
        + 'static,
    <Child as BehaviorSettlements>::Settlements: ClassifySettlement + Send + 'static,
    Child::Event: Send + 'static,
    BehaviorMessage<Child>: Send + 'static,
    Child::Sends: Send + 'static,
    Child::Error: Send + 'static,
    <Child::Birth as BirthMode>::Child: Send + 'static,
    BindingTerminal<Bindings>: Send + 'static,
    NestedBindings<Bindings, Child>:
        Default + RetireChildTasks<Root = BindingTerminal<Bindings>> + Send + 'static,
    ActionInterpreter<
        ApplicationCapabilities<
            Child,
            N,
            LocalParentReports<C::Event, Child, C::Birth>,
            NestedBindings<Bindings, Child>,
            StructuralOrigins<Child::Base>,
        >,
    >: CommitActions<Child, Retired = Vec<BindingTerminal<Bindings>>> + Send + 'static,
    Origins: ProjectChildTerminal<Position, Child, BindingTerminal<Bindings>>,
{
    #[allow(
        clippy::too_many_lines,
        reason = "one exhaustive child-establishment transition owns allocation, launch, binding, and every exact settlement"
    )]
    async fn establish_child(
        &mut self,
        creation: RoutedCreation<MailAddr, Child>,
    ) -> ItemSettlement<
        RoutedCreation<MailAddr, Child>,
        ChildCreationOutcome<Child, Position>,
        CreationRejection,
        Never,
    > {
        let id = creation.id();
        let kind = creation.kind();
        let route = creation.route();
        let (request, _) = creation.into_parts();
        let (_, child, _) = request.into_parts();
        let address = match self.allocations.allocate() {
            Ok(address) => address,
            Err(reason) => {
                self.child_bindings
                    .record_creation(id, CreationBinding::Rejected);
                return ItemSettlement::Rejected {
                    item: routed_creation(id, kind, route, child),
                    reason: CreationRejection::Allocation(reason),
                };
            }
        };
        let actors = self.child_bindings.child_actors();
        let actor_spaces = self.actor_spaces.clone();
        let allocations = self.allocations.clone();
        let parent = self.control.clone();
        let installed = spawn_owned_with(
            actors,
            communication::Config::new(DEFAULT_USER_CAPACITY),
            address,
            child,
            move |control, terminal_reports, timers, observations| {
                ActionInterpreter::new(
                    ApplicationCapabilities::<
                        Child,
                        N,
                        NoParent,
                        NestedBindings<Bindings, Child>,
                        StructuralOrigins<Child::Base>,
                    >::new_with_bindings(
                        ApplicationCapabilityInputs {
                            address,
                            actor_spaces,
                            allocations,
                            control,
                            timers,
                            observations,
                            terminal_reports,
                        },
                        NestedBindings::<Bindings, Child>::default(),
                    )
                    .with_parent(
                        LocalParentReports::<C::Event, Child, C::Birth>::new(id, parent),
                    ),
                )
            },
        )
        .await;
        match installed {
            Ok(mut installed) => {
                let endpoint = installed.actor.clone();
                let control = installed.control.clone();
                let binding = installed
                    .binding
                    .take()
                    .expect("fresh child installation awaits one binding acknowledgement");
                let task = Origins::task(installed.task, address, route);
                self.child_bindings.record_creation(
                    id,
                    CreationBinding::Established {
                        kind,
                        endpoint: endpoint.clone(),
                        control: installed.control,
                        task,
                    },
                );
                let acknowledgement = binding.send(());
                assert!(
                    acknowledgement.is_ok(),
                    "the privately committed child awaits its recorded binding"
                );
                let actor =
                    EstablishedActor::<Child>::issued(InstalledActor::new(endpoint, control));
                ItemSettlement::Accepted(ChildCreationOutcome::Established(CommittedChild::new(
                    id, kind, actor,
                )))
            }
            Err(SpawnError::AllocationRejected { behavior, reason }) => {
                self.child_bindings
                    .record_creation(id, CreationBinding::Rejected);
                ItemSettlement::Rejected {
                    item: routed_creation(id, kind, route, behavior),
                    reason: CreationRejection::Allocation(reason),
                }
            }
            Err(SpawnError::InitializationRejected {
                behavior,
                error,
                control,
                user,
                descendants,
            }) => {
                assert!(control.is_empty());
                assert!(user.is_empty());
                assert!(descendants.is_empty());
                self.child_bindings
                    .record_creation(id, CreationBinding::Rejected);
                ItemSettlement::Accepted(ChildCreationOutcome::InitializationRejected {
                    creation: routed_creation(id, kind, route, behavior),
                    error,
                })
            }
            Err(SpawnError::InitializationPanicked {
                behavior,
                control,
                user,
                descendants,
            }) => {
                assert!(control.is_empty());
                assert!(user.is_empty());
                assert!(descendants.is_empty());
                self.child_bindings
                    .record_creation(id, CreationBinding::Rejected);
                ItemSettlement::Accepted(ChildCreationOutcome::InitializationPanicked {
                    creation: routed_creation(id, kind, route, behavior),
                })
            }
            Err(SpawnError::HostRejected {
                behavior,
                initialization,
                error,
                control,
                user,
                descendants,
            }) => {
                assert!(control.is_empty());
                assert!(user.is_empty());
                assert!(descendants.is_empty());
                let reason = match error {
                    ClaimError::AddressInUse(_) => CreationRejection::Allocation(
                        behavior::AllocationRejection::AddressAlreadyClaimed,
                    ),
                    ClaimError::RegistrationIdsExhausted(_) => CreationRejection::EnvironmentFailed,
                };
                self.child_bindings
                    .record_creation(id, CreationBinding::Rejected);
                ItemSettlement::Accepted(ChildCreationOutcome::HostRejected {
                    creation: routed_creation(id, kind, route, behavior),
                    initialization,
                    reason,
                })
            }
            Err(SpawnError::BindingAbandoned {
                behavior,
                initialization,
                control,
                user,
                descendants,
            }) => {
                assert!(control.is_empty());
                assert!(user.is_empty());
                assert!(descendants.is_empty());
                self.child_bindings
                    .record_creation(id, CreationBinding::Rejected);
                ItemSettlement::Accepted(ChildCreationOutcome::HostRejected {
                    creation: routed_creation(id, kind, route, behavior),
                    initialization,
                    reason: CreationRejection::EnvironmentFailed,
                })
            }
            Err(SpawnError::Panicked | SpawnError::Cancelled | SpawnError::Unpublished(_)) => {
                unreachable!(
                    "private child commitment either returns its owned task or a precommit rejection"
                )
            }
        }
    }
}

impl<C, N, P, Bindings, Origins, Target, RootEvent, Path>
    InterpretItem<Delivery<Target>, RootEvent, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send,
    N: ResolveLogical<Target> + Send + Sync,
    Self: Send,
{
    async fn interpret_item(
        &mut self,
        delivery: Delivery<Target>,
    ) -> ItemSettlement<Delivery<Target>, (), LogicalDeliveryReason, Never> {
        let address = delivery.to.address();
        let Some(actor) = self.actor_spaces.resolve_logical(address) else {
            return ItemSettlement::Rejected {
                item: delivery,
                reason: LogicalDeliveryReason::UnknownAddress,
            };
        };
        let Delivery { to, message } = delivery;
        match actor.send_from(self.address, message).await {
            Ok(()) => ItemSettlement::Accepted(()),
            Err(error) => ItemSettlement::Rejected {
                item: Delivery::new(to, error.into_message()),
                reason: LogicalDeliveryReason::ClosedRecipient,
            },
        }
    }
}

impl<C, N, P, Bindings, Origins, Target, RootEvent, Path>
    InterpretItem<EstablishedDelivery<Target>, RootEvent, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send,
    Self: Send,
{
    async fn interpret_item(
        &mut self,
        delivery: EstablishedDelivery<Target>,
    ) -> ItemSettlement<EstablishedDelivery<Target>, (), ExactDeliveryReason, Never> {
        let EstablishedDelivery { to, message } = delivery;
        let endpoint = to.clone().interpret(&mut ExtractLocalEndpoint);
        match endpoint.send_from(self.address, message).await {
            Ok(()) => ItemSettlement::Accepted(()),
            Err(error) => ItemSettlement::Rejected {
                item: EstablishedDelivery::new(to, error.into_message()),
                reason: ExactDeliveryReason::ClosedRecipient,
            },
        }
    }
}

fn reunite_customer_delivery<Target, Leaf, Reason>(
    settlement: ItemSettlement<Leaf, (), Reason, Never>,
    restore: impl FnOnce(Leaf) -> CustomerDelivery<Target>,
    classify: impl FnOnce(Reason) -> ReplyDelivery<LogicalDeliveryReason, ExactDeliveryReason>,
) -> ItemSettlement<
    CustomerDelivery<Target>,
    (),
    ReplyDelivery<LogicalDeliveryReason, ExactDeliveryReason>,
    Never,
>
where
    Target: Protocol<Addr = MailAddr>,
{
    match settlement {
        ItemSettlement::Accepted(()) => ItemSettlement::Accepted(()),
        ItemSettlement::Rejected { item, reason } => ItemSettlement::Rejected {
            item: restore(item),
            reason: classify(reason),
        },
        ItemSettlement::Blocked { prerequisite, .. } => match prerequisite {},
        ItemSettlement::Corrupt { item, fault } => ItemSettlement::Corrupt {
            item: restore(item),
            fault,
        },
    }
}

impl<C, N, Parent, Bindings, Origins, Target, RootEvent, Path>
    InterpretItem<CustomerDelivery<Target>, RootEvent, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send,
    Self: InterpretItem<Delivery<Target>, RootEvent, Path>
        + InterpretItem<EstablishedDelivery<Target>, RootEvent, Path>
        + Send,
{
    async fn interpret_item(
        &mut self,
        customer_delivery: CustomerDelivery<Target>,
    ) -> ItemSettlement<
        CustomerDelivery<Target>,
        (),
        ReplyDelivery<LogicalDeliveryReason, ExactDeliveryReason>,
        Never,
    > {
        match customer_delivery {
            CustomerDelivery::Logical { delivery } => reunite_customer_delivery(
                self.interpret_item(delivery).await,
                |delivery| CustomerDelivery::Logical { delivery },
                ReplyDelivery::Logical,
            ),
            CustomerDelivery::Established { delivery } => reunite_customer_delivery(
                self.interpret_item(delivery).await,
                |delivery| CustomerDelivery::Established { delivery },
                ReplyDelivery::Established,
            ),
            CustomerDelivery::RejectedLogical { delivery, customer } => reunite_customer_delivery(
                self.interpret_item(delivery).await,
                |delivery| CustomerDelivery::RejectedLogical { delivery, customer },
                ReplyDelivery::Logical,
            ),
            CustomerDelivery::RejectedEstablished { delivery, customer } => {
                reunite_customer_delivery(
                    self.interpret_item(delivery).await,
                    |delivery| CustomerDelivery::RejectedEstablished { delivery, customer },
                    ReplyDelivery::Established,
                )
            }
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Target, RootEvent, Path>
    InterpretItem<DiagnosticAction<Recipient<Target>, Target::Msg>, RootEvent, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send,
    Self: InterpretItem<Delivery<Target>, RootEvent, Path> + Send,
{
    async fn interpret_item(
        &mut self,
        action: DiagnosticAction<Recipient<Target>, Target::Msg>,
    ) -> ItemSettlement<
        DiagnosticAction<Recipient<Target>, Target::Msg>,
        DiagnosticAccepted<Target::Msg>,
        LogicalDeliveryReason,
        Never,
    > {
        match action {
            DiagnosticAction::Terminal { diagnostic } => {
                ItemSettlement::Accepted(DiagnosticAccepted::terminal(diagnostic))
            }
            DiagnosticAction::Deliver { route, diagnostic } => {
                match self.interpret_item(Delivery::new(route, diagnostic)).await {
                    ItemSettlement::Accepted(()) => {
                        ItemSettlement::Accepted(DiagnosticAccepted::delivered())
                    }
                    ItemSettlement::Rejected {
                        item: Delivery { to, message },
                        reason,
                    } => ItemSettlement::Rejected {
                        item: DiagnosticAction::deliver(to, message),
                        reason,
                    },
                    ItemSettlement::Blocked { prerequisite, .. } => match prerequisite {},
                    ItemSettlement::Corrupt {
                        item: Delivery { to, message },
                        fault,
                    } => ItemSettlement::Corrupt {
                        item: DiagnosticAction::deliver(to, message),
                        fault,
                    },
                }
            }
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Target, RootEvent, Path>
    InterpretItem<DiagnosticAction<EstablishedRecipient<Target>, Target::Msg>, RootEvent, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send,
    Self: InterpretItem<EstablishedDelivery<Target>, RootEvent, Path> + Send,
{
    async fn interpret_item(
        &mut self,
        action: DiagnosticAction<EstablishedRecipient<Target>, Target::Msg>,
    ) -> ItemSettlement<
        DiagnosticAction<EstablishedRecipient<Target>, Target::Msg>,
        DiagnosticAccepted<Target::Msg>,
        ExactDeliveryReason,
        Never,
    > {
        match action {
            DiagnosticAction::Terminal { diagnostic } => {
                ItemSettlement::Accepted(DiagnosticAccepted::terminal(diagnostic))
            }
            DiagnosticAction::Deliver { route, diagnostic } => {
                match self
                    .interpret_item(EstablishedDelivery::new(route, diagnostic))
                    .await
                {
                    ItemSettlement::Accepted(()) => {
                        ItemSettlement::Accepted(DiagnosticAccepted::delivered())
                    }
                    ItemSettlement::Rejected {
                        item: EstablishedDelivery { to, message },
                        reason,
                    } => ItemSettlement::Rejected {
                        item: DiagnosticAction::deliver(to, message),
                        reason,
                    },
                    ItemSettlement::Blocked { prerequisite, .. } => match prerequisite {},
                    ItemSettlement::Corrupt {
                        item: EstablishedDelivery { to, message },
                        fault,
                    } => ItemSettlement::Corrupt {
                        item: DiagnosticAction::deliver(to, message),
                        fault,
                    },
                }
            }
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Diagnostic, RootEvent, Path>
    InterpretItem<DiagnosticAction<Infallible, Diagnostic>, RootEvent, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Diagnostic: Send,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        action: DiagnosticAction<Infallible, Diagnostic>,
    ) -> ItemSettlement<
        DiagnosticAction<Infallible, Diagnostic>,
        DiagnosticAccepted<Diagnostic>,
        Never,
        Never,
    > {
        match action {
            DiagnosticAction::Deliver { route, .. } => match route {},
            DiagnosticAction::Terminal { diagnostic } => {
                ItemSettlement::Accepted(DiagnosticAccepted::terminal(diagnostic))
            }
        }
    }
}

impl<C, N, P, Bindings, Origins, Target, Occurrence, RootEvent, Path>
    InterpretItem<ChildDelivery<Target, Occurrence>, RootEvent, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>> + ResolveChildOccurrence<Occurrence>,
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send,
    ResolvedChild<C, Occurrence>: Behavior<Protocol = Target>,
    Bindings:
        ChildBindingAt<ResolvedChildPosition<C, Occurrence>, Child = ResolvedChild<C, Occurrence>>,
    Self: Send,
{
    async fn interpret_item(
        &mut self,
        delivery: ChildDelivery<Target, Occurrence>,
    ) -> ItemSettlement<
        ChildDelivery<Target, Occurrence>,
        (),
        ChildDeliveryReason,
        CreationCorrelation<Target, Occurrence>,
    > {
        let actor = match self.child_bindings.creation(delivery.creation) {
            Some(CreationBinding::Established { endpoint, .. }) => endpoint.clone(),
            Some(CreationBinding::Rejected) => {
                let prerequisite = CreationCorrelation::new(delivery.creation);
                return ItemSettlement::Blocked {
                    item: delivery,
                    prerequisite,
                };
            }
            None => {
                return ItemSettlement::Rejected {
                    item: delivery,
                    reason: ChildDeliveryReason::MissingBinding,
                };
            }
        };
        let ChildDelivery {
            creation, message, ..
        } = delivery;
        match actor.send_from(self.address, message).await {
            Ok(()) => ItemSettlement::Accepted(()),
            Err(error) => ItemSettlement::Rejected {
                item: ChildDelivery::after(creation, error.into_message()),
                reason: ChildDeliveryReason::ClosedRecipient,
            },
        }
    }
}

impl<C, N, P, Bindings, Origins, Child, Source, Input, Occurrence, RootEvent, Path>
    InterpretItem<ChildInput<Child, Source, Input, Occurrence>, RootEvent, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>
        + ResolveChildOccurrence<Occurrence, Child = Child>,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child::Event: InjectEvent<ShutdownRequested, Here>,
    Child::Event: ChildInputIngress<Source, Input> + Send,
    Input: Send,
    Bindings: ChildBindingAt<ResolvedChildPosition<C, Occurrence>, Child = Child> + Send,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        input: ChildInput<Child, Source, Input, Occurrence>,
    ) -> ItemSettlement<
        ChildInput<Child, Source, Input, Occurrence>,
        (),
        ChildInputReason,
        CreationCorrelation<Child::Protocol, Occurrence>,
    > {
        let control = match self.child_bindings.creation(input.creation) {
            Some(CreationBinding::Established { control, .. }) => control.clone(),
            Some(CreationBinding::Rejected) => {
                let prerequisite = CreationCorrelation::new(input.creation);
                return ItemSettlement::Blocked {
                    item: input,
                    prerequisite,
                };
            }
            None => {
                return ItemSettlement::Rejected {
                    item: input,
                    reason: ChildInputReason::MissingBinding,
                };
            }
        };
        let event = Child::Event::child_input(input.input);
        match control.send(event) {
            Ok(()) => ItemSettlement::Accepted(()),
            Err(ControlClosed(_)) => {
                unreachable!("an owned child control lane outlives its parent binding")
            }
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Worker, Plan> ProxyControlAdmission<Worker, Plan>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>
        + ResolveChildOccurrence<ChildHead, Child = StableProxy<Worker, Plan>>,
    Worker: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Plan: ActivationPlan,
    StableProxy<Worker, Plan>: Behavior<Protocol = Worker::Protocol>,
    <StableProxy<Worker, Plan> as Behavior>::Event: ChildInputIngress<StableProxy<Worker, Plan>, ProxyControl<Worker, Plan>>
        + RecoverEvent<ProxyControl<Worker, Plan>, Here>,
    Bindings: ChildBindingAt<ResolvedChildPosition<C, ChildHead>, Child = StableProxy<Worker, Plan>>
        + Send,
    Self: Send,
{
    fn admit_proxy_control(
        &mut self,
        creation: CreationId,
        request: ProxyControl<Worker, Plan>,
    ) -> ItemSettlement<
        ProxyControl<Worker, Plan>,
        EstablishedActor<StableProxy<Worker, Plan>>,
        ChildInputReason,
        Never,
    > {
        let Some(CreationBinding::Established {
            endpoint, control, ..
        }) = self.child_bindings.creation(creation)
        else {
            return ItemSettlement::Rejected {
                item: request,
                reason: ChildInputReason::MissingBinding,
            };
        };
        let actor = EstablishedActor::<StableProxy<Worker, Plan>>::issued(InstalledActor::new(
            endpoint.clone(),
            control.clone(),
        ));
        let event = <StableProxy<Worker, Plan> as Behavior>::Event::child_input(request);
        match control.send(event) {
            Ok(()) => ItemSettlement::Accepted(actor),
            Err(ControlClosed(event)) => {
                let request = <StableProxy<Worker, Plan> as Behavior>::Event::recover(event)
                    .unwrap_or_else(|_| unreachable!("the proxy control event was just injected"));
                ItemSettlement::Rejected {
                    item: request,
                    reason: ChildInputReason::ClosedControlLane,
                }
            }
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Worker, Plan, RootEvent, Path>
    InterpretItem<ProxyOperation<Here, Worker, Plan>, RootEvent, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Worker: Behavior<Protocol: Protocol<Addr = MailAddr>> + Send,
    Plan: ActivationPlan,
    StableProxy<Worker, Plan>: Behavior<Protocol = Worker::Protocol>,
    EstablishedActor<StableProxy<Worker, Plan>>: Send,
    Self: ProxyControlAdmission<Worker, Plan> + Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        operation: ProxyOperation<Here, Worker, Plan>,
    ) -> ItemSettlement<
        ProxyOperation<Here, Worker, Plan>,
        ProxyInputReceipt<Worker, Plan>,
        ChildInputReason,
        Never,
    > {
        operation.settle(self)
    }
}

impl<C, N, Parent, Bindings, Origins, Target, Job, RootEvent, Path>
    InterpretItem<AssignWorker<Target, Job>, RootEvent, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr, Msg = Assignment<Job>>,
    Job: Send,
    Self: InterpretItem<behavior::EstablishedDelivery<Target>, RootEvent, Path> + Send,
{
    async fn interpret_item(
        &mut self,
        assignment: AssignWorker<Target, Job>,
    ) -> ItemSettlement<AssignWorker<Target, Job>, AssignmentReceipt, ExactDeliveryReason, Never>
    {
        assignment.settle(self).await
    }
}

impl<C, N, Parent, Bindings, Origins, Worker, Plan, Path>
    InterpretItem<InitializeWorker<Worker, Plan>, C::Event, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<WorkerInitializationReport<Worker, Plan>, Path>,
    Worker: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Worker::Protocol: Protocol<Addr = MailAddr>,
    BehaviorMessage<Worker>: Send,
    Plan: Send,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        request: InitializeWorker<Worker, Plan>,
    ) -> ItemSettlement<InitializeWorker<Worker, Plan>, (), Never, Never> {
        let worker = request.target().interpret(&mut ExtractLocalEndpoint);
        let outcome = match worker.termination_observation().try_get() {
            Some(termination) => WorkerInitializationOutcome::Stopped(ChildStopped::new(
                request.worker().creation(),
                termination,
                Instant::now(),
            )),
            None => WorkerInitializationOutcome::ReadyForActivation,
        };
        self.inject_control_event::<_, Path>(request.resolve(outcome));
        ItemSettlement::Accepted(())
    }
}

impl<C, N, Parent, Bindings, Origins, Worker, Plan, Path>
    InterpretItem<BeginActivation<Worker, Plan>, C::Event, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<WorkerActivation<Worker, Plan>, Path> + Send + 'static,
    Worker: Behavior<Protocol: Protocol<Addr = MailAddr>> + 'static,
    BehaviorMessage<Worker>: Send,
    Plan: ActivationPlan + 'static,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        request: BeginActivation<Worker, Plan>,
    ) -> ItemSettlement<BeginActivation<Worker, Plan>, (), ActivationStartRejection, Never> {
        self.inject_control_event::<_, Path>(request.started());
        let control = self.control.clone();
        self.activation_tasks.spawn(async move {
            let event = C::Event::inject_at(request.activate().await);
            control.send(event).map_err(|ControlClosed(event)| event)
        });
        ItemSettlement::Accepted(())
    }
}

impl<C, N, P, Bindings, Origins, Source, Role, Worker, Plan, RootEvent, Path>
    InterpretItem<PrepareWorkers<Source, Role, Worker, Plan>, RootEvent, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<WorkerPreparation<Source, Role, Worker, Plan>, Path> + Send + 'static,
    Source:
        WorkerSource<Role, Worker, Plan> + WorkerPreparationSource<Role, Worker, Plan> + 'static,
    Role: Send + Sync + 'static,
    Worker: Behavior + Send + 'static,
    Plan: ActivationPlan + 'static,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        request: PrepareWorkers<Source, Role, Worker, Plan>,
    ) -> ItemSettlement<
        PrepareWorkers<Source, Role, Worker, Plan>,
        WorkerPreparationStarted,
        Never,
        Never,
    > {
        let (receipt, starting) = request.start();
        let control = self.control.clone();
        self.activation_tasks.spawn(async move {
            let event = C::Event::inject_at(settle_worker_preparation(starting).await);
            control.send(event).map_err(|ControlClosed(event)| event)
        });
        ItemSettlement::Accepted(receipt)
    }
}

impl<C, N, P, Bindings, Origins, Path> InterpretItem<ScheduleAt, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<TimerElapsed, Path>,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        request: ScheduleAt,
    ) -> ItemSettlement<ScheduleAt, TimerScheduled, ScheduleAtRejection, Never> {
        match self.timers.schedule_at::<Path>(request) {
            Ok(()) => ItemSettlement::Accepted(TimerScheduled {
                id: request.id,
                generation: request.generation,
            }),
            Err(crate::time::TimerError::GenerationExhausted) => ItemSettlement::Rejected {
                item: request,
                reason: ScheduleAtRejection::QueueGenerationExhausted,
            },
            Err(crate::time::TimerError::SequenceExhausted) => ItemSettlement::Rejected {
                item: request,
                reason: ScheduleAtRejection::QueueSequenceExhausted,
            },
            Err(crate::time::TimerError::DeadlineOverflow) => {
                unreachable!("absolute scheduling does not add a relative deadline")
            }
        }
    }
}

impl<C, N, P, Bindings, Origins, D, Path> InterpretItem<EntityAdmission<D>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    D: EntityDefinition,
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal>,
    Self: Send,
{
    async fn interpret_item(
        &mut self,
        request: EntityAdmission<D>,
    ) -> ItemSettlement<EntityAdmission<D>, (), Never, Never> {
        request.interpret(self.address).await;
        ItemSettlement::Accepted(())
    }
}

impl<C, N, P, Bindings, Origins, Path> InterpretItem<ScheduleAfter, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<TimerElapsed, Path>,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        request: ScheduleAfter,
    ) -> ItemSettlement<ScheduleAfter, TimerScheduled, ScheduleAfterRejection, Never> {
        match self.timers.schedule_after::<Path>(request) {
            Ok(()) => ItemSettlement::Accepted(TimerScheduled {
                id: request.id,
                generation: request.generation,
            }),
            Err(crate::time::TimerError::DeadlineOverflow) => ItemSettlement::Rejected {
                item: request,
                reason: ScheduleAfterRejection::DeadlineOverflow,
            },
            Err(crate::time::TimerError::GenerationExhausted) => ItemSettlement::Rejected {
                item: request,
                reason: ScheduleAfterRejection::QueueGenerationExhausted,
            },
            Err(crate::time::TimerError::SequenceExhausted) => ItemSettlement::Rejected {
                item: request,
                reason: ScheduleAfterRejection::QueueSequenceExhausted,
            },
        }
    }
}

impl<C, N, P, Bindings, Origins, ChildProtocol, Occurrence, Path>
    InterpretItem<ObserveCreation<ChildProtocol, Occurrence>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>> + ResolveChildOccurrence<Occurrence>,
    ChildProtocol: Protocol<Addr = MailAddr>,
    ResolvedChild<C, Occurrence>: Behavior<Protocol = ChildProtocol>,
    Bindings:
        ChildBindingAt<ResolvedChildPosition<C, Occurrence>, Child = ResolvedChild<C, Occurrence>>,
    C::Event: InjectEvent<CreationResolved<MailAddr>, Path>,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        request: ObserveCreation<ChildProtocol, Occurrence>,
    ) -> ItemSettlement<
        ObserveCreation<ChildProtocol, Occurrence>,
        (),
        Never,
        CreationCorrelation<ChildProtocol, Occurrence>,
    > {
        match self.child_bindings.creation(request.creation) {
            Some(CreationBinding::Established { endpoint, kind, .. }) => {
                let address = endpoint.address();
                let kind = *kind;
                self.inject_control_event::<_, Path>(CreationResolved::installed(
                    request.creation,
                    kind,
                    address,
                ));
                ItemSettlement::Accepted(())
            }
            Some(CreationBinding::Rejected) => ItemSettlement::Blocked {
                prerequisite: CreationCorrelation::new(request.creation),
                item: request,
            },
            None => ItemSettlement::Corrupt {
                item: request,
                fault: InterpreterFault::MissingCapability,
            },
        }
    }
}

impl<C, N, P, Bindings, Origins, Child, Occurrence, Path>
    InterpretItem<ObserveEstablishedCreation<Child, Occurrence>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>
        + ResolveChildOccurrence<Occurrence, Child = Child>,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Bindings: ChildBindingAt<ResolvedChildPosition<C, Occurrence>, Child = Child>,
    C::Event: InjectEvent<EstablishedCreation<Child, Occurrence>, Path>,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        request: ObserveEstablishedCreation<Child, Occurrence>,
    ) -> ItemSettlement<
        ObserveEstablishedCreation<Child, Occurrence>,
        (),
        Never,
        CreationCorrelation<Child::Protocol, Occurrence>,
    > {
        match self.child_bindings.creation(request.creation) {
            Some(CreationBinding::Established {
                endpoint,
                control,
                kind,
                ..
            }) => {
                let actor = EstablishedActor::<Child>::issued(InstalledActor::new(
                    endpoint.clone(),
                    control.clone(),
                ));
                let child = CommittedChild::new(request.creation, *kind, actor);
                self.inject_control_event::<_, Path>(EstablishedCreation::installed(child));
                ItemSettlement::Accepted(())
            }
            Some(CreationBinding::Rejected) => ItemSettlement::Blocked {
                prerequisite: CreationCorrelation::new(request.creation),
                item: request,
            },
            None => ItemSettlement::Corrupt {
                item: request,
                fault: InterpreterFault::MissingCapability,
            },
        }
    }
}

impl<C, N, P, Bindings, Origins, Path> InterpretItem<ObservePeer<MailAddr>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<PeerStopped<MailAddr>, Path> + Send + 'static,
    BehaviorMessage<C>: Send,
    N: Hosts<C::Protocol>,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        request: ObservePeer<MailAddr>,
    ) -> ItemSettlement<ObservePeer<MailAddr>, (), PeerObservationRejection, Never> {
        let result = observe_peer::<C::Protocol, C::Event, Path>(
            self.actor_spaces.space(),
            request,
            &mut self.observations,
        );
        match result {
            Ok(()) => ItemSettlement::Accepted(()),
            Err(_) => ItemSettlement::Rejected {
                item: request,
                reason: PeerObservationRejection::UnknownAddress,
            },
        }
    }
}

impl<C, N, P, Bindings, Origins, ChildProtocol, Occurrence, Path>
    InterpretItem<ObserveChild<ChildProtocol, Occurrence>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>> + ResolveChildOccurrence<Occurrence>,
    ChildProtocol: Protocol<Addr = MailAddr>,
    C::Event: InjectEvent<ChildStopped<MailAddr>, Path> + Send + 'static,
    ResolvedChild<C, Occurrence>: Behavior<Protocol = ChildProtocol>,
    Bindings:
        ChildBindingAt<ResolvedChildPosition<C, Occurrence>, Child = ResolvedChild<C, Occurrence>>,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        request: ObserveChild<ChildProtocol, Occurrence>,
    ) -> ItemSettlement<
        ObserveChild<ChildProtocol, Occurrence>,
        (),
        Never,
        CreationCorrelation<ChildProtocol, Occurrence>,
    > {
        let child = match self.child_bindings.creation(request.child) {
            Some(CreationBinding::Established { endpoint, .. }) => endpoint.clone(),
            Some(CreationBinding::Rejected) => {
                return ItemSettlement::Blocked {
                    prerequisite: CreationCorrelation::new(request.child),
                    item: request,
                };
            }
            None => {
                return ItemSettlement::Corrupt {
                    item: request,
                    fault: InterpreterFault::MissingCapability,
                };
            }
        };
        self.observations
            .insert_child::<Path>(request.child, child.termination_observation());
        ItemSettlement::Accepted(())
    }
}

impl<C, N, P, Bindings, Origins, Child, Occurrence, Path>
    InterpretItem<ShutdownChild<Child, Occurrence>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>
        + ResolveChildOccurrence<Occurrence, Child = Child>,
    C::Event: InjectEvent<ChildStopped<MailAddr>, Path> + Send + 'static,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child::Event: InjectEvent<ShutdownRequested, Here>,
    Bindings: ChildBindingAt<ResolvedChildPosition<C, Occurrence>, Child = Child>,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        request: ShutdownChild<Child, Occurrence>,
    ) -> ItemSettlement<
        ShutdownChild<Child, Occurrence>,
        (),
        ChildShutdownRejection,
        CreationCorrelation<Child::Protocol, Occurrence>,
    > {
        let (child, control) = match self.child_bindings.creation(request.child) {
            Some(CreationBinding::Established {
                endpoint, control, ..
            }) => (endpoint.clone(), control.clone()),
            Some(CreationBinding::Rejected) => {
                return ItemSettlement::Blocked {
                    prerequisite: CreationCorrelation::new(request.child),
                    item: request,
                };
            }
            None => {
                return ItemSettlement::Corrupt {
                    item: request,
                    fault: InterpreterFault::MissingCapability,
                };
            }
        };
        match request_actor_shutdown(&child, &control, request.ingress) {
            Ok(()) => {
                self.observations
                    .insert_child::<Path>(request.child, child.termination_observation());
                ItemSettlement::Accepted(())
            }
            Err(ShutdownRejection::AlreadyStopping) => ItemSettlement::Rejected {
                item: request,
                reason: ChildShutdownRejection::AlreadyStopping,
            },
            Err(ShutdownRejection::AlreadyStopped) => ItemSettlement::Rejected {
                item: request,
                reason: ChildShutdownRejection::NotEstablished,
            },
        }
    }
}

impl<C, N, P, Bindings, Origins, Report, Path> InterpretItem<ReportToParent<Report>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    P: ParentReporting<Report> + Send,
    Report: Send,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        request: ReportToParent<Report>,
    ) -> ItemSettlement<ReportToParent<Report>, (), ParentReportReason, Never> {
        self.parent_reports.report(request.into_inner());
        ItemSettlement::Accepted(())
    }
}

impl<C, N, P, Bindings, Origins, Path>
    InterpretItem<ReportTerminalOutcome<MailAddr>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        request: ReportTerminalOutcome<MailAddr>,
    ) -> ItemSettlement<ReportTerminalOutcome<MailAddr>, (), Never, Never> {
        self.terminal_reports.report_outcome(request);
        ItemSettlement::Accepted(())
    }
}

impl<C, N, P, Bindings, Origins, Plan, Path> InterpretItem<ReportShutdownPlan<Plan>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: EventIngress<Here, InstallShutdownPlan<Plan>>,
    Plan: Send,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        request: ReportShutdownPlan<Plan>,
    ) -> ItemSettlement<ReportShutdownPlan<Plan>, (), Never, Never> {
        match self.control.send(request.into_event()) {
            Ok(()) => ItemSettlement::Accepted(()),
            Err(ControlClosed(_)) => {
                unreachable!("the actor control lane remains live while its effects commit")
            }
        }
    }
}

struct EstablishedObservationInterpreter<'a, Capabilities, Path> {
    capabilities: &'a mut Capabilities,
    path: PhantomData<fn() -> Path>,
}

impl<'a, Capabilities, Path> EstablishedObservationInterpreter<'a, Capabilities, Path> {
    fn new(capabilities: &'a mut Capabilities) -> Self {
        Self {
            capabilities,
            path: PhantomData,
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Target, Path> InterpretEstablishedObservation<Target>
    for EstablishedObservationInterpreter<
        '_,
        ApplicationCapabilities<C, N, Parent, Bindings, Origins>,
        Path,
    >
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr> + Send + 'static,
    Target::Msg: Send,
    C::Event: InjectEvent<EstablishedObservation<Target>, Path> + Send + 'static,
    Self: Send,
{
    type Output = ();

    fn observe(&mut self, id: ObservationId, endpoint: ActorRef<Target>) {
        let mut observations = self
            .capabilities
            .exact_observations
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if observations.contains_key(&id) {
            drop(observations);
            self.capabilities
                .inject_control_event::<_, Path>(EstablishedObservation::rejected(
                    id,
                    ObservationOperation::Start,
                    ObservationRejection::IdAlreadyBound,
                ));
            return;
        }
        let (cancel, cancelled) = oneshot::channel();
        observations.insert(id, cancel);
        drop(observations);
        let active = self.capabilities.exact_observations.clone();
        let control = self.capabilities.control.clone();
        let termination = endpoint.termination();
        self.capabilities.activation_tasks.spawn(async move {
            tokio::select! {
                outcome = termination => {
                    active.lock().unwrap_or_else(PoisonError::into_inner).remove(&id);
                    let event = C::Event::inject_at(EstablishedObservation::<Target>::stopped(
                        id,
                        outcome,
                        Instant::now(),
                    ));
                    control.send(event).map_err(|ControlClosed(event)| event)
                }
                _ = cancelled => Ok(()),
            }
        });
        self.capabilities
            .inject_control_event::<_, Path>(EstablishedObservation::<Target>::started(id));
    }

    fn cancel(&mut self, id: ObservationId) {
        let cancellation = self
            .capabilities
            .exact_observations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&id);
        match cancellation {
            Some(cancellation) => {
                match cancellation.send(()) {
                    Ok(()) | Err(()) => {}
                }
                self.capabilities.inject_control_event::<_, Path>(
                    EstablishedObservation::<Target>::cancelled(id),
                );
            }
            None => {
                self.capabilities.inject_control_event::<_, Path>(
                    EstablishedObservation::rejected(
                        id,
                        ObservationOperation::Cancel,
                        ObservationRejection::NotObserved,
                    ),
                );
            }
        }
    }
}

impl<C, N, P, Bindings, Origins, Target, Path>
    InterpretItem<ObserveEstablished<Target>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr> + Send + 'static,
    Target::Msg: Send,
    C::Event: InjectEvent<EstablishedObservation<Target>, Path> + Send + 'static,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        request: ObserveEstablished<Target>,
    ) -> ItemSettlement<ObserveEstablished<Target>, (), Never, Never> {
        request.interpret(&mut EstablishedObservationInterpreter::<_, Path>::new(self));
        ItemSettlement::Accepted(())
    }
}

impl<C, N, P, Bindings, Origins, Target, Path>
    InterpretItem<CancelObservation<Target>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr> + Send + 'static,
    Target::Msg: Send,
    C::Event: InjectEvent<EstablishedObservation<Target>, Path> + Send + 'static,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        request: CancelObservation<Target>,
    ) -> ItemSettlement<CancelObservation<Target>, (), Never, Never> {
        request.interpret(&mut EstablishedObservationInterpreter::<_, Path>::new(self));
        ItemSettlement::Accepted(())
    }
}

impl<C, N, Parent, Bindings, Origins, Child> InterpretEstablishedShutdown<Child, Here>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child::Event: InjectEvent<ShutdownRequested, Here>,
{
    fn shutdown(
        &mut self,
        _id: ShutdownId,
        installed: InstalledActor<Child>,
        ingress: behavior::Ingress<ShutdownRequested, Here>,
    ) -> Result<(), ShutdownRejection> {
        installed.request_shutdown(ingress)
    }
}

impl<C, N, P, Bindings, Origins, Child, Path>
    InterpretItem<ShutdownEstablished<Child, Here>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<EstablishedShutdownResolved<Child::Protocol>, Path> + Send + 'static,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child::Event: InjectEvent<ShutdownRequested, Here> + Send,
    BehaviorMessage<Child>: Send,
    Self: Send,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn interpret_item(
        &mut self,
        request: ShutdownEstablished<Child, Here>,
    ) -> ItemSettlement<ShutdownEstablished<Child, Here>, ShutdownId, ShutdownRejection, Never>
    {
        let shutdown = request.id;
        let settlement = request.settle(self);
        let resolution = match &settlement {
            ItemSettlement::Accepted(_) => EstablishedShutdownResolved::accepted(shutdown),
            ItemSettlement::Rejected { reason, .. } => {
                EstablishedShutdownResolved::rejected(shutdown, *reason)
            }
            ItemSettlement::Blocked { prerequisite, .. } => match *prerequisite {},
            ItemSettlement::Corrupt { .. } => {
                unreachable!("exact shutdown settlement cannot create interpreter corruption")
            }
        };
        self.inject_control_event::<_, Path>(resolution);
        settlement
    }
}

impl<C, N, P, Bindings, Origins> RetireCapabilities
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: Send + 'static,
    BehaviorMessage<C>: Send,
    Bindings: RetireChildTasks + Send,
    BindingTerminal<Bindings>: Send,
    Self: Send,
{
    type Event = C::Event;
    type Descendants = Vec<BindingTerminal<Bindings>>;

    async fn next_local_event(&mut self) -> Self::Event {
        tokio::select! {
            biased;
            termination_event = self.observations.next() => termination_event,
            task = self.activation_tasks.next_event() => task,
        }
    }

    fn next_deadline(&mut self) -> Option<Instant> {
        self.timers.next_deadline()
    }

    fn pop_due(&mut self, now: Instant) -> Option<Self::Event> {
        self.timers.pop_due(now)
    }

    async fn retire(self) -> CapabilityRetirement<Self::Event, Self::Descendants> {
        let Self {
            child_bindings,
            activation_tasks,
            exact_observations,
            terminal_reports,
            ..
        } = self;
        let cancellations = {
            let mut observations = exact_observations
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            observations
                .drain()
                .map(|(_, cancel)| cancel)
                .collect::<Vec<_>>()
        };
        for cancellation in cancellations {
            match cancellation.send(()) {
                Ok(()) | Err(()) => {}
            }
        }
        let descendants = child_bindings.retire_child_tasks().await;
        terminal_reports.retire();
        drop(exact_observations);
        CapabilityRetirement {
            activation_tasks,
            descendants,
        }
    }
}

#[cfg(test)]
mod atomic_interpretation_contract {
    use core::convert::Infallible;
    use core::future::Future;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use behavior::{
        Actions, ActiveTurn, Behavior, BehaviorActed, EstablishedRecipient, EventIngress, Here,
        InjectEvent, InterpretItem, MessageProtocol, Never, NoBirths, Recipient, SourceAdmission,
        User, UserEvent,
    };
    use behavior_actors::atomic::{
        AssignWorker, Assignment, BeginActivation, CustomerDelivery, DiagnosticAction,
        ImmediateActivation, InitializeWorker, ProxyOperation, StableProxy,
    };
    use behavior_actors::{
        EstablishedObservation, InterpretEstablishedObservation, ObservationId, ScheduleAt,
        TimerElapsed, TimerGeneration, TimerId,
    };
    use communication::{Config, Received, mailbox_channel};
    use tokio::sync::oneshot;

    use super::{
        ApplicationCapabilities, ApplicationCapabilityInputs, EstablishedObservationInterpreter,
        NoChildBindings,
    };
    use crate::ActorSpace;
    use crate::actor;
    use crate::actor_interface::{ActorInterface, ExtractLocalEndpoint};
    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::child_bindings::ChildBindings;
    use crate::interpret::{ActionInterpreter, RetireCapabilities};
    use crate::local::CommitActions;
    use crate::observation::TerminationObservations;
    use crate::reports::LocalTerminalReports;
    use crate::time::LocalTimers;
    use crate::topology::HostedActorSpaces;

    struct Worker;

    #[actor(message = Never)]
    impl Worker {}

    type Proxy = StableProxy<Worker, ImmediateActivation>;

    struct ProxyParent;

    #[actor(
        message = Never,
        births = { service: Proxy },
        creation_settlements = retain_for_retirement,
    )]
    impl ProxyParent {}

    struct SourceActor;

    struct SourceOwner;

    type ObservationProtocol = MessageProtocol<MailAddr, Never>;

    enum SourceControlEvent {
        SourceInput(u64),
        Observation(EstablishedObservation<ObservationProtocol>),
        Timer(TimerElapsed),
    }

    impl UserEvent for SourceControlEvent {
        type Addr = MailAddr;
        type Message = Never;

        fn user(_: Self::Addr, message: Self::Message) -> Self {
            match message {}
        }

        fn into_user(self) -> Result<User<Self::Addr, Self::Message>, Self> {
            Err(self)
        }
    }

    impl EventIngress<SourceOwner, u64> for SourceControlEvent {
        fn ingress(input: u64) -> Self {
            Self::SourceInput(input)
        }
    }

    impl InjectEvent<EstablishedObservation<ObservationProtocol>, Here> for SourceControlEvent {
        fn inject_at(report: EstablishedObservation<ObservationProtocol>) -> Self {
            Self::Observation(report)
        }
    }

    impl InjectEvent<TimerElapsed, Here> for SourceControlEvent {
        fn inject_at(elapsed: TimerElapsed) -> Self {
            Self::Timer(elapsed)
        }
    }

    impl Behavior for SourceActor {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = SourceControlEvent;
        type Sends = Vec<Never>;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;

        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            match event {
                SourceControlEvent::SourceInput(_)
                | SourceControlEvent::Observation(_)
                | SourceControlEvent::Timer(_) => Ok(Actions::cont()),
            }
        }
    }

    #[tokio::test]
    async fn source_admission_places_the_exact_input_on_the_actor_control_lane() {
        let (control, mailbox_owner, mailbox_ref, mut receiver) =
            mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(1));
        let (terminal_sender, terminal_receiver) = oneshot::channel();
        let inputs = ApplicationCapabilityInputs::<SourceActor, ()> {
            address: MailAddr::APPLICATION_ROOT,
            actor_spaces: Arc::new(()),
            allocations: ApplicationAddresses::new(),
            control,
            timers: LocalTimers::new(),
            observations: TerminationObservations::new(),
            terminal_reports: LocalTerminalReports::new(terminal_sender),
        };
        let mut capabilities = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
            inputs,
            NoChildBindings::default(),
        );
        let admitted = SourceAdmission::<SourceControlEvent, SourceOwner, u64>::admit_source(
            &mut capabilities,
            47,
        )
        .await;
        assert_eq!(admitted, Ok(()));

        let mut next_event = pin!(receiver.recv());
        let mut context = Context::from_waker(Waker::noop());
        let admitted_control = next_event.as_mut().poll(&mut context);
        assert!(matches!(
            admitted_control,
            Poll::Ready(Some(Received::Control(SourceControlEvent::SourceInput(47))))
        ));
        drop(mailbox_owner);
        drop(mailbox_ref);
        drop(terminal_receiver);
    }

    #[tokio::test]
    async fn exact_observation_start_and_cancel_publish_distinct_control_events() {
        let (control, mailbox_owner, mailbox_ref, mut receiver) =
            mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(2));
        let (terminal_sender, terminal_receiver) = oneshot::channel();
        let inputs = ApplicationCapabilityInputs::<SourceActor, ()> {
            address: MailAddr::APPLICATION_ROOT,
            actor_spaces: Arc::new(()),
            allocations: ApplicationAddresses::new(),
            control,
            timers: LocalTimers::new(),
            observations: TerminationObservations::new(),
            terminal_reports: LocalTerminalReports::new(terminal_sender),
        };
        let mut capabilities = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
            inputs,
            NoChildBindings::default(),
        );
        let interface = ActorInterface::new((), ApplicationAddresses::new());
        let external = interface
            .external::<ObservationProtocol>()
            .expect("the observed external actor is established");
        let endpoint = external.recipient().interpret(&mut ExtractLocalEndpoint);
        let mut interpreter = EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities);
        let observation = ObservationId(41);

        interpreter.observe(observation, endpoint);
        {
            let mut started_event = pin!(receiver.recv());
            let mut context = Context::from_waker(Waker::noop());
            let started = started_event.as_mut().poll(&mut context);
            assert!(matches!(
                started,
                Poll::Ready(Some(Received::Control(SourceControlEvent::Observation(
                    EstablishedObservation::Started { id, .. }
                )))) if id == observation
            ));
        }

        interpreter.cancel(observation);
        {
            let mut cancelled_event = pin!(receiver.recv());
            let mut context = Context::from_waker(Waker::noop());
            let cancelled = cancelled_event.as_mut().poll(&mut context);
            assert!(matches!(
                cancelled,
                Poll::Ready(Some(Received::Control(SourceControlEvent::Observation(
                    EstablishedObservation::Cancelled { id, .. }
                )))) if id == observation
            ));
        }
        drop(external);
        drop(mailbox_owner);
        drop(mailbox_ref);
        drop(terminal_receiver);
    }

    #[test]
    fn application_timer_deadlines_and_elapsed_events_cross_both_ports() {
        let (control, mailbox_owner, mailbox_ref, receiver) =
            mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(1));
        let (terminal_sender, terminal_receiver) = oneshot::channel();
        let inputs = ApplicationCapabilityInputs::<SourceActor, ()> {
            address: MailAddr::APPLICATION_ROOT,
            actor_spaces: Arc::new(()),
            allocations: ApplicationAddresses::new(),
            control,
            timers: LocalTimers::new(),
            observations: TerminationObservations::new(),
            terminal_reports: LocalTerminalReports::new(terminal_sender),
        };
        let mut capabilities = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
            inputs,
            NoChildBindings::default(),
        );
        let first_deadline = Instant::now() + Duration::from_secs(1);
        let first_id = TimerId(7);
        let first_generation = TimerGeneration(2);
        capabilities
            .timers
            .schedule_at::<Here>(ScheduleAt::new(first_id, first_generation, first_deadline))
            .expect("the first timer is scheduled");
        assert_eq!(
            RetireCapabilities::next_deadline(&mut capabilities),
            Some(first_deadline)
        );
        let first_elapsed = RetireCapabilities::pop_due(&mut capabilities, first_deadline);
        assert!(matches!(
            first_elapsed,
            Some(SourceControlEvent::Timer(TimerElapsed { id, generation }))
                if id == first_id && generation == first_generation
        ));

        let second_deadline = first_deadline + Duration::from_secs(1);
        let second_id = TimerId(8);
        let second_generation = TimerGeneration(3);
        capabilities
            .timers
            .schedule_at::<Here>(ScheduleAt::new(
                second_id,
                second_generation,
                second_deadline,
            ))
            .expect("the second timer is scheduled");
        let mut interpreter = ActionInterpreter::new(capabilities);
        assert_eq!(
            <ActionInterpreter<_> as CommitActions<SourceActor>>::next_deadline(&mut interpreter),
            Some(second_deadline)
        );
        let second_elapsed = <ActionInterpreter<_> as CommitActions<SourceActor>>::pop_due(
            &mut interpreter,
            second_deadline,
        );
        assert!(matches!(
            second_elapsed,
            Some(SourceControlEvent::Timer(TimerElapsed { id, generation }))
                if id == second_id && generation == second_generation
        ));
        drop(mailbox_owner);
        drop(mailbox_ref);
        drop(receiver);
        drop(terminal_receiver);
    }

    #[test]
    fn local_application_interprets_exact_worker_assignment() {
        type WorkerProtocol = MessageProtocol<crate::MailAddr, Assignment<u64>>;

        fn require<Capabilities>()
        where
            Capabilities:
                InterpretItem<AssignWorker<WorkerProtocol, u64>, <Worker as Behavior>::Event, Here>,
        {
        }

        require::<ApplicationCapabilities<Worker, (), super::NoParent, NoChildBindings>>();
    }

    #[test]
    fn local_application_interprets_complete_customer_delivery() {
        type CustomerProtocol = MessageProtocol<crate::MailAddr, u64>;
        type Spaces = HostedActorSpaces<ActorSpace<CustomerProtocol>>;

        fn require<Capabilities>()
        where
            Capabilities: InterpretItem<CustomerDelivery<CustomerProtocol>, <Worker as Behavior>::Event, Here>,
        {
        }

        require::<ApplicationCapabilities<Worker, Spaces, super::NoParent, NoChildBindings>>();
    }

    #[test]
    fn local_application_interprets_routed_and_terminal_diagnostics() {
        type DiagnosticProtocol = MessageProtocol<crate::MailAddr, u64>;
        type Spaces = HostedActorSpaces<ActorSpace<DiagnosticProtocol>>;

        fn require<Capabilities>()
        where
            Capabilities: InterpretItem<
                    DiagnosticAction<Recipient<DiagnosticProtocol>, u64>,
                    <Worker as Behavior>::Event,
                    Here,
                > + InterpretItem<
                    DiagnosticAction<EstablishedRecipient<DiagnosticProtocol>, u64>,
                    <Worker as Behavior>::Event,
                    Here,
                > + InterpretItem<DiagnosticAction<Infallible, u64>, <Worker as Behavior>::Event, Here>,
        {
        }

        require::<ApplicationCapabilities<Worker, Spaces, super::NoParent, NoChildBindings>>();
    }

    #[test]
    fn local_application_interprets_exact_worker_initialization() {
        type Proxy = StableProxy<Worker, ImmediateActivation>;

        fn require<Capabilities>()
        where
            Capabilities: InterpretItem<
                    InitializeWorker<Worker, ImmediateActivation>,
                    <Proxy as Behavior>::Event,
                    Here,
                >,
        {
        }

        require::<ApplicationCapabilities<Proxy, (), super::NoParent, NoChildBindings>>();
    }

    #[test]
    fn local_application_interprets_exact_worker_activation() {
        fn require<Capabilities>()
        where
            Capabilities: InterpretItem<
                    BeginActivation<Worker, ImmediateActivation>,
                    <Proxy as Behavior>::Event,
                    Here,
                >,
        {
        }

        require::<ApplicationCapabilities<Proxy, (), super::NoParent, NoChildBindings>>();
    }

    #[test]
    fn local_application_interprets_exact_proxy_operation() {
        fn require<Capabilities>()
        where
            Capabilities: InterpretItem<
                    ProxyOperation<Here, Worker, ImmediateActivation>,
                    <ProxyParent as Behavior>::Event,
                    Here,
                >,
        {
        }

        require::<
            ApplicationCapabilities<
                ProxyParent,
                (),
                super::NoParent,
                ChildBindings<ProxyParent, Never>,
            >,
        >();
    }
}
