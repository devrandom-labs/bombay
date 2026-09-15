//! Static application assembly and local capability interpretation.

use core::fmt;
use core::future::Future;
use core::marker::PhantomData;
use std::collections::{HashMap, HashSet};
use std::io;
#[cfg(feature = "axum")]
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use behavior::{
    Actions, Behavior, BehaviorBase, BehaviorMessage, BirthMode, BirthNodeAppend, BirthNodeMapper,
    Births, CancelObservation, ChildCons, ChildDelivery, ChildHead, ChildInput, ChildInputIngress,
    ChildProduct, ChildShutdownRejected, ChildShutdownRejection, ChildTail, Children,
    ChildrenError, CreationRejection, CreationResolved, Delivery, EstablishedCreation,
    EstablishedObservation, EstablishedShutdownResolved, EventIngress, FoldBirthNode,
    FoldedBirthNode, Here, InjectEvent, InterpretChildDelivery, InterpretChildInput,
    InterpretDelivery, InterpretEstablishedDelivery, InterpretEstablishedObservation,
    InterpretEstablishedShutdown, InterpretRequest, Never, NoChildren, ObservationId,
    ObservationOperation, ObservationRejection, ObserveChild, ObserveCreation, ObserveEstablished,
    ObserveEstablishedCreation, ObservePeer, Protocol, ReportShutdownPlan,
    ReportSupervisionFailure, ReportTerminalOutcome, ReportToParent, ResolveChildOccurrence,
    ResolvedChild, ResolvedChildPosition, ScheduleAfter, ScheduleAt, SendInterpreter,
    ShutdownChild, ShutdownEstablished, ShutdownRejection, ShutdownRequested,
};
use bombay_address::ClaimError;
use communication::{ControlClosed, ControlSender};
use tokio::sync::oneshot;

use crate::actor_interface::ActorInterface;
use crate::address::{ApplicationAddresses, MailAddr};
use crate::application::Application;
use crate::child_bindings::{
    ChildBindingAt, HostChildAt, NestedBindings, NestedChildBindings, NoChildBindings,
    OccurrenceBindings, RetireChildTasks, RuntimeChildBindings, RuntimeChildSpaces,
};
use crate::entity::{
    EntityAdmission, EntityApplicationFamilies, EntityDefinition, EntityFamilyAt,
    InstallEntityFamilies, InstalledEntityFamilies, NativeEntityHost, Passivation,
};
use crate::interpret::{
    ActionInterpreter, BirthInstaller, CreationResults, CreationTransaction,
    EffectInterpretationError, RetireCapabilities, SpawnChild,
};
use crate::launch::{
    ActorSpace, OwnedTask, ProjectedTask, SpawnError, spawn_owned_with, spawn_root_with,
};
use crate::local::{ActivationTasks, ActorRef, CapabilityRetirement, CommitActions, Termination};
use crate::observation::{FactQueue, LocalPeerObservations};
use crate::reports::{
    LocalParentReports, LocalTerminalReports, ParentReporting, TerminalReportTransaction,
};
use crate::terminal::{ActorOrigin, ActorRetirement, LocalOutcome, ProjectTerminal};
use crate::termination::TerminalReportDisposition;
use crate::time::LocalTimers;
use crate::topology::{HostedActorSpaces, Hosts, ResolveLogical};

const DEFAULT_USER_CAPACITY: usize = 1_024;

type RootBirthNode<Root> = <<Root as Behavior>::Birth as BirthMode>::Child;
type ApplicationProduct<Members> = <Members as StageApplicationChildren>::Product;
type ApplicationBirthNode<Members> =
    <ApplicationProduct<Members> as ChildProduct<MailAddr>>::Choice;
type RunningBirthNode<Root, Members> =
    <RootBirthNode<Root> as BirthNodeAppend<ApplicationBirthNode<Members>>>::Output;
type BindingTerminal<Bindings> = <Bindings as RetireChildTasks>::Root;
type RootOriginProduct<Root> = FoldedBirthNode<RootBirthNode<Root>, RootTerminalOriginMapper>;

type RootCapabilities<Actor, Spaces, Terminal, Origins> = ApplicationCapabilities<
    Actor,
    HostedActorSpaces<Spaces>,
    NoParent,
    OccurrenceBindings<Actor, Terminal>,
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
    pub(crate) facts: FactQueue<MailAddr, C::Event>,
    pub(crate) terminal_reports: LocalTerminalReports,
}

/// Failure at the complete local application boundary.
pub enum RunError<RootError = Never> {
    /// Tokio could not construct the application executor.
    Runtime(io::Error),
    /// The root address source rejected allocation.
    AllocationRejected(behavior::AllocationRejection),
    /// The root's controlled initialization fold failed.
    InitializationRejected(RootError),
    /// The runtime could not commit the root's address claim.
    HostRejected(ClaimError<MailAddr>),
    /// The runtime could not interpret the root's initialization effects.
    EffectsRejected(EffectInterpretationError),
    /// The root task panicked before activation.
    Panicked,
    /// The executor cancelled the root task before activation.
    Cancelled,
    /// The root stopped before publishing its live reference.
    Ended(bombay_engine::Completion),
    /// The private running application was initialized more than once.
    InitializedTwice,
    /// No collision-free creator-local nonce remained for an application actor.
    NonceExhausted,
    /// Application actor staging produced a duplicate creator-local nonce.
    DuplicateNonce(u64),
}

impl<RootError> fmt::Debug for RunError<RootError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Runtime(_) => "Runtime",
            Self::AllocationRejected(_) => "AllocationRejected",
            Self::InitializationRejected(_) => "InitializationRejected",
            Self::HostRejected(_) => "HostRejected",
            Self::EffectsRejected(_) => "EffectsRejected",
            Self::Panicked => "Panicked",
            Self::Cancelled => "Cancelled",
            Self::Ended(_) => "Ended",
            Self::InitializedTwice => "InitializedTwice",
            Self::NonceExhausted => "NonceExhausted",
            Self::DuplicateNonce(_) => "DuplicateNonce",
        })
    }
}

impl<RootError> fmt::Display for RunError<RootError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Runtime(_) => "Bombay could not construct its Tokio runtime",
            Self::AllocationRejected(_) => "the root address source rejected allocation",
            Self::InitializationRejected(_) => "the root rejected initialization",
            Self::HostRejected(_) => "the actor host rejected root initialization",
            Self::EffectsRejected(_) => "the actor host rejected root initialization effects",
            Self::Panicked => "root initialization panicked",
            Self::Cancelled => "root initialization was cancelled",
            Self::Ended(_) => "the root ended before publishing activation",
            Self::InitializedTwice => "the running application was initialized more than once",
            Self::NonceExhausted => "application actor nonce allocation was exhausted",
            Self::DuplicateNonce(_) => "application actor staging produced a duplicate nonce",
        })
    }
}

impl<RootError> std::error::Error for RunError<RootError> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::EffectsRejected(error) => Some(error),
            _ => None,
        }
    }
}

/// A live application's typed root and capability projections.
pub struct ApplicationHandle<P: Protocol, Families = ()> {
    root: ActorRef<P>,
    allocations: ApplicationAddresses,
    families: Families,
}

impl<P: Protocol, Families: Clone> Clone for ApplicationHandle<P, Families> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            allocations: self.allocations.clone(),
            families: self.families.clone(),
        }
    }
}

impl<P: Protocol, Families> ApplicationHandle<P, Families> {
    fn new(root: ActorRef<P>, allocations: ApplicationAddresses, families: Families) -> Self {
        Self {
            root,
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
    pub fn lifecycle(&self) -> ApplicationLifecycle<P> {
        ApplicationLifecycle {
            root: self.root.clone(),
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
pub struct ApplicationLifecycle<P: Protocol> {
    root: ActorRef<P>,
}

impl<P: Protocol> Clone for ApplicationLifecycle<P> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
        }
    }
}

impl<P: Protocol> ApplicationLifecycle<P> {
    /// Request shutdown through the root lifecycle lane.
    ///
    /// # Errors
    ///
    /// Returns the exact rejection when the root is already stopping or has
    /// already stopped.
    pub fn request_shutdown(&self) -> Result<(), ShutdownRejection> {
        self.root.request_shutdown()
    }

    /// Observe termination of the exact root incarnation.
    pub fn termination(&self) -> impl Future<Output = Termination<P::Addr>> + use<P> {
        self.root.termination()
    }
}

/// Failure at the opt-in Axum HTTP boundary.
#[cfg(feature = "axum")]
#[derive(Debug, thiserror::Error)]
pub enum AxumRunError<RootTerminal, RootError = Never> {
    #[error(transparent)]
    Application(#[from] RunError<RootError>),
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

trait RootProjection<Actor, Terminal, CommitError>
where
    Actor: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    type Error;

    fn project(
        address: MailAddr,
        outcome: LocalOutcome<Actor, Vec<Terminal>, CommitError>,
    ) -> Terminal;

    fn startup_error(error: SpawnError<Actor, CommitError, Vec<Terminal>>)
    -> RunError<Self::Error>;
}

impl<Actor, Terminal, CommitError> RootProjection<Actor, Terminal, CommitError> for DirectRoot
where
    Actor: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    CommitError: Into<EffectInterpretationError>,
    Terminal:
        ProjectTerminal<ActorOrigin<Actor, Here>, ActorRetirement<Actor, Terminal, CommitError>>,
{
    type Error = Actor::Error;

    fn project(
        address: MailAddr,
        outcome: LocalOutcome<Actor, Vec<Terminal>, CommitError>,
    ) -> Terminal {
        Terminal::project(
            ActorOrigin::<Actor, Here>::root(address),
            ActorRetirement::from_local(outcome),
        )
    }

    fn startup_error(
        error: SpawnError<Actor, CommitError, Vec<Terminal>>,
    ) -> RunError<Self::Error> {
        match error {
            SpawnError::AllocationRejected { reason, .. } => RunError::AllocationRejected(reason),
            SpawnError::InitializationRejected { error, .. } => {
                RunError::InitializationRejected(error)
            }
            SpawnError::HostRejected { error, .. } => RunError::HostRejected(error),
            SpawnError::EffectsRejected { error, .. } => RunError::EffectsRejected(error.into()),
            SpawnError::Panicked => RunError::Panicked,
            SpawnError::Cancelled => RunError::Cancelled,
            SpawnError::Ended(completion) => RunError::Ended(completion),
        }
    }
}

trait LaunchSystem<Actor, Terminal, Origins, Projection>
where
    Actor: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    type RootError;

    fn launch(
        self,
        root: Actor,
    ) -> impl Future<Output = Result<Terminal, RunError<Self::RootError>>> + Send;

    fn launch_with<Families, Boundary, BoundaryFuture, Output>(
        self,
        root: Actor,
        allocations: ApplicationAddresses,
        families: Families,
        boundary: Boundary,
    ) -> impl Future<Output = Result<(Output, Terminal), RunError<Self::RootError>>> + Send
    where
        Families: Clone + Send + 'static,
        Boundary: FnOnce(ApplicationHandle<Actor::Protocol, Families>) -> BoundaryFuture + Send,
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
        Router: FnOnce(ApplicationHandle<Actor::Protocol>) -> axum::Router + Send;
}

impl<Actor, Spaces, Terminal, Origins, Projection>
    LaunchSystem<Actor, Terminal, Origins, Projection> for Spaces
where
    Actor: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Actor::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    BehaviorMessage<Actor>: Send + 'static,
    Actor::Sends: Send + 'static,
    Actor::Error: Send + 'static,
    <Actor::Birth as BirthMode>::Child: FoldBirthNode<RuntimeChildBindings<Terminal>>
        + FoldBirthNode<RuntimeChildSpaces>
        + Send
        + 'static,
    Spaces: Hosts<Actor::Protocol> + Send + Sync + 'static,
    OccurrenceBindings<Actor, Terminal>:
        Default + RetireChildTasks<Root = Terminal> + Send + 'static,
    RootInterpreter<Actor, Spaces, Terminal, Origins>:
        CommitActions<Actor, Retired = Vec<Terminal>> + Send + 'static,
    <RootInterpreter<Actor, Spaces, Terminal, Origins> as CommitActions<Actor>>::Error:
        Send + 'static,
    Terminal: Send + 'static,
    Projection: RootProjection<
            Actor,
            Terminal,
            <RootInterpreter<Actor, Spaces, Terminal, Origins> as CommitActions<Actor>>::Error,
        >,
{
    type RootError = <Projection as RootProjection<
        Actor,
        Terminal,
        <RootInterpreter<Actor, Spaces, Terminal, Origins> as CommitActions<Actor>>::Error,
    >>::Error;

    async fn launch(self, root: Actor) -> Result<Terminal, RunError<Self::RootError>> {
        <Spaces as LaunchSystem<Actor, Terminal, Origins, Projection>>::launch_with(
            self,
            root,
            ApplicationAddresses::new(),
            (),
            |_: ApplicationHandle<Actor::Protocol>| async {},
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
    ) -> Result<(Output, Terminal), RunError<Self::RootError>>
    where
        Families: Clone + Send + 'static,
        Boundary: FnOnce(ApplicationHandle<Actor::Protocol, Families>) -> BoundaryFuture + Send,
        BoundaryFuture: Future<Output = Output> + Send,
        Output: Send,
    {
        let roots = <Spaces as Hosts<Actor::Protocol>>::space(&self).clone();
        let actor_spaces = Arc::new(HostedActorSpaces(self));
        let interface_allocations = allocations.clone();
        let address = MailAddr::APPLICATION_ROOT;
        let root = spawn_root_with(
            roots,
            communication::Config::new(DEFAULT_USER_CAPACITY),
            address,
            root,
            {
                let actor_spaces = actor_spaces.clone();
                move |control, terminal_reports, timers, facts| {
                    ActionInterpreter::new(ApplicationCapabilities::<
                        Actor,
                        HostedActorSpaces<Spaces>,
                        NoParent,
                        OccurrenceBindings<Actor, Terminal>,
                        Origins,
                    >::new_with_bindings(
                        ApplicationCapabilityInputs {
                            address,
                            actor_spaces,
                            allocations,
                            control,
                            timers,
                            facts,
                            terminal_reports,
                        },
                        OccurrenceBindings::<Actor, Terminal>::default(),
                    ))
                }
            },
        )
        .await
        .map_err(Projection::startup_error)?;

        let application = ApplicationHandle::new(root.actor, interface_allocations, families);
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
        Router: FnOnce(ApplicationHandle<Actor::Protocol>) -> axum::Router + Send,
    {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .map_err(|source| AxumRunError::Bind { address, source })?;
        let roots = <Spaces as Hosts<Actor::Protocol>>::space(&self).clone();
        let actor_spaces = Arc::new(HostedActorSpaces(self));
        let allocations = ApplicationAddresses::new();
        let interface_allocations = allocations.clone();
        let root_address = MailAddr::APPLICATION_ROOT;
        let root = spawn_root_with(
            roots,
            communication::Config::new(DEFAULT_USER_CAPACITY),
            root_address,
            root,
            {
                let actor_spaces = actor_spaces.clone();
                move |control, terminal_reports, timers, facts| {
                    ActionInterpreter::new(ApplicationCapabilities::<
                        Actor,
                        HostedActorSpaces<Spaces>,
                        NoParent,
                        OccurrenceBindings<Actor, Terminal>,
                        Origins,
                    >::new_with_bindings(
                        ApplicationCapabilityInputs {
                            address: root_address,
                            actor_spaces,
                            allocations,
                            control,
                            timers,
                            facts,
                            terminal_reports,
                        },
                        OccurrenceBindings::<Actor, Terminal>::default(),
                    ))
                }
            },
        )
        .await
        .map_err(Projection::startup_error)?;

        let application = ApplicationHandle::new(root.actor, interface_allocations, ());
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
    pub fn run<Terminal>(self) -> Result<Terminal, RunError<Root::Error>>
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
    ) -> Result<(Output, Terminal), RunError<Root::Error>>
    where
        Spaces: LaunchSystem<
                Root,
                Terminal,
                StructuralOrigins<Root::Base>,
                DirectRoot,
                RootError = Root::Error,
            >,
        Boundary: FnOnce(ApplicationHandle<Root::Protocol>) -> BoundaryFuture + Send,
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
        router: impl FnOnce(ApplicationHandle<Root::Protocol>) -> axum::Router + Send,
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
        RunError<Root::Error>,
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
        reserved: &mut HashSet<u64>,
        next: &mut u64,
    ) -> Result<Children<MailAddr, Self::Product>, ApplicationStagingError>;
}

impl StageApplicationChildren for () {
    type Product = NoChildren;

    fn stage(
        self,
        _: &mut HashSet<u64>,
        _: &mut u64,
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
        reserved: &mut HashSet<u64>,
        next: &mut u64,
    ) -> Result<Children<MailAddr, Self::Product>, ApplicationStagingError> {
        let (role, actor, tail) = self;
        let children = tail.stage(reserved, next)?;
        let nonce =
            next_child_nonce(reserved, next).ok_or(ApplicationStagingError::NonceExhausted)?;
        drop(role);
        Ok(children.child(nonce, actor))
    }
}

fn next_child_nonce(reserved: &mut HashSet<u64>, next: &mut u64) -> Option<u64> {
    let first = *next;
    loop {
        let candidate = *next;
        *next = next.wrapping_add(1);
        if reserved.insert(candidate) {
            return Some(candidate);
        }
        if *next == first {
            return None;
        }
    }
}

enum ApplicationDefinitionError<RootError> {
    Root(RootError),
    InitializedTwice,
    NonceExhausted,
    DuplicateNonce(u64),
}

enum ApplicationStagingError {
    NonceExhausted,
}

struct RunningApplication<Root, Members> {
    root: Root,
    members: Option<Members>,
}

impl<Root, Members> RunningApplication<Root, Members> {
    const fn new(root: Root, members: Members) -> Self {
        Self {
            root,
            members: Some(members),
        }
    }
}

impl<Root, Members> Behavior for RunningApplication<Root, Members>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    Members: StageApplicationChildren,
    RootBirthNode<Root>: BirthNodeAppend<ApplicationBirthNode<Members>>,
{
    type Protocol = Root::Protocol;
    type Event = Root::Event;
    type Sends = Root::Sends;
    type Ph = Never;
    type Error = ApplicationDefinitionError<Root::Error>;
    type Birth = Births<RunningBirthNode<Root, Members>>;

    fn init(&mut self, _: behavior::InitializationTurn) -> behavior::BehaviorActed<Self> {
        let actions =
            behavior::initialize(&mut self.root).map_err(ApplicationDefinitionError::Root)?;
        let members = self
            .members
            .take()
            .ok_or(ApplicationDefinitionError::InitializedTwice)?;
        let mut reserved = actions
            .creates
            .iter()
            .map(|creation| creation.nonce)
            .collect::<HashSet<_>>();
        let mut next = 0;
        let application = members
            .stage(&mut reserved, &mut next)
            .map_err(|ApplicationStagingError::NonceExhausted| {
                ApplicationDefinitionError::NonceExhausted
            })?
            .into_creates()
            .map_err(|error| match error {
                ChildrenError::DuplicateNonce { nonce } => {
                    ApplicationDefinitionError::DuplicateNonce(nonce)
                }
            })?;
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
            creates: RootBirthNode::<Root>::append_creations(actions.creates, Vec::new()),
            become_: actions.become_,
        })
    }
}

impl<Root, Members> BehaviorBase for RunningApplication<Root, Members>
where
    Root: BehaviorBase,
{
    type Base = Root::Base;

    fn base(&self) -> &Self::Base {
        self.root.base()
    }
}

impl<Root, Members, Terminal, CommitError>
    RootProjection<RunningApplication<Root, Members>, Terminal, CommitError> for DeclaredRoot
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    Members: StageApplicationChildren,
    RootBirthNode<Root>: BirthNodeAppend<ApplicationBirthNode<Members>>,
    CommitError: Into<EffectInterpretationError>,
    Terminal:
        ProjectTerminal<ActorOrigin<Root, Here>, ActorRetirement<Root, Terminal, CommitError>>,
{
    type Error = Root::Error;

    fn project(
        address: MailAddr,
        outcome: LocalOutcome<RunningApplication<Root, Members>, Vec<Terminal>, CommitError>,
    ) -> Terminal {
        let retirement = ActorRetirement::from_local(outcome);
        let retirement = match retirement {
            ActorRetirement::Completed {
                behavior,
                control,
                user,
                descendants,
                completion,
            } => ActorRetirement::Completed {
                behavior: behavior.root,
                control,
                user,
                descendants,
                completion,
            },
            ActorRetirement::BehaviorFailed {
                behavior,
                control,
                user,
                descendants,
                error: ApplicationDefinitionError::Root(error),
            } => ActorRetirement::BehaviorFailed {
                behavior: behavior.root,
                control,
                user,
                descendants,
                error,
            },
            ActorRetirement::EffectsFailed {
                behavior,
                error,
                control,
                user,
                descendants,
            } => ActorRetirement::EffectsFailed {
                behavior: behavior.root,
                error,
                control,
                user,
                descendants,
            },
            ActorRetirement::OwnerCancelled {
                behavior,
                control,
                user,
                descendants,
            } => ActorRetirement::OwnerCancelled {
                behavior: behavior.root,
                control,
                user,
                descendants,
            },
            ActorRetirement::Panicked => ActorRetirement::Panicked,
            ActorRetirement::Cancelled => ActorRetirement::Cancelled,
            ActorRetirement::AllocationRejected { .. }
            | ActorRetirement::InitializationRejected { .. }
            | ActorRetirement::HostRejected { .. }
            | ActorRetirement::InitializationEffectsFailed { .. }
            | ActorRetirement::EndedBeforeActivation { .. }
            | ActorRetirement::BehaviorFailed { .. } => {
                unreachable!("a published application root cannot return a startup disposition")
            }
        };
        Terminal::project(ActorOrigin::<Root, Here>::root(address), retirement)
    }

    fn startup_error(
        error: SpawnError<RunningApplication<Root, Members>, CommitError, Vec<Terminal>>,
    ) -> RunError<Self::Error> {
        match error {
            SpawnError::AllocationRejected { reason, .. } => RunError::AllocationRejected(reason),
            SpawnError::InitializationRejected { error, .. } => match error {
                ApplicationDefinitionError::Root(error) => RunError::InitializationRejected(error),
                ApplicationDefinitionError::InitializedTwice => RunError::InitializedTwice,
                ApplicationDefinitionError::NonceExhausted => RunError::NonceExhausted,
                ApplicationDefinitionError::DuplicateNonce(nonce) => {
                    RunError::DuplicateNonce(nonce)
                }
            },
            SpawnError::HostRejected { error, .. } => RunError::HostRejected(error),
            SpawnError::EffectsRejected { error, .. } => RunError::EffectsRejected(error.into()),
            SpawnError::Panicked => RunError::Panicked,
            SpawnError::Cancelled => RunError::Cancelled,
            SpawnError::Ended(completion) => RunError::Ended(completion),
        }
    }
}

#[allow(
    private_bounds,
    reason = "the running application and its launch proof remain private"
)]
impl<Root, Members> Application<Root, Members>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Members: StageApplicationChildren,
    RootBirthNode<Root>: BirthNodeAppend<ApplicationBirthNode<Members>>,
{
    /// Run the declared application to exact root termination.
    ///
    /// The terminal sum must cover the declared root's exact retirement:
    ///
    /// ```compile_fail,E0277
    /// use bombay::actors::ActorExt;
    /// use bombay::prelude::{Never, RunError};
    ///
    /// struct Root;
    ///
    /// #[bombay::actor(message = Never)]
    /// impl Root {}
    ///
    /// struct MissingRootTerminal;
    ///
    /// fn run() -> Result<MissingRootTerminal, RunError> {
    ///     bombay::Application::new(Root.stop_on_shutdown()).run()
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns the exact runtime-construction or application-startup failure.
    pub fn run<Terminal>(self) -> Result<Terminal, RunError<Root::Error>>
    where
        ActorSpace<Root::Protocol>: LaunchSystem<
                RunningApplication<Root, Members>,
                Terminal,
                ApplicationOrigins<Root, Members>,
                DeclaredRoot,
                RootError = Root::Error,
            >,
    {
        let (root, members) = self.into_parts();
        let running = RunningApplication::new(root, members);
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(RunError::Runtime)?
            .block_on(<ActorSpace<Root::Protocol> as LaunchSystem<
                RunningApplication<Root, Members>,
                Terminal,
                ApplicationOrigins<Root, Members>,
                DeclaredRoot,
            >>::launch(ActorSpace::new(), running))
    }

    /// Run the declared application with one live external boundary.
    ///
    /// # Errors
    ///
    /// Returns the exact runtime-construction or application-startup failure.
    pub fn run_with<Terminal, Boundary, BoundaryFuture, Output>(
        self,
        boundary: Boundary,
    ) -> Result<(Output, Terminal), RunError<Root::Error>>
    where
        ActorSpace<Root::Protocol>: LaunchSystem<
                RunningApplication<Root, Members>,
                Terminal,
                ApplicationOrigins<Root, Members>,
                DeclaredRoot,
                RootError = Root::Error,
            >,
        Boundary: FnOnce(ApplicationHandle<Root::Protocol>) -> BoundaryFuture + Send,
        BoundaryFuture: Future<Output = Output> + Send,
        Output: Send,
    {
        let (root, members) = self.into_parts();
        let running = RunningApplication::new(root, members);
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(RunError::Runtime)?
            .block_on(<ActorSpace<Root::Protocol> as LaunchSystem<
                RunningApplication<Root, Members>,
                Terminal,
                ApplicationOrigins<Root, Members>,
                DeclaredRoot,
            >>::launch_with(
                ActorSpace::new(),
                running,
                ApplicationAddresses::new(),
                (),
                boundary,
            ))
    }

    #[cfg(feature = "axum")]
    /// Run the declared application behind an Axum HTTP boundary.
    ///
    /// # Errors
    ///
    /// Returns the exact application startup, listener bind, or server error.
    pub fn run_axum<Terminal>(
        self,
        address: SocketAddr,
        router: impl FnOnce(ApplicationHandle<Root::Protocol>) -> axum::Router + Send,
    ) -> Result<Terminal, AxumRunError<Terminal, Root::Error>>
    where
        ActorSpace<Root::Protocol>: LaunchSystem<
                RunningApplication<Root, Members>,
                Terminal,
                ApplicationOrigins<Root, Members>,
                DeclaredRoot,
                RootError = Root::Error,
            >,
    {
        let (root, members) = self.into_parts();
        let running = RunningApplication::new(root, members);
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(RunError::Runtime)?
            .block_on(<ActorSpace<Root::Protocol> as LaunchSystem<
                RunningApplication<Root, Members>,
                Terminal,
                ApplicationOrigins<Root, Members>,
                DeclaredRoot,
            >>::launch_axum(
                ActorSpace::new(), running, address, router
            ))
    }
}

trait ProjectChildTerminal<Position, Child, Terminal, CommitError>
where
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>, CommitError>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal>;

    fn failure(
        error: SpawnError<Child, CommitError, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> Terminal;
}

impl<Owner, Position, Child, Terminal, CommitError>
    ProjectChildTerminal<Position, Child, Terminal, CommitError> for StructuralOrigins<Owner>
where
    Owner: 'static,
    Position: 'static,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Child::Event: Send + 'static,
    BehaviorMessage<Child>: Send + 'static,
    Child::Sends: Send + 'static,
    Child::Error: Send + 'static,
    <Child::Birth as BirthMode>::Child: Send + 'static,
    CommitError: Send + 'static,
    Terminal: ProjectTerminal<ActorOrigin<Owner, Position>, ActorRetirement<Child, Terminal, CommitError>>
        + Send
        + 'static,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>, CommitError>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal> {
        ProjectedTask::project(task, ActorOrigin::<Owner, Position>::child(address, nonce))
    }

    fn failure(
        error: SpawnError<Child, CommitError, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> Terminal {
        Terminal::project(
            ActorOrigin::<Owner, Position>::child(address, nonce),
            error.into_retirement(),
        )
    }
}

struct RootTerminalOriginMapper;
struct NoRootTerminalOrigins;
struct RootTerminalOrigin<Child, Tail>(PhantomData<fn() -> (Child, Tail)>);
struct AppendedOrigins<RootOrigins, Members>(PhantomData<fn() -> (RootOrigins, Members)>);

impl BirthNodeMapper for RootTerminalOriginMapper {
    type Empty = NoRootTerminalOrigins;
    type Mapped<Position, Child: Behavior, Tail> = RootTerminalOrigin<Child, Tail>;
}

trait ProjectAppendedChild<Target, Absolute, Root, Child, Terminal, CommitError>
where
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>, CommitError>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal>;

    fn failure(
        error: SpawnError<Child, CommitError, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> Terminal;
}

trait ProjectDeclaredChild<Target, Root, Child, Terminal, CommitError>
where
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>, CommitError>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal>;

    fn failure(
        error: SpawnError<Child, CommitError, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> Terminal;
}

impl<Members, Target, Absolute, Root, Child, Terminal, CommitError>
    ProjectAppendedChild<Target, Absolute, Root, Child, Terminal, CommitError>
    for AppendedOrigins<NoRootTerminalOrigins, Members>
where
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    Members: ProjectDeclaredChild<Target, Root, Child, Terminal, CommitError>,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>, CommitError>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal> {
        Members::task(task, address, nonce)
    }

    fn failure(
        error: SpawnError<Child, CommitError, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> Terminal {
        Members::failure(error, address, nonce)
    }
}

impl<RootChild, Rest, Members, Absolute, Root, Terminal, CommitError>
    ProjectAppendedChild<ChildHead, Absolute, Root, RootChild, Terminal, CommitError>
    for AppendedOrigins<RootTerminalOrigin<RootChild, Rest>, Members>
where
    Root: BehaviorBase,
    Root::Base: 'static,
    Absolute: 'static,
    RootChild: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    RootChild::Event: Send + 'static,
    BehaviorMessage<RootChild>: Send + 'static,
    RootChild::Sends: Send + 'static,
    RootChild::Error: Send + 'static,
    <RootChild::Birth as BirthMode>::Child: Send + 'static,
    CommitError: Send + 'static,
    Terminal: ProjectTerminal<
            ActorOrigin<Root::Base, Absolute>,
            ActorRetirement<RootChild, Terminal, CommitError>,
        > + Send
        + 'static,
{
    fn task(
        task: OwnedTask<RootChild, Vec<Terminal>, CommitError>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<RootChild, Terminal> {
        ProjectedTask::project(
            task,
            ActorOrigin::<Root::Base, Absolute>::child(address, nonce),
        )
    }

    fn failure(
        error: SpawnError<RootChild, CommitError, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> Terminal {
        Terminal::project(
            ActorOrigin::<Root::Base, Absolute>::child(address, nonce),
            error.into_retirement(),
        )
    }
}

impl<RootChild, Rest, Members, Relative, Absolute, Root, Child, Terminal, CommitError>
    ProjectAppendedChild<ChildTail<Relative>, Absolute, Root, Child, Terminal, CommitError>
    for AppendedOrigins<RootTerminalOrigin<RootChild, Rest>, Members>
where
    RootChild: Behavior,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    AppendedOrigins<Rest, Members>:
        ProjectAppendedChild<Relative, Absolute, Root, Child, Terminal, CommitError>,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>, CommitError>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal> {
        <AppendedOrigins<Rest, Members> as ProjectAppendedChild<
            Relative,
            Absolute,
            Root,
            Child,
            Terminal,
            CommitError,
        >>::task(task, address, nonce)
    }

    fn failure(
        error: SpawnError<Child, CommitError, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> Terminal {
        <AppendedOrigins<Rest, Members> as ProjectAppendedChild<
            Relative,
            Absolute,
            Root,
            Child,
            Terminal,
            CommitError,
        >>::failure(error, address, nonce)
    }
}

impl<Role, Child, Tail, Root, Terminal, CommitError>
    ProjectDeclaredChild<ChildHead, Root, Child, Terminal, CommitError> for (Role, Child, Tail)
where
    Root: 'static,
    Role: 'static,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Child::Event: Send + 'static,
    BehaviorMessage<Child>: Send + 'static,
    Child::Sends: Send + 'static,
    Child::Error: Send + 'static,
    <Child::Birth as BirthMode>::Child: Send + 'static,
    CommitError: Send + 'static,
    Terminal: ProjectTerminal<ActorOrigin<Root, Role>, ActorRetirement<Child, Terminal, CommitError>>
        + Send
        + 'static,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>, CommitError>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal> {
        ProjectedTask::project(task, ActorOrigin::<Root, Role>::child(address, nonce))
    }

    fn failure(
        error: SpawnError<Child, CommitError, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> Terminal {
        Terminal::project(
            ActorOrigin::<Root, Role>::child(address, nonce),
            error.into_retirement(),
        )
    }
}

impl<Role, Declared, Tail, Relative, Root, Child, Terminal, CommitError>
    ProjectDeclaredChild<ChildTail<Relative>, Root, Child, Terminal, CommitError>
    for (Role, Declared, Tail)
where
    Declared: Behavior,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    Tail: ProjectDeclaredChild<Relative, Root, Child, Terminal, CommitError>,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>, CommitError>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal> {
        Tail::task(task, address, nonce)
    }

    fn failure(
        error: SpawnError<Child, CommitError, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> Terminal {
        Tail::failure(error, address, nonce)
    }
}

impl<Root, Members, Position, Child, Terminal, CommitError>
    ProjectChildTerminal<Position, Child, Terminal, CommitError>
    for ApplicationOrigins<Root, Members>
where
    Root: Behavior + BehaviorBase,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    RootBirthNode<Root>: FoldBirthNode<RootTerminalOriginMapper>,
    AppendedOrigins<RootOriginProduct<Root>, Members>:
        ProjectAppendedChild<Position, Position, Root, Child, Terminal, CommitError>,
{
    fn task(
        task: OwnedTask<Child, Vec<Terminal>, CommitError>,
        address: MailAddr,
        nonce: u64,
    ) -> ProjectedTask<Child, Terminal> {
        <AppendedOrigins<RootOriginProduct<Root>, Members> as ProjectAppendedChild<
            Position,
            Position,
            Root,
            Child,
            Terminal,
            CommitError,
        >>::task(task, address, nonce)
    }

    fn failure(
        error: SpawnError<Child, CommitError, Vec<Terminal>>,
        address: MailAddr,
        nonce: u64,
    ) -> Terminal {
        <AppendedOrigins<RootOriginProduct<Root>, Members> as ProjectAppendedChild<
            Position,
            Position,
            Root,
            Child,
            Terminal,
            CommitError,
        >>::failure(error, address, nonce)
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
    facts: FactQueue<MailAddr, C::Event>,
    peers: Option<LocalPeerObservations<C::Protocol, C::Event>>,
    creations: CreationResults<MailAddr>,
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
            facts: inputs.facts,
            peers: None,
            creations: CreationResults::new(),
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
            facts: self.facts,
            peers: self.peers,
            creations: self.creations,
            child_bindings: self.child_bindings,
            activation_tasks: self.activation_tasks,
            exact_observations: self.exact_observations,
            terminal_reports: self.terminal_reports,
            parent_reports,
            origins: PhantomData,
        }
    }

    fn return_fact<Fact, Path>(&self, fact: Fact)
    where
        C::Event: InjectEvent<Fact, Path>,
    {
        let event = C::Event::inject_at(fact);
        match self.control.send(event) {
            Ok(()) => {}
            Err(ControlClosed(_)) => {
                unreachable!("the actor control lane remains live while its effects commit")
            }
        }
    }

    fn current_creation(
        &self,
        nonce: u64,
    ) -> Result<CreationResolved<MailAddr>, EffectInterpretationError> {
        self.creations
            .resolve(&nonce)
            .ok_or(EffectInterpretationError::UnknownCreation(nonce))
    }
}

impl<C, N, P, Bindings, Origins> SendInterpreter
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Self: Send,
{
    type Error = EffectInterpretationError;
}

impl<C, N, P, Bindings, Origins> TerminalReportTransaction
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn begin_terminal_reports(&self) {
        self.terminal_reports.begin();
    }

    fn finish_terminal_reports(&self, disposition: TerminalReportDisposition) {
        self.terminal_reports.finish(disposition);
    }
}

impl<C, N, P, Bindings, Origins> BirthInstaller<MailAddr>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    type Error = EffectInterpretationError;
}

impl<C, N, P, Bindings, Origins> CreationTransaction
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn begin_creations(&mut self) {
        self.creations.begin();
    }
}

impl<C, N, P, Bindings, Origins, Position, Child>
    SpawnChild<MailAddr, Position, Child, EffectInterpretationError>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>> + BehaviorBase,
    C::Event: Send + 'static,
    BehaviorMessage<C>: Send,
    N: Send + Sync + 'static,
    P: Send,
    Position: 'static,
    Bindings: ChildBindingAt<Position, Child = Child, Root = BindingTerminal<Bindings>>
        + HostChildAt<Position, Child>
        + NestedChildBindings<Child>
        + RetireChildTasks
        + Send,
    Child:
        Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase + Send + 'static,
    Child::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
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
            LocalParentReports<C::Event, Child, Position>,
            NestedBindings<Bindings, Child>,
            StructuralOrigins<Child::Base>,
        >,
    >: CommitActions<Child, Retired = Vec<BindingTerminal<Bindings>>> + Send + 'static,
    <ActionInterpreter<
        ApplicationCapabilities<
            Child,
            N,
            LocalParentReports<C::Event, Child, Position>,
            NestedBindings<Bindings, Child>,
            StructuralOrigins<Child::Base>,
        >,
    > as CommitActions<Child>>::Error: Send + 'static,
    Origins: ProjectChildTerminal<
            Position,
            Child,
            BindingTerminal<Bindings>,
            <ActionInterpreter<
                ApplicationCapabilities<
                    Child,
                    N,
                    LocalParentReports<C::Event, Child, Position>,
                    NestedBindings<Bindings, Child>,
                    StructuralOrigins<Child::Base>,
                >,
            > as CommitActions<Child>>::Error,
        >,
{
    async fn spawn_child(
        &mut self,
        creation: behavior::Create<MailAddr, Child>,
    ) -> Result<(), EffectInterpretationError> {
        let behavior::Create { nonce, child, kind } = creation;
        if self.child_bindings.endpoint(nonce).is_some() {
            self.creations.record(CreationResolved::rejected(
                nonce,
                kind,
                CreationRejection::NonceAlreadyBound,
            ));
            return Ok(());
        }
        let address = match self.allocations.allocate() {
            Ok(address) => address,
            Err(reason) => {
                self.creations.record(CreationResolved::rejected(
                    nonce,
                    kind,
                    CreationRejection::Allocation(reason),
                ));
                return Ok(());
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
            move |control, terminal_reports, timers, facts| {
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
                            facts,
                            terminal_reports,
                        },
                        NestedBindings::<Bindings, Child>::default(),
                    )
                    .with_parent(
                        LocalParentReports::<C::Event, Child, Position>::new(nonce, parent),
                    ),
                )
            },
        )
        .await;
        match installed {
            Ok(installed) => {
                let endpoint = installed.actor.clone();
                let previous = self.child_bindings.bind(nonce, endpoint, installed.control);
                assert!(
                    previous.is_none(),
                    "one serialized creation transaction binds each nonce once"
                );
                let task = Origins::task(installed.task, address, nonce);
                self.child_bindings.retain_task(task);
                self.creations
                    .record(CreationResolved::installed(nonce, kind, address));
            }
            Err(error) => {
                let reason = error.rejection();
                let terminal = Origins::failure(error, address, nonce);
                self.child_bindings.retain_terminal(terminal);
                self.creations
                    .record(CreationResolved::rejected(nonce, kind, reason));
            }
        }
        Ok(())
    }
}

impl<C, N, P, Bindings, Origins, Target> InterpretDelivery<Target>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send,
    N: ResolveLogical<Target> + Send + Sync,
    Self: Send,
{
    async fn interpret_delivery(&mut self, delivery: Delivery<Target>) -> Result<(), Self::Error> {
        let address = delivery.to.address();
        let actor = self
            .actor_spaces
            .resolve_logical(address)
            .ok_or(EffectInterpretationError::UnknownRecipient(address))?;
        actor
            .send_from(self.address, delivery.message)
            .await
            .map_err(|_| EffectInterpretationError::ClosedRecipient(address))
    }
}

impl<C, N, P, Bindings, Origins, Target> InterpretEstablishedDelivery<Target>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send,
    Self: Send,
{
    async fn interpret_established_delivery(
        &mut self,
        endpoint: ActorRef<Target>,
        message: Target::Msg,
    ) -> Result<(), Self::Error> {
        let address = endpoint.address();
        endpoint
            .send_from(self.address, message)
            .await
            .map_err(|_| EffectInterpretationError::ClosedEstablishedRecipient(address))
    }
}

impl<C, N, P, Bindings, Origins, Target, Occurrence> InterpretChildDelivery<Target, Occurrence>
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
    async fn interpret_child_delivery(
        &mut self,
        delivery: ChildDelivery<Target, Occurrence>,
    ) -> Result<(), Self::Error> {
        if matches!(
            self.creations.resolve(&delivery.nonce),
            Some(CreationResolved { result: Err(_), .. })
        ) {
            return Err(EffectInterpretationError::RejectedCreation(delivery.nonce));
        }
        let actor = self
            .child_bindings
            .endpoint(delivery.nonce)
            .ok_or(EffectInterpretationError::UnknownChild(delivery.nonce))?;
        actor
            .send_from(self.address, delivery.message)
            .await
            .map_err(|_| EffectInterpretationError::ClosedChild(delivery.nonce))
    }
}

impl<C, N, P, Bindings, Origins, Child, Source, Input, Occurrence>
    InterpretChildInput<Child, Source, Input, Occurrence>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>
        + ResolveChildOccurrence<Occurrence, Child = Child>,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child::Event: ChildInputIngress<Source, Input> + Send,
    Input: Send,
    Bindings: ChildBindingAt<ResolvedChildPosition<C, Occurrence>, Child = Child> + Send,
    Self: Send,
{
    async fn interpret_child_input(
        &mut self,
        input: ChildInput<Child, Source, Input, Occurrence>,
    ) -> Result<(), Self::Error> {
        if matches!(
            self.creations.resolve(&input.nonce),
            Some(CreationResolved { result: Err(_), .. })
        ) {
            return Err(EffectInterpretationError::RejectedCreation(input.nonce));
        }
        let control = self
            .child_bindings
            .control(input.nonce)
            .ok_or(EffectInterpretationError::UnknownChild(input.nonce))?;
        let event = Child::Event::child_input(input.input);
        control
            .send(event)
            .map_err(|_| EffectInterpretationError::ClosedChildControl(input.nonce))
    }
}

impl<C, N, P, Bindings, Origins, Path> InterpretRequest<ScheduleAt, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<behavior::TimerElapsed, Path>,
    Self: Send,
{
    async fn interpret_request(&mut self, request: ScheduleAt) -> Result<(), Self::Error> {
        self.timers.schedule_at::<Path>(request).map_err(Into::into)
    }
}

impl<C, N, P, Bindings, Origins, D, Path> InterpretRequest<EntityAdmission<D>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    D: EntityDefinition,
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal>,
    Self: Send,
{
    async fn interpret_request(&mut self, request: EntityAdmission<D>) -> Result<(), Self::Error> {
        request.interpret(self.address).await;
        Ok(())
    }
}

impl<C, N, P, Bindings, Origins, Path> InterpretRequest<ScheduleAfter, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<behavior::TimerElapsed, Path>,
    Self: Send,
{
    async fn interpret_request(&mut self, request: ScheduleAfter) -> Result<(), Self::Error> {
        self.timers
            .schedule_after::<Path>(request)
            .map_err(Into::into)
    }
}

impl<C, N, P, Bindings, Origins, Occurrence, Path>
    InterpretRequest<ObserveCreation<MailAddr, Occurrence>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<CreationResolved<MailAddr>, Path>,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ObserveCreation<MailAddr, Occurrence>,
    ) -> Result<(), Self::Error> {
        let result = self.current_creation(request.nonce)?;
        self.return_fact::<_, Path>(result);
        Ok(())
    }
}

impl<C, N, P, Bindings, Origins, ChildProtocol, Occurrence, Path>
    InterpretRequest<ObserveEstablishedCreation<ChildProtocol, Occurrence>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>> + ResolveChildOccurrence<Occurrence>,
    ChildProtocol: Protocol<Addr = MailAddr>,
    ResolvedChild<C, Occurrence>: Behavior<Protocol = ChildProtocol>,
    Bindings:
        ChildBindingAt<ResolvedChildPosition<C, Occurrence>, Child = ResolvedChild<C, Occurrence>>,
    C::Event: InjectEvent<EstablishedCreation<ChildProtocol, Occurrence>, Path>,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ObserveEstablishedCreation<ChildProtocol, Occurrence>,
    ) -> Result<(), Self::Error> {
        let result = self.current_creation(request.nonce)?;
        let fact = match result.result {
            Ok(_) => {
                let actor = self
                    .child_bindings
                    .endpoint(result.nonce)
                    .ok_or(EffectInterpretationError::UnknownChild(result.nonce))?;
                EstablishedCreation::installed(
                    result.nonce,
                    result.kind,
                    actor.established_recipient(),
                )
            }
            Err(reason) => EstablishedCreation::rejected(result.nonce, result.kind, reason),
        };
        self.return_fact::<_, Path>(fact);
        Ok(())
    }
}

impl<C, N, P, Bindings, Origins, Path> InterpretRequest<ObservePeer<MailAddr>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<behavior::PeerStopped<MailAddr>, Path> + Send + 'static,
    BehaviorMessage<C>: Send,
    N: Hosts<C::Protocol>,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ObservePeer<MailAddr>,
    ) -> Result<(), Self::Error> {
        self.peers
            .get_or_insert_with(|| {
                LocalPeerObservations::new(self.actor_spaces.space().clone(), self.facts.clone())
            })
            .observe::<Path>(request)
            .map_err(Into::into)
    }
}

impl<C, N, P, Bindings, Origins, Occurrence, Path>
    InterpretRequest<ObserveChild<MailAddr, Occurrence>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>> + ResolveChildOccurrence<Occurrence>,
    C::Event: InjectEvent<behavior::ChildStopped<MailAddr>, Path> + Send + 'static,
    ResolvedChild<C, Occurrence>: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Bindings:
        ChildBindingAt<ResolvedChildPosition<C, Occurrence>, Child = ResolvedChild<C, Occurrence>>,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ObserveChild<MailAddr, Occurrence>,
    ) -> Result<(), Self::Error> {
        if matches!(
            self.creations.resolve(&request.nonce),
            Some(CreationResolved { result: Err(_), .. })
        ) {
            return Ok(());
        }
        let child = self
            .child_bindings
            .endpoint(request.nonce)
            .ok_or(EffectInterpretationError::UnknownChild(request.nonce))?;
        self.facts
            .insert_child::<Path>(request.nonce, child.termination_observation());
        Ok(())
    }
}

impl<C, N, P, Bindings, Origins, Child, Occurrence, Path>
    InterpretRequest<ShutdownChild<Child, Occurrence>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>
        + ResolveChildOccurrence<Occurrence, Child = Child>,
    C::Event: InjectEvent<ChildShutdownRejected<u64>, Path>
        + InjectEvent<behavior::ChildStopped<MailAddr>, Path>
        + Send
        + 'static,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Bindings: ChildBindingAt<ResolvedChildPosition<C, Occurrence>, Child = Child>,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ShutdownChild<Child, Occurrence>,
    ) -> Result<(), Self::Error> {
        if matches!(
            self.creations.resolve(&request.nonce),
            Some(CreationResolved { result: Err(_), .. })
        ) {
            self.return_fact::<_, Path>(ChildShutdownRejected::new(
                request.nonce,
                ChildShutdownRejection::NotEstablished,
            ));
            return Ok(());
        }
        let Some(child) = self.child_bindings.endpoint(request.nonce) else {
            self.return_fact::<_, Path>(ChildShutdownRejected::new(
                request.nonce,
                ChildShutdownRejection::NotEstablished,
            ));
            return Ok(());
        };
        match child.request_shutdown() {
            Ok(()) => {
                self.facts
                    .insert_child::<Path>(request.nonce, child.termination_observation());
            }
            Err(ShutdownRejection::AlreadyStopping) => {
                self.return_fact::<_, Path>(ChildShutdownRejected::new(
                    request.nonce,
                    ChildShutdownRejection::AlreadyStopping,
                ));
            }
            Err(ShutdownRejection::AlreadyStopped) => {
                self.return_fact::<_, Path>(ChildShutdownRejected::new(
                    request.nonce,
                    ChildShutdownRejection::NotEstablished,
                ));
            }
        }
        Ok(())
    }
}

impl<C, N, P, Bindings, Origins, Report, Path>
    InterpretRequest<ReportToParent<Report>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    P: ParentReporting<Report> + Send,
    Report: Send,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ReportToParent<Report>,
    ) -> Result<(), Self::Error> {
        self.parent_reports.report(request.into_inner());
        Ok(())
    }
}

impl<C, N, P, Bindings, Origins, Path>
    InterpretRequest<ReportTerminalOutcome<MailAddr>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ReportTerminalOutcome<MailAddr>,
    ) -> Result<(), Self::Error> {
        self.terminal_reports.report_outcome(request);
        Ok(())
    }
}

impl<C, N, P, Bindings, Origins, Path>
    InterpretRequest<ReportSupervisionFailure<MailAddr>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ReportSupervisionFailure<MailAddr>,
    ) -> Result<(), Self::Error> {
        self.terminal_reports.report_supervision(request);
        Ok(())
    }
}

impl<C, N, P, Bindings, Origins, Plan, Path>
    InterpretRequest<ReportShutdownPlan<Plan>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: EventIngress<Here, behavior::InstallShutdownPlan<Plan>>,
    Plan: Send,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ReportShutdownPlan<Plan>,
    ) -> Result<(), Self::Error> {
        match self.control.send(request.into_event()) {
            Ok(()) => Ok(()),
            Err(ControlClosed(_)) => {
                unreachable!("the actor control lane remains live while its effects commit")
            }
        }
    }
}

struct ObservationAt<'a, Capabilities, Path> {
    capabilities: &'a mut Capabilities,
    path: PhantomData<fn() -> Path>,
}

impl<'a, Capabilities, Path> ObservationAt<'a, Capabilities, Path> {
    fn new(capabilities: &'a mut Capabilities) -> Self {
        Self {
            capabilities,
            path: PhantomData,
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Target, Path> InterpretEstablishedObservation<Target>
    for ObservationAt<'_, ApplicationCapabilities<C, N, Parent, Bindings, Origins>, Path>
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
                .return_fact::<_, Path>(EstablishedObservation::rejected(
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
            .return_fact::<_, Path>(EstablishedObservation::<Target>::started(id));
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
                self.capabilities
                    .return_fact::<_, Path>(EstablishedObservation::<Target>::cancelled(id));
            }
            None => self
                .capabilities
                .return_fact::<_, Path>(EstablishedObservation::rejected(
                    id,
                    ObservationOperation::Cancel,
                    ObservationRejection::NotObserved,
                )),
        }
    }
}

impl<C, N, P, Bindings, Origins, Target, Path>
    InterpretRequest<ObserveEstablished<Target>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr> + Send + 'static,
    Target::Msg: Send,
    C::Event: InjectEvent<EstablishedObservation<Target>, Path> + Send + 'static,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ObserveEstablished<Target>,
    ) -> Result<(), Self::Error> {
        request.interpret(&mut ObservationAt::<_, Path>::new(self));
        Ok(())
    }
}

impl<C, N, P, Bindings, Origins, Target, Path>
    InterpretRequest<CancelObservation<Target>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr> + Send + 'static,
    Target::Msg: Send,
    C::Event: InjectEvent<EstablishedObservation<Target>, Path> + Send + 'static,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: CancelObservation<Target>,
    ) -> Result<(), Self::Error> {
        request.interpret(&mut ObservationAt::<_, Path>::new(self));
        Ok(())
    }
}

struct ShutdownAt<'a, Capabilities, Path> {
    capabilities: &'a Capabilities,
    path: PhantomData<fn() -> Path>,
}

impl<'a, Capabilities, Path> ShutdownAt<'a, Capabilities, Path> {
    fn new(capabilities: &'a Capabilities) -> Self {
        Self {
            capabilities,
            path: PhantomData,
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Child, Path> InterpretEstablishedShutdown<Child, Here>
    for ShutdownAt<'_, ApplicationCapabilities<C, N, Parent, Bindings, Origins>, Path>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<EstablishedShutdownResolved<Child::Protocol>, Path>,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child::Event: InjectEvent<ShutdownRequested, Here>,
{
    type Output = ();

    fn shutdown(
        &mut self,
        id: behavior::ShutdownId,
        endpoint: ActorRef<Child::Protocol>,
        _ingress: behavior::Ingress<ShutdownRequested, Here>,
    ) {
        let fact = match endpoint.request_shutdown() {
            Ok(()) => EstablishedShutdownResolved::accepted(id),
            Err(reason) => EstablishedShutdownResolved::rejected(id, reason),
        };
        self.capabilities.return_fact::<_, Path>(fact);
    }
}

impl<C, N, P, Bindings, Origins, Child, Path>
    InterpretRequest<ShutdownEstablished<Child, Here>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<EstablishedShutdownResolved<Child::Protocol>, Path>,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child::Event: InjectEvent<ShutdownRequested, Here>,
    BehaviorMessage<Child>: Send,
    Self: Send,
{
    async fn interpret_request(
        &mut self,
        request: ShutdownEstablished<Child, Here>,
    ) -> Result<(), Self::Error> {
        request.interpret(&mut ShutdownAt::<_, Path>::new(self));
        Ok(())
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

    async fn retire(self) -> CapabilityRetirement<Self::Event, Self::Descendants> {
        let Self {
            peers,
            child_bindings,
            activation_tasks,
            exact_observations,
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
        drop((peers, exact_observations));
        CapabilityRetirement {
            activation_tasks,
            descendants,
        }
    }
}
