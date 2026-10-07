//! Static application assembly and local capability interpretation.

use core::convert::Infallible;
use core::fmt;
use core::future::{Future, Ready, poll_fn};
use core::marker::PhantomData;
use core::ops::AsyncFnOnce;
use core::pin::pin;
use core::task::Poll;
use std::any::Any;
use std::collections::HashMap;
use std::error::Error;
use std::io;
#[cfg(feature = "axum")]
use std::net::SocketAddr;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Instant;
#[cfg(feature = "axum")]
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Handle, TryCurrentError};
use tokio::sync::oneshot::error::RecvError;
use tokio::task::{JoinError, JoinHandle};

use behavior::{
    ActionItem, Actions, Behavior, BehaviorBase, BehaviorMessage, BehaviorSettlements, BirthMode,
    BirthNodeAppend, Births, ChildCons, ChildCreationOutcome, ChildDelivery, ChildDeliveryReason,
    ChildHead, ChildInput, ChildInputIngress, ChildInputReason, ChildNamespaceExhausted,
    ChildOccurrenceProduct, ChildOccurrenceShape, ChildOccurrences, ChildProduct, ChildTail,
    Children, ClassifySettlement, CommittedChild, CreateChild, CreationCorrelation, CreationId,
    CreationKind, CreationRejection, CreationSequence, Creations, Delivery, EstablishChild,
    EstablishedActor, EstablishedCreation, EstablishedDelivery, EstablishedRecipient, EventIngress,
    ExactDeliveryReason, Here, Ingress, InjectEvent, InterpretItem, InterpretationProgress,
    InterpreterFault, ItemSettlement, LogicalDeliveryReason, Never, NoBirths, NoChildren, Protocol,
    Recipient, RecoverEvent, ReportToParent, ResolveChildOccurrence, ResolvedChild,
    ResolvedChildPosition, RetirementBirths, RoutedCreation, SourceAdmission,
};
use behavior_actors::atomic::{
    ActivationPlan, AssignWorker, Assignment, BeginActivation, CustomerDelivery,
    DiagnosticAccepted, DiagnosticAction, InitializeWorker, PrepareWorkers, ProxyControl,
    ProxyControlAdmission, ProxyOperation, StableProxy, WorkerActivation,
    WorkerInitializationOutcome, WorkerInitializationReport, WorkerPreparation, WorkerSource,
};
use behavior_actors::{
    CancelObservation, ChildShutdownRejection, ChildStopped, CreationResolved,
    EstablishedObservation, EstablishedShutdownResolved, InstallShutdownPlan,
    InterpretEstablishedObservation, InterpretEstablishedShutdown, ObservationAuthority,
    ObservationId, ObservationRejection, ObserveChild, ObserveCreation, ObserveEstablished,
    ObserveEstablishedCreation, ObservePeer, PeerObservationRejection, PeerStopped, ReplyDelivery,
    ReportShutdownPlan, ReportTerminalOutcome, ScheduleAfter, ScheduleAfterRejection, ScheduleAt,
    ScheduleAtRejection, ShutdownChild, ShutdownEstablished, ShutdownId, ShutdownRejection,
    ShutdownRequested, TimerElapsed, TimerScheduled,
};
use bombay_address::ClaimError;
use bombay_engine::DriverError;
use communication::{ControlClosed, ControlSender};
use tokio::sync::oneshot;

use crate::ActorExecutionOutcome;
use crate::actor_interface::{ActorInterface, ExtractLocalEndpoint};
use crate::address::{ApplicationAddresses, MailAddr};
use crate::application::Application;
use crate::child_bindings::{
    ChildBindingAt, ChildBindings, CreationBinding, NestedBindings, NestedChildBindings,
    NoChildBindings, RetireChildTasks, RuntimeChildBindings,
};
use crate::entity::{
    EntityAdmission, EntityDefinition, EntityFamilyAt, InstallEntityFamilies,
    InstalledEntityFamilies, NativeEntityHost, Passivation,
};
use crate::interpret::{ActionInterpreter, RetireCapabilities};
use crate::launch::{
    ActorSpace, ProjectedTask, SpawnError, spawn_local_execution, spawn_owned_with,
};
use crate::local::{
    ActivationTasks, ActorRef, CapabilityRetirement, CommitActions, InstalledActor,
    LocalActivationRejection, LocalResidual, StandardIngress, Termination, request_actor_shutdown,
};
use crate::observation::{TerminationObservations, observe_peer};
use crate::reports::{
    LocalParentReports, LocalTerminalReports, ParentReporting, TerminalReportTransaction,
};
use crate::terminal::{ActorRetirement, ChildOrigin, ProjectTerminal, RootOrigin};
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
    ChildBindings<Actor, Terminal, Origins>,
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

/// Failure while constructing or entering the owned blocking executor.
#[derive(thiserror::Error)]
pub enum RunError {
    /// Blocking execution was requested while a Tokio runtime was entered.
    #[error("blocking application execution is forbidden inside an entered Tokio runtime")]
    BlockingInEnteredRuntime,
    /// Tokio could not construct the application executor.
    #[error("Bombay could not construct its Tokio runtime")]
    Runtime(#[source] io::Error),
}

impl fmt::Debug for RunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::BlockingInEnteredRuntime => "BlockingInEnteredRuntime",
            Self::Runtime(_) => "Runtime",
        })
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
                <<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::ChildFailures,
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

pub(crate) struct StructuralOrigins<Owner>(PhantomData<fn() -> Owner>);
struct ApplicationOrigins<Root, Members>(PhantomData<fn() -> (Root, Members)>);

#[allow(
    private_bounds,
    reason = "the launch proof is private static runtime composition"
)]
impl<Root, Spaces> App<Root, Spaces>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
{
    /// Construct genuine absent work with original inputs and joined raw root facts.
    /// `Completed { output: None }` means an actual startup grant was acquired without supplied work.
    /// This comparison selects no terminal projection and constructs no callable value.
    /// The execution remains cold; only polling stages or starts the actor.
    ///
    /// # Errors
    /// Returns the untouched application and actual entered-host error.
    ///
    /// # Panics
    /// Execution propagates staging panics; receiving preserves the actual cleanup publication result.
    #[expect(
        clippy::type_complexity,
        reason = "the existing concrete pair preserves original inputs and independent exact root custody without a new wrapper"
    )]
    pub fn execute<Terminal, ChildFailures>(self) -> Result<(impl Future<Output = ()>, impl Future<Output = ApplicationOutcome<Self, Option<Never>, Option<Never>, (RootOrigin<Root>, Result<ActorRetirement<Root, Terminal, ChildFailures>, JoinError>), Never, (Root, Spaces)>>), (Self, TryCurrentError)>
where
    Root: BehaviorBase + BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Root::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    BehaviorMessage<Root>: Send + 'static,
    Root::Sends: Send + 'static,
    Root::Error: Send + 'static,
    Root::InterpretationCustody: Send + 'static,
    Root::SourceCustody: Send + 'static,
    <Root::Birth as BirthMode>::Child:
        ChildOccurrenceProduct<RuntimeChildBindings<Terminal, StructuralOrigins<Root::Base>>> + Send + 'static,
    Spaces: Hosts<Root::Protocol> + Send + Sync + 'static,
    ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>>:
        Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
    RootInterpreter<Root, Spaces, Terminal, StructuralOrigins<Root::Base>>: CommitActions<
            Root,
            Retired = (
                Vec<Terminal>,
                <ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>> as RetireChildTasks>::Failures,
            ),
        > + Send
        + 'static,
    <Root as BehaviorSettlements>::Settlements: ClassifySettlement + Send,
    Terminal: Send + 'static,
    ChildFailures: Send + 'static,
{
        let executor = match Handle::try_current() {
            Ok(executor) => executor,
            Err(error) => return Err((self, error)),
        };
        Ok(execute_application_with::<
            Self,
            Root,
            Root,
            Spaces,
            StructuralOrigins<Root::Base>,
            Terminal,
            ChildFailures,
            (),
            Never,
            _,
            _,
            _,
            Never,
            fn(Never, ApplicationHandle<Root::Protocol, Root::Event>) -> Ready<Never>,
            Ready<Never>,
            Never,
            _,
            _,
            _,
        >(
            executor,
            self,
            None,
            async |_| {},
            |App {
                 root,
                 spaces,
                 families: (),
             },
             _work,
             (),
             _allocations| Ok((root, spaces, ())),
            ((), |_executor, (), ()| None),
            |root| root,
        ))
    }

    /// Await genuine absent work with original inputs and joined raw root facts.
    /// `Completed { output: None }` means an actual startup grant was acquired without supplied work.
    /// This comparison selects no terminal projection and constructs no callable value.
    /// The execution remains cold; only polling stages or starts the actor.
    ///
    /// # Errors
    /// Returns the untouched application and actual entered-host error.
    ///
    /// # Panics
    /// Execution propagates staging panics; receiving preserves the actual cleanup publication result.
    pub async fn run<Terminal, ChildFailures>(self) -> Result<ApplicationOutcome<Self, Option<Never>, Option<Never>, (RootOrigin<Root>, Result<ActorRetirement<Root, Terminal, ChildFailures>, JoinError>), Never, (Root, Spaces)>, (Self, TryCurrentError)>
where
    Root: BehaviorBase + BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Root::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    BehaviorMessage<Root>: Send + 'static,
    Root::Sends: Send + 'static,
    Root::Error: Send + 'static,
    Root::InterpretationCustody: Send + 'static,
    Root::SourceCustody: Send + 'static,
    <Root::Birth as BirthMode>::Child:
        ChildOccurrenceProduct<RuntimeChildBindings<Terminal, StructuralOrigins<Root::Base>>> + Send + 'static,
    Spaces: Hosts<Root::Protocol> + Send + Sync + 'static,
    ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>>:
        Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
    RootInterpreter<Root, Spaces, Terminal, StructuralOrigins<Root::Base>>: CommitActions<
            Root,
            Retired = (
                Vec<Terminal>,
                <ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>> as RetireChildTasks>::Failures,
            ),
        > + Send
        + 'static,
    <Root as BehaviorSettlements>::Settlements: ClassifySettlement + Send,
    Terminal: Send + 'static,
    ChildFailures: Send + 'static,
{
        let (execution, result) = self.execute::<Terminal, ChildFailures>()?;
        execution.await;
        Ok(result.await)
    }

    /// Construct caller-local execution and its independently owned result receiver.
    ///
    /// The returned execution is cold. The original work may borrow and return
    /// non-Send values. Only the exact actor and static cleanup move to Tokio.
    /// The current executor is selected here; the synchronous actor handoff
    /// enters that executor before its first spawn.
    /// Original interpretation and source custody travel with actor retirement;
    /// their `Send + 'static` requirements do not constrain caller-local work.
    ///
    /// # Errors
    ///
    /// Returns the original application and callable with the actual Tokio
    /// context error when no executor is currently entered.
    ///
    /// # Panics
    ///
    /// The execution future propagates panics from application setup and work.
    #[expect(
        clippy::type_complexity,
        reason = "the independent execution and result preserve exact input, output and two join boundaries"
    )]
    pub fn execute_with<Terminal, ChildFailures, Work, WorkFuture, Output>(
        self,
        work: Work,
    ) -> Result<
        (
            impl Future<Output = ()>,
            impl Future<Output = ApplicationOutcome<
                Self,
                Option<Work>,
                Option<Output>,
                (RootOrigin<Root>, Result<ActorRetirement<Root, Terminal, ChildFailures>, JoinError>),
                (Root, Work, Never),
                (Root, Spaces),
            >>,
        ),
        (Self, Work, TryCurrentError),
    >
where
    Root: BehaviorBase + BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Root::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    BehaviorMessage<Root>: Send + 'static,
    Root::Sends: Send + 'static,
    Root::Error: Send + 'static,
    Root::InterpretationCustody: Send + 'static,
    Root::SourceCustody: Send + 'static,
    <Root::Birth as BirthMode>::Child:
        ChildOccurrenceProduct<RuntimeChildBindings<Terminal, StructuralOrigins<Root::Base>>> + Send + 'static,
    Spaces: Hosts<Root::Protocol> + Send + Sync + 'static,
    ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>>:
        Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
    RootInterpreter<Root, Spaces, Terminal, StructuralOrigins<Root::Base>>: CommitActions<
            Root,
            Retired = (
                Vec<Terminal>,
                <ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>> as RetireChildTasks>::Failures,
            ),
        > + Send
        + 'static,
    <Root as BehaviorSettlements>::Settlements: ClassifySettlement + Send,
    Terminal: Send + 'static,
    ChildFailures: Send + 'static,
    Work: FnOnce(ApplicationHandle<Root::Protocol, Root::Event>) -> WorkFuture,
        WorkFuture: Future<Output = Output>,
{
        let executor = match Handle::try_current() {
            Ok(executor) => executor,
            Err(error) => return Err((self, work, error)),
        };
        Ok(execute_application_with::<
            Self,
            Root,
            Root,
            Spaces,
            StructuralOrigins<Root::Base>,
            Terminal,
            ChildFailures,
            (),
            (Root, Work, Never),
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
        >(
            executor,
            self,
            Some((work, |work: Work, application| work(application))),
            async |_| {},
            |App {
                 root,
                 spaces,
                 families: (),
             },
             _work,
             (),
             _allocations| Ok((root, spaces, ())),
            ((), |_executor, (), ()| None),
            |root| root,
        ))
    }

    /// Await caller-local work and the exact application result on the entered host.
    ///
    /// This future is cold until polled. Work and its output may borrow or be non-Send.
    /// Use `execute_with` when the result receiver must remain independently owned
    /// after dropping execution; dropping this convenience also drops that receiver.
    ///
    /// # Errors
    /// Returns the original application and work with the actual entered-host error.
    ///
    /// # Panics
    /// Setup and work may unwind; original native causes remain with the caller.
    pub async fn run_with<Terminal, ChildFailures, Work, WorkFuture, Output>(
        self,
        work: Work,
    ) -> Result<
        ApplicationOutcome<
            Self,
            Option<Work>,
            Option<Output>,
            (
                RootOrigin<Root>,
                Result<ActorRetirement<Root, Terminal, ChildFailures>, JoinError>,
            ),
            (Root, Work, Never),
            (Root, Spaces),
        >,
        (Self, Work, TryCurrentError),
    >
where
    Root: BehaviorBase + BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Root::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    BehaviorMessage<Root>: Send + 'static,
    Root::Sends: Send + 'static,
    Root::Error: Send + 'static,
    Root::InterpretationCustody: Send + 'static,
    Root::SourceCustody: Send + 'static,
    <Root::Birth as BirthMode>::Child:
        ChildOccurrenceProduct<RuntimeChildBindings<Terminal, StructuralOrigins<Root::Base>>> + Send + 'static,
    Spaces: Hosts<Root::Protocol> + Send + Sync + 'static,
    ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>>:
        Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
    RootInterpreter<Root, Spaces, Terminal, StructuralOrigins<Root::Base>>: CommitActions<
            Root,
            Retired = (
                Vec<Terminal>,
                <ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>> as RetireChildTasks>::Failures,
            ),
        > + Send
        + 'static,
    <Root as BehaviorSettlements>::Settlements: ClassifySettlement + Send,
    Terminal: Send + 'static,
    ChildFailures: Send + 'static,
    Work: FnOnce(ApplicationHandle<Root::Protocol, Root::Event>) -> WorkFuture,
        WorkFuture: Future<Output = Output>,
{
        let (execution, result) =
            self.execute_with::<Terminal, ChildFailures, Work, WorkFuture, Output>(work)?;
        execution.await;
        Ok(result.await)
    }

    #[cfg(feature = "axum")]
    /// Pair asynchronous prebind and serving with the original application result receiver.
    /// Cold inputs remain published during bind; invocation owns the bare router and listener.
    /// Primary root projection is caller-owned after acquiring the raw joined outcome.
    ///
    /// # Errors
    /// Returns the untouched application, router and address with the actual entered-host error.
    /// Bind refusal is an exact `StagingRejected` product before actor handoff.
    ///
    /// # Panics
    /// Setup or router invocation can unwind; the original cause remains with the caller.
    #[expect(
        clippy::type_complexity,
        reason = "retain original router/listener and all raw root result boundaries"
    )]
    pub fn execute_axum<Terminal, ChildFailures, Router>(
        self,
        address: SocketAddr,
        router: Router,
    ) -> Result<
        (
            impl Future<Output = ()>,
            impl Future<Output = ApplicationOutcome<
                (Self, Router, SocketAddr),
                Option<(Router, TcpListener)>,
                Option<Result<(), io::Error>>,
                (RootOrigin<Root>, Result<ActorRetirement<Root, Terminal, ChildFailures>, JoinError>),
                (Self, Router, SocketAddr, io::Error),
                (Root, Spaces),
            >>,
        ),
        ((Self, Router, SocketAddr), TryCurrentError),
    >
where
    Root: BehaviorBase + BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Root::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    BehaviorMessage<Root>: Send + 'static,
    Root::Sends: Send + 'static,
    Root::Error: Send + 'static,
    Root::InterpretationCustody: Send + 'static,
    Root::SourceCustody: Send + 'static,
    <Root::Birth as BirthMode>::Child:
        ChildOccurrenceProduct<RuntimeChildBindings<Terminal, StructuralOrigins<Root::Base>>> + Send + 'static,
    Spaces: Hosts<Root::Protocol> + Send + Sync + 'static,
    ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>>:
        Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
    RootInterpreter<Root, Spaces, Terminal, StructuralOrigins<Root::Base>>: CommitActions<
            Root,
            Retired = (
                Vec<Terminal>,
                <ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>> as RetireChildTasks>::Failures,
            ),
        > + Send
        + 'static,
    <Root as BehaviorSettlements>::Settlements: ClassifySettlement + Send,
    Terminal: Send + 'static,
    ChildFailures: Send + 'static,
    Router: FnOnce(ApplicationHandle<Root::Protocol, Root::Event>) -> axum::Router,
{
        let executor = match Handle::try_current() {
            Ok(executor) => executor,
            Err(error) => return Err(((self, router, address), error)),
        };
        Ok(execute_application_with::<
            _,
            Root,
            Root,
            Spaces,
            StructuralOrigins<Root::Base>,
            Terminal,
            ChildFailures,
            (),
            (Self, Router, SocketAddr, io::Error),
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
        >(
            executor,
            (self, router, address),
            None,
            async |(_, _, address): &(Self, Router, SocketAddr)| TcpListener::bind(*address).await,
            |(application, router, address), work, listener, _allocations| {
                let listener = match listener {
                    Ok(listener) => listener,
                    Err(error) => return Err((application, router, address, error)),
                };
                *work = Some(((router, listener), async |(router, listener): (Router, TcpListener), application: ApplicationHandle<Root::Protocol, Root::Event>| {
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
                    serve
                }));
                let App {
                    root,
                    spaces,
                    families: (),
                } = application;
                Ok((root, spaces, ()))
            },
            ((), |_executor, (), ()| None),
            |root| root,
        ))
    }

    #[cfg(feature = "axum")]
    /// Await asynchronous prebind, caller-local serving and the complete raw root result.
    /// Use `execute_axum` when a separately retained result receiver is required.
    ///
    /// # Errors
    /// Returns original cold inputs and the native entered-host error.
    /// Actual bind refusal remains an explicit `StagingRejected` product.
    ///
    /// # Panics
    /// Setup or serving can unwind; the original native cause remains with the caller.
    pub async fn run_axum<Terminal, ChildFailures, Router>(
        self,
        address: SocketAddr,
        router: Router,
    ) -> Result<
        ApplicationOutcome<
            (Self, Router, SocketAddr),
            Option<(Router, TcpListener)>,
            Option<Result<(), io::Error>>,
            (RootOrigin<Root>, Result<ActorRetirement<Root, Terminal, ChildFailures>, JoinError>),
            (Self, Router, SocketAddr, io::Error),
            (Root, Spaces),
        >,
        ((Self, Router, SocketAddr), TryCurrentError),
    >
where
    Root: BehaviorBase + BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Root::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    BehaviorMessage<Root>: Send + 'static,
    Root::Sends: Send + 'static,
    Root::Error: Send + 'static,
    Root::InterpretationCustody: Send + 'static,
    Root::SourceCustody: Send + 'static,
    <Root::Birth as BirthMode>::Child:
        ChildOccurrenceProduct<RuntimeChildBindings<Terminal, StructuralOrigins<Root::Base>>> + Send + 'static,
    Spaces: Hosts<Root::Protocol> + Send + Sync + 'static,
    ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>>:
        Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
    RootInterpreter<Root, Spaces, Terminal, StructuralOrigins<Root::Base>>: CommitActions<
            Root,
            Retired = (
                Vec<Terminal>,
                <ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>> as RetireChildTasks>::Failures,
            ),
        > + Send
        + 'static,
    <Root as BehaviorSettlements>::Settlements: ClassifySettlement + Send,
    Terminal: Send + 'static,
    ChildFailures: Send + 'static,
    Router: FnOnce(ApplicationHandle<Root::Protocol, Root::Event>) -> axum::Router,
{
        let (execution, result) =
            self.execute_axum::<Terminal, ChildFailures, Router>(address, router)?;
        execution.await;
        Ok(result.await)
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
    /// Pair caller-local Entity work with original root and family retirement receiving.
    /// The receiving product keeps work/startup phase, root receiving and each family receiving result.
    /// Its cleanup join remains the actual unit producer outcome beside independently acquired facts.
    /// A receiving error never infers root absence or completed family shutdown.
    /// Definitions install before receptionists; the original borrowed setup cause stays native.
    ///
    /// # Errors
    /// Returns untouched application and bare Work with actual missing-host error.
    ///
    /// # Panics
    /// Original borrowed setup or Work causes unwind in the caller; retained receiving remains independent.
    #[expect(
        clippy::type_complexity,
        reason = "retain independent work phase, original root reply and role-indexed shutdown receiving results"
    )]
    pub fn execute_with_entities<Terminal, ChildFailures, Work, WorkFuture, Output>(
        self,
        work: Work,
    ) -> Result<
        (
            impl Future<Output = ()>,
            impl Future<Output = (ApplicationOutcome<
                Self,
                Option<Work>,
                Option<Output>,
                (),
                (Root, Work, Never),
                (Root, Arc<Spaces>),
            >,
                Result<(RootOrigin<Root>, Result<ActorRetirement<Root, Terminal, ChildFailures>, JoinError>), RecvError>,
                Families::Shutdowns,
            )>,
        ),
        (Self, Work, TryCurrentError),
    >
where
    Root: BehaviorBase + BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Root::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    BehaviorMessage<Root>: Send + 'static,
    Root::Sends: Send + 'static,
    Root::Error: Send + 'static,
    Root::InterpretationCustody: Send + 'static,
    Root::SourceCustody: Send + 'static,
    <Root::Birth as BirthMode>::Child:
        ChildOccurrenceProduct<RuntimeChildBindings<Terminal, StructuralOrigins<Root::Base>>> + Send + 'static,
    Spaces: Hosts<Root::Protocol> + Send + Sync + 'static,
    ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>>:
        Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
    RootInterpreter<Root, Arc<Spaces>, Terminal, StructuralOrigins<Root::Base>>: CommitActions<
            Root,
            Retired = (
                Vec<Terminal>,
                <ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>> as RetireChildTasks>::Failures,
            ),
        > + Send
        + 'static,
    <Root as BehaviorSettlements>::Settlements: ClassifySettlement + Send,
    Terminal: Send + 'static,
    ChildFailures: Send + 'static,
    Families::Installed: 'static,
    Work: FnOnce(ApplicationHandle<Root::Protocol, Root::Event, Families::Receptionists>) -> WorkFuture,
    WorkFuture: Future<Output = Output>,
{
        let executor = match Handle::try_current() {
            Ok(executor) => executor,
            Err(error) => return Err((self, work, error)),
        };
        let entity_executor = executor.clone();
        let (root_publication, received_root) = oneshot::channel();
        let (family_publications, received_families) =
            <Families::Installed as InstalledEntityFamilies>::shutdown_receiving();
        let (execution, receiving) = execute_application_with::<
            Self,
            Root,
            Root,
            Arc<Spaces>,
            StructuralOrigins<Root::Base>,
            Terminal,
            ChildFailures,
            Families::Installed,
            (Root, Work, Never),
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
        >(
            executor,
            self,
            Some((work, |work: Work, application| work(application))),
            async |_| {},
            move |App {
                      root,
                      spaces,
                      families,
                  },
                  _work,
                  (),
                  allocations| {
                let spaces = Arc::new(spaces);
                let installed_families =
                    families.install(Arc::clone(&spaces), allocations, entity_executor);
                Ok((root, spaces, installed_families))
            },
            (
                family_publications,
                |executor: Handle, installed_families: Families::Installed, publications| {
                    Some(executor.spawn(installed_families.shutdown(publications)))
                },
            ),
            move |root| match root_publication.send(root) {
                Ok(()) => {}
                // The final receiving owner explicitly surrendered its receipt.
                // Rejected send returns the exact original fact for one discharge.
                Err(root) => drop(root),
            },
        );
        let receiving = async move {
            let work_outcome = receiving.await;
            let root_retirement = received_root.await;
            let family_retirements = received_families.await;
            (work_outcome, root_retirement, family_retirements)
        };
        Ok((execution, receiving))
    }

    /// Await caller-local Entity work and the exact independent root/family receiving product.
    /// Use `execute_with_entities` to retain receiving independently after execution is dropped.
    ///
    /// # Errors
    /// Returns original application and Work with the actual entered-host error.
    ///
    /// # Panics
    /// Setup and Work can unwind; original native causes remain caller-owned.
    pub async fn run_with_entities<Terminal, ChildFailures, Work, WorkFuture, Output>(
        self,
        work: Work,
    ) -> Result<
        (ApplicationOutcome<
            Self,
            Option<Work>,
            Option<Output>,
            (),
            (Root, Work, Never),
            (Root, Arc<Spaces>),
        >,
            Result<(RootOrigin<Root>, Result<ActorRetirement<Root, Terminal, ChildFailures>, JoinError>), RecvError>,
            Families::Shutdowns,
        ),
        (Self, Work, TryCurrentError),
    >
where
    Root: BehaviorBase + BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Root::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    BehaviorMessage<Root>: Send + 'static,
    Root::Sends: Send + 'static,
    Root::Error: Send + 'static,
    Root::InterpretationCustody: Send + 'static,
    Root::SourceCustody: Send + 'static,
    <Root::Birth as BirthMode>::Child:
        ChildOccurrenceProduct<RuntimeChildBindings<Terminal, StructuralOrigins<Root::Base>>> + Send + 'static,
    Spaces: Hosts<Root::Protocol> + Send + Sync + 'static,
    ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>>:
        Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
    RootInterpreter<Root, Arc<Spaces>, Terminal, StructuralOrigins<Root::Base>>: CommitActions<
            Root,
            Retired = (
                Vec<Terminal>,
                <ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>> as RetireChildTasks>::Failures,
            ),
        > + Send
        + 'static,
    <Root as BehaviorSettlements>::Settlements: ClassifySettlement + Send,
    Terminal: Send + 'static,
    ChildFailures: Send + 'static,
    Families::Installed: 'static,
    Work: FnOnce(ApplicationHandle<Root::Protocol, Root::Event, Families::Receptionists>) -> WorkFuture,
    WorkFuture: Future<Output = Output>,
{
        let (execution, result) =
            self.execute_with_entities::<Terminal, ChildFailures, Work, WorkFuture, Output>(work)?;
        execution.await;
        Ok(result.await)
    }
}

trait StageApplicationChildren: Sized {
    type Product: ChildProduct<MailAddr>;
    type Failure;

    fn stage(
        self,
        creations: &mut CreationSequence,
    ) -> Result<Children<MailAddr, Self::Product>, Self::Failure>;
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
    type Failure = Never;

    fn stage(
        self,
        _: &mut CreationSequence,
    ) -> Result<Children<MailAddr, Self::Product>, Self::Failure> {
        Ok(Children::new())
    }
}

impl<Role, Actor, Tail> StageApplicationChildren for (Role, Actor, Tail)
where
    Actor: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Tail: StageApplicationChildren,
{
    type Product = ChildCons<MailAddr, Actor, Tail::Product>;
    type Failure = ApplicationStagingError<Role, Actor, Tail::Product, Tail::Failure>;

    fn stage(
        self,
        creations: &mut CreationSequence,
    ) -> Result<Children<MailAddr, Self::Product>, Self::Failure> {
        let (role, actor, tail) = self;
        let children = match tail.stage(creations) {
            Ok(children) => children,
            Err(tail) => return Err(ApplicationStagingError::TailRejected { role, actor, tail }),
        };
        let Some(id) = creations.issue() else {
            return Err(ApplicationStagingError::NonceExhausted {
                role,
                actor,
                children,
            });
        };
        match catch_unwind(AssertUnwindSafe(|| drop(role))) {
            Ok(()) => Ok(children.child(id, actor)),
            Err(cause) => Err(ApplicationStagingError::RoleDisposalPanicked {
                id,
                actor,
                children,
                cause,
            }),
        }
    }
}

/// Failure while initializing the exact composed application behavior.
pub enum ApplicationDefinitionError<RootError> {
    Root(RootError),
    InitializedTwice,
}

/// Exact available cold declarations when application staging stops before any actor starts.
///
/// A consumed role is not reconstructed. The original root and uninvoked caller work
/// remain in the enclosing run error, independently of this child product.
#[must_use = "original cold declarations and native causes require explicit custody"]
pub enum ApplicationStagingError<Role, Actor, Product, TailFailure>
where
    Product: ChildProduct<MailAddr>,
{
    /// The tail returned a partial; the current role and actor were never attempted.
    TailRejected {
        role: Role,
        actor: Actor,
        tail: TailFailure,
    },
    /// No current ID was issued; the current role, actor and completed tail survive.
    NonceExhausted {
        role: Role,
        actor: Actor,
        children: Children<MailAddr, Product>,
    },
    /// The current role was consumed; the actual ID, actor, completed tail and cause survive.
    RoleDisposalPanicked {
        id: CreationId,
        actor: Actor,
        children: Children<MailAddr, Product>,
        cause: Box<dyn Any + Send>,
    },
}

impl<Role, Actor, Product, TailFailure> fmt::Debug
    for ApplicationStagingError<Role, Actor, Product, TailFailure>
where
    Product: ChildProduct<MailAddr>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TailRejected { .. } => "TailRejected",
            Self::NonceExhausted { .. } => "NonceExhausted",
            Self::RoleDisposalPanicked { .. } => "RoleDisposalPanicked",
        })
    }
}

impl<Role, Actor, Product, TailFailure> fmt::Display
    for ApplicationStagingError<Role, Actor, Product, TailFailure>
where
    Product: ChildProduct<MailAddr>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TailRejected { .. } => "the tail retains a stopped cold declaration",
            Self::NonceExhausted { .. } => "no creator-local ID remained for the cold declaration",
            Self::RoleDisposalPanicked { .. } => {
                "the cold declaration retains a role disposal panic"
            }
        })
    }
}

impl<Role, Actor, Product, TailFailure> Error
    for ApplicationStagingError<Role, Actor, Product, TailFailure>
where
    Product: ChildProduct<MailAddr>,
{
}

fn stage_application<Root, Members>(
    root: Root,
    members: Members,
) -> Result<ApplicationBehavior<Root, ApplicationProduct<Members>>, (Root, Members::Failure)>
where
    Root: Behavior,
    Members: StageApplicationChildren,
{
    let mut creations = CreationSequence::new();
    match members.stage(&mut creations) {
        Ok(application) => Ok(ApplicationBehavior::new(root, application)),
        Err(failure) => Err((root, failure)),
    }
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

trait ComposeApplication<Root>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
{
    type Actor: Behavior<Protocol = Root::Protocol, Ph = Never>;
    type Origins;
    type StagingFailure;

    fn compose(self, root: Root) -> Result<Self::Actor, (Root, Self::StagingFailure)>;
}

impl<Root> ComposeApplication<Root> for ()
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
{
    type Actor = Root;
    type Origins = StructuralOrigins<Root::Base>;
    type StagingFailure = Never;

    fn compose(self, root: Root) -> Result<Self::Actor, (Root, Self::StagingFailure)> {
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
    type StagingFailure = <Self as StageApplicationChildren>::Failure;

    fn compose(self, root: Root) -> Result<Self::Actor, (Root, Self::StagingFailure)> {
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
    /// Execute declared cold composition and caller-local work with an independent result receiver.
    ///
    /// A cold staging rejection owns the actual partial and original callable without a cleanup fiction.
    /// Original interpretation and source custody travel with actor retirement;
    /// their `Send + 'static` requirements do not constrain caller-local work.
    ///
    /// # Errors
    ///
    /// Returns the original application and callable with the exact `TryCurrentError`
    /// when no Tokio runtime is currently entered; neither input has been started.
    #[expect(
        clippy::type_complexity,
        reason = "return independent local work and actual cold-partial or joined actor products without another wrapper"
    )]
    pub fn execute_with<Actor, StagingFailure, Terminal, ChildFailures, Work, WorkFuture, Output>(
        self,
        work: Work,
    ) -> Result<
        (
            impl Future<Output = ()>,
            impl Future<
                Output = ApplicationOutcome<
                    Self,
                    Option<Work>,
                    Option<Output>,
                    (
                        RootOrigin<Root>,
                        Result<ActorRetirement<Actor, Terminal, ChildFailures>, JoinError>,
                    ),
                    (Root, Work, StagingFailure),
                    (Actor, ActorSpace<Root::Protocol>),
                >,
            >,
        ),
        (Self, Work, TryCurrentError),
    >
    where
        Members: ComposeApplication<Root, Actor = Actor, StagingFailure = StagingFailure>,
        Actor: BehaviorBase
            + BehaviorSettlements<Protocol = Root::Protocol, Ph = Never>
            + Send
            + 'static,
        Actor::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
        BehaviorMessage<Actor>: Send + 'static,
        Actor::Sends: Send + 'static,
        Actor::Error: Send + 'static,
        Actor::InterpretationCustody: Send + 'static,
        Actor::SourceCustody: Send + 'static,
        <Actor::Birth as BirthMode>::Child: ChildOccurrenceProduct<RuntimeChildBindings<Terminal, Members::Origins>>
            + Send
            + 'static,
        ChildBindings<Actor, Terminal, Members::Origins>:
            Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
        RootInterpreter<Actor, ActorSpace<Root::Protocol>, Terminal, Members::Origins>:
            CommitActions<Actor, Retired = (Vec<Terminal>, ChildFailures)> + Send + 'static,
        Actor::Settlements: ClassifySettlement + Send,
        RootOrigin<Root>: Send + 'static,
        Terminal: Send + 'static,
        ChildFailures: Send + 'static,
        Work: FnOnce(ApplicationHandle<Root::Protocol, Actor::Event>) -> WorkFuture,
        WorkFuture: Future<Output = Output>,
    {
        let executor = match Handle::try_current() {
            Ok(executor) => executor,
            Err(error) => return Err((self, work, error)),
        };
        Ok(execute_application_with::<
            Self,
            Root,
            Actor,
            ActorSpace<Root::Protocol>,
            Members::Origins,
            Terminal,
            ChildFailures,
            (),
            (Root, Work, StagingFailure),
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
        >(
            executor,
            self,
            Some((work, |work: Work, application| work(application))),
            async |_| {},
            |application, work, (), _allocations| {
                let (root, members) = application.into_parts();
                match members.compose(root) {
                    Ok(actor) => Ok((actor, ActorSpace::new(), ())),
                    Err((root, failure)) => {
                        let Some((work, _invoke)) = work.take() else {
                            unreachable!("this constructor supplies actual work");
                        };
                        Err((root, work, failure))
                    }
                }
            },
            ((), |_executor, (), ()| None),
            |root| root,
        ))
    }

    /// Construct genuine absent work with original inputs and joined raw root facts.
    /// `Completed { output: None }` means an actual startup grant was acquired without supplied work.
    /// This comparison selects no terminal projection and constructs no callable value.
    /// The execution remains cold; only polling stages or starts the actor.
    ///
    /// # Errors
    /// Returns the untouched application and actual entered-host error.
    ///
    /// # Panics
    /// Execution propagates staging panics; receiving preserves the actual cleanup publication result.
    #[expect(
        clippy::type_complexity,
        reason = "the existing concrete pair preserves original inputs and independent exact root custody without a new wrapper"
    )]
    pub fn execute<Actor, StagingFailure, Terminal, ChildFailures>(
        self,
    ) -> Result<
        (
            impl Future<Output = ()>,
            impl Future<
                Output = ApplicationOutcome<
                    Self,
                    Option<Never>,
                    Option<Never>,
                    (
                        RootOrigin<Root>,
                        Result<ActorRetirement<Actor, Terminal, ChildFailures>, JoinError>,
                    ),
                    (Root, StagingFailure),
                    (Actor, ActorSpace<Root::Protocol>),
                >,
            >,
        ),
        (Self, TryCurrentError),
    >
    where
        Members: ComposeApplication<Root, Actor = Actor, StagingFailure = StagingFailure>,
        Actor: BehaviorBase
            + BehaviorSettlements<Protocol = Root::Protocol, Ph = Never>
            + Send
            + 'static,
        Actor::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
        BehaviorMessage<Actor>: Send + 'static,
        Actor::Sends: Send + 'static,
        Actor::Error: Send + 'static,
        Actor::InterpretationCustody: Send + 'static,
        Actor::SourceCustody: Send + 'static,
        <Actor::Birth as BirthMode>::Child: ChildOccurrenceProduct<RuntimeChildBindings<Terminal, Members::Origins>>
            + Send
            + 'static,
        ChildBindings<Actor, Terminal, Members::Origins>:
            Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
        RootInterpreter<Actor, ActorSpace<Root::Protocol>, Terminal, Members::Origins>:
            CommitActions<Actor, Retired = (Vec<Terminal>, ChildFailures)> + Send + 'static,
        Actor::Settlements: ClassifySettlement + Send,
        RootOrigin<Root>: Send + 'static,
        Terminal: Send + 'static,
        ChildFailures: Send + 'static,
    {
        let executor = match Handle::try_current() {
            Ok(executor) => executor,
            Err(error) => return Err((self, error)),
        };
        Ok(execute_application_with::<
            Self,
            Root,
            Actor,
            ActorSpace<Root::Protocol>,
            Members::Origins,
            Terminal,
            ChildFailures,
            (),
            (Root, StagingFailure),
            _,
            _,
            _,
            Never,
            fn(Never, ApplicationHandle<Root::Protocol, Actor::Event>) -> Ready<Never>,
            Ready<Never>,
            Never,
            _,
            _,
            _,
        >(
            executor,
            self,
            None,
            async |_| {},
            |application, _work, (), _allocations| {
                let (root, members) = application.into_parts();
                members
                    .compose(root)
                    .map(|actor| (actor, ActorSpace::new(), ()))
            },
            ((), |_executor, (), ()| None),
            |root| root,
        ))
    }

    /// Await genuine absent work with original inputs and joined raw root facts.
    /// `Completed { output: None }` means an actual startup grant was acquired without supplied work.
    /// This comparison selects no terminal projection and constructs no callable value.
    /// The execution remains cold; only polling stages or starts the actor.
    ///
    /// # Errors
    /// Returns the untouched application and actual entered-host error.
    ///
    /// # Panics
    /// Execution propagates staging panics; receiving preserves the actual cleanup publication result.
    pub async fn run<Actor, StagingFailure, Terminal, ChildFailures>(
        self,
    ) -> Result<
        ApplicationOutcome<
            Self,
            Option<Never>,
            Option<Never>,
            (
                RootOrigin<Root>,
                Result<ActorRetirement<Actor, Terminal, ChildFailures>, JoinError>,
            ),
            (Root, StagingFailure),
            (Actor, ActorSpace<Root::Protocol>),
        >,
        (Self, TryCurrentError),
    >
    where
        Members: ComposeApplication<Root, Actor = Actor, StagingFailure = StagingFailure>,
        Actor: BehaviorBase
            + BehaviorSettlements<Protocol = Root::Protocol, Ph = Never>
            + Send
            + 'static,
        Actor::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
        BehaviorMessage<Actor>: Send + 'static,
        Actor::Sends: Send + 'static,
        Actor::Error: Send + 'static,
        Actor::InterpretationCustody: Send + 'static,
        Actor::SourceCustody: Send + 'static,
        <Actor::Birth as BirthMode>::Child: ChildOccurrenceProduct<RuntimeChildBindings<Terminal, Members::Origins>>
            + Send
            + 'static,
        ChildBindings<Actor, Terminal, Members::Origins>:
            Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
        RootInterpreter<Actor, ActorSpace<Root::Protocol>, Terminal, Members::Origins>:
            CommitActions<Actor, Retired = (Vec<Terminal>, ChildFailures)> + Send + 'static,
        Actor::Settlements: ClassifySettlement + Send,
        RootOrigin<Root>: Send + 'static,
        Terminal: Send + 'static,
        ChildFailures: Send + 'static,
    {
        let (execution, result) =
            self.execute::<Actor, StagingFailure, Terminal, ChildFailures>()?;
        execution.await;
        Ok(result.await)
    }

    /// Drive this same cold application future on one configured owned Tokio host.
    /// Configure and enable the required drivers on the supplied Builder first.
    /// Neither scheduler nor worker count is substituted or defaulted here.
    ///
    /// The outer error returns the untouched declaration and the same Builder.
    /// `RunError::BlockingInEnteredRuntime` rejects an entered Tokio runtime;
    /// `RunError::Runtime` retains the actual construction error. Neither starts an actor.
    /// The outer success retains the existing checked async result unchanged.
    ///
    /// # Errors
    /// Returns original inputs before construction when called inside Tokio;
    /// construction failure preserves the actual error and retryable inputs.
    /// Async execution retains its complete original result and entered-host error.
    ///
    /// # Panics
    /// Invalid Builder configuration may have panicked before this call. Native
    /// declaration/setup panics propagate as for run; receiver surrender and host
    /// destruction are not a promise that async cleanup completed.
    #[expect(
        clippy::type_complexity,
        reason = "the existing async result and exact cold Builder/input refusal remain independent"
    )]
    #[expect(
        clippy::result_large_err,
        reason = "return the actual configured Builder and declaration without boxing originals"
    )]
    pub fn run_blocking<Actor, StagingFailure, Terminal, ChildFailures>(
        self,
        mut builder: Builder,
    ) -> Result<
        Result<
            ApplicationOutcome<
                Self,
                Option<Never>,
                Option<Never>,
                (
                    RootOrigin<Root>,
                    Result<ActorRetirement<Actor, Terminal, ChildFailures>, JoinError>,
                ),
                (Root, StagingFailure),
                (Actor, ActorSpace<Root::Protocol>),
            >,
            (Self, TryCurrentError),
        >,
        (Self, Builder, RunError),
    >
    where
        Members: ComposeApplication<Root, Actor = Actor, StagingFailure = StagingFailure>,
        Actor: BehaviorBase
            + BehaviorSettlements<Protocol = Root::Protocol, Ph = Never>
            + Send
            + 'static,
        Actor::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
        BehaviorMessage<Actor>: Send + 'static,
        Actor::Sends: Send + 'static,
        Actor::Error: Send + 'static,
        Actor::InterpretationCustody: Send + 'static,
        Actor::SourceCustody: Send + 'static,
        <Actor::Birth as BirthMode>::Child: ChildOccurrenceProduct<RuntimeChildBindings<Terminal, Members::Origins>>
            + Send
            + 'static,
        ChildBindings<Actor, Terminal, Members::Origins>:
            Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
        RootInterpreter<Actor, ActorSpace<Root::Protocol>, Terminal, Members::Origins>:
            CommitActions<Actor, Retired = (Vec<Terminal>, ChildFailures)> + Send + 'static,
        Actor::Settlements: ClassifySettlement + Send,
        RootOrigin<Root>: Send + 'static,
        Terminal: Send + 'static,
        ChildFailures: Send + 'static,
    {
        if let Ok(entered_executor) = Handle::try_current() {
            drop(entered_executor);
            return Err((self, builder, RunError::BlockingInEnteredRuntime));
        }
        let application_host = match builder.build() {
            Ok(application_host) => application_host,
            Err(source) => return Err((self, builder, RunError::Runtime(source))),
        };
        Ok(application_host.block_on(self.run::<Actor, StagingFailure, Terminal, ChildFailures>()))
    }

    /// Await declared composition, caller-local work and the exact joined result.
    ///
    /// This future is cold until polled. Bare original work survives actual staging
    /// refusal; supplied work and completed output occupy their actual Some axes.
    /// Use `execute_with` to retain the result receiver independently of execution.
    ///
    /// # Errors
    /// Returns the untouched declaration and work with the actual entered-host error.
    ///
    /// # Panics
    /// Setup and work may unwind; the actual cause remains with the caller.
    pub async fn run_with<
        Terminal,
        Actor,
        ChildFailures,
        StagingFailure,
        Work,
        WorkFuture,
        Output,
    >(
        self,
        work: Work,
    ) -> Result<
        ApplicationOutcome<
            Self,
            Option<Work>,
            Option<Output>,
            (
                RootOrigin<Root>,
                Result<ActorRetirement<Actor, Terminal, ChildFailures>, JoinError>,
            ),
            (Root, Work, StagingFailure),
            (Actor, ActorSpace<Root::Protocol>),
        >,
        (Self, Work, TryCurrentError),
    >
    where
        Members: ComposeApplication<Root, Actor = Actor, StagingFailure = StagingFailure>,
        Actor: BehaviorBase
            + BehaviorSettlements<Protocol = Root::Protocol, Ph = Never>
            + Send
            + 'static,
        Actor::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
        BehaviorMessage<Actor>: Send + 'static,
        Actor::Sends: Send + 'static,
        Actor::Error: Send + 'static,
        Actor::InterpretationCustody: Send + 'static,
        Actor::SourceCustody: Send + 'static,
        <Actor::Birth as BirthMode>::Child: ChildOccurrenceProduct<RuntimeChildBindings<Terminal, Members::Origins>>
            + Send
            + 'static,
        ChildBindings<Actor, Terminal, Members::Origins>:
            Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
        RootInterpreter<Actor, ActorSpace<Root::Protocol>, Terminal, Members::Origins>:
            CommitActions<Actor, Retired = (Vec<Terminal>, ChildFailures)> + Send + 'static,
        Actor::Settlements: ClassifySettlement + Send,
        RootOrigin<Root>: Send + 'static,
        Terminal: Send + 'static,
        ChildFailures: Send + 'static,
        Work: FnOnce(ApplicationHandle<Root::Protocol, Actor::Event>) -> WorkFuture,
        WorkFuture: Future<Output = Output>,
    {
        let (execution, result) = self.execute_with::<
            Actor,
            StagingFailure,
            Terminal,
            ChildFailures,
            Work,
            WorkFuture,
            Output,
        >(work)?;
        execution.await;
        Ok(result.await)
    }

    #[cfg(feature = "axum")]
    /// Pair prebinding and caller-local HTTP serving with the same application owner.
    /// Bind refusal retains the untouched declaration/router/address/error before
    /// composition. Staging refusal retains actual remaining root/declarations,
    /// router and acquired listener. Each phase is explicit in the existing result.
    /// No root terminal conversion runs inside serving or the execution kernel.
    ///
    /// # Errors
    /// Missing host returns untouched cold inputs. `StagingRejected` inputs contain
    /// Ok(original bind-refusal inputs) or Err(actual partial composition inputs).
    /// Serving returns its exact `io::Error` beside the independently joined root.
    ///
    /// # Panics
    /// Native consuming composition or router causes remain caller-owned. The
    /// separately retained result receiver preserves every still-owned fact.
    #[expect(
        clippy::type_complexity,
        reason = "retain original prebind and partial composition inputs beside exact HTTP/root results"
    )]
    pub fn execute_axum<Terminal, Actor, StagingFailure, ChildFailures, Router>(
        self,
        address: SocketAddr,
        router: Router,
    ) -> Result<
        (
            impl Future<Output = ()>,
            impl Future<
                Output = ApplicationOutcome<
                    (Self, Router, SocketAddr),
                    Option<(Router, TcpListener)>,
                    Option<Result<(), io::Error>>,
                    (
                        RootOrigin<Root>,
                        Result<ActorRetirement<Actor, Terminal, ChildFailures>, JoinError>,
                    ),
                    Result<
                        (Self, Router, SocketAddr, io::Error),
                        (Root, Router, StagingFailure, TcpListener),
                    >,
                    (Actor, ActorSpace<Root::Protocol>),
                >,
            >,
        ),
        ((Self, Router, SocketAddr), TryCurrentError),
    >
    where
        Members: ComposeApplication<Root, Actor = Actor, StagingFailure = StagingFailure>,
        Actor: BehaviorBase
            + BehaviorSettlements<Protocol = Root::Protocol, Ph = Never>
            + Send
            + 'static,
        Actor::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
        BehaviorMessage<Actor>: Send + 'static,
        Actor::Sends: Send + 'static,
        Actor::Error: Send + 'static,
        Actor::InterpretationCustody: Send + 'static,
        Actor::SourceCustody: Send + 'static,
        <Actor::Birth as BirthMode>::Child: ChildOccurrenceProduct<RuntimeChildBindings<Terminal, Members::Origins>>
            + Send
            + 'static,
        ChildBindings<Actor, Terminal, Members::Origins>:
            Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
        RootInterpreter<Actor, ActorSpace<Root::Protocol>, Terminal, Members::Origins>:
            CommitActions<Actor, Retired = (Vec<Terminal>, ChildFailures)> + Send + 'static,
        Actor::Settlements: ClassifySettlement + Send,
        RootOrigin<Root>: Send + 'static,
        Terminal: Send + 'static,
        ChildFailures: Send + 'static,
        Router: FnOnce(ApplicationHandle<Root::Protocol, Actor::Event>) -> axum::Router,
    {
        let executor = match Handle::try_current() {
            Ok(executor) => executor,
            Err(error) => return Err(((self, router, address), error)),
        };
        Ok(execute_application_with::<
            _,
            Root,
            Actor,
            ActorSpace<Root::Protocol>,
            Members::Origins,
            Terminal,
            ChildFailures,
            (),
            Result<
                (Self, Router, SocketAddr, io::Error),
                (Root, Router, StagingFailure, TcpListener),
            >,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
        >(
            executor,
            (self, router, address),
            None,
            async |(_, _, address): &(Self, Router, SocketAddr)| TcpListener::bind(*address).await,
            |(application, router, address), work, listener, _allocations| {
                let listener = match listener {
                    Ok(listener) => listener,
                    Err(error) => return Err(Ok((application, router, address, error))),
                };
                // Guard already-acquired HTTP inputs before the consuming user composition cut.
                *work = Some(((router, listener), async |(router, listener): (Router, TcpListener), application: ApplicationHandle<Root::Protocol, Actor::Event>| {
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
                    serve
                }));
                let (root, members) = application.into_parts();
                let actor = match members.compose(root) {
                    Ok(actor) => actor,
                    Err((root, failure)) => {
                        let Some(((router, listener), _invoke)) = work.take() else {
                            unreachable!(
                                "the single preparation owner retains its acquired HTTP work before composition"
                            );
                        };
                        return Err(Err((root, router, failure, listener)));
                    }
                };
                Ok((actor, ActorSpace::new(), ()))
            },
            ((), |_executor, (), ()| None),
            |root| root,
        ))
    }

    #[cfg(feature = "axum")]
    /// Await the same prebind/serving/root pair. Use `execute_axum` to retain receiving separately.
    ///
    /// # Errors
    /// Missing host returns cold originals; all actual bind/staging/serving/root
    /// outcomes remain in their explicit independent result fields.
    ///
    /// # Panics
    /// Native composition/serving causes propagate; dropping this combined future
    /// surrenders both owning execution and receiving, unlike a separately retained pair.
    pub async fn run_axum<Terminal, Actor, StagingFailure, ChildFailures, Router>(
        self,
        address: SocketAddr,
        router: Router,
    ) -> Result<
        ApplicationOutcome<
            (Self, Router, SocketAddr),
            Option<(Router, TcpListener)>,
            Option<Result<(), io::Error>>,
            (
                RootOrigin<Root>,
                Result<ActorRetirement<Actor, Terminal, ChildFailures>, JoinError>,
            ),
            Result<
                (Self, Router, SocketAddr, io::Error),
                (Root, Router, StagingFailure, TcpListener),
            >,
            (Actor, ActorSpace<Root::Protocol>),
        >,
        ((Self, Router, SocketAddr), TryCurrentError),
    >
    where
        Members: ComposeApplication<Root, Actor = Actor, StagingFailure = StagingFailure>,
        Actor: BehaviorBase
            + BehaviorSettlements<Protocol = Root::Protocol, Ph = Never>
            + Send
            + 'static,
        Actor::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
        BehaviorMessage<Actor>: Send + 'static,
        Actor::Sends: Send + 'static,
        Actor::Error: Send + 'static,
        Actor::InterpretationCustody: Send + 'static,
        Actor::SourceCustody: Send + 'static,
        <Actor::Birth as BirthMode>::Child: ChildOccurrenceProduct<RuntimeChildBindings<Terminal, Members::Origins>>
            + Send
            + 'static,
        ChildBindings<Actor, Terminal, Members::Origins>:
            Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
        RootInterpreter<Actor, ActorSpace<Root::Protocol>, Terminal, Members::Origins>:
            CommitActions<Actor, Retired = (Vec<Terminal>, ChildFailures)> + Send + 'static,
        Actor::Settlements: ClassifySettlement + Send,
        RootOrigin<Root>: Send + 'static,
        Terminal: Send + 'static,
        ChildFailures: Send + 'static,
        Router: FnOnce(ApplicationHandle<Root::Protocol, Actor::Event>) -> axum::Router,
    {
        let (execution, receiving) = self
            .execute_axum::<Terminal, Actor, StagingFailure, ChildFailures, Router>(
                address, router,
            )?;
        execution.await;
        Ok(receiving.await)
    }
}

pub(crate) trait ChildOriginAt<Position, Child: Behavior> {
    type Origin: Send + 'static;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin;
}

impl<Owner: 'static, Position: 'static, Child: Behavior> ChildOriginAt<Position, Child>
    for StructuralOrigins<Owner>
{
    type Origin = ChildOrigin<Owner, Position>;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin {
        ChildOrigin::new(address, nonce)
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

trait AppendedChildOriginAt<Target, Absolute, Root, Child: Behavior> {
    type Origin: Send + 'static;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin;
}

trait DeclaredChildOriginAt<Target, Root, Child: Behavior> {
    type Origin: Send + 'static;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin;
}

impl<Members, Target, Absolute, Root, Child: Behavior>
    AppendedChildOriginAt<Target, Absolute, Root, Child>
    for AppendedOrigins<NoRootTerminalOrigins, Members>
where
    Members: DeclaredChildOriginAt<Target, Root, Child>,
{
    type Origin = Members::Origin;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin {
        Members::origin(address, nonce)
    }
}

impl<RootChild: Behavior, Rest, Members, Absolute: 'static, Root>
    AppendedChildOriginAt<ChildHead, Absolute, Root, RootChild>
    for AppendedOrigins<RootTerminalOrigin<RootChild, Rest>, Members>
where
    Root: BehaviorBase,
    Root::Base: 'static,
{
    type Origin = ChildOrigin<Root::Base, Absolute>;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin {
        ChildOrigin::new(address, nonce)
    }
}

impl<RootChild: Behavior, Rest, Members, Relative, Absolute, Root, Child: Behavior>
    AppendedChildOriginAt<ChildTail<Relative>, Absolute, Root, Child>
    for AppendedOrigins<RootTerminalOrigin<RootChild, Rest>, Members>
where
    AppendedOrigins<Rest, Members>: AppendedChildOriginAt<Relative, Absolute, Root, Child>,
{
    type Origin = <AppendedOrigins<Rest, Members> as AppendedChildOriginAt<
        Relative,
        Absolute,
        Root,
        Child,
    >>::Origin;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin {
        <AppendedOrigins<Rest, Members> as
            AppendedChildOriginAt<Relative, Absolute, Root, Child>>::origin(address, nonce)
    }
}

impl<Role: 'static, Child: Behavior, Tail, Root: 'static>
    DeclaredChildOriginAt<ChildHead, Root, Child> for (Role, Child, Tail)
{
    type Origin = ChildOrigin<Root, Role>;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin {
        ChildOrigin::new(address, nonce)
    }
}

impl<Role, Declared: Behavior, Tail, Relative, Root, Child: Behavior>
    DeclaredChildOriginAt<ChildTail<Relative>, Root, Child> for (Role, Declared, Tail)
where
    Tail: DeclaredChildOriginAt<Relative, Root, Child>,
{
    type Origin = Tail::Origin;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin {
        Tail::origin(address, nonce)
    }
}

impl<Root, Members, Position, Child: Behavior> ChildOriginAt<Position, Child>
    for ApplicationOrigins<Root, Members>
where
    Root: Behavior + BehaviorBase,
    RootBirthNode<Root>: ChildOccurrenceProduct<RootTerminalOriginMapper>,
    AppendedOrigins<RootOriginProduct<Root>, Members>:
        AppendedChildOriginAt<Position, Position, Root, Child>,
{
    type Origin = <AppendedOrigins<RootOriginProduct<Root>, Members> as AppendedChildOriginAt<
        Position,
        Position,
        Root,
        Child,
    >>::Origin;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin {
        <AppendedOrigins<RootOriginProduct<Root>, Members> as AppendedChildOriginAt<
            Position,
            Position,
            Root,
            Child,
        >>::origin(address, nonce)
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
    child_bindings: Option<Bindings>,
    activation_tasks: Option<ActivationTasks<C::Event>>,
    #[expect(
        clippy::type_complexity,
        reason = "Keep admission ownership, exact identity, and revocation in the same existing map."
    )]
    exact_observations:
        Arc<Mutex<Option<HashMap<ObservationId, (Arc<ObservationId>, oneshot::Sender<()>)>>>>,
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
            child_bindings: Some(child_bindings),
            activation_tasks: Some(ActivationTasks::new()),
            exact_observations: Arc::new(Mutex::new(Some(HashMap::new()))),
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <Creations<CreateChild<MailAddr, New>> as ActionItem>::Input<'a>,
        received: &'a mut Option<<Creations<CreateChild<MailAddr, New>> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        Creations<CreateChild<MailAddr, New>>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(creations) = input.take() else {
                return;
            };
            let settlement = 'settlement: {
                let Ok(count) = u64::try_from(creations.len()) else {
                    break 'settlement ItemSettlement::Rejected {
                        item: creations,
                        reason: ChildNamespaceExhausted,
                    };
                };
                let Some(next) = self.next_child_route.checked_add(count) else {
                    break 'settlement ItemSettlement::Rejected {
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
            };
            *received = Some(settlement);
        }
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn admit_source(
        &mut self,
        input: &mut Option<Input>,
        received: &mut Option<Result<(), Input>>,
    ) -> impl Future<Output = ()> + Send {
        async move {
            if received.is_some() {
                return;
            }
            let Some(input) = input.take() else {
                return;
            };
            let event = C::Event::ingress(input);
            match self.control.send(event) {
                Ok(()) => *received = Some(Ok(())),
                Err(ControlClosed(_)) => {
                    unreachable!("the actor control lane remains live while its effects commit")
                }
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
    Bindings: ChildBindingAt<Position, Child = Child, Root = BindingTerminal<Bindings>, Origins = Origins>
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
    Child::InterpretationCustody: Send + 'static,
    Child::SourceCustody: Send + 'static,
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
    >: CommitActions<
            Child,
            Retired = (
                Vec<BindingTerminal<Bindings>>,
                <NestedBindings<Bindings, Child> as RetireChildTasks>::Failures,
            ),
        > + Send
        + 'static,
    Origins: ChildOriginAt<Position, Child>,
    BindingTerminal<Bindings>: ProjectTerminal<
            Origins::Origin,
            ActorRetirement<
                Child,
                BindingTerminal<Bindings>,
                <NestedBindings<Bindings, Child> as RetireChildTasks>::Failures,
            >,
        >,
    <NestedBindings<Bindings, Child> as RetireChildTasks>::Failures: Send + 'static,
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
                    .as_mut()
                    .expect("live child bindings remain installed")
                    .record_creation(id, CreationBinding::Rejected);
                return ItemSettlement::Rejected {
                    item: routed_creation(id, kind, route, child),
                    reason: CreationRejection::Allocation(reason),
                };
            }
        };
        let actors = self
            .child_bindings
            .as_ref()
            .expect("live child bindings remain installed")
            .child_actors();
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
                let task = ProjectedTask::project(installed.task, Origins::origin(address, route));
                self.child_bindings
                    .as_mut()
                    .expect("live child bindings remain installed")
                    .record_creation(
                        id,
                        CreationBinding::Established {
                            kind,
                            route,
                            endpoint: endpoint.clone(),
                            control: installed.control,
                            task: Some(task),
                            joined: None,
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
                    .as_mut()
                    .expect("live child bindings remain installed")
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
                descendants: (descendants, child_failures),
                capability_failures,
                additional_failures,
                terminal_report,
                retirement_failures,
                received_interpretation: None,
                received_source: None,
                source_index: None,
                acquired_ingress: None,
                unread_owner_cancellation,
            }) => {
                assert!(capability_failures.is_empty());
                assert!(unread_owner_cancellation.is_none());
                assert!(control.is_empty());
                assert!(user.is_empty());
                assert!(descendants.is_empty());
                // This standard child has not crossed its binding ACK, so its freshly
                // constructed nested bindings have interpreted no initialization effects.
                drop(child_failures);
                self.child_bindings
                    .as_mut()
                    .expect("live child bindings remain installed")
                    .record_creation(
                        id,
                        CreationBinding::StartupRejected {
                            kind,
                            address,
                            route,
                            primary_failure: None,
                            additional_failures,
                            terminal_report,
                            retirement_failures,
                        },
                    );
                ItemSettlement::Accepted(ChildCreationOutcome::InitializationRejected {
                    creation: routed_creation(id, kind, route, behavior),
                    error,
                })
            }
            Err(SpawnError::InitializationPanicked {
                behavior,
                payload,
                control,
                user,
                descendants: (descendants, child_failures),
                capability_failures,
                additional_failures,
                terminal_report,
                retirement_failures,
                received_interpretation: None,
                received_source: None,
                source_index: None,
                acquired_ingress: None,
                unread_owner_cancellation,
            }) => {
                assert!(capability_failures.is_empty());
                assert!(unread_owner_cancellation.is_none());
                assert!(control.is_empty());
                assert!(user.is_empty());
                assert!(descendants.is_empty());
                // This standard child has not crossed its binding ACK, so its freshly
                // constructed nested bindings have interpreted no initialization effects.
                drop(child_failures);
                self.child_bindings
                    .as_mut()
                    .expect("live child bindings remain installed")
                    .record_creation(
                        id,
                        CreationBinding::StartupRejected {
                            kind,
                            address,
                            route,
                            primary_failure: Some(DriverError::InitializationPanicked(payload)),
                            additional_failures,
                            terminal_report,
                            retirement_failures,
                        },
                    );
                ItemSettlement::Accepted(ChildCreationOutcome::InitializationPanicked {
                    creation: routed_creation(id, kind, route, behavior),
                })
            }
            Err(SpawnError::HostRejected {
                behavior,
                initialization: InterpretationProgress::Original(initialization),
                error,
                control,
                user,
                descendants: (descendants, child_failures),
                capability_failures,
                additional_failures,
                terminal_report,
                retirement_failures,
                received_interpretation: None,
                received_source: None,
                source_index: None,
                acquired_ingress: None,
                unread_owner_cancellation,
            }) => {
                assert!(capability_failures.is_empty());
                assert!(unread_owner_cancellation.is_none());
                assert!(control.is_empty());
                assert!(user.is_empty());
                assert!(descendants.is_empty());
                // This standard child has not crossed its binding ACK, so its freshly
                // constructed nested bindings have interpreted no initialization effects.
                drop(child_failures);
                let reason = match &error {
                    ClaimError::AddressInUse(_) => CreationRejection::Allocation(
                        behavior::AllocationRejection::AddressAlreadyClaimed,
                    ),
                    ClaimError::RegistrationIdsExhausted(_) => CreationRejection::EnvironmentFailed,
                };
                self.child_bindings
                    .as_mut()
                    .expect("live child bindings remain installed")
                    .record_creation(
                        id,
                        CreationBinding::StartupRejected {
                            kind,
                            address,
                            route,
                            primary_failure: Some(DriverError::Activation(
                                LocalActivationRejection::Address(error),
                            )),
                            additional_failures,
                            terminal_report,
                            retirement_failures,
                        },
                    );
                ItemSettlement::Accepted(ChildCreationOutcome::HostRejected {
                    creation: routed_creation(id, kind, route, behavior),
                    initialization,
                    reason,
                })
            }
            Err(SpawnError::BindingAbandoned {
                behavior,
                initialization: InterpretationProgress::Original(initialization),
                control,
                user,
                descendants: (descendants, child_failures),
                capability_failures,
                additional_failures,
                terminal_report,
                retirement_failures,
                received_interpretation: None,
                received_source: None,
                source_index: None,
                acquired_ingress: None,
                unread_owner_cancellation,
            }) => {
                assert!(capability_failures.is_empty());
                assert!(unread_owner_cancellation.is_none());
                assert!(control.is_empty());
                assert!(user.is_empty());
                assert!(descendants.is_empty());
                // This standard child has not crossed its binding ACK, so its freshly
                // constructed nested bindings have interpreted no initialization effects.
                drop(child_failures);
                self.child_bindings
                    .as_mut()
                    .expect("live child bindings remain installed")
                    .record_creation(
                        id,
                        CreationBinding::StartupRejected {
                            kind,
                            address,
                            route,
                            primary_failure: Some(DriverError::Activation(
                                LocalActivationRejection::BindingAbandoned,
                            )),
                            additional_failures,
                            terminal_report,
                            retirement_failures,
                        },
                    );
                ItemSettlement::Accepted(ChildCreationOutcome::HostRejected {
                    creation: routed_creation(id, kind, route, behavior),
                    initialization,
                    reason: CreationRejection::EnvironmentFailed,
                })
            }
            Err(SpawnError::Unpublished(ActorExecutionOutcome::ActivationPanicked {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization: InterpretationProgress::Original(initialization),
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation: None,
                        received_source: None,
                        source_index: None,
                        acquired_ingress: None,
                        unread_owner_cancellation,
                    },
                additional_failures,
                payload,
            })) => {
                // This branch is reached only after the real private committed
                // receiver closed. No ACK or initialization interpreter ran.
                assert!(activation_tasks.is_empty());
                assert!(capability_failures.is_empty());
                assert!(unread_owner_cancellation.is_none());
                assert!(ingress.control.is_empty());
                assert!(ingress.user.is_empty());
                assert!(descendants.is_empty());
                drop(activation_tasks);
                // The exact freshly defaulted nested product has no creation
                // occurrence; its failure lanes have no admitted child rows.
                drop(child_failures);
                self.child_bindings
                    .as_mut()
                    .expect("live child bindings remain installed")
                    .record_creation(
                        id,
                        CreationBinding::StartupRejected {
                            kind,
                            address,
                            route,
                            primary_failure: Some(DriverError::ActivationPanicked(payload)),
                            additional_failures,
                            terminal_report,
                            retirement_failures,
                        },
                    );
                ItemSettlement::Accepted(ChildCreationOutcome::HostRejected {
                    creation: routed_creation(id, kind, route, behavior),
                    initialization,
                    reason: CreationRejection::EnvironmentFailed,
                })
            }
            Err(unexpected_startup) => {
                // The concrete private commitment producer cannot reach an
                // active phase without returning its endpoint and ACK sender.
                // Only the enumerated Prepared/Original startup phases above
                // can be returned before that successful transfer. Changing
                // this standard producer requires revisiting this invariant.
                panic!(
                    "the standard private child returned an invalid startup phase: {unexpected_startup:?}"
                );
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
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <Delivery<Target> as ActionItem>::Input<'a>,
        received: &'a mut Option<<Delivery<Target> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        Delivery<Target>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(delivery) = input.take() else {
                return;
            };
            let settlement = 'settlement: {
                let address = delivery.to.address();
                let Some(actor) = self.actor_spaces.resolve_logical(address) else {
                    break 'settlement ItemSettlement::Rejected {
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
            };
            *received = Some(settlement);
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
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <EstablishedDelivery<Target> as ActionItem>::Input<'a>,
        received: &'a mut Option<<EstablishedDelivery<Target> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        EstablishedDelivery<Target>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(delivery) = input.take() else {
                return;
            };
            let settlement = {
                let EstablishedDelivery { to, message } = delivery;
                let endpoint = to.clone().interpret(&mut ExtractLocalEndpoint);
                match endpoint.send_from(self.address, message).await {
                    Ok(()) => ItemSettlement::Accepted(()),
                    Err(error) => ItemSettlement::Rejected {
                        item: EstablishedDelivery::new(to, error.into_message()),
                        reason: ExactDeliveryReason::ClosedRecipient,
                    },
                }
            };
            *received = Some(settlement);
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
    #[expect(
        clippy::too_many_lines,
        reason = "one complete customer-delivery interpretation preserves all original source items, refused payloads and acquired typed replies across the owning lane"
    )]
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <CustomerDelivery<Target> as ActionItem>::Input<'a>,
        received: &'a mut Option<<CustomerDelivery<Target> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        CustomerDelivery<Target>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(customer_delivery) = input.take() else {
                return;
            };
            match customer_delivery {
                CustomerDelivery::Logical { delivery } => {
                    let mut delivery = Some(delivery);
                    let mut delivery_received = None;
                    <Self as InterpretItem<Delivery<Target>, RootEvent, Path>>::interpret_item(
                        self,
                        &mut delivery,
                        &mut delivery_received,
                    )
                    .await;
                    match delivery_received {
                        Some(settlement) => {
                            *received = Some(reunite_customer_delivery(
                                settlement,
                                |delivery| CustomerDelivery::Logical { delivery },
                                ReplyDelivery::Logical,
                            ));
                        }
                        None => {
                            if let Some(delivery) = delivery {
                                *input = Some(CustomerDelivery::Logical { delivery });
                            }
                        }
                    }
                }
                CustomerDelivery::Established { delivery } => {
                    let mut delivery = Some(delivery);
                    let mut delivery_received = None;
                    <Self as InterpretItem<EstablishedDelivery<Target>, RootEvent, Path>>::interpret_item(
                        self, &mut delivery, &mut delivery_received,
                    ).await;
                    match delivery_received {
                        Some(settlement) => {
                            *received = Some(reunite_customer_delivery(
                                settlement,
                                |delivery| CustomerDelivery::Established { delivery },
                                ReplyDelivery::Established,
                            ));
                        }
                        None => {
                            if let Some(delivery) = delivery {
                                *input = Some(CustomerDelivery::Established { delivery });
                            }
                        }
                    }
                }
                CustomerDelivery::RejectedLogical { delivery, customer } => {
                    let mut delivery = Some(delivery);
                    let mut delivery_received = None;
                    <Self as InterpretItem<Delivery<Target>, RootEvent, Path>>::interpret_item(
                        self,
                        &mut delivery,
                        &mut delivery_received,
                    )
                    .await;
                    match delivery_received {
                        Some(settlement) => {
                            *received = Some(reunite_customer_delivery(
                                settlement,
                                |delivery| CustomerDelivery::RejectedLogical { delivery, customer },
                                ReplyDelivery::Logical,
                            ));
                        }
                        None => {
                            if let Some(delivery) = delivery {
                                *input =
                                    Some(CustomerDelivery::RejectedLogical { delivery, customer });
                            }
                        }
                    }
                }
                CustomerDelivery::RejectedEstablished { delivery, customer } => {
                    let mut delivery = Some(delivery);
                    let mut delivery_received = None;
                    <Self as InterpretItem<EstablishedDelivery<Target>, RootEvent, Path>>::interpret_item(
                        self, &mut delivery, &mut delivery_received,
                    ).await;
                    match delivery_received {
                        Some(settlement) => {
                            *received = Some(reunite_customer_delivery(
                                settlement,
                                |delivery| CustomerDelivery::RejectedEstablished {
                                    delivery,
                                    customer,
                                },
                                ReplyDelivery::Established,
                            ));
                        }
                        None => {
                            if let Some(delivery) = delivery {
                                *input = Some(CustomerDelivery::RejectedEstablished {
                                    delivery,
                                    customer,
                                });
                            }
                        }
                    }
                }
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
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <DiagnosticAction<Recipient<Target>, Target::Msg> as ActionItem>::Input<'a>,
        received: &'a mut Option<
            <DiagnosticAction<Recipient<Target>, Target::Msg> as ActionItem>::Reply,
        >,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        DiagnosticAction<Recipient<Target>, Target::Msg>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(action) = input.take() else {
                return;
            };
            match action {
                DiagnosticAction::Terminal { diagnostic } => {
                    *received = Some(ItemSettlement::Accepted(DiagnosticAccepted::terminal(
                        diagnostic,
                    )));
                }
                DiagnosticAction::Deliver { route, diagnostic } => {
                    let mut delivery = Some(Delivery::new(route, diagnostic));
                    let mut delivery_received = None;
                    <Self as InterpretItem<Delivery<Target>, RootEvent, Path>>::interpret_item(
                        self,
                        &mut delivery,
                        &mut delivery_received,
                    )
                    .await;
                    if let Some(Delivery { to, message }) = delivery {
                        *input = Some(DiagnosticAction::deliver(to, message));
                    }
                    if let Some(settlement) = delivery_received {
                        *received = Some(match settlement {
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
                        });
                    }
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
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <DiagnosticAction<EstablishedRecipient<Target>, Target::Msg> as ActionItem>::Input<
            'a,
        >,
        received: &'a mut Option<
            <DiagnosticAction<EstablishedRecipient<Target>, Target::Msg> as ActionItem>::Reply,
        >,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        DiagnosticAction<EstablishedRecipient<Target>, Target::Msg>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(action) = input.take() else {
                return;
            };
            match action {
                DiagnosticAction::Terminal { diagnostic } => {
                    *received = Some(ItemSettlement::Accepted(DiagnosticAccepted::terminal(
                        diagnostic,
                    )));
                }
                DiagnosticAction::Deliver { route, diagnostic } => {
                    let mut delivery = Some(EstablishedDelivery::new(route, diagnostic));
                    let mut delivery_received = None;
                    <Self as InterpretItem<EstablishedDelivery<Target>, RootEvent, Path>>::interpret_item(
                        self, &mut delivery, &mut delivery_received,
                    ).await;
                    if let Some(EstablishedDelivery { to, message }) = delivery {
                        *input = Some(DiagnosticAction::deliver(to, message));
                    }
                    if let Some(settlement) = delivery_received {
                        *received = Some(match settlement {
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
                        });
                    }
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <DiagnosticAction<Infallible, Diagnostic> as ActionItem>::Input<'a>,
        received: &'a mut Option<<DiagnosticAction<Infallible, Diagnostic> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        DiagnosticAction<Infallible, Diagnostic>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(action) = input.take() else {
                return;
            };
            let settlement = {
                match action {
                    DiagnosticAction::Deliver { route, .. } => match route {},
                    DiagnosticAction::Terminal { diagnostic } => {
                        ItemSettlement::Accepted(DiagnosticAccepted::terminal(diagnostic))
                    }
                }
            };
            *received = Some(settlement);
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
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ChildDelivery<Target, Occurrence> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ChildDelivery<Target, Occurrence> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ChildDelivery<Target, Occurrence>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(delivery) = input.take() else {
                return;
            };
            let settlement = 'settlement: {
                let actor = match self
                    .child_bindings
                    .as_ref()
                    .expect("live child bindings remain installed")
                    .creation(delivery.creation)
                {
                    Some(CreationBinding::Established { endpoint, .. }) => endpoint.clone(),
                    Some(CreationBinding::Rejected | CreationBinding::StartupRejected { .. }) => {
                        let prerequisite = CreationCorrelation::new(delivery.creation);
                        break 'settlement ItemSettlement::Blocked {
                            item: delivery,
                            prerequisite,
                        };
                    }
                    None => {
                        break 'settlement ItemSettlement::Rejected {
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
            };
            *received = Some(settlement);
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ChildInput<Child, Source, Input, Occurrence> as ActionItem>::Input<'a>,
        received: &'a mut Option<
            <ChildInput<Child, Source, Input, Occurrence> as ActionItem>::Reply,
        >,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ChildInput<Child, Source, Input, Occurrence>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(input) = input.take() else {
                return;
            };
            let settlement = 'settlement: {
                let control = match self
                    .child_bindings
                    .as_ref()
                    .expect("live child bindings remain installed")
                    .creation(input.creation)
                {
                    Some(CreationBinding::Established { control, .. }) => control.clone(),
                    Some(CreationBinding::Rejected | CreationBinding::StartupRejected { .. }) => {
                        let prerequisite = CreationCorrelation::new(input.creation);
                        break 'settlement ItemSettlement::Blocked {
                            item: input,
                            prerequisite,
                        };
                    }
                    None => {
                        break 'settlement ItemSettlement::Rejected {
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
            };
            *received = Some(settlement);
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
        }) = self
            .child_bindings
            .as_ref()
            .expect("live child bindings remain installed")
            .creation(creation)
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ProxyOperation<Here, Worker, Plan> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ProxyOperation<Here, Worker, Plan> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ProxyOperation<Here, Worker, Plan>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let (creation, control) = input;
            let Some(control) = control.take() else {
                return;
            };
            *received = Some(self.admit_proxy_control(creation, control));
        }
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
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <AssignWorker<Target, Job> as ActionItem>::Input<'a>,
        received: &'a mut Option<<AssignWorker<Target, Job> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        AssignWorker<Target, Job>: 'a,
    {
        async move {
            <Self as InterpretItem<EstablishedDelivery<Target>, RootEvent, Path>>::interpret_item(
                self, input, received,
            )
            .await;
        }
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <InitializeWorker<Worker, Plan> as ActionItem>::Input<'a>,
        received: &'a mut Option<<InitializeWorker<Worker, Plan> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        InitializeWorker<Worker, Plan>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
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
            };
            *received = Some(settlement);
        }
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <BeginActivation<Worker, Plan> as ActionItem>::Input<'a>,
        received: &'a mut Option<<BeginActivation<Worker, Plan> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        BeginActivation<Worker, Plan>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.as_ref() else {
                return;
            };
            self.inject_control_event::<_, Path>(request.started());
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                let control = self.control.clone();
                self.activation_tasks
                    .as_mut()
                    .expect("live activation tasks remain installed")
                    .spawn(async move {
                        let event = C::Event::inject_at(request.activate().await);
                        control.send(event).map_err(|ControlClosed(event)| event)
                    });
                ItemSettlement::Accepted(())
            };
            *received = Some(settlement);
        }
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <PrepareWorkers<Source, Role, Worker, Plan> as ActionItem>::Input<'a>,
        received: &'a mut Option<<PrepareWorkers<Source, Role, Worker, Plan> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        PrepareWorkers<Source, Role, Worker, Plan>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                let (receipt, starting) = request.start();
                let control = self.control.clone();
                self.activation_tasks
                    .as_mut()
                    .expect("live activation tasks remain installed")
                    .spawn(async move {
                        let event = C::Event::inject_at(settle_worker_preparation(starting).await);
                        control.send(event).map_err(|ControlClosed(event)| event)
                    });
                ItemSettlement::Accepted(receipt)
            };
            *received = Some(settlement);
        }
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ScheduleAt as ActionItem>::Input<'a>,
        received: &'a mut Option<<ScheduleAt as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ScheduleAt: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
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
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, P, Bindings, Origins, D, Path> InterpretItem<EntityAdmission<D>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    D: EntityDefinition,
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal, D::ChildFailures>,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <EntityAdmission<D> as ActionItem>::Input<'a>,
        received: &'a mut Option<<EntityAdmission<D> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        EntityAdmission<D>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            if input.is_none() {
                return;
            }
            EntityAdmission::interpret(input, self.address).await;
            *received = Some(ItemSettlement::Accepted(()));
        }
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ScheduleAfter as ActionItem>::Input<'a>,
        received: &'a mut Option<<ScheduleAfter as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ScheduleAfter: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
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
            };
            *received = Some(settlement);
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ObserveCreation<ChildProtocol, Occurrence> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ObserveCreation<ChildProtocol, Occurrence> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ObserveCreation<ChildProtocol, Occurrence>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                match self
                    .child_bindings
                    .as_ref()
                    .expect("live child bindings remain installed")
                    .creation(request.creation)
                {
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
                    Some(CreationBinding::Rejected | CreationBinding::StartupRejected { .. }) => {
                        ItemSettlement::Blocked {
                            prerequisite: CreationCorrelation::new(request.creation),
                            item: request,
                        }
                    }
                    None => ItemSettlement::Corrupt {
                        item: request,
                        fault: InterpreterFault::MissingCapability,
                    },
                }
            };
            *received = Some(settlement);
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ObserveEstablishedCreation<Child, Occurrence> as ActionItem>::Input<'a>,
        received: &'a mut Option<
            <ObserveEstablishedCreation<Child, Occurrence> as ActionItem>::Reply,
        >,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ObserveEstablishedCreation<Child, Occurrence>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                match self
                    .child_bindings
                    .as_ref()
                    .expect("live child bindings remain installed")
                    .creation(request.creation)
                {
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
                    Some(CreationBinding::Rejected | CreationBinding::StartupRejected { .. }) => {
                        ItemSettlement::Blocked {
                            prerequisite: CreationCorrelation::new(request.creation),
                            item: request,
                        }
                    }
                    None => ItemSettlement::Corrupt {
                        item: request,
                        fault: InterpreterFault::MissingCapability,
                    },
                }
            };
            *received = Some(settlement);
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ObservePeer<MailAddr> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ObservePeer<MailAddr> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ObservePeer<MailAddr>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
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
            };
            *received = Some(settlement);
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ObserveChild<ChildProtocol, Occurrence> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ObserveChild<ChildProtocol, Occurrence> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ObserveChild<ChildProtocol, Occurrence>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = 'settlement: {
                let child = match self
                    .child_bindings
                    .as_ref()
                    .expect("live child bindings remain installed")
                    .creation(request.child)
                {
                    Some(CreationBinding::Established { endpoint, .. }) => endpoint.clone(),
                    Some(CreationBinding::Rejected | CreationBinding::StartupRejected { .. }) => {
                        break 'settlement ItemSettlement::Blocked {
                            prerequisite: CreationCorrelation::new(request.child),
                            item: request,
                        };
                    }
                    None => {
                        break 'settlement ItemSettlement::Corrupt {
                            item: request,
                            fault: InterpreterFault::MissingCapability,
                        };
                    }
                };
                self.observations
                    .insert_child::<Path>(request.child, child.termination_observation());
                ItemSettlement::Accepted(())
            };
            *received = Some(settlement);
        }
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ShutdownChild<Child, Occurrence> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ShutdownChild<Child, Occurrence> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ShutdownChild<Child, Occurrence>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = 'settlement: {
                let (child, control) = match self
                    .child_bindings
                    .as_ref()
                    .expect("live child bindings remain installed")
                    .creation(request.child)
                {
                    Some(CreationBinding::Established {
                        endpoint, control, ..
                    }) => (endpoint.clone(), control.clone()),
                    Some(CreationBinding::Rejected | CreationBinding::StartupRejected { .. }) => {
                        break 'settlement ItemSettlement::Blocked {
                            prerequisite: CreationCorrelation::new(request.child),
                            item: request,
                        };
                    }
                    None => {
                        break 'settlement ItemSettlement::Corrupt {
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
            };
            *received = Some(settlement);
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ReportToParent<Report> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ReportToParent<Report> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ReportToParent<Report>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                self.parent_reports.report(request.into_inner());
                ItemSettlement::Accepted(())
            };
            *received = Some(settlement);
        }
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ReportTerminalOutcome<MailAddr> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ReportTerminalOutcome<MailAddr> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ReportTerminalOutcome<MailAddr>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                self.terminal_reports.report_outcome(request);
                ItemSettlement::Accepted(())
            };
            *received = Some(settlement);
        }
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ReportShutdownPlan<Plan> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ReportShutdownPlan<Plan> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ReportShutdownPlan<Plan>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                match self.control.send(request.into_event()) {
                    Ok(()) => ItemSettlement::Accepted(()),
                    Err(ControlClosed(_)) => {
                        unreachable!("the actor control lane remains live while its effects commit")
                    }
                }
            };
            *received = Some(settlement);
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

    fn observe(&mut self, request: ObserveEstablished<Target>, endpoint: ActorRef<Target>) {
        let id = request.id();
        let mut observation_admission = self
            .capabilities
            .exact_observations
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Some(observations) = observation_admission.as_mut() else {
            unreachable!("exclusive live capability interpretation precedes capability retirement");
        };
        if observations.contains_key(&id) {
            drop(observation_admission);
            self.capabilities.inject_control_event::<_, Path>(
                EstablishedObservation::observe_rejected(
                    request,
                    ObservationRejection::IdAlreadyBound,
                ),
            );
            return;
        }
        let authority = ObservationAuthority::issued(request);
        let relationship = authority.relationship().clone();
        let (revocation, revoked) = oneshot::channel();
        observations.insert(id, (relationship.identity().clone(), revocation));
        drop(observation_admission);
        // Synchronous exclusive interpretation publishes the committed grant
        // before spawning its observation. No completion may precede Started.
        self.capabilities
            .inject_control_event::<_, Path>(EstablishedObservation::started(authority));
        let notifications = self.capabilities.exact_observations.clone();
        let control = self.capabilities.control.clone();
        let termination = endpoint.termination();
        self.capabilities
            .activation_tasks
            .as_mut()
            .expect("live activation tasks remain installed")
            .spawn(async move {
                tokio::pin!(termination);
                let completed = {
                    let completion = poll_fn(|context| {
                        let mut observation_admission =
                            notifications.lock().unwrap_or_else(PoisonError::into_inner);
                        let Some(observations) = observation_admission.as_mut() else {
                            return Poll::Ready(None);
                        };
                        let registered = observations.get(&id).is_some_and(|(registered, _)| {
                            Arc::ptr_eq(registered, relationship.identity())
                        });
                        if !registered {
                            return Poll::Ready(None);
                        }
                        match termination.as_mut().poll(context) {
                            Poll::Pending => Poll::Pending,
                            Poll::Ready(outcome) => {
                                // The irrevocable completion decision removes its
                                // exact member under cancellation's same guard.
                                observations.remove(&id);
                                Poll::Ready(Some((outcome, Instant::now())))
                            }
                        }
                    });
                    tokio::select! {
                        _ = revoked => None,
                        completed = completion => completed,
                    }
                };
                let Some((outcome, at)) = completed else {
                    return Ok(());
                };
                // The scoped poll closure has released its relationship borrow.
                // Application conversion occurs outside the owner guard. A
                // consuming panic preserves only available outside values and
                // the original task failure, not destroyed application inputs.
                #[cfg(test)]
                atomic_interpretation_contract::hold_acquired_completion(id, at);
                let event = C::Event::inject_at(EstablishedObservation::<Target>::stopped(
                    relationship,
                    outcome,
                    at,
                ));
                let observation_admission =
                    notifications.lock().unwrap_or_else(PoisonError::into_inner);
                if observation_admission.is_none() {
                    return Err(event);
                }
                // Retirement takes this owner before draining control. Admission
                // completes while the same owner still possesses notification.
                control.send(event).map_err(|ControlClosed(event)| event)
            });
    }

    fn cancel(&mut self, request: CancelObservation<Target>) {
        let id = request.id();
        let mut observation_admission = self
            .capabilities
            .exact_observations
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Some(observations) = observation_admission.as_mut() else {
            unreachable!("exclusive live capability interpretation precedes capability retirement");
        };
        let matching = observations
            .get(&id)
            .is_some_and(|(identity, _)| Arc::ptr_eq(identity, request.relationship().identity()));
        let cancellation = if matching {
            observations.remove(&id)
        } else {
            None
        };
        drop(observation_admission);
        match cancellation {
            Some((_, cancellation)) => {
                match cancellation.send(()) {
                    Ok(()) | Err(()) => {}
                }
                self.capabilities.inject_control_event::<_, Path>(
                    EstablishedObservation::<Target>::cancelled(request),
                );
            }
            None => self.capabilities.inject_control_event::<_, Path>(
                EstablishedObservation::cancel_rejected(request, ObservationRejection::NotObserved),
            ),
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ObserveEstablished<Target> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ObserveEstablished<Target> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ObserveEstablished<Target>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                request.interpret(&mut EstablishedObservationInterpreter::<_, Path>::new(self));
                ItemSettlement::Accepted(())
            };
            *received = Some(settlement);
        }
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
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <CancelObservation<Target> as ActionItem>::Input<'a>,
        received: &'a mut Option<<CancelObservation<Target> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        CancelObservation<Target>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                request.interpret(&mut EstablishedObservationInterpreter::<_, Path>::new(self));
                ItemSettlement::Accepted(())
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Child, TargetPath>
    InterpretEstablishedShutdown<Child, TargetPath>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child::Event: InjectEvent<ShutdownRequested, TargetPath>,
{
    fn shutdown(
        &mut self,
        _id: ShutdownId,
        installed: InstalledActor<Child>,
        ingress: behavior::Ingress<ShutdownRequested, TargetPath>,
    ) -> Result<(), ShutdownRejection> {
        installed.request_shutdown(ingress)
    }
}

impl<C, N, P, Bindings, Origins, Child, Path, TargetPath>
    InterpretItem<ShutdownEstablished<Child, TargetPath>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<EstablishedShutdownResolved<Child::Protocol>, Path> + Send + 'static,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child::Event: InjectEvent<ShutdownRequested, TargetPath> + Send,
    BehaviorMessage<Child>: Send,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ShutdownEstablished<Child, TargetPath> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ShutdownEstablished<Child, TargetPath> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ShutdownEstablished<Child, TargetPath>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let shutdown = request.id;
            *received = Some(request.settle(self));
            let resolution = match received.as_ref() {
                Some(ItemSettlement::Accepted(_)) => {
                    EstablishedShutdownResolved::accepted(shutdown)
                }
                Some(ItemSettlement::Rejected { reason, .. }) => {
                    EstablishedShutdownResolved::rejected(shutdown, *reason)
                }
                Some(ItemSettlement::Blocked { prerequisite, .. }) => match *prerequisite {},
                Some(ItemSettlement::Corrupt { .. }) => {
                    unreachable!("exact shutdown settlement cannot create interpreter corruption")
                }
                None => return,
            };
            self.inject_control_event::<_, Path>(resolution);
        }
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
    type Descendants = (Vec<BindingTerminal<Bindings>>, Bindings::Failures);

    async fn next_local_event(&mut self) -> Result<Self::Event, JoinError> {
        tokio::select! {
            biased;
            termination_event = self.observations.next() => Ok(termination_event),
            task = self.activation_tasks.as_mut().expect("live activation tasks remain installed").next_event() => task,
        }
    }

    fn next_deadline(&mut self) -> Option<Instant> {
        self.timers.next_deadline()
    }

    fn pop_due(&mut self, now: Instant) -> Option<Self::Event> {
        self.timers.pop_due(now)
    }

    async fn receive_retirement(
        capabilities: &mut Option<Self>,
        received: &mut Option<CapabilityRetirement<Self::Event, Self::Descendants>>,
    ) {
        let Some(owner) = capabilities.as_mut() else {
            return;
        };
        match (&owner.activation_tasks, received.as_ref()) {
            (Some(_), Some(_)) | (None, None) => return,
            (None, Some(_)) => {}
            (Some(_), None) => {
                let failures = Bindings::retirement_failures();
                let activation_tasks = owner
                    .activation_tasks
                    .take()
                    .expect("the original activation task owner was just observed");
                *received = Some(CapabilityRetirement {
                    activation_tasks,
                    descendants: (Vec::new(), failures),
                    terminal_report: None,
                    retirement_failures: Vec::new(),
                });
            }
        }
        let cancellations = {
            let mut observations = owner
                .exact_observations
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            observations
                .take()
                .into_iter()
                .flat_map(HashMap::into_values)
                .map(|(_, cancel)| cancel)
                .collect::<Vec<_>>()
        };
        for cancellation in cancellations {
            match cancellation.send(()) {
                Ok(()) | Err(()) => {}
            }
        }
        let retirement = received
            .as_mut()
            .expect("acquired capability lanes remain outside retirement");
        Bindings::receive_retirement(
            &mut owner.child_bindings,
            &mut retirement.descendants.0,
            &mut retirement.descendants.1,
        )
        .await;
        if owner.child_bindings.is_some() {
            return;
        }
        // The report transaction has already installed its original send result
        // before its existing invariant panic. Capture that native fact once;
        // retirement_complete below never resends or reconstructs the sender.
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
            owner
                .terminal_reports
                .receive_retirement(&mut retirement.terminal_report);
        })) {
            retirement.retirement_failures.push(payload);
        }
        if !owner.terminal_reports.retirement_complete() {
            return;
        }
        // All original output lanes are acquired before this explicit disposal.
        // A user destructor may destroy its own consumed values; retain the
        // original native cause alongside the actual completed cleanup outputs.
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(capabilities.take()))) {
            retirement.retirement_failures.push(payload);
        }
    }

    #[cfg(test)]
    async fn retire(self) -> CapabilityRetirement<Self::Event, Self::Descendants> {
        let mut capabilities = Some(self);
        let mut received = None;
        Self::receive_retirement(&mut capabilities, &mut received).await;
        assert!(
            capabilities.is_none(),
            "capability retirement retains incomplete original owners"
        );
        received.expect("completed retirement acquires the exact original capability lanes")
    }
}

#[cfg(test)]
mod atomic_interpretation_contract {
    use core::convert::Infallible;
    use core::future::Future;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::{Duration, Instant};

    use behavior::{
        Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, Creations, EstablishedDelivery,
        EstablishedRecipient, EventIngress, EventLayer, Here, InjectEvent, Inside, InterpretItem,
        MessageProtocol, Never, NoBirths, NoSends, Recipient, SendLayer, SourceAdmission, Step,
        User, UserEvent,
    };
    use behavior_actors::atomic::{
        AssignWorker, Assignment, BeginActivation, CustomerDelivery, DiagnosticAction,
        ImmediateActivation, InitializeWorker, ProxyOperation, StableProxy,
    };
    use behavior_actors::{
        CancelObservation, EstablishedObservation, EstablishedTerminationMonitor,
        InterpretEstablishedObservation, ObservationId, ObservationRejection, ObserveEstablished,
        ScheduleAt, ShutdownRequested, StopOnShutdown, TerminationMonitorError, TimerElapsed,
        TimerGeneration, TimerId,
    };
    use bombay_engine::{Completion, Driver};
    use communication::{Config, Received, mailbox_channel};
    use tokio::sync::oneshot;

    use super::{
        ApplicationCapabilities, ApplicationCapabilityInputs, EstablishedObservationInterpreter,
        NoChildBindings, StructuralOrigins,
    };
    use crate::actor;
    use crate::actor_interface::{ActorInterface, ExtractLocalEndpoint};
    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::child_bindings::ChildBindings;
    use crate::interpret::{ActionInterpreter, RetireCapabilities};
    use crate::local::{
        ActivationTasks, CommitActions, LocalEnvironment, LocalResidual, StandardIngress,
    };
    use crate::observation::TerminationObservations;
    use crate::observe;
    use crate::reports::LocalTerminalReports;
    use crate::terminal::{ActorRetirement, LocalOutcome};
    use crate::time::LocalTimers;
    use crate::topology::HostedActorSpaces;
    use crate::{ActorExecutionOutcome, ActorSpace};

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
    type MessageObservationProtocol = MessageProtocol<MailAddr, u64>;

    enum SourceControlEvent {
        SourceInput(u64),
        Observation(EstablishedObservation<ObservationProtocol>),
        MessageObservation(EstablishedObservation<MessageObservationProtocol>),
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

    // This resource belongs to application conversion, outside every fold.
    // Only the dedicated conversion-cut controller installs it.
    static COMPLETION_CONVERSION: Mutex<Option<(oneshot::Sender<Instant>, mpsc::Receiver<()>)>> =
        Mutex::new(None);

    // Runtime test synchronization only, outside every Behavior fold.
    // The same owner gates generic EventLayer and concrete report conversion.
    pub(super) fn hold_acquired_completion(id: ObservationId, at: Instant) {
        if id != ObservationId(71) {
            return;
        }
        let permission = COMPLETION_CONVERSION.lock().unwrap().take();
        if let Some((entered, continue_conversion)) = permission {
            match entered.send(at) {
                Ok(()) | Err(_) => {}
            }
            match continue_conversion.recv() {
                Ok(()) | Err(_) => {}
            }
        }
    }

    impl InjectEvent<EstablishedObservation<ObservationProtocol>, Here> for SourceControlEvent {
        fn inject_at(report: EstablishedObservation<ObservationProtocol>) -> Self {
            Self::Observation(report)
        }
    }

    impl InjectEvent<EstablishedObservation<MessageObservationProtocol>, Inside<Here>>
        for SourceControlEvent
    {
        fn inject_at(report: EstablishedObservation<MessageObservationProtocol>) -> Self {
            Self::MessageObservation(report)
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
                | SourceControlEvent::MessageObservation(_)
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
        let mut source_input = Some(47);
        let mut admitted = None;
        SourceAdmission::<SourceControlEvent, SourceOwner, u64>::admit_source(
            &mut capabilities,
            &mut source_input,
            &mut admitted,
        )
        .await;
        assert_eq!(source_input, None);
        assert_eq!(admitted, Some(Ok(())));

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
        let observation = ObservationId(41);
        let request = ObserveEstablished::new(observation, external.recipient());

        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .observe(request, endpoint);
        let authority = {
            let mut started_event = pin!(receiver.recv());
            let mut context = Context::from_waker(Waker::noop());
            let started = started_event.as_mut().poll(&mut context);
            assert!(matches!(
                &started,
                Poll::Ready(Some(Received::Control(SourceControlEvent::Observation(
                    EstablishedObservation::Started { authority }
                )))) if authority.relationship().id() == observation
            ));
            let Poll::Ready(Some(Received::Control(SourceControlEvent::Observation(
                EstablishedObservation::Started { authority },
            )))) = started
            else {
                panic!("the started control event transfers its registered observation authority");
            };
            authority
        };
        let original_relationship = authority.relationship().clone();
        let registered = {
            let observations = capabilities.exact_observations.lock().unwrap();
            observations
                .as_ref()
                .expect("live observation registrations remain installed")
                .get(&observation)
                .expect("the started control event follows exact registration")
                .0
                .clone()
        };

        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .cancel(CancelObservation::new(authority));
        {
            let mut cancelled_event = pin!(receiver.recv());
            let mut context = Context::from_waker(Waker::noop());
            let cancelled = cancelled_event.as_mut().poll(&mut context);
            assert!(matches!(
                cancelled,
                Poll::Ready(Some(Received::Control(SourceControlEvent::Observation(
                    EstablishedObservation::Cancelled { relationship }
                )))) if relationship.id() == observation && relationship == original_relationship
            ));
        }
        let retirement = RetireCapabilities::retire(capabilities).await;
        let (returned, failures) = retirement.activation_tasks.settle().await;
        assert!(Arc::ptr_eq(&registered, original_relationship.identity()));
        assert!(returned.is_empty());
        assert!(failures.is_empty());
        let (descendants, ()) = retirement.descendants;
        assert_eq!(descendants, []);
        assert!(retirement.terminal_report.is_none());
        assert!(retirement.retirement_failures.is_empty());
        drop(external);
        drop(mailbox_owner);
        drop(mailbox_ref);
        drop(terminal_receiver);
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete deterministic controller, joined cleanup, and whole typed custody oracles together."
    )]
    async fn exact_observation_cancel_wins_before_completion_and_rejects_stale_reused_id() {
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
        let old = interface
            .external::<ObservationProtocol>()
            .expect("original target");
        let endpoint = old.recipient().interpret(&mut ExtractLocalEndpoint);
        let id = ObservationId(41);
        let original = ObserveEstablished::new(id, old.recipient());
        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .observe(original, endpoint);
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Started { authority },
        ))) = receiver.recv().await
        else {
            panic!("Started transfers the original committed grant first");
        };
        let original_relationship = authority.relationship().clone();
        let registered = {
            let current = capabilities.exact_observations.lock().unwrap();
            current
                .as_ref()
                .expect("live owner")
                .get(&id)
                .expect("committed before Started")
                .0
                .clone()
        };
        // Completion wins this first relationship while its affine grant stays
        // outside the runtime. This makes a genuinely stale in-flight cancel.
        drop(old);
        let original_stopped = receiver.recv().await;
        let replacement = interface
            .external::<ObservationProtocol>()
            .expect("replacement target");
        let replacement_endpoint = replacement.recipient().interpret(&mut ExtractLocalEndpoint);
        let new_request = ObserveEstablished::new(id, replacement.recipient());
        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .observe(new_request, replacement_endpoint);
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Started {
                authority: replacement_authority,
            },
        ))) = receiver.recv().await
        else {
            panic!("numeric reuse has a fresh accepted identity");
        };
        let replacement_relationship = replacement_authority.relationship().clone();
        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .cancel(CancelObservation::new(authority));
        // Collect the complete reply even when the exact-member omission
        // mutant accepts this stale grant. No custody oracle runs yet.
        let stale_response = receiver.recv().await;
        let replacement_after_stale = {
            let current = capabilities.exact_observations.lock().unwrap();
            current.as_ref().and_then(|observations| {
                observations.get(&id).map(|(identity, _)| identity.clone())
            })
        };
        // Both real cancellation interpretations synchronously return one
        // reply, including NotObserved in the stale-accepted counterfactual.
        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .cancel(CancelObservation::new(replacement_authority));
        let replacement_response = receiver.recv().await;
        drop(replacement);
        let retirement = RetireCapabilities::retire(capabilities).await;
        let (returned, failures) = retirement.activation_tasks.settle().await;
        let replay = {
            let mut next = pin!(receiver.recv());
            let mut context = Context::from_waker(Waker::noop());
            next.as_mut().poll(&mut context)
        };
        // The target is released and every observation task has joined before
        // the exact-member omission can fail any authoritative oracle.
        assert_eq!(returned.len(), 0);
        assert_eq!(failures.len(), 0);
        assert!(retirement.terminal_report.is_none());
        assert!(retirement.retirement_failures.is_empty());
        let (descendants, ()) = &retirement.descendants;
        assert_eq!(descendants.len(), 0);
        assert!(Arc::ptr_eq(&registered, original_relationship.identity()));
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Stopped {
                relationship,
                outcome,
                at,
            },
        ))) = original_stopped
        else {
            panic!("actual captured future preserves original termination");
        };
        assert_eq!(relationship, original_relationship);
        assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
        assert_ne!(replacement_relationship, original_relationship);
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::CancelRejected { request, reason },
        ))) = stale_response
        else {
            panic!("stale exact grant returns whole rejected request");
        };
        assert_eq!(reason, ObservationRejection::NotObserved);
        assert_eq!(request.relationship(), &original_relationship);
        let Some(replacement_after_stale) = replacement_after_stale else {
            panic!("stale request cannot remove replacement membership");
        };
        assert!(Arc::ptr_eq(
            &replacement_after_stale,
            replacement_relationship.identity()
        ));
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Cancelled {
                relationship: cancelled,
            },
        ))) = replacement_response
        else {
            panic!("only the fresh grant cancels the replacement");
        };
        assert_eq!(cancelled, replacement_relationship);
        assert!(replay.is_pending());
        drop((
            request,
            original_relationship,
            relationship,
            at,
            mailbox_owner,
            mailbox_ref,
            terminal_receiver,
        ));
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete deterministic controller, joined cleanup, and whole typed custody oracles together."
    )]
    async fn exact_observation_duplicate_rejects_whole_original_and_consumers_remain_independent() {
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
        let target = interface
            .external::<ObservationProtocol>()
            .expect("actual target");
        let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
        for id in [ObservationId(51), ObservationId(52)] {
            let request = ObserveEstablished::new(id, target.recipient());
            EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
                .observe(request, endpoint.clone());
        }
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Started { authority: first },
        ))) = receiver.recv().await
        else {
            panic!("first accepted grant");
        };
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Started { authority: second },
        ))) = receiver.recv().await
        else {
            panic!("independent accepted grant");
        };
        let first_relationship = first.relationship().clone();
        let second_relationship = second.relationship().clone();
        let duplicate = ObserveEstablished::new(ObservationId(51), target.recipient());
        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .observe(duplicate, endpoint.clone());
        // A duplicate-guard omission returns a whole additional Started.
        // Retain it until all affected members/tasks have completed.
        let duplicate_response = receiver.recv().await;
        let original_terminal = endpoint.termination();
        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .cancel(CancelObservation::new(first));
        let cancellation_response = receiver.recv().await;
        drop(target);
        let actual = original_terminal.await;
        // Do not await one predicted report count: a guard omission admits an
        // additional member while replacing the old revocation. Join the
        // actual task owner first; every admitted completion is now available.
        let tasks = capabilities
            .activation_tasks
            .replace(ActivationTasks::new())
            .expect("original activation tasks remain installed");
        let (returned, failures) = tasks.settle().await;
        let retirement = RetireCapabilities::retire(capabilities).await;
        let (late, late_failures) = retirement.activation_tasks.settle().await;
        let mut completed_reports = Vec::new();
        let mut context = Context::from_waker(Waker::noop());
        let replay = loop {
            let acquired = {
                let mut next = pin!(receiver.recv());
                next.as_mut().poll(&mut context)
            };
            match acquired {
                Poll::Ready(Some(report)) => completed_reports.push(report),
                remaining => break remaining,
            }
        };
        // Every wait is joined and the exact target is released before the
        // duplicate-omission or cancellation/reaction oracle can fail.
        assert_eq!(returned.len(), 0);
        assert_eq!(failures.len(), 0);
        assert_eq!(late.len(), 0);
        assert_eq!(late_failures.len(), 0);
        assert!(retirement.terminal_report.is_none());
        assert!(retirement.retirement_failures.is_empty());
        let (descendants, ()) = &retirement.descendants;
        assert_eq!(descendants.len(), 0);
        assert_eq!(first_relationship.id(), ObservationId(51));
        assert_eq!(second_relationship.id(), ObservationId(52));
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::ObserveRejected { request, reason },
        ))) = duplicate_response
        else {
            panic!("duplicate preserves the whole original rejected request");
        };
        assert_eq!(reason, ObservationRejection::IdAlreadyBound);
        let (id, recipient) = request.into_inputs();
        let returned_endpoint = recipient.interpret(&mut ExtractLocalEndpoint);
        let recovered = returned_endpoint.termination().await;
        assert_eq!(id, ObservationId(51));
        assert_eq!(returned_endpoint.address(), endpoint.address());
        assert_eq!(actual, recovered);
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Cancelled {
                relationship: cancelled,
            },
        ))) = cancellation_response
        else {
            panic!("only first original consumer cancelled");
        };
        assert_eq!(cancelled, first_relationship);
        assert_eq!(completed_reports.len(), 1);
        let mut completed = completed_reports.into_iter();
        let Received::Control(SourceControlEvent::Observation(EstablishedObservation::Stopped {
            relationship,
            outcome,
            at,
        })) = completed
            .next()
            .expect("whole independent consumer completion")
        else {
            panic!("independent terminal report");
        };
        let extra = completed.next();
        assert!(extra.is_none());
        assert_eq!(relationship, second_relationship);
        assert_eq!(outcome, actual);
        assert!(replay.is_pending());
        drop((
            second,
            relationship,
            at,
            mailbox_owner,
            mailbox_ref,
            terminal_receiver,
        ));
    }

    #[tokio::test]
    async fn two_protocols_share_one_membership_owner_and_keep_distinct_static_acknowledgements() {
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
        let first = interface
            .external::<ObservationProtocol>()
            .expect("first concrete protocol");
        let second = interface
            .external::<MessageObservationProtocol>()
            .expect("second concrete protocol");
        let first_endpoint = first.recipient().interpret(&mut ExtractLocalEndpoint);
        let second_endpoint = second.recipient().interpret(&mut ExtractLocalEndpoint);
        let first_request = ObserveEstablished::new(ObservationId(81), first.recipient());
        let second_request = ObserveEstablished::new(ObservationId(82), second.recipient());
        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .observe(first_request, first_endpoint);
        EstablishedObservationInterpreter::<_, Inside<Here>>::new(&mut capabilities)
            .observe(second_request, second_endpoint);
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Started {
                authority: first_authority,
            },
        ))) = receiver.recv().await
        else {
            panic!("first static acknowledgement");
        };
        let Some(Received::Control(SourceControlEvent::MessageObservation(
            EstablishedObservation::Started {
                authority: second_authority,
            },
        ))) = receiver.recv().await
        else {
            panic!("second static acknowledgement");
        };
        let first_relationship = first_authority.relationship().clone();
        let second_relationship = second_authority.relationship().clone();
        EstablishedObservationInterpreter::<_, Inside<Here>>::new(&mut capabilities)
            .cancel(CancelObservation::new(second_authority));
        let Some(Received::Control(SourceControlEvent::MessageObservation(
            EstablishedObservation::Cancelled {
                relationship: cancelled,
            },
        ))) = receiver.recv().await
        else {
            panic!("cancellation returns only through second acknowledgement lane");
        };
        drop((first, second));
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Stopped {
                relationship: stopped,
                outcome,
                at,
            },
        ))) = receiver.recv().await
        else {
            panic!("first protocol remains independently observed");
        };
        let retirement = RetireCapabilities::retire(capabilities).await;
        let (returned, failures) = retirement.activation_tasks.settle().await;
        let replay = {
            let mut next = pin!(receiver.recv());
            let mut context = Context::from_waker(Waker::noop());
            next.as_mut().poll(&mut context)
        };
        assert_eq!(cancelled, second_relationship);
        assert_eq!(stopped, first_relationship);
        assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
        assert_eq!(returned.len(), 0);
        assert_eq!(failures.len(), 0);
        assert!(retirement.terminal_report.is_none());
        assert!(retirement.retirement_failures.is_empty());
        let (descendants, ()) = &retirement.descendants;
        assert_eq!(descendants.len(), 0);
        assert!(replay.is_pending());
        drop((
            first_authority,
            at,
            mailbox_owner,
            mailbox_ref,
            terminal_receiver,
        ));
    }

    fn stop_source_after_terminal(
        _: &mut SourceActor,
        report: EstablishedObservation<ObservationProtocol>,
    ) -> Actions<MailAddr, Never, Vec<Never>, NoBirths> {
        // Explicit test policy: an actually delivered terminal stops the actor.
        drop(report);
        Actions::stop()
    }

    enum CompletionAdmission {
        BeforeCancellation,
        AfterCancellation,
        AfterRetirement,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete deterministic controller, joined cleanup, and whole typed custody oracles together."
    )]
    async fn exact_completion_decision_preserves_both_control_orders_and_retired_admission() {
        type Root = StopOnShutdown<EstablishedTerminationMonitor<SourceActor, ObservationProtocol>>;
        for admission in [
            CompletionAdmission::BeforeCancellation,
            CompletionAdmission::AfterCancellation,
            CompletionAdmission::AfterRetirement,
        ] {
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
            let target = interface
                .external::<ObservationProtocol>()
                .expect("actual target");
            let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
            let request = ObserveEstablished::new(ObservationId(71), target.recipient());
            EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
                .observe(request, endpoint);
            let Some(Received::Control(SourceControlEvent::Observation(
                EstablishedObservation::Started { authority },
            ))) = receiver.recv().await
            else {
                panic!("committed original grant");
            };
            let relationship = authority.relationship().clone();
            let (entered, decision) = oneshot::channel();
            let (continue_conversion, permission) = mpsc::channel();
            *COMPLETION_CONVERSION.lock().unwrap() = Some((entered, permission));
            drop(target);
            // Ready has already removed the exact member, but conversion has
            // not returned E and no control admission has yet been attempted.
            let conversion_entered = decision.await;
            let mut reports = Vec::new();
            let retirement = match admission {
                CompletionAdmission::BeforeCancellation => {
                    let released = continue_conversion.send(());
                    let stopped = receiver.recv().await;
                    reports.push(stopped);
                    EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
                        .cancel(CancelObservation::new(authority));
                    reports.push(receiver.recv().await);
                    let retirement = RetireCapabilities::retire(capabilities).await;
                    match released {
                        Ok(()) | Err(_) => {}
                    }
                    retirement
                }
                CompletionAdmission::AfterCancellation => {
                    EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
                        .cancel(CancelObservation::new(authority));
                    reports.push(receiver.recv().await);
                    let released = continue_conversion.send(());
                    reports.push(receiver.recv().await);
                    let retirement = RetireCapabilities::retire(capabilities).await;
                    match released {
                        Ok(()) | Err(_) => {}
                    }
                    retirement
                }
                CompletionAdmission::AfterRetirement => {
                    let retirement = RetireCapabilities::retire(capabilities).await;
                    let released = continue_conversion.send(());
                    match released {
                        Ok(()) | Err(_) => {}
                    }
                    drop(authority);
                    retirement
                }
            };
            let (returned, failures) = retirement.activation_tasks.settle().await;
            let replay = {
                let mut remaining = pin!(receiver.recv());
                let mut context = Context::from_waker(Waker::noop());
                remaining.as_mut().poll(&mut context)
            };
            // Both gates and every owned task are settled before any oracle.
            let original_at = conversion_entered
                .expect("actual completion timestamp acquired outside conversion");
            assert_eq!(failures.len(), 0);
            assert!(retirement.terminal_report.is_none());
            assert!(retirement.retirement_failures.is_empty());
            let (descendants, ()) = &retirement.descendants;
            assert_eq!(descendants.len(), 0);
            assert!(replay.is_pending());
            match admission {
                CompletionAdmission::BeforeCancellation
                | CompletionAdmission::AfterCancellation => {
                    assert_eq!(returned.len(), 0);
                    assert_eq!(reports.len(), 2);
                    let mut stopped_count = 0;
                    let mut rejected_count = 0;
                    for (position, report) in reports.into_iter().enumerate() {
                        match report {
                            Some(Received::Control(SourceControlEvent::Observation(
                                EstablishedObservation::Stopped {
                                    relationship: original,
                                    outcome,
                                    at,
                                },
                            ))) => {
                                let expected_position = match admission {
                                    CompletionAdmission::BeforeCancellation => 0,
                                    CompletionAdmission::AfterCancellation => 1,
                                    CompletionAdmission::AfterRetirement => unreachable!(),
                                };
                                assert_eq!(position, expected_position);
                                assert_eq!(original, relationship);
                                assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
                                stopped_count += 1;
                                assert_eq!(at, original_at);
                            }
                            Some(Received::Control(SourceControlEvent::Observation(
                                EstablishedObservation::CancelRejected { request, reason },
                            ))) => {
                                assert_eq!(request.relationship(), &relationship);
                                assert_eq!(reason, ObservationRejection::NotObserved);
                                rejected_count += 1;
                                drop(request);
                            }
                            _ => panic!(
                                "completion-winning has only complete Stopped and whole rejected cancellation"
                            ),
                        }
                    }
                    assert_eq!((stopped_count, rejected_count), (1, 1));
                }
                CompletionAdmission::AfterRetirement => {
                    assert_eq!(reports.len(), 0);
                    assert_eq!(returned.len(), 1);
                    let mut originals = returned.into_iter();
                    let SourceControlEvent::Observation(EstablishedObservation::Stopped {
                        relationship: original,
                        outcome,
                        at,
                    }) = originals.next().expect("original converted event")
                    else {
                        panic!("retired admission retains whole original E");
                    };
                    let extra = originals.next();
                    assert!(extra.is_none());
                    assert_eq!(original, relationship);
                    assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
                    assert_eq!(at, original_at);
                }
            }
            drop((mailbox_owner, mailbox_ref, terminal_receiver));
        }

        // Advanced host only: this Consumer is deliberately independent of
        // LocalEnvironment's same-actor lifetime. Preserve genuine acquired E.
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
        let target = interface
            .external::<ObservationProtocol>()
            .expect("actual advanced target");
        let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
        let request = ObserveEstablished::new(ObservationId(71), target.recipient());
        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .observe(request, endpoint);
        let started = receiver.recv().await;
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Started { authority },
        ))) = started
        else {
            panic!("actual committed advanced registration");
        };
        let relationship = authority.relationship().clone();
        let (entered, decision) = oneshot::channel();
        let (continue_conversion, permission) = mpsc::channel();
        *COMPLETION_CONVERSION.lock().unwrap() = Some((entered, permission));
        drop(target);
        let conversion_entered = decision.await;
        drop(receiver);
        let released = continue_conversion.send(());
        // Join before retiring the membership owner. Its value remains Some,
        // so the task reaches actual ControlClosed, rather than the None cut.
        let acquired_tasks = capabilities
            .activation_tasks
            .replace(ActivationTasks::new())
            .expect("original activation tasks remain installed");
        let (returned, failures) = acquired_tasks.settle().await;
        let retirement = RetireCapabilities::retire(capabilities).await;
        let (remaining, retirement_failures) = retirement.activation_tasks.settle().await;
        // All resources and the blocked conversion are settled before oracles.
        assert!(released.is_ok());
        assert_eq!(failures.len(), 0);
        assert_eq!(remaining.len(), 0);
        assert_eq!(retirement_failures.len(), 0);
        assert!(retirement.terminal_report.is_none());
        assert!(retirement.retirement_failures.is_empty());
        let (descendants, ()) = &retirement.descendants;
        assert_eq!(descendants.len(), 0);
        assert_eq!(returned.len(), 1);
        let mut returned = returned.into_iter();
        let original = returned.next();
        let extra = returned.next();
        let Some(SourceControlEvent::Observation(EstablishedObservation::Stopped {
            relationship: retained,
            outcome,
            at,
        })) = original
        else {
            panic!("closed advanced control returns the whole original notification");
        };
        let original_at = conversion_entered.expect("actual completion decision timestamp");
        assert!(extra.is_none());
        assert_eq!(retained, relationship);
        assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
        assert_eq!(at, original_at);
        drop((
            authority,
            retained,
            at,
            mailbox_owner,
            mailbox_ref,
            terminal_receiver,
        ));

        // Standard lifetime: real typed Shutdown retires the actor after its
        // target completion decision, before its notification can be admitted.
        let interface = ActorInterface::new((), ApplicationAddresses::new());
        let target = interface
            .external::<ObservationProtocol>()
            .expect("actual standard target");
        let request = ObserveEstablished::new(ObservationId(71), target.recipient());
        let root = StopOnShutdown::new(EstablishedTerminationMonitor::established(
            SourceActor,
            request,
            stop_source_after_terminal,
        ));
        let (_publisher, termination) = observe::pair();
        let (_cancel, cancellation) = oneshot::channel();
        let (reports, report_received) = oneshot::channel();
        let (published, publication) = oneshot::channel();
        let environment = LocalEnvironment::<Root, _, StandardIngress>::prepare(
            MailAddr(173),
            ActorSpace::new(),
            Config::new(2),
            termination,
            cancellation,
            move |control, timers, observations| {
                ActionInterpreter::new(ApplicationCapabilities::<Root, ()>::new_with_bindings(
                    ApplicationCapabilityInputs {
                        address: MailAddr(173),
                        actor_spaces: Arc::new(()),
                        allocations: ApplicationAddresses::new(),
                        control,
                        timers,
                        observations,
                        terminal_reports: LocalTerminalReports::new(reports),
                    },
                    NoChildBindings::default(),
                ))
            },
        );
        let shutdown = environment.control();
        let environment = environment.publish_with(move |actor| match published.send(actor) {
            Ok(()) | Err(_) => {}
        });
        let joined = tokio::spawn(Driver::new(root, environment).run());
        let actor = publication.await.expect("actual standard root publication");
        // Started was synchronously queued by the initialization request before
        // publication. Same control FIFO puts that genuine grant before Shutdown.
        let (entered, decision) = oneshot::channel();
        let (continue_conversion, permission) = mpsc::channel();
        *COMPLETION_CONVERSION.lock().unwrap() = Some((entered, permission));
        drop(target);
        let conversion_entered = decision.await;
        let admitted = shutdown.send(EventLayer::Owned(ShutdownRequested));
        let driver_retirement = joined.await;
        // Local retirement transferred task ownership without joining this gate.
        let released = continue_conversion.send(());
        let driver_retirement = driver_retirement.expect("real standard Driver joined");
        let driver_retirement = match driver_retirement {
            Ok(retirement) => retirement,
            Err(driver) => {
                drop(driver);
                panic!("original actor must complete its Driver retirement");
            }
        };
        let outcome: LocalOutcome<Root, (Vec<Never>, ())> = driver_retirement.into();
        let ActorExecutionOutcome::Completed {
            behavior,
            residual,
            additional_failures,
            completion,
        } = outcome
        else {
            panic!("real standard shutdown returns its completed outcome");
        };
        let residual = residual.settle_activation_tasks().await;
        let retirement =
            ActorRetirement::<Root, Never, ()>::from_local(ActorExecutionOutcome::Completed {
                behavior,
                residual,
                additional_failures,
                completion,
            });
        // Every gate, task and source owner settled before custody assertions.
        assert!(admitted.is_ok());
        assert!(released.is_ok());
        let ActorRetirement::Completed {
            interpretation: retirement_interpretation,
            source: retirement_source,
            received_interpretation: retirement_received_interpretation,
            received_source: retirement_received_source,
            source_index: retirement_source_index,
            acquired_ingress: retirement_acquired_ingress,
            terminal_report: retirement_terminal_report,
            retirement_failures: retirement_native_failures,
            additional_failures: retirement_additional_failures,
            child_failures: (),
            behavior,
            settlements,
            control,
            user,
            descendants,
            capability_failures,
            unread_owner_cancellation,
            completion,
        } = retirement
        else {
            panic!("real completed actor retirement owns returned notification");
        };
        assert!(retirement_interpretation.is_none());
        assert!(retirement_source.is_none());
        assert!(retirement_received_interpretation.is_none());
        assert!(retirement_received_source.is_none());
        assert!(retirement_source_index.is_none());
        assert!(retirement_acquired_ingress.is_none());
        assert!(retirement_terminal_report.is_none());
        assert!(retirement_native_failures.is_empty());
        assert!(retirement_additional_failures.is_empty());

        assert!(matches!(completion, Completion::Stopped));
        assert_eq!(settlements.len(), 1);
        for settlement in settlements {
            assert_eq!(settlement.sends.owned, NoSends);
            assert_eq!(settlement.sends.inner.owned.len(), 0);
            assert_eq!(settlement.sends.inner.inner.len(), 0);
            assert_eq!(settlement.creations.len(), 0);
            assert!(matches!(settlement.become_, Step::Stop(_)));
        }
        assert_eq!(user.len(), 0);
        assert_eq!(descendants.len(), 0);
        assert_eq!(capability_failures.len(), 0);
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(control.len(), 1);
        let mut control = control.into_iter();
        let retained = control.next();
        let extra = control.next();
        let Some(EventLayer::Inner(EventLayer::Owned(EstablishedObservation::Stopped {
            relationship,
            outcome,
            at,
        }))) = retained
        else {
            panic!("the completed product preserves whole original Stopped");
        };
        let mut monitor = behavior.into_inner();
        let cancellation = monitor
            .take_cancellation()
            .expect("genuine Started grant reached the actor before shutdown");
        let original_at = conversion_entered.expect("actual original acquired timestamp");
        assert!(extra.is_none());
        assert_eq!(cancellation.relationship(), &relationship);
        assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
        assert_eq!(at, original_at);
        let (source, observation) = monitor.into_parts();
        drop((
            source,
            observation,
            cancellation,
            relationship,
            actor,
            shutdown,
            report_received,
        ));
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
                ChildBindings<ProxyParent, Never, StructuralOrigins<ProxyParent>>,
            >,
        >();
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete deterministic controller, joined cleanup, and whole typed custody oracles together."
    )]
    async fn foreign_observer_grant_cannot_cancel_same_id_and_shared_target_consumers_remain_independent()
     {
        let interface = ActorInterface::new((), ApplicationAddresses::new());
        let target = interface
            .external::<ObservationProtocol>()
            .expect("one exact shared target");
        let mut observers = Vec::new();
        for address in [MailAddr(171), MailAddr(173)] {
            let (control, owner, mailbox, receiver) =
                mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(2));
            let (reports, report_received) = oneshot::channel();
            let mut capabilities = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
                ApplicationCapabilityInputs {
                    address,
                    actor_spaces: Arc::new(()),
                    allocations: ApplicationAddresses::new(),
                    control,
                    timers: LocalTimers::new(),
                    observations: TerminationObservations::new(),
                    terminal_reports: LocalTerminalReports::new(reports),
                },
                NoChildBindings::default(),
            );
            let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
            let request = ObserveEstablished::new(ObservationId(171), target.recipient());
            EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
                .observe(request, endpoint);
            observers.push((capabilities, owner, mailbox, receiver, report_received));
        }
        let mut observers = observers.into_iter();
        let (mut first, first_owner, first_mailbox, mut first_receiver, first_reports) =
            observers.next().expect("first real owning interpreter");
        let (mut second, second_owner, second_mailbox, mut second_receiver, second_reports) =
            observers.next().expect("second real owning interpreter");
        let extra = observers.next();
        assert!(extra.is_none());
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Started {
                authority: first_authority,
            },
        ))) = first_receiver.recv().await
        else {
            panic!("first committed grant");
        };
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Started {
                authority: second_authority,
            },
        ))) = second_receiver.recv().await
        else {
            panic!("second committed grant");
        };
        let first_relationship = first_authority.relationship().clone();
        let second_relationship = second_authority.relationship().clone();
        EstablishedObservationInterpreter::<_, Here>::new(&mut second)
            .cancel(CancelObservation::new(first_authority));
        let foreign_response = second_receiver.recv().await;
        let (returned_rejection, unexpected_foreign) = match foreign_response {
            Some(Received::Control(SourceControlEvent::Observation(
                EstablishedObservation::CancelRejected {
                    request: returned,
                    reason,
                },
            ))) => {
                let returned_relationship = returned.relationship().clone();
                EstablishedObservationInterpreter::<_, Here>::new(&mut first).cancel(returned);
                (Some((returned_relationship, reason)), None)
            }
            report => (None, report),
        };
        drop(target);
        // Await the existing task owners while the live membership owner can
        // complete. In a numeric-only mutant either task may already have
        // been cancelled; both sets still settle without waiting for a report
        // that the mutant suppresses. No new observation task is introduced.
        let first_tasks = first
            .activation_tasks
            .replace(ActivationTasks::new())
            .expect("original activation tasks remain installed");
        let second_tasks = second
            .activation_tasks
            .replace(ActivationTasks::new())
            .expect("original activation tasks remain installed");
        let (first_returned, first_failures) = first_tasks.settle().await;
        let (second_returned, second_failures) = second_tasks.settle().await;
        let mut context = Context::from_waker(Waker::noop());
        let first_cancelled = {
            let mut first_event = pin!(first_receiver.recv());
            first_event.as_mut().poll(&mut context)
        };
        let second_stopped = {
            let mut second_event = pin!(second_receiver.recv());
            second_event.as_mut().poll(&mut context)
        };
        let first_retirement = RetireCapabilities::retire(first).await;
        let second_retirement = RetireCapabilities::retire(second).await;
        let (late_first, late_first_failures) = first_retirement.activation_tasks.settle().await;
        let (late_second, late_second_failures) = second_retirement.activation_tasks.settle().await;
        let mut context = Context::from_waker(Waker::noop());
        let first_replay = {
            let mut first_next = pin!(first_receiver.recv());
            first_next.as_mut().poll(&mut context)
        };
        let second_replay = {
            let mut second_next = pin!(second_receiver.recv());
            second_next.as_mut().poll(&mut context)
        };
        // Every source wait is joined before identity, rejection and replay oracles.
        assert_ne!(first_relationship, second_relationship);
        assert!(unexpected_foreign.is_none());
        let Some((returned_relationship, reason)) = returned_rejection else {
            panic!("foreign grant is returned whole");
        };
        assert_eq!(returned_relationship, first_relationship);
        assert_eq!(reason, ObservationRejection::NotObserved);
        let Poll::Ready(Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Cancelled { relationship },
        )))) = first_cancelled
        else {
            panic!("returned original grant still cancels only its own member");
        };
        assert_eq!(relationship, first_relationship);
        let Poll::Ready(Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Stopped {
                relationship,
                outcome,
                at,
            },
        )))) = second_stopped
        else {
            panic!("independent observer retains original shared-target completion");
        };
        assert_eq!(relationship, second_relationship);
        assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
        assert_eq!(first_returned.len(), 0);
        assert_eq!(second_returned.len(), 0);
        assert_eq!(first_failures.len(), 0);
        assert_eq!(second_failures.len(), 0);
        assert_eq!(late_first.len(), 0);
        assert_eq!(late_second.len(), 0);
        assert_eq!(late_first_failures.len(), 0);
        assert_eq!(late_second_failures.len(), 0);
        assert!(first_retirement.terminal_report.is_none());
        assert!(first_retirement.retirement_failures.is_empty());
        let (first_descendants, ()) = &first_retirement.descendants;
        assert_eq!(first_descendants.len(), 0);
        assert!(second_retirement.terminal_report.is_none());
        assert!(second_retirement.retirement_failures.is_empty());
        let (second_descendants, ()) = &second_retirement.descendants;
        assert_eq!(second_descendants.len(), 0);
        assert!(first_replay.is_pending());
        assert!(second_replay.is_pending());
        drop((
            second_authority,
            at,
            first_owner,
            first_mailbox,
            first_receiver,
            first_reports,
            second_owner,
            second_mailbox,
            second_receiver,
            second_reports,
        ));
    }

    type TerminalReply = MessageProtocol<MailAddr, EstablishedObservation<ObservationProtocol>>;

    struct TerminalPublisher {
        values: Vec<u64>,
        reply_to: EstablishedRecipient<TerminalReply>,
    }

    impl BehaviorBase for TerminalPublisher {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }

    impl Behavior for TerminalPublisher {
        type Protocol = MessageProtocol<MailAddr, ()>;
        type Event = User<MailAddr, ()>;
        type Sends = Vec<EstablishedDelivery<TerminalReply>>;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;

        fn transition(&mut self, _: ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }
    }

    fn publish_terminal(
        actor: &mut TerminalPublisher,
        report: EstablishedObservation<ObservationProtocol>,
    ) -> Actions<MailAddr, Never, Vec<EstablishedDelivery<TerminalReply>>, NoBirths> {
        Actions::new(
            vec![EstablishedDelivery::new(actor.reply_to.clone(), report)],
            Creations::empty(),
            Step::Continue,
        )
    }

    fn publish_terminal_inside_shutdown(
        actor: &mut StopOnShutdown<TerminalPublisher>,
        report: EstablishedObservation<ObservationProtocol>,
    ) -> Actions<
        MailAddr,
        Never,
        SendLayer<NoSends, Vec<EstablishedDelivery<TerminalReply>>>,
        NoBirths,
    > {
        Actions::new(
            SendLayer::new(
                NoSends,
                vec![EstablishedDelivery::new(
                    actor.base().reply_to.clone(),
                    report,
                )],
            ),
            Creations::empty(),
            Step::Continue,
        )
    }

    enum CompletionReportArrival {
        TerminalFirst,
        RejectionFirst,
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete deterministic controller, joined cleanup, and whole typed custody oracles together."
    )]
    async fn outer_shutdown_driver_retirement_returns_whole_rejected_cancel_in_both_report_orders()
    {
        type Root =
            StopOnShutdown<EstablishedTerminationMonitor<TerminalPublisher, ObservationProtocol>>;
        for arrival in [
            CompletionReportArrival::TerminalFirst,
            CompletionReportArrival::RejectionFirst,
        ] {
            let interface = ActorInterface::new((), ApplicationAddresses::new());
            let target = interface
                .external::<ObservationProtocol>()
                .expect("actual observed target");
            let mut reply = interface
                .external::<TerminalReply>()
                .expect("actual terminal reply endpoint");
            let original_reply = reply.recipient();
            let original_reply_address = original_reply
                .clone()
                .interpret(&mut ExtractLocalEndpoint)
                .address();
            let values = vec![151, 157];
            let allocation = values.as_ptr() as usize;
            let publisher = TerminalPublisher {
                values,
                reply_to: original_reply.clone(),
            };
            let request = ObserveEstablished::new(ObservationId(151), target.recipient());
            let mut monitor =
                EstablishedTerminationMonitor::established(publisher, request, publish_terminal);
            let initial =
                behavior::initialize(&mut monitor).expect("pure complete initial actions");
            assert_eq!(initial.sends.owned.len(), 1);
            assert_eq!(initial.sends.inner.len(), 0);
            assert_eq!(initial.creates.len(), 0);
            assert_eq!(initial.become_, Step::Continue);
            let mut requests = initial.sends.owned.into_iter();
            let original = requests.next().expect("whole original request");
            let extra = requests.next();
            assert!(extra.is_none());
            let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
            let (control, owner, mailbox, mut receiver) =
                mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(2));
            let (reports, report_received) = oneshot::channel();
            let mut source = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
                ApplicationCapabilityInputs {
                    address: MailAddr(151),
                    actor_spaces: Arc::new(()),
                    allocations: ApplicationAddresses::new(),
                    control,
                    timers: LocalTimers::new(),
                    observations: TerminationObservations::new(),
                    terminal_reports: LocalTerminalReports::new(reports),
                },
                NoChildBindings::default(),
            );
            EstablishedObservationInterpreter::<_, Here>::new(&mut source)
                .observe(original, endpoint);
            let Some(Received::Control(SourceControlEvent::Observation(started))) =
                receiver.recv().await
            else {
                panic!("actual committed Started");
            };
            let actions =
                behavior::delegate_transition(&mut monitor, EventLayer::Owned(started)).unwrap();
            assert_eq!(actions.sends.owned.len(), 0);

            assert_eq!(actions.sends.inner.len(), 0);
            assert_eq!(actions.creates.len(), 0);
            assert_eq!(actions.become_, Step::Continue);
            let cancellation = monitor
                .take_cancellation()
                .expect("one affine original grant");
            let relationship = cancellation.relationship().clone();
            drop(target);
            let Some(Received::Control(SourceControlEvent::Observation(stopped))) =
                receiver.recv().await
            else {
                panic!("actual completion wins");
            };
            let EstablishedObservation::Stopped { at, .. } = &stopped else {
                panic!("complete original terminal report");
            };
            let original_at = *at;
            EstablishedObservationInterpreter::<_, Here>::new(&mut source).cancel(cancellation);
            let Some(Received::Control(SourceControlEvent::Observation(rejected))) =
                receiver.recv().await
            else {
                panic!("whole actual rejected cancellation");
            };
            let ordered = match arrival {
                CompletionReportArrival::TerminalFirst => [stopped, rejected],
                CompletionReportArrival::RejectionFirst => [rejected, stopped],
            };
            let mut rejected_reports = Vec::new();
            for report in ordered {
                let actions =
                    match behavior::delegate_transition(&mut monitor, EventLayer::Owned(report)) {
                        Ok(actions) => actions,
                        Err(error) => {
                            rejected_reports.push(error);
                            continue;
                        }
                    };
                assert_eq!(actions.sends.owned.len(), 0);

                assert_eq!(actions.creates.len(), 0);
                assert_eq!(actions.become_, Step::Continue);
                for delivery in actions.sends.inner {
                    let mut delivery = Some(delivery);
                    let mut settlement = None;
                    <ApplicationCapabilities<SourceActor, ()> as InterpretItem<
                        EstablishedDelivery<TerminalReply>,
                        SourceControlEvent,
                        Here,
                    >>::interpret_item(
                        &mut source, &mut delivery, &mut settlement
                    )
                    .await;
                    assert!(delivery.is_none());
                    assert!(matches!(
                        settlement,
                        Some(behavior::ItemSettlement::Accepted(()))
                    ));
                }
            }
            let source_retirement = RetireCapabilities::retire(source).await;
            let (returned_source, source_failures) =
                source_retirement.activation_tasks.settle().await;
            assert_eq!(returned_source.len(), 0);
            assert_eq!(source_failures.len(), 0);
            assert!(source_retirement.terminal_report.is_none());
            assert!(source_retirement.retirement_failures.is_empty());
            let (source_descendants, ()) = &source_retirement.descendants;
            assert_eq!(source_descendants.len(), 0);
            drop((owner, mailbox, receiver, report_received));
            let (root_publication, termination) = observe::pair();
            let (_cancel, cancellation) = oneshot::channel();
            let (reports, report_received) = oneshot::channel();
            let (actor_notice, publication) = oneshot::channel();
            let environment = LocalEnvironment::<Root, _, StandardIngress>::prepare(
                MailAddr(161),
                ActorSpace::new(),
                Config::new(2),
                termination,
                cancellation,
                move |control, timers, observations| {
                    ActionInterpreter::new(ApplicationCapabilities::<Root, ()>::new_with_bindings(
                        ApplicationCapabilityInputs {
                            address: MailAddr(161),
                            actor_spaces: Arc::new(()),
                            allocations: ApplicationAddresses::new(),
                            control,
                            timers,
                            observations,
                            terminal_reports: LocalTerminalReports::new(reports),
                        },
                        NoChildBindings::default(),
                    ))
                },
            );
            let shutdown = environment.control();
            let environment =
                environment.publish_with(move |actor| match actor_notice.send(actor) {
                    Ok(()) | Err(_) => {}
                });
            let joined = tokio::spawn(Driver::new(StopOnShutdown::new(monitor), environment).run());
            let actor = publication.await.expect("actual root actor_notice");
            let admitted = shutdown.send(EventLayer::Owned(ShutdownRequested));
            let retirement = joined
                .await
                .expect("original wrapped behavior and environment join");
            let retirement = match retirement {
                Ok(retirement) => retirement,
                Err(driver) => {
                    drop(driver);
                    panic!("original actor must complete its Driver retirement");
                }
            };
            let residual = retirement.residual.settle_activation_tasks().await;
            // This direct Driver has no launch termination-publishing step.
            // Release the original unused publication owner only after joins.
            drop(root_publication);
            let (publisher, target) = retirement.behavior.into_inner().into_parts();
            let delivered = {
                let mut callback_delivery = pin!(reply.receive());
                let mut context = Context::from_waker(Waker::noop());
                callback_delivery.as_mut().poll(&mut context)
            };
            let Poll::Ready(Some(delivered)) = delivered else {
                panic!("original callback delivery is available after every root/source join");
            };
            // Transfer the same whole typed payload through the recipient held
            // by the actual retired Behavior, outside every Behavior fold.
            let original_from = delivered.from;
            let retained_endpoint = publisher
                .reply_to
                .clone()
                .interpret(&mut ExtractLocalEndpoint);
            let retained_reply_address = retained_endpoint.address();
            let retained_delivery = retained_endpoint
                .send_from(original_from, delivered.message)
                .await;
            let delivered = {
                let mut callback_delivery = pin!(reply.receive());
                let mut context = Context::from_waker(Waker::noop());
                callback_delivery.as_mut().poll(&mut context)
            };
            drop(reply);
            // Source tasks and the root have already joined. A missing reply
            // is a finite Pending observation, never a hanging receive.
            // All operation/task/root cleanup precedes every final custody oracle.
            assert!(admitted.is_ok());
            for error in &rejected_reports {
                let TerminationMonitorError::UnexpectedReport { report, .. } = error else {
                    panic!(
                        "the original rejection suppresses the terminal callback, not an inner fold"
                    );
                };
                let EstablishedObservation::Stopped {
                    relationship: original,
                    outcome,
                    at,
                } = report
                else {
                    panic!(
                        "the original source Stopped remains whole even when its reaction is suppressed"
                    );
                };
                assert_eq!(original, &relationship);
                assert_eq!(*outcome, Ok(behavior_actors::Exit::Normal));
                assert_eq!(*at, original_at);
            }
            assert_eq!(rejected_reports.len(), 0);
            assert!(matches!(retirement.disposition, Ok(Completion::Stopped)));
            let LocalResidual::Retired {
                interpretation: retirement_interpretation,
                source: retirement_source,
                received_interpretation: retirement_received_interpretation,
                received_source: retirement_received_source,
                source_index: retirement_source_index,
                acquired_ingress: retirement_acquired_ingress,
                terminal_report: retirement_terminal_report,
                retirement_failures: retirement_native_failures,
                settlements,
                ingress,
                activation_tasks,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            } = residual
            else {
                panic!("whole local retirement");
            };
            assert!(retirement_interpretation.is_none());
            assert!(retirement_source.is_none());
            assert!(retirement_received_interpretation.is_none());
            assert!(retirement_received_source.is_none());
            assert!(retirement_source_index.is_none());
            assert!(retirement_acquired_ingress.is_none());
            assert!(retirement_terminal_report.is_none());
            assert!(retirement_native_failures.is_empty());

            let (remaining_controls, remaining_failures) = activation_tasks.settle().await;
            assert_eq!(settlements.len(), 1);
            for settlement in settlements {
                assert_eq!(settlement.sends.owned, NoSends);
                assert_eq!(settlement.sends.inner.owned.len(), 0);
                assert_eq!(settlement.sends.inner.inner.len(), 0);
                assert_eq!(settlement.creations.len(), 0);
                assert!(matches!(settlement.become_, Step::Stop(_)));
            }
            assert_eq!(ingress.control.len(), 0);
            assert_eq!(ingress.user.len(), 0);
            assert_eq!(remaining_controls.len(), 0);
            assert_eq!(remaining_failures.len(), 0);
            let (descendants, ()) = descendants;
            assert_eq!(descendants.len(), 0);
            assert_eq!(capability_failures.len(), 0);
            assert!(unread_owner_cancellation.is_none());

            assert_eq!(publisher.values.as_ptr() as usize, allocation);
            assert_eq!(publisher.values, [151, 157]);
            assert_eq!(retained_reply_address, original_reply_address);
            assert!(retained_delivery.is_ok());
            let Ok((original, reason)) = target.into_rejected_cancel() else {
                panic!("actual retired target returns the original affine grant");
            };
            assert_eq!(original.relationship(), &relationship);
            assert_eq!(original.id(), ObservationId(151));
            assert_eq!(reason, ObservationRejection::NotObserved);
            let Poll::Ready(Some(delivered)) = delivered else {
                panic!("one complete actual terminal reply remains available after joins");
            };
            assert_eq!(delivered.from, MailAddr(151));
            assert_eq!(delivered.from, original_from);
            let EstablishedObservation::Stopped {
                relationship: delivered_relationship,
                outcome,
                at,
            } = delivered.message
            else {
                panic!("whole original terminal payload");
            };
            assert_eq!(delivered_relationship, relationship);
            assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
            assert_eq!(at, original_at);
            drop((original, actor, shutdown, publisher, report_received));
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete deterministic controller, joined cleanup, and whole typed custody oracles together."
    )]
    async fn inner_shutdown_driver_retirement_returns_whole_rejected_cancel_in_both_report_orders()
    {
        type Root =
            EstablishedTerminationMonitor<StopOnShutdown<TerminalPublisher>, ObservationProtocol>;
        for arrival in [
            CompletionReportArrival::TerminalFirst,
            CompletionReportArrival::RejectionFirst,
        ] {
            let interface = ActorInterface::new((), ApplicationAddresses::new());
            let target = interface
                .external::<ObservationProtocol>()
                .expect("actual observed target");
            let mut reply = interface
                .external::<TerminalReply>()
                .expect("actual terminal reply endpoint");
            let original_reply = reply.recipient();
            let original_reply_address = original_reply
                .clone()
                .interpret(&mut ExtractLocalEndpoint)
                .address();
            let values = vec![151, 157];
            let allocation = values.as_ptr() as usize;
            let publisher = TerminalPublisher {
                values,
                reply_to: original_reply.clone(),
            };
            let request = ObserveEstablished::new(ObservationId(151), target.recipient());
            let mut monitor = EstablishedTerminationMonitor::established(
                StopOnShutdown::new(publisher),
                request,
                publish_terminal_inside_shutdown,
            );
            let initial =
                behavior::initialize(&mut monitor).expect("pure complete initial actions");
            assert_eq!(initial.sends.owned.len(), 1);
            assert_eq!(initial.sends.inner.owned, NoSends);
            assert_eq!(initial.sends.inner.inner.len(), 0);
            assert_eq!(initial.creates.len(), 0);
            assert_eq!(initial.become_, Step::Continue);
            let mut requests = initial.sends.owned.into_iter();
            let original = requests.next().expect("whole original request");
            let extra = requests.next();
            assert!(extra.is_none());
            let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
            let (control, owner, mailbox, mut receiver) =
                mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(2));
            let (reports, report_received) = oneshot::channel();
            let mut source = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
                ApplicationCapabilityInputs {
                    address: MailAddr(151),
                    actor_spaces: Arc::new(()),
                    allocations: ApplicationAddresses::new(),
                    control,
                    timers: LocalTimers::new(),
                    observations: TerminationObservations::new(),
                    terminal_reports: LocalTerminalReports::new(reports),
                },
                NoChildBindings::default(),
            );
            EstablishedObservationInterpreter::<_, Here>::new(&mut source)
                .observe(original, endpoint);
            let Some(Received::Control(SourceControlEvent::Observation(started))) =
                receiver.recv().await
            else {
                panic!("actual committed Started");
            };
            let actions =
                behavior::delegate_transition(&mut monitor, EventLayer::Owned(started)).unwrap();
            assert_eq!(actions.sends.owned.len(), 0);
            assert_eq!(actions.sends.inner.owned, NoSends);
            assert_eq!(actions.sends.inner.inner.len(), 0);
            assert_eq!(actions.creates.len(), 0);
            assert_eq!(actions.become_, Step::Continue);
            let cancellation = monitor
                .take_cancellation()
                .expect("one affine original grant");
            let relationship = cancellation.relationship().clone();
            drop(target);
            let Some(Received::Control(SourceControlEvent::Observation(stopped))) =
                receiver.recv().await
            else {
                panic!("actual completion wins");
            };
            let EstablishedObservation::Stopped { at, .. } = &stopped else {
                panic!("complete original terminal report");
            };
            let original_at = *at;
            EstablishedObservationInterpreter::<_, Here>::new(&mut source).cancel(cancellation);
            let Some(Received::Control(SourceControlEvent::Observation(rejected))) =
                receiver.recv().await
            else {
                panic!("whole actual rejected cancellation");
            };
            let ordered = match arrival {
                CompletionReportArrival::TerminalFirst => [stopped, rejected],
                CompletionReportArrival::RejectionFirst => [rejected, stopped],
            };
            let mut rejected_reports = Vec::new();
            for report in ordered {
                let actions =
                    match behavior::delegate_transition(&mut monitor, EventLayer::Owned(report)) {
                        Ok(actions) => actions,
                        Err(error) => {
                            rejected_reports.push(error);
                            continue;
                        }
                    };
                assert_eq!(actions.sends.owned.len(), 0);
                assert_eq!(actions.sends.inner.owned, NoSends);
                assert_eq!(actions.creates.len(), 0);
                assert_eq!(actions.become_, Step::Continue);
                for delivery in actions.sends.inner.inner {
                    let mut delivery = Some(delivery);
                    let mut settlement = None;
                    <ApplicationCapabilities<SourceActor, ()> as InterpretItem<
                        EstablishedDelivery<TerminalReply>,
                        SourceControlEvent,
                        Here,
                    >>::interpret_item(
                        &mut source, &mut delivery, &mut settlement
                    )
                    .await;
                    assert!(delivery.is_none());
                    assert!(matches!(
                        settlement,
                        Some(behavior::ItemSettlement::Accepted(()))
                    ));
                }
            }
            let source_retirement = RetireCapabilities::retire(source).await;
            let (returned_source, source_failures) =
                source_retirement.activation_tasks.settle().await;
            assert_eq!(returned_source.len(), 0);
            assert_eq!(source_failures.len(), 0);
            assert!(source_retirement.terminal_report.is_none());
            assert!(source_retirement.retirement_failures.is_empty());
            let (source_descendants, ()) = &source_retirement.descendants;
            assert_eq!(source_descendants.len(), 0);
            drop((owner, mailbox, receiver, report_received));
            let (root_publication, termination) = observe::pair();
            let (_cancel, cancellation) = oneshot::channel();
            let (reports, report_received) = oneshot::channel();
            let (actor_notice, publication) = oneshot::channel();
            let environment = LocalEnvironment::<Root, _, StandardIngress>::prepare(
                MailAddr(161),
                ActorSpace::new(),
                Config::new(2),
                termination,
                cancellation,
                move |control, timers, observations| {
                    ActionInterpreter::new(ApplicationCapabilities::<Root, ()>::new_with_bindings(
                        ApplicationCapabilityInputs {
                            address: MailAddr(161),
                            actor_spaces: Arc::new(()),
                            allocations: ApplicationAddresses::new(),
                            control,
                            timers,
                            observations,
                            terminal_reports: LocalTerminalReports::new(reports),
                        },
                        NoChildBindings::default(),
                    ))
                },
            );
            let shutdown = environment.control();
            let environment =
                environment.publish_with(move |actor| match actor_notice.send(actor) {
                    Ok(()) | Err(_) => {}
                });
            let joined = tokio::spawn(Driver::new(monitor, environment).run());
            let actor = publication.await.expect("actual root actor_notice");
            let admitted = shutdown.send(EventLayer::Inner(EventLayer::Owned(ShutdownRequested)));
            let retirement = joined
                .await
                .expect("original wrapped behavior and environment join");
            let retirement = match retirement {
                Ok(retirement) => retirement,
                Err(driver) => {
                    drop(driver);
                    panic!("original actor must complete its Driver retirement");
                }
            };
            let residual = retirement.residual.settle_activation_tasks().await;
            // This direct Driver has no launch termination-publishing step.
            // Release the original unused publication owner only after joins.
            drop(root_publication);
            let (shutdown_behavior, target) = retirement.behavior.into_parts();
            let publisher = shutdown_behavior.into_inner();
            let delivered = {
                let mut callback_delivery = pin!(reply.receive());
                let mut context = Context::from_waker(Waker::noop());
                callback_delivery.as_mut().poll(&mut context)
            };
            let Poll::Ready(Some(delivered)) = delivered else {
                panic!("original callback delivery is available after every root/source join");
            };
            // Transfer the same whole typed payload through the recipient held
            // by the actual retired Behavior, outside every Behavior fold.
            let original_from = delivered.from;
            let retained_endpoint = publisher
                .reply_to
                .clone()
                .interpret(&mut ExtractLocalEndpoint);
            let retained_reply_address = retained_endpoint.address();
            let retained_delivery = retained_endpoint
                .send_from(original_from, delivered.message)
                .await;
            let delivered = {
                let mut callback_delivery = pin!(reply.receive());
                let mut context = Context::from_waker(Waker::noop());
                callback_delivery.as_mut().poll(&mut context)
            };
            drop(reply);
            // Source tasks and the root have already joined. A missing reply
            // is a finite Pending observation, never a hanging receive.
            // All operation/task/root cleanup precedes every final custody oracle.
            assert!(admitted.is_ok());
            for error in &rejected_reports {
                let TerminationMonitorError::UnexpectedReport { report, .. } = error else {
                    panic!(
                        "the original rejection suppresses the terminal callback, not an inner fold"
                    );
                };
                let EstablishedObservation::Stopped {
                    relationship: original,
                    outcome,
                    at,
                } = report
                else {
                    panic!(
                        "the original source Stopped remains whole even when its reaction is suppressed"
                    );
                };
                assert_eq!(original, &relationship);
                assert_eq!(*outcome, Ok(behavior_actors::Exit::Normal));
                assert_eq!(*at, original_at);
            }
            assert_eq!(rejected_reports.len(), 0);
            assert!(matches!(retirement.disposition, Ok(Completion::Stopped)));
            let LocalResidual::Retired {
                interpretation: retirement_interpretation,
                source: retirement_source,
                received_interpretation: retirement_received_interpretation,
                received_source: retirement_received_source,
                source_index: retirement_source_index,
                acquired_ingress: retirement_acquired_ingress,
                terminal_report: retirement_terminal_report,
                retirement_failures: retirement_native_failures,
                settlements,
                ingress,
                activation_tasks,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            } = residual
            else {
                panic!("whole local retirement");
            };
            assert!(retirement_interpretation.is_none());
            assert!(retirement_source.is_none());
            assert!(retirement_received_interpretation.is_none());
            assert!(retirement_received_source.is_none());
            assert!(retirement_source_index.is_none());
            assert!(retirement_acquired_ingress.is_none());
            assert!(retirement_terminal_report.is_none());
            assert!(retirement_native_failures.is_empty());

            let (remaining_controls, remaining_failures) = activation_tasks.settle().await;
            assert_eq!(settlements.len(), 1);
            for settlement in settlements {
                assert_eq!(settlement.sends.owned.len(), 0);
                assert_eq!(settlement.sends.inner.owned, NoSends);
                assert_eq!(settlement.sends.inner.inner.len(), 0);
                assert_eq!(settlement.creations.len(), 0);
                assert!(matches!(settlement.become_, Step::Stop(_)));
            }
            assert_eq!(ingress.control.len(), 0);
            assert_eq!(ingress.user.len(), 0);
            assert_eq!(remaining_controls.len(), 0);
            assert_eq!(remaining_failures.len(), 0);
            let (descendants, ()) = descendants;
            assert_eq!(descendants.len(), 0);
            assert_eq!(capability_failures.len(), 0);
            assert!(unread_owner_cancellation.is_none());

            assert_eq!(publisher.values.as_ptr() as usize, allocation);
            assert_eq!(publisher.values, [151, 157]);
            assert_eq!(retained_reply_address, original_reply_address);
            assert!(retained_delivery.is_ok());
            let Ok((original, reason)) = target.into_rejected_cancel() else {
                panic!("actual retired target returns the original affine grant");
            };
            assert_eq!(original.relationship(), &relationship);
            assert_eq!(original.id(), ObservationId(151));
            assert_eq!(reason, ObservationRejection::NotObserved);
            let Poll::Ready(Some(delivered)) = delivered else {
                panic!("one complete actual terminal reply remains available after joins");
            };
            assert_eq!(delivered.from, MailAddr(151));
            assert_eq!(delivered.from, original_from);
            let EstablishedObservation::Stopped {
                relationship: delivered_relationship,
                outcome,
                at,
            } = delivered.message
            else {
                panic!("whole original terminal payload");
            };
            assert_eq!(delivered_relationship, relationship);
            assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
            assert_eq!(at, original_at);
            drop((original, actor, shutdown, publisher, report_received));
        }
    }
}

#[cfg(test)]
mod live_capability_retirement {
    use std::sync::Arc;

    use behavior::{
        Actions, ActiveTurn, Behavior, BehaviorActed, Here, InjectEvent, MessageProtocol, Never,
        NoBirths, NoSends, User, UserEvent,
    };
    use behavior_actors::{
        EstablishedObservation, InterpretEstablishedObservation, ObservationId, ObserveEstablished,
    };
    use bombay_engine::{Completion, Driver};
    use communication::Config;
    use tokio::sync::oneshot;

    use super::{
        ApplicationCapabilities, ApplicationCapabilityInputs, EstablishedObservationInterpreter,
    };
    use crate::actor_interface::{ActorInterface, ExtractLocalEndpoint};
    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::child_bindings::NoChildBindings;
    use crate::interpret::ActionInterpreter;
    use crate::launch::ActorSpace;
    use crate::local::{LocalEnvironment, LocalResidual, LocalRetirementRequest, StandardIngress};
    use crate::observe;
    use crate::reports::LocalTerminalReports;

    type ObservedProtocol = MessageProtocol<MailAddr, Never>;

    enum CapabilityEvent {
        Observation(EstablishedObservation<ObservedProtocol>),
    }

    impl UserEvent for CapabilityEvent {
        type Addr = MailAddr;
        type Message = Never;

        fn user(_: MailAddr, message: Never) -> Self {
            match message {}
        }

        fn into_user(self) -> Result<User<MailAddr, Never>, Self> {
            Err(self)
        }
    }

    impl InjectEvent<EstablishedObservation<ObservedProtocol>, Here> for CapabilityEvent {
        fn inject_at(observation: EstablishedObservation<ObservedProtocol>) -> Self {
            match observation {
                EstablishedObservation::Stopped { .. } => panic!("observation conversion failed"),
                observation => Self::Observation(observation),
            }
        }
    }

    struct ObservingActor {
        values: Vec<u64>,
        observations: Vec<ObservationId>,
    }

    impl Behavior for ObservingActor {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = CapabilityEvent;
        type Sends = NoSends;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;

        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            let CapabilityEvent::Observation(observation) = event;
            match observation {
                EstablishedObservation::Started { authority } => {
                    self.observations.push(authority.relationship().id());
                }
                _ => panic!("only the successful start can reach this actor"),
            }
            Ok(Actions::cont())
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "keep the complete outside-fold controller, joined disposal and whole typed original-value oracles together; shortening it would split the single custody law or weaken observations"
    )]
    async fn acquired_capability_failure_preserves_available_actor_values() {
        let values = vec![77, 177];
        let allocation = values.as_ptr() as usize;
        let interface = ActorInterface::new((), ApplicationAddresses::new());
        let target = interface
            .external::<ObservedProtocol>()
            .expect("established target");
        let recipient = target.recipient();
        let endpoint = recipient.clone().interpret(&mut ExtractLocalEndpoint);
        let (publisher, observation) = observe::pair();
        let (request, cancellation) = oneshot::channel();
        let (reports, report_received) = oneshot::channel();
        let environment = LocalEnvironment::<ObservingActor, _, StandardIngress>::prepare(
            MailAddr(81),
            ActorSpace::new(),
            Config::new(2),
            observation,
            cancellation,
            move |control, timers, observations| {
                let mut capabilities =
                    ApplicationCapabilities::<ObservingActor, ()>::new_with_bindings(
                        ApplicationCapabilityInputs {
                            address: MailAddr(81),
                            actor_spaces: Arc::new(()),
                            allocations: ApplicationAddresses::new(),
                            control,
                            timers,
                            observations,
                            terminal_reports: LocalTerminalReports::new(reports),
                        },
                        NoChildBindings::default(),
                    );
                EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities).observe(
                    ObserveEstablished::new(ObservationId(1), recipient),
                    endpoint,
                );
                ActionInterpreter::new(capabilities)
            },
        );
        let joined = tokio::spawn(
            Driver::new(
                ObservingActor {
                    values,
                    observations: Vec::new(),
                },
                environment,
            )
            .run(),
        );
        drop(target);
        let retirement = joined.await.expect(
            "acquired capability failure must retire with available actor state instead of unwinding",
        );
        let retirement = match retirement {
            Ok(retirement) => retirement,
            Err(driver) => {
                drop(driver);
                panic!("original actor must complete its Driver retirement");
            }
        };
        assert_eq!(retirement.behavior.values, [77, 177]);
        assert_eq!(retirement.behavior.values.as_ptr() as usize, allocation);
        assert_eq!(retirement.behavior.observations, [ObservationId(1)]);
        let Ok(Completion::RetirementRequested(LocalRetirementRequest::CapabilityFailed(failure))) =
            retirement.disposition
        else {
            panic!("live acquisition owns the original capability task failure");
        };
        let acquired_task_id = failure.id();
        let residual = retirement.residual.settle_activation_tasks().await;
        let LocalResidual::Retired {
            interpretation: retirement_interpretation,
            source: retirement_source,
            received_interpretation: retirement_received_interpretation,
            received_source: retirement_received_source,
            source_index: retirement_source_index,
            acquired_ingress: retirement_acquired_ingress,
            terminal_report: retirement_terminal_report,
            retirement_failures: retirement_native_failures,
            settlements,
            ingress,
            activation_tasks,
            descendants,
            capability_failures,
            unread_owner_cancellation,
        } = residual
        else {
            panic!("the failed live actor retains its actual retired environment");
        };
        assert!(retirement_interpretation.is_none());
        assert!(retirement_source.is_none());
        assert!(retirement_received_interpretation.is_none());
        assert!(retirement_received_source.is_none());
        assert!(retirement_source_index.is_none());
        assert!(retirement_acquired_ingress.is_none());
        assert!(retirement_terminal_report.is_none());
        assert!(retirement_native_failures.is_empty());

        assert_eq!(settlements.len(), 0);
        assert!(ingress.control.is_empty());
        assert_eq!(ingress.user.len(), 0);
        assert!(activation_tasks.is_empty());
        let (descendants, ()) = descendants;
        assert_eq!(descendants.len(), 0);
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(failure.is_panic());
        assert_eq!(failure.id(), acquired_task_id);
        drop((
            retirement.behavior,
            failure,
            request,
            publisher,
            report_received,
        ));
    }
}

#[cfg(test)]
mod root_join_custody {
    use crate::address::MailAddr;
    use crate::interpret::ActionSettlementOf;
    use crate::launch::{InertCapabilities, spawn_local_execution};
    use crate::local::{
        CapabilityRetirement, CommitActions, LocalResidual, LocalRetirementRequest,
        OwnerCancellation, StandardIngress,
    };
    use crate::terminal::LocalOutcome;
    use crate::{ActorExecutionOutcome, ActorSpace};
    use behavior::{
        Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorSettlements, EventLayer, Here,
        InitializationTurn, InterpretationProgress, MessageProtocol, Never, NoBirths, NoSends,
        SourceProgress, SourceSettlementCustody, Step, Stopped, User,
    };
    use behavior_actors::ShutdownRequested;
    use bombay_engine::{ActionsOf, Completion};
    use communication::Config;
    use core::future::{Future, pending};
    use core::task::{Context, Poll, Waker};
    use std::sync::{Arc, Mutex, Weak};
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum RootFinish {
        Stop,
        Cancel,
        Unpublished,
    }
    #[derive(Clone, Copy, Debug)]
    enum ResultCustody {
        Retain,
        Surrender,
    }
    impl ResultCustody {
        async fn finish_root_cleanup<T>(
            self,
            receipt: JoinHandle<T>,
            release: oneshot::Sender<()>,
            released: &Weak<Vec<u8>>,
            verification: oneshot::Receiver<()>,
        ) {
            match self {
                ResultCustody::Retain => {
                    let sent = release.send(());
                    sent.expect("root cleanup remains owned");
                    let result = receipt
                        .await
                        .expect("exact root receipt remains after wait drop");
                    assert_eq!(released.strong_count(), 1);
                    drop(result);
                }
                ResultCustody::Surrender => {
                    drop(receipt);
                    assert_eq!(released.strong_count(), 1);
                    let sent = release.send(());
                    sent.expect("receiverless cleanup remains owned");
                    while released.strong_count() != 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }
            verification.await.expect("root oracles verified");
        }
    }

    struct RootState {
        values: Arc<Vec<u8>>,
        finish: RootFinish,
    }
    impl Behavior for RootState {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = EventLayer<ShutdownRequested, User<MailAddr, Never>>;
        type Sends = NoSends;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;
        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            Ok(match self.finish {
                RootFinish::Unpublished => Actions::stop(),
                _ => Actions::cont(),
            })
        }
        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            match event {
                EventLayer::Owned(_) => Ok(Actions::stop()),
                EventLayer::Inner(user) => match user.message {},
            }
        }
    }
    struct RootCleanup {
        decisions: Arc<Mutex<Vec<Step<Never, Stopped>>>>,
        initializing: Option<(Option<oneshot::Sender<()>>, oneshot::Receiver<()>)>,
        retiring: Option<oneshot::Sender<()>>,
        release: oneshot::Receiver<()>,
    }
    impl CommitActions<RootState> for RootCleanup {
        type Retired = ();
        async fn commit(
            &mut self,
            progress: &mut Option<
                InterpretationProgress<
                    ActionsOf<RootState>,
                    <RootState as BehaviorSettlements>::InterpretationCustody,
                    ActionSettlementOf<RootState>,
                >,
            >,
        ) {
            if let Some((initializing, admitted)) = self.initializing.as_mut() {
                if let Some(initializing) = initializing.take() {
                    let sent = initializing.send(());
                    sent.expect("startup observer remains");
                }
                admitted
                    .await
                    .expect("initial action interpretation is explicitly admitted");
                drop(self.initializing.take());
            }
            if let Some(InterpretationProgress::Original(actions)) = progress.as_ref() {
                assert_eq!(actions.sends, NoSends);
                assert!(actions.creates.is_empty());
                self.decisions.lock().unwrap().push(actions.become_);
            }
            ActionsOf::<RootState>::interpret::<_, <RootState as Behavior>::Event, Here>(
                progress,
                &mut InertCapabilities,
            )
            .await;
        }
        async fn offer_next(
            &mut self,
            progress: &mut Option<
                SourceProgress<
                    ActionSettlementOf<RootState>,
                    <RootState as BehaviorSettlements>::SourceCustody,
                >,
            >,
        ) {
            <ActionSettlementOf<RootState> as SourceSettlementCustody<
                InertCapabilities,
                <RootState as Behavior>::Event,
            >>::prepare_source(progress);
            if let Some(SourceProgress::Offering(custody)) = progress {
                <ActionSettlementOf<RootState> as SourceSettlementCustody<
                    InertCapabilities,
                    <RootState as Behavior>::Event,
                >>::offer_next_to_source(custody, &mut InertCapabilities)
                .await;
            }
            <ActionSettlementOf<RootState> as SourceSettlementCustody<
                InertCapabilities,
                <RootState as Behavior>::Event,
            >>::finish_source(progress);
        }
        async fn next_local_event(
            &mut self,
        ) -> Result<<RootState as Behavior>::Event, tokio::task::JoinError> {
            pending().await
        }
        async fn receive_retirement(
            interpreter: &mut Option<Self>,
            received: &mut Option<CapabilityRetirement<<RootState as Behavior>::Event, ()>>,
        ) {
            if received.is_some() {
                return;
            }
            let Some(original) = interpreter.as_mut() else {
                return;
            };
            if let Some(retiring) = original.retiring.take() {
                let sent = retiring.send(());
                sent.expect("cleanup observer remains");
            }
            (&mut original.release)
                .await
                .expect("cleanup must explicitly finish");
            *received = Some(CapabilityRetirement::without_activations(()));
            drop(interpreter.take());
        }
    }
    fn assert_root_retirement(
        outcome: &LocalOutcome<RootState, ()>,
        finish: RootFinish,
        allocation: usize,
    ) {
        let ActorExecutionOutcome::Completed {
            behavior,
            residual,
            additional_failures,
            completion,
        } = outcome
        else {
            panic!("complete actor state must survive")
        };
        assert!(additional_failures.is_empty());
        assert_eq!(behavior.values.as_slice(), [31, 37]);
        assert_eq!(behavior.finish, finish);
        assert_eq!(behavior.values.as_ptr() as usize, allocation);
        match (finish, completion) {
            (RootFinish::Stop | RootFinish::Unpublished, Completion::Stopped)
            | (
                RootFinish::Cancel,
                Completion::RetirementRequested(LocalRetirementRequest::OwnerCancellation(
                    OwnerCancellation,
                )),
            ) => {}
            _ => panic!("the exact root retirement cause must survive"),
        }
        let LocalResidual::Retired {
            interpretation,
            source,
            settlements,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            terminal_report,
            retirement_failures,
            ingress,
            activation_tasks,
            descendants,
            capability_failures,
            unread_owner_cancellation,
        } = residual
        else {
            panic!("retirement keeps all ambient fields")
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        let expected = match finish {
            RootFinish::Cancel => 0,
            _ => 1,
        };
        assert_eq!(settlements.len(), expected);
        for (index, settlement) in settlements.iter().enumerate() {
            assert_eq!(settlement.sends, NoSends);
            assert!(settlement.creations.is_empty());
            match (finish, index, &settlement.become_) {
                (RootFinish::Unpublished | RootFinish::Stop, 0, Step::Stop(_)) => {}
                _ => panic!("exact fold sequence survives"),
            }
        }
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(ingress.user.len(), 0);
        assert!(activation_tasks.is_empty());
        assert_eq!(*descendants, ());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn root_join_receipt_preserves_exact_outcome_through_cleanup() {
        for finish in [
            RootFinish::Stop,
            RootFinish::Cancel,
            RootFinish::Unpublished,
        ] {
            for custody in [ResultCustody::Retain, ResultCustody::Surrender] {
                let addresses = ActorSpace::new();
                let bytes = vec![31, 37];
                let allocation = bytes.as_ptr() as usize;
                let values = Arc::new(bytes);
                let released_root = Arc::downgrade(&values);
                let decisions = Arc::new(Mutex::new(Vec::new()));
                let interpreted = decisions.clone();
                let (initializing, initialization) = oneshot::channel();
                let (admit, admitted) = oneshot::channel();
                let (retiring, retirement) = oneshot::channel();
                let (release, released) = oneshot::channel();
                let (authority, startup, control, task) =
                    spawn_local_execution::<RootState, _, StandardIngress, _, _, _>(
                        addresses.clone(),
                        Config::new(2),
                        MailAddr::APPLICATION_ROOT,
                        RootState { values, finish },
                        |_, _, _, _| RootCleanup {
                            decisions: interpreted,
                            initializing: Some((Some(initializing), admitted)),
                            retiring: Some(retiring),
                            release: released,
                        },
                        |environment| {
                            let (publication, startup) = oneshot::channel();
                            let control = environment.shutdown_control();
                            let environment = environment.publish_with(move |actor| {
                                drop(publication.send(actor));
                            });
                            (environment, startup, control)
                        },
                    );
                let authority = match finish {
                    RootFinish::Cancel => {
                        drop(authority);
                        None
                    }
                    _ => Some(authority),
                };
                initialization.await.expect("initial actions admitted");
                assert!(addresses.resolve(&MailAddr::APPLICATION_ROOT).is_none());
                let sent = admit.send(());
                sent.expect("the original actor remains owned");
                let startup = match finish {
                    RootFinish::Cancel | RootFinish::Unpublished => Some(startup),
                    RootFinish::Stop => {
                        let actor = startup.await.expect("continuing root is published");
                        let control = control.upgrade().expect("the actor owns control");
                        let sent = control.send(EventLayer::Owned(ShutdownRequested));
                        sent.expect("the root accepts shutdown");
                        drop(actor);
                        None
                    }
                };
                retirement.await.expect("actual root cleanup begins");
                let expected_decisions = match finish {
                    RootFinish::Stop => vec![Step::Continue, Step::Stop(Stopped)],
                    RootFinish::Cancel => vec![Step::Continue],
                    RootFinish::Unpublished => vec![Step::Stop(Stopped)],
                };
                assert_eq!(*decisions.lock().unwrap(), expected_decisions);
                let (verified, verification) = oneshot::channel();
                let mut receipt = tokio::spawn(async move {
                    let outcome = task.await.expect("actor joined");
                    let startup_rejection = match startup {
                        Some(startup) => Some(startup.await.expect_err("root unpublished")),
                        None => None,
                    };
                    assert_root_retirement(&outcome, finish, allocation);
                    let notified = verified.send(());
                    notified.expect("the root oracle receiver remains");
                    (outcome, startup_rejection)
                });
                let mut waiting = Box::pin(&mut receipt);
                let polled = waiting
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()));
                assert!(matches!(polled, Poll::Pending));
                drop(waiting);
                assert_eq!(released_root.strong_count(), 1);
                custody
                    .finish_root_cleanup(receipt, release, &released_root, verification)
                    .await;
                assert_eq!(released_root.strong_count(), 0);
                assert!(addresses.resolve(&MailAddr::APPLICATION_ROOT).is_none());
                drop(authority);
            }
        }
    }
}
#[cfg(test)]
mod installed_shutdown_contract {
    use behavior::{
        Actions, ActiveTurn, Become, Behavior, BehaviorActed, BehaviorBase, ChildCreationOutcome,
        ChildHead, CreateChild, CreationKind, CreationSequence, EstablishChild, EventLayer, Here,
        Ingress, Inside, InterpretItem, ItemSettlement, MessageProtocol, Never, NoBirths, NoSends,
        RoutedCreation, Step, Stopped, User, delegate_transition,
    };
    use behavior_actors::{
        EstablishedShutdownResolved, Exit, ShutdownEstablished, ShutdownId, ShutdownRejection,
        ShutdownRequested, StopOnShutdown,
    };
    use bombay_engine::Completion;
    use communication::{Config, mailbox_channel};
    use core::future::Future;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;

    use super::{
        ApplicationCapabilities, ApplicationCapabilityInputs, ApplicationLifecycle, NoParent,
        StructuralOrigins,
    };
    use crate::ActorExecutionOutcome;
    use crate::ActorSpace;
    use crate::actor;
    use crate::actor_interface::{ActorInterface, ExtractLocalEndpoint};
    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::child_bindings::{ChildBinding, NoChildBindings, RetireChildTasks};
    use crate::launch::launch_inert;
    use crate::local::LocalResidual;
    use crate::observation::TerminationObservations;
    use crate::reports::LocalTerminalReports;
    use crate::terminal::{ActorRetirement, ChildOrigin, ProjectTerminal};
    use crate::time::LocalTimers;

    struct ShutdownLedger {
        entries: Vec<u64>,
        received: Vec<User<MailAddr, Vec<u64>>>,
    }
    #[actor]
    impl ShutdownLedger {
        fn receive(&mut self, from: MailAddr, entries: Vec<u64>) -> BehaviorActed<Self> {
            self.received.push(User::new(from, entries));
            Ok(Actions::cont())
        }
    }
    type DirectLedger = StopOnShutdown<ShutdownLedger>;
    type NestedLedger = StopOnShutdown<DirectLedger>;
    type LedgerProtocol = ShutdownLedger;

    #[derive(Default)]
    struct ShutdownObserver {
        resolutions: Vec<(ShutdownId, Result<(), ShutdownRejection>)>,
    }
    impl Behavior for ShutdownObserver {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = EventLayer<EstablishedShutdownResolved<LedgerProtocol>, User<MailAddr, Never>>;
        type Sends = NoSends;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;
        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            match event {
                EventLayer::Owned(EstablishedShutdownResolved::Accepted { id, .. }) => {
                    self.resolutions.push((id, Ok(())));
                }
                EventLayer::Owned(EstablishedShutdownResolved::Rejected { id, reason, .. }) => {
                    self.resolutions.push((id, Err(reason)));
                }
                EventLayer::Inner(user) => match user.message {},
            }
            Ok(Actions::cont())
        }
    }
    impl BehaviorBase for ShutdownObserver {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }
    enum ShutdownChildren {
        Direct {
            origin: ChildOrigin<ShutdownObserver, ChildHead>,
            retirement: ActorRetirement<DirectLedger, Self, ()>,
        },
        Nested {
            origin: ChildOrigin<ShutdownObserver, ChildHead>,
            retirement: ActorRetirement<NestedLedger, Self, ()>,
        },
    }
    impl
        ProjectTerminal<
            ChildOrigin<ShutdownObserver, ChildHead>,
            ActorRetirement<DirectLedger, Self, ()>,
        > for ShutdownChildren
    {
        fn project(
            origin: ChildOrigin<ShutdownObserver, ChildHead>,
            retirement: ActorRetirement<DirectLedger, Self, ()>,
        ) -> Self {
            Self::Direct { origin, retirement }
        }
    }
    impl
        ProjectTerminal<
            ChildOrigin<ShutdownObserver, ChildHead>,
            ActorRetirement<NestedLedger, Self, ()>,
        > for ShutdownChildren
    {
        fn project(
            origin: ChildOrigin<ShutdownObserver, ChildHead>,
            retirement: ActorRetirement<NestedLedger, Self, ()>,
        ) -> Self {
            Self::Nested { origin, retirement }
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep complete commitment, typed transfer, and both original terminal products in one trace."
    )]
    async fn committed_distinct_event_targets_transfer_to_unrelated_shutdown_interpreter() {
        let allocations = ApplicationAddresses::new();
        let (direct_control, direct_owner, direct_mailbox, direct_receiver) =
            mailbox_channel::<<ShutdownObserver as Behavior>::Event, User<MailAddr, Never>>(
                Config::new(1),
            );
        let (direct_terminal, direct_report) = oneshot::channel();
        let mut direct_creator = ApplicationCapabilities::<
            ShutdownObserver,
            (),
            NoParent,
            ChildBinding<
                ChildHead,
                DirectLedger,
                ShutdownChildren,
                StructuralOrigins<ShutdownObserver>,
                NoChildBindings<ShutdownChildren>,
            >,
            StructuralOrigins<ShutdownObserver>,
        >::new_with_bindings(
            ApplicationCapabilityInputs {
                address: MailAddr(501),
                actor_spaces: Arc::new(()),
                allocations: allocations.clone(),
                control: direct_control,
                timers: LocalTimers::new(),
                observations: TerminationObservations::new(),
                terminal_reports: LocalTerminalReports::new(direct_terminal),
            },
            ChildBinding::default(),
        );
        let (nested_control, nested_owner, nested_mailbox, nested_receiver) =
            mailbox_channel::<<ShutdownObserver as Behavior>::Event, User<MailAddr, Never>>(
                Config::new(1),
            );
        let (nested_terminal, nested_report) = oneshot::channel();
        let mut nested_creator = ApplicationCapabilities::<
            ShutdownObserver,
            (),
            NoParent,
            ChildBinding<
                ChildHead,
                NestedLedger,
                ShutdownChildren,
                StructuralOrigins<ShutdownObserver>,
                NoChildBindings<ShutdownChildren>,
            >,
            StructuralOrigins<ShutdownObserver>,
        >::new_with_bindings(
            ApplicationCapabilityInputs {
                address: MailAddr(503),
                actor_spaces: Arc::new(()),
                allocations,
                control: nested_control,
                timers: LocalTimers::new(),
                observations: TerminationObservations::new(),
                terminal_reports: LocalTerminalReports::new(nested_terminal),
            },
            ChildBinding::default(),
        );
        let mut creations = CreationSequence::new();
        let direct_id = creations.issue().expect("direct child identity");
        let nested_id = creations.issue().expect("nested child identity");
        let direct_entries = vec![11, 13];
        let direct_allocation = direct_entries.as_ptr();
        let direct = EstablishChild::<ChildHead, DirectLedger>::establish_child(
            &mut direct_creator,
            RoutedCreation::new(
                CreateChild::birth(
                    direct_id,
                    StopOnShutdown::new(ShutdownLedger {
                        entries: direct_entries,
                        received: Vec::new(),
                    }),
                ),
                0,
            ),
        )
        .await;
        let ItemSettlement::Accepted(direct) = direct else {
            panic!("actual direct child commitment")
        };
        let ChildCreationOutcome::Established(committed) = &direct else {
            panic!("actual committed direct child")
        };
        assert_eq!(committed.id(), direct_id);
        assert_eq!(committed.kind(), CreationKind::Birth);
        let direct = direct
            .into_actor()
            .unwrap_or_else(|_| panic!("original direct installed authority"));
        let nested_entries = vec![17, 19];
        let nested_allocation = nested_entries.as_ptr();
        let nested = EstablishChild::<ChildHead, NestedLedger>::establish_child(
            &mut nested_creator,
            RoutedCreation::new(
                CreateChild::birth(
                    nested_id,
                    StopOnShutdown::new(StopOnShutdown::new(ShutdownLedger {
                        entries: nested_entries,
                        received: Vec::new(),
                    })),
                ),
                1,
            ),
        )
        .await;
        let ItemSettlement::Accepted(nested) = nested else {
            panic!("actual nested child commitment")
        };
        let ChildCreationOutcome::Established(committed) = &nested else {
            panic!("actual committed nested child")
        };
        assert_eq!(committed.id(), nested_id);
        assert_eq!(committed.kind(), CreationKind::Birth);
        let nested = nested
            .into_actor()
            .unwrap_or_else(|_| panic!("original nested installed authority"));
        let direct_endpoint = direct.recipient().interpret(&mut ExtractLocalEndpoint);
        let nested_endpoint = nested.recipient().interpret(&mut ExtractLocalEndpoint);
        let (observer_control, observer_owner, observer_mailbox, mut observer_receiver) =
            mailbox_channel::<<ShutdownObserver as Behavior>::Event, User<MailAddr, Never>>(
                Config::new(1),
            );
        let (observer_terminal, observer_report) = oneshot::channel();
        let mut observer = ApplicationCapabilities::<
            ShutdownObserver,
            (),
            NoParent,
            NoChildBindings,
        >::new_with_bindings(
            ApplicationCapabilityInputs {
                address: MailAddr(509),
                actor_spaces: Arc::new(()),
                allocations: ApplicationAddresses::new(),
                control: observer_control,
                timers: LocalTimers::new(),
                observations: TerminationObservations::new(),
                terminal_reports: LocalTerminalReports::new(observer_terminal),
            },
            NoChildBindings::default(),
        );
        let direct_request = ShutdownEstablished::new(
            ShutdownId(23),
            direct.clone(),
            Ingress::<ShutdownRequested, Here>::new(),
        );
        let mut shutdown_request = Some(direct_request);
        let mut accepted = None;
        InterpretItem::<
            ShutdownEstablished<DirectLedger, Here>,
            <ShutdownObserver as Behavior>::Event,
            Here,
        >::interpret_item(&mut observer, &mut shutdown_request, &mut accepted)
        .await;
        let Some(accepted) = accepted else {
            panic!("actual native shutdown admission reply")
        };
        assert!(shutdown_request.is_none());
        assert!(matches!(accepted, ItemSettlement::Accepted(ShutdownId(23))));
        let nested_request = ShutdownEstablished::new(
            ShutdownId(29),
            nested.clone(),
            Ingress::<ShutdownRequested, Inside<Here>>::new(),
        );
        let mut shutdown_request = Some(nested_request);
        let mut accepted = None;
        InterpretItem::<
            ShutdownEstablished<NestedLedger, Inside<Here>>,
            <ShutdownObserver as Behavior>::Event,
            Here,
        >::interpret_item(&mut observer, &mut shutdown_request, &mut accepted)
        .await;
        let Some(accepted) = accepted else {
            panic!("actual native shutdown admission reply")
        };
        assert!(shutdown_request.is_none());
        assert!(matches!(accepted, ItemSettlement::Accepted(ShutdownId(29))));
        let direct_stopped = direct_endpoint.termination().await;
        let nested_stopped = nested_endpoint.termination().await;
        assert_eq!(direct_stopped, Ok(Exit::Normal));
        assert_eq!(nested_stopped, Ok(Exit::Normal));
        for id in [31, 37] {
            let request = ShutdownEstablished::new(
                ShutdownId(id),
                nested.clone(),
                Ingress::<ShutdownRequested, Inside<Here>>::new(),
            );
            let mut shutdown_request = Some(request);
            let mut rejected = None;
            InterpretItem::<
                ShutdownEstablished<NestedLedger, Inside<Here>>,
                <ShutdownObserver as Behavior>::Event,
                Here,
            >::interpret_item(&mut observer, &mut shutdown_request, &mut rejected)
            .await;
            let Some(rejected) = rejected else {
                panic!("actual native shutdown admission reply")
            };
            assert!(shutdown_request.is_none());
            let ItemSettlement::Rejected { item, reason } = rejected else {
                panic!("exact stopped nested request")
            };
            assert_eq!(item.id, ShutdownId(id));
            let rejected_endpoint = item
                .actor()
                .recipient()
                .interpret(&mut ExtractLocalEndpoint);
            assert_eq!(rejected_endpoint.address(), nested_endpoint.address());
            let rejected_terminal = rejected_endpoint.termination().await;
            assert_eq!(rejected_terminal, Ok(Exit::Normal));
            assert_eq!(reason, ShutdownRejection::AlreadyStopped);
            let mut shutdown_request = Some(item);
            let mut retry = None;
            InterpretItem::<
                ShutdownEstablished<NestedLedger, Inside<Here>>,
                <ShutdownObserver as Behavior>::Event,
                Here,
            >::interpret_item(&mut observer, &mut shutdown_request, &mut retry)
            .await;
            let Some(retry) = retry else {
                panic!("actual native shutdown admission reply")
            };
            assert!(shutdown_request.is_none());
            let ItemSettlement::Rejected { item, reason } = retry else {
                panic!("replayed original stopped request")
            };
            assert_eq!(item.id, ShutdownId(id));
            assert_eq!(reason, ShutdownRejection::AlreadyStopped);
            let replayed_endpoint = item
                .actor()
                .recipient()
                .interpret(&mut ExtractLocalEndpoint);
            assert_eq!(replayed_endpoint.address(), nested_endpoint.address());
            let replayed_terminal = replayed_endpoint.termination().await;
            assert_eq!(replayed_terminal, Ok(Exit::Normal));
        }
        let mut resolution_ledger = ShutdownObserver::default();
        for _ in 0..6 {
            let event = observer_receiver
                .recv_control()
                .await
                .expect("one complete immediate resolution");
            let actions = delegate_transition(&mut resolution_ledger, event).unwrap();
            assert!(actions.creates.is_empty());
            assert!(matches!(actions.sends, NoSends));
            assert!(matches!(actions.become_, Step::Continue));
        }
        assert_eq!(
            resolution_ledger.resolutions,
            [
                (ShutdownId(23), Ok(())),
                (ShutdownId(29), Ok(())),
                (ShutdownId(31), Err(ShutdownRejection::AlreadyStopped)),
                (ShutdownId(31), Err(ShutdownRejection::AlreadyStopped)),
                (ShutdownId(37), Err(ShutdownRejection::AlreadyStopped)),
                (ShutdownId(37), Err(ShutdownRejection::AlreadyStopped))
            ]
        );
        let (direct_retired, (direct_failures, ())) = direct_creator
            .child_bindings
            .take()
            .expect("original child bindings remain installed")
            .retire_child_tasks()
            .await;
        let (nested_retired, (nested_failures, ())) = nested_creator
            .child_bindings
            .take()
            .expect("original child bindings remain installed")
            .retire_child_tasks()
            .await;
        assert!(direct_failures.is_empty());
        assert!(nested_failures.is_empty());
        let [ShutdownChildren::Direct { origin, retirement }] = direct_retired.as_slice() else {
            panic!("one original direct result")
        };
        assert_eq!(origin.address(), direct_endpoint.address());
        assert_eq!(origin.nonce(), 0);
        let ActorRetirement::Completed {
            interpretation: retirement_interpretation,
            source: retirement_source,
            received_interpretation: retirement_received_interpretation,
            received_source: retirement_received_source,
            source_index: retirement_source_index,
            acquired_ingress: retirement_acquired_ingress,
            terminal_report: retirement_terminal_report,
            retirement_failures: retirement_native_failures,
            additional_failures: retirement_additional_failures,
            child_failures: (),
            behavior,
            settlements,
            control,
            user,
            descendants,
            completion,
            capability_failures,
            unread_owner_cancellation,
        } = retirement
        else {
            panic!("direct completed result")
        };
        assert!(retirement_interpretation.is_none());
        assert!(retirement_source.is_none());
        assert!(retirement_received_interpretation.is_none());
        assert!(retirement_received_source.is_none());
        assert!(retirement_source_index.is_none());
        assert!(retirement_acquired_ingress.is_none());
        assert!(retirement_terminal_report.is_none());
        assert!(retirement_native_failures.is_empty());
        assert!(retirement_additional_failures.is_empty());

        assert_eq!(behavior.base().entries, [11, 13]);
        assert_eq!(behavior.base().entries.as_ptr(), direct_allocation);
        assert_eq!(behavior.base().received.len(), 0);
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(*completion, Completion::Stopped);
        assert!(control.is_empty() && user.is_empty() && descendants.is_empty());
        assert_eq!(settlements.len(), 1);
        assert!(settlements[0].creations.is_empty());
        assert!(matches!(settlements[0].sends.owned, NoSends));
        assert!(matches!(settlements[0].sends.inner, NoSends));
        assert!(matches!(settlements[0].become_, Step::Stop(_)));
        let [ShutdownChildren::Nested { origin, retirement }] = nested_retired.as_slice() else {
            panic!("one original nested result")
        };
        assert_eq!(origin.address(), nested_endpoint.address());
        assert_eq!(origin.nonce(), 1);
        let ActorRetirement::Completed {
            interpretation: retirement_interpretation,
            source: retirement_source,
            received_interpretation: retirement_received_interpretation,
            received_source: retirement_received_source,
            source_index: retirement_source_index,
            acquired_ingress: retirement_acquired_ingress,
            terminal_report: retirement_terminal_report,
            retirement_failures: retirement_native_failures,
            additional_failures: retirement_additional_failures,
            child_failures: (),
            behavior,
            settlements,
            control,
            user,
            descendants,
            completion,
            capability_failures,
            unread_owner_cancellation,
        } = retirement
        else {
            panic!("nested completed result")
        };
        assert!(retirement_interpretation.is_none());
        assert!(retirement_source.is_none());
        assert!(retirement_received_interpretation.is_none());
        assert!(retirement_received_source.is_none());
        assert!(retirement_source_index.is_none());
        assert!(retirement_acquired_ingress.is_none());
        assert!(retirement_terminal_report.is_none());
        assert!(retirement_native_failures.is_empty());
        assert!(retirement_additional_failures.is_empty());

        assert_eq!(behavior.base().entries, [17, 19]);
        assert_eq!(behavior.base().entries.as_ptr(), nested_allocation);
        assert_eq!(behavior.base().received.len(), 0);
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(*completion, Completion::Stopped);
        assert!(control.is_empty() && user.is_empty() && descendants.is_empty());
        assert_eq!(settlements.len(), 1);
        assert!(settlements[0].creations.is_empty());
        assert!(matches!(settlements[0].sends.owned, NoSends));
        assert!(matches!(settlements[0].sends.inner.owned, NoSends));
        assert!(matches!(settlements[0].sends.inner.inner, NoSends));
        assert!(matches!(settlements[0].become_, Step::Stop(_)));
        drop((
            direct_owner,
            direct_mailbox,
            direct_receiver,
            direct_report,
            nested_owner,
            nested_mailbox,
            nested_receiver,
            nested_report,
            observer_owner,
            observer_mailbox,
            observer_report,
        ));
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep both joined incarnations and stale-view observations in one trace."
    )]
    async fn old_root_lifecycle_cannot_close_actual_same_address_replacement() {
        let actors = ActorSpace::<LedgerProtocol>::new();
        let address = MailAddr(541);
        let old_actions = Arc::new(Mutex::new(Vec::<Become<Never>>::new()));
        let recording = Arc::clone(&old_actions);
        let old = launch_inert(
            actors.clone(),
            Config::new(1),
            address,
            StopOnShutdown::new(ShutdownLedger {
                entries: vec![43],
                received: Vec::new(),
            }),
            move |actions| {
                assert!(actions.creates.is_empty());
                assert!(matches!(actions.sends.owned, NoSends));
                assert!(matches!(actions.sends.inner, NoSends));
                recording
                    .lock()
                    .expect("complete action observation")
                    .push(actions.become_);
            },
        )
        .await
        .unwrap_or_else(|_| panic!("old root launch"));
        let lifecycle = ApplicationLifecycle {
            root: old.actor.clone(),
            control: old.shutdown_control.clone(),
        };
        let repeated_lifecycle = lifecycle.clone();
        let accepted = lifecycle.request_shutdown();
        let repeated = repeated_lifecycle.request_shutdown();
        assert_eq!(accepted, Ok(()));
        assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopping));
        let old_termination = lifecycle.termination().await;
        assert_eq!(old_termination, Ok(Exit::Normal));
        let old_result = old.task.finish().await;
        let Ok(ActorExecutionOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    interpretation,
                    source,
                    settlements,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    terminal_report,
                    retirement_failures,
                    ingress,
                    descendants,
                    activation_tasks,
                    capability_failures,
                    unread_owner_cancellation,
                },
            additional_failures,
            completion,
        }) = old_result
        else {
            panic!("joined old root")
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert_eq!(behavior.base().entries, [43]);
        assert_eq!(behavior.base().received.len(), 0);
        assert!(ingress.control.is_empty() && ingress.user.is_empty());
        assert_eq!(descendants, ());
        assert!(activation_tasks.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(matches!(completion, Completion::Stopped));
        assert_eq!(settlements.len(), 1);
        assert!(settlements[0].creations.is_empty());
        assert!(matches!(settlements[0].sends.owned, NoSends));
        assert!(matches!(settlements[0].sends.inner, NoSends));
        assert!(matches!(settlements[0].become_, Step::Stop(_)));
        assert_eq!(
            *old_actions.lock().expect("old full actions"),
            [Step::Continue, Step::Stop(Stopped)]
        );
        let absent = actors.resolve(&address);
        assert!(absent.is_none());
        assert!(lifecycle.control.upgrade().is_none());
        let fresh_actions = Arc::new(Mutex::new(Vec::<Become<Never>>::new()));
        let recording = Arc::clone(&fresh_actions);
        let fresh = launch_inert(
            actors.clone(),
            Config::new(1),
            address,
            StopOnShutdown::new(ShutdownLedger {
                entries: vec![47],
                received: Vec::new(),
            }),
            move |actions| {
                assert!(actions.creates.is_empty());
                assert!(matches!(actions.sends.owned, NoSends));
                assert!(matches!(actions.sends.inner, NoSends));
                recording
                    .lock()
                    .expect("complete action observation")
                    .push(actions.become_);
            },
        )
        .await
        .unwrap_or_else(|_| panic!("fresh root claim"));
        let fresh_lifecycle = ApplicationLifecycle {
            root: fresh.actor.clone(),
            control: fresh.shutdown_control.clone(),
        };
        let old_request = lifecycle.request_shutdown();
        let replay = repeated_lifecycle.request_shutdown();
        assert_eq!(old_request, Err(ShutdownRejection::AlreadyStopped));
        assert_eq!(replay, Err(ShutdownRejection::AlreadyStopped));
        let mut fresh_termination = pin!(fresh_lifecycle.termination());
        let mut context = Context::from_waker(Waker::noop());
        let waiting = Future::poll(fresh_termination.as_mut(), &mut context);
        assert!(matches!(waiting, Poll::Pending));
        let values = vec![53, 59];
        let allocation = values.as_ptr();
        let delivered = fresh.actor.send_from(MailAddr(61), values).await;
        assert!(delivered.is_ok());
        let new_request = fresh_lifecycle.request_shutdown();
        assert_eq!(new_request, Ok(()));
        let stopped = fresh_termination.await;
        assert_eq!(stopped, Ok(Exit::Normal));
        let retired = fresh.task.finish().await;
        let Ok(ActorExecutionOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    interpretation,
                    source,
                    settlements,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    terminal_report,
                    retirement_failures,
                    ingress,
                    descendants,
                    activation_tasks,
                    capability_failures,
                    unread_owner_cancellation,
                },
            additional_failures,
            completion,
        }) = retired
        else {
            panic!("whole joined replacement")
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert_eq!(behavior.base().entries, [47]);
        assert!(activation_tasks.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(matches!(completion, Completion::Stopped));
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(descendants, ());
        let mut expected_actions = vec![Step::Continue; 1 + behavior.base().received.len()];
        expected_actions.push(Step::Stop(Stopped));
        assert_eq!(
            *fresh_actions.lock().expect("fresh full actions"),
            expected_actions
        );
        let mut originals = behavior.base().received.iter().chain(ingress.user.iter());
        let original = originals.next().expect("one admitted original user input");
        assert_eq!(original.from, MailAddr(61));
        assert_eq!(original.message, [53, 59]);
        assert_eq!(original.message.as_ptr(), allocation);
        assert!(originals.next().is_none());
        assert_ne!(settlements.len(), 0);
        let final_turn = settlements.len() - 1;
        for (turn, settlement) in settlements.into_iter().enumerate() {
            assert!(settlement.creations.is_empty());
            assert!(matches!(settlement.sends.owned, NoSends));
            assert!(matches!(settlement.sends.inner, NoSends));
            match settlement.become_ {
                Step::Continue => assert!(turn < final_turn),
                Step::Stop(_) => assert_eq!(turn, final_turn),
            }
        }
        let absent = actors.resolve(&address);
        assert!(absent.is_none());
        let fresh_replay = fresh_lifecycle.request_shutdown();
        assert_eq!(fresh_replay, Err(ShutdownRejection::AlreadyStopped));
        let old_replay = lifecycle.request_shutdown();
        assert_eq!(old_replay, Err(ShutdownRejection::AlreadyStopped));
    }

    #[tokio::test]
    async fn external_customer_owns_admission_without_behavior_shutdown() {
        let interface = ActorInterface::new((), ApplicationAddresses::new());
        let mut external = interface
            .external::<LedgerProtocol>()
            .expect("real external endpoint");
        let endpoint = external.recipient().interpret(&mut ExtractLocalEndpoint);
        let values = vec![127, 131];
        let allocation = values.as_ptr();
        let accepted = endpoint.send_from(MailAddr(137), values).await;
        assert!(accepted.is_ok());
        external.close_admission();
        let values = vec![139, 149];
        let allocation_rejected = values.as_ptr();
        let rejected = endpoint.send_from(MailAddr(151), values).await;
        let original = rejected
            .expect_err("external owner closed admission")
            .into_message();
        assert_eq!(original, [139, 149]);
        assert_eq!(original.as_ptr(), allocation_rejected);
        let admitted = external
            .receive()
            .await
            .expect("exact admitted external message");
        assert_eq!(admitted.from, MailAddr(137));
        assert_eq!(admitted.message, [127, 131]);
        assert_eq!(admitted.message.as_ptr(), allocation);
        let exhausted = external.receive().await;
        assert!(exhausted.is_none());
        drop(external);
        let terminal = endpoint.termination().await;
        assert_eq!(terminal, Ok(Exit::Normal));
    }
}

#[cfg(test)]
mod parent_conversion_custody {
    use std::panic::panic_any;
    use std::sync::{Arc, Mutex, PoisonError};

    use behavior::{
        Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, ChildChoice, ChildCons,
        ChildCreationOutcome, ChildHead, ChildProduct, Children, ComposedEvent, CreationId,
        CreationKind, CreationSequence, CreationSettlement, Creations, EstablishedCreation,
        EstablishedDelivery, EstablishedRecipient, Here, InitializationTurn, InjectEvent,
        Interpretation, InterpretationProgress, InterpreterRequests, ItemSettlement,
        MessageProtocol, Never, NoBirths, NoChildren, NoSends, RetirementBirths, SendEffects,
        SendLayer, SettledItem, Step, Stopped, User, UserEvent,
    };
    use behavior_actors::ObserveEstablishedCreation;
    use bombay_engine::Completion;
    use communication::{Config, Received, mailbox_channel};
    use tokio::runtime::Builder;
    use tokio::sync::oneshot;

    use super::{
        ApplicationCapabilities, ApplicationCapabilityInputs, NoParent, StructuralOrigins,
    };
    use crate::actor_interface::ExtractLocalEndpoint;
    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::child_bindings::ChildBindings;
    use crate::interpret::ActionInterpreter;
    use crate::launch::spawn_local_execution;
    use crate::local::{ActorRef, Admission, LocalResidual, StandardIngress};
    use crate::observe;
    use crate::terminal::{ActorRetirement, ChildOrigin, ProjectTerminal};
    use crate::topology::HostedActorSpaces;
    use crate::{ActorExecutionOutcome, ActorSpace};

    static CONVERSION_SERIAL: Mutex<()> = Mutex::new(());
    #[expect(
        clippy::type_complexity,
        reason = "original report and panic payload have one affine custody"
    )]
    static CONVERSION_CUSTODY: Mutex<
        Option<(
            oneshot::Sender<EstablishedCreation<Child, ChildHead>>,
            Arc<Vec<u8>>,
        )>,
    > = Mutex::new(None);
    #[expect(
        clippy::type_complexity,
        reason = "child verification keeps its original publication and allocation"
    )]
    static CHILD_RETIREMENT: Mutex<
        Option<(oneshot::Sender<ChildOrigin<Parent, ChildHead>>, usize)>,
    > = Mutex::new(None);

    #[derive(Clone, Copy)]
    enum ParentCommand {
        Inspect,
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Notice {
        Occupied(u8),
        BirthsCommitted,
    }

    type NoticeProtocol = MessageProtocol<MailAddr, Notice>;
    type ParentProtocol = MessageProtocol<MailAddr, ParentCommand>;
    type ParentChildren = ChildCons<MailAddr, Child, NoChildren>;
    type ParentSends = SendLayer<
        InterpreterRequests<ObserveEstablishedCreation<Child, ChildHead>>,
        Vec<EstablishedDelivery<NoticeProtocol>>,
    >;

    enum ParentEvent {
        User(User<MailAddr, ParentCommand>),
        CapabilityReturned(Arc<Vec<u8>>),
    }

    impl UserEvent for ParentEvent {
        type Addr = MailAddr;
        type Message = ParentCommand;

        fn user(from: MailAddr, message: ParentCommand) -> Self {
            Self::User(User::new(from, message))
        }

        fn into_user(self) -> Result<User<MailAddr, ParentCommand>, Self> {
            match self {
                Self::User(user) => Ok(user),
                returned @ Self::CapabilityReturned(_) => Err(returned),
            }
        }
    }

    impl ComposedEvent for ParentEvent {
        type Inner = User<MailAddr, ParentCommand>;

        fn from_inner(event: Self::Inner) -> Self {
            Self::User(event)
        }
    }

    impl InjectEvent<EstablishedCreation<Child, ChildHead>, Here> for ParentEvent {
        fn inject_at(report: EstablishedCreation<Child, ChildHead>) -> Self {
            let custody = CONVERSION_CUSTODY
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
                .expect("the dedicated conversion owns its original publication and payload");
            let (publication, payload) = custody;
            let admitted = publication.send(report);
            match admitted {
                Ok(()) => {}
                Err(rejected) => {
                    drop(rejected);
                    panic!("the original report receiver must remain alive");
                }
            }
            // This application conversion is outside every Behavior fold.
            // No inspection of the resulting native payload is performed.
            panic_any(payload);
        }
    }

    struct Child {
        entries: Arc<Vec<u8>>,
    }

    impl Behavior for Child {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = User<MailAddr, Never>;
        type Sends = NoSends;
        type Birth = NoBirths;
        type Ph = Never;
        type Error = Never;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            Ok(Actions::stop())
        }

        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            match event.message {}
        }
    }

    impl BehaviorBase for Child {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }

    struct Parent {
        entries: Arc<Vec<u8>>,
        child: Option<Child>,
        child_id: CreationId,
        recipient: EstablishedRecipient<NoticeProtocol>,
        inspection: Option<ParentCommand>,
        turns: usize,
        returned: Vec<Arc<Vec<u8>>>,
    }

    impl Behavior for Parent {
        type Protocol = ParentProtocol;
        type Event = ParentEvent;
        type Sends = ParentSends;
        type Birth = RetirementBirths<<ParentChildren as ChildProduct<MailAddr>>::Choice>;
        type Ph = Never;
        type Error = Never;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            let child = self.child.take().expect("one original declared child");
            let creates = Children::<MailAddr>::new()
                .child(self.child_id, child)
                .into_creates();
            let deliveries = vec![EstablishedDelivery::new(
                self.recipient.clone(),
                Notice::BirthsCommitted,
            )];
            let requests = match self.inspection.take() {
                Some(ParentCommand::Inspect) => InterpreterRequests::one(
                    ObserveEstablishedCreation::<Child, ChildHead>::new(self.child_id),
                ),
                None => InterpreterRequests::empty(),
            };
            let sends = ParentSends::new(requests, deliveries);
            Ok(Actions::new(sends, creates, Step::Continue))
        }

        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            match event {
                ParentEvent::User(User {
                    message: ParentCommand::Inspect,
                    ..
                }) => {
                    self.turns += 1;
                    let requests =
                        InterpreterRequests::one(
                            ObserveEstablishedCreation::<Child, ChildHead>::new(self.child_id),
                        );
                    let sends = ParentSends::new(requests, Vec::new());
                    Ok(Actions::new(sends, Creations::default(), Step::Continue))
                }
                ParentEvent::CapabilityReturned(entries) => {
                    self.returned.push(entries);
                    Ok(Actions::cont())
                }
            }
        }
    }

    impl BehaviorBase for Parent {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }

    struct ChildTerminal {
        origin: ChildOrigin<Parent, ChildHead>,
        retirement: ActorRetirement<Child, Self, ()>,
    }

    impl ProjectTerminal<ChildOrigin<Parent, ChildHead>, ActorRetirement<Child, Self, ()>>
        for ChildTerminal
    {
        fn project(
            origin: ChildOrigin<Parent, ChildHead>,
            retirement: ActorRetirement<Child, Self, ()>,
        ) -> Self {
            let observation = CHILD_RETIREMENT
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
                .expect("the original child owns its verification publication");
            // The acknowledgement closes if any complete retirement oracle panics.
            let (publication, allocation) = observation;
            let ActorRetirement::Completed {
                interpretation: retirement_interpretation,
                source: retirement_source,
                received_interpretation: retirement_received_interpretation,
                received_source: retirement_received_source,
                source_index: retirement_source_index,
                acquired_ingress: retirement_acquired_ingress,
                terminal_report: retirement_terminal_report,
                retirement_failures: retirement_native_failures,
                additional_failures: retirement_additional_failures,
                child_failures: (),
                behavior,
                settlements,
                control,
                user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
                completion,
            } = &retirement
            else {
                panic!("the pure child stops in its ordinary initialization");
            };
            assert!(retirement_interpretation.is_none());
            assert!(retirement_source.is_none());
            assert!(retirement_received_interpretation.is_none());
            assert!(retirement_received_source.is_none());
            assert!(retirement_source_index.is_none());
            assert!(retirement_acquired_ingress.is_none());
            assert!(retirement_terminal_report.is_none());
            assert!(retirement_native_failures.is_empty());
            assert!(retirement_additional_failures.is_empty());

            assert_eq!(behavior.entries.as_slice(), [31, 37, 41]);
            assert_eq!(behavior.entries.as_ptr() as usize, allocation);
            assert_eq!(completion, &Completion::Stopped);
            assert_eq!(settlements.len(), 1);
            assert_eq!(control.as_slice(), []);
            assert_eq!(user.as_slice(), []);
            assert!(descendants.is_empty());
            assert!(capability_failures.is_empty());
            assert!(unread_owner_cancellation.is_none());
            let settlement = &settlements[0];
            assert!(settlement.creations.is_empty());
            assert!(matches!(settlement.sends, NoSends));
            assert!(matches!(settlement.become_, Step::Stop(Stopped)));
            let verified = publication.send(origin);
            match verified {
                Ok(()) => {}
                Err(rejected) => panic!("the host retains the child verifier: {rejected:?}"),
            }
            // No await follows publication: on the current-thread runtime this
            // poll returns the complete original terminal before the host resumes.
            Self { origin, retirement }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one complete actor cleanup precedes every ownership assertion"
    )]
    fn assert_parent_conversion_preserves_owned_retirement(inspection: Option<ParentCommand>) {
        let _serial = CONVERSION_SERIAL
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the actual local actor runtime");
        let original_parent = Arc::new(vec![43, 47, 53]);
        let original_child = Arc::new(vec![31, 37, 41]);
        let original_capability = Arc::new(vec![59, 61, 67]);
        let original_payload = Arc::new(vec![71, 73, 79]);
        let parent_allocation = original_parent.as_ptr() as usize;
        let child_allocation = original_child.as_ptr() as usize;
        let capability_allocation = original_capability.as_ptr() as usize;
        let payload_allocation = original_payload.as_ptr() as usize;
        let parent_owner = Arc::downgrade(&original_parent);
        let child_owner = Arc::downgrade(&original_child);
        let capability_owner = Arc::downgrade(&original_capability);
        let payload_owner = Arc::downgrade(&original_payload);
        let payload_before_cleanup = payload_owner.clone();
        let (report_publication, report_receiver) = oneshot::channel();
        let (child_publication, child_receiver) = oneshot::channel();
        *CONVERSION_CUSTODY
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((report_publication, original_payload));
        *CHILD_RETIREMENT
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((child_publication, child_allocation));
        let mut creations = CreationSequence::new();
        let id = creations.issue().expect("one actual original creation ID");
        let roots = ActorSpace::<ParentProtocol>::new();
        let addresses = ApplicationAddresses::new();
        let notice_address = addresses.allocate().expect("one actual external address");
        let (
            joined,
            task_id,
            observed_origin,
            reported,
            first,
            second,
            third,
            startup,
            payload_count_before_cleanup,
        ) = runtime.block_on(async move {
            let (_control, owner, mailbox, mut receiver) =
                mailbox_channel::<Never, User<MailAddr, Notice>>(Config::new(2));
            let admission = Arc::new(Admission::new(owner));
            let (_notice_publication, notice_observation) = observe::pair();
            let notice_endpoint = ActorRef::<NoticeProtocol>::external(
                notice_address,
                mailbox,
                Arc::downgrade(&admission),
                notice_observation,
            );
            // Communication rounds the minimum capacity up with floor two.
            // Both original slots are full before the parent starts.
            let earlier_occupied = notice_endpoint
                .send_from(MailAddr::APPLICATION_ROOT, Notice::Occupied(11))
                .await;
            let later_occupied = notice_endpoint
                .send_from(MailAddr::APPLICATION_ROOT, Notice::Occupied(13))
                .await;
            let (capability_ready, capability_started) = oneshot::channel();
            let (release_capability, capability_permission) = oneshot::channel();
            let actor_spaces = Arc::new(HostedActorSpaces(roots.clone()));
            let parent = Parent {
                entries: original_parent,
                child: Some(Child {
                    entries: original_child,
                }),
                child_id: id,
                recipient: notice_endpoint.established_recipient(),
                inspection,
                turns: 0,
                returned: Vec::new(),
            };
            let (authority, startup, (), task) =
                spawn_local_execution::<Parent, _, StandardIngress, _, _, _>(
                    roots,
                    Config::new(1),
                    MailAddr::APPLICATION_ROOT,
                    parent,
                    move |control, terminal_reports, timers, observations| {
                        let mut capabilities = ApplicationCapabilities::<
                        Parent,
                        HostedActorSpaces<ActorSpace<ParentProtocol>>,
                        NoParent,
                        ChildBindings<Parent, ChildTerminal, StructuralOrigins<Parent>>,
                        StructuralOrigins<Parent>,
                    >::new_with_bindings(
                        ApplicationCapabilityInputs {
                            address: MailAddr::APPLICATION_ROOT,
                            actor_spaces,
                            allocations: addresses,
                            control,
                            timers,
                            observations,
                            terminal_reports,
                        },
                        ChildBindings::<Parent, ChildTerminal, StructuralOrigins<Parent>>::default(
                        ),
                    );
                        // Advanced runtime/test-host admission only. The exact
                        // existing ActivationTasks owner, not a new work service,
                        // owns this original gate and immutable returned value.
                        capabilities
                            .activation_tasks
                            .as_mut()
                            .expect("live activation tasks remain installed")
                            .spawn(async move {
                                let admitted = capability_ready.send(());
                                match admitted {
                                    Ok(()) | Err(()) => {}
                                }
                                let permission = capability_permission.await;
                                match permission {
                                    Ok(()) | Err(_) => {}
                                }
                                Err(ParentEvent::CapabilityReturned(original_capability))
                            });
                        ActionInterpreter::new(capabilities)
                    },
                    |environment| {
                        let (publication, startup) = oneshot::channel();
                        let environment = environment.publish_with(move |actor| {
                            let admitted = publication.send(actor);
                            match admitted {
                                Ok(()) | Err(_) => {}
                            }
                        });
                        (environment, startup, ())
                    },
                );
            let task_id = task.id();
            let started = capability_started.await;
            let observed_origin = child_receiver.await;
            // The first delivery cannot be admitted before this actual
            // child task and its total eager projector finish their poll.
            let first = receiver.recv().await;
            let second = receiver.recv().await;
            let third = receiver.recv().await;
            let startup = match inspection {
                Some(ParentCommand::Inspect) => startup.await.map(|_| ()),
                None => match startup.await {
                    Ok(actor) => {
                        let admitted = actor
                            .send_from(notice_address, ParentCommand::Inspect)
                            .await;
                        match admitted {
                            Ok(()) => Ok(()),
                            Err(rejected) => match rejected.into_message() {
                                ParentCommand::Inspect => {
                                    panic!("the live parent admits the actual user turn");
                                }
                            },
                        }
                    }
                    Err(error) => Err(error),
                },
            };
            let reported = report_receiver.await;
            // No assertion or negative oracle can retain this gate.
            let payload_count_before_cleanup = payload_before_cleanup.strong_count();
            let released = release_capability.send(());
            match released {
                Ok(()) | Err(()) => {}
            }
            let joined = task.await;
            drop(authority);
            // All control calls/transfers happen before these observations.
            assert!(earlier_occupied.is_ok());
            assert!(later_occupied.is_ok());
            assert!(started.is_ok());
            (
                joined,
                task_id,
                observed_origin,
                reported,
                first,
                second,
                third,
                startup,
                payload_count_before_cleanup,
            )
        });
        // Gate release and the actual raw root join precede executor destruction;
        // runtime Drop settles remaining detached/aborted task resources.
        drop(runtime);
        let stale_conversion = CONVERSION_CUSTODY
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let stale_child = CHILD_RETIREMENT
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        drop((stale_conversion, stale_child));
        let origin = observed_origin.expect("the actual child completed every typed oracle");
        let report = reported.expect("the original conversion transferred its complete report");
        assert_eq!(report.id(), id);
        assert_eq!(report.kind(), CreationKind::Birth);
        let Ok(committed) = report.into_committed() else {
            panic!("the actual child binding committed");
        };
        let (_, _, actor) = committed.into_parts();
        let recipient = actor.into_recipient();
        let endpoint = recipient.interpret(&mut ExtractLocalEndpoint);
        assert_eq!(endpoint.address(), origin.address());
        match first {
            Some(Received::User(user)) => {
                assert_eq!(user.from, MailAddr::APPLICATION_ROOT);
                assert_eq!(user.message, Notice::Occupied(11));
            }
            Some(Received::Control(never)) => match never {},
            Some(Received::UserLaneClosed) => {
                panic!("the original user lane remains open through retirement")
            }
            None => panic!("the actual external mailbox remains open through retirement"),
        }
        match second {
            Some(Received::User(user)) => {
                assert_eq!(user.from, MailAddr::APPLICATION_ROOT);
                assert_eq!(user.message, Notice::Occupied(13));
            }
            Some(Received::Control(never)) => match never {},
            Some(Received::UserLaneClosed) => {
                panic!("the original user lane remains open through retirement")
            }
            None => panic!("the actual external mailbox remains open through retirement"),
        }
        match third {
            Some(Received::User(user)) => {
                assert_eq!(user.from, MailAddr::APPLICATION_ROOT);
                assert_eq!(user.message, Notice::BirthsCommitted);
            }
            Some(Received::Control(never)) => match never {},
            Some(Received::UserLaneClosed) => {
                panic!("the original user lane remains open through retirement")
            }
            None => panic!("the actual external mailbox remains open through retirement"),
        }
        match inspection {
            Some(ParentCommand::Inspect) => assert!(startup.is_err()),
            None => assert!(startup.is_ok()),
        }
        if let Err(error) = &joined {
            assert_eq!(error.id(), task_id);
            assert!(error.is_panic());
        }
        let parent_retained = parent_owner.upgrade();
        let child_retained = child_owner.upgrade();
        let capability_retained = capability_owner.upgrade();
        let payload_retained = payload_owner.upgrade();
        // The original raw JoinError already preserves its native payload. It
        // does not preserve the outside Parent, child result or capability value.
        assert_eq!(payload_count_before_cleanup, 1);
        assert!(payload_retained.is_some());
        assert!(
            joined.is_ok(),
            "conversion must retire the outside parent ownership before returning"
        );
        let Ok(outcome) = &joined else {
            panic!("the joined actor returns its outside owners");
        };
        match (inspection, outcome) {
            (
                Some(ParentCommand::Inspect),
                ActorExecutionOutcome::ActivationPanicked {
                    residual: LocalResidual::Uncommitted { .. },
                    ..
                },
            )
            | (
                None,
                ActorExecutionOutcome::HostExecutionPanicked {
                    residual: LocalResidual::Retired { .. },
                    ..
                },
            ) => {}
            _ => panic!(
                "the originating host operation must retain its exact native panic provenance"
            ),
        }
        let (ActorExecutionOutcome::ActivationPanicked {
            behavior: parent,
            residual,
            additional_failures,
            ..
        }
        | ActorExecutionOutcome::HostExecutionPanicked {
            behavior: parent,
            residual,
            additional_failures,
            ..
        }) = outcome
        else {
            panic!("classification cannot substitute for available actor ownership");
        };
        assert!(additional_failures.is_empty());
        assert_eq!(parent.entries.as_slice(), [43, 47, 53]);
        assert_eq!(parent.entries.as_ptr() as usize, parent_allocation);
        assert!(parent.child.is_none());
        assert_eq!(parent.child_id, id);
        assert!(parent.inspection.is_none());
        assert_eq!(parent.turns, usize::from(inspection.is_none()));
        assert_eq!(parent.returned.len(), 0);
        let parent_endpoint = parent
            .recipient
            .clone()
            .interpret(&mut ExtractLocalEndpoint);
        assert_eq!(parent_endpoint.address(), notice_address);
        let (
            descendants,
            ingress,
            activation_tasks,
            failures,
            unread,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            terminal_report,
            retirement_failures,
            current,
        ) = match residual {
            LocalResidual::Uncommitted {
                initialization,
                descendants,
                ingress,
                activation_tasks,
                capability_failures,
                unread_owner_cancellation,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                terminal_report,
                retirement_failures,
            } => (
                descendants,
                ingress,
                activation_tasks,
                capability_failures,
                unread_owner_cancellation,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                terminal_report,
                retirement_failures,
                Some(initialization),
            ),
            LocalResidual::Retired {
                interpretation,
                source,
                settlements: _,
                descendants,
                ingress,
                activation_tasks,
                capability_failures,
                unread_owner_cancellation,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                terminal_report,
                retirement_failures,
            } => {
                assert!(source.is_none());
                (
                    descendants,
                    ingress,
                    activation_tasks,
                    capability_failures,
                    unread_owner_cancellation,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    terminal_report,
                    retirement_failures,
                    interpretation.as_ref(),
                )
            }
            LocalResidual::Prepared { .. } => {
                panic!("the real conversion began with installed original Actions progress");
            }
        };
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(current.is_some());
        assert!(activation_tasks.is_empty());
        assert_eq!(failures.len(), 0);
        assert!(unread.is_none());
        assert_eq!(ingress.user.len(), 0);
        assert_eq!(ingress.control.len(), 1);
        let ParentEvent::CapabilityReturned(returned) = &ingress.control[0] else {
            panic!("the original completed operation is retained as its typed control fact");
        };
        assert_eq!(returned.as_slice(), [59, 61, 67]);
        assert_eq!(returned.as_ptr() as usize, capability_allocation);
        let (descendants, (occurrence_failures, ())) = descendants;
        assert_eq!(occurrence_failures.len(), 0);
        assert_eq!(descendants.len(), 1);
        let terminal = &descendants[0];
        assert_eq!(terminal.origin.address(), origin.address());
        assert_eq!(terminal.origin, origin);
        let ActorRetirement::Completed {
            interpretation: child_retirement_interpretation,
            source: child_retirement_source,
            received_interpretation: child_retirement_received_interpretation,
            received_source: child_retirement_received_source,
            source_index: child_retirement_source_index,
            acquired_ingress: child_retirement_acquired_ingress,
            terminal_report: child_retirement_terminal_report,
            retirement_failures: child_retirement_native_failures,
            additional_failures: child_retirement_additional_failures,
            behavior: child,
            settlements: child_settlements,
            control: child_control,
            user: child_user,
            descendants: child_descendants,
            child_failures: (),
            completion: child_completion,
            capability_failures: child_failures,
            unread_owner_cancellation: child_unread,
        } = &terminal.retirement
        else {
            panic!("the original full child terminal remains available");
        };
        assert!(child_retirement_interpretation.is_none());
        assert!(child_retirement_source.is_none());
        assert!(child_retirement_received_interpretation.is_none());
        assert!(child_retirement_received_source.is_none());
        assert!(child_retirement_source_index.is_none());
        assert!(child_retirement_acquired_ingress.is_none());
        assert!(child_retirement_terminal_report.is_none());
        assert!(child_retirement_native_failures.is_empty());
        assert!(child_retirement_additional_failures.is_empty());

        assert_eq!(child.entries.as_slice(), [31, 37, 41]);
        assert_eq!(child.entries.as_ptr() as usize, child_allocation);
        assert_eq!(*child_completion, Completion::Stopped);
        assert_eq!(child_control.len(), 0);
        assert_eq!(child_user.len(), 0);
        assert_eq!(child_descendants.len(), 0);
        assert_eq!(child_failures.len(), 0);
        assert!(child_unread.is_none());
        assert_eq!(child_settlements.len(), 1);
        assert_eq!(child_settlements[0].sends, NoSends);
        assert!(child_settlements[0].creations.is_empty());
        assert!(matches!(child_settlements[0].become_, Step::Stop(Stopped)));
        let parent_retained =
            parent_retained.expect("the returned outcome owns original parent state");
        let child_retained =
            child_retained.expect("the returned child terminal owns original state");
        let capability_retained =
            capability_retained.expect("the joined capability return is retained");
        assert_eq!(parent_retained.as_slice(), [43, 47, 53]);
        assert_eq!(parent_retained.as_ptr() as usize, parent_allocation);
        assert_eq!(child_retained.as_slice(), [31, 37, 41]);
        assert_eq!(child_retained.as_ptr() as usize, child_allocation);
        assert_eq!(capability_retained.as_slice(), [59, 61, 67]);
        assert_eq!(capability_retained.as_ptr() as usize, capability_allocation);
        let payload_retained =
            payload_retained.expect("the exact native payload remains with the returned outcome");
        assert_eq!(payload_retained.as_slice(), [71, 73, 79]);
        assert_eq!(payload_retained.as_ptr() as usize, payload_allocation);
        // Current original progress was observed above independently of the
        // complete prior row. Explicit final discharge retains the original
        // opaque cause and every recovered row until all actual joins finish.
        let outcome = joined.expect("outside-owner retirement returned");
        let (
            current,
            prior,
            behavior,
            control,
            user,
            descendants,
            child_failures,
            capability_failures,
            additional_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
            terminal_report,
            unread_owner_cancellation,
            payload,
        ) = match ActorRetirement::from_local(outcome) {
            ActorRetirement::HostCommitPanicked {
                initialization: Some(current),
                behavior,
                control,
                user,
                descendants,
                child_failures,
                capability_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
                unread_owner_cancellation,
                payload,
            } => (
                current,
                Vec::new(),
                behavior,
                control,
                user,
                descendants,
                child_failures,
                capability_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
                unread_owner_cancellation,
                payload,
            ),
            ActorRetirement::HostExecutionPanicked {
                interpretation: Some(current),
                source: None,
                settlements: prior,
                behavior,
                control,
                user,
                descendants,
                child_failures,
                capability_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
                unread_owner_cancellation,
                payload,
            } => (
                current,
                prior,
                behavior,
                control,
                user,
                descendants,
                child_failures,
                capability_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
                unread_owner_cancellation,
                payload,
            ),
            other => {
                drop(other);
                panic!(
                    "the exact caught operation retains its original current progress and native cause"
                );
            }
        };
        assert!(additional_failures.is_empty());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(retirement_failures.is_empty());
        assert!(terminal_report.is_none());
        assert!(unread_owner_cancellation.is_none());
        let InterpretationProgress::Interpreting((
            (
                Some(InterpretationProgress::Completed(Interpretation::Complete(creations))),
                Some(InterpretationProgress::Interpreting(sends)),
            ),
            Step::Continue,
        )) = current
        else {
            panic!("original current progress keeps completed creations and interrupted sends");
        };
        let CreationSettlement::Settled(created) = creations.into_settlement() else {
            panic!("current creation policy preserves its exact original Settled product");
        };
        let expected_current = usize::from(inspection.is_some());
        assert_eq!(created.len(), expected_current);
        for creation in &created {
            let SettledItem::Attempted(ItemSettlement::Accepted(ChildChoice::Head(
                ChildCreationOutcome::Established(committed),
            ))) = creation
            else {
                panic!("current original head-role establishment remains exact");
            };
            assert_eq!(committed.id(), id);
            assert_eq!(committed.kind(), CreationKind::Birth);
            let endpoint = committed
                .actor()
                .into_recipient()
                .interpret(&mut ExtractLocalEndpoint);
            assert_eq!(endpoint.address(), origin.address());
        }
        let Some(InterpretationProgress::Completed(Interpretation::Complete(deliveries))) =
            &sends.inner
        else {
            panic!("earlier standard delivery results survive the conversion producer");
        };
        assert_eq!(deliveries.len(), expected_current);
        for delivery in deliveries {
            assert!(matches!(
                delivery,
                SettledItem::Attempted(ItemSettlement::Accepted(()))
            ));
        }
        let Some(InterpretationProgress::Interpreting(requests)) = &sends.owned else {
            panic!("actual interrupted observation request retains its original owning lane");
        };
        assert_eq!(requests.len(), 1);
        assert!(matches!(
            &requests[0],
            Some(InterpretationProgress::Interpreting((None, None)))
        ));
        // This application conversion explicitly consumed the report into the
        // original external receiver. No lower reply or input is reconstructed.
        assert_eq!(prior.len(), usize::from(inspection.is_none()));
        for initial in prior {
            assert!(matches!(initial.become_, Step::Continue));
            assert_eq!(initial.sends.owned.len(), 0);
            // Accepted unit delivery is explicitly discharged on source admission.
            assert_eq!(initial.sends.inner.len(), 0);
            let CreationSettlement::Settled(created) = initial.creations.into_settlement() else {
                panic!("original prior creation receipt remains retained");
            };
            let Ok(created) = created.into_one() else {
                panic!("one original prior creation receipt remains");
            };
            let SettledItem::Attempted(ItemSettlement::Accepted(ChildChoice::Head(
                ChildCreationOutcome::Established(committed),
            ))) = created
            else {
                panic!("prior original head-role establishment remains exact");
            };
            let (created_id, created_kind, actor) = committed.into_parts();
            assert_eq!(created_id, id);
            assert_eq!(created_kind, CreationKind::Birth);
            let endpoint = actor.into_recipient().interpret(&mut ExtractLocalEndpoint);
            assert_eq!(endpoint.address(), origin.address());
        }
        drop((
            created,
            sends,
            behavior,
            control,
            user,
            descendants,
            child_failures,
            capability_failures,
            additional_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
            terminal_report,
            unread_owner_cancellation,
            payload,
        ));
        drop(parent_retained);
        drop(child_retained);
        drop(capability_retained);
        drop(payload_retained);
        // Explicit final result discharge only AFTER raw actor and its known
        // children/capability tasks have completed their normal retirement.
        assert_eq!(parent_owner.strong_count(), 0);
        assert_eq!(child_owner.strong_count(), 0);
        assert_eq!(capability_owner.strong_count(), 0);
        assert_eq!(payload_owner.strong_count(), 0);
    }

    #[test]
    fn initialization_conversion_keeps_parent_child_and_capability_owners() {
        assert_parent_conversion_preserves_owned_retirement(Some(ParentCommand::Inspect));
    }

    #[test]
    fn active_conversion_keeps_parent_child_and_capability_owners() {
        assert_parent_conversion_preserves_owned_retirement(None);
    }
}

/// Caller-local work disposition alongside the actual unit application cleanup.
///
/// Work values never move into the executor-owned cleanup task. `Unstarted`
/// returns the original untouched application and callable. `Prepared` retains
/// the actual actor and Spaces before handoff; `NotInvoked` retains the callable
/// while setup or startup owns consumed inputs. `Interrupted` does not claim
/// recovery of a callable or work future consumed by invocation.
/// Cleanup first retains its publication receiving result, then its actual task
/// join result; each constructor retains its exact joined-root product.
/// Entity receiving additionally returns independently held original root and family receipts.
/// Those actual receiving errors never substitute a no-root or completed-family classification.
/// For genuine absent work, `execute` and async `run` retain `Option<Never>`:
/// `None` is no supplied callable/output, and Completed means actual startup grant.
/// Supplied `execute_with` retains original Work and Ready Output inside `Some`.
#[must_use = "application inputs, output and joined actor outcome require explicit custody"]
pub enum ApplicationOutcome<
    ApplicationInputs,
    Work,
    Output,
    Cleanup,
    StagingInputs = Never,
    PreparedInputs = Never,
> {
    /// Staging stopped before actor startup; exact cold partial inputs and callable survive.
    StagingRejected { inputs: StagingInputs },
    Unstarted {
        application: ApplicationInputs,
        work: Work,
    },
    /// Prepared originals remain available before this owner transfers its actor.
    /// The original native setup cause propagates to the unwinding caller.
    Prepared {
        inputs: PreparedInputs,
        work: Work,
        cleanup: Result<Result<Cleanup, JoinError>, RecvError>,
    },
    NotInvoked {
        work: Work,
        startup_error: Option<RecvError>,
        cleanup: Result<Result<Cleanup, JoinError>, RecvError>,
    },
    Completed {
        output: Output,
        cleanup: Result<Result<Cleanup, JoinError>, RecvError>,
    },
    Interrupted {
        cleanup: Result<Result<Cleanup, JoinError>, RecvError>,
    },
}

enum ApplicationWorkCustody<
    ApplicationInputs,
    Work,
    Output,
    StagingInputs = Never,
    PreparedInputs = Never,
> {
    StagingRejected(StagingInputs),
    Unstarted(ApplicationInputs, Work),
    Prepared(PreparedInputs, Work),
    NotInvoked(Work, Option<RecvError>),
    Completed(Output),
}

#[expect(
    clippy::type_complexity,
    reason = "one affine publication pairs exact phase custody with its sole original sender"
)]
struct ApplicationWorkPublication<
    ApplicationInputs,
    Work,
    Output,
    StagingInputs = Never,
    PreparedInputs = Never,
> {
    publication: Option<(
        ApplicationWorkCustody<ApplicationInputs, Work, Output, StagingInputs, PreparedInputs>,
        oneshot::Sender<
            ApplicationWorkCustody<ApplicationInputs, Work, Output, StagingInputs, PreparedInputs>,
        >,
    )>,
}

impl<ApplicationInputs, Work, Output, StagingInputs, PreparedInputs> Drop
    for ApplicationWorkPublication<ApplicationInputs, Work, Output, StagingInputs, PreparedInputs>
{
    fn drop(&mut self) {
        if let Some((custody, publication)) = self.publication.take() {
            match publication.send(custody) {
                Ok(()) => {}
                Err(custody) => drop(custody),
            }
        }
    }
}

#[expect(
    clippy::type_complexity,
    clippy::too_many_lines,
    reason = "one existing affine cold-input/startup/work/permission/join owner preserves exact products at every drop cut"
)]
fn execute_application_with<
    Inputs,
    Owner,
    Actor,
    Spaces,
    Origins,
    Terminal,
    ChildFailures,
    InstalledFamilies,
    StagingInputs,
    AcquireStartupInputs,
    StartupInputs,
    Prepare,
    Work,
    Invoke,
    WorkFuture,
    Output,
    Cleanup,
    RetireUnstartedFamilies,
    RetainRootRetirement,
>(
    executor: Handle,
    application: Inputs,
    work: Option<(Work, Invoke)>,
    acquire_startup_inputs: AcquireStartupInputs,
    prepare: Prepare,
    (family_publications, retire_unstarted_families): (
        InstalledFamilies::ShutdownPublications,
        RetireUnstartedFamilies,
    ),
    retain_root_retirement: RetainRootRetirement,
) -> (
    impl Future<Output = ()>,
    impl Future<
        Output = ApplicationOutcome<
            Inputs,
            Option<Work>,
            Option<Output>,
            Cleanup,
            StagingInputs,
            (Actor, Spaces),
        >,
    >,
)
where
    Actor: BehaviorBase
        + BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>
        + Send
        + 'static,
    Actor::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    BehaviorMessage<Actor>: Send + 'static,
    Actor::Sends: Send + 'static,
    Actor::Error: Send + 'static,
    Actor::InterpretationCustody: Send + 'static,
    Actor::SourceCustody: Send + 'static,
    <Actor::Birth as BirthMode>::Child:
        ChildOccurrenceProduct<RuntimeChildBindings<Terminal, Origins>> + Send + 'static,
    Spaces: Hosts<Actor::Protocol> + Send + Sync + 'static,
    ChildBindings<Actor, Terminal, Origins>:
        Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
    RootInterpreter<Actor, Spaces, Terminal, Origins>:
        CommitActions<Actor, Retired = (Vec<Terminal>, ChildFailures)> + Send + 'static,
    <Actor as BehaviorSettlements>::Settlements: ClassifySettlement + Send,
    RootOrigin<Owner>: Send + 'static,
    Terminal: Send + 'static,
    ChildFailures: Send + 'static,
    AcquireStartupInputs: AsyncFnOnce(&Inputs) -> StartupInputs,
    InstalledFamilies: InstalledEntityFamilies + Send + 'static,
    Prepare: FnOnce(
        Inputs,
        &mut Option<(Work, Invoke)>,
        StartupInputs,
        ApplicationAddresses,
    ) -> Result<(Actor, Spaces, InstalledFamilies), StagingInputs>,
    Invoke: FnOnce(
        Work,
        ApplicationHandle<Actor::Protocol, Actor::Event, InstalledFamilies::Receptionists>,
    ) -> WorkFuture,
    WorkFuture: Future<Output = Output>,
    Cleanup: Send + 'static,
    RetireUnstartedFamilies: FnOnce(
        Handle,
        InstalledFamilies,
        InstalledFamilies::ShutdownPublications,
    ) -> Option<JoinHandle<Cleanup>>,
    RetainRootRetirement: FnOnce(
            (
                RootOrigin<Owner>,
                Result<ActorRetirement<Actor, Terminal, ChildFailures>, JoinError>,
            ),
        ) -> Cleanup
        + Send
        + 'static,
{
    let (publication, work_result) = oneshot::channel();
    let original_work = ApplicationWorkPublication {
        publication: Some((
            ApplicationWorkCustody::Unstarted(application, work),
            publication,
        )),
    };
    let (cleanup_publication, cleanup_result) = oneshot::channel();
    let execution = async move {
        let mut original_work = original_work;
        let startup_inputs = {
            let Some((ApplicationWorkCustody::Unstarted(application, _), _)) =
                original_work.publication.as_ref()
            else {
                unreachable!("startup acquisition borrows the original cold inputs");
            };
            let mut startup_acquisition = pin!(acquire_startup_inputs(application));
            poll_fn(|context| {
                let entered_executor = executor.enter();
                let acquired = startup_acquisition.as_mut().poll(context);
                drop(entered_executor);
                acquired
            })
            .await
        };
        let Some((ApplicationWorkCustody::Unstarted(application, work), publication)) =
            original_work.publication.take()
        else {
            unreachable!("execution owns the single original input publication");
        };
        original_work.publication =
            Some((ApplicationWorkCustody::NotInvoked(work, None), publication));
        let allocations = ApplicationAddresses::new();
        let prepared_inputs = {
            let Some((ApplicationWorkCustody::NotInvoked(work, None), _)) =
                original_work.publication.as_mut()
            else {
                unreachable!("preparation borrows the original uninvoked work publication");
            };
            prepare(application, work, startup_inputs, allocations.clone())
        };
        let Some((ApplicationWorkCustody::NotInvoked(work, None), publication)) =
            original_work.publication.take()
        else {
            unreachable!("preparation owns the single uninvoked callable publication");
        };
        let (root, spaces, installed_families) = match prepared_inputs {
            Ok(prepared) => prepared,
            Err(inputs) => {
                original_work.publication =
                    Some((ApplicationWorkCustody::StagingRejected(inputs), publication));
                drop(original_work);
                return;
            }
        };
        original_work.publication = Some((
            ApplicationWorkCustody::Prepared((root, spaces), work),
            publication,
        ));
        let setup = catch_unwind(AssertUnwindSafe(|| {
            let Some((ApplicationWorkCustody::Prepared((_, spaces), _), _)) =
                original_work.publication.as_ref()
            else {
                unreachable!("setup borrows the single prepared input publication");
            };
            let roots = <Spaces as Hosts<Actor::Protocol>>::space(spaces).clone();
            let bindings = ChildBindings::<Actor, Terminal, Origins>::default();
            let receptionists = installed_families.receptionists();
            (roots, bindings, receptionists)
        }));
        let (roots, bindings, receptionists) = match setup {
            Ok(setup) => setup,
            Err(cause) => {
                // The actor is still inside Prepared; no task was handed off at this cut.
                if let Some(cleanup) = retire_unstarted_families(
                    executor.clone(),
                    installed_families,
                    family_publications,
                ) {
                    match cleanup_publication.send(cleanup) {
                        Ok(()) => {}
                        Err(cleanup) => drop(cleanup),
                    }
                }
                resume_unwind(cause);
            }
        };
        let interface_allocations = allocations.clone();
        let (permission, permitted) = oneshot::channel();
        let (authority, startup, shutdown_control, actor_join) = {
            let entered_executor = executor.enter();
            let address = MailAddr::APPLICATION_ROOT;
            let config = communication::Config::new(DEFAULT_USER_CAPACITY);
            let Some((ApplicationWorkCustody::Prepared((root, spaces), work), publication)) =
                original_work.publication.take()
            else {
                unreachable!("handoff consumes the single prepared input publication");
            };
            original_work.publication =
                Some((ApplicationWorkCustody::NotInvoked(work, None), publication));
            let actor_spaces = Arc::new(HostedActorSpaces(spaces));
            let started = spawn_local_execution::<Actor, _, StandardIngress, _, _, _>(
                roots,
                config,
                address,
                root,
                move |control, terminal_reports, timers, observations| {
                    ActionInterpreter::new(ApplicationCapabilities::<
                        Actor,
                        HostedActorSpaces<Spaces>,
                        NoParent,
                        ChildBindings<Actor, Terminal, Origins>,
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
                        bindings,
                    ))
                },
                |environment| {
                    let (publication, startup) = oneshot::channel();
                    let control = environment.shutdown_control();
                    let environment =
                        environment.publish_with(move |actor| match publication.send(actor) {
                            Ok(()) => {}
                            Err(actor) => drop(actor),
                        });
                    (environment, startup, control)
                },
            );
            drop(entered_executor);
            started
        };
        let (joined_publication, joined) = oneshot::channel();
        let origin = RootOrigin::<Owner>::new(MailAddr::APPLICATION_ROOT);
        let cleanup = executor.spawn(async move {
            match permitted.await {
                Ok(()) | Err(_) => {}
            }
            let retirement = actor_join.await.map(ActorRetirement::from_local);
            let cleanup = retain_root_retirement((origin, retirement));
            installed_families.shutdown(family_publications).await;
            match joined_publication.send(()) {
                Ok(()) | Err(()) => {}
            }
            cleanup
        });
        match cleanup_publication.send(cleanup) {
            Ok(()) => {}
            Err(cleanup) => drop(cleanup),
        }
        let started = startup.await;
        let Some((ApplicationWorkCustody::NotInvoked(work, None), publication)) =
            original_work.publication.take()
        else {
            unreachable!("startup owns the single uninvoked callable publication");
        };
        match started {
            Ok(actor) => {
                if let Some((work, invoke)) = work {
                    let application = ApplicationHandle::new(
                        actor,
                        shutdown_control,
                        interface_allocations,
                        receptionists,
                    );
                    {
                        let mut application_work = pin!(invoke(work, application));
                        let output =
                            poll_fn(|context| application_work.as_mut().poll(context)).await;
                        original_work.publication =
                            Some((ApplicationWorkCustody::Completed(Some(output)), publication));
                    }
                } else {
                    // Discharge the genuine unused startup projections in their existing field order.
                    original_work.publication =
                        Some((ApplicationWorkCustody::Completed(None), publication));
                    drop((
                        actor,
                        shutdown_control,
                        interface_allocations,
                        receptionists,
                    ));
                }
                drop(original_work);
            }
            Err(error) => {
                match publication.send(ApplicationWorkCustody::NotInvoked(work, Some(error))) {
                    Ok(()) => {}
                    Err(work) => drop(work),
                }
            }
        }
        match permission.send(()) {
            Ok(()) | Err(()) => {}
        }
        match joined.await {
            Ok(()) | Err(_) => {}
        }
        drop(authority);
    };
    let result = async move {
        let custody = work_result.await;
        match custody {
            Ok(ApplicationWorkCustody::Unstarted(application, work)) => {
                let work = match work {
                    Some((work, invoke)) => {
                        drop(invoke);
                        Some(work)
                    }
                    None => None,
                };
                ApplicationOutcome::Unstarted { application, work }
            }
            Ok(ApplicationWorkCustody::StagingRejected(inputs)) => {
                ApplicationOutcome::StagingRejected { inputs }
            }
            custody => {
                // Publication failure does not classify actor or task existence.
                // Retain its actual error alongside the acquired work custody.
                let cleanup = match cleanup_result.await {
                    Ok(cleanup) => Ok(cleanup.await),
                    Err(error) => Err(error),
                };
                match custody {
                    Ok(ApplicationWorkCustody::Prepared(inputs, work)) => {
                        let work = match work {
                            Some((work, invoke)) => {
                                drop(invoke);
                                Some(work)
                            }
                            None => None,
                        };
                        ApplicationOutcome::Prepared {
                            inputs,
                            work,
                            cleanup,
                        }
                    }
                    Ok(ApplicationWorkCustody::NotInvoked(work, startup_error)) => {
                        let work = match work {
                            Some((work, invoke)) => {
                                drop(invoke);
                                Some(work)
                            }
                            None => None,
                        };
                        ApplicationOutcome::NotInvoked {
                            work,
                            startup_error,
                            cleanup,
                        }
                    }
                    Ok(ApplicationWorkCustody::Completed(output)) => {
                        ApplicationOutcome::Completed { output, cleanup }
                    }
                    Err(_) => ApplicationOutcome::Interrupted { cleanup },
                    Ok(ApplicationWorkCustody::StagingRejected(_)) => {
                        unreachable!("the cold staging rejection branch already returned")
                    }
                    Ok(ApplicationWorkCustody::Unstarted(_, _)) => {
                        unreachable!("the original untouched input branch already returned")
                    }
                }
            }
        }
    };
    (execution, result)
}

#[cfg(test)]
mod capability_task_retirement {
    use super::{
        ApplicationCapabilities, ApplicationCapabilityInputs, NoParent, StructuralOrigins,
    };
    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::child_bindings::ChildBindings;
    use crate::interpret::{ActionInterpreter, RetireCapabilities};
    use crate::launch::{ActorSpace, spawn_local_execution};
    use crate::local::{CapabilityRetirement, StandardIngress};
    use crate::reports::TerminalReportTransaction;
    use crate::terminal::{ActorRetirement, ChildOrigin, ProjectTerminal};
    use crate::termination::TerminalReportDisposition;
    use crate::topology::HostedActorSpaces;
    use crate::worker_preparation::{WorkerPreparationSource, WorkerPreparationStart};
    use behavior::{
        ActionItem, ActionItemResult, Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorAddr,
        BehaviorBase, ChildCons, ChildCreationOutcome, ChildHead, ChildProduct, Children,
        ComposedEvent, CreateChild, CreationId, CreationRejection, CreationSequence, Creations,
        EndpointAddress, EstablishChild, EstablishedRecipient, EventIngress, Here,
        InitializationTurn, InjectEvent, Inside, InterpretItem, InterpreterRequests,
        ItemSettlement, MessageProtocol, Never, NoBirths, NoChildren, NoSends, Protocol,
        RetirementBirths, RoutedCreation, SendEffects, SendLayer, SourceActions, SourceAdmission,
        Step, Stopped, User, UserEvent,
    };
    use behavior_actors::ShutdownRequested;
    use behavior_actors::atomic::{
        ActivationPermit, ActivationPlan, BeginActivation, ImmediateActivation, OrderedRoles,
        PendingWorkerPreparation, PrepareWorkers, StartingWorkerPreparation, WorkerActivation,
        WorkerActivationGrant, WorkerPreparation, WorkerSource,
    };
    use bombay_engine::Completion;
    use communication::{Config, ControlClosed, ControlSender};
    use core::future::{Future, poll_fn};
    use core::ops::ControlFlow;
    use core::task::Poll;
    use std::any::Any;
    use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
    use std::sync::{Arc, Mutex, PoisonError, Weak};
    use std::time::Instant;
    use tokio::sync::Mutex as TaskCustody;
    use tokio::sync::oneshot;
    use tokio::task::{self, Id, JoinError};
    /// Factual identity of the original Tokio producer, before or after its first poll.
    #[derive(Debug, Eq, PartialEq)]
    enum WorkerCapabilityTask {
        Unpolled,
        Started(Id),
    }

    /// Exact source boundary of the original payload moved into Tokio's `JoinError`.
    #[derive(Debug, Eq, PartialEq)]
    enum WorkerCapabilityOperation {
        Construction,
        Execution,
        Disposal,
        Receiving,
    }

    /// Original native cause at one known task-owned boundary.
    struct WorkerCapabilityFailure {
        operation: WorkerCapabilityOperation,
        payload: Box<dyn Any + Send>,
    }

    impl WorkerCapabilityFailure {
        fn into_parts(self) -> (WorkerCapabilityOperation, Box<dyn Any + Send>) {
            (self.operation, self.payload)
        }
    }

    /// Only source phases whose concrete values remain owned by the runtime.
    enum WorkerPreparationPhase<Source, Role, Worker, Plan>
    where
        Source: WorkerSource<Role, Worker, Plan>,
        Worker: Behavior + Send,
        Plan: ActivationPlan,
    {
        Starting(StartingWorkerPreparation<Source, Role, Worker, Plan>),
        Pending(PendingWorkerPreparation<Source, Role, Worker, Plan>),
        /// The completed report was actually moved into `Event::inject_at`.
        ReportTransferred,
    }

    /// Outside-task source phase, acquired Event and additional original failures.
    struct WorkerPreparationCustody<Source, Role, Worker, Plan, Event>
    where
        Source: WorkerSource<Role, Worker, Plan>,
        Worker: Behavior + Send,
        Plan: ActivationPlan,
    {
        task: WorkerCapabilityTask,
        phase: Option<WorkerPreparationPhase<Source, Role, Worker, Plan>>,
        event: Option<Event>,
        failures: Vec<WorkerCapabilityFailure>,
        resumed_failure_origin: Option<WorkerCapabilityOperation>,
    }

    /// Preserve actual Ready reply across disposal of its borrowing future.
    /// The output remains caller-local until the source borrow has ended.
    async fn capability_work_reply<Work>(
        constructed: Result<Work, WorkerCapabilityFailure>,
    ) -> (
        Result<Work::Output, WorkerCapabilityFailure>,
        Option<WorkerCapabilityFailure>,
    )
    where
        Work: Future + Send,
    {
        let mut work = match constructed {
            Ok(work) => Some(Box::pin(work)),
            Err(failure) => return (Err(failure), None),
        };
        poll_fn(move |cx| {
            let polled = {
                let current = work.as_mut().expect("only pending work can be polled");
                catch_unwind(AssertUnwindSafe(|| current.as_mut().poll(cx)))
            };
            match polled {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(reply)) => {
                    // reply is closure-local, not captured across a Pending.
                    // take removes the owning pinned future before wrapper Ready.
                    let disposal = catch_unwind(AssertUnwindSafe(|| drop(work.take())))
                        .err()
                        .map(|payload| WorkerCapabilityFailure {
                            operation: WorkerCapabilityOperation::Disposal,
                            payload,
                        });
                    Poll::Ready((Ok(reply), disposal))
                }
                Err(payload) => {
                    // Drop runs after catch returned, outside primary unwind.
                    let disposal = catch_unwind(AssertUnwindSafe(|| drop(work.take())))
                        .err()
                        .map(|payload| WorkerCapabilityFailure {
                            operation: WorkerCapabilityOperation::Disposal,
                            payload,
                        });
                    Poll::Ready((
                        Err(WorkerCapabilityFailure {
                            operation: WorkerCapabilityOperation::Execution,
                            payload,
                        }),
                        disposal,
                    ))
                }
            }
        })
        .await
    }

    /// Keep original source metadata outside the task without output lifetime bounds.
    #[expect(
        clippy::type_complexity,
        clippy::too_many_lines,
        reason = "Keep the original source/role/worker/plan/Event cell and its exhaustive borrowing work, acquired reply, disposal, phase advancement and native handoff in one owning transaction."
    )]
    async fn prepare_workers<Source, Role, Worker, Plan, Event, Path>(
        custody: &Arc<TaskCustody<WorkerPreparationCustody<Source, Role, Worker, Plan, Event>>>,
        control: &ControlSender<Event>,
    ) where
        Source: WorkerPreparationSource<Role, Worker, Plan>,
        Role: Send + Sync,
        Worker: Behavior + Send,
        Plan: ActivationPlan,
        Event: InjectEvent<WorkerPreparation<Source, Role, Worker, Plan>, Path>,
    {
        loop {
            let mut original = custody.lock().await;
            original.task = WorkerCapabilityTask::Started(task::id());
            let WorkerPreparationCustody {
                phase,
                event,
                failures,
                resumed_failure_origin,
                task: _,
            } = &mut *original;
            let completed = match phase
                .as_mut()
                .expect("original source phase stays installed during work")
            {
                WorkerPreparationPhase::Starting(request) => {
                    let (source, role) = request.source_and_role();
                    let constructed = catch_unwind(AssertUnwindSafe(move || {
                        let source = source;
                        source.prepare_first(role)
                    }))
                    .map_err(|payload| WorkerCapabilityFailure {
                        operation: WorkerCapabilityOperation::Construction,
                        payload,
                    });
                    let (reply, disposal) = capability_work_reply(constructed).await;
                    let reply = match reply {
                        Ok(reply) => Some(reply),
                        Err(failure) => {
                            failures.push(failure);
                            None
                        }
                    };
                    if let Some(failure) = disposal {
                        failures.push(failure);
                    }
                    match reply {
                        Some(reply) => {
                            let WorkerPreparationPhase::Starting(request) =
                                phase.take().expect("same original starting phase")
                            else {
                                unreachable!("polled the starting phase");
                            };
                            let next = match reply {
                                WorkerPreparationStart::Submitted(submission) => {
                                    request.accept(submission)
                                }
                                WorkerPreparationStart::WorkerRejected(reason) => {
                                    ControlFlow::Break(request.reject(reason))
                                }
                                WorkerPreparationStart::SourceRejected(reason) => {
                                    ControlFlow::Break(request.reject_source(reason))
                                }
                            };
                            match next {
                                ControlFlow::Continue(request) => {
                                    *phase = Some(WorkerPreparationPhase::Pending(request));
                                    None
                                }
                                ControlFlow::Break(report) => Some(report),
                            }
                        }
                        None => None,
                    }
                }
                WorkerPreparationPhase::Pending(request) => {
                    let (source, role) = request.source_and_role();
                    let constructed = catch_unwind(AssertUnwindSafe(move || {
                        let source = source;
                        source.prepare_next(role)
                    }))
                    .map_err(|payload| WorkerCapabilityFailure {
                        operation: WorkerCapabilityOperation::Construction,
                        payload,
                    });
                    let (reply, disposal) = capability_work_reply(constructed).await;
                    let reply = match reply {
                        Ok(reply) => Some(reply),
                        Err(failure) => {
                            failures.push(failure);
                            None
                        }
                    };
                    if let Some(failure) = disposal {
                        failures.push(failure);
                    }
                    match reply {
                        Some(reply) => {
                            let WorkerPreparationPhase::Pending(request) =
                                phase.take().expect("same original pending phase")
                            else {
                                unreachable!("polled the pending phase");
                            };
                            let next = match reply {
                                Ok(submission) => request.accept(submission),
                                Err(reason) => ControlFlow::Break(request.reject(reason)),
                            };
                            match next {
                                ControlFlow::Continue(request) => {
                                    *phase = Some(WorkerPreparationPhase::Pending(request));
                                    None
                                }
                                ControlFlow::Break(report) => Some(report),
                            }
                        }
                        None => None,
                    }
                }
                WorkerPreparationPhase::ReportTransferred => {
                    unreachable!("one producer owns its report transfer")
                }
            };
            if let Some(report) = completed {
                // The original report is deliberately consumed by this actual
                // receiving conversion. No intact source is claimed afterward.
                *phase = Some(WorkerPreparationPhase::ReportTransferred);
                match catch_unwind(AssertUnwindSafe(|| Event::inject_at(report))) {
                    Ok(acquired) => *event = Some(acquired),
                    Err(payload) => failures.push(WorkerCapabilityFailure {
                        operation: WorkerCapabilityOperation::Receiving,
                        payload,
                    }),
                }
            }
            if !failures.is_empty() {
                // Every other actual failure and the surviving phase/Event stay in
                // the actor-owned cell. Tokio receives the exact earliest payload.
                let (origin, first) = failures.remove(0).into_parts();
                *resumed_failure_origin = Some(origin);
                resume_unwind(first);
            }
            match phase {
                Some(WorkerPreparationPhase::ReportTransferred) => {
                    let acquired = event
                        .take()
                        .expect("normal receiving acquired its exact Event");
                    match control.send(acquired) {
                        Ok(()) => {}
                        Err(ControlClosed(acquired)) => *event = Some(acquired),
                    }
                    return;
                }
                Some(WorkerPreparationPhase::Starting(_) | WorkerPreparationPhase::Pending(_)) => {}
                None => unreachable!("pure phase transition reinstalls original custody"),
            }
        }
    }

    /// Original grant and endpoint remain outside the Plan's owning future.
    struct WorkerActivationCustody<W, P, Event>
    where
        W: Behavior,
        BehaviorAddr<W>: EndpointAddress,
        P: ActivationPlan,
    {
        task: WorkerCapabilityTask,
        permit: ActivationPermit<W>,
        grant: WorkerActivationGrant<W, P>,
        event: Option<Event>,
        failures: Vec<WorkerCapabilityFailure>,
        resumed_failure_origin: Option<WorkerCapabilityOperation>,
    }

    async fn activate_worker<W, P, Event, Path>(
        plan: P,
        custody: &Arc<TaskCustody<WorkerActivationCustody<W, P, Event>>>,
        control: &ControlSender<Event>,
    ) where
        W: Behavior,
        BehaviorAddr<W>: EndpointAddress,
        P: ActivationPlan,
        Event: InjectEvent<WorkerActivation<W, P>, Path>,
    {
        let mut original = custody.lock().await;
        original.task = WorkerCapabilityTask::Started(task::id());
        let constructed =
            catch_unwind(AssertUnwindSafe(move || plan.activate())).map_err(|payload| {
                WorkerCapabilityFailure {
                    operation: WorkerCapabilityOperation::Construction,
                    payload,
                }
            });
        let (reply, disposal) = capability_work_reply(constructed).await;
        let reply = match reply {
            Ok(reply) => Some(reply),
            Err(failure) => {
                original.failures.push(failure);
                None
            }
        };
        if let Some(failure) = disposal {
            original.failures.push(failure);
        }
        if let Some(reply) = reply {
            // Selected ActivationPlan outputs are already Send. No output static
            // bound or additional Send bound is imposed by this disposal boundary.
            // Original grant is borrowed; conversion may consume only its input.
            let input = original.grant.resolve(reply);
            match catch_unwind(AssertUnwindSafe(|| Event::inject_at(input))) {
                Ok(event) => original.event = Some(event),
                Err(payload) => original.failures.push(WorkerCapabilityFailure {
                    operation: WorkerCapabilityOperation::Receiving,
                    payload,
                }),
            }
        }
        if !original.failures.is_empty() {
            let (origin, payload) = original.failures.remove(0).into_parts();
            original.resumed_failure_origin = Some(origin);
            resume_unwind(payload);
        }
        let event = original
            .event
            .take()
            .expect("normal reply produced the actual Event");
        match control.send(event) {
            Ok(()) => {}
            Err(ControlClosed(event)) => original.event = Some(event),
        }
    }

    // This ordinary product has no lookup, scheduling or second task owner.
    // Each vector is a passive ordered result lane; the original ActivationTasks
    // remains the only owner of every spawned task and every resulting JoinError.
    impl<Inner, Activation, Preparation> TerminalReportTransaction
        for (
            Option<Inner>,
            Activation,
            Preparation,
            Option<CapabilityRetirement<Inner::Event, Inner::Descendants>>,
        )
    where
        Inner: TerminalReportTransaction + RetireCapabilities,
    {
        fn finish_terminal_reports(&mut self, disposition: TerminalReportDisposition) {
            if let Some(inner) = self.0.as_mut() {
                inner.finish_terminal_reports(disposition);
            }
        }
    }

    impl<Inner, Activation, Preparation> RetireCapabilities
        for (
            Option<Inner>,
            Activation,
            Preparation,
            Option<CapabilityRetirement<Inner::Event, Inner::Descendants>>,
        )
    where
        Inner: RetireCapabilities + Send,
        Inner::Event: Send,
        Inner::Descendants: Send,
        Activation: Send,
        Preparation: Send,
    {
        type Event = Inner::Event;
        type Descendants = (Inner::Descendants, (Activation, Preparation));
        async fn next_local_event(&mut self) -> Result<Self::Event, JoinError> {
            self.0
                .as_mut()
                .expect("live original capabilities remain")
                .next_local_event()
                .await
        }
        fn next_deadline(&mut self) -> Option<Instant> {
            self.0
                .as_mut()
                .expect("live original capabilities remain")
                .next_deadline()
        }
        fn pop_due(&mut self, now: Instant) -> Option<Self::Event> {
            self.0
                .as_mut()
                .expect("live original capabilities remain")
                .pop_due(now)
        }
        async fn receive_retirement(
            capabilities: &mut Option<Self>,
            received: &mut Option<CapabilityRetirement<Self::Event, Self::Descendants>>,
        ) {
            if received.is_some() {
                return;
            }
            let Some(original) = capabilities.as_mut() else {
                return;
            };
            Inner::receive_retirement(&mut original.0, &mut original.3).await;
            if !matches!((&original.0, &original.3), (None, Some(_))) {
                return;
            }
            let Some((None, activation, preparation, Some(lower))) = capabilities.take() else {
                unreachable!("the complete lower retirement was checked without another callback");
            };
            let CapabilityRetirement {
                activation_tasks,
                descendants,
                terminal_report,
                retirement_failures,
            } = lower;
            *received = Some(CapabilityRetirement {
                activation_tasks,
                descendants: (descendants, (activation, preparation)),
                terminal_report,
                retirement_failures,
            });
        }
        async fn retire(self) -> CapabilityRetirement<Self::Event, Self::Descendants> {
            let mut capabilities = Some(self);
            let mut received = None;
            Self::receive_retirement(&mut capabilities, &mut received).await;
            received.expect("normal lower retirement installs its actual output")
        }
    }

    impl<Inner, Activation, Preparation, Event, Source, Input> SourceAdmission<Event, Source, Input>
        for ActionInterpreter<(
            Option<Inner>,
            Activation,
            Preparation,
            Option<CapabilityRetirement<Inner::Event, Inner::Descendants>>,
        )>
    where
        Inner: SourceAdmission<Event, Source, Input> + RetireCapabilities + Send,
        Event: EventIngress<Source, Input>,
        Inner::Event: Send,
        Inner::Descendants: Send,
        Activation: Send,
        Preparation: Send,
        Input: Send,
    {
        #[expect(
            clippy::manual_async_fn,
            reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
        )]
        fn admit_source(
            &mut self,
            input: &mut Option<Input>,
            received: &mut Option<Result<(), Input>>,
        ) -> impl Future<Output = ()> + Send {
            async move {
                self.capabilities_mut()
                    .0
                    .as_mut()
                    .expect("live original capabilities remain")
                    .admit_source(input, received)
                    .await;
            }
        }
    }

    impl<Inner, Activation, Preparation, New, Event, Path>
        InterpretItem<Creations<CreateChild<MailAddr, New>>, Event, Path>
        for ActionInterpreter<(
            Option<Inner>,
            Activation,
            Preparation,
            Option<CapabilityRetirement<Inner::Event, Inner::Descendants>>,
        )>
    where
        Inner: InterpretItem<Creations<CreateChild<MailAddr, New>>, Event, Path>
            + RetireCapabilities
            + Send,
        New: Send,
        Inner::Event: Send,
        Inner::Descendants: Send,
        Activation: Send,
        Preparation: Send,
    {
        #[expect(
            clippy::manual_async_fn,
            reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
        )]
        fn interpret_item<'a>(
            &'a mut self,
            input: <Creations<CreateChild<MailAddr, New>> as ActionItem>::Input<'a>,
            received: &'a mut Option<<Creations<CreateChild<MailAddr, New>> as ActionItem>::Reply>,
        ) -> impl Future<Output = ()> + Send + 'a
        where
            Creations<CreateChild<MailAddr, New>>: 'a,
        {
            async move {
                <Inner as InterpretItem<Creations<CreateChild<MailAddr, New>>, Event, Path>>::interpret_item(
                    self.capabilities_mut().0.as_mut().expect("live original capabilities remain"), input, received,
                ).await;
            }
        }
    }

    impl<Inner, Activation, Preparation, Position, Child> EstablishChild<Position, Child>
        for ActionInterpreter<(
            Option<Inner>,
            Activation,
            Preparation,
            Option<CapabilityRetirement<Inner::Event, Inner::Descendants>>,
        )>
    where
        Inner: EstablishChild<Position, Child> + RetireCapabilities + Send,
        Child: Behavior<Protocol: Protocol<Addr = MailAddr>> + Send,
        Inner::Event: Send,
        Inner::Descendants: Send,
        Activation: Send,
        Preparation: Send,
    {
        async fn establish_child(
            &mut self,
            creation: RoutedCreation<MailAddr, Child>,
        ) -> ItemSettlement<
            RoutedCreation<MailAddr, Child>,
            ChildCreationOutcome<Child, Position>,
            CreationRejection,
            Never,
        > {
            self.capabilities_mut()
                .0
                .as_mut()
                .expect("live original capabilities remain")
                .establish_child(creation)
                .await
        }
    }

    impl<C, N, Parent, Bindings, Origins, W, P, Preparation, Path>
        InterpretItem<BeginActivation<W, P>, C::Event, Path>
        for ActionInterpreter<(
            Option<ApplicationCapabilities<C, N, Parent, Bindings, Origins>>,
            Arc<Mutex<Vec<Arc<TaskCustody<WorkerActivationCustody<W, P, C::Event>>>>>>,
            Preparation,
            Option<CapabilityRetirement<
                <ApplicationCapabilities<C, N, Parent, Bindings, Origins> as RetireCapabilities>::Event,
                <ApplicationCapabilities<C, N, Parent, Bindings, Origins> as RetireCapabilities>::Descendants,
            >>,
        )>
    where
        C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
        C::Event: InjectEvent<WorkerActivation<W, P>, Path> + Send + 'static,
        W: Behavior + 'static,
        BehaviorAddr<W>: EndpointAddress,
        EstablishedRecipient<W::Protocol>: Send,
        ActivationPermit<W>: Send,
        P: ActivationPlan + 'static,
        Preparation: Send,
        ApplicationCapabilities<C, N, Parent, Bindings, Origins>: RetireCapabilities,
        Self: Send,
    {
        #[expect(
            clippy::manual_async_fn,
            reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
        )]
        fn interpret_item<'a>(
            &'a mut self,
            input: <BeginActivation<W, P> as ActionItem>::Input<'a>,
            received: &'a mut Option<<BeginActivation<W, P> as ActionItem>::Reply>,
        ) -> impl Future<Output = ()> + Send + 'a
        where
            BeginActivation<W, P>: 'a,
        {
            async move {
                if received.is_some() {
                    return;
                }
                let Some(request) = input.as_ref() else {
                    return;
                };
                self.capabilities_mut().0.as_mut().expect("live original capabilities remain").inject_control_event::<_, Path>(request.started());
                let Some(request) = input.take() else {
                    return;
                };
                let settlement = {
            let (permit, plan, grant) = request.into_parts();
            let custody = Arc::new(TaskCustody::new(WorkerActivationCustody {
                task: WorkerCapabilityTask::Unpolled,
                permit,
                grant,
                event: None,
                failures: Vec::new(),
                resumed_failure_origin: None,
            }));
            self.capabilities_mut()
                .1
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(custody.clone());
            let control = self.capabilities_mut().0.as_ref().expect("live original capabilities remain").control.clone();
            self.capabilities_mut()
                .0
                .as_mut().expect("live original capabilities remain")
                .activation_tasks
                .as_mut().expect("live original capability tasks remain")
                .spawn(async move {
                    activate_worker::<W, P, C::Event, Path>(plan, &custody, &control).await;
                    Ok(())
                });
            ItemSettlement::Accepted(())

                };
                *received = Some(settlement);

            }
        }
    }

    impl<C, N, Parent, Bindings, Origins, Source, Role, W, P, Activation, Path>
        InterpretItem<PrepareWorkers<Source, Role, W, P>, C::Event, Path>
        for ActionInterpreter<(
            Option<ApplicationCapabilities<C, N, Parent, Bindings, Origins>>,
            Activation,
            Arc<
                Mutex<
                    Vec<Arc<TaskCustody<WorkerPreparationCustody<Source, Role, W, P, C::Event>>>>,
                >,
            >,
            Option<CapabilityRetirement<
                <ApplicationCapabilities<C, N, Parent, Bindings, Origins> as RetireCapabilities>::Event,
                <ApplicationCapabilities<C, N, Parent, Bindings, Origins> as RetireCapabilities>::Descendants,
            >>,
        )>
    where
        C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
        C::Event: InjectEvent<WorkerPreparation<Source, Role, W, P>, Path> + Send + 'static,
        Source: WorkerPreparationSource<Role, W, P> + Send + 'static,
        Role: Send + Sync + 'static,
        W: Behavior + Send + 'static,
        P: ActivationPlan + 'static,
        Activation: Send,
        ApplicationCapabilities<C, N, Parent, Bindings, Origins>: RetireCapabilities,
        Self: Send,
    {
        #[expect(
            clippy::manual_async_fn,
            reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
        )]
        fn interpret_item<'a>(
            &'a mut self,
            input: <PrepareWorkers<Source, Role, W, P> as ActionItem>::Input<'a>,
            received: &'a mut Option<<PrepareWorkers<Source, Role, W, P> as ActionItem>::Reply>,
        ) -> impl Future<Output = ()> + Send + 'a
        where
            PrepareWorkers<Source, Role, W, P>: 'a,
        {
            async move {
                if received.is_some() {
                    return;
                }
                let Some(request) = input.take() else {
                    return;
                };
                let settlement = {
            let (receipt, starting) = request.start();
            let custody = Arc::new(TaskCustody::new(WorkerPreparationCustody {
                task: WorkerCapabilityTask::Unpolled,
                phase: Some(WorkerPreparationPhase::Starting(starting)),
                event: None,
                failures: Vec::new(),
                resumed_failure_origin: None,
            }));
            self.capabilities_mut()
                .2
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(custody.clone());
            let control = self.capabilities_mut().0.as_ref().expect("live original capabilities remain").control.clone();
            self.capabilities_mut()
                .0
                .as_mut().expect("live original capabilities remain")
                .activation_tasks
                .as_mut().expect("live original capability tasks remain")
                .spawn(async move {
                    prepare_workers::<Source, Role, W, P, C::Event, Path>(&custody, &control).await;
                    Ok(())
                });
            ItemSettlement::Accepted(receipt)

                };
                *received = Some(settlement);

            }
        }
    }
    #[cfg(test)]
    mod activation_declaration {
        use core::marker::PhantomData;
        use std::panic::panic_any;
        use std::sync::Arc;

        use crate::local::ActivationTasks;
        use behavior::{
            Actions, ActiveTurn, Address, Behavior, BehaviorActed, BehaviorBase,
            ChildCreationOutcome, CommittedChild, CreateChild, CreationId, CreationKind,
            CreationSettlement, Creations, CreationsSettled, EndpointAddress, EstablishedActor,
            InterpretEstablished, ItemSettlement, MessageProtocol, Never, NoBirths, NoSends,
            Protocol, SettledItem, Step, User,
        };
        use behavior_actors::atomic::{
            ActivationPlan, InitialWorkerOutcome, ProxyControl, ProxyOutcome, StableProxy,
            WorkerActivation, WorkerInitializationOutcome, WorkerInitializationReport,
            WorkerStartResult,
        };
        use behavior_actors::{Activate as _, Active};
        use communication::{Config, ControlClosed, mailbox_channel};

        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(super) struct ActivationMailAddr(u64);

        impl Address for ActivationMailAddr {
            type Nonce = u64;
        }

        #[derive(Clone)]
        pub(super) struct ActivationEndpoint(pub(super) Arc<Vec<u8>>);

        impl InterpretEstablished<MessageProtocol<ActivationMailAddr, Never>> for ActivationEndpoint {
            type Output = ();

            fn interpret_established(&mut self, endpoint: ActivationEndpoint) {
                *self = endpoint;
            }
        }

        pub(super) struct ActivationInstalled<B: Behavior>(
            ActivationEndpoint,
            PhantomData<fn() -> B>,
        );

        impl<B: Behavior> Clone for ActivationInstalled<B> {
            fn clone(&self) -> Self {
                Self(self.0.clone(), PhantomData)
            }
        }

        impl EndpointAddress for ActivationMailAddr {
            type Established<P>
                = ActivationEndpoint
            where
                P: Protocol<Addr = Self>;
            type Installed<B>
                = ActivationInstalled<B>
            where
                B: Behavior<Protocol: Protocol<Addr = Self>>;

            fn recipient<B>(installed: &Self::Installed<B>) -> Self::Established<B::Protocol>
            where
                B: Behavior<Protocol: Protocol<Addr = Self>>,
            {
                installed.0.clone()
            }
        }

        pub(super) struct ActivationWorker;

        impl BehaviorBase for ActivationWorker {
            type Base = Self;

            fn base(&self) -> &Self::Base {
                self
            }
        }

        impl Behavior for ActivationWorker {
            type Protocol = MessageProtocol<ActivationMailAddr, Never>;
            type Event = User<ActivationMailAddr, Never>;
            type Sends = NoSends;
            type Ph = Never;
            type Error = Never;
            type Birth = NoBirths;

            fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
                match event.message {}
            }
        }

        pub(super) enum ActivationWork {
            Ready,
            Panicked(Arc<Vec<u8>>),
        }

        impl ActivationPlan for ActivationWork {
            type Ready = ();
            type Rejection = Never;

            #[expect(
                clippy::unused_async_trait_impl,
                reason = "Keep the original selected plan and its native panic cold until the future is polled."
            )]
            async fn activate(self) -> Result<(), Never> {
                match self {
                    Self::Ready => Ok(()),
                    Self::Panicked(original) => panic_any(original),
                }
            }
        }

        #[expect(
            clippy::type_complexity,
            reason = "observe the existing proxy's complete concrete birth and seven named send lanes"
        )]
        pub(super) fn assert_proxy_actions(
            actions: &Actions<
                ActivationMailAddr,
                Never,
                <StableProxy<ActivationWorker, ActivationWork> as Behavior>::Sends,
                <StableProxy<ActivationWorker, ActivationWork> as Behavior>::Birth,
            >,
            births: usize,
            observation: &[CreationId],
            initialization: usize,
            activation: usize,
            reports: usize,
        ) {
            assert_eq!(actions.creates.len(), births);
            let actual: Vec<_> = actions
                .sends
                .worker_observations
                .iter()
                .map(|request| request.child)
                .collect();
            assert_eq!(actual, observation);
            assert_eq!(actions.sends.worker_initializations.len(), initialization);
            assert_eq!(actions.sends.worker_activations.len(), activation);
            assert_eq!(actions.sends.worker_shutdowns.len(), 0);
            assert_eq!(actions.sends.worker_deliveries.len(), 0);
            assert_eq!(actions.sends.owner_outcomes.len(), reports);
            assert_eq!(actions.sends.diagnostics.len(), 0);
            assert!(matches!(actions.become_, Step::Continue));
        }

        pub(super) fn initialize_activation(
            original_endpoint: Arc<Vec<u8>>,
            plan: ActivationWork,
        ) -> (
            Active<StableProxy<ActivationWorker, ActivationWork>>,
            WorkerInitializationReport<ActivationWorker, ActivationWork>,
            CreationId,
        ) {
            let proxy = StableProxy::<ActivationWorker, ActivationWork>::activated();
            let initialized = proxy
                .initialize()
                .unwrap_or_else(|_| panic!("pure proxy initialization"));
            assert_proxy_actions(&initialized.actions, 0, &[], 0, 0, 0);
            let mut proxy = initialized.behavior;
            let creating = proxy
                .on(ProxyControl::start_with(ActivationWorker, plan))
                .unwrap_or_else(|_| panic!("original worker start is admitted"));
            let ids: Vec<_> = creating.creates.iter().map(CreateChild::id).collect();
            assert_proxy_actions(&creating, 1, &ids, 0, 0, 0);
            let mut births = creating.creates.into_iter();
            let birth = births.next().expect("one original worker birth");
            let extra = births.next();
            assert!(extra.is_none());
            let (id, worker, kind) = birth.into_parts();
            assert_eq!(kind, CreationKind::Birth);
            let initialized_worker = worker
                .initialize()
                .unwrap_or_else(|_| panic!("pure worker initialization"));
            assert_eq!(initialized_worker.actions.creates.len(), 0);
            assert_eq!(initialized_worker.actions.sends.owned, NoSends);
            assert_eq!(initialized_worker.actions.sends.inner, NoSends);
            assert!(matches!(initialized_worker.actions.become_, Step::Continue));
            let established = EstablishedActor::issued(ActivationInstalled(
                ActivationEndpoint(original_endpoint),
                PhantomData,
            ));
            let committed = CreationsSettled::new(CreationSettlement::Settled(Creations::one(
                SettledItem::Attempted(ItemSettlement::Accepted(
                    ChildCreationOutcome::Established(CommittedChild::new(id, kind, established)),
                )),
            )));
            let initializing = proxy
                .on(committed)
                .unwrap_or_else(|_| panic!("original worker binding commits"));
            assert_proxy_actions(&initializing, 0, &[], 1, 0, 0);
            let mut requests = initializing.sends.worker_initializations.into_requests();
            let request = requests.pop().expect("one original initialization request");
            assert_eq!(requests.len(), 0);
            assert_eq!(request.worker().creation(), id);
            let report = request.resolve(WorkerInitializationOutcome::ReadyForActivation);
            (proxy, report, id)
        }

        #[tokio::test]
        async fn ordinary_public_activation_returns_original_ready_policy() {
            let endpoint = Arc::new(vec![107, 109]);
            let endpoint_original = Arc::downgrade(&endpoint);
            let (mut proxy, report, id) = initialize_activation(endpoint, ActivationWork::Ready);
            let activating = proxy
                .on(report)
                .unwrap_or_else(|_| panic!("original initialization correlation is accepted"));
            assert_proxy_actions(&activating, 0, &[], 0, 1, 0);
            let mut requests = activating.sends.worker_activations.into_requests();
            let request = requests.pop().expect("one original BeginActivation");
            assert_eq!(requests.len(), 0);
            let worker = request.worker();
            let started = proxy
                .on(request.started())
                .unwrap_or_else(|_| panic!("actual private activation grant is accepted"));
            assert_proxy_actions(&started, 0, &[], 0, 0, 0);
            let (control, owner, mailbox, receiver) = mailbox_channel::<
                WorkerActivation<ActivationWorker, ActivationWork>,
                User<ActivationMailAddr, Never>,
            >(Config::new(2));
            drop(receiver);
            let mut tasks = ActivationTasks::new();
            tasks.spawn(async move {
                control
                    .send(request.activate().await)
                    .map_err(|ControlClosed(event)| event)
            });
            let (mut closed, failures) = tasks.settle().await;
            assert_eq!(closed.len(), 1);
            assert_eq!(failures.len(), 0);
            let returned = closed
                .pop()
                .expect("the actual closed lane returns original Ready input");
            assert_eq!(returned.worker(), worker);
            assert_eq!(worker.creation(), id);
            let reported = proxy.on(returned).unwrap_or_else(|_| {
                panic!("the actual private grant admits its original Ready input")
            });
            assert_proxy_actions(&reported, 0, &[], 0, 0, 1);
            let mut reports = reported.sends.owner_outcomes.into_requests();
            let outcome = reports
                .pop()
                .expect("one complete original readiness report")
                .into_inner();
            assert_eq!(reports.len(), 0);
            match &outcome {
                ProxyOutcome::Initial {
                    outcome:
                        InitialWorkerOutcome::Resolved {
                            result: WorkerStartResult::Ready { attempt, readiness },
                        },
                } => {
                    assert_eq!(*readiness, ());
                    assert_eq!(attempt.to_owned(), worker);
                    assert_eq!(attempt.creation(), id);
                }
                ProxyOutcome::Initial { .. }
                | ProxyOutcome::Replacement { .. }
                | ProxyOutcome::WorkerStopped { .. }
                | ProxyOutcome::Unavailable { .. } => {
                    panic!("complete original Ready outcome required")
                }
            }
            drop((outcome, reports, proxy, closed, failures, owner, mailbox));
            let released = endpoint_original.strong_count();
            assert_eq!(released, 0);
        }
    }
    mod grouped_preparation {
        use std::convert::Infallible;
        use std::marker::PhantomData;
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        use behavior::{
            Actions, ActiveTurn, Address, Behavior, BehaviorActed, BehaviorBase,
            ChildCreationOutcome, ChildInputReason, ChildReport, CommittedChild, CreateChild,
            CreationId, CreationKind, CreationSettlement, Creations, CreationsSettled,
            EndpointAddress, EstablishedActor, Interpretation, InterpretationProgress,
            ItemSettlement, MessageProtocol, Never, NoBirths, NoSends, Protocol, SettledItem, Step,
            User,
        };
        use behavior_actors::atomic::{
            ActivationPolicy, ActorDrainPolicy, DiagnosticDisposition, FailureReaction,
            FixedSupervisor, ImmediateActivation, InitialWorkerOutcome, OrderedRoles,
            PrepareWorkers, ProxyControl, ProxyControlAdmission, ProxyOperation, ProxyOutcome,
            Recovery, RestartLimit, RestartRelease, StableProxy, Strategy,
            WorkerInitializationOutcome, WorkerSource, WorkerStartResult, WorkerSubmission, fixed,
        };
        use behavior_actors::{Activate as _, ChildStopped, Exit};

        use crate::worker_preparation::{WorkerPreparationSource, WorkerPreparationStart};
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(super) struct PoolMailAddr(u64);

        impl Address for PoolMailAddr {
            type Nonce = u64;
        }

        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(super) struct PoolEndpoint(u64);

        pub(super) struct PoolInstalled<B: Behavior>(PoolEndpoint, PhantomData<fn() -> B>);

        impl<B: Behavior> Clone for PoolInstalled<B> {
            fn clone(&self) -> Self {
                Self(self.0, PhantomData)
            }
        }

        impl EndpointAddress for PoolMailAddr {
            type Established<P>
                = PoolEndpoint
            where
                P: Protocol<Addr = Self>;

            type Installed<B>
                = PoolInstalled<B>
            where
                B: Behavior<Protocol: Protocol<Addr = Self>>;

            fn recipient<B>(installed: &Self::Installed<B>) -> Self::Established<B::Protocol>
            where
                B: Behavior<Protocol: Protocol<Addr = Self>>,
            {
                installed.0
            }
        }

        #[derive(Debug, Eq, PartialEq)]
        pub(super) struct WorkerRole(pub(super) Arc<Vec<u8>>);

        pub(super) struct ReplacementWorker(pub(super) Arc<Vec<u8>>);

        impl BehaviorBase for ReplacementWorker {
            type Base = Self;

            fn base(&self) -> &Self::Base {
                self
            }
        }

        impl Behavior for ReplacementWorker {
            type Protocol = MessageProtocol<PoolMailAddr, Never>;
            type Event = User<PoolMailAddr, Never>;
            type Sends = NoSends;
            type Ph = Never;
            type Error = Never;
            type Birth = NoBirths;

            fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
                match event.message {}
            }
        }

        pub(super) struct PreparationSource {
            pub(super) material: Arc<Vec<u8>>,
            pub(super) first_worker: Option<Arc<Vec<u8>>>,
            pub(super) requested_roles: Vec<Vec<u8>>,
        }

        impl WorkerSource<WorkerRole, ReplacementWorker, ImmediateActivation> for PreparationSource {
            type WorkerRejection = Never;
            type SourceRejection = Never;
        }

        impl WorkerPreparationSource<WorkerRole, ReplacementWorker, ImmediateActivation>
            for PreparationSource
        {
            #[expect(
                clippy::unused_async_trait_impl,
                reason = "Prepare the original worker only when this borrowed operation is polled."
            )]
            async fn prepare_first(
                &mut self,
                role: &WorkerRole,
            ) -> WorkerPreparationStart<ReplacementWorker, ImmediateActivation, Never, Never>
            {
                self.requested_roles.push(role.0.as_ref().clone());
                let original = self
                    .first_worker
                    .take()
                    .expect("one original replacement worker");
                WorkerPreparationStart::Submitted(WorkerSubmission::immediate(ReplacementWorker(
                    original,
                )))
            }

            async fn prepare_next(
                &mut self,
                role: &WorkerRole,
            ) -> Result<WorkerSubmission<ReplacementWorker, ImmediateActivation>, Never>
            {
                self.requested_roles.push(role.0.as_ref().clone());
                panic!("later preparation failed while borrowing the source");
            }
        }

        struct ProxyAdmission {
            actor: Option<EstablishedActor<StableProxy<ReplacementWorker, ImmediateActivation>>>,
            admitted: Option<(
                CreationId,
                ProxyControl<ReplacementWorker, ImmediateActivation>,
            )>,
        }

        impl ProxyControlAdmission<ReplacementWorker, ImmediateActivation> for ProxyAdmission {
            fn admit_proxy_control(
                &mut self,
                creation: CreationId,
                control: ProxyControl<ReplacementWorker, ImmediateActivation>,
            ) -> ItemSettlement<
                ProxyControl<ReplacementWorker, ImmediateActivation>,
                EstablishedActor<StableProxy<ReplacementWorker, ImmediateActivation>>,
                ChildInputReason,
                Never,
            > {
                assert!(self.admitted.is_none());
                self.admitted = Some((creation, control));
                ItemSettlement::Accepted(self.actor.take().expect("one original committed proxy"))
            }
        }

        #[expect(
            clippy::type_complexity,
            reason = "the observation spells the actual supervisor creation and send products without replacing its owning lanes"
        )]
        fn assert_supervisor_actions(
            actions: &Actions<
                PoolMailAddr,
                Never,
                <FixedSupervisor<
                    WorkerRole,
                    ReplacementWorker,
                    ImmediateActivation,
                    PreparationSource,
                    Infallible,
                    Infallible,
                > as Behavior>::Sends,
                <FixedSupervisor<
                    WorkerRole,
                    ReplacementWorker,
                    ImmediateActivation,
                    PreparationSource,
                    Infallible,
                    Infallible,
                > as Behavior>::Birth,
            >,
            creations: usize,
            observations: &[CreationId],
            operations: usize,
            preparations: usize,
        ) {
            assert_eq!(actions.creates.len(), creations);
            let actual: Vec<_> = actions
                .sends
                .proxy_observations
                .iter()
                .map(|request| request.child)
                .collect();
            assert_eq!(actual, observations);
            assert_eq!(actions.sends.proxy_operations.len(), operations);
            assert_eq!(actions.sends.worker_preparations.len(), preparations);
            assert!(actions.sends.restart_schedules.is_empty());
            assert_eq!(actions.sends.lifecycle, NoSends);
            assert!(actions.sends.status_replies.as_slice().is_empty());
            assert!(actions.sends.capability_replies.as_slice().is_empty());
            assert!(actions.sends.diagnostics.is_empty());
            assert!(matches!(actions.become_, Step::Continue));
        }

        #[expect(
            clippy::type_complexity,
            reason = "the observation preserves the actual proxy birth and send associated products"
        )]
        pub(super) fn assert_proxy_actions(
            actions: &Actions<
                PoolMailAddr,
                Never,
                <StableProxy<ReplacementWorker, ImmediateActivation> as Behavior>::Sends,
                <StableProxy<ReplacementWorker, ImmediateActivation> as Behavior>::Birth,
            >,
            creations: usize,
            observations: &[CreationId],
            initializations: usize,
            activations: usize,
            outcomes: usize,
        ) {
            assert_eq!(actions.creates.len(), creations);
            let actual: Vec<_> = actions
                .sends
                .worker_observations
                .iter()
                .map(|request| request.child)
                .collect();
            assert_eq!(actual, observations);
            assert_eq!(actions.sends.worker_initializations.len(), initializations);
            assert_eq!(actions.sends.worker_activations.len(), activations);
            assert!(actions.sends.worker_shutdowns.is_empty());
            assert_eq!(actions.sends.worker_deliveries.len(), 0);
            assert_eq!(actions.sends.owner_outcomes.len(), outcomes);
            assert!(actions.sends.diagnostics.is_empty());
            assert!(matches!(actions.become_, Step::Continue));
        }

        #[expect(
            clippy::too_many_lines,
            reason = "one complete grouped producer trace retains proxy creation, worker commitment, initialization, activation and preparation receipt ownership"
        )]
        pub(super) async fn admitted_group(
            source: PreparationSource,
            roles: OrderedRoles<WorkerRole>,
        ) -> PrepareWorkers<PreparationSource, WorkerRole, ReplacementWorker, ImmediateActivation>
        {
            let activation = ActivationPolicy::new(3).expect("three positive activation slots");
            let built = fixed(
                |role: &WorkerRole| -> Result<WorkerSubmission<ReplacementWorker, ImmediateActivation>, Never> {
                    Ok(WorkerSubmission::immediate(ReplacementWorker(Arc::new(role.0.as_ref().clone()))))
                },
                roles,
                activation,
                Recovery::permanent(source, Strategy::OneForAll, RestartLimit::new(3, Duration::from_secs(60)), RestartRelease::immediate()),
                FailureReaction::StopSupervisor,
                ActorDrainPolicy::WaitForActorGraph,
                DiagnosticDisposition::<Infallible>::terminate(),
            ).build::<ReplacementWorker, ImmediateActivation, Never>()
                .unwrap_or_else(|_| panic!("all original declarations prepare"));
            let initialized = built
                .initialize()
                .unwrap_or_else(|_| panic!("pure fixed initialization"));
            let mut supervisor = initialized.behavior;
            let proxy_ids: Vec<_> = initialized
                .actions
                .creates
                .iter()
                .map(CreateChild::id)
                .collect();
            assert_supervisor_actions(&initialized.actions, 3, &proxy_ids, 0, 0);
            let mut proxies = Vec::new();
            let mut committed = Vec::new();
            for (creation, endpoint) in initialized.actions.creates.into_iter().zip([801, 802, 803])
            {
                let (id, proxy, kind) = creation.into_parts();
                assert_eq!(kind, CreationKind::Birth);
                let initialized_proxy = proxy
                    .initialize()
                    .unwrap_or_else(|_| panic!("pure proxy initialization"));
                assert_proxy_actions(&initialized_proxy.actions, 0, &[], 0, 0, 0);
                proxies.push((id, initialized_proxy.behavior));
                let actor =
                    EstablishedActor::issued(PoolInstalled(PoolEndpoint(endpoint), PhantomData));
                committed.push(SettledItem::Attempted(ItemSettlement::Accepted(
                    ChildCreationOutcome::Established(CommittedChild::new(id, kind, actor)),
                )));
            }
            let dispatched = supervisor
                .on(CreationsSettled::new(CreationSettlement::Settled(
                    committed.into_iter().collect(),
                )))
                .unwrap_or_else(|_| panic!("actual declared proxy routes commit"));
            assert_supervisor_actions(&dispatched, 0, &[], 3, 0);
            let mut operations = dispatched.sends.proxy_operations.into_items();
            let operation = operations.remove(0);
            let proxy_id = operation.creation();
            let (original_proxy_id, mut proxy) = proxies.remove(0);
            assert_eq!(proxy_id, original_proxy_id);
            let mut admission = ProxyAdmission {
                actor: Some(EstablishedActor::issued(PoolInstalled(
                    PoolEndpoint(801),
                    PhantomData,
                ))),
                admitted: None,
            };
            let mut operation = Some(InterpretationProgress::Original(operation));
            ProxyOperation::settle(&mut operation, &mut admission);
            let admitted = match operation {
                Some(InterpretationProgress::Completed(admitted)) => admitted,
                retained => {
                    drop(retained);
                    panic!("original proxy admission must return its complete settlement");
                }
            };
            let Interpretation::Complete(ItemSettlement::Accepted(receipt)) = admitted else {
                drop(admitted);
                panic!("original proxy admission must be complete and accepted");
            };
            let (admitted_id, control) = admission
                .admitted
                .take()
                .expect("whole original control retained");
            assert_eq!(admitted_id, proxy_id);
            let settled = supervisor
                .on(SettledItem::Attempted(ItemSettlement::Accepted(receipt)))
                .unwrap_or_else(|_| panic!("actual proxy receipt accepted"));
            assert_supervisor_actions(&settled, 0, &[], 0, 0);
            let creating = proxy
                .on(control)
                .unwrap_or_else(|_| panic!("original initial worker control"));
            let worker_ids: Vec<_> = creating.creates.iter().map(CreateChild::id).collect();
            assert_proxy_actions(&creating, 1, &worker_ids, 0, 0, 0);
            let mut births = creating.creates.into_iter();
            let creation = births.next().expect("one exact original worker birth");
            let extra = births.next();
            assert!(extra.is_none());
            let (worker_id, worker, kind) = creation.into_parts();
            assert_eq!(kind, CreationKind::Birth);
            let original_worker = worker
                .initialize()
                .unwrap_or_else(|_| panic!("pure worker initialization"));
            assert_eq!(original_worker.behavior.base().0.as_slice(), &[10]);
            assert!(original_worker.actions.creates.is_empty());
            assert_eq!(original_worker.actions.sends.inner, NoSends);
            assert_eq!(original_worker.actions.sends.owned, NoSends);
            assert!(matches!(original_worker.actions.become_, Step::Continue));
            let worker_commit = CreationsSettled::new(CreationSettlement::Settled(Creations::one(
                SettledItem::Attempted(ItemSettlement::Accepted(
                    ChildCreationOutcome::Established(CommittedChild::new(
                        worker_id,
                        kind,
                        EstablishedActor::issued(PoolInstalled(PoolEndpoint(990), PhantomData)),
                    )),
                )),
            )));
            let initializing = proxy
                .on(worker_commit)
                .unwrap_or_else(|_| panic!("actual worker route commits"));
            assert_proxy_actions(&initializing, 0, &[], 1, 0, 0);
            let mut initializations = initializing.sends.worker_initializations.into_requests();
            let initialization = initializations
                .pop()
                .expect("one exact initialization observation");
            assert!(initializations.is_empty());
            let activating = proxy
                .on(initialization.resolve(WorkerInitializationOutcome::ReadyForActivation))
                .unwrap_or_else(|_| panic!("actual initialization correlated"));
            assert_proxy_actions(&activating, 0, &[], 0, 1, 0);
            let mut activations = activating.sends.worker_activations.into_requests();
            let activation = activations.pop().expect("one activation permit");
            assert!(activations.is_empty());
            let starting = proxy
                .on(activation.started())
                .unwrap_or_else(|_| panic!("exact activation start"));
            assert_proxy_actions(&starting, 0, &[], 0, 0, 0);
            let ready = activation.activate().await;
            let reporting = proxy
                .on(ready)
                .unwrap_or_else(|_| panic!("actual immediate readiness"));
            assert_proxy_actions(&reporting, 0, &[], 0, 0, 1);
            let mut reports = reporting.sends.owner_outcomes.into_requests();
            let outcome = reports
                .pop()
                .expect("one whole original readiness report")
                .into_inner();
            assert!(reports.is_empty());
            let attempt = match &outcome {
                ProxyOutcome::Initial {
                    outcome:
                        InitialWorkerOutcome::Resolved {
                            result: WorkerStartResult::Ready { attempt, readiness },
                        },
                } => {
                    assert_eq!(*readiness, ());
                    assert_eq!(attempt.creation(), worker_id);
                    attempt.clone()
                }
                ProxyOutcome::Initial { .. }
                | ProxyOutcome::Replacement { .. }
                | ProxyOutcome::WorkerStopped { .. }
                | ProxyOutcome::Unavailable { .. } => {
                    panic!("complete initial Ready outcome required")
                }
            };
            let roster_ready = supervisor
                .on(ChildReport::new(proxy_id, outcome))
                .unwrap_or_else(|_| panic!("actual roster readiness"));
            assert_supervisor_actions(&roster_ready, 0, &[], 0, 0);
            let stopped = ChildStopped::new(attempt.creation(), Ok(Exit::Normal), Instant::now());
            let preparing = supervisor
                .on(ChildReport::new(
                    proxy_id,
                    ProxyOutcome::WorkerStopped {
                        worker: attempt,
                        stopped,
                    },
                ))
                .unwrap_or_else(|_| panic!("OneForAll selects all declared roles"));
            assert_supervisor_actions(&preparing, 0, &[], 0, 1);
            let mut requests = preparing.sends.worker_preparations.into_items();
            let request = requests.pop().expect("one actual grouped producer request");
            assert!(requests.is_empty());
            // These pure declarations and unattempted proxy requests are explicitly discharged.
            // No actor task was installed by this pure producer controller.
            drop(supervisor);
            drop(operations);
            drop(proxies);
            drop(proxy);
            drop(original_worker.behavior);
            request
        }
    }

    use std::mem;
    use std::panic::panic_any;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::Context;

    enum WorkDisposal {
        Ordinary,
        Panicked(Arc<Vec<u8>>),
    }
    struct BorrowedCapabilityWork<'a> {
        original: &'a Vec<u8>,
        disposal: WorkDisposal,
    }
    impl<'a> Future for BorrowedCapabilityWork<'a> {
        type Output = (&'a Vec<u8>, Rc<Vec<u8>>);
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready((self.original, Rc::new(vec![151, 157])))
        }
    }
    impl Drop for BorrowedCapabilityWork<'_> {
        fn drop(&mut self) {
            let disposal = mem::replace(&mut self.disposal, WorkDisposal::Ordinary);
            match disposal {
                WorkDisposal::Ordinary => {}
                WorkDisposal::Panicked(payload) => panic_any(payload),
            }
        }
    }

    #[tokio::test]
    async fn borrowed_ready_reply_survives_native_disposal_without_output_send_bound() {
        let source = vec![139, 149];
        let original_source_pointer = source.as_ptr();
        let cause = Arc::new(vec![163, 167]);
        let original_cause = Arc::downgrade(&cause);
        let (reply, disposal) = capability_work_reply(Ok(BorrowedCapabilityWork {
            original: &source,
            disposal: WorkDisposal::Panicked(cause),
        }))
        .await;
        let (copied, original_reply) = match reply {
            Ok(reply) => reply,
            Err(failure) => {
                drop(failure);
                panic!("actual Ready is acquired before disposal")
            }
        };
        let reply_original = Rc::downgrade(&original_reply);
        assert_eq!(copied, &source);
        assert_eq!(copied.as_ptr(), original_source_pointer);
        assert_eq!(original_reply.as_slice(), [151, 157]);
        let failure = disposal.expect("actual original future destructor panicked");
        assert_eq!(failure.operation, WorkerCapabilityOperation::Disposal);
        assert_eq!(original_cause.strong_count(), 1);
        assert_eq!(reply_original.strong_count(), 1);
        drop(failure);
        let cause_released = original_cause.strong_count();
        let reply_retained = reply_original.strong_count();
        drop(original_reply);
        let reply_released = reply_original.strong_count();
        assert_eq!(cause_released, 0);
        assert_eq!(reply_retained, 1);
        assert_eq!(reply_released, 0);
    }

    struct PanickingCapabilityWork {
        poll: Option<Arc<Vec<u8>>>,
        disposal: Option<Arc<Vec<u8>>>,
    }
    impl Future for PanickingCapabilityWork {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            panic_any(self.poll.take().expect("one original poll cause"));
        }
    }
    impl Drop for PanickingCapabilityWork {
        fn drop(&mut self) {
            if let Some(payload) = self.disposal.take() {
                panic_any(payload);
            }
        }
    }
    #[tokio::test]
    async fn capability_poll_and_disposal_keep_both_original_native_causes() {
        let poll = Arc::new(vec![173]);
        let poll_original = Arc::downgrade(&poll);
        let disposal = Arc::new(vec![179]);
        let disposal_original = Arc::downgrade(&disposal);
        let (reply, disposed) = capability_work_reply(Ok(PanickingCapabilityWork {
            poll: Some(poll),
            disposal: Some(disposal),
        }))
        .await;
        let Err(primary) = reply else {
            panic!("the genuine poll panics")
        };
        let secondary = disposed.expect("the genuine destructor independently panics");
        assert_eq!(primary.operation, WorkerCapabilityOperation::Execution);
        assert_eq!(secondary.operation, WorkerCapabilityOperation::Disposal);
        assert_eq!(poll_original.strong_count(), 1);
        assert_eq!(disposal_original.strong_count(), 1);
        drop((primary, secondary));
        let released = (
            poll_original.strong_count(),
            disposal_original.strong_count(),
        );
        assert_eq!(released, (0, 0));
    }
    use crate::actor_interface::ExtractLocalEndpoint;
    use activation_declaration::{
        ActivationEndpoint, ActivationWork, ActivationWorker, initialize_activation,
    };
    use behavior::{ChildChoice, CreationKind, CreationSettlement, SettledItem};
    use grouped_preparation::{PreparationSource, ReplacementWorker, WorkerRole, admitted_group};

    struct CapabilityChild {
        original: Arc<Vec<u8>>,
    }
    impl BehaviorBase for CapabilityChild {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }
    impl Behavior for CapabilityChild {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = User<MailAddr, Never>;
        type Sends = NoSends;
        type Birth = NoBirths;
        type Ph = Never;
        type Error = Never;
        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            Ok(Actions::stop())
        }
        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            match event.message {}
        }
    }

    enum CapabilityEvent {
        User(User<MailAddr, Never>),
        Activation(WorkerActivation<ActivationWorker, ActivationWork>),
        Preparation(
            WorkerPreparation<
                PreparationSource,
                WorkerRole,
                ReplacementWorker,
                ImmediateActivation,
            >,
        ),
        ActivationSource(ActionItemResult<BeginActivation<ActivationWorker, ActivationWork>>),
        PreparationSource(
            ActionItemResult<
                PrepareWorkers<
                    PreparationSource,
                    WorkerRole,
                    ReplacementWorker,
                    ImmediateActivation,
                >,
            >,
        ),
        Shutdown(ShutdownRequested),
    }
    impl UserEvent for CapabilityEvent {
        type Addr = MailAddr;
        type Message = Never;
        fn user(from: MailAddr, message: Never) -> Self {
            Self::User(User::new(from, message))
        }
        fn into_user(self) -> Result<User<MailAddr, Never>, Self> {
            match self {
                Self::User(user) => Ok(user),
                other => Err(other),
            }
        }
    }
    impl ComposedEvent for CapabilityEvent {
        type Inner = User<MailAddr, Never>;
        fn from_inner(event: Self::Inner) -> Self {
            Self::User(event)
        }
    }
    impl InjectEvent<WorkerActivation<ActivationWorker, ActivationWork>, Here> for CapabilityEvent {
        fn inject_at(event: WorkerActivation<ActivationWorker, ActivationWork>) -> Self {
            Self::Activation(event)
        }
    }
    impl
        InjectEvent<
            WorkerPreparation<
                PreparationSource,
                WorkerRole,
                ReplacementWorker,
                ImmediateActivation,
            >,
            Inside<Here>,
        > for CapabilityEvent
    {
        fn inject_at(
            event: WorkerPreparation<
                PreparationSource,
                WorkerRole,
                ReplacementWorker,
                ImmediateActivation,
            >,
        ) -> Self {
            Self::Preparation(event)
        }
    }
    impl
        EventIngress<
            BeginActivation<ActivationWorker, ActivationWork>,
            ActionItemResult<BeginActivation<ActivationWorker, ActivationWork>>,
        > for CapabilityEvent
    {
        fn ingress(
            event: ActionItemResult<BeginActivation<ActivationWorker, ActivationWork>>,
        ) -> Self {
            Self::ActivationSource(event)
        }
    }
    impl
        EventIngress<
            PrepareWorkers<PreparationSource, WorkerRole, ReplacementWorker, ImmediateActivation>,
            ActionItemResult<
                PrepareWorkers<
                    PreparationSource,
                    WorkerRole,
                    ReplacementWorker,
                    ImmediateActivation,
                >,
            >,
        > for CapabilityEvent
    {
        fn ingress(
            event: ActionItemResult<
                PrepareWorkers<
                    PreparationSource,
                    WorkerRole,
                    ReplacementWorker,
                    ImmediateActivation,
                >,
            >,
        ) -> Self {
            Self::PreparationSource(event)
        }
    }
    impl InjectEvent<ShutdownRequested, Here> for CapabilityEvent {
        fn inject_at(event: ShutdownRequested) -> Self {
            Self::Shutdown(event)
        }
    }

    struct CapabilityParent {
        original: Arc<Vec<u8>>,
        child: Option<CapabilityChild>,
        creation: CreationId,
        activation: Option<BeginActivation<ActivationWorker, ActivationWork>>,
        preparation: Option<
            PrepareWorkers<PreparationSource, WorkerRole, ReplacementWorker, ImmediateActivation>,
        >,
        received: Vec<CapabilityEvent>,
    }
    impl BehaviorBase for CapabilityParent {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }
    impl Behavior for CapabilityParent {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = CapabilityEvent;
        type Sends = SendLayer<
            InterpreterRequests<BeginActivation<ActivationWorker, ActivationWork>>,
            SourceActions<
                PrepareWorkers<
                    PreparationSource,
                    WorkerRole,
                    ReplacementWorker,
                    ImmediateActivation,
                >,
            >,
        >;
        type Birth = RetirementBirths<
            <ChildCons<MailAddr, CapabilityChild, NoChildren> as ChildProduct<MailAddr>>::Choice,
        >;
        type Ph = Never;
        type Error = Never;
        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            let child = self.child.take().expect("one genuine original leaf child");
            let activation = self
                .activation
                .take()
                .expect("one actual public activation request");
            let preparation = self
                .preparation
                .take()
                .expect("one actual public grouped request");
            let creates = Children::<MailAddr>::new()
                .child(self.creation, child)
                .into_creates();
            Ok(Actions::new(
                SendLayer::new(
                    InterpreterRequests::one(activation),
                    SourceActions::sending(preparation),
                ),
                creates,
                Step::Continue,
            ))
        }
        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            self.received.push(event);
            // This test-only domain policy stops on its first actual control
            // input. Environment retirement still joins both admitted tasks.
            Ok(Actions::stop())
        }
    }

    struct CapabilityChildTerminal {
        origin: ChildOrigin<CapabilityParent, ChildHead>,
        retirement: ActorRetirement<CapabilityChild, Self, ()>,
    }
    impl
        ProjectTerminal<
            ChildOrigin<CapabilityParent, ChildHead>,
            ActorRetirement<CapabilityChild, Self, ()>,
        > for CapabilityChildTerminal
    {
        fn project(
            origin: ChildOrigin<CapabilityParent, ChildHead>,
            retirement: ActorRetirement<CapabilityChild, Self, ()>,
        ) -> Self {
            // This total conversion moves the exact input; it performs no
            // assertion, callback, lookup or independent task spawning.
            Self { origin, retirement }
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the single actual startup/source arbitration, original task/child joins and every complete typed custody/discharge oracle together so finite omissions fail only after cleanup."
    )]
    async fn actual_parent_retirement_keeps_both_capability_lanes_and_joined_child() {
        let endpoint = Arc::new(vec![107, 109]);
        let endpoint_original = Arc::downgrade(&endpoint);
        let cause = Arc::new(vec![113, 127]);
        let cause_original = Arc::downgrade(&cause);
        let (mut proxy, report, activation_creation) =
            initialize_activation(endpoint, ActivationWork::Panicked(cause));
        let actions = proxy
            .on(report)
            .unwrap_or_else(|_| panic!("genuine proxy activation issuance"));
        activation_declaration::assert_proxy_actions(&actions, 0, &[], 0, 1, 0);
        let mut requests = actions.sends.worker_activations.into_requests();
        let activation = requests.pop().expect("one genuine BeginActivation");
        assert_eq!(requests.len(), 0);
        let expected_worker = activation.worker();
        assert_eq!(expected_worker.creation(), activation_creation);
        let material = Arc::new(vec![40, 41]);
        let worker = Arc::new(vec![50, 51]);
        let first = Arc::new(vec![10]);
        let current = Arc::new(vec![20]);
        let tail = Arc::new(vec![30]);
        let originals = [
            Arc::downgrade(&material),
            Arc::downgrade(&worker),
            Arc::downgrade(&first),
            Arc::downgrade(&current),
            Arc::downgrade(&tail),
        ];
        let roles = OrderedRoles::new(WorkerRole(first), [WorkerRole(current), WorkerRole(tail)])
            .expect("three distinct original roles");
        let preparation = admitted_group(
            PreparationSource {
                material,
                first_worker: Some(worker),
                requested_roles: Vec::new(),
            },
            roles,
        )
        .await;
        let original_parent = Arc::new(vec![43, 47]);
        let parent_original = Arc::downgrade(&original_parent);
        let original_child = Arc::new(vec![31, 37]);
        let child_original = Arc::downgrade(&original_child);
        let mut creations = CreationSequence::new();
        let creation = creations.issue().expect("actual parent namespace");
        let roots = ActorSpace::<MessageProtocol<MailAddr, Never>>::new();
        let actor_spaces = Arc::new(HostedActorSpaces(roots.clone()));
        let allocations = ApplicationAddresses::new();
        let activation_lane = Arc::new(Mutex::new(Vec::new()));
        let preparation_lane = Arc::new(Mutex::new(Vec::new()));
        let receiving_activation = activation_lane.clone();
        let receiving_preparation = preparation_lane.clone();
        let parent = CapabilityParent {
            original: original_parent,
            child: Some(CapabilityChild {
                original: original_child,
            }),
            creation,
            activation: Some(activation),
            preparation: Some(preparation),
            received: Vec::new(),
        };
        let (authority, startup, (), task) =
            spawn_local_execution::<CapabilityParent, _, StandardIngress, _, _, _>(
                roots.clone(),
                Config::new(2),
                MailAddr::APPLICATION_ROOT,
                parent,
                move |control, terminal_reports, timers, observations| {
                    let capabilities = ApplicationCapabilities::<
                        CapabilityParent,
                        HostedActorSpaces<ActorSpace<MessageProtocol<MailAddr, Never>>>,
                        NoParent,
                        ChildBindings<
                            CapabilityParent,
                            CapabilityChildTerminal,
                            StructuralOrigins<CapabilityParent>,
                        >,
                        StructuralOrigins<CapabilityParent>,
                    >::new_with_bindings(
                        ApplicationCapabilityInputs {
                            address: MailAddr::APPLICATION_ROOT,
                            actor_spaces,
                            allocations,
                            control,
                            timers,
                            observations,
                            terminal_reports,
                        },
                        ChildBindings::<
                            CapabilityParent,
                            CapabilityChildTerminal,
                            StructuralOrigins<CapabilityParent>,
                        >::default(),
                    );
                    ActionInterpreter::new((
                        Some(capabilities),
                        receiving_activation,
                        receiving_preparation,
                        None,
                    ))
                },
                |environment| {
                    let (publication, startup) = oneshot::channel();
                    let environment = environment.publish_with(move |actor| {
                        let admitted = publication.send(actor);
                        match admitted {
                            Ok(()) => {}
                            Err(actor) => drop(actor),
                        }
                    });
                    (environment, startup, ())
                },
            );
        let original_root_task = task.id();
        // The initial source admission is transitive Driver work before publication.
        // Both background tasks remain owned by the original retirement path.
        let publication = startup.await;
        let joined = task.await;
        drop(authority);
        let outcome = match joined {
            Ok(outcome) => outcome,
            Err(error) => panic!("unexpected raw root fault {error:?}"),
        };
        let (retirement, acquired) = ActorRetirement::from_capability_local(outcome);
        let Some((returned_activation, returned_preparation)) = acquired else {
            panic!("normal joined residual owns its actual lane product")
        };
        assert!(Arc::ptr_eq(&activation_lane, &returned_activation));
        assert!(Arc::ptr_eq(&preparation_lane, &returned_preparation));
        drop((returned_activation, returned_preparation));
        let (
            mut behavior,
            interpretation,
            source,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            terminal_report,
            retirement_failures,
            additional_failures,
            settlements,
            control,
            user,
            mut descendants,
            child_failures,
            mut capability_failures,
            unread,
            disposition,
        ) = match retirement {
            ActorRetirement::Completed {
                behavior,
                interpretation,
                source,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                terminal_report,
                retirement_failures,
                additional_failures,
                settlements,
                control,
                user,
                descendants,
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                completion,
            } => (
                behavior,
                interpretation,
                source,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                terminal_report,
                retirement_failures,
                additional_failures,
                settlements,
                control,
                user,
                descendants,
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                Ok(completion),
            ),
            ActorRetirement::CapabilityFailed {
                behavior,
                interpretation,
                source,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                terminal_report,
                retirement_failures,
                additional_failures,
                settlements,
                control,
                user,
                descendants,
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                error,
            } => (
                behavior,
                interpretation,
                source,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                terminal_report,
                retirement_failures,
                additional_failures,
                settlements,
                control,
                user,
                descendants,
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                Err(error),
            ),
            other => {
                drop(other);
                panic!(
                    "actual source arbitration returns Stop or the original capability task failure"
                )
            }
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(additional_failures.is_empty());
        // The source port deliberately arbitrates control and task completion
        // without priority. Preserve its actual primary cause, never predict it.
        match disposition {
            Ok(completion) => {
                assert_eq!(completion, Completion::Stopped);
                assert_eq!(behavior.received.len(), 1);
                assert_eq!(control.len(), 1);
                assert_eq!(settlements.len(), 2);
                assert_eq!(capability_failures.len(), 2);
            }
            Err(failure) => {
                assert!(failure.is_panic());
                assert_eq!(behavior.received.len(), 0);
                assert_eq!(control.len(), 2);
                assert_eq!(settlements.len(), 1);
                assert_eq!(capability_failures.len(), 1);
                // This is the exact separately acquired primary JoinError;
                // append it to the outside receiving collection once.
                capability_failures.push(failure);
            }
        }
        assert_eq!(behavior.original.as_slice(), [43, 47]);
        assert!(behavior.child.is_none());
        assert!(behavior.activation.is_none());
        assert!(behavior.preparation.is_none());
        assert_eq!(behavior.creation, creation);
        assert!(publication.is_err());
        assert_eq!(user.len(), 0);
        assert!(unread.is_none());
        let mut observed_inputs = mem::take(&mut behavior.received).into_iter().chain(control);
        let activation_input = observed_inputs
            .next()
            .expect("one original synchronous Started input");
        match activation_input {
            CapabilityEvent::Activation(report) => {
                assert_eq!(report.worker(), expected_worker);
                let started = proxy.on(report).unwrap_or_else(|_| {
                    panic!("original proxy admits the exact actually received grant")
                });
                activation_declaration::assert_proxy_actions(&started, 0, &[], 0, 0, 0);
            }
            CapabilityEvent::User(user) => match user.message {},
            CapabilityEvent::Preparation(report) => {
                drop(report);
                panic!("unexpected completed preparation before the original Started")
            }
            CapabilityEvent::ActivationSource(receipt) => {
                drop(receipt);
                panic!("unexpected activation source receipt before the original Started")
            }
            CapabilityEvent::PreparationSource(_) | CapabilityEvent::Shutdown(_) => {
                panic!("the actual synchronous Started precedes source admission")
            }
        }
        let preparation_start = match observed_inputs
            .next()
            .expect("the actual preparation start returns to its source")
        {
            CapabilityEvent::PreparationSource(SettledItem::Attempted(
                ItemSettlement::Accepted(started),
            )) => started,
            CapabilityEvent::User(user) => match user.message {},
            CapabilityEvent::Preparation(report) => {
                drop(report);
                panic!("unexpected completed preparation instead of its original start receipt")
            }
            CapabilityEvent::ActivationSource(receipt) => {
                drop(receipt);
                panic!(
                    "unexpected activation source receipt instead of the original preparation start"
                )
            }
            CapabilityEvent::Activation(_)
            | CapabilityEvent::PreparationSource(_)
            | CapabilityEvent::Shutdown(_) => {
                panic!("the whole original accepted preparation start remains unread")
            }
        };
        let extra_input = observed_inputs.next();
        assert!(extra_input.is_none());
        let (failures, ()) = child_failures;
        assert_eq!(failures.len(), 0);
        assert_eq!(descendants.len(), 1);
        let child = descendants
            .pop()
            .expect("whole genuine child result after parent join");
        let ActorRetirement::Completed {
            interpretation: child_retirement_interpretation,
            source: child_retirement_source,
            received_interpretation: child_retirement_received_interpretation,
            received_source: child_retirement_received_source,
            source_index: child_retirement_source_index,
            acquired_ingress: child_retirement_acquired_ingress,
            terminal_report: child_retirement_terminal_report,
            retirement_failures: child_retirement_native_failures,
            additional_failures: child_retirement_additional_failures,
            behavior: child_behavior,
            settlements: child_settlements,
            control: child_control,
            user: child_user,
            descendants: child_descendants,
            child_failures: (),
            capability_failures: child_capabilities,
            unread_owner_cancellation: child_unread,
            completion: child_completion,
        } = child.retirement
        else {
            panic!("pure stop leaf has its original completed result")
        };
        assert!(child_retirement_interpretation.is_none());
        assert!(child_retirement_source.is_none());
        assert!(child_retirement_received_interpretation.is_none());
        assert!(child_retirement_received_source.is_none());
        assert!(child_retirement_source_index.is_none());
        assert!(child_retirement_acquired_ingress.is_none());
        assert!(child_retirement_terminal_report.is_none());
        assert!(child_retirement_native_failures.is_empty());
        assert!(child_retirement_additional_failures.is_empty());

        assert_eq!(child_behavior.original.as_slice(), [31, 37]);
        assert_eq!(child_completion, Completion::Stopped);
        assert_eq!(child_settlements.len(), 1);
        assert_eq!(child_control.len(), 0);
        assert_eq!(child_user.len(), 0);
        assert_eq!(child_descendants.len(), 0);
        assert_eq!(child_capabilities.len(), 0);
        // The standard parent drain issues cancellation to each child before
        // joining. Depending on whether this stopped child had already retired,
        // its exact unread request is absent or is the accepted original unit.
        // Both are explicitly discharged here, never reclassified as its cause.
        match child_unread {
            None | Some(()) => {}
        }
        for settlement in &child_settlements {
            assert_eq!(settlement.creations.len(), 0);
            assert_eq!(settlement.sends, NoSends);
            assert!(matches!(settlement.become_, Step::Stop(Stopped)));
        }
        // Stop adds one actual front settlement; a primary task failure does
        // not. In either case preserve the whole original initialization row.
        let mut rows = settlements.into_iter();
        let first = rows
            .next()
            .expect("the whole original initialization remains");
        let initial = match &first.become_ {
            Step::Stop(Stopped) => {
                assert_eq!(first.sends.owned.len(), 0);
                let stopped_preparation = first.sends.inner.into_inputs();
                assert_eq!(stopped_preparation.len(), 0);
                let CreationSettlement::Settled(empty_births) = first.creations.into_settlement()
                else {
                    panic!("actual empty stop creation settlement")
                };
                assert_eq!(empty_births.len(), 0);
                drop((stopped_preparation, empty_births));
                rows.next()
                    .expect("original initialization follows the actual Stop row")
            }
            Step::Continue => first,
            Step::Goto(phase) => match *phase {},
        };
        let extra = rows.next();
        assert!(extra.is_none());
        assert!(matches!(initial.become_, Step::Continue));
        assert_eq!(initial.sends.owned.len(), 1);
        let mut initial_activation = initial.sends.owned.into_iter();
        let accepted_activation = initial_activation.next();
        let Some(SettledItem::Attempted(ItemSettlement::Accepted(()))) = accepted_activation else {
            panic!("the original activation unit acceptance remains before its source discharge")
        };
        let extra_activation = initial_activation.next();
        assert!(extra_activation.is_none());
        let initial_preparation = initial.sends.inner.into_inputs();
        assert_eq!(initial_preparation.len(), 0);
        let CreationSettlement::Settled(births) = initial.creations.into_settlement() else {
            panic!("actual committed creation row")
        };
        assert_eq!(births.len(), 1);
        let birth = births
            .into_iter()
            .next()
            .expect("one actual attempted birth");
        let SettledItem::Attempted(ItemSettlement::Accepted(ChildChoice::Head(
            ChildCreationOutcome::Established(committed),
        ))) = birth
        else {
            panic!("exact original committed child")
        };
        assert_eq!(committed.id(), creation);
        assert_eq!(committed.kind(), CreationKind::Birth);
        let mut endpoint_conversion = ExtractLocalEndpoint;
        let child_endpoint = committed
            .actor()
            .recipient()
            .interpret(&mut endpoint_conversion);
        assert_eq!(child_endpoint.address(), child.origin.address());
        assert_eq!(capability_failures.len(), 2);
        assert!(capability_failures.iter().all(JoinError::is_panic));
        let ids: Vec<_> = capability_failures.iter().map(JoinError::id).collect();
        assert_ne!(ids[0], ids[1]);
        assert!(ids.iter().all(|id| *id != original_root_task));
        let activation_cells = activation_lane
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let preparation_cells = preparation_lane
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        assert_eq!(activation_cells.len(), 1);
        assert_eq!(preparation_cells.len(), 1);
        let activation = activation_cells[0].lock().await;
        let activation_task: Id = match &activation.task {
            WorkerCapabilityTask::Started(id) => id.to_owned(),
            WorkerCapabilityTask::Unpolled => {
                panic!("the original activation task actually polled")
            }
        };
        assert!(ids.contains(&activation_task));
        assert_eq!(activation.permit.worker(), expected_worker);
        assert_eq!(
            activation.resumed_failure_origin,
            Some(WorkerCapabilityOperation::Execution)
        );
        assert_eq!(activation.failures.len(), 0);
        assert!(activation.event.is_none());
        let mut observed = ActivationEndpoint(Arc::new(Vec::new()));
        activation.permit.target().interpret(&mut observed);
        assert_eq!(observed.0.as_slice(), [107, 109]);
        assert_eq!(Arc::as_ptr(&observed.0), endpoint_original.as_ptr());
        drop((observed, activation));
        let mut preparation = preparation_cells[0].lock().await;
        let preparation_task: Id = match &preparation.task {
            WorkerCapabilityTask::Started(id) => id.to_owned(),
            WorkerCapabilityTask::Unpolled => {
                panic!("the original preparation task actually polled")
            }
        };
        assert!(ids.contains(&preparation_task));
        assert_ne!(activation_task, preparation_task);
        assert_eq!(
            preparation.resumed_failure_origin,
            Some(WorkerCapabilityOperation::Execution)
        );
        assert_eq!(preparation.failures.len(), 0);
        assert!(preparation.event.is_none());
        let Some(WorkerPreparationPhase::Pending(pending)) = &mut preparation.phase else {
            panic!("whole prepared prefix and current/tail remain after later panic")
        };
        let (source, role) = pending.source_and_role();
        let original_source = originals[0].upgrade().expect("original source live");
        assert_eq!(source.material.as_slice(), [40, 41]);
        assert_eq!(Arc::as_ptr(&source.material), Arc::as_ptr(&original_source));
        drop(original_source);
        assert_eq!(source.requested_roles, vec![vec![10], vec![20]]);
        assert!(source.first_worker.is_none());
        assert_eq!(role.0.as_slice(), [20]);
        assert_eq!(Arc::as_ptr(&role.0), originals[3].as_ptr());
        drop(preparation);
        // Normal and unread control inputs are typed policy observations, not
        // extra copies of a native cause. Their complete values are surrendered
        // together only after the actual parent and child task owners join.
        drop((
            proxy,
            preparation_start,
            observed_inputs,
            initial_preparation,
            initial_activation,
            behavior,
            child_behavior,
            child_settlements,
            child_endpoint,
            committed,
            publication,
        ));
        let retained: Vec<_> = originals.iter().map(Weak::strong_count).collect();
        let endpoint_retained = endpoint_original.strong_count();
        let cause_retained = cause_original.strong_count();
        drop(capability_failures);
        let cause_released = cause_original.strong_count();
        drop((
            activation_cells,
            preparation_cells,
            activation_lane,
            preparation_lane,
        ));
        let released: Vec<_> = originals.iter().map(Weak::strong_count).collect();
        let endpoint_released = endpoint_original.strong_count();
        let parent_released = parent_original.strong_count();
        let child_released = child_original.strong_count();
        assert_eq!(retained, vec![1, 1, 1, 1, 1]);
        assert_eq!(endpoint_retained, 1);
        assert_eq!(cause_retained, 1);
        assert_eq!(cause_released, 0);
        assert_eq!(released, vec![0, 0, 0, 0, 0]);
        assert_eq!(endpoint_released, 0);
        assert_eq!(parent_released, 0);
        assert_eq!(child_released, 0);
    }
}

#[cfg(test)]
mod child_projection_panic {
    use std::any::Any;
    use std::future::Future;
    use std::panic::resume_unwind;
    use std::pin::pin;
    use std::ptr;
    use std::sync::{Arc, Mutex, PoisonError};
    use std::task::{Context, Poll, Waker};

    use behavior::{
        ActionSettlement, Actions, BehaviorActed, BehaviorBase, ChildCreationOutcome, ChildHead,
        CreateChild, CreationId, CreationKind, CreationSequence, CreationSettlement, Creations,
        EventLayer, ItemSettlement, Never, NoSends, SendLayer, SettledItem, Step, Stopped,
    };
    use behavior_actors::{ShutdownRequested, StopOnShutdown};
    use bombay_engine::Completion;
    use communication::Config;
    use tokio::runtime::Builder;
    use tokio::sync::oneshot;
    use tokio::task::{Id, id};

    use super::{
        ApplicationCapabilities, ApplicationCapabilityInputs, NoParent, StructuralOrigins,
    };
    use crate::actor;
    use crate::actor_interface::ExtractLocalEndpoint;
    use crate::actors::ActorExt;
    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::child_bindings::ChildBindings;
    use crate::interpret::ActionInterpreter;
    use crate::launch::{OwnedTask, spawn_root_with};
    use crate::terminal::{ActorRetirement, ChildFailure, ChildOrigin, ProjectTerminal};
    use crate::topology::HostedActorSpaces;
    use crate::{ActorSpace, ActorSpaces};

    type ProjectionCustody = (oneshot::Sender<(Id, ChildTerminal)>, Box<dyn Any + Send>);
    static PROJECTION_CUSTODY: Mutex<Option<ProjectionCustody>> = Mutex::new(None);

    enum ChildDisposition {
        Stop,
        Continue,
    }

    struct ProjectionChild {
        disposition: ChildDisposition,
        entries: Arc<Vec<u64>>,
    }

    #[actor(message = Never)]
    impl ProjectionChild {
        #[allow(
            clippy::unnecessary_wraps,
            reason = "the generated fold retains its exact controlled-error boundary"
        )]
        fn init(&mut self) -> BehaviorActed<Self> {
            match self.disposition {
                ChildDisposition::Stop => Ok(Actions::stop()),
                ChildDisposition::Continue => Ok(Actions::cont()),
            }
        }
    }

    struct ProjectionParent {
        first: CreationId,
        later: CreationId,
        first_entries: Option<Arc<Vec<u64>>>,
        later_entries: Option<Arc<Vec<u64>>>,
    }

    #[actor(
        message = Never,
        births = { child: StopOnShutdown<ProjectionChild> },
        creation_settlements = retain_for_retirement,
    )]
    impl ProjectionParent {
        #[allow(
            clippy::unnecessary_wraps,
            reason = "the generated fold retains its exact controlled-error boundary"
        )]
        fn init(&mut self) -> BehaviorActed<Self> {
            let first = ProjectionChild {
                disposition: ChildDisposition::Stop,
                entries: self
                    .first_entries
                    .take()
                    .expect("one original first child input"),
            };
            let later = ProjectionChild {
                disposition: ChildDisposition::Continue,
                entries: self
                    .later_entries
                    .take()
                    .expect("one original later child input"),
            };
            Ok(Actions::new(
                NoSends,
                Creations::one(CreateChild::birth(self.first, first.stop_on_shutdown()))
                    .and(CreateChild::birth(self.later, later.stop_on_shutdown())),
                Step::Continue,
            ))
        }
    }

    #[derive(ActorSpaces)]
    struct ProjectionSpaces {
        parents: ActorSpace<ProjectionParent>,
    }

    struct ChildTerminal {
        origin: ChildOrigin<ProjectionParent, ChildHead>,
        retirement: ActorRetirement<StopOnShutdown<ProjectionChild>, Self, ()>,
    }

    impl
        ProjectTerminal<
            ChildOrigin<ProjectionParent, ChildHead>,
            ActorRetirement<StopOnShutdown<ProjectionChild>, Self, ()>,
        > for ChildTerminal
    {
        fn project(
            origin: ChildOrigin<ProjectionParent, ChildHead>,
            retirement: ActorRetirement<StopOnShutdown<ProjectionChild>, Self, ()>,
        ) -> Self {
            match retirement {
                retirement @ ActorRetirement::Completed { .. } => {
                    let (publication, payload) = PROJECTION_CUSTODY
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .take()
                        .expect("the actual eager projector owns its publication and native cause");
                    let transferred = publication.send((id(), Self { origin, retirement }));
                    match transferred {
                        Ok(()) => {}
                        Err(original) => {
                            drop(original);
                            panic!("the complete original child report receiver remains live");
                        }
                    }
                    // Runtime projection is outside every Behavior fold. The
                    // original opaque Box becomes this actual task's native cause.
                    resume_unwind(payload)
                }
                retirement => Self { origin, retirement },
            }
        }
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one finite native projector controller joins owners before complete custody oracles"
    )]
    #[expect(
        clippy::default_trait_access,
        reason = "the concrete constructor infers the binding type; the non-injective ChildBindings alias cannot name its Default implementation"
    )]
    fn actual_projection_panic_keeps_origin_native_cause_and_later_sibling() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("one actual local actor executor");
        let first_entries = Arc::new(vec![31, 37, 41]);
        let later_entries = Arc::new(vec![43, 47, 53]);
        let original_cause = Arc::new(vec![59_u64, 61, 67]);
        let first_allocation = first_entries.as_ptr();
        let later_allocation = later_entries.as_ptr();
        let cause_allocation = original_cause.as_ptr();
        let first_owner = Arc::downgrade(&first_entries);
        let later_owner = Arc::downgrade(&later_entries);
        let cause_owner = Arc::downgrade(&original_cause);
        let mut creations = CreationSequence::new();
        let first = creations
            .issue()
            .expect("one actual first creation identity");
        let later = creations
            .issue()
            .expect("one actual later creation identity");
        let (publication, projected) = oneshot::channel();
        let original_payload: Box<dyn Any + Send> = Box::new(original_cause);
        let payload_allocation = ptr::from_ref(original_payload.as_ref()).cast::<()>();
        let prior_custody = PROJECTION_CUSTODY
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace((publication, original_payload));
        drop(prior_custody);
        let spaces = ProjectionSpaces {
            parents: ActorSpace::new(),
        };
        let roots = spaces.parents.clone();
        let parent = ProjectionParent {
            first,
            later,
            first_entries: Some(first_entries),
            later_entries: Some(later_entries),
        }
        .stop_on_shutdown();
        let (reported, parent_poll, shutdown, received, remaining_owner) =
            runtime.block_on(async move {
                let actor_spaces = Arc::new(HostedActorSpaces(spaces));
                let root = spawn_root_with(
                    roots.clone(),
                    Config::new(2),
                    MailAddr::APPLICATION_ROOT,
                    parent,
                    move |control, terminal_reports, timers, observations| {
                        ActionInterpreter::new(ApplicationCapabilities::<
                            StopOnShutdown<ProjectionParent>,
                            HostedActorSpaces<ProjectionSpaces>,
                            NoParent,
                            ChildBindings<
                                StopOnShutdown<ProjectionParent>,
                                ChildTerminal,
                                StructuralOrigins<ProjectionParent>,
                            >,
                            StructuralOrigins<ProjectionParent>,
                        >::new_with_bindings(
                            ApplicationCapabilityInputs {
                                address: MailAddr::APPLICATION_ROOT,
                                actor_spaces,
                                allocations: ApplicationAddresses::new(),
                                control,
                                timers,
                                observations,
                                terminal_reports,
                            },
                            Default::default(),
                        ))
                    },
                )
                .await
                .unwrap_or_else(|_| panic!("the actual parent publishes both committed births"));
                let mut owned = Some(root.task);
                let mut received = None;
                let reported = projected.await;
                // The eager synchronous callback transfers then unwinds in the same
                // task poll. No await separates publication from its native panic.
                let parent_poll = {
                    let mut waiting = pin!(OwnedTask::receive_finish(&mut owned, &mut received));
                    waiting
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                };
                let shutdown = match root.shutdown_control.upgrade() {
                    Some(control) => control.send(EventLayer::Owned(ShutdownRequested)),
                    None => panic!("the parent remains live until its explicit shutdown"),
                };
                OwnedTask::receive_finish(&mut owned, &mut received).await;
                drop(root.actor);
                (reported, parent_poll, shutdown, received, owned)
            });
        drop(runtime);
        let stale_custody = PROJECTION_CUSTODY
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        drop(stale_custody);
        // Native root join and executor cleanup precede every custody oracle.
        assert!(remaining_owner.is_none());
        assert!(matches!(parent_poll, Poll::Pending));
        assert!(shutdown.is_ok());
        let (projection_task, first_terminal) =
            reported.expect("the actual first child transferred its complete retirement");
        let outcome = received
            .expect("the actual root join was acquired")
            .expect("the projector panic must not substitute for the parent task result");
        let retirement = ActorRetirement::from_local(outcome);
        let ActorRetirement::Completed {
            behavior,
            interpretation,
            source,
            settlements,
            control,
            user,
            descendants,
            child_failures: (mut failures, ()),
            capability_failures,
            additional_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
            terminal_report,
            unread_owner_cancellation,
            completion,
        } = retirement
        else {
            panic!("explicit shutdown completes the original live parent");
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert_eq!(control.len(), 0);
        assert_eq!(user.len(), 0);
        assert!(capability_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(retirement_failures.is_empty());
        assert!(terminal_report.is_none());
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(completion, Completion::Stopped);
        let parent = behavior.into_inner();
        assert_eq!(parent.first, first);
        assert_eq!(parent.later, later);
        assert!(parent.first_entries.is_none());
        assert!(parent.later_entries.is_none());
        let [stopped, settlement] = settlements.try_into().unwrap_or_else(|_| {
            panic!("the parent retains its ordered shutdown Stop and initialization Continue settlements");
        });
        assert_eq!(stopped.sends.owned, NoSends);
        assert_eq!(stopped.sends.inner, NoSends);
        assert_eq!(stopped.become_, Step::Stop(Stopped));
        let CreationSettlement::Settled(stopped_creations) = stopped.creations.into_settlement()
        else {
            panic!("the actual shutdown Stop retains its complete empty creation lane");
        };
        assert!(stopped_creations.is_empty());
        assert_eq!(settlement.sends.owned, NoSends);
        assert_eq!(settlement.sends.inner, NoSends);
        assert_eq!(settlement.become_, Step::Continue);
        let CreationSettlement::Settled(reports) = settlement.creations.into_settlement() else {
            panic!("both original births retain their complete routed settlement lane");
        };
        let reports: Vec<_> = reports.into_iter().collect();
        let [first_report, later_report] = reports.try_into().unwrap_or_else(|_| {
            panic!("the actual initialized product owns both committed creations");
        });
        let mut committed_addresses = Vec::new();
        for (report, expected) in [(first_report, first), (later_report, later)] {
            let SettledItem::Attempted(ItemSettlement::Accepted(
                ChildCreationOutcome::Established(report),
            )) = report
            else {
                panic!("both children committed before parent shutdown");
            };
            assert_eq!(report.id(), expected);
            assert_eq!(report.kind(), CreationKind::Birth);
            let endpoint = report
                .actor()
                .into_recipient()
                .interpret(&mut ExtractLocalEndpoint);
            committed_addresses.push(endpoint.address());
        }
        assert_eq!(failures.len(), 1);
        let failure = failures.remove(0);
        let ChildFailure::ProjectionTaskFailed {
            id,
            kind,
            origin,
            actor,
            error,
        } = failure
        else {
            panic!("the actual native cause belongs to the projection task");
        };
        assert_eq!(id, first);
        assert_eq!(kind, CreationKind::Birth);
        assert_eq!(origin, first_terminal.origin);
        assert_eq!(origin.address(), committed_addresses[0]);
        let endpoint = actor.into_recipient().interpret(&mut ExtractLocalEndpoint);
        assert_eq!(endpoint.address(), origin.address());
        assert_eq!(error.id(), projection_task);
        assert!(error.is_panic());
        assert!(!error.is_cancelled());
        let payload = error.into_panic();
        assert_eq!(
            ptr::from_ref(payload.as_ref()).cast::<()>(),
            payload_allocation
        );
        let original_cause = payload
            .downcast::<Arc<Vec<u64>>>()
            .expect("the original opaque native panic payload retains its exact type");
        assert_eq!(original_cause.as_ptr(), cause_allocation);
        assert_eq!(original_cause.as_slice(), [59, 61, 67]);
        let [later_terminal] = descendants.as_slice() else {
            panic!("the later sibling is joined despite the earlier native projector failure");
        };
        assert_eq!(later_terminal.origin.address(), committed_addresses[1]);
        assert_ne!(
            first_terminal.origin.address(),
            later_terminal.origin.address()
        );
        assert_ne!(first_terminal.origin.nonce(), later_terminal.origin.nonce());
        let mut terminals = vec![first_terminal];
        terminals.extend(descendants);
        let stopped_child_settlements = vec![ActionSettlement {
            creations: Creations::empty(),
            sends: SendLayer::new(NoSends, NoSends),
            become_: Step::Stop(Stopped),
        }];
        for (terminal, allocation, entries) in [
            (&terminals[0], first_allocation, &[31, 37, 41][..]),
            (&terminals[1], later_allocation, &[43, 47, 53][..]),
        ] {
            let (
                behavior,
                interpretation,
                source,
                control,
                user,
                descendants,
                capability_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
                unread_owner_cancellation,
            ) = match &terminal.retirement {
                ActorRetirement::Completed {
                    behavior,
                    interpretation,
                    source,
                    settlements,
                    control,
                    user,
                    descendants,
                    child_failures: (),
                    capability_failures,
                    additional_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                    terminal_report,
                    unread_owner_cancellation,
                    completion,
                } => {
                    assert_eq!(*completion, Completion::Stopped);
                    assert_eq!(settlements, &stopped_child_settlements);
                    assert_eq!(terminal.origin, origin);
                    (
                        behavior,
                        interpretation,
                        source,
                        control,
                        user,
                        descendants,
                        capability_failures,
                        additional_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        retirement_failures,
                        terminal_report,
                        unread_owner_cancellation,
                    )
                }
                ActorRetirement::OwnerCancelled {
                    behavior,
                    interpretation,
                    source,
                    settlements,
                    control,
                    user,
                    descendants,
                    child_failures: (),
                    capability_failures,
                    additional_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                    terminal_report,
                    unread_owner_cancellation,
                } => {
                    assert_eq!(settlements.len(), 0);
                    assert_ne!(terminal.origin, origin);
                    (
                        behavior,
                        interpretation,
                        source,
                        control,
                        user,
                        descendants,
                        capability_failures,
                        additional_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        retirement_failures,
                        terminal_report,
                        unread_owner_cancellation,
                    )
                }
                _ => panic!("the first child stopped and the later sibling was owner-cancelled"),
            };
            match &behavior.base().disposition {
                ChildDisposition::Stop => assert_eq!(terminal.origin, origin),
                ChildDisposition::Continue => assert_ne!(terminal.origin, origin),
            }
            assert_eq!(behavior.base().entries.as_ptr(), allocation);
            assert_eq!(behavior.base().entries.as_slice(), entries);
            assert!(interpretation.is_none());
            assert!(source.is_none());
            assert_eq!(control.len(), 0);
            assert_eq!(user.len(), 0);
            assert!(descendants.is_empty());
            assert!(capability_failures.is_empty());
            assert!(additional_failures.is_empty());
            assert!(received_interpretation.is_none());
            assert!(received_source.is_none());
            assert!(source_index.is_none());
            assert!(acquired_ingress.is_none());
            assert!(retirement_failures.is_empty());
            assert!(terminal_report.is_none());
            assert!(unread_owner_cancellation.is_none());
        }
        assert_eq!(first_owner.strong_count(), 1);
        assert_eq!(later_owner.strong_count(), 1);
        assert_eq!(cause_owner.strong_count(), 1);
        drop((terminals, original_cause));
        assert_eq!(first_owner.strong_count(), 0);
        assert_eq!(later_owner.strong_count(), 0);
        assert_eq!(cause_owner.strong_count(), 0);
    }
}
