//! Nominal native Entity definitions, stable references, and family products.

use core::fmt;
use core::future::Future;
use core::hash::Hash;
use core::num::NonZeroUsize;
use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use behavior::{
    ActionItem, Behavior, BehaviorBase, BehaviorMessage, BehaviorSettlements, BirthMode,
    ClassifySettlement, Here, InjectEvent, Inside, InterpretationProgress, InterpreterRequest,
    ItemSettlement, LogicalHostRequirements, Never, NoBirthProtocols, NoReturnToEmitter, Protocol,
    finish_item, prepare_item,
};
use behavior_actors::ShutdownRequested;
use tokio::runtime::Handle;

use crate::ActorRetirement;
use crate::address::{ApplicationAddresses, MailAddr};
use crate::local::ActorRef;
use crate::topology::Hosts as LocalHosts;

use super::bombay::{
    BombayEntityRuntime, NativeEntityHost, NativeEntityLease, bombay_entity_runtime,
};
use super::runtime::EntityReceptionist;
use super::{
    ActivationId, AdmissionFailure, DirectoryConfig, DrainFailure, EntityId, EntityRuntime,
    EntityShutdown, Passivation,
};

/// One nominal, asynchronously hydrated native Entity family.
pub trait EntityDefinition: Send + Sync + 'static {
    /// Stable domain identity stored by one Entity reference.
    type Id: Clone + Eq + Hash + Send + Sync + 'static;
    /// Complete authored actor stack for one live incarnation.
    /// Interpretation and source custody survive in the native task retirement result.
    type Behavior: BehaviorSettlements<
            Protocol: Protocol<Addr = MailAddr, Msg: Send + 'static>,
            Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
            Sends: Send + 'static,
            Error: Send + 'static,
            Birth: BirthMode<Child: Send + 'static>,
            Settlements: ClassifySettlement + Send + 'static,
            InterpretationCustody: Send + 'static,
            SourceCustody: Send + 'static,
            Ph = Never,
        > + BehaviorBase
        + LogicalHostRequirements
        + Send
        + 'static;
    /// Concrete application host product used by this native definition.
    type Hosts: LocalHosts<<Self::Behavior as Behavior>::Protocol> + Send + Sync + 'static;
    /// Exact failure returned while reconstructing domain state.
    type HydrationError: Send + 'static;
    /// Application terminal sum used by children of the incarnation.
    type Terminal: Send + 'static;
    /// Exact associated failure product from this behavior's child binding owner.
    type ChildFailures: Send + 'static;

    /// Reconstruct the authored actor state before it becomes routable.
    fn hydrate(
        &self,
        id: EntityId<Self::Id>,
    ) -> impl Future<Output = Result<Self::Behavior, Self::HydrationError>> + Send;

    /// Consume one exact activation failure.
    fn activation_failed(
        &self,
        id: EntityId<Self::Id>,
        activation: ActivationId,
        failure: EntityActivationError<
            Self::HydrationError,
            Self::Behavior,
            Self::Terminal,
            Self::ChildFailures,
        >,
    );

    /// Consume one exact actor-originated admission refusal.
    fn admission_refused(
        &self,
        id: EntityId<Self::Id>,
        failure: AdmissionFailure<BehaviorMessage<Self::Behavior>>,
    );

    /// Observe the original identity and consume the exact forced-drain reason.
    /// Invoked before shutdown conversion and actor join, with the original lease
    /// held outside the user call. Any key clone performed here is application policy.
    fn forced_retirement(
        &self,
        id: &EntityId<Self::Id>,
        activation: ActivationId,
        failure: DrainFailure,
    );

    /// Consume the original acquired actor result while borrowing its identity.
    /// Normal live retirement invokes this notification before later reactivation.
    /// Values consumed and destroyed inside a panicking callback are unavailable;
    /// outside keys, modes, prior results and original callback panics survive.
    #[expect(
        clippy::type_complexity,
        reason = "the consuming notification retains behavior, terminal, child failures and raw actor join failure"
    )]
    fn retired(
        &self,
        id: &EntityId<Self::Id>,
        activation: ActivationId,
        retirement: Result<
            ActorRetirement<Self::Behavior, Self::Terminal, Self::ChildFailures>,
            tokio::task::JoinError,
        >,
    );
}

/// Available failures after a native actor task has finished and its original
/// result has been handed to the consuming application retirement notification.
/// Payloads are original opaque panics from shutdown conversion and independent user callbacks.
/// A task-unavailable row never claims that descendant cleanup was acquired.
#[derive(thiserror::Error)]
pub enum EntityRetirementFailure {
    /// The raw actor task ended without an acquired full actor retirement.
    #[error("actor retirement unavailable after actor task join")]
    ActorRetirementUnavailable {
        shutdown_request: Option<Box<dyn Any + Send>>,
        forced: Option<Box<dyn Any + Send>>,
        retired: Option<Box<dyn Any + Send>>,
    },
    /// Original consuming shutdown conversion panicked; later failures coexist.
    #[error("application shutdown conversion panicked during native retirement")]
    ShutdownRequestPanicked {
        shutdown_request: Box<dyn Any + Send>,
        forced: Option<Box<dyn Any + Send>>,
        retired: Option<Box<dyn Any + Send>>,
    },
    /// The forced-retirement notification panicked; its final notification still ran.
    #[error("application forced retirement notification panicked")]
    ForcedRetirementPanicked {
        forced: Box<dyn Any + Send>,
        retired: Option<Box<dyn Any + Send>>,
    },
    /// The final consuming retirement notification panicked.
    #[error("application retirement notification panicked")]
    RetirementPanicked { retired: Box<dyn Any + Send> },
}

impl fmt::Debug for EntityRetirementFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ActorRetirementUnavailable { .. } => formatter
                .debug_struct("ActorRetirementUnavailable")
                .finish_non_exhaustive(),
            Self::ShutdownRequestPanicked { .. } => formatter
                .debug_struct("ShutdownRequestPanicked")
                .finish_non_exhaustive(),
            Self::ForcedRetirementPanicked { .. } => formatter
                .debug_struct("ForcedRetirementPanicked")
                .finish_non_exhaustive(),
            Self::RetirementPanicked { .. } => formatter
                .debug_struct("RetirementPanicked")
                .finish_non_exhaustive(),
        }
    }
}

/// Independent native activation bounds for one family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntityCapacity {
    concurrent_hydrations: NonZeroUsize,
    residents: NonZeroUsize,
}

impl EntityCapacity {
    /// Bind independent hydration-work and active-or-activating limits.
    #[must_use]
    pub const fn new(concurrent_hydrations: NonZeroUsize, residents: NonZeroUsize) -> Self {
        Self {
            concurrent_hydrations,
            residents,
        }
    }

    pub(crate) const fn concurrent_hydrations(self) -> NonZeroUsize {
        self.concurrent_hydrations
    }

    pub(crate) const fn residents(self) -> NonZeroUsize {
        self.residents
    }
}

/// Exact phase and fact that prevented native activation.
#[expect(
    clippy::large_enum_variant,
    reason = "launch refusal retains the original unboxed actor retirement; adding a Box would add an allocation and disposal owner to the actual failure"
)]
pub enum EntityActivationError<Hydration, B, Terminal, ChildFailures>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    /// The explicit active-or-activating family bound was full.
    ResidentCapacity,
    /// Domain reconstruction failed before address allocation.
    Hydration(Hydration),
    /// Actor launch failed with exact final state when state existed.
    Launch(ActorRetirement<B, Terminal, ChildFailures>),
}

/// Fixed-cardinality observations for one native family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntityMetrics {
    pub activations: usize,
    pub hydration_failures: usize,
    pub launch_failures: usize,
    pub capacity_refusals: usize,
    pub forced_retirements: usize,
    pub peak_hydrations: usize,
    pub residents: usize,
}

#[derive(Default)]
pub(crate) struct EntityMetricState {
    activations: AtomicUsize,
    hydration_failures: AtomicUsize,
    launch_failures: AtomicUsize,
    capacity_refusals: AtomicUsize,
    forced_retirements: AtomicUsize,
    active_hydrations: AtomicUsize,
    peak_hydrations: AtomicUsize,
    residents: AtomicUsize,
}

impl EntityMetricState {
    pub(crate) fn activation_succeeded(&self) {
        self.activations.fetch_add(1, Ordering::Relaxed);
        self.residents.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn hydration_started(&self) {
        let active = self.active_hydrations.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_hydrations.fetch_max(active, Ordering::Relaxed);
    }

    pub(crate) fn hydration_finished(&self) {
        self.active_hydrations.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn hydration_failed(&self) {
        self.hydration_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn launch_failed(&self) {
        self.launch_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn capacity_refused(&self) {
        self.capacity_refusals.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn forced_retirement(&self) {
        self.forced_retirements.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn retired(&self) {
        self.residents.fetch_sub(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> EntityMetrics {
        EntityMetrics {
            activations: self.activations.load(Ordering::Relaxed),
            hydration_failures: self.hydration_failures.load(Ordering::Relaxed),
            launch_failures: self.launch_failures.load(Ordering::Relaxed),
            capacity_refusals: self.capacity_refusals.load(Ordering::Relaxed),
            forced_retirements: self.forced_retirements.load(Ordering::Relaxed),
            peak_hydrations: self.peak_hydrations.load(Ordering::Relaxed),
            residents: self.residents.load(Ordering::Relaxed),
        }
    }
}

type InstalledRuntimeFor<D> = EntityRuntime<
    <D as EntityDefinition>::Id,
    BehaviorMessage<<D as EntityDefinition>::Behavior>,
    BombayEntityRuntime<D>,
>;
type ReceptionistFor<D> = EntityReceptionist<
    <D as EntityDefinition>::Id,
    BehaviorMessage<<D as EntityDefinition>::Behavior>,
    BombayEntityRuntime<D>,
    MailAddr,
    ActorRef<<<D as EntityDefinition>::Behavior as Behavior>::Protocol>,
    NativeEntityLease<D>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinError,
    EntityRetirementFailure,
>;

/// Cloneable receptionist for one application-installed native family.
pub struct Entities<D>
where
    D: EntityDefinition,
{
    receptionist: ReceptionistFor<D>,
    definition: Arc<D>,
}

impl<D> Clone for Entities<D>
where
    D: EntityDefinition,
{
    fn clone(&self) -> Self {
        Self {
            receptionist: self.receptionist.clone(),
            definition: Arc::clone(&self.definition),
        }
    }
}

impl<D> Entities<D>
where
    D: EntityDefinition,
{
    fn from_runtime(runtime: &InstalledRuntimeFor<D>, definition: Arc<D>) -> Self
    where
        D::Hosts: NativeEntityHost<D::Behavior, D::Terminal, D::ChildFailures>,
    {
        Self {
            receptionist: runtime.receptionist(),
            definition,
        }
    }

    /// Bind one domain identity into a stable family-specific reference.
    #[must_use]
    pub fn entity(&self, id: D::Id) -> EntityRef<D> {
        EntityRef {
            entities: self.clone(),
            id: EntityId::new(id),
        }
    }

    pub(crate) fn passivate(&self, id: &D::Id) -> Passivation
    where
        D::Hosts: NativeEntityHost<D::Behavior, D::Terminal, D::ChildFailures>,
    {
        self.receptionist.passivate(&EntityId::new(id.clone()))
    }
}

/// Stable identity reference to one native Entity definition.
pub struct EntityRef<D>
where
    D: EntityDefinition,
{
    entities: Entities<D>,
    id: EntityId<D::Id>,
}

impl<D> Clone for EntityRef<D>
where
    D: EntityDefinition,
{
    fn clone(&self) -> Self {
        Self {
            entities: self.entities.clone(),
            id: self.id.clone(),
        }
    }
}

impl<D> EntityRef<D>
where
    D: EntityDefinition,
{
    /// Borrow the stable domain identity.
    #[must_use]
    pub const fn id(&self) -> &D::Id {
        self.id.get()
    }

    pub(crate) async fn admit_from(
        &self,
        origin: MailAddr,
        command: BehaviorMessage<D::Behavior>,
    ) -> Result<(), AdmissionFailure<BehaviorMessage<D::Behavior>>>
    where
        D::Hosts: NativeEntityHost<D::Behavior, D::Terminal, D::ChildFailures>,
    {
        self.entities
            .receptionist
            .admit(origin, self.id.clone(), command)
            .await
    }

    /// Form one typed request for the emitting actor's interpreter.
    #[must_use]
    pub fn request(&self, command: BehaviorMessage<D::Behavior>) -> EntityAdmission<D> {
        EntityAdmission {
            entity: self.clone(),
            command,
        }
    }
}

/// One actor-originated command admitted by the emitting actor's interpreter.
pub struct EntityAdmission<D>
where
    D: EntityDefinition,
{
    pub(crate) entity: EntityRef<D>,
    pub(crate) command: BehaviorMessage<D::Behavior>,
}

impl<D> InterpreterRequest for EntityAdmission<D>
where
    D: EntityDefinition,
{
    type ReturnToEmitter = NoReturnToEmitter;
    type LogicalProtocols = NoBirthProtocols;
}

impl<D> ActionItem for EntityAdmission<D>
where
    D: EntityDefinition,
{
    type Accepted = ();
    type Rejection = Never;
    type Prerequisite = Never;
    type Custody = (Option<Self>, Option<Self::Reply>);
    type Input<'a>
        = &'a mut Option<Self>
    where
        Self: 'a;
    type Reply = ItemSettlement<Self, (), Never, Never>;

    fn prepare_interpretation(
        progress: &mut Option<InterpretationProgress<Self, Self::Custody, Self::Reply>>,
    ) {
        prepare_item::<Self>(progress);
    }

    fn interpretation_input<'a>(
        custody: &'a mut Self::Custody,
    ) -> Option<(Self::Input<'a>, &'a mut Option<Self::Reply>)>
    where
        Self: 'a,
    {
        let (input, received) = custody;
        if input.is_some() && received.is_none() {
            Some((input, received))
        } else {
            None
        }
    }

    fn finish_interpretation(
        progress: &mut Option<InterpretationProgress<Self, Self::Custody, Self::Reply>>,
    ) {
        finish_item::<Self>(progress);
    }
}

#[expect(
    private_bounds,
    reason = "native admission is constructed only by Bombay's private application host proof"
)]
impl<D> EntityAdmission<D>
where
    D: EntityDefinition,
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal, D::ChildFailures>,
{
    pub(crate) async fn interpret(input: &mut Option<Self>, origin: MailAddr) {
        let Some(admission) = input.as_ref() else {
            return;
        };
        let notification_id = admission.entity.id.clone();
        let admission_id = admission.entity.id.clone();
        let Some(Self { entity, command }) = input.take() else {
            return;
        };
        if let Err(failure) = entity
            .entities
            .receptionist
            .admit(origin, admission_id, command)
            .await
        {
            entity
                .entities
                .definition
                .admission_refused(notification_id, failure);
        }
    }
}

mod application_families_sealed {
    pub trait Sealed {}
}

/// Static application family product and its live/shutdown projections.
pub trait EntityApplicationFamilies<Hosts>: application_families_sealed::Sealed {
    type Receptionists: Clone + Send + 'static;
    /// Original role-indexed shutdown facts, metrics and native installed-family disposal failure.
    type Shutdowns: Send + 'static;
}

impl application_families_sealed::Sealed for () {}

impl<Hosts> EntityApplicationFamilies<Hosts> for ()
where
    Hosts: Send + Sync + 'static,
{
    type Receptionists = ();
    type Shutdowns = ();
}

impl<Role, D, Tail> application_families_sealed::Sealed
    for (Role, D, DirectoryConfig, EntityCapacity, Tail)
where
    D: EntityDefinition,
{
}

impl<Hosts, Role, D, Tail> EntityApplicationFamilies<Hosts>
    for (Role, D, DirectoryConfig, EntityCapacity, Tail)
where
    Hosts: Send + Sync + 'static,
    Role: Clone + Send + 'static,
    D: EntityDefinition<Hosts = Hosts>,
    Hosts: NativeEntityHost<D::Behavior, D::Terminal, D::ChildFailures>,
    Tail: EntityApplicationFamilies<Hosts>,
{
    type Receptionists = (Role, Entities<D>, Tail::Receptionists);
    type Shutdowns = (
        Role,
        (
            EntityShutdown<tokio::task::JoinError, EntityRetirementFailure, D::Id>,
            EntityMetrics,
            Option<Box<dyn Any + Send>>,
        ),
        Tail::Shutdowns,
    );
}

mod family_at_sealed {
    pub trait Sealed<Role, Position> {}
}

/// Compile-time selection of one application-declared Entity family role.
pub trait EntityFamilyAt<Role, Position>: family_at_sealed::Sealed<Role, Position> {
    type Definition: EntityDefinition;

    #[doc(hidden)]
    fn select_family(&self) -> Entities<Self::Definition>;
}

impl<Role, D, Tail> family_at_sealed::Sealed<Role, Here> for (Role, Entities<D>, Tail) where
    D: EntityDefinition
{
}

impl<Role, D, Tail> EntityFamilyAt<Role, Here> for (Role, Entities<D>, Tail)
where
    D: EntityDefinition,
{
    type Definition = D;

    fn select_family(&self) -> Entities<Self::Definition> {
        self.1.clone()
    }
}

impl<Role, HeadRole, HeadDefinition, Tail, Position>
    family_at_sealed::Sealed<Role, Inside<Position>> for (HeadRole, Entities<HeadDefinition>, Tail)
where
    HeadDefinition: EntityDefinition,
    Tail: family_at_sealed::Sealed<Role, Position>,
{
}

impl<Role, HeadRole, HeadDefinition, Tail, Position> EntityFamilyAt<Role, Inside<Position>>
    for (HeadRole, Entities<HeadDefinition>, Tail)
where
    HeadDefinition: EntityDefinition,
    Tail: EntityFamilyAt<Role, Position>,
{
    type Definition = Tail::Definition;

    fn select_family(&self) -> Entities<Self::Definition> {
        self.2.select_family()
    }
}

pub(crate) struct InstalledEntityFamily<D>
where
    D: EntityDefinition,
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal, D::ChildFailures>,
{
    runtime: InstalledRuntimeFor<D>,
    entities: Entities<D>,
    metrics: Arc<EntityMetricState>,
}

impl<D> InstalledEntityFamily<D>
where
    D: EntityDefinition,
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal, D::ChildFailures>,
{
    async fn shutdown(
        &self,
    ) -> (
        EntityShutdown<tokio::task::JoinError, EntityRetirementFailure, D::Id>,
        EntityMetrics,
    ) {
        let directory = self.runtime.shutdown().await;
        (directory, self.metrics.snapshot())
    }
}

pub(crate) trait InstallEntityFamilies<Hosts>: EntityApplicationFamilies<Hosts> {
    type Installed: InstalledEntityFamilies<Receptionists = Self::Receptionists, Shutdowns = Self::Shutdowns>
        + Send;

    fn install(
        self,
        hosts: Arc<Hosts>,
        allocations: ApplicationAddresses,
        executor: Handle,
    ) -> Self::Installed;
}

impl<Hosts> InstallEntityFamilies<Hosts> for ()
where
    Hosts: Send + Sync + 'static,
{
    type Installed = ();

    fn install(self, _: Arc<Hosts>, _: ApplicationAddresses, _: Handle) -> Self::Installed {}
}

impl<Hosts, Role, D, Tail> InstallEntityFamilies<Hosts>
    for (Role, D, DirectoryConfig, EntityCapacity, Tail)
where
    Hosts: Send + Sync + 'static,
    Role: Clone + Send + 'static,
    D: EntityDefinition<Hosts = Hosts>,
    Hosts: NativeEntityHost<D::Behavior, D::Terminal, D::ChildFailures>,
    Tail: InstallEntityFamilies<Hosts>,
{
    type Installed = (Role, InstalledEntityFamily<D>, Tail::Installed);

    fn install(
        self,
        hosts: Arc<Hosts>,
        allocations: ApplicationAddresses,
        executor: Handle,
    ) -> Self::Installed {
        let (role, definition, directory, capacity, tail) = self;
        let definition = Arc::new(definition);
        let metrics = Arc::new(EntityMetricState::default());
        let runtime = bombay_entity_runtime(
            Arc::clone(&definition),
            Arc::clone(&hosts),
            allocations.clone(),
            capacity,
            Arc::clone(&metrics),
            executor.clone(),
        );
        let Ok(runtime) = EntityRuntime::new(directory, runtime) else {
            unreachable!("EntityCapacity retains a validated directory configuration")
        };
        let entities = Entities::from_runtime(&runtime, definition);
        let family = InstalledEntityFamily {
            runtime,
            entities,
            metrics,
        };
        let tail = tail.install(hosts, allocations, executor);
        (role, family, tail)
    }
}

pub(crate) trait InstalledEntityFamilies {
    type Receptionists: Clone + Send + 'static;
    type Shutdowns: Send + 'static;

    fn receptionists(&self) -> Self::Receptionists;

    fn shutdown(self) -> impl Future<Output = Self::Shutdowns> + Send;
}

impl InstalledEntityFamilies for () {
    type Receptionists = ();
    type Shutdowns = ();

    fn receptionists(&self) -> Self::Receptionists {}

    async fn shutdown(self) -> Self::Shutdowns {}
}

impl<Role, D, Tail> InstalledEntityFamilies for (Role, InstalledEntityFamily<D>, Tail)
where
    Role: Clone + Send + 'static,
    D: EntityDefinition,
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal, D::ChildFailures>,
    Tail: InstalledEntityFamilies + Send,
{
    type Receptionists = (Role, Entities<D>, Tail::Receptionists);
    type Shutdowns = (
        Role,
        (
            EntityShutdown<tokio::task::JoinError, EntityRetirementFailure, D::Id>,
            EntityMetrics,
            Option<Box<dyn Any + Send>>,
        ),
        Tail::Shutdowns,
    );

    fn receptionists(&self) -> Self::Receptionists {
        (
            self.0.clone(),
            self.1.entities.clone(),
            self.2.receptionists(),
        )
    }

    async fn shutdown(self) -> Self::Shutdowns {
        let (role, family, tail) = self;
        let (shutdown, metrics) = family.shutdown().await;
        let family_disposal_failure = catch_unwind(AssertUnwindSafe(|| drop(family))).err();
        let tail = tail.shutdown().await;
        (role, (shutdown, metrics, family_disposal_failure), tail)
    }
}
