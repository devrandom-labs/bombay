//! Nominal native Entity definitions, stable references, and family products.

#![allow(
    private_bounds,
    reason = "native execution proof remains private application composition"
)]

use core::future::Future;
use core::hash::Hash;
use core::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use behavior::{
    ActionItem, Behavior, BehaviorBase, BehaviorMessage, BehaviorSettlements, BirthMode,
    ClassifySettlement, Here, InjectEvent, Inside, InterpreterRequest, LogicalHostRequirements,
    Never, NoReturnToEmitter, Protocol,
};
use behavior_actors::ShutdownRequested;

use crate::ActorRetirement;
use crate::address::{ApplicationAddresses, MailAddr};
use crate::local::ActorRef;
use crate::topology::Hosts as LocalHosts;

use super::bombay::{
    BombayEntityRuntime, EntityTaskOwner, NativeEntityHost, NativeEntityLease,
    bombay_entity_runtime,
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
    type Behavior: BehaviorSettlements<
            Protocol: Protocol<Addr = MailAddr, Msg: Send + 'static>,
            Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
            Sends: Send + 'static,
            Error: Send + 'static,
            Birth: BirthMode<Child: Send + 'static>,
            Settlements: ClassifySettlement + Send + 'static,
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
        failure: EntityActivationError<Self::HydrationError, Self::Behavior, Self::Terminal>,
    );

    /// Consume one exact actor-originated admission refusal.
    fn admission_refused(
        &self,
        id: EntityId<Self::Id>,
        failure: AdmissionFailure<BehaviorMessage<Self::Behavior>>,
    );

    /// Consume the exact reason a graceful drain became forced retirement.
    fn forced_retirement(
        &self,
        id: EntityId<Self::Id>,
        activation: ActivationId,
        failure: DrainFailure,
    );

    /// Consume the final actor state, descendants, and terminal disposition.
    fn retired(
        &self,
        id: EntityId<Self::Id>,
        activation: ActivationId,
        retirement: ActorRetirement<Self::Behavior, Self::Terminal>,
    );
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
pub enum EntityActivationError<Hydration, B, Terminal>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    /// The explicit active-or-activating family bound was full.
    ResidentCapacity,
    /// Domain reconstruction failed before address allocation.
    Hydration(Hydration),
    /// Actor launch failed with exact final state when state existed.
    Launch(ActorRetirement<B, Terminal>),
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
        D::Hosts: NativeEntityHost<D::Behavior, D::Terminal>,
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
        D::Hosts: NativeEntityHost<D::Behavior, D::Terminal>,
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
        D::Hosts: NativeEntityHost<D::Behavior, D::Terminal>,
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
}

impl<D> ActionItem for EntityAdmission<D>
where
    D: EntityDefinition,
{
    type Accepted = ();
    type Rejection = Never;
    type Prerequisite = Never;
}

impl<D> EntityAdmission<D>
where
    D: EntityDefinition,
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal>,
{
    pub(crate) async fn interpret(self, origin: MailAddr) {
        let Self { entity, command } = self;
        let id = entity.id.clone();
        if let Err(failure) = entity.admit_from(origin, command).await {
            entity.entities.definition.admission_refused(id, failure);
        }
    }
}

mod application_families_sealed {
    pub trait Sealed {}
}

/// Static application family product and its live/shutdown projections.
pub trait EntityApplicationFamilies<Hosts>: application_families_sealed::Sealed {
    type Receptionists: Clone + Send + 'static;
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
    Hosts: NativeEntityHost<D::Behavior, D::Terminal>,
    Tail: EntityApplicationFamilies<Hosts>,
{
    type Receptionists = (Role, Entities<D>, Tail::Receptionists);
    type Shutdowns = (Role, (EntityShutdown, EntityMetrics), Tail::Shutdowns);
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
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal>,
{
    runtime: InstalledRuntimeFor<D>,
    entities: Entities<D>,
    tasks: Arc<EntityTaskOwner>,
    metrics: Arc<EntityMetricState>,
}

impl<D> InstalledEntityFamily<D>
where
    D: EntityDefinition,
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal>,
{
    async fn shutdown(self) -> (EntityShutdown, EntityMetrics) {
        let directory = self.runtime.shutdown().await;
        self.tasks.join().await;
        (directory, self.metrics.snapshot())
    }
}

pub(crate) trait InstallEntityFamilies<Hosts>: EntityApplicationFamilies<Hosts> {
    type Installed: InstalledEntityFamilies<Receptionists = Self::Receptionists, Shutdowns = Self::Shutdowns>
        + Send;

    fn install(self, hosts: Arc<Hosts>, allocations: ApplicationAddresses) -> Self::Installed;
}

impl<Hosts> InstallEntityFamilies<Hosts> for ()
where
    Hosts: Send + Sync + 'static,
{
    type Installed = ();

    fn install(self, _: Arc<Hosts>, _: ApplicationAddresses) -> Self::Installed {}
}

impl<Hosts, Role, D, Tail> InstallEntityFamilies<Hosts>
    for (Role, D, DirectoryConfig, EntityCapacity, Tail)
where
    Hosts: Send + Sync + 'static,
    Role: Clone + Send + 'static,
    D: EntityDefinition<Hosts = Hosts>,
    Hosts: NativeEntityHost<D::Behavior, D::Terminal>,
    Tail: InstallEntityFamilies<Hosts>,
{
    type Installed = (Role, InstalledEntityFamily<D>, Tail::Installed);

    fn install(self, hosts: Arc<Hosts>, allocations: ApplicationAddresses) -> Self::Installed {
        let (role, definition, directory, capacity, tail) = self;
        let definition = Arc::new(definition);
        let metrics = Arc::new(EntityMetricState::default());
        let (runtime, tasks) = bombay_entity_runtime(
            Arc::clone(&definition),
            Arc::clone(&hosts),
            allocations.clone(),
            capacity,
            Arc::clone(&metrics),
        );
        let Ok(runtime) = EntityRuntime::new(directory, runtime) else {
            unreachable!("EntityCapacity retains a validated directory configuration")
        };
        let entities = Entities::from_runtime(&runtime, definition);
        let family = InstalledEntityFamily {
            runtime,
            entities,
            tasks,
            metrics,
        };
        let tail = tail.install(hosts, allocations);
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
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal>,
    Tail: InstalledEntityFamilies + Send,
{
    type Receptionists = (Role, Entities<D>, Tail::Receptionists);
    type Shutdowns = (Role, (EntityShutdown, EntityMetrics), Tail::Shutdowns);

    fn receptionists(&self) -> Self::Receptionists {
        (
            self.0.clone(),
            self.1.entities.clone(),
            self.2.receptionists(),
        )
    }

    async fn shutdown(self) -> Self::Shutdowns {
        let (role, family, tail) = self;
        let family = family.shutdown().await;
        let tail = tail.shutdown().await;
        (role, family, tail)
    }
}
