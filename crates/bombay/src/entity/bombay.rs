//! Native binding from Entity lifecycle effects to Bombay incarnations.

use core::future::Future;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use behavior::{
    Behavior, BehaviorBase, BehaviorMessage, BirthMode, FoldBirthNode, Here, InjectEvent, Never,
    Protocol, ShutdownRejection, ShutdownRequested,
};
use communication::Config;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::ActorRetirement;
use crate::address::{ApplicationAddresses, MailAddr};
use crate::application_runtime::{ApplicationCapabilities, NoParent, StructuralOrigins};
use crate::child_bindings::{
    OccurrenceBindings, RetireChildTasks, RuntimeChildBindings, RuntimeChildSpaces,
};
use crate::interpret::{ActionInterpreter, EffectInterpretationError};
use crate::launch::{OwnedActor, SpawnError, spawn_owned_entity_with};
use crate::local::{ActorRef, CommitActions};
use crate::topology::{HostedActorSpaces, Hosts};

use super::family::{EntityCapacity, EntityDefinition, EntityMetricState};
use super::{
    Activated, ActivationId, EntityActivationError, EntityId, FenceFailure, LocalEntityRuntime,
    RetirementMode,
};

const USER_CAPACITY: usize = 1_024;

type NativeEntityCapabilities<B, N, Terminal> = ApplicationCapabilities<
    B,
    HostedActorSpaces<Arc<N>>,
    NoParent,
    OccurrenceBindings<B, Terminal>,
    StructuralOrigins<<B as BehaviorBase>::Base>,
>;
type NativeEntityInterpreter<B, N, Terminal> =
    ActionInterpreter<NativeEntityCapabilities<B, N, Terminal>>;
type NativeEntityActor<B, Terminal> = OwnedActor<B, Vec<Terminal>, EffectInterpretationError>;

#[diagnostic::on_unimplemented(
    message = "the application host product cannot execute Entity behavior `{B}`",
    label = "missing a required actor host or typed effect interpreter"
)]
pub(crate) trait NativeEntityHost<B, Terminal>: Send + Sync + Sized
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
{
    fn launch_entity(
        self: Arc<Self>,
        address: MailAddr,
        allocations: ApplicationAddresses,
        behavior: B,
    ) -> impl Future<Output = Result<NativeEntityActor<B, Terminal>, ActorRetirement<B, Terminal>>> + Send;
}

impl<B, N, Terminal> NativeEntityHost<B, Terminal> for N
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase + Send + 'static,
    B::Error: Send + 'static,
    B::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    B::Sends: Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    <B::Birth as BirthMode>::Child: FoldBirthNode<RuntimeChildBindings<Terminal>>
        + FoldBirthNode<RuntimeChildSpaces>
        + Send
        + 'static,
    N: Hosts<B::Protocol> + Send + Sync + 'static,
    OccurrenceBindings<B, Terminal>: Default + RetireChildTasks<Root = Terminal> + Send + 'static,
    NativeEntityInterpreter<B, N, Terminal>: CommitActions<B, Error = EffectInterpretationError, Retired = Vec<Terminal>>
        + Send
        + 'static,
    Terminal: Send + 'static,
{
    async fn launch_entity(
        self: Arc<Self>,
        address: MailAddr,
        allocations: ApplicationAddresses,
        behavior: B,
    ) -> Result<NativeEntityActor<B, Terminal>, ActorRetirement<B, Terminal>> {
        let addresses = <N as Hosts<B::Protocol>>::space(&self).clone();
        spawn_owned_entity_with(
            addresses,
            Config::new(USER_CAPACITY),
            address,
            behavior,
            move |control, terminal_reports, timers, facts| {
                ActionInterpreter::new(ApplicationCapabilities::new_with_bindings(
                    crate::application_runtime::ApplicationCapabilityInputs {
                        address,
                        actor_spaces: Arc::new(HostedActorSpaces(self)),
                        allocations,
                        control,
                        timers,
                        facts,
                        terminal_reports,
                    },
                    OccurrenceBindings::<B, Terminal>::default(),
                ))
            },
        )
        .await
        .map_err(crate::launch::SpawnError::into_retirement)
    }
}

pub(crate) struct EntityTaskOwner {
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl EntityTaskOwner {
    fn new() -> Self {
        Self {
            tasks: Mutex::new(Vec::new()),
        }
    }

    fn track(&self, task: tokio::task::JoinHandle<()>) {
        let mut tasks = self.tasks.lock().unwrap_or_else(PoisonError::into_inner);
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
    }

    pub(crate) async fn join(&self) {
        let tasks = {
            let mut tasks = self.tasks.lock().unwrap_or_else(PoisonError::into_inner);
            tasks.drain(..).collect::<Vec<_>>()
        };
        for task in tasks {
            drop(task.await);
        }
    }
}

impl Drop for EntityTaskOwner {
    fn drop(&mut self) {
        let tasks = self.tasks.get_mut().unwrap_or_else(PoisonError::into_inner);
        for task in tasks.drain(..) {
            task.abort();
        }
    }
}

struct HydrationMeasurement {
    metrics: Arc<EntityMetricState>,
}

impl HydrationMeasurement {
    fn begin(metrics: Arc<EntityMetricState>) -> Self {
        metrics.hydration_started();
        Self { metrics }
    }
}

impl Drop for HydrationMeasurement {
    fn drop(&mut self) {
        self.metrics.hydration_finished();
    }
}

pub(crate) struct NativeEntityLease<D>
where
    D: EntityDefinition,
{
    actor: NativeEntityActor<D::Behavior, D::Terminal>,
    resident: OwnedSemaphorePermit,
}

pub(crate) struct BombayEntityRuntime<D>
where
    D: EntityDefinition,
{
    definition: Arc<D>,
    actors: Arc<D::Hosts>,
    allocations: ApplicationAddresses,
    hydrations: Arc<Semaphore>,
    residents: Arc<Semaphore>,
    metrics: Arc<EntityMetricState>,
    tasks: Weak<EntityTaskOwner>,
}

impl<D> Clone for BombayEntityRuntime<D>
where
    D: EntityDefinition,
{
    fn clone(&self) -> Self {
        Self {
            definition: Arc::clone(&self.definition),
            actors: Arc::clone(&self.actors),
            allocations: self.allocations.clone(),
            hydrations: Arc::clone(&self.hydrations),
            residents: Arc::clone(&self.residents),
            metrics: Arc::clone(&self.metrics),
            tasks: self.tasks.clone(),
        }
    }
}

pub(crate) fn bombay_entity_runtime<D>(
    definition: Arc<D>,
    actors: Arc<D::Hosts>,
    allocations: ApplicationAddresses,
    capacity: EntityCapacity,
    metrics: Arc<EntityMetricState>,
) -> (BombayEntityRuntime<D>, Arc<EntityTaskOwner>)
where
    D: EntityDefinition,
{
    let tasks = Arc::new(EntityTaskOwner::new());
    let runtime = BombayEntityRuntime {
        definition,
        actors,
        allocations,
        hydrations: Arc::new(Semaphore::new(capacity.concurrent_hydrations().get())),
        residents: Arc::new(Semaphore::new(capacity.residents().get())),
        metrics,
        tasks: Arc::downgrade(&tasks),
    };
    (runtime, tasks)
}

impl<D> LocalEntityRuntime<D::Id, BehaviorMessage<D::Behavior>> for BombayEntityRuntime<D>
where
    D: EntityDefinition,
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal>,
{
    type Origin = MailAddr;
    type Endpoint = ActorRef<<D::Behavior as Behavior>::Protocol>;
    type Lease = NativeEntityLease<D>;
    type ActivationError = EntityActivationError<D::HydrationError, D::Behavior, D::Terminal>;

    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        let Some(owner) = self.tasks.upgrade() else {
            return;
        };
        owner.track(tokio::spawn(task));
    }

    async fn activate(
        &self,
        entity_id: EntityId<D::Id>,
        _: ActivationId,
    ) -> Result<Activated<Self::Endpoint, Self::Lease>, Self::ActivationError> {
        let resident = Arc::clone(&self.residents)
            .try_acquire_owned()
            .map_err(|_| {
                self.metrics.capacity_refused();
                EntityActivationError::ResidentCapacity
            })?;
        let hydration = Arc::clone(&self.hydrations)
            .acquire_owned()
            .await
            .expect("the application-owned hydration semaphore remains open");
        let measurement = HydrationMeasurement::begin(Arc::clone(&self.metrics));
        let behavior = self.definition.hydrate(entity_id).await.map_err(|error| {
            self.metrics.hydration_failed();
            EntityActivationError::Hydration(error)
        })?;
        drop((measurement, hydration));
        let address = match self.allocations.allocate() {
            Ok(address) => address,
            Err(reason) => {
                self.metrics.launch_failed();
                let failure = SpawnError::<
                    D::Behavior,
                    EffectInterpretationError,
                    Vec<D::Terminal>,
                >::AllocationRejected {
                    behavior,
                    reason,
                };
                return Err(EntityActivationError::Launch(failure.into_retirement()));
            }
        };
        let actor = Arc::clone(&self.actors)
            .launch_entity(address, self.allocations.clone(), behavior)
            .await
            .map_err(|retirement| {
                self.metrics.launch_failed();
                EntityActivationError::Launch(retirement)
            })?;
        self.metrics.activation_succeeded();
        Ok(Activated {
            endpoint: actor.actor.clone(),
            lease: NativeEntityLease { actor, resident },
        })
    }

    fn activation_failed(
        &self,
        entity_id: EntityId<D::Id>,
        activation_id: ActivationId,
        error: Self::ActivationError,
    ) {
        self.definition
            .activation_failed(entity_id, activation_id, error);
    }

    async fn deliver(
        &self,
        endpoint: Self::Endpoint,
        origin: Self::Origin,
        command: BehaviorMessage<D::Behavior>,
    ) -> Result<(), BehaviorMessage<D::Behavior>> {
        endpoint
            .send_from(origin, command)
            .await
            .map_err(crate::SendError::into_message)
    }

    async fn fence(&self, endpoint: Self::Endpoint) -> Result<(), FenceFailure> {
        endpoint.fence().await
    }

    async fn retire(
        &self,
        entity_id: EntityId<D::Id>,
        activation_id: ActivationId,
        lease: Self::Lease,
        retirement: RetirementMode,
    ) {
        if let RetirementMode::Forced(failure) = retirement {
            self.metrics.forced_retirement();
            self.definition
                .forced_retirement(entity_id.clone(), activation_id, failure);
        }
        let actor = lease.actor.actor.clone();
        match actor.request_shutdown() {
            Ok(())
            | Err(ShutdownRejection::AlreadyStopping | ShutdownRejection::AlreadyStopped) => {}
        }
        match actor.termination().await {
            Ok(_) | Err(_) => {}
        }
        let retirement = ActorRetirement::from_local(lease.actor.task.retire().await);
        self.definition
            .retired(entity_id, activation_id, retirement);
        self.metrics.retired();
        drop(lease.resident);
    }
}
