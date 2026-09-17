//! Native binding from Entity lifecycle effects to Bombay incarnations.

use core::future::Future;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use behavior::{
    Behavior, BehaviorBase, BehaviorMessage, BehaviorSettlements, BirthMode,
    ChildOccurrenceProduct, ClassifySettlement, Here, InjectEvent, Never, Protocol,
};
use behavior_actors::{ShutdownRejection, ShutdownRequested};
use communication::Config;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::ActorRetirement;
use crate::address::{ApplicationAddresses, MailAddr};
use crate::application_runtime::{ApplicationCapabilities, NoParent, StructuralOrigins};
use crate::child_bindings::{
    OccurrenceBindings, RetireChildTasks, RuntimeChildBindings, RuntimeChildSpaces,
};
use crate::interpret::{ActionInterpreter, ActionSettlementOf};
use crate::launch::{OwnedActor, SpawnError, spawn_owned_entity_with};
use crate::local::{ActorRef, CommitActions};
use crate::topology::{HostedActorSpaces, Hosts};

use super::family::{EntityCapacity, EntityDefinition, EntityMetricState};
use super::{
    ActivationId, AdmissionFailure, DrainStage, EffectInterpreter, EntityActivationError,
    EntityId, EntityTaskGroup, LocalDirectory, PendingCommand, RetirementMode,
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
type NativeEntityActor<B, Terminal> = OwnedActor<B, Vec<Terminal>>;

#[diagnostic::on_unimplemented(
    message = "the application host product cannot execute Entity behavior `{B}`",
    label = "missing a required actor host or typed effect interpreter"
)]
pub(crate) trait NativeEntityHost<B, Terminal>: Send + Sync + Sized
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
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
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>
        + BehaviorBase
        + Send
        + 'static,
    B::Error: Send + 'static,
    B::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    B::Sends: Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    <B::Birth as BirthMode>::Child: ChildOccurrenceProduct<RuntimeChildBindings<Terminal>>
        + ChildOccurrenceProduct<RuntimeChildSpaces>
        + Send
        + 'static,
    N: Hosts<B::Protocol> + Send + Sync + 'static,
    OccurrenceBindings<B, Terminal>: Default + RetireChildTasks<Root = Terminal> + Send + 'static,
    NativeEntityInterpreter<B, N, Terminal>:
        CommitActions<B, Retired = Vec<Terminal>> + Send + 'static,
    ActionSettlementOf<B>: ClassifySettlement + Send + 'static,
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

/// Directory instantiation for the native Entity binding.
pub(crate) type NativeEntityDirectory<D> = LocalDirectory<
    <D as EntityDefinition>::Id,
    PendingCommand<MailAddr, BehaviorMessage<<D as EntityDefinition>::Behavior>>,
    ActorRef<<<D as EntityDefinition>::Behavior as Behavior>::Protocol>,
    NativeEntityLease<D>,
>;

/// Stage at which an ordered fence operation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum FenceFailure {
    /// The fence was not enqueued.
    #[error("fence was not enqueued")]
    Enqueue,
    /// The fence was enqueued but not acknowledged.
    #[error("fence was enqueued but not acknowledged")]
    Acknowledgement,
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
    directory: Arc<NativeEntityDirectory<D>>,
    settlement: Arc<EntityTaskGroup>,
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
            directory: Arc::clone(&self.directory),
            settlement: Arc::clone(&self.settlement),
        }
    }
}

pub(crate) fn bombay_entity_runtime<D>(
    definition: Arc<D>,
    actors: Arc<D::Hosts>,
    allocations: ApplicationAddresses,
    capacity: EntityCapacity,
    metrics: Arc<EntityMetricState>,
    directory: Arc<NativeEntityDirectory<D>>,
    settlement: Arc<EntityTaskGroup>,
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
        directory,
        settlement,
    };
    (runtime, tasks)
}

impl<D>
    EffectInterpreter<
        <D as EntityDefinition>::Id,
        PendingCommand<MailAddr, BehaviorMessage<<D as EntityDefinition>::Behavior>>,
        ActorRef<<<D as EntityDefinition>::Behavior as Behavior>::Protocol>,
        NativeEntityLease<D>,
    > for BombayEntityRuntime<D>
where
    D: EntityDefinition,
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal>,
{
    fn start_activation(
        &self,
        entity_id: EntityId<D::Id>,
        activation_id: ActivationId,
    ) {
        let runtime = self.clone();
        self.spawn_settled(async move {
            let activation = runtime.activate(entity_id.clone(), activation_id).await;
            let output = match activation {
                Ok((endpoint, lease)) => runtime.directory.activation_succeeded(
                    &entity_id,
                    activation_id,
                    endpoint,
                    lease,
                ),
                Err(error) => {
                    runtime.activation_failed(entity_id.clone(), activation_id, error);
                    runtime
                        .directory
                        .activation_failed(&entity_id, activation_id)
                }
            };
            runtime.directory.interpret(output, &runtime);
        });
    }

    fn deliver(
        &self,
        entity_id: EntityId<D::Id>,
        activation_id: ActivationId,
        dispatch_id: DispatchId,
        endpoint: ActorRef<<<D as EntityDefinition>::Behavior as Behavior>::Protocol>,
        pending: PendingCommand<MailAddr, BehaviorMessage<<D as EntityDefinition>::Behavior>>,
    ) {
        let runtime = self.clone();
        self.spawn_settled(async move {
            let PendingCommand {
                origin,
                command,
                publisher,
            } = pending;
            let failure = match runtime.deliver(endpoint, origin.clone(), command).await {
                Ok(()) => {
                    publisher.complete(Ok(()));
                    None
                }
                Err(command) => Some((
                    dispatch_id,
                    PendingCommand {
                        origin,
                        command,
                        publisher,
                    },
                )),
            };
            let output = runtime
                .directory
                .delivery_resolved(&entity_id, activation_id, failure);
            runtime.directory.interpret(output, &runtime);
        });
    }

    fn reject(
        &self,
        _: DispatchId,
        pending: PendingCommand<MailAddr, BehaviorMessage<<D as EntityDefinition>::Behavior>>,
        reason: Refusal,
    ) {
        pending.publisher.complete(Err(AdmissionFailure::Refused {
            command: pending.command,
            reason,
        }));
    }

    fn enqueue_fence(
        &self,
        entity_id: EntityId<D::Id>,
        activation_id: ActivationId,
        endpoint: ActorRef<<<D as EntityDefinition>::Behavior as Behavior>::Protocol>,
    ) {
        let runtime = self.clone();
        self.spawn_settled(async move {
            let output = match runtime.fence(endpoint).await {
                Ok(()) => runtime
                    .directory
                    .fence_acknowledged(&entity_id, activation_id),
                Err(failure) => runtime.directory.force_drain(
                    &entity_id,
                    activation_id,
                    DrainFailure {
                        stage: match failure {
                            FenceFailure::Enqueue => DrainStage::FenceEnqueue,
                            FenceFailure::Acknowledgement => DrainStage::FenceAcknowledgement,
                        },
                        outstanding_reservations: 0,
                    },
                ),
            };
            runtime.directory.interpret(output, &runtime);
        });
    }

    fn retire(
        &self,
        entity_id: EntityId<D::Id>,
        activation_id: ActivationId,
        lease: NativeEntityLease<D>,
        retirement: RetirementMode,
    ) {
        let runtime = self.clone();
        self.spawn_settled(async move {
            runtime
                .retire(entity_id.clone(), activation_id, lease, retirement)
                .await;
            let output = runtime.directory.terminated(&entity_id, activation_id);
            runtime.directory.interpret(output, &runtime);
        });
    }
}

impl<D> BombayEntityRuntime<D>
where
    D: EntityDefinition,
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal>,
{
    /// Schedule one lifecycle task under a settlement guard and the owned
    /// application task registry.
    fn spawn_settled(&self, task: impl Future<Output = ()> + Send + 'static) {
        let guard = self.settlement.begin();
        let Some(owner) = self.tasks.upgrade() else {
            return;
        };
        owner.track(tokio::spawn(async move {
            let _guard = guard;
            task.await;
        }));
    }

    async fn activate(
        &self,
        entity_id: EntityId<D::Id>,
        _: ActivationId,
    ) -> Result<
        (
            ActorRef<<D::Behavior as Behavior>::Protocol>,
            NativeEntityLease<D>,
        ),
        EntityActivationError<D::HydrationError, D::Behavior, D::Terminal>,
    > {
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
                let failure = SpawnError::<D::Behavior, Vec<D::Terminal>>::AllocationRejected {
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
        Ok((actor.actor.clone(), NativeEntityLease { actor, resident }))
    }

    fn activation_failed(
        &self,
        entity_id: EntityId<D::Id>,
        activation_id: ActivationId,
        error: EntityActivationError<D::HydrationError, D::Behavior, D::Terminal>,
    ) {
        self.definition
            .activation_failed(entity_id, activation_id, error);
    }

    async fn deliver(
        &self,
        endpoint: ActorRef<<D::Behavior as Behavior>::Protocol>,
        origin: MailAddr,
        command: BehaviorMessage<D::Behavior>,
    ) -> Result<(), BehaviorMessage<D::Behavior>> {
        endpoint
            .send_from(origin, command)
            .await
            .map_err(crate::SendError::into_message)
    }

    async fn fence(&self, endpoint: ActorRef<<D::Behavior as Behavior>::Protocol>) -> Result<(), FenceFailure> {
        endpoint.fence().await
    }

    async fn retire(
        &self,
        entity_id: EntityId<D::Id>,
        activation_id: ActivationId,
        lease: NativeEntityLease<D>,
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
