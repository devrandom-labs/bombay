//! Concise asynchronous API over the local entity directory.

use core::future::Future;
use core::hash::Hash;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::{Arc, Mutex, PoisonError, Weak};

use crate::observe::{AffineObservation, Observation, Publisher, affine_pair, pair};

use super::{
    ActivationId, DirectoryConfig, DirectoryError, DispatchId, DrainFailure, DrainStage,
    EffectInterpreter, EntityId, LifecycleEdge, LifecyclePhase, LocalDirectory, Refusal,
    RetirementMode, TransitionEvidence,
};

/// Exact-incarnation capabilities returned by transactional activation.
pub struct Activated<E, L> {
    /// Delivery-only capability.
    pub endpoint: E,
    /// Affine retirement capability.
    pub lease: L,
}

/// Stage at which an ordered fence operation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FenceFailure {
    /// The fence was not enqueued.
    #[error("fence was not enqueued")]
    Enqueue,
    /// The fence was enqueued but not acknowledged.
    #[error("fence was enqueued but not acknowledged")]
    Acknowledgement,
}

/// Outcome of one [`EntityRuntime::passivate`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Passivation {
    /// Admission was closed at this call's lifecycle linearization point.
    Begun,
    /// No active incarnation exists to passivate.
    NotActive,
    /// A passivation was already in progress for the exact incarnation.
    AlreadyPassivating,
    /// The observed incarnation was superseded before the drain linearized.
    Superseded,
    /// Family shutdown already owns draining and retirement.
    ShuttingDown,
}

/// Runtime port implemented once by an actor runtime integration.
///
/// Bombay implements this port without exposing its addresses or
/// provisional activation types through the stable entity API.
pub trait LocalEntityRuntime<I, C>: Send + Sync + 'static {
    /// Caller provenance carried to exact mailbox admission.
    type Origin: Clone + Send + 'static;
    /// Cloneable exact-incarnation delivery capability.
    type Endpoint: Clone + Send + Sync + 'static;
    /// Affine exact-incarnation retirement capability.
    type Lease: Send + 'static;
    /// Transactional activation failure.
    type ActivationError: Send + 'static;
    /// Owned handle returned by one scheduled lifecycle task.
    type Task: Send + 'static;
    /// Exact failure returned when the owned task is joined.
    type TaskFailure: Send + 'static;

    /// Schedule work owned by the entity directory and return its join handle.
    ///
    /// Scheduling is infallible at this port boundary: dropping the future
    /// instead strands its required lifecycle fact. A task panic or cancel is
    /// retained by the join result and returned from family shutdown.
    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) -> Self::Task;

    /// Join one exact task and preserve its concrete failure on panic or cancel.
    /// Dropping the returned future before it resolves must leave `task`
    /// joinable by a later family shutdown claimant.
    fn join(task: &mut Self::Task) -> impl Future<Output = Result<(), Self::TaskFailure>> + Send;

    /// Prepare and transactionally activate an exact incarnation.
    fn activate(
        &self,
        entity_id: EntityId<I>,
        activation_id: ActivationId,
    ) -> impl Future<Output = Result<Activated<Self::Endpoint, Self::Lease>, Self::ActivationError>> + Send;

    /// Consume the exact failure from one activation attempt.
    ///
    /// The lifecycle directory separately returns each waiting command with
    /// [`Refusal::Unavailable`]. This callback preserves the authoritative
    /// runtime diagnostic without cloning it across those independent command
    /// owners or erasing it inside the coordinator.
    fn activation_failed(
        &self,
        entity_id: EntityId<I>,
        activation_id: ActivationId,
        error: Self::ActivationError,
    );

    /// Enqueue a command, returning ownership on failure.
    fn deliver(
        &self,
        endpoint: Self::Endpoint,
        origin: Self::Origin,
        command: C,
    ) -> impl Future<Output = Result<(), C>> + Send;

    /// Enqueue and await acknowledgement of an ordered processing fence.
    fn fence(
        &self,
        endpoint: Self::Endpoint,
    ) -> impl Future<Output = Result<(), FenceFailure>> + Send;

    /// Retire and await exact termination of one incarnation.
    fn retire(
        &self,
        entity_id: EntityId<I>,
        activation_id: ActivationId,
        lease: Self::Lease,
        retirement: RetirementMode,
    ) -> impl Future<Output = ()> + Send;
}

/// Failure from [`EntityRuntime::admit`] with command ownership preserved.
#[derive(Debug, thiserror::Error)]
pub enum AdmissionFailure<C> {
    /// Lifecycle admission or delivery refused the command.
    #[error("lifecycle admission or delivery refused the command")]
    Refused {
        /// Original command.
        command: C,
        /// Exact refusal classification.
        reason: Refusal,
    },
    /// The non-reusable activation identity namespace is exhausted.
    #[error("activation identity namespace is exhausted")]
    ActivationIdsExhausted(C),
    /// The non-reusable dispatch identity namespace is exhausted.
    #[error("dispatch identity namespace is exhausted")]
    DispatchIdsExhausted(C),
}

struct PendingCommand<O, C> {
    origin: O,
    command: C,
    publisher: Publisher<Result<(), AdmissionFailure<C>>>,
}

/// Local stable-routing facade used by applications.
pub struct EntityRuntime<I, C, R>
where
    R: LocalEntityRuntime<I, C>,
{
    inner: Arc<RuntimeState<I, C, R>>,
}

struct Runtime<I, C, R, Origin, Endpoint, Lease, Task, TaskFailure> {
    directory: LocalDirectory<I, PendingCommand<Origin, C>, Endpoint, Lease>,
    port: R,
    admission: Mutex<EntityAdmission>,
    tasks: Arc<EntityTaskGroup<Task, TaskFailure>>,
}

type RuntimeState<I, C, R> = Runtime<
    I,
    C,
    R,
    <R as LocalEntityRuntime<I, C>>::Origin,
    <R as LocalEntityRuntime<I, C>>::Endpoint,
    <R as LocalEntityRuntime<I, C>>::Lease,
    <R as LocalEntityRuntime<I, C>>::Task,
    <R as LocalEntityRuntime<I, C>>::TaskFailure,
>;
type RuntimeReceptionist<I, C, R> = EntityReceptionist<
    I,
    C,
    R,
    <R as LocalEntityRuntime<I, C>>::Origin,
    <R as LocalEntityRuntime<I, C>>::Endpoint,
    <R as LocalEntityRuntime<I, C>>::Lease,
    <R as LocalEntityRuntime<I, C>>::Task,
    <R as LocalEntityRuntime<I, C>>::TaskFailure,
>;

pub(crate) struct EntityReceptionist<I, C, R, Origin, Endpoint, Lease, Task, TaskFailure> {
    #[expect(
        clippy::type_complexity,
        reason = "the private receptionist retains exact runtime task and join-failure types"
    )]
    inner: Arc<Runtime<I, C, R, Origin, Endpoint, Lease, Task, TaskFailure>>,
}

enum EntityAdmission {
    Open,
    Closed,
}

struct EntityTaskGroup<Task, TaskFailure> {
    state: Mutex<EntityTaskState<Task, TaskFailure>>,
}

enum EntityTaskState<Task, TaskFailure> {
    Open {
        active: usize,
        idle_epoch: Option<(Publisher<()>, Observation<()>)>,
        tasks: Vec<EntityTaskRecord<Task, TaskFailure>>,
        shutdown: ShutdownClaimPhase,
        drain: FamilyDrainPhase,
    },
    Closed,
}

enum ShutdownClaimPhase {
    Available,
    Claimed,
}

#[derive(Clone, Copy)]
enum FamilyDrainPhase {
    NotStarted,
    Started { represented: usize },
}

enum EntityTaskRecord<Task, TaskFailure> {
    Running(Task),
    Joined,
    Failed(TaskFailure),
}

struct EntityShutdownClaim<Task, TaskFailure> {
    group: Arc<EntityTaskGroup<Task, TaskFailure>>,
    tasks: Vec<EntityTaskRecord<Task, TaskFailure>>,
}

struct EntityTaskGuard<Task, TaskFailure> {
    group: Arc<EntityTaskGroup<Task, TaskFailure>>,
}

/// Exact result of one family shutdown request.
#[derive(Debug)]
pub enum EntityShutdown<TaskFailure> {
    /// Every represented incarnation retired and every lifecycle task joined.
    Settled { represented: usize },
    /// Another caller owns or already received the affine shutdown result.
    AlreadyClaimed,
    /// A lifecycle task failed; exact failures remain owned by the caller.
    TaskFailed {
        represented: usize,
        failures: Vec<TaskFailure>,
    },
}

impl<Task, TaskFailure> EntityTaskGroup<Task, TaskFailure> {
    fn new() -> Self {
        Self {
            state: Mutex::new(EntityTaskState::Open {
                active: 0,
                idle_epoch: None,
                tasks: Vec::new(),
                shutdown: ShutdownClaimPhase::Available,
                drain: FamilyDrainPhase::NotStarted,
            }),
        }
    }

    fn begin(self: &Arc<Self>) -> EntityTaskGuard<Task, TaskFailure> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let EntityTaskState::Open {
            active, idle_epoch, ..
        } = &mut *state
        else {
            panic!("entity lifecycle task scheduled after family shutdown");
        };
        if *active == 0 {
            *idle_epoch = Some(pair());
        }
        *active = active
            .checked_add(1)
            .expect("entity lifecycle task count exhausted");
        EntityTaskGuard {
            group: Arc::clone(self),
        }
    }

    fn claim(self: &Arc<Self>) -> Option<EntityShutdownClaim<Task, TaskFailure>> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match &mut *state {
            EntityTaskState::Open { shutdown, .. } => match shutdown {
                ShutdownClaimPhase::Available => {
                    *shutdown = ShutdownClaimPhase::Claimed;
                    Some(EntityShutdownClaim {
                        group: Arc::clone(self),
                        tasks: Vec::new(),
                    })
                }
                ShutdownClaimPhase::Claimed => None,
            },
            EntityTaskState::Closed => None,
        }
    }

    fn track(&self, task: Task) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let EntityTaskState::Open { tasks, .. } = &mut *state else {
            panic!("entity lifecycle task registered after family shutdown");
        };
        tasks.push(EntityTaskRecord::Running(task));
    }

    async fn wait_idle(&self) {
        loop {
            let observation = {
                let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                match &*state {
                    EntityTaskState::Open { idle_epoch, .. } => idle_epoch
                        .as_ref()
                        .map(|(_, observation)| observation.clone()),
                    EntityTaskState::Closed => None,
                }
            };
            let Some(observation) = observation else {
                return;
            };
            observation.await;
        }
    }

    fn finish(&self) {
        let publisher = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let EntityTaskState::Open {
                active, idle_epoch, ..
            } = &mut *state
            else {
                panic!("entity lifecycle task completed after family shutdown");
            };
            *active = active
                .checked_sub(1)
                .expect("entity lifecycle task completed without registration");
            (*active == 0)
                .then(|| idle_epoch.take().map(|(publisher, _)| publisher))
                .flatten()
        };
        if let Some(publisher) = publisher {
            publisher.complete(());
        }
    }

    fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let EntityTaskState::Open {
            active,
            idle_epoch,
            tasks,
            shutdown,
            ..
        } = &*state
        else {
            return;
        };
        assert_eq!(*active, 0, "entity lifecycle tasks remain at shutdown");
        assert!(idle_epoch.is_none(), "idle entity epoch was not discharged");
        assert!(
            tasks.is_empty(),
            "entity lifecycle task handles remain at shutdown"
        );
        assert!(matches!(shutdown, ShutdownClaimPhase::Claimed));
        *state = EntityTaskState::Closed;
    }
}

impl<Task, TaskFailure> EntityShutdownClaim<Task, TaskFailure> {
    fn drain_phase(&self) -> FamilyDrainPhase {
        let state = self
            .group
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let EntityTaskState::Open { drain, .. } = &*state else {
            panic!("family drain phase requested after shutdown");
        };
        *drain
    }

    fn record_drain(&self, represented: usize) {
        let mut state = self
            .group
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let EntityTaskState::Open {
            drain: phase @ FamilyDrainPhase::NotStarted,
            ..
        } = &mut *state
        else {
            panic!("family drain was already recorded");
        };
        *phase = FamilyDrainPhase::Started { represented };
    }

    fn take_tasks(&mut self) {
        let mut state = self
            .group
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let EntityTaskState::Open {
            tasks,
            shutdown: ShutdownClaimPhase::Claimed,
            ..
        } = &mut *state
        else {
            panic!("only the family shutdown claimant can take lifecycle tasks");
        };
        self.tasks.append(tasks);
    }

    fn complete(mut self) -> Vec<TaskFailure> {
        let mut failures = Vec::new();
        for task in self.tasks.drain(..) {
            if let EntityTaskRecord::Failed(failure) = task {
                failures.push(failure);
            }
        }
        self.group.close();
        failures
    }
}

impl<Task, TaskFailure> Drop for EntityShutdownClaim<Task, TaskFailure> {
    fn drop(&mut self) {
        let mut state = self
            .group
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let EntityTaskState::Open {
            tasks, shutdown, ..
        } = &mut *state
        {
            tasks.append(&mut self.tasks);
            *shutdown = ShutdownClaimPhase::Available;
        }
    }
}

impl<Task, TaskFailure> Drop for EntityTaskGuard<Task, TaskFailure> {
    fn drop(&mut self) {
        self.group.finish();
    }
}

impl<I, C, R> Clone for EntityRuntime<I, C, R>
where
    R: LocalEntityRuntime<I, C>,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<I, C, R, Origin, Endpoint, Lease, Task, TaskFailure> Clone
    for EntityReceptionist<I, C, R, Origin, Endpoint, Lease, Task, TaskFailure>
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<I, C, R> EntityRuntime<I, C, R>
where
    I: Clone + Eq + Hash + Send + Sync + 'static,
    C: Send + 'static,
    R: LocalEntityRuntime<I, C>,
{
    /// Construct stable local entity routing over an actor-runtime port.
    ///
    /// # Errors
    ///
    /// Returns [`DirectoryError::InvalidShardCount`] for a shard count that is
    /// not a power of two.
    pub fn new(config: DirectoryConfig, runtime_port: R) -> Result<Self, DirectoryError<C>> {
        let directory = LocalDirectory::new(config).map_err(
            |error: DirectoryError<PendingCommand<R::Origin, C>>| match error {
                DirectoryError::InvalidShardCount => DirectoryError::InvalidShardCount,
                DirectoryError::ActivationIdsExhausted(pending) => {
                    DirectoryError::ActivationIdsExhausted(pending.command)
                }
                DirectoryError::DispatchIdsExhausted(pending) => {
                    DirectoryError::DispatchIdsExhausted(pending.command)
                }
            },
        )?;
        Ok(Self {
            inner: Arc::new(Runtime {
                directory,
                port: runtime_port,
                admission: Mutex::new(EntityAdmission::Open),
                tasks: Arc::new(EntityTaskGroup::new()),
            }),
        })
    }

    pub(crate) fn receptionist(&self) -> RuntimeReceptionist<I, C, R> {
        EntityReceptionist {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Deliver a command through stable entity routing.
    ///
    /// Dropping this future cancels its command only while it remains a bounded
    /// activation waiter. The shared activation task remains directory-owned,
    /// and a command already moved into active delivery is not retracted.
    ///
    /// # Errors
    ///
    /// Returns a typed refusal or exhausted identity namespace with the
    /// original command.
    ///
    /// # Panics
    ///
    /// Panics after synchronization poison or violation of an internal
    /// directory identity invariant.
    pub async fn admit(
        &self,
        origin: R::Origin,
        entity_id: EntityId<I>,
        command: C,
    ) -> Result<(), AdmissionFailure<C>> {
        let (observation, activation_id, dispatch_id) = {
            let admission = self
                .inner
                .admission
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            match *admission {
                EntityAdmission::Open => {}
                EntityAdmission::Closed => {
                    return Err(AdmissionFailure::Refused {
                        command,
                        reason: Refusal::Shutdown,
                    });
                }
            }
            let (publisher, observation) = affine_pair();
            let pending = PendingCommand {
                origin,
                command,
                publisher,
            };
            let dispatched = self
                .inner
                .directory
                .dispatch(entity_id.clone(), pending)
                .map_err(|error| match error {
                    DirectoryError::InvalidShardCount => {
                        unreachable!("configuration was validated")
                    }
                    DirectoryError::ActivationIdsExhausted(pending) => {
                        AdmissionFailure::ActivationIdsExhausted(pending.command)
                    }
                    DirectoryError::DispatchIdsExhausted(pending) => {
                        AdmissionFailure::DispatchIdsExhausted(pending.command)
                    }
                })?;
            let dispatch_id = dispatched.dispatch_id;
            let activation_id = dispatched.decision.activation_id;
            self.inner
                .directory
                .interpret(dispatched.decision, &self.inner);
            (observation, activation_id, dispatch_id)
        };
        DispatchWait {
            observation,
            entity_id,
            activation_id,
            dispatch_id,
            runtime: Arc::downgrade(&self.inner),
        }
        .await
    }

    /// Close admission, settle installed work, drain every represented exact
    /// incarnation, join every lifecycle task, and close the task group.
    ///
    /// # Panics
    ///
    /// Panics after synchronization poison or if an internal lifecycle law is
    /// violated by late task scheduling, an unmatched task completion, or a
    /// represented slot that survives the drain without a task failure.
    pub async fn shutdown(&self) -> EntityShutdown<R::TaskFailure> {
        {
            let mut admission = self
                .inner
                .admission
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            match *admission {
                EntityAdmission::Open => *admission = EntityAdmission::Closed,
                EntityAdmission::Closed => {}
            }
        }
        let Some(mut claim) = self.inner.tasks.claim() else {
            return EntityShutdown::AlreadyClaimed;
        };

        self.inner.tasks.wait_idle().await;
        let represented = match claim.drain_phase() {
            FamilyDrainPhase::NotStarted => {
                let drains = self.inner.directory.begin_family_drain();
                let represented = drains.len();
                for decision in drains {
                    self.inner.directory.interpret(decision, &self.inner);
                }
                claim.record_drain(represented);
                represented
            }
            FamilyDrainPhase::Started { represented } => represented,
        };
        self.inner.tasks.wait_idle().await;
        claim.take_tasks();
        for task in &mut claim.tasks {
            if let EntityTaskRecord::Running(handle) = task {
                *task = match R::join(handle).await {
                    Ok(()) => EntityTaskRecord::Joined,
                    Err(failure) => EntityTaskRecord::Failed(failure),
                };
            }
        }
        let settled = self.inner.directory.is_empty();
        let failures = claim.complete();
        if failures.is_empty() {
            assert!(settled, "settled entity family retained a lifecycle slot");
            EntityShutdown::Settled { represented }
        } else {
            EntityShutdown::TaskFailed {
                represented,
                failures,
            }
        }
    }

    /// Begin graceful passivation if the entity currently has an active incarnation.
    ///
    /// Admission closes at this call's lifecycle linearization point. Fence and
    /// retirement work continues in directory-owned tasks.
    ///
    /// # Panics
    ///
    /// Panics if directory synchronization was poisoned.
    pub fn passivate(&self, entity_id: &EntityId<I>) -> Passivation {
        let admission = self
            .inner
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if matches!(*admission, EntityAdmission::Closed) {
            return Passivation::ShuttingDown;
        }
        let Some(activation_id) = self.inner.directory.current_activation(entity_id) else {
            return Passivation::NotActive;
        };
        let decision = self.inner.directory.begin_drain(entity_id, activation_id);
        let passivation = match decision.evidence {
            TransitionEvidence::Traversed(LifecycleEdge::BeginDrain) => Passivation::Begun,
            TransitionEvidence::Traversed(_)
            | TransitionEvidence::SelfLoop
            | TransitionEvidence::Ignored => match decision.phase {
                LifecyclePhase::Active => Passivation::Superseded,
                LifecyclePhase::Draining | LifecyclePhase::Retiring => {
                    Passivation::AlreadyPassivating
                }
                LifecyclePhase::Inactive | LifecyclePhase::Activating => Passivation::NotActive,
            },
        };
        self.inner.directory.interpret(decision, &self.inner);
        passivation
    }
}

impl<I, C, R, Origin, Endpoint, Lease, Task, TaskFailure>
    EntityReceptionist<I, C, R, Origin, Endpoint, Lease, Task, TaskFailure>
where
    I: Clone + Eq + Hash + Send + Sync + 'static,
    C: Send + 'static,
    R: LocalEntityRuntime<
            I,
            C,
            Origin = Origin,
            Endpoint = Endpoint,
            Lease = Lease,
            Task = Task,
            TaskFailure = TaskFailure,
        >,
{
    pub(crate) async fn admit(
        &self,
        origin: Origin,
        entity_id: EntityId<I>,
        command: C,
    ) -> Result<(), AdmissionFailure<C>> {
        EntityRuntime {
            inner: Arc::clone(&self.inner),
        }
        .admit(origin, entity_id, command)
        .await
    }

    pub(crate) fn passivate(&self, entity_id: &EntityId<I>) -> Passivation {
        EntityRuntime {
            inner: Arc::clone(&self.inner),
        }
        .passivate(entity_id)
    }
}

struct DispatchWait<I, C, R>
where
    I: Clone + Eq + Hash + Send + Sync + 'static,
    C: Send + 'static,
    R: LocalEntityRuntime<I, C>,
{
    observation: AffineObservation<Result<(), AdmissionFailure<C>>>,
    entity_id: EntityId<I>,
    activation_id: Option<ActivationId>,
    dispatch_id: DispatchId,
    runtime: Weak<RuntimeState<I, C, R>>,
}

// The observation owns its pinned state in a heap-stable slot and is Unpin.
// No other field is projected as pinned or refers into this future.
impl<I, C, R> Unpin for DispatchWait<I, C, R>
where
    I: Clone + Eq + Hash + Send + Sync + 'static,
    C: Send + 'static,
    R: LocalEntityRuntime<I, C>,
{
}

impl<I, C, R> Future for DispatchWait<I, C, R>
where
    I: Clone + Eq + Hash + Send + Sync + 'static,
    C: Send + 'static,
    R: LocalEntityRuntime<I, C>,
{
    type Output = Result<(), AdmissionFailure<C>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let observation = Pin::new(&mut this.observation);
        match observation.poll(context) {
            Poll::Ready(result) => {
                this.activation_id.take();
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<I, C, R> Drop for DispatchWait<I, C, R>
where
    I: Clone + Eq + Hash + Send + Sync + 'static,
    C: Send + 'static,
    R: LocalEntityRuntime<I, C>,
{
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.upgrade()
            && let Some(activation_id) = self.activation_id.take()
        {
            let decision =
                runtime
                    .directory
                    .cancel_waiter(&self.entity_id, activation_id, self.dispatch_id);
            runtime.directory.interpret(decision, &runtime);
        }
    }
}

impl<I, C, R> EffectInterpreter<I, PendingCommand<R::Origin, C>, R::Endpoint, R::Lease>
    for Arc<RuntimeState<I, C, R>>
where
    I: Clone + Eq + Hash + Send + Sync + 'static,
    C: Send + 'static,
    R: LocalEntityRuntime<I, C>,
{
    fn start_activation(&self, entity_id: EntityId<I>, activation_id: ActivationId) {
        let runtime = Arc::clone(self);
        self.spawn_owned(async move {
            let result = runtime
                .port
                .activate(entity_id.clone(), activation_id)
                .await;
            let decision = match result {
                Ok(activated) => runtime.directory.activation_succeeded(
                    &entity_id,
                    activation_id,
                    activated.endpoint,
                    activated.lease,
                ),
                Err(error) => {
                    runtime
                        .port
                        .activation_failed(entity_id.clone(), activation_id, error);
                    runtime
                        .directory
                        .activation_failed(&entity_id, activation_id)
                }
            };
            runtime.directory.interpret(decision, &runtime);
        });
    }

    fn deliver(
        &self,
        entity_id: EntityId<I>,
        activation_id: ActivationId,
        dispatch_id: DispatchId,
        endpoint: R::Endpoint,
        pending: PendingCommand<R::Origin, C>,
    ) {
        let runtime = Arc::clone(self);
        self.spawn_owned(async move {
            let PendingCommand {
                origin,
                command,
                publisher,
            } = pending;
            let failure = match runtime
                .port
                .deliver(endpoint, origin.clone(), command)
                .await
            {
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
            let decision = runtime
                .directory
                .delivery_resolved(&entity_id, activation_id, failure);
            runtime.directory.interpret(decision, &runtime);
        });
    }

    fn reject(&self, _: DispatchId, pending: PendingCommand<R::Origin, C>, reason: Refusal) {
        pending.publisher.complete(Err(AdmissionFailure::Refused {
            command: pending.command,
            reason,
        }));
    }

    fn enqueue_fence(
        &self,
        entity_id: EntityId<I>,
        activation_id: ActivationId,
        endpoint: R::Endpoint,
    ) {
        let runtime = Arc::clone(self);
        self.spawn_owned(async move {
            let decision = match runtime.port.fence(endpoint).await {
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
            runtime.directory.interpret(decision, &runtime);
        });
    }

    fn retire(
        &self,
        entity_id: EntityId<I>,
        activation_id: ActivationId,
        lease: R::Lease,
        retirement: RetirementMode,
    ) {
        let runtime = Arc::clone(self);
        self.spawn_owned(async move {
            runtime
                .port
                .retire(entity_id.clone(), activation_id, lease, retirement)
                .await;
            let decision = runtime.directory.terminated(&entity_id, activation_id);
            runtime.directory.interpret(decision, &runtime);
        });
    }
}

impl<I, C, R> RuntimeState<I, C, R>
where
    I: Clone + Eq + Hash + Send + Sync + 'static,
    C: Send + 'static,
    R: LocalEntityRuntime<I, C>,
{
    fn spawn_owned(self: &Arc<Self>, task: impl Future<Output = ()> + Send + 'static) {
        let guard = self.tasks.begin();
        let scheduled = self.port.spawn(async move {
            let _guard = guard;
            task.await;
        });
        self.tasks.track(scheduled);
    }
}
