//! Concise asynchronous API over the local entity directory.

use core::future::Future;
use core::hash::Hash;
use core::mem;
use core::pin::Pin;
use core::task::{Context, Poll};
#[cfg(bombay_entity_loom)]
use loom::sync::Arc as DirectoryArc;
#[cfg(not(bombay_entity_loom))]
use std::sync::Arc as DirectoryArc;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use crate::observe::{AffineObservation, Observation, Publisher, affine_pair, pair};

use super::directory::{InstalledEffectSource, InstalledSlotDecision, Slot};
use super::{
    ActivationId, DirectoryConfig, DirectoryError, DispatchId, DrainFailure, DrainStage, EntityId,
    LifecycleEdge, LifecyclePhase, LocalDirectory, Refusal, RetirementMode, SlotEffect, SlotEvent,
    TransitionEvidence,
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
    /// Available failures after the exact actor task has been joined, including
    /// unavailable actor retirement or independently panicking application consumers.
    type RetirementFailure: Send + 'static;

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
    /// Resolving this future proves the exact incarnation task has ended and its
    /// lease is no longer actionable. An error may still report unavailable full
    /// actor retirement or application conversion/notification failures. Neither
    /// result alone asserts that otherwise unavailable descendants were joined.
    fn retire(
        &self,
        entity_id: &EntityId<I>,
        activation_id: ActivationId,
        lease: Self::Lease,
        retirement: RetirementMode,
    ) -> impl Future<Output = Result<(), Self::RetirementFailure>> + Send;
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

struct Runtime<I, C, R, Origin, Endpoint, Lease, Task, TaskFailure, RetirementFailure> {
    directory: LocalDirectory<I, PendingCommand<Origin, C>, Endpoint, Lease>,
    port: R,
    admission: Mutex<EntityAdmission>,
    #[expect(
        clippy::type_complexity,
        reason = "the task owner retains original family keys, slots and independent retirement failures"
    )]
    tasks: Arc<
        EntityTaskGroup<
            Task,
            TaskFailure,
            (
                DirectoryArc<EntityId<I>>,
                DirectoryArc<Slot<PendingCommand<Origin, C>, Endpoint, Lease>>,
            ),
            (
                DirectoryArc<EntityId<I>>,
                Option<DirectoryArc<Slot<PendingCommand<Origin, C>, Endpoint, Lease>>>,
                ActivationId,
                RetirementMode,
                RetirementFailure,
            ),
        >,
    >,
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
    <R as LocalEntityRuntime<I, C>>::RetirementFailure,
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
    <R as LocalEntityRuntime<I, C>>::RetirementFailure,
>;

pub(crate) struct EntityReceptionist<
    I,
    C,
    R,
    Origin,
    Endpoint,
    Lease,
    Task,
    TaskFailure,
    RetirementFailure,
> {
    #[expect(
        clippy::type_complexity,
        reason = "the private receptionist retains exact runtime task and join-failure types"
    )]
    inner: Arc<Runtime<I, C, R, Origin, Endpoint, Lease, Task, TaskFailure, RetirementFailure>>,
}

enum EntityAdmission {
    Open,
    Closed,
}

struct EntityTaskGroup<Task, TaskFailure, Family, Retirement> {
    state: Mutex<EntityTaskState<Task, TaskFailure, Family, Retirement>>,
}

enum EntityTaskState<Task, TaskFailure, Family, Retirement> {
    Open {
        active: usize,
        idle_epoch: Option<(Publisher<()>, Observation<()>)>,
        tasks: Vec<EntityTaskRecord<Task, TaskFailure>>,
        shutdown: ShutdownClaimPhase,
        drain: FamilyDrainPhase<Family>,
        retired: Vec<Family>,
        retirements: Vec<Retirement>,
    },
    Closed,
}

enum ShutdownClaimPhase {
    Available,
    Claimed,
}

enum FamilyDrainPhase<Family> {
    NotStarted,
    Started {
        represented: usize,
        rows: Vec<Family>,
    },
}

enum EntityTaskRecord<Task, TaskFailure> {
    Running(Task),
    Joined,
    Failed(TaskFailure),
}

struct EntityShutdownClaim<Task, TaskFailure, Family, Retirement> {
    group: Arc<EntityTaskGroup<Task, TaskFailure, Family, Retirement>>,
    tasks: Vec<EntityTaskRecord<Task, TaskFailure>>,
}

struct EntityTaskGuard<Task, TaskFailure, Family, Retirement> {
    group: Arc<EntityTaskGroup<Task, TaskFailure, Family, Retirement>>,
}

/// Exact result of one family shutdown request.
#[derive(Debug)]
pub enum EntityShutdown<TaskFailure, RetirementFailure, I> {
    /// All owned task joins completed; every original represented slot is inactive.
    Settled {
        represented: usize,
        entities: Vec<EntityId<I>>,
    },
    /// Another caller owns or already received this affine shutdown result.
    AlreadyClaimed,
    /// Available rows and actual failures; no destroyed state is reconstructed.
    Unsettled {
        represented: usize,
        #[expect(
            clippy::type_complexity,
            reason = "unsettled family rows retain original keys, activation disposition and retirement failures"
        )]
        entities: Vec<(
            EntityId<I>,
            Result<(), Option<ActivationId>>,
            Vec<(ActivationId, RetirementMode, RetirementFailure)>,
        )>,
        failures: Vec<TaskFailure>,
    },
}

impl<Task, TaskFailure, Family, Retirement> EntityTaskGroup<Task, TaskFailure, Family, Retirement> {
    fn new() -> Self {
        Self {
            state: Mutex::new(EntityTaskState::Open {
                active: 0,
                idle_epoch: None,
                tasks: Vec::new(),
                shutdown: ShutdownClaimPhase::Available,
                drain: FamilyDrainPhase::NotStarted,
                retired: Vec::new(),
                retirements: Vec::new(),
            }),
        }
    }

    fn begin(self: &Arc<Self>) -> EntityTaskGuard<Task, TaskFailure, Family, Retirement> {
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

    fn claim(
        self: &Arc<Self>,
    ) -> Option<EntityShutdownClaim<Task, TaskFailure, Family, Retirement>> {
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

    fn retirement_failed(&self, retirement: Retirement) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let EntityTaskState::Open { retirements, .. } = &mut *state else {
            panic!("retirement failure returned after task owner closed");
        };
        retirements.push(retirement);
    }

    fn retain_retired(&self, row: Family, matches: impl Fn(&Retirement) -> bool) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let EntityTaskState::Open {
            retired,
            retirements,
            ..
        } = &mut *state
        else {
            panic!("original family row removed after task owner closed");
        };
        // A successful live notification explicitly discharged its original key.
        // Only still-owned failure custody keeps a removed live row here.
        if retirements.iter().any(matches) {
            retired.push(row);
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

impl<Task, TaskFailure, Family, Retirement>
    EntityShutdownClaim<Task, TaskFailure, Family, Retirement>
{
    fn record_drain(&self, take: impl FnOnce() -> Vec<Family>) {
        let mut state = self
            .group
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let EntityTaskState::Open { drain, .. } = &mut *state else {
            panic!("family drain requested after shutdown");
        };
        if matches!(drain, FamilyDrainPhase::NotStarted) {
            let rows = take();
            *drain = FamilyDrainPhase::Started {
                represented: rows.len(),
                rows,
            };
        }
    }

    fn rows(&self, copy: impl Fn(&Family) -> Family) -> Vec<Family> {
        let state = self
            .group
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let EntityTaskState::Open {
            drain: FamilyDrainPhase::Started { rows, .. },
            ..
        } = &*state
        else {
            panic!("family rows requested before drain");
        };
        rows.iter().map(copy).collect()
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

    fn complete(mut self) -> (usize, Vec<Family>, Vec<Retirement>, Vec<TaskFailure>) {
        let mut failures = Vec::new();
        for task in self.tasks.drain(..) {
            match task {
                EntityTaskRecord::Failed(failure) => failures.push(failure),
                EntityTaskRecord::Joined => {}
                EntityTaskRecord::Running(_) => panic!("unjoined lifecycle task at shutdown"),
            }
        }
        let (represented, rows, retirements) = {
            let mut state = self
                .group
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let EntityTaskState::Open {
                drain,
                retired,
                retirements,
                ..
            } = &mut *state
            else {
                panic!("family shutdown completed twice");
            };
            let FamilyDrainPhase::Started {
                represented,
                mut rows,
            } = mem::replace(drain, FamilyDrainPhase::NotStarted)
            else {
                panic!("family shutdown without owned rows");
            };
            rows.append(retired);
            (represented, rows, mem::take(retirements))
        };
        self.group.close();
        (represented, rows, retirements, failures)
    }
}

impl<Task, TaskFailure, Family, Retirement> Drop
    for EntityShutdownClaim<Task, TaskFailure, Family, Retirement>
{
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

impl<Task, TaskFailure, Family, Retirement> Drop
    for EntityTaskGuard<Task, TaskFailure, Family, Retirement>
{
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

impl<I, C, R, Origin, Endpoint, Lease, Task, TaskFailure, RetirementFailure> Clone
    for EntityReceptionist<I, C, R, Origin, Endpoint, Lease, Task, TaskFailure, RetirementFailure>
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
            self.inner.interpret(dispatched.decision);
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
    #[expect(
        clippy::too_many_lines,
        reason = "one affine shutdown claim joins all owned tasks before consuming original family rows"
    )]
    pub async fn shutdown(&self) -> EntityShutdown<R::TaskFailure, R::RetirementFailure, I> {
        {
            let mut admission = self
                .inner
                .admission
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            *admission = EntityAdmission::Closed;
        }
        let Some(mut claim) = self.inner.tasks.claim() else {
            return EntityShutdown::AlreadyClaimed;
        };
        self.inner.tasks.wait_idle().await;
        self.join_owned(&mut claim).await;
        claim.record_drain(|| self.inner.directory.take_family());

        let rows = claim.rows(|(id, slot)| (DirectoryArc::clone(id), DirectoryArc::clone(slot)));
        // Closed-family transitions consume available leases without user clones.
        // Retained original effect leaves remain in each slot until interpreted.
        for (id, slot) in &rows {
            let retirement = slot.close_family();
            self.inner
                .interpret_closed_slot(DirectoryArc::clone(id), DirectoryArc::clone(slot));
            if let Some((activation, endpoint, lease, fence)) = retirement {
                let runtime = Arc::clone(&self.inner);
                let id = DirectoryArc::clone(id);
                let slot = DirectoryArc::clone(slot);
                self.inner.spawn_owned(async move {
                    let mode = match fence {
                        Ok(()) => match runtime.port.fence(endpoint).await {
                            Ok(()) => RetirementMode::Graceful,
                            Err(failure) => RetirementMode::Forced(DrainFailure {
                                stage: match failure {
                                    FenceFailure::Enqueue => DrainStage::FenceEnqueue,
                                    FenceFailure::Acknowledgement => {
                                        DrainStage::FenceAcknowledgement
                                    }
                                },
                                outstanding_reservations: 0,
                            }),
                        },
                        Err(failure) => RetirementMode::Forced(failure),
                    };
                    runtime
                        .retire_owned(id, slot, activation, lease, mode)
                        .await;
                });
            }
        }
        loop {
            self.inner.tasks.wait_idle().await;
            self.join_owned(&mut claim).await;
            if !rows.iter().any(|(_, slot)| slot.has_pending()) {
                break;
            }
            for (id, slot) in &rows {
                self.inner
                    .interpret_closed_slot(DirectoryArc::clone(id), DirectoryArc::clone(slot));
            }
        }
        drop(rows);
        let (represented, rows, mut retirements, failures) = claim.complete();
        let mut entities = Vec::new();
        for (id, slot) in rows {
            let disposition = slot.retirement_disposition();
            let receipts = retirements
                .extract_if(.., |(_, original, _, _, _)| {
                    original
                        .as_ref()
                        .is_some_and(|original| DirectoryArc::ptr_eq(original, &slot))
                })
                .map(|(_, _, activation, mode, failure)| (activation, mode, failure))
                .collect();
            // No reference to a key escapes a completed lifecycle task or callback.
            let Ok(id) = DirectoryArc::try_unwrap(id) else {
                panic!("original family key still owned after all task joins");
            };
            entities.push((id, disposition, receipts));
        }
        for (id, slot, activation, mode, failure) in retirements {
            assert!(
                slot.is_none(),
                "mapped retirement failure lost its original family row"
            );
            let Ok(id) = DirectoryArc::try_unwrap(id) else {
                panic!("transient retirement key still owned after task joins");
            };
            entities.push((id, Ok(()), vec![(activation, mode, failure)]));
        }
        if failures.is_empty()
            && entities
                .iter()
                .all(|(_, disposition, receipts)| disposition.is_ok() && receipts.is_empty())
        {
            EntityShutdown::Settled {
                represented,
                entities: entities.into_iter().map(|(id, _, _)| id).collect(),
            }
        } else {
            EntityShutdown::Unsettled {
                represented,
                entities,
                failures,
            }
        }
    }

    #[expect(
        clippy::type_complexity,
        reason = "the shutdown claim owns original task failures, family rows and retirement failures"
    )]
    async fn join_owned(
        &self,
        claim: &mut EntityShutdownClaim<
            R::Task,
            R::TaskFailure,
            (
                DirectoryArc<EntityId<I>>,
                DirectoryArc<Slot<PendingCommand<R::Origin, C>, R::Endpoint, R::Lease>>,
            ),
            (
                DirectoryArc<EntityId<I>>,
                Option<DirectoryArc<Slot<PendingCommand<R::Origin, C>, R::Endpoint, R::Lease>>>,
                ActivationId,
                RetirementMode,
                R::RetirementFailure,
            ),
        >,
    ) {
        claim.take_tasks();
        for task in &mut claim.tasks {
            if let EntityTaskRecord::Running(handle) = task {
                *task = match R::join(handle).await {
                    Ok(()) => EntityTaskRecord::Joined,
                    Err(failure) => EntityTaskRecord::Failed(failure),
                };
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
        self.inner.interpret(decision);
        passivation
    }
}

impl<I, C, R, Origin, Endpoint, Lease, Task, TaskFailure, RetirementFailure>
    EntityReceptionist<I, C, R, Origin, Endpoint, Lease, Task, TaskFailure, RetirementFailure>
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
            RetirementFailure = RetirementFailure,
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
            let admission = runtime
                .admission
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if matches!(*admission, EntityAdmission::Closed) {
                return;
            }
            let decision =
                runtime
                    .directory
                    .cancel_waiter(&self.entity_id, activation_id, self.dispatch_id);
            runtime.interpret(decision);
        }
    }
}

impl<I, C, R> RuntimeState<I, C, R>
where
    I: Clone + Eq + Hash + Send + Sync + 'static,
    C: Send + 'static,
    R: LocalEntityRuntime<I, C>,
{
    fn interpret(
        self: &Arc<Self>,
        decision: InstalledSlotDecision<I, PendingCommand<R::Origin, C>, R::Endpoint, R::Lease>,
    ) {
        let InstalledSlotDecision {
            entity_id, target, ..
        } = decision;
        let id = DirectoryArc::new(entity_id);
        match target {
            InstalledEffectSource::Mapped(slot) => self.interpret_slot(id, slot),
            InstalledEffectSource::Transient(effects) => {
                for effect in effects {
                    self.interpret_effect(DirectoryArc::clone(&id), None, effect);
                }
            }
        }
    }

    #[expect(
        clippy::type_complexity,
        reason = "slot dispatch retains the exact command origin, endpoint and lease types"
    )]
    fn interpret_slot(
        self: &Arc<Self>,
        id: DirectoryArc<EntityId<I>>,
        slot: DirectoryArc<Slot<PendingCommand<R::Origin, C>, R::Endpoint, R::Lease>>,
    ) {
        slot.dispatch_pending(&|effect| {
            self.interpret_effect(
                DirectoryArc::clone(&id),
                Some(DirectoryArc::clone(&slot)),
                effect,
            );
        });
        drop(slot);
        drop(id);
    }

    #[expect(
        clippy::type_complexity,
        reason = "closed slot dispatch retains the exact command origin, endpoint and lease types"
    )]
    fn interpret_closed_slot(
        self: &Arc<Self>,
        id: DirectoryArc<EntityId<I>>,
        slot: DirectoryArc<Slot<PendingCommand<R::Origin, C>, R::Endpoint, R::Lease>>,
    ) {
        slot.dispatch_pending(&|effect| {
            match effect {
                SlotEffect::StartActivation { .. } => {
                    // close_family explicitly returned all original unstarted waiters.
                }
                effect => self.interpret_effect(
                    DirectoryArc::clone(&id),
                    Some(DirectoryArc::clone(&slot)),
                    effect,
                ),
            }
        });
        drop(slot);
        drop(id);
    }

    #[expect(
        clippy::type_complexity,
        clippy::too_many_lines,
        reason = "one exhaustive slot effect interpretation preserves each original command, lease and task owner"
    )]
    fn interpret_effect(
        self: &Arc<Self>,
        id: DirectoryArc<EntityId<I>>,
        slot: Option<DirectoryArc<Slot<PendingCommand<R::Origin, C>, R::Endpoint, R::Lease>>>,
        effect: SlotEffect<PendingCommand<R::Origin, C>, R::Endpoint, R::Lease>,
    ) {
        match effect {
            SlotEffect::StartActivation { activation_id } => {
                let runtime = Arc::clone(self);
                self.spawn_owned(async move {
                    let result = runtime.port.activate((*id).clone(), activation_id).await;
                    let Some(slot) = slot else {
                        panic!("activation without installed slot")
                    };
                    match result {
                        Ok(activated) => {
                            slot.submit_owned(SlotEvent::ActivationSucceeded {
                                activation_id,
                                endpoint: activated.endpoint,
                                lease: activated.lease,
                            });
                            runtime.interpret_slot(id, slot);
                        }
                        Err(error) => {
                            // Install all original command returns before a consuming
                            // user diagnostic callback can fail.
                            slot.submit_owned(SlotEvent::ActivationFailed { activation_id });
                            runtime.interpret_slot(DirectoryArc::clone(&id), slot);
                            runtime
                                .port
                                .activation_failed((*id).clone(), activation_id, error);
                        }
                    }
                });
            }
            SlotEffect::Deliver {
                activation_id,
                dispatch_id,
                endpoint,
                command: pending,
            } => {
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
                    if let Some(slot) = slot {
                        slot.submit_owned(SlotEvent::DeliveryResolved {
                            activation_id,
                            failure,
                        });
                        runtime.interpret_slot(id, slot);
                    } else if let Some((dispatch_id, pending)) = failure {
                        Self::reject(dispatch_id, pending, Refusal::Unavailable);
                    }
                });
            }
            SlotEffect::Reject {
                dispatch_id,
                command,
                reason,
            } => Self::reject(dispatch_id, command, reason),
            SlotEffect::EnqueueFence {
                activation_id,
                endpoint,
            } => {
                let runtime = Arc::clone(self);
                self.spawn_owned(async move {
                    let event = match runtime.port.fence(endpoint).await {
                        Ok(()) => SlotEvent::FenceAcknowledged { activation_id },
                        Err(failure) => SlotEvent::ForceDrain {
                            activation_id,
                            failure: DrainFailure {
                                stage: match failure {
                                    FenceFailure::Enqueue => DrainStage::FenceEnqueue,
                                    FenceFailure::Acknowledgement => {
                                        DrainStage::FenceAcknowledgement
                                    }
                                },
                                outstanding_reservations: 0,
                            },
                        },
                    };
                    if let Some(slot) = slot {
                        slot.submit_owned(event);
                        runtime.interpret_slot(id, slot);
                    }
                });
            }
            SlotEffect::Retire {
                activation_id,
                lease,
                retirement,
            } => {
                let runtime = Arc::clone(self);
                self.spawn_owned(async move {
                    if let Some(slot) = slot {
                        runtime
                            .retire_owned(id, slot, activation_id, lease, retirement)
                            .await;
                    } else {
                        let result = runtime
                            .port
                            .retire(&id, activation_id, lease, retirement)
                            .await;
                        if let Err(failure) = result {
                            runtime.tasks.retirement_failed((
                                id,
                                None,
                                activation_id,
                                retirement,
                                failure,
                            ));
                        }
                    }
                });
            }
            SlotEffect::Remove { .. } => {
                if let Some(slot) = slot
                    && let Some(row) = self.directory.take_slot(&slot)
                {
                    self.tasks.retain_retired(row, |(_, original, _, _, _)| {
                        original
                            .as_ref()
                            .is_some_and(|original| DirectoryArc::ptr_eq(original, &slot))
                    });
                }
            }
        }
    }

    fn reject(_: DispatchId, pending: PendingCommand<R::Origin, C>, reason: Refusal) {
        pending.publisher.complete(Err(AdmissionFailure::Refused {
            command: pending.command,
            reason,
        }));
    }

    #[expect(
        clippy::type_complexity,
        reason = "retirement retains the original key and exact slot through the joined actor result"
    )]
    async fn retire_owned(
        self: &Arc<Self>,
        id: DirectoryArc<EntityId<I>>,
        slot: DirectoryArc<Slot<PendingCommand<R::Origin, C>, R::Endpoint, R::Lease>>,
        activation: ActivationId,
        lease: R::Lease,
        mode: RetirementMode,
    ) {
        let result = self.port.retire(&id, activation, lease, mode).await;
        if let Err(failure) = result {
            self.tasks.retirement_failed((
                DirectoryArc::clone(&id),
                Some(DirectoryArc::clone(&slot)),
                activation,
                mode,
                failure,
            ));
        }
        slot.submit_owned(SlotEvent::Terminated {
            activation_id: activation,
        });
        self.interpret_slot(id, slot);
    }

    fn spawn_owned(self: &Arc<Self>, task: impl Future<Output = ()> + Send + 'static) {
        let guard = self.tasks.begin();
        let scheduled = self.port.spawn(async move {
            let _guard = guard;
            task.await;
        });
        self.tasks.track(scheduled);
    }
}
