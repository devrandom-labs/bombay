//! Concurrent storage for local entity lifecycle machines.

#[cfg(bombay_entity_loom)]
use loom::sync::atomic::{AtomicU64, Ordering};
#[cfg(bombay_entity_loom)]
use loom::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, RandomState};
use std::num::{NonZeroU64, NonZeroUsize};
#[cfg(not(bombay_entity_loom))]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(bombay_entity_loom))]
use std::sync::{Arc, Mutex};

use super::{
    ActivationId, DispatchId, DrainFailure, EntityId, LifecycleMachine, LifecycleOutput, Refusal,
    RetirementMode, SlotEffect, SlotEvent, TransitionEvidence, lifecycle_machine,
};
use bombay_machine::Machine;
use bombay_machine::executor::{LinearizedExecutor, OutputEvidence};

/// Fixed sizing and admission limits for a local directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectoryConfig {
    /// Number of independently locked map shards; must be a power of two.
    pub shards: NonZeroUsize,
    /// Maximum commands retained by one in-flight activation.
    pub activation_waiters: NonZeroUsize,
}

impl Default for DirectoryConfig {
    fn default() -> Self {
        Self {
            shards: NonZeroUsize::new(64).expect("64 is non-zero"),
            activation_waiters: NonZeroUsize::new(64).expect("64 is non-zero"),
        }
    }
}

/// Failure before a command can be submitted to a lifecycle machine.
#[derive(Debug, thiserror::Error)]
pub enum DirectoryError<C> {
    /// The shard count was not a power of two.
    #[error("shard count was not a power of two")]
    InvalidShardCount,
    /// The monotonically increasing activation namespace is exhausted.
    #[error("activation identity namespace is exhausted")]
    ActivationIdsExhausted(C),
    /// The monotonically increasing dispatch namespace is exhausted.
    #[error("dispatch identity namespace is exhausted")]
    DispatchIdsExhausted(C),
}

/// Runtime capabilities used to interpret lifecycle effects.
///
/// Methods run without directory or slot synchronization held. Asynchronous
/// implementations should schedule owned work and later submit its typed fact
/// through the directory. Calls for one slot occur in declared effect order.
pub trait EffectInterpreter<I, C, E, L> {
    /// Start one directory-owned activation attempt.
    fn start_activation(&self, entity_id: EntityId<I>, activation_id: ActivationId);
    /// Start delivery to one exact-incarnation endpoint.
    fn deliver(
        &self,
        entity_id: EntityId<I>,
        activation_id: ActivationId,
        dispatch_id: DispatchId,
        endpoint: E,
        command: C,
    );
    /// Return a command that was not admitted or delivered.
    fn reject(&self, dispatch_id: DispatchId, command: C, reason: Refusal);
    /// Enqueue an ordered processing fence for one exact incarnation.
    fn enqueue_fence(&self, entity_id: EntityId<I>, activation_id: ActivationId, endpoint: E);
    /// Exercise exact-incarnation retirement authority.
    fn retire(
        &self,
        entity_id: EntityId<I>,
        activation_id: ActivationId,
        lease: L,
        retirement: RetirementMode,
    );
}

/// One installed lifecycle decision awaiting effect interpretation.
#[must_use = "installed lifecycle decision awaits effect interpretation via LocalDirectory::interpret"]
pub struct DirectoryOutput<I, C, E, L> {
    /// Checked evidence for the installed lifecycle decision.
    pub evidence: TransitionEvidence,
    pub(crate) activation_id: Option<ActivationId>,
    entity_id: EntityId<I>,
    target: OutputTarget<C, E, L>,
}

/// Where an installed decision's effects await interpretation.
enum OutputTarget<C, E, L> {
    /// A directory-mapped slot with queued effects.
    Mapped(Arc<Slot<C, E, L>>),
    /// Concrete ordered effects for an event addressed to an absent entry.
    /// Empty batches retain no allocation; owned stale payloads allocate only
    /// on this cold path and remain statically typed.
    Transient(Vec<SlotEffect<C, E, L>>),
}

/// The installed decision for one dispatched command, with its guaranteed
/// correlation identity.
#[must_use = "installed lifecycle decision awaits effect interpretation via LocalDirectory::interpret"]
pub struct DispatchOutput<I, C, E, L> {
    /// Correlation identity allocated for the dispatched command.
    pub dispatch_id: DispatchId,
    /// Installed decision awaiting effect interpretation.
    pub output: DirectoryOutput<I, C, E, L>,
}

struct Slot<C, E, L> {
    lifecycle: LinearizedExecutor<LifecycleMachine<C, E, L>>,
}

struct Installed {
    evidence: TransitionEvidence,
    activation_id: Option<ActivationId>,
}

impl<C, E: Clone, L> Slot<C, E, L> {
    fn new() -> Self {
        Self {
            lifecycle: LinearizedExecutor::new(lifecycle_machine()),
        }
    }

    fn submit(&self, event: SlotEvent<C, E, L>) -> Installed {
        let (evidence, activation_id) = self.lifecycle.submit(event);
        Installed {
            evidence,
            activation_id,
        }
    }

    fn activation_id(&self) -> Option<ActivationId> {
        self.lifecycle.evidence().and_then(|evidence| evidence.1)
    }
}

type Shard<I, C, E, L> = Mutex<HashMap<EntityId<I>, Arc<Slot<C, E, L>>>>;

/// Sharded local storage for authoritative per-entity lifecycle machines.
///
/// Hashing first selects a shard before locking; [`Hash`] and [`Eq`] for `I`
/// then run during table operations while that shard lock is held and therefore
/// must not reenter this directory. Lifecycle effect callbacks run only after
/// directory synchronization is released.
pub struct LocalDirectory<I, C, E, L, S = RandomState> {
    shards: Vec<Shard<I, C, E, L>>,
    mask: u64,
    hash_builder: S,
    waiter_limit: NonZeroUsize,
    next_activation: AtomicU64,
    next_dispatch: AtomicU64,
}

impl<I, C, E, L> LocalDirectory<I, C, E, L>
where
    I: Eq + Hash + Clone,
    E: Clone,
{
    /// Construct a directory using the standard randomized hash builder.
    ///
    /// # Errors
    ///
    /// Returns [`DirectoryError::InvalidShardCount`] unless the configured
    /// shard count is a power of two.
    pub fn new(config: DirectoryConfig) -> Result<Self, DirectoryError<C>> {
        Self::with_hasher(config, RandomState::new())
    }
}

impl<I, C, E, L, S> LocalDirectory<I, C, E, L, S>
where
    I: Eq + Hash + Clone,
    E: Clone,
    S: BuildHasher,
{
    /// Construct a directory with an explicit hash builder.
    ///
    /// # Errors
    ///
    /// Returns [`DirectoryError::InvalidShardCount`] unless the configured
    /// shard count is a power of two.
    pub fn with_hasher(
        config: DirectoryConfig,
        hash_builder: S,
    ) -> Result<Self, DirectoryError<C>> {
        if !config.shards.get().is_power_of_two() {
            return Err(DirectoryError::InvalidShardCount);
        }
        let mask = config
            .shards
            .get()
            .checked_sub(1)
            .and_then(|count| u64::try_from(count).ok())
            .ok_or(DirectoryError::InvalidShardCount)?;
        let shards = (0..config.shards.get())
            .map(|_| Mutex::new(HashMap::new()))
            .collect();
        Ok(Self {
            shards,
            mask,
            hash_builder,
            waiter_limit: config.activation_waiters,
            next_activation: AtomicU64::new(1),
            next_dispatch: AtomicU64::new(1),
        })
    }

    /// Dispatch a command through the stable slot for `entity_id`.
    ///
    /// The first caller atomically installs an activating machine. Concurrent
    /// callers use that same slot and bounded activation attempt.
    ///
    /// # Errors
    ///
    /// Returns the original command if either identity namespace is exhausted.
    ///
    /// # Panics
    ///
    /// Panics after a synchronization poison, which cannot be recovered without
    /// risking lifecycle corruption.
    pub fn dispatch(
        &self,
        entity_id: EntityId<I>,
        command: C,
    ) -> Result<DispatchOutput<I, C, E, L>, DirectoryError<C>> {
        let Some(dispatch_sequence) = allocate(&self.next_dispatch) else {
            return Err(DirectoryError::DispatchIdsExhausted(command));
        };
        let dispatch_id = DispatchId::new(dispatch_sequence);
        let shard = &self.shards[self.shard_index(&entity_id)];
        let mut entries = shard.lock().expect("directory shard lock poisoned");
        if let Some(slot) = entries.get(&entity_id) {
            let slot = Arc::clone(slot);
            drop(entries);
            let installed = slot.submit(SlotEvent::Dispatch {
                dispatch_id,
                command,
            });
            return Ok(DispatchOutput {
                dispatch_id,
                output: directory_output(
                    entity_id,
                    installed.evidence,
                    installed.activation_id,
                    OutputTarget::Mapped(slot),
                ),
            });
        }
        let Some(activation_sequence) = allocate(&self.next_activation) else {
            return Err(DirectoryError::ActivationIdsExhausted(command));
        };
        let activation_id = ActivationId::new(activation_sequence);
        let slot = Arc::new(Slot::new());
        let installed = slot.submit(SlotEvent::ClaimActivation {
            activation_id,
            dispatch_id,
            command,
            waiter_limit: self.waiter_limit,
        });
        entries.insert(entity_id.clone(), Arc::clone(&slot));
        drop(entries);
        Ok(DispatchOutput {
            dispatch_id,
            output: directory_output(
                entity_id,
                installed.evidence,
                installed.activation_id,
                OutputTarget::Mapped(slot),
            ),
        })
    }

    /// Submit successful exact-incarnation activation to the represented slot.
    pub fn activation_succeeded(
        &self,
        entity_id: &EntityId<I>,
        activation_id: ActivationId,
        endpoint: E,
        lease: L,
    ) -> DirectoryOutput<I, C, E, L> {
        let event = SlotEvent::ActivationSucceeded {
            activation_id,
            endpoint,
            lease,
        };
        self.submit_or_inactive(entity_id, event)
    }

    /// Submit a failed activation attempt to the represented slot.
    pub fn activation_failed(
        &self,
        entity_id: &EntityId<I>,
        activation_id: ActivationId,
    ) -> DirectoryOutput<I, C, E, L> {
        self.submit_or_inactive(entity_id, SlotEvent::ActivationFailed { activation_id })
    }

    /// Cancel one bounded activation waiter.
    pub fn cancel_waiter(
        &self,
        entity_id: &EntityId<I>,
        activation_id: ActivationId,
        dispatch_id: DispatchId,
    ) -> DirectoryOutput<I, C, E, L> {
        self.submit_or_inactive(
            entity_id,
            SlotEvent::CancelWaiter {
                activation_id,
                dispatch_id,
            },
        )
    }

    /// Resolve one previously admitted exact-incarnation delivery.
    pub fn delivery_resolved(
        &self,
        entity_id: &EntityId<I>,
        activation_id: ActivationId,
        failure: Option<(DispatchId, C)>,
    ) -> DirectoryOutput<I, C, E, L> {
        self.submit_or_inactive(
            entity_id,
            SlotEvent::DeliveryResolved {
                activation_id,
                failure,
            },
        )
    }

    /// Atomically close admission for an exact active incarnation.
    pub fn begin_drain(
        &self,
        entity_id: &EntityId<I>,
        activation_id: ActivationId,
    ) -> DirectoryOutput<I, C, E, L> {
        self.submit_or_inactive(entity_id, SlotEvent::BeginDrain { activation_id })
    }

    /// Submit acknowledgement of the ordered processing fence.
    pub fn fence_acknowledged(
        &self,
        entity_id: &EntityId<I>,
        activation_id: ActivationId,
    ) -> DirectoryOutput<I, C, E, L> {
        self.submit_or_inactive(entity_id, SlotEvent::FenceAcknowledged { activation_id })
    }

    /// Force a bounded drain to retirement with its exact failure stage.
    pub fn force_drain(
        &self,
        entity_id: &EntityId<I>,
        activation_id: ActivationId,
        failure: DrainFailure,
    ) -> DirectoryOutput<I, C, E, L> {
        self.submit_or_inactive(
            entity_id,
            SlotEvent::ForceDrain {
                activation_id,
                failure,
            },
        )
    }

    /// Submit an exact-incarnation termination observation.
    pub fn terminated(
        &self,
        entity_id: &EntityId<I>,
        activation_id: ActivationId,
    ) -> DirectoryOutput<I, C, E, L> {
        self.submit_or_inactive(entity_id, SlotEvent::Terminated { activation_id })
    }

    /// Return the number of represented stable routing slots.
    ///
    /// # Panics
    ///
    /// Panics if a directory shard lock was poisoned.
    #[must_use]
    pub fn len(&self) -> usize {
        // The sum cannot overflow: every counted entry is a live heap
        // allocation, so the total is bounded far below usize::MAX.
        self.shards
            .iter()
            .map(|shard| shard.lock().expect("directory shard lock poisoned").len())
            .sum()
    }

    /// Return whether no stable routing slots are represented.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Install a drain request for every currently represented incarnation.
    ///
    /// The owning [`super::EntityRuntime`] closes family admission and settles
    /// activation and delivery tasks before calling this operation. Snapshot
    /// entries are addressed by their exact activation identity, so a stale
    /// output cannot drain a replacement incarnation.
    pub(crate) fn begin_family_drain(&self) -> Vec<DirectoryOutput<I, C, E, L>> {
        let represented = self
            .shards
            .iter()
            .flat_map(|shard| {
                shard
                    .lock()
                    .expect("directory shard lock poisoned")
                    .iter()
                    .filter_map(|(entity_id, slot)| {
                        slot.activation_id()
                            .map(|activation_id| (entity_id.clone(), activation_id))
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        represented
            .into_iter()
            .map(|(entity_id, activation_id)| self.begin_drain(&entity_id, activation_id))
            .collect()
    }

    pub(crate) fn current_activation(&self, entity_id: &EntityId<I>) -> Option<ActivationId> {
        self.shards[self.shard_index(entity_id)]
            .lock()
            .expect("directory shard lock poisoned")
            .get(entity_id)
            .cloned()
            .and_then(|slot| slot.activation_id())
    }

    fn submit_or_inactive(
        &self,
        entity_id: &EntityId<I>,
        event: SlotEvent<C, E, L>,
    ) -> DirectoryOutput<I, C, E, L> {
        let slot = self.shards[self.shard_index(entity_id)]
            .lock()
            .expect("directory shard lock poisoned")
            .get(entity_id)
            .cloned();
        if let Some(slot) = slot {
            let installed = slot.submit(event);
            return directory_output(
                entity_id.clone(),
                installed.evidence,
                installed.activation_id,
                OutputTarget::Mapped(slot),
            );
        }
        // No entry: reduce the event on a stack-resident machine instead of
        // allocating a slot that would be discarded after interpretation.
        submit_absent(entity_id.clone(), event)
    }

    /// Interpret all effects queued for the output's stable slot.
    ///
    /// A concurrent or reentrant call only contributes its already-queued
    /// effects; the current interpreter retains ownership until the queue is
    /// empty. Runtime callbacks execute without directory synchronization held.
    pub fn interpret<R>(&self, output: DirectoryOutput<I, C, E, L>, runtime: &R)
    where
        R: EffectInterpreter<I, C, E, L>,
    {
        let DirectoryOutput {
            entity_id, target, ..
        } = output;
        match target {
            OutputTarget::Mapped(slot) => {
                slot.lifecycle
                    .dispatch_pending(&|output: LifecycleOutput<C, E, L>| {
                        output.effects.for_each(|effect| {
                            self.apply_effect(&entity_id, Some(&slot), effect, runtime);
                        });
                    });
            }
            OutputTarget::Transient(effects) => {
                for effect in effects {
                    self.apply_effect(&entity_id, None, effect, runtime);
                }
            }
        }
    }

    #[inline]
    fn apply_effect<R>(
        &self,
        entity_id: &EntityId<I>,
        slot: Option<&Arc<Slot<C, E, L>>>,
        effect: SlotEffect<C, E, L>,
        runtime: &R,
    ) where
        R: EffectInterpreter<I, C, E, L>,
    {
        match effect {
            SlotEffect::StartActivation { activation_id } => {
                runtime.start_activation(entity_id.clone(), activation_id);
            }
            SlotEffect::Deliver {
                activation_id,
                dispatch_id,
                endpoint,
                command,
            } => runtime.deliver(
                entity_id.clone(),
                activation_id,
                dispatch_id,
                endpoint,
                command,
            ),
            SlotEffect::Reject {
                dispatch_id,
                command,
                reason,
            } => runtime.reject(dispatch_id, command, reason),
            SlotEffect::EnqueueFence {
                activation_id,
                endpoint,
            } => runtime.enqueue_fence(entity_id.clone(), activation_id, endpoint),
            SlotEffect::Retire {
                activation_id,
                lease,
                retirement,
            } => runtime.retire(entity_id.clone(), activation_id, lease, retirement),
            // A mapped slot never acquires a second activation, so pointer
            // identity is the exact removal authority. A transient decision
            // belongs to no mapped slot, so its Remove cannot match anything.
            SlotEffect::Remove { .. } => {
                if let Some(slot) = slot {
                    self.remove_matching(entity_id, slot);
                }
            }
        }
    }

    fn remove_matching(&self, entity_id: &EntityId<I>, slot: &Arc<Slot<C, E, L>>) {
        let mut entries = self.shards[self.shard_index(entity_id)]
            .lock()
            .expect("directory shard lock poisoned");
        if entries
            .get(entity_id)
            .is_some_and(|stored| Arc::ptr_eq(stored, slot))
        {
            entries.remove(entity_id);
        }
    }

    fn shard_index(&self, entity_id: &EntityId<I>) -> usize {
        // The mask derives from the shard count, so the masked hash always
        // fits the pointer width that allocated the shards.
        usize::try_from(self.hash_builder.hash_one(entity_id) & self.mask)
            .expect("masked hash fits usize")
    }
}

impl<C, E, L> OutputEvidence for LifecycleOutput<C, E, L> {
    type Evidence = (TransitionEvidence, Option<ActivationId>);

    fn evidence(&self) -> Self::Evidence {
        (self.evidence, self.activation_id)
    }
}

/// Allocate the next non-zero identity from a monotonic sequence.
///
/// `Relaxed` suffices by structural argument: the only requirement is
/// uniqueness, which the atomic read-modify-write modification order already
/// guarantees for any ordering. No non-atomic state is published through these
/// counters — every identity is an opaque token, and all slot state it later
/// names is synchronized by the shard and executor mutexes instead.
fn allocate(sequence: &AtomicU64) -> Option<NonZeroU64> {
    sequence
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .ok()
        .and_then(NonZeroU64::new)
}

/// Reduce an event addressed to an absent entry on a stack-resident machine.
#[cold]
#[inline(never)]
fn submit_absent<I, C, E: Clone, L>(
    entity_id: EntityId<I>,
    event: SlotEvent<C, E, L>,
) -> DirectoryOutput<I, C, E, L> {
    let (output, _) = lifecycle_machine().step(event);
    directory_output(
        entity_id,
        output.evidence,
        output.activation_id,
        OutputTarget::Transient(output.effects.into_vec()),
    )
}

fn directory_output<I, C, E, L>(
    entity_id: EntityId<I>,
    evidence: TransitionEvidence,
    activation_id: Option<ActivationId>,
    target: OutputTarget<C, E, L>,
) -> DirectoryOutput<I, C, E, L> {
    DirectoryOutput {
        evidence,
        activation_id,
        entity_id,
        target,
    }
}
