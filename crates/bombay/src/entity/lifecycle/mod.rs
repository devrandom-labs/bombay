//! Pure lifecycle algebra for one stable entity routing slot.

use core::cmp::Ordering;
use core::num::{NonZeroU64, NonZeroUsize};
use core::ops::Add;

use bombay_machine::{Decision, Reducer};

mod inactive;
mod machine;
mod retiring;

use inactive::decide_inactive;
use retiring::decide_retiring;

pub use machine::{
    LIFECYCLE_TOPOLOGY, LifecycleEdge, LifecycleMachine, LifecycleOutput, LifecyclePhase,
    LifecycleTopologyError, LifecycleTrigger, TransitionEvidence, lifecycle_machine,
    validate_lifecycle_topology,
};

/// Globally unique identity of one activation attempt and incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActivationId(NonZeroU64);

impl ActivationId {
    /// Construct an activation identity from a non-zero directory sequence.
    #[must_use]
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    /// Return the directory sequence value.
    #[must_use]
    pub const fn get(self) -> NonZeroU64 {
        self.0
    }

    fn classify<T>(self, observed: Self, value: T) -> Generation<T> {
        match self.cmp(&observed) {
            Ordering::Equal => Generation::Current(value),
            Ordering::Less | Ordering::Greater => Generation::Stale(value),
        }
    }
}

enum Generation<T> {
    Current(T),
    Stale(T),
}

/// Identity of one dispatch operation within a slot.
///
/// The field is crate-internal and non-zero, so the unallocated zero sentinel
/// is unrepresentable. Correlation is meaningful only for identities allocated
/// by the directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DispatchId(pub(crate) NonZeroU64);

impl DispatchId {
    /// Construct a dispatch identity from a non-zero directory sequence.
    #[must_use]
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    /// Return the directory sequence value.
    #[must_use]
    pub const fn get(self) -> NonZeroU64 {
        self.0
    }
}

/// Typed reason why a command was not admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The bounded activation waiter set is full.
    Busy,
    /// Passivation has closed admission.
    Draining,
    /// Activation failed without installing an incarnation.
    Unavailable,
    /// Family shutdown closed admission before this command was installed.
    Shutdown,
}

/// Stage at which a bounded drain failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainStage {
    /// Reserved dispatches had not resolved.
    Reservations,
    /// The fence was being enqueued.
    FenceEnqueue,
    /// The runtime was awaiting fence acknowledgement.
    FenceAcknowledgement,
    /// The runtime was awaiting exact termination.
    Retirement,
}

/// Classification attached to forced retirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainFailure {
    /// Stage at which graceful draining stopped.
    pub stage: DrainStage,
    /// Reservations still outstanding at that point.
    pub outstanding_reservations: usize,
}

/// Retirement mode granted to the slot interpreter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetirementMode {
    /// Processing was proven by a successful fence acknowledgement.
    Graceful,
    /// Graceful draining failed and command completion is unknown.
    Forced(DrainFailure),
}

/// Command retained while a shared activation is in flight.
#[derive(Debug)]
pub struct ActivationWaiter<C> {
    /// Correlation identity of the waiting dispatch.
    pub dispatch_id: DispatchId,
    /// Command retained on behalf of its caller.
    pub command: C,
}

/// Opaque state of one directory-owned activation and its bounded waiters.
#[derive(Debug)]
pub struct ActivatingSlot<C> {
    activation_id: ActivationId,
    waiters: Vec<ActivationWaiter<C>>,
    waiter_limit: NonZeroUsize,
}

/// Opaque state of one committed and admitting incarnation.
#[derive(Debug)]
pub struct ActiveSlot<E, L> {
    activation_id: ActivationId,
    endpoint: E,
    lease: L,
    reservations: ReservationCount,
}

/// Opaque state of one incarnation whose admission is closed.
#[derive(Debug)]
pub struct DrainingSlot<E, L> {
    activation_id: ActivationId,
    endpoint: E,
    lease: L,
    progress: DrainProgress,
}

/// Number of delivery operations that reserved admission but have not resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservationCount {
    /// Every reserved delivery has either enqueued or recovered its command.
    Drained,
    /// One or more delivery operations remain unresolved.
    Pending(NonZeroUsize),
}

impl ReservationCount {
    fn from_len(value: usize) -> Self {
        match NonZeroUsize::new(value) {
            Some(pending) => Self::Pending(pending),
            None => Self::Drained,
        }
    }

    fn reserve(self) -> Self {
        match self {
            Self::Drained => Self::Pending(NonZeroUsize::MIN),
            // Overflow is structurally unreachable: every reservation is held by
            // one live dispatch operation, and that many live operations cannot
            // fit in the address space.
            Self::Pending(value) => Self::Pending(
                NonZeroUsize::new(
                    value
                        .get()
                        .checked_add(1)
                        .expect("reservations bounded by live dispatches"),
                )
                .expect("non-zero count remains non-zero"),
            ),
        }
    }

    fn resolve(self) -> ReservationResolution {
        match self {
            Self::Drained => ReservationResolution::Unexpected,
            Self::Pending(value) => match value.get().checked_sub(1).and_then(NonZeroUsize::new) {
                Some(remaining) => ReservationResolution::Pending(Self::Pending(remaining)),
                None => ReservationResolution::Drained,
            },
        }
    }
}

enum ReservationResolution {
    Unexpected,
    Pending(ReservationCount),
    Drained,
}

/// Progress of the ordered processing-fence protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainProgress {
    /// Admission is closed but earlier reserved sends remain unresolved.
    Reservations(NonZeroUsize),
    /// Every reservation resolved and the fence has been requested.
    FenceAcknowledgement,
}

/// Pure state of one stable entity routing slot.
#[derive(Debug)]
pub enum EntitySlot<C, E, L> {
    /// No activation or incarnation exists. Directories normally remove this state.
    Inactive,
    /// Exactly one directory-owned activation is in flight.
    Activating(ActivatingSlot<C>),
    /// An incarnation is committed and accepting dispatch reservations.
    Active(ActiveSlot<E, L>),
    /// Admission is closed while reservations and the processing fence drain.
    Draining(DrainingSlot<E, L>),
    /// Retirement authority has moved to the interpreter.
    Retiring {
        /// Exact incarnation whose termination is awaited.
        activation_id: ActivationId,
    },
}

/// External fact submitted to one entity slot.
#[derive(Debug)]
pub enum SlotEvent<C, E, L> {
    /// Address an inactive slot and claim a new activation.
    ClaimActivation {
        /// Fresh identity allocated by the directory.
        activation_id: ActivationId,
        /// First dispatch correlation identity.
        dispatch_id: DispatchId,
        /// First command retained for activation.
        command: C,
        /// Non-zero bound for activation waiters.
        waiter_limit: NonZeroUsize,
    },
    /// Address an already represented slot.
    Dispatch {
        /// Dispatch correlation identity.
        dispatch_id: DispatchId,
        /// Command whose ownership must be delivered or returned.
        command: C,
    },
    /// Remove one canceled caller from an in-flight activation.
    CancelWaiter {
        /// Activation whose waiter set owns the command.
        activation_id: ActivationId,
        /// Dispatch correlation identity to remove.
        dispatch_id: DispatchId,
    },
    /// Transactional incarnation activation succeeded.
    ActivationSucceeded {
        /// Identity carried by the asynchronous activation result.
        activation_id: ActivationId,
        /// Private exact-incarnation delivery capability.
        endpoint: E,
        /// Affine exact-incarnation retirement capability.
        lease: L,
    },
    /// Preparation or transactional incarnation activation failed.
    ActivationFailed {
        /// Identity carried by the asynchronous activation result.
        activation_id: ActivationId,
    },
    /// A reserved dispatch resolved its mailbox enqueue attempt.
    DeliveryResolved {
        /// Identity carried by the asynchronous delivery result.
        activation_id: ActivationId,
        /// Failed delivery data, or `None` after successful enqueue.
        failure: Option<(DispatchId, C)>,
    },
    /// Atomically close admission for an active incarnation.
    BeginDrain {
        /// Incarnation requested for passivation.
        activation_id: ActivationId,
    },
    /// The ordered processing fence was acknowledged.
    FenceAcknowledged {
        /// Incarnation whose fence completed.
        activation_id: ActivationId,
    },
    /// Graceful draining failed and policy requires forced retirement.
    ForceDrain {
        /// Incarnation being drained.
        activation_id: ActivationId,
        /// Honest failure classification.
        failure: DrainFailure,
    },
    /// Exact incarnation termination was observed.
    Terminated {
        /// Identity of the terminated incarnation.
        activation_id: ActivationId,
    },
}

/// Request emitted by the pure entity-slot state machine.
#[derive(Debug)]
pub enum SlotEffect<C, E, L> {
    /// Run preparation and transactional activation in a directory-owned task.
    StartActivation {
        /// Fresh identity claimed by the slot.
        activation_id: ActivationId,
    },
    /// Deliver one reserved command without holding the slot lock.
    Deliver {
        /// Exact incarnation to which delivery is bound.
        activation_id: ActivationId,
        /// Dispatch correlation identity.
        dispatch_id: DispatchId,
        /// Clone of the delivery-only capability.
        endpoint: E,
        /// Owned command transferred to the delivery future.
        command: C,
    },
    /// Return a command with a typed admission refusal.
    Reject {
        /// Dispatch correlation identity.
        dispatch_id: DispatchId,
        /// Original command returned without cloning.
        command: C,
        /// Typed reason for refusal.
        reason: Refusal,
    },
    /// Enqueue the ordered user-lane fence after all reservations resolve.
    EnqueueFence {
        /// Exact incarnation whose user lane must be fenced.
        activation_id: ActivationId,
        /// Delivery capability for the fence protocol.
        endpoint: E,
    },
    /// Exercise the affine retirement capability and await exact termination.
    Retire {
        /// Exact incarnation being retired.
        activation_id: ActivationId,
        /// Unique retirement authority.
        lease: L,
        /// Graceful or explicitly forced classification.
        retirement: RetirementMode,
    },
    /// Remove the directory entry; slot pointer identity is the exact removal
    /// authority because one slot allocation never serves a second activation.
    Remove {
        /// Identity of the incarnation whose lifecycle produced this removal.
        activation_id: ActivationId,
    },
}

/// Ordered monoidal collection of slot effects.
#[derive(Debug)]
pub struct SlotEffectBatch<C, E, L>(EffectStorage<C, E, L>);

#[derive(Debug)]
enum EffectStorage<C, E, L> {
    Empty,
    One(SlotEffect<C, E, L>),
    Many(Vec<SlotEffect<C, E, L>>),
}

impl<C, E, L> SlotEffectBatch<C, E, L> {
    fn one(effect: SlotEffect<C, E, L>) -> Self {
        Self(EffectStorage::One(effect))
    }

    /// Borrow the ordered effects.
    #[must_use]
    pub fn as_slice(&self) -> &[SlotEffect<C, E, L>] {
        match &self.0 {
            EffectStorage::Empty => &[],
            EffectStorage::One(effect) => core::slice::from_ref(effect),
            EffectStorage::Many(effects) => effects,
        }
    }

    /// Consume the batch and return its ordered effects.
    #[must_use]
    pub fn into_vec(self) -> Vec<SlotEffect<C, E, L>> {
        match self.0 {
            EffectStorage::Empty => Vec::new(),
            EffectStorage::One(effect) => vec![effect],
            EffectStorage::Many(effects) => effects,
        }
    }

    /// Consume effects in declaration order without allocating for zero or one effect.
    pub fn for_each(self, mut interpret: impl FnMut(SlotEffect<C, E, L>)) {
        match self.0 {
            EffectStorage::Empty => {}
            EffectStorage::One(effect) => interpret(effect),
            EffectStorage::Many(effects) => effects.into_iter().for_each(interpret),
        }
    }

    fn push(&mut self, effect: SlotEffect<C, E, L>) {
        match core::mem::replace(&mut self.0, EffectStorage::Empty) {
            EffectStorage::Empty => self.0 = EffectStorage::One(effect),
            EffectStorage::One(first) => self.0 = EffectStorage::Many(vec![first, effect]),
            EffectStorage::Many(mut effects) => {
                effects.push(effect);
                self.0 = EffectStorage::Many(effects);
            }
        }
    }
}

impl<C, E, L> Extend<SlotEffect<C, E, L>> for SlotEffectBatch<C, E, L> {
    fn extend<T: IntoIterator<Item = SlotEffect<C, E, L>>>(&mut self, effects: T) {
        effects.into_iter().for_each(|effect| self.push(effect));
    }
}

impl<C, E, L> FromIterator<SlotEffect<C, E, L>> for SlotEffectBatch<C, E, L> {
    fn from_iter<T: IntoIterator<Item = SlotEffect<C, E, L>>>(effects: T) -> Self {
        let mut batch = Self::default();
        batch.extend(effects);
        batch
    }
}

impl<C, E, L> Default for SlotEffectBatch<C, E, L> {
    fn default() -> Self {
        Self(EffectStorage::Empty)
    }
}

impl<C, E, L> Add for SlotEffectBatch<C, E, L> {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        rhs.for_each(|effect| self.push(effect));
        self
    }
}

/// Result of one deterministic entity-slot reduction.
pub type SlotDecision<C, E, L> = Decision<EntitySlot<C, E, L>, SlotEffectBatch<C, E, L>>;

/// Pure reducer for one stable entity slot.
#[derive(Debug, Clone, Copy, Default)]
pub struct SlotReducer;

impl<C, E: Clone, L> Reducer<EntitySlot<C, E, L>, SlotEvent<C, E, L>> for SlotReducer {
    type Effects = SlotEffectBatch<C, E, L>;

    fn reduce(
        &self,
        state: EntitySlot<C, E, L>,
        event: SlotEvent<C, E, L>,
    ) -> Decision<EntitySlot<C, E, L>, Self::Effects> {
        match (state, event) {
            (EntitySlot::Inactive, event) => decide_inactive(event),
            (EntitySlot::Activating(state), event) => decide_activating(state, event),
            (EntitySlot::Active(state), event) => decide_active(state, event),
            (EntitySlot::Draining(state), event) => decide_draining(state, event),
            (EntitySlot::Retiring { activation_id }, event) => {
                decide_retiring(activation_id, event)
            }
        }
    }
}

impl<C, E: Clone, L> EntitySlot<C, E, L> {
    /// Return the compact lifecycle phase represented by this state.
    #[must_use]
    pub const fn phase(&self) -> LifecyclePhase {
        match self {
            Self::Inactive => LifecyclePhase::Inactive,
            Self::Activating(_) => LifecyclePhase::Activating,
            Self::Active(_) => LifecyclePhase::Active,
            Self::Draining(_) => LifecyclePhase::Draining,
            Self::Retiring { .. } => LifecyclePhase::Retiring,
        }
    }

    /// Return the activation represented by this slot, when one exists.
    #[must_use]
    pub const fn activation_id(&self) -> Option<ActivationId> {
        match self {
            Self::Inactive => None,
            Self::Activating(state) => Some(state.activation_id),
            Self::Active(state) => Some(state.activation_id),
            Self::Draining(state) => Some(state.activation_id),
            Self::Retiring { activation_id } => Some(*activation_id),
        }
    }

    /// Consume one fact and return the next state plus an effect batch.
    pub fn decide(self, event: SlotEvent<C, E, L>) -> SlotDecision<C, E, L> {
        SlotReducer.reduce(self, event)
    }

    /// Fold an ordered stream of facts through this slot.
    ///
    /// This is primarily useful to deterministic simulations and reference
    /// models. A concurrent directory normally installs each individual
    /// decision at its own linearization point.
    pub fn decide_all<I>(self, events: I) -> SlotDecision<C, E, L>
    where
        I: IntoIterator<Item = SlotEvent<C, E, L>>,
    {
        events.into_iter().fold(
            Decision::new(self, SlotEffectBatch::default()),
            |accumulated, event| {
                let decision = SlotReducer.reduce(accumulated.state, event);
                Decision::new(decision.state, accumulated.effects + decision.effects)
            },
        )
    }
}

impl<C, E, L> SlotEvent<C, E, L> {
    const fn trigger(&self) -> LifecycleTrigger {
        match self {
            Self::ClaimActivation { .. } => LifecycleTrigger::ClaimActivation,
            Self::Dispatch { .. } => LifecycleTrigger::Dispatch,
            Self::CancelWaiter { .. } => LifecycleTrigger::CancelWaiter,
            Self::ActivationSucceeded { .. } => LifecycleTrigger::ActivationSucceeded,
            Self::ActivationFailed { .. } => LifecycleTrigger::ActivationFailed,
            Self::DeliveryResolved { .. } => LifecycleTrigger::DeliveryResolved,
            Self::BeginDrain { .. } => LifecycleTrigger::BeginDrain,
            Self::FenceAcknowledged { .. } => LifecycleTrigger::FenceAcknowledged,
            Self::ForceDrain { .. } => LifecycleTrigger::ForceDrain,
            Self::Terminated { .. } => LifecycleTrigger::Terminated,
        }
    }
}

impl<C, E, L> EntitySlot<C, E, L> {
    fn handles(&self, event: &SlotEvent<C, E, L>) -> bool {
        match (self, event) {
            (Self::Inactive, SlotEvent::Dispatch { .. })
            | (
                Self::Activating(_) | Self::Active(_) | Self::Draining(_) | Self::Retiring { .. },
                SlotEvent::Dispatch { .. } | SlotEvent::ClaimActivation { .. },
            ) => true,
            (
                Self::Activating(state),
                SlotEvent::CancelWaiter { activation_id, .. }
                | SlotEvent::ActivationSucceeded { activation_id, .. }
                | SlotEvent::ActivationFailed { activation_id },
            ) => state.activation_id == *activation_id,
            (
                Self::Active(state),
                SlotEvent::DeliveryResolved { activation_id, .. }
                | SlotEvent::BeginDrain { activation_id },
            ) => state.activation_id == *activation_id,
            (
                Self::Draining(state),
                SlotEvent::DeliveryResolved { activation_id, .. }
                | SlotEvent::FenceAcknowledged { activation_id }
                | SlotEvent::ForceDrain { activation_id, .. },
            ) => state.activation_id == *activation_id,
            (
                Self::Retiring { activation_id },
                SlotEvent::Terminated {
                    activation_id: observed,
                },
            ) => activation_id == observed,
            _ => false,
        }
    }
}

fn decision<C, E, L>(
    state: EntitySlot<C, E, L>,
    effects: SlotEffectBatch<C, E, L>,
) -> SlotDecision<C, E, L> {
    Decision::new(state, effects)
}

fn reject<C, E, L>(
    state: EntitySlot<C, E, L>,
    dispatch_id: DispatchId,
    command: C,
    reason: Refusal,
) -> SlotDecision<C, E, L> {
    decision(
        state,
        SlotEffectBatch::one(SlotEffect::Reject {
            dispatch_id,
            command,
            reason,
        }),
    )
}

fn decide_activating<C, E: Clone, L>(
    mut state: ActivatingSlot<C>,
    event: SlotEvent<C, E, L>,
) -> SlotDecision<C, E, L> {
    match event {
        SlotEvent::Dispatch {
            dispatch_id,
            command,
        } => match state.waiters.len().cmp(&state.waiter_limit.get()) {
            Ordering::Less => {
                state.waiters.push(ActivationWaiter {
                    dispatch_id,
                    command,
                });
                decision(EntitySlot::Activating(state), SlotEffectBatch::default())
            }
            Ordering::Equal | Ordering::Greater => reject(
                EntitySlot::Activating(state),
                dispatch_id,
                command,
                Refusal::Busy,
            ),
        },
        SlotEvent::CancelWaiter {
            activation_id,
            dispatch_id,
        } => match state.activation_id.classify(activation_id, dispatch_id) {
            Generation::Current(dispatch_id) => {
                state
                    .waiters
                    .retain(|waiter| waiter.dispatch_id != dispatch_id);
                decision(EntitySlot::Activating(state), SlotEffectBatch::default())
            }
            Generation::Stale(_) => {
                decision(EntitySlot::Activating(state), SlotEffectBatch::default())
            }
        },
        SlotEvent::ActivationSucceeded {
            activation_id,
            endpoint,
            lease,
        } => finish_activation(state, activation_id, endpoint, lease),
        SlotEvent::ActivationFailed { activation_id } => fail_activation(state, activation_id),
        SlotEvent::ClaimActivation {
            dispatch_id,
            command,
            ..
        } => reject(
            EntitySlot::Activating(state),
            dispatch_id,
            command,
            Refusal::Busy,
        ),
        // A late failed delivery from a previous incarnation still owns its
        // command; the command returns as unavailable.
        SlotEvent::DeliveryResolved { failure, .. } => {
            reject_failed_delivery(EntitySlot::Activating(state), failure, Refusal::Unavailable)
        }
        _ => decision(EntitySlot::Activating(state), SlotEffectBatch::default()),
    }
}

fn finish_activation<C, E: Clone, L>(
    state: ActivatingSlot<C>,
    activation_id: ActivationId,
    endpoint: E,
    lease: L,
) -> SlotDecision<C, E, L> {
    match state
        .activation_id
        .classify(activation_id, (endpoint, lease))
    {
        Generation::Current((endpoint, lease)) => {
            let reservations = ReservationCount::from_len(state.waiters.len());
            let effects = state
                .waiters
                .into_iter()
                .map(|waiter| SlotEffect::Deliver {
                    activation_id,
                    dispatch_id: waiter.dispatch_id,
                    endpoint: endpoint.clone(),
                    command: waiter.command,
                })
                .collect();
            decision(
                EntitySlot::Active(ActiveSlot {
                    activation_id,
                    endpoint,
                    lease,
                    reservations,
                }),
                effects,
            )
        }
        Generation::Stale((_, lease)) => {
            retire_stale(EntitySlot::Activating(state), activation_id, lease)
        }
    }
}

fn fail_activation<C, E, L>(
    state: ActivatingSlot<C>,
    activation_id: ActivationId,
) -> SlotDecision<C, E, L> {
    match state.activation_id.classify(activation_id, ()) {
        Generation::Current(()) => {
            let effects = state
                .waiters
                .into_iter()
                .map(|waiter| SlotEffect::Reject {
                    dispatch_id: waiter.dispatch_id,
                    command: waiter.command,
                    reason: Refusal::Unavailable,
                })
                .chain(core::iter::once(SlotEffect::Remove { activation_id }))
                .collect();
            decision(EntitySlot::Inactive, effects)
        }
        Generation::Stale(()) => {
            decision(EntitySlot::Activating(state), SlotEffectBatch::default())
        }
    }
}

fn decide_active<C, E: Clone, L>(
    state: ActiveSlot<E, L>,
    event: SlotEvent<C, E, L>,
) -> SlotDecision<C, E, L> {
    match event {
        SlotEvent::Dispatch {
            dispatch_id,
            command,
        } => {
            let activation_id = state.activation_id;
            let endpoint = state.endpoint.clone();
            decision(
                EntitySlot::Active(ActiveSlot {
                    reservations: state.reservations.reserve(),
                    ..state
                }),
                SlotEffectBatch::one(SlotEffect::Deliver {
                    activation_id,
                    dispatch_id,
                    endpoint,
                    command,
                }),
            )
        }
        SlotEvent::DeliveryResolved {
            activation_id,
            failure,
        } => match state.activation_id.classify(activation_id, failure) {
            Generation::Current(failure) => resolve_active_delivery(state, failure),
            Generation::Stale(failure) => {
                reject_failed_delivery(EntitySlot::Active(state), failure, Refusal::Unavailable)
            }
        },
        SlotEvent::BeginDrain { activation_id } => {
            match state.activation_id.classify(activation_id, ()) {
                Generation::Current(()) => begin_drain(state),
                Generation::Stale(()) => {
                    decision(EntitySlot::Active(state), SlotEffectBatch::default())
                }
            }
        }
        SlotEvent::ActivationSucceeded {
            activation_id,
            lease,
            ..
        } => retire_stale(EntitySlot::Active(state), activation_id, lease),
        SlotEvent::ClaimActivation {
            dispatch_id,
            command,
            ..
        } => reject(
            EntitySlot::Active(state),
            dispatch_id,
            command,
            Refusal::Busy,
        ),
        _ => decision(EntitySlot::Active(state), SlotEffectBatch::default()),
    }
}

fn resolve_active_delivery<C, E, L>(
    state: ActiveSlot<E, L>,
    failure: Option<(DispatchId, C)>,
) -> SlotDecision<C, E, L> {
    match state.reservations.resolve() {
        ReservationResolution::Unexpected => {
            reject_failed_delivery(EntitySlot::Active(state), failure, Refusal::Unavailable)
        }
        ReservationResolution::Pending(reservations) => reject_failed_delivery(
            EntitySlot::Active(ActiveSlot {
                reservations,
                ..state
            }),
            failure,
            Refusal::Unavailable,
        ),
        ReservationResolution::Drained => reject_failed_delivery(
            EntitySlot::Active(ActiveSlot {
                reservations: ReservationCount::Drained,
                ..state
            }),
            failure,
            Refusal::Unavailable,
        ),
    }
}

fn begin_drain<C, E: Clone, L>(state: ActiveSlot<E, L>) -> SlotDecision<C, E, L> {
    match state.reservations {
        ReservationCount::Drained => {
            let activation_id = state.activation_id;
            let endpoint = state.endpoint.clone();
            decision(
                EntitySlot::Draining(DrainingSlot {
                    activation_id,
                    endpoint: state.endpoint,
                    lease: state.lease,
                    progress: DrainProgress::FenceAcknowledgement,
                }),
                SlotEffectBatch::one(SlotEffect::EnqueueFence {
                    activation_id,
                    endpoint,
                }),
            )
        }
        ReservationCount::Pending(pending) => decision(
            EntitySlot::Draining(DrainingSlot {
                activation_id: state.activation_id,
                endpoint: state.endpoint,
                lease: state.lease,
                progress: DrainProgress::Reservations(pending),
            }),
            SlotEffectBatch::default(),
        ),
    }
}

fn decide_draining<C, E: Clone, L>(
    state: DrainingSlot<E, L>,
    event: SlotEvent<C, E, L>,
) -> SlotDecision<C, E, L> {
    match event {
        SlotEvent::Dispatch {
            dispatch_id,
            command,
        }
        | SlotEvent::ClaimActivation {
            dispatch_id,
            command,
            ..
        } => reject(
            EntitySlot::Draining(state),
            dispatch_id,
            command,
            Refusal::Draining,
        ),
        SlotEvent::DeliveryResolved {
            activation_id,
            failure,
        } => match state.activation_id.classify(activation_id, failure) {
            Generation::Current(failure) => resolve_draining_delivery(state, failure),
            Generation::Stale(failure) => {
                reject_failed_delivery(EntitySlot::Draining(state), failure, Refusal::Unavailable)
            }
        },
        SlotEvent::FenceAcknowledged { activation_id } => {
            match state.activation_id.classify(activation_id, ()) {
                Generation::Current(()) => acknowledge_fence(state),
                Generation::Stale(()) => {
                    decision(EntitySlot::Draining(state), SlotEffectBatch::default())
                }
            }
        }
        SlotEvent::ForceDrain {
            activation_id,
            failure,
        } => match state.activation_id.classify(activation_id, failure) {
            Generation::Current(failure) => force_drain(state, failure),
            Generation::Stale(_) => {
                decision(EntitySlot::Draining(state), SlotEffectBatch::default())
            }
        },
        SlotEvent::ActivationSucceeded {
            activation_id,
            lease,
            ..
        } => retire_stale(EntitySlot::Draining(state), activation_id, lease),
        _ => decision(EntitySlot::Draining(state), SlotEffectBatch::default()),
    }
}

fn resolve_draining_delivery<C, E: Clone, L>(
    state: DrainingSlot<E, L>,
    failure: Option<(DispatchId, C)>,
) -> SlotDecision<C, E, L> {
    match state.progress {
        DrainProgress::FenceAcknowledgement => {
            reject_failed_delivery(EntitySlot::Draining(state), failure, Refusal::Unavailable)
        }
        // The count is non-zero by construction, so subtraction cannot
        // underflow; reaching zero means the last reservation resolved.
        DrainProgress::Reservations(pending) => {
            if let Some(remaining) = pending.get().checked_sub(1).and_then(NonZeroUsize::new) {
                reject_failed_delivery(
                    EntitySlot::Draining(DrainingSlot {
                        progress: DrainProgress::Reservations(remaining),
                        ..state
                    }),
                    failure,
                    Refusal::Unavailable,
                )
            } else {
                let activation_id = state.activation_id;
                let endpoint = state.endpoint.clone();
                let next = EntitySlot::Draining(DrainingSlot {
                    progress: DrainProgress::FenceAcknowledgement,
                    ..state
                });
                with_effect(
                    reject_failed_delivery(next, failure, Refusal::Unavailable),
                    SlotEffect::EnqueueFence {
                        activation_id,
                        endpoint,
                    },
                )
            }
        }
    }
}

fn acknowledge_fence<C, E, L>(state: DrainingSlot<E, L>) -> SlotDecision<C, E, L> {
    match state.progress {
        DrainProgress::Reservations(_) => {
            decision(EntitySlot::Draining(state), SlotEffectBatch::default())
        }
        DrainProgress::FenceAcknowledgement => decision(
            EntitySlot::Retiring {
                activation_id: state.activation_id,
            },
            SlotEffectBatch::one(SlotEffect::Retire {
                activation_id: state.activation_id,
                lease: state.lease,
                retirement: RetirementMode::Graceful,
            }),
        ),
    }
}

fn force_drain<C, E, L>(state: DrainingSlot<E, L>, failure: DrainFailure) -> SlotDecision<C, E, L> {
    decision(
        EntitySlot::Retiring {
            activation_id: state.activation_id,
        },
        SlotEffectBatch::one(SlotEffect::Retire {
            activation_id: state.activation_id,
            lease: state.lease,
            retirement: RetirementMode::Forced(failure),
        }),
    )
}

fn reject_failed_delivery<C, E, L>(
    state: EntitySlot<C, E, L>,
    failure: Option<(DispatchId, C)>,
    reason: Refusal,
) -> SlotDecision<C, E, L> {
    match failure {
        Some((dispatch_id, command)) => reject(state, dispatch_id, command, reason),
        None => decision(state, SlotEffectBatch::default()),
    }
}

fn retire_stale<C, E, L>(
    state: EntitySlot<C, E, L>,
    activation_id: ActivationId,
    lease: L,
) -> SlotDecision<C, E, L> {
    decision(
        state,
        SlotEffectBatch::one(SlotEffect::Retire {
            activation_id,
            lease,
            retirement: RetirementMode::Forced(DrainFailure {
                stage: DrainStage::Retirement,
                outstanding_reservations: 0,
            }),
        }),
    )
}

fn with_effect<C, E, L>(
    decision: SlotDecision<C, E, L>,
    effect: SlotEffect<C, E, L>,
) -> SlotDecision<C, E, L> {
    Decision::new(
        decision.state,
        decision.effects + SlotEffectBatch::one(effect),
    )
}

#[cfg(test)]
mod tests {
    use core::num::{NonZeroU64, NonZeroUsize};

    use super::{
        ActivationId, DispatchId, EntitySlot, Refusal, RetirementMode, SlotEffect, SlotEvent,
    };

    fn activation(value: u64) -> ActivationId {
        ActivationId::new(NonZeroU64::new(value).unwrap())
    }

    fn dispatch(value: u64) -> DispatchId {
        DispatchId(NonZeroU64::new(value).unwrap())
    }

    #[test]
    fn concurrent_demand_starts_one_activation_and_bounds_waiters() {
        let first = EntitySlot::<u8, u8, u8>::Inactive.decide(SlotEvent::ClaimActivation {
            activation_id: activation(1),
            dispatch_id: dispatch(1),
            command: 10,
            waiter_limit: NonZeroUsize::new(2).unwrap(),
        });
        assert!(matches!(
            first.effects.as_slice(),
            [SlotEffect::StartActivation { .. }]
        ));

        let second = first.state.decide(SlotEvent::Dispatch {
            dispatch_id: dispatch(2),
            command: 20,
        });
        assert!(second.effects.as_slice().is_empty());
        let third = second.state.decide(SlotEvent::Dispatch {
            dispatch_id: dispatch(3),
            command: 30,
        });
        assert!(matches!(
            third.effects.as_slice(),
            [SlotEffect::Reject {
                command: 30,
                reason: Refusal::Busy,
                ..
            }]
        ));
    }

    #[test]
    fn drain_waits_for_reserved_delivery_before_enqueuing_fence() {
        let active = EntitySlot::<u8, u8, u8>::Active(super::ActiveSlot {
            activation_id: activation(2),
            endpoint: 7,
            lease: 9,
            reservations: super::ReservationCount::Pending(NonZeroUsize::MIN),
        });
        let draining = active.decide(SlotEvent::BeginDrain {
            activation_id: activation(2),
        });
        assert!(draining.effects.as_slice().is_empty());

        let resolved = draining.state.decide(SlotEvent::DeliveryResolved {
            activation_id: activation(2),
            failure: None,
        });
        assert!(matches!(
            resolved.effects.as_slice(),
            [SlotEffect::EnqueueFence { .. }]
        ));

        let acknowledged = resolved.state.decide(SlotEvent::FenceAcknowledged {
            activation_id: activation(2),
        });
        assert!(matches!(
            acknowledged.effects.as_slice(),
            [SlotEffect::Retire {
                retirement: RetirementMode::Graceful,
                ..
            }]
        ));
    }

    #[test]
    fn stale_activation_cannot_replace_live_incarnation() {
        let active = EntitySlot::<u8, u8, u8>::Active(super::ActiveSlot {
            activation_id: activation(5),
            endpoint: 5,
            lease: 5,
            reservations: super::ReservationCount::Drained,
        });
        let stale = active.decide(SlotEvent::ActivationSucceeded {
            activation_id: activation(4),
            endpoint: 4,
            lease: 4,
        });
        assert!(matches!(
            stale.state,
            EntitySlot::Active(super::ActiveSlot { activation_id, .. }) if activation_id == activation(5)
        ));
        assert!(
            matches!(stale.effects.as_slice(), [SlotEffect::Retire { activation_id, .. }] if *activation_id == activation(4))
        );
    }

    #[test]
    fn stale_termination_cannot_remove_newer_incarnation() {
        let retiring = EntitySlot::<u8, u8, u8>::Retiring {
            activation_id: activation(7),
        };
        let stale = retiring.decide(SlotEvent::Terminated {
            activation_id: activation(6),
        });
        assert!(matches!(
            stale.state,
            EntitySlot::Retiring { activation_id } if activation_id == activation(7)
        ));
        assert!(stale.effects.as_slice().is_empty());
    }

    #[test]
    fn draining_never_reopens_admission() {
        let draining = EntitySlot::<u8, u8, u8>::Draining(super::DrainingSlot {
            activation_id: activation(3),
            endpoint: 3,
            lease: 3,
            progress: super::DrainProgress::FenceAcknowledgement,
        });
        let refused = draining.decide(SlotEvent::Dispatch {
            dispatch_id: dispatch(8),
            command: 9,
        });
        assert!(matches!(refused.state, EntitySlot::Draining(_)));
        assert!(matches!(
            refused.effects.as_slice(),
            [SlotEffect::Reject {
                reason: Refusal::Draining,
                command: 9,
                ..
            }]
        ));
    }

    #[test]
    fn activation_failure_preserves_waiter_order_before_removal() {
        let first = EntitySlot::<u8, u8, u8>::Inactive.decide(SlotEvent::ClaimActivation {
            activation_id: activation(1),
            dispatch_id: dispatch(1),
            command: 10,
            waiter_limit: NonZeroUsize::new(2).unwrap(),
        });
        let second = first.state.decide(SlotEvent::Dispatch {
            dispatch_id: dispatch(2),
            command: 20,
        });
        let failed = second.state.decide(SlotEvent::ActivationFailed {
            activation_id: activation(1),
        });
        assert!(matches!(
            failed.effects.as_slice(),
            [
                SlotEffect::Reject { dispatch_id: first_id, command: 10, .. },
                SlotEffect::Reject { dispatch_id: second_id, command: 20, .. },
                SlotEffect::Remove { activation_id }
            ] if *first_id == dispatch(1) && *second_id == dispatch(2) && *activation_id == activation(1)
        ));
    }

    #[test]
    fn effect_batch_has_identity_and_associative_order() {
        let a = super::SlotEffectBatch::<u8, u8, u8>::one(SlotEffect::Remove {
            activation_id: activation(1),
        });
        let b = super::SlotEffectBatch::one(SlotEffect::Remove {
            activation_id: activation(2),
        });
        let combined = super::SlotEffectBatch::default() + a + b;

        assert!(matches!(
            combined.as_slice(),
            [
                SlotEffect::Remove { activation_id: first },
                SlotEffect::Remove { activation_id: second }
            ] if *first == activation(1) && *second == activation(2)
        ));
    }
}
