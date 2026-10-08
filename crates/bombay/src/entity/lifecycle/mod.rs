//! Pure lifecycle algebra for one stable entity routing slot.

use core::cmp::Ordering;
use core::num::{NonZeroU64, NonZeroUsize};
use core::ops::Add;

mod inactive;
mod retiring;
mod transition;

use inactive::decide_inactive;
use retiring::decide_retiring;

pub use transition::{LifecycleEdge, LifecyclePhase, TransitionEvidence};

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

/// Complete result of one deterministic entity-slot transition.
#[must_use = "the successor, effects, and disposition must be retained"]
#[derive(Debug)]
pub struct SlotDecision<C, E, L> {
    /// Successor slot state.
    pub state: EntitySlot<C, E, L>,
    /// Ordered effects, including exact returned commands and leases.
    pub effects: SlotEffectBatch<C, E, L>,
    /// Classification selected by the executable transition branch.
    pub evidence: TransitionEvidence,
}

impl<C, E, L> SlotDecision<C, E, L> {
    fn with_evidence(mut self, evidence: TransitionEvidence) -> Self {
        self.evidence = evidence;
        self
    }
}

impl<C, E, L> EntitySlot<C, E, L> {
    #[expect(
        clippy::type_complexity,
        reason = "closing a slot returns its successor, original pending effects and exact available lease"
    )]
    pub(super) fn close_family(
        self,
    ) -> (
        Self,
        SlotEffectBatch<C, E, L>,
        Option<(ActivationId, E, L, Result<(), DrainFailure>)>,
    ) {
        match self {
            Self::Inactive => (Self::Inactive, SlotEffectBatch::default(), None),
            Self::Activating(state) => {
                let mut effects: SlotEffectBatch<_, _, _> = state
                    .waiters
                    .into_iter()
                    .map(|waiter| SlotEffect::Reject {
                        dispatch_id: waiter.dispatch_id,
                        command: waiter.command,
                        reason: Refusal::Shutdown,
                    })
                    .collect();
                effects.push(SlotEffect::Remove {
                    activation_id: state.activation_id,
                });
                (Self::Inactive, effects, None)
            }
            Self::Active(state) => {
                let admission = match state.reservations {
                    ReservationCount::Drained => Ok(()),
                    ReservationCount::Pending(count) => Err(DrainFailure {
                        stage: DrainStage::Reservations,
                        outstanding_reservations: count.get(),
                    }),
                };
                (
                    Self::Retiring {
                        activation_id: state.activation_id,
                    },
                    SlotEffectBatch::default(),
                    Some((state.activation_id, state.endpoint, state.lease, admission)),
                )
            }
            Self::Draining(state) => {
                let failure = match state.progress {
                    DrainProgress::Reservations(count) => DrainFailure {
                        stage: DrainStage::Reservations,
                        outstanding_reservations: count.get(),
                    },
                    DrainProgress::FenceAcknowledgement => DrainFailure {
                        stage: DrainStage::FenceAcknowledgement,
                        outstanding_reservations: 0,
                    },
                };
                (
                    Self::Retiring {
                        activation_id: state.activation_id,
                    },
                    SlotEffectBatch::default(),
                    Some((
                        state.activation_id,
                        state.endpoint,
                        state.lease,
                        Err(failure),
                    )),
                )
            }
            state @ Self::Retiring { .. } => (state, SlotEffectBatch::default(), None),
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
        match (self, event) {
            (EntitySlot::Inactive, event) => decide_inactive(event),
            (EntitySlot::Activating(state), event) => decide_activating(state, event),
            (EntitySlot::Active(state), event) => decide_active(state, event),
            (EntitySlot::Draining(state), event) => decide_draining(state, event),
            (EntitySlot::Retiring { activation_id }, event) => {
                decide_retiring(activation_id, event)
            }
        }
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
            decision(self, SlotEffectBatch::default()),
            |accumulated, event| {
                let next = accumulated.state.decide(event);
                SlotDecision {
                    state: next.state,
                    effects: accumulated.effects + next.effects,
                    evidence: next.evidence,
                }
            },
        )
    }
}

fn decision<C, E, L>(
    state: EntitySlot<C, E, L>,
    effects: SlotEffectBatch<C, E, L>,
) -> SlotDecision<C, E, L> {
    SlotDecision {
        state,
        effects,
        evidence: TransitionEvidence::Ignored,
    }
}

fn handled<C, E, L>(
    state: EntitySlot<C, E, L>,
    effects: SlotEffectBatch<C, E, L>,
) -> SlotDecision<C, E, L> {
    SlotDecision {
        state,
        effects,
        evidence: TransitionEvidence::SelfLoop,
    }
}

fn traversed<C, E, L>(
    state: EntitySlot<C, E, L>,
    effects: SlotEffectBatch<C, E, L>,
    edge: LifecycleEdge,
) -> SlotDecision<C, E, L> {
    SlotDecision {
        state,
        effects,
        evidence: TransitionEvidence::Traversed(edge),
    }
}

fn reject<C, E, L>(
    state: EntitySlot<C, E, L>,
    dispatch_id: DispatchId,
    command: C,
    reason: Refusal,
) -> SlotDecision<C, E, L> {
    handled(
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
                handled(EntitySlot::Activating(state), SlotEffectBatch::default())
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
                let prior_waiters = state.waiters.len();
                state
                    .waiters
                    .retain(|waiter| waiter.dispatch_id != dispatch_id);
                if state.waiters.len() < prior_waiters {
                    handled(EntitySlot::Activating(state), SlotEffectBatch::default())
                } else {
                    decision(EntitySlot::Activating(state), SlotEffectBatch::default())
                }
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
            traversed(
                EntitySlot::Active(ActiveSlot {
                    activation_id,
                    endpoint,
                    lease,
                    reservations,
                }),
                effects,
                LifecycleEdge::ActivationSucceeded,
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
            traversed(
                EntitySlot::Inactive,
                effects,
                LifecycleEdge::ActivationFailed,
            )
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
            handled(
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
        )
        .with_evidence(TransitionEvidence::SelfLoop),
        ReservationResolution::Drained => reject_failed_delivery(
            EntitySlot::Active(ActiveSlot {
                reservations: ReservationCount::Drained,
                ..state
            }),
            failure,
            Refusal::Unavailable,
        )
        .with_evidence(TransitionEvidence::SelfLoop),
    }
}

fn begin_drain<C, E: Clone, L>(state: ActiveSlot<E, L>) -> SlotDecision<C, E, L> {
    match state.reservations {
        ReservationCount::Drained => {
            let activation_id = state.activation_id;
            let endpoint = state.endpoint.clone();
            traversed(
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
                LifecycleEdge::BeginDrain,
            )
        }
        ReservationCount::Pending(pending) => traversed(
            EntitySlot::Draining(DrainingSlot {
                activation_id: state.activation_id,
                endpoint: state.endpoint,
                lease: state.lease,
                progress: DrainProgress::Reservations(pending),
            }),
            SlotEffectBatch::default(),
            LifecycleEdge::BeginDrain,
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
                .with_evidence(TransitionEvidence::SelfLoop)
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
                .with_evidence(TransitionEvidence::SelfLoop)
            }
        }
    }
}

fn acknowledge_fence<C, E, L>(state: DrainingSlot<E, L>) -> SlotDecision<C, E, L> {
    match state.progress {
        DrainProgress::Reservations(_) => {
            decision(EntitySlot::Draining(state), SlotEffectBatch::default())
        }
        DrainProgress::FenceAcknowledgement => traversed(
            EntitySlot::Retiring {
                activation_id: state.activation_id,
            },
            SlotEffectBatch::one(SlotEffect::Retire {
                activation_id: state.activation_id,
                lease: state.lease,
                retirement: RetirementMode::Graceful,
            }),
            LifecycleEdge::FenceAcknowledged,
        ),
    }
}

fn force_drain<C, E, L>(state: DrainingSlot<E, L>, failure: DrainFailure) -> SlotDecision<C, E, L> {
    traversed(
        EntitySlot::Retiring {
            activation_id: state.activation_id,
        },
        SlotEffectBatch::one(SlotEffect::Retire {
            activation_id: state.activation_id,
            lease: state.lease,
            retirement: RetirementMode::Forced(failure),
        }),
        LifecycleEdge::ForceDrain,
    )
}

fn reject_failed_delivery<C, E, L>(
    state: EntitySlot<C, E, L>,
    failure: Option<(DispatchId, C)>,
    reason: Refusal,
) -> SlotDecision<C, E, L> {
    match failure {
        Some((dispatch_id, command)) => decision(
            state,
            SlotEffectBatch::one(SlotEffect::Reject {
                dispatch_id,
                command,
                reason,
            }),
        ),
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
    SlotDecision {
        state: decision.state,
        effects: decision.effects + SlotEffectBatch::one(effect),
        evidence: decision.evidence,
    }
}

#[cfg(test)]
mod tests {
    use core::num::{NonZeroU64, NonZeroUsize};

    use core::marker::PhantomData;

    use behavior::{
        Actions, ActiveTurn, Address, Behavior, BehaviorActed, EndpointAddress, Never, NoBirths,
        Protocol, User,
    };
    use behavior_actors::Activate;
    use behavior_actors::atomic::{
        ImmediateActivation, InitialWorkerOutcome, ProxyControl, ProxyOutcome, ProxyPhase,
        StableProxy,
    };

    use super::{
        ActivationId, DispatchId, DrainStage, EntitySlot, Refusal, RetirementMode, SlotEffect,
        SlotEvent,
    };

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct ProxyAddress;

    impl Address for ProxyAddress {
        type Nonce = u64;
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct ProxyEndpoint;

    struct ProxyInstalled<B: Behavior>(ProxyEndpoint, PhantomData<fn() -> B>);

    impl<B: Behavior> Clone for ProxyInstalled<B> {
        fn clone(&self) -> Self {
            Self(self.0, PhantomData)
        }
    }

    impl EndpointAddress for ProxyAddress {
        type Established<P>
            = ProxyEndpoint
        where
            P: Protocol<Addr = Self>;

        type Installed<B>
            = ProxyInstalled<B>
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
    struct ProxyWorker(u8);

    impl Protocol for ProxyWorker {
        type Addr = ProxyAddress;
        type Msg = ();
    }

    impl Behavior for ProxyWorker {
        type Protocol = Self;
        type Event = User<ProxyAddress, ()>;
        type Sends = Vec<Never>;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;

        fn transition(&mut self, _: ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }
    }

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
    fn entity_first_demand_is_not_stable_proxy_worker_start() {
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

        assert!(second.effects.as_slice().is_empty());
        assert!(matches!(second.state, EntitySlot::Activating(_)));

        let initialized = StableProxy::<ProxyWorker, ImmediateActivation>::immediate()
            .initialize()
            .unwrap_or_else(|_| panic!("proxy initialization is pure"));
        let mut proxy = initialized.behavior;
        let started = proxy
            .on(ProxyControl::start(ProxyWorker(1)))
            .unwrap_or_else(|_| panic!("the first worker start is accepted"));
        assert_eq!(proxy.phase(), ProxyPhase::Creating);
        assert_eq!(started.creates.len(), 1);

        let overlap = proxy
            .on(ProxyControl::start(ProxyWorker(2)))
            .unwrap_or_else(|_| panic!("the overlapping worker start is classified"));
        assert!(overlap.creates.is_empty());
        assert!(overlap.sends.diagnostics.is_empty());
        let outcome = overlap
            .sends
            .owner_outcomes
            .into_requests()
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("the overlapping worker returns to its owner"))
            .into_inner();
        match outcome {
            ProxyOutcome::Initial {
                outcome:
                    InitialWorkerOutcome::Overlap {
                        worker,
                        activation,
                        phase,
                    },
            } => {
                assert_eq!(worker, ProxyWorker(2));
                assert_eq!(activation, ImmediateActivation);
                assert_eq!(phase, ProxyPhase::Creating);
            }
            _ => panic!("the proxy returned a different owner outcome"),
        }
    }

    fn active_after_hydration_failure_and_retry() -> EntitySlot<u8, u8, u8> {
        let first = EntitySlot::<u8, u8, u8>::Inactive.decide(SlotEvent::ClaimActivation {
            activation_id: activation(1),
            dispatch_id: dispatch(1),
            command: 10,
            waiter_limit: NonZeroUsize::new(2).unwrap(),
        });
        assert!(matches!(
            first.effects.as_slice(),
            [SlotEffect::StartActivation { activation_id }] if *activation_id == activation(1)
        ));
        let second = first.state.decide(SlotEvent::Dispatch {
            dispatch_id: dispatch(2),
            command: 20,
        });
        assert!(second.effects.as_slice().is_empty());
        let failed = second.state.decide(SlotEvent::ActivationFailed {
            activation_id: activation(1),
        });
        assert!(matches!(failed.state, EntitySlot::Inactive));
        assert!(matches!(
            failed.effects.as_slice(),
            [
                SlotEffect::Reject {
                    dispatch_id: first_dispatch,
                    command: 10,
                    reason: Refusal::Unavailable,
                },
                SlotEffect::Reject {
                    dispatch_id: second_dispatch,
                    command: 20,
                    reason: Refusal::Unavailable,
                },
                SlotEffect::Remove { activation_id },
            ] if *first_dispatch == dispatch(1)
                && *second_dispatch == dispatch(2)
                && *activation_id == activation(1)
        ));

        let replacement = failed.state.decide(SlotEvent::ClaimActivation {
            activation_id: activation(2),
            dispatch_id: dispatch(3),
            command: 30,
            waiter_limit: NonZeroUsize::new(2).unwrap(),
        });
        let activated = replacement.state.decide(SlotEvent::ActivationSucceeded {
            activation_id: activation(2),
            endpoint: 7,
            lease: 9,
        });
        assert!(matches!(
            activated.effects.as_slice(),
            [SlotEffect::Deliver {
                activation_id,
                dispatch_id,
                endpoint: 7,
                command: 30,
            }] if *activation_id == activation(2) && *dispatch_id == dispatch(3)
        ));
        let delivered = activated.state.decide(SlotEvent::DeliveryResolved {
            activation_id: activation(2),
            failure: None,
        });
        assert!(delivered.effects.as_slice().is_empty());
        delivered.state
    }

    #[test]
    fn entity_trace_owns_hydration_waiters_fence_and_runtime_generation() {
        let stale =
            active_after_hydration_failure_and_retry().decide(SlotEvent::ActivationSucceeded {
                activation_id: activation(1),
                endpoint: 1,
                lease: 2,
            });
        assert!(matches!(
            stale.state,
            EntitySlot::Active(ref active) if active.activation_id == activation(2)
        ));
        assert!(matches!(
            stale.effects.as_slice(),
            [SlotEffect::Retire {
                activation_id,
                lease: 2,
                retirement: RetirementMode::Forced(failure),
            }] if *activation_id == activation(1)
                && failure.stage == DrainStage::Retirement
                && failure.outstanding_reservations == 0
        ));

        let draining = stale.state.decide(SlotEvent::BeginDrain {
            activation_id: activation(2),
        });
        assert!(matches!(draining.state, EntitySlot::Draining(_)));
        assert!(matches!(
            draining.effects.as_slice(),
            [SlotEffect::EnqueueFence {
                activation_id,
                endpoint: 7,
            }] if *activation_id == activation(2)
        ));
        let stale_fence = draining.state.decide(SlotEvent::FenceAcknowledged {
            activation_id: activation(1),
        });
        assert!(matches!(stale_fence.state, EntitySlot::Draining(_)));
        assert!(stale_fence.effects.as_slice().is_empty());
        let fenced = stale_fence.state.decide(SlotEvent::FenceAcknowledged {
            activation_id: activation(2),
        });
        assert!(matches!(
            fenced.state,
            EntitySlot::Retiring { activation_id } if activation_id == activation(2)
        ));
        assert!(matches!(
            fenced.effects.as_slice(),
            [SlotEffect::Retire {
                activation_id,
                lease: 9,
                retirement: RetirementMode::Graceful,
            }] if *activation_id == activation(2)
        ));
        let stale_termination = fenced.state.decide(SlotEvent::Terminated {
            activation_id: activation(1),
        });
        assert!(matches!(
            stale_termination.state,
            EntitySlot::Retiring { activation_id } if activation_id == activation(2)
        ));
        assert!(stale_termination.effects.as_slice().is_empty());
        let terminated = stale_termination.state.decide(SlotEvent::Terminated {
            activation_id: activation(2),
        });
        assert!(matches!(terminated.state, EntitySlot::Inactive));
        assert!(matches!(
            terminated.effects.as_slice(),
            [SlotEffect::Remove { activation_id }] if *activation_id == activation(2)
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

#[cfg(test)]
mod family_closure_tests {
    use super::{
        ActivatingSlot, ActivationId, ActivationWaiter, ActiveSlot, DispatchId, DrainFailure,
        DrainProgress, DrainStage, DrainingSlot, EntitySlot, Refusal, ReservationCount, SlotEffect,
    };
    use core::num::{NonZeroU64, NonZeroUsize};

    #[test]
    fn closed_active_slot_moves_exact_fence_inputs_without_clone() {
        let activation = ActivationId::new(NonZeroU64::MIN);
        let mut endpoint_values = vec![17_u8, 43];
        let endpoint_allocation = endpoint_values.as_ptr();
        let endpoint = endpoint_values.as_mut_slice();
        let lease = vec![31_u8, 61];
        let lease_allocation = lease.as_ptr();
        let state = EntitySlot::<Vec<u8>, _, _>::Active(ActiveSlot {
            activation_id: activation,
            endpoint,
            lease,
            reservations: ReservationCount::Drained,
        });
        let (state, effects, retirement) = state.close_family();
        let EntitySlot::Retiring { activation_id } = state else {
            panic!("retirement owns lease")
        };
        let Some((observed, endpoint, lease, fence)) = retirement else {
            panic!("original fence inputs")
        };
        assert_eq!(activation_id, activation);
        assert_eq!(observed, activation);
        assert_eq!(&*endpoint, &[17, 43]);
        assert_eq!(endpoint.as_ptr(), endpoint_allocation);
        assert_eq!(lease, [31, 61]);
        assert_eq!(lease.as_ptr(), lease_allocation);
        assert_eq!(fence, Ok(()));
        assert!(effects.as_slice().is_empty());
    }

    #[test]
    fn closed_unstarted_activation_returns_every_original_waiter() {
        let activation = ActivationId::new(NonZeroU64::MIN);
        let first = vec![17_u8, 43];
        let allocation = first.as_ptr();
        let first_dispatch = DispatchId::new(NonZeroU64::MIN);
        let second_dispatch = DispatchId::new(NonZeroU64::new(2).unwrap());
        let state = EntitySlot::<_, Vec<u8>, Vec<u8>>::Activating(ActivatingSlot {
            activation_id: activation,
            waiters: vec![
                ActivationWaiter {
                    dispatch_id: first_dispatch,
                    command: first,
                },
                ActivationWaiter {
                    dispatch_id: second_dispatch,
                    command: vec![31, 61],
                },
            ],
            waiter_limit: NonZeroUsize::new(2).unwrap(),
        });
        let (state, effects, retirement) = state.close_family();
        let [
            SlotEffect::Reject {
                dispatch_id: first,
                command: first_command,
                reason: first_reason,
            },
            SlotEffect::Reject {
                dispatch_id: second,
                command: second_command,
                reason: second_reason,
            },
            SlotEffect::Remove { activation_id },
        ] = effects.as_slice()
        else {
            panic!("whole ordered return")
        };
        assert!(matches!(state, EntitySlot::Inactive));
        assert!(retirement.is_none());
        assert_eq!(*first, first_dispatch);
        assert_eq!(*second, second_dispatch);
        assert_eq!(first_command, &[17, 43]);
        assert_eq!(first_command.as_ptr(), allocation);
        assert_eq!(second_command, &[31, 61]);
        assert_eq!(*first_reason, Refusal::Shutdown);
        assert_eq!(*second_reason, Refusal::Shutdown);
        assert_eq!(*activation_id, activation);
    }

    #[test]
    fn closed_incomplete_drain_retains_actual_stage_and_affine_lease() {
        let activation = ActivationId::new(NonZeroU64::MIN);
        for (progress, expected) in [
            (
                DrainProgress::Reservations(NonZeroUsize::new(2).unwrap()),
                DrainFailure {
                    stage: DrainStage::Reservations,
                    outstanding_reservations: 2,
                },
            ),
            (
                DrainProgress::FenceAcknowledgement,
                DrainFailure {
                    stage: DrainStage::FenceAcknowledgement,
                    outstanding_reservations: 0,
                },
            ),
        ] {
            let lease = vec![31_u8, 61];
            let allocation = lease.as_ptr();
            let mut endpoint_values = [17_u8, 43];
            let state = EntitySlot::<Vec<u8>, _, _>::Draining(DrainingSlot {
                activation_id: activation,
                endpoint: &mut endpoint_values[..],
                lease,
                progress,
            });
            let (state, effects, retirement) = state.close_family();
            let EntitySlot::Retiring { activation_id } = state else {
                panic!("exact retiring slot")
            };
            let Some((observed, endpoint, lease, fence)) = retirement else {
                panic!("available authority")
            };
            assert_eq!(activation_id, activation);
            assert_eq!(observed, activation);
            assert_eq!(&*endpoint, &[17, 43]);
            assert_eq!(lease, [31, 61]);
            assert_eq!(lease.as_ptr(), allocation);
            assert_eq!(fence, Err(expected));
            assert!(effects.as_slice().is_empty());
        }
    }
}
