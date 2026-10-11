//! Exact application-owned projection of local actor retirement.

use core::fmt;
use core::marker::PhantomData;
use core::ops::ControlFlow;
use std::any::Any;

use behavior::{
    Behavior, BehaviorAddr, BehaviorMessage, BehaviorSettlements, ChildRole, CreationId,
    CreationKind, EstablishedActor, Interpretation, InterpretationProgress, Never, Protocol,
    SourceCustody, SourceProgress, User,
};
use bombay_address::ClaimError;
use bombay_engine::{ActionsOf, Completion, DriverError, SettlementFailure};

use tokio::task::JoinError;

use crate::ActorExecutionOutcome;
use crate::address::MailAddr;
use crate::local::effects::ActionSettlementOf;
use crate::local::environment::{LocalActivationRejection, LocalResidual};
use crate::local::execution::{LocalRetirementRequest, OwnerCancellation};
use crate::termination::Termination;

pub(crate) type LocalOutcome<B, Descendants> = ActorExecutionOutcome<
    B,
    LocalResidual<B, User<BehaviorAddr<B>, BehaviorMessage<B>>, Descendants>,
    <B as Behavior>::Error,
    LocalActivationRejection<BehaviorAddr<B>>,
    LocalRetirementRequest,
>;

/// Exact allocated address of the application's root actor retirement.
pub struct RootOrigin<Owner> {
    address: MailAddr,
    owner: PhantomData<fn() -> Owner>,
}

impl<Owner> Copy for RootOrigin<Owner> {}

impl<Owner> Clone for RootOrigin<Owner> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Owner> RootOrigin<Owner> {
    pub(crate) const fn new(address: MailAddr) -> Self {
        Self {
            address,
            owner: PhantomData,
        }
    }

    /// Return the exact allocated address of the root incarnation.
    #[must_use]
    pub const fn address(&self) -> MailAddr {
        self.address
    }
}

impl<Owner> core::fmt::Debug for RootOrigin<Owner> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("RootOrigin")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl<Owner> PartialEq for RootOrigin<Owner> {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address
    }
}

impl<Owner> Eq for RootOrigin<Owner> {}

/// Exact allocated address, creator-local route and creation provenance of a child retirement.
/// The owner and role preserve the statically declared application meaning.
pub struct ChildOrigin<Owner, Role> {
    address: MailAddr,
    nonce: u64,
    creation_id: CreationId,
    creation_kind: CreationKind,
    declaration: PhantomData<fn() -> (Owner, Role)>,
}

impl<Owner, Role> Copy for ChildOrigin<Owner, Role> {}

impl<Owner, Role> Clone for ChildOrigin<Owner, Role> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Owner, Role> ChildOrigin<Owner, Role> {
    pub(crate) const fn new(
        address: MailAddr,
        nonce: u64,
        creation_id: CreationId,
        creation_kind: CreationKind,
    ) -> Self {
        Self {
            address,
            nonce,
            creation_id,
            creation_kind,
            declaration: PhantomData,
        }
    }

    /// Return the exact allocated address of the child incarnation.
    #[must_use]
    pub const fn address(&self) -> MailAddr {
        self.address
    }

    /// Return the creator-local nonce of this child occurrence.
    #[must_use]
    pub const fn nonce(&self) -> u64 {
        self.nonce
    }
    /// Return the original creator-local identity of this child creation.
    #[must_use]
    pub const fn creation_id(&self) -> CreationId {
        self.creation_id
    }

    /// Return the explicit original birth or replacement provenance.
    #[must_use]
    pub const fn creation_kind(&self) -> CreationKind {
        self.creation_kind
    }
}

impl<Owner, Position> ChildOrigin<Owner, Position>
where
    Owner: Behavior,
{
    /// Convert a structural occurrence into its statically proven child role.
    #[doc(hidden)]
    #[must_use]
    pub const fn into_declared_child<Role>(self) -> ChildOrigin<Owner, Role>
    where
        Role: ChildRole<Owner, Position = Position>,
    {
        ChildOrigin {
            address: self.address,
            nonce: self.nonce,
            creation_id: self.creation_id,
            creation_kind: self.creation_kind,
            declaration: PhantomData,
        }
    }
}

impl<Owner, Role> core::fmt::Debug for ChildOrigin<Owner, Role> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ChildOrigin")
            .field("address", &self.address)
            .field("nonce", &self.nonce)
            .field("creation_id", &self.creation_id)
            .field("creation_kind", &self.creation_kind)
            .finish_non_exhaustive()
    }
}

impl<Owner, Role> PartialEq for ChildOrigin<Owner, Role> {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address
            && self.nonce == other.nonce
            && self.creation_id == other.creation_id
            && self.creation_kind == other.creation_kind
    }
}

impl<Owner, Role> Eq for ChildOrigin<Owner, Role> {}

/// Native actor projection; child raw descendants are selected before launch.
/// Pure owned presentation uses `From<(Origin, Retirement)>`.
pub trait ProjectTerminal<Origin, Actor, ChildFailures>
where
    Actor: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    /// The descendant report family owned by the child before projection.
    type Descendants;

    /// Consume the exact origin and complete retirement in the native policy context.
    fn project(
        origin: Origin,
        terminal: ActorRetirement<Actor, Self::Descendants, ChildFailures>,
    ) -> Self;
}

/// Available failure of one exact child creation or installed incarnation.
/// The original child is owned by its Core creation receipt or was consumed
/// inside the failed task; this value never claims to reconstruct it.
#[must_use = "the child failure must be inspected or explicitly discharged"]
pub enum ChildFailure<Origin, Child>
where
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    InitializationPanicked {
        origin: Origin,
        payload: Box<dyn Any + Send>,
        additional_failures: Vec<DriverError<Child::Error, LocalActivationRejection<MailAddr>>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
    },
    /// Startup cleanup owns independent available facts after Core received the child input.
    StartupRetirementFailed {
        origin: Origin,
        primary_failure: Option<DriverError<Child::Error, LocalActivationRejection<MailAddr>>>,
        additional_failures: Vec<DriverError<Child::Error, LocalActivationRejection<MailAddr>>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
    },
    StartupActorTaskFailed {
        origin: Origin,
        error: JoinError,
    },
    StartupProjectionTaskFailed {
        origin: Origin,
        error: JoinError,
    },
    ActorTaskFailed {
        origin: Origin,
        actor: EstablishedActor<Child>,
        error: JoinError,
    },
    ProjectionTaskFailed {
        origin: Origin,
        actor: EstablishedActor<Child>,
        error: JoinError,
    },
}

impl<Origin, Child> fmt::Debug for ChildFailure<Origin, Child>
where
    Origin: fmt::Debug,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InitializationPanicked { origin, .. } => formatter
                .debug_struct("InitializationPanicked")
                .field("origin", origin)
                .finish_non_exhaustive(),
            Self::StartupRetirementFailed { origin, .. } => formatter
                .debug_struct("StartupRetirementFailed")
                .field("origin", origin)
                .finish_non_exhaustive(),
            Self::StartupActorTaskFailed { origin, error } => formatter
                .debug_struct("StartupActorTaskFailed")
                .field("origin", origin)
                .field("error", error)
                .finish_non_exhaustive(),
            Self::StartupProjectionTaskFailed { origin, error } => formatter
                .debug_struct("StartupProjectionTaskFailed")
                .field("origin", origin)
                .field("error", error)
                .finish_non_exhaustive(),
            Self::ActorTaskFailed { origin, error, .. } => formatter
                .debug_struct("ActorTaskFailed")
                .field("origin", origin)
                .field("error", error)
                .finish_non_exhaustive(),
            Self::ProjectionTaskFailed { origin, error, .. } => formatter
                .debug_struct("ProjectionTaskFailed")
                .field("origin", origin)
                .field("error", error)
                .finish_non_exhaustive(),
        }
    }
}

/// The complete coexisting custody retained after actor runtime retirement.
///
/// This product owns the original actor, unread communications, descendants,
/// child failures and every acquired runtime lane. Primary outcomes and
/// phase-specific unfinished actions remain in [`ActorRetirement`]. No field
/// classifies success or grants new lifecycle authority.
#[must_use = "retained actor custody must be inspected or explicitly discharged"]
pub struct ActorCustody<BehaviorState, Root, ChildFailures>
where
    BehaviorState: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = behavior::Never>,
{
    pub behavior: BehaviorState,
    pub control: Vec<BehaviorState::Event>,
    pub user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
    pub descendants: Vec<Root>,
    pub child_failures: ChildFailures,
    pub capability_failures: Vec<JoinError>,
    pub additional_failures:
        Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
    pub received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
    pub received_source: Option<SourceCustody<BehaviorState::Settlements>>,
    pub source_index: Option<usize>,
    /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
    /// passively reports the original zero-field owner-cancellation fact;
    /// Break(Err(error)) owns the original capability `JoinError`. This report
    /// grants no cancellation sender authority.
    #[expect(
        clippy::type_complexity,
        reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
    )]
    pub acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
    pub retirement_failures: Vec<Box<dyn Any + Send>>,
    pub terminal_report: Option<Result<(), Termination<MailAddr>>>,
    pub unread_owner_cancellation: Option<()>,
}

/// Complete retirement of one local actor.
///
/// Every variant either retains final owned state or names the executor event
/// that made such custody unavailable. `CapabilityFailed` owns the first task
/// failure acquired as the primary cause. `capability_failures` retains other
/// task failures joined during cleanup without changing that cause.
/// `unread_owner_cancellation` records an accepted owner request not acquired by
/// execution. Its unit carries every field of the private zero-field request;
/// the report conveys no further cancellation authority.
#[must_use = "the exact actor retirement must be inspected or explicitly discharged"]
pub enum ActorRetirement<BehaviorState, Root, ChildFailures>
where
    BehaviorState: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = behavior::Never>,
{
    AllocationRejected {
        behavior: BehaviorState,
        reason: behavior::AllocationRejection,
    },
    InitializationRejected {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        error: BehaviorState::Error,
    },
    InitializationPanicked {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        payload: Box<dyn Any + Send>,
    },
    HostCommitPanicked {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        initialization: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        payload: Box<dyn Any + Send>,
    },
    TransitionPanicked {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        payload: Box<dyn Any + Send>,
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
    },
    HostExecutionPanicked {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        payload: Box<dyn Any + Send>,
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
    },
    HostRejected {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        initialization: InterpretationProgress<
            ActionsOf<BehaviorState>,
            BehaviorState::InterpretationCustody,
            BehaviorState::Settlements,
        >,
        error: ClaimError<MailAddr>,
    },
    BindingAbandoned {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        initialization: InterpretationProgress<
            ActionsOf<BehaviorState>,
            BehaviorState::InterpretationCustody,
            BehaviorState::Settlements,
        >,
    },
    Completed {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        completion: Completion,
    },
    BehaviorFailed {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        error: BehaviorState::Error,
    },
    EffectsFailed {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        initialization: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        error: SettlementFailure,
    },
    OwnerCancelled {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
    },
    /// Live execution acquired this exact capability task failure as its primary cause.
    CapabilityFailed {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        error: JoinError,
    },
    PreparedRetirementPanicked {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        payload: Box<dyn Any + Send>,
    },
    PreparedInterpreterContractFailed {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
    },
    InitializationRetirementPanicked {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        initialization: InterpretationProgress<
            ActionsOf<BehaviorState>,
            BehaviorState::InterpretationCustody,
            BehaviorState::Settlements,
        >,
        payload: Box<dyn Any + Send>,
    },
    InitializationInterpreterContractFailed {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        initialization: InterpretationProgress<
            ActionsOf<BehaviorState>,
            BehaviorState::InterpretationCustody,
            BehaviorState::Settlements,
        >,
    },
    ActiveRetirementPanicked {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        payload: Box<dyn Any + Send>,
    },
    ActiveActivationPanicked {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        payload: Box<dyn Any + Send>,
    },
    ActiveInterpreterContractFailed {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
    },
    PreparedEffectsFailed {
        custody: ActorCustody<BehaviorState, Root, ChildFailures>,
        error: SettlementFailure,
    },
    /// The executor returned no actor state or residual; the original task failure survives.
    ActorTaskFailed(JoinError),
    Panicked,
    Cancelled,
}

/// Project the exact private request into a passive existing standard sum.
/// This consumes no event or failure and reconstructs no cancellation authority.
pub(crate) fn retirement_ingress<E>(
    ingress: Option<ControlFlow<LocalRetirementRequest, Option<E>>>,
) -> Option<ControlFlow<Result<(), JoinError>, Option<E>>> {
    ingress.map(|ingress| match ingress {
        ControlFlow::Continue(event) => ControlFlow::Continue(event),
        ControlFlow::Break(LocalRetirementRequest::OwnerCancellation(OwnerCancellation)) => {
            ControlFlow::Break(Ok(()))
        }
        ControlFlow::Break(LocalRetirementRequest::CapabilityFailed(error)) => {
            ControlFlow::Break(Err(error))
        }
    })
}

impl<BehaviorState, Root, ChildFailures> ActorRetirement<BehaviorState, Root, ChildFailures>
where
    BehaviorState: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = behavior::Never>,
{
    /// Borrow the actor's normal completion without consuming its retirement.
    ///
    /// Only `Completed` returns a reason, distinguishing explicit stop from
    /// source exhaustion. Failure and owner cancellation return `None` while
    /// retaining their complete original values. A completion reason does not
    /// certify that descendants or cleanup have no independent failures.
    #[must_use]
    pub const fn completion(&self) -> Option<&Completion> {
        match self {
            Self::Completed { completion, .. } => Some(completion),
            Self::AllocationRejected { .. }
            | Self::InitializationRejected { .. }
            | Self::InitializationPanicked { .. }
            | Self::HostCommitPanicked { .. }
            | Self::TransitionPanicked { .. }
            | Self::HostExecutionPanicked { .. }
            | Self::HostRejected { .. }
            | Self::BindingAbandoned { .. }
            | Self::BehaviorFailed { .. }
            | Self::EffectsFailed { .. }
            | Self::OwnerCancelled { .. }
            | Self::CapabilityFailed { .. }
            | Self::PreparedRetirementPanicked { .. }
            | Self::PreparedInterpreterContractFailed { .. }
            | Self::InitializationRetirementPanicked { .. }
            | Self::InitializationInterpreterContractFailed { .. }
            | Self::ActiveRetirementPanicked { .. }
            | Self::ActiveActivationPanicked { .. }
            | Self::ActiveInterpreterContractFailed { .. }
            | Self::PreparedEffectsFailed { .. }
            | Self::ActorTaskFailed(_)
            | Self::Panicked
            | Self::Cancelled => None,
        }
    }

    /// Borrow the exact actor state retained by this retirement.
    ///
    /// State remains available after allocation, initialization and execution
    /// failures as well as normal completion. Its presence does not certify
    /// success. Only `ActorTaskFailed`, `Panicked` and `Cancelled` lack state.
    /// This inspection leaves every owned value and cause in the retirement.
    #[must_use]
    pub const fn behavior(&self) -> Option<&BehaviorState> {
        if let Self::AllocationRejected { behavior, .. } = self {
            return Some(behavior);
        }
        match self.custody() {
            Some(custody) => Some(&custody.behavior),
            None => None,
        }
    }

    /// Borrow every coexisting actor and runtime custody value.
    ///
    /// Allocation refusal precedes runtime custody. Executor failure, panic
    /// and cancellation without returned state also have no custody product.
    /// Presence does not certify success; the exact outcome remains in `self`.
    ///
    /// ```
    /// struct Total(u32);
    /// #[bombay::actor]
    /// impl Total {
    ///     fn receive(&mut self, value: u32) -> bombay::behavior::BehaviorActed<Self> {
    ///         self.0 = value;
    ///         Ok(bombay::behavior::Actions::cont())
    ///     }
    /// }
    /// fn inspect(retirement: &bombay::ActorRetirement<Total, bombay::behavior::Never, ()>) {
    ///     if let Some(custody) = retirement.custody() {
    ///         let unread: &[bombay::behavior::User<bombay::MailAddr, u32>] = &custody.user;
    ///         let total = custody.behavior.0;
    ///         // The complete retirement, including its cause, remains owned by the caller.
    ///     }
    /// }
    /// ```
    #[must_use]
    pub const fn custody(&self) -> Option<&ActorCustody<BehaviorState, Root, ChildFailures>> {
        match self {
            Self::InitializationRejected { custody, .. }
            | Self::InitializationPanicked { custody, .. }
            | Self::HostCommitPanicked { custody, .. }
            | Self::TransitionPanicked { custody, .. }
            | Self::HostExecutionPanicked { custody, .. }
            | Self::HostRejected { custody, .. }
            | Self::BindingAbandoned { custody, .. }
            | Self::Completed { custody, .. }
            | Self::BehaviorFailed { custody, .. }
            | Self::EffectsFailed { custody, .. }
            | Self::OwnerCancelled { custody, .. }
            | Self::CapabilityFailed { custody, .. }
            | Self::PreparedRetirementPanicked { custody, .. }
            | Self::PreparedInterpreterContractFailed { custody, .. }
            | Self::InitializationRetirementPanicked { custody, .. }
            | Self::InitializationInterpreterContractFailed { custody, .. }
            | Self::ActiveRetirementPanicked { custody, .. }
            | Self::ActiveActivationPanicked { custody, .. }
            | Self::ActiveInterpreterContractFailed { custody, .. }
            | Self::PreparedEffectsFailed { custody, .. } => Some(custody),
            Self::AllocationRejected { .. }
            | Self::ActorTaskFailed(_)
            | Self::Panicked
            | Self::Cancelled => None,
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one closed Completion conversion retains every original typed field and invariant; another forwarding function would only relocate the same conservation law"
    )]
    fn from_completed(outcome: LocalOutcome<BehaviorState, (Vec<Root>, ChildFailures)>) -> Self {
        let ActorExecutionOutcome::Completed {
            behavior,
            residual,
            completion,
            additional_failures,
        } = outcome
        else {
            unreachable!("the completed terminal projection received another outcome")
        };
        let LocalResidual::Retired {
            interpretation,
            source,
            settlements,
            ingress,
            activation_tasks,
            descendants: (descendants, child_failures),
            capability_failures,
            terminal_report,
            retirement_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            unread_owner_cancellation,
        } = residual
        else {
            unreachable!("standard completion follows actual active retirement")
        };
        assert!(
            activation_tasks.is_empty(),
            "terminal projection follows every original capability task join"
        );
        let completion = match completion {
            Completion::Stopped => Completion::Stopped,
            Completion::Exhausted => Completion::Exhausted,
            Completion::RetirementRequested(LocalRetirementRequest::OwnerCancellation(
                OwnerCancellation,
            )) => {
                return Self::OwnerCancelled {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    interpretation,
                    source,
                    settlements,
                };
            }
            Completion::RetirementRequested(LocalRetirementRequest::CapabilityFailed(error)) => {
                return Self::CapabilityFailed {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    interpretation,
                    source,
                    settlements,
                    error,
                };
            }
        };
        Self::Completed {
            custody: ActorCustody {
                behavior,
                control: ingress.control,
                user: ingress.user,
                descendants,
                child_failures,
                capability_failures,
                additional_failures,
                terminal_report,
                retirement_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress: retirement_ingress(acquired_ingress),
                unread_owner_cancellation,
            },
            interpretation,
            source,
            settlements,
            completion,
        }
    }
    /// Project each actual received residual phase without discarding current
    /// progress, a refused report or coexisting execution/retirement failures.
    #[expect(
        clippy::too_many_lines,
        reason = "one exhaustive owning phase/failure conversion preserves every original typed field; forwarding functions or aliases would only relocate the same conservation obligation"
    )]
    pub(crate) fn from_local(
        outcome: LocalOutcome<BehaviorState, (Vec<Root>, ChildFailures)>,
    ) -> Self {
        match outcome {
            completed @ ActorExecutionOutcome::Completed { .. } => Self::from_completed(completed),
            ActorExecutionOutcome::BehaviorFailed {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                error,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows every original capability task join"
                );
                Self::InitializationRejected {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    error,
                }
            }
            ActorExecutionOutcome::BehaviorFailed {
                behavior,
                residual:
                    LocalResidual::Retired {
                        interpretation,
                        source,
                        settlements,
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                error,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows every original capability task join"
                );
                Self::BehaviorFailed {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    interpretation,
                    source,
                    settlements,
                    error,
                }
            }
            ActorExecutionOutcome::InitializationPanicked {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                payload,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows every original capability task join"
                );
                Self::InitializationPanicked {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    payload,
                }
            }
            ActorExecutionOutcome::TransitionPanicked {
                behavior,
                residual:
                    LocalResidual::Retired {
                        interpretation,
                        source,
                        settlements,
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                payload,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows every original capability task join"
                );
                Self::TransitionPanicked {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    interpretation,
                    source,
                    settlements,
                    payload,
                }
            }
            ActorExecutionOutcome::HostExecutionPanicked {
                behavior,
                residual:
                    LocalResidual::Retired {
                        interpretation,
                        source,
                        settlements,
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                payload,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows every original capability task join"
                );
                Self::HostExecutionPanicked {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    interpretation,
                    source,
                    settlements,
                    payload,
                }
            }
            ActorExecutionOutcome::ActivationPanicked {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                payload,
            }
            | ActorExecutionOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                error: LocalActivationRejection::HostCommitPanicked(payload),
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows every original capability task join"
                );
                Self::HostCommitPanicked {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    initialization: None,
                    payload,
                }
            }
            ActorExecutionOutcome::ActivationPanicked {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                payload,
            }
            | ActorExecutionOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                error: LocalActivationRejection::HostCommitPanicked(payload),
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows every original capability task join"
                );
                Self::HostCommitPanicked {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    initialization: Some(initialization),
                    payload,
                }
            }
            ActorExecutionOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                error: LocalActivationRejection::Address(error),
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows every original capability task join"
                );
                Self::HostRejected {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    initialization,
                    error,
                }
            }
            ActorExecutionOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                error: LocalActivationRejection::BindingAbandoned,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows every original capability task join"
                );
                Self::BindingAbandoned {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    initialization,
                }
            }
            ActorExecutionOutcome::SettlementFailed {
                behavior,
                residual:
                    LocalResidual::Retired {
                        interpretation,
                        source,
                        settlements,
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                error,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows every original capability task join"
                );
                Self::EffectsFailed {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    interpretation,
                    source,
                    settlements,
                    initialization: None,
                    error,
                }
            }
            ActorExecutionOutcome::SettlementFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                error,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows every original capability task join"
                );
                Self::EffectsFailed {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    interpretation: None,
                    source: None,
                    settlements: Vec::new(),
                    initialization: Some(initialization),
                    error,
                }
            }
            ActorExecutionOutcome::SettlementFailed {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                error,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows every original capability task join"
                );
                Self::PreparedEffectsFailed {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    error,
                }
            }
            ActorExecutionOutcome::InterpreterContractFailed {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "capability tasks must join before terminal projection"
                );
                Self::PreparedInterpreterContractFailed {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                }
            }
            ActorExecutionOutcome::InterpreterContractFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "capability tasks must join before terminal projection"
                );
                Self::InitializationInterpreterContractFailed {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    initialization,
                }
            }
            ActorExecutionOutcome::InterpreterContractFailed {
                behavior,
                residual:
                    LocalResidual::Retired {
                        interpretation,
                        source,
                        settlements,
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "capability tasks must join before terminal projection"
                );
                Self::ActiveInterpreterContractFailed {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    interpretation,
                    source,
                    settlements,
                }
            }
            ActorExecutionOutcome::RetirementPanicked {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                payload,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows every original capability task join"
                );
                Self::PreparedRetirementPanicked {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    payload,
                }
            }
            ActorExecutionOutcome::RetirementPanicked {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                payload,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows every original capability task join"
                );
                Self::InitializationRetirementPanicked {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    initialization,
                    payload,
                }
            }
            ActorExecutionOutcome::RetirementPanicked {
                behavior,
                residual:
                    LocalResidual::Retired {
                        interpretation,
                        source,
                        settlements,
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                payload,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows every original capability task join"
                );
                Self::ActiveRetirementPanicked {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    interpretation,
                    source,
                    settlements,
                    payload,
                }
            }
            ActorExecutionOutcome::ActivationPanicked {
                behavior,
                residual:
                    LocalResidual::Retired {
                        interpretation,
                        source,
                        settlements,
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        unread_owner_cancellation,
                    },
                additional_failures,
                payload,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows every original capability task join"
                );
                Self::ActiveActivationPanicked {
                    custody: ActorCustody {
                        behavior,
                        control: ingress.control,
                        user: ingress.user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress: retirement_ingress(acquired_ingress),
                        unread_owner_cancellation,
                    },
                    interpretation,
                    source,
                    settlements,
                    payload,
                }
            }
            ActorExecutionOutcome::Panicked => Self::Panicked,
            ActorExecutionOutcome::Cancelled => Self::Cancelled,
            ActorExecutionOutcome::BehaviorFailed {
                residual: LocalResidual::Uncommitted { .. },
                ..
            }
            | ActorExecutionOutcome::InitializationPanicked {
                residual: LocalResidual::Uncommitted { .. } | LocalResidual::Retired { .. },
                ..
            }
            | ActorExecutionOutcome::TransitionPanicked {
                residual: LocalResidual::Prepared { .. } | LocalResidual::Uncommitted { .. },
                ..
            }
            | ActorExecutionOutcome::HostExecutionPanicked {
                residual: LocalResidual::Prepared { .. } | LocalResidual::Uncommitted { .. },
                ..
            }
            | ActorExecutionOutcome::ActivationFailed { .. } => {
                unreachable!("the standard local receiver cannot produce this residual phase")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use core::ptr;
    use std::future;
    use std::panic::resume_unwind;
    use tokio::runtime::Builder;

    struct StartupChild;
    #[crate::actor(message = behavior::Never)]
    impl StartupChild {}

    #[test]
    fn startup_actor_task_failure_retains_exact_native_error_without_endpoint_authority() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("one native actor task executor");
        let error = runtime.block_on(async {
            let task = tokio::spawn(future::pending::<()>());
            task.abort();
            task.await.expect_err("original native task was cancelled")
        });
        let original_task = error.id();
        let mut sequence = behavior::CreationSequence::new();
        let id = sequence.issue().expect("one fresh creation");
        let previous = sequence.issue().expect("one explicit predecessor");
        let origin = ChildOrigin::<Root, behavior::ChildHead>::new(
            MailAddr(17),
            23,
            id,
            CreationKind::replacement(previous),
        );
        let failure = ChildFailure::<_, StartupChild>::StartupActorTaskFailed { origin, error };
        let ChildFailure::StartupActorTaskFailed {
            origin: received_origin,
            error,
        } = failure
        else {
            panic!("original startup task cause survives");
        };
        assert_eq!(received_origin.creation_id(), id);
        assert_eq!(
            received_origin.creation_kind(),
            behavior::CreationKind::replacement(previous)
        );
        assert_eq!(received_origin, origin);
        assert_eq!(error.id(), original_task);
        assert!(error.is_cancelled());
    }

    #[test]
    fn startup_projection_task_failure_returns_original_native_payload_without_endpoint_authority()
    {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("one native projection executor");
        let payload = Box::new(String::from("original startup projection cause"));
        let original_allocation = ptr::from_ref(payload.as_ref()).addr();
        let error = runtime.block_on(async move {
            let task = tokio::spawn(async move {
                resume_unwind(payload);
            });
            task.await.expect_err("original projector failed")
        });
        let original_task = error.id();
        let mut sequence = behavior::CreationSequence::new();
        let id = sequence.issue().expect("one fresh creation");
        let origin = ChildOrigin::<Root, behavior::ChildHead>::new(
            MailAddr(29),
            31,
            id,
            CreationKind::Birth,
        );
        let failure =
            ChildFailure::<_, StartupChild>::StartupProjectionTaskFailed { origin, error };
        let ChildFailure::StartupProjectionTaskFailed {
            origin: received_origin,
            error,
        } = failure
        else {
            panic!("original startup projection cause survives");
        };
        assert_eq!(received_origin.creation_id(), id);
        assert_eq!(
            received_origin.creation_kind(),
            behavior::CreationKind::Birth
        );
        assert_eq!(received_origin, origin);
        assert_eq!(error.id(), original_task);
        let payload = error.into_panic();
        assert_eq!(
            ptr::from_ref(payload.as_ref()).cast::<()>().addr(),
            original_allocation
        );
    }

    struct Root;
    struct ChildRole;

    #[test]
    fn root_and_child_origins_keep_distinct_exact_payloads() {
        let id = behavior::CreationSequence::new()
            .issue()
            .expect("one exact origin identity");
        let root = RootOrigin::<Root>::new(MailAddr(1));
        let child = ChildOrigin::<Root, ChildRole>::new(MailAddr(7), 13, id, CreationKind::Birth);
        assert_eq!(root.address(), MailAddr(1));
        assert_eq!(child.address(), MailAddr(7));
        assert_eq!(child.nonce(), 13);
    }

    #[test]
    fn root_origin_identity_and_diagnostic_preserve_its_address() {
        let first = RootOrigin::<Root>::new(MailAddr(1));
        let same = RootOrigin::<Root>::new(MailAddr(1));
        let other = RootOrigin::<Root>::new(MailAddr(2));

        assert_eq!(first, same);
        assert_ne!(first, other);
        let diagnostic = format!("{first:?}");
        assert!(diagnostic.contains("RootOrigin"));
        assert!(diagnostic.contains("MailAddr(1)"));
    }

    #[test]
    fn child_origin_identity_and_diagnostic_preserve_address_and_nonce() {
        let id = behavior::CreationSequence::new()
            .issue()
            .expect("one exact origin identity");
        let first = ChildOrigin::<Root, ChildRole>::new(MailAddr(7), 13, id, CreationKind::Birth);
        let same = ChildOrigin::<Root, ChildRole>::new(MailAddr(7), 13, id, CreationKind::Birth);
        let other_address =
            ChildOrigin::<Root, ChildRole>::new(MailAddr(8), 13, id, CreationKind::Birth);
        let other_nonce =
            ChildOrigin::<Root, ChildRole>::new(MailAddr(7), 14, id, CreationKind::Birth);

        assert_eq!(first, same);
        assert_ne!(first, other_address);
        assert_ne!(first, other_nonce);
        let mut different_ids = behavior::CreationSequence::new();
        let _ = different_ids.issue().expect("first correlation");
        let other_id = different_ids.issue().expect("different correlation");
        let same_route_other_id =
            ChildOrigin::<Root, ChildRole>::new(MailAddr(7), 13, other_id, CreationKind::Birth);
        let same_route_other_kind = ChildOrigin::<Root, ChildRole>::new(
            MailAddr(7),
            13,
            id,
            CreationKind::replacement(other_id),
        );
        assert_ne!(first, same_route_other_id);
        assert_ne!(first, same_route_other_kind);
        assert_eq!(first.creation_id(), id);
        assert_eq!(first.creation_kind(), CreationKind::Birth);
        let diagnostic = format!("{first:?}");
        assert!(diagnostic.contains("ChildOrigin"));
        assert!(diagnostic.contains("MailAddr(7)"));
        assert!(diagnostic.contains("nonce: 13"));
    }
}

#[cfg(test)]
mod capability_retirement_projection {
    use crate::ActorCustody;

    use std::panic::panic_any;

    use behavior::{
        Actions, ActiveTurn, BehaviorActed, InterpretationProgress, MessageProtocol, Never,
        NoBirths, NoSends, Step, User,
    };
    use communication::Drained;
    use tokio::{sync::oneshot, task};

    use crate::ActorExecutionOutcome;
    use crate::MailAddr;
    use crate::local::environment::{LocalActivationRejection, LocalResidual};
    use crate::local::execution::{ActivationTasks, LocalRetirementRequest, OwnerCancellation};
    use crate::terminal::{ActorRetirement, LocalOutcome};
    use bombay_engine::Completion;

    struct RetiringActor {
        values: Vec<u64>,
    }

    impl behavior::Behavior for RetiringActor {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = User<MailAddr, Never>;
        type Sends = NoSends;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;

        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            match event.message {}
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    enum RetirementCause {
        InitializationPanicked,
        BindingAbandoned,
        Stopped,
        Exhausted,
        OwnerCancellation,
        CapabilityFailed,
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one complete typed projection trace observes all three residual phases and their separate primary causes"
    )]
    async fn every_residual_phase_preserves_later_failure_and_unread_owner_in_public_retirement() {
        for cause in [
            RetirementCause::InitializationPanicked,
            RetirementCause::BindingAbandoned,
            RetirementCause::Stopped,
            RetirementCause::Exhausted,
            RetirementCause::OwnerCancellation,
            RetirementCause::CapabilityFailed,
        ] {
            let values = vec![77, 177];
            let allocation = values.as_ptr();
            let panic_values = Box::new(vec![88_u64, 188]);
            let (late_started, late_received) = oneshot::channel();
            let mut tasks = ActivationTasks::new();
            tasks.spawn(async move {
                let notice = late_started.send(task::id());
                notice.expect("the late task identity observer remains live");
                panic_any(panic_values)
            });
            let behavior = RetiringActor { values };
            let ingress = Drained {
                control: Vec::new(),
                user: Vec::new(),
            };
            let residual = match cause {
                RetirementCause::InitializationPanicked => LocalResidual::Prepared {
                    received_interpretation: None,
                    received_source: None,
                    source_index: None,
                    acquired_ingress: None,
                    retirement_failures: Vec::new(),
                    ingress,
                    activation_tasks: tasks,
                    descendants: (vec![99], ()),
                    capability_failures: Vec::new(),
                    terminal_report: None,
                    unread_owner_cancellation: Some(()),
                },
                RetirementCause::BindingAbandoned => LocalResidual::Uncommitted {
                    initialization: InterpretationProgress::Original(Actions::cont()),
                    received_interpretation: None,
                    received_source: None,
                    source_index: None,
                    acquired_ingress: None,
                    retirement_failures: Vec::new(),
                    ingress,
                    activation_tasks: tasks,
                    descendants: (vec![99], ()),
                    capability_failures: Vec::new(),
                    terminal_report: None,
                    unread_owner_cancellation: Some(()),
                },
                RetirementCause::Stopped
                | RetirementCause::Exhausted
                | RetirementCause::OwnerCancellation
                | RetirementCause::CapabilityFailed => LocalResidual::Retired {
                    interpretation: None,
                    source: None,
                    settlements: Vec::new(),
                    received_interpretation: None,
                    received_source: None,
                    source_index: None,
                    acquired_ingress: None,
                    retirement_failures: Vec::new(),
                    ingress,
                    activation_tasks: tasks,
                    descendants: (vec![99], ()),
                    capability_failures: Vec::new(),
                    terminal_report: None,
                    unread_owner_cancellation: match cause {
                        RetirementCause::OwnerCancellation => None,
                        _ => Some(()),
                    },
                },
            };
            let residual = residual.settle_activation_tasks().await;
            let late_task_id = late_received
                .await
                .expect("the joined late task issued its identity");
            let mut acquired_primary_id = None;
            let outcome: LocalOutcome<RetiringActor, (Vec<u64>, ())> = match cause {
                RetirementCause::InitializationPanicked => {
                    ActorExecutionOutcome::InitializationPanicked {
                        additional_failures: Vec::new(),
                        payload: Box::new(()),
                        behavior,
                        residual,
                    }
                }
                RetirementCause::BindingAbandoned => ActorExecutionOutcome::ActivationFailed {
                    additional_failures: Vec::new(),
                    behavior,
                    residual,
                    error: LocalActivationRejection::BindingAbandoned,
                },
                RetirementCause::Stopped => ActorExecutionOutcome::Completed {
                    additional_failures: Vec::new(),
                    behavior,
                    residual,
                    completion: Completion::Stopped,
                },
                RetirementCause::Exhausted => ActorExecutionOutcome::Completed {
                    additional_failures: Vec::new(),
                    behavior,
                    residual,
                    completion: Completion::Exhausted,
                },
                RetirementCause::OwnerCancellation => ActorExecutionOutcome::Completed {
                    additional_failures: Vec::new(),
                    behavior,
                    residual,
                    completion: Completion::RetirementRequested(
                        LocalRetirementRequest::OwnerCancellation(OwnerCancellation),
                    ),
                },
                RetirementCause::CapabilityFailed => {
                    let primary_task =
                        tokio::spawn(async { panic_any(Box::new(vec![66_u64, 166])) });
                    let primary_task_id = primary_task.id();
                    let primary = primary_task
                        .await
                        .expect_err("the primary task failure is retained");
                    assert_eq!(primary.id(), primary_task_id);
                    acquired_primary_id = Some(primary.id());
                    ActorExecutionOutcome::Completed {
                        additional_failures: Vec::new(),
                        behavior,
                        residual,
                        completion: Completion::RetirementRequested(
                            LocalRetirementRequest::CapabilityFailed(primary),
                        ),
                    }
                }
            };
            let retirement = ActorRetirement::from_local(outcome);
            let completion = retirement.completion();
            match cause {
                RetirementCause::Stopped => assert_eq!(completion, Some(&Completion::Stopped)),
                RetirementCause::Exhausted => {
                    assert_eq!(completion, Some(&Completion::Exhausted));
                }
                RetirementCause::InitializationPanicked
                | RetirementCause::BindingAbandoned
                | RetirementCause::OwnerCancellation
                | RetirementCause::CapabilityFailed => assert!(completion.is_none()),
            }
            let (behavior, control, user, descendants, mut failures, terminal_report, unread) =
                match retirement {
                    ActorRetirement::InitializationPanicked {
                        custody:
                            ActorCustody {
                                behavior,
                                control,
                                user,
                                descendants,
                                child_failures: (),
                                capability_failures,
                                terminal_report,
                                unread_owner_cancellation,
                                additional_failures,
                                received_interpretation,
                                received_source,
                                source_index,
                                acquired_ingress,
                                retirement_failures,
                            },
                        payload: _,
                    } => {
                        assert!(additional_failures.is_empty());
                        assert!(received_interpretation.is_none());
                        assert!(received_source.is_none());
                        assert!(source_index.is_none());
                        assert!(acquired_ingress.is_none());
                        assert!(retirement_failures.is_empty());
                        assert_eq!(cause, RetirementCause::InitializationPanicked);
                        (
                            behavior,
                            control,
                            user,
                            descendants,
                            capability_failures,
                            terminal_report,
                            unread_owner_cancellation,
                        )
                    }
                    ActorRetirement::BindingAbandoned {
                        custody:
                            ActorCustody {
                                behavior,
                                control,
                                user,
                                descendants,
                                child_failures: (),
                                capability_failures,
                                terminal_report,
                                unread_owner_cancellation,
                                additional_failures,
                                received_interpretation,
                                received_source,
                                source_index,
                                acquired_ingress,
                                retirement_failures,
                            },
                        initialization,
                    } => {
                        assert!(additional_failures.is_empty());
                        assert!(received_interpretation.is_none());
                        assert!(received_source.is_none());
                        assert!(source_index.is_none());
                        assert!(acquired_ingress.is_none());
                        assert!(retirement_failures.is_empty());
                        assert_eq!(cause, RetirementCause::BindingAbandoned);
                        let InterpretationProgress::Original(initialization) = initialization
                        else {
                            panic!("rejected binding retains the original complete initialization");
                        };
                        assert_eq!(initialization.sends, NoSends);
                        assert!(initialization.creates.is_empty());
                        assert!(matches!(initialization.become_, Step::Continue));
                        (
                            behavior,
                            control,
                            user,
                            descendants,
                            capability_failures,
                            terminal_report,
                            unread_owner_cancellation,
                        )
                    }
                    ActorRetirement::Completed {
                        custody:
                            ActorCustody {
                                behavior,
                                control,
                                user,
                                descendants,
                                child_failures: (),
                                capability_failures,
                                terminal_report,
                                unread_owner_cancellation,
                                additional_failures,
                                received_interpretation,
                                received_source,
                                source_index,
                                acquired_ingress,
                                retirement_failures,
                            },
                        settlements,
                        completion,
                        interpretation,
                        source,
                    } => {
                        assert!(interpretation.is_none());
                        assert!(source.is_none());
                        assert!(additional_failures.is_empty());
                        assert!(received_interpretation.is_none());
                        assert!(received_source.is_none());
                        assert!(source_index.is_none());
                        assert!(acquired_ingress.is_none());
                        assert!(retirement_failures.is_empty());
                        assert!(matches!(
                            (&cause, completion),
                            (RetirementCause::Stopped, Completion::Stopped)
                                | (RetirementCause::Exhausted, Completion::Exhausted)
                        ));
                        assert_eq!(settlements.len(), 0);
                        (
                            behavior,
                            control,
                            user,
                            descendants,
                            capability_failures,
                            terminal_report,
                            unread_owner_cancellation,
                        )
                    }
                    ActorRetirement::OwnerCancelled {
                        custody:
                            ActorCustody {
                                behavior,
                                control,
                                user,
                                descendants,
                                child_failures: (),
                                capability_failures,
                                terminal_report,
                                unread_owner_cancellation,
                                additional_failures,
                                received_interpretation,
                                received_source,
                                source_index,
                                acquired_ingress,
                                retirement_failures,
                            },
                        settlements,
                        interpretation,
                        source,
                    } => {
                        assert!(interpretation.is_none());
                        assert!(source.is_none());
                        assert!(additional_failures.is_empty());
                        assert!(received_interpretation.is_none());
                        assert!(received_source.is_none());
                        assert!(source_index.is_none());
                        assert!(acquired_ingress.is_none());
                        assert!(retirement_failures.is_empty());
                        assert_eq!(cause, RetirementCause::OwnerCancellation);
                        assert_eq!(settlements.len(), 0);
                        (
                            behavior,
                            control,
                            user,
                            descendants,
                            capability_failures,
                            terminal_report,
                            unread_owner_cancellation,
                        )
                    }
                    ActorRetirement::CapabilityFailed {
                        custody:
                            ActorCustody {
                                behavior,
                                control,
                                user,
                                descendants,
                                child_failures: (),
                                capability_failures,
                                terminal_report,
                                unread_owner_cancellation,
                                additional_failures,
                                received_interpretation,
                                received_source,
                                source_index,
                                acquired_ingress,
                                retirement_failures,
                            },
                        settlements,
                        error,
                        interpretation,
                        source,
                    } => {
                        assert!(interpretation.is_none());
                        assert!(source.is_none());
                        assert!(additional_failures.is_empty());
                        assert!(received_interpretation.is_none());
                        assert!(received_source.is_none());
                        assert!(source_index.is_none());
                        assert!(acquired_ingress.is_none());
                        assert!(retirement_failures.is_empty());
                        assert_eq!(cause, RetirementCause::CapabilityFailed);
                        assert_eq!(settlements.len(), 0);
                        assert!(error.is_panic());
                        assert_eq!(Some(error.id()), acquired_primary_id);
                        (
                            behavior,
                            control,
                            user,
                            descendants,
                            capability_failures,
                            terminal_report,
                            unread_owner_cancellation,
                        )
                    }
                    _ => panic!(
                        "the selected primary cause was changed during exact public projection"
                    ),
                };
            assert_eq!(behavior.values, [77, 177]);
            assert_eq!(behavior.values.as_ptr(), allocation);
            assert_eq!(control.len(), 0);
            assert_eq!(user.len(), 0);
            assert_eq!(descendants, [99]);
            assert!(terminal_report.is_none());
            assert_eq!(
                unread,
                match cause {
                    RetirementCause::OwnerCancellation => None,
                    _ => Some(()),
                }
            );
            assert_eq!(failures.len(), 1);
            let failure = failures
                .pop()
                .expect("one exact late failure survives every projection");
            assert!(failure.is_panic());
            assert_eq!(failure.id(), late_task_id);
        }
    }
}

#[cfg(test)]
impl<B, Root, ChildFailures> ActorRetirement<B, Root, ChildFailures>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = behavior::Never>,
{
    pub(crate) fn from_capability_local<Capabilities>(
        outcome: LocalOutcome<B, ((Vec<Root>, ChildFailures), Capabilities)>,
    ) -> (Self, Option<Capabilities>) {
        let (outcome, capabilities) = separate_capability_outcome(outcome);
        (Self::from_local(outcome), capabilities)
    }
}

#[cfg(test)]
#[expect(
    clippy::too_many_lines,
    reason = "Split the original capability product once across every existing actor outcome without erasing any behavior, settlement, cause, ingress or child field."
)]
pub(crate) fn separate_capability_outcome<B, Descendants, Capabilities>(
    outcome: LocalOutcome<B, (Descendants, Capabilities)>,
) -> (LocalOutcome<B, Descendants>, Option<Capabilities>)
where
    B: BehaviorSettlements,
{
    match outcome {
        ActorExecutionOutcome::Completed {
            additional_failures,
            behavior,
            residual,
            completion,
        } => {
            let (residual, capabilities) = residual.separate_capabilities();
            (
                ActorExecutionOutcome::Completed {
                    additional_failures,
                    behavior,
                    residual,
                    completion,
                },
                Some(capabilities),
            )
        }
        ActorExecutionOutcome::BehaviorFailed {
            additional_failures,
            behavior,
            residual,
            error,
        } => {
            let (residual, capabilities) = residual.separate_capabilities();
            (
                ActorExecutionOutcome::BehaviorFailed {
                    additional_failures,
                    behavior,
                    residual,
                    error,
                },
                Some(capabilities),
            )
        }
        ActorExecutionOutcome::InitializationPanicked {
            additional_failures,
            behavior,
            residual,
            payload,
        } => {
            let (residual, capabilities) = residual.separate_capabilities();
            (
                ActorExecutionOutcome::InitializationPanicked {
                    additional_failures,
                    behavior,
                    residual,
                    payload,
                },
                Some(capabilities),
            )
        }
        ActorExecutionOutcome::TransitionPanicked {
            additional_failures,
            behavior,
            residual,
            payload,
        } => {
            let (residual, capabilities) = residual.separate_capabilities();
            (
                ActorExecutionOutcome::TransitionPanicked {
                    additional_failures,
                    behavior,
                    residual,
                    payload,
                },
                Some(capabilities),
            )
        }
        ActorExecutionOutcome::HostExecutionPanicked {
            additional_failures,
            behavior,
            residual,
            payload,
        } => {
            let (residual, capabilities) = residual.separate_capabilities();
            (
                ActorExecutionOutcome::HostExecutionPanicked {
                    additional_failures,
                    behavior,
                    residual,
                    payload,
                },
                Some(capabilities),
            )
        }
        ActorExecutionOutcome::ActivationFailed {
            additional_failures,
            behavior,
            residual,
            error,
        } => {
            let (residual, capabilities) = residual.separate_capabilities();
            (
                ActorExecutionOutcome::ActivationFailed {
                    additional_failures,
                    behavior,
                    residual,
                    error,
                },
                Some(capabilities),
            )
        }
        ActorExecutionOutcome::SettlementFailed {
            additional_failures,
            behavior,
            residual,
            error,
        } => {
            let (residual, capabilities) = residual.separate_capabilities();
            (
                ActorExecutionOutcome::SettlementFailed {
                    additional_failures,
                    behavior,
                    residual,
                    error,
                },
                Some(capabilities),
            )
        }
        ActorExecutionOutcome::ActivationPanicked {
            behavior,
            residual,
            additional_failures,
            payload,
        } => {
            let (residual, capabilities) = residual.separate_capabilities();
            (
                ActorExecutionOutcome::ActivationPanicked {
                    behavior,
                    residual,
                    additional_failures,
                    payload,
                },
                Some(capabilities),
            )
        }
        ActorExecutionOutcome::RetirementPanicked {
            behavior,
            residual,
            additional_failures,
            payload,
        } => {
            let (residual, capabilities) = residual.separate_capabilities();
            (
                ActorExecutionOutcome::RetirementPanicked {
                    behavior,
                    residual,
                    additional_failures,
                    payload,
                },
                Some(capabilities),
            )
        }
        ActorExecutionOutcome::InterpreterContractFailed {
            behavior,
            residual,
            additional_failures,
        } => {
            let (residual, capabilities) = residual.separate_capabilities();
            (
                ActorExecutionOutcome::InterpreterContractFailed {
                    behavior,
                    residual,
                    additional_failures,
                },
                Some(capabilities),
            )
        }
        ActorExecutionOutcome::Panicked => (ActorExecutionOutcome::Panicked, None),
        ActorExecutionOutcome::Cancelled => (ActorExecutionOutcome::Cancelled, None),
    }
}
