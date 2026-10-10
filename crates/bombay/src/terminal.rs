//! Exact application-owned projection of local actor retirement.

use core::fmt;
use core::marker::PhantomData;
use core::ops::ControlFlow;
use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};

use behavior::{
    Behavior, BehaviorAddr, BehaviorMessage, BehaviorSettlements, ChildRole, CreationId,
    CreationKind, EstablishedActor, Interpretation, InterpretationProgress, Protocol,
    SourceCustody, SourceProgress, User,
};
use bombay_address::ClaimError;
use bombay_engine::{ActionsOf, Completion, DriverError, SettlementFailure};

use tokio::sync::oneshot::error::RecvError;
use tokio::task::JoinError;

use crate::ActorExecutionOutcome;
use crate::address::MailAddr;
use crate::local::effects::ActionSettlementOf;
use crate::local::environment::{LocalActivationRejection, LocalResidual};
use crate::local::execution::{LocalRetirementRequest, OwnerCancellation};
use crate::observe::Publisher;
use crate::termination::Termination;

/// Whether the actor and its owned subtree finished retiring.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetirementAssessment {
    /// Actual joined owners established complete retirement of the subtree.
    Established,
    /// Available evidence does not establish complete subtree retirement.
    NotEstablished,
}

/// Failure evidence preserved by the actor's runtime owners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActorFailureAssessment {
    /// At least one recorded actor or runtime failure was found.
    FailuresFound,
    /// Complete checking found no recorded actor or runtime failure.
    NoFailuresFound,
    /// Checking was incomplete and no failure has yet been established.
    Incomplete,
}

impl ActorFailureAssessment {
    pub(crate) const fn combine(self, later: Self) -> Self {
        match (self, later) {
            (Self::FailuresFound, _) | (_, Self::FailuresFound) => Self::FailuresFound,
            (Self::Incomplete, _) | (_, Self::Incomplete) => Self::Incomplete,
            (Self::NoFailuresFound, Self::NoFailuresFound) => Self::NoFailuresFound,
        }
    }
}

/// A runtime-issued assessment of one joined actor's owned subtree.
///
/// Retirement and failure evidence are independent: an actor can finish all
/// retirement while retaining an execution or cleanup failure. Original state
/// and errors remain in the native result; this snapshot owns no stop authority.
/// Later report-notification or parent-conversion failures do not rewrite it.
#[must_use = "the actor retirement assessment must be inspected or explicitly discharged"]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActorRetirementReport {
    retirement: RetirementAssessment,
    failures: ActorFailureAssessment,
}

impl ActorRetirementReport {
    /// Return whether complete retirement of the owned subtree was established.
    #[must_use]
    pub const fn retirement(&self) -> RetirementAssessment {
        self.retirement
    }

    /// Return the independently retained failure assessment.
    #[must_use]
    pub const fn failures(&self) -> ActorFailureAssessment {
        self.failures
    }

    pub(crate) const fn new(
        retirement: RetirementAssessment,
        failures: ActorFailureAssessment,
    ) -> Self {
        Self {
            retirement,
            failures,
        }
    }

    pub(crate) const fn combine(self, later: Self) -> Self {
        let retirement = match (self.retirement, later.retirement) {
            (RetirementAssessment::Established, RetirementAssessment::Established) => {
                RetirementAssessment::Established
            }
            (RetirementAssessment::NotEstablished, _)
            | (_, RetirementAssessment::NotEstablished) => RetirementAssessment::NotEstablished,
        };
        Self::new(retirement, self.failures.combine(later.failures))
    }

    pub(crate) const fn with_failures(self, failures: ActorFailureAssessment) -> Self {
        Self::new(self.retirement, self.failures.combine(failures))
    }

    /// Inspect retained runtime evidence without invoking application policy.
    pub(crate) fn from_joined<B, Descendants>(
        joined: &Result<LocalOutcome<B, Descendants>, JoinError>,
        termination_notification: &Result<(), RetirementNotificationError>,
    ) -> Self
    where
        B: BehaviorSettlements,
    {
        let report = match joined {
            Ok(ActorExecutionOutcome::Completed {
                residual,
                additional_failures,
                completion,
                ..
            }) => {
                let report = residual.retirement_report();
                let completion_failures = match completion {
                    Completion::RetirementRequested(LocalRetirementRequest::CapabilityFailed(
                        _,
                    )) => ActorFailureAssessment::FailuresFound,
                    Completion::Stopped
                    | Completion::Exhausted
                    | Completion::RetirementRequested(LocalRetirementRequest::OwnerCancellation(
                        _,
                    )) => ActorFailureAssessment::NoFailuresFound,
                };
                let additional_failures = if additional_failures.is_empty() {
                    ActorFailureAssessment::NoFailuresFound
                } else {
                    ActorFailureAssessment::FailuresFound
                };
                report.with_failures(completion_failures.combine(additional_failures))
            }
            Ok(
                ActorExecutionOutcome::BehaviorFailed { residual, .. }
                | ActorExecutionOutcome::InitializationPanicked { residual, .. }
                | ActorExecutionOutcome::TransitionPanicked { residual, .. }
                | ActorExecutionOutcome::HostExecutionPanicked { residual, .. }
                | ActorExecutionOutcome::ActivationPanicked { residual, .. }
                | ActorExecutionOutcome::RetirementPanicked { residual, .. }
                | ActorExecutionOutcome::InterpreterContractFailed { residual, .. }
                | ActorExecutionOutcome::ActivationFailed { residual, .. }
                | ActorExecutionOutcome::SettlementFailed { residual, .. },
            ) => residual
                .retirement_report()
                .with_failures(ActorFailureAssessment::FailuresFound),
            Ok(ActorExecutionOutcome::Panicked | ActorExecutionOutcome::Cancelled) | Err(_) => {
                Self::new(
                    RetirementAssessment::NotEstablished,
                    ActorFailureAssessment::FailuresFound,
                )
            }
        };
        if termination_notification.is_err() {
            report.with_failures(ActorFailureAssessment::FailuresFound)
        } else {
            report
        }
    }
}

/// Original results from the actor's two distinct notification stages.
///
/// These coexist with the native actor result. Publication can commit its fact
/// and then encounter an observer panic; a fault does not retract that fact.
#[must_use = "the original actor notification results must be inspected or explicitly discharged"]
#[derive(Debug)]
pub struct ActorNotificationReceipts {
    /// Publication of ordinary actor termination, before final task settlement.
    pub termination: Result<(), RetirementNotificationError>,
    /// Publication of the joined actor-retirement report.
    pub retirement: Result<(), RetirementNotificationError>,
}

impl ActorNotificationReceipts {
    pub(crate) fn has_failures(&self) -> bool {
        self.termination.is_err() || self.retirement.is_err()
    }
}

pub(crate) fn publish_retirement_report(
    publisher: Publisher<ActorRetirementReport>,
    report: ActorRetirementReport,
) -> Result<(), RetirementNotificationError> {
    catch_unwind(AssertUnwindSafe(|| publisher.complete(report)))
        .map_err(|payload| RetirementNotificationError::Panicked { payload })
}

/// The original failure of publishing an actor lifecycle notification.
///
/// An observer's panic does not retract a fact already committed by Observe.
/// This error retains the original cause separately from the actor's native
/// result. A closed receipt means its producer did not transfer a result;
/// it does not establish whether publication succeeded.
#[derive(thiserror::Error)]
pub enum RetirementNotificationError {
    /// A notification resumed the original panic from an observer.
    #[error("an actor notification observer panicked")]
    Panicked {
        /// The original panic allocation, without cloning or conversion.
        payload: Box<dyn Any + Send>,
    },
    /// The actual notification-result sender disappeared before transfer.
    #[error("the actor notification receipt closed: {error}")]
    ReceiptClosed {
        /// The original error returned by the owning oneshot receiver.
        #[source]
        error: RecvError,
    },
}

impl fmt::Debug for RetirementNotificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Panicked { .. } => formatter.debug_struct("Panicked").finish_non_exhaustive(),
            Self::ReceiptClosed { error } => formatter
                .debug_struct("ReceiptClosed")
                .field("error", error)
                .finish(),
        }
    }
}

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

/// Exact allocated address and creator-local nonce of a child retirement.
/// The owner and role preserve the statically declared application meaning.
pub struct ChildOrigin<Owner, Role> {
    address: MailAddr,
    nonce: u64,
    declaration: PhantomData<fn() -> (Owner, Role)>,
}

impl<Owner, Role> Copy for ChildOrigin<Owner, Role> {}

impl<Owner, Role> Clone for ChildOrigin<Owner, Role> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Owner, Role> ChildOrigin<Owner, Role> {
    pub(crate) const fn new(address: MailAddr, nonce: u64) -> Self {
        Self {
            address,
            nonce,
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
            .finish_non_exhaustive()
    }
}

impl<Owner, Role> PartialEq for ChildOrigin<Owner, Role> {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.nonce == other.nonce
    }
}

impl<Owner, Role> Eq for ChildOrigin<Owner, Role> {}

/// Total static lift from one exact terminal source into an application sum.
pub trait ProjectTerminal<Origin, Terminal> {
    /// Preserve the supplied origin and terminal value in the application sum.
    fn project(origin: Origin, terminal: Terminal) -> Self;
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
        id: CreationId,
        kind: CreationKind,
        origin: Origin,
        payload: Box<dyn Any + Send>,
        additional_failures: Vec<DriverError<Child::Error, LocalActivationRejection<MailAddr>>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        termination_notification: Result<(), RetirementNotificationError>,
        retirement_report: ActorRetirementReport,
    },
    /// Startup cleanup owns independent available facts after Core received the child input.
    StartupRetirementFailed {
        id: CreationId,
        kind: CreationKind,
        origin: Origin,
        primary_failure: Option<DriverError<Child::Error, LocalActivationRejection<MailAddr>>>,
        additional_failures: Vec<DriverError<Child::Error, LocalActivationRejection<MailAddr>>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        termination_notification: Result<(), RetirementNotificationError>,
        retirement_report: ActorRetirementReport,
    },
    ActorTaskFailed {
        id: CreationId,
        kind: CreationKind,
        origin: Origin,
        actor: EstablishedActor<Child>,
        error: JoinError,
        notifications: Result<ActorNotificationReceipts, RecvError>,
    },
    ProjectionTaskFailed {
        id: CreationId,
        kind: CreationKind,
        origin: Origin,
        actor: EstablishedActor<Child>,
        error: JoinError,
        notifications: Result<ActorNotificationReceipts, RecvError>,
    },
    /// The projected native result is retained independently of this failure.
    NotificationsFailed {
        id: CreationId,
        kind: CreationKind,
        origin: Origin,
        actor: EstablishedActor<Child>,
        notifications: Result<ActorNotificationReceipts, RecvError>,
    },
}

impl<Origin, Child> fmt::Debug for ChildFailure<Origin, Child>
where
    Origin: fmt::Debug,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InitializationPanicked {
                id, kind, origin, ..
            } => formatter
                .debug_struct("InitializationPanicked")
                .field("id", id)
                .field("kind", kind)
                .field("origin", origin)
                .finish_non_exhaustive(),
            Self::StartupRetirementFailed {
                id, kind, origin, ..
            } => formatter
                .debug_struct("StartupRetirementFailed")
                .field("id", id)
                .field("kind", kind)
                .field("origin", origin)
                .finish_non_exhaustive(),
            Self::ActorTaskFailed {
                id,
                kind,
                origin,
                error,
                ..
            } => formatter
                .debug_struct("ActorTaskFailed")
                .field("id", id)
                .field("kind", kind)
                .field("origin", origin)
                .field("error", error)
                .finish_non_exhaustive(),
            Self::ProjectionTaskFailed {
                id,
                kind,
                origin,
                error,
                ..
            } => formatter
                .debug_struct("ProjectionTaskFailed")
                .field("id", id)
                .field("kind", kind)
                .field("origin", origin)
                .field("error", error)
                .finish_non_exhaustive(),
            Self::NotificationsFailed {
                id,
                kind,
                origin,
                notifications,
                ..
            } => formatter
                .debug_struct("NotificationsFailed")
                .field("id", id)
                .field("kind", kind)
                .field("origin", origin)
                .field("notifications", notifications)
                .finish_non_exhaustive(),
        }
    }
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
        behavior: BehaviorState,
        error: BehaviorState::Error,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
    },
    InitializationPanicked {
        payload: Box<dyn Any + Send>,
        behavior: BehaviorState,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
    },
    HostCommitPanicked {
        initialization: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        behavior: BehaviorState,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
        payload: Box<dyn Any + Send>,
    },
    TransitionPanicked {
        behavior: BehaviorState,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
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
        behavior: BehaviorState,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
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
        behavior: BehaviorState,
        initialization: InterpretationProgress<
            ActionsOf<BehaviorState>,
            BehaviorState::InterpretationCustody,
            BehaviorState::Settlements,
        >,
        error: ClaimError<MailAddr>,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
    },
    BindingAbandoned {
        behavior: BehaviorState,
        initialization: InterpretationProgress<
            ActionsOf<BehaviorState>,
            BehaviorState::InterpretationCustody,
            BehaviorState::Settlements,
        >,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
    },
    Completed {
        behavior: BehaviorState,
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
        completion: Completion,
    },
    BehaviorFailed {
        behavior: BehaviorState,
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
        error: BehaviorState::Error,
    },
    EffectsFailed {
        initialization: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        behavior: BehaviorState,
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
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
    },
    OwnerCancelled {
        behavior: BehaviorState,
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
    },
    /// Live execution acquired this exact capability task failure as its primary cause.
    CapabilityFailed {
        behavior: BehaviorState,
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        error: JoinError,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
    },
    PreparedRetirementPanicked {
        behavior: BehaviorState,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
        payload: Box<dyn Any + Send>,
    },
    PreparedInterpreterContractFailed {
        behavior: BehaviorState,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
    },
    InitializationRetirementPanicked {
        initialization: InterpretationProgress<
            ActionsOf<BehaviorState>,
            BehaviorState::InterpretationCustody,
            BehaviorState::Settlements,
        >,
        behavior: BehaviorState,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
        payload: Box<dyn Any + Send>,
    },
    InitializationInterpreterContractFailed {
        initialization: InterpretationProgress<
            ActionsOf<BehaviorState>,
            BehaviorState::InterpretationCustody,
            BehaviorState::Settlements,
        >,
        behavior: BehaviorState,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
    },
    ActiveRetirementPanicked {
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        behavior: BehaviorState,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
        payload: Box<dyn Any + Send>,
    },
    ActiveActivationPanicked {
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        behavior: BehaviorState,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
        payload: Box<dyn Any + Send>,
    },
    ActiveInterpreterContractFailed {
        interpretation: Option<
            InterpretationProgress<
                ActionsOf<BehaviorState>,
                BehaviorState::InterpretationCustody,
                BehaviorState::Settlements,
            >,
        >,
        source: Option<SourceProgress<BehaviorState::Settlements, BehaviorState::SourceCustody>>,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        behavior: BehaviorState,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
    },
    PreparedEffectsFailed {
        behavior: BehaviorState,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        child_failures: ChildFailures,
        capability_failures: Vec<JoinError>,
        additional_failures:
            Vec<DriverError<BehaviorState::Error, LocalActivationRejection<MailAddr>>>,
        received_interpretation: Option<Interpretation<BehaviorState::Settlements>>,
        received_source: Option<SourceCustody<BehaviorState::Settlements>>,
        source_index: Option<usize>,
        /// Continue retains the exact acquired event/exhaustion. Break(Ok(()))
        /// passively reports the original zero-field owner-cancellation fact;
        /// Break(Err(error)) owns the original capability `JoinError`. This report
        /// grants no cancellation sender authority.
        #[expect(
            clippy::type_complexity,
            reason = "this acquired source reply coexists with event/exhaustion and exact cancellation or capability JoinError; an alias would merely hide the owning equation"
        )]
        acquired_ingress: Option<ControlFlow<Result<(), JoinError>, Option<BehaviorState::Event>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        unread_owner_cancellation: Option<()>,
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
            operation_failures: _,
            descendant_report: _,
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
                    behavior,
                    interpretation,
                    source,
                    settlements,
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
                };
            }
            Completion::RetirementRequested(LocalRetirementRequest::CapabilityFailed(error)) => {
                return Self::CapabilityFailed {
                    behavior,
                    interpretation,
                    source,
                    settlements,
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
                    error,
                };
            }
        };
        Self::Completed {
            behavior,
            interpretation,
            source,
            settlements,
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
            completion,
        }
    }
    /// Project each actual received residual phase without discarding current
    /// progress, a refused report or coexisting execution/retirement failures.
    ///
    /// The owning caller first derives and separately preserves the joined
    /// report. This native projection explicitly discharges its two Copy
    /// assessment inputs after their combination into that report; all original
    /// affine state, failures and settlement values remain in the native lanes.
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
                        operation_failures: _,
                        descendant_report: _,
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
                    error,
                }
            }
            ActorExecutionOutcome::BehaviorFailed {
                behavior,
                residual:
                    LocalResidual::Retired {
                        operation_failures: _,
                        descendant_report: _,
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
                    behavior,
                    interpretation,
                    source,
                    settlements,
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
                    error,
                }
            }
            ActorExecutionOutcome::InitializationPanicked {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        operation_failures: _,
                        descendant_report: _,
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
                    payload,
                }
            }
            ActorExecutionOutcome::TransitionPanicked {
                behavior,
                residual:
                    LocalResidual::Retired {
                        operation_failures: _,
                        descendant_report: _,
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
                    behavior,
                    interpretation,
                    source,
                    settlements,
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
                    payload,
                }
            }
            ActorExecutionOutcome::HostExecutionPanicked {
                behavior,
                residual:
                    LocalResidual::Retired {
                        operation_failures: _,
                        descendant_report: _,
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
                    behavior,
                    interpretation,
                    source,
                    settlements,
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
                    payload,
                }
            }
            ActorExecutionOutcome::ActivationPanicked {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        operation_failures: _,
                        descendant_report: _,
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
                        operation_failures: _,
                        descendant_report: _,
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
                    behavior,
                    initialization: None,
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
                    payload,
                }
            }
            ActorExecutionOutcome::ActivationPanicked {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        operation_failures: _,
                        descendant_report: _,
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
                        operation_failures: _,
                        descendant_report: _,
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
                    behavior,
                    initialization: Some(initialization),
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
                    payload,
                }
            }
            ActorExecutionOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        operation_failures: _,
                        descendant_report: _,
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
                    behavior,
                    initialization,
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
                    error,
                }
            }
            ActorExecutionOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        operation_failures: _,
                        descendant_report: _,
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
                    behavior,
                    initialization,
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
                }
            }
            ActorExecutionOutcome::SettlementFailed {
                behavior,
                residual:
                    LocalResidual::Retired {
                        operation_failures: _,
                        descendant_report: _,
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
                    behavior,
                    interpretation,
                    source,
                    settlements,
                    initialization: None,
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
                    error,
                }
            }
            ActorExecutionOutcome::SettlementFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        operation_failures: _,
                        descendant_report: _,
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
                    behavior,
                    interpretation: None,
                    source: None,
                    settlements: Vec::new(),
                    initialization: Some(initialization),
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
                    error,
                }
            }
            ActorExecutionOutcome::SettlementFailed {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        operation_failures: _,
                        descendant_report: _,
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
                    error,
                }
            }
            ActorExecutionOutcome::InterpreterContractFailed {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        operation_failures: _,
                        descendant_report: _,
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
                }
            }
            ActorExecutionOutcome::InterpreterContractFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        operation_failures: _,
                        descendant_report: _,
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
                    initialization,
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
                }
            }
            ActorExecutionOutcome::InterpreterContractFailed {
                behavior,
                residual:
                    LocalResidual::Retired {
                        operation_failures: _,
                        descendant_report: _,
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
                    interpretation,
                    source,
                    settlements,
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
                }
            }
            ActorExecutionOutcome::RetirementPanicked {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        operation_failures: _,
                        descendant_report: _,
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
                    payload,
                }
            }
            ActorExecutionOutcome::RetirementPanicked {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        operation_failures: _,
                        descendant_report: _,
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
                    behavior,
                    initialization,
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
                    payload,
                }
            }
            ActorExecutionOutcome::RetirementPanicked {
                behavior,
                residual:
                    LocalResidual::Retired {
                        operation_failures: _,
                        descendant_report: _,
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
                    behavior,
                    interpretation,
                    source,
                    settlements,
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
                    payload,
                }
            }
            ActorExecutionOutcome::ActivationPanicked {
                behavior,
                residual:
                    LocalResidual::Retired {
                        operation_failures: _,
                        descendant_report: _,
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
                    behavior,
                    interpretation,
                    source,
                    settlements,
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

    struct Root;
    struct ChildRole;

    #[test]
    fn root_and_child_origins_keep_distinct_exact_payloads() {
        let root = RootOrigin::<Root>::new(MailAddr(1));
        let child = ChildOrigin::<Root, ChildRole>::new(MailAddr(7), 13);
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
        let first = ChildOrigin::<Root, ChildRole>::new(MailAddr(7), 13);
        let same = ChildOrigin::<Root, ChildRole>::new(MailAddr(7), 13);
        let other_address = ChildOrigin::<Root, ChildRole>::new(MailAddr(8), 13);
        let other_nonce = ChildOrigin::<Root, ChildRole>::new(MailAddr(7), 14);

        assert_eq!(first, same);
        assert_ne!(first, other_address);
        assert_ne!(first, other_nonce);
        let diagnostic = format!("{first:?}");
        assert!(diagnostic.contains("ChildOrigin"));
        assert!(diagnostic.contains("MailAddr(7)"));
        assert!(diagnostic.contains("nonce: 13"));
    }
}

#[cfg(test)]
mod capability_retirement_projection {
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
    use crate::terminal::{
        ActorFailureAssessment, ActorRetirement, ActorRetirementReport, LocalOutcome,
        RetirementAssessment,
    };
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
        OwnerCancellation,
        CapabilityFailed,
    }

    #[tokio::test]
    async fn joined_report_preserves_primary_capability_failure_without_later_failures() {
        let task = tokio::spawn(async { panic_any(Box::new(vec![191_u64, 193])) });
        let original_task = task.id();
        let error = task.await.expect_err("the actual capability task panics");
        let joined: Result<LocalOutcome<RetiringActor, (Vec<u64>, ())>, _> =
            Ok(ActorExecutionOutcome::Completed {
                behavior: RetiringActor {
                    values: vec![197, 199],
                },
                completion: Completion::RetirementRequested(
                    LocalRetirementRequest::CapabilityFailed(error),
                ),
                additional_failures: Vec::new(),
                residual: LocalResidual::Retired {
                    operation_failures: ActorFailureAssessment::Incomplete,
                    descendant_report: ActorRetirementReport::new(
                        RetirementAssessment::NotEstablished,
                        ActorFailureAssessment::Incomplete,
                    ),
                    interpretation: None,
                    source: None,
                    settlements: Vec::new(),
                    received_interpretation: None,
                    received_source: None,
                    source_index: None,
                    acquired_ingress: None,
                    ingress: Drained {
                        control: Vec::new(),
                        user: Vec::new(),
                    },
                    activation_tasks: ActivationTasks::new(),
                    descendants: (Vec::new(), ()),
                    capability_failures: Vec::new(),
                    terminal_report: None,
                    retirement_failures: Vec::new(),
                    unread_owner_cancellation: None,
                },
            });
        let report = ActorRetirementReport::from_joined(&joined, &Ok(()));
        assert_eq!(report.failures(), ActorFailureAssessment::FailuresFound);
        // This synthetic native fixture cannot certify standard-owner retirement.
        assert_eq!(report.retirement(), RetirementAssessment::NotEstablished);
        let Ok(local) = joined else {
            panic!("the assessed native outcome remains acquired");
        };
        let native = ActorRetirement::from_local(local);
        let ActorRetirement::CapabilityFailed {
            error,
            behavior,
            capability_failures,
            additional_failures,
            ..
        } = native
        else {
            panic!("the original primary capability failure remains a separate native cause");
        };
        assert_eq!(error.id(), original_task);
        assert!(error.is_panic());
        assert_eq!(behavior.values, [197, 199]);
        assert!(capability_failures.is_empty());
        assert!(additional_failures.is_empty());
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
                    operation_failures: ActorFailureAssessment::Incomplete,
                    descendant_report: ActorRetirementReport::new(
                        RetirementAssessment::NotEstablished,
                        ActorFailureAssessment::Incomplete,
                    ),
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
                    operation_failures: ActorFailureAssessment::Incomplete,
                    descendant_report: ActorRetirementReport::new(
                        RetirementAssessment::NotEstablished,
                        ActorFailureAssessment::Incomplete,
                    ),
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
                | RetirementCause::OwnerCancellation
                | RetirementCause::CapabilityFailed => LocalResidual::Retired {
                    operation_failures: ActorFailureAssessment::Incomplete,
                    descendant_report: ActorRetirementReport::new(
                        RetirementAssessment::NotEstablished,
                        ActorFailureAssessment::Incomplete,
                    ),
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
            let (behavior, control, user, descendants, mut failures, terminal_report, unread) =
                match ActorRetirement::from_local(outcome) {
                    ActorRetirement::InitializationPanicked {
                        payload: _,
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
                        behavior,
                        initialization,
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
                        behavior,
                        settlements,
                        control,
                        user,
                        descendants,
                        child_failures: (),
                        completion,
                        capability_failures,
                        terminal_report,
                        unread_owner_cancellation,
                        interpretation,
                        source,
                        additional_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        retirement_failures,
                    } => {
                        assert!(interpretation.is_none());
                        assert!(source.is_none());
                        assert!(additional_failures.is_empty());
                        assert!(received_interpretation.is_none());
                        assert!(received_source.is_none());
                        assert!(source_index.is_none());
                        assert!(acquired_ingress.is_none());
                        assert!(retirement_failures.is_empty());
                        assert_eq!(cause, RetirementCause::Stopped);
                        assert_eq!(completion, Completion::Stopped);
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
                        behavior,
                        settlements,
                        control,
                        user,
                        descendants,
                        child_failures: (),
                        capability_failures,
                        terminal_report,
                        unread_owner_cancellation,
                        interpretation,
                        source,
                        additional_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        retirement_failures,
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
                        behavior,
                        settlements,
                        control,
                        user,
                        descendants,
                        child_failures: (),
                        error,
                        capability_failures,
                        terminal_report,
                        unread_owner_cancellation,
                        interpretation,
                        source,
                        additional_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        retirement_failures,
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
