//! One concrete task-launch boundary for local actors.

use core::fmt;
use std::any::Any;
use std::error::Error;
use std::future::{Future, poll_fn};
use std::ops::ControlFlow;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
#[cfg(test)]
use std::sync::Weak;
use std::task::Poll;

use crate::address::MailAddr;
use crate::local::effects::ActionSettlementOf;
use crate::local::effects::observation::TerminationObservations;
use crate::local::effects::timers::LocalTimers;
use crate::observe;
use crate::terminal::{
    ActorRetirement, LocalOutcome, ProjectTerminal, RetirementNotificationError, retirement_ingress,
};
use crate::{ActorExecutionOutcome, Retirement};
#[cfg(test)]
use behavior::{
    ActionSettlement, ActionSettlements, Actions, Creations, Here, InterpretSends, NoBirths,
    SendSettlements, SourceProgress, SourceSettlementCustody,
};
use behavior::{
    Behavior, BehaviorMessage, BehaviorSettlements, BirthMode, ClassifySettlement, Interpretation,
    InterpretationProgress, Never, Protocol, SourceCustody, User,
};
#[cfg(test)]
use behavior_actors::ShutdownRequested;
use bombay_address::{AddressSpace, ClaimError};
use bombay_engine::Driver;
use bombay_engine::{ActionsOf, Completion, DriverError};
use communication::Config;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};

#[cfg(test)]
use crate::local::effects::InterpretedActionSettlement;

use super::ActorExecution;
use super::termination::TerminationPublication;
#[cfg(test)]
use crate::local::effects::CapabilityRetirement;
use crate::local::effects::{CommitActions, LocalTerminalReports};
use crate::local::environment::{LocalEnvironment, LocalResidual};
use crate::local::execution::{LocalRetirementRequest, OwnerCancellation};
use crate::local::ingress::{EntityIngress, IngressMode, StandardIngress};
use crate::local::{ActorRef, LocalActivationRejection};
use crate::termination::Termination;

/// Failure before a launched actor publishes its live reference.
pub(crate) enum SpawnError<B, Descendants = ()>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>>,
{
    AllocationRejected {
        behavior: B,
        reason: behavior::AllocationRejection,
    },
    InitializationRejected {
        behavior: B,
        error: B::Error,
        control: Vec<B::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<B>>>,
        descendants: Descendants,
        capability_failures: Vec<JoinError>,
        additional_failures: Vec<DriverError<B::Error, LocalActivationRejection<MailAddr>>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        received_interpretation: Option<Interpretation<B::Settlements>>,
        received_source: Option<SourceCustody<B::Settlements>>,
        source_index: Option<usize>,
        acquired_ingress: Option<ControlFlow<LocalRetirementRequest, Option<B::Event>>>,
        unread_owner_cancellation: Option<()>,
        termination_notification: Result<(), RetirementNotificationError>,
    },
    InitializationPanicked {
        payload: Box<dyn Any + Send>,
        behavior: B,
        control: Vec<B::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<B>>>,
        descendants: Descendants,
        capability_failures: Vec<JoinError>,
        additional_failures: Vec<DriverError<B::Error, LocalActivationRejection<MailAddr>>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        received_interpretation: Option<Interpretation<B::Settlements>>,
        received_source: Option<SourceCustody<B::Settlements>>,
        source_index: Option<usize>,
        acquired_ingress: Option<ControlFlow<LocalRetirementRequest, Option<B::Event>>>,
        unread_owner_cancellation: Option<()>,
        termination_notification: Result<(), RetirementNotificationError>,
    },
    HostRejected {
        behavior: B,
        initialization:
            InterpretationProgress<ActionsOf<B>, B::InterpretationCustody, B::Settlements>,
        error: ClaimError<MailAddr>,
        control: Vec<B::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<B>>>,
        descendants: Descendants,
        capability_failures: Vec<JoinError>,
        additional_failures: Vec<DriverError<B::Error, LocalActivationRejection<MailAddr>>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        received_interpretation: Option<Interpretation<B::Settlements>>,
        received_source: Option<SourceCustody<B::Settlements>>,
        source_index: Option<usize>,
        acquired_ingress: Option<ControlFlow<LocalRetirementRequest, Option<B::Event>>>,
        unread_owner_cancellation: Option<()>,
        termination_notification: Result<(), RetirementNotificationError>,
    },
    BindingAbandoned {
        behavior: B,
        initialization:
            InterpretationProgress<ActionsOf<B>, B::InterpretationCustody, B::Settlements>,
        control: Vec<B::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<B>>>,
        descendants: Descendants,
        capability_failures: Vec<JoinError>,
        additional_failures: Vec<DriverError<B::Error, LocalActivationRejection<MailAddr>>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        received_interpretation: Option<Interpretation<B::Settlements>>,
        received_source: Option<SourceCustody<B::Settlements>>,
        source_index: Option<usize>,
        acquired_ingress: Option<ControlFlow<LocalRetirementRequest, Option<B::Event>>>,
        unread_owner_cancellation: Option<()>,
        termination_notification: Result<(), RetirementNotificationError>,
    },
    Unpublished {
        outcome: LocalOutcome<B, Descendants>,
        termination_notification: Result<(), RetirementNotificationError>,
    },
    /// The executor returned no local outcome; its original task failure survives.
    ActorTaskFailed {
        error: JoinError,
        termination_notification: Result<(), RetirementNotificationError>,
    },
    Panicked {
        termination_notification: Result<(), RetirementNotificationError>,
    },
    Cancelled {
        termination_notification: Result<(), RetirementNotificationError>,
    },
}

impl<B, Descendants> fmt::Debug for SpawnError<B, Descendants>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AllocationRejected { .. } => "AllocationRejected",
            Self::InitializationRejected { .. } => "InitializationRejected",
            Self::InitializationPanicked { .. } => "InitializationPanicked",
            Self::HostRejected { .. } => "HostRejected",
            Self::BindingAbandoned { .. } => "BindingAbandoned",
            Self::Unpublished { .. } => "Unpublished",
            Self::ActorTaskFailed { .. } => "ActorTaskFailed",
            Self::Panicked { .. } => "Panicked",
            Self::Cancelled { .. } => "Cancelled",
        })
    }
}

impl<B, Descendants> fmt::Display for SpawnError<B, Descendants>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AllocationRejected { .. } => {
                formatter.write_str("the actor address source rejected allocation")
            }
            Self::InitializationRejected { .. } => {
                formatter.write_str("the behavior rejected initialization")
            }
            Self::InitializationPanicked { .. } => {
                formatter.write_str("the behavior panicked during pure initialization")
            }
            Self::HostRejected { .. } => {
                formatter.write_str("the actor host rejected initialization")
            }
            Self::BindingAbandoned { .. } => {
                formatter.write_str("the actor's private binding was abandoned")
            }
            Self::Unpublished { .. } => formatter.write_str("the actor retired before publication"),
            Self::ActorTaskFailed { .. } => {
                formatter.write_str("the actor task failed before publishing its live reference")
            }
            Self::Panicked { .. } => formatter.write_str("actor initialization panicked"),
            Self::Cancelled { .. } => formatter.write_str("actor initialization was cancelled"),
        }
    }
}

impl<B, Descendants> Error for SpawnError<B, Descendants>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>>,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ActorTaskFailed { error, .. } => Some(error),
            Self::AllocationRejected { .. }
            | Self::InitializationRejected { .. }
            | Self::InitializationPanicked { .. }
            | Self::HostRejected { .. }
            | Self::BindingAbandoned { .. }
            | Self::Unpublished { .. }
            | Self::Panicked { .. }
            | Self::Cancelled { .. } => None,
        }
    }
}

impl<B, Descendants> SpawnError<B, Descendants>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    #[expect(
        clippy::too_many_lines,
        reason = "one exhaustive owning phase/failure conversion preserves every original typed field; forwarding functions or aliases would only relocate the same conservation obligation"
    )]
    fn from_local(
        outcome: LocalOutcome<B, Descendants>,
        termination_notification: Result<(), RetirementNotificationError>,
    ) -> Self {
        let tasks = match &outcome {
            ActorExecutionOutcome::BehaviorFailed {
                residual:
                    LocalResidual::Prepared {
                        activation_tasks, ..
                    },
                ..
            }
            | ActorExecutionOutcome::InitializationPanicked {
                residual:
                    LocalResidual::Prepared {
                        activation_tasks, ..
                    },
                ..
            }
            | ActorExecutionOutcome::ActivationFailed {
                residual:
                    LocalResidual::Uncommitted {
                        activation_tasks, ..
                    },
                ..
            } => Some(activation_tasks),
            _ => None,
        };
        if tasks.is_some_and(|tasks| !tasks.is_empty()) {
            return Self::Unpublished {
                outcome,
                termination_notification,
            };
        }
        match outcome {
            ActorExecutionOutcome::BehaviorFailed {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        ingress,
                        activation_tasks,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                    },
                additional_failures,
                error,
            } => {
                drop(activation_tasks);
                Self::InitializationRejected {
                    termination_notification,
                    behavior,
                    error,
                    control: ingress.control,
                    user: ingress.user,
                    descendants,
                    capability_failures,
                    unread_owner_cancellation,
                    additional_failures,
                    terminal_report,
                    retirement_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                }
            }
            ActorExecutionOutcome::InitializationPanicked {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        ingress,
                        activation_tasks,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                    },
                additional_failures,
                payload,
            } => {
                drop(activation_tasks);
                Self::InitializationPanicked {
                    termination_notification,
                    behavior,
                    payload,
                    control: ingress.control,
                    user: ingress.user,
                    descendants,
                    capability_failures,
                    unread_owner_cancellation,
                    additional_failures,
                    terminal_report,
                    retirement_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                }
            }
            ActorExecutionOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        activation_tasks,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                    },
                additional_failures,
                error: LocalActivationRejection::Address(error),
            } => {
                drop(activation_tasks);
                Self::HostRejected {
                    termination_notification,
                    behavior,
                    initialization,
                    error,
                    control: ingress.control,
                    user: ingress.user,
                    descendants,
                    capability_failures,
                    unread_owner_cancellation,
                    additional_failures,
                    terminal_report,
                    retirement_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                }
            }
            ActorExecutionOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        activation_tasks,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
                        terminal_report,
                        retirement_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                    },
                additional_failures,
                error: LocalActivationRejection::BindingAbandoned,
            } => {
                drop(activation_tasks);
                Self::BindingAbandoned {
                    termination_notification,
                    behavior,
                    initialization,
                    control: ingress.control,
                    user: ingress.user,
                    descendants,
                    capability_failures,
                    unread_owner_cancellation,
                    additional_failures,
                    terminal_report,
                    retirement_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                }
            }
            ActorExecutionOutcome::Panicked => Self::Panicked {
                termination_notification,
            },
            ActorExecutionOutcome::Cancelled => Self::Cancelled {
                termination_notification,
            },
            outcome => Self::Unpublished {
                outcome,
                termination_notification,
            },
        }
    }
}

impl<B, Root, ChildFailures> SpawnError<B, (Vec<Root>, ChildFailures)>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    #[expect(
        clippy::too_many_lines,
        reason = "one exhaustive owning phase/failure conversion preserves every original typed field; forwarding functions or aliases would only relocate the same conservation obligation"
    )]
    pub(crate) fn into_retirement(
        self,
    ) -> Result<
        (
            ActorRetirement<B, Root, ChildFailures>,
            Result<(), RetirementNotificationError>,
        ),
        (B, behavior::AllocationRejection),
    > {
        match self {
            Self::AllocationRejected { behavior, reason } => Err((behavior, reason)),
            Self::InitializationRejected {
                termination_notification,
                error,
                behavior,
                control,
                user,
                descendants: (descendants, child_failures),
                capability_failures,
                unread_owner_cancellation,
                additional_failures,
                terminal_report,
                retirement_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
            } => Ok((
                ActorRetirement::InitializationRejected {
                    error,
                    behavior,
                    control,
                    user,
                    descendants,
                    child_failures,
                    capability_failures,
                    unread_owner_cancellation,
                    additional_failures,
                    terminal_report,
                    retirement_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress: retirement_ingress(acquired_ingress),
                },
                termination_notification,
            )),
            Self::InitializationPanicked {
                termination_notification,
                payload,
                behavior,
                control,
                user,
                descendants: (descendants, child_failures),
                capability_failures,
                unread_owner_cancellation,
                additional_failures,
                terminal_report,
                retirement_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
            } => Ok((
                ActorRetirement::InitializationPanicked {
                    payload,
                    behavior,
                    control,
                    user,
                    descendants,
                    child_failures,
                    capability_failures,
                    unread_owner_cancellation,
                    additional_failures,
                    terminal_report,
                    retirement_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress: retirement_ingress(acquired_ingress),
                },
                termination_notification,
            )),
            Self::HostRejected {
                termination_notification,
                initialization,
                error,
                behavior,
                control,
                user,
                descendants: (descendants, child_failures),
                capability_failures,
                unread_owner_cancellation,
                additional_failures,
                terminal_report,
                retirement_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
            } => Ok((
                ActorRetirement::HostRejected {
                    initialization,
                    error,
                    behavior,
                    control,
                    user,
                    descendants,
                    child_failures,
                    capability_failures,
                    unread_owner_cancellation,
                    additional_failures,
                    terminal_report,
                    retirement_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress: retirement_ingress(acquired_ingress),
                },
                termination_notification,
            )),
            Self::BindingAbandoned {
                termination_notification,
                initialization,
                behavior,
                control,
                user,
                descendants: (descendants, child_failures),
                capability_failures,
                unread_owner_cancellation,
                additional_failures,
                terminal_report,
                retirement_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
            } => Ok((
                ActorRetirement::BindingAbandoned {
                    initialization,
                    behavior,
                    control,
                    user,
                    descendants,
                    child_failures,
                    capability_failures,
                    unread_owner_cancellation,
                    additional_failures,
                    terminal_report,
                    retirement_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress: retirement_ingress(acquired_ingress),
                },
                termination_notification,
            )),
            Self::Unpublished {
                outcome,
                termination_notification,
            } => Ok((
                ActorRetirement::from_local(outcome),
                termination_notification,
            )),
            Self::ActorTaskFailed {
                error,
                termination_notification,
            } => Ok((
                ActorRetirement::ActorTaskFailed(error),
                termination_notification,
            )),
            Self::Panicked {
                termination_notification,
            } => Ok((ActorRetirement::Panicked, termination_notification)),
            Self::Cancelled {
                termination_notification,
            } => Ok((ActorRetirement::Cancelled, termination_notification)),
        }
    }
}

pub(crate) struct OwnedActor<B, Descendants>
where
    B: BehaviorSettlements,
{
    pub(crate) actor: ActorRef<B::Protocol>,
    pub(crate) control: communication::ControlSender<B::Event>,
    pub(crate) task: OwnedTask<B, Descendants>,
    pub(crate) binding: Option<oneshot::Sender<()>>,
}

impl<B, Descendants> OwnedActor<B, Descendants>
where
    B: BehaviorSettlements,
{
    pub(crate) fn acknowledge_binding(&mut self) {
        if let Some(binding) = self.binding.take() {
            let acknowledgement = binding.send(());
            assert!(
                acknowledgement.is_ok(),
                "the privately committed actor awaits its owning binding"
            );
        }
    }
}

pub(crate) struct OwnedTask<B, Descendants>
where
    B: BehaviorSettlements,
{
    task: JoinHandle<LocalOutcome<B, Descendants>>,
    termination_notification: oneshot::Receiver<Result<(), RetirementNotificationError>>,
    cancellation: OwnerCancellationAuthority,
}

/// Requests actor cleanup if its startup or join waiter disappears.
pub(crate) struct OwnerCancellationAuthority {
    sender: Option<oneshot::Sender<OwnerCancellation>>,
}

impl OwnerCancellationAuthority {
    fn new(sender: oneshot::Sender<OwnerCancellation>) -> Self {
        Self {
            sender: Some(sender),
        }
    }

    fn request(&mut self) {
        if let Some(sender) = self.sender.take() {
            match sender.send(OwnerCancellation) {
                Ok(()) | Err(OwnerCancellation) => {}
            }
        }
    }

    fn disarm(&mut self) {
        drop(self.sender.take());
    }

    async fn finish<T>(mut self, task: JoinHandle<T>) -> Result<T, JoinError> {
        let joined = task.await;
        self.disarm();
        joined
    }
}

impl Drop for OwnerCancellationAuthority {
    fn drop(&mut self) {
        self.request();
    }
}

pub(crate) struct ProjectedTask<B, Root>
where
    B: Behavior,
{
    task: JoinHandle<Result<Root, JoinError>>,
    termination_notification: oneshot::Receiver<Result<(), RetirementNotificationError>>,
    cancellation: OwnerCancellationAuthority,
    behavior: core::marker::PhantomData<fn() -> B>,
}

#[cfg(test)]
pub(crate) struct RootActor<B, Descendants>
where
    B: BehaviorSettlements,
{
    pub(crate) actor: ActorRef<B::Protocol>,
    pub(crate) shutdown_control: Weak<communication::ControlSender<B::Event>>,
    pub(crate) task: OwnedTask<B, Descendants>,
}

impl<B, Descendants> OwnedTask<B, Descendants>
where
    B: BehaviorSettlements,
    B::Event: Send + 'static,
{
    /// Acquire this exact actor result in the caller's destination before
    /// disposing the join operation. Empty or coexisting slots stay unchanged.
    pub(crate) async fn receive_finish(
        owned: &mut Option<Self>,
        received: &mut Option<Result<LocalOutcome<B, Descendants>, JoinError>>,
        termination_notification: &mut Option<Result<(), RetirementNotificationError>>,
    ) {
        poll_fn(|context| {
            let Some(actor_task) = owned.as_mut() else {
                return Poll::Ready(());
            };
            if received.is_none() {
                match Pin::new(&mut actor_task.task).poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(joined) => {
                        *received = Some(joined);
                        actor_task.cancellation.disarm();
                    }
                }
            }
            if termination_notification.is_none() {
                match Pin::new(&mut actor_task.termination_notification).poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(notification) => {
                        *termination_notification = Some(notification.unwrap_or_else(|error| {
                            Err(RetirementNotificationError::ReceiptClosed { error })
                        }));
                    }
                }
            }
            // Both original facts are outside before their owning operation is disposed.
            drop(owned.take());
            Poll::Ready(())
        })
        .await;
    }

    pub(crate) async fn receive_retirement(
        owned: &mut Option<Self>,
        received: &mut Option<Result<LocalOutcome<B, Descendants>, JoinError>>,
        termination_notification: &mut Option<Result<(), RetirementNotificationError>>,
    ) {
        if received.is_none() {
            if let Some(actor_task) = owned.as_mut() {
                actor_task.cancellation.request();
            }
        }
        Self::receive_finish(owned, received, termination_notification).await;
    }

    pub(crate) async fn retire(
        self,
    ) -> (
        Result<LocalOutcome<B, Descendants>, JoinError>,
        Result<(), RetirementNotificationError>,
    ) {
        let mut owned = Some(self);
        let mut received = None;
        let mut termination_notification = None;
        Self::receive_retirement(&mut owned, &mut received, &mut termination_notification).await;
        (
            received.expect("the original actor retirement result was acquired"),
            termination_notification.expect("the original termination notification was acquired"),
        )
    }

    #[cfg(test)]
    pub(crate) async fn finish(
        self,
    ) -> (
        Result<LocalOutcome<B, Descendants>, JoinError>,
        Result<(), RetirementNotificationError>,
    ) {
        let mut owned = Some(self);
        let mut received = None;
        let mut termination_notification = None;
        Self::receive_finish(&mut owned, &mut received, &mut termination_notification).await;
        (
            received.expect("the original actor finish result was acquired"),
            termination_notification.expect("the original termination notification was acquired"),
        )
    }
}

async fn settle_local_outcome<B, Descendants>(
    mut outcome: LocalOutcome<B, Descendants>,
) -> LocalOutcome<B, Descendants>
where
    B: BehaviorSettlements,
    B::Event: Send + 'static,
{
    let residual = match &mut outcome {
        ActorExecutionOutcome::Completed { residual, .. }
        | ActorExecutionOutcome::BehaviorFailed { residual, .. }
        | ActorExecutionOutcome::InitializationPanicked { residual, .. }
        | ActorExecutionOutcome::ActivationFailed { residual, .. }
        | ActorExecutionOutcome::SettlementFailed { residual, .. }
        | ActorExecutionOutcome::TransitionPanicked { residual, .. }
        | ActorExecutionOutcome::HostExecutionPanicked { residual, .. }
        | ActorExecutionOutcome::ActivationPanicked { residual, .. }
        | ActorExecutionOutcome::RetirementPanicked { residual, .. }
        | ActorExecutionOutcome::InterpreterContractFailed { residual, .. } => Some(residual),
        ActorExecutionOutcome::Panicked | ActorExecutionOutcome::Cancelled => None,
    };
    if let Some(residual) = residual {
        residual.receive_activation_tasks().await;
    }
    outcome
}

async fn startup_failure<B, Descendants>(
    task: JoinHandle<LocalOutcome<B, Descendants>>,
    cancellation: OwnerCancellationAuthority,
    termination_notification: oneshot::Receiver<Result<(), RetirementNotificationError>>,
) -> SpawnError<B, Descendants>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    B::Event: Send + 'static,
{
    let joined = cancellation.finish(task).await;
    let termination_notification = termination_notification
        .await
        .unwrap_or_else(|error| Err(RetirementNotificationError::ReceiptClosed { error }));
    match joined {
        Ok(outcome) => SpawnError::from_local(outcome, termination_notification),
        Err(error) => SpawnError::ActorTaskFailed {
            error,
            termination_notification,
        },
    }
}

impl<B, Root> ProjectedTask<B, Root>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    B::Event: Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    <B::Birth as BirthMode>::Child: Send,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    B::InterpretationCustody: Send + 'static,
    B::SourceCustody: Send + 'static,
    ActionSettlementOf<B>: Send + 'static,
    Root: Send + 'static,
{
    pub(crate) fn project<Origin, ChildFailures>(
        task: OwnedTask<B, (Vec<Root>, ChildFailures)>,
        origin: Origin,
    ) -> Self
    where
        Origin: Send + 'static,
        ChildFailures: Send + 'static,
        Root: ProjectTerminal<Origin, ActorRetirement<B, Root, ChildFailures>>,
    {
        let OwnedTask {
            task: actor_task,
            termination_notification,
            cancellation,
        } = task;
        let (notification_publication, notification_receipt) = oneshot::channel();
        let task = tokio::spawn(async move {
            let joined = actor_task.await;
            let notification = termination_notification
                .await
                .unwrap_or_else(|error| Err(RetirementNotificationError::ReceiptClosed { error }));
            let refused_notification = notification_publication.send(notification).err();
            let projected = match joined {
                Ok(outcome) => Ok(Root::project(origin, ActorRetirement::from_local(outcome))),
                Err(error) => Err(error),
            };
            drop(refused_notification);
            projected
        });
        Self {
            task,
            termination_notification: notification_receipt,
            cancellation,
            behavior: core::marker::PhantomData,
        }
    }
}

impl<B, Root> ProjectedTask<B, Root>
where
    B: Behavior,
{
    pub(crate) fn request_retirement(&mut self) {
        self.cancellation.request();
    }

    /// Request the exact projected child's retirement and acquire the whole
    /// actor/projection result before the borrowing join producer is disposed.
    pub(crate) async fn receive_retirement(
        owned: &mut Option<Self>,
        received: &mut Option<Result<Result<Root, JoinError>, JoinError>>,
        termination_notification: &mut Option<Result<(), RetirementNotificationError>>,
    ) {
        if received.is_none() {
            if let Some(projection_task) = owned.as_mut() {
                projection_task.cancellation.request();
            }
        }
        poll_fn(|context| {
            let Some(projection_task) = owned.as_mut() else {
                return Poll::Ready(());
            };
            if received.is_none() {
                match Pin::new(&mut projection_task.task).poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(joined) => {
                        *received = Some(joined);
                        projection_task.cancellation.disarm();
                    }
                }
            }
            if termination_notification.is_none() {
                match Pin::new(&mut projection_task.termination_notification).poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(notification) => {
                        *termination_notification = Some(notification.unwrap_or_else(|error| {
                            Err(RetirementNotificationError::ReceiptClosed { error })
                        }));
                    }
                }
            }
            drop(owned.take());
            Poll::Ready(())
        })
        .await;
    }

    #[cfg(test)]
    pub(crate) async fn finish(
        self,
    ) -> (
        Result<Result<Root, JoinError>, JoinError>,
        Result<(), RetirementNotificationError>,
    ) {
        let Self {
            task,
            termination_notification,
            cancellation,
            behavior: _,
        } = self;
        let joined = cancellation.finish(task).await;
        let notification = termination_notification
            .await
            .unwrap_or_else(|error| Err(RetirementNotificationError::ReceiptClosed { error }));
        (joined, notification)
    }
}

/// The exact Address-owned endpoint table for one concrete Behavior protocol.
#[doc(hidden)]
pub type ActorSpace<P> = AddressSpace<<P as Protocol>::Addr, ActorRef<P>>;

#[expect(
    clippy::result_large_err,
    reason = "Return the complete original actor rejection by value without adding allocation."
)]
pub(crate) async fn spawn_owned_with<B, I>(
    addresses: ActorSpace<B::Protocol>,
    config: Config,
    address: MailAddr,
    behavior: B,
    make_interpreter: impl FnOnce(
        communication::ControlSender<B::Event>,
        LocalTerminalReports,
        LocalTimers<B::Event>,
        TerminationObservations<MailAddr, B::Event>,
    ) -> I,
) -> Result<OwnedActor<B, I::Retired>, SpawnError<B, I::Retired>>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    B::Event: Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    B::InterpretationCustody: Send + 'static,
    B::SourceCustody: Send + 'static,
    <B::Birth as BirthMode>::Child: Send + 'static,
    I: CommitActions<B> + Send + 'static,
    crate::local::effects::ActionSettlementOf<B>: ClassifySettlement + Send,
    I::Retired: Send + 'static,
{
    spawn_owned_with_mode::<B, I, StandardIngress>(
        addresses,
        config,
        address,
        behavior,
        make_interpreter,
    )
    .await
}

#[cfg(test)]
#[expect(
    clippy::result_large_err,
    reason = "Return the complete original actor rejection by value without adding allocation."
)]
pub(crate) async fn spawn_root_with<B, I>(
    addresses: ActorSpace<B::Protocol>,
    config: Config,
    address: MailAddr,
    behavior: B,
    make_interpreter: impl FnOnce(
        communication::ControlSender<B::Event>,
        LocalTerminalReports,
        LocalTimers<B::Event>,
        TerminationObservations<MailAddr, B::Event>,
    ) -> I,
) -> Result<RootActor<B, I::Retired>, SpawnError<B, I::Retired>>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    B::Event: behavior::InjectEvent<ShutdownRequested, behavior::Here> + Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    B::InterpretationCustody: Send + 'static,
    B::SourceCustody: Send + 'static,
    <B::Birth as BirthMode>::Child: Send + 'static,
    I: CommitActions<B> + Send + 'static,
    crate::local::effects::ActionSettlementOf<B>: ClassifySettlement + Send,
    I::Retired: Send + 'static,
{
    let (actor, shutdown_control, task) = spawn_local_with::<B, I, StandardIngress, _, _, _>(
        addresses,
        config,
        address,
        behavior,
        make_interpreter,
        |environment| {
            let (publication, activation) = oneshot::channel();
            let shutdown_control = environment.shutdown_control();
            let environment = environment.publish_with(move |actor| {
                drop(publication.send(actor));
            });
            (environment, activation, shutdown_control)
        },
    )
    .await?;
    Ok(RootActor {
        actor,
        shutdown_control,
        task,
    })
}

#[expect(
    clippy::result_large_err,
    reason = "Return the complete original actor rejection by value without adding allocation."
)]
pub(crate) async fn spawn_owned_entity_with<B, I>(
    addresses: ActorSpace<B::Protocol>,
    config: Config,
    address: MailAddr,
    behavior: B,
    make_interpreter: impl FnOnce(
        communication::ControlSender<B::Event>,
        LocalTerminalReports,
        LocalTimers<B::Event>,
        TerminationObservations<MailAddr, B::Event>,
    ) -> I,
) -> Result<OwnedActor<B, I::Retired>, SpawnError<B, I::Retired>>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    B::Event: Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    B::InterpretationCustody: Send + 'static,
    B::SourceCustody: Send + 'static,
    <B::Birth as BirthMode>::Child: Send + 'static,
    I: CommitActions<B> + Send + 'static,
    crate::local::effects::ActionSettlementOf<B>: ClassifySettlement + Send,
    I::Retired: Send + 'static,
{
    spawn_owned_with_mode::<B, I, EntityIngress>(
        addresses,
        config,
        address,
        behavior,
        make_interpreter,
    )
    .await
}

#[expect(
    clippy::result_large_err,
    reason = "Return the complete original actor rejection by value without adding allocation."
)]
async fn spawn_owned_with_mode<B, I, M>(
    addresses: ActorSpace<B::Protocol>,
    config: Config,
    address: MailAddr,
    behavior: B,
    make_interpreter: impl FnOnce(
        communication::ControlSender<B::Event>,
        LocalTerminalReports,
        LocalTimers<B::Event>,
        TerminationObservations<MailAddr, B::Event>,
    ) -> I,
) -> Result<OwnedActor<B, I::Retired>, SpawnError<B, I::Retired>>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    M: IngressMode<B, Retired = User<MailAddr, BehaviorMessage<B>>> + 'static,
    B::Event: Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    B::InterpretationCustody: Send + 'static,
    B::SourceCustody: Send + 'static,
    <B::Birth as BirthMode>::Child: Send + 'static,
    I: CommitActions<B> + Send + 'static,
    crate::local::effects::ActionSettlementOf<B>: ClassifySettlement + Send,
    I::Retired: Send + 'static,
{
    let ((actor, binding), control, task) = spawn_local_with::<B, I, M, _, _, _>(
        addresses,
        config,
        address,
        behavior,
        make_interpreter,
        |environment| {
            let (commitment, committed) = oneshot::channel();
            let control = environment.control();
            let environment = environment.with_private_commitment(commitment);
            (environment, committed, control)
        },
    )
    .await?;
    Ok(OwnedActor {
        actor,
        control,
        task,
        binding: Some(binding),
    })
}

#[expect(
    clippy::result_large_err,
    reason = "Return the complete original actor rejection by value without adding allocation."
)]
async fn spawn_local_with<B, I, M, P, Start, Control>(
    addresses: ActorSpace<B::Protocol>,
    config: Config,
    address: MailAddr,
    behavior: B,
    make_interpreter: impl FnOnce(
        communication::ControlSender<B::Event>,
        LocalTerminalReports,
        LocalTimers<B::Event>,
        TerminationObservations<MailAddr, B::Event>,
    ) -> I,
    publish: impl FnOnce(
        LocalEnvironment<B, I, M>,
    ) -> (
        LocalEnvironment<B, I, M, P>,
        oneshot::Receiver<Start>,
        Control,
    ),
) -> Result<(Start, Control, OwnedTask<B, I::Retired>), SpawnError<B, I::Retired>>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    M: IngressMode<B, Retired = User<MailAddr, BehaviorMessage<B>>> + 'static,
    P: FnOnce(ActorRef<B::Protocol>) + Send + 'static,
    B::Event: Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    B::InterpretationCustody: Send + 'static,
    B::SourceCustody: Send + 'static,
    <B::Birth as BirthMode>::Child: Send + 'static,
    I: CommitActions<B> + Send + 'static,
    crate::local::effects::ActionSettlementOf<B>: ClassifySettlement + Send,
    I::Retired: Send + 'static,
    Start: Send,
    Control: Send,
{
    let (cancellation, startup, control, task, termination_notification) = spawn_local_execution(
        addresses,
        config,
        address,
        behavior,
        make_interpreter,
        publish,
    );

    if let Ok(started) = startup.await {
        Ok((
            started,
            control,
            OwnedTask {
                task,
                cancellation,
                termination_notification,
            },
        ))
    } else {
        Err(startup_failure(task, cancellation, termination_notification).await)
    }
}

/// Transfer the original startup authority, receipt, control, and actor join before awaiting activation.
#[expect(
    clippy::type_complexity,
    reason = "five independently owned actor startup values"
)]
pub(crate) fn spawn_local_execution<B, I, M, P, Start, Control>(
    addresses: ActorSpace<B::Protocol>,
    config: Config,
    address: MailAddr,
    behavior: B,
    make_interpreter: impl FnOnce(
        communication::ControlSender<B::Event>,
        LocalTerminalReports,
        LocalTimers<B::Event>,
        TerminationObservations<MailAddr, B::Event>,
    ) -> I,
    publish: impl FnOnce(
        LocalEnvironment<B, I, M>,
    ) -> (
        LocalEnvironment<B, I, M, P>,
        oneshot::Receiver<Start>,
        Control,
    ),
) -> (
    OwnerCancellationAuthority,
    oneshot::Receiver<Start>,
    Control,
    JoinHandle<LocalOutcome<B, I::Retired>>,
    oneshot::Receiver<Result<(), RetirementNotificationError>>,
)
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    M: IngressMode<B, Retired = User<MailAddr, BehaviorMessage<B>>> + 'static,
    P: FnOnce(ActorRef<B::Protocol>) + Send + 'static,
    B::Event: Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    B::InterpretationCustody: Send + 'static,
    B::SourceCustody: Send + 'static,
    <B::Birth as BirthMode>::Child: Send + 'static,
    I: CommitActions<B> + Send + 'static,
    crate::local::effects::ActionSettlementOf<B>: ClassifySettlement + Send,
    I::Retired: Send + 'static,
{
    let (termination_publisher, termination) = observe::pair();
    let (report_sender, selected_report) = oneshot::channel();
    let (cancellation, cancellation_request) = oneshot::channel();
    let cancellation = OwnerCancellationAuthority::new(cancellation);
    let environment = LocalEnvironment::<B, I, M>::prepare(
        address,
        addresses,
        config,
        termination,
        cancellation_request,
        move |control, timers, observations| {
            make_interpreter(
                control,
                LocalTerminalReports::new(report_sender),
                timers,
                observations,
            )
        },
    );
    let (environment, startup, control) = publish(environment);
    let (notification_publication, termination_notification) = oneshot::channel();
    let retirement = LocalRetirement::<B>::new(
        TerminationPublication::new(termination_publisher, selected_report),
        notification_publication,
    );
    let actor_execution = ActorExecution::new(Driver::new(behavior, environment), retirement);
    let task = tokio::spawn(async move {
        let (outcome, refused_notification) = actor_execution.run().await;
        let outcome = settle_local_outcome(outcome).await;
        // Explicit receiver abandonment cannot preempt original owned-task settlement.
        drop(refused_notification);
        outcome
    });
    (
        cancellation,
        startup,
        control,
        task,
        termination_notification,
    )
}

/// Launch one local actor in a focused lower-layer test.
///
/// `commit` receives each complete action value exactly once, including
/// initialization.
#[cfg(test)]
#[expect(
    clippy::result_large_err,
    reason = "Return the complete original actor rejection by value without adding allocation."
)]
pub(crate) async fn launch_inert<B, F>(
    addresses: ActorSpace<B::Protocol>,
    config: Config,
    address: MailAddr,
    behavior: B,
    commit: F,
) -> Result<RootActor<B, ()>, SpawnError<B>>
where
    B: BehaviorSettlements<
            Protocol: Protocol<Addr = MailAddr>,
            Ph = Never,
            Birth = NoBirths,
            Settlements = InterpretedActionSettlement<B>,
            InterpretationCustody = <ActionsOf<B> as ActionSettlements>::InterpretationCustody,
            SourceCustody = <ActionsOf<B> as ActionSettlements>::SourceCustody,
        > + Send
        + 'static,
    B::Event: behavior::InjectEvent<ShutdownRequested, behavior::Here> + Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    B::InterpretationCustody: Send + 'static,
    B::SourceCustody: Send + 'static,
    <B::Birth as BirthMode>::Child: Send + 'static,
    B::Sends: InterpretSends<InertCapabilities, B::Event, Here>,
    <B::Sends as SendSettlements>::InterpretationCustody: Send,
    ActionSettlementOf<B>:
        SourceSettlementCustody<InertCapabilities, B::Event, Custody = B::SourceCustody> + Send,
    F: FnMut(&ActionsOf<B>) + Send + 'static,
{
    spawn_root_with(addresses, config, address, behavior, move |_, _, _, _| {
        ObserveInertActions(commit)
    })
    .await
}

/// Launch one Entity-capable actor in a focused lower-layer test.
#[cfg(test)]
#[expect(
    clippy::result_large_err,
    reason = "Return the complete original actor rejection by value without adding allocation."
)]
pub(crate) async fn launch_inert_entity<B, F>(
    addresses: ActorSpace<B::Protocol>,
    config: Config,
    address: MailAddr,
    behavior: B,
    commit: F,
) -> Result<OwnedActor<B, ()>, SpawnError<B>>
where
    B: BehaviorSettlements<
            Protocol: Protocol<Addr = MailAddr>,
            Ph = Never,
            Birth = NoBirths,
            Settlements = InterpretedActionSettlement<B>,
            InterpretationCustody = <ActionsOf<B> as ActionSettlements>::InterpretationCustody,
            SourceCustody = <ActionsOf<B> as ActionSettlements>::SourceCustody,
        > + Send
        + 'static,
    B::Event: behavior::InjectEvent<ShutdownRequested, behavior::Here> + Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    B::InterpretationCustody: Send + 'static,
    B::SourceCustody: Send + 'static,
    <B::Birth as BirthMode>::Child: Send + 'static,
    B::Sends: InterpretSends<InertCapabilities, B::Event, Here>,
    <B::Sends as SendSettlements>::InterpretationCustody: Send,
    ActionSettlementOf<B>:
        SourceSettlementCustody<InertCapabilities, B::Event, Custody = B::SourceCustody> + Send,
    F: FnMut(&ActionsOf<B>) + Send + 'static,
{
    spawn_owned_entity_with(addresses, config, address, behavior, move |_, _, _, _| {
        ObserveInertActions(commit)
    })
    .await
    .map(|mut owned| {
        owned.acknowledge_binding();
        owned
    })
}

struct LocalRetirement<B>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    termination: TerminationPublication<MailAddr>,
    termination_notification: oneshot::Sender<Result<(), RetirementNotificationError>>,
    behavior: core::marker::PhantomData<fn() -> B>,
}

impl<B> LocalRetirement<B>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn new(
        termination: TerminationPublication<MailAddr>,
        termination_notification: oneshot::Sender<Result<(), RetirementNotificationError>>,
    ) -> Self {
        Self {
            termination,
            termination_notification,
            behavior: core::marker::PhantomData,
        }
    }
}

impl<B, Descendants>
    Retirement<
        B,
        LocalResidual<B, User<MailAddr, BehaviorMessage<B>>, Descendants>,
        B::Error,
        LocalActivationRejection<MailAddr>,
        LocalRetirementRequest,
    > for LocalRetirement<B>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    type Output = (
        LocalOutcome<B, Descendants>,
        Option<RetirementNotificationError>,
    );

    fn retire(self, outcome: LocalOutcome<B, Descendants>) -> Self::Output {
        let publication = catch_unwind(AssertUnwindSafe(|| match &outcome {
            ActorExecutionOutcome::Completed {
                completion:
                    Completion::RetirementRequested(LocalRetirementRequest::OwnerCancellation(
                        OwnerCancellation,
                    )),
                ..
            } => self.termination.publish_owner_cancellation(),
            ActorExecutionOutcome::Completed {
                completion:
                    Completion::RetirementRequested(LocalRetirementRequest::CapabilityFailed(_)),
                ..
            } => self.termination.publish_capability_failure(),
            ActorExecutionOutcome::ActivationFailed {
                error: LocalActivationRejection::HostCommitPanicked(_),
                ..
            } => self.termination.publish_host_panic(),
            _ => self.termination.publish(&outcome),
        }));
        let notification =
            publication.map_err(|payload| RetirementNotificationError::Panicked { payload });
        let refused_notification = match self.termination_notification.send(notification) {
            Ok(()) | Err(Ok(())) => None,
            Err(Err(error)) => Some(error),
        };
        (outcome, refused_notification)
    }
}

#[cfg(test)]
pub(crate) struct ObserveInertActions<F>(F);

#[cfg(test)]
pub(crate) struct InertCapabilities;

#[cfg(test)]
struct ObserveActionsWithRetirement<F, R> {
    observe: F,
    retirement: R,
}

#[cfg(test)]
impl<B, F> CommitActions<B> for ObserveInertActions<F>
where
    B: BehaviorSettlements<
            Protocol: Protocol<Addr = MailAddr>,
            Ph = Never,
            Birth = NoBirths,
            Settlements = ActionSettlement<Creations<Never>, <<B as Behavior>::Sends as SendSettlements>::Settlements, Never>,
            InterpretationCustody = <Actions<MailAddr, Never, <B as Behavior>::Sends, NoBirths> as ActionSettlements>::InterpretationCustody,
            SourceCustody = <Actions<MailAddr, Never, <B as Behavior>::Sends, NoBirths> as ActionSettlements>::SourceCustody,
        >,
    B::Sends: InterpretSends<InertCapabilities, B::Event, Here> + Send,
    <B::Sends as SendSettlements>::InterpretationCustody: Send,
    ActionSettlement<Creations<Never>, <B::Sends as SendSettlements>::Settlements, Never>:
        SourceSettlementCustody<InertCapabilities, B::Event, Custody = B::SourceCustody> + Send,
    B::Event: Send,
    F: FnMut(&Actions<MailAddr, Never, B::Sends, NoBirths>) + Send,
{
    type Retired = ();

    async fn commit(
        &mut self,
        progress: &mut Option<
            InterpretationProgress<ActionsOf<B>, B::InterpretationCustody, ActionSettlementOf<B>>,
        >,
    ) {
        if let Some(InterpretationProgress::Original(actions)) = progress.as_ref() {
            (self.0)(actions);
        }
        ActionsOf::<B>::interpret::<_, B::Event, Here>(progress, &mut InertCapabilities).await;
    }

    async fn offer_next(
        &mut self,
        progress: &mut Option<SourceProgress<ActionSettlementOf<B>, B::SourceCustody>>,
    ) {
        <ActionSettlementOf<B> as SourceSettlementCustody<InertCapabilities, B::Event>>::prepare_source(progress);
        if let Some(SourceProgress::Offering(custody)) = progress {
            <ActionSettlementOf<B> as SourceSettlementCustody<InertCapabilities, B::Event>>::offer_next_to_source(custody, &mut InertCapabilities).await;
        }
        <ActionSettlementOf<B> as SourceSettlementCustody<InertCapabilities, B::Event>>::finish_source(progress);
    }

    async fn next_local_event(&mut self) -> Result<B::Event, JoinError> {
        core::future::pending().await
    }

    async fn receive_retirement(
        interpreter: &mut Option<Self>,
        received: &mut Option<CapabilityRetirement<B::Event, ()>>,
    ) {
        if received.is_some() {
            return;
        }
        let Some(owner) = interpreter.take() else {
            return;
        };
        *received = Some(CapabilityRetirement::without_activations(()));
        drop(owner);
    }

}

#[cfg(test)]
impl<B, F, R> CommitActions<B> for ObserveActionsWithRetirement<F, R>
where
    B: BehaviorSettlements<
            Protocol: Protocol<Addr = MailAddr>,
            Ph = Never,
            Birth = NoBirths,
            Settlements = ActionSettlement<Creations<Never>, <<B as Behavior>::Sends as SendSettlements>::Settlements, Never>,
            InterpretationCustody = <Actions<MailAddr, Never, <B as Behavior>::Sends, NoBirths> as ActionSettlements>::InterpretationCustody,
            SourceCustody = <Actions<MailAddr, Never, <B as Behavior>::Sends, NoBirths> as ActionSettlements>::SourceCustody,
        >,
    B::Sends: InterpretSends<InertCapabilities, B::Event, Here> + Send,
    <B::Sends as SendSettlements>::InterpretationCustody: Send,
    ActionSettlement<Creations<Never>, <B::Sends as SendSettlements>::Settlements, Never>:
        SourceSettlementCustody<InertCapabilities, B::Event, Custody = B::SourceCustody> + Send,
    B::Event: Send,
    F: FnMut(&Actions<MailAddr, Never, B::Sends, NoBirths>) + Send,
    R: Send,
{
    type Retired = R;

    async fn commit(
        &mut self,
        progress: &mut Option<
            InterpretationProgress<ActionsOf<B>, B::InterpretationCustody, ActionSettlementOf<B>>,
        >,
    ) {
        if let Some(InterpretationProgress::Original(actions)) = progress.as_ref() {
            (self.observe)(actions);
        }
        ActionsOf::<B>::interpret::<_, B::Event, Here>(progress, &mut InertCapabilities).await;
    }

    async fn offer_next(
        &mut self,
        progress: &mut Option<SourceProgress<ActionSettlementOf<B>, B::SourceCustody>>,
    ) {
        <ActionSettlementOf<B> as SourceSettlementCustody<InertCapabilities, B::Event>>::prepare_source(progress);
        if let Some(SourceProgress::Offering(custody)) = progress {
            <ActionSettlementOf<B> as SourceSettlementCustody<InertCapabilities, B::Event>>::offer_next_to_source(custody, &mut InertCapabilities).await;
        }
        <ActionSettlementOf<B> as SourceSettlementCustody<InertCapabilities, B::Event>>::finish_source(progress);
    }

    async fn next_local_event(&mut self) -> Result<B::Event, JoinError> {
        core::future::pending().await
    }

    async fn receive_retirement(
        interpreter: &mut Option<Self>,
        received: &mut Option<CapabilityRetirement<B::Event, R>>,
    ) {
        if received.is_some() {
            return;
        }
        let Some(owner) = interpreter.take() else {
            return;
        };
        *received = Some(CapabilityRetirement::without_activations(owner.retirement));
        drop(owner.observe);
    }

}

#[cfg(test)]
mod tests {
    use crate::terminal::ChildOrigin;
    use std::any::Any;
    use std::convert::Infallible;
    use std::error::Error as StdError;
    use std::future::pending;
    use std::panic::resume_unwind;
    use std::ptr;
    use std::sync::Arc;

    use behavior::{
        ActionSettlement, Actions, BehaviorActed, CreationSequence, Creations, EventLayer,
        InitializationTurn, MessageProtocol, Never, NoBirths, NoSends, SettlementStatus, User,
    };
    use behavior_actors::{Crash, ShutdownRequested};
    use bombay_engine::Completion;
    use communication::{Config, Drained};

    use super::*;
    use crate::MailAddr;
    use crate::local::execution::ActivationTasks;

    struct RootProbe {
        terminal_marker: u8,
    }

    struct ProbeChildRole;

    #[derive(crate::TerminalProjection)]
    enum ProbeTerminal {
        #[application_actor]
        Probe {
            origin: ChildOrigin<RootProbe, ProbeChildRole>,
            terminal: ActorRetirement<RootProbe, Self, ()>,
        },
    }

    fn ignore_root_actions(_: &ActionsOf<RootProbe>) {}

    fn closed_parent_admission<T>(terminal: T) -> Result<(), T> {
        Err(terminal)
    }

    impl Behavior for RootProbe {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = EventLayer<ShutdownRequested, User<MailAddr, Never>>;
        type Sends = NoSends;
        type Ph = Never;
        type Error = Infallible;
        type Birth = NoBirths;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            self.terminal_marker = 19;
            Ok(Actions::stop())
        }

        fn transition(
            &mut self,
            _: behavior::ActiveTurn,
            event: Self::Event,
        ) -> BehaviorActed<Self> {
            match event {
                EventLayer::Owned(_) => Ok(Actions::stop()),
                EventLayer::Inner(user) => match user.message {},
            }
        }
    }

    #[test]
    fn spawn_error_names_the_exact_startup_failure() {
        let failure = SpawnError::<RootProbe>::Panicked {
            termination_notification: Ok(()),
        };
        assert_eq!(format!("{failure:?}"), "Panicked");
        assert_eq!(failure.to_string(), "actor initialization panicked");
    }

    #[test]
    fn owner_cancellation_request_and_disarm_transfer_distinct_authority() {
        let (request_sender, mut requested) = oneshot::channel();
        let mut request_authority = OwnerCancellationAuthority::new(request_sender);
        request_authority.request();
        assert!(matches!(requested.try_recv(), Ok(OwnerCancellation)));

        let (disarm_sender, mut disarmed) = oneshot::channel();
        let mut disarm_authority = OwnerCancellationAuthority::new(disarm_sender);
        disarm_authority.disarm();
        drop(disarm_authority);
        assert!(matches!(
            disarmed.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
    }

    #[tokio::test]
    async fn startup_failure_retains_original_native_payload_lifetime() {
        let payload = Box::new(Arc::new(vec![173_u64, 179, 181]));
        let payload_identity: *const (dyn Any + Send) = payload.as_ref();
        let retained = Arc::downgrade(payload.as_ref());
        let (cancellation, request) = oneshot::channel();
        let task: JoinHandle<LocalOutcome<RootProbe, ()>> = tokio::spawn(async move {
            resume_unwind(payload);
        });
        let original_task = task.id();
        println!(
            "original native startup task: {original_task:?}, cause: {:p}",
            payload_identity.cast::<()>(),
        );
        let failure = startup_failure(task, OwnerCancellationAuthority::new(cancellation), {
            let (publication, receipt) = oneshot::channel();
            drop(publication);
            receipt
        })
        .await;
        let cancellation_request = request.await;
        assert!(cancellation_request.is_err());
        assert_eq!(retained.strong_count(), 1);
        assert!(StdError::source(&failure).is_some());
        let SpawnError::ActorTaskFailed {
            error: task_failure,
            termination_notification,
        } = failure
        else {
            panic!("startup returns its original executor failure");
        };
        assert!(matches!(
            termination_notification,
            Err(RetirementNotificationError::ReceiptClosed { .. })
        ));
        assert_eq!(task_failure.id(), original_task);
        assert!(task_failure.is_panic());
        let received = task_failure.into_panic();
        let received_identity: *const (dyn Any + Send) = received.as_ref();
        assert!(ptr::eq(
            received_identity.cast::<()>(),
            payload_identity.cast::<()>(),
        ));
        drop(received);
        assert_eq!(retained.strong_count(), 0);
    }

    #[tokio::test]
    async fn startup_failure_retains_cancelled_task_failure_source() {
        let (cancellation, request) = oneshot::channel();
        let task = tokio::spawn(pending::<LocalOutcome<RootProbe, ()>>());
        let original_task = task.id();
        task.abort();
        let failure = startup_failure(task, OwnerCancellationAuthority::new(cancellation), {
            let (publication, receipt) = oneshot::channel();
            drop(publication);
            receipt
        })
        .await;
        let cancellation_request = request.await;
        println!("original cancelled startup task: {original_task:?}");
        assert!(cancellation_request.is_err());
        assert!(StdError::source(&failure).is_some());
        let SpawnError::ActorTaskFailed {
            error: task_failure,
            termination_notification,
        } = failure
        else {
            panic!("startup returns its original cancelled executor failure");
        };
        assert!(matches!(
            termination_notification,
            Err(RetirementNotificationError::ReceiptClosed { .. })
        ));
        assert_eq!(task_failure.id(), original_task);
        assert!(task_failure.is_cancelled());
    }

    #[tokio::test]
    async fn owned_outcome_classifies_task_panic_and_cancellation_separately() {
        let panicking: JoinHandle<LocalOutcome<RootProbe, ()>> =
            tokio::spawn(async { panic!("the actor task panicked") });
        let panic_task = panicking.id();
        let (cancellation, _request) = oneshot::channel();
        let failure = startup_failure(panicking, OwnerCancellationAuthority::new(cancellation), {
            let (publication, receipt) = oneshot::channel();
            drop(publication);
            receipt
        })
        .await;
        let SpawnError::ActorTaskFailed {
            error: panic_error,
            termination_notification,
        } = failure
        else {
            panic!("the original panic remains an owned actor task failure");
        };
        assert!(matches!(
            termination_notification,
            Err(RetirementNotificationError::ReceiptClosed { .. })
        ));
        assert_eq!(panic_error.id(), panic_task);
        assert!(panic_error.is_panic());

        let pending_task = tokio::spawn(pending::<LocalOutcome<RootProbe, ()>>());
        let cancellation_task = pending_task.id();
        pending_task.abort();
        let (cancellation, _request) = oneshot::channel();
        let failure = startup_failure(
            pending_task,
            OwnerCancellationAuthority::new(cancellation),
            {
                let (publication, receipt) = oneshot::channel();
                drop(publication);
                receipt
            },
        )
        .await;
        let SpawnError::ActorTaskFailed {
            error: cancelled,
            termination_notification,
        } = failure
        else {
            panic!("the original cancellation remains an owned actor task failure");
        };
        assert!(matches!(
            termination_notification,
            Err(RetirementNotificationError::ReceiptClosed { .. })
        ));
        assert_eq!(cancelled.id(), cancellation_task);
        assert!(cancelled.is_cancelled());
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "keep the complete outside-fold controller, joined disposal and whole typed original-value oracles together; shortening it would split the single custody law or weaken observations"
    )]
    async fn owned_outcome_preserves_activation_panic_and_cancellation() {
        let panic_payload: Box<dyn Any + Send> = Box::new(vec![19_u64, 23]);
        let panic_payload_identity: *const (dyn Any + Send) = panic_payload.as_ref();
        let panicking = tokio::spawn(async move {
            resume_unwind(panic_payload);
        });
        let panic_task_id = panicking.id();
        let pending_task = tokio::spawn(async { pending::<()>().await });
        let cancellation_task_id = pending_task.id();
        assert_ne!(panic_task_id, cancellation_task_id);
        let panic_error = panicking
            .await
            .expect_err("the original activation panic remains a native task failure");
        pending_task.abort();
        let cancellation = pending_task
            .await
            .expect_err("the original activation cancellation remains a native task failure");
        assert!(panic_error.is_panic());
        assert!(cancellation.is_cancelled());

        for (failure, task_id) in [
            (panic_error, panic_task_id),
            (cancellation, cancellation_task_id),
        ] {
            let later_task = tokio::spawn(async { pending::<()>().await });
            let later_task_id = later_task.id();
            later_task.abort();
            let later_failure = later_task
                .await
                .expect_err("a coexisting native failure remains independently owned");
            let descendant_values = vec![31_u64, 37];
            let descendant_allocation = descendant_values.as_ptr();
            let outcome: LocalOutcome<RootProbe, Vec<u64>> = ActorExecutionOutcome::Completed {
                behavior: RootProbe {
                    terminal_marker: 41,
                },
                completion: Completion::RetirementRequested(
                    LocalRetirementRequest::CapabilityFailed(failure),
                ),
                residual: LocalResidual::Retired {
                    interpretation: None,
                    source: None,
                    settlements: vec![ActionSettlement {
                        creations: Creations::empty(),
                        sends: NoSends,
                        become_: behavior::Step::Continue,
                    }],
                    received_interpretation: None,
                    received_source: None,
                    source_index: None,
                    acquired_ingress: None,
                    ingress: Drained {
                        control: vec![EventLayer::Owned(ShutdownRequested)],
                        user: Vec::new(),
                    },
                    activation_tasks: ActivationTasks::new(),
                    descendants: descendant_values,
                    capability_failures: vec![later_failure],
                    terminal_report: Some(Err(Err(Crash::EnvironmentFailed))),
                    retirement_failures: Vec::new(),
                    unread_owner_cancellation: None,
                },
                additional_failures: vec![DriverError::Activation(
                    LocalActivationRejection::BindingAbandoned,
                )],
            };
            let producer = tokio::spawn(async move { outcome });
            let (cancellation, _request) = oneshot::channel();
            let failure =
                startup_failure(producer, OwnerCancellationAuthority::new(cancellation), {
                    let (publication, receipt) = oneshot::channel();
                    drop(publication);
                    receipt
                })
                .await;
            let SpawnError::Unpublished {
                outcome: returned,
                termination_notification,
            } = failure
            else {
                panic!("the complete original local outcome remains owned before publication");
            };
            assert!(matches!(
                termination_notification,
                Err(RetirementNotificationError::ReceiptClosed { .. })
            ));
            let ActorExecutionOutcome::Completed {
                behavior,
                completion:
                    Completion::RetirementRequested(LocalRetirementRequest::CapabilityFailed(failure)),
                residual:
                    LocalResidual::Retired {
                        interpretation,
                        source,
                        settlements,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        ingress,
                        activation_tasks,
                        descendants,
                        mut capability_failures,
                        terminal_report,
                        retirement_failures,
                        unread_owner_cancellation,
                    },
                additional_failures,
            } = returned
            else {
                drop(returned);
                panic!(
                    "activation failure cannot become ordinary completion or actor-task classification"
                );
            };
            assert_eq!(behavior.terminal_marker, 41);
            assert!(interpretation.is_none());
            assert!(source.is_none());
            let expected_settlements = vec![ActionSettlement {
                creations: Creations::empty(),
                sends: NoSends,
                become_: behavior::Step::Continue,
            }];
            assert_eq!(settlements, expected_settlements);
            assert!(received_interpretation.is_none());
            assert!(received_source.is_none());
            assert!(source_index.is_none());
            assert!(acquired_ingress.is_none());
            assert!(matches!(
                ingress.control.as_slice(),
                [EventLayer::Owned(ShutdownRequested)]
            ));
            assert_eq!(ingress.user, []);
            assert!(activation_tasks.is_empty());
            assert_eq!(descendants.as_slice(), [31, 37]);
            assert_eq!(descendants.as_ptr(), descendant_allocation);
            assert_eq!(terminal_report, Some(Err(Err(Crash::EnvironmentFailed))));
            assert!(retirement_failures.is_empty());
            assert!(unread_owner_cancellation.is_none());
            assert!(matches!(
                additional_failures.as_slice(),
                [DriverError::Activation(
                    LocalActivationRejection::BindingAbandoned
                )]
            ));
            assert_eq!(capability_failures.len(), 1);
            let later_failure = capability_failures
                .pop()
                .expect("the whole original later failure remains in its own lane");
            assert_eq!(later_failure.id(), later_task_id);
            assert!(later_failure.is_cancelled());
            assert_eq!(failure.id(), task_id);
            if task_id == panic_task_id {
                assert!(failure.is_panic());
                let payload = failure.into_panic();
                let received_payload_identity: *const (dyn Any + Send) = payload.as_ref();
                assert!(ptr::eq(received_payload_identity, panic_payload_identity));
            } else {
                assert!(failure.is_cancelled());
            }
        }
    }

    #[tokio::test]
    async fn owned_root_task_returns_final_behavior_and_complete_residual() {
        let unpublished = spawn_root_with(
            ActorSpace::new(),
            Config::new(2),
            MailAddr::APPLICATION_ROOT,
            RootProbe { terminal_marker: 0 },
            |_, _, _, _| ObserveInertActions(ignore_root_actions),
        )
        .await;

        let outcome = match unpublished {
            Err(SpawnError::Unpublished {
                outcome,
                termination_notification,
            }) => {
                termination_notification.expect("the actual first publication succeeded");
                outcome
            }
            Err(error) => panic!("the committed root was misclassified: {error:?}"),
            Ok(_) => panic!("a stopping root was publicly published"),
        };
        let ActorExecutionOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    interpretation,
                    source,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    terminal_report,
                    retirement_failures,

                    capability_failures,
                    unread_owner_cancellation,
                    settlements,
                    ingress,
                    activation_tasks,
                    descendants,
                },
            completion,
            additional_failures,
        } = outcome
        else {
            panic!("the root task lost its complete terminal outcome")
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());

        assert_eq!(behavior.terminal_marker, 19);
        let settlement_status = settlements.settlement_status();
        assert_eq!(settlement_status, SettlementStatus::Accepted);
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(ingress.user.len(), 0);
        assert!(activation_tasks.is_empty());
        assert_eq!(descendants, ());
        assert!(matches!(completion, Completion::Stopped));
    }

    #[tokio::test]
    async fn address_reservation_rejection_returns_untouched_initialization() {
        let addresses = ActorSpace::new();
        let reservation = addresses
            .try_reserve(MailAddr::APPLICATION_ROOT)
            .expect("the competing reservation must own the address");
        let rejection = spawn_root_with(
            addresses.clone(),
            Config::new(2),
            MailAddr::APPLICATION_ROOT,
            RootProbe { terminal_marker: 0 },
            |_, _, _, _| {
                ObserveInertActions(|_: &ActionsOf<RootProbe>| {
                    panic!("rejected initialization must remain uncommitted")
                })
            },
        )
        .await;

        let Err(SpawnError::HostRejected {
            termination_notification,
            additional_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            terminal_report,
            retirement_failures,
            capability_failures,
            unread_owner_cancellation,
            behavior,
            initialization,
            error: ClaimError::AddressInUse(address),
            control,
            user,
            descendants,
        }) = rejection
        else {
            panic!("reservation refusal must preserve the exact uncommitted root")
        };
        termination_notification.expect("the actual first publication succeeded");
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(additional_failures.is_empty());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert_eq!(address, MailAddr::APPLICATION_ROOT);
        assert_eq!(behavior.terminal_marker, 19);
        let InterpretationProgress::Original(initialization) = initialization else {
            panic!("uncommitted initialization must retain its exact original whole Actions");
        };
        assert_eq!(initialization.sends, NoSends);
        assert!(initialization.creates.is_empty());
        assert!(matches!(initialization.become_, behavior::Step::Stop(_)));
        assert_eq!(control.len(), 0);
        assert_eq!(user.len(), 0);
        assert_eq!(descendants, ());
        assert!(addresses.resolve(&MailAddr::APPLICATION_ROOT).is_none());
        drop(reservation);
    }

    #[tokio::test]
    async fn owned_child_task_returns_final_behavior_and_complete_residual() {
        let mut child = spawn_owned_with(
            ActorSpace::new(),
            Config::new(2),
            MailAddr(1),
            RootProbe { terminal_marker: 0 },
            |_, _, _, _| ObserveInertActions(ignore_root_actions),
        )
        .await
        .expect("the child must transfer private commitment before publication");

        child.acknowledge_binding();
        assert!(child.binding.is_none());
        let outcome = child.task.finish().await;
        outcome
            .1
            .expect("the actual first termination publication succeeded");
        let outcome = outcome
            .0
            .expect("the child actor returns its exact outcome");
        let ActorExecutionOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    interpretation,
                    source,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    terminal_report,
                    retirement_failures,

                    capability_failures,
                    unread_owner_cancellation,
                    settlements,
                    ingress,
                    activation_tasks,
                    descendants,
                },
            completion,
            additional_failures,
        } = outcome
        else {
            panic!("the child task lost its complete terminal outcome")
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());

        assert_eq!(behavior.terminal_marker, 19);
        let settlement_status = settlements.settlement_status();
        assert_eq!(settlement_status, SettlementStatus::Accepted);
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(ingress.user.len(), 0);
        assert!(activation_tasks.is_empty());
        assert_eq!(descendants, ());
        assert!(matches!(completion, Completion::Stopped));
    }

    #[tokio::test]
    async fn abandoned_private_binding_returns_untouched_initialization() {
        let addresses = ActorSpace::new();
        let mut child = spawn_owned_with(
            addresses.clone(),
            Config::new(2),
            MailAddr(1),
            RootProbe { terminal_marker: 0 },
            |_, _, _, _| ObserveInertActions(ignore_root_actions),
        )
        .await
        .expect("reservation and private commitment must succeed");

        assert!(addresses.resolve(&MailAddr(1)).is_none());
        let binding = child
            .binding
            .take()
            .expect("private binding awaits its owner");
        drop(binding);
        let outcome = child.task.finish().await;
        outcome
            .1
            .expect("the actual first termination publication succeeded");
        let outcome = outcome
            .0
            .expect("the child actor returns its exact outcome");
        let ActorExecutionOutcome::ActivationFailed {
            behavior,
            residual:
                LocalResidual::Uncommitted {
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    terminal_report,
                    retirement_failures,

                    capability_failures,
                    unread_owner_cancellation,
                    initialization,
                    ingress,
                    activation_tasks,
                    descendants,
                },
            error: LocalActivationRejection::BindingAbandoned,
            additional_failures,
        } = outcome
        else {
            panic!("abandonment must return the exact uncommitted child and actions")
        };
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());

        assert_eq!(behavior.terminal_marker, 19);
        let InterpretationProgress::Original(initialization) = initialization else {
            panic!("uncommitted initialization must retain its exact original whole Actions");
        };
        assert_eq!(initialization.sends, NoSends);
        assert!(initialization.creates.is_empty());
        assert!(matches!(initialization.become_, behavior::Step::Stop(_)));
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(ingress.user.len(), 0);
        assert!(activation_tasks.is_empty());
        assert_eq!(descendants, ());
        assert!(addresses.resolve(&MailAddr(1)).is_none());
        let termination = child.actor.termination().await;
        assert_eq!(termination, Err(Crash::EnvironmentFailed));
        let replay = child.actor.termination().await;
        assert_eq!(replay, termination);
    }

    #[tokio::test]
    async fn projected_child_retirement_preserves_origin_state_and_descendants() {
        let mut descendant_creations = CreationSequence::new();
        let descendant_nonce = descendant_creations
            .issue()
            .expect("the descendant's first creator-local ID exists")
            .get();
        let descendant_origin =
            ChildOrigin::<RootProbe, ProbeChildRole>::new(MailAddr(2), descendant_nonce);
        let descendant = ProbeTerminal::Probe {
            origin: descendant_origin,
            terminal: ActorRetirement::Cancelled,
        };
        let mut child = spawn_owned_with(
            ActorSpace::new(),
            Config::new(2),
            MailAddr(1),
            RootProbe { terminal_marker: 0 },
            |_, _, _, _| ObserveActionsWithRetirement {
                observe: ignore_root_actions,
                retirement: (vec![descendant], ()),
            },
        )
        .await
        .expect("the child must transfer private commitment before publication");
        child.acknowledge_binding();
        assert!(child.binding.is_none());
        let mut child_creations = CreationSequence::new();
        let child_nonce = child_creations
            .issue()
            .expect("the child's first creator-local ID exists")
            .get();
        let child_origin = ChildOrigin::<RootProbe, ProbeChildRole>::new(MailAddr(1), child_nonce);

        let terminal = ProjectedTask::project(child.task, child_origin)
            .finish()
            .await;
        terminal
            .1
            .expect("the actual first termination publication succeeded");
        let terminal = terminal
            .0
            .expect("the projection task returns its exact result")
            .expect("the child actor returns its exact terminal");
        let rejected = closed_parent_admission(terminal)
            .expect_err("closed parent admission returns the exact terminal value");
        let ProbeTerminal::Probe { origin, terminal } = rejected;
        let ActorRetirement::Completed {
            interpretation,
            source,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            terminal_report,
            retirement_failures,
            additional_failures,

            capability_failures,
            unread_owner_cancellation,
            behavior,
            settlements,
            control,
            user,
            descendants,
            child_failures: (),
            completion,
        } = terminal
        else {
            panic!("the projected child must preserve its completed disposition")
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());

        assert_eq!(origin, child_origin);
        assert_eq!(behavior.terminal_marker, 19);
        let settlement_status = settlements.settlement_status();
        assert_eq!(settlement_status, SettlementStatus::Accepted);
        assert_eq!(control.len(), 0);
        assert_eq!(user.len(), 0);
        assert_eq!(descendants.len(), 1);
        let ProbeTerminal::Probe {
            origin,
            terminal: ActorRetirement::Cancelled,
        } = descendants
            .into_iter()
            .next()
            .expect("the nested terminal remains in the child retirement")
        else {
            panic!("the exact nested cancellation must remain classified")
        };
        assert_eq!(origin, descendant_origin);
        assert!(matches!(completion, Completion::Stopped));
    }
}

#[cfg(test)]
mod ordinary_owner_retirement {
    use super::ObserveInertActions;
    use crate::MailAddr;
    use crate::launch::ActorSpace;
    use crate::local::environment::{LocalEnvironment, LocalResidual};
    use crate::local::execution::{LocalRetirementRequest, OwnerCancellation};
    use crate::local::ingress::StandardIngress;
    use crate::observe;
    use behavior::{
        Actions, ActiveTurn, Behavior, BehaviorActed, EventLayer, Here, InjectEvent,
        MessageProtocol, Never, NoBirths, NoSends, User,
    };
    use behavior_actors::ShutdownRequested;
    use bombay_engine::{ActionsOf, Completion, Driver};
    use communication::Config;
    use std::sync::Arc;
    use tokio::sync::oneshot;

    struct WaitingActor {
        final_payload: Arc<Vec<u8>>,
    }
    impl Behavior for WaitingActor {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = EventLayer<ShutdownRequested, User<MailAddr, Never>>;
        type Sends = NoSends;
        type Ph = Never;
        type Birth = NoBirths;
        type Error = Never;
        fn transition(&mut self, _: ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }
    }

    #[tokio::test]
    async fn ordinary_owner_retirement_is_distinct_from_live_source_exhaustion() {
        let final_payload = Arc::new(vec![8, 13, 21]);
        let allocation = final_payload.as_ptr();
        let retained_payload = Arc::downgrade(&final_payload);
        let (publisher, observation) = observe::pair();
        let (request, cancellation) = oneshot::channel();
        let environment = LocalEnvironment::<WaitingActor, _, StandardIngress>::prepare(
            MailAddr(94),
            ActorSpace::new(),
            Config::new(2),
            observation,
            cancellation,
            |_, _, _| ObserveInertActions(|_: &ActionsOf<WaitingActor>| {}),
        );
        let control = environment.control();
        let shutdown = <EventLayer<ShutdownRequested, User<MailAddr, Never>> as InjectEvent<
            ShutdownRequested,
            Here,
        >>::inject_at(ShutdownRequested);
        let admission = control.send(shutdown);
        assert!(
            admission.is_ok(),
            "a live exact control input precedes owner retirement"
        );
        let sent = request.send(OwnerCancellation);
        assert!(sent.is_ok());
        let retirement = Driver::new(WaitingActor { final_payload }, environment)
            .run()
            .await
            .unwrap_or_else(|driver| {
                drop(driver);
                panic!("ordinary owner retirement must return its complete actual Driver result");
            });
        assert!(retirement.additional_failures.is_empty());
        let LocalResidual::Retired {
            interpretation,
            source,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            terminal_report,
            retirement_failures,

            capability_failures,
            unread_owner_cancellation,
            settlements,
            ingress,
            activation_tasks,
            descendants,
        } = retirement.residual
        else {
            panic!("the actual local owner request remains an exact typed fact");
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(settlements.len(), 0);
        assert_eq!(
            ingress.control.len(),
            1,
            "a live admitted control input proves retirement is not source exhaustion"
        );
        assert_eq!(ingress.user.len(), 0);
        assert!(activation_tasks.is_empty());
        assert_eq!(descendants, ());
        assert_eq!(retirement.behavior.final_payload.as_slice(), [8, 13, 21]);
        assert_eq!(retirement.behavior.final_payload.as_ptr(), allocation);
        assert_eq!(retained_payload.strong_count(), 1);
        let completion = retirement
            .disposition
            .expect("actual cancellation crosses the complete retirement barrier");
        let Completion::RetirementRequested(LocalRetirementRequest::OwnerCancellation(
            OwnerCancellation,
        )) = completion
        else {
            panic!(
                "the authoritative owner request cannot claim permanent live-event-source exhaustion"
            );
        };
        eprintln!(
            "ordinary owner request retains exact pendingcontrol/state and affine request in RetirementRequested"
        );
        drop((
            retirement.behavior,
            ingress,
            activation_tasks,
            settlements,
            control,
            publisher,
        ));
        assert_eq!(retained_payload.strong_count(), 0);
    }
}

#[cfg(test)]
mod transitive_source_cancellation {
    use super::ActorExecution;
    use crate::ActorExecutionOutcome;
    use crate::MailAddr;
    use crate::address::ApplicationAddresses;
    use crate::launch::ActorSpace;
    use crate::launch::LocalRetirement;
    use crate::local::children::NoChildBindings;
    use crate::local::effects::LocalTerminalReports;
    use crate::local::effects::{ActionInterpreter, ActionSettlementOf};
    use crate::local::effects::{ApplicationCapabilities, ApplicationCapabilityInputs};
    use crate::local::effects::{CapabilityRetirement, CommitActions};
    use crate::local::environment::{LocalEnvironment, LocalResidual};
    use crate::local::execution::{LocalRetirementRequest, OwnerCancellation};
    use crate::local::ingress::StandardIngress;
    use crate::observe;
    use crate::terminal::{ActorRetirement, LocalOutcome};
    use crate::termination::TerminationPublication;
    use behavior::{
        ActionItemResult, Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorSettlements,
        ClassifySettlement, EventIngress, Here, InitializationTurn, InjectEvent,
        InterpretationProgress, ItemSettlement, MessageProtocol, Never, NoBirths, Own,
        SourceActions, SourceCustody, SourceProgress, Step, User, UserEvent,
    };
    use behavior_actors::{
        Crash, ScheduleAfter, ShutdownRequested, TimerElapsed, TimerGeneration, TimerId,
    };
    use bombay_engine::{ActionsOf, Completion, Driver};
    use communication::Config;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::sync::oneshot;
    use tokio::task::JoinError;

    enum SourceCycleEvent {
        Scheduling(ActionItemResult<ScheduleAfter>),
        Elapsed(TimerElapsed),
        Shutdown(ShutdownRequested),
    }
    impl UserEvent for SourceCycleEvent {
        type Addr = MailAddr;
        type Message = Never;
        fn user(_: MailAddr, message: Never) -> Self {
            match message {}
        }
        fn into_user(self) -> Result<User<MailAddr, Never>, Self> {
            Err(self)
        }
    }
    impl EventIngress<ScheduleAfter, ActionItemResult<ScheduleAfter>> for SourceCycleEvent {
        fn ingress(report: ActionItemResult<ScheduleAfter>) -> Self {
            Self::Scheduling(report)
        }
    }
    impl InjectEvent<TimerElapsed, Here> for SourceCycleEvent {
        fn inject_at(elapsed: TimerElapsed) -> Self {
            Self::Elapsed(elapsed)
        }
    }
    impl InjectEvent<ShutdownRequested, Here> for SourceCycleEvent {
        fn inject_at(shutdown: ShutdownRequested) -> Self {
            Self::Shutdown(shutdown)
        }
    }
    struct SourceCycleActor {
        scheduling_receipts: usize,
    }
    impl SourceCycleActor {
        fn schedule_again() -> ActionsOf<Self> {
            Actions::cont().with_send::<ScheduleAfter, Own>(ScheduleAfter::new(
                TimerId(1),
                TimerGeneration(1),
                Duration::from_hours(1),
            ))
        }
    }
    impl Behavior for SourceCycleActor {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = SourceCycleEvent;
        type Sends = SourceActions<ScheduleAfter>;
        type Ph = Never;
        type Birth = NoBirths;
        type Error = Never;
        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            Ok(
                Self::schedule_again().with_send::<ScheduleAfter, Own>(ScheduleAfter::new(
                    TimerId(2),
                    TimerGeneration(1),
                    Duration::from_hours(1),
                )),
            )
        }
        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            match event {
                SourceCycleEvent::Scheduling(report) => {
                    match report {
                        behavior::SettledItem::Attempted(ItemSettlement::Accepted(_)) => {
                            self.scheduling_receipts += 1;
                            Ok(Self::schedule_again())
                        }
                        behavior::SettledItem::Attempted(ItemSettlement::Rejected {
                            item,
                            reason: _,
                        }) => {
                            // This research actor deliberately retries the exact rejected schedule,
                            // including after timer queue resource exhaustion.
                            Ok(Actions::cont().with_send::<ScheduleAfter, Own>(item))
                        }
                        _ => Ok(Actions::stop()),
                    }
                }
                SourceCycleEvent::Elapsed(TimerElapsed {
                    id: _,
                    generation: _,
                })
                | SourceCycleEvent::Shutdown(_) => Ok(Actions::stop()),
            }
        }
    }
    type SourceCycleInterpreter = ActionInterpreter<ApplicationCapabilities<SourceCycleActor, ()>>;
    struct YieldingSourceInterpreter {
        interpreter: Option<SourceCycleInterpreter>,
        source_offers: usize,
        entered: Option<oneshot::Sender<()>>,
        release_first: Option<oneshot::Receiver<()>>,
        progressed: Option<oneshot::Sender<()>>,
        release_progress: Option<oneshot::Receiver<()>>,
    }
    impl CommitActions<SourceCycleActor> for YieldingSourceInterpreter {
        type Retired = (Vec<Never>, ());
        async fn commit(
            &mut self,
            progress: &mut Option<
                InterpretationProgress<
                    ActionsOf<SourceCycleActor>,
                    <SourceCycleActor as BehaviorSettlements>::InterpretationCustody,
                    ActionSettlementOf<SourceCycleActor>,
                >,
            >,
        ) {
            let interpreter = self
                .interpreter
                .as_mut()
                .expect("the original source interpreter remains installed");
            <SourceCycleInterpreter as CommitActions<SourceCycleActor>>::commit(
                interpreter,
                progress,
            )
            .await;
        }
        async fn offer_next(
            &mut self,
            progress: &mut Option<
                SourceProgress<
                    ActionSettlementOf<SourceCycleActor>,
                    <SourceCycleActor as BehaviorSettlements>::SourceCustody,
                >,
            >,
        ) {
            let interpreter = self
                .interpreter
                .as_mut()
                .expect("the original source interpreter remains installed");
            <SourceCycleInterpreter as CommitActions<SourceCycleActor>>::offer_next(
                interpreter,
                progress,
            )
            .await;
            if matches!(
                progress,
                Some(SourceProgress::Completed(SourceCustody::Admitted(_)))
            ) {
                self.source_offers += 1;
                if self.source_offers == 1 {
                    let sent = self
                        .entered
                        .take()
                        .expect("the first offer has one observer")
                        .send(());
                    sent.expect("the first offer observer remains live");
                    let released = self
                        .release_first
                        .take()
                        .expect("the first offer has one release")
                        .await;
                    released.expect("owner cancellation precedes first source acquisition");
                }
                tokio::task::yield_now().await;
                if self.source_offers == 16 {
                    let sent = self
                        .progressed
                        .take()
                        .expect("source progress has one observer")
                        .send(());
                    sent.expect("the continued source observer remains live");
                    let released = self
                        .release_progress
                        .take()
                        .expect("source progress has one release")
                        .await;
                    released.expect("the external shutdown releases the progress gate");
                }
            }
        }
        async fn next_local_event(&mut self) -> Result<SourceCycleEvent, JoinError> {
            <SourceCycleInterpreter as CommitActions<SourceCycleActor>>::next_local_event(
                self.interpreter
                    .as_mut()
                    .expect("the live source interpreter remains installed"),
            )
            .await
        }
        fn next_deadline(&mut self) -> Option<Instant> {
            <SourceCycleInterpreter as CommitActions<SourceCycleActor>>::next_deadline(
                self.interpreter
                    .as_mut()
                    .expect("the live source interpreter remains installed"),
            )
        }
        fn pop_due(&mut self, now: Instant) -> Option<SourceCycleEvent> {
            <SourceCycleInterpreter as CommitActions<SourceCycleActor>>::pop_due(
                self.interpreter
                    .as_mut()
                    .expect("the live source interpreter remains installed"),
                now,
            )
        }
        async fn receive_retirement(
            interpreter: &mut Option<Self>,
            received: &mut Option<CapabilityRetirement<SourceCycleEvent, Self::Retired>>,
        ) {
            let Some(owner) = interpreter.as_mut() else {
                return;
            };
            <SourceCycleInterpreter as CommitActions<SourceCycleActor>>::receive_retirement(
                &mut owner.interpreter,
                received,
            )
            .await;
            if owner.interpreter.is_none() && received.is_some() {
                drop(interpreter.take());
            }
        }
    }

    #[tokio::test]
    async fn typed_source_owner_retirement_preserves_admitted_and_unoffered_receipts() {
        let (publisher, termination) = observe::pair();
        let terminal_observer = termination.clone();
        let (report, selected_report) = oneshot::channel();
        let (cancel, cancellation) = oneshot::channel();
        let (entered, first_offer) = oneshot::channel();
        let (release_first, first_release) = oneshot::channel();
        let (progressed, continued_source) = oneshot::channel();
        let (release_progress, progress_release) = oneshot::channel();
        let addresses = ActorSpace::new();
        let observer = addresses.clone();
        let address = MailAddr(91);
        let environment = LocalEnvironment::<
            SourceCycleActor,
            YieldingSourceInterpreter,
            StandardIngress,
        >::prepare(
            address,
            addresses,
            Config::new(2),
            termination,
            cancellation,
            move |control, timers, observations| {
                let capabilities =
                    ApplicationCapabilities::<SourceCycleActor, ()>::new_with_bindings(
                        ApplicationCapabilityInputs {
                            address,
                            actor_spaces: Arc::new(()),
                            allocations: ApplicationAddresses::new(),
                            control,
                            timers,
                            observations,
                            terminal_reports: LocalTerminalReports::new(report),
                        },
                        NoChildBindings::default(),
                    );
                YieldingSourceInterpreter {
                    interpreter: Some(ActionInterpreter::new(capabilities)),
                    source_offers: 0,
                    entered: Some(entered),
                    release_first: Some(first_release),
                    progressed: Some(progressed),
                    release_progress: Some(progress_release),
                }
            },
        );
        let control = environment.control();
        let (notification_publication, notification_receipt) = oneshot::channel();
        let retirement = LocalRetirement::<SourceCycleActor>::new(
            TerminationPublication::new(publisher, selected_report),
            notification_publication,
        );
        let task = tokio::spawn(
            ActorExecution::new(
                Driver::new(
                    SourceCycleActor {
                        scheduling_receipts: 0,
                    },
                    environment,
                ),
                retirement,
            )
            .run(),
        );
        first_offer
            .await
            .expect("actual timer receipt was admitted before cancellation");
        let requested = cancel.send(OwnerCancellation);
        assert!(
            requested.is_ok(),
            "the running local environment owns its cancellation receiver"
        );
        release_first
            .send(())
            .expect("the first source gate remains owned");
        let mut task = task;
        let retirement = tokio::select! {
            biased;
            retirement = &mut task => retirement.expect("typed owner retirement completes actual Driver cleanup"),
            progressed = continued_source => {
                progressed.expect("continued source progress is observational evidence of the original omitted retirement boundary");
                release_progress.send(()).expect("the test releases the bounded observer before testing unbounded recurrence");
                if let Ok(retirement) = tokio::time::timeout(Duration::from_millis(250), &mut task).await {
                    retirement.expect("a later typed boundary still returns exact retirement")
                } else {
                    task.abort();
                    let _aborted_test_execution = task.await;
                    panic!("selected typed owner retirement cannot wait for the fair transitive source chain");
                }
            }
        };
        assert_eq!(
            terminal_observer.await,
            Err(Crash::Cancelled),
            "actual LocalRetirement publishes cancellation from its exact owning request"
        );
        let (retirement, refused_notification) = retirement;
        assert!(refused_notification.is_none());
        let notification = notification_receipt
            .await
            .expect("the actual termination cause transferred");
        notification.expect("the actual publication succeeded");
        verify_source_retirement_custody(retirement, &observer, address);

        drop(control);
    }
    fn verify_source_retirement_custody(
        retirement: LocalOutcome<SourceCycleActor, (Vec<Never>, ())>,
        observer: &ActorSpace<<SourceCycleActor as Behavior>::Protocol>,
        address: MailAddr,
    ) {
        let ActorExecutionOutcome::Completed {
            completion:
                Completion::RetirementRequested(LocalRetirementRequest::OwnerCancellation(
                    OwnerCancellation,
                )),
            residual: LocalResidual::Retired {
                activation_tasks, ..
            },
            ..
        } = &retirement
        else {
            panic!(
                "the single affine request selects retirement while complete retired fields remain owned"
            );
        };
        assert!(activation_tasks.is_empty());
        let ActorRetirement::OwnerCancelled {
            interpretation,
            source,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            terminal_report,
            retirement_failures,
            additional_failures,

            child_failures: (),
            capability_failures,
            unread_owner_cancellation,
            behavior,
            settlements,
            control: admitted,
            user,
            descendants,
        } = ActorRetirement::<SourceCycleActor, Never, ()>::from_local(retirement)
        else {
            panic!("the public exact projection preserves the same selected owner fact");
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(
            behavior.scheduling_receipts, 0,
            "an admitted receipt remains exact custody when owner retirement wins before its fold"
        );
        assert_eq!(descendants.len(), 0);
        assert_eq!(user.len(), 0);
        assert_eq!(admitted.len(), 1);
        let SourceCycleEvent::Scheduling(behavior::SettledItem::Attempted(
            ItemSettlement::Accepted(receipt),
        )) = admitted
            .into_iter()
            .next()
            .expect("one admitted source receipt stays owned")
        else {
            panic!("the exact first accepted receipt remains admitted, not replayed or folded");
        };
        assert_eq!(receipt.id, TimerId(1));
        assert_eq!(receipt.generation, TimerGeneration(1));
        assert_eq!(settlements.len(), 1);
        let settlement = settlements
            .into_iter()
            .next()
            .expect("the exact initialization product remains owned");
        let status = settlement.settlement_status();
        assert_eq!(status, behavior::SettlementStatus::Accepted);
        assert!(settlement.creations.is_empty());
        assert!(matches!(settlement.become_, Step::Continue));
        let mut unoffered = settlement.sends.into_inputs();
        assert_eq!(unoffered.len(), 1);
        let behavior::SettledItem::Attempted(ItemSettlement::Accepted(receipt)) = unoffered
            .pop()
            .expect("the untouched accepted suffix is owned")
        else {
            panic!(
                "source retirement keeps the second exact accepted receipt without reinterpretation"
            );
        };
        assert_eq!(receipt.id, TimerId(2));
        assert_eq!(receipt.generation, TimerGeneration(1));
        assert!(observer.resolve(&address).is_none());
        eprintln!(
            "RetirementRequested(OwnerCancellation)/Retired -> public OwnerCancelled; coarse Cancelled; state0; admitted accepted id1/gen1; unoffered accepted id2/gen1; one exact Accepted product; no publication/user/task/descendant"
        );
    }
}

#[cfg(all(test, tokio_unstable))]
mod independent_actor_execution {
    use crate::actor_execution::tests::{
        allocations_during, begin_allocations, current_allocations, finish_allocations,
    };
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::future::{Future, pending};
    use std::hint::black_box;
    use std::sync::{Arc, Mutex, mpsc};
    use std::thread::{self, ThreadId};
    use std::time::Instant;

    use crate::actors::ActorExt;
    use behavior::{
        ActionItem, Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase,
        BehaviorSettlements, ChildCreationOutcome, ChildDelivery, ChildHead, CreateChild,
        CreationId, CreationKind, CreationSequence, CreationSettlement, Creations, Here,
        InitializationTurn, Inside, InterpretItem, InterpretationProgress, InterpreterRequest,
        InterpreterRequests, ItemSettlement, MessageProtocol, Never, NoBirthProtocols, NoBirths,
        NoReturnToEmitter, NoSends, Own, RetirementBirths, SendEffects, SettledItem, SourceCustody,
        SourceProgress, SourceSettlementCustody, Step, User, finish_item, prepare_item,
    };
    use behavior_actors::{Exit, StopOnShutdown};
    use bombay_address::AddressSpace;
    use bombay_engine::{ActionsOf, Completion};
    use communication::Config;
    use tokio::runtime::Builder;
    use tokio::sync::oneshot;
    use tokio::sync::{Barrier as WorkBarrier, Mutex as WorkMutex};
    use tokio::task::{Id, JoinError, id, try_id};

    use super::spawn_root_with;
    use crate::local::effects::ActionSettlementOf;
    use crate::local::effects::{CapabilityRetirement, CommitActions};
    use crate::local::environment::LocalResidual;
    use crate::terminal::LocalOutcome;
    use crate::{
        ActorExecutionOutcome, ActorRetirement, Application, ApplicationOutcome, ChildOrigin,
        MailAddr, ProjectTerminal, actor,
    };

    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    enum WorkOwner {
        Oak,
        Ash,
    }

    #[derive(Debug)]
    enum WorkCommand {
        Compute(Vec<u64>),
        Finish,
    }

    #[derive(Debug)]
    struct ComputeWork {
        values: Vec<u64>,
    }

    #[derive(Debug)]
    struct ComputedWork {
        values: Vec<u64>,
        sum: u64,
    }

    impl InterpreterRequest for ComputeWork {
        type ReturnToEmitter = NoReturnToEmitter;
        type LogicalProtocols = NoBirthProtocols;
    }

    impl ActionItem for ComputeWork {
        type Accepted = ComputedWork;
        type Rejection = Never;
        type Prerequisite = Never;
        type Custody = (Option<Self>, Option<Self::Reply>);
        type Input<'a>
            = &'a mut Option<Self>
        where
            Self: 'a;
        type Reply = ItemSettlement<Self, ComputedWork, Never, Never>;
        fn prepare_interpretation(
            progress: &mut Option<InterpretationProgress<Self, Self::Custody, Self::Reply>>,
        ) {
            prepare_item::<Self>(progress);
        }
        fn interpretation_input<'a>(
            custody: &'a mut Self::Custody,
        ) -> Option<(Self::Input<'a>, &'a mut Option<Self::Reply>)>
        where
            Self: 'a,
        {
            match custody {
                (input @ Some(_), received @ None) => Some((input, received)),
                _ => None,
            }
        }
        fn finish_interpretation(
            progress: &mut Option<InterpretationProgress<Self, Self::Custody, Self::Reply>>,
        ) {
            finish_item::<Self>(progress);
        }

        fn retain_accepted(work: ComputedWork) -> Option<ComputedWork> {
            Some(work)
        }
    }

    struct ComputingActor {
        initialization_count: usize,
        commands: Vec<MailAddr>,
    }

    impl BehaviorBase for ComputingActor {
        type Base = Self;

        fn base(&self) -> &Self {
            self
        }
    }

    impl Behavior for ComputingActor {
        type Protocol = MessageProtocol<MailAddr, WorkCommand>;
        type Event = User<MailAddr, WorkCommand>;
        type Sends = InterpreterRequests<ComputeWork>;
        type Ph = Never;
        type Error = Infallible;
        type Birth = NoBirths;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            self.initialization_count += 1;
            Ok(Actions::cont())
        }

        fn transition(&mut self, _: ActiveTurn, user: Self::Event) -> BehaviorActed<Self> {
            self.commands.push(user.from);
            match user.message {
                WorkCommand::Compute(values) => {
                    let mut actions: ActionsOf<Self> = Actions::cont();
                    actions.sends.send::<_, Own>(ComputeWork { values });
                    Ok(actions)
                }
                WorkCommand::Finish => Ok(Actions::stop()),
            }
        }
    }

    type HostedComputingActor = StopOnShutdown<ComputingActor>;
    type WorkEvent = <HostedComputingActor as Behavior>::Event;

    #[derive(Clone)]
    enum WorkAdmission {
        Independent,
        Serialized(Arc<WorkMutex<()>>),
    }

    #[derive(Debug)]
    enum ExecutionObservation {
        PollStarted {
            task: Id,
            ordinal: u64,
            worker: ThreadId,
        },
        PollReturned {
            task: Id,
            ordinal: u64,
            worker: ThreadId,
        },
        SourceOffered {
            owner: WorkOwner,
            retained_work: usize,
        },
        WaitingForCommand(WorkOwner),
        WorkEntered {
            owner: WorkOwner,
            worker: ThreadId,
            allocation: usize,
            length: usize,
        },
        WorkReturned(WorkOwner),
        PollViolation(Id),
        TaskTerminated(Id),
    }

    enum PollOwnership {
        Returned { next: u64 },
        Running { ordinal: u64, worker: ThreadId },
    }

    struct PollAdmission {
        task: Id,
        permit: mpsc::Receiver<()>,
    }

    struct ActorPolls {
        tasks: HashMap<Id, PollOwnership>,
        admission: Option<PollAdmission>,
        observations: mpsc::Sender<ExecutionObservation>,
    }

    impl ActorPolls {
        fn begin(&mut self, task: Id) -> Option<mpsc::Receiver<()>> {
            let worker = thread::current().id();
            let ordinal = match self.tasks.remove(&task) {
                None => 0,
                Some(PollOwnership::Returned { next }) => next,
                Some(PollOwnership::Running { .. }) => {
                    drop(
                        self.observations
                            .send(ExecutionObservation::PollViolation(task)),
                    );
                    0
                }
            };
            self.tasks
                .insert(task, PollOwnership::Running { ordinal, worker });
            drop(self.observations.send(ExecutionObservation::PollStarted {
                task,
                ordinal,
                worker,
            }));
            if self
                .admission
                .as_ref()
                .is_some_and(|admission| admission.task == task)
            {
                self.admission.take().map(|admission| admission.permit)
            } else {
                None
            }
        }

        fn finish(&mut self, task: Id) {
            match self.tasks.remove(&task) {
                Some(PollOwnership::Running { ordinal, worker }) => {
                    self.tasks
                        .insert(task, PollOwnership::Returned { next: ordinal + 1 });
                    drop(self.observations.send(ExecutionObservation::PollReturned {
                        task,
                        ordinal,
                        worker,
                    }));
                }
                None | Some(PollOwnership::Returned { .. }) => {
                    drop(
                        self.observations
                            .send(ExecutionObservation::PollViolation(task)),
                    );
                }
            }
        }
    }

    struct WorkRelease(Vec<mpsc::Sender<()>>);

    impl Drop for WorkRelease {
        fn drop(&mut self) {
            for release in &self.0 {
                let _gate_release = release.send(());
            }
        }
    }

    struct WorkInterpreter {
        owner: WorkOwner,
        admission: WorkAdmission,
        work_permit: mpsc::Receiver<()>,
        observations: mpsc::Sender<ExecutionObservation>,
        waiting_notice: Option<mpsc::Sender<ExecutionObservation>>,
    }

    impl InterpretItem<ComputeWork, WorkEvent, Inside<Here>> for WorkInterpreter {
        #[expect(
            clippy::manual_async_fn,
            reason = "preserve the original receiving-loan future and its qualified native poll attribution"
        )]
        fn interpret_item<'a>(
            &'a mut self,
            input: <ComputeWork as ActionItem>::Input<'a>,
            received: &'a mut Option<<ComputeWork as ActionItem>::Reply>,
        ) -> impl Future<Output = ()> + Send + 'a
        where
            ComputeWork: 'a,
        {
            async move {
                if received.is_some() {
                    return;
                }
                let Some(work) = input.as_ref() else {
                    return;
                };
                let _exclusive_work = match &self.admission {
                    WorkAdmission::Independent => None,
                    WorkAdmission::Serialized(admission) => Some(admission.lock().await),
                };
                drop(self.observations.send(ExecutionObservation::WorkEntered {
                    owner: self.owner,
                    worker: thread::current().id(),
                    allocation: work.values.as_ptr() as usize,
                    length: work.values.len(),
                }));
                // The runtime owns this gate. No fold contains or calls it.
                let _work_permission = self.work_permit.recv();
                let sum = work.values.iter().sum();
                drop(
                    self.observations
                        .send(ExecutionObservation::WorkReturned(self.owner)),
                );
                let Some(work) = input.take() else {
                    return;
                };
                *received = Some(ItemSettlement::Accepted(ComputedWork {
                    values: work.values,
                    sum,
                }));
            }
        }
    }

    impl CommitActions<HostedComputingActor> for WorkInterpreter {
        type Retired = ();

        async fn commit(
            &mut self,
            progress: &mut Option<
                InterpretationProgress<
                    ActionsOf<HostedComputingActor>,
                    <HostedComputingActor as BehaviorSettlements>::InterpretationCustody,
                    ActionSettlementOf<HostedComputingActor>,
                >,
            >,
        ) {
            ActionsOf::<HostedComputingActor>::interpret::<_, WorkEvent, Here>(progress, self)
                .await;
        }

        async fn offer_next(
            &mut self,
            progress: &mut Option<
                SourceProgress<
                    ActionSettlementOf<HostedComputingActor>,
                    <HostedComputingActor as BehaviorSettlements>::SourceCustody,
                >,
            >,
        ) {
            <ActionSettlementOf<HostedComputingActor> as SourceSettlementCustody<
                Self,
                WorkEvent,
            >>::prepare_source(progress);
            if let Some(SourceProgress::Offering(custody)) = progress.as_mut() {
                <ActionSettlementOf<HostedComputingActor> as SourceSettlementCustody<
                    Self,
                    WorkEvent,
                >>::offer_next_to_source(custody, self)
                .await;
            }
            <ActionSettlementOf<HostedComputingActor> as SourceSettlementCustody<
                Self,
                WorkEvent,
            >>::finish_source(progress);
            let Some(SourceProgress::Completed(custody)) = progress.as_ref() else {
                return;
            };
            match custody {
                SourceCustody::Exhausted(settlement) | SourceCustody::Retained(settlement) => {
                    // All source lanes are checked before the next-port notice.
                    match &settlement.become_ {
                        Step::Continue => {}
                        Step::Goto(never) => match *never {},
                        Step::Stop(_) => panic!("a stop settlement was offered while active"),
                    }
                    assert!(settlement.creations.is_empty());
                    assert_eq!(settlement.sends.owned, NoSends);
                    drop(self.observations.send(ExecutionObservation::SourceOffered {
                        owner: self.owner,
                        retained_work: settlement.sends.inner.len(),
                    }));
                }
                SourceCustody::Admitted(_) | SourceCustody::Closed(_) => {
                    panic!("computation has no emitter source input")
                }
            }
        }

        async fn next_local_event(&mut self) -> Result<WorkEvent, JoinError> {
            if let Some(notice) = self.waiting_notice.take() {
                drop(notice.send(ExecutionObservation::WaitingForCommand(self.owner)));
            }
            pending().await
        }

        async fn receive_retirement(
            interpreter: &mut Option<Self>,
            received: &mut Option<CapabilityRetirement<WorkEvent, ()>>,
        ) {
            if received.is_some() || interpreter.is_none() {
                return;
            }
            *received = Some(CapabilityRetirement::without_activations(()));
            drop(interpreter.take());
        }
    }

    struct ActorPairExecution {
        observations: Vec<ExecutionObservation>,
        oak_task: Id,
        ash_task: Id,
        oak_outcome: LocalOutcome<HostedComputingActor, ()>,
        ash_outcome: LocalOutcome<HostedComputingActor, ()>,
        oak_allocation: usize,
        ash_allocation: usize,
        ash_work_poll: u64,
    }

    fn observe_until(
        observation_receiver: &mpsc::Receiver<ExecutionObservation>,
        observations: &mut Vec<ExecutionObservation>,
        accepted: impl Fn(&ExecutionObservation) -> bool,
    ) {
        loop {
            let observation = observation_receiver
                .recv()
                .expect("the owning runtime observation lane closed");
            if let ExecutionObservation::TaskTerminated(task) = &observation {
                panic!("actor task {task} terminated before its required host observation");
            }
            let matches = accepted(&observation);
            observations.push(observation);
            if matches {
                return;
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Keep the actual actor ownership, source frontier, and deterministic releases in one trace."
    )]
    fn execute_actor_pair(admission: WorkAdmission) -> ActorPairExecution {
        let (observation_sender, observation_receiver) = mpsc::channel();
        let polls = Arc::new(Mutex::new(ActorPolls {
            tasks: HashMap::new(),
            admission: None,
            observations: observation_sender.clone(),
        }));
        let before_polls = Arc::clone(&polls);
        let after_polls = Arc::clone(&polls);
        let terminated = observation_sender.clone();
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .on_before_task_poll(move |task| {
                let permit = before_polls.lock().unwrap().begin(task.id());
                if let Some(permit) = permit {
                    let _poll_permission = permit.recv();
                }
            })
            .on_after_task_poll(move |task| after_polls.lock().unwrap().finish(task.id()))
            .on_task_terminate(move |task| {
                drop(terminated.send(ExecutionObservation::TaskTerminated(task.id())));
            })
            .build()
            .unwrap();
        let (oak_release, oak_permit) = mpsc::channel();
        let (ash_release, ash_permit) = mpsc::channel();
        let (poll_release, poll_permit) = mpsc::channel();
        // This guard is declared after Runtime, so unwinding releases every
        // blocked host/hook before Runtime::drop joins its workers.
        let release = WorkRelease(vec![
            oak_release.clone(),
            ash_release.clone(),
            poll_release.clone(),
        ]);
        let addresses = AddressSpace::new();
        let oak = runtime
            .block_on(spawn_root_with(
                addresses.clone(),
                Config::new(4),
                MailAddr(71),
                ComputingActor {
                    initialization_count: 0,
                    commands: vec![],
                }
                .stop_on_shutdown(),
                {
                    let observations = observation_sender.clone();
                    let admission = admission.clone();
                    move |_, _, _, _| WorkInterpreter {
                        owner: WorkOwner::Oak,
                        admission,
                        work_permit: oak_permit,
                        waiting_notice: Some(observations.clone()),
                        observations,
                    }
                },
            ))
            .unwrap();
        let ash = runtime
            .block_on(spawn_root_with(
                addresses.clone(),
                Config::new(4),
                MailAddr(73),
                ComputingActor {
                    initialization_count: 0,
                    commands: vec![],
                }
                .stop_on_shutdown(),
                {
                    let observations = observation_sender.clone();
                    move |_, _, _, _| WorkInterpreter {
                        owner: WorkOwner::Ash,
                        admission,
                        work_permit: ash_permit,
                        waiting_notice: Some(observations.clone()),
                        observations,
                    }
                },
            ))
            .unwrap();
        let oak_task = oak.task.task.id();
        let ash_task = ash.task.task.id();
        let mut observations = vec![];
        // Startup has already offered the complete initialization. Observe
        // the actual pending next port and its enclosing poll return too.
        let mut waiting_actors = Vec::new();
        let mut returned_actor_polls = Vec::new();
        while !(waiting_actors.contains(&WorkOwner::Oak)
            && waiting_actors.contains(&WorkOwner::Ash)
            && returned_actor_polls.contains(&oak_task)
            && returned_actor_polls.contains(&ash_task))
        {
            let observation = observation_receiver.recv().unwrap();
            match &observation {
                ExecutionObservation::WaitingForCommand(owner) => waiting_actors.push(*owner),
                ExecutionObservation::PollReturned { task, .. }
                    if (*task == oak_task && waiting_actors.contains(&WorkOwner::Oak))
                        || (*task == ash_task && waiting_actors.contains(&WorkOwner::Ash)) =>
                {
                    returned_actor_polls.push(*task);
                }
                ExecutionObservation::TaskTerminated(task) => {
                    panic!("actor {task} terminated before its initial source boundary")
                }
                _ => {}
            }
            observations.push(observation);
        }
        let oak_values = vec![17, 23, 31];
        let ash_values = vec![43, 47, 61];
        let oak_allocation = oak_values.as_ptr() as usize;
        let ash_allocation = ash_values.as_ptr() as usize;
        runtime
            .block_on(
                oak.actor
                    .send_from(MailAddr(79), WorkCommand::Compute(oak_values)),
            )
            .unwrap();
        observe_until(&observation_receiver, &mut observations, |observation| {
            matches!(
                observation,
                ExecutionObservation::WorkEntered {
                    owner: WorkOwner::Oak,
                    ..
                }
            )
        });
        polls.lock().unwrap().admission = Some(PollAdmission {
            task: ash_task,
            permit: poll_permit,
        });
        // Queue the whole typed User before letting its exact task poll run.
        runtime
            .block_on(
                ash.actor
                    .send_from(MailAddr(83), WorkCommand::Compute(ash_values)),
            )
            .unwrap();
        observe_until(
            &observation_receiver,
            &mut observations,
            |observation| matches!(observation, ExecutionObservation::PollStarted { task, .. } if *task == ash_task),
        );
        let ash_work_poll = match observations.last().unwrap() {
            ExecutionObservation::PollStarted { ordinal, .. } => *ordinal,
            _ => unreachable!(),
        };
        let _poll_release = poll_release.send(());
        observe_until(
            &observation_receiver,
            &mut observations,
            |observation| match observation {
                ExecutionObservation::WorkEntered {
                    owner: WorkOwner::Ash,
                    ..
                } => true,
                ExecutionObservation::PollReturned { task, ordinal, .. } => {
                    *task == ash_task && *ordinal == ash_work_poll
                }
                _ => false,
            },
        );
        let _oak_release = oak_release.send(());
        match observations.last() {
            Some(ExecutionObservation::WorkEntered {
                owner: WorkOwner::Ash,
                ..
            }) => {}
            Some(ExecutionObservation::PollReturned { .. }) => {
                observe_until(&observation_receiver, &mut observations, |observation| {
                    matches!(
                        observation,
                        ExecutionObservation::WorkEntered {
                            owner: WorkOwner::Ash,
                            ..
                        }
                    )
                });
            }
            _ => unreachable!("the exact work poll produced a different observation"),
        }
        let _ash_release = ash_release.send(());
        runtime
            .block_on(oak.actor.send_from(MailAddr(89), WorkCommand::Finish))
            .unwrap();
        runtime
            .block_on(ash.actor.send_from(MailAddr(97), WorkCommand::Finish))
            .unwrap();
        let (oak_retirement, oak_notification) = runtime.block_on(oak.task.finish());
        let (ash_retirement, ash_notification) = runtime.block_on(ash.task.finish());
        oak_notification.expect("oak first termination was published");
        ash_notification.expect("ash first termination was published");
        drop(release);
        drop(runtime);
        observations.extend(observation_receiver.try_iter());
        let (oak_outcome, ash_outcome) = match (oak_retirement, ash_retirement) {
            (Ok(oak_outcome), Ok(ash_outcome)) => (oak_outcome, ash_outcome),
            retirements => {
                drop(retirements);
                panic!("both independently joined actors must return their original outcomes");
            }
        };
        ActorPairExecution {
            observations,
            oak_task,
            ash_task,
            oak_outcome,
            ash_outcome,
            oak_allocation,
            ash_allocation,
            ash_work_poll,
        }
    }

    fn verify_retirement(
        outcome: LocalOutcome<HostedComputingActor, ()>,
        allocation: usize,
        values: &[u64],
        origins: &[MailAddr],
    ) {
        let ActorExecutionOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    interpretation,
                    source,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    terminal_report,
                    retirement_failures,
                    settlements,
                    ingress,
                    activation_tasks,
                    descendants,
                    capability_failures,
                    unread_owner_cancellation,
                },
            completion,
            additional_failures,
        } = outcome
        else {
            panic!("the actual actor did not complete its retirement")
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(matches!(completion, Completion::Stopped));
        assert_eq!(behavior.base().initialization_count, 1);
        assert_eq!(behavior.base().commands, origins);
        assert!(ingress.control.is_empty());
        assert!(ingress.user.is_empty());
        assert!(activation_tasks.is_empty());
        assert_eq!(descendants, ());
        assert_eq!(settlements.len(), 2);
        let mut settlements = settlements.into_iter();
        let stopped = settlements.next().unwrap();
        assert!(stopped.creations.is_empty());
        assert_eq!(stopped.sends.owned, NoSends);
        assert!(stopped.sends.inner.is_empty());
        assert!(matches!(stopped.become_, Step::Stop(_)));
        let computed = settlements.next().unwrap();
        assert!(computed.creations.is_empty());
        assert_eq!(computed.sends.owned, NoSends);
        assert_eq!(computed.become_, Step::Continue);
        assert_eq!(computed.sends.inner.len(), 1);
        let SettledItem::Attempted(ItemSettlement::Accepted(work)) =
            computed.sends.inner.into_iter().next().unwrap()
        else {
            panic!("the complete computation receipt was lost")
        };
        assert_eq!(work.values, values);
        assert_eq!(work.values.as_ptr() as usize, allocation);
        let expected_sum: u64 = values.iter().sum();
        assert_eq!(work.sum, expected_sum);
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Verify the complete independent poll, work, and terminal trace together."
    )]
    fn verify_actor_pair(execution: ActorPairExecution) -> usize {
        let ActorPairExecution {
            observations,
            oak_task,
            ash_task,
            oak_outcome,
            ash_outcome,
            oak_allocation,
            ash_allocation,
            ash_work_poll,
        } = execution;
        assert_ne!(oak_task, ash_task);
        let mut active = HashMap::new();
        let mut ordinals = HashMap::new();
        let mut entered = Vec::new();
        let mut maximum_overlap = 0;
        let mut worker_owners = HashMap::new();
        let mut initial_offers = HashMap::new();
        let mut work_offers = HashMap::new();
        let mut waiting_notices = HashMap::new();
        let mut terminations = Vec::new();
        for observation in &observations {
            match observation {
                ExecutionObservation::PollStarted {
                    task,
                    ordinal,
                    worker,
                } => {
                    let previous = active.insert(*task, (*ordinal, *worker));
                    assert_eq!(previous, None, "one actor task was polled concurrently");
                    let expected = ordinals.entry(*task).or_insert(0);
                    assert_eq!(*ordinal, *expected);
                }
                ExecutionObservation::PollReturned {
                    task,
                    ordinal,
                    worker,
                } => {
                    let started = active.remove(task);
                    assert_eq!(started, Some((*ordinal, *worker)));
                    ordinals.insert(*task, ordinal + 1);
                }
                ExecutionObservation::SourceOffered {
                    owner,
                    retained_work,
                } => {
                    assert!(*retained_work <= 1);
                    if *retained_work == 0 {
                        assert!(!worker_owners.contains_key(owner));
                        *initial_offers.entry(*owner).or_insert(0) += 1;
                    } else {
                        *work_offers.entry(*owner).or_insert(0) += 1;
                    }
                }
                ExecutionObservation::WorkEntered {
                    owner,
                    worker,
                    allocation,
                    length,
                } => {
                    let task = match owner {
                        WorkOwner::Oak => oak_task,
                        WorkOwner::Ash => ash_task,
                    };
                    assert_eq!(active.get(&task).map(|(_, worker)| worker), Some(worker));
                    if *owner == WorkOwner::Ash {
                        assert!(
                            active
                                .get(&task)
                                .is_some_and(|(ordinal, _)| *ordinal >= ash_work_poll)
                        );
                    }
                    assert_eq!(*length, 3);
                    assert_eq!(
                        *allocation,
                        match owner {
                            WorkOwner::Oak => oak_allocation,
                            WorkOwner::Ash => ash_allocation,
                        }
                    );
                    let prior_worker = worker_owners.insert(*owner, *worker);
                    assert_eq!(
                        prior_worker, None,
                        "one original work request entered twice"
                    );
                    entered.push(*owner);
                    maximum_overlap = maximum_overlap.max(entered.len());
                }
                ExecutionObservation::WorkReturned(owner) => {
                    let index = entered.iter().position(|entered| entered == owner).unwrap();
                    entered.remove(index);
                }
                ExecutionObservation::WaitingForCommand(owner) => {
                    assert_eq!(initial_offers.get(owner), Some(&1));
                    *waiting_notices.entry(*owner).or_insert(0) += 1;
                }
                ExecutionObservation::TaskTerminated(task) => terminations.push(*task),
                ExecutionObservation::PollViolation(task) => {
                    panic!("invalid task poll ownership: {task}")
                }
            }
        }
        assert!(active.is_empty());
        assert_eq!(entered, []);
        assert_eq!(worker_owners.len(), 2);
        assert_eq!(ordinals.len(), 2);
        for owner in [WorkOwner::Oak, WorkOwner::Ash] {
            assert_eq!(initial_offers.get(&owner), Some(&1));
            assert_eq!(work_offers.get(&owner), Some(&1));
            assert_eq!(waiting_notices.get(&owner), Some(&1));
        }
        let actor_terminations = terminations
            .iter()
            .filter(|task| **task == oak_task || **task == ash_task)
            .collect::<Vec<_>>();
        assert_eq!(actor_terminations.len(), 2);
        assert!(actor_terminations.contains(&&oak_task));
        assert!(actor_terminations.contains(&&ash_task));
        // Task hooks do not count folds. The stronger implication uses the
        // sole consumed Driver, whose exclusive mutable Behavior is owned by
        // this exact actor task for initialization and every event fold.
        verify_retirement(
            oak_outcome,
            oak_allocation,
            &[17, 23, 31],
            &[MailAddr(79), MailAddr(89)],
        );
        verify_retirement(
            ash_outcome,
            ash_allocation,
            &[43, 47, 61],
            &[MailAddr(83), MailAddr(97)],
        );
        maximum_overlap
    }

    #[test]
    fn independent_actors_overlap_runtime_work_on_distinct_workers() {
        let execution = execute_actor_pair(WorkAdmission::Independent);
        let workers = execution
            .observations
            .iter()
            .filter_map(|observation| match observation {
                ExecutionObservation::WorkEntered { worker, .. } => Some(*worker),
                _ => None,
            })
            .collect::<Vec<_>>();
        let overlap = verify_actor_pair(execution);
        assert_eq!(overlap, 2, "independent actor runtime work was serialized");
        assert_eq!(workers.len(), 2);
        assert_ne!(workers[0], workers[1]);
    }

    #[test]
    fn shared_work_serialization_is_observable_without_a_deadline() {
        let execution = execute_actor_pair(WorkAdmission::Serialized(Arc::new(WorkMutex::new(()))));
        let overlap = verify_actor_pair(execution);
        assert_eq!(overlap, 1);
    }
    #[derive(Clone, Copy)]
    enum WorkAllocation {
        Original,
        Additional,
    }

    struct MeasuredWork {
        start: Option<Arc<WorkBarrier>>,
        completion: Option<oneshot::Sender<()>>,
        expected: usize,
        allocation: WorkAllocation,
        observations: Vec<(usize, usize)>,
    }

    impl InterpretItem<ComputeWork, WorkEvent, Inside<Here>> for MeasuredWork {
        #[expect(
            clippy::manual_async_fn,
            reason = "preserve the original receiving-loan future and its qualified native poll attribution"
        )]
        fn interpret_item<'a>(
            &'a mut self,
            input: <ComputeWork as ActionItem>::Input<'a>,
            received: &'a mut Option<<ComputeWork as ActionItem>::Reply>,
        ) -> impl Future<Output = ()> + Send + 'a
        where
            ComputeWork: 'a,
        {
            async move {
                if received.is_some() {
                    return;
                }
                let Some(work) = input.as_ref() else {
                    return;
                };
                if let Some(start) = self.start.take() {
                    start.wait().await;
                }
                // An awaited gate may resume in another poll; snapshot only now.
                let before = current_allocations();
                match self.allocation {
                    WorkAllocation::Original => {}
                    WorkAllocation::Additional => {
                        let original = Box::new(black_box(137_u64));
                        black_box(&original);
                        drop(original);
                    }
                }
                let sum = work.values.iter().sum();
                for _ in 0..256 {
                    let repeated: u64 = black_box(&work.values).iter().sum();
                    black_box(repeated);
                }
                let after = current_allocations();
                self.observations.push((before, after));
                let Some(work) = input.take() else {
                    return;
                };
                *received = Some(ItemSettlement::Accepted(ComputedWork {
                    values: work.values,
                    sum,
                }));
            }
        }
    }

    impl CommitActions<HostedComputingActor> for MeasuredWork {
        type Retired = Vec<(usize, usize)>;
        async fn commit(
            &mut self,
            progress: &mut Option<
                InterpretationProgress<
                    ActionsOf<HostedComputingActor>,
                    <HostedComputingActor as BehaviorSettlements>::InterpretationCustody,
                    ActionSettlementOf<HostedComputingActor>,
                >,
            >,
        ) {
            ActionsOf::<HostedComputingActor>::interpret::<_, WorkEvent, Here>(progress, self)
                .await;
        }
        async fn offer_next(
            &mut self,
            progress: &mut Option<
                SourceProgress<
                    ActionSettlementOf<HostedComputingActor>,
                    <HostedComputingActor as BehaviorSettlements>::SourceCustody,
                >,
            >,
        ) {
            <ActionSettlementOf<HostedComputingActor> as SourceSettlementCustody<
                Self,
                WorkEvent,
            >>::prepare_source(progress);
            if let Some(SourceProgress::Offering(custody)) = progress.as_mut() {
                <ActionSettlementOf<HostedComputingActor> as SourceSettlementCustody<
                    Self,
                    WorkEvent,
                >>::offer_next_to_source(custody, self)
                .await;
            }
            <ActionSettlementOf<HostedComputingActor> as SourceSettlementCustody<
                Self,
                WorkEvent,
            >>::finish_source(progress);
            if !matches!(progress, Some(SourceProgress::Completed(_))) {
                return;
            }
            if self.observations.len() == self.expected
                && let Some(completion) = self.completion.take()
            {
                let delivered = completion.send(());
                assert!(delivered.is_ok());
            }
        }
        async fn next_local_event(&mut self) -> Result<WorkEvent, JoinError> {
            pending().await
        }
        async fn receive_retirement(
            interpreter: &mut Option<Self>,
            received: &mut Option<CapabilityRetirement<WorkEvent, Self::Retired>>,
        ) {
            if received.is_some() {
                return;
            }
            let Some(original) = interpreter.take() else {
                return;
            };
            *received = Some(CapabilityRetirement::without_activations(
                original.observations,
            ));
        }
    }

    #[derive(Default)]
    struct TaskMeasurements {
        spawned: Vec<Id>,
        polls: HashMap<Id, (usize, usize)>,
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Keep measured region boundaries and whole joined originals visible together."
    )]
    fn measure_actor_work(allocation: WorkAllocation, requests: usize) -> (usize, usize) {
        let measurements = Arc::new(Mutex::new(TaskMeasurements {
            spawned: Vec::with_capacity(8),
            polls: HashMap::with_capacity(8),
        }));
        let spawned = Arc::clone(&measurements);
        let returned = Arc::clone(&measurements);
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .on_task_spawn(move |task| spawned.lock().unwrap().spawned.push(task.id()))
            .on_before_task_poll(|_| begin_allocations())
            .on_after_task_poll(move |task| {
                // Disable counting before host instrumentation allocates or locks.
                let count = finish_allocations().expect("finite actor-poll allocation count");
                let mut measurements = returned.lock().unwrap();
                let totals = measurements.polls.entry(task.id()).or_default();
                totals.0 = totals
                    .0
                    .checked_add(count)
                    .expect("finite measurement total");
                totals.1 += 1;
            })
            .build()
            .unwrap();
        let addresses = AddressSpace::new();
        let start = Arc::new(WorkBarrier::new(3));
        let mut actors = Vec::with_capacity(2);
        let mut completed_work = Vec::with_capacity(2);
        let construction = allocations_during(|| {
            for address in [MailAddr(1019), MailAddr(1021)] {
                let (completion, completed) = oneshot::channel();
                let first_work = Arc::clone(&start);
                completed_work.push(completed);
                let actor = runtime
                    .block_on(spawn_root_with(
                        addresses.clone(),
                        Config::new(requests + 1),
                        address,
                        ComputingActor {
                            initialization_count: 0,
                            commands: Vec::with_capacity(requests + 1),
                        }
                        .stop_on_shutdown(),
                        move |_, _, _, _| MeasuredWork {
                            start: Some(first_work),
                            completion: Some(completion),
                            expected: requests,
                            allocation,
                            observations: Vec::with_capacity(requests),
                        },
                    ))
                    .unwrap();
                actors.push(actor);
            }
        });
        let ids: Vec<_> = actors.iter().map(|actor| actor.task.task.id()).collect();
        assert_ne!(ids[0], ids[1]);
        let mut originals = Vec::with_capacity(requests * 2);
        let mut submissions = Vec::with_capacity(requests * 2);
        for actor in 0..2 {
            for ordinal in 0..requests {
                let values: Vec<_> = (0..1024).map(|value| value + ordinal as u64).collect();
                originals.push((
                    actor,
                    ordinal,
                    values.as_ptr() as usize,
                    values.iter().sum::<u64>(),
                ));
                submissions.push((actor, values));
            }
        }
        let started = Instant::now();
        runtime.block_on(async {
            for (actor, values) in submissions {
                let sent = actors[actor]
                    .actor
                    .send_from(MailAddr(1031), WorkCommand::Compute(values))
                    .await;
                assert!(sent.is_ok());
            }
            // Every original typed input is admitted before the shared start.
            start.wait().await;
            for completed in completed_work {
                let completed = completed.await;
                assert!(completed.is_ok());
            }
        });
        let elapsed = started.elapsed();
        let mut outcomes = Vec::with_capacity(2);
        let cleanup = allocations_during(|| {
            runtime.block_on(async {
                for actor in actors {
                    let sent = actor
                        .actor
                        .send_from(MailAddr(1033), WorkCommand::Finish)
                        .await;
                    assert!(sent.is_ok());
                    let (outcome, notification) = actor.task.finish().await;
                    notification.expect("the actual termination was published");
                    outcomes.push(outcome.expect("the joined actor returns its exact outcome"));
                }
            });
        });
        // Joining can wake before its after-poll hook; runtime drop completes it.
        drop(runtime);
        let mut work_allocations = 0;
        for (actor, outcome) in outcomes.into_iter().enumerate() {
            let ActorExecutionOutcome::Completed {
                behavior,
                residual:
                    LocalResidual::Retired {
                        interpretation,
                        source,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        terminal_report,
                        retirement_failures,
                        settlements,
                        ingress,
                        activation_tasks,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
                    },
                completion,
                additional_failures,
            } = outcome
            else {
                panic!("whole measured actor retirement")
            };
            assert!(interpretation.is_none());
            assert!(source.is_none());
            assert!(received_interpretation.is_none());
            assert!(received_source.is_none());
            assert!(source_index.is_none());
            assert!(acquired_ingress.is_none());
            assert!(terminal_report.is_none());
            assert!(retirement_failures.is_empty());
            assert!(additional_failures.is_empty());
            assert!(capability_failures.is_empty());
            assert!(unread_owner_cancellation.is_none());
            assert!(matches!(completion, Completion::Stopped));
            assert_eq!(behavior.base().initialization_count, 1);
            assert_eq!(behavior.base().commands.len(), requests + 1);
            assert_eq!(
                &behavior.base().commands[..requests],
                vec![MailAddr(1031); requests]
            );
            assert_eq!(behavior.base().commands[requests], MailAddr(1033));
            assert!(ingress.control.is_empty() && ingress.user.is_empty());
            assert!(activation_tasks.is_empty());
            assert_eq!(descendants.len(), requests);
            for &(before, after) in &descendants {
                let expected = match allocation {
                    WorkAllocation::Original => 0,
                    WorkAllocation::Additional => 1,
                };
                assert_eq!(
                    after - before,
                    expected,
                    "interpreted allocation was not attributed to its actual actor poll"
                );
                work_allocations += after - before;
            }
            assert_eq!(settlements.len(), requests + 1);
            let mut returned = Vec::with_capacity(requests);
            for settlement in settlements {
                assert!(settlement.creations.is_empty());
                assert_eq!(settlement.sends.owned, NoSends);
                match settlement.become_ {
                    Step::Continue => {
                        assert_eq!(settlement.sends.inner.len(), 1);
                        for work in settlement.sends.inner {
                            let SettledItem::Attempted(ItemSettlement::Accepted(work)) = work
                            else {
                                panic!("complete original work receipt")
                            };
                            returned.push(work);
                        }
                    }
                    Step::Stop(_) => assert!(settlement.sends.inner.is_empty()),
                    Step::Goto(never) => match never {},
                }
            }
            assert_eq!(returned.len(), requests);
            for (turn, work) in returned.into_iter().enumerate() {
                // Driver retains the newest settlement at the front.
                let ordinal = requests - 1 - turn;
                let (_, _, pointer, sum) = originals[actor * requests + ordinal];
                assert_eq!(work.values.as_ptr() as usize, pointer);
                assert_eq!(
                    work.values,
                    (0..1024)
                        .map(|value| value + ordinal as u64)
                        .collect::<Vec<_>>()
                );
                assert_eq!(work.sum, sum);
            }
        }
        let measurements = measurements.lock().unwrap();
        for id in &ids {
            assert!(measurements.spawned.contains(id));
        }
        let actor_allocations: usize = ids.iter().map(|id| measurements.polls[id].0).sum();
        let actor_polls: usize = ids.iter().map(|id| measurements.polls[id].1).sum();
        println!(
            "workers=2 actor_ids={ids:?} observed_spawned_ids={:?} requests={} elapsed={elapsed:?} requests_per_second={} controller_construction={construction} controller_joined_cleanup={cleanup} selected_actor_polls={actor_polls} selected_actor_poll_allocations={actor_allocations} interpreted_work_allocations={work_allocations}; timing includes typed delivery/shared async batch-start/custody completion; runtime construction/off-poll worker allocations and host reporting excluded; no whole-runtime total",
            measurements.spawned,
            requests * 2,
            f64::from(u32::try_from(requests * 2).expect("measured workload fits u32"))
                / elapsed.as_secs_f64()
        );
        (work_allocations, actor_allocations)
    }

    #[derive(Debug)]
    enum ParentWorkCommand {
        Compute { parent: Vec<u64>, child: Vec<u64> },
        FinishChild,
        Finish,
    }

    struct ExecutionChild {
        computed: Vec<(Vec<u64>, u64)>,
    }

    #[actor]
    impl ExecutionChild {
        fn receive(&mut self, command: WorkCommand) -> BehaviorActed<Self> {
            match command {
                WorkCommand::Compute(values) => {
                    self.computed.push(compute_execution_batch(values));
                    Ok(Actions::cont())
                }
                WorkCommand::Finish => Ok(Actions::stop()),
            }
        }
    }

    fn compute_execution_batch(values: Vec<u64>) -> (Vec<u64>, u64) {
        let sum = values.iter().sum();
        for _ in 0..256 {
            let repeated: u64 = black_box(&values).iter().sum();
            black_box(repeated);
        }
        (values, sum)
    }

    struct ExecutionParent {
        child: Option<ExecutionChild>,
        creation: Option<CreationId>,
        computed: Vec<(Vec<u64>, u64)>,
    }

    #[actor(
        sends = {
            computations: Vec<ChildDelivery<ExecutionChild, ChildHead>>,
        },
        births = RetirementBirths<StopOnShutdown<ExecutionChild>>,
        creation_settlements = retain_for_retirement,
    )]
    impl ExecutionParent {
        #[expect(
            clippy::unnecessary_wraps,
            reason = "the owning actor expansion requires the exact fallible BehaviorActed fold signature"
        )]
        fn init(&mut self) -> BehaviorActed<Self> {
            let mut creations = CreationSequence::new();
            let creation = creations.issue().expect("one actual worker birth");
            self.creation = Some(creation);
            let child = self.child.take().expect("one original worker definition");
            Ok(Actions::create(Creations::one(CreateChild::birth(
                creation,
                child.stop_on_shutdown(),
            ))))
        }

        fn receive(&mut self, command: ParentWorkCommand) -> BehaviorActed<Self> {
            let creation = self
                .creation
                .expect("the issued worker birth remains owned");
            match command {
                ParentWorkCommand::Compute { parent, child } => {
                    self.computed.push(compute_execution_batch(parent));
                    Ok(Actions::cont().send_computations(ChildDelivery::after(
                        creation,
                        WorkCommand::Compute(child),
                    )))
                }
                ParentWorkCommand::FinishChild => Ok(Actions::cont()
                    .send_computations(ChildDelivery::after(creation, WorkCommand::Finish))),
                ParentWorkCommand::Finish => Ok(Actions::stop()),
            }
        }
    }

    struct CompletedExecutionChild {
        origin: ChildOrigin<ExecutionParent, ChildHead>,
        terminal: ActorRetirement<StopOnShutdown<ExecutionChild>, Self, ()>,
    }

    #[expect(
        clippy::type_complexity,
        reason = "one existing sender retains coexisting actual projector ID and original typed origin; an alias would add no owner"
    )]
    static CHILD_EXECUTION_COMPLETION: Mutex<
        Option<oneshot::Sender<(Id, ChildOrigin<ExecutionParent, ChildHead>)>>,
    > = Mutex::new(None);

    impl
        ProjectTerminal<
            ChildOrigin<ExecutionParent, ChildHead>,
            ActorRetirement<StopOnShutdown<ExecutionChild>, Self, ()>,
        > for CompletedExecutionChild
    {
        fn project(
            origin: ChildOrigin<ExecutionParent, ChildHead>,
            terminal: ActorRetirement<StopOnShutdown<ExecutionChild>, Self, ()>,
        ) -> Self {
            let publication = CHILD_EXECUTION_COMPLETION
                .lock()
                .unwrap()
                .take()
                .expect("the runtime projector owns one original completion publication");
            let sent = publication.send((id(), origin));
            sent.expect("the caller retains its actual child completion receiver");
            Self { origin, terminal }
        }
    }

    #[test]
    #[ignore = "Explicit whole parent/child workload timing and scoped allocation measurement."]
    #[expect(
        clippy::too_many_lines,
        reason = "keep exact measured regions, native task provenance and complete joined parent/child custody visible together"
    )]
    fn measure_declared_parent_child_throughput_and_scoped_allocations() {
        let requests = 128;
        let measurements = Arc::new(Mutex::new(TaskMeasurements {
            spawned: Vec::with_capacity(8),
            polls: HashMap::with_capacity(8),
        }));
        let spawning_contexts = Arc::new(Mutex::new(Vec::with_capacity(8)));
        let spawned = Arc::clone(&measurements);
        let spawning = Arc::clone(&spawning_contexts);
        let returned = Arc::clone(&measurements);
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .on_task_spawn(move |task| {
                spawned.lock().unwrap().spawned.push(task.id());
                spawning.lock().unwrap().push((task.id(), try_id()));
            })
            .on_before_task_poll(|_| begin_allocations())
            .on_after_task_poll(move |task| {
                let count = finish_allocations().expect("finite task-poll allocation count");
                let mut measurements = returned.lock().unwrap();
                let totals = measurements.polls.entry(task.id()).or_default();
                totals.0 = totals
                    .0
                    .checked_add(count)
                    .expect("finite task allocation total");
                totals.1 = totals.1.checked_add(1).expect("finite task poll total");
            })
            .build()
            .unwrap();
        let mut originals = Vec::with_capacity(requests);
        let mut submissions = Vec::with_capacity(requests);
        for ordinal in 0..requests {
            let parent: Vec<_> = (0..1024).map(|value| value + ordinal as u64).collect();
            let child: Vec<_> = (0..1024).map(|value| value + ordinal as u64 + 17).collect();
            originals.push((
                parent.as_ptr() as usize,
                child.as_ptr() as usize,
                parent.iter().sum::<u64>(),
                child.iter().sum::<u64>(),
            ));
            submissions.push(ParentWorkCommand::Compute { parent, child });
        }
        let (publication, completed_child) = oneshot::channel();
        let prior = CHILD_EXECUTION_COMPLETION
            .lock()
            .unwrap()
            .replace(publication);
        drop(prior);
        let application = Application::new(
            ExecutionParent {
                child: Some(ExecutionChild {
                    computed: Vec::with_capacity(requests),
                }),
                creation: None,
                computed: Vec::with_capacity(requests),
            }
            .stop_on_shutdown(),
        );
        let mut formed = None;
        let formation = allocations_during(|| {
            let entered = runtime.enter();
            formed = Some(
                application
                    .execute_with::<_, _, CompletedExecutionChild, _, _, _>(
                        move |application| async move {
                            for command in submissions {
                                let sent =
                                    application.root().send_from(MailAddr(1031), command).await;
                                sent.expect("each original parent computation is admitted");
                            }
                            let sent = application
                                .root()
                                .send_from(MailAddr(1031), ParentWorkCommand::FinishChild)
                                .await;
                            sent.expect("the child finish follows all original computations");
                            let (projector, projected_origin) = completed_child
                                .await
                                .expect("actual eager child projection");
                            let sent = application
                                .root()
                                .send_from(MailAddr(1031), ParentWorkCommand::Finish)
                                .await;
                            sent.expect(
                                "parent finish is explicit after original child completion",
                            );
                            (
                                projector,
                                projected_origin,
                                application.lifecycle().termination().await,
                            )
                        },
                    )
                    .unwrap_or_else(|_| panic!("the explicit measured host is entered")),
            );
            drop(entered);
        });
        let (execution, result) = formed.expect("one original cold paired execution");
        let mut joined = None;
        let started = Instant::now();
        let controller = allocations_during(|| {
            joined = Some(runtime.block_on(async { tokio::join!(execution, result).1 }));
        });
        let elapsed = started.elapsed();
        drop(runtime);
        let stale = CHILD_EXECUTION_COMPLETION.lock().unwrap().take();
        assert!(stale.is_none());
        let application_outcome = joined.expect("whole original application result remains");
        if let ApplicationOutcome::Completed {
            output: _,
            cleanup: Ok((_, ActorRetirement::ActorTaskFailed(_))),
        } = &application_outcome
        {
            panic!("the measured application owns completed work and both native joins");
        }
        let ApplicationOutcome::Completed {
            output: (projector, projected_origin, termination),
            cleanup: Ok((origin, retirement)),
        } = application_outcome
        else {
            panic!("the measured application owns completed work and both native joins");
        };
        assert_eq!(termination, Ok(Exit::Normal));
        assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
        let ActorRetirement::Completed {
            behavior,
            interpretation,
            source,
            settlements,
            control,
            user,
            descendants,
            child_failures: (child_failures, ()),
            capability_failures,
            additional_failures,
            terminal_report,
            retirement_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            unread_owner_cancellation,
            completion,
        } = retirement
        else {
            panic!("the parent stopped after its complete workload");
        };
        assert!(interpretation.is_none() && source.is_none());
        assert!(received_interpretation.is_none() && received_source.is_none());
        assert!(source_index.is_none() && acquired_ingress.is_none());
        assert!(unread_owner_cancellation.is_none() && terminal_report.is_none());
        assert!(control.is_empty() && user.is_empty());
        assert!(additional_failures.is_empty() && capability_failures.is_empty());
        assert!(retirement_failures.is_empty() && child_failures.is_empty());
        assert!(matches!(completion, Completion::Stopped));
        let parent = behavior.base();
        assert!(parent.child.is_none());
        let creation = parent
            .creation
            .expect("the original issued worker creation survives");
        assert_eq!(parent.computed.len(), requests);
        for (ordinal, (values, sum)) in parent.computed.iter().enumerate() {
            assert_eq!(values.as_ptr() as usize, originals[ordinal].0);
            assert_eq!(*sum, originals[ordinal].2);
            assert_eq!(
                values,
                &(0..1024).map(|v| v + ordinal as u64).collect::<Vec<_>>()
            );
        }
        // ChildDelivery owns unit acceptance; selected ActionItem::retain_accepted
        // discharges it. Actual child vectors/sums below prove the full delivery trace.
        assert_eq!(settlements.len(), 2);
        for (turn, settlement) in settlements.into_iter().enumerate() {
            assert_eq!(settlement.sends.owned, NoSends);
            assert!(settlement.sends.inner.computations.is_empty());
            let CreationSettlement::Settled(creations) = settlement.creations.into_settlement()
            else {
                panic!("every parent creation lane retains its complete settlement");
            };
            let creations: Vec<_> = creations.into_iter().collect();
            if turn == 0 {
                assert!(matches!(settlement.become_, Step::Stop(_)));
                assert!(creations.is_empty());
            } else {
                assert_eq!(settlement.become_, Step::Continue);
                let [created] = creations
                    .try_into()
                    .unwrap_or_else(|_| panic!("one whole birth"));
                let SettledItem::Attempted(ItemSettlement::Accepted(
                    ChildCreationOutcome::Established(committed),
                )) = created
                else {
                    panic!("the original child actually committed");
                };
                assert_eq!(committed.id(), creation);
                assert_eq!(committed.kind(), CreationKind::Birth);
                drop(committed);
            }
        }
        drop(behavior);
        let [
            CompletedExecutionChild {
                origin: child_origin,
                terminal,
            },
        ] = descendants
            .try_into()
            .unwrap_or_else(|_| panic!("one whole projected child remains"));
        assert_eq!(child_origin, projected_origin);
        assert_ne!(child_origin.address(), origin.address());
        let ActorRetirement::Completed {
            behavior,
            interpretation,
            source,
            settlements,
            control,
            user,
            descendants,
            child_failures: (),
            capability_failures,
            additional_failures,
            terminal_report,
            retirement_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            unread_owner_cancellation,
            completion,
        } = terminal
        else {
            panic!("the actual child completed its original computation batch");
        };
        assert!(interpretation.is_none() && source.is_none());
        assert!(received_interpretation.is_none() && received_source.is_none());
        assert!(source_index.is_none() && acquired_ingress.is_none());
        assert!(unread_owner_cancellation.is_none() && terminal_report.is_none());
        assert!(control.is_empty() && user.is_empty() && descendants.is_empty());
        assert!(additional_failures.is_empty() && capability_failures.is_empty());
        assert!(retirement_failures.is_empty());
        assert!(matches!(completion, Completion::Stopped));
        assert_eq!(behavior.base().computed.len(), requests);
        for (ordinal, (values, sum)) in behavior.base().computed.iter().enumerate() {
            assert_eq!(values.as_ptr() as usize, originals[ordinal].1);
            assert_eq!(*sum, originals[ordinal].3);
            assert_eq!(
                values,
                &(0..1024)
                    .map(|v| v + ordinal as u64 + 17)
                    .collect::<Vec<_>>()
            );
        }
        let [stopped] = settlements
            .try_into()
            .unwrap_or_else(|_| panic!("one full child Stop row"));
        assert_eq!(stopped.sends.owned, NoSends);
        assert_eq!(stopped.sends.inner, NoSends);
        assert!(stopped.creations.is_empty());
        assert!(matches!(stopped.become_, Step::Stop(_)));
        let measurements = measurements.lock().unwrap();
        let contexts = spawning_contexts.lock().unwrap();
        assert_eq!(measurements.spawned.len(), 4);
        assert_eq!(contexts.len(), 4);
        let child_spawns: Vec<_> = contexts
            .iter()
            .filter_map(|(id, parent)| parent.map(|p| (*id, p)))
            .collect();
        assert_eq!(child_spawns.len(), 2);
        let root = child_spawns[0].1;
        assert_eq!(child_spawns[1].1, root);
        assert!(child_spawns.iter().any(|(id, _)| *id == projector));
        let child = child_spawns
            .iter()
            .find_map(|(id, _)| (*id != projector).then_some(*id))
            .expect("the original child actor is distinct from its projector");
        let root_join = contexts
            .iter()
            .find_map(|(id, parent)| (parent.is_none() && *id != root).then_some(*id))
            .expect("actual root join task");
        let roles = [
            ("parent", root),
            ("child", child),
            ("projection", projector),
            ("root_join", root_join),
        ];
        for (position, (_, id)) in roles.iter().enumerate() {
            assert_eq!(
                measurements
                    .spawned
                    .iter()
                    .filter(|native| *native == id)
                    .count(),
                1
            );
            assert!(roles[..position].iter().all(|(_, prior)| prior != id));
            assert!(measurements.polls.contains_key(id));
        }
        assert!(
            contexts
                .iter()
                .any(|(id, parent)| *id == root && parent.is_none())
        );
        let task_allocations: usize = roles.iter().map(|(_, id)| measurements.polls[id].0).sum();
        println!(
            "parent_child requests={} elapsed={elapsed:?} computations_per_second={} native_roles={roles:?} spawning_contexts={contexts:?} cold_pair_formation_allocations={formation} controller_execution_and_join_allocations={controller} all_four_task_poll_allocations={task_allocations} task_poll_totals={:?}; payload setup/runtime construction/off-poll worker allocations/host reporting excluded; no whole-runtime heap total; eager_child_tasks=2, deferred_one_task_equation_is_unexecuted",
            requests * 2,
            f64::from(u32::try_from(requests * 2).expect("finite measured workload"))
                / elapsed.as_secs_f64(),
            measurements.polls
        );
        drop((measurements, contexts, behavior, child_origin));
    }

    #[test]
    fn interpreted_allocations_belong_to_actual_actor_polls() {
        let original = measure_actor_work(WorkAllocation::Original, 4);
        let additional = measure_actor_work(WorkAllocation::Additional, 4);
        assert_eq!(original.0, 0);
        assert_eq!(additional.0, 8);
    }

    #[test]
    #[ignore = "Explicit scoped execution measurement, not an ordinary test or full EV30 benchmark."]
    fn measure_independent_actor_throughput_and_scoped_allocations() {
        let measurement = measure_actor_work(WorkAllocation::Original, 128);
        assert_eq!(measurement.0, 0);
    }
    #[test]
    fn task_poll_allocation_measurement_finishes_after_future_panic() {
        let observed = Arc::new(Mutex::new(Vec::with_capacity(4)));
        let returned = Arc::clone(&observed);
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .on_before_task_poll(|_| begin_allocations())
            .on_after_task_poll(move |task| {
                let count = finish_allocations();
                returned.lock().unwrap().push((task.id(), count));
            })
            .build()
            .unwrap();
        let task = runtime.spawn(async { panic!("measured task future panicked") });
        let id = task.id();
        let joined = runtime.block_on(task);
        drop(runtime);
        let failure = joined.expect_err("original task panic");
        assert!(failure.is_panic());
        assert_eq!(failure.id(), id);
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].0, id);
        assert!(observed[0].1.is_ok());
    }
}

#[cfg(test)]
mod root_join_custody {
    use crate::address::MailAddr;
    use crate::launch::{
        InertCapabilities, LocalRetirement, ObserveInertActions, OwnedTask, spawn_local_execution,
    };
    use crate::local::effects::ActionSettlementOf;
    use crate::local::effects::{CapabilityRetirement, CommitActions};
    use crate::local::environment::{LocalEnvironment, LocalResidual};
    use crate::local::execution::{ActivationTasks, LocalRetirementRequest, OwnerCancellation};
    use crate::local::ingress::StandardIngress;
    use crate::terminal::{LocalOutcome, RetirementNotificationError};
    use crate::termination::TerminationPublication;
    use crate::{ActorExecution, ActorExecutionOutcome, ActorSpace, observe};
    use behavior::{
        Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorSettlements, EventLayer, Here,
        InitializationTurn, InterpretationProgress, MessageProtocol, Never, NoBirths, NoSends,
        SourceProgress, SourceSettlementCustody, Step, Stopped, User,
    };
    use behavior_actors::ShutdownRequested;
    use bombay_engine::{ActionsOf, Completion, Driver};
    use communication::Config;
    use core::future::{Future, pending};
    use core::task::{Context, Poll, Waker};
    use std::any::Any;
    use std::panic::resume_unwind;
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Weak};
    use std::task::Wake;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum RootFinish {
        Stop,
        Cancel,
        Unpublished,
    }
    #[derive(Clone, Copy, Debug)]
    enum ResultCustody {
        Retain,
        Surrender,
    }
    impl ResultCustody {
        async fn finish_root_cleanup<T>(
            self,
            receipt: JoinHandle<T>,
            release: oneshot::Sender<()>,
            released: &Weak<Vec<u8>>,
            verification: oneshot::Receiver<()>,
        ) {
            match self {
                ResultCustody::Retain => {
                    let sent = release.send(());
                    sent.expect("root cleanup remains owned");
                    let result = receipt
                        .await
                        .expect("exact root receipt remains after wait drop");
                    assert_eq!(released.strong_count(), 1);
                    drop(result);
                }
                ResultCustody::Surrender => {
                    drop(receipt);
                    assert_eq!(released.strong_count(), 1);
                    let sent = release.send(());
                    sent.expect("receiverless cleanup remains owned");
                    while released.strong_count() != 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }
            verification.await.expect("root oracles verified");
        }
    }

    struct RootState {
        values: Arc<Vec<u8>>,
        finish: RootFinish,
    }
    impl Behavior for RootState {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = EventLayer<ShutdownRequested, User<MailAddr, Never>>;
        type Sends = NoSends;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;
        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            Ok(match self.finish {
                RootFinish::Unpublished => Actions::stop(),
                _ => Actions::cont(),
            })
        }
        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            match event {
                EventLayer::Owned(_) => Ok(Actions::stop()),
                EventLayer::Inner(user) => match user.message {},
            }
        }
    }
    struct RootCleanup {
        decisions: Arc<Mutex<Vec<Step<Never, Stopped>>>>,
        initializing: Option<(Option<oneshot::Sender<()>>, oneshot::Receiver<()>)>,
        retiring: Option<oneshot::Sender<()>>,
        release: oneshot::Receiver<()>,
        activation_tasks: ActivationTasks<<RootState as Behavior>::Event>,
    }
    impl CommitActions<RootState> for RootCleanup {
        type Retired = ();
        async fn commit(
            &mut self,
            progress: &mut Option<
                InterpretationProgress<
                    ActionsOf<RootState>,
                    <RootState as BehaviorSettlements>::InterpretationCustody,
                    ActionSettlementOf<RootState>,
                >,
            >,
        ) {
            if let Some((initializing, admitted)) = self.initializing.as_mut() {
                if let Some(initializing) = initializing.take() {
                    let sent = initializing.send(());
                    sent.expect("startup observer remains");
                }
                admitted
                    .await
                    .expect("initial action interpretation is explicitly admitted");
                drop(self.initializing.take());
            }
            if let Some(InterpretationProgress::Original(actions)) = progress.as_ref() {
                assert_eq!(actions.sends, NoSends);
                assert!(actions.creates.is_empty());
                self.decisions.lock().unwrap().push(actions.become_);
            }
            ActionsOf::<RootState>::interpret::<_, <RootState as Behavior>::Event, Here>(
                progress,
                &mut InertCapabilities,
            )
            .await;
        }
        async fn offer_next(
            &mut self,
            progress: &mut Option<
                SourceProgress<
                    ActionSettlementOf<RootState>,
                    <RootState as BehaviorSettlements>::SourceCustody,
                >,
            >,
        ) {
            <ActionSettlementOf<RootState> as SourceSettlementCustody<
                InertCapabilities,
                <RootState as Behavior>::Event,
            >>::prepare_source(progress);
            if let Some(SourceProgress::Offering(custody)) = progress {
                <ActionSettlementOf<RootState> as SourceSettlementCustody<
                    InertCapabilities,
                    <RootState as Behavior>::Event,
                >>::offer_next_to_source(custody, &mut InertCapabilities)
                .await;
            }
            <ActionSettlementOf<RootState> as SourceSettlementCustody<
                InertCapabilities,
                <RootState as Behavior>::Event,
            >>::finish_source(progress);
        }
        async fn next_local_event(
            &mut self,
        ) -> Result<<RootState as Behavior>::Event, tokio::task::JoinError> {
            pending().await
        }
        async fn receive_retirement(
            interpreter: &mut Option<Self>,
            received: &mut Option<CapabilityRetirement<<RootState as Behavior>::Event, ()>>,
        ) {
            if received.is_some() {
                return;
            }
            let Some(original) = interpreter.as_mut() else {
                return;
            };
            if let Some(retiring) = original.retiring.take() {
                let sent = retiring.send(());
                sent.expect("cleanup observer remains");
            }
            (&mut original.release)
                .await
                .expect("cleanup must explicitly finish");
            let original = interpreter
                .take()
                .expect("the owning retirement remains acquired");
            *received = Some(CapabilityRetirement {
                activation_tasks: original.activation_tasks,
                descendants: (),
                terminal_report: None,
                retirement_failures: Vec::new(),
            });
        }
    }
    fn assert_root_retirement(
        outcome: &LocalOutcome<RootState, ()>,
        finish: RootFinish,
        allocation: usize,
    ) {
        let ActorExecutionOutcome::Completed {
            behavior,
            residual,
            additional_failures,
            completion,
        } = outcome
        else {
            panic!("complete actor state must survive")
        };
        assert!(additional_failures.is_empty());
        assert_eq!(behavior.values.as_slice(), [31, 37]);
        assert_eq!(behavior.finish, finish);
        assert_eq!(behavior.values.as_ptr() as usize, allocation);
        match (finish, completion) {
            (RootFinish::Stop | RootFinish::Unpublished, Completion::Stopped)
            | (
                RootFinish::Cancel,
                Completion::RetirementRequested(LocalRetirementRequest::OwnerCancellation(
                    OwnerCancellation,
                )),
            ) => {}
            _ => panic!("the exact root retirement cause must survive"),
        }
        let LocalResidual::Retired {
            interpretation,
            source,
            settlements,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            terminal_report,
            retirement_failures,
            ingress,
            activation_tasks,
            descendants,
            capability_failures,
            unread_owner_cancellation,
        } = residual
        else {
            panic!("retirement keeps all ambient fields")
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        let expected = match finish {
            RootFinish::Cancel => 0,
            _ => 1,
        };
        assert_eq!(settlements.len(), expected);
        for (index, settlement) in settlements.iter().enumerate() {
            assert_eq!(settlement.sends, NoSends);
            assert!(settlement.creations.is_empty());
            match (finish, index, &settlement.become_) {
                (RootFinish::Unpublished | RootFinish::Stop, 0, Step::Stop(_)) => {}
                _ => panic!("exact fold sequence survives"),
            }
        }
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(ingress.user.len(), 0);
        assert!(activation_tasks.is_empty());
        assert_eq!(*descendants, ());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn root_join_receipt_preserves_exact_outcome_through_cleanup() {
        for finish in [
            RootFinish::Stop,
            RootFinish::Cancel,
            RootFinish::Unpublished,
        ] {
            for custody in [ResultCustody::Retain, ResultCustody::Surrender] {
                let addresses = ActorSpace::new();
                let bytes = vec![31, 37];
                let allocation = bytes.as_ptr() as usize;
                let values = Arc::new(bytes);
                let released_root = Arc::downgrade(&values);
                let decisions = Arc::new(Mutex::new(Vec::new()));
                let interpreted = decisions.clone();
                let (initializing, initialization) = oneshot::channel();
                let (admit, admitted) = oneshot::channel();
                let (retiring, retirement) = oneshot::channel();
                let (release, released) = oneshot::channel();
                let (authority, startup, control, task, termination_notification) =
                    spawn_local_execution::<RootState, _, StandardIngress, _, _, _>(
                        addresses.clone(),
                        Config::new(2),
                        MailAddr::APPLICATION_ROOT,
                        RootState { values, finish },
                        |_, _, _, _| RootCleanup {
                            decisions: interpreted,
                            initializing: Some((Some(initializing), admitted)),
                            retiring: Some(retiring),
                            release: released,
                            activation_tasks: ActivationTasks::new(),
                        },
                        |environment| {
                            let (publication, startup) = oneshot::channel();
                            let control = environment.shutdown_control();
                            let environment = environment.publish_with(move |actor| {
                                drop(publication.send(actor));
                            });
                            (environment, startup, control)
                        },
                    );
                let authority = match finish {
                    RootFinish::Cancel => {
                        drop(authority);
                        None
                    }
                    _ => Some(authority),
                };
                initialization.await.expect("initial actions admitted");
                assert!(addresses.resolve(&MailAddr::APPLICATION_ROOT).is_none());
                let sent = admit.send(());
                sent.expect("the original actor remains owned");
                let startup = match finish {
                    RootFinish::Cancel | RootFinish::Unpublished => Some(startup),
                    RootFinish::Stop => {
                        let actor = startup.await.expect("continuing root is published");
                        let control = control.upgrade().expect("the actor owns control");
                        let sent = control.send(EventLayer::Owned(ShutdownRequested));
                        sent.expect("the root accepts shutdown");
                        drop(actor);
                        None
                    }
                };
                retirement.await.expect("actual root cleanup begins");
                let expected_decisions = match finish {
                    RootFinish::Stop => vec![Step::Continue, Step::Stop(Stopped)],
                    RootFinish::Cancel => vec![Step::Continue],
                    RootFinish::Unpublished => vec![Step::Stop(Stopped)],
                };
                assert_eq!(*decisions.lock().unwrap(), expected_decisions);
                let (verified, verification) = oneshot::channel();
                let mut receipt = tokio::spawn(async move {
                    let outcome = task.await.expect("actor joined");
                    let notification = termination_notification
                        .await
                        .expect("the actual first producer transferred");
                    notification.expect("the first publication succeeded");
                    let startup_rejection = match startup {
                        Some(startup) => Some(startup.await.expect_err("root unpublished")),
                        None => None,
                    };
                    assert_root_retirement(&outcome, finish, allocation);
                    let notified = verified.send(());
                    notified.expect("the root oracle receiver remains");
                    (outcome, startup_rejection)
                });
                let mut waiting = Box::pin(&mut receipt);
                let polled = waiting
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()));
                assert!(matches!(polled, Poll::Pending));
                drop(waiting);
                assert_eq!(released_root.strong_count(), 1);
                custody
                    .finish_root_cleanup(receipt, release, &released_root, verification)
                    .await;
                assert_eq!(released_root.strong_count(), 0);
                assert!(addresses.resolve(&MailAddr::APPLICATION_ROOT).is_none());
                drop(authority);
            }
        }
    }
    enum NotificationWake {
        Return,
        Unwind(Box<dyn Any + Send>),
    }

    struct TerminationWaiter {
        publication: Mutex<Option<NotificationWake>>,
        attempts: AtomicUsize,
    }

    impl Wake for TerminationWaiter {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            let publication = self.publication.lock().unwrap().take();
            match publication {
                Some(NotificationWake::Return) | None => {}
                Some(NotificationWake::Unwind(original)) => resume_unwind(original),
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn termination_notification_preserves_original_native_retirement() {
        for notification in [
            NotificationWake::Return,
            NotificationWake::Unwind(Box::new(Arc::new(vec![43_u8, 47]))),
        ] {
            let addresses = ActorSpace::new();
            let values = Arc::new(vec![31, 37]);
            let allocation = values.as_ptr() as usize;
            let original_state = Arc::downgrade(&values);
            let decisions = Arc::new(Mutex::new(Vec::new()));
            let interpreted = decisions.clone();
            let (admit, admitted) = oneshot::channel();
            let (release, released) = oneshot::channel();
            let (authority, startup, control, task, termination_notification) =
                spawn_local_execution::<RootState, _, StandardIngress, _, _, _>(
                    addresses.clone(),
                    Config::new(2),
                    MailAddr::APPLICATION_ROOT,
                    RootState {
                        values,
                        finish: RootFinish::Stop,
                    },
                    |_, _, _, _| RootCleanup {
                        decisions: interpreted,
                        initializing: Some((None, admitted)),
                        retiring: None,
                        release: released,
                        activation_tasks: ActivationTasks::new(),
                    },
                    |environment| {
                        let (publication, startup) = oneshot::channel();
                        let control = environment.shutdown_control();
                        let environment = environment.publish_with(move |actor| {
                            drop(publication.send(actor));
                        });
                        (environment, startup, control)
                    },
                );
            let sent = admit.send(());
            sent.expect("the original startup remains owned");
            let actor = startup.await.expect("the root is active");
            let expected_panic = match &notification {
                NotificationWake::Return => None,
                NotificationWake::Unwind(payload) => {
                    let original = payload
                        .downcast_ref::<Arc<Vec<u8>>>()
                        .expect("the original observer payload");
                    Some((Arc::downgrade(original), ptr::from_ref(payload.as_ref())))
                }
            };
            let waiter = Arc::new(TerminationWaiter {
                publication: Mutex::new(Some(notification)),
                attempts: AtomicUsize::new(0),
            });
            let waker = Waker::from(waiter.clone());
            let mut termination = Box::pin(actor.termination());
            let registered = termination.as_mut().poll(&mut Context::from_waker(&waker));
            assert!(matches!(registered, Poll::Pending));
            let control = control
                .upgrade()
                .expect("the exact root retains its control");
            let accepted = control.send(EventLayer::Owned(ShutdownRequested));
            accepted.expect("the exact shutdown is accepted");
            let released = release.send(());
            released.expect("the owning retirement remains waiting");
            let joined = task.await;
            let notification = termination_notification
                .await
                .expect("the actual first producer transferred");
            let terminated = termination.await;
            assert_eq!(terminated, Ok(behavior_actors::Exit::Normal));
            assert_eq!(waiter.attempts.load(Ordering::SeqCst), 1);
            assert_eq!(
                original_state.strong_count(),
                1,
                "termination notification must conserve the original acquired native actor state"
            );
            let outcome = joined.expect("notification cannot erase the native joined result");
            assert_root_retirement(&outcome, RootFinish::Stop, allocation);
            assert_eq!(
                *decisions.lock().unwrap(),
                [Step::Continue, Step::Stop(Stopped)]
            );
            assert!(addresses.resolve(&MailAddr::APPLICATION_ROOT).is_none());
            match (expected_panic, notification) {
                (None, Ok(())) => {}
                (
                    Some((original, identity)),
                    Err(RetirementNotificationError::Panicked { payload }),
                ) => {
                    assert!(ptr::eq(ptr::from_ref(payload.as_ref()), identity));
                    assert_eq!(original.strong_count(), 1);
                    drop(payload);
                    assert_eq!(original.strong_count(), 0);
                }
                _ => {
                    panic!("the exact publication result must survive alongside the native result")
                }
            }
            let replayed = actor.termination().await;
            assert_eq!(replayed, terminated);
            assert_eq!(waiter.attempts.load(Ordering::SeqCst), 1);
            drop((outcome, actor, control, authority));
            assert_eq!(original_state.strong_count(), 0);
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn termination_notification_waits_for_owned_task_settlement_and_survives_wait_cancellation()
     {
        let values = Arc::new(vec![31, 37]);
        let allocation = values.as_ptr() as usize;
        let original_state = Arc::downgrade(&values);
        let original_panic = Arc::new(vec![53_u8, 59]);
        let retained_panic = Arc::downgrade(&original_panic);
        let payload: Box<dyn Any + Send> = Box::new(original_panic);
        let identity = ptr::from_ref(payload.as_ref());
        let (complete, completed) = oneshot::channel();
        let joined_work = Arc::new(AtomicUsize::new(0));
        let settled_work = joined_work.clone();
        let mut activation_tasks = ActivationTasks::new();
        activation_tasks.spawn(async move {
            completed.await.expect("the actual owned task remains held");
            settled_work.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        let decisions = Arc::new(Mutex::new(Vec::new()));
        let interpreted = decisions.clone();
        let (admit, admitted) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let (authority, startup, control, task, termination_notification) =
            spawn_local_execution::<RootState, _, StandardIngress, _, _, _>(
                ActorSpace::new(),
                Config::new(2),
                MailAddr::APPLICATION_ROOT,
                RootState {
                    values,
                    finish: RootFinish::Stop,
                },
                |_, _, _, _| RootCleanup {
                    decisions: interpreted,
                    initializing: Some((None, admitted)),
                    retiring: None,
                    release: released,
                    activation_tasks,
                },
                |environment| {
                    let (publication, startup) = oneshot::channel();
                    let control = environment.shutdown_control();
                    let environment = environment.publish_with(move |actor| {
                        drop(publication.send(actor));
                    });
                    (environment, startup, control)
                },
            );
        let admitted = admit.send(());
        admitted.expect("startup remains owned");
        let actor = startup.await.expect("the exact root is active");
        let waiter = Arc::new(TerminationWaiter {
            publication: Mutex::new(Some(NotificationWake::Unwind(payload))),
            attempts: AtomicUsize::new(0),
        });
        let waker = Waker::from(waiter.clone());
        let mut registered = Box::pin(actor.termination());
        let initial = registered.as_mut().poll(&mut Context::from_waker(&waker));
        assert!(matches!(initial, Poll::Pending));
        let control = control.upgrade().expect("the exact actor retains control");
        let sent = control.send(EventLayer::Owned(ShutdownRequested));
        sent.expect("the exact actor accepts shutdown");
        let released = release.send(());
        released.expect("capability retirement remains owned");
        let terminated = actor.termination().await;
        assert_eq!(terminated, Ok(behavior_actors::Exit::Normal));
        let mut owned = Some(OwnedTask {
            task,
            cancellation: authority,
            termination_notification,
        });
        let mut native = None;
        let mut notification = None;
        let mut waiting = Box::pin(OwnedTask::receive_finish(
            &mut owned,
            &mut native,
            &mut notification,
        ));
        let first_wait = waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        assert!(matches!(first_wait, Poll::Pending));
        drop(waiting);
        assert!(owned.is_some());
        assert!(native.is_none());
        assert!(notification.is_none());
        assert_eq!(joined_work.load(Ordering::SeqCst), 0);
        assert_eq!(original_state.strong_count(), 1);
        assert_eq!(retained_panic.strong_count(), 1);
        let released = complete.send(());
        released.expect("the actual owned task remains held after the wait is cancelled");
        OwnedTask::receive_finish(&mut owned, &mut native, &mut notification).await;
        assert!(owned.is_none());
        assert_eq!(joined_work.load(Ordering::SeqCst), 1);
        let outcome = native
            .take()
            .expect("the actual native join is acquired")
            .expect("the native actor state survives");
        assert_root_retirement(&outcome, RootFinish::Stop, allocation);
        assert_eq!(
            *decisions.lock().unwrap(),
            [Step::Continue, Step::Stop(Stopped)]
        );
        let Err(RetirementNotificationError::Panicked { payload }) = notification
            .as_ref()
            .expect("the actual first receipt is acquired")
        else {
            panic!("the original notification panic survives the join");
        };
        assert!(ptr::eq(ptr::from_ref(payload.as_ref()), identity));
        OwnedTask::receive_finish(&mut owned, &mut native, &mut notification).await;
        assert!(native.is_none());
        assert_eq!(waiter.attempts.load(Ordering::SeqCst), 1);
        drop(notification);
        assert_eq!(retained_panic.strong_count(), 0);
        drop((registered, outcome, actor, control));
        assert_eq!(original_state.strong_count(), 0);
    }

    enum GuardRetirement {
        Cancel,
        Unwind(Box<dyn Any + Send>),
    }

    #[tokio::test(flavor = "current_thread")]
    async fn termination_notification_survives_actual_terminal_guard_cancellation_and_unwind() {
        for guard in [
            GuardRetirement::Cancel,
            GuardRetirement::Unwind(Box::new(vec![61_u8, 67])),
        ] {
            let addresses = ActorSpace::new();
            let values = Arc::new(vec![31, 37]);
            let original_state = Arc::downgrade(&values);
            let panic = Arc::new(vec![71_u8, 73]);
            let original_panic = Arc::downgrade(&panic);
            let payload: Box<dyn Any + Send> = Box::new(panic);
            let identity = ptr::from_ref(payload.as_ref());
            let (publisher, termination) = observe::pair();
            let (report, selected) = oneshot::channel();
            drop(report);
            let (notification_publication, notification_receipt) = oneshot::channel();
            let (_authority, cancellation) = oneshot::channel();
            let environment = LocalEnvironment::<RootState, _, StandardIngress>::prepare(
                MailAddr::APPLICATION_ROOT,
                addresses.clone(),
                Config::new(2),
                termination,
                cancellation,
                |_, _, _| ObserveInertActions(|_: &ActionsOf<RootState>| {}),
            );
            let (publication, startup) = oneshot::channel();
            let environment = environment.publish_with(move |actor| {
                drop(publication.send(actor));
            });
            let retirement = LocalRetirement::<RootState>::new(
                TerminationPublication::new(publisher, selected),
                notification_publication,
            );
            let execution = ActorExecution::new(
                Driver::new(
                    RootState {
                        values,
                        finish: RootFinish::Stop,
                    },
                    environment,
                ),
                retirement,
            );
            let (release, released) = oneshot::channel();
            let expected_guard = match &guard {
                GuardRetirement::Cancel => (behavior_actors::Crash::Cancelled, None),
                GuardRetirement::Unwind(payload) => (
                    behavior_actors::Crash::Panicked,
                    Some(ptr::from_ref(payload.as_ref())),
                ),
            };
            let task = tokio::spawn(async move {
                let mut execution = Box::pin(execution.run());
                let active = execution
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()));
                assert!(matches!(active, Poll::Pending));
                released
                    .await
                    .expect("the outside guard gate remains owned");
                match guard {
                    GuardRetirement::Cancel => pending::<()>().await,
                    GuardRetirement::Unwind(payload) => resume_unwind(payload),
                }
                drop(execution);
            });
            let task_id = task.id();
            let actor = startup
                .await
                .expect("the actual actor is active before guard retirement");
            let waiter = Arc::new(TerminationWaiter {
                publication: Mutex::new(Some(NotificationWake::Unwind(payload))),
                attempts: AtomicUsize::new(0),
            });
            let waker = Waker::from(waiter.clone());
            let mut termination = Box::pin(actor.termination());
            let registered = termination.as_mut().poll(&mut Context::from_waker(&waker));
            assert!(matches!(registered, Poll::Pending));
            match expected_guard.1 {
                None => task.abort(),
                Some(_) => {
                    let released = release.send(());
                    released.expect("the outside unwind gate remains owned");
                }
            }
            let error = task
                .await
                .expect_err("the actual terminal guard preserves native cancellation or unwind");
            assert_eq!(error.id(), task_id);
            match expected_guard.1 {
                None => assert!(error.is_cancelled()),
                Some(identity) => {
                    assert!(error.is_panic());
                    let original = error.into_panic();
                    assert!(ptr::eq(ptr::from_ref(original.as_ref()), identity));
                    drop(original);
                }
            }
            let notification = notification_receipt
                .await
                .expect("the guard transfers the exact publication cause");
            let Err(RetirementNotificationError::Panicked { payload }) = notification else {
                panic!("the guard's acquired notification cause is retained independently");
            };
            assert!(ptr::eq(ptr::from_ref(payload.as_ref()), identity));
            assert_eq!(original_panic.strong_count(), 1);
            let terminated = termination.await;
            assert_eq!(terminated, Err(expected_guard.0));
            let replayed = actor.termination().await;
            assert_eq!(replayed, terminated);
            assert_eq!(waiter.attempts.load(Ordering::SeqCst), 1);
            assert_eq!(original_state.strong_count(), 0);
            assert!(addresses.resolve(&MailAddr::APPLICATION_ROOT).is_none());
            drop(payload);
            assert_eq!(original_panic.strong_count(), 0);
        }
    }
}
