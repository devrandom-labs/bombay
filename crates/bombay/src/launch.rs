//! One concrete task-launch boundary for local actors.

use core::fmt;
use std::sync::Weak;

use crate::address::MailAddr;
use crate::observation::TerminationObservations;
use crate::observe;
use crate::terminal::{ActorRetirement, ChildOrigin, LocalOutcome, ProjectTerminal};
use crate::time::LocalTimers;
use crate::{ActorExecutionOutcome, Retirement};
use behavior::{
    Behavior, BehaviorMessage, BehaviorSettlements, BirthMode, ClassifySettlement, Never, Protocol,
    User,
};
#[cfg(test)]
use behavior::{
    Here, InterpretSends, Interpretation, NoBirths, SourceCustody, SourceSettlementCustody,
};
use behavior_actors::ShutdownRequested;
use bombay_address::{AddressSpace, ClaimError};
use bombay_engine::Driver;
use bombay_engine::{ActionsOf, Completion};
use communication::Config;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};

#[cfg(test)]
use crate::interpret::{ActionSettlementOf, InterpretedActionSettlement};

use super::ActorExecution;
#[cfg(test)]
use super::local::CapabilityRetirement;
use super::local::{
    ActorRef, CommitActions, EntityIngress, IngressMode, LocalActivationRejection,
    LocalEnvironment, LocalResidual, LocalRetirementRequest, OwnerCancellation, StandardIngress,
};
use super::reports::LocalTerminalReports;
use super::termination::TerminationPublication;

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
        unread_owner_cancellation: Option<()>,
    },
    InitializationPanicked {
        behavior: B,
        control: Vec<B::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<B>>>,
        descendants: Descendants,
        capability_failures: Vec<JoinError>,
        unread_owner_cancellation: Option<()>,
    },
    HostRejected {
        behavior: B,
        initialization: ActionsOf<B>,
        error: ClaimError<MailAddr>,
        control: Vec<B::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<B>>>,
        descendants: Descendants,
        capability_failures: Vec<JoinError>,
        unread_owner_cancellation: Option<()>,
    },
    BindingAbandoned {
        behavior: B,
        initialization: ActionsOf<B>,
        control: Vec<B::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<B>>>,
        descendants: Descendants,
        capability_failures: Vec<JoinError>,
        unread_owner_cancellation: Option<()>,
    },
    Unpublished(LocalOutcome<B, Descendants>),
    Panicked,
    Cancelled,
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
            Self::Unpublished(_) => "Unpublished",
            Self::Panicked => "Panicked",
            Self::Cancelled => "Cancelled",
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
            Self::Unpublished(_) => formatter.write_str("the actor retired before publication"),
            Self::Panicked => formatter.write_str("actor initialization panicked"),
            Self::Cancelled => formatter.write_str("actor initialization was cancelled"),
        }
    }
}

impl<B, Descendants> std::error::Error for SpawnError<B, Descendants> where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>>
{
}

impl<B, Descendants> SpawnError<B, Descendants>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    fn from_local(outcome: LocalOutcome<B, Descendants>) -> Self {
        match outcome {
            ActorExecutionOutcome::BehaviorFailed {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        ingress,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
                        ..
                    },
                error,
            } => Self::InitializationRejected {
                behavior,
                error,
                control: ingress.control,
                user: ingress.user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            },
            ActorExecutionOutcome::InitializationPanicked {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        ingress,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
                        ..
                    },
            } => Self::InitializationPanicked {
                behavior,
                control: ingress.control,
                user: ingress.user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            },
            ActorExecutionOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
                        ..
                    },
                error: LocalActivationRejection::Address(error),
            } => Self::HostRejected {
                behavior,
                initialization,
                error,
                control: ingress.control,
                user: ingress.user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            },
            ActorExecutionOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
                        ..
                    },
                error: LocalActivationRejection::BindingAbandoned,
            } => Self::BindingAbandoned {
                behavior,
                initialization,
                control: ingress.control,
                user: ingress.user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            },
            ActorExecutionOutcome::Panicked => Self::Panicked,
            ActorExecutionOutcome::Cancelled => Self::Cancelled,
            unpublished => Self::Unpublished(unpublished),
        }
    }
}

impl<B, Root> SpawnError<B, Vec<Root>>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    pub(crate) fn into_retirement(self) -> ActorRetirement<B, Root> {
        match self {
            Self::AllocationRejected { behavior, reason } => {
                ActorRetirement::AllocationRejected { behavior, reason }
            }
            Self::InitializationRejected {
                behavior,
                error,
                control,
                user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            } => ActorRetirement::InitializationRejected {
                behavior,
                error,
                control,
                user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            },
            Self::InitializationPanicked {
                behavior,
                control,
                user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            } => ActorRetirement::InitializationPanicked {
                behavior,
                control,
                user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            },
            Self::HostRejected {
                behavior,
                initialization,
                error,
                control,
                user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            } => ActorRetirement::HostRejected {
                behavior,
                initialization,
                error,
                control,
                user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            },
            Self::BindingAbandoned {
                behavior,
                initialization,
                control,
                user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            } => ActorRetirement::BindingAbandoned {
                behavior,
                initialization,
                control,
                user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            },
            Self::Unpublished(outcome) => ActorRetirement::from_local(outcome),
            Self::Panicked => ActorRetirement::Panicked,
            Self::Cancelled => ActorRetirement::Cancelled,
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

    async fn retire<T>(mut self, task: JoinHandle<T>) -> Result<T, JoinError> {
        self.request();
        task.await
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
    task: tokio::task::JoinHandle<Root>,
    cancellation: OwnerCancellationAuthority,
    behavior: core::marker::PhantomData<fn() -> B>,
}

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
    pub(crate) async fn retire(self) -> LocalOutcome<B, Descendants> {
        let Self { task, cancellation } = self;
        owned_outcome(cancellation.retire(task).await)
    }

    pub(crate) async fn finish(self) -> LocalOutcome<B, Descendants> {
        let Self { task, cancellation } = self;
        owned_outcome(cancellation.finish(task).await)
    }
}

fn owned_outcome<B, Descendants>(
    joined: Result<LocalOutcome<B, Descendants>, JoinError>,
) -> LocalOutcome<B, Descendants>
where
    B: BehaviorSettlements,
    B::Event: Send + 'static,
{
    match joined {
        Ok(outcome) => outcome,
        Err(error) if error.is_panic() => ActorExecutionOutcome::Panicked,
        Err(_) => ActorExecutionOutcome::Cancelled,
    }
}

async fn settle_local_outcome<B, Descendants>(
    outcome: LocalOutcome<B, Descendants>,
) -> LocalOutcome<B, Descendants>
where
    B: BehaviorSettlements,
    B::Event: Send + 'static,
{
    match outcome {
        ActorExecutionOutcome::Completed {
            behavior,
            residual,
            completion,
        } => ActorExecutionOutcome::Completed {
            behavior,
            residual: residual.settle_activation_tasks().await,
            completion,
        },
        ActorExecutionOutcome::BehaviorFailed {
            behavior,
            residual,
            error,
        } => ActorExecutionOutcome::BehaviorFailed {
            behavior,
            residual: residual.settle_activation_tasks().await,
            error,
        },
        ActorExecutionOutcome::InitializationPanicked { behavior, residual } => {
            ActorExecutionOutcome::InitializationPanicked {
                behavior,
                residual: residual.settle_activation_tasks().await,
            }
        }
        ActorExecutionOutcome::ActivationFailed {
            behavior,
            residual,
            error,
        } => ActorExecutionOutcome::ActivationFailed {
            behavior,
            residual: residual.settle_activation_tasks().await,
            error,
        },
        ActorExecutionOutcome::SettlementFailed {
            behavior,
            residual,
            error,
        } => ActorExecutionOutcome::SettlementFailed {
            behavior,
            residual: residual.settle_activation_tasks().await,
            error,
        },
        ActorExecutionOutcome::Panicked => ActorExecutionOutcome::Panicked,
        ActorExecutionOutcome::Cancelled => ActorExecutionOutcome::Cancelled,
    }
}

async fn startup_failure<B, Descendants>(
    task: JoinHandle<LocalOutcome<B, Descendants>>,
    cancellation: OwnerCancellationAuthority,
) -> SpawnError<B, Descendants>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    B::Event: Send + 'static,
{
    SpawnError::from_local(owned_outcome(cancellation.finish(task).await))
}

impl<B, Root> ProjectedTask<B, Root>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    B::Event: Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    <B::Birth as BirthMode>::Child: Send,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    crate::interpret::ActionSettlementOf<B>: Send + 'static,
    Root: Send + 'static,
{
    pub(crate) fn project<Owner, Role>(
        task: OwnedTask<B, Vec<Root>>,
        origin: ChildOrigin<Owner, Role>,
    ) -> Self
    where
        Owner: 'static,
        Role: 'static,
        Root: ProjectTerminal<ChildOrigin<Owner, Role>, ActorRetirement<B, Root>>,
    {
        let OwnedTask {
            task: actor_task,
            cancellation,
        } = task;
        let task = tokio::spawn(async move {
            Root::project(
                origin,
                ActorRetirement::from_local(owned_outcome(actor_task.await)),
            )
        });
        Self {
            task,
            cancellation,
            behavior: core::marker::PhantomData,
        }
    }
}

impl<B, Root> ProjectedTask<B, Root>
where
    B: Behavior,
{
    pub(crate) async fn retire(self) -> Root {
        let Self {
            task,
            cancellation,
            behavior: _,
        } = self;
        cancellation
            .retire(task)
            .await
            .unwrap_or_else(|error| panic!("typed terminal projection task failed: {error}"))
    }

    #[cfg(test)]
    pub(crate) async fn finish(self) -> Root {
        let Self {
            task,
            cancellation,
            behavior: _,
        } = self;
        cancellation
            .finish(task)
            .await
            .unwrap_or_else(|error| panic!("typed terminal projection task failed: {error}"))
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
    <B::Birth as BirthMode>::Child: Send + 'static,
    I: CommitActions<B> + Send + 'static,
    crate::interpret::ActionSettlementOf<B>: ClassifySettlement + Send,
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
    <B::Birth as BirthMode>::Child: Send + 'static,
    I: CommitActions<B> + Send + 'static,
    crate::interpret::ActionSettlementOf<B>: ClassifySettlement + Send,
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
    <B::Birth as BirthMode>::Child: Send + 'static,
    I: CommitActions<B> + Send + 'static,
    crate::interpret::ActionSettlementOf<B>: ClassifySettlement + Send,
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
    <B::Birth as BirthMode>::Child: Send + 'static,
    I: CommitActions<B> + Send + 'static,
    crate::interpret::ActionSettlementOf<B>: ClassifySettlement + Send,
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
    <B::Birth as BirthMode>::Child: Send + 'static,
    I: CommitActions<B> + Send + 'static,
    crate::interpret::ActionSettlementOf<B>: ClassifySettlement + Send,
    I::Retired: Send + 'static,
    Start: Send,
    Control: Send,
{
    let (cancellation, startup, control, task) = spawn_local_execution(
        addresses,
        config,
        address,
        behavior,
        make_interpreter,
        publish,
    );

    if let Ok(started) = startup.await {
        Ok((started, control, OwnedTask { task, cancellation }))
    } else {
        Err(startup_failure(task, cancellation).await)
    }
}

/// Transfer the original startup authority, receipt, control, and actor join before awaiting activation.
#[expect(
    clippy::type_complexity,
    reason = "four independently owned actor startup values"
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
)
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    M: IngressMode<B, Retired = User<MailAddr, BehaviorMessage<B>>> + 'static,
    P: FnOnce(ActorRef<B::Protocol>) + Send + 'static,
    B::Event: Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    <B::Birth as BirthMode>::Child: Send + 'static,
    I: CommitActions<B> + Send + 'static,
    crate::interpret::ActionSettlementOf<B>: ClassifySettlement + Send,
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
    let retirement = LocalRetirement::<B>::new(TerminationPublication::new(
        termination_publisher,
        selected_report,
    ));
    let actor_execution = ActorExecution::new(Driver::new(behavior, environment), retirement);
    let task = tokio::spawn(async move {
        let outcome = actor_execution.run().await;
        settle_local_outcome(outcome).await
    });
    (cancellation, startup, control, task)
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
    B: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never, Birth = NoBirths> + Send + 'static,
    B::Event: behavior::InjectEvent<ShutdownRequested, behavior::Here> + Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    <B::Birth as BirthMode>::Child: Send + 'static,
    B::Sends: InterpretSends<InertCapabilities, B::Event, Here>,
    ActionSettlementOf<B>: SourceSettlementCustody<InertCapabilities, B::Event> + Send,
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
    B: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never, Birth = NoBirths> + Send + 'static,
    B::Event: behavior::InjectEvent<ShutdownRequested, behavior::Here> + Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    <B::Birth as BirthMode>::Child: Send + 'static,
    B::Sends: InterpretSends<InertCapabilities, B::Event, Here>,
    ActionSettlementOf<B>: SourceSettlementCustody<InertCapabilities, B::Event> + Send,
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
    behavior: core::marker::PhantomData<fn() -> B>,
}

impl<B> LocalRetirement<B>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn new(termination: TerminationPublication<MailAddr>) -> Self {
        Self {
            termination,
            behavior: core::marker::PhantomData,
        }
    }
}

impl<B, Descendants>
    Retirement<
        B,
        LocalResidual<
            ActionsOf<B>,
            crate::interpret::ActionSettlementOf<B>,
            B::Event,
            User<MailAddr, BehaviorMessage<B>>,
            Descendants,
        >,
        B::Error,
        LocalActivationRejection<MailAddr>,
        LocalRetirementRequest,
    > for LocalRetirement<B>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    type Output = LocalOutcome<B, Descendants>;

    fn retire(self, outcome: LocalOutcome<B, Descendants>) -> Self::Output {
        let observation: ActorExecutionOutcome<_, _, _, _> = match &outcome {
            ActorExecutionOutcome::Completed {
                completion:
                    Completion::RetirementRequested(LocalRetirementRequest::OwnerCancellation(
                        OwnerCancellation,
                    )),
                ..
            } => {
                self.termination.publish_owner_cancellation();
                return outcome;
            }
            ActorExecutionOutcome::Completed {
                completion:
                    Completion::RetirementRequested(LocalRetirementRequest::CapabilityFailed(_)),
                ..
            } => {
                self.termination.publish_capability_failure();
                return outcome;
            }
            ActorExecutionOutcome::Completed {
                behavior,
                residual,
                completion: Completion::Stopped,
            } => ActorExecutionOutcome::Completed {
                behavior,
                residual,
                completion: Completion::Stopped,
            },
            ActorExecutionOutcome::Completed {
                behavior,
                residual,
                completion: Completion::Exhausted,
            } => ActorExecutionOutcome::Completed {
                behavior,
                residual,
                completion: Completion::Exhausted,
            },
            ActorExecutionOutcome::BehaviorFailed {
                behavior,
                residual,
                error,
            } => ActorExecutionOutcome::BehaviorFailed {
                behavior,
                residual,
                error,
            },
            ActorExecutionOutcome::InitializationPanicked { behavior, residual } => {
                ActorExecutionOutcome::InitializationPanicked { behavior, residual }
            }
            ActorExecutionOutcome::ActivationFailed {
                behavior,
                residual,
                error,
            } => ActorExecutionOutcome::ActivationFailed {
                behavior,
                residual,
                error,
            },
            ActorExecutionOutcome::SettlementFailed {
                behavior,
                residual,
                error,
            } => ActorExecutionOutcome::SettlementFailed {
                behavior,
                residual,
                error: *error,
            },
            ActorExecutionOutcome::Panicked => ActorExecutionOutcome::Panicked,
            ActorExecutionOutcome::Cancelled => ActorExecutionOutcome::Cancelled,
        };
        self.termination.publish(&observation);
        outcome
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
            Settlements = InterpretedActionSettlement<B>,
        >,
    B::Sends: InterpretSends<InertCapabilities, B::Event, Here> + Send,
    ActionSettlementOf<B>: SourceSettlementCustody<InertCapabilities, B::Event> + Send,
    F: FnMut(&ActionsOf<B>) + Send,
{
    type Retired = ();

    async fn commit(&mut self, actions: ActionsOf<B>) -> Interpretation<ActionSettlementOf<B>> {
        (self.0)(&actions);
        actions
            .interpret::<_, B::Event, Here>(&mut InertCapabilities)
            .await
    }

    async fn offer_next(
        &mut self,
        settlement: ActionSettlementOf<B>,
    ) -> SourceCustody<ActionSettlementOf<B>> {
        settlement
            .offer_next_to_source(&mut InertCapabilities)
            .await
    }

    async fn next_local_event(&mut self) -> Result<B::Event, JoinError> {
        core::future::pending().await
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn retire(self) -> CapabilityRetirement<B::Event, Self::Retired> {
        CapabilityRetirement::without_activations(())
    }
}

#[cfg(test)]
impl<B, F, R> CommitActions<B> for ObserveActionsWithRetirement<F, R>
where
    B: BehaviorSettlements<
            Protocol: Protocol<Addr = MailAddr>,
            Ph = Never,
            Birth = NoBirths,
            Settlements = InterpretedActionSettlement<B>,
        >,
    B::Sends: InterpretSends<InertCapabilities, B::Event, Here> + Send,
    ActionSettlementOf<B>: SourceSettlementCustody<InertCapabilities, B::Event> + Send,
    F: FnMut(&ActionsOf<B>) + Send,
    R: Send,
{
    type Retired = R;

    async fn commit(&mut self, actions: ActionsOf<B>) -> Interpretation<ActionSettlementOf<B>> {
        (self.observe)(&actions);
        actions
            .interpret::<_, B::Event, Here>(&mut InertCapabilities)
            .await
    }

    async fn offer_next(
        &mut self,
        settlement: ActionSettlementOf<B>,
    ) -> SourceCustody<ActionSettlementOf<B>> {
        settlement
            .offer_next_to_source(&mut InertCapabilities)
            .await
    }

    async fn next_local_event(&mut self) -> Result<B::Event, JoinError> {
        core::future::pending().await
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn retire(self) -> CapabilityRetirement<B::Event, Self::Retired> {
        CapabilityRetirement::without_activations(self.retirement)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future::pending;

    use behavior::{
        Actions, BehaviorActed, CreationSequence, EventLayer, InitializationTurn, MessageProtocol,
        Never, NoBirths, NoSends, SettlementStatus, User,
    };
    use behavior_actors::{Crash, ShutdownRequested};
    use bombay_engine::Completion;
    use communication::Config;

    use super::*;
    use crate::MailAddr;

    struct RootProbe {
        terminal_marker: u8,
    }

    struct ProbeChildRole;

    #[derive(crate::TerminalProjection)]
    enum ProbeTerminal {
        #[application_actor]
        Probe {
            origin: ChildOrigin<RootProbe, ProbeChildRole>,
            terminal: ActorRetirement<RootProbe, Self>,
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
        let failure = SpawnError::<RootProbe>::Panicked;
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
    async fn owned_outcome_classifies_task_panic_and_cancellation_separately() {
        let panicking = tokio::spawn(async { panic!("the actor task panicked") });
        let panic_error = panicking.await.expect_err("the task panic is retained");
        let panicked = owned_outcome::<RootProbe, ()>(Err(panic_error));
        assert!(matches!(panicked, ActorExecutionOutcome::Panicked));

        let pending_task = tokio::spawn(async { pending::<()>().await });
        pending_task.abort();
        let cancellation = pending_task
            .await
            .expect_err("the task cancellation is retained");
        let cancelled = owned_outcome::<RootProbe, ()>(Err(cancellation));
        assert!(matches!(cancelled, ActorExecutionOutcome::Cancelled));
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
            Err(SpawnError::Unpublished(outcome)) => outcome,
            Err(error) => panic!("the committed root was misclassified: {error:?}"),
            Ok(_) => panic!("a stopping root was publicly published"),
        };
        let ActorExecutionOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    capability_failures,
                    unread_owner_cancellation,
                    settlements,
                    ingress,
                    activation_tasks,
                    descendants,
                },
            completion,
        } = outcome
        else {
            panic!("the root task lost its complete terminal outcome")
        };
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
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(address, MailAddr::APPLICATION_ROOT);
        assert_eq!(behavior.terminal_marker, 19);
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
        let ActorExecutionOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    capability_failures,
                    unread_owner_cancellation,
                    settlements,
                    ingress,
                    activation_tasks,
                    descendants,
                },
            completion,
        } = outcome
        else {
            panic!("the child task lost its complete terminal outcome")
        };
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
        let ActorExecutionOutcome::ActivationFailed {
            behavior,
            residual:
                LocalResidual::Uncommitted {
                    capability_failures,
                    unread_owner_cancellation,
                    initialization,
                    ingress,
                    activation_tasks,
                    descendants,
                },
            error: LocalActivationRejection::BindingAbandoned,
        } = outcome
        else {
            panic!("abandonment must return the exact uncommitted child and actions")
        };
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());

        assert_eq!(behavior.terminal_marker, 19);
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
                retirement: vec![descendant],
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
        let rejected = closed_parent_admission(terminal)
            .expect_err("closed parent admission returns the exact terminal value");
        let ProbeTerminal::Probe { origin, terminal } = rejected;
        let ActorRetirement::Completed {
            capability_failures,
            unread_owner_cancellation,
            behavior,
            settlements,
            control,
            user,
            descendants,
            completion,
        } = terminal
        else {
            panic!("the projected child must preserve its completed disposition")
        };
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
    use super::{ActorSpace, ObserveInertActions};
    use crate::MailAddr;
    use crate::local::{
        LocalEnvironment, LocalResidual, LocalRetirementRequest, OwnerCancellation, StandardIngress,
    };
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
            .await;
        let LocalResidual::Retired {
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
    use super::{ActorExecution, ActorExecutionOutcome, LocalRetirement, TerminationPublication};
    use crate::MailAddr;
    use crate::address::ApplicationAddresses;
    use crate::application_runtime::{ApplicationCapabilities, ApplicationCapabilityInputs};
    use crate::child_bindings::NoChildBindings;
    use crate::interpret::{ActionInterpreter, ActionSettlementOf};
    use crate::launch::ActorSpace;
    use crate::local::{
        CapabilityRetirement, CommitActions, LocalEnvironment, LocalResidual,
        LocalRetirementRequest, OwnerCancellation, StandardIngress,
    };
    use crate::observe;
    use crate::reports::LocalTerminalReports;
    use crate::terminal::{ActorRetirement, LocalOutcome};
    use behavior::{
        ActionItemResult, Actions, ActiveTurn, Behavior, BehaviorActed, ClassifySettlement,
        EventIngress, Here, InitializationTurn, InjectEvent, ItemSettlement, MessageProtocol,
        Never, NoBirths, Own, SourceActions, SourceCustody, Step, User, UserEvent,
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
        interpreter: SourceCycleInterpreter,
        source_offers: usize,
        entered: Option<oneshot::Sender<()>>,
        release_first: Option<oneshot::Receiver<()>>,
        progressed: Option<oneshot::Sender<()>>,
        release_progress: Option<oneshot::Receiver<()>>,
    }
    impl CommitActions<SourceCycleActor> for YieldingSourceInterpreter {
        type Retired = Vec<Never>;
        async fn commit(
            &mut self,
            actions: ActionsOf<SourceCycleActor>,
        ) -> behavior::Interpretation<ActionSettlementOf<SourceCycleActor>> {
            <SourceCycleInterpreter as CommitActions<SourceCycleActor>>::commit(
                &mut self.interpreter,
                actions,
            )
            .await
        }
        async fn offer_next(
            &mut self,
            settlement: ActionSettlementOf<SourceCycleActor>,
        ) -> SourceCustody<ActionSettlementOf<SourceCycleActor>> {
            let custody = <SourceCycleInterpreter as CommitActions<SourceCycleActor>>::offer_next(
                &mut self.interpreter,
                settlement,
            )
            .await;
            if matches!(custody, SourceCustody::Admitted(_)) {
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
            custody
        }
        async fn next_local_event(&mut self) -> Result<SourceCycleEvent, JoinError> {
            <SourceCycleInterpreter as CommitActions<SourceCycleActor>>::next_local_event(
                &mut self.interpreter,
            )
            .await
        }
        fn next_deadline(&mut self) -> Option<Instant> {
            <SourceCycleInterpreter as CommitActions<SourceCycleActor>>::next_deadline(
                &mut self.interpreter,
            )
        }
        fn pop_due(&mut self, now: Instant) -> Option<SourceCycleEvent> {
            <SourceCycleInterpreter as CommitActions<SourceCycleActor>>::pop_due(
                &mut self.interpreter,
                now,
            )
        }
        async fn retire(self) -> CapabilityRetirement<SourceCycleEvent, Self::Retired> {
            <SourceCycleInterpreter as CommitActions<SourceCycleActor>>::retire(self.interpreter)
                .await
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
                    interpreter: ActionInterpreter::new(capabilities),
                    source_offers: 0,
                    entered: Some(entered),
                    release_first: Some(first_release),
                    progressed: Some(progressed),
                    release_progress: Some(progress_release),
                }
            },
        );
        let control = environment.control();
        let retirement = LocalRetirement::<SourceCycleActor>::new(TerminationPublication::new(
            publisher,
            selected_report,
        ));
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
        verify_source_retirement_custody(retirement, &observer, address);

        drop(control);
    }
    fn verify_source_retirement_custody(
        retirement: LocalOutcome<SourceCycleActor, Vec<Never>>,
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
            capability_failures,
            unread_owner_cancellation,
            behavior,
            settlements,
            control: admitted,
            user,
            descendants,
        } = ActorRetirement::<SourceCycleActor, Never>::from_local(retirement)
        else {
            panic!("the public exact projection preserves the same selected owner fact");
        };
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
