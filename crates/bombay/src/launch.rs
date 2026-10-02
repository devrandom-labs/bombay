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
use bombay_engine::ActionsOf;
use bombay_engine::Driver;
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
    LocalEnvironment, LocalResidual, OwnerCancellation, StandardIngress,
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
    },
    InitializationPanicked {
        behavior: B,
        control: Vec<B::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<B>>>,
        descendants: Descendants,
    },
    HostRejected {
        behavior: B,
        initialization: ActionsOf<B>,
        error: ClaimError<MailAddr>,
        control: Vec<B::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<B>>>,
        descendants: Descendants,
    },
    BindingAbandoned {
        behavior: B,
        initialization: ActionsOf<B>,
        control: Vec<B::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<B>>>,
        descendants: Descendants,
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
                        ..
                    },
                error,
            } => Self::InitializationRejected {
                behavior,
                error,
                control: ingress.control,
                user: ingress.user,
                descendants,
            },
            ActorExecutionOutcome::InitializationPanicked {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        ingress,
                        descendants,
                        ..
                    },
            } => Self::InitializationPanicked {
                behavior,
                control: ingress.control,
                user: ingress.user,
                descendants,
            },
            ActorExecutionOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        descendants,
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
            },
            ActorExecutionOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        descendants,
                        ..
                    },
                error: LocalActivationRejection::BindingAbandoned,
            } => Self::BindingAbandoned {
                behavior,
                initialization,
                control: ingress.control,
                user: ingress.user,
                descendants,
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
            } => ActorRetirement::InitializationRejected {
                behavior,
                error,
                control,
                user,
                descendants,
            },
            Self::InitializationPanicked {
                behavior,
                control,
                user,
                descendants,
            } => ActorRetirement::InitializationPanicked {
                behavior,
                control,
                user,
                descendants,
            },
            Self::HostRejected {
                behavior,
                initialization,
                error,
                control,
                user,
                descendants,
            } => ActorRetirement::HostRejected {
                behavior,
                initialization,
                error,
                control,
                user,
                descendants,
            },
            Self::BindingAbandoned {
                behavior,
                initialization,
                control,
                user,
                descendants,
            } => ActorRetirement::BindingAbandoned {
                behavior,
                initialization,
                control,
                user,
                descendants,
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
    task: JoinHandle<Result<LocalOutcome<B, Descendants>, JoinError>>,
    cancellation: OwnerCancellationAuthority,
}

/// Requests actor cleanup if its startup or join waiter disappears.
struct OwnerCancellationAuthority {
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
    joined: Result<Result<LocalOutcome<B, Descendants>, JoinError>, JoinError>,
) -> LocalOutcome<B, Descendants>
where
    B: BehaviorSettlements,
    B::Event: Send + 'static,
{
    match joined {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(error)) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Ok(Err(error)) => panic!("activation task was cancelled unexpectedly: {error}"),
        Err(error) if error.is_panic() => ActorExecutionOutcome::Panicked,
        Err(_) => ActorExecutionOutcome::Cancelled,
    }
}

async fn settle_local_outcome<B, Descendants>(
    outcome: LocalOutcome<B, Descendants>,
) -> Result<LocalOutcome<B, Descendants>, JoinError>
where
    B: BehaviorSettlements,
    B::Event: Send + 'static,
{
    Ok(match outcome {
        ActorExecutionOutcome::Completed {
            behavior,
            residual,
            completion,
        } => ActorExecutionOutcome::Completed {
            behavior,
            residual: residual.settle_activation_tasks().await?,
            completion,
        },
        ActorExecutionOutcome::BehaviorFailed {
            behavior,
            residual,
            error,
        } => ActorExecutionOutcome::BehaviorFailed {
            behavior,
            residual: residual.settle_activation_tasks().await?,
            error,
        },
        ActorExecutionOutcome::InitializationPanicked { behavior, residual } => {
            ActorExecutionOutcome::InitializationPanicked {
                behavior,
                residual: residual.settle_activation_tasks().await?,
            }
        }
        ActorExecutionOutcome::ActivationFailed {
            behavior,
            residual,
            error,
        } => ActorExecutionOutcome::ActivationFailed {
            behavior,
            residual: residual.settle_activation_tasks().await?,
            error,
        },
        ActorExecutionOutcome::SettlementFailed {
            behavior,
            residual,
            error,
        } => ActorExecutionOutcome::SettlementFailed {
            behavior,
            residual: residual.settle_activation_tasks().await?,
            error,
        },
        ActorExecutionOutcome::Panicked => ActorExecutionOutcome::Panicked,
        ActorExecutionOutcome::Cancelled => ActorExecutionOutcome::Cancelled,
    })
}

async fn startup_failure<B, Descendants>(
    task: JoinHandle<Result<LocalOutcome<B, Descendants>, JoinError>>,
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

    if let Ok(started) = startup.await {
        Ok((started, control, OwnedTask { task, cancellation }))
    } else {
        Err(startup_failure(task, cancellation).await)
    }
}

/// Launch one local actor in a focused lower-layer test.
///
/// `commit` receives each complete action value exactly once, including
/// initialization.
#[cfg(test)]
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
    > for LocalRetirement<B>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    type Output = LocalOutcome<B, Descendants>;

    fn retire(self, outcome: LocalOutcome<B, Descendants>) -> Self::Output {
        match &outcome {
            ActorExecutionOutcome::Completed {
                residual: LocalResidual::OwnerCancelled { .. },
                ..
            } => self.termination.publish_owner_cancellation(),
            _ => self.termination.publish(&outcome),
        }
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

    async fn next_local_event(&mut self) -> B::Event {
        core::future::pending().await
    }

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

    async fn next_local_event(&mut self) -> B::Event {
        core::future::pending().await
    }

    async fn retire(self) -> CapabilityRetirement<B::Event, Self::Retired> {
        CapabilityRetirement::without_activations(self.retirement)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future::pending;
    use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};

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

    struct ActivationPanicMarker;

    #[tokio::test]
    async fn owned_outcome_preserves_activation_panic_and_rejects_cancellation() {
        let panicking = tokio::spawn(async { panic_any(ActivationPanicMarker) });
        let panic_error = panicking
            .await
            .expect_err("the activation panic is retained");
        let Err(replayed) = catch_unwind(AssertUnwindSafe(|| {
            owned_outcome::<RootProbe, ()>(Ok(Err(panic_error)))
        })) else {
            panic!("the exact activation panic must resume");
        };
        assert!(replayed.is::<ActivationPanicMarker>());

        let pending_task = tokio::spawn(async { pending::<()>().await });
        pending_task.abort();
        let cancellation = pending_task
            .await
            .expect_err("the activation cancellation is retained");
        let Err(rejected) = catch_unwind(AssertUnwindSafe(|| {
            owned_outcome::<RootProbe, ()>(Ok(Err(cancellation)))
        })) else {
            panic!("unexpected activation cancellation must be rejected");
        };
        let description = rejected
            .downcast_ref::<String>()
            .expect("the cancellation diagnostic owns its text");
        assert!(description.contains("activation task was cancelled unexpectedly"));
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

        assert_eq!(behavior.terminal_marker, 19);
        let settlement_status = settlements.settlement_status();
        assert_eq!(settlement_status, SettlementStatus::Accepted);
        assert!(ingress.control.is_empty());
        assert!(ingress.user.is_empty());
        assert!(activation_tasks.is_empty());
        assert_eq!(descendants, ());
        assert_eq!(completion, Completion::Stopped);
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
        assert_eq!(address, MailAddr::APPLICATION_ROOT);
        assert_eq!(behavior.terminal_marker, 19);
        assert_eq!(initialization.sends, NoSends);
        assert!(initialization.creates.is_empty());
        assert!(matches!(initialization.become_, behavior::Step::Stop(_)));
        assert!(control.is_empty());
        assert!(user.is_empty());
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

        assert_eq!(behavior.terminal_marker, 19);
        let settlement_status = settlements.settlement_status();
        assert_eq!(settlement_status, SettlementStatus::Accepted);
        assert!(ingress.control.is_empty());
        assert!(ingress.user.is_empty());
        assert!(activation_tasks.is_empty());
        assert_eq!(descendants, ());
        assert_eq!(completion, Completion::Stopped);
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

        assert_eq!(behavior.terminal_marker, 19);
        assert_eq!(initialization.sends, NoSends);
        assert!(initialization.creates.is_empty());
        assert!(matches!(initialization.become_, behavior::Step::Stop(_)));
        assert!(ingress.control.is_empty());
        assert!(ingress.user.is_empty());
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

        assert_eq!(origin, child_origin);
        assert_eq!(behavior.terminal_marker, 19);
        let settlement_status = settlements.settlement_status();
        assert_eq!(settlement_status, SettlementStatus::Accepted);
        assert!(control.is_empty());
        assert!(user.is_empty());
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
        assert_eq!(completion, Completion::Stopped);
    }
}
