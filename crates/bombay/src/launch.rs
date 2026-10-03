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

    async fn next_local_event(&mut self) -> B::Event {
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
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(ingress.user.len(), 0);
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
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(ingress.user.len(), 0);
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
        assert_eq!(completion, Completion::Stopped);
    }
}

#[cfg(all(test, tokio_unstable))]
mod independent_actor_execution {
    use crate::actor_execution::tests::{
        allocations_during, begin_allocations, current_allocations, finish_allocations,
    };
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::future::pending;
    use std::hint::black_box;
    use std::sync::{Arc, Mutex, mpsc};
    use std::thread::{self, ThreadId};
    use std::time::Instant;

    use crate::actors::ActorExt;
    use behavior::{
        ActionItem, Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, Here,
        InitializationTurn, Inside, InterpretItem, Interpretation, InterpreterRequest,
        InterpreterRequests, ItemSettlement, MessageProtocol, Never, NoBirthProtocols, NoBirths,
        NoReturnToEmitter, NoSends, Own, SendEffects, SettledItem, SourceCustody,
        SourceSettlementCustody, Step, User,
    };
    use behavior_actors::StopOnShutdown;
    use bombay_address::AddressSpace;
    use bombay_engine::{ActionsOf, Completion};
    use communication::Config;
    use tokio::runtime::Builder;
    use tokio::sync::oneshot;
    use tokio::sync::{Barrier as WorkBarrier, Mutex as WorkMutex};
    use tokio::task::Id;

    use super::{LocalOutcome, spawn_root_with};
    use crate::interpret::ActionSettlementOf;
    use crate::local::{CapabilityRetirement, CommitActions, LocalResidual};
    use crate::{ActorExecutionOutcome, MailAddr};

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
        async fn interpret_item(
            &mut self,
            work: ComputeWork,
        ) -> ItemSettlement<ComputeWork, ComputedWork, Never, Never> {
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
            ItemSettlement::Accepted(ComputedWork {
                values: work.values,
                sum,
            })
        }
    }

    impl CommitActions<HostedComputingActor> for WorkInterpreter {
        type Retired = ();

        async fn commit(
            &mut self,
            actions: ActionsOf<HostedComputingActor>,
        ) -> Interpretation<ActionSettlementOf<HostedComputingActor>> {
            actions.interpret::<_, WorkEvent, Here>(self).await
        }

        async fn offer_next(
            &mut self,
            settlement: ActionSettlementOf<HostedComputingActor>,
        ) -> SourceCustody<ActionSettlementOf<HostedComputingActor>> {
            let custody =
                SourceSettlementCustody::<Self, WorkEvent>::offer_next_to_source(settlement, self)
                    .await;
            match &custody {
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
            custody
        }

        async fn next_local_event(&mut self) -> WorkEvent {
            if let Some(notice) = self.waiting_notice.take() {
                drop(notice.send(ExecutionObservation::WaitingForCommand(self.owner)));
            }
            pending().await
        }

        #[expect(
            clippy::unused_async_trait_impl,
            reason = "Keep retirement on the polled runtime port."
        )]
        async fn retire(self) -> CapabilityRetirement<WorkEvent, ()> {
            CapabilityRetirement::without_activations(())
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
        let oak_outcome = runtime.block_on(oak.task.finish());
        let ash_outcome = runtime.block_on(ash.task.finish());
        drop(release);
        drop(runtime);
        observations.extend(observation_receiver.try_iter());
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
                    settlements,
                    ingress,
                    activation_tasks,
                    descendants,
                },
            completion,
        } = outcome
        else {
            panic!("the actual actor did not complete its retirement")
        };
        assert_eq!(completion, Completion::Stopped);
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
        async fn interpret_item(
            &mut self,
            work: ComputeWork,
        ) -> ItemSettlement<ComputeWork, ComputedWork, Never, Never> {
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
            ItemSettlement::Accepted(ComputedWork {
                values: work.values,
                sum,
            })
        }
    }

    impl CommitActions<HostedComputingActor> for MeasuredWork {
        type Retired = Vec<(usize, usize)>;
        async fn commit(
            &mut self,
            actions: ActionsOf<HostedComputingActor>,
        ) -> Interpretation<ActionSettlementOf<HostedComputingActor>> {
            actions.interpret::<_, WorkEvent, Here>(self).await
        }
        async fn offer_next(
            &mut self,
            settlement: ActionSettlementOf<HostedComputingActor>,
        ) -> SourceCustody<ActionSettlementOf<HostedComputingActor>> {
            let custody =
                SourceSettlementCustody::<Self, WorkEvent>::offer_next_to_source(settlement, self)
                    .await;
            if self.observations.len() == self.expected
                && let Some(completion) = self.completion.take()
            {
                let delivered = completion.send(());
                assert!(delivered.is_ok());
            }
            custody
        }
        async fn next_local_event(&mut self) -> WorkEvent {
            pending().await
        }
        #[expect(
            clippy::unused_async_trait_impl,
            reason = "The owned work receipts retire through the runtime port."
        )]
        async fn retire(self) -> CapabilityRetirement<WorkEvent, Self::Retired> {
            CapabilityRetirement::without_activations(self.observations)
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
                    outcomes.push(actor.task.finish().await);
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
                        settlements,
                        ingress,
                        activation_tasks,
                        descendants,
                    },
                completion,
            } = outcome
            else {
                panic!("whole measured actor retirement")
            };
            assert_eq!(completion, Completion::Stopped);
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
