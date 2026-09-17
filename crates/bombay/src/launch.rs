//! One concrete task-launch boundary for local actors.

use core::fmt;
use core::hash::Hash;
use std::sync::{Arc, Mutex, PoisonError};

use crate::address::MailAddr;
use crate::observation::FactQueue;
use crate::observe::{self, Publisher};
use crate::terminal::{ActorOrigin, ActorRetirement, LocalOutcome, ProjectTerminal};
use crate::time::LocalTimers;
use crate::{IncarnationOutcome, Retirement};
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

#[cfg(test)]
use crate::interpret::{ActionSettlementOf, InterpretedActionSettlement};

use super::Incarnation;
#[cfg(test)]
use super::local::CapabilityRetirement;
use super::local::{
    ActorRef, CommitActions, EntityIngress, IngressMode, LocalEnvironment, LocalResidual,
    OwnerCancellation, StandardIngress,
};
use super::reports::LocalTerminalReports;
use super::termination::{TerminationPublication, TerminationSelection};

/// Failure before a launched actor publishes its live reference.
pub(crate) enum SpawnError<B, Descendants = ()>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
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
    HostRejected {
        behavior: B,
        initialization: ActionsOf<B>,
        error: ClaimError<MailAddr>,
        control: Vec<B::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<B>>>,
        descendants: Descendants,
    },
    Panicked,
    Cancelled,
    Ended(bombay_engine::Completion),
}

impl<B, Descendants> fmt::Debug for SpawnError<B, Descendants>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AllocationRejected { .. } => "AllocationRejected",
            Self::InitializationRejected { .. } => "InitializationRejected",
            Self::HostRejected { .. } => "HostRejected",
            Self::Panicked => "Panicked",
            Self::Cancelled => "Cancelled",
            Self::Ended(_) => "Ended",
        })
    }
}

impl<B, Descendants> fmt::Display for SpawnError<B, Descendants>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AllocationRejected { .. } => {
                formatter.write_str("the actor address source rejected allocation")
            }
            Self::InitializationRejected { .. } => {
                formatter.write_str("the behavior rejected initialization")
            }
            Self::HostRejected { .. } => {
                formatter.write_str("the actor host rejected initialization")
            }
            Self::Panicked => formatter.write_str("actor initialization panicked"),
            Self::Cancelled => formatter.write_str("actor initialization was cancelled"),
            Self::Ended(completion) => {
                write!(
                    formatter,
                    "the actor ended before publishing activation: {completion:?}"
                )
            }
        }
    }
}

impl<B, Descendants> std::error::Error for SpawnError<B, Descendants> where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>
{
}

impl<B, Descendants> SpawnError<B, Descendants>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    fn from_local(outcome: LocalOutcome<B, Descendants>) -> Self
    where
        B: BehaviorSettlements,
    {
        match outcome {
            IncarnationOutcome::BehaviorFailed {
                behavior,
                residual:
                    LocalResidual::Retired {
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
            IncarnationOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        descendants,
                        ..
                    },
                error,
            } => Self::HostRejected {
                behavior,
                initialization,
                error,
                control: ingress.control,
                user: ingress.user,
                descendants,
            },
            IncarnationOutcome::Panicked => Self::Panicked,
            IncarnationOutcome::Cancelled => Self::Cancelled,
            IncarnationOutcome::Completed { completion, .. } => Self::Ended(completion),
            IncarnationOutcome::BehaviorFailed {
                residual: LocalResidual::Uncommitted { .. },
                ..
            }
            | IncarnationOutcome::ActivationFailed {
                residual: LocalResidual::Retired { .. },
                ..
            }
            | IncarnationOutcome::SettlementFailed { .. } => {
                unreachable!("the active environment cannot fail before activation publication")
            }
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
            Self::Panicked => ActorRetirement::Panicked,
            Self::Cancelled => ActorRetirement::Cancelled,
            Self::Ended(completion) => ActorRetirement::EndedBeforeActivation { completion },
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
}

pub(crate) struct OwnedTask<B, Descendants>
where
    B: BehaviorSettlements,
{
    task: tokio::task::JoinHandle<LocalOutcome<B, Descendants>>,
    cancellation: oneshot::Sender<OwnerCancellation>,
}

pub(crate) struct ProjectedTask<B, Root>
where
    B: Behavior,
{
    task: tokio::task::JoinHandle<Root>,
    cancellation: oneshot::Sender<OwnerCancellation>,
    behavior: core::marker::PhantomData<fn() -> B>,
}

pub(crate) struct RootActor<B, Descendants>
where
    B: BehaviorSettlements,
{
    pub(crate) actor: ActorRef<B::Protocol>,
    pub(crate) task: OwnedTask<B, Descendants>,
}

#[derive(Clone, Copy)]
enum ActivationRejection {
    EndedBeforePublication,
}

type Activation<B> = Result<ActorRef<<B as Behavior>::Protocol>, ActivationRejection>;
type ActivationPublisher<B> = Arc<Mutex<Option<Publisher<Activation<B>>>>>;
impl<B, Descendants> OwnedTask<B, Descendants>
where
    B: BehaviorSettlements,
    B::Event: Send + 'static,
{
    pub(crate) async fn retire(self) -> LocalOutcome<B, Descendants> {
        let Self { task, cancellation } = self;
        drop(cancellation.send(OwnerCancellation));
        finish_owned_task(task).await
    }

    pub(crate) async fn finish(self) -> LocalOutcome<B, Descendants> {
        let Self { task, cancellation } = self;
        drop(cancellation);
        finish_owned_task(task).await
    }
}

async fn finish_owned_task<B, Descendants>(
    task: tokio::task::JoinHandle<LocalOutcome<B, Descendants>>,
) -> LocalOutcome<B, Descendants>
where
    B: BehaviorSettlements,
    B::Event: Send + 'static,
{
    let outcome = match task.await {
        Ok(outcome) => outcome,
        Err(error) if error.is_panic() => IncarnationOutcome::Panicked,
        Err(_) => IncarnationOutcome::Cancelled,
    };
    settle_local_outcome(outcome).await
}

async fn settle_local_outcome<B, Descendants>(
    outcome: LocalOutcome<B, Descendants>,
) -> LocalOutcome<B, Descendants>
where
    B: BehaviorSettlements,
    B::Event: Send + 'static,
{
    match outcome {
        IncarnationOutcome::Completed {
            behavior,
            residual,
            completion,
        } => IncarnationOutcome::Completed {
            behavior,
            residual: residual.settle_activation_tasks().await,
            completion,
        },
        IncarnationOutcome::BehaviorFailed {
            behavior,
            residual,
            error,
        } => IncarnationOutcome::BehaviorFailed {
            behavior,
            residual: residual.settle_activation_tasks().await,
            error,
        },
        IncarnationOutcome::ActivationFailed {
            behavior,
            residual,
            error,
        } => IncarnationOutcome::ActivationFailed {
            behavior,
            residual: residual.settle_activation_tasks().await,
            error,
        },
        IncarnationOutcome::SettlementFailed {
            behavior,
            residual,
            error,
        } => IncarnationOutcome::SettlementFailed {
            behavior,
            residual: residual.settle_activation_tasks().await,
            error,
        },
        IncarnationOutcome::Panicked => IncarnationOutcome::Panicked,
        IncarnationOutcome::Cancelled => IncarnationOutcome::Cancelled,
    }
}

async fn startup_failure<B, Descendants>(
    task: tokio::task::JoinHandle<LocalOutcome<B, Descendants>>,
) -> SpawnError<B, Descendants>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    B::Event: Send + 'static,
{
    match task.await {
        Ok(outcome) => SpawnError::from_local(settle_local_outcome(outcome).await),
        Err(error) if error.is_panic() => SpawnError::Panicked,
        Err(_) => SpawnError::Cancelled,
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
    crate::interpret::ActionSettlementOf<B>: Send + 'static,
    Root: Send + 'static,
{
    pub(crate) fn project<Owner, Role>(
        task: OwnedTask<B, Vec<Root>>,
        origin: ActorOrigin<Owner, Role>,
    ) -> Self
    where
        Owner: 'static,
        Role: 'static,
        Root: ProjectTerminal<ActorOrigin<Owner, Role>, ActorRetirement<B, Root>>,
    {
        let OwnedTask {
            task: actor_task,
            cancellation,
        } = task;
        let task = tokio::spawn(async move {
            Root::project(
                origin,
                ActorRetirement::from_local(finish_owned_task(actor_task).await),
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
            behavior,
        } = self;
        drop((cancellation.send(OwnerCancellation), behavior));
        task.await
            .unwrap_or_else(|error| panic!("typed terminal projection task failed: {error}"))
    }

    #[cfg(test)]
    pub(crate) async fn finish(self) -> Root {
        let Self {
            task,
            cancellation,
            behavior,
        } = self;
        drop((cancellation, behavior));
        task.await
            .unwrap_or_else(|error| panic!("typed terminal projection task failed: {error}"))
    }
}

/// The exact Address-owned endpoint table for one concrete Behavior protocol.
///
/// The alias is deliberately `#[doc(hidden)]`: applications never name it.
/// Hosting is claimed through [`HostedAddresses`], and the application runtime
/// owns the table this alias names.
#[doc(hidden)]
pub type LocalAddresses<P> = AddressSpace<<P as Protocol>::Addr, ActorRef<P>>;

/// Proof that an application-owned product hosts one concrete protocol's
/// Address-owned endpoint table.
///
/// This is the only hosting vocabulary: the proof carries the exact table
/// (`LocalAddresses<P>`) rather than naming a resolver, so hosting stays an
/// Address-ownership fact. The [`crate::HostedAddresses`] derive implements it
/// for every named `LocalAddresses<P>` field of an application-owned product.
#[diagnostic::on_unimplemented(
    message = "the actor system does not host actor protocol `{P}` locally",
    label = "missing local actor protocol",
    note = "add a `LocalAddresses<{P}>` and implement `HostedAddresses<{P}>` for the hosting product"
)]
pub trait HostedAddresses<P>
where
    P: Protocol,
    P::Addr: Hash,
{
    fn addresses(&self) -> &LocalAddresses<P>;
}

impl<P> HostedAddresses<P> for LocalAddresses<P>
where
    P: Protocol,
    P::Addr: Hash,
{
    fn addresses(&self) -> &LocalAddresses<P> {
        self
    }
}

impl<P, N> HostedAddresses<P> for Arc<N>
where
    P: Protocol,
    P::Addr: Hash,
    N: HostedAddresses<P>,
{
    fn addresses(&self) -> &LocalAddresses<P> {
        self.as_ref().addresses()
    }
}

#[cfg(test)]
pub(crate) async fn spawn_with<B, I>(
    addresses: LocalAddresses<B::Protocol>,
    config: Config,
    address: MailAddr,
    behavior: B,
    make_interpreter: impl FnOnce(
        communication::ControlSender<B::Event>,
        LocalTerminalReports,
        LocalTimers<B::Event>,
        FactQueue<MailAddr, B::Event>,
    ) -> I,
) -> Result<ActorRef<B::Protocol>, SpawnError<B, I::Retired>>
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
    spawn_owned_with(addresses, config, address, behavior, make_interpreter)
        .await
        .map(|owned| owned.actor)
}

pub(crate) async fn spawn_owned_with<B, I>(
    addresses: LocalAddresses<B::Protocol>,
    config: Config,
    address: MailAddr,
    behavior: B,
    make_interpreter: impl FnOnce(
        communication::ControlSender<B::Event>,
        LocalTerminalReports,
        LocalTimers<B::Event>,
        FactQueue<MailAddr, B::Event>,
    ) -> I,
) -> Result<OwnedActor<B, I::Retired>, SpawnError<B, I::Retired>>
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
    addresses: LocalAddresses<B::Protocol>,
    config: Config,
    address: MailAddr,
    behavior: B,
    make_interpreter: impl FnOnce(
        communication::ControlSender<B::Event>,
        LocalTerminalReports,
        LocalTimers<B::Event>,
        FactQueue<MailAddr, B::Event>,
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
    let (activation_publisher, activation) = observe::affine_pair();
    let activation_publisher = Arc::new(Mutex::new(Some(activation_publisher)));
    let (termination_publisher, termination) = observe::pair();
    let termination_selection = Arc::new(TerminationSelection::new());
    let (cancellation, cancellation_request) = oneshot::channel();
    let published = activation_publisher.clone();
    let interpreter_selection = termination_selection.clone();
    let environment = LocalEnvironment::<B, I, StandardIngress>::prepare(
        address,
        addresses,
        config,
        termination,
        cancellation_request,
        move |control, timers, facts| {
            make_interpreter(
                control,
                LocalTerminalReports::new(interpreter_selection),
                timers,
                facts,
            )
        },
    );
    let environment = environment.publish_with(move |actor| {
        complete_activation::<B>(&published, Ok(actor));
    });
    let retirement = LocalRetirement::<B>::new(
        TerminationPublication::new(termination_publisher, termination_selection),
        activation_publisher,
    );
    let task = tokio::spawn(Incarnation::new(Driver::new(behavior, environment), retirement).run());

    match activation.await {
        Ok(actor) => Ok(RootActor {
            actor,
            task: OwnedTask { task, cancellation },
        }),
        Err(ActivationRejection::EndedBeforePublication) => Err(startup_failure(task).await),
    }
}

#[allow(
    dead_code,
    reason = "the native Entity adapter remains private until typed application topology can materialize its requirements"
)]
pub(crate) async fn spawn_owned_entity_with<B, I>(
    addresses: LocalAddresses<B::Protocol>,
    config: Config,
    address: MailAddr,
    behavior: B,
    make_interpreter: impl FnOnce(
        communication::ControlSender<B::Event>,
        LocalTerminalReports,
        LocalTimers<B::Event>,
        FactQueue<MailAddr, B::Event>,
    ) -> I,
) -> Result<OwnedActor<B, I::Retired>, SpawnError<B, I::Retired>>
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
    addresses: LocalAddresses<B::Protocol>,
    config: Config,
    address: MailAddr,
    behavior: B,
    make_interpreter: impl FnOnce(
        communication::ControlSender<B::Event>,
        LocalTerminalReports,
        LocalTimers<B::Event>,
        FactQueue<MailAddr, B::Event>,
    ) -> I,
) -> Result<OwnedActor<B, I::Retired>, SpawnError<B, I::Retired>>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    M: IngressMode<B, Retired = User<MailAddr, BehaviorMessage<B>>> + 'static,
    B::Event: behavior::InjectEvent<ShutdownRequested, behavior::Here> + Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    <B::Birth as BirthMode>::Child: Send + 'static,
    I: CommitActions<B> + Send + 'static,
    crate::interpret::ActionSettlementOf<B>: ClassifySettlement + Send,
    I::Retired: Send + 'static,
{
    let (activation_publisher, activation) = observe::affine_pair();
    let activation_publisher = Arc::new(Mutex::new(Some(activation_publisher)));
    let (termination_publisher, termination) = observe::pair();
    let termination_selection = Arc::new(TerminationSelection::new());
    let (cancellation, cancellation_request) = oneshot::channel();
    let published = activation_publisher.clone();
    let interpreter_selection = termination_selection.clone();
    let environment = LocalEnvironment::<B, I, M>::prepare(
        address,
        addresses,
        config,
        termination,
        cancellation_request,
        move |control, timers, facts| {
            make_interpreter(
                control,
                LocalTerminalReports::new(interpreter_selection),
                timers,
                facts,
            )
        },
    );
    let control = environment.control();
    let environment = environment.publish_with(move |actor| {
        complete_activation::<B>(&published, Ok(actor));
    });
    let retirement = LocalRetirement::<B>::new(
        TerminationPublication::new(termination_publisher, termination_selection),
        activation_publisher,
    );
    let incarnation = Incarnation::new(Driver::new(behavior, environment), retirement);
    let task = tokio::spawn(incarnation.run());

    match activation.await {
        Ok(actor) => Ok(OwnedActor {
            actor,
            control,
            task: OwnedTask { task, cancellation },
        }),
        Err(ActivationRejection::EndedBeforePublication) => Err(startup_failure(task).await),
    }
}

/// Launch one local actor in a focused lower-layer test.
///
/// `commit` receives each complete action value exactly once, including
/// initialization.
#[cfg(test)]
pub(crate) async fn launch_inert<B, F>(
    addresses: LocalAddresses<B::Protocol>,
    config: Config,
    address: MailAddr,
    behavior: B,
    commit: F,
) -> Result<ActorRef<B::Protocol>, SpawnError<B>>
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
    spawn_with(addresses, config, address, behavior, move |_, _, _, _| {
        ObserveInertActions(commit)
    })
    .await
}

/// Launch one Entity-capable actor in a focused lower-layer test.
#[cfg(test)]
pub(crate) async fn launch_inert_entity<B, F>(
    addresses: LocalAddresses<B::Protocol>,
    config: Config,
    address: MailAddr,
    behavior: B,
    commit: F,
) -> Result<ActorRef<B::Protocol>, SpawnError<B>>
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
    .map(|owned| owned.actor)
}

struct LocalRetirement<B>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    termination: TerminationPublication<MailAddr>,
    activation: ActivationPublisher<B>,
}

impl<B> LocalRetirement<B>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn new(
        termination: TerminationPublication<MailAddr>,
        activation: ActivationPublisher<B>,
    ) -> Self {
        Self {
            termination,
            activation,
        }
    }
}

fn complete_activation<B>(
    publisher: &Mutex<Option<Publisher<Activation<B>>>>,
    outcome: Activation<B>,
) where
    B: Behavior,
{
    if let Some(publisher) = publisher
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
    {
        publisher.complete(outcome);
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
        ClaimError<MailAddr>,
    > for LocalRetirement<B>
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    type Output = LocalOutcome<B, Descendants>;

    fn retire(self, outcome: LocalOutcome<B, Descendants>) -> Self::Output {
        let publisher = self
            .activation
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let Some(publisher) = publisher else {
            match &outcome {
                IncarnationOutcome::Completed {
                    residual:
                        LocalResidual::Retired {
                            owner_cancellation: Some(_),
                            ..
                        },
                    ..
                } => self.termination.publish_owner_cancellation(),
                _ => self.termination.publish(&outcome),
            }
            return outcome;
        };
        publisher.complete(Err(ActivationRejection::EndedBeforePublication));
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

    async fn retire(self) -> CapabilityRetirement<B::Event, Self::Retired> {
        CapabilityRetirement::without_activations(self.retirement)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use behavior::{
        Actions, BehaviorActed, EventLayer, InitializationTurn, MessageProtocol, Never, NoBirths,
        NoSends, SettlementStatus, User,
    };
    use behavior_actors::ShutdownRequested;
    use bombay_engine::Completion;
    use communication::Config;

    use super::*;
    use crate::MailAddr;

    struct RootProbe {
        terminal_marker: u8,
    }

    #[derive(crate::TerminalProjection)]
    enum ProbeTerminal {
        Probe {
            origin: ActorOrigin<RootProbe>,
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

    #[tokio::test]
    async fn owned_root_task_returns_final_behavior_and_complete_residual() {
        let root = spawn_root_with(
            LocalAddresses::new(),
            Config::new(2),
            MailAddr::APPLICATION_ROOT,
            RootProbe { terminal_marker: 0 },
            |_, _, _, _| ObserveInertActions(ignore_root_actions),
        )
        .await
        .expect("the root must publish after initialization commitment");

        let outcome = root.task.finish().await;
        let IncarnationOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    settlements,
                    ingress,
                    activation_tasks,
                    descendants,
                    owner_cancellation,
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
        assert!(owner_cancellation.is_none());
        assert_eq!(descendants, ());
        assert_eq!(completion, Completion::Stopped);
    }

    #[tokio::test]
    async fn owned_child_task_returns_final_behavior_and_complete_residual() {
        let child = spawn_owned_with(
            LocalAddresses::new(),
            Config::new(2),
            MailAddr(1),
            RootProbe { terminal_marker: 0 },
            |_, _, _, _| ObserveInertActions(ignore_root_actions),
        )
        .await
        .expect("the child must publish after initialization commitment");

        let outcome = child.task.finish().await;
        let IncarnationOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    settlements,
                    ingress,
                    activation_tasks,
                    descendants,
                    owner_cancellation,
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
        assert!(owner_cancellation.is_none());
        assert_eq!(descendants, ());
        assert_eq!(completion, Completion::Stopped);
    }

    #[tokio::test]
    async fn projected_child_retirement_preserves_origin_state_and_descendants() {
        let descendant_origin = ActorOrigin::<RootProbe>::child(MailAddr(2), 0);
        let descendant = ProbeTerminal::Probe {
            origin: descendant_origin,
            terminal: ActorRetirement::Cancelled,
        };
        let child = spawn_owned_with(
            LocalAddresses::new(),
            Config::new(2),
            MailAddr(1),
            RootProbe { terminal_marker: 0 },
            |_, _, _, _| ObserveActionsWithRetirement {
                observe: ignore_root_actions,
                retirement: vec![descendant],
            },
        )
        .await
        .expect("the child must publish after initialization commitment");
        let child_origin = ActorOrigin::<RootProbe, Here>::child(MailAddr(1), 0);

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

        assert_eq!(origin, child_origin.into_declared_root());
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
