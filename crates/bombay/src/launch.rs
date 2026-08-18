//! One concrete task-launch boundary for local actors.

#[cfg(test)]
use core::future::Future;
use core::hash::Hash;

use behavior::{
    Address, Behavior, BehaviorAddr, BehaviorMessage, BirthMode, CreationRejection, Never, Protocol,
};
use bombay_address::AddressSpace;
#[cfg(test)]
use bombay_engine::ActionsOf;
use bombay_engine::Driver;
use communication::Config;
use observe::Publisher;

use super::Incarnation;
use super::generation::{NormalizedRetirement, TerminalOverride};
use super::local::{ActorRef, CommitActions, LocalEnvironment};

/// Failure before a launched actor publishes its live reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("actor creation was rejected: {rejection:?}")]
pub(crate) struct SpawnError {
    rejection: CreationRejection,
}

impl SpawnError {
    pub(crate) const fn rejection(self) -> CreationRejection {
        self.rejection
    }
}

pub(crate) struct OwnedActor<B: Behavior> {
    pub(crate) actor: ActorRef<B::Protocol>,
    pub(crate) task: OwnedTask,
}

pub(crate) struct OwnedTask {
    task: tokio::task::JoinHandle<()>,
}

type Activation<B> = Result<ActorRef<<B as Behavior>::Protocol>, CreationRejection>;
type ActivationPublisher<B> = std::sync::Arc<std::sync::Mutex<Option<Publisher<Activation<B>>>>>;

impl OwnedTask {
    pub(crate) async fn retire(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

/// The exact Address-owned endpoint table for one concrete Behavior protocol.
#[doc(hidden)]
pub type LocalAddresses<P> = AddressSpace<<P as Protocol>::Addr, ActorRef<P>>;

pub(crate) async fn spawn_with<B, I>(
    addresses: LocalAddresses<B::Protocol>,
    config: Config,
    address: BehaviorAddr<B>,
    behavior: B,
    make_interpreter: impl FnOnce(
        communication::ControlSender<B::Event>,
        std::sync::Arc<TerminalOverride<BehaviorAddr<B>>>,
        crate::time::LocalTimers<B::Event>,
        crate::observation::FactQueue<B::Event>,
    ) -> I,
) -> Result<ActorRef<B::Protocol>, SpawnError>
where
    B: Behavior<Ph = Never> + Send + 'static,
    BehaviorAddr<B>: Hash + Clone + Send + Sync + 'static,
    <BehaviorAddr<B> as Address>::Nonce: Send + 'static,
    B::Event: behavior::InjectEvent<behavior::ShutdownRequested, behavior::Here> + Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    <B::Birth as BirthMode>::Child: Send + 'static,
    I: CommitActions<B> + Send + 'static,
    I::Error: Send + 'static,
{
    spawn_owned_with(addresses, config, address, behavior, make_interpreter)
        .await
        .map(|owned| owned.actor)
}

pub(crate) async fn spawn_owned_with<B, I>(
    addresses: LocalAddresses<B::Protocol>,
    config: Config,
    address: BehaviorAddr<B>,
    behavior: B,
    make_interpreter: impl FnOnce(
        communication::ControlSender<B::Event>,
        std::sync::Arc<TerminalOverride<BehaviorAddr<B>>>,
        crate::time::LocalTimers<B::Event>,
        crate::observation::FactQueue<B::Event>,
    ) -> I,
) -> Result<OwnedActor<B>, SpawnError>
where
    B: Behavior<Ph = Never> + Send + 'static,
    BehaviorAddr<B>: Hash + Clone + Send + Sync + 'static,
    <BehaviorAddr<B> as Address>::Nonce: Send + 'static,
    B::Event: behavior::InjectEvent<behavior::ShutdownRequested, behavior::Here> + Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    <B::Birth as BirthMode>::Child: Send + 'static,
    I: CommitActions<B> + Send + 'static,
    I::Error: Send + 'static,
{
    let (activation_publisher, activation) = observe::pair();
    let activation_publisher =
        std::sync::Arc::new(std::sync::Mutex::new(Some(activation_publisher)));
    let (termination_publisher, termination) = observe::pair();
    let terminal_override = std::sync::Arc::new(TerminalOverride::new());
    let published = activation_publisher.clone();
    let environment = LocalEnvironment::with_interpreter(
        address,
        addresses,
        config,
        termination,
        make_interpreter,
        terminal_override.clone(),
    )
    .publish_with(move |actor| {
        complete_activation::<B>(&published, Ok(actor));
    });
    let retirement = LaunchRetirement::<BehaviorAddr<B>, B>::new(
        NormalizedRetirement::new(termination_publisher, terminal_override),
        activation_publisher,
    );
    let incarnation = Incarnation::new(Driver::new(behavior, environment), retirement);
    let task = tokio::spawn(incarnation.run());

    match activation.await {
        Ok(actor) => Ok(OwnedActor {
            actor,
            task: OwnedTask { task },
        }),
        Err(rejection) => {
            let _ = task.await;
            Err(SpawnError { rejection })
        }
    }
}

/// Launch one local actor in a focused lower-layer test.
///
/// `commit` receives each complete action value exactly once, including
/// initialization.
#[cfg(test)]
pub(crate) async fn spawn<B, F, Fut, E>(
    addresses: LocalAddresses<B::Protocol>,
    config: Config,
    address: BehaviorAddr<B>,
    behavior: B,
    commit: F,
) -> Result<ActorRef<B::Protocol>, SpawnError>
where
    B: Behavior<Ph = Never> + Send + 'static,
    BehaviorAddr<B>: Hash + Clone + Send + Sync + 'static,
    <BehaviorAddr<B> as Address>::Nonce: Send + 'static,
    B::Event: behavior::InjectEvent<behavior::ShutdownRequested, behavior::Here> + Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    B::Sends: Send + 'static,
    B::Error: Send + 'static,
    <B::Birth as BirthMode>::Child: Send + 'static,
    F: FnMut(ActionsOf<B>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Send + 'static,
{
    spawn_with(addresses, config, address, behavior, move |_, _, _, _| {
        CommitWith(commit)
    })
    .await
}

struct LaunchRetirement<A: Address, B: Behavior<Protocol: Protocol<Addr = A>>> {
    normalized: NormalizedRetirement<A>,
    activation: ActivationPublisher<B>,
}

impl<A: Address, B: Behavior<Protocol: Protocol<Addr = A>>> LaunchRetirement<A, B> {
    fn new(normalized: NormalizedRetirement<A>, activation: ActivationPublisher<B>) -> Self {
        Self {
            normalized,
            activation,
        }
    }
}

fn complete_activation<B: Behavior>(
    publisher: &std::sync::Mutex<Option<Publisher<Activation<B>>>>,
    outcome: Activation<B>,
) {
    if let Some(publisher) = publisher
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    {
        publisher.complete(outcome);
    }
}

impl<A, B, BehaviorError, ActivationError, EnvironmentError>
    crate::Retirement<BehaviorError, ActivationError, EnvironmentError> for LaunchRetirement<A, B>
where
    A: Address,
    B: Behavior<Protocol: Protocol<Addr = A>>,
{
    fn retire(
        self,
        outcome: crate::IncarnationOutcome<BehaviorError, ActivationError, EnvironmentError>,
    ) {
        let rejection = match &outcome {
            crate::IncarnationOutcome::BehaviorFailed(_) => CreationRejection::InitializationFailed,
            crate::IncarnationOutcome::ActivationFailed(_)
            | crate::IncarnationOutcome::EnvironmentFailed(_)
            | crate::IncarnationOutcome::Panicked
            | crate::IncarnationOutcome::Cancelled
            | crate::IncarnationOutcome::Completed(_) => CreationRejection::EnvironmentFailed,
        };
        complete_activation::<B>(&self.activation, Err(rejection));
        self.normalized.retire(outcome);
    }
}

#[cfg(test)]
struct CommitWith<F>(F);

#[cfg(test)]
impl<B, F, Fut, E> CommitActions<B> for CommitWith<F>
where
    B: Behavior<Ph = Never>,
    F: FnMut(ActionsOf<B>) -> Fut,
    Fut: Future<Output = Result<(), E>> + Send,
{
    type Error = E;

    fn commit(&mut self, actions: ActionsOf<B>) -> impl Future<Output = Result<(), Self::Error>> {
        (self.0)(actions)
    }
}
